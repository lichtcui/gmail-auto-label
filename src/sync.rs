//! IMAP sync module.
//!
//! Connects to Gmail via IMAP, fetches all inbox messages with full body content,
//! parses them, groups by thread, and summarizes each thread via LLM.
//! The summaries are stored in the cache so the main pipeline can use them
//! instead of calling the Gmail API search endpoint.

use std::collections::HashMap;

use rayon::prelude::*;

use crate::errors::AppError;
use crate::llm::{LlmConfig, call_chat};
use crate::models::{CacheData, SyncConfig, SyncedThread, ThreadInfo};
use crate::utils::{auto_llm_workers, log, now_ts};

/// How many messages to fetch in a single IMAP FETCH command
const FETCH_BATCH_SIZE: usize = 100;
/// Maximum characters of body text to send to LLM (also used as prompt limit)
const MAX_BODY_CHARS: usize = 3000;
/// Maximum characters in the LLM summary
const MAX_SUMMARY_CHARS: usize = 500;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Run the IMAP sync: connect, fetch all inbox messages, parse, group by thread,
/// and summarize each new thread via LLM. Results are stored in `cache.synced_threads`.
pub(crate) fn run_sync(
    config: &SyncConfig,
    cache: &mut CacheData,
    llm_config: &LlmConfig,
) -> Result<(), AppError> {
    log(&format!(
        "IMAP_SYNC: connecting to {}:{} ...",
        config.imap_host, config.imap_port
    ));

    let mut session = connect_imap(config)?;
    session
        .select("INBOX")
        .map_err(|e| AppError::Other(format!("IMAP SELECT INBOX failed: {e}")))?;

    let all_uids = search_all_uids(&mut session)?;
    log(&format!(
        "IMAP_SYNC: found {} messages in INBOX",
        all_uids.len()
    ));

    // Limit to most recent N messages if --sync-max is set
    let uids: &[u32] = if config.max_messages > 0 && all_uids.len() > config.max_messages {
        let start = all_uids.len() - config.max_messages;
        log(&format!(
            "IMAP_SYNC: limiting to {} most recent messages",
            config.max_messages
        ));
        &all_uids[start..]
    } else {
        &all_uids
    };

    let messages = fetch_all_messages(&mut session, uids)?;
    log(&format!("IMAP_SYNC: parsed {} messages", messages.len()));

    let threads = group_by_thread(&messages);
    log(&format!(
        "IMAP_SYNC: grouped into {} threads",
        threads.len()
    ));

    // Collect uncached thread data for parallel summarization
    struct SyncJob {
        thread_id: String,
        sender: String,
        subject: String,
        body: String,
        message_count: usize,
    }

    let jobs: Vec<SyncJob> = threads
        .iter()
        .filter(|(tid, _)| !cache.synced_threads.contains_key(*tid))
        .map(|(tid, msgs)| SyncJob {
            thread_id: tid.clone(),
            sender: msgs[0].sender.clone(),
            subject: msgs[0].subject.clone(),
            body: msgs
                .iter()
                .map(|m| m.body_text.as_str())
                .collect::<Vec<_>>()
                .join("\n---\n"),
            message_count: msgs.len(),
        })
        .collect();

    let skipped = threads.len() - jobs.len();
    let total_jobs = jobs.len();

    if total_jobs == 0 {
        log(&format!(
            "IMAP_SYNC: all {} threads already cached, nothing to summarize",
            threads.len()
        ));
        return Ok(());
    }

    // Determine parallel worker count (same heuristic as the main classify pipeline)
    let sync_workers = auto_llm_workers();
    log(&format!(
        "IMAP_SYNC: summarizing {} threads with {} workers...",
        total_jobs, sync_workers
    ));

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(sync_workers)
        .build()
        .map_err(|e| AppError::Other(format!("Failed to create sync thread pool: {e}")))?;

    let now = now_ts();
    let results: Vec<(String, Result<SyncedThread, String>)> = pool.install(|| {
        jobs.into_par_iter()
            .map(|job| {
                let truncated_body: String = job.body.chars().take(MAX_BODY_CHARS).collect();
                match summarize_thread_body(&truncated_body, llm_config) {
                    Ok(summary) => {
                        let thread_id = job.thread_id.clone();
                        (
                            thread_id,
                            Ok(SyncedThread {
                                thread_id: job.thread_id,
                                sender: job.sender,
                                subject: job.subject,
                                body_summary: summary,
                                message_count: job.message_count,
                                ts: now,
                            }),
                        )
                    }
                    Err(e) => (job.thread_id, Err(e.to_string())),
                }
            })
            .collect()
    });

    let mut summarized = 0usize;
    let mut failed = 0usize;
    for (thread_id, result) in results {
        match result {
            Ok(thread) => {
                cache.synced_threads.insert(thread_id.clone(), thread);
                summarized += 1;
                log(&format!(
                    "IMAP_SYNC: summarized thread {} ({}/{})",
                    thread_id.chars().take(12).collect::<String>(),
                    summarized + failed,
                    total_jobs
                ));
            }
            Err(msg) => {
                failed += 1;
                log(&format!(
                    "IMAP_SYNC_FAILED: thread {} — {}. Skipping.",
                    thread_id.chars().take(12).collect::<String>(),
                    msg
                ));
            }
        }
    }

    log(&format!(
        "IMAP_SYNC: done — {} new threads summarized, {} already cached, {} total",
        summarized,
        skipped,
        cache.synced_threads.len()
    ));
    Ok(())
}

/// Convert synced threads from cache into `ThreadInfo` vec suitable for the
/// existing classification pipeline. The LLM summary is placed in `snippet`.
pub(crate) fn use_synced_data(cache: &CacheData) -> Vec<ThreadInfo> {
    let mut threads: Vec<ThreadInfo> = cache
        .synced_threads
        .values()
        .map(|st| ThreadInfo {
            id: st.thread_id.clone(),
            sender: st.sender.clone(),
            subject: st.subject.clone(),
            snippet: st.body_summary.clone(),
        })
        .collect();
    // Stable order: by thread_id
    threads.sort_by(|a, b| a.id.cmp(&b.id));
    threads
}

// ---------------------------------------------------------------------------
// IMAP connection and fetching
// ---------------------------------------------------------------------------

fn connect_imap(
    config: &SyncConfig,
) -> Result<imap::Session<native_tls::TlsStream<std::net::TcpStream>>, AppError> {
    let tls = native_tls::TlsConnector::builder()
        .build()
        .map_err(|e| AppError::Other(format!("IMAP TLS init failed: {e}")))?;

    let client = imap::connect(
        (config.imap_host.as_str(), config.imap_port),
        config.imap_host.as_str(),
        &tls,
    )
    .map_err(|e| AppError::Other(format!("IMAP connect failed: {e}")))?;

    let session = client
        .login(&config.imap_user, &config.imap_pass)
        .map_err(|(e, _)| AppError::Other(format!("IMAP login failed: {e}")))?;

    Ok(session)
}

fn search_all_uids(
    session: &mut imap::Session<native_tls::TlsStream<std::net::TcpStream>>,
) -> Result<Vec<u32>, AppError> {
    let ids: Vec<u32> = session
        .uid_search("ALL")
        .map_err(|e| AppError::Other(format!("IMAP SEARCH ALL failed: {e}")))?
        .into_iter()
        .collect();
    Ok(ids)
}

/// A raw parsed message from IMAP
struct ParsedMessage {
    _uid: u32,
    thread_id: String,
    sender: String,
    subject: String,
    body_text: String,
}

fn fetch_all_messages(
    session: &mut imap::Session<native_tls::TlsStream<std::net::TcpStream>>,
    uids: &[u32],
) -> Result<Vec<ParsedMessage>, AppError> {
    let mut all_messages = Vec::with_capacity(uids.len());

    for chunk in uids.chunks(FETCH_BATCH_SIZE) {
        let uid_set = chunk
            .iter()
            .map(|u| u.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let fetches = session
            .uid_fetch(&uid_set, "(BODY[] X-GM-THRID)")
            .map_err(|e| AppError::Other(format!("IMAP FETCH failed: {e}")))?;

        // uid_fetch returns the fetched data directly (not Option in this version)
        for fetch in fetches.iter() {
            let uid = fetch.uid.unwrap_or(0);
            let body_slice = match fetch.body() {
                Some(b) => b,
                None => continue,
            };

            let parsed = match mailparse::parse_mail(body_slice) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let thread_id = extract_header(&parsed, "X-GM-THRID")
                .or_else(|| extract_header(&parsed, "Message-ID").map(|s| tidify(&s)))
                .unwrap_or_else(|| format!("uid:{uid}"));

            let sender = extract_header(&parsed, "From").unwrap_or_default();
            let subject = extract_header(&parsed, "Subject").unwrap_or_default();
            let body_text = extract_body_text(&parsed);

            all_messages.push(ParsedMessage {
                _uid: uid,
                thread_id,
                sender,
                subject,
                body_text,
            });
        }
    }

    Ok(all_messages)
}

// ---------------------------------------------------------------------------
// MIME / email parsing helpers
// ---------------------------------------------------------------------------

fn extract_header(parsed: &mailparse::ParsedMail, key: &str) -> Option<String> {
    parsed
        .headers
        .iter()
        .find(|h| h.get_key().eq_ignore_ascii_case(key))
        .map(|h| h.get_value())
        .map(|v| v.trim().to_string())
}

fn extract_body_text(parsed: &mailparse::ParsedMail) -> String {
    let mut text = String::new();
    match parsed.ctype.mimetype.as_str() {
        "text/plain" => {
            if let Ok(body) = parsed.get_body() {
                text.push_str(&body);
            }
        }
        "text/html" => {
            if let Ok(body) = parsed.get_body() {
                text.push_str(&strip_html(&body));
            }
        }
        "message/rfc822" => {
            if let Ok(raw) = parsed.get_body_raw() {
                if let Ok(inner) = mailparse::parse_mail(&raw) {
                    text.push_str(&extract_body_text(&inner));
                }
            }
        }
        _ => {
            for sub in &parsed.subparts {
                text.push_str(&extract_body_text(sub));
            }
        }
    }
    text
}

fn strip_html(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut in_entity = false;
    let mut entity = String::new();

    for c in html.chars() {
        if in_tag {
            if c == '>' {
                in_tag = false;
            }
            continue;
        }
        if c == '<' {
            in_tag = true;
            continue;
        }
        if c == '&' {
            in_entity = true;
            entity.clear();
            continue;
        }
        if in_entity {
            if c == ';' {
                in_entity = false;
                match entity.as_str() {
                    "nbsp" => out.push(' '),
                    "amp" => out.push('&'),
                    "lt" => out.push('<'),
                    "gt" => out.push('>'),
                    "quot" => out.push('"'),
                    _ => decode_numerical_entity(&entity, &mut out),
                }
                continue;
            }
            entity.push(c);
            continue;
        }
        out.push(c);
    }

    // Collapse whitespace runs into a single space, trim leading/trailing
    let mut cleaned = String::with_capacity(out.len());
    let mut prev_space = false;
    for c in out.chars() {
        if c.is_whitespace() || c == '\u{00a0}' {
            if !prev_space {
                cleaned.push(' ');
                prev_space = true;
            }
        } else {
            cleaned.push(c);
            prev_space = false;
        }
    }
    cleaned.trim().to_string()
}

/// Decode `&#N;` (decimal) or `&#xH;` (hex) numerical HTML entities.
/// Silently ignores invalid encodings (output unchanged).
fn decode_numerical_entity(entity: &str, out: &mut String) {
    if let Some(rest) = entity.strip_prefix('#') {
        if let Some(hex) = rest.strip_prefix('x').or_else(|| rest.strip_prefix('X')) {
            if let Ok(codepoint) = u32::from_str_radix(hex, 16) {
                if let Some(ch) = char::from_u32(codepoint) {
                    out.push(ch);
                }
            }
        } else if let Ok(codepoint) = rest.parse::<u32>() {
            if let Some(ch) = char::from_u32(codepoint) {
                out.push(ch);
            }
        }
    }
}

/// Normalise an arbitrary string into a compact thread-id-like string.
fn tidify(s: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(s.as_bytes());
    let h = hasher.finalize();
    format!("{:x}", h).chars().take(20).collect()
}

// ---------------------------------------------------------------------------
// Thread grouping
// ---------------------------------------------------------------------------

fn group_by_thread(messages: &[ParsedMessage]) -> HashMap<String, Vec<&ParsedMessage>> {
    let mut map: HashMap<String, Vec<&ParsedMessage>> = HashMap::new();
    for msg in messages {
        map.entry(msg.thread_id.clone()).or_default().push(msg);
    }
    map
}

// ---------------------------------------------------------------------------
// LLM summarization
// ---------------------------------------------------------------------------

fn summarize_thread_body(body: &str, llm_config: &LlmConfig) -> Result<String, AppError> {
    let prompt = format!(
        r#"You are an email summarization assistant. \
Provide a concise summary of this email in one or two sentences ({} chars max). \
Focus on: who sent it, what it's about, and any action required.

Output STRICT JSON ONLY - no other text, no markdown:
{{"summary":"your concise summary here"}}

Email body:
{}"#,
        MAX_SUMMARY_CHARS,
        body.chars().take(MAX_BODY_CHARS).collect::<String>()
    );

    let raw = call_chat(&prompt, llm_config)?;

    // Try JSON parse
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
        if let Some(s) = v.get("summary").and_then(|x| x.as_str()) {
            let s = s.trim().to_string();
            if !s.is_empty() {
                return Ok(s.chars().take(MAX_SUMMARY_CHARS).collect());
            }
        }
    }

    // Fallback: body head
    let fallback: String = body.chars().take(200).collect();
    log("LLM_SUMMARY_FALLBACK: using body head (JSON parse failed or summary field missing)");
    Ok(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_html_removes_tags() {
        let html = "<p>Hello <b>world</b>!</p>";
        assert_eq!(strip_html(html), "Hello world!");
    }

    #[test]
    fn test_strip_html_handles_entities() {
        let html = "foo&nbsp;bar &amp; baz";
        assert_eq!(strip_html(html), "foo bar & baz");
    }

    #[test]
    fn test_strip_html_multiline() {
        let html = "<div>\n  <p>Line one</p>\n  <p>Line two</p>\n</div>";
        assert_eq!(strip_html(html), "Line one Line two");
    }

    #[test]
    fn test_strip_html_numerical_entities() {
        assert_eq!(strip_html("foo&#160;bar"), "foo bar");
        assert_eq!(strip_html("&#x41;"), "A");
        assert_eq!(strip_html("&#x61;"), "a");
        assert_eq!(strip_html("&#38;"), "&");
        assert_eq!(strip_html("&#x2F;"), "/");
        // Invalid — left unchanged (empty in output)
        assert_eq!(strip_html("&#xZZ;"), "");
        assert_eq!(strip_html("&#;"), "");
    }

    #[test]
    fn test_tidify_produces_consistent_hash() {
        let a = tidify("msg-abc-123");
        let b = tidify("msg-abc-123");
        let c = tidify("different");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 20);
    }
}
