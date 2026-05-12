//! IMAP sync module.
//!
//! Connects to Gmail via IMAP, fetches all inbox messages with full body content,
//! parses them, groups by thread, and summarizes each thread via LLM.
//! The summaries are stored in the cache so the main pipeline can use them
//! instead of calling the Gmail API search endpoint.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rayon::prelude::*;

use crate::cache::save_cache;
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
    restore_spam: bool,
) -> Result<(), AppError> {
    log(&format!(
        "IMAP_SYNC: connecting to {}:{} ...",
        config.imap_host, config.imap_port
    ));

    let mut session = connect_imap(config)?;

    // Phase 0: restore spam emails to Inbox before fetching
    if restore_spam {
        restore_spam_emails(&mut session, config)?;
        // Reconnect — Gmail may invalidate the session after label modifications
        session = connect_imap(config)?;
    }

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

    // Build thread_id -> [UIDs] map for later label application via IMAP
    let mut thread_uids: HashMap<String, Vec<u32>> = HashMap::new();
    for (tid, msgs) in &threads {
        thread_uids.insert(
            tid.clone(),
            msgs.iter().map(|m| m._uid).collect(),
        );
    }

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

    if skipped > 0 {
        log(&format!(
            "IMAP_SYNC: {} threads already cached, skipping them",
            skipped
        ));
    }

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
    let mut summarized = 0usize;

    // Show ~20 progress lines across the entire run
    let log_every = (total_jobs / 20).max(1);

    // Process jobs in batches of 40 so progress is saved frequently.
    // If interrupted, only the current batch's work is lost.
    for batch in jobs.chunks(40) {
        let done_so_far = summarized;
        let batch_progress = Arc::new(AtomicUsize::new(0));
        let batch_log_every = log_every;

        let batch_results: Vec<(String, Result<SyncedThread, String>)> = pool.install(|| {
            batch
                .into_par_iter()
                .map(|job| {
                    let truncated_body: String =
                        job.body.chars().take(MAX_BODY_CHARS).collect();
                    let result = match summarize_thread_body(&truncated_body, llm_config) {
                        Ok(summary) => (
                            job.thread_id.clone(),
                            Ok(SyncedThread {
                                thread_id: job.thread_id.clone(),
                                sender: job.sender.clone(),
                                subject: job.subject.clone(),
                                body_summary: summary,
                                message_count: job.message_count,
                                uids: thread_uids
                                    .get(&job.thread_id)
                                    .cloned()
                                    .unwrap_or_default(),
                                ts: now,
                            }),
                        ),
                        Err(e) => (job.thread_id.clone(), Err(e.to_string())),
                    };
                    let done = done_so_far + batch_progress.fetch_add(1, Ordering::Relaxed) + 1;
                    if done % batch_log_every == 0 || done == total_jobs {
                        log(&format!(
                            "IMAP_SYNC: summarized {}/{} threads",
                            done, total_jobs
                        ));
                    }
                    result
                })
                .collect()
        });

        for (thread_id, result) in batch_results {
            match result {
                Ok(thread) => {
                    cache.synced_threads.insert(thread_id.clone(), thread);
                    summarized += 1;
                    log(&format!(
                        "IMAP_SYNC: summarized thread {} ({}/{})",
                        thread_id.chars().take(12).collect::<String>(),
                        summarized,
                        total_jobs
                    ));
                }
                Err(msg) => {
                    log(&format!(
                        "IMAP_SYNC_FAILED: thread {} — {}. Skipping.",
                        thread_id.chars().take(12).collect::<String>(),
                        msg
                    ));
                }
            }
        }

        log(&format!(
            "IMAP_SYNC: saving cache ({}/{})...",
            summarized, total_jobs
        ));
        if let Err(e) = save_cache(&config.cache_file, cache) {
            log(&format!("IMAP_SYNC_SAVE_WARN: failed to save cache — {}", e));
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

/// Restore all emails from the Spam folder to Inbox.
///
/// Tries common Gmail Spam folder names in order: `[Gmail]/Spam` (English),
/// `[Gmail]/&ANTNaBpZAEg-` (Chinese). For each message in Spam, copies it to
/// INBOX and removes the Spam label.
fn restore_spam_emails(
    session: &mut imap::Session<native_tls::TlsStream<std::net::TcpStream>>,
    _config: &SyncConfig,
) -> Result<(), AppError> {
    // Try to find the Spam folder — iterate over possible names
    let spam_folders = [
        "[Gmail]/Spam",
        "[Gmail]/&ANTNaBpZAEg-", // Chinese: 垃圾邮件
    ];

    let spam_folder = spam_folders
        .iter()
        .find(|&&name| {
            session
                .select(name)
                .map(|_| {
                    log(&format!("SPAM_RESTORE: selected folder \"{name}\""));
                    true
                })
                .unwrap_or(false)
        })
        .cloned();

    let spam_folder = match spam_folder {
        Some(f) => f,
        None => {
            log("SPAM_RESTORE: no Spam folder found (tried English and Chinese names)");
            return Ok(());
        }
    };

    let uids: Vec<u32> = session
        .uid_search("ALL")
        .map_err(|e| AppError::Other(format!("SPAM_RESTORE SEARCH failed: {e}")))?
        .into_iter()
        .collect();

    if uids.is_empty() {
        log("SPAM_RESTORE: no messages in Spam folder");
        return Ok(());
    }

    log(&format!(
        "SPAM_RESTORE: restoring {} messages from Spam to Inbox...",
        uids.len()
    ));

    let mut restored = 0usize;
    for chunk in uids.chunks(100) {
        let uid_str = chunk
            .iter()
            .map(|u| u.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Copy to Inbox
        if let Err(e) = session.run_command(format!("UID COPY {uid_str} INBOX")) {
            log(&format!("SPAM_RESTORE COPY failed for batch: {e}"));
            continue;
        }

        // Remove Spam label and add Inbox label
        if let Err(e) = session.run_command(format!(
            "UID STORE {uid_str} -X-GM-LABELS (\\Spam)"
        )) {
            log(&format!("SPAM_RESTORE STORE -Spam failed for batch: {e}"));
        }

        restored += chunk.len();
        log(&format!("SPAM_RESTORE: restored {}/{} messages", restored, uids.len()));
    }

    log(&format!(
        "SPAM_RESTORE: done — {restored} messages restored from {spam_folder}"
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
            snippet: if st.body_summary == "No email content provided." {
                st.subject.clone()
            } else {
                st.body_summary.clone()
            },
        })
        .collect();
    // Stable order: by thread_id
    threads.sort_by(|a, b| a.id.cmp(&b.id));
    threads
}

// ---------------------------------------------------------------------------
// IMAP connection and fetching
// ---------------------------------------------------------------------------

pub(crate) fn connect_imap(
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

fn parse_fetch(fetch: &imap::types::Fetch) -> Option<ParsedMessage> {
    let uid = fetch.uid.unwrap_or(0);
    let body_slice = fetch.body()?;
    let parsed = mailparse::parse_mail(body_slice).ok()?;

    let thread_id = extract_header(&parsed, "X-GM-THRID")
        .or_else(|| extract_header(&parsed, "Message-ID").map(|s| tidify(&s)))
        .unwrap_or_else(|| format!("uid:{uid}"));

    let sender = extract_header(&parsed, "From").unwrap_or_default();
    let subject = extract_header(&parsed, "Subject").unwrap_or_default();
    let body_text = extract_body_text(&parsed);

    Some(ParsedMessage {
        _uid: uid,
        thread_id,
        sender,
        subject,
        body_text,
    })
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

        // Try batch fetch first. If it fails (e.g. the `imap` crate can't parse a
        // Gmail extension response), fall back to individual fetches, skipping bad UIDs.
        let batch_result = session.uid_fetch(&uid_set, "(BODY[])");
        match batch_result {
            Ok(fetches) => {
                for fetch in fetches.iter() {
                    if let Some(parsed) = parse_fetch(&fetch) {
                        all_messages.push(parsed);
                    }
                }
            }
            Err(e) => {
                log(&format!(
                    "IMAP FETCH batch of {} failed: {}, retrying individually...",
                    chunk.len(),
                    e
                ));
                for &uid in chunk {
                    match session.uid_fetch(&uid.to_string(), "(BODY[])") {
                        Ok(fetches) => {
                            for fetch in fetches.iter() {
                                if let Some(parsed) = parse_fetch(&fetch) {
                                    all_messages.push(parsed);
                                }
                            }
                        }
                        Err(e) => {
                            log(&format!(
                                "IMAP FETCH failed for UID {}, skipping: {}",
                                uid, e
                            ));
                        }
                    }
                }
            }
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

    let raw = call_chat(&prompt, llm_config, None)?;

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

/// Apply Gmail labels to threads via IMAP STORE +X-GM-LABELS.
/// Uses the stored UIDs from the sync phase to avoid Gmail API rate limits.
pub(crate) fn apply_labels_via_imap(
    config: &SyncConfig,
    grouped: &HashMap<String, Vec<String>>,
    cache: &CacheData,
) -> Result<(), AppError> {
    let mut session = connect_imap(config)?;
    session
        .select("INBOX")
        .map_err(|e| AppError::Other(format!("IMAP SELECT INBOX failed: {e}")))?;

    // Collect all thread_ids that need UID lookup via IMAP search
    let mut missing_uids: Vec<String> = Vec::new();
    let mut all_thread_uids: HashMap<&str, Vec<u32>> = HashMap::new();
    for (_label, thread_ids) in grouped.iter() {
        for tid in thread_ids {
            if all_thread_uids.contains_key(tid.as_str()) {
                continue;
            }
            if let Some(st) = cache.synced_threads.get(tid) {
                if !st.uids.is_empty() {
                    all_thread_uids.insert(tid.as_str(), st.uids.clone());
                    continue;
                }
            }
            missing_uids.push(tid.clone());
        }
    }

    // Search for UIDs of threads that weren't cached with UIDs
    if !missing_uids.is_empty() {
        log(&format!(
            "IMAP_UID_SEARCH: looking up UIDs for {} threads via X-GM-THRID",
            missing_uids.len()
        ));
        let mut search_ok = true;
        for tid in &missing_uids {
            if !search_ok {
                log(&format!("IMAP_UID_SEARCH_SKIP: thread={tid} (previous search corrupted session)"));
                continue;
            }
            match session.uid_search(format!("X-GM-THRID {tid}")) {
                Ok(ids) => {
                    let uids: Vec<u32> = ids.into_iter().collect();
                    if !uids.is_empty() {
                        all_thread_uids.insert(tid.as_str(), uids);
                    }
                }
                Err(e) => {
                    log(&format!(
                        "IMAP_UID_SEARCH_FAILED: thread={tid} error={e}"
                    ));
                    search_ok = false;
                    // Reconnect after a search failure
                    match connect_imap(config) {
                        Ok(mut new_session) => {
                            new_session.select("INBOX").ok();
                            session = new_session;
                            log("IMAP_RECONNECTED: reconnected after search failure");
                        }
                        Err(re) => log(&format!("IMAP_RECONNECT_FAILED: {re}")),
                    }
                }
            }
        }
    }

    let mut total_applied = 0usize;
    for (label, thread_ids) in grouped {
        let mut uids: Vec<u32> = Vec::new();
        for tid in thread_ids {
            if let Some(found) = all_thread_uids.get(tid.as_str()) {
                uids.extend(found);
            }
        }
        uids.sort();
        uids.dedup();

        if uids.is_empty() {
            log(&format!("IMAP_LABEL_SKIP: label={label} no UIDs found"));
            continue;
        }

        let mut label_applied = 0usize;
        for chunk in uids.chunks(100) {
            let uid_str = chunk
                .iter()
                .map(|u| u.to_string())
                .collect::<Vec<_>>()
                .join(",");

            // Quote label name if it contains IMAP-special chars
            let imap_label = if label.contains(' ') || label.contains('"') || label.contains('\\')
            {
                format!("\"{}\"", label.replace('\\', "\\\\").replace('"', "\\\""))
            } else {
                label.clone()
            };

            // Use run_command (fire-and-forget) to avoid the imap-proto parser
            // choking on Gmail's X-GM-LABELS FETCH responses.
            if session
                .run_command(format!("UID STORE {uid_str} +X-GM-LABELS ({imap_label})"))
                .is_err()
            {
                log(&format!(
                    "IMAP_LABEL_SKIP: label={label} uids={}... send failed",
                    chunk.len()
                ));
                continue;
            }
            // Archive: remove from INBOX (best-effort)
            let _ = session
                .run_command(format!("UID STORE {uid_str} -X-GM-LABELS (\\Inbox)"));

            label_applied += chunk.len();
        }

        // Drain unreadable X-GM-LABELS responses by reconnecting.
        match connect_imap(config) {
            Ok(mut new_session) => {
                new_session.select("INBOX").ok();
                session = new_session;
            }
            Err(e) => {
                log(&format!("IMAP_RECONNECT_FAILED: label={label} error={e}"));
            }
        }

        total_applied += label_applied;
        log(&format!("IMAP_LABEL_APPLIED: {label} -> {label_applied} messages"));
    }

    log(&format!("IMAP_LABEL_DONE: applied labels to {total_applied} messages"));
    Ok(())
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
