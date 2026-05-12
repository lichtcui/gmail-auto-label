use std::collections::HashMap;

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

pub(crate) const CACHE_VERSION: &str = "v2";
pub(crate) const DEFAULT_CACHE_FILE: &str = "/tmp/gmail_auto_label_codex_cache.json";
pub(crate) const DEFAULT_CACHE_TTL_HOURS: i64 = 24 * 14;
pub(crate) const DEFAULT_CACHE_MAX_RULES: usize = 500;
pub(crate) const DEFAULT_CACHE_MAX_MEMOS: usize = 5000;
pub(crate) const DEFAULT_MAX_ACTIVE_LABELS: usize = 10;
pub(crate) const DEFAULT_MERGED_LABEL: &str = "others";
pub(crate) const DEFAULT_FEEDBACK_FILE: &str = "/tmp/gmail_auto_label_feedback.json";
pub(crate) const DEFAULT_FEEDBACK_BAD_THRESHOLD: u32 = 3;
pub(crate) const DEFAULT_FEEDBACK_HIT_PENALTY: i64 = 2;
pub(crate) const DEFAULT_FEEDBACK_MAX_AGE_HOURS: i64 = 24 * 14;
pub(crate) const DEFAULT_FEEDBACK_MAX_APPLIED_IDS: usize = 10000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum OutputFormat {
    Text,
    Json,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Text
    }
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Automatic Gmail labeling tool (cache-first + LLM fallback)")]
pub(crate) struct Args {
    #[arg(long)]
    pub(crate) account: Option<String>,
    /// DeepSeek API key (or set DEEPSEEK_API_KEY env var)
    #[arg(long, env = "DEEPSEEK_API_KEY")]
    pub(crate) api_key: Option<String>,
    /// DeepSeek model name
    #[arg(long, default_value = "deepseek-v4-flash")]
    pub(crate) model: String,
    #[arg(long, hide = true, default_value = DEFAULT_CACHE_FILE)]
    pub(crate) cache_file: String,
    #[arg(long, default_value_t = DEFAULT_MAX_ACTIVE_LABELS)]
    pub(crate) max_labels: usize,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,
    #[arg(long, hide = true, default_value = DEFAULT_MERGED_LABEL)]
    pub(crate) merged_label: String,
    // --- IMAP sync mode ---
    /// Run IMAP sync to fetch and summarize all inbox emails, then exit.
    /// Use --from-cache separately to classify and apply labels from cached data.
    #[arg(long)]
    pub(crate) sync: bool,
    /// IMAP username (email address) for sync mode
    #[arg(long)]
    pub(crate) imap_user: Option<String>,
    /// IMAP password or app password for sync mode
    #[arg(long)]
    pub(crate) imap_pass: Option<String>,
    /// IMAP server hostname
    #[arg(long, default_value = "imap.gmail.com")]
    pub(crate) imap_host: String,
    /// IMAP server port
    #[arg(long, default_value_t = 993)]
    pub(crate) imap_port: u16,
    /// Use previously synced cache data instead of live Gmail API fetch
    #[arg(long)]
    pub(crate) from_cache: bool,
    /// Max number of messages to sync via IMAP (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub(crate) sync_max: usize,
    /// Review classification results before applying labels
    #[arg(long)]
    pub(crate) confirm: bool,
    /// Force LLM classification for all threads (clears memos and consolidation cache)
    #[arg(long)]
    pub(crate) force_llm: bool,
    /// Restore emails from Spam folder to Inbox before syncing, so they
    /// get classified and labeled instead of sitting in spam.
    #[arg(long)]
    pub(crate) restore_spam: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct Rule {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) label: String,
    #[serde(default)]
    pub(crate) description: String,
    #[serde(default)]
    pub(crate) include_keywords: Vec<String>,
    #[serde(default)]
    pub(crate) exclude_keywords: Vec<String>,
    #[serde(default)]
    pub(crate) hits: i64,
    #[serde(default)]
    pub(crate) bad_hits: u32,
    #[serde(default)]
    pub(crate) updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct Memo {
    #[serde(default)]
    pub(crate) label: String,
    #[serde(default)]
    pub(crate) rule_id: String,
    #[serde(default)]
    pub(crate) ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheData {
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) rules: Vec<Rule>,
    #[serde(default)]
    pub(crate) memos: HashMap<String, Memo>,
    #[serde(default)]
    pub(crate) label_aliases: HashMap<String, String>,
    #[serde(default)]
    pub(crate) feedback_applied_ids: Vec<String>,
    /// Threads synced via IMAP and summarized by LLM
    #[serde(default)]
    pub(crate) synced_threads: HashMap<String, SyncedThread>,
    /// Fingerprint of the label set used for the last LLM consolidation.
    /// If labels haven't changed, reuses the cached mapping instead of calling LLM again.
    #[serde(default)]
    pub(crate) consolidation_fingerprint: String,
    /// Cached label consolidation mapping from the last LLM call.
    #[serde(default)]
    pub(crate) consolidation_mapping: HashMap<String, String>,
}

impl Default for CacheData {
    fn default() -> Self {
        Self {
            version: CACHE_VERSION.to_string(),
            rules: Vec::new(),
            memos: HashMap::new(),
            label_aliases: HashMap::new(),
            feedback_applied_ids: Vec::new(),
            synced_threads: HashMap::new(),
            consolidation_fingerprint: String::new(),
            consolidation_mapping: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LlmClassify {
    pub(crate) ok: bool,
    pub(crate) label: String,
    pub(crate) summary: String,
    pub(crate) rule: RuleInput,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct RuleInput {
    #[serde(default)]
    pub(crate) description: String,
    #[serde(default)]
    pub(crate) include_keywords: Vec<String>,
    #[serde(default)]
    pub(crate) exclude_keywords: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ThreadInfo {
    pub(crate) id: String,
    pub(crate) sender: String,
    pub(crate) subject: String,
    pub(crate) snippet: String,
}

/// A thread that was synced via IMAP and summarized by LLM.
/// Stored in cache so the main pipeline can read from it instead of
/// calling the Gmail API search endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SyncedThread {
    /// Gmail thread ID (X-GM-THRID)
    pub(crate) thread_id: String,
    /// Sender of the first/original message
    pub(crate) sender: String,
    /// Subject of the thread
    pub(crate) subject: String,
    /// LLM-generated summary of the full email body
    pub(crate) body_summary: String,
    /// Number of messages in this thread
    #[serde(default)]
    pub(crate) message_count: usize,
    /// Sync timestamp
    pub(crate) ts: i64,
    /// IMAP UIDs of messages in this thread (for label application via IMAP)
    #[serde(default)]
    pub(crate) uids: Vec<u32>,
}

/// Configuration for IMAP sync mode.
#[derive(Debug, Clone)]
pub(crate) struct SyncConfig {
    pub(crate) imap_user: String,
    pub(crate) imap_pass: String,
    pub(crate) imap_host: String,
    pub(crate) imap_port: u16,
    pub(crate) max_messages: usize,
    pub(crate) cache_file: String,
}
