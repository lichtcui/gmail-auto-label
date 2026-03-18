use std::collections::HashMap;

use clap::Parser;
use serde::{Deserialize, Serialize};

pub(crate) const CACHE_VERSION: &str = "v2";
pub(crate) const DEFAULT_CACHE_FILE: &str = "/tmp/gmail_auto_label_codex_cache.json";
pub(crate) const DEFAULT_CACHE_TTL_HOURS: i64 = 24 * 14;
pub(crate) const DEFAULT_CACHE_MAX_RULES: usize = 500;
pub(crate) const DEFAULT_CACHE_MAX_MEMOS: usize = 5000;
pub(crate) const DEFAULT_CODEX_WORKERS: usize = 0;
pub(crate) const DEFAULT_MAX_ACTIVE_LABELS: usize = 10;
pub(crate) const DEFAULT_MERGED_LABEL: &str = "其他通知";
pub(crate) const DEFAULT_GMAIL_BATCH_SIZE: usize = 100;
pub(crate) const DEFAULT_GMAIL_BATCH_RETRIES: u32 = 2;
pub(crate) const DEFAULT_GMAIL_BATCH_RETRY_BACKOFF_SECS: u64 = 1;
pub(crate) const DEFAULT_FEEDBACK_FILE: &str = "/tmp/gmail_auto_label_feedback.json";
pub(crate) const DEFAULT_FEEDBACK_BAD_THRESHOLD: u32 = 3;
pub(crate) const DEFAULT_FEEDBACK_HIT_PENALTY: i64 = 2;
pub(crate) const DEFAULT_FEEDBACK_MAX_AGE_HOURS: i64 = 24 * 14;
pub(crate) const DEFAULT_FEEDBACK_MAX_APPLIED_IDS: usize = 10000;

#[derive(Parser, Debug, Clone)]
#[command(about = "Gmail 自动分类打标签脚本（缓存优先 + Codex 分析）")]
pub(crate) struct Args {
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    #[arg(long, default_value_t = 300)]
    pub(crate) interval: u64,
    #[arg(long)]
    pub(crate) r#loop: bool,
    #[arg(long)]
    pub(crate) account: Option<String>,
    #[arg(long)]
    pub(crate) dry_run: bool,
    #[arg(long, default_value = "codex exec")]
    pub(crate) codex_cmd: String,
    #[arg(long, default_value = DEFAULT_CACHE_FILE)]
    pub(crate) cache_file: String,
    #[arg(long, default_value_t = DEFAULT_CACHE_TTL_HOURS)]
    pub(crate) cache_ttl_hours: i64,
    #[arg(long, default_value_t = DEFAULT_CACHE_MAX_RULES)]
    pub(crate) cache_max_rules: usize,
    #[arg(long, default_value_t = DEFAULT_CACHE_MAX_MEMOS)]
    pub(crate) cache_max_memos: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_ACTIVE_LABELS)]
    pub(crate) max_labels: usize,
    #[arg(long, default_value = DEFAULT_MERGED_LABEL)]
    pub(crate) merged_label: String,
    #[arg(long, default_value_t = DEFAULT_CODEX_WORKERS)]
    pub(crate) codex_workers: usize,
    #[arg(long)]
    pub(crate) keep_inbox: bool,
    #[arg(long, default_value_t = DEFAULT_GMAIL_BATCH_SIZE)]
    pub(crate) gmail_batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_GMAIL_BATCH_RETRIES)]
    pub(crate) gmail_batch_retries: u32,
    #[arg(long, default_value_t = DEFAULT_GMAIL_BATCH_RETRY_BACKOFF_SECS)]
    pub(crate) gmail_batch_retry_backoff_secs: u64,
    #[arg(long, default_value = DEFAULT_FEEDBACK_FILE)]
    pub(crate) feedback_file: String,
    #[arg(long, default_value_t = DEFAULT_FEEDBACK_BAD_THRESHOLD)]
    pub(crate) feedback_bad_threshold: u32,
    #[arg(long, default_value_t = DEFAULT_FEEDBACK_HIT_PENALTY)]
    pub(crate) feedback_hit_penalty: i64,
    #[arg(long, default_value_t = DEFAULT_FEEDBACK_MAX_AGE_HOURS)]
    pub(crate) feedback_max_age_hours: i64,
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
}

impl Default for CacheData {
    fn default() -> Self {
        Self {
            version: CACHE_VERSION.to_string(),
            rules: Vec::new(),
            memos: HashMap::new(),
            label_aliases: HashMap::new(),
            feedback_applied_ids: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CodexClassify {
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
