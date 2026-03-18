use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Local;
use clap::Parser;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use wait_timeout::ChildExt;

const SEARCH_QUERY: &str = "in:inbox";
const GOG_TIMEOUT_SECONDS: u64 = 30;
const CODEX_TIMEOUT_SECONDS: u64 = 25;
const GOG_RATE_LIMIT_MAX_RETRIES: u32 = 4;
const GOG_RATE_LIMIT_BASE_BACKOFF_SECS: u64 = 2;
const GOG_RATE_LIMIT_MAX_BACKOFF_SECS: u64 = 30;

const CACHE_VERSION: &str = "v2";
const DEFAULT_CACHE_FILE: &str = "/tmp/gmail_auto_label_codex_cache.json";
const DEFAULT_CACHE_TTL_HOURS: i64 = 24 * 14;
const DEFAULT_CACHE_MAX_RULES: usize = 500;
const DEFAULT_CACHE_MAX_MEMOS: usize = 5000;
const DEFAULT_CODEX_WORKERS: usize = 0;
const DEFAULT_MAX_ACTIVE_LABELS: usize = 10;
const DEFAULT_MERGED_LABEL: &str = "其他通知";

#[derive(Debug)]
struct RateLimitError(String);

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RateLimitError {}

#[derive(Parser, Debug, Clone)]
#[command(about = "Gmail 自动分类打标签脚本（缓存优先 + Codex 分析）")]
struct Args {
    #[arg(long, default_value_t = 20)]
    limit: usize,
    #[arg(long, default_value_t = 300)]
    interval: u64,
    #[arg(long)]
    r#loop: bool,
    #[arg(long)]
    account: Option<String>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long, default_value = "codex exec")]
    codex_cmd: String,
    #[arg(long, default_value = DEFAULT_CACHE_FILE)]
    cache_file: String,
    #[arg(long, default_value_t = DEFAULT_CACHE_TTL_HOURS)]
    cache_ttl_hours: i64,
    #[arg(long, default_value_t = DEFAULT_CACHE_MAX_RULES)]
    cache_max_rules: usize,
    #[arg(long, default_value_t = DEFAULT_CACHE_MAX_MEMOS)]
    cache_max_memos: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_ACTIVE_LABELS)]
    max_labels: usize,
    #[arg(long, default_value = DEFAULT_MERGED_LABEL)]
    merged_label: String,
    #[arg(long, default_value_t = DEFAULT_CODEX_WORKERS)]
    codex_workers: usize,
    #[arg(long)]
    keep_inbox: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Rule {
    #[serde(default)]
    id: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    include_keywords: Vec<String>,
    #[serde(default)]
    exclude_keywords: Vec<String>,
    #[serde(default)]
    hits: i64,
    #[serde(default)]
    updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Memo {
    #[serde(default)]
    label: String,
    #[serde(default)]
    rule_id: String,
    #[serde(default)]
    ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheData {
    version: String,
    #[serde(default)]
    rules: Vec<Rule>,
    #[serde(default)]
    memos: HashMap<String, Memo>,
    #[serde(default)]
    label_aliases: HashMap<String, String>,
}

impl Default for CacheData {
    fn default() -> Self {
        Self {
            version: CACHE_VERSION.to_string(),
            rules: Vec::new(),
            memos: HashMap::new(),
            label_aliases: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct CodexClassify {
    ok: bool,
    label: String,
    summary: String,
    rule: RuleInput,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuleInput {
    #[serde(default)]
    description: String,
    #[serde(default)]
    include_keywords: Vec<String>,
    #[serde(default)]
    exclude_keywords: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct ThreadInfo {
    id: String,
    sender: String,
    subject: String,
    snippet: String,
}

fn now_str() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn log(msg: &str) {
    println!("[{}] {}", now_str(), msg);
}

fn auto_codex_workers(limit: usize) -> usize {
    let cpu = std::thread::available_parallelism().map_or(4, usize::from);
    let mut workers = std::cmp::max(2, cpu / 2);
    workers = std::cmp::min(workers, 8);
    workers = std::cmp::min(workers, std::cmp::max(1, limit));
    workers
}

fn normalize_label(label: &str) -> String {
    let cleaned = label.split_whitespace().collect::<Vec<_>>().join(" ");
    let clipped: String = cleaned.chars().take(80).collect();
    if clipped.is_empty() {
        "待分类".to_string()
    } else {
        clipped
    }
}

fn resolve_label_alias(label: &str, cache: &CacheData) -> String {
    let mut cur = label.to_string();
    let mut visited = HashSet::new();
    while let Some(next) = cache.label_aliases.get(&cur) {
        if visited.contains(&cur) {
            break;
        }
        visited.insert(cur.clone());
        let normalized = normalize_label(next);
        if normalized == cur {
            break;
        }
        cur = normalized;
    }
    cur
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn rule_id(
    label: &str,
    description: &str,
    include_keywords: &[String],
    exclude_keywords: &[String],
) -> String {
    let payload = json!({
        "label": label,
        "description": description,
        "include_keywords": include_keywords,
        "exclude_keywords": exclude_keywords,
    });
    let s = serde_json::to_string(&payload).unwrap_or_default();
    sha256_hex(&s)
}

fn memo_key(sender: &str, subject: &str, snippet: &str) -> String {
    let normalized = format!(
        "{}|{}|{}",
        sender.trim().to_lowercase(),
        subject.trim().to_lowercase(),
        snippet.trim().to_lowercase()
    );
    sha256_hex(&format!("memo:{normalized}"))
}

fn load_cache(path: &str) -> CacheData {
    let p = Path::new(path);
    if !p.exists() {
        return CacheData::default();
    }
    match fs::read_to_string(p)
        .ok()
        .and_then(|s| serde_json::from_str::<CacheData>(&s).ok())
    {
        Some(mut cache) => {
            cache.version = CACHE_VERSION.to_string();
            cache
        }
        None => CacheData::default(),
    }
}

fn save_cache(path: &str, cache: &CacheData) -> Result<()> {
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建缓存目录失败: {}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(cache)?;
    fs::write(p, body).with_context(|| format!("写缓存失败: {}", p.display()))
}

fn prune_cache(cache: &mut CacheData, max_rules: usize, max_memos: usize, ttl_hours: i64) {
    let now = now_ts();
    let ttl_seconds = ttl_hours * 3600;

    cache
        .memos
        .retain(|_, memo| now - memo.ts <= ttl_seconds && normalize_label(&memo.label) != "待分类");

    if cache.memos.len() > max_memos {
        let mut pairs: Vec<(String, i64)> =
            cache.memos.iter().map(|(k, v)| (k.clone(), v.ts)).collect();
        pairs.sort_by_key(|(_, ts)| *ts);
        let drop_count = cache.memos.len() - max_memos;
        for (k, _) in pairs.into_iter().take(drop_count) {
            cache.memos.remove(&k);
        }
    }

    cache.rules.retain(|r| {
        now - r.updated_at <= ttl_seconds
            && normalize_label(&r.label) != "待分类"
            && r.include_keywords.iter().any(|x| !x.trim().is_empty())
    });
    cache.rules.sort_by(|a, b| {
        (b.hits, b.updated_at)
            .cmp(&(a.hits, a.updated_at))
            .then_with(|| a.id.cmp(&b.id))
    });
    cache.rules.truncate(max_rules);

    let mut cleaned = HashMap::new();
    for (k, v) in &cache.label_aliases {
        let src = normalize_label(k);
        let dst = normalize_label(v);
        if src != dst {
            cleaned.insert(src, dst);
        }
    }
    cache.label_aliases = cleaned;
}

fn run_cmd_with_timeout(mut cmd: Command, timeout_secs: u64) -> Result<(i32, String, String)> {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("启动命令失败")?;

    let timeout = Duration::from_secs(timeout_secs);
    let status_opt = child.wait_timeout(timeout)?;
    if status_opt.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        bail!("命令超时（{}s）", timeout_secs);
    }

    let output = child.wait_with_output().context("读取命令输出失败")?;
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    Ok((code, stdout, stderr))
}

fn run_gog(
    args: &[String],
    account: &Option<String>,
    expect_json: bool,
) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let display = format!("gog {}", args.join(" "));
    let mut last_rate_limit_message = String::new();

    for attempt in 0..=GOG_RATE_LIMIT_MAX_RETRIES {
        let mut cmd = Command::new("gog");
        if let Some(acct) = account {
            cmd.arg("--account").arg(acct);
        }
        for a in args {
            cmd.arg(a);
        }
        cmd.arg("--no-input");
        if expect_json {
            cmd.arg("--json");
        }

        let (code, out, err) = match run_cmd_with_timeout(cmd, GOG_TIMEOUT_SECONDS) {
            Ok(v) => v,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("No such file or directory") {
                    return Err(anyhow!("未找到 `gog` 命令，请先安装并完成登录。").into());
                }
                return Err(anyhow!("命令超时（{}s）", GOG_TIMEOUT_SECONDS).into());
            }
        };

        let merged = format!("{}\n{}", out.trim(), err.trim());
        if is_gmail_rate_limit_error(&merged) {
            last_rate_limit_message = merged.trim().to_string();
            if attempt < GOG_RATE_LIMIT_MAX_RETRIES {
                let sleep_secs = rate_limit_backoff_secs(attempt);
                log(&format!(
                    "Gmail 限流，{} 秒后重试（{}/{}）: {}",
                    sleep_secs,
                    attempt + 1,
                    GOG_RATE_LIMIT_MAX_RETRIES + 1,
                    display
                ));
                thread::sleep(Duration::from_secs(sleep_secs));
                continue;
            }
            return Err(Box::new(RateLimitError(last_rate_limit_message)));
        }

        if code != 0 {
            return Err(anyhow!("命令失败: {}\n{}", display, merged.trim()).into());
        }

        if !expect_json || out.trim().is_empty() {
            return Ok(json!({}));
        }

        return serde_json::from_str::<Value>(out.trim()).map_err(|_| {
            anyhow!(
                "JSON 解析失败:\n{}",
                out.chars().take(500).collect::<String>()
            )
            .into()
        });
    }

    Err(Box::new(RateLimitError(last_rate_limit_message)))
}

fn is_gmail_rate_limit_error(raw: &str) -> bool {
    let msg = raw.to_lowercase();
    [
        "rate limit exceeded",
        "too many requests",
        "429",
        "ratelimitexceeded",
        "userratelimitexceeded",
        "quota exceeded",
        "exceeded quota",
    ]
    .iter()
    .any(|pat| msg.contains(pat))
}

fn rate_limit_backoff_secs(attempt: u32) -> u64 {
    let factor = 1u64 << std::cmp::min(attempt, 10);
    let backoff = GOG_RATE_LIMIT_BASE_BACKOFF_SECS.saturating_mul(factor);
    std::cmp::min(backoff, GOG_RATE_LIMIT_MAX_BACKOFF_SECS)
}

fn fetch_existing_labels(
    account: &Option<String>,
) -> std::result::Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let args = vec!["gmail", "labels", "list"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let data = run_gog(&args, account, true)?;
    let mut labels = HashSet::new();
    if let Some(items) = data.get("labels").and_then(Value::as_array) {
        for item in items {
            if let Some(name) = item.get("name").and_then(Value::as_str) {
                let n = name.trim();
                if !n.is_empty() {
                    labels.insert(n.to_string());
                }
            }
        }
    }
    Ok(labels)
}

fn ensure_label(
    label: &str,
    existing_labels: &mut HashSet<String>,
    account: &Option<String>,
    dry_run: bool,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if existing_labels.contains(label) {
        return Ok(());
    }
    if dry_run {
        log(&format!("[dry-run] 将创建标签: {label}"));
        existing_labels.insert(label.to_string());
        return Ok(());
    }

    let args = vec!["gmail", "labels", "create", label]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    run_gog(&args, account, true)?;
    existing_labels.insert(label.to_string());
    log(&format!("已创建标签: {label}"));
    Ok(())
}

fn fetch_pending(
    limit: usize,
    account: &Option<String>,
) -> std::result::Result<Vec<ThreadInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let args = vec!["gmail", "search", SEARCH_QUERY, "--max", &limit.to_string()]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let data = run_gog(&args, account, true)?;
    let mut threads = Vec::new();
    if let Some(items) = data.get("threads").and_then(Value::as_array) {
        for t in items {
            let id = t
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if id.is_empty() {
                continue;
            }
            let sender = t
                .get("from")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let subject = t
                .get("subject")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let snippet = t
                .get("snippet")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            threads.push(ThreadInfo {
                id,
                sender,
                subject,
                snippet,
            });
        }
    }
    Ok(threads)
}

fn rule_matches(rule: &Rule, sender: &str, subject: &str, snippet: &str) -> bool {
    let text = format!("{} {} {}", sender, subject, snippet).to_lowercase();
    let include_keywords = rule
        .include_keywords
        .iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    let exclude_keywords = rule
        .exclude_keywords
        .iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

    if !include_keywords.is_empty() && !include_keywords.iter().any(|kw| text.contains(kw)) {
        return false;
    }
    if exclude_keywords.iter().any(|kw| text.contains(kw)) {
        return false;
    }
    !include_keywords.is_empty()
}

fn classify_from_cache(
    sender: &str,
    subject: &str,
    snippet: &str,
    cache: &mut CacheData,
    ttl_hours: i64,
) -> Option<(String, String)> {
    let now = now_ts();
    let ttl_seconds = ttl_hours * 3600;

    let mkey = memo_key(sender, subject, snippet);
    if let Some(memo) = cache.memos.get(&mkey) {
        if now - memo.ts <= ttl_seconds {
            let label = normalize_label(&memo.label);
            let final_label = resolve_label_alias(&label, cache);
            return Some((final_label, "memo".to_string()));
        }
    }

    let mut indexes = (0..cache.rules.len()).collect::<Vec<_>>();
    indexes.sort_by(|&a, &b| {
        (cache.rules[b].hits, cache.rules[b].updated_at)
            .cmp(&(cache.rules[a].hits, cache.rules[a].updated_at))
    });

    for idx in indexes {
        let matched = {
            let rule = &cache.rules[idx];
            rule_matches(rule, sender, subject, snippet)
        };
        if !matched {
            continue;
        }

        let rid = cache.rules[idx].id.clone();
        let label = normalize_label(&cache.rules[idx].label);
        let final_label = resolve_label_alias(&label, cache);

        cache.rules[idx].hits += 1;
        cache.rules[idx].updated_at = now;
        cache.memos.insert(
            mkey,
            Memo {
                label: final_label.clone(),
                rule_id: rid.clone(),
                ts: now,
            },
        );
        return Some((
            final_label,
            format!("rule:{}", rid.chars().take(8).collect::<String>()),
        ));
    }

    None
}

fn codex_analyze_email(
    sender: &str,
    subject: &str,
    snippet: &str,
    codex_cmd: &str,
) -> CodexClassify {
    let prompt = format!(
        "你是邮件分类与规则抽取器。\n任务：根据邮件信息输出一个标签，并给出可复用的判定方式。\n输出必须是严格 JSON，不要输出任何额外文本。\nJSON 格式：\n{{\n  \"label\": \"标签名\",\n  \"summary\": \"一句话总结\",\n  \"rule\": {{\n    \"description\": \"该标签判定方式\",\n    \"include_keywords\": [\"关键词1\", \"关键词2\"],\n    \"exclude_keywords\": [\"排除词1\"]\n  }}\n}}\n要求：\n1. 标签尽量简洁（2-8字），面向 Gmail 打标签。\n2. include_keywords 至少提供 1 个，且应便于后续文本匹配。\n3. 如果邮件内容不足，仍需给出最合理标签和可执行规则。\n\n发件人: {}\n主题: {}\n摘要: {}\n",
        sender, subject, snippet
    );

    let mut parts = match shell_words::split(codex_cmd) {
        Ok(p) if !p.is_empty() => p,
        _ => vec!["codex".to_string(), "exec".to_string()],
    };
    if parts.len() >= 2
        && parts[0] == "codex"
        && parts[1] == "exec"
        && !parts.iter().any(|x| x == "--skip-git-repo-check")
    {
        parts.push("--skip-git-repo-check".to_string());
    }
    parts.push(prompt);

    let mut cmd = Command::new(&parts[0]);
    for a in &parts[1..] {
        cmd.arg(a);
    }

    let fallback = |summary: &str, description: &str| CodexClassify {
        ok: false,
        label: "待分类".to_string(),
        summary: summary.to_string(),
        rule: RuleInput {
            description: description.to_string(),
            include_keywords: vec![String::new()],
            exclude_keywords: vec![],
        },
    };

    let (code, out, err) = match run_cmd_with_timeout(cmd, CODEX_TIMEOUT_SECONDS) {
        Ok(v) => v,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("No such file or directory") {
                return fallback("codex_not_found", "命令不存在");
            }
            if msg.contains("超时") {
                return fallback("codex_timeout", "超时兜底");
            }
            return fallback("codex_exec_error", "执行异常");
        }
    };

    if code != 0 {
        let stderr = err.trim();
        if !stderr.is_empty() {
            log(&format!(
                "CODEX_STDERR: {}",
                stderr.chars().take(500).collect::<String>()
            ));
        }
        return fallback("codex_non_zero_exit", "返回码异常");
    }

    let trimmed = out.trim();
    if trimmed.is_empty() {
        return fallback("codex_empty_output", "空输出");
    }

    let v: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return fallback("codex_invalid_json", "输出不是合法 JSON"),
    };
    let label = normalize_label(v.get("label").and_then(Value::as_str).unwrap_or("待分类"));
    let summary = v
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("无总结")
        .trim()
        .to_string();
    let rule = v
        .get("rule")
        .and_then(|r| serde_json::from_value::<RuleInput>(r.clone()).ok())
        .unwrap_or_default();

    CodexClassify {
        ok: true,
        label,
        summary: if summary.is_empty() {
            "无总结".to_string()
        } else {
            summary
        },
        rule,
    }
}

fn upsert_rule(cache: &mut CacheData, label: &str, rule_input: &RuleInput) -> String {
    let description = if rule_input.description.trim().is_empty() {
        "无描述".to_string()
    } else {
        rule_input.description.trim().to_string()
    };
    let mut include_keywords = rule_input
        .include_keywords
        .iter()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    let exclude_keywords = rule_input
        .exclude_keywords
        .iter()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

    if include_keywords.is_empty() {
        include_keywords = vec![label.to_string()];
    }

    let rid = rule_id(label, &description, &include_keywords, &exclude_keywords);
    let now = now_ts();

    if let Some(r) = cache.rules.iter_mut().find(|r| r.id == rid) {
        r.label = label.to_string();
        r.description = description;
        r.include_keywords = include_keywords;
        r.exclude_keywords = exclude_keywords;
        r.hits += 1;
        r.updated_at = now;
        return rid;
    }

    cache.rules.push(Rule {
        id: rid.clone(),
        label: label.to_string(),
        description,
        include_keywords,
        exclude_keywords,
        hits: 1,
        updated_at: now,
    });
    rid
}

fn classify_with_codex_result(
    sender: &str,
    subject: &str,
    snippet: &str,
    cache: &mut CacheData,
    result: &CodexClassify,
) -> (String, String, String) {
    let label = normalize_label(&result.label);
    let summary = if result.summary.trim().is_empty() {
        "无总结".to_string()
    } else {
        result.summary.trim().to_string()
    };

    if !result.ok {
        return (label, "codex:error".to_string(), summary);
    }

    let rid = upsert_rule(cache, &label, &result.rule);
    cache.memos.insert(
        memo_key(sender, subject, snippet),
        Memo {
            label: label.clone(),
            rule_id: rid.clone(),
            ts: now_ts(),
        },
    );

    let final_label = resolve_label_alias(&label, cache);
    (
        final_label,
        format!("codex:{}", rid.chars().take(8).collect::<String>()),
        summary,
    )
}

fn compress_labels_if_needed(cache: &mut CacheData, max_active_labels: usize, merged_label: &str) {
    let mut scores: HashMap<String, i64> = HashMap::new();
    for r in &cache.rules {
        let label = normalize_label(&r.label);
        if label == "待分类" {
            continue;
        }
        let final_label = resolve_label_alias(&label, cache);
        *scores.entry(final_label).or_insert(0) += r.hits;
    }

    let mut active_labels: Vec<String> = scores.keys().cloned().collect();
    active_labels.sort_by(|a, b| scores.get(b).cmp(&scores.get(a)).then_with(|| a.cmp(b)));
    if active_labels.len() <= max_active_labels {
        return;
    }

    let keep_count = std::cmp::max(1, max_active_labels.saturating_sub(1));
    let target = normalize_label(merged_label);
    let mut merged_from = Vec::new();
    for label in active_labels.into_iter().skip(keep_count) {
        if label == target {
            continue;
        }
        cache.label_aliases.insert(label.clone(), target.clone());
        merged_from.push(label);
    }

    if !merged_from.is_empty() {
        log(&format!(
            "标签压缩: 超过 {} 个，已合并 {} 个标签 -> {}",
            max_active_labels,
            merged_from.len(),
            target
        ));
    }
}

fn apply_labels(
    grouped: &HashMap<String, Vec<String>>,
    account: &Option<String>,
    dry_run: bool,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut labels: Vec<String> = grouped.keys().cloned().collect();
    labels.sort();
    for label in labels {
        let ids = grouped.get(&label).cloned().unwrap_or_default();
        if ids.is_empty() {
            continue;
        }
        if dry_run {
            log(&format!(
                "[dry-run] 标签={}，数量={}，线程={:?}",
                label,
                ids.len(),
                ids
            ));
            continue;
        }

        let mut args = vec!["gmail", "labels", "modify"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        args.extend(ids.iter().cloned());
        args.push("--add".to_string());
        args.push(label.clone());

        run_gog(&args, account, false)?;
        log(&format!("已打标签: {} -> {} 封", label, ids.len()));
    }
    Ok(())
}

fn archive_threads(
    thread_ids: &[String],
    account: &Option<String>,
    dry_run: bool,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if thread_ids.is_empty() {
        return Ok(());
    }
    if dry_run {
        log(&format!(
            "[dry-run] 将归档线程: 数量={}，线程={:?}",
            thread_ids.len(),
            thread_ids
        ));
        return Ok(());
    }

    let mut args = vec!["gmail", "labels", "modify"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    args.extend(thread_ids.iter().cloned());
    args.push("--remove".to_string());
    args.push("INBOX".to_string());
    run_gog(&args, account, false)?;
    log(&format!(
        "已归档线程（移出收件箱）: {} 封",
        thread_ids.len()
    ));
    Ok(())
}

fn process_once(
    args: &Args,
    cache: &mut CacheData,
    existing_labels: &mut HashSet<String>,
    effective_workers: usize,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let threads = fetch_pending(args.limit, &args.account)?;
    if threads.is_empty() {
        log("DONE_NO_PENDING: 没有待整理邮件，任务结束。");
        return Ok("done".to_string());
    }

    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    let mut processed_ids: Vec<String> = Vec::new();
    let mut codex_jobs: Vec<ThreadInfo> = Vec::new();

    for t in &threads {
        if let Some((label, source)) = classify_from_cache(
            &t.sender,
            &t.subject,
            &t.snippet,
            cache,
            args.cache_ttl_hours,
        ) {
            ensure_label(&label, existing_labels, &args.account, args.dry_run)?;
            grouped.entry(label.clone()).or_default().push(t.id.clone());
            processed_ids.push(t.id.clone());
            log(&format!(
                "分类: 线程={} 标签={} 来源={} 总结=缓存命中",
                t.id, label, source
            ));
        } else {
            codex_jobs.push(t.clone());
        }
    }

    if !codex_jobs.is_empty() {
        log(&format!(
            "缓存未命中 {} 封，使用 {} 并发调用 Codex...",
            codex_jobs.len(),
            effective_workers
        ));

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(effective_workers)
            .build()
            .context("创建线程池失败")?;

        let codex_cmd = args.codex_cmd.clone();
        let results: Vec<(ThreadInfo, CodexClassify)> = pool.install(|| {
            codex_jobs
                .into_par_iter()
                .map(|job| {
                    let res =
                        codex_analyze_email(&job.sender, &job.subject, &job.snippet, &codex_cmd);
                    (job, res)
                })
                .collect()
        });

        let total_results = results.len();
        let mut codex_setup_failures = 0usize;

        for (job, result) in results {
            let (label, source, summary) =
                classify_with_codex_result(&job.sender, &job.subject, &job.snippet, cache, &result);
            if source == "codex:error" && label == "待分类" {
                let hint = codex_error_hint(&summary).unwrap_or("请检查 codex 环境后重试。");
                if matches!(summary.as_str(), "codex_not_found" | "codex_non_zero_exit") {
                    codex_setup_failures += 1;
                }
                log(&format!(
                    "分类失败: 线程={}，原因={}。{} 已跳过打标，等待下轮重试。",
                    job.id, summary, hint
                ));
                continue;
            }
            ensure_label(&label, existing_labels, &args.account, args.dry_run)?;
            grouped
                .entry(label.clone())
                .or_default()
                .push(job.id.clone());
            processed_ids.push(job.id.clone());
            log(&format!(
                "分类: 线程={} 标签={} 来源={} 总结={}",
                job.id, label, source, summary
            ));
        }

        if total_results > 0 && codex_setup_failures == total_results {
            return Err(anyhow!(
                "Codex 配置不可用：本轮所有未命中缓存邮件都因 codex 配置问题失败。请先修复 codex 后重试。"
            )
            .into());
        }
    }

    compress_labels_if_needed(cache, args.max_labels, &args.merged_label);

    if !cache.label_aliases.is_empty() {
        let mut regrouped: HashMap<String, Vec<String>> = HashMap::new();
        for (label, ids) in grouped {
            let final_label = resolve_label_alias(&label, cache);
            regrouped.entry(final_label).or_default().extend(ids);
        }
        grouped = regrouped;
    }

    apply_labels(&grouped, &args.account, args.dry_run)?;
    if !args.keep_inbox {
        archive_threads(&processed_ids, &args.account, args.dry_run)?;
    }

    let total: usize = grouped.values().map(Vec::len).sum();
    let mut keys: Vec<String> = grouped.keys().cloned().collect();
    keys.sort();
    let summary = keys
        .into_iter()
        .filter_map(|k| grouped.get(&k).map(|ids| (k, ids.len())))
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| format!("{}:{}", k, n))
        .collect::<Vec<_>>()
        .join(" | ");
    log(&format!("本轮完成: 总计={} | {}", total, summary));

    Ok("processed".to_string())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.limit == 0 {
        bail!("--limit 必须大于 0");
    }
    if args.interval == 0 {
        bail!("--interval 必须大于 0");
    }
    if args.cache_ttl_hours <= 0 {
        bail!("--cache-ttl-hours 必须大于 0");
    }
    if args.cache_max_rules == 0 {
        bail!("--cache-max-rules 必须大于 0");
    }
    if args.cache_max_memos == 0 {
        bail!("--cache-max-memos 必须大于 0");
    }
    if args.max_labels < 2 {
        bail!("--max-labels 必须大于等于 2");
    }
    Ok(())
}

fn extract_cmd_binary(cmd: &str) -> Result<String> {
    let parts = shell_words::split(cmd).map_err(|e| anyhow!("命令解析失败: {e}"))?;
    if parts.is_empty() {
        bail!("命令为空");
    }
    Ok(parts[0].clone())
}

fn ensure_codex_command_available(codex_cmd: &str) -> Result<()> {
    let binary = extract_cmd_binary(codex_cmd)?;
    let status = Command::new(&binary)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    match status {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            bail!("未找到可执行文件: `{binary}`");
        }
        Err(e) if e.kind() == ErrorKind::PermissionDenied => {
            bail!("命令无执行权限: `{binary}`");
        }
        Err(e) => bail!("命令检查失败: {e}"),
    }
}

fn build_gog_setup_error(raw_error: &str, account: &Option<String>) -> anyhow::Error {
    let account_hint = account
        .as_ref()
        .map(|a| format!("当前传入账号: `{a}`\n"))
        .unwrap_or_default();
    anyhow!(
        "gog 配置检查失败。\n\
         原始错误: {raw_error}\n\n\
         排查建议:\n\
         1. 确认已安装 gog，并可执行 `gog --help`\n\
         2. 执行 `gog auth login` 完成登录\n\
         3. 执行 `gog auth status` 检查认证状态\n\
         4. 执行 `gog gmail labels list --no-input --json` 验证 Gmail 访问\n\
         {account_hint}\
         5. 如使用多账号，运行时加 `--account <name>` 指定账号"
    )
}

fn build_codex_setup_error(raw_error: &str, codex_cmd: &str) -> anyhow::Error {
    anyhow!(
        "Codex 配置检查失败。\n\
         原始错误: {raw_error}\n\
         当前 --codex-cmd: `{codex_cmd}`\n\n\
         排查建议:\n\
         1. 确认可执行 `codex --help` 或你自定义命令的 `--help`\n\
         2. 若命令不存在，安装/修正 PATH，或通过 `--codex-cmd` 指定正确命令\n\
         3. 如需登录，先执行对应登录步骤后再运行本工具"
    )
}

fn codex_error_hint(summary: &str) -> Option<&'static str> {
    match summary {
        "codex_not_found" => {
            Some("未找到 codex 命令。请安装 codex，或通过 `--codex-cmd` 指向正确可执行命令。")
        }
        "codex_non_zero_exit" => {
            Some("codex 返回非 0。请先单独执行一次 `codex exec \"test\"` 检查认证/权限和环境。")
        }
        "codex_timeout" => Some("codex 超时。请检查网络与模型响应，或降低并发后重试。"),
        "codex_invalid_json" => {
            Some("codex 输出不是 JSON。请检查 `--codex-cmd` 是否被包裹了额外输出。")
        }
        _ => None,
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let effective_workers = if args.codex_workers == 0 {
        auto_codex_workers(args.limit)
    } else {
        args.codex_workers
    };
    log(&format!(
        "Codex 并发设置: {}（用户输入: {}）",
        effective_workers, args.codex_workers
    ));

    let mut cache = load_cache(&args.cache_file);
    prune_cache(
        &mut cache,
        args.cache_max_rules,
        args.cache_max_memos,
        args.cache_ttl_hours,
    );

    ensure_codex_command_available(&args.codex_cmd)
        .map_err(|e| build_codex_setup_error(&e.to_string(), &args.codex_cmd))?;

    let mut existing_labels = fetch_existing_labels(&args.account)
        .map_err(|e| build_gog_setup_error(&e.to_string(), &args.account))?;

    loop {
        let state = match process_once(&args, &mut cache, &mut existing_labels, effective_workers) {
            Ok(state) => state,
            Err(e) => {
                if e.downcast_ref::<RateLimitError>().is_some() {
                    log(&format!("RATE_LIMIT: 本轮跳过，等待下轮。详情: {}", e));
                    "rate_limit".to_string()
                } else {
                    let msg = e.to_string();
                    if msg.contains("Codex 配置不可用") || msg.contains("配置检查失败") {
                        return Err(anyhow!(msg));
                    }
                    log(&format!("ERROR: {}", e));
                    "error".to_string()
                }
            }
        };

        prune_cache(
            &mut cache,
            args.cache_max_rules,
            args.cache_max_memos,
            args.cache_ttl_hours,
        );
        save_cache(&args.cache_file, &cache)?;

        if !args.r#loop || state == "done" {
            break;
        }

        log(&format!("休眠 {} 秒后继续...", args.interval));
        thread::sleep(Duration::from_secs(args.interval));
    }

    Ok(())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_label() {
        assert_eq!(normalize_label("  账单   通知 "), "账单 通知");
        assert_eq!(normalize_label(""), "待分类");
    }

    #[test]
    fn test_rule_matches() {
        let rule = Rule {
            include_keywords: vec!["invoice".to_string()],
            exclude_keywords: vec!["spam".to_string()],
            ..Default::default()
        };
        assert!(rule_matches(&rule, "sender", "invoice arrived", "body"));
        assert!(!rule_matches(&rule, "sender", "hello", "body"));
        assert!(!rule_matches(&rule, "sender", "invoice", "spam body"));
    }

    #[test]
    fn test_alias_resolve_chain() {
        let mut cache = CacheData::default();
        cache.label_aliases.insert("A".to_string(), "B".to_string());
        cache.label_aliases.insert("B".to_string(), "C".to_string());
        assert_eq!(resolve_label_alias("A", &cache), "C");
    }

    #[test]
    fn test_gog_setup_error_contains_fix_steps() {
        let err = build_gog_setup_error("mock error", &Some("work".to_string())).to_string();
        assert!(err.contains("gog 配置检查失败"));
        assert!(err.contains("gog auth login"));
        assert!(err.contains("--account <name>"));
    }

    #[test]
    fn test_codex_setup_error_contains_fix_steps() {
        let err = build_codex_setup_error("not found", "codex exec").to_string();
        assert!(err.contains("Codex 配置检查失败"));
        assert!(err.contains("codex --help"));
        assert!(err.contains("--codex-cmd"));
    }

    #[test]
    fn test_codex_error_hint_for_not_found() {
        let hint = codex_error_hint("codex_not_found").unwrap_or("");
        assert!(hint.contains("--codex-cmd"));
    }

    #[test]
    fn test_detect_gmail_rate_limit_error() {
        assert!(is_gmail_rate_limit_error("Error: Rate limit exceeded"));
        assert!(is_gmail_rate_limit_error("HTTP 429 Too Many Requests"));
        assert!(is_gmail_rate_limit_error("reason=userRateLimitExceeded"));
        assert!(!is_gmail_rate_limit_error("permission denied"));
    }

    #[test]
    fn test_rate_limit_backoff_secs_capped() {
        assert_eq!(rate_limit_backoff_secs(0), 2);
        assert_eq!(rate_limit_backoff_secs(1), 4);
        assert_eq!(rate_limit_backoff_secs(2), 8);
        assert_eq!(rate_limit_backoff_secs(3), 16);
        assert_eq!(rate_limit_backoff_secs(4), 30);
        assert_eq!(rate_limit_backoff_secs(10), 30);
    }
}
