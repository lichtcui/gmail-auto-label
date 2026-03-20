use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::models::{CACHE_VERSION, CacheData, CustomLabelRule, DEFAULT_FEEDBACK_MAX_APPLIED_IDS};
use crate::utils::{log, normalize_label, now_ts};

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(crate) fn rule_id(
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

pub(crate) fn memo_key(sender: &str, subject: &str, snippet: &str) -> String {
    let normalized = format!(
        "{}|{}|{}",
        sender.trim().to_lowercase(),
        subject.trim().to_lowercase(),
        snippet.trim().to_lowercase()
    );
    sha256_hex(&format!("memo:{normalized}"))
}

pub(crate) fn load_cache(path: &str) -> CacheData {
    let p = Path::new(path);
    if !p.exists() {
        return CacheData::default();
    }
    let raw = match fs::read_to_string(p) {
        Ok(raw) => raw,
        Err(err) => {
            log(&format!(
                "WARN_LOAD_CACHE: failed to read cache, ignored {}: {}",
                p.display(),
                err
            ));
            return CacheData::default();
        }
    };
    let mut cache = match serde_json::from_str::<CacheData>(&raw) {
        Ok(cache) => cache,
        Err(err) => {
            log(&format!(
                "WARN_LOAD_CACHE: failed to parse cache, ignored {}: {}",
                p.display(),
                err
            ));
            return CacheData::default();
        }
    };
    cache.version = CACHE_VERSION.to_string();
    normalize_cache_rules(&mut cache);
    cache
}

pub(crate) fn load_custom_label_rules(path: &str) -> Result<Vec<CustomLabelRule>> {
    let p = Path::new(path);
    let raw = fs::read_to_string(p)
        .with_context(|| format!("读取自定义标签文件失败: {}", p.display()))?;
    let mut rules = serde_json::from_str::<Vec<CustomLabelRule>>(&raw)
        .with_context(|| format!("解析自定义标签文件失败: {}", p.display()))?;

    for (idx, rule) in rules.iter_mut().enumerate() {
        let raw_label = rule.label.trim().to_string();
        let normalized_label = normalize_label(&raw_label);
        if raw_label.is_empty() {
            bail!("第 {} 条自定义标签规则缺少非空 label", idx + 1);
        }

        rule.label = normalized_label;
        rule.include_keywords = normalize_keywords(&rule.include_keywords);
        rule.exclude_keywords = normalize_keywords(&rule.exclude_keywords);

        if rule.include_keywords.is_empty() {
            bail!(
                "第 {} 条自定义标签规则至少需要 1 个 include_keywords",
                idx + 1
            );
        }
    }

    Ok(rules)
}

pub(crate) fn save_cache(path: &str, cache: &CacheData) -> Result<()> {
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建缓存目录失败: {}", parent.display()))?;
    }
    let body = serde_json::to_vec_pretty(cache)?;
    let tmp = temp_cache_path(p);
    {
        let mut file = fs::File::create(&tmp)
            .with_context(|| format!("创建缓存临时文件失败: {}", tmp.display()))?;
        file.write_all(&body)
            .with_context(|| format!("写缓存临时文件失败: {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("同步缓存临时文件失败: {}", tmp.display()))?;
    }
    fs::rename(&tmp, p)
        .with_context(|| format!("替换缓存文件失败: {} -> {}", tmp.display(), p.display()))
}

pub(crate) fn cache_fingerprint(cache: &CacheData) -> Result<String> {
    let bytes = serde_json::to_vec(cache)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Debug, Default)]
pub(crate) struct FeedbackApplySummary {
    pub(crate) total_events: usize,
    pub(crate) applied_events: usize,
    pub(crate) skipped_events: usize,
    pub(crate) affected_rules: usize,
    pub(crate) dropped_rules: usize,
}

#[derive(Debug, Deserialize)]
struct FeedbackEntry {
    event_id: String,
    rule_id: String,
    verdict: String,
    ts: i64,
}

pub(crate) fn apply_feedback_from_file(
    cache: &mut CacheData,
    feedback_file: &str,
    bad_threshold: u32,
    hit_penalty: i64,
    max_age_hours: i64,
) -> Result<FeedbackApplySummary> {
    let path = Path::new(feedback_file);
    if !path.exists() {
        return Ok(FeedbackApplySummary::default());
    }

    let raw = fs::read_to_string(path)
        .with_context(|| format!("读取反馈文件失败: {}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(FeedbackApplySummary::default());
    }

    let entries = serde_json::from_str::<Vec<FeedbackEntry>>(&raw)
        .with_context(|| format!("解析反馈文件失败: {}", path.display()))?;

    let mut good_counts: HashMap<String, u32> = HashMap::new();
    let mut bad_counts: HashMap<String, u32> = HashMap::new();
    let mut skipped_events = 0usize;
    let mut applied_event_ids = std::collections::HashSet::<String>::new();
    let mut existing_applied = cache
        .feedback_applied_ids
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let now = now_ts();
    let max_age_secs = max_age_hours * 3600;

    for entry in &entries {
        let event_id = entry.event_id.trim();
        let rid = entry.rule_id.trim();
        if rid.is_empty() || event_id.is_empty() {
            skipped_events += 1;
            continue;
        }
        if entry.ts <= 0 || now.saturating_sub(entry.ts) > max_age_secs {
            skipped_events += 1;
            continue;
        }
        if existing_applied.contains(event_id) || applied_event_ids.contains(event_id) {
            skipped_events += 1;
            continue;
        }
        let verdict = entry.verdict.trim().to_lowercase();
        if verdict == "good" {
            *good_counts.entry(rid.to_string()).or_insert(0) += 1;
            applied_event_ids.insert(event_id.to_string());
        } else if verdict == "bad" {
            *bad_counts.entry(rid.to_string()).or_insert(0) += 1;
            applied_event_ids.insert(event_id.to_string());
        } else {
            skipped_events += 1;
        }
    }

    let mut affected_rules = 0usize;
    for rule in &mut cache.rules {
        let good = good_counts.get(&rule.id).copied().unwrap_or(0);
        let bad = bad_counts.get(&rule.id).copied().unwrap_or(0);
        if good == 0 && bad == 0 {
            continue;
        }
        affected_rules += 1;
        if good > 0 {
            rule.hits = rule.hits.saturating_add(good as i64);
        }
        if bad > 0 {
            rule.bad_hits = rule.bad_hits.saturating_add(bad);
            let penalty = (bad as i64).saturating_mul(hit_penalty.max(0));
            rule.hits = std::cmp::max(0, rule.hits.saturating_sub(penalty));
        }
        rule.updated_at = now;
    }

    let dropped_ids = cache
        .rules
        .iter()
        .filter(|r| r.bad_hits >= bad_threshold)
        .map(|r| r.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let dropped_rules = dropped_ids.len();
    if dropped_rules > 0 {
        cache.rules.retain(|r| !dropped_ids.contains(&r.id));
        cache.memos.retain(|_, m| !dropped_ids.contains(&m.rule_id));
    }

    let applied_events = applied_event_ids.len();
    if !applied_event_ids.is_empty() {
        cache.feedback_applied_ids.extend(applied_event_ids.clone());
        if cache.feedback_applied_ids.len() > DEFAULT_FEEDBACK_MAX_APPLIED_IDS {
            let drop_n = cache.feedback_applied_ids.len() - DEFAULT_FEEDBACK_MAX_APPLIED_IDS;
            cache.feedback_applied_ids.drain(0..drop_n);
        }
        for event_id in applied_event_ids {
            existing_applied.insert(event_id);
        }
    }

    fs::write(path, "[]").with_context(|| format!("重置反馈文件失败: {}", path.display()))?;

    Ok(FeedbackApplySummary {
        total_events: entries.len(),
        applied_events,
        skipped_events,
        affected_rules,
        dropped_rules,
    })
}

pub(crate) fn prune_cache(
    cache: &mut CacheData,
    max_rules: usize,
    max_memos: usize,
    ttl_hours: i64,
) {
    normalize_cache_rules(cache);
    let now = now_ts();
    let ttl_seconds = ttl_hours * 3600;

    cache
        .memos
        .retain(|_, memo| now - memo.ts <= ttl_seconds && normalize_label(&memo.label) != "待分类");

    cache
        .processed_threads
        .retain(|_, entry| now - entry.ts <= ttl_seconds && !entry.content_key.trim().is_empty());

    if cache.memos.len() > max_memos {
        let mut pairs: Vec<(String, i64)> =
            cache.memos.iter().map(|(k, v)| (k.clone(), v.ts)).collect();
        pairs.sort_by_key(|(_, ts)| *ts);
        let drop_count = cache.memos.len() - max_memos;
        for (k, _) in pairs.into_iter().take(drop_count) {
            cache.memos.remove(&k);
        }
    }

    if cache.processed_threads.len() > max_memos {
        let mut pairs: Vec<(String, i64)> = cache
            .processed_threads
            .iter()
            .map(|(k, v)| (k.clone(), v.ts))
            .collect();
        pairs.sort_by_key(|(_, ts)| *ts);
        let drop_count = cache.processed_threads.len() - max_memos;
        for (k, _) in pairs.into_iter().take(drop_count) {
            cache.processed_threads.remove(&k);
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

fn normalize_cache_rules(cache: &mut CacheData) {
    for rule in &mut cache.rules {
        rule.include_keywords = normalize_keywords(&rule.include_keywords);
        rule.exclude_keywords = normalize_keywords(&rule.exclude_keywords);
    }
}

fn normalize_keywords(keywords: &[String]) -> Vec<String> {
    keywords
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
}

fn temp_cache_path(path: &Path) -> std::path::PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cache.json");
    path.with_file_name(format!(".{file_name}.tmp-{}-{ts}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::models::{ProcessedThread, Rule};
    use crate::utils::now_ts;

    fn tmp_feedback_file() -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("/tmp/gmail_feedback_test_{ts}.json")
    }

    fn tmp_cache_file() -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("/tmp/gmail_cache_test_{ts}.json")
    }

    fn tmp_custom_labels_file() -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("/tmp/gmail_custom_labels_test_{ts}.json")
    }

    #[test]
    fn test_apply_feedback_drops_bad_rules() {
        let path = tmp_feedback_file();
        let now = now_ts();
        fs::write(
            &path,
            format!(
                r#"[{{"event_id":"e1","rule_id":"r1","verdict":"bad","ts":{now}}},{{"event_id":"e2","rule_id":"r1","verdict":"bad","ts":{now}}},{{"event_id":"e3","rule_id":"r1","verdict":"bad","ts":{now}}}]"#
            ),
        )
        .expect("write feedback file");

        let mut cache = CacheData::default();
        cache.rules.push(Rule {
            id: "r1".to_string(),
            label: "账单".to_string(),
            include_keywords: vec!["invoice".to_string()],
            hits: 10,
            ..Default::default()
        });

        let summary = apply_feedback_from_file(&mut cache, &path, 3, 2, 24 * 7)
            .expect("apply feedback failed");
        assert_eq!(summary.total_events, 3);
        assert_eq!(summary.applied_events, 3);
        assert_eq!(summary.dropped_rules, 1);
        assert!(cache.rules.is_empty());

        let consumed = fs::read_to_string(&path).expect("read feedback file");
        assert_eq!(consumed.trim(), "[]");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_apply_feedback_skips_stale_or_replayed_events() {
        let path = tmp_feedback_file();
        let now = now_ts();
        let stale = now - 24 * 8 * 3600;
        fs::write(
            &path,
            format!(
                r#"[{{"event_id":"dup1","rule_id":"r1","verdict":"bad","ts":{now}}},{{"event_id":"dup1","rule_id":"r1","verdict":"bad","ts":{now}}},{{"event_id":"old1","rule_id":"r1","verdict":"bad","ts":{stale}}}]"#
            ),
        )
        .expect("write feedback file");

        let mut cache = CacheData::default();
        cache.rules.push(Rule {
            id: "r1".to_string(),
            label: "账单".to_string(),
            include_keywords: vec!["invoice".to_string()],
            hits: 10,
            ..Default::default()
        });
        cache.feedback_applied_ids.push("dup1".to_string());

        let summary = apply_feedback_from_file(&mut cache, &path, 3, 2, 24 * 7)
            .expect("apply feedback failed");
        assert_eq!(summary.total_events, 3);
        assert_eq!(summary.applied_events, 0);
        assert_eq!(summary.skipped_events, 3);
        assert_eq!(summary.affected_rules, 0);
        assert_eq!(summary.dropped_rules, 0);
        assert_eq!(cache.rules[0].hits, 10);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_save_and_load_cache_roundtrip() {
        let path = tmp_cache_file();
        let mut cache = CacheData::default();
        cache.rules.push(Rule {
            id: "r1".to_string(),
            label: "账单".to_string(),
            include_keywords: vec![" invoice ".to_string()],
            ..Default::default()
        });
        cache.processed_threads.insert(
            "t1".to_string(),
            ProcessedThread {
                content_key: "memo:abc".to_string(),
                ts: now_ts(),
            },
        );

        save_cache(&path, &cache).expect("save cache should succeed");
        let loaded = load_cache(&path);

        assert_eq!(loaded.version, CACHE_VERSION);
        assert_eq!(loaded.rules.len(), 1);
        assert_eq!(
            loaded.rules[0].include_keywords,
            vec!["invoice".to_string()]
        );
        assert!(loaded.processed_threads.contains_key("t1"));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_load_cache_returns_default_for_invalid_json() {
        let path = tmp_cache_file();
        fs::write(&path, "{invalid json").expect("write invalid cache");

        let loaded = load_cache(&path);

        assert_eq!(loaded.version, CACHE_VERSION);
        assert!(loaded.rules.is_empty());
        assert!(loaded.memos.is_empty());
        assert!(loaded.processed_threads.is_empty());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_load_custom_label_rules_normalizes_valid_file() {
        let path = tmp_custom_labels_file();
        fs::write(
            &path,
            r#"[{"label":" 重要客户 ","include_keywords":[" vip "," invoice "],"exclude_keywords":[" spam "]}]"#,
        )
        .expect("write custom labels");

        let rules = load_custom_label_rules(&path).expect("custom rules should load");

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].label, "重要客户");
        assert_eq!(
            rules[0].include_keywords,
            vec!["vip".to_string(), "invoice".to_string()]
        );
        assert_eq!(rules[0].exclude_keywords, vec!["spam".to_string()]);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_load_custom_label_rules_rejects_rule_without_include_keywords() {
        let path = tmp_custom_labels_file();
        fs::write(
            &path,
            r#"[{"label":"重要客户","include_keywords":["   "],"exclude_keywords":[]}]"#,
        )
        .expect("write custom labels");

        let err = load_custom_label_rules(&path).expect_err("custom rules should fail");
        assert!(err.to_string().contains("include_keywords"));

        let _ = fs::remove_file(&path);
    }
}
