use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::models::{CACHE_VERSION, CacheData, DEFAULT_FEEDBACK_MAX_APPLIED_IDS};
use crate::utils::{normalize_label, now_ts};

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
    match fs::read_to_string(p)
        .ok()
        .and_then(|s| serde_json::from_str::<CacheData>(&s).ok())
    {
        Some(mut cache) => {
            cache.version = CACHE_VERSION.to_string();
            normalize_cache_rules(&mut cache);
            cache
        }
        None => CacheData::default(),
    }
}

pub(crate) fn save_cache(path: &str, cache: &CacheData) -> Result<()> {
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建缓存目录失败: {}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(cache)?;
    fs::write(p, body).with_context(|| format!("写缓存失败: {}", p.display()))
}

pub(crate) fn cache_fingerprint(cache: &CacheData) -> Result<String> {
    let bytes = serde_json::to_vec(cache)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Debug, Default)]
pub(crate) struct FeedbackApplySummary {
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
        applied_events: entries.len(),
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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::models::Rule;
    use crate::utils::now_ts;

    fn tmp_feedback_file() -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("/tmp/gmail_feedback_test_{ts}.json")
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
        assert_eq!(summary.applied_events, 3);
        assert_eq!(summary.skipped_events, 3);
        assert_eq!(summary.affected_rules, 0);
        assert_eq!(summary.dropped_rules, 0);
        assert_eq!(cache.rules[0].hits, 10);

        let _ = fs::remove_file(&path);
    }
}
