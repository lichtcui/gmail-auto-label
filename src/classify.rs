use std::collections::HashMap;

use serde_json::Value;

use crate::cache::{memo_key, rule_id};
use crate::llm::{LlmConfig, call_chat};
use crate::models::{CacheData, LlmClassify, Memo, Rule, RuleInput};
use crate::utils::{log, normalize_label, now_ts, resolve_label_alias};

pub(crate) fn build_rule_priority_indexes(cache: &CacheData) -> Vec<usize> {
    let mut indexes = (0..cache.rules.len()).collect::<Vec<_>>();
    indexes.sort_by(|&a, &b| {
        (cache.rules[b].hits, cache.rules[b].updated_at)
            .cmp(&(cache.rules[a].hits, cache.rules[a].updated_at))
    });
    indexes
}

fn normalized_match_text(sender: &str, subject: &str, snippet: &str) -> String {
    format!("{sender} {subject} {snippet}").to_lowercase()
}

fn keywords_match_text(
    include_keywords: &[String],
    exclude_keywords: &[String],
    text: &str,
) -> bool {
    let mut has_include = false;
    let mut include_matched = false;
    for raw in include_keywords {
        let kw = raw.trim();
        if kw.is_empty() {
            continue;
        }
        has_include = true;
        if text.contains(kw) {
            include_matched = true;
            break;
        }
    }
    if has_include && !include_matched {
        return false;
    }

    for raw in exclude_keywords {
        let kw = raw.trim();
        if kw.is_empty() {
            continue;
        }
        if text.contains(kw) {
            return false;
        }
    }

    has_include
}

fn rule_matches_text(rule: &Rule, text: &str) -> bool {
    keywords_match_text(&rule.include_keywords, &rule.exclude_keywords, text)
}

#[cfg(test)]
pub(crate) fn rule_matches(rule: &Rule, sender: &str, subject: &str, snippet: &str) -> bool {
    let text = normalized_match_text(sender, subject, snippet);
    rule_matches_text(rule, &text)
}

pub(crate) fn classify_from_cache_with_indexes(
    sender: &str,
    subject: &str,
    snippet: &str,
    cache: &mut CacheData,
    ttl_hours: i64,
    rule_indexes: &[usize],
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

    let match_text = normalized_match_text(sender, subject, snippet);
    for &idx in rule_indexes {
        if idx >= cache.rules.len() {
            continue;
        }
        let matched = {
            let rule = &cache.rules[idx];
            rule_matches_text(rule, &match_text)
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

pub(crate) fn llm_classify_email(
    sender: &str,
    subject: &str,
    snippet: &str,
    llm_config: &LlmConfig,
) -> LlmClassify {
    let prompt = format!(
        "You are an email classification and rule extraction assistant.\nTask: classify the email into one label and provide a reusable rule.\nOutput must be strict JSON only, with no extra text.\nJSON format:\n{{\n  \"label\": \"label_name\",\n  \"summary\": \"one_sentence_summary\",\n  \"rule\": {{\n    \"description\": \"how this label is determined\",\n    \"include_keywords\": [\"keyword1\", \"keyword2\"],\n    \"exclude_keywords\": [\"exclude1\"]\n  }}\n}}\nRequirements:\n1. Keep label concise (about 2-8 words), suitable for Gmail labels.\n2. include_keywords must contain at least one item and should be useful for future text matching.\n3. If content is limited, still provide the most reasonable label and an actionable rule.\n\nSender: {}\nSubject: {}\nSnippet: {}\n",
        sender, subject, snippet
    );

    let fallback = |summary: &str, description: &str| LlmClassify {
        ok: false,
        label: "uncategorized".to_string(),
        summary: summary.to_string(),
        rule: RuleInput {
            description: description.to_string(),
            include_keywords: vec![String::new()],
            exclude_keywords: vec![],
        },
    };

    let trimmed = match call_chat(&prompt, llm_config) {
        Ok(t) => t,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("timed out") {
                return fallback("llm_timeout", "timeout_fallback");
            }
            if msg.contains("rate limit") || msg.contains("429") {
                return fallback("llm_rate_limited", "rate_limit_fallback");
            }
            return fallback("llm_error", "execution_error");
        }
    };

    if trimmed.is_empty() {
        return fallback("llm_empty_output", "empty_output");
    }

    let v: Value = match serde_json::from_str(&trimmed) {
        Ok(v) => v,
        Err(_) => return fallback("llm_invalid_json", "output_not_valid_json"),
    };
    let label = normalize_label(
        v.get("label")
            .and_then(Value::as_str)
            .unwrap_or("uncategorized"),
    );
    let summary = v
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("no_summary")
        .trim()
        .to_string();
    let rule = v
        .get("rule")
        .and_then(|r| serde_json::from_value::<RuleInput>(r.clone()).ok())
        .unwrap_or_default();

    LlmClassify {
        ok: true,
        label,
        summary: if summary.is_empty() {
            "no_summary".to_string()
        } else {
            summary
        },
        rule,
    }
}

pub(crate) fn upsert_rule(cache: &mut CacheData, label: &str, rule_input: &RuleInput) -> String {
    let description = if rule_input.description.trim().is_empty() {
        "no_description".to_string()
    } else {
        rule_input.description.trim().to_string()
    };
    let mut include_keywords = rule_input
        .include_keywords
        .iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    let exclude_keywords = rule_input
        .exclude_keywords
        .iter()
        .map(|x| x.trim().to_lowercase())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

    if include_keywords.is_empty() {
        include_keywords = vec![label.to_lowercase()];
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
        bad_hits: 0,
        updated_at: now,
    });
    rid
}

pub(crate) fn classify_with_llm_result(
    sender: &str,
    subject: &str,
    snippet: &str,
    cache: &mut CacheData,
    result: &LlmClassify,
) -> (String, String, String) {
    let label = normalize_label(&result.label);
    let summary = if result.summary.trim().is_empty() {
        "no_summary".to_string()
    } else {
        result.summary.trim().to_string()
    };

    if !result.ok {
        return (label, "llm:error".to_string(), summary);
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
        format!("llm:{}", rid.chars().take(8).collect::<String>()),
        summary,
    )
}

pub(crate) fn compress_labels_if_needed(
    cache: &mut CacheData,
    max_active_labels: usize,
    merged_label: &str,
) {
    let mut scores: HashMap<String, i64> = HashMap::new();
    for r in &cache.rules {
        let label = normalize_label(&r.label);
        if label == "uncategorized" {
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
            "LABEL_COMPRESSION: exceeded {}, merged {} labels -> {}",
            max_active_labels,
            merged_from.len(),
            target
        ));
    }
}

pub(crate) fn llm_error_hint(summary: &str) -> Option<&'static str> {
    match summary {
        "llm_timeout" => {
            Some("LLM API timed out. Check network connectivity and API responsiveness.")
        }
        "llm_rate_limited" => Some("LLM API rate limited. The system will back off and retry."),
        "llm_invalid_json" => {
            Some("LLM response is not valid JSON. The model may have returned extra text.")
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_with_llm_result_parses_success() {
        let mut cache = CacheData::default();
        let result = LlmClassify {
            ok: true,
            label: "账单".to_string(),
            summary: "每月账单提醒".to_string(),
            rule: RuleInput {
                description: "账单类邮件".to_string(),
                include_keywords: vec!["invoice".to_string()],
                exclude_keywords: vec![],
            },
        };
        let (label, source, summary) = classify_with_llm_result(
            "billing@example.com",
            "invoice",
            "monthly invoice",
            &mut cache,
            &result,
        );
        assert_eq!(label, "账单");
        assert_eq!(summary, "每月账单提醒");
        assert!(source.starts_with("llm:"));
        // A rule should have been upserted
        assert_eq!(cache.rules.len(), 1);
        assert_eq!(cache.rules[0].label, "账单");
        // A memo should have been written
        assert!(cache.memos.contains_key(&memo_key(
            "billing@example.com",
            "invoice",
            "monthly invoice"
        )));
    }

    #[test]
    fn test_classify_with_llm_result_fallback_on_error() {
        let mut cache = CacheData::default();
        let result = LlmClassify {
            ok: false,
            label: "uncategorized".to_string(),
            summary: "llm_timeout".to_string(),
            rule: RuleInput::default(),
        };
        let (label, source, summary) =
            classify_with_llm_result("x@example.com", "hello", "test", &mut cache, &result);
        assert_eq!(label, "uncategorized");
        assert_eq!(source, "llm:error");
        assert_eq!(summary, "llm_timeout");
        // No rule or memo should be added on error
        assert!(cache.rules.is_empty());
    }
}
