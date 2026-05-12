use std::collections::{HashMap, HashSet};

use sha2::{Digest, Sha256};

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
    _sender: &str,
    subject: &str,
    snippet: &str,
    llm_config: &LlmConfig,
) -> LlmClassify {
    let prompt = format!(
        "将以下邮件分类到六个类别之一。不要轻易使用\"Others\"。如果无法确定，优先归入 Newsletter。\n\n\
类别定义：\n\n\
1. CI/CD\n\
   - CI/CD流水线、构建、部署、测试失败\n\
   - PR通知、依赖更新\n\
   - 关键词：build broken, pipeline failed, test flaky, Jenkins, GitHub Actions, pull request, merge request, dependency update, PR通知, 依赖更新\n\n\
2. Security\n\
   - 安全相关通知（提醒、告警、验证、密码变更、2FA、登录提醒）\n\
   - 关键词：security alert, security warning, 2FA, two-factor authentication, login alert, password reset, password change, verification, 安全提醒, 安全告警, 登录通知, 密码重置, 邮箱验证, 账号恢复\n\n\
3. Recruitment\n\
   - 招聘提醒、招聘邀请、入职相关通知\n\
   - 关键词：job alert, we're hiring, career opportunity, recruitment, job opening, 招聘广告, 入职通知, 入职指引, onboarding, offer, welcome aboard, 新员工, 入职流程\n\n\
4. Invoice\n\
   - 仅限：需要用户主动付款的账单、发票、付款请求\n\
   - 关键词：invoice, bill, payment required, 请付款, 待支付, 发票待开\n\
   - 不包括：自动续费通知、到期提醒、购买成功确认、试用开始、兑换码（这些归 Newsletter）\n\n\
5. Newsletter\n\
   - 所有订阅资讯、产品公告、活动通知、游戏资讯、营销推广、调查问卷、公司动态、社交通知、经验分享、配信通知\n\
   - 也包括：产品到期提醒、续费提醒、购买成功通知、试用开始通知、注册确认、兑换码发放、自动续费状态通知、服务状态更新\n\
   - 关键词：product update, event invitation, game, survey, promotion, marketing, company news, social notification, 产品发布, 活动邀请, 游戏更新, 调查问卷, 营销邮件, 经验分享, 到期, 过期, 续期, 续费提醒, 购买成功, 订单成功, 试用, 注册确认, 兑换码, 自动续费, auto-renewal, expiration, renew, trial, welcome, receipt (仅作记录的非付款类收据), purchase confirmation\n\n\
6. Others\n\
   - 仅用于完全无法归类的邮件（如乱码、测试邮件、个人非业务邮件）\n\n\
分类规则：\n\
- 优先级：CI/CD > Security > Invoice > Recruitment > Newsletter > Others\n\
- Invoice 与 Newsletter 的关键区分：\n\
  - 如果邮件包含\"立即付款\"/\"支付账单\"/\"invoice\"且要求用户主动付款 → Invoice\n\
  - 如果是到期提醒、续费提醒、购买成功确认、试用开始、兑换码、自动续费状态 → Newsletter\n\
  - 如果是 Nintendo 收据（purchase receipt）但仅作记录、无付款要求 → Newsletter\n\
- 只有完全无法匹配时才使用 Others\n\n\
邮件标题：{subject}\n\
邮件正文预览（如有）：{body_preview}\n\n\
输出格式：仅输出类别名称（CI/CD / Security / Newsletter / Recruitment / Invoice / Others）",
        subject = subject,
        body_preview = snippet,
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

    let trimmed = match call_chat(&prompt, llm_config, None) {
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

    // The response is just a category name (plain string), no JSON
    let label = trimmed.trim().to_string();
    LlmClassify {
        ok: true,
        label,
        summary: "llm_classified".to_string(),
        rule: RuleInput::default(),
    }
}

pub(crate) fn llm_classify_refine(
    subject: &str,
    snippet: &str,
    llm_config: &LlmConfig,
) -> LlmClassify {
    let prompt = format!(
        "将以下邮件分类到六个类别之一。不要轻易使用\"Others\"。如果无法确定，优先归入 Newsletter。\n\n\
类别定义：\n\n\
1. CI/CD\n\
   - CI/CD流水线、构建、部署、测试失败\n\
   - PR通知、依赖更新\n\
   - 关键词：build broken, pipeline failed, test flaky, Jenkins, GitHub Actions, pull request, merge request, dependency update, PR通知, 依赖更新\n\n\
2. Security\n\
   - 安全相关通知（提醒、告警、验证、密码变更、2FA、登录提醒）\n\
   - 关键词：security alert, security warning, 2FA, two-factor authentication, login alert, password reset, password change, verification, 安全提醒, 安全告警, 登录通知, 密码重置, 邮箱验证, 账号恢复\n\n\
3. Recruitment\n\
   - 招聘提醒、招聘邀请、入职相关通知\n\
   - 关键词：job alert, we're hiring, career opportunity, recruitment, job opening, 招聘广告, 入职通知, 入职指引, onboarding, offer, welcome aboard, 新员工, 入职流程\n\n\
4. Invoice\n\
   - 仅限：需要用户主动付款的账单、发票、付款请求\n\
   - 关键词：invoice, bill, payment required, 请付款, 待支付, 发票待开\n\
   - 不包括：自动续费通知、到期提醒、购买成功确认、试用开始、兑换码（这些归 Newsletter）\n\n\
5. Newsletter\n\
   - 所有订阅资讯、产品公告、活动通知、游戏资讯、营销推广、调查问卷、公司动态、社交通知、经验分享、配信通知\n\
   - 也包括：产品到期提醒、续费提醒、购买成功通知、试用开始通知、注册确认、兑换码发放、自动续费状态通知、服务状态更新\n\
   - 关键词：product update, event invitation, game, survey, promotion, marketing, company news, social notification, 产品发布, 活动邀请, 游戏更新, 调查问卷, 营销邮件, 经验分享, 到期, 过期, 续期, 续费提醒, 购买成功, 订单成功, 试用, 注册确认, 兑换码, 自动续费, auto-renewal, expiration, renew, trial, welcome, receipt (仅作记录的非付款类收据), purchase confirmation\n\n\
6. Others\n\
   - 仅用于完全无法归类的邮件（如乱码、测试邮件、个人非业务邮件）\n\n\
分类规则：\n\
- 优先级：CI/CD > Security > Invoice > Recruitment > Newsletter > Others\n\
- Invoice 与 Newsletter 的关键区分：\n\
  - 如果邮件包含\"立即付款\"/\"支付账单\"/\"invoice\"且要求用户主动付款 → Invoice\n\
  - 如果是到期提醒、续费提醒、购买成功确认、试用开始、兑换码、自动续费状态 → Newsletter\n\
  - 如果是 Nintendo 收据（purchase receipt）但仅作记录、无付款要求 → Newsletter\n\
- 只有完全无法匹配时才使用 Others\n\n\
邮件标题：{subject}\n\
邮件正文预览（如有）：{body_preview}\n\n\
输出格式：仅输出类别名称（CI/CD / Security / Newsletter / Recruitment / Invoice / Others）",
        subject = subject,
        body_preview = snippet,
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

    let trimmed = match call_chat(&prompt, llm_config, None) {
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

    // The response is just a category name (plain string), no JSON
    let label = trimmed.trim().to_string();
    LlmClassify {
        ok: true,
        label,
        summary: "llm_classified".to_string(),
        rule: RuleInput::default(),
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

pub(crate) fn llm_consolidate_labels(
    cache: &mut CacheData,
    max_active_labels: usize,
    llm_config: &LlmConfig,
) -> bool {
    let mut scores: HashMap<String, i64> = HashMap::new();
    for r in &cache.rules {
        let label = normalize_label(&r.label);
        if label == "uncategorized" {
            continue;
        }
        *scores.entry(label).or_insert(0) += r.hits;
    }

    if scores.len() <= max_active_labels {
        return false;
    }

    let mut sorted_labels: Vec<&String> = scores.keys().collect();
    sorted_labels.sort();
    let mut hasher = Sha256::new();
    for lbl in &sorted_labels {
        hasher.update(lbl.as_bytes());
        hasher.update(b"\0");
    }
    let fingerprint = format!("{:x}", hasher.finalize());

    if fingerprint == cache.consolidation_fingerprint && !cache.consolidation_mapping.is_empty() {
        log("LLM_CONSOLIDATE_CACHE_HIT: reusing previous label consolidation");
        let mapping = &cache.consolidation_mapping;
        let mut applied = 0usize;
        for label in &sorted_labels {
            if let Some(group) = mapping.get(*label) {
                let norm_group = normalize_label(group);
                if **label != norm_group {
                    cache
                        .label_aliases
                        .insert((*label).clone(), norm_group);
                    applied += 1;
                }
            }
        }
        if applied > 0 {
            let unique_groups: HashSet<&String> = mapping.values().collect();
            log(&format!(
                "LLM_CONSOLIDATE_CACHE_HIT: {applied} labels merged, reduced from {} to {} groups",
                sorted_labels.len(),
                unique_groups.len()
            ));
        }
        return true;
    }

    let label_lines: Vec<String> = sorted_labels
        .iter()
        .map(|l| format!("  \"{l}\" (hits: {})", scores.get(*l).unwrap_or(&0)))
        .collect();

    let prompt = format!(
        "You are a label consolidation assistant. Group similar email labels into broad categories.\n\n\
         Examples:\n\
         \"CI Failure\", \"CI/CD Failure\" → \"CI/CD\"  (same CI/CD pipeline topic)\n\
         \"Newsletter A\", \"Promotions\", \"Deals\" → \"Newsletter\"  (all are newsletters/promotions)\n\
         \"Meeting Notes\", \"Meeting Reminder\", \"Calendar\" → \"Meetings\"  (meeting related)\n\
         \"Job Alert\", \"Interview Invite\" → \"Job Alerts\"  (job search related)\n\n\
         Rules:\n\
         - MERGE AGGRESSIVELY: any labels sharing the same topic area go into one group\n\
         - The group name should be a short single noun or phrase (1-3 words)\n\
         - Use the most common existing label as the group name when possible\n\
         - Every input label MUST be assigned to exactly one group\n\
         - Output MUST be valid JSON only, with EXACTLY these keys\n\
         - Total unique groups MUST be at most {max_active_labels}\n\n\
         Input labels (label: hit_count):\n{labels}\n\n\
         Output JSON (map each original label to its group name):",
        labels = label_lines.join("\n")
    );

    log(&format!(
        "LLM_CONSOLIDATE: classifying {} labels semantically (max_tokens=8192)...",
        sorted_labels.len()
    ));
    let response = match call_chat(&prompt, llm_config, Some(8192)) {
        Ok(r) => r,
        Err(e) => {
            log(&format!("LLM_CONSOLIDATE_FAILED: {e}"));
            return false;
        }
    };

    let mapping: HashMap<String, String> = match serde_json::from_str(&response) {
        Ok(m) => m,
        Err(e) => {
            log(&format!("LLM_CONSOLIDATE_PARSE_FAILED: {e}"));
            return false;
        }
    };

    let mut applied = 0usize;
    for (original, group) in &mapping {
        let norm_orig = normalize_label(original);
        let norm_group = normalize_label(group);
        if norm_orig != norm_group && scores.contains_key(&norm_orig) {
            cache.label_aliases.insert(norm_orig, norm_group);
            applied += 1;
        }
    }

    if applied > 0 {
        let unique_groups: HashSet<&String> = mapping.values().collect();
        log(&format!(
            "LLM_CONSOLIDATED: {applied} labels merged, reduced from {} to {} groups",
            sorted_labels.len(),
            unique_groups.len()
        ));
    }

    cache.consolidation_fingerprint = fingerprint;
    cache.consolidation_mapping = mapping;
    true
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
