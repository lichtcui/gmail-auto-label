use std::collections::HashMap;
use std::io::ErrorKind;
use std::process::{Command, Stdio};

use anyhow::{Result, anyhow, bail};
use serde_json::Value;

use crate::cache::{memo_key, rule_id};
use crate::command::{CommandRunner, SystemCommandRunner};
use crate::models::{CacheData, CodexClassify, Memo, Rule, RuleInput};
use crate::utils::{log, normalize_label, now_ts, resolve_label_alias};

const CODEX_TIMEOUT_SECONDS: u64 = 25;

pub(crate) fn build_rule_priority_indexes(cache: &CacheData) -> Vec<usize> {
    let mut indexes = (0..cache.rules.len()).collect::<Vec<_>>();
    indexes.sort_by(|&a, &b| {
        (cache.rules[b].hits, cache.rules[b].updated_at)
            .cmp(&(cache.rules[a].hits, cache.rules[a].updated_at))
    });
    indexes
}

pub(crate) fn rule_matches(rule: &Rule, sender: &str, subject: &str, snippet: &str) -> bool {
    let text = format!("{} {} {}", sender, subject, snippet).to_lowercase();
    let mut has_include = false;
    let mut include_matched = false;
    for raw in &rule.include_keywords {
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

    for raw in &rule.exclude_keywords {
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

    for &idx in rule_indexes {
        if idx >= cache.rules.len() {
            continue;
        }
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

pub(crate) fn codex_analyze_email(
    sender: &str,
    subject: &str,
    snippet: &str,
    codex_cmd: &str,
) -> CodexClassify {
    codex_analyze_email_with_runner(&SystemCommandRunner, sender, subject, snippet, codex_cmd)
}

pub(crate) fn codex_analyze_email_with_runner<R: CommandRunner>(
    runner: &R,
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

    let program = parts[0].clone();
    let cmd_args = parts[1..].to_vec();

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

    let (code, out, err) = match runner.run(&program, &cmd_args, CODEX_TIMEOUT_SECONDS) {
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

pub(crate) fn upsert_rule(cache: &mut CacheData, label: &str, rule_input: &RuleInput) -> String {
    let description = if rule_input.description.trim().is_empty() {
        "无描述".to_string()
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

pub(crate) fn classify_with_codex_result(
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

pub(crate) fn compress_labels_if_needed(
    cache: &mut CacheData,
    max_active_labels: usize,
    merged_label: &str,
) {
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

pub(crate) fn extract_cmd_binary(cmd: &str) -> Result<String> {
    let parts = shell_words::split(cmd).map_err(|e| anyhow!("命令解析失败: {e}"))?;
    if parts.is_empty() {
        bail!("命令为空");
    }
    Ok(parts[0].clone())
}

pub(crate) fn ensure_codex_command_available(codex_cmd: &str) -> Result<()> {
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

pub(crate) fn codex_error_hint(summary: &str) -> Option<&'static str> {
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use anyhow::Result as AnyResult;

    use super::*;

    struct MockRunner {
        outputs: Mutex<Vec<AnyResult<(i32, String, String)>>>,
    }

    impl MockRunner {
        fn from(outputs: Vec<AnyResult<(i32, String, String)>>) -> Self {
            Self {
                outputs: Mutex::new(outputs),
            }
        }
    }

    impl CommandRunner for MockRunner {
        fn run(
            &self,
            _program: &str,
            _args: &[String],
            _timeout_secs: u64,
        ) -> AnyResult<(i32, String, String)> {
            let mut guard = self.outputs.lock().expect("lock poisoned");
            if guard.is_empty() {
                return Ok((0, "{}".to_string(), String::new()));
            }
            guard.remove(0)
        }
    }

    #[test]
    fn test_codex_analyze_email_with_runner_success() {
        let runner = MockRunner::from(vec![Ok((
            0,
            r#"{"label":"账单","summary":"每月账单","rule":{"description":"账单邮件","include_keywords":["invoice"],"exclude_keywords":[]}}"#.to_string(),
            String::new(),
        ))]);
        let out = codex_analyze_email_with_runner(
            &runner,
            "billing@example.com",
            "invoice",
            "monthly invoice",
            "codex exec",
        );
        assert!(out.ok);
        assert_eq!(out.label, "账单");
        assert_eq!(out.summary, "每月账单");
        assert_eq!(out.rule.include_keywords, vec!["invoice".to_string()]);
    }
}
