use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::Duration;

use serde_json::{Value, json};

use crate::command::{CommandRunner, SystemCommandRunner};
use crate::errors::AppError;
use crate::models::{
    DEFAULT_GMAIL_BATCH_RETRIES, DEFAULT_GMAIL_BATCH_RETRY_BACKOFF_SECS, DEFAULT_GMAIL_BATCH_SIZE,
};
use crate::utils::log;

const GOG_TIMEOUT_SECONDS: u64 = 30;
const GOG_RATE_LIMIT_MAX_RETRIES: u32 = 4;
const GOG_RATE_LIMIT_BASE_BACKOFF_SECS: u64 = 2;
const GOG_RATE_LIMIT_MAX_BACKOFF_SECS: u64 = 30;
#[derive(Debug, Clone, Copy)]
pub(crate) struct GmailWriteOptions {
    pub(crate) batch_size: usize,
    pub(crate) batch_retries: u32,
    pub(crate) batch_retry_backoff_secs: u64,
}

impl Default for GmailWriteOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_GMAIL_BATCH_SIZE,
            batch_retries: DEFAULT_GMAIL_BATCH_RETRIES,
            batch_retry_backoff_secs: DEFAULT_GMAIL_BATCH_RETRY_BACKOFF_SECS,
        }
    }
}

pub(crate) fn run_gog(
    args: &[String],
    account: &Option<String>,
    expect_json: bool,
) -> Result<Value, AppError> {
    run_gog_with_runner(&SystemCommandRunner, args, account, expect_json)
}

pub(crate) fn run_gog_with_runner<R: CommandRunner>(
    runner: &R,
    args: &[String],
    account: &Option<String>,
    expect_json: bool,
) -> Result<Value, AppError> {
    let display = format!("gog {}", args.join(" "));
    let mut last_rate_limit_message = String::new();
    let max_retries = GOG_RATE_LIMIT_MAX_RETRIES;

    for attempt in 0..=max_retries {
        let mut cmd_args = Vec::new();
        if let Some(acct) = account {
            cmd_args.push("--account".to_string());
            cmd_args.push(acct.clone());
        }
        for a in args {
            cmd_args.push(a.clone());
        }
        cmd_args.push("--no-input".to_string());
        if expect_json {
            cmd_args.push("--json".to_string());
        }

        let (code, out, err) = match runner.run("gog", &cmd_args, GOG_TIMEOUT_SECONDS) {
            Ok(v) => v,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("No such file or directory") {
                    return Err(AppError::Command(
                        "`gog` command not found. Please install it and complete login first."
                            .to_string(),
                    ));
                }
                if msg.contains("Command timed out") {
                    return Err(AppError::Command(format!(
                        "Command timed out ({}s)",
                        GOG_TIMEOUT_SECONDS
                    )));
                }
                return Err(AppError::Command(format!("Failed to run `gog`: {}", msg)));
            }
        };

        let merged = format!("{}\n{}", out.trim(), err.trim());
        if is_gmail_rate_limit_error(&merged) {
            last_rate_limit_message = merged.trim().to_string();
            if attempt < max_retries {
                let sleep_secs = rate_limit_backoff_secs(attempt);
                log(&format!(
                    "Gmail rate limited, retrying in {} seconds ({}/{}): {}",
                    sleep_secs,
                    attempt + 1,
                    max_retries + 1,
                    display
                ));
                thread::sleep(Duration::from_secs(sleep_secs));
                continue;
            }
            return Err(AppError::RateLimit(last_rate_limit_message));
        }

        if code != 0 {
            return Err(AppError::Command(format!(
                "Command failed: {}\n{}",
                display,
                merged.trim()
            )));
        }

        if !expect_json || out.trim().is_empty() {
            return Ok(json!({}));
        }

        return serde_json::from_str::<Value>(out.trim()).map_err(|_| {
            AppError::Parse(format!(
                "Failed to parse JSON:\n{}",
                out.chars().take(500).collect::<String>()
            ))
        });
    }

    Err(AppError::RateLimit(last_rate_limit_message))
}

pub(crate) fn is_gmail_rate_limit_error(raw: &str) -> bool {
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

pub(crate) fn rate_limit_backoff_secs(attempt: u32) -> u64 {
    let factor = 1u64 << std::cmp::min(attempt, 10);
    let backoff = GOG_RATE_LIMIT_BASE_BACKOFF_SECS.saturating_mul(factor);
    std::cmp::min(backoff, GOG_RATE_LIMIT_MAX_BACKOFF_SECS)
}

pub(crate) fn ensure_label(
    label: &str,
    existing_labels: &mut HashSet<String>,
    account: &Option<String>,
) -> Result<(), AppError> {
    if existing_labels.contains(label) {
        return Ok(());
    }

    let args = vec!["gmail", "labels", "create", label]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    if let Err(err) = run_gog(&args, account, true) {
        if is_label_already_exists_error(&err.to_string()) {
            existing_labels.insert(label.to_string());
            log(&format!("Label already exists: {label}"));
            return Ok(());
        }
        return Err(err);
    }
    existing_labels.insert(label.to_string());
    log(&format!("Created label: {label}"));
    Ok(())
}

fn is_label_already_exists_error(raw: &str) -> bool {
    let msg = raw.to_lowercase();
    msg.contains("already exists")
        || msg.contains("name exists")
        || msg.contains("label exists")
        || msg.contains("duplicate")
}

pub(crate) fn apply_labels_with_options(
    grouped: &HashMap<String, Vec<String>>,
    account: &Option<String>,
    write_options: GmailWriteOptions,
) -> Result<(), AppError> {
    apply_labels_with_runner_and_options(
        &SystemCommandRunner,
        grouped,
        account,
        write_options,
    )
}

pub(crate) fn apply_labels_with_runner_and_options<R: CommandRunner>(
    runner: &R,
    grouped: &HashMap<String, Vec<String>>,
    account: &Option<String>,
    write_options: GmailWriteOptions,
) -> Result<(), AppError> {
    let mut labels: Vec<String> = grouped.keys().cloned().collect();
    labels.sort();
    for label in labels {
        let ids = grouped.get(&label).cloned().unwrap_or_default();
        if ids.is_empty() {
            continue;
        }

        let mut total_applied = 0usize;
        for chunk in ids.chunks(write_options.batch_size) {
            let mut args = vec!["gmail", "labels", "modify"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>();
            args.extend(chunk.iter().cloned());
            args.push("--add".to_string());
            args.push(label.clone());
            args.push("--remove".to_string());
            args.push("INBOX".to_string());

            run_gog_batch_with_retry(runner, &args, account, write_options)?;
            total_applied += chunk.len();
        }
        log(&format!("Labeled: {} -> {} threads", label, total_applied));
    }
    Ok(())
}

fn run_gog_batch_with_retry<R: CommandRunner>(
    runner: &R,
    args: &[String],
    account: &Option<String>,
    write_options: GmailWriteOptions,
) -> Result<Value, AppError> {
    let mut last_err: Option<AppError> = None;
    for attempt in 0..=write_options.batch_retries {
        match run_gog_with_runner(runner, args, account, false) {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !should_retry_batch_error(&e) {
                    return Err(e);
                }
                last_err = Some(e);
                if attempt < write_options.batch_retries {
                    let backoff = write_options.batch_retry_backoff_secs * (attempt as u64 + 1);
                    log(&format!(
                        "Gmail batch failed, retrying in {} seconds ({}/{})",
                        backoff,
                        attempt + 1,
                        write_options.batch_retries + 1
                    ));
                    thread::sleep(Duration::from_secs(backoff));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| AppError::Command("Gmail batch failed".to_string())))
}

fn should_retry_batch_error(err: &AppError) -> bool {
    // RateLimit is handled by the caller, so don't retry here.
    // Parse errors (invalid JSON from gog) are also permanent — retrying won't help.
    matches!(err, AppError::Command(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use anyhow::Result as AnyResult;

    struct MockRunner {
        outputs: Mutex<Vec<AnyResult<(i32, String, String)>>>,
        calls: Mutex<usize>,
    }

    impl MockRunner {
        fn from(outputs: Vec<AnyResult<(i32, String, String)>>) -> Self {
            Self {
                outputs: Mutex::new(outputs),
                calls: Mutex::new(0),
            }
        }

        fn call_count(&self) -> usize {
            *self.calls.lock().expect("lock poisoned")
        }
    }

    impl CommandRunner for MockRunner {
        fn run(
            &self,
            _program: &str,
            _args: &[String],
            _timeout_secs: u64,
        ) -> AnyResult<(i32, String, String)> {
            let mut calls = self.calls.lock().expect("lock poisoned");
            *calls += 1;
            drop(calls);
            let mut guard = self.outputs.lock().expect("lock poisoned");
            if guard.is_empty() {
                return Ok((0, "{}".to_string(), String::new()));
            }
            guard.remove(0)
        }
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

    #[test]
    fn test_detect_label_already_exists_error() {
        assert!(is_label_already_exists_error("Label already exists"));
        assert!(is_label_already_exists_error("duplicate label name"));
        assert!(!is_label_already_exists_error("permission denied"));
    }

    #[test]
    fn test_should_retry_batch_error() {
        assert!(!should_retry_batch_error(&AppError::RateLimit(
            "429".to_string()
        )));
        assert!(should_retry_batch_error(&AppError::Command(
            "timeout".to_string()
        )));
        assert!(!should_retry_batch_error(&AppError::Parse(
            "invalid json".to_string()
        )));
        assert!(!should_retry_batch_error(&AppError::Other(
            "generic".to_string()
        )));
    }

    #[test]
    fn test_run_gog_with_runner_parses_json() {
        let runner = MockRunner::from(vec![Ok((
            0,
            "{\"labels\":[{\"name\":\"INBOX\"}]}".to_string(),
            String::new(),
        ))]);
        let args = vec![
            "gmail".to_string(),
            "labels".to_string(),
            "list".to_string(),
        ];
        let v = run_gog_with_runner(&runner, &args, &None, true).expect("run_gog failed");
        let labels = v
            .get("labels")
            .and_then(Value::as_array)
            .expect("labels should be array");
        assert_eq!(labels.len(), 1);
    }

    #[test]
    fn test_apply_labels_with_runner_batches_large_groups() {
        let runner = MockRunner::from(vec![
            Ok((0, "{}".to_string(), String::new())),
            Ok((0, "{}".to_string(), String::new())),
            Ok((0, "{}".to_string(), String::new())),
        ]);
        let mut grouped = HashMap::new();
        let ids = (0..250).map(|i| format!("t{i}")).collect::<Vec<_>>();
        grouped.insert("账单".to_string(), ids);

        apply_labels_with_runner_and_options(
            &runner,
            &grouped,
            &None,
            GmailWriteOptions::default(),
        )
        .expect("apply labels failed");
        assert_eq!(runner.call_count(), 3);
    }
}
