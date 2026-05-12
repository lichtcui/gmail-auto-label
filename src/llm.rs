//! DeepSeek API client (OpenAI-compatible chat completions).
//!
//! Replaces the previous subprocess approach with a direct HTTP API call.
//! Supports `--api-key` argument and `DEEPSEEK_API_KEY` environment variable.

use std::sync::OnceLock;
use std::time::Duration;

use crate::errors::AppError;
use crate::utils::log;

const DEFAULT_MODEL: &str = "deepseek-v4-flash";
const REQUEST_TIMEOUT_SECS: u64 = 60;
const MAX_RETRIES: u32 = 3;
const BASE_BACKOFF_MS: u64 = 1000;

/// Configuration for the DeepSeek API client.
#[derive(Debug, Clone)]
pub(crate) struct LlmConfig {
    pub(crate) api_key: String,
    pub(crate) model: String,
}

impl LlmConfig {
    /// Build config from CLI args and env vars.
    /// Precedence: --api-key > DEEPSEEK_API_KEY env var > error.
    pub(crate) fn from_opt(
        api_key: Option<String>,
        model: Option<String>,
    ) -> Result<Self, AppError> {
        let key = api_key
            .or_else(|| std::env::var("DEEPSEEK_API_KEY").ok())
            .ok_or_else(|| {
                AppError::Config(
                    "DeepSeek API key required. Set --api-key <KEY> or DEEPSEEK_API_KEY env var."
                        .to_string(),
                )
            })?;
        Ok(Self {
            api_key: key,
            model: model.unwrap_or_else(|| DEFAULT_MODEL.to_string()),
        })
    }
}

/// Shared HTTP client, created once and reused across all LLM calls.
fn shared_client() -> &'static reqwest::blocking::Client {
    static CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .pool_idle_timeout(Duration::from_secs(120))
            .pool_max_idle_per_host(10)
            .build()
            .expect("Failed to create HTTP client (tls backend missing?)")
    })
}

/// Call DeepSeek chat completions in non-streaming mode.
/// Retries transient failures (rate limits, 5xx, network errors) with exponential backoff.
/// Returns the raw text content of the assistant's reply.
pub(crate) fn call_chat(prompt: &str, config: &LlmConfig, max_tokens: Option<u32>) -> Result<String, AppError> {
    let body = serde_json::json!({
        "model": config.model,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.01,
        "max_tokens": max_tokens.unwrap_or(1024),
        "stream": false,
    });

    for attempt in 0..=MAX_RETRIES {
        match try_request(&body, config) {
            Ok(text) => return Ok(text),
            Err(e) if attempt < MAX_RETRIES && is_transient(&e) => {
                let backoff = BASE_BACKOFF_MS * (1 << attempt);
                log(&format!(
                    "LLM_RETRY: attempt={} backoff={}ms reason=\"{}\"",
                    attempt + 1,
                    backoff,
                    error_summary(&e)
                ));
                std::thread::sleep(Duration::from_millis(backoff));
            }
            Err(e) => return Err(e),
        }
    }
    // All retry attempts exhausted without returning — the final iteration
    // (attempt == MAX_RETRIES) always enters the `else` branch.
    unreachable!()
}

/// The actual HTTP call without retry logic.
fn try_request(body: &serde_json::Value, config: &LlmConfig) -> Result<String, AppError> {
    let client = shared_client();
    let resp = client
        .post("https://api.deepseek.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", config.api_key))
        .json(body)
        .send()
        .map_err(|e| {
            if e.is_timeout() {
                AppError::Other(format!(
                    "DeepSeek API timed out ({}s)",
                    REQUEST_TIMEOUT_SECS
                ))
            } else if e.is_connect() {
                AppError::Other(format!("DeepSeek API connection failed: {e}"))
            } else {
                AppError::Other(format!("DeepSeek API request failed: {e}"))
            }
        })?;

    let status = resp.status();
    if !status.is_success() {
        let body_text = resp.text().unwrap_or_default();
        if status.as_u16() == 401 {
            return Err(AppError::Config(
                "DeepSeek API authentication failed. Check your API key.".to_string(),
            ));
        }
        if status.as_u16() == 429 {
            return Err(AppError::RateLimit(format!(
                "DeepSeek rate limited ({}): {}",
                status.as_u16(),
                body_text
            )));
        }
        // 4xx client errors (other than 401/429) — not transient, don't retry
        if status.as_u16() < 500 {
            return Err(AppError::Other(format!(
                "DeepSeek API client_error ({}): {}",
                status.as_u16(),
                body_text
            )));
        }
        // 5xx server errors — transient, will be retried
        return Err(AppError::Other(format!(
            "DeepSeek API error ({}): {}",
            status.as_u16(),
            body_text
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .map_err(|e| AppError::Parse(format!("Failed to parse DeepSeek response JSON: {e}")))?;

    body["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.trim().to_string())
        .ok_or_else(|| {
            AppError::Parse("DeepSeek response missing choices[0].message.content".to_string())
        })
}

/// Whether an error is transient and worth retrying.
fn is_transient(err: &AppError) -> bool {
    match err {
        // 429 rate limit — transient
        AppError::RateLimit(_) => true,
        // Network errors, timeouts, 5xx — transient
        // "client_error" marker indicates a 4xx (excluding 401/429) — not transient
        AppError::Other(m) => !m.contains("authentication") && !m.contains("client_error"),
        _ => false,
    }
}

/// Short one-line description of an error for retry logging.
fn error_summary(err: &AppError) -> &str {
    match err {
        AppError::RateLimit(_) => "rate_limited",
        AppError::Other(m) => {
            if m.contains("timed out") {
                "timeout"
            } else if m.contains("connection failed") {
                "connection_failed"
            } else if m.contains("client_error") {
                "client_error"
            } else {
                "server_error"
            }
        }
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_llm_config_from_env() {
        let cfg = LlmConfig::from_opt(Some("sk-test".into()), None).unwrap();
        assert_eq!(cfg.api_key, "sk-test");
        assert_eq!(cfg.model, DEFAULT_MODEL);
    }

    #[test]
    fn test_llm_config_no_key_errors() {
        let err = LlmConfig::from_opt(None, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("API key"),
            "error should mention API key: {msg}"
        );
    }

    #[test]
    fn test_llm_config_custom_model() {
        let cfg = LlmConfig::from_opt(Some("sk-x".into()), Some("deepseek-v4-pro".into())).unwrap();
        assert_eq!(cfg.model, "deepseek-v4-pro");
    }

    #[test]
    fn test_is_transient_classifies_correctly() {
        assert!(is_transient(&AppError::RateLimit("429".into())));
        assert!(is_transient(&AppError::Other("connection reset".into())));
        assert!(is_transient(&AppError::Other(
            "DeepSeek API error (503): Service Unavailable".into()
        )));
        assert!(is_transient(&AppError::Other(
            "DeepSeek API timed out (60s)".into()
        )));
        assert!(!is_transient(&AppError::Other(
            "authentication token expired".into()
        )));
        assert!(!is_transient(&AppError::Other(
            "DeepSeek API client_error (403): Forbidden".into()
        )));
        assert!(!is_transient(&AppError::Other(
            "DeepSeek API client_error (422): ...".into()
        )));
        assert!(!is_transient(&AppError::Config("bad key".into())));
        assert!(!is_transient(&AppError::Parse("bad json".into())));
    }

    #[test]
    fn test_error_summary_maps_correctly() {
        assert_eq!(
            error_summary(&AppError::RateLimit("".into())),
            "rate_limited"
        );
        assert_eq!(
            error_summary(&AppError::Other("DeepSeek API timed out (60s)".into())),
            "timeout"
        );
        assert_eq!(
            error_summary(&AppError::Other("connection failed: dns error".into())),
            "connection_failed"
        );
        assert_eq!(
            error_summary(&AppError::Other(
                "DeepSeek API client_error (422): ...".into()
            )),
            "client_error"
        );
        assert_eq!(
            error_summary(&AppError::Other("DeepSeek API error (503): ...".into())),
            "server_error"
        );
    }
}
