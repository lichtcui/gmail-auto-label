use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::models::CacheData;

static LOG_TO_STDERR: AtomicBool = AtomicBool::new(false);

pub(crate) fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn log(msg: &str) {
    if LOG_TO_STDERR.load(Ordering::Relaxed) {
        eprintln!("{msg}");
        return;
    }
    println!("{msg}");
}

pub(crate) fn set_machine_readable_output(enabled: bool) {
    LOG_TO_STDERR.store(enabled, Ordering::Relaxed);
}

pub(crate) fn auto_llm_workers() -> usize {
    let cpu = std::thread::available_parallelism().map_or(4, usize::from);
    let mut workers = std::cmp::max(2, cpu / 2);
    workers = std::cmp::min(workers, 8);
    workers
}

pub(crate) fn normalize_label(label: &str) -> String {
    let cleaned = label.split_whitespace().collect::<Vec<_>>().join(" ");
    let clipped: String = cleaned.chars().take(80).collect();
    if clipped.is_empty() {
        return "uncategorized".to_string();
    }
    // Normalize canonical category names (handle LLM case inconsistencies)
    let lower = clipped.to_lowercase();
    match lower.as_str() {
        "ci/cd" => return "CI/CD".to_string(),
        "security" => return "Security".to_string(),
        "newsletter" => return "Newsletter".to_string(),
        "recruitment" => return "Recruitment".to_string(),
        "invoice" => return "Invoice".to_string(),
        "others" => return "Others".to_string(),
        _ => {}
    }
    clipped
}

pub(crate) fn resolve_label_alias(label: &str, cache: &CacheData) -> String {
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
