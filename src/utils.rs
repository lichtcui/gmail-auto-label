use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::models::CacheData;

pub(crate) fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn log(msg: &str) {
    println!("{msg}");
}

pub(crate) fn auto_codex_workers(limit: usize) -> usize {
    let cpu = std::thread::available_parallelism().map_or(4, usize::from);
    let mut workers = std::cmp::max(2, cpu / 2);
    workers = std::cmp::min(workers, 8);
    workers = std::cmp::min(workers, std::cmp::max(1, limit));
    workers
}

pub(crate) fn normalize_label(label: &str) -> String {
    let cleaned = label.split_whitespace().collect::<Vec<_>>().join(" ");
    let clipped: String = cleaned.chars().take(80).collect();
    if clipped.is_empty() {
        "待分类".to_string()
    } else {
        clipped
    }
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
