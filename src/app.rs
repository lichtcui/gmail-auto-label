use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::Result;
use rayon::ThreadPool;
use rayon::prelude::*;
use serde::Serialize;

use crate::cache::{
    apply_feedback_from_file, cache_fingerprint, load_cache, prune_cache, save_cache,
};
use crate::classify::{
    build_rule_priority_indexes, classify_from_cache_with_indexes, classify_with_llm_result,
    compress_labels_if_needed, llm_classify_email, llm_error_hint,
};
use crate::errors::AppError;
use crate::gog::{GmailWriteOptions, apply_labels_with_options, ensure_label};
use crate::llm::LlmConfig;
use crate::models::{
    Args, CacheData, DEFAULT_CACHE_MAX_MEMOS, DEFAULT_CACHE_MAX_RULES, DEFAULT_CACHE_TTL_HOURS,
    DEFAULT_FEEDBACK_BAD_THRESHOLD, DEFAULT_FEEDBACK_FILE, DEFAULT_FEEDBACK_HIT_PENALTY,
    DEFAULT_FEEDBACK_MAX_AGE_HOURS, LlmClassify, OutputFormat, SyncConfig, ThreadInfo,
};
use crate::sync::{run_sync, use_synced_data};
use crate::utils::{
    auto_llm_workers, log, resolve_label_alias, set_machine_readable_output,
};

type DynErr = AppError;

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct RoundMetrics {
    pub(crate) total_threads: usize,
    pub(crate) labeled_threads: usize,
    pub(crate) cache_hits: usize,
    pub(crate) llm_jobs: usize,
    pub(crate) llm_success: usize,
    pub(crate) llm_failures: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct AppRunSummary {
    pub(crate) ok: bool,
    pub(crate) final_state: String,
    pub(crate) rounds: usize,
    pub(crate) processed_rounds: usize,
    pub(crate) total_labeled_threads: usize,
    pub(crate) last_metrics: RoundMetrics,
}

trait AppDeps: Sync {
    fn ensure_label(
        &self,
        label: &str,
        existing_labels: &mut HashSet<String>,
        account: &Option<String>,
    ) -> std::result::Result<(), DynErr>;
    fn llm_classify_email(
        &self,
        sender: &str,
        subject: &str,
        snippet: &str,
        llm_config: &LlmConfig,
    ) -> LlmClassify;
    fn apply_labels(
        &self,
        grouped: &HashMap<String, Vec<String>>,
        account: &Option<String>,
        write_options: GmailWriteOptions,
    ) -> std::result::Result<(), DynErr>;
}

struct RealDeps;

impl AppDeps for RealDeps {
    fn ensure_label(
        &self,
        label: &str,
        existing_labels: &mut HashSet<String>,
        account: &Option<String>,
    ) -> std::result::Result<(), DynErr> {
        ensure_label(label, existing_labels, account)
    }

    fn llm_classify_email(
        &self,
        sender: &str,
        subject: &str,
        snippet: &str,
        llm_config: &LlmConfig,
    ) -> LlmClassify {
        llm_classify_email(sender, subject, snippet, llm_config)
    }

    fn apply_labels(
        &self,
        grouped: &HashMap<String, Vec<String>>,
        account: &Option<String>,
        write_options: GmailWriteOptions,
    ) -> std::result::Result<(), DynErr> {
        apply_labels_with_options(grouped, account, write_options)
    }
}

fn collect_threads(
    cache: &mut CacheData,
    threads: &[ThreadInfo],
) -> std::result::Result<
    (
        HashMap<String, Vec<String>>,
        Vec<String>,
        Vec<ThreadInfo>,
        RoundMetrics,
    ),
    DynErr,
> {
    let rule_indexes = build_rule_priority_indexes(cache);
    let mut metrics = RoundMetrics {
        total_threads: threads.len(),
        ..RoundMetrics::default()
    };
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    let mut processed_ids: Vec<String> = Vec::new();
    let mut llm_jobs: Vec<ThreadInfo> = Vec::new();

    for t in threads {
        if let Some((label, source)) = classify_from_cache_with_indexes(
            &t.sender,
            &t.subject,
            &t.snippet,
            cache,
            DEFAULT_CACHE_TTL_HOURS,
            &rule_indexes,
        ) {
            metrics.cache_hits += 1;
            grouped.entry(label.clone()).or_default().push(t.id.clone());
            processed_ids.push(t.id.clone());
            log(&format!(
                "CLASSIFY: thread={} label={} source={} summary=cache_hit",
                t.id, label, source
            ));
        } else {
            llm_jobs.push(t.clone());
        }
    }

    metrics.llm_jobs = llm_jobs.len();
    Ok((grouped, processed_ids, llm_jobs, metrics))
}

#[allow(clippy::too_many_arguments)]
fn run_llm_classify<D: AppDeps>(
    deps: &D,
    cache: &mut CacheData,
    llm_config: &LlmConfig,
    effective_workers: usize,
    llm_pool: &mut Option<ThreadPool>,
    llm_jobs: Vec<ThreadInfo>,
    grouped: &mut HashMap<String, Vec<String>>,
    processed_ids: &mut Vec<String>,
) -> std::result::Result<(usize, usize), DynErr> {
    if llm_jobs.is_empty() {
        return Ok((0, 0));
    }

    log(&format!(
        "CACHE_MISS: {} threads, calling LLM with {} workers...",
        llm_jobs.len(),
        effective_workers
    ));

    let results = if effective_workers <= 1 {
        llm_jobs
            .into_iter()
            .map(|job| {
                let res =
                    deps.llm_classify_email(&job.sender, &job.subject, &job.snippet, llm_config);
                (job, res)
            })
            .collect::<Vec<_>>()
    } else {
        if llm_pool.is_none() {
            *llm_pool = Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(effective_workers)
                    .build()
                    .map_err(|e| {
                        AppError::Other(format!("Failed to create LLM thread pool: {e}"))
                    })?,
            );
        }
        let pool = llm_pool
            .as_ref()
            .ok_or_else(|| AppError::Other("LLM thread pool is not initialized".to_string()))?;
        pool.install(|| {
            llm_jobs
                .into_par_iter()
                .map(|job| {
                    let res = deps.llm_classify_email(
                        &job.sender,
                        &job.subject,
                        &job.snippet,
                        llm_config,
                    );
                    (job, res)
                })
                .collect::<Vec<_>>()
        })
    };

    let total_results = results.len();
    let mut llm_setup_failures = 0usize;
    let mut llm_success = 0usize;
    let mut llm_failures = 0usize;

    for (job, result) in results {
        let (label, source, summary) =
            classify_with_llm_result(&job.sender, &job.subject, &job.snippet, cache, &result);
        if source == "llm:error" && label == "uncategorized" {
            let hint = llm_error_hint(&summary)
                .unwrap_or("Please check the LLM API key and network, then retry.");
            if matches!(summary.as_str(), "llm_error" | "llm_timeout") {
                llm_setup_failures += 1;
            }
            log(&format!(
                "CLASSIFY_FAILED: thread={} reason={}. {} Skipped labeling and will retry next round.",
                job.id, summary, hint
            ));
            llm_failures += 1;
            continue;
        }
        llm_success += 1;
        grouped
            .entry(label.clone())
            .or_default()
            .push(job.id.clone());
        processed_ids.push(job.id.clone());
        log(&format!(
            "CLASSIFY: thread={} label={} source={} summary={}",
            job.id, label, source, summary
        ));
    }

    if total_results > 0 && llm_setup_failures == total_results {
        return Err(AppError::Config(
            "LLM API unavailable: all classification requests failed (network / API key / timeout). Check --api-key and network connectivity."
                .to_string(),
        ));
    }

    Ok((llm_success, llm_failures))
}

fn regroup_by_alias(
    grouped: HashMap<String, Vec<String>>,
    cache: &CacheData,
) -> HashMap<String, Vec<String>> {
    if cache.label_aliases.is_empty() {
        return grouped;
    }
    let mut regrouped: HashMap<String, Vec<String>> = HashMap::new();
    for (label, ids) in grouped {
        let final_label = resolve_label_alias(&label, cache);
        regrouped.entry(final_label).or_default().extend(ids);
    }
    regrouped
}

fn ensure_grouped_labels_exist<D: AppDeps>(
    deps: &D,
    grouped: &HashMap<String, Vec<String>>,
    account: &Option<String>,
    existing_labels: &mut HashSet<String>,
) -> std::result::Result<(), DynErr> {
    let mut labels = grouped
        .iter()
        .filter(|(_, ids)| !ids.is_empty())
        .map(|(label, _)| label.as_str())
        .collect::<Vec<_>>();
    labels.sort_unstable();

    for label in labels {
        deps.ensure_label(label, existing_labels, account)?;
    }

    Ok(())
}

fn seed_existing_labels(
    cache: &CacheData,
    merged_label: &str,
) -> HashSet<String> {
    let mut labels = HashSet::new();
    for rule in &cache.rules {
        let label = rule.label.trim();
        if !label.is_empty() {
            labels.insert(label.to_string());
        }
    }
    for alias_target in cache.label_aliases.values() {
        let label = alias_target.trim();
        if !label.is_empty() {
            labels.insert(label.to_string());
        }
    }
    let merged = merged_label.trim();
    if !merged.is_empty() {
        labels.insert(merged.to_string());
    }
    labels
}

#[allow(clippy::too_many_arguments)]
fn process_once_with_deps<D: AppDeps>(
    deps: &D,
    args: &Args,
    cache: &mut CacheData,
    existing_labels: &mut HashSet<String>,
    effective_workers: usize,
    llm_pool: &mut Option<ThreadPool>,
    write_options: GmailWriteOptions,
    llm_config: &LlmConfig,
) -> std::result::Result<(String, RoundMetrics), DynErr> {
    let pending_threads = use_synced_data(cache);
    log(&format!(
        "CACHED_INPUT: using {} synced threads from cache",
        pending_threads.len()
    ));
    if pending_threads.is_empty() {
        log("DONE_NO_PENDING: no pending emails, run finished.");
        return Ok(("done".to_string(), RoundMetrics::default()));
    }

    let (mut grouped, mut processed_ids, llm_jobs, mut metrics) =
        collect_threads(cache, &pending_threads)?;
    let (llm_success, llm_failures) = run_llm_classify(
        deps,
        cache,
        llm_config,
        effective_workers,
        llm_pool,
        llm_jobs,
        &mut grouped,
        &mut processed_ids,
    )?;
    metrics.llm_success = llm_success;
    metrics.llm_failures = llm_failures;

    compress_labels_if_needed(cache, args.max_labels, &args.merged_label);
    let grouped = regroup_by_alias(grouped, cache);
    ensure_grouped_labels_exist(deps, &grouped, &args.account, existing_labels)?;
    deps.apply_labels(
        &grouped,
        &args.account,
        write_options,
    )?;

    let total: usize = grouped.values().map(Vec::len).sum();
    metrics.labeled_threads = total;
    let mut keys: Vec<String> = grouped.keys().cloned().collect();
    keys.sort();
    let summary = keys
        .into_iter()
        .filter_map(|k| grouped.get(&k).map(|ids| (k, ids.len())))
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| format!("{}:{}", k, n))
        .collect::<Vec<_>>()
        .join(" | ");
    log(&format!("ROUND_DONE: total={} | {}", total, summary));

    Ok(("processed".to_string(), metrics))
}

fn validate_args(args: &Args) -> std::result::Result<(), AppError> {
    if args.max_labels < 2 {
        return Err(AppError::Config(
            "--max-labels must be at least 2".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn run_with_args(args: Args) -> Result<AppRunSummary> {
    set_machine_readable_output(args.output == OutputFormat::Json);
    validate_args(&args)?;

    let effective_workers = auto_llm_workers();
    log(&format!("LLM worker count: {}", effective_workers));
    let mut llm_pool: Option<ThreadPool> = None;
    log(
        "Cold-start optimization enabled: LLM thread pool is lazy-init (triggered only on cache miss)",
    );

    let mut cache = load_cache(&args.cache_file);
    prune_cache(
        &mut cache,
        DEFAULT_CACHE_MAX_RULES,
        DEFAULT_CACHE_MAX_MEMOS,
        DEFAULT_CACHE_TTL_HOURS,
    );
    let last_saved_fingerprint = cache_fingerprint(&cache)?;

    // Avoid startup `labels list` call to reduce Gmail API usage.
    // Seed from local known labels, then create on demand when needed.
    let mut existing_labels = seed_existing_labels(&cache, &args.merged_label);

    let llm_config = LlmConfig::from_opt(args.api_key.clone(), Some(args.model.clone()))?;

    // --- Phase 1: IMAP sync ---
    if args.sync {
        let imap_user = args
            .imap_user
            .clone()
            .ok_or_else(|| AppError::Config("--imap-user is required with --sync".to_string()))?;
        let imap_pass = args
            .imap_pass
            .clone()
            .ok_or_else(|| AppError::Config("--imap-pass is required with --sync".to_string()))?;
        let sync_cfg = SyncConfig {
            imap_user,
            imap_pass,
            imap_host: args.imap_host.clone(),
            imap_port: args.imap_port,
            max_messages: args.sync_max,
        };
        run_sync(&sync_cfg, &mut cache, &llm_config)?;
        save_cache(&args.cache_file, &cache)?;
        log(&format!(
            "SYNC: IMAP sync complete — {} threads in cache",
            cache.synced_threads.len()
        ));
        // --sync always exits after syncing; use --from-cache separately to process
        return Ok(AppRunSummary {
            ok: true,
            final_state: "synced".to_string(),
            rounds: 0,
            processed_rounds: 0,
            total_labeled_threads: 0,
            last_metrics: RoundMetrics::default(),
        });
    }

    // --- Phase 2: Process from cache ---
    if !args.from_cache {
        return Err(AppError::Config(
            "Use --sync for Phase 1 (IMAP sync + LLM summarize) or --from-cache for Phase 2 (classify + apply labels)."
                .to_string(),
        )
        .into());
    }

    // Apply feedback before processing
    let feedback_summary = apply_feedback_from_file(
        &mut cache,
        DEFAULT_FEEDBACK_FILE,
        DEFAULT_FEEDBACK_BAD_THRESHOLD,
        DEFAULT_FEEDBACK_HIT_PENALTY,
        DEFAULT_FEEDBACK_MAX_AGE_HOURS,
    )?;
    if feedback_summary.total_events > 0 {
        log(&format!(
            "FEEDBACK_APPLIED: total_events={} applied_events={} skipped={} affected_rules={} dropped_rules={}",
            feedback_summary.total_events,
            feedback_summary.applied_events,
            feedback_summary.skipped_events,
            feedback_summary.affected_rules,
            feedback_summary.dropped_rules
        ));
    }

    let round_started = Instant::now();
    let (state, metrics) = match process_once_with_deps(
        &RealDeps,
        &args,
        &mut cache,
        &mut existing_labels,
        effective_workers,
        &mut llm_pool,
        GmailWriteOptions::default(),
        &llm_config,
    ) {
        Ok(v) => v,
        Err(e) => match e {
            AppError::Config(msg) => {
                return Err(AppError::Config(msg).into());
            }
            other => {
                log(&format!("ERROR: {}", other));
                ("error".to_string(), RoundMetrics::default())
            }
        },
    };

    let rounds = 1usize;
    let processed_rounds = if state == "processed" { 1 } else { 0 };
    let total_labeled_threads = if state == "processed" {
        metrics.labeled_threads
    } else {
        0
    };

    if state == "processed" {
        log(&format!(
            "ROUND_METRICS: total={} labeled={} cache_hits={} llm_jobs={} llm_success={} llm_failures={} elapsed_ms={}",
            metrics.total_threads,
            metrics.labeled_threads,
            metrics.cache_hits,
            metrics.llm_jobs,
            metrics.llm_success,
            metrics.llm_failures,
            round_started.elapsed().as_millis()
        ));
        prune_cache(
            &mut cache,
            DEFAULT_CACHE_MAX_RULES,
            DEFAULT_CACHE_MAX_MEMOS,
            DEFAULT_CACHE_TTL_HOURS,
        );
    }

    let current_fingerprint = cache_fingerprint(&cache)?;
    if current_fingerprint != last_saved_fingerprint {
        save_cache(&args.cache_file, &cache)?;
    }

    Ok(AppRunSummary {
        ok: true,
        final_state: state,
        rounds,
        processed_rounds,
        total_labeled_threads,
        last_metrics: metrics,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::cache::memo_key;
    use crate::classify::{llm_error_hint, rule_matches};
    use crate::models::{
        DEFAULT_CACHE_FILE, DEFAULT_MAX_ACTIVE_LABELS, DEFAULT_MERGED_LABEL, OutputFormat, Rule,
        RuleInput, SyncedThread,
    };
    use crate::utils::{normalize_label, now_ts};

    fn add_synced(cache: &mut CacheData, id: &str, sender: &str, subject: &str, snippet: &str) {
        cache.synced_threads.insert(
            id.to_string(),
            SyncedThread {
                thread_id: id.to_string(),
                sender: sender.to_string(),
                subject: subject.to_string(),
                body_summary: snippet.to_string(),
                message_count: 1,
                ts: now_ts(),
            },
        );
    }

    struct MockDeps {
        llm_result: LlmClassify,
        applied: Mutex<Vec<HashMap<String, Vec<String>>>>,
        remove_inbox_flags: Mutex<Vec<bool>>,
        llm_calls: Mutex<usize>,
    }

    impl AppDeps for MockDeps {
        fn ensure_label(
            &self,
            label: &str,
            existing_labels: &mut HashSet<String>,
            _account: &Option<String>,
        ) -> std::result::Result<(), DynErr> {
            existing_labels.insert(label.to_string());
            Ok(())
        }

        fn llm_classify_email(
            &self,
            _sender: &str,
            _subject: &str,
            _snippet: &str,
            _llm_config: &LlmConfig,
        ) -> LlmClassify {
            let mut calls = self.llm_calls.lock().expect("lock poisoned");
            *calls += 1;
            self.llm_result.clone()
        }

        fn apply_labels(
            &self,
            grouped: &HashMap<String, Vec<String>>,
            _account: &Option<String>,
            _write_options: GmailWriteOptions,
        ) -> std::result::Result<(), DynErr> {
            self.applied
                .lock()
                .expect("lock poisoned")
                .push(grouped.clone());
            self.remove_inbox_flags
                .lock()
                .expect("lock poisoned")
                .push(true);
            Ok(())
        }
    }

    fn make_args() -> Args {
        Args {
            account: None,
            api_key: Some("test-key".to_string()),
            model: "deepseek-v4-flash".to_string(),
            cache_file: DEFAULT_CACHE_FILE.to_string(),
            max_labels: DEFAULT_MAX_ACTIVE_LABELS,
            output: OutputFormat::Text,
            merged_label: DEFAULT_MERGED_LABEL.to_string(),
            sync: false,
            imap_user: None,
            imap_pass: None,
            imap_host: "imap.gmail.com".to_string(),
            imap_port: 993,
            from_cache: false,
            sync_max: 0,
        }
    }

    fn default_write_options() -> GmailWriteOptions {
        GmailWriteOptions::default()
    }

    fn test_llm_config() -> LlmConfig {
        LlmConfig::from_opt(Some("test-key".into()), None).unwrap()
    }

    #[test]
    fn test_normalize_label() {
        assert_eq!(normalize_label("  账单   通知 "), "账单 通知");
        assert_eq!(normalize_label(""), "uncategorized");
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
    fn test_llm_error_hint_for_known_errors() {
        assert!(llm_error_hint("llm_timeout").is_some());
        assert!(llm_error_hint("llm_rate_limited").is_some());
        assert!(llm_error_hint("llm_invalid_json").is_some());
        assert!(llm_error_hint("unknown_error").is_none());
    }

    #[test]
    fn test_process_once_with_deps_no_external_commands() {
        let deps = MockDeps {
            llm_result: LlmClassify {
                ok: true,
                label: "账单".to_string(),
                summary: "账单邮件".to_string(),
                rule: RuleInput {
                    description: "账单".to_string(),
                    include_keywords: vec!["invoice".to_string()],
                    exclude_keywords: vec![],
                },
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            llm_calls: Mutex::new(0),
        };
        let args = make_args();
        let mut cache = CacheData::default();
        add_synced(
            &mut cache,
            "t1",
            "billing@example.com",
            "monthly invoice",
            "invoice attached",
        );
        let mut existing_labels = HashSet::new();
        let mut llm_pool: Option<ThreadPool> = None;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            &mut existing_labels,
            1,
            &mut llm_pool,
            default_write_options(),
            &test_llm_config(),
        )
        .expect("process_once should succeed");
        assert_eq!(state, "processed");
        assert_eq!(metrics.total_threads, 1);
        assert_eq!(metrics.llm_jobs, 1);
        assert_eq!(metrics.llm_success, 1);

        let applied = deps.applied.lock().expect("lock poisoned");
        assert_eq!(applied.len(), 1);
        let grouped = &applied[0];
        let ids = grouped.get("账单").expect("expected 账单 label");
        assert_eq!(ids, &vec!["t1".to_string()]);

        let remove_inbox_flags = deps.remove_inbox_flags.lock().expect("lock poisoned");
        assert_eq!(remove_inbox_flags.len(), 1);
        assert!(remove_inbox_flags[0]);
        assert_eq!(*deps.llm_calls.lock().expect("lock poisoned"), 1);
    }

    #[test]
    fn test_e2e_cache_hit_path_skips_llm() {
        let mut cache = CacheData::default();
        add_synced(
            &mut cache,
            "t-cache",
            "billing@example.com",
            "invoice ready",
            "monthly invoice",
        );
        cache.rules.push(Rule {
            id: "r-cache".to_string(),
            label: "账单".to_string(),
            include_keywords: vec!["invoice".to_string()],
            hits: 3,
            ..Default::default()
        });
        let deps = MockDeps {
            llm_result: LlmClassify {
                ok: true,
                label: "不应触发".to_string(),
                summary: "unused".to_string(),
                rule: RuleInput::default(),
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            llm_calls: Mutex::new(0),
        };
        let args = make_args();
        let mut existing_labels = HashSet::new();
        let mut llm_pool: Option<ThreadPool> = None;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            &mut existing_labels,
            1,
            &mut llm_pool,
            default_write_options(),
            &test_llm_config(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.llm_jobs, 0);
        assert_eq!(*deps.llm_calls.lock().expect("lock poisoned"), 0);
    }

    #[test]
    fn test_e2e_llm_failure_skips_writes() {
        let deps = MockDeps {
            llm_result: LlmClassify {
                ok: false,
                label: "uncategorized".to_string(),
                summary: "llm_timeout".to_string(),
                rule: RuleInput::default(),
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            llm_calls: Mutex::new(0),
        };
        let args = make_args();
        let mut cache = CacheData::default();
        add_synced(&mut cache, "t-fail", "x@example.com", "unknown", "unknown");
        let mut existing_labels = HashSet::new();
        let mut llm_pool: Option<ThreadPool> = None;

        let err = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            &mut existing_labels,
            1,
            &mut llm_pool,
            default_write_options(),
            &test_llm_config(),
        )
        .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("LLM API unavailable"),
            "expected LLM API error, got: {msg}"
        );
        assert_eq!(*deps.llm_calls.lock().expect("lock poisoned"), 1);
    }

    #[test]
    fn test_process_once_ensures_merged_label_before_apply() {
        let deps = MockDeps {
            llm_result: LlmClassify {
                ok: true,
                label: "会议".to_string(),
                summary: "会议提醒".to_string(),
                rule: RuleInput {
                    description: "会议类邮件".to_string(),
                    include_keywords: vec!["meeting".to_string()],
                    exclude_keywords: vec![],
                },
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            llm_calls: Mutex::new(0),
        };
        let mut args = make_args();
        args.max_labels = 2;
        args.merged_label = "统一收纳".to_string();

        let mut cache = CacheData::default();
        add_synced(
            &mut cache,
            "t-merge",
            "calendar@example.com",
            "meeting invite",
            "team sync",
        );
        cache.rules.push(Rule {
            id: "r-finance".to_string(),
            label: "财务".to_string(),
            include_keywords: vec!["invoice".to_string()],
            hits: 10,
            updated_at: 100,
            ..Default::default()
        });
        cache.rules.push(Rule {
            id: "r-subscription".to_string(),
            label: "订阅".to_string(),
            include_keywords: vec!["newsletter".to_string()],
            hits: 9,
            updated_at: 90,
            ..Default::default()
        });

        let mut existing_labels = HashSet::new();
        let mut llm_pool: Option<ThreadPool> = None;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            &mut existing_labels,
            1,
            &mut llm_pool,
            default_write_options(),
            &test_llm_config(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.llm_success, 1);
        assert!(existing_labels.contains("统一收纳"));
        assert!(!existing_labels.contains("会议"));

        let applied = deps.applied.lock().expect("lock poisoned");
        assert_eq!(applied.len(), 1);
        let grouped = &applied[0];
        let ids = grouped
            .get("统一收纳")
            .expect("expected merged label to be applied");
        assert_eq!(ids, &vec!["t-merge".to_string()]);
    }

}
