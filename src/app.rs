use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    compress_labels_if_needed, llm_classify_email, llm_classify_refine, llm_consolidate_labels, llm_error_hint,
};
use crate::errors::AppError;
use crate::llm::LlmConfig;
use crate::models::{
    Args, CacheData, DEFAULT_CACHE_MAX_MEMOS, DEFAULT_CACHE_MAX_RULES, DEFAULT_CACHE_TTL_HOURS,
    DEFAULT_FEEDBACK_BAD_THRESHOLD, DEFAULT_FEEDBACK_FILE, DEFAULT_FEEDBACK_HIT_PENALTY,
    DEFAULT_FEEDBACK_MAX_AGE_HOURS, LlmClassify, OutputFormat, SyncConfig, ThreadInfo,
};
use crate::sync::{apply_labels_via_imap, run_sync, use_synced_data};
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
    fn llm_classify_email(
        &self,
        sender: &str,
        subject: &str,
        snippet: &str,
        llm_config: &LlmConfig,
    ) -> LlmClassify;
}

struct RealDeps;

impl AppDeps for RealDeps {
    fn llm_classify_email(
        &self,
        sender: &str,
        subject: &str,
        snippet: &str,
        llm_config: &LlmConfig,
    ) -> LlmClassify {
        llm_classify_email(sender, subject, snippet, llm_config)
    }
}

fn collect_threads(
    cache: &mut CacheData,
    threads: &[ThreadInfo],
    force_llm: bool,
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
        if !force_llm {
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
                continue;
            }
        }
        llm_jobs.push(t.clone());
    }

    metrics.llm_jobs = llm_jobs.len();
    Ok((grouped, processed_ids, llm_jobs, metrics))
}

#[allow(clippy::too_many_arguments)]
fn run_llm_classify<D: AppDeps>(
    deps: &D,
    cache: &mut CacheData,
    cache_file: &str,
    llm_config: &LlmConfig,
    effective_workers: usize,
    llm_pool: &mut Option<ThreadPool>,
    llm_jobs: Vec<ThreadInfo>,
    grouped: &mut HashMap<String, Vec<String>>,
    processed_ids: &mut Vec<String>,
) -> std::result::Result<(usize, usize, Vec<ThreadInfo>), DynErr> {
    if llm_jobs.is_empty() {
        return Ok((0, 0, Vec::new()));
    }

    log(&format!(
        "CACHE_MISS: {} threads, calling LLM with {} workers...",
        llm_jobs.len(),
        effective_workers
    ));

    let started = Instant::now();
    let total = llm_jobs.len();

    let results = if effective_workers <= 1 {
        llm_jobs
            .into_iter()
            .enumerate()
            .map(|(i, job)| {
                let res =
                    deps.llm_classify_email(&job.sender, &job.subject, &job.snippet, llm_config);
                let done = i + 1;
                if done % 50 == 0 || done == total {
                    log(&format!(
                        "LLM_PROGRESS: {done}/{total} ({:.0}%) elapsed={}s",
                        done as f64 / total as f64 * 100.0,
                        started.elapsed().as_secs()
                    ));
                }
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
        let counter = AtomicUsize::new(0);
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
                    let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if done % 50 == 0 || done == total {
                        log(&format!(
                            "LLM_PROGRESS: {done}/{total} ({:.0}%) elapsed={}s",
                            done as f64 / total as f64 * 100.0,
                            started.elapsed().as_secs()
                        ));
                    }
                    (job, res)
                })
                .collect::<Vec<_>>()
        })
    };

    let total_results = results.len();
    let mut llm_setup_failures = 0usize;
    let mut llm_success = 0usize;
    let mut llm_failures = 0usize;
    let mut others_entries: Vec<ThreadInfo> = Vec::new();

    for (n, (job, result)) in results.into_iter().enumerate() {
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
        if label == "Others" {
            others_entries.push(job.clone());
        }
        processed_ids.push(job.id.clone());
        log(&format!(
            "CLASSIFY: thread={} label={} source={} summary={}",
            job.id, label, source, summary
        ));

        let done = n + 1;
        if done % 50 == 0 || done == total_results {
            if let Err(e) = save_cache(cache_file, cache) {
                log(&format!("CACHE_SAVE_WARN: periodic save failed: {e}"));
            }
        }
    }

    if total_results > 0 && llm_setup_failures == total_results {
        return Err(AppError::Config(
            "LLM API unavailable: all classification requests failed (network / API key / timeout). Check --api-key and network connectivity."
                .to_string(),
        ));
    }

    Ok((llm_success, llm_failures, others_entries))
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

#[allow(clippy::too_many_arguments)]
fn process_once_with_deps<D: AppDeps>(
    deps: &D,
    args: &Args,
    cache: &mut CacheData,
    effective_workers: usize,
    llm_pool: &mut Option<ThreadPool>,
    llm_config: &LlmConfig,
    imap_config: Option<&SyncConfig>,
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
        collect_threads(cache, &pending_threads, args.force_llm)?;
    let (llm_success, llm_failures, others_entries) = run_llm_classify(
        deps,
        cache,
        &args.cache_file,
        llm_config,
        effective_workers,
        llm_pool,
        llm_jobs,
        &mut grouped,
        &mut processed_ids,
    )?;
    metrics.llm_success = llm_success;
    metrics.llm_failures = llm_failures;

    if !llm_consolidate_labels(cache, args.max_labels, llm_config) {
        compress_labels_if_needed(cache, args.max_labels, &args.merged_label);
    }
    let mut grouped = regroup_by_alias(grouped, cache);

    if !others_entries.is_empty() {
        eprintln!("\n>>> Re-classifying {} Others entries with refined prompt...", others_entries.len());
        let others_ids: std::collections::HashSet<&str> =
            others_entries.iter().map(|j| j.id.as_str()).collect();
        if let Some(others_group) = grouped.get_mut("Others") {
            others_group.retain(|id| !others_ids.contains(id.as_str()));
            if others_group.is_empty() {
                grouped.remove("Others");
            }
        }

        struct RefineDeps;
        impl AppDeps for RefineDeps {
            fn llm_classify_email(
                &self,
                _sender: &str,
                subject: &str,
                snippet: &str,
                llm_config: &LlmConfig,
            ) -> LlmClassify {
                llm_classify_refine(subject, snippet, llm_config)
            }
        }

        run_llm_classify(
            &RefineDeps,
            cache,
            &args.cache_file,
            llm_config,
            effective_workers,
            llm_pool,
            others_entries,
            &mut grouped,
            &mut processed_ids,
        )?;
    }

    let total: usize = grouped.values().map(Vec::len).sum();
    metrics.labeled_threads = total;
    let mut keys: Vec<String> = grouped.keys().cloned().collect();
    keys.sort();
    let summary = keys
        .iter()
        .filter_map(|k| grouped.get(k).map(|ids| (k, ids.len())))
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| format!("{}:{}", k, n))
        .collect::<Vec<_>>()
        .join(" | ");
    log(&format!("CLASSIFY_DONE: total={} | {}", total, summary));

    if let Some(imap_cfg) = imap_config {
        if args.confirm {
            eprintln!("\n=== Classification Summary ===");
            for k in &keys {
                if let Some(ids) = grouped.get(k) {
                    if !ids.is_empty() {
                        eprintln!("  {k}: {} threads", ids.len());
                    }
                }
            }
            eprintln!("  Total: {total} threads\n");
            eprint!("Apply these labels? [y/N] ");
            use std::io::BufRead;
            let stdin = std::io::stdin();
            let input = stdin.lock().lines().next().and_then(|r| r.ok()).unwrap_or_default();
            if !input.trim().eq_ignore_ascii_case("y") {
                log("CONFIRM_SKIP: user declined, labels not applied");
                return Ok(("classified".to_string(), metrics));
            }
        }
        apply_labels_via_imap(imap_cfg, &grouped, cache)?;
    }

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

    if args.force_llm {
        let memo_count = cache.memos.len();
        cache.memos.clear();
        cache.consolidation_fingerprint.clear();
        cache.consolidation_mapping.clear();
        log(&format!(
            "FORCE_LLM: cleared {} memos and consolidation cache, all threads go to LLM",
            memo_count
        ));
    }

    let last_saved_fingerprint = cache_fingerprint(&cache)?;

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
            cache_file: args.cache_file.clone(),
        };
        run_sync(&sync_cfg, &mut cache, &llm_config, args.restore_spam)?;
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

    // IMAP credentials required for label application (gog/Gmail API removed)
    let imap_config = if args.from_cache {
        let imap_user = args
            .imap_user
            .clone()
            .ok_or_else(|| AppError::Config("--imap-user is required with --from-cache (gog removed)".to_string()))?;
        let imap_pass = args
            .imap_pass
            .clone()
            .ok_or_else(|| AppError::Config("--imap-pass is required with --from-cache (gog removed)".to_string()))?;
        Some(SyncConfig {
            imap_user,
            imap_pass,
            imap_host: args.imap_host.clone(),
            imap_port: args.imap_port,
            max_messages: 0,
            cache_file: String::new(),
        })
    } else {
        None
    };

    let round_started = Instant::now();
    let (state, metrics) = match process_once_with_deps(
        &RealDeps,
        &args,
        &mut cache,
        effective_workers,
        &mut llm_pool,
        &llm_config,
        imap_config.as_ref(),
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
    if state == "processed" && current_fingerprint != last_saved_fingerprint {
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
                uids: vec![],
                ts: now_ts(),
            },
        );
    }

    struct MockDeps {
        llm_result: LlmClassify,
        llm_calls: Mutex<usize>,
    }

    impl AppDeps for MockDeps {
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
            confirm: false,
            force_llm: false,
            restore_spam: false,
        }
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

        let mut llm_pool: Option<ThreadPool> = None;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            1,
            &mut llm_pool,
            &test_llm_config(),
            None,
        )
        .expect("process_once should succeed");
        assert_eq!(state, "processed");
        assert_eq!(metrics.total_threads, 1);
        assert_eq!(metrics.llm_jobs, 1);
        assert_eq!(metrics.llm_success, 1);
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
            llm_calls: Mutex::new(0),
        };
        let args = make_args();

        let mut llm_pool: Option<ThreadPool> = None;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            1,
            &mut llm_pool,
            &test_llm_config(),
            None,
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
            llm_calls: Mutex::new(0),
        };
        let args = make_args();
        let mut cache = CacheData::default();
        add_synced(&mut cache, "t-fail", "x@example.com", "unknown", "unknown");

        let mut llm_pool: Option<ThreadPool> = None;

        let err = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            1,
            &mut llm_pool,
            &test_llm_config(),
            None,
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


        let mut llm_pool: Option<ThreadPool> = None;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &mut cache,
            1,
            &mut llm_pool,
            &test_llm_config(),
            None,
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.llm_success, 1);
    }

}
