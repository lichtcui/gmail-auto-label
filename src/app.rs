use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use rayon::ThreadPool;
use rayon::prelude::*;

use crate::cache::{
    apply_feedback_from_file, cache_fingerprint, load_cache, load_custom_label_rules, memo_key,
    prune_cache, save_cache,
};
use crate::classify::{
    build_rule_priority_indexes, classify_from_cache_with_indexes, classify_with_codex_result,
    classify_with_custom_rules, codex_analyze_email, codex_error_hint, compress_labels_if_needed,
    ensure_codex_command_available,
};
use crate::errors::AppError;
use crate::gog::{
    GmailWriteOptions, apply_labels_with_options, ensure_label, fetch_pending,
    set_watch_no_retry_mode,
};
use crate::models::{
    Args, CacheData, CodexClassify, CustomLabelRule, DEFAULT_CACHE_MAX_MEMOS,
    DEFAULT_CACHE_MAX_RULES, DEFAULT_CACHE_TTL_HOURS, DEFAULT_FEEDBACK_BAD_THRESHOLD,
    DEFAULT_FEEDBACK_FILE, DEFAULT_FEEDBACK_HIT_PENALTY, DEFAULT_FEEDBACK_MAX_AGE_HOURS,
    ProcessedThread, ThreadInfo,
};
use crate::utils::{auto_codex_workers, log, now_ts, resolve_label_alias};

type DynErr = AppError;
const MAX_WATCH_IDLE_BACKOFF_MULTIPLIER: u64 = 8;

#[derive(Debug, Default, Clone)]
struct RoundMetrics {
    total_threads: usize,
    cache_hits: usize,
    codex_jobs: usize,
    codex_success: usize,
    codex_failures: usize,
}

#[derive(Debug, Clone)]
struct AdaptiveWriteTuner {
    current: GmailWriteOptions,
    baseline: GmailWriteOptions,
    consecutive_success: u32,
    no_retry_mode: bool,
}

impl AdaptiveWriteTuner {
    fn new(baseline: GmailWriteOptions, no_retry_mode: bool) -> Self {
        Self {
            current: baseline,
            baseline,
            consecutive_success: 0,
            no_retry_mode,
        }
    }

    fn on_rate_limit(&mut self) {
        self.consecutive_success = 0;
        self.current.batch_size = std::cmp::max(10, self.current.batch_size / 2);
        if !self.no_retry_mode {
            self.current.batch_retries = std::cmp::min(self.current.batch_retries + 1, 6);
        }
        self.current.batch_retry_backoff_secs =
            std::cmp::min(self.current.batch_retry_backoff_secs.saturating_mul(2), 30);
    }

    fn on_success(&mut self) {
        self.consecutive_success += 1;
        if self.consecutive_success < 3 {
            return;
        }
        self.consecutive_success = 0;
        if self.current.batch_size < self.baseline.batch_size {
            self.current.batch_size =
                std::cmp::min(self.current.batch_size + 10, self.baseline.batch_size);
        }
        if !self.no_retry_mode && self.current.batch_retries > self.baseline.batch_retries {
            self.current.batch_retries -= 1;
        }
        if self.current.batch_retry_backoff_secs > self.baseline.batch_retry_backoff_secs {
            self.current.batch_retry_backoff_secs -= 1;
        }
    }
}

trait AppDeps: Sync {
    fn fetch_pending(
        &self,
        limit: usize,
        account: &Option<String>,
    ) -> std::result::Result<Vec<ThreadInfo>, DynErr>;
    fn ensure_label(
        &self,
        label: &str,
        existing_labels: &mut HashSet<String>,
        account: &Option<String>,
        dry_run: bool,
    ) -> std::result::Result<(), DynErr>;
    fn codex_analyze_email(
        &self,
        sender: &str,
        subject: &str,
        snippet: &str,
        codex_cmd: &str,
    ) -> CodexClassify;
    fn ensure_codex_ready(&self, codex_cmd: &str) -> std::result::Result<(), DynErr>;
    fn apply_labels(
        &self,
        grouped: &HashMap<String, Vec<String>>,
        account: &Option<String>,
        dry_run: bool,
        remove_inbox: bool,
        write_options: GmailWriteOptions,
    ) -> std::result::Result<(), DynErr>;
}

struct RealDeps;

impl AppDeps for RealDeps {
    fn fetch_pending(
        &self,
        limit: usize,
        account: &Option<String>,
    ) -> std::result::Result<Vec<ThreadInfo>, DynErr> {
        fetch_pending(limit, account)
    }

    fn ensure_label(
        &self,
        label: &str,
        existing_labels: &mut HashSet<String>,
        account: &Option<String>,
        dry_run: bool,
    ) -> std::result::Result<(), DynErr> {
        ensure_label(label, existing_labels, account, dry_run)
    }

    fn codex_analyze_email(
        &self,
        sender: &str,
        subject: &str,
        snippet: &str,
        codex_cmd: &str,
    ) -> CodexClassify {
        codex_analyze_email(sender, subject, snippet, codex_cmd)
    }

    fn ensure_codex_ready(&self, codex_cmd: &str) -> std::result::Result<(), DynErr> {
        ensure_codex_command_available(codex_cmd)
            .map_err(|e| build_codex_setup_error(&e.to_string(), codex_cmd))
    }

    fn apply_labels(
        &self,
        grouped: &HashMap<String, Vec<String>>,
        account: &Option<String>,
        dry_run: bool,
        remove_inbox: bool,
        write_options: GmailWriteOptions,
    ) -> std::result::Result<(), DynErr> {
        apply_labels_with_options(grouped, account, dry_run, remove_inbox, write_options)
    }
}

#[allow(clippy::type_complexity)]
fn collect_threads(
    custom_rules: &[CustomLabelRule],
    cache: &mut CacheData,
    threads: &[ThreadInfo],
) -> std::result::Result<
    (
        HashMap<String, Vec<String>>,
        Vec<String>,
        Vec<ThreadInfo>,
        RoundMetrics,
        HashSet<String>,
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
    let mut codex_jobs: Vec<ThreadInfo> = Vec::new();
    let mut custom_labels = HashSet::new();

    for t in threads {
        if let Some((label, source)) =
            classify_with_custom_rules(&t.sender, &t.subject, &t.snippet, custom_rules)
        {
            custom_labels.insert(label.clone());
            grouped.entry(label.clone()).or_default().push(t.id.clone());
            processed_ids.push(t.id.clone());
            log(&format!(
                "CLASSIFY: thread={} label={} source={} summary=custom_rule_hit",
                t.id, label, source
            ));
        } else if let Some((label, source)) = classify_from_cache_with_indexes(
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
            codex_jobs.push(t.clone());
        }
    }

    metrics.codex_jobs = codex_jobs.len();
    Ok((grouped, processed_ids, codex_jobs, metrics, custom_labels))
}

#[allow(clippy::too_many_arguments)]
fn run_codex_for_jobs<D: AppDeps>(
    deps: &D,
    cache: &mut CacheData,
    codex_cmd: &str,
    effective_workers: usize,
    codex_pool: &mut Option<ThreadPool>,
    codex_jobs: Vec<ThreadInfo>,
    grouped: &mut HashMap<String, Vec<String>>,
    processed_ids: &mut Vec<String>,
) -> std::result::Result<(usize, usize), DynErr> {
    if codex_jobs.is_empty() {
        return Ok((0, 0));
    }

    log(&format!(
        "CACHE_MISS: {} threads, calling Codex with {} workers...",
        codex_jobs.len(),
        effective_workers
    ));

    let results = if effective_workers <= 1 {
        codex_jobs
            .into_iter()
            .map(|job| {
                let res =
                    deps.codex_analyze_email(&job.sender, &job.subject, &job.snippet, codex_cmd);
                (job, res)
            })
            .collect::<Vec<_>>()
    } else {
        if codex_pool.is_none() {
            *codex_pool = Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(effective_workers)
                    .build()
                    .context("Failed to create Codex thread pool")?,
            );
        }
        let pool = codex_pool
            .as_ref()
            .context("Codex thread pool is not initialized")?;
        pool.install(|| {
            codex_jobs
                .into_par_iter()
                .map(|job| {
                    let res = deps.codex_analyze_email(
                        &job.sender,
                        &job.subject,
                        &job.snippet,
                        codex_cmd,
                    );
                    (job, res)
                })
                .collect::<Vec<_>>()
        })
    };

    let total_results = results.len();
    let mut codex_setup_failures = 0usize;
    let mut codex_success = 0usize;
    let mut codex_failures = 0usize;

    for (job, result) in results {
        let (label, source, summary) =
            classify_with_codex_result(&job.sender, &job.subject, &job.snippet, cache, &result);
        if source == "codex:error" && label == "uncategorized" {
            let hint = codex_error_hint(&summary)
                .unwrap_or("Please check the Codex environment and retry.");
            if matches!(summary.as_str(), "codex_not_found" | "codex_non_zero_exit") {
                codex_setup_failures += 1;
            }
            log(&format!(
                "CLASSIFY_FAILED: thread={} reason={}. {} Skipped labeling and will retry next round.",
                job.id, summary, hint
            ));
            codex_failures += 1;
            continue;
        }
        codex_success += 1;
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

    if total_results > 0 && codex_setup_failures == total_results {
        return Err(AppError::Config(
            "Codex configuration is unavailable: all cache-miss emails in this round failed due to Codex setup issues. Please fix Codex and retry."
                .to_string(),
        ));
    }

    Ok((codex_success, codex_failures))
}

fn regroup_by_alias(
    grouped: HashMap<String, Vec<String>>,
    cache: &CacheData,
    custom_labels: &HashSet<String>,
) -> HashMap<String, Vec<String>> {
    if cache.label_aliases.is_empty() {
        return grouped;
    }
    let mut regrouped: HashMap<String, Vec<String>> = HashMap::new();
    for (label, ids) in grouped {
        let final_label = if custom_labels.contains(&label) {
            label
        } else {
            resolve_label_alias(&label, cache)
        };
        regrouped.entry(final_label).or_default().extend(ids);
    }
    regrouped
}

fn ensure_grouped_labels_exist<D: AppDeps>(
    deps: &D,
    grouped: &HashMap<String, Vec<String>>,
    args: &Args,
    existing_labels: &mut HashSet<String>,
) -> std::result::Result<(), DynErr> {
    let mut labels = grouped
        .iter()
        .filter(|(_, ids)| !ids.is_empty())
        .map(|(label, _)| label.as_str())
        .collect::<Vec<_>>();
    labels.sort_unstable();

    for label in labels {
        deps.ensure_label(label, existing_labels, &args.account, args.dry_run)?;
    }

    Ok(())
}

fn seed_existing_labels(
    cache: &CacheData,
    custom_rules: &[CustomLabelRule],
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
    for rule in custom_rules {
        let label = rule.label.trim();
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

fn filter_recently_processed_keep_inbox_threads(
    args: &Args,
    cache: &CacheData,
    threads: Vec<ThreadInfo>,
) -> Vec<ThreadInfo> {
    if !args.keep_inbox {
        return threads;
    }

    let now = now_ts();
    let ttl_seconds = DEFAULT_CACHE_TTL_HOURS * 3600;
    let mut skipped = 0usize;
    let mut filtered = Vec::with_capacity(threads.len());

    for thread in threads {
        let content_key = memo_key(&thread.sender, &thread.subject, &thread.snippet);
        let should_skip = cache
            .processed_threads
            .get(&thread.id)
            .is_some_and(|entry| {
                now.saturating_sub(entry.ts) <= ttl_seconds && entry.content_key == content_key
            });
        if should_skip {
            skipped += 1;
            continue;
        }
        filtered.push(thread);
    }

    if skipped > 0 {
        log(&format!(
            "KEEP_INBOX_SKIP: skipped {} processed threads with unchanged content",
            skipped
        ));
    }

    filtered
}

fn remember_processed_keep_inbox_threads(
    cache: &mut CacheData,
    threads: &[ThreadInfo],
    processed_ids: &[String],
) {
    if processed_ids.is_empty() {
        return;
    }

    let processed = processed_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let ts = now_ts();

    for thread in threads {
        if !processed.contains(thread.id.as_str()) {
            continue;
        }
        cache.processed_threads.insert(
            thread.id.clone(),
            ProcessedThread {
                content_key: memo_key(&thread.sender, &thread.subject, &thread.snippet),
                ts,
            },
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn process_once_with_deps<D: AppDeps>(
    deps: &D,
    args: &Args,
    custom_rules: &[CustomLabelRule],
    cache: &mut CacheData,
    existing_labels: &mut HashSet<String>,
    effective_workers: usize,
    codex_pool: &mut Option<ThreadPool>,
    codex_checked: &mut bool,
    write_options: GmailWriteOptions,
) -> std::result::Result<(String, RoundMetrics), DynErr> {
    let pending_threads = deps.fetch_pending(args.limit, &args.account)?;
    if pending_threads.is_empty() {
        log("DONE_NO_PENDING: no pending emails, run finished.");
        return Ok(("done".to_string(), RoundMetrics::default()));
    }
    let threads = filter_recently_processed_keep_inbox_threads(args, cache, pending_threads);
    if threads.is_empty() {
        log(
            "IDLE_NO_ELIGIBLE: only already-processed unchanged threads matched; waiting for next round.",
        );
        return Ok(("idle".to_string(), RoundMetrics::default()));
    }

    let (mut grouped, mut processed_ids, codex_jobs, mut metrics, custom_labels) =
        collect_threads(custom_rules, cache, &threads)?;
    if !codex_jobs.is_empty() && !*codex_checked {
        deps.ensure_codex_ready(&args.codex_cmd)?;
        *codex_checked = true;
    }
    let (codex_success, codex_failures) = run_codex_for_jobs(
        deps,
        cache,
        &args.codex_cmd,
        effective_workers,
        codex_pool,
        codex_jobs,
        &mut grouped,
        &mut processed_ids,
    )?;
    metrics.codex_success = codex_success;
    metrics.codex_failures = codex_failures;

    compress_labels_if_needed(cache, args.max_labels, &args.merged_label);
    let grouped = regroup_by_alias(grouped, cache, &custom_labels);
    ensure_grouped_labels_exist(deps, &grouped, args, existing_labels)?;
    deps.apply_labels(
        &grouped,
        &args.account,
        args.dry_run,
        !args.keep_inbox,
        write_options,
    )?;
    if args.keep_inbox {
        if !args.dry_run {
            remember_processed_keep_inbox_threads(cache, &threads, &processed_ids);
        }
    }

    let total: usize = grouped.values().map(Vec::len).sum();
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

fn validate_args(args: &Args) -> Result<()> {
    if args.limit == 0 {
        bail!("--limit must be greater than 0");
    }
    if args.watch == Some(0) {
        bail!("--watch must be greater than 0");
    }
    if args.r#loop && args.interval == 0 {
        bail!("--interval must be greater than 0");
    }
    if args.max_labels < 2 {
        bail!("--max-labels must be at least 2");
    }
    Ok(())
}

fn should_continue_after_round(args: &Args, _state: &str) -> bool {
    args.watch_interval_secs().is_some()
}

fn compute_watch_sleep_secs(base_secs: u64, state: &str, idle_rounds: &mut u32) -> u64 {
    if state == "processed" {
        *idle_rounds = 0;
        return base_secs;
    }

    if state == "idle" || state == "done" {
        *idle_rounds = idle_rounds.saturating_add(1);
        let multiplier = std::cmp::min(
            1u64 << std::cmp::min(*idle_rounds, 3),
            MAX_WATCH_IDLE_BACKOFF_MULTIPLIER,
        );
        return base_secs.saturating_mul(multiplier);
    }

    *idle_rounds = 0;
    base_secs
}

pub(crate) fn build_codex_setup_error(raw_error: &str, codex_cmd: &str) -> AppError {
    AppError::Config(format!(
        "Codex configuration check failed.\n\
         Raw error: {raw_error}\n\
         Current --codex-cmd: `{codex_cmd}`\n\n\
         Suggested checks:\n\
         1. Ensure `codex --help` works, or run `--help` for your custom command\n\
         2. If the command is missing, install it/fix PATH, or set a valid command via `--codex-cmd`\n\
         3. If login is required, complete login first and rerun this tool"
    ))
}

pub(crate) fn build_custom_labels_setup_error(raw_error: &str, path: &str) -> AppError {
    AppError::Config(format!(
        "Custom label configuration check failed.\n\
         Raw error: {raw_error}\n\
         Current --custom-labels-file: `{path}`\n\n\
         Suggested checks:\n\
         1. Ensure the file exists and is readable\n\
         2. Ensure the file content is a JSON array\n\
         3. Each rule must include a non-empty `label`\n\
         4. Each rule must provide at least one `include_keywords` item\n\
         5. Example: [{{\"label\":\"Important Client\",\"include_keywords\":[\"vip\",\"invoice\"],\"exclude_keywords\":[\"spam\"]}}]"
    ))
}

pub(crate) fn run() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let custom_label_rules = match &args.custom_labels_file {
        Some(path) => load_custom_label_rules(path)
            .map_err(|e| anyhow!(build_custom_labels_setup_error(&e.to_string(), path)))?,
        None => Vec::new(),
    };
    if !custom_label_rules.is_empty() {
        log(&format!(
            "Loaded {} custom label rules",
            custom_label_rules.len()
        ));
    }

    let effective_workers = auto_codex_workers(args.limit);
    let watch_mode = args.watch_interval_secs().is_some();
    set_watch_no_retry_mode(watch_mode);
    log(&format!("Codex worker count: {}", effective_workers));
    let mut codex_pool: Option<ThreadPool> = None;
    let mut codex_checked = false;
    log(
        "Cold-start optimization enabled: Codex precheck and lazy thread pool init (triggered only on cache miss)",
    );
    let mut baseline_write_options = GmailWriteOptions::default();
    if watch_mode {
        baseline_write_options.batch_retries = 0;
    }
    let mut write_tuner = AdaptiveWriteTuner::new(baseline_write_options, watch_mode);

    let mut cache = load_cache(&args.cache_file);
    prune_cache(
        &mut cache,
        DEFAULT_CACHE_MAX_RULES,
        DEFAULT_CACHE_MAX_MEMOS,
        DEFAULT_CACHE_TTL_HOURS,
    );
    let mut last_saved_fingerprint = cache_fingerprint(&cache)?;

    // Avoid startup `labels list` call to reduce Gmail API usage.
    // Seed from local known labels, then create on demand when needed.
    let mut existing_labels = seed_existing_labels(&cache, &custom_label_rules, &args.merged_label);
    let mut idle_rounds: u32 = 0;

    loop {
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
            &custom_label_rules,
            &mut cache,
            &mut existing_labels,
            effective_workers,
            &mut codex_pool,
            &mut codex_checked,
            write_tuner.current,
        ) {
            Ok(v) => v,
            Err(e) => match e {
                AppError::RateLimit(msg) => {
                    log(&format!(
                        "RATE_LIMIT: skipped this round, waiting for next round. details: {}",
                        msg
                    ));
                    write_tuner.on_rate_limit();
                    log(&format!(
                        "ADAPTIVE_TUNING: write params adjusted after rate limit batch_size={} retries={} backoff_secs={}",
                        write_tuner.current.batch_size,
                        write_tuner.current.batch_retries,
                        write_tuner.current.batch_retry_backoff_secs
                    ));
                    ("rate_limit".to_string(), RoundMetrics::default())
                }
                AppError::Config(msg) => {
                    return Err(anyhow!(msg));
                }
                other => {
                    log(&format!("ERROR: {}", other));
                    ("error".to_string(), RoundMetrics::default())
                }
            },
        };
        if state == "processed" {
            write_tuner.on_success();
            log(&format!(
                "ROUND_METRICS: total={} cache_hits={} codex_jobs={} codex_success={} codex_failures={} elapsed_ms={}",
                metrics.total_threads,
                metrics.cache_hits,
                metrics.codex_jobs,
                metrics.codex_success,
                metrics.codex_failures,
                round_started.elapsed().as_millis()
            ));
            log(&format!(
                "ADAPTIVE_TUNING: current write params batch_size={} retries={} backoff_secs={}",
                write_tuner.current.batch_size,
                write_tuner.current.batch_retries,
                write_tuner.current.batch_retry_backoff_secs
            ));
        }

        prune_cache(
            &mut cache,
            DEFAULT_CACHE_MAX_RULES,
            DEFAULT_CACHE_MAX_MEMOS,
            DEFAULT_CACHE_TTL_HOURS,
        );
        let current_fingerprint = cache_fingerprint(&cache)?;
        if current_fingerprint != last_saved_fingerprint {
            save_cache(&args.cache_file, &cache)?;
            last_saved_fingerprint = current_fingerprint;
        }

        if !should_continue_after_round(&args, &state) {
            break;
        }
        let base_interval_secs = args
            .watch_interval_secs()
            .expect("watch interval should exist when continuing");
        let interval_secs = compute_watch_sleep_secs(base_interval_secs, &state, &mut idle_rounds);

        log(&format!(
            "Sleeping {} seconds before next round... (base={} state={} idle_rounds={})",
            interval_secs, base_interval_secs, state, idle_rounds
        ));
        thread::sleep(Duration::from_secs(interval_secs));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::cache::memo_key;
    use crate::classify::{codex_error_hint, rule_matches};
    use crate::models::{
        CustomLabelRule, DEFAULT_CACHE_FILE, DEFAULT_MAX_ACTIVE_LABELS, DEFAULT_MERGED_LABEL, Rule,
        RuleInput,
    };
    use crate::utils::{normalize_label, now_ts};

    struct MockDeps {
        pending: Vec<ThreadInfo>,
        codex_result: CodexClassify,
        applied: Mutex<Vec<HashMap<String, Vec<String>>>>,
        remove_inbox_flags: Mutex<Vec<bool>>,
        codex_calls: Mutex<usize>,
        codex_ready_calls: Mutex<usize>,
    }

    impl AppDeps for MockDeps {
        fn fetch_pending(
            &self,
            _limit: usize,
            _account: &Option<String>,
        ) -> std::result::Result<Vec<ThreadInfo>, DynErr> {
            Ok(self.pending.clone())
        }

        fn ensure_label(
            &self,
            label: &str,
            existing_labels: &mut HashSet<String>,
            _account: &Option<String>,
            _dry_run: bool,
        ) -> std::result::Result<(), DynErr> {
            existing_labels.insert(label.to_string());
            Ok(())
        }

        fn codex_analyze_email(
            &self,
            _sender: &str,
            _subject: &str,
            _snippet: &str,
            _codex_cmd: &str,
        ) -> CodexClassify {
            let mut calls = self.codex_calls.lock().expect("lock poisoned");
            *calls += 1;
            self.codex_result.clone()
        }

        fn ensure_codex_ready(&self, _codex_cmd: &str) -> std::result::Result<(), DynErr> {
            let mut calls = self.codex_ready_calls.lock().expect("lock poisoned");
            *calls += 1;
            Ok(())
        }

        fn apply_labels(
            &self,
            grouped: &HashMap<String, Vec<String>>,
            _account: &Option<String>,
            _dry_run: bool,
            remove_inbox: bool,
            _write_options: GmailWriteOptions,
        ) -> std::result::Result<(), DynErr> {
            self.applied
                .lock()
                .expect("lock poisoned")
                .push(grouped.clone());
            self.remove_inbox_flags
                .lock()
                .expect("lock poisoned")
                .push(remove_inbox);
            Ok(())
        }
    }

    fn make_args() -> Args {
        Args {
            limit: 20,
            watch: None,
            interval: 300,
            r#loop: false,
            account: None,
            dry_run: false,
            codex_cmd: "codex exec".to_string(),
            cache_file: DEFAULT_CACHE_FILE.to_string(),
            custom_labels_file: None,
            max_labels: DEFAULT_MAX_ACTIVE_LABELS,
            merged_label: DEFAULT_MERGED_LABEL.to_string(),
            keep_inbox: false,
        }
    }

    fn default_write_options() -> GmailWriteOptions {
        GmailWriteOptions::default()
    }

    fn custom_rule(label: &str, include_keywords: &[&str]) -> CustomLabelRule {
        CustomLabelRule {
            label: label.to_string(),
            include_keywords: include_keywords.iter().map(|x| x.to_string()).collect(),
            exclude_keywords: Vec::new(),
        }
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
    fn test_codex_setup_error_contains_fix_steps() {
        let err = build_codex_setup_error("not found", "codex exec").to_string();
        assert!(err.contains("Codex configuration check failed"));
        assert!(err.contains("codex --help"));
        assert!(err.contains("--codex-cmd"));
    }

    #[test]
    fn test_custom_labels_setup_error_contains_fix_steps() {
        let err =
            build_custom_labels_setup_error("invalid json", "/tmp/custom-labels.json").to_string();
        assert!(err.contains("Custom label configuration check failed"));
        assert!(err.contains("--custom-labels-file"));
        assert!(err.contains("JSON array"));
    }

    #[test]
    fn test_codex_error_hint_for_not_found() {
        let hint = codex_error_hint("codex_not_found").unwrap_or("");
        assert!(hint.contains("--codex-cmd"));
    }

    #[test]
    fn test_process_once_with_deps_no_external_commands() {
        let deps = MockDeps {
            pending: vec![ThreadInfo {
                id: "t1".to_string(),
                sender: "billing@example.com".to_string(),
                subject: "monthly invoice".to_string(),
                snippet: "invoice attached".to_string(),
            }],
            codex_result: CodexClassify {
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
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let args = make_args();
        let mut cache = CacheData::default();
        let mut existing_labels = HashSet::new();

        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;
        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &[],
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");
        assert_eq!(state, "processed");
        assert_eq!(metrics.total_threads, 1);
        assert_eq!(metrics.codex_jobs, 1);
        assert_eq!(metrics.codex_success, 1);

        let applied = deps.applied.lock().expect("lock poisoned");
        assert_eq!(applied.len(), 1);
        let grouped = &applied[0];
        let ids = grouped.get("账单").expect("expected 账单 label");
        assert_eq!(ids, &vec!["t1".to_string()]);

        let remove_inbox_flags = deps.remove_inbox_flags.lock().expect("lock poisoned");
        assert_eq!(remove_inbox_flags.len(), 1);
        assert!(remove_inbox_flags[0]);
        assert_eq!(*deps.codex_calls.lock().expect("lock poisoned"), 1);
        assert_eq!(*deps.codex_ready_calls.lock().expect("lock poisoned"), 1);
    }

    #[test]
    fn test_adaptive_write_tuner_tightens_and_recovers() {
        let base = GmailWriteOptions {
            batch_size: 100,
            batch_retries: 2,
            batch_retry_backoff_secs: 1,
        };
        let mut tuner = AdaptiveWriteTuner::new(base, false);

        tuner.on_rate_limit();
        assert_eq!(tuner.current.batch_size, 50);
        assert_eq!(tuner.current.batch_retries, 3);
        assert_eq!(tuner.current.batch_retry_backoff_secs, 2);

        tuner.on_success();
        tuner.on_success();
        tuner.on_success();
        assert_eq!(tuner.current.batch_size, 60);
        assert_eq!(tuner.current.batch_retries, 2);
        assert_eq!(tuner.current.batch_retry_backoff_secs, 1);
    }

    #[test]
    fn test_adaptive_write_tuner_watch_mode_keeps_retries_zero() {
        let base = GmailWriteOptions {
            batch_size: 100,
            batch_retries: 0,
            batch_retry_backoff_secs: 1,
        };
        let mut tuner = AdaptiveWriteTuner::new(base, true);
        tuner.on_rate_limit();
        assert_eq!(tuner.current.batch_retries, 0);
    }

    #[test]
    fn test_should_continue_after_round_respects_watch_mode() {
        let single_run = make_args();
        assert!(!should_continue_after_round(&single_run, "processed"));

        let mut watch_args = make_args();
        watch_args.watch = Some(300);
        assert!(should_continue_after_round(&watch_args, "done"));
        assert!(should_continue_after_round(&watch_args, "idle"));
    }

    #[test]
    fn test_compute_watch_sleep_secs_backoff_and_reset() {
        let base = 300u64;
        let mut idle_rounds = 0u32;
        assert_eq!(
            compute_watch_sleep_secs(base, "idle", &mut idle_rounds),
            600
        );
        assert_eq!(idle_rounds, 1);
        assert_eq!(
            compute_watch_sleep_secs(base, "done", &mut idle_rounds),
            1200
        );
        assert_eq!(idle_rounds, 2);
        assert_eq!(
            compute_watch_sleep_secs(base, "idle", &mut idle_rounds),
            2400
        );
        assert_eq!(idle_rounds, 3);
        // capped by multiplier=8
        assert_eq!(
            compute_watch_sleep_secs(base, "idle", &mut idle_rounds),
            2400
        );
        assert_eq!(idle_rounds, 4);

        assert_eq!(
            compute_watch_sleep_secs(base, "processed", &mut idle_rounds),
            base
        );
        assert_eq!(idle_rounds, 0);
    }

    #[test]
    fn test_custom_rule_takes_precedence_without_mutating_learned_state() {
        let thread = ThreadInfo {
            id: "t-custom".to_string(),
            sender: "vip@example.com".to_string(),
            subject: "invoice ready".to_string(),
            snippet: "vip invoice".to_string(),
        };
        let deps = MockDeps {
            pending: vec![thread.clone()],
            codex_result: CodexClassify {
                ok: true,
                label: "不应触发".to_string(),
                summary: "unused".to_string(),
                rule: RuleInput::default(),
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let args = make_args();
        let custom_rules = vec![custom_rule("重要客户", &["vip", "invoice"])];

        let mut cache = CacheData::default();
        let mkey = memo_key(&thread.sender, &thread.subject, &thread.snippet);
        cache.memos.insert(
            mkey.clone(),
            crate::models::Memo {
                label: "旧缓存标签".to_string(),
                rule_id: "r-old".to_string(),
                ts: now_ts(),
            },
        );
        cache.rules.push(Rule {
            id: "r-learned".to_string(),
            label: "学习标签".to_string(),
            include_keywords: vec!["invoice".to_string()],
            hits: 5,
            updated_at: now_ts(),
            ..Default::default()
        });

        let mut existing_labels = HashSet::new();
        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &custom_rules,
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.cache_hits, 0);
        assert_eq!(metrics.codex_jobs, 0);
        assert_eq!(*deps.codex_calls.lock().expect("lock poisoned"), 0);

        let applied = deps.applied.lock().expect("lock poisoned");
        let grouped = &applied[0];
        let ids = grouped.get("重要客户").expect("expected custom label");
        assert_eq!(ids, &vec!["t-custom".to_string()]);

        assert_eq!(cache.rules.len(), 1);
        assert_eq!(cache.rules[0].label, "学习标签");
        let memo = cache.memos.get(&mkey).expect("existing memo should remain");
        assert_eq!(memo.label, "旧缓存标签");
    }

    #[test]
    fn test_e2e_cache_hit_path_skips_codex() {
        let mut cache = CacheData::default();
        cache.rules.push(Rule {
            id: "r-cache".to_string(),
            label: "账单".to_string(),
            include_keywords: vec!["invoice".to_string()],
            hits: 3,
            ..Default::default()
        });
        let deps = MockDeps {
            pending: vec![ThreadInfo {
                id: "t-cache".to_string(),
                sender: "billing@example.com".to_string(),
                subject: "invoice ready".to_string(),
                snippet: "monthly invoice".to_string(),
            }],
            codex_result: CodexClassify {
                ok: true,
                label: "不应触发".to_string(),
                summary: "unused".to_string(),
                rule: RuleInput::default(),
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let args = make_args();
        let mut existing_labels = HashSet::new();
        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &[],
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.codex_jobs, 0);
        assert_eq!(*deps.codex_calls.lock().expect("lock poisoned"), 0);
        assert_eq!(*deps.codex_ready_calls.lock().expect("lock poisoned"), 0);
    }

    #[test]
    fn test_e2e_codex_failure_skips_writes() {
        let deps = MockDeps {
            pending: vec![ThreadInfo {
                id: "t-fail".to_string(),
                sender: "x@example.com".to_string(),
                subject: "unknown".to_string(),
                snippet: "unknown".to_string(),
            }],
            codex_result: CodexClassify {
                ok: false,
                label: "uncategorized".to_string(),
                summary: "codex_timeout".to_string(),
                rule: RuleInput::default(),
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let args = make_args();
        let mut cache = CacheData::default();
        let mut existing_labels = HashSet::new();
        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &[],
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.codex_jobs, 1);
        assert_eq!(metrics.codex_failures, 1);
        assert_eq!(*deps.codex_calls.lock().expect("lock poisoned"), 1);
        assert_eq!(*deps.codex_ready_calls.lock().expect("lock poisoned"), 1);
    }

    #[test]
    fn test_process_once_ensures_merged_label_before_apply() {
        let deps = MockDeps {
            pending: vec![ThreadInfo {
                id: "t-merge".to_string(),
                sender: "calendar@example.com".to_string(),
                subject: "meeting invite".to_string(),
                snippet: "team sync".to_string(),
            }],
            codex_result: CodexClassify {
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
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let mut args = make_args();
        args.max_labels = 2;
        args.merged_label = "统一收纳".to_string();

        let mut cache = CacheData::default();
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
        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &[],
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.codex_success, 1);
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

    #[test]
    fn test_custom_label_skips_alias_merge() {
        let thread = ThreadInfo {
            id: "t-custom-merge".to_string(),
            sender: "vip@example.com".to_string(),
            subject: "vip sync".to_string(),
            snippet: "vip meeting".to_string(),
        };
        let deps = MockDeps {
            pending: vec![thread.clone()],
            codex_result: CodexClassify {
                ok: true,
                label: "不应触发".to_string(),
                summary: "unused".to_string(),
                rule: RuleInput::default(),
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let mut args = make_args();
        args.max_labels = 2;
        args.merged_label = "统一收纳".to_string();
        let custom_rules = vec![custom_rule("重要客户", &["vip"])];

        let mut cache = CacheData::default();
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
        cache.rules.push(Rule {
            id: "r-custom-shadow".to_string(),
            label: "重要客户".to_string(),
            include_keywords: vec!["vip".to_string()],
            hits: 1,
            updated_at: 10,
            ..Default::default()
        });

        let mut existing_labels = HashSet::new();
        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &custom_rules,
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.codex_jobs, 0);
        assert!(existing_labels.contains("重要客户"));
        assert!(!existing_labels.contains("统一收纳"));

        let applied = deps.applied.lock().expect("lock poisoned");
        let grouped = &applied[0];
        let ids = grouped
            .get("重要客户")
            .expect("expected custom label to bypass alias merge");
        assert_eq!(ids, &vec!["t-custom-merge".to_string()]);
    }

    #[test]
    fn test_keep_inbox_skips_recently_processed_unchanged_threads() {
        let thread = ThreadInfo {
            id: "t-keep".to_string(),
            sender: "alerts@example.com".to_string(),
            subject: "weekly digest".to_string(),
            snippet: "same content".to_string(),
        };
        let deps = MockDeps {
            pending: vec![thread.clone()],
            codex_result: CodexClassify {
                ok: true,
                label: "订阅".to_string(),
                summary: "订阅邮件".to_string(),
                rule: RuleInput {
                    description: "订阅类邮件".to_string(),
                    include_keywords: vec!["digest".to_string()],
                    exclude_keywords: vec![],
                },
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let mut args = make_args();
        args.keep_inbox = true;

        let mut cache = CacheData::default();
        cache.processed_threads.insert(
            thread.id.clone(),
            ProcessedThread {
                content_key: memo_key(&thread.sender, &thread.subject, &thread.snippet),
                ts: now_ts(),
            },
        );

        let mut existing_labels = HashSet::new();
        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &[],
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "idle");
        assert_eq!(metrics.total_threads, 0);
        assert_eq!(*deps.codex_calls.lock().expect("lock poisoned"), 0);
        assert!(deps.applied.lock().expect("lock poisoned").is_empty());
        assert!(
            deps.remove_inbox_flags
                .lock()
                .expect("lock poisoned")
                .is_empty()
        );
    }

    #[test]
    fn test_keep_inbox_reprocesses_thread_when_content_changes() {
        let thread = ThreadInfo {
            id: "t-keep-change".to_string(),
            sender: "alerts@example.com".to_string(),
            subject: "weekly digest".to_string(),
            snippet: "new content".to_string(),
        };
        let deps = MockDeps {
            pending: vec![thread.clone()],
            codex_result: CodexClassify {
                ok: true,
                label: "订阅".to_string(),
                summary: "订阅邮件".to_string(),
                rule: RuleInput {
                    description: "订阅类邮件".to_string(),
                    include_keywords: vec!["digest".to_string()],
                    exclude_keywords: vec![],
                },
            },
            applied: Mutex::new(Vec::new()),
            remove_inbox_flags: Mutex::new(Vec::new()),
            codex_calls: Mutex::new(0),
            codex_ready_calls: Mutex::new(0),
        };
        let mut args = make_args();
        args.keep_inbox = true;

        let mut cache = CacheData::default();
        cache.processed_threads.insert(
            thread.id.clone(),
            ProcessedThread {
                content_key: memo_key(&thread.sender, &thread.subject, "old content"),
                ts: now_ts(),
            },
        );

        let mut existing_labels = HashSet::new();
        let mut codex_pool: Option<ThreadPool> = None;
        let mut codex_checked = false;

        let (state, metrics) = process_once_with_deps(
            &deps,
            &args,
            &[],
            &mut cache,
            &mut existing_labels,
            1,
            &mut codex_pool,
            &mut codex_checked,
            default_write_options(),
        )
        .expect("process_once should succeed");

        assert_eq!(state, "processed");
        assert_eq!(metrics.codex_success, 1);
        assert_eq!(*deps.codex_calls.lock().expect("lock poisoned"), 1);
        let remove_inbox_flags = deps.remove_inbox_flags.lock().expect("lock poisoned");
        assert_eq!(remove_inbox_flags.len(), 1);
        assert!(!remove_inbox_flags[0]);
        assert!(cache.processed_threads.contains_key("t-keep-change"));
        let saved = cache
            .processed_threads
            .get("t-keep-change")
            .expect("processed thread should be stored");
        assert_eq!(
            saved.content_key,
            memo_key(&thread.sender, &thread.subject, &thread.snippet)
        );
    }
}
