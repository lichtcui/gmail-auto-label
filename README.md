# gmail-auto-label (Rust)

[![Documentation](https://img.shields.io/badge/docs-docs.rs-blue)](https://docs.rs/crate/gmail-auto-label/latest)
[![License](https://img.shields.io/github/license/lichtcui/gmail-auto-label)](LICENSE)
[![Crates.io](https://img.shields.io/badge/crates.io-coming_soon-informational)](https://crates.io/crates/gmail-auto-label)
[![Crates.io](https://img.shields.io/badge/downloads-n%2Fa-lightgrey)](https://crates.io/crates/gmail-auto-label)

An automatic Gmail labeling tool built with `gog` + `codex` (Rust version).  
This is the primary documentation.

🌐 Languages: [🇺🇸 English](README.md) · [🇨🇳 简体中文](README_ZH.md)

## Features

- Auto-scan inbox threads and classify emails into business-friendly labels
- Cache-first classification (memo + reusable rules) to reduce repeated LLM calls
- Codex fallback for uncached emails, then persist extracted rules for later reuse
- Auto-create missing Gmail labels and apply labels in batches
- Optional archive step (remove `INBOX`) after labeling, with `--keep-inbox` support
- Label compression when active labels exceed limit (`--max-labels` + `--merged-label`)
- Gmail rate-limit handling with automatic retry and exponential backoff
- Dry-run mode for safe preview without write operations

## Prerequisites

- `gog` is installed and authenticated
- `codex` CLI is installed (default command: `codex exec`)
- Rust toolchain is installed

## gog Setup

1. Install `gog` for your operating system.
2. Sign in:

```bash
gog auth login
```

3. Verify Gmail access:

```bash
gog gmail labels list --no-input --json
```

4. List local accounts (to confirm account names):

```bash
gog auth list
```

5. For multiple accounts, pass account name when running this tool:

```bash
cargo run -- --account your-account-name
```

Note: all Gmail operations are executed through `gog`. If auth or permissions are missing, the tool will fail at runtime.

### gog Troubleshooting

1. Check current auth/session status:

```bash
gog auth status
```

2. Quick Gmail API read test:

```bash
gog gmail search "in:inbox" --max 1 --no-input --json
```

3. Re-login if token/permission is invalid:

```bash
gog auth login
```

## Build

```bash
cargo build --release
```

Binary:

```bash
./target/release/gmail-auto-label
```

## Install from crates.io (after publish)

Once this crate is published, install with:

```bash
cargo install gmail-auto-label
```

Optional (pin a version):

```bash
cargo install gmail-auto-label --version 0.1.1
```

## Common Usage

1. Single pass (default: 20 threads):

```bash
cargo run -- --limit 20
```

2. Dry run (no write operations):

```bash
cargo run -- --dry-run --limit 20
```

3. Loop mode (every 5 minutes):

```bash
cargo run -- --loop --interval 300
```

4. Keep messages in inbox (label only, no archive):

```bash
cargo run -- --keep-inbox
```

## Key Options

- `--limit`: max threads per run, default `20`
- `--loop`: keep running until no pending threads
- `--interval`: loop sleep seconds, default `300`
- `--dry-run`: print actions only, no writes
- `--codex-cmd`: custom Codex command prefix, default `"codex exec"`
- `--cache-file`: cache file path, default `/tmp/gmail_auto_label_codex_cache.json`
- `--cache-ttl-hours`: cache TTL in hours, default `336`
- `--cache-max-rules`: max cached rules, default `500`
- `--cache-max-memos`: max cached memos, default `5000`
- `--max-labels`: max active labels, default `10`
- `--merged-label`: merge target when labels exceed limit, default `其他通知`
- `--codex-workers`: Codex concurrency, `0` means auto
- `--keep-inbox`: do not remove processed threads from inbox
- `--gmail-batch-size`: Gmail modify batch size, default `100`
- `--gmail-batch-retries`: retries per Gmail write batch, default `2`
- `--gmail-batch-retry-backoff-secs`: backoff seconds between batch retries, default `1`
- `--feedback-file`: feedback file path, default `/tmp/gmail_auto_label_feedback.json`
- `--feedback-bad-threshold`: drop rule when bad feedback count reaches this threshold, default `3`
- `--feedback-hit-penalty`: reduce rule hits per bad feedback event, default `2`
- `--feedback-max-age-hours`: max accepted age of feedback events, default `336`

Feedback file format (`--feedback-file`):

```json
[
  {
    "event_id": "evt-20260318-001",
    "rule_id": "rule_sha256_id",
    "verdict": "bad",
    "ts": 1773800000
  }
]
```

Notes:
- `event_id` must be unique; duplicated/replayed events are skipped.
- `ts` uses Unix seconds; stale events older than `--feedback-max-age-hours` are skipped.

## Help

```bash
cargo run -- --help
```
