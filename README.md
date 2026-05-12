# gmail-auto-label (Rust)

[![Documentation](https://img.shields.io/badge/docs-docs.rs-blue)](https://docs.rs/crate/gmail-auto-label/latest)
[![License](https://img.shields.io/github/license/lichtcui/gmail-auto-label)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)
[![Crates.io](https://img.shields.io/crates/d/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)

An automatic Gmail labeling tool powered by LLM (DeepSeek API).  
This is the primary documentation.

🌐 Languages: [🇺🇸 English](README.md) · [🇨🇳 简体中文](README_ZH.md)

## Features

- Auto-scan inbox threads and classify emails into business-friendly labels
- **Two-phase workflow**: IMAP sync + LLM summarization → batch classification + label application (no Gmail API calls needed)
- Cache-first classification (memo + reusable rules) to reduce repeated LLM calls
- LLM fallback for uncached emails, then persist extracted rules for later reuse
- LLM-based label consolidation: when labels exceed the limit, semantically groups similar labels instead of blindly merging low-frequency ones
- Labels auto-created by Gmail IMAP (no separate API calls required)
- Automatic archive step (remove `INBOX`) after labeling via IMAP
- Machine-readable JSON output (`--output json`)

## Prerequisites

- DeepSeek API key (set via `--api-key` or `DEEPSEEK_API_KEY` env var)
- Rust toolchain is installed
- Gmail IMAP app password (see [IMAP Setup](#imap-setup) below)

## IMAP Setup

Gmail requires an app password for IMAP access. Regular passwords will not work.

1. Enable **2-Step Verification** on your Google account at https://myaccount.google.com/security
2. Generate an **App Password** at https://myaccount.google.com/apppasswords (select "Mail" as the app)
3. Use that 16-character app password as `--imap-pass` (spaces optional)

## Build

```bash
cargo build --release
```

Binary:

```bash
./target/release/gmail-auto-label
```

## Install from crates.io

Install with:

```bash
cargo install gmail-auto-label
```

After installation, run the binary directly:

```bash
gmail-auto-label --help
```

## LLM Setup

Set your DeepSeek API key (the model defaults to `deepseek-v4-flash`):

```bash
# Via environment variable (recommended)
export DEEPSEEK_API_KEY=sk-your-key-here

# Or via CLI argument
gmail-auto-label --api-key sk-your-key-here
```

To use a different model:

```bash
gmail-auto-label --model deepseek-v4-pro
```

## Usage

### Two-Phase Workflow

Phase 1 — Sync all emails via IMAP and summarize via LLM (zero Gmail API reads):

```bash
gmail-auto-label --sync --imap-user your@gmail.com --imap-pass your-app-password
```

> **IMAP app password required**: Google no longer accepts regular passwords for IMAP access.
> 1. Enable **2-Step Verification** on your Google account at https://myaccount.google.com/security
> 2. Generate an **App Password** at https://myaccount.google.com/apppasswords (select "Mail" as the app)
> 3. Use that 16-character app password (spaces optional) as `--imap-pass`

Optionally limit the number of messages to sync:

```bash
gmail-auto-label --sync --imap-user your@gmail.com --imap-pass xxxx --sync-max 5000
```

Phase 2 — Classify from cached summaries and batch-apply labels via IMAP:

```bash
gmail-auto-label --from-cache --imap-user your@gmail.com --imap-pass your-app-password
```

This phase uses IMAP `STORE +X-GM-LABELS` to apply labels — zero Gmail API calls. Labels are auto-created by Gmail IMAP when they don't exist.

Machine-readable JSON output:

```bash
gmail-auto-label --output json
```

## Key Options

| Option | Description | Default |
|--------|-------------|---------|
| `--api-key` | DeepSeek API key (or `DEEPSEEK_API_KEY` env) | — |
| `--model` | DeepSeek model name | `deepseek-v4-flash` |
| `--max-labels` | Max active labels before compression | `10` |
| `--output` | Output format (`text` \| `json`) | `text` |
| `--sync` | Run IMAP sync phase (fetch + summarize) | — |
| `--from-cache` | Process from previously synced cache data (requires `--imap-user` + `--imap-pass`) | — |
| `--imap-user` | IMAP username (Gmail address) | — |
| `--imap-pass` | IMAP app password (regular password won't work) | — |
| `--imap-host` | IMAP server hostname | `imap.gmail.com` |
| `--imap-port` | IMAP server port | `993` |
| `--sync-max` | Max messages to sync (0 = unlimited) | `0` |

## Advanced Options

These flags remain supported for compatibility, but are hidden by default:

- `--cache-file`
- `--merged-label`

Built-in feedback file format (internal path is fixed to `/tmp/gmail_auto_label_feedback.json`):

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
- `ts` uses Unix seconds; stale events older than the built-in feedback retention window are skipped.

## How It Works

### Data Flow (Two-Phase Mode)

```
Phase 1: IMAP Sync (--sync)
  IMAP inbox ──► parse MIME ──► extract body text ──► LLM summarize ──► cache (local JSON)

Phase 2: Process from Cache (--from-cache)
  cache ──► classify (cached memos → learned rules → LLM) ──► IMAP STORE +X-GM-LABELS
```

Phase 2 reads exclusively from the local cache — zero Gmail API calls. Labels are applied via IMAP's `STORE +X-GM-LABELS` command, which also auto-creates labels if they don't exist.

### Classification Priority

1. Cache memo (exact sender+subject+snippet match)
2. Learned keyword rules (sorted by hit count)
3. LLM classification (DeepSeek API) with automatic rule extraction

### Rate Limiting

- Read operations (IMAP sync, Phase 1): No Gmail API calls
- Write operations (label create/modify, Phase 2): All operations via IMAP — no Gmail API calls, no rate limits
- LLM calls: Automatic retry with exponential backoff (1s, 2s, 4s) for transient failures

## Help

```bash
gmail-auto-label --help
```
