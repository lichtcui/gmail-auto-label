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
- **Two-phase workflow**: IMAP sync + LLM summarization → batch classification + label application (avoids Gmail API rate limits)
- Cache-first classification (memo + reusable rules) to reduce repeated LLM calls
- LLM fallback for uncached emails, then persist extracted rules for later reuse
- Auto-create missing Gmail labels and apply labels in batches
- Automatic archive step (remove `INBOX`) after labeling
- Label compression when active labels exceed the limit (`--max-labels`, merge target defaults to `others`)
- Gmail rate-limit handling with automatic retry/backoff
- Machine-readable JSON output (`--output json`)

## Prerequisites

- `gog` is installed and authenticated (for Gmail write operations)
- DeepSeek API key (set via `--api-key` or `DEEPSEEK_API_KEY` env var)
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
gmail-auto-label --account your-account-name
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

Phase 2 — Classify from cached summaries and batch-apply labels:

```bash
gmail-auto-label --from-cache
```

This phase only calls the Gmail API for label creation and modification (write operations), bypassing read quota entirely.

Machine-readable JSON output:

```bash
gmail-auto-label --output json
```

## Key Options

| Option | Description | Default |
|--------|-------------|---------|
| `--account` | gog account name | — |
| `--api-key` | DeepSeek API key (or `DEEPSEEK_API_KEY` env) | — |
| `--model` | DeepSeek model name | `deepseek-v4-flash` |
| `--max-labels` | Max active labels before compression | `10` |
| `--output` | Output format (`text` \| `json`) | `text` |
| `--sync` | Run IMAP sync phase (fetch + summarize) | — |
| `--from-cache` | Process from previously synced cache data | — |
| `--imap-user` | IMAP username for sync mode | — |
| `--imap-pass` | IMAP app password (regular password won't work) | — |
| `--imap-host` | IMAP server hostname | `imap.gmail.com` |
| `--imap-port` | IMAP server port | `993` |
| `--sync-max` | Max messages to sync (0 = unlimited) | `0` |

## Advanced Options

These flags remain supported for compatibility, but are hidden by default:

- `--cache-file`
- `--merged-label`
- Legacy compatibility: `--loop` + `--interval` still work, but `--watch` is the preferred form

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
  cache ──► classify (cached memos → learned rules → LLM) ──► batch apply labels
```

Phase 2 reads exclusively from the local cache — zero Gmail API read calls. Only the final label write step (via `gog gmail labels modify`) touches the Gmail API, which means the full read quota is available for write operations.

### Classification Priority

1. Cache memo (exact sender+subject+snippet match)
2. Learned keyword rules (sorted by hit count)
3. LLM classification (DeepSeek API) with automatic rule extraction

### Rate Limiting

- Read operations (IMAP sync, Phase 1): No Gmail API calls
- Write operations (label create/modify, Phase 2): Uses Gmail API with adaptive batch sizing and retry/backoff
- LLM calls: Automatic retry with exponential backoff (1s, 2s, 4s) for transient failures

## Help

```bash
gmail-auto-label --help
```
