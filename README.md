# gmail-auto-label (Rust)

[![Documentation](https://img.shields.io/badge/docs-docs.rs-blue)](https://docs.rs/crate/gmail-auto-label/latest)
[![License](https://img.shields.io/github/license/lichtcui/gmail-auto-label)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)
[![Crates.io](https://img.shields.io/crates/d/gmail-auto-label.svg)](https://crates.io/crates/gmail-auto-label)

An automatic Gmail labeling tool built with `gog` + `codex` (Rust version).  
This is the primary documentation.

🌐 Languages: [🇺🇸 English](README.md) · [🇨🇳 简体中文](README_ZH.md)

## Features

- Auto-scan inbox threads and classify emails into business-friendly labels
- Optional custom label rules with higher priority than memoized, learned, and Codex-generated matches
- Cache-first classification (memo + reusable rules) to reduce repeated LLM calls
- Codex fallback for uncached emails, then persist extracted rules for later reuse
- Auto-create missing Gmail labels and apply labels in batches
- Optional archive step (remove `INBOX`) after labeling, with `--keep-inbox` support
- Label compression when active labels exceed the limit (public control is `--max-labels`, merge target defaults to `others`)
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

Optional (pin a version):

```bash
cargo install gmail-auto-label --version 0.1.3
```

After installation, run the binary directly:

```bash
gmail-auto-label --help
```

## Common Usage

1. Single pass (default: 20 threads):

```bash
gmail-auto-label --limit 20
```

2. Dry run (no write operations):

```bash
gmail-auto-label --dry-run --limit 20
```

3. Watch mode (every 5 minutes):

```bash
gmail-auto-label --watch 300
```

If a round finds no pending inbox threads, watch mode waits for the next interval instead of exiting.

4. Keep messages in inbox (label only, no archive):

```bash
gmail-auto-label --keep-inbox
```

5. Use custom label rules:

```bash
gmail-auto-label --custom-labels-file ./custom-labels.json
```

## Key Options

- `--limit`: max threads per run, default `20`
- `--watch`: polling interval in seconds, for example `--watch 300`
- `--account`: gog account name
- `--dry-run`: print actions only, no writes
- `--custom-labels-file`: load custom label rules from a JSON file
- `--max-labels`: max active labels, default `10`
- `--keep-inbox`: do not remove processed threads from inbox

## Advanced Options

These flags remain supported for compatibility, but are hidden by default:

- `--codex-cmd`
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

## Custom Label Rules

Use `--custom-labels-file <path>` to load user-defined label rules before a run starts. Matching order is:

1. Custom label rules
2. Memoized matches
3. Learned rules
4. Codex fallback

Rules are evaluated in file order, first match wins. Custom labels are not auto-pruned by feedback and are not merged into the learned-label compression target.

Example file:

```json
[
  {
    "label": "Important Client",
    "include_keywords": ["vip", "invoice"],
    "exclude_keywords": ["spam"]
  }
]
```

Validation rules:
- File must be readable JSON
- Top-level value must be an array
- Each rule must include a non-empty `label`
- Each rule must include at least one non-empty `include_keywords` value

## Help

```bash
gmail-auto-label --help
```
