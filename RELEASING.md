# `gmail-auto-label` Release Process (Minimal)

## 1. Version Rules

- Follow Semantic Versioning: `MAJOR.MINOR.PATCH`
- `Cargo.toml` uses `<VERSION>` (example: `0.1.6`)
- Git tag and GitHub Release use `v<VERSION>` (example: `v0.1.6`)

## 2. Pre-Release Checks

Verify repo state first, then run quality gates.

```bash
# Sync with remote (this repo currently uses master)
git fetch origin
git status -sb

# Working tree must be clean
git diff --quiet && git diff --cached --quiet

# Checks aligned with CI
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc

# Packaging validation before publish
cargo publish --dry-run
cargo package --list
```

## 3. Prepare and Commit Version

1. Set target release version.
2. Update `version` in `Cargo.toml` (for example `0.1.4 -> 0.1.6`).
3. Commit the version bump.

```bash
# set this once per release
VERSION="0.1.6"

git add Cargo.toml Cargo.lock
git commit -m "chore(release): bump version to $VERSION"
```

## 4. Publish to crates.io

```bash
cargo login
cargo publish
```

Run verification outside the repo to avoid reading the local crate:

```bash
(cd /tmp && cargo info gmail-auto-label)
```

## 5. Tag and Create GitHub Release

```bash
git tag "v$VERSION"
git push origin "v$VERSION"
gh release create "v$VERSION" \
  --repo lichtcui/gmail-auto-label \
  --title "v$VERSION" \
  --generate-notes
```

## 6. Post-Release Verification

```bash
cargo install gmail-auto-label --version "$VERSION"
gmail-auto-label --help
```

Checklist:

- `<VERSION>` appears on crates.io
- docs.rs build is successful
- GitHub Release exists with correct notes
