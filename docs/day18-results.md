# Day 18 verification results

Date: 2026-09-26 (Europe/Moscow)

Status: local preflight Steps 1–5 passed. Live VM acceptance is pending
explicit authorization and has not been performed. No VM connection, remote
crontab change, SSH change, deployment, or reboot is claimed by this document.

## Local deterministic verification

All commands ran locally on 2026-09-26:

- `cargo fmt --check`: passed;
- `cargo clippy --locked --all-targets --all-features -- -D warnings`: passed;
- `cargo test --locked --all-targets --all-features`: 280 passed, 5 ignored,
  0 failed;
- `uv sync --frozen --project telegram_mcp --group dev`: audited 39 packages;
- `uv run --project telegram_mcp --group dev pytest telegram_mcp/tests -q`:
  50 passed;
- `git diff --check`: passed.

The legacy-name scan reported only reviewed, non-legacy uses: export result
summary variables, negative configuration tests that reject removed sections,
and Unix sticky-directory security tests. The forced-command key example scan
reported no credential field, Telegram credential field, or task text field.

## Live acceptance

Pending authorization. The following ignored checks remain intentionally
unexecuted during local preflight:

- DeepSeek response without tools;
- two-dialog SSH roundtrip;
- read-only Telegram MCP call through the VM loopback service;
- explicitly confirmed cron create/delete roundtrip;
- persistence and service recovery after an authorized reboot.

When authorized, record timestamps and safe pass/fail summaries only. Do not
paste credentials, key material, private addresses, task text, answers, raw MCP
payloads, database rows, or export contents here.
