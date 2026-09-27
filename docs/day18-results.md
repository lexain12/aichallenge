# Day 18 verification results

Date: 2026-09-27 (Europe/Moscow)

Status: local preflight Steps 1–5 passed. Read-only live Cronie discovery found
and isolated a deployment blocker; mutating VM acceptance remains pending. No
remote crontab change, SSH change, deployment, service change, or reboot is
claimed by this document.

## Local deterministic verification

Rust commands were rerun locally on 2026-09-27:

- `cargo fmt --check`: passed;
- `cargo clippy --locked --all-targets --all-features -- -D warnings`: passed;
- `cargo test --locked --all-targets --all-features`: 297 passed, 5 ignored,
  0 failed;

Python verification last ran locally on 2026-09-26:

- `uv sync --frozen --project telegram_mcp --group dev`: audited 39 packages;
- `uv run --project telegram_mcp --group dev pytest telegram_mcp/tests -q`:
  50 passed;
- `git diff --check`: passed.

The legacy-name scan reported only reviewed, non-legacy uses: export result
summary variables, negative configuration tests that reject removed sections,
and Unix sticky-directory security tests. The forced-command key example scan
reported no credential field, Telegram credential field, or task text field.

The local deployment contract was also verified: the dedicated account HOME,
working directory, default configuration, database, and managed Cronie HOME all
resolve under `/var/lib/light-agent`. This is documentation and deterministic
rendering evidence only; it is not evidence from a real VM.

## Live acceptance

Read-only discovery on the target Ubuntu 25.10 environment established that
Cronie 1.7.2 is installed and that `crontab -V` succeeds. It also established
that piping a candidate to `crontab -T -` fails with a bounded `premature EOF`
diagnostic because this Cronie expects `-T <file>`. No candidate task text,
address, credential, or other secret was recorded.

The local backend and runbook now create an owner-only, fsynced temporary
regular file, pass only its random path as the `-T` argument, and clean it up on
success, failure, timeout, or cancellation. Deployment of that correction and
the live path-form recheck remain pending; this document does not claim the VM
has been fixed.

The following ignored checks also remain intentionally unexecuted:

- DeepSeek response without tools;
- two-dialog SSH roundtrip;
- read-only Telegram MCP call through the VM loopback service;
- explicitly confirmed cron create/delete roundtrip;
- persistence and service recovery after an authorized reboot.

When authorized, record timestamps and safe pass/fail summaries only. Do not
paste credentials, key material, private addresses, task text, answers, raw MCP
payloads, database rows, or export contents here.
