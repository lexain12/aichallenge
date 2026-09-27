# Day 18 verification results

Date: 2026-09-27 (Europe/Moscow)

Status: local preflight Steps 1–5 passed. The light-agent runtime, forced-command
SSH access, terminal client, Cronie, and loopback Telegram MCP are installed and
partially accepted on the target VM. Reboot recovery passed. End-to-end model
turns and model-driven cron mutation remain blocked because authenticated
DeepSeek requests from the VM time out, while the same key succeeds from the
Mac. This document therefore does not claim complete production readiness.

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

The authorized live window on the target Ubuntu 25.10 environment established:

- Cronie 1.7.2 is installed, enabled, and active. Installing it replaced the
  distribution `cron` package and removed the `ubuntu-standard` metapackage; a
  root-only pre-install rollback snapshot was retained on the VM.
- `crontab -V` succeeds. The corrected owner-only path-form preflight,
  `crontab -T <temporary-file>`, succeeds without installing the candidate.
  The deployed runtime is the reviewed `0ab9298` source build, and `cron-sync`
  succeeds before and after reboot with zero scheduled commands.
- The `light-agent` account has `/var/lib/light-agent` as HOME, `/bin/sh` as its
  forced-command shell, no password login or sudo access, a `0700` home, `0600`
  configuration/database/key files, and the root-owned executable under
  `/opt/light-agent/bin`.
- OpenSSH's existing `AllowUsers` policy was extended to include only the new
  account, validated with `sshd -t`, and reloaded without losing the existing
  administrative connection. The dedicated key returns the versioned protocol
  hello and cannot request an arbitrary remote command.
- Telegram MCP is enabled as a separate systemd service and listens only on
  `127.0.0.1:8000`. The checked-in unit passed Ubuntu's
  `systemd-analyze verify`, was installed, and restarted active. MCP initialize
  returned success. A direct, read-only probe verified exactly the expected
  `list_chats`, `read_chat`, and `send_message` catalog annotations and
  completed `list_chats` without recording its result.
- The Mac terminal client is installed owner-locally with an owner-only config.
  A PTY session created and listed two independent dialogs, then exited cleanly.
  The VM-local restricted SQL shell returned only requested status fields. A
  bounded `/export` completed with local byte-count/checksum verification; the
  temporary export was then removed.
- An authorized reboot changed the kernel boot ID. Cronie and Telegram MCP
  returned enabled/active, the MCP listener remained loopback-only, file modes
  remained correct, `cron-sync` succeeded, and ignored test
  `live_reboot_recovery` reopened a persisted dialog.

### External DeepSeek blocker

The ignored `live_deepseek_no_tools` check reached the VM and created a durable
turn, but the turn ended with the safe code `provider_error`. Follow-up probes
recorded no prompts, answers, credentials, or provider bodies and established:

- unauthenticated TLS access from the VM reaches the official endpoint quickly;
- authenticated `/models`, `/user/balance`, and a minimal streaming completion
  all time out from the VM;
- the same API key receives HTTP 200 for `/models` from the Mac through the same
  resolved CDN edge;
- the VM has no usable IPv6 route for an alternate source path.

Together these probes show an observed source-path-specific authenticated
timeout. They do not establish whether the cause is provider policy, an
intermediary, routing, or another source-dependent network condition. The Mac
comparison narrows the symptom but does not prove that the key, model, DNS, or
TLS can be excluded for every VM request. Until authenticated requests from the
VM succeed, the following tests cannot honestly pass and remain pending:

- DeepSeek response without tools;
- model responses in two independent dialogs;
- model-selected read-only Telegram tool use;
- explicitly confirmed cron create/delete roundtrip and execution.

No credential, key material, private address, task text, model answer, raw MCP
payload, database row, or export content is recorded here.
