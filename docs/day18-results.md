# Day 18 verification results

Date: 2026-09-27 (Europe/Moscow)

Status: the light agent is deployed on the target VM and accepted end to end
through the Mac-routed DeepSeek and Telegram paths. Forced-command SSH, multiple
dialogs, persisted dialog recovery, confirmed scheduling, VM-local inspection,
and a scheduled Telegram write were exercised live. Availability of both
external services now depends on the Mac being awake, logged in, and running the
two LaunchAgents.

## Local deterministic verification

Rust commands were rerun locally on 2026-09-27:

- `cargo fmt --check`: passed;
- `cargo clippy --locked --all-targets --all-features -- -D warnings`: passed;
- `cargo test --locked --all-targets --all-features`: 313 passed, 5 ignored,
  0 failed;

Python verification ran locally on 2026-09-27:

- `/usr/bin/python3 -m unittest discover -s deploy/light-agent/mac -p
  'test_*.py'`: 10 passed;
- `telegram_mcp/.venv/bin/python -m pytest telegram_mcp/tests -q`: 71 passed;
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

### Mac-routed DeepSeek and Telegram acceptance

The source-path-specific DeepSeek timeout was resolved without exposing the
provider credential to the Mac proxy. A loopback-only CONNECT proxy on the Mac
accepts only `api.deepseek.com:443`; a dedicated, non-login VM identity exposes
it as `127.0.0.1:18080`. The provider uses that endpoint explicitly and does not
fall back to ambient proxies or direct egress. Both LaunchAgents recovered after
restart. An authenticated status request returned HTTP 200 through the route.

The following ignored live checks passed through the installed Mac terminal
client and VM runtime:

- a DeepSeek response without tools;
- model responses in two independent dialogs;
- model-selected read-only Telegram MCP use;
- an explicitly confirmed cron create/delete roundtrip;
- cleanup with zero scheduled crontab commands.

The VM could not reach its Telegram DC directly. The existing restricted reverse
tunnel was extended with a second fixed listener at `127.0.0.1:18082`, forwarding
only to the session's current DC through the Mac. Telethon preserves the original
DC id and auth key, overrides only the endpoint, and fails closed if Telegram
requests a DC migration. This is not a SOCKS or general-purpose proxy. Telegram
MCP remained bound to `127.0.0.1:8000`, and its list/read path passed through the
relay.

With explicit operator authorization, a `once_at` job was created through the
terminal confirmation preview for 2026-09-27 16:04 Europe/Moscow. Its prompt
targeted one named private chat and Saved Messages without embedding message
content. The cron run completed once. Metadata-only audit recorded two successful
`list_chats` calls, one successful `read_chat`, and one successful
`send_message`; an independent status-only MCP check observed a new recent
outgoing Saved Messages entry. The message text, chat identifiers, and tool
payloads were not printed or recorded here.

Generic MCP writes still do not have an in-protocol confirmation gate. The live
Telegram write relied on the operator's explicit one-time authorization and an
exact terminal preview for the scheduled task. The scheduler confirmation
mechanism applies to creating the job, not to arbitrary MCP writes inside a
normal or scheduled turn.

No credential, key material, private Telegram endpoint, task payload, model
answer, raw MCP payload, database row, or message content is recorded here.
