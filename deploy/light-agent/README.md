# Light-agent deployment and operator runbook

This runbook is preparatory. It does not authorize connecting to a VM, changing
SSH access, installing a crontab, rebooting a host, or provisioning credentials.
Perform those actions only in a separately authorized live acceptance window.

## One runtime identity and one home

The deployment contract has one dedicated `light-agent` account whose home
directory is exactly `/var/lib/light-agent`. Both OpenSSH and Cronie start the
runtime with `HOME=/var/lib/light-agent`, and their working directory is
`/var/lib/light-agent`. This is security-relevant: the fixed commands intentionally
omit `--config`, so `light-agent` resolves its default `light-agent.toml` from
that working directory.

The account needs a real `/bin/sh` because `sshd` invokes a forced command
through the account shell. It is not an interactive administration account:
lock its password, give it no sudo membership, and authorize only the dedicated
forced-command key. During an authorized install, an administrator can use:

```bash
sudo useradd --system --create-home --home-dir /var/lib/light-agent --shell /bin/sh light-agent
sudo passwd --lock light-agent
sudo install -d -o light-agent -g light-agent -m 0700 /var/lib/light-agent
sudo install -d -o root -g root -m 0755 /opt/light-agent/bin
sudo install -o root -g root -m 0755 target/release/light-agent /opt/light-agent/bin/light-agent
sudo install -o light-agent -g light-agent -m 0600 light-agent.example.toml /var/lib/light-agent/light-agent.toml
```

For an existing account, verify rather than silently changing it:

```bash
getent passwd light-agent
sudo -u light-agent -H sh -c 'cd /var/lib/light-agent && test "$HOME" = /var/lib/light-agent && test "$PWD" = /var/lib/light-agent && test -r light-agent.toml'
```

The owner-only configuration is
`/var/lib/light-agent/light-agent.toml` (mode `0600`). Durable state is
`/var/lib/light-agent/state.sqlite3`; SQLite and runtime lock sidecars are
created beside it under the mode `0700` home. The example intentionally omits
`scheduler.lock_path`, so the validated default is derived beside the database,
not under `/run/lock`. Keep provider and MCP credentials out of process arguments
and shell history.

## Cronie preflight and managed environment

Only Cronie is supported. Before allowing scheduled mutations, verify on the VM:

```bash
/usr/bin/crontab -V
printf 'HOME=/var/lib/light-agent\nCRON_TZ=UTC\n0 0 * * * /bin/true\n' | /usr/bin/crontab -T -
```

The version must identify Cronie and syntax validation must exit successfully.
Install and inspect only the `light-agent` account's crontab. The managed block
sets `HOME=/var/lib/light-agent`, then emits a validated `CRON_TZ` and only:

```text
/opt/light-agent/bin/light-agent run-job <canonical-job-id>
```

Cronie also starts jobs in the account home from the passwd database. The HOME
line and passwd home must agree; the deployment acceptance fails if either is
different. Task text never appears in the crontab. Do not manually edit inside
the managed block. Reconciliation removes an existing managed block and appends
one normalized block at the end of the crontab. All unmanaged bytes retain their
original order; only one newline separator is added when the unmanaged content
does not already end in a newline. Keeping the block at EOF prevents its `HOME`
or final `CRON_TZ` assignment from changing later unmanaged entries.

Run reconciliation from the same home contract:

```bash
sudo -u light-agent -H sh -c 'cd /var/lib/light-agent && exec /opt/light-agent/bin/light-agent cron-sync'
sudo -u light-agent /usr/bin/crontab -l
```

## Forced-command SSH

Provision a separate owner key for the terminal client. Add one line based on
`authorized_keys.example` to `/var/lib/light-agent/.ssh/authorized_keys` only
after replacing the key-material placeholder locally. The `.ssh` directory and
file must be owned by `light-agent` with modes `0700` and `0600`. Keep the fixed
command exactly:

```bash
sudo install -d -o light-agent -g light-agent -m 0700 /var/lib/light-agent/.ssh
sudo install -o light-agent -g light-agent -m 0600 /path/to/prepared-authorized-keys /var/lib/light-agent/.ssh/authorized_keys
```

```text
/opt/light-agent/bin/light-agent serve-stdio
```

OpenSSH sets HOME and changes to the account home before invoking the account
shell with the forced command. The key entry disables PTY, agent forwarding,
port forwarding, X11 forwarding, and user-supplied commands. The runtime is an
NDJSON stdio process, not a network listener.

On the Mac, keep connection policy in `~/.ssh/config`:

```sshconfig
Host light-agent-vm
    HostName <PRIVATE_ADDRESS>
    User light-agent
    Port 22
    IdentityFile ~/.ssh/<DEDICATED_IDENTITY_FILE>
    IdentitiesOnly yes
    StrictHostKeyChecking yes
```

Verify a new or changed host key out of band before accepting it. Then create
an owner-only `light-agent-client.toml` from
`light-agent-client.example.toml` and run:

```bash
light-agent-client --config ./light-agent-client.toml
```

The client launches system OpenSSH as the argument vector
`ssh -T <alias> /opt/light-agent/bin/light-agent serve-stdio`; the client does
not invoke a local shell or override the user, port, identity, or host-key policy.

## Telegram MCP

Deploy `telegram_mcp/` as a separate service account and bind it only to
`127.0.0.1:8000`. Point the server-side MCP entry to that loopback URL. Never
expose the MCP endpoint through a public listener or SSH forwarding. Its
credential file must be owner-only and excluded from logs, process arguments,
Git, and ordinary backups unless the backup is explicitly protected as secret
material.

## Inspection and export

The terminal client supports multiple dialogs and the read-only views used by
`/dialogs`, `/history`, `/jobs`, `/job`, `/runs`, `/audit`, and `/dump`.
`/export <local-path>` streams a bounded JSONL export to the Mac, verifies byte
count and SHA-256 incrementally, fsyncs, and atomically renames only after a
complete transfer. The local path is never sent to the server.

The SQL shell is deliberately VM-local and uses the same home/config contract:

```bash
sudo -u light-agent -H sh -c 'cd /var/lib/light-agent && exec /opt/light-agent/bin/light-agent db-shell --readonly'
```

It accepts only one bounded read statement and must not be exposed through the
SSH forced command. Prefer logical `/dump` or `/export` for ordinary inspection.

Schedule create/update/enable/disable/delete requests require an explicit,
terminal-safe confirmation showing the action, job identity, name, schedule,
timezone, and full task text. An unanswered or expired confirmation must not
mutate SQLite or the crontab.

## Backup and restore

The configuration, database, and exports are sensitive: they can contain dialog
text, scheduled task text, tool audit metadata, model output, and credentials.
Encrypt backups, restrict their ownership, and never paste their content into
logs or tickets. Do not copy only the main SQLite file while writers are active.

For an authorized backup window, first prevent new SSH sessions and cron starts,
wait for existing `light-agent` processes to exit, then use SQLite's online
backup operation to a pre-created protected destination. Record a checksum but
not file contents. Back up the configuration separately under the same secret
handling policy. Re-enable entry points only after the backup succeeds.

For restore, keep the service quiescent, retain the previous database and its
sidecars as a recoverable rollback set, place the restored database at
`/var/lib/light-agent/state.sqlite3` with owner `light-agent:light-agent` and
mode `0600`, and validate startup. Then run `cron-sync` from the runtime home and
compare the managed crontab block with `/jobs`. Never merge arbitrary sidecars
from different snapshots.

## Startup and crash recovery

Every `serve-stdio`, `run-job`, and `cron-sync` process acquires a runtime-owner
lease before mutating durable work. Startup serializes owner inspection with a
coordinator lock. A recorded owner is recovered only when its exact owner lock
can be acquired exclusively, proving that process is dead. A live owner keeps
its lock and is left untouched, including work belonging to another concurrent
SSH session or cron process.

Recovery is terminal and does not replay provider or MCP calls:

- a pending read-only tool becomes `failed` with `process_interrupted`;
- a pending write tool becomes `uncertain` with `process_interrupted`, because
  the external side effect may already have happened;
- a pending interactive turn becomes `interrupted`, so no partial assistant
  answer is committed as completed;
- a pending cron run becomes `interrupted`; it is not reported as completed.

Inspect `/audit`, `/history`, and `/runs` after recovery. Never automatically
retry an uncertain write tool; verify the external system first. If ownership
metadata or lock identity cannot be verified, startup fails closed instead of
guessing that work is dead.

After an authorized reboot:

1. Verify the `light-agent` passwd home, file ownership, `0700` home, `0600`
   configuration/database, root-owned executable, and Telegram loopback service.
2. Confirm no unexpected public listener exists and inspect the `light-agent`
   crontab without printing task text (the crontab contains IDs only).
3. Connect once through the Mac client; startup recovery should terminalize only
   provably dead owners. Use `/dialogs`, `/history`, `/jobs`, `/runs`, `/audit`,
   and `/dump` to verify durable state without raw database access.
4. Run `cron-sync` from `/var/lib/light-agent`, re-read the crontab, and verify
   scheduled jobs are reconciled. If reconciliation fails, leave saved jobs in
   their visible failed/pending sync state and repair Cronie before retrying.
5. Keep one live client turn active while opening a second session and verify the
   live owner remains pending rather than being recovered. Perform destructive
   crash simulation only in a separately approved test window.
6. If startup validation fails, keep cron and SSH entry points quiescent and use
   the restore procedure above; do not delete the rollback database or sidecars
   until dialogs, jobs, runs, audit rows, and a new backup are verified.

## Live acceptance gate

First run all deterministic checks documented in the repository README. Then,
in an explicitly authorized window, set `LIGHT_AGENT_LIVE=1` and
`LIGHT_AGENT_CLIENT_CONFIG` to an owner-only Mac client file and invoke ignored
tests individually. Set `LIGHT_AGENT_CRON_LIVE=1` only while authorizing the
confirmed create/delete cron roundtrip. Reboot recovery additionally uses a
non-sensitive persisted dialog ID prepared before the reboot.

Record only commands, timestamps, safe status codes, and redacted outcomes in
`docs/day18-results.md`. Never record credentials, task text, model responses,
raw MCP payloads, raw database rows, key material, or private host addresses.
