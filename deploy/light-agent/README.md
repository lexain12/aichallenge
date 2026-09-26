# Light-agent deployment and operator runbook

This runbook is preparatory. It does not authorize connecting to a VM, changing
SSH access, installing a crontab, rebooting a host, or provisioning credentials.
Perform those actions only in a separately authorized live acceptance window.

## Runtime layout

Create a dedicated non-login `light-agent` account with no sudo membership.
Install the root-owned executable at `/opt/light-agent/bin/light-agent`. Keep the
server configuration in an owner-only location such as
`/etc/light-agent/light-agent.toml` (`0600`, readable by the runtime account via
the chosen ownership policy). Store durable state at
`/var/lib/light-agent/state.sqlite3`; its directory must be owned by
`light-agent` and mode `0700`.

The configuration and SQLite database are sensitive. The database contains
dialog text, scheduled task text, tool audit metadata, and model output. Back up
the database and its live SQLite sidecars as one consistent snapshot, encrypt
the backup, restrict access, and test restore separately. Do not copy raw data
into tickets or logs.

Install `light-agent.example.toml` as the starting point, change its database
path to `/var/lib/light-agent/state.sqlite3`, and keep provider and MCP
credentials out of shell arguments. If an environment file is used, make it
owner-only and never source it from an interactive shell history.

## Cronie preflight

Only Cronie is supported. Before allowing scheduled mutations, verify on the VM:

```bash
/usr/bin/crontab -V
printf 'CRON_TZ=UTC\n0 0 * * * /bin/true\n' | /usr/bin/crontab -T -
```

The version must identify Cronie and syntax validation must exit successfully.
The agent owns one marked crontab block and invokes only
`/opt/light-agent/bin/light-agent run-job <canonical-job-id>`. Task text never
appears in the crontab. Do not manually edit inside the managed block.

## Forced-command SSH

Provision a separate owner key for the terminal client. Add one line based on
`authorized_keys.example` to the `light-agent` account only after replacing the
key-material placeholder locally. Keep the fixed command exactly:

```text
/opt/light-agent/bin/light-agent serve-stdio
```

The key entry disables PTY, agent forwarding, port forwarding, X11 forwarding,
and user-supplied commands. The runtime is an NDJSON stdio process, not a
network listener.

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

The client always launches system OpenSSH as the argument vector
`ssh -T <alias> /opt/light-agent/bin/light-agent serve-stdio`; it does not use a
shell and does not override the user, port, identity, or host-key policy.

## Telegram MCP

Deploy `telegram_mcp/` as a separate service account and bind it only to
`127.0.0.1:8000`. Point the server-side MCP entry to that loopback URL. Never
expose the MCP endpoint through a public listener or SSH forwarding. Its
credential file must be owner-only and excluded from logs, process arguments,
Git, and backups unless the backup is explicitly protected as secret material.

## Operator commands

The terminal client supports multiple dialogs and the read-only views used by
`/dialogs`, `/history`, `/jobs`, `/job`, `/runs`, `/audit`, and `/dump`.
`/export <local-path>` streams a bounded JSONL export to the Mac, verifies byte
count and SHA-256 incrementally, fsyncs, and atomically renames only after a
complete transfer. The local path is never sent to the server.

The SQL shell is deliberately VM-local:

```bash
/opt/light-agent/bin/light-agent \
  --config /etc/light-agent/light-agent.toml db-shell --readonly
```

It accepts only one bounded read statement and must not be exposed through the
SSH forced command. Prefer logical `/dump` or `/export` for ordinary inspection.

Schedule create/update/enable/disable/delete requests require an explicit,
terminal-safe confirmation showing the action, job identity, name, schedule,
timezone, and full task text. An unanswered or expired confirmation must not
mutate SQLite or the crontab.

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
