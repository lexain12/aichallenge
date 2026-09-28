# Day 18 light agent

Day 18 is a small remote agent: DeepSeek streaming, bounded tool calling, MCP,
multiple durable dialogs, and cron jobs with per-dialog confirmation policy.
The agent runs on a VM over a forced-command SSH stdio session; the interactive
terminal client runs on macOS.
There is no TCP listener, daemon mode, compaction layer, or hidden background
conversation process.

The Rust workspace intentionally ships only two binaries:

- `light-agent`: VM runtime (`serve-stdio`, `run-job`, `cron-sync`, and the
  VM-local read-only database shell);
- `light-agent-client`: the default macOS terminal client.

`telegram_mcp/` remains a separate Python service and must listen on loopback.

## Local verification

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
uv sync --frozen --project telegram_mcp --group dev
uv run --project telegram_mcp --group dev pytest telegram_mcp/tests -q
```

The five tests in `tests/day18_live.rs` are ignored by default. They require
`LIGHT_AGENT_LIVE=1` and a client configuration selected with
`LIGHT_AGENT_CLIENT_CONFIG`. The cron mutation test additionally refuses to run
without `LIGHT_AGENT_CRON_LIVE=1`.

## Configuration and operation

Start from `light-agent.example.toml` on the VM and
`light-agent-client.example.toml` on the Mac. The client configuration contains
only the system SSH binary, a safe SSH config alias, and the fixed remote
command. Host verification, identity selection, user, and port stay in the
owner's `~/.ssh/config`; normal OpenSSH host-key verification remains enabled.

See [the deployment runbook](deploy/light-agent/README.md) for installation,
forced-command SSH, Cronie preflight, backups, inspection/export, and acceptance
steps. Historical day notes remain under `docs/`.
