# Repository agent instructions

- Before connecting to, diagnosing, or deploying the Day 18 remote agent, read
  `REMOTE_SERVER.local.md` in the repository root.
- That file is local and gitignored because it contains environment-specific
  host, access, deployment, and rollback information. Never commit it.
- If the file is missing or stale, do not guess the live state. Recreate or
  update it from status-only checks that do not expose credentials, prompts,
  Telegram data, or tool arguments.
