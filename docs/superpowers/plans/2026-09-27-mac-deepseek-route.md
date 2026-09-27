# Mac DeepSeek Route Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route only the VM agent's DeepSeek HTTPS traffic through the user's Mac while leaving SSH client access and the VM-local Telegram MCP path unchanged.

**Architecture:** A loopback-only HTTP CONNECT proxy on the Mac accepts only `api.deepseek.com:443`. A persistent OpenSSH reverse forward exposes that proxy as `127.0.0.1:18080` on the VM, and the agent's provider configuration explicitly opts into that proxy. TLS and the DeepSeek bearer token remain end-to-end between the agent and DeepSeek.

**Tech Stack:** Rust/reqwest, Python standard library, OpenSSH reverse forwarding, macOS launchd, Linux sshd.

**Spec:** Approved conversation design plus `docs/superpowers/specs/2026-09-26-day-18-light-agent-design.md` and `docs/superpowers/specs/2026-09-26-secure-vm-deployment.md`.

**Completion:** Implemented, reviewed, deployed, and accepted live on 2026-09-27.
The checkboxes below preserve the original execution checklist; sanitized live
evidence is recorded in `docs/day18-results.md`.

## Global Constraints

- The Mac proxy binds only to `127.0.0.1:18081` and allows only `CONNECT api.deepseek.com:443`.
- The VM reverse-forward listener binds only to `127.0.0.1:18080`.
- DeepSeek TLS remains end-to-end; neither proxy nor SSH configuration receives the API key.
- Only DeepSeek provider traffic uses the proxy; Telegram MCP remains `127.0.0.1:8000` on the VM.
- `provider.proxy_url` is optional and, when present, must be an HTTP URL for an IP loopback host with an explicit port and no credentials, path, query, or fragment.
- Existing SSH administrator and restricted agent access must remain working.
- Logs and tests must not print API keys, prompts, model answers, Telegram data, or database contents.

## Review Focus

- A non-loopback proxy URL must fail configuration loading.
- Proxy credentials, paths, queries, fragments, and missing ports must fail configuration loading.
- The configured reqwest client must issue CONNECT to the configured proxy for an HTTPS provider endpoint.
- The proxy must reject non-CONNECT methods and every destination other than `api.deepseek.com:443`.
- Tunnel or Mac unavailability must fail closed without falling back to direct DeepSeek egress.

---

### Task 1: Explicit DeepSeek proxy configuration

**Files:**
- Modify: `src/settings.rs`
- Modify: `src/provider/deepseek.rs`
- Modify: `tests/settings.rs`
- Modify: `tests/deepseek.rs`

**Interfaces:**
- Consumes: existing `[provider]` TOML and `DeepSeekProvider::new(&ProviderSettings)`.
- Produces: `ProviderSettings::proxy_url() -> Option<&Url>` and reqwest HTTPS proxy configuration.

- [ ] **Step 1: Write failing settings tests**

Add tests proving a valid `proxy_url = 'http://127.0.0.1:18080'` loads, is redacted from `Debug`, and malformed or non-loopback variants are rejected.

- [ ] **Step 2: Run the settings tests and verify RED**

Run: `cargo test --locked --test settings proxy`

- [ ] **Step 3: Write a failing provider CONNECT test**

Use a local TCP proxy fixture and an HTTPS provider endpoint so the test observes the CONNECT authority without exposing request content.

- [ ] **Step 4: Run the provider test and verify RED**

Run: `cargo test --locked --test deepseek proxy`

- [ ] **Step 5: Implement the minimal settings and reqwest wiring**

Parse and validate the optional URL in `ServerSettings::load`; add `reqwest::Proxy::https` only when configured. Do not read ambient proxy environment variables.

- [ ] **Step 6: Verify GREEN and commit**

Run focused tests, `cargo fmt --check`, `cargo clippy --locked --all-targets --all-features -- -D warnings`, and `cargo test --locked`.

Commit: `feat: support loopback DeepSeek proxy`

### Task 2: Restricted Mac CONNECT proxy assets

**Files:**
- Create: `deploy/light-agent/mac/deepseek_connect_proxy.py`
- Create: `deploy/light-agent/mac/test_deepseek_connect_proxy.py`
- Modify: `deploy/light-agent/README.md`
- Modify: `tests/deployment_contract.rs`

**Interfaces:**
- Consumes: local TCP connections on `127.0.0.1:18081`.
- Produces: an HTTP/1.1 CONNECT proxy that relays only `api.deepseek.com:443` and emits no request data to logs.

- [ ] **Step 1: Write failing Python and deployment-contract tests**

Cover the accepted CONNECT authority, rejected methods/targets, bounded headers, loopback binding, and documented launchd/reverse-tunnel commands.

- [ ] **Step 2: Run tests and verify RED**

Run: `python3 -m unittest deploy/light-agent/mac/test_deepseek_connect_proxy.py` and `cargo test --locked --test deployment_contract mac_deepseek`.

- [ ] **Step 3: Implement the minimal proxy and runbook**

Use only the Python standard library, bounded reads, bidirectional relay, generic logging, and no shell execution.

- [ ] **Step 4: Verify GREEN and commit**

Run the focused tests plus the full Rust and Python suites and `git diff --check`.

Commit: `feat: add restricted Mac DeepSeek route`

### Task 3: Deploy and verify the route

**Files:**
- Modify: `docs/day18-results.md`

**Interfaces:**
- Consumes: Task 1 binary and Task 2 proxy asset.
- Produces: persistent Mac launchd proxy/tunnel, loopback-only VM listener, and an agent configuration using `http://127.0.0.1:18080`.

- [ ] **Step 1: Prove the temporary path**

Start the proxy and reverse tunnel temporarily, verify the VM listener is loopback-only, and make an authenticated DeepSeek status-only request through it.

- [ ] **Step 2: Install persistent identities and services**

Create a dedicated tunnel key/account, constrain remote forwarding to `127.0.0.1:18080`, validate sshd configuration before reload, install two launchd agents, and update the owner-only agent config.

- [ ] **Step 3: Deploy the rebuilt agent and verify recovery**

Install the binary with rollback backup, reload launchd components, and prove that stopping and restarting them restores the route.

- [ ] **Step 4: Run live acceptance**

Run the opt-in DeepSeek, two-dialog, Telegram MCP, and confirmed-cron live tests. Verify the temporary cron job is removed.

- [ ] **Step 5: Record sanitized evidence and commit**

Update `docs/day18-results.md` with status-only evidence and no secrets or user content.

Commit: `chore: verify Mac-routed DeepSeek access`

### Task 4: Route Telegram DC through the fixed Mac tunnel

**Files:**
- Modify: `telegram_mcp/src/telegram_mcp/config.py`
- Modify: `telegram_mcp/src/telegram_mcp/telegram.py`
- Modify: `telegram_mcp/tests/test_config.py`
- Modify: `telegram_mcp/tests/test_telegram.py`
- Modify: `deploy/light-agent/README.md`
- Modify: `tests/deployment_contract.rs`

**Interfaces:**
- Consumes: the existing dedicated reverse-tunnel identity and the Telegram session's original DC address/port.
- Produces: optional `TELEGRAM_RELAY_PORT=18082`, which changes only the Telethon connection endpoint to `127.0.0.1:18082` while preserving the original DC id and auth key.

- [ ] **Step 1: Write failing configuration and connection tests**

Require the relay port to be absent or exactly a valid nonzero TCP port; prove that enabling it calls `StringSession.set_dc` with the original DC id, loopback host, and configured port before constructing `TelegramClient`.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `telegram_mcp/.venv/bin/python -m pytest telegram_mcp/tests/test_config.py telegram_mcp/tests/test_telegram.py -q`.

- [ ] **Step 3: Implement the minimal fixed relay override**

Parse only `TELEGRAM_RELAY_PORT`; the relay host is fixed in code to `127.0.0.1`. Do not persist a rewritten Telegram session string or log its original DC target.

- [ ] **Step 4: Document and contract-test the second fixed reverse forward**

The VM listener is only `127.0.0.1:18082`; the launchd SSH command maps it to the session's current Telegram DC through the Mac. The tunnel account may listen only on ports 18080 and 18082. This is not a general SOCKS proxy.

- [ ] **Step 5: Verify, review, deploy, and run live acceptance**

Run all deterministic suites, deploy the two Python modules with rollback copies, extend the constrained key/sshd policy and launchd tunnel, restart Telegram MCP, prove DC connectivity through loopback, then rerun the read/copy-to-Saved live tests. Record only status evidence.

Commit: `feat: route Telegram MCP through Mac`
