# Secure VM Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy and verify the Day-17 DeepSeek CLI and Telegram MCP on a supported, single-user VM with no public application or SSH ingress and tightly bounded outbound access.

**Architecture:** Reprovision onto Ubuntu 26.04 LTS, bootstrap through key-only SSH, then move administration to a WireGuard-based private network before closing public ingress. Build the pinned public GitHub revision, install the Telegram MCP as a loopback-only sandboxed service, and keep the interactive Rust CLI and its state separate from the source checkout.

**Tech Stack:** Ubuntu 26.04 LTS, OpenSSH, Tailscale/WireGuard, nftables or UFW, systemd, AppArmor, Rust/Cargo, Python 3, uv, Git, SQLite.

**Spec:** `docs/superpowers/specs/2026-09-26-secure-vm-deployment.md`

## Global Constraints

- Never remove the last tested administration path.
- Never print or request Telegram or DeepSeek secret values in chat or command output.
- Deploy branch `Day-17` pinned initially to `ae88ad7f188f860e6ac2e126a7f529c7116d0418`.
- Keep the MCP on `127.0.0.1:8000`; do not expose it through a public reverse proxy.
- Use lock files without updating them during deployment.
- Treat public cloud firewall and host firewall as independent layers.
- Do not promise permanent hostname allowlisting with static IP firewall rules.

## Review Focus

- A changed host key after reprovision must be verified out of band, never accepted blindly.
- Private-network enrollment or owner IP changes must not leave an untested public fallback indefinitely.
- Egress controls must not silently disable security updates, Telegram connectivity, or DeepSeek calls.
- Secrets, message payloads, and tool arguments must not appear in journald, debug logs, process arguments, or snapshots.
- Reboot and MCP restart behavior must be tested because the Rust MCP client connects once at CLI startup.

---

### Task 1: Replace the unsupported base operating system

**Files:**
- Inspect: `/etc/os-release`
- Inspect: `/etc/apt/sources.list.d/`

**Interfaces:**
- Produces: a clean Ubuntu 26.04 LTS host reachable by the owner's existing Ed25519 key.

- [ ] **Step 1: Record the current access fingerprint and empty-host inventory**

Run read-only SSH commands for host key fingerprints, users, disks, listeners, and the current owner key. Expected: only disposable host configuration is present; no application secrets exist.

- [ ] **Step 2: Reprovision through the provider control plane**

Select Ubuntu 26.04 LTS. Expected: the provider reports a successful reinstall and the VM boots with a new SSH host key.

- [ ] **Step 3: Verify the new host key out of band before updating known_hosts**

Compare the provider-console fingerprint with `ssh-keyscan` output. Expected: exact ED25519 fingerprint match.

- [ ] **Step 4: Verify the supported release and owner access**

Run `cat /etc/os-release`, `id`, and `sudo -n true`. Expected: Ubuntu 26.04 LTS, user `user`, and working non-interactive sudo.

### Task 2: Establish private administration and close public ingress

**Files:**
- Create: `/etc/ssh/sshd_config.d/00-aichallenge-hardening.conf`
- Modify: host firewall rules
- Modify: private-network ACL policy outside the VM

**Interfaces:**
- Consumes: clean supported host from Task 1.
- Produces: owner-only SSH on the private interface and no public listening access.

- [ ] **Step 1: Capture the insecure baseline**

Run `ss -lntup`, `sshd -T`, and firewall status. Expected before changes: public SSH is reachable; capture this as the negative baseline.

- [ ] **Step 2: Install SSH key-only hardening**

Set `PermitRootLogin no`, `PasswordAuthentication no`, `KbdInteractiveAuthentication no`, `AuthenticationMethods publickey`, `AllowUsers user`, disable X11/agent/TCP forwarding and tunnels, then run `sshd -t`. Expected: validation succeeds.

- [ ] **Step 3: Open a separate SSH session using the owner key**

Expected: the new session and `sudo -n true` succeed before any firewall restriction.

- [ ] **Step 4: Enroll the VM and owner Mac in the private network**

Use a narrowly scoped ACL permitting only the owner's identity/device to reach SSH. Expected: SSH succeeds through the private address from the Mac.

- [ ] **Step 5: Enable default-deny inbound firewall policy**

Allow established traffic, loopback, required ICMP, DHCP, and SSH only on the private interface. Expected: public port 22 is unreachable while private SSH remains available.

- [ ] **Step 6: Verify from a fresh public and private connection**

Expected: public SSH fails; private SSH succeeds; `127.0.0.1:8000` is not reachable externally.

### Task 3: Apply the operating-system security baseline

**Files:**
- Create: `/etc/apt/apt.conf.d/52aichallenge-unattended-upgrades`
- Create: `/etc/systemd/coredump.conf.d/aichallenge.conf`
- Create: `/etc/sysctl.d/60-aichallenge-hardening.conf`
- Track: `/etc` through `etckeeper`

**Interfaces:**
- Consumes: private administration from Task 2.
- Produces: patched host with automatic security maintenance and minimized runtime exposure.

- [ ] **Step 1: Record package, service, AppArmor, swap, and coredump baselines**

Expected: output is saved without secrets and identifies every enabled network service.

- [ ] **Step 2: Apply all supported security updates and reboot if required**

Expected: package operations succeed and private SSH returns after reboot.

- [ ] **Step 3: Configure unattended security updates and bounded automatic reboot policy**

Expected: `unattended-upgrades --dry-run --debug` completes without repository errors.

- [ ] **Step 4: Disable core dumps and preserve zero-swap operation**

Expected: `ulimit -c`/systemd configuration and `swapon --show` show no persistent secret-bearing dump path.

- [ ] **Step 5: Remove or disable only demonstrably unused services**

Keep provider-console and network dependencies needed for recovery. Expected: no unexpected public listeners and no failed units.

- [ ] **Step 6: Commit the baseline in etckeeper**

Expected: `/etc` changes are attributable and contain no application secrets.

### Task 4: Clone, test, and build the pinned application

**Files:**
- Create: `/srv/aichallenge/source/`
- Create: `/opt/aichallenge/bin/deepseek-cli`
- Create: `/opt/aichallenge/telegram-mcp/`

**Interfaces:**
- Consumes: patched host from Task 3.
- Produces: tested immutable application artifacts and a frozen Python environment.

- [ ] **Step 1: Install build prerequisites from signed Ubuntu repositories**

Expected: Git, compiler/linker prerequisites, Rust toolchain, Python tooling, and uv are version-reported without errors.

- [ ] **Step 2: Clone branch `Day-17` and verify the exact commit**

Run `git rev-parse HEAD`. Expected: `ae88ad7f188f860e6ac2e126a7f529c7116d0418`.

- [ ] **Step 3: Run the Rust test suite with the lock file**

Run `cargo test --locked`. Expected: exit 0.

- [ ] **Step 4: Build the Rust release binary with the lock file**

Run `cargo build --release --locked`. Expected: `target/release/deepseek-cli` exists and reports help successfully.

- [ ] **Step 5: Create the frozen Python environment and run tests**

Run `uv sync --project telegram_mcp --frozen --group dev` and `uv run --project telegram_mcp pytest telegram_mcp/tests -q`. Expected: exit 0.

- [ ] **Step 6: Install root-owned artifacts without build-tree write access**

Expected: checksums match the tested artifacts and service accounts cannot modify executables or configuration.

### Task 5: Install the loopback-only Telegram MCP service

**Files:**
- Create: `/etc/systemd/system/telegram-mcp.service`
- Create: `/etc/aichallenge/telegram-mcp.env.example`
- Create: `/var/lib/telegram-mcp/`

**Interfaces:**
- Consumes: installed Python artifact from Task 4.
- Produces: sandboxed MCP service definition awaiting owner-provisioned credentials.

- [ ] **Step 1: Create a locked, non-login `telegram-mcp` system account**

Expected: no password, shell, sudo membership, or writable application binary.

- [ ] **Step 2: Install a systemd service with filesystem, privilege, device, capability, and address-family restrictions**

Expected: `systemd-analyze security telegram-mcp.service` has no avoidable high-risk settings; the service binds only loopback.

- [ ] **Step 3: Install a root-only credential template without values**

Expected: mode `0600`, root ownership, placeholder names only, and no secrets in journald.

- [ ] **Step 4: Verify failure is safe while credentials are absent**

Expected: service does not start, emits only the missing variable name, and never opens port 8000.

### Task 6: Configure the interactive Rust CLI and durable state

**Files:**
- Create: `/home/user/.config/aichallenge/deepseek.toml`
- Create: `/home/user/.local/share/aichallenge/`
- Create: `/usr/local/bin/aichallenge`

**Interfaces:**
- Consumes: release binary from Task 4 and local MCP endpoint from Task 5.
- Produces: an owner-only CLI launcher with durable SQLite state and no embedded API key.

- [ ] **Step 1: Install a key-free configuration pointing at `http://127.0.0.1:8000/mcp`**

Expected: file mode prevents other local users from reading configuration or state.

- [ ] **Step 2: Install a launcher with explicit config and database paths**

Expected: `aichallenge --help` succeeds and no credential appears in `ps` output.

- [ ] **Step 3: Verify payload logging remains disabled**

Expected: `debug.log_payloads = false` and no debug log path is enabled by default.

### Task 7: Enforce and verify bounded egress

**Files:**
- Modify: host firewall output policy
- Create: proxy policy only if required by the selected enforcement design
- Create: `/etc/aichallenge/NETWORK-POLICY.md`

**Interfaces:**
- Consumes: installed services from Tasks 5 and 6.
- Produces: documented, testable runtime and maintenance egress policy.

- [ ] **Step 1: Resolve the required maintenance and runtime flows**

Inventory DNS, NTP, Ubuntu repositories, private-network control/relay, GitHub, crates.io, PyPI, DeepSeek, and Telegram endpoints. Expected: every allowed flow has an owner and reason.

- [ ] **Step 2: Separate build/update egress from steady-state runtime egress**

Expected: package/source endpoints are not required by the MCP service during normal operation.

- [ ] **Step 3: Apply default-deny output with explicit protocol/service exceptions**

Use hostname-aware proxy enforcement where supported and dynamic service exceptions where unavoidable. Expected: arbitrary test destinations are blocked without breaking DNS, NTP, updates, private administration, DeepSeek, or Telegram.

- [ ] **Step 4: Document operational renewal and failure behavior**

Expected: the owner can distinguish a provider outage from a stale egress allowlist and has a private recovery path.

### Task 8: Owner secret provisioning and end-to-end verification

**Files:**
- Populate after assistant access removal: `/etc/aichallenge/telegram-mcp.env`
- Populate after assistant access removal: owner-selected DeepSeek credential store

**Interfaces:**
- Consumes: hardened host, service definitions, and owner-held credentials.
- Produces: working end-to-end deployment without assistant access to secrets.

- [ ] **Step 1: Remove temporary assistant access and audit all authorized keys/users**

Expected: only the owner's key and intended system accounts remain.

- [ ] **Step 2: Have the owner provision secrets directly**

Expected: values never appear in chat, shell history, process arguments, Git, or logs.

- [ ] **Step 3: Start the MCP and verify loopback binding and Telegram authorization**

Expected: service is active, only `127.0.0.1:8000` listens, and a read-only Saved Messages query succeeds.

- [ ] **Step 4: Start a fresh CLI and verify one read-only tool call**

Expected: DeepSeek and MCP calls succeed; no write tool is used for the smoke test.

- [ ] **Step 5: Reboot and repeat security and functionality checks**

Expected: private SSH, automatic service startup, firewall policy, DNS/NTP, DeepSeek, Telegram, and SQLite persistence all pass after reboot.

- [ ] **Step 6: Produce the final access and recovery inventory**

Expected: the owner receives exact service names, paths, update procedure, credential rotation procedure, and recovery steps without secret values.
