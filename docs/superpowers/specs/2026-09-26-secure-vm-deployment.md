# Secure VM deployment specification

## Objective

Prepare a single-user VM for the `aichallenge` Day-17 stack: the Rust
`deepseek-cli` and the loopback-only Python Telegram MCP.

## Security requirements

- Use a currently supported Ubuntu LTS release; target Ubuntu 26.04 LTS.
- Preserve a tested recovery path while changing SSH or firewall rules.
- Administrative access is limited to the owner's Mac through a private
  WireGuard-based network; no public SSH or public MCP listener remains.
- Password login and root SSH login are disabled. Only the existing owner
  Ed25519 identity may administer the host.
- The Telegram MCP remains bound to `127.0.0.1:8000`.
- Run the MCP as a dedicated unprivileged system account under a sandboxed
  systemd unit.
- Inbound traffic is denied by default. Outbound traffic is limited to the
  protocols and destinations needed for private administration, DNS, time
  synchronization, OS security updates, source/dependency retrieval,
  DeepSeek, and Telegram.
- Do not claim that a packet firewall can reliably pin CDN-backed services to
  permanent IP addresses. Enforce hostname policy through a proxy where the
  application supports it and document unavoidable dynamic exceptions.
- Do not place Telegram or DeepSeek credentials in Git, shell history,
  command-line arguments, images, or payload logs.
- The assistant prepares credential locations and permissions without seeing
  real credential values. The owner provisions secrets after temporary
  assistant access is removed.
- Keep `debug.log_payloads = false`.
- Enable automatic security updates, time synchronization, log rotation, and
  an auditable record of host configuration changes.

## Deployment requirements

- Clone `https://github.com/lexain12/aichallenge.git` branch `Day-17` and pin
  the initial deployment to commit
  `ae88ad7f188f860e6ac2e126a7f529c7116d0418`.
- Build Rust dependencies from `Cargo.lock` and Python dependencies from
  `telegram_mcp/uv.lock` without silently refreshing lock files.
- Run the repository's Rust and Python test suites before installation.
- Install the release Rust binary and a frozen Python environment.
- Keep persistent CLI state outside the source checkout with restrictive
  permissions.
- Reboot once at the end and repeat access, firewall, service, and application
  checks.

## Operational boundary

The VM is hardened against ordinary internet exposure and credential misuse;
it is not represented as secure against a malicious cloud hypervisor or an
already-compromised owner device. Exact destination-only egress for DeepSeek,
Telegram, Tailscale, Ubuntu mirrors, GitHub, crates.io, and PyPI requires
ongoing maintenance because several use changing CDN or service addresses.
