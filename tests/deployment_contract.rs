use std::path::Path;

use deepseek_cli::scheduler::RUNTIME_HOME;

const RUNBOOK: &str = include_str!("../deploy/light-agent/README.md");
const SERVER_EXAMPLE: &str = include_str!("../light-agent.example.toml");
const AUTHORIZED_KEY: &str = include_str!("../deploy/light-agent/authorized_keys.example");
const GITIGNORE: &str = include_str!("../.gitignore");
const TELEGRAM_CONFIG: &str = include_str!("../telegram_mcp/src/telegram_mcp/config.py");
const RESULTS: &str = include_str!("../docs/day18-results.md");

fn deployment_file(path: &str) -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|error| panic!("missing deployment artifact {path}: {error}"))
}

#[test]
fn mac_deepseek_runbook_documents_a_persistent_loopback_route() {
    for required in [
        "127.0.0.1:18081",
        "127.0.0.1:18080",
        "api.deepseek.com:443",
        "http://127.0.0.1:18080",
        "ssh -N -T",
        "-R 127.0.0.1:18080:127.0.0.1:18081",
        "ExitOnForwardFailure=yes",
        "ServerAliveInterval=30",
        "ServerAliveCountMax=3",
        "launchctl bootstrap",
        "launchctl kickstart",
        "KeepAlive",
        "dedicated tunnel identity",
        "Mac is off or asleep",
        "no direct fallback",
    ] {
        assert!(
            RUNBOOK.contains(required),
            "Mac route runbook missing {required}"
        );
    }
    for required in [
        "local.light-agent.deepseek-proxy",
        "local.light-agent.deepseek-tunnel",
        "<key>ProgramArguments</key>",
        "<key>RunAtLoad</key><true/>",
        "<key>KeepAlive</key><true/>",
        "plutil -lint ~/Library/LaunchAgents/local.light-agent.deepseek-proxy.plist",
        "plutil -lint ~/Library/LaunchAgents/local.light-agent.deepseek-tunnel.plist",
        "launchctl print gui/$(id -u)/local.light-agent.deepseek-tunnel",
    ] {
        assert!(
            RUNBOOK.contains(required),
            "Mac launchd runbook missing {required}"
        );
    }
}

#[test]
fn telegram_relay_runbook_limits_second_forward_to_one_dc_endpoint() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "TELEGRAM_RELAY_PORT=18082",
        "127.0.0.1:18082",
        "-R 127.0.0.1:18082:<OPERATOR_RESOLVED_TELEGRAM_DC_IP>:<TELEGRAM_DC_PORT>",
        "PermitListen 127.0.0.1:18080 127.0.0.1:18082",
        "permitlisten=\"127.0.0.1:18080\",permitlisten=\"127.0.0.1:18082\"",
        "not a SOCKS or general-purpose proxy",
    ] {
        assert!(
            runbook.contains(required),
            "Telegram relay runbook missing {required}"
        );
    }
}

#[test]
fn deployment_paths_make_fixed_commands_resolve_the_default_config() {
    assert_eq!(RUNTIME_HOME, "/var/lib/light-agent");
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "home directory is exactly `/var/lib/light-agent`",
        "`/var/lib/light-agent/light-agent.toml`",
        "`/var/lib/light-agent/state.sqlite3`",
        "mode `0700`",
        "mode `0600`",
        "working directory is `/var/lib/light-agent`",
        "HOME=/var/lib/light-agent",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
    assert!(SERVER_EXAMPLE.contains("path = \"/var/lib/light-agent/state.sqlite3\""));
    assert!(!SERVER_EXAMPLE.contains("lock_path"));
    assert!(AUTHORIZED_KEY.contains("command=\"/opt/light-agent/bin/light-agent serve-stdio\""));
    assert!(!AUTHORIZED_KEY.contains("--config"));
}

#[test]
fn runbook_documents_owner_aware_recovery_and_reconciliation() {
    for required in [
        "live owner",
        "read-only tool",
        "write tool",
        "interrupted",
        "process_interrupted",
        "cron-sync",
        "reboot",
        "restore",
    ] {
        assert!(RUNBOOK.contains(required), "runbook missing {required}");
    }
}

#[test]
fn runbook_uses_a_cleaned_owner_only_file_for_cronie_validation() {
    for required in [
        "mktemp",
        "chmod 0600",
        "trap 'rm -f -- \"$validation_file\"'",
        "/usr/bin/crontab -T \"$validation_file\"",
    ] {
        assert!(RUNBOOK.contains(required), "runbook missing {required}");
    }
    assert!(!RUNBOOK.contains("crontab -T -"));
    assert!(GITIGNORE.contains("*.light-agent-crontab-*.part"));
}

#[test]
fn telegram_mcp_systemd_unit_is_loopback_service_with_secrets_kept_external() {
    let unit = deployment_file("deploy/light-agent/telegram-mcp.service");
    for required in [
        "User=telegram-mcp",
        "Group=telegram-mcp",
        "WorkingDirectory=/opt/telegram-mcp/site",
        "EnvironmentFile=/etc/telegram-mcp/telegram-mcp.env",
        "Environment=PYTHONPATH=/opt/telegram-mcp/site",
        "ExecStart=/usr/bin/python3 -c \"from telegram_mcp.server import main; main()\"",
        "NoNewPrivileges=true",
        "PrivateTmp=true",
        "ProtectSystem=strict",
        "ProtectHome=true",
        "UMask=0077",
    ] {
        assert!(unit.contains(required), "systemd unit missing {required}");
    }
    for forbidden in [
        "TELEGRAM_API_ID=",
        "TELEGRAM_API_HASH=",
        "TELETHON_SESSION_STRING=",
        "0.0.0.0",
        ".venv",
        "/usr/local/bin/uv",
    ] {
        assert!(
            !unit.contains(forbidden),
            "systemd unit must not embed {forbidden}"
        );
    }
    assert!(
        TELEGRAM_CONFIG.contains(r#"host: str = field(default="127.0.0.1", init=False)"#),
        "Telegram MCP application bind address must remain fixed to loopback"
    );
}

#[test]
fn runbook_extracts_top_level_release_archive_and_verifies_loopback_service() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "top_level_count",
        "--strip-components=1",
        "/opt/telegram-mcp/site.new",
        "find /opt/telegram-mcp/site.new -type d -exec chmod 0755 {} +",
        "find /opt/telegram-mcp/site.new -type f -exec chmod 0644 {} +",
        "PYTHONPATH=/opt/telegram-mcp/site.new",
        "from telegram_mcp.server import main",
        "systemctl stop telegram-mcp.service",
        "mv -T /opt/telegram-mcp/site /opt/telegram-mcp/site.previous",
        "mv -T /opt/telegram-mcp/site.new /opt/telegram-mcp/site",
        "/etc/telegram-mcp/telegram-mcp.env",
        "systemd-analyze verify",
        "systemctl enable --now telegram-mcp.service",
        "127.0.0.1:8000",
        "must not show `0.0.0.0:8000`",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
    for forbidden in ["uv sync", ".venv", "/opt/light-agent/telegram_mcp"] {
        assert!(
            !runbook.contains(forbidden),
            "runbook retains unsupported deployment path {forbidden}"
        );
    }
}

#[test]
fn telegram_rollout_preserves_unit_environment_and_site_for_rollback() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "rollback=/root/telegram-mcp.rollback",
        "test ! -e \"$rollback\"",
        "install -d -o root -g root -m 0700 \"$rollback\"",
        "cp --archive --no-dereference \"$unit\" \"$rollback/telegram-mcp.service\"",
        "cp --archive --no-dereference \"$environment\" \"$rollback/telegram-mcp.env\"",
        "\"$rollback/unit.absent\"",
        "\"$rollback/environment.absent\"",
        "cp --archive --no-dereference \"$rollback/telegram-mcp.service\" \"$unit\"",
        "cp --archive --no-dereference \"$rollback/telegram-mcp.env\" \"$environment\"",
        "systemctl daemon-reload",
        "systemctl restart telegram-mcp.service",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
}

#[test]
fn telegram_rollout_records_and_restores_supported_service_state() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "supported existing states are exactly enabled+active or disabled+inactive",
        "--property=LoadState --value",
        "--property=FragmentPath --value",
        "--property=UnitFileState --value",
        "--property=ActiveState --value",
        "\"$rollback/service.absent\"",
        "\"$rollback/service.enabled\"",
        "\"$rollback/service.active\"",
        "\"$rollback/service.disabled\"",
        "\"$rollback/service.inactive\"",
        "systemctl enable telegram-mcp.service",
        "systemctl disable telegram-mcp.service",
        "systemctl restart telegram-mcp.service",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
}

#[test]
fn telegram_site_rollback_handles_the_between_renames_gap() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "rename gap",
        "`site.previous` exists but `site` is absent",
        "mv -T /opt/telegram-mcp/site.previous /opt/telegram-mcp/site",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
}

#[test]
fn telegram_archive_checksum_is_digest_only_and_bound_to_the_archive() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "digest-only file containing exactly 64 hexadecimal characters and one newline",
        "wc -c < \"$checksum_file\"",
        "sha256sum \"$archive\"",
        "test \"$actual_sha256\" = \"$expected_sha256\"",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
    assert!(
        !runbook.contains("sha256sum --check"),
        "an independent checksum manifest filename must not select the archive"
    );
}

#[test]
fn runbook_documents_reversible_ubuntu_cronie_package_transition() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "apt-get --simulate install cronie",
        "cron` and `ubuntu-standard` may be removed",
        "rollback=/root/light-agent-cronie.rollback",
        "test ! -e \"$rollback\"",
        "cp --archive --no-dereference /etc/crontab",
        "cp --archive --no-dereference /etc/cron.d",
        "cp --archive --no-dereference /var/spool/cron",
        "apt-get install cronie",
        "systemctl is-enabled cronie.service",
        "systemctl is-active cronie.service",
        "apt-get install cron ubuntu-standard",
        "systemctl stop cron.service",
        "systemctl enable --now cron.service",
        "cron=INSTALLED(",
        "ubuntu-standard=INSTALLED(",
        "cronie=ABSENT",
        "\"$rollback/cron.INSTALLED\"",
        "\"$rollback/ubuntu-standard.INSTALLED\"",
        "\"$rollback/cronie.ABSENT\"",
        "\"$rollback/cron.service.enabled\"",
        "\"$rollback/cron.service.active\"",
        "require an operator-specific plan",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
    for forbidden in [
        "apt-get install --reinstall",
        "2>/dev/null | sudo /usr/bin/tee",
    ] {
        assert!(
            !runbook.contains(forbidden),
            "runbook retains unsafe package transition {forbidden}"
        );
    }
}

#[test]
fn live_results_report_routed_acceptance_and_remaining_boundaries() {
    let results = RESULTS.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        results.contains("authenticated status request returned HTTP 200 through the route"),
        "results must record the routed status evidence"
    );
    assert!(
        results.contains("Mac being awake, logged in, and running the two LaunchAgents"),
        "results must retain the Mac availability dependency"
    );
    assert!(
        results.contains("Generic MCP writes still do not have an in-protocol confirmation gate"),
        "results must retain the generic MCP write confirmation boundary"
    );
    for forbidden in [
        "This isolates the remaining failure",
        "Until DeepSeek accepts the VM's egress address",
        "Until authenticated requests from the VM succeed",
    ] {
        assert!(
            !results.contains(forbidden),
            "results retain unsupported causal claim {forbidden}"
        );
    }
}

#[test]
fn runbook_requires_allowusers_validation_reload_and_rollback() {
    let runbook = RUNBOOK.split_whitespace().collect::<Vec<_>>().join(" ");
    for required in [
        "AllowUsers",
        "light-agent",
        "/usr/sbin/sshd -t",
        "/usr/sbin/sshd -T",
        "systemctl reload ssh",
        "Keep the current administrator session open",
        "rollback",
    ] {
        assert!(runbook.contains(required), "runbook missing {required}");
    }
}
