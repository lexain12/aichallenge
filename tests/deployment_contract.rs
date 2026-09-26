use deepseek_cli::scheduler::RUNTIME_HOME;

const RUNBOOK: &str = include_str!("../deploy/light-agent/README.md");
const SERVER_EXAMPLE: &str = include_str!("../light-agent.example.toml");
const AUTHORIZED_KEY: &str = include_str!("../deploy/light-agent/authorized_keys.example");

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
