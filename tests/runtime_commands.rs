use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};

fn server_binary() -> &'static str {
    env!("CARGO_BIN_EXE_light-agent")
}

fn client_binary() -> &'static str {
    env!("CARGO_BIN_EXE_light-agent-client")
}

#[test]
fn server_help_exposes_only_the_planned_commands_and_defaults() {
    let output = Command::new(server_binary())
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["serve-stdio", "run-job", "cron-sync", "db-shell"] {
        assert!(help.contains(command), "missing {command}: {help}");
    }
    assert!(help.contains("light-agent.toml"));
    assert!(!help.contains("listen"));
    assert!(!help.contains("daemon"));
}

#[test]
fn client_help_has_only_the_transport_config_entry_point() {
    let output = Command::new(client_binary())
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--config"));
    assert!(help.contains("light-agent-client.toml"));
    assert!(!help.contains("api-key"));
    assert!(!help.contains("database"));
}

#[test]
fn db_shell_requires_explicit_readonly_flag() {
    let output = Command::new(server_binary())
        .arg("db-shell")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--readonly"));
}

#[test]
fn malformed_job_id_is_rejected_before_configuration_or_database_access() {
    let output = Command::new(server_binary())
        .args([
            "--config",
            "/definitely/missing/SECRET-config.toml",
            "run-job",
            "not-a-canonical-job-id",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid"));
    assert!(!stderr.contains("SECRET-config"));
}

#[test]
fn configuration_failure_is_nonzero_bounded_and_safe() {
    let output = Command::new(server_binary())
        .args(["--config", "/definitely/missing/SECRET.toml", "cron-sync"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("configuration_error"));
    assert!(!stderr.contains("SECRET.toml"));
}

#[test]
fn serve_stdio_stdout_contains_protocol_lines_only() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("server.toml");
    let database = directory.path().join("agent.sqlite3");
    let lock = directory.path().join("cron.lock");
    fs::write(
        &config_path,
        format!(
            r#"
[provider]
api_key = "test-key"

[database]
path = "{}"

[scheduler]
lock_path = "{}"
binary_path = "/opt/light-agent/bin/light-agent"
crontab_binary = "/usr/bin/crontab"
"#,
            database.display(),
            lock.display(),
        ),
    )
    .unwrap();
    let mut child = Command::new(server_binary())
        .args(["--config", config_path.to_str().unwrap(), "serve-stdio"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().flush().unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{:?}", output.stderr);
    assert!(output.stderr.is_empty(), "{:?}", output.stderr);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "{stdout}");
    let event: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(event["protocol_version"], 1);
    assert_eq!(event["event"]["type"], "hello");
}
