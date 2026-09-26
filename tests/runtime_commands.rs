use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};

use chrono::Utc;
use chrono_tz::Europe::Moscow;
use deepseek_cli::domain::RequestId;
use deepseek_cli::domain::{CronRunStatus, ToolOwner, ToolRunStatus};
use deepseek_cli::protocol::{ClientRequest, PROTOCOL_VERSION, RequestEnvelope};
use deepseek_cli::scheduler::ScheduleSpec;
use deepseek_cli::store::{JobCreate, RunClaim, Store, ToolRunStart};

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

fn write_server_config(directory: &tempfile::TempDir) -> (std::path::PathBuf, Store) {
    write_server_config_with_provider(directory, "https://api.deepseek.com")
}

fn write_server_config_with_provider(
    directory: &tempfile::TempDir,
    provider_url: &str,
) -> (std::path::PathBuf, Store) {
    let config_path = directory.path().join("server.toml");
    let database = directory.path().join("agent.sqlite3");
    let lock = directory.path().join("cron.lock");
    fs::write(
        &config_path,
        format!(
            r#"
[provider]
api_key = "test-key"
base_url = "{}"

[database]
path = "{}"

[scheduler]
lock_path = "{}"
binary_path = "/opt/light-agent/bin/light-agent"
crontab_binary = "/usr/bin/crontab"
"#,
            provider_url,
            database.display(),
            lock.display(),
        ),
    )
    .unwrap();
    (config_path, Store::open(database).unwrap())
}

fn start_server(config: &std::path::Path) -> (Child, BufReader<std::process::ChildStdout>) {
    let mut child = Command::new(server_binary())
        .args(["--config", config.to_str().unwrap(), "serve-stdio"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut hello = String::new();
    stdout.read_line(&mut hello).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&hello).unwrap()["event"]["type"],
        "hello"
    );
    (child, stdout)
}

fn stop_server(mut child: Child) {
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
}

fn turn_status(directory: &tempfile::TempDir, turn_id: deepseek_cli::domain::TurnId) -> String {
    rusqlite::Connection::open(directory.path().join("agent.sqlite3"))
        .unwrap()
        .query_row(
            "SELECT status FROM turns WHERE id=?",
            [turn_id.get()],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn second_live_server_never_recovers_the_first_process_work() {
    let directory = tempfile::tempdir().unwrap();
    let (config, store) = write_server_config(&directory);
    let (first, _first_stdout) = start_server(&config);
    let dialog = store.create_dialog("live").unwrap();
    let turn = store.begin_turn(dialog.id, "still running").unwrap();

    let (second, _second_stdout) = start_server(&config);
    assert_eq!(turn_status(&directory, turn.turn_id), "pending");
    stop_server(second);
    assert_eq!(turn_status(&directory, turn.turn_id), "pending");
    stop_server(first);

    let (recovery, _stdout) = start_server(&config);
    stop_server(recovery);
    assert_eq!(turn_status(&directory, turn.turn_id), "interrupted");
}

#[test]
fn exclusive_startup_recovers_cron_run_and_tools_after_simulated_sigkill() {
    let directory = tempfile::tempdir().unwrap();
    let (config, store) = write_server_config(&directory);
    let dialog = store.create_dialog("cron").unwrap().id;
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "job".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "work".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = store.claim_run(job.id, Utc::now()).unwrap() else {
        panic!("claim")
    };
    for (call_id, read_only) in [("read", true), ("write", false)] {
        store
            .start_tool_run(ToolRunStart {
                owner: ToolOwner::CronRun(claim.run.id),
                call_id: call_id.into(),
                server_name: "fixture".into(),
                tool_name: call_id.into(),
                read_only,
            })
            .unwrap();
    }

    let (recovery, _stdout) = start_server(&config);
    stop_server(recovery);
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Interrupted
    );
    let tools = store.list_tool_runs().unwrap();
    assert_eq!(tools[0].status, ToolRunStatus::Failed);
    assert_eq!(tools[1].status, ToolRunStatus::Uncertain);
    assert!(matches!(
        store
            .claim_run(job.id, Utc::now() + chrono::Duration::minutes(1))
            .unwrap(),
        RunClaim::Claimed(_)
    ));
}

#[cfg(unix)]
#[test]
fn live_cron_process_is_not_recovered_by_concurrent_server_startups() {
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let provider_url = format!("http://{}", listener.local_addr().unwrap());
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let listener_thread = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        accepted_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(Duration::from_secs(5));
    });

    let directory = tempfile::tempdir().unwrap();
    let (config, store) = write_server_config_with_provider(&directory, &provider_url);
    let dialog = store.create_dialog("cron-live").unwrap().id;
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "live".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "wait for provider".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();

    let mut runner = Command::new(server_binary())
        .args([
            "--config",
            config.to_str().unwrap(),
            "run-job",
            &job.id.to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if accepted_rx.recv_timeout(Duration::from_secs(5)).is_err() {
        let status = runner.try_wait().unwrap();
        let _ = runner.kill();
        let _ = runner.wait();
        let mut stderr = String::new();
        runner
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        panic!("cron runner did not call provider; status={status:?}; stderr={stderr}");
    }
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Pending
    );

    let (first_server, _first_stdout) = start_server(&config);
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Pending
    );

    assert!(
        Command::new("/bin/kill")
            .args(["-KILL", &runner.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let _ = runner.wait().unwrap();
    let _ = release_tx.send(());
    listener_thread.join().unwrap();

    // A healthy server still owns a shared lifetime lease, so another
    // startup must not mistake the killed runner's row for globally orphaned
    // work while that server is alive.
    let (second_server, _second_stdout) = start_server(&config);
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Pending
    );
    stop_server(second_server);
    stop_server(first_server);

    let (recovery, _stdout) = start_server(&config);
    stop_server(recovery);
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Interrupted
    );
}

#[cfg(unix)]
#[test]
fn sigint_and_sigterm_cancel_active_turn_and_wait_for_durable_finish() {
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    for signal in ["-INT", "-TERM"] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let provider_url = format!("http://{}", listener.local_addr().unwrap());
        let (accepted_tx, accepted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let listener_thread = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            accepted_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(3));
        });
        let directory = tempfile::tempdir().unwrap();
        let (config, store) = write_server_config_with_provider(&directory, &provider_url);
        let dialog = store.create_dialog("signal").unwrap();
        let (mut child, _stdout) = start_server(&config);
        let request = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: RequestId::new(),
            request: ClientRequest::SendMessage {
                dialog_id: dialog.id,
                message: "wait".into(),
            },
        };
        let stdin = child.stdin.as_mut().unwrap();
        serde_json::to_writer(&mut *stdin, &request).unwrap();
        stdin.write_all(b"\n").unwrap();
        stdin.flush().unwrap();
        accepted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            Command::new("/bin/kill")
                .args([signal, &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let turn_id = rusqlite::Connection::open(directory.path().join("agent.sqlite3"))
            .unwrap()
            .query_row("SELECT id FROM turns", [], |row| row.get::<_, i64>(0))
            .unwrap();
        let turn_id = deepseek_cli::domain::TurnId::new(turn_id).unwrap();
        let durable_deadline = Instant::now() + Duration::from_secs(1);
        while turn_status(&directory, turn_id) != "interrupted" {
            assert!(
                Instant::now() < durable_deadline,
                "{signal} did not durably interrupt turn"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        // Let the inert fixture socket close after durable cancellation; the
        // real provider peer would observe the cancelled request directly.
        let _ = release_tx.send(());
        let deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let current = rusqlite::Connection::open(directory.path().join("agent.sqlite3"))
                    .unwrap()
                    .query_row("SELECT status FROM turns", [], |row| {
                        row.get::<_, String>(0)
                    })
                    .unwrap();
                let _ = child.kill();
                panic!("{signal} did not stop runtime; turn={current}");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(status.success(), "{signal}: {status}");
        assert_eq!(turn_status(&directory, turn_id), "interrupted", "{signal}");
        listener_thread.join().unwrap();
    }
}
