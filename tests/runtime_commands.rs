mod common;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};

use chrono::Utc;
use chrono_tz::Europe::Moscow;
use deepseek_cli::domain::RequestId;
use deepseek_cli::domain::{CronRunStatus, ToolOwner, ToolRunStatus};
use deepseek_cli::protocol::{ClientRequest, PROTOCOL_VERSION, RequestEnvelope};
use deepseek_cli::runtime::ProcessLease;
use deepseek_cli::scheduler::ScheduleSpec;
use deepseek_cli::store::{
    CronRunFinish, JobCreate, RunClaim, SafeErrorCode, Store, StoreError, ToolRunFinish,
    ToolRunStart,
};

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

#[cfg(unix)]
#[test]
fn sigterm_cancels_and_joins_cron_sync_sqlite_worker() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};

    let directory = common::private_tempdir();
    let database = directory.path().join("agent.sqlite3");
    let store = Store::open(&database).unwrap();
    let dialog = store.create_dialog("cron sync signal").unwrap().id;
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "pending sync".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "never install".into(),
        })
        .unwrap();
    let marker = directory.path().join("validation-ready");
    let release = directory.path().join("validation-release");
    let crontab = directory.path().join("fake-crontab");
    fs::write(
        &crontab,
        format!(
            "#!/bin/sh\ncase \"$1\" in\n  -V) printf 'cronie 1.7.2\\n' ;;\n  -T) [ \"$#\" -eq 2 ] && cat \"$2\" >/dev/null || exit 8; touch '{}'; while [ ! -f '{}' ]; do sleep 0.01; done ;;\n  *) exit 9 ;;\nesac\n",
            marker.display(),
            release.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&crontab, fs::Permissions::from_mode(0o700)).unwrap();
    let config = directory.path().join("server.toml");
    fs::write(
        &config,
        format!(
            r#"
[provider]
api_key = "test-key"

[database]
path = "{}"

[scheduler]
lock_path = "{}"
binary_path = "/opt/light-agent/bin/light-agent"
crontab_binary = "{}"
run_timeout_seconds = 5
"#,
            database.display(),
            directory.path().join("cron.lock").display(),
            crontab.display(),
        ),
    )
    .unwrap();

    let mut child = Command::new(server_binary())
        .args(["--config", config.to_str().unwrap(), "cron-sync"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let ready_deadline = Instant::now() + Duration::from_secs(5);
    while !marker.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "cron-sync exited before crontab validation"
        );
        assert!(
            Instant::now() < ready_deadline,
            "cron preflight did not start"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let blocker = rusqlite::Connection::open(&database).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    fs::write(&release, "go").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let exit_deadline = Instant::now() + Duration::from_secs(2);
    while child.try_wait().unwrap().is_none() {
        assert!(
            Instant::now() < exit_deadline,
            "cron-sync abandoned a locked SQLite worker"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(blocker);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        store.get_job(job.id).unwrap().sync_state,
        deepseek_cli::domain::JobSyncState::Pending
    );
}

#[test]
fn serve_stdio_stdout_contains_protocol_lines_only() {
    let directory = common::private_tempdir();
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
    assert_eq!(event["protocol_version"], 2);
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

fn insert_v3_style_pending_tool(
    database: &std::path::Path,
    owner_id: i64,
    call_id: &str,
    read_only: bool,
) {
    rusqlite::Connection::open(database)
        .unwrap()
        .execute(
            "INSERT INTO tool_runs(owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,runtime_owner_id)
             VALUES('interactive_turn',?1,?2,'v3_fixture','call',?3,'pending',NULL)",
            rusqlite::params![owner_id, call_id, read_only],
        )
        .unwrap();
}

fn insert_v3_style_pending_cron_tool(
    database: &std::path::Path,
    owner_id: i64,
    call_id: &str,
    read_only: bool,
) {
    rusqlite::Connection::open(database)
        .unwrap()
        .execute(
            "INSERT INTO tool_runs(owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,runtime_owner_id)
             VALUES('cron_run',?1,?2,'v3_fixture','call',?3,'pending',NULL)",
            rusqlite::params![owner_id, call_id, read_only],
        )
        .unwrap();
}

#[test]
fn second_live_server_never_recovers_the_first_process_work() {
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
    let directory = common::private_tempdir();
    let (config, store) = write_server_config_with_provider(&directory, &provider_url);
    let (mut first, _first_stdout) = start_server(&config);
    let dialog = store.create_dialog("live").unwrap();
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: RequestId::new(),
        request: ClientRequest::SendMessage {
            dialog_id: dialog.id,
            message: "still running".into(),
        },
    };
    let stdin = first.stdin.as_mut().unwrap();
    serde_json::to_writer(&mut *stdin, &request).unwrap();
    stdin.write_all(b"\n").unwrap();
    stdin.flush().unwrap();
    accepted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let turn_id = rusqlite::Connection::open(directory.path().join("agent.sqlite3"))
        .unwrap()
        .query_row("SELECT id FROM turns", [], |row| row.get::<_, i64>(0))
        .unwrap();
    let turn_id = deepseek_cli::domain::TurnId::new(turn_id).unwrap();

    let (second, _second_stdout) = start_server(&config);
    assert_eq!(turn_status(&directory, turn_id), "pending");
    stop_server(second);
    assert_eq!(turn_status(&directory, turn_id), "pending");
    stop_server(first);
    let _ = release_tx.send(());
    listener_thread.join().unwrap();
    assert_eq!(turn_status(&directory, turn_id), "interrupted");
}

#[test]
fn initialized_owner_registry_rejects_new_unowned_work() {
    let directory = common::private_tempdir();
    let database = directory.path().join("agent.sqlite3");
    let store = Store::open(&database).unwrap();
    let first = ProcessLease::acquire(&store).unwrap();
    drop(first);

    let dialog = store.create_dialog("unowned").unwrap();
    assert_eq!(
        store.begin_turn(dialog.id, "not attributable"),
        Err(deepseek_cli::store::StoreError::InvalidOwner)
    );
    let second = ProcessLease::acquire(&store).unwrap();

    assert_eq!(second.recovery_report().turns, 0);
}

#[test]
fn existing_coordination_marker_recovers_missing_terminal_and_unowned_null_tools() {
    let directory = common::private_tempdir();
    let database = directory.path().join("agent.sqlite3");
    let store = Store::open(&database).unwrap();
    let terminal_dialog = store.create_dialog("terminal parent").unwrap();
    let terminal_turn = store.begin_turn(terminal_dialog.id, "finished").unwrap();
    store.complete_turn(terminal_turn.turn_id, "done").unwrap();
    let initial = ProcessLease::acquire(&store).unwrap();
    drop(initial);

    insert_v3_style_pending_tool(&database, 999_999, "missing_parent", false);
    insert_v3_style_pending_tool(
        &database,
        terminal_turn.turn_id.get(),
        "terminal_parent",
        true,
    );
    let unowned_dialog = store.create_dialog("unowned parent").unwrap();
    let db = rusqlite::Connection::open(&database).unwrap();
    db.execute(
        "INSERT INTO turns(dialog_id,status,started_at,runtime_owner_id) VALUES(?,'pending',?,NULL)",
        rusqlite::params![unowned_dialog.id.get(), Utc::now().to_rfc3339()],
    )
    .unwrap();
    let unowned_turn = db.last_insert_rowid();
    drop(db);
    insert_v3_style_pending_tool(&database, unowned_turn, "unowned_parent", false);

    let recovered = ProcessLease::acquire(&store).unwrap();
    assert_eq!(recovered.recovery_report().tool_runs, 3);
    let tools = store.list_tool_runs().unwrap();
    assert_eq!(tools[0].status, ToolRunStatus::Uncertain);
    assert_eq!(tools[1].status, ToolRunStatus::Failed);
    assert_eq!(tools[2].status, ToolRunStatus::Uncertain);
    assert_eq!(
        turn_status(
            &directory,
            deepseek_cli::domain::TurnId::new(unowned_turn).unwrap()
        ),
        "interrupted"
    );
}

#[test]
fn v3_style_null_tool_follows_live_then_dead_parent_owner() {
    let directory = common::private_tempdir();
    let database = directory.path().join("agent.sqlite3");
    let store = Store::open(&database).unwrap();
    let owner = ProcessLease::acquire(&store).unwrap();
    let owned = owner.owned_store(&store).unwrap();
    let dialog = store.create_dialog("rolling v3 writer").unwrap();
    let turn = owned.begin_turn(dialog.id, "question").unwrap();
    insert_v3_style_pending_tool(&database, turn.turn_id.get(), "v3_live", false);

    let concurrent = ProcessLease::acquire(&store).unwrap();
    assert_eq!(concurrent.recovery_report().tool_runs, 0);
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending
    );
    drop(concurrent);
    drop(owned);
    drop(owner);

    let recovered = ProcessLease::acquire(&store).unwrap();
    assert_eq!(recovered.recovery_report().tool_runs, 1);
    assert_eq!(recovered.recovery_report().turns, 1);
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Uncertain
    );
    assert_eq!(turn_status(&directory, turn.turn_id), "interrupted");
}

#[test]
fn v3_style_null_cron_tool_follows_live_then_dead_parent_owner() {
    let directory = common::private_tempdir();
    let database = directory.path().join("agent.sqlite3");
    let store = Store::open(&database).unwrap();
    let owner = ProcessLease::acquire(&store).unwrap();
    let owned = owner.owned_store(&store).unwrap();
    let dialog = store.create_dialog("rolling v3 cron writer").unwrap();
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog.id,
            name: "v3 cron".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "work".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = owned.claim_run(job.id, Utc::now()).unwrap() else {
        panic!("claim")
    };
    insert_v3_style_pending_cron_tool(&database, claim.run.id.get(), "v3_cron_live", true);

    let concurrent = ProcessLease::acquire(&store).unwrap();
    assert_eq!(concurrent.recovery_report().tool_runs, 0);
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending
    );
    drop(concurrent);
    drop(owned);
    drop(owner);

    let recovered = ProcessLease::acquire(&store).unwrap();
    assert_eq!(recovered.recovery_report().tool_runs, 1);
    assert_eq!(recovered.recovery_report().cron_runs, 1);
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Failed
    );
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Interrupted
    );
}

#[test]
fn null_tool_with_unverifiable_nonnull_parent_owner_is_not_recovered() {
    let directory = common::private_tempdir();
    let database = directory.path().join("agent.sqlite3");
    let store = Store::open(&database).unwrap();
    let initial = ProcessLease::acquire(&store).unwrap();
    let dialog = store.create_dialog("unverifiable owner").unwrap();
    let db = rusqlite::Connection::open(&database).unwrap();
    db.pragma_update(None, "foreign_keys", false).unwrap();
    db.execute(
        "INSERT INTO turns(dialog_id,status,started_at,runtime_owner_id) VALUES(?,'pending',?,?)",
        rusqlite::params![
            dialog.id.get(),
            Utc::now().to_rfc3339(),
            "123e4567-e89b-42d3-a456-426614174000"
        ],
    )
    .unwrap();
    let turn_id = db.last_insert_rowid();
    drop(db);
    insert_v3_style_pending_tool(&database, turn_id, "unverifiable_owner", true);

    let concurrent = ProcessLease::acquire(&store).unwrap();
    assert_eq!(concurrent.recovery_report().tool_runs, 0);
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending
    );
    drop(concurrent);
    drop(initial);
}

#[test]
fn one_live_runtime_owner_cannot_mutate_another_owners_active_work() {
    let directory = common::private_tempdir();
    let store = Store::open(directory.path().join("agent.sqlite3")).unwrap();
    let owner_a = ProcessLease::acquire(&store).unwrap();
    let store_a = owner_a.owned_store(&store).unwrap();
    let owner_b = ProcessLease::acquire(&store).unwrap();
    let store_b = owner_b.owned_store(&store).unwrap();

    let dialog = store.create_dialog("owner fence").unwrap();
    let turn = store_a.begin_turn(dialog.id, "question").unwrap();
    assert_eq!(
        store_b.complete_turn(turn.turn_id, "stolen"),
        Err(StoreError::InvalidOwner)
    );
    assert_eq!(
        store_b.start_tool_run(ToolRunStart {
            owner: ToolOwner::InteractiveTurn(turn.turn_id),
            call_id: "foreign".into(),
            server_name: "fixture".into(),
            tool_name: "read".into(),
            read_only: true,
            arguments: "{}".into(),
        }),
        Err(StoreError::InvalidOwner)
    );
    let tool_id = store_a
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::InteractiveTurn(turn.turn_id),
            call_id: "owned".into(),
            server_name: "fixture".into(),
            tool_name: "read".into(),
            read_only: true,
            arguments: "{}".into(),
        })
        .unwrap();
    assert_eq!(
        store_b.finish_tool_run(tool_id, ToolRunFinish::completed()),
        Err(StoreError::InvalidOwner)
    );
    store_a
        .finish_tool_run(tool_id, ToolRunFinish::completed())
        .unwrap();
    store_a.complete_turn(turn.turn_id, "answer").unwrap();

    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog.id,
            name: "owned cron".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "run".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = store_a.claim_run(job.id, Utc::now()).unwrap() else {
        panic!("owner A must claim")
    };
    assert_eq!(
        store_b.finish_run(claim.run.id, CronRunFinish::completed("stolen")),
        Err(StoreError::InvalidOwner)
    );
    store_a
        .finish_run(claim.run.id, CronRunFinish::completed("done"))
        .unwrap();
}

#[test]
fn owned_store_clone_keeps_the_runtime_lock_alive_after_lease_drop() {
    let directory = common::private_tempdir();
    let store = Store::open(directory.path().join("agent.sqlite3")).unwrap();
    let lease = ProcessLease::acquire(&store).unwrap();
    let owned = lease.owned_store(&store).unwrap();
    let survivor = owned.clone();
    let dialog = store.create_dialog("capability lifetime").unwrap();
    let turn = owned.begin_turn(dialog.id, "question").unwrap();

    drop(owned);
    drop(lease);
    let concurrent = ProcessLease::acquire(&store).unwrap();
    assert_eq!(concurrent.recovery_report().turns, 0);
    assert_eq!(turn_status(&directory, turn.turn_id), "pending");

    survivor
        .interrupt_turn(turn.turn_id, SafeErrorCode::Interrupted)
        .unwrap();
    drop(survivor);
}

#[test]
fn runtime_capability_cannot_be_rebound_to_a_different_database() {
    let first_directory = common::private_tempdir();
    let second_directory = common::private_tempdir();
    let first_path = first_directory.path().join("agent.sqlite3");
    let second_path = second_directory.path().join("agent.sqlite3");
    let first = Store::open(&first_path).unwrap();
    let second = Store::open(&second_path).unwrap();
    let lease = ProcessLease::acquire(&first).unwrap();

    let owner: (String, String, i64, i64, String) = rusqlite::Connection::open(&first_path)
        .unwrap()
        .query_row(
            "SELECT owner_id,lock_name,lock_device,lock_inode,created_at FROM runtime_owners",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    rusqlite::Connection::open(&second_path)
        .unwrap()
        .execute(
            "INSERT INTO runtime_owners(owner_id,lock_name,lock_device,lock_inode,created_at) VALUES(?,?,?,?,?)",
            rusqlite::params![owner.0, owner.1, owner.2, owner.3, owner.4],
        )
        .unwrap();

    assert!(lease.owned_store(&second).is_err());
}

#[cfg(unix)]
#[test]
fn runtime_coordination_rejects_a_symlink_lock() {
    let directory = common::private_tempdir();
    let database = directory.path().join("agent.sqlite3");
    let store = Store::open(&database).unwrap();
    let target = directory.path().join("attacker-controlled");
    std::fs::File::create(&target).unwrap();
    let coordinator = database.with_file_name("agent.sqlite3.runtime.lock");
    std::os::unix::fs::symlink(target, coordinator).unwrap();

    assert!(ProcessLease::acquire(&store).is_err());
}

#[test]
fn expired_startup_budget_creates_no_database_or_runtime_sidecars() {
    let directory = common::private_tempdir();
    let database = directory.path().join("expired.sqlite3");
    let cancellation = tokio_util::sync::CancellationToken::new();
    assert!(matches!(
        Store::open_with_deadline(&database, std::time::Instant::now(), &cancellation,),
        Err(StoreError::Busy)
    ));
    assert!(!database.exists());

    let live_database = directory.path().join("live.sqlite3");
    let store = Store::open(&live_database).unwrap();
    assert!(
        ProcessLease::acquire_with_deadline(&store, std::time::Instant::now(), &cancellation,)
            .is_err()
    );
    assert!(
        std::fs::read_dir(directory.path())
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().contains(".runtime."))
    );
}

#[cfg(unix)]
#[test]
fn simultaneous_startups_register_distinct_owners_and_reap_only_the_crashed_one() {
    let directory = common::private_tempdir();
    let (config, _store) = write_server_config(&directory);
    let spawn = || {
        Command::new(server_binary())
            .args(["--config", config.to_str().unwrap(), "serve-stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut first = spawn();
    let mut second = spawn();
    for child in [&mut first, &mut second] {
        let mut hello = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut hello)
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&hello).unwrap()["event"]["type"],
            "hello"
        );
    }
    let owner_count = || {
        rusqlite::Connection::open(directory.path().join("agent.sqlite3"))
            .unwrap()
            .query_row("SELECT count(*) FROM runtime_owners", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
    };
    let owner_lock_count = || {
        std::fs::read_dir(directory.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .contains(".runtime.owner.")
            })
            .count()
    };
    assert_eq!(owner_count(), 2);
    assert_eq!(owner_lock_count(), 2);
    assert!(
        Command::new("/bin/kill")
            .args(["-KILL", &first.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let _ = first.wait().unwrap();

    let (third, _stdout) = start_server(&config);
    assert_eq!(owner_count(), 2);
    assert_eq!(owner_lock_count(), 2);
    stop_server(second);
    stop_server(third);
}

#[test]
fn exclusive_startup_recovers_cron_run_and_tools_after_simulated_sigkill() {
    let directory = common::private_tempdir();
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
                arguments: "{}".into(),
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
    let next_owner = ProcessLease::acquire(&store).unwrap();
    let owned_store = next_owner.owned_store(&store).unwrap();
    assert!(matches!(
        owned_store
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

    let directory = common::private_tempdir();
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

    // A healthy unrelated server must not delay recovery of a provably dead
    // cron owner. The next startup scans per-owner locks and repairs only the
    // killed runner's rows.
    let (second_server, _second_stdout) = start_server(&config);
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Interrupted
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
fn run_job_deadline_includes_mcp_discovery() {
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mcp_url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let listener_thread = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        accepted_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(Duration::from_secs(5));
    });

    let directory = common::private_tempdir();
    let config = directory.path().join("server.toml");
    let database = directory.path().join("agent.sqlite3");
    let lock = directory.path().join("cron.lock");
    fs::write(
        &config,
        format!(
            r#"
[provider]
api_key = "test-key"

[database]
path = "{}"

[mcp]
connect_timeout_seconds = 30

[[mcp.servers]]
name = "hanging"
url = "{}"

[scheduler]
run_timeout_seconds = 1
lock_path = "{}"
binary_path = "/opt/light-agent/bin/light-agent"
crontab_binary = "/usr/bin/crontab"
"#,
            database.display(),
            mcp_url,
            lock.display(),
        ),
    )
    .unwrap();
    assert_eq!(
        deepseek_cli::settings::ServerSettings::load(&config, None)
            .unwrap()
            .scheduler()
            .run_timeout(),
        Duration::from_secs(1)
    );
    let store = Store::open(&database).unwrap();
    let dialog = store.create_dialog("deadline").unwrap().id;
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "deadline".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "must not run".into(),
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
    accepted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let accepted_at = Instant::now();
    let wait_deadline = Instant::now() + Duration::from_millis(1_500);
    let status = loop {
        if let Some(status) = runner.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= wait_deadline {
            let _ = runner.kill();
            let _ = runner.wait();
            let _ = release_tx.send(());
            listener_thread.join().unwrap();
            panic!("MCP discovery escaped the complete run deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let _ = release_tx.send(());
    listener_thread.join().unwrap();
    assert!(!status.success());
    assert!(accepted_at.elapsed() < Duration::from_millis(1_500));
    assert!(store.list_runs(job.id).unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn sigterm_interrupts_run_job_while_provider_is_blocked() {
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let provider_url = format!("http://{}", listener.local_addr().unwrap());
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let listener_thread = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        accepted_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(Duration::from_secs(3));
    });
    let directory = common::private_tempdir();
    let (config, store) = write_server_config_with_provider(&directory, &provider_url);
    let dialog = store.create_dialog("signal cron").unwrap().id;
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "signal cron".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "wait".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let mut child = Command::new(server_binary())
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
    accepted_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(1);
    while child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "run-job ignored SIGTERM");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Interrupted
    );
    let _ = release_tx.send(());
    listener_thread.join().unwrap();
}

#[cfg(unix)]
#[test]
fn sigterm_interrupts_run_job_while_finalization_is_locked() {
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let provider_url = format!("http://{}", listener.local_addr().unwrap());
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let listener_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        accepted_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(Duration::from_secs(3));
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
        stream.flush().unwrap();
    });
    let directory = common::private_tempdir();
    let (config, store) = write_server_config_with_provider(&directory, &provider_url);
    let dialog = store.create_dialog("finalize signal").unwrap().id;
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "finalize signal".into(),
            schedule: ScheduleSpec::parse_cron("* * * * *", Moscow).unwrap(),
            prompt: "finish".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let mut child = Command::new(server_binary())
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
    accepted_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let blocker = rusqlite::Connection::open(directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    release_tx.send(()).unwrap();
    listener_thread.join().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(1);
    while child.try_wait().unwrap().is_none() {
        assert!(
            Instant::now() < deadline,
            "locked run finalization ignored SIGTERM"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(blocker);
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Pending
    );
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
        let directory = common::private_tempdir();
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
