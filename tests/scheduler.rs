mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use chrono_tz::{Europe::Moscow, US::Eastern};
use deepseek_cli::domain::JobSyncState;
use deepseek_cli::scheduler::{
    CronRenderer, CronSynchronizer, CrontabBackend, CrontabFuture, ScheduleSpec, SchedulerError,
    SyncReport, SystemCrontabBackend,
};
use deepseek_cli::store::{JobCreate, Store};
use tokio_util::sync::CancellationToken;

#[test]
fn cron_parser_accepts_numeric_lists_ranges_and_steps() {
    for expression in [
        "0 9 * * 1-5",
        "0,15,30,45 */2 1-31/2 1,6,12 0-7",
        "59 23 31 12 7",
    ] {
        let parsed = ScheduleSpec::parse_cron(expression, Moscow).unwrap();
        assert_eq!(parsed.timezone(), Moscow);
    }
}

#[test]
fn cron_parser_rejects_non_numeric_or_unsafe_and_out_of_range_fields() {
    for expression in [
        "0 9 * * MON",
        "@daily",
        "0 9 * * * echo",
        "0 9 * *",
        "0 9 * * * *",
        "60 9 * * *",
        "0 24 * * *",
        "0 9 0 * *",
        "0 9 * 13 *",
        "0 9 * * 8",
        "*/0 9 * * *",
        "4-2 9 * * *",
        "0 9 * * *%mail",
        "0 9 * * *\n* * * * *",
        "0 9 * * *;id",
        "０ 9 * * *",
    ] {
        assert!(
            ScheduleSpec::parse_cron(expression, Moscow).is_err(),
            "accepted {expression:?}"
        );
    }
    let overlong = format!("{} 9 * * *", "0,".repeat(70));
    assert!(ScheduleSpec::parse_cron(&overlong, Moscow).is_err());
}

#[test]
fn once_at_rejects_invalid_zone_local_gaps_and_ambiguity() {
    assert!("Not/A_Zone".parse::<chrono_tz::Tz>().is_err());
    assert!(ScheduleSpec::parse_once_at("2026-03-08T02:30", Eastern).is_err());
    assert!(ScheduleSpec::parse_once_at("2026-11-01T01:30", Eastern).is_err());
    let parsed = ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap();
    assert_eq!(
        parsed.at().unwrap(),
        Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 0).unwrap()
    );
}

#[test]
fn deserialize_rejects_schedule_values_that_bypass_parsers() {
    let invalid_cron =
        r#"{"kind":"cron","expression":"0 9 * * *\n/bin/evil","timezone":"Europe/Moscow"}"#;
    assert!(serde_json::from_str::<ScheduleSpec>(invalid_cron).is_err());
    let invalid_once =
        r#"{"kind":"once_at","at":"2026-09-26T07:30:01Z","timezone":"Europe/Moscow"}"#;
    assert!(serde_json::from_str::<ScheduleSpec>(invalid_once).is_err());
}

#[test]
fn renderer_contains_only_timezone_fixed_binary_and_canonical_job_id() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let dialog = store.create_dialog("source").unwrap().id;
    let cron = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "secret name".into(),
            schedule: ScheduleSpec::parse_cron("0 09 * * 1-5", Moscow).unwrap(),
            prompt: "secret prompt; $(id)".into(),
        })
        .unwrap();
    let once = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "once".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-27T11:45", Moscow).unwrap(),
            prompt: "another secret".into(),
        })
        .unwrap();
    let output = CronRenderer::new(PathBuf::from("/opt/light-agent/bin/light-agent"))
        .unwrap()
        .render(&[cron.clone(), once.clone()])
        .unwrap();
    assert!(output.contains("CRON_TZ=Europe/Moscow\n"));
    assert!(output.contains(&format!(
        "0 9 * * 1-5 /opt/light-agent/bin/light-agent run-job {}\n",
        cron.id
    )));
    assert!(output.contains(&format!(
        "45 11 27 9 * /opt/light-agent/bin/light-agent run-job {}\n",
        once.id
    )));
    for secret in ["secret name", "secret prompt", "$(id)", "another secret"] {
        assert!(!output.contains(secret));
    }
}

#[test]
fn renderer_defensively_rejects_direct_invalid_schedule_and_normalizes_valid_cron() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let dialog = store.create_dialog("source").unwrap().id;
    let mut job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "job".into(),
            schedule: ScheduleSpec::parse_cron("0 9 * * *", Moscow).unwrap(),
            prompt: "prompt".into(),
        })
        .unwrap();
    let renderer = CronRenderer::new(PathBuf::from("/opt/light-agent/bin/light-agent")).unwrap();

    job.schedule = ScheduleSpec::Cron {
        expression: "0 09 * * 1-5".into(),
        timezone: Moscow,
    };
    assert!(
        renderer
            .render(&[job.clone()])
            .unwrap()
            .contains("0 9 * * 1-5 ")
    );

    job.schedule = ScheduleSpec::Cron {
        expression: "0 9 * * *;id".into(),
        timezone: Moscow,
    };
    assert_eq!(
        renderer.render(&[job]).unwrap_err(),
        SchedulerError::InvalidSchedule
    );
}

#[test]
fn managed_block_replacement_preserves_every_outside_byte() {
    let existing = "MAILTO=me@example.com\r\n# user before\n# BEGIN LIGHT-AGENT MANAGED JOBS\nold\n# END LIGHT-AGENT MANAGED JOBS\n# user after\r\n";
    let renderer = CronRenderer::new(PathBuf::from("/opt/light-agent/bin/light-agent")).unwrap();
    let replaced = renderer.replace_managed_block(existing, &[]).unwrap();
    assert_eq!(
        replaced,
        "MAILTO=me@example.com\r\n# user before\n# BEGIN LIGHT-AGENT MANAGED JOBS\n# END LIGHT-AGENT MANAGED JOBS\n# user after\r\n"
    );
}

#[test]
fn malformed_managed_markers_fail_closed() {
    let renderer = CronRenderer::new(PathBuf::from("/opt/light-agent/bin/light-agent")).unwrap();
    for existing in [
        "# BEGIN LIGHT-AGENT MANAGED JOBS\nold\n",
        "# END LIGHT-AGENT MANAGED JOBS\n",
        "# BEGIN LIGHT-AGENT MANAGED JOBS\n# BEGIN LIGHT-AGENT MANAGED JOBS\n# END LIGHT-AGENT MANAGED JOBS\n",
        "# BEGIN LIGHT-AGENT MANAGED JOBS\n# END LIGHT-AGENT MANAGED JOBS\n# END LIGHT-AGENT MANAGED JOBS\n",
        "# END LIGHT-AGENT MANAGED JOBS\n# BEGIN LIGHT-AGENT MANAGED JOBS\n",
    ] {
        assert!(renderer.replace_managed_block(existing, &[]).is_err());
    }
}

#[derive(Clone)]
struct FakeBackend {
    existing: String,
    fail_install: bool,
    calls: Arc<Mutex<Vec<String>>>,
    installed: Arc<Mutex<Option<String>>>,
}

impl FakeBackend {
    fn success(existing: &str) -> Self {
        Self {
            existing: existing.into(),
            fail_install: false,
            calls: Arc::new(Mutex::new(Vec::new())),
            installed: Arc::new(Mutex::new(None)),
        }
    }
}

impl CrontabBackend for FakeBackend {
    fn preflight(&self) -> CrontabFuture<'_, ()> {
        self.calls.lock().unwrap().push("preflight".into());
        Box::pin(async { Ok(()) })
    }

    fn list(&self) -> CrontabFuture<'_, String> {
        self.calls.lock().unwrap().push("list".into());
        let existing = self.existing.clone();
        Box::pin(async move { Ok(existing) })
    }

    fn validate<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("validate:{}", candidate.len()));
        Box::pin(async { Ok(()) })
    }

    fn install<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()> {
        self.calls.lock().unwrap().push("install".into());
        let fail = self.fail_install;
        let installed = self.installed.clone();
        let candidate = candidate.to_owned();
        Box::pin(async move {
            if fail {
                Err(SchedulerError::Backend)
            } else {
                *installed.lock().unwrap() = Some(candidate);
                Ok(())
            }
        })
    }
}

fn sync_fixture() -> (tempfile::TempDir, Store, deepseek_cli::store::CronJob) {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let dialog = store.create_dialog("source").unwrap().id;
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "job".into(),
            schedule: ScheduleSpec::parse_cron("0 9 * * *", Moscow).unwrap(),
            prompt: "prompt".into(),
        })
        .unwrap();
    (dir, store, job)
}

#[test]
fn synchronizer_rejects_lock_outside_the_store_trusted_directory() {
    let (store_dir, store, _) = sync_fixture();
    let lock_dir = common::private_tempdir();
    let result = CronSynchronizer::new(
        store,
        Arc::new(FakeBackend::success("")),
        lock_dir.path().join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    );
    assert!(matches!(result, Err(SchedulerError::InvalidPath)));
    drop(store_dir);
}

#[test]
fn synchronizer_rejects_database_and_runtime_lock_namespace_collisions() {
    let (dir, store, _) = sync_fixture();
    for name in [
        "agent.sqlite",
        "agent.sqlite.runtime.lock",
        "agent.sqlite.runtime.owner.123e4567-e89b-42d3-a456-426614174000.lock",
        "agent.sqlite.runtime.owner.reserved-future-name",
    ] {
        let result = CronSynchronizer::new(
            store.clone(),
            Arc::new(FakeBackend::success("")),
            dir.path().join(name),
            PathBuf::from("/opt/light-agent/bin/light-agent"),
        );
        assert!(
            matches!(result, Err(SchedulerError::InvalidPath)),
            "accepted colliding lock name {name}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn synchronizer_rejects_replaced_trusted_directory_before_opening_lock() {
    use std::os::unix::fs::PermissionsExt;

    let root = common::private_tempdir();
    let db_dir = root.path().join("database");
    std::fs::create_dir(&db_dir).unwrap();
    std::fs::set_permissions(&db_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = Store::open(db_dir.join("agent.sqlite")).unwrap();
    let backend = Arc::new(FakeBackend::success(""));
    let sync = CronSynchronizer::new(
        store,
        backend.clone(),
        db_dir.join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    )
    .unwrap();

    let original_dir = root.path().join("original-database");
    std::fs::rename(&db_dir, &original_dir).unwrap();
    std::fs::create_dir(&db_dir).unwrap();
    std::fs::set_permissions(&db_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::rename(
        original_dir.join("agent.sqlite"),
        db_dir.join("agent.sqlite"),
    )
    .unwrap();

    assert_eq!(sync.sync().await.unwrap_err(), SchedulerError::Store);
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(!db_dir.join("cron.lock").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn synchronizer_pins_canonical_lock_parent_instead_of_path_alias() {
    use std::os::unix::fs::symlink;

    let (store_dir, store, _) = sync_fixture();
    let aliases = common::private_tempdir();
    let other = common::private_tempdir();
    let alias = aliases.path().join("database-alias");
    symlink(store_dir.path(), &alias).unwrap();
    let sync = CronSynchronizer::new(
        store,
        Arc::new(FakeBackend::success("")),
        alias.join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    )
    .unwrap();
    std::fs::remove_file(&alias).unwrap();
    symlink(other.path(), &alias).unwrap();

    assert_eq!(
        sync.sync().await.unwrap(),
        SyncReport::Installed { jobs: 1 }
    );
    assert!(store_dir.path().join("cron.lock").exists());
    assert!(!other.path().join("cron.lock").exists());
}

#[tokio::test]
async fn successful_sync_marks_rendered_jobs_applied_after_validation_and_install() {
    let (dir, store, job) = sync_fixture();
    let backend = Arc::new(FakeBackend::success("# user\n"));
    let sync = CronSynchronizer::new(
        store.clone(),
        backend.clone(),
        dir.path().join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    )
    .unwrap();
    assert_eq!(
        sync.sync().await.unwrap(),
        SyncReport::Installed { jobs: 1 }
    );
    assert_eq!(
        store.get_job(job.id).unwrap().sync_state,
        JobSyncState::Applied
    );
    let calls = backend.calls.lock().unwrap().clone();
    assert_eq!(calls[0], "preflight");
    assert_eq!(calls[1], "list");
    assert!(calls[2].starts_with("validate:"));
    assert_eq!(calls[3], "install");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(dir.path().join("cron.lock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
    }
}

#[tokio::test]
async fn install_failure_marks_jobs_failed_and_reports_saved_not_installed() {
    let (dir, store, job) = sync_fixture();
    let mut fake = FakeBackend::success("");
    fake.fail_install = true;
    let sync = CronSynchronizer::new(
        store.clone(),
        Arc::new(fake),
        dir.path().join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    )
    .unwrap();
    assert_eq!(
        sync.sync().await.unwrap(),
        SyncReport::SavedNotInstalled { jobs: 1 }
    );
    assert_eq!(
        store.get_job(job.id).unwrap().sync_state,
        JobSyncState::Failed
    );
}

#[tokio::test]
async fn ignored_sync_state_write_cannot_report_applied() {
    let (dir, store, job) = sync_fixture();
    rusqlite::Connection::open(dir.path().join("agent.sqlite"))
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER ignore_sync BEFORE UPDATE OF sync_state ON cron_jobs BEGIN SELECT RAISE(IGNORE); END;",
        )
        .unwrap();
    let backend = Arc::new(FakeBackend::success(""));
    let sync = CronSynchronizer::new(
        store.clone(),
        backend,
        dir.path().join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    )
    .unwrap();
    assert_eq!(sync.sync().await.unwrap_err(), SchedulerError::Store);
    assert_eq!(
        store.get_job(job.id).unwrap().sync_state,
        JobSyncState::Pending
    );
}

#[tokio::test]
async fn malformed_existing_block_never_calls_install() {
    let (dir, store, _) = sync_fixture();
    let backend = Arc::new(FakeBackend::success(
        "# BEGIN LIGHT-AGENT MANAGED JOBS\nmissing end\n",
    ));
    let sync = CronSynchronizer::new(
        store,
        backend.clone(),
        dir.path().join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    )
    .unwrap();
    assert_eq!(
        sync.sync().await.unwrap_err(),
        SchedulerError::MalformedManagedBlock
    );
    assert!(
        !backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call == "install")
    );
}

#[tokio::test]
async fn sqlite_lock_during_reconciliation_obeys_deadline_and_never_installs_late() {
    let (dir, store, _) = sync_fixture();
    let backend = Arc::new(FakeBackend::success(""));
    let sync = CronSynchronizer::new(
        store,
        backend.clone(),
        dir.path().join("cron.lock"),
        PathBuf::from("/opt/light-agent/bin/light-agent"),
    )
    .unwrap();
    let mut blocker = rusqlite::Connection::open(dir.path().join("agent.sqlite")).unwrap();
    let transaction = blocker
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let started = Instant::now();

    let result = sync
        .sync_at_until(
            Utc::now(),
            started + Duration::from_millis(60),
            CancellationToken::new(),
        )
        .await;
    assert_eq!(result.unwrap_err(), SchedulerError::Busy);
    assert!(started.elapsed() < Duration::from_millis(500));
    drop(transaction);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(&*backend.calls.lock().unwrap(), &["preflight"]);
    assert!(backend.installed.lock().unwrap().is_none());
}

#[cfg(unix)]
fn fake_crontab(dir: &std::path::Path, version: &str) -> (PathBuf, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let executable = dir.join("fake-crontab");
    let log = dir.join("argv.log");
    let installed = dir.join("installed");
    let script = format!(
        "#!/bin/sh\n[ \"$LC_ALL\" = C ] && [ \"$LANG\" = C ] || exit 7\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in\n  -V) printf '%s\\n' '{}' ;;\n  -T) input=$(cat); case \"$input\" in *CRON_TZ=*) exit 0 ;; *) exit 9 ;; esac ;;\n  -l) printf 'no crontab for test\\n' >&2; exit 1 ;;\n  -) cat > '{}' ;;\n  *) exit 8 ;;\nesac\n",
        log.display(),
        version,
        installed.display()
    );
    std::fs::write(&executable, script).unwrap();
    let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&executable, permissions).unwrap();
    (executable, log, installed)
}

#[cfg(unix)]
#[tokio::test]
async fn system_preflight_requires_cronie_and_t_validation_without_a_shell() {
    let dir = common::private_tempdir();
    let (not_cronie, _, _) = fake_crontab(dir.path(), "Vixie Cron 4.1");
    let backend = SystemCrontabBackend::new(not_cronie).unwrap();
    assert_eq!(
        backend.preflight().await.unwrap_err(),
        SchedulerError::UnsupportedCron
    );

    let other = common::private_tempdir();
    let (cronie, log, installed) = fake_crontab(other.path(), "cronie 1.7.2");
    let backend = SystemCrontabBackend::new(cronie).unwrap();
    backend.preflight().await.unwrap();
    assert_eq!(backend.list().await.unwrap(), "");
    let candidate = "CRON_TZ=Europe/Moscow\n0 9 * * * /opt/light-agent/bin/light-agent run-job 123e4567-e89b-42d3-a456-426614174000\n";
    backend.validate(candidate).await.unwrap();
    backend.install(candidate).await.unwrap();
    assert_eq!(std::fs::read_to_string(installed).unwrap(), candidate);
    assert_eq!(
        std::fs::read_to_string(log).unwrap(),
        "-V\n-T -\n-l\n-T -\n-\n"
    );
}

#[cfg(unix)]
fn hanging_crontab(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let executable = dir.join("hanging-crontab");
    let version_pid = dir.join("version.pid");
    let list_pid = dir.join("list.pid");
    let script = format!(
        "#!/bin/sh\ncase \"$1\" in\n  -V) printf '%s' \"$$\" > '{}'; exec sleep 30 ;;\n  -l) printf '%s' \"$$\" > '{}'; exec sleep 30 ;;\n  *) exit 0 ;;\nesac\n",
        version_pid.display(),
        list_pid.display()
    );
    std::fs::write(&executable, script).unwrap();
    let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&executable, permissions).unwrap();
    (executable, version_pid, list_pid)
}

#[cfg(unix)]
async fn wait_for_pid_file(path: &std::path::Path) -> u32 {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok(contents) = std::fs::read_to_string(path)
            && let Ok(pid) = contents.parse()
        {
            return pid;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "child never wrote its pid"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[cfg(unix)]
async fn assert_process_exits(pid: u32) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let alive = std::process::Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if !alive {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "crontab child {pid} survived cancellation"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_preflight_and_list_kills_and_reaps_hanging_crontab_children() {
    let dir = common::private_tempdir();
    let (executable, version_pid_path, list_pid_path) = hanging_crontab(dir.path());

    let backend = SystemCrontabBackend::new(executable.clone()).unwrap();
    let preflight = tokio::spawn(async move { backend.preflight().await });
    let version_pid = wait_for_pid_file(&version_pid_path).await;
    preflight.abort();
    let _ = preflight.await;
    assert_process_exits(version_pid).await;

    let backend = SystemCrontabBackend::new(executable).unwrap();
    let list = tokio::spawn(async move { backend.list().await });
    let list_pid = wait_for_pid_file(&list_pid_path).await;
    list.abort();
    let _ = list.await;
    assert_process_exits(list_pid).await;
}
