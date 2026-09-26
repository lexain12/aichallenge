use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::{TimeZone, Utc};
use chrono_tz::{Europe::Moscow, US::Eastern};
use deepseek_cli::domain::JobSyncState;
use deepseek_cli::scheduler::{
    CronRenderer, CronSynchronizer, CrontabBackend, CrontabFuture, ScheduleSpec, SchedulerError,
    SyncReport, SystemCrontabBackend,
};
use deepseek_cli::store::{JobCreate, Store};

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
fn renderer_contains_only_timezone_fixed_binary_and_canonical_job_id() {
    let dir = tempfile::tempdir().unwrap();
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
    let dir = tempfile::tempdir().unwrap();
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

#[cfg(unix)]
fn fake_crontab(dir: &std::path::Path, version: &str) -> (PathBuf, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let executable = dir.join("fake-crontab");
    let log = dir.join("argv.log");
    let installed = dir.join("installed");
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in\n  -V) printf '%s\\n' '{}' ;;\n  -T) input=$(cat); case \"$input\" in *CRON_TZ=*) exit 0 ;; *) exit 9 ;; esac ;;\n  -l) printf 'no crontab for test\\n' >&2; exit 1 ;;\n  -) cat > '{}' ;;\n  *) exit 8 ;;\nesac\n",
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
    let dir = tempfile::tempdir().unwrap();
    let (not_cronie, _, _) = fake_crontab(dir.path(), "Vixie Cron 4.1");
    let backend = SystemCrontabBackend::new(not_cronie).unwrap();
    assert_eq!(
        backend.preflight().await.unwrap_err(),
        SchedulerError::UnsupportedCron
    );

    let other = tempfile::tempdir().unwrap();
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
