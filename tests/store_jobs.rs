mod common;

use chrono::{TimeZone, Utc};
use chrono_tz::Europe::Moscow;
use deepseek_cli::domain::{CronRunStatus, JobDesiredState, JobSyncState, ToolOwner};
use deepseek_cli::scheduler::ScheduleSpec;
use deepseek_cli::store::{
    CronRunFinish, JobCreate, RunClaim, SafeErrorCode, Store, StoreError, ToolRunFinish,
    ToolRunStart,
};

fn setup() -> (tempfile::TempDir, Store, deepseek_cli::domain::DialogId) {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let dialog = store.create_dialog("scheduler").unwrap().id;
    (dir, store, dialog)
}

#[test]
fn v1_database_migrates_jobs_and_runs_without_losing_dialogs() {
    let dir = common::private_tempdir();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    let dialog = store.create_dialog("before migration").unwrap().id;
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "DROP INDEX pending_tools_by_runtime_owner;
         ALTER TABLE tool_runs DROP COLUMN arguments;
         ALTER TABLE tool_runs DROP COLUMN runtime_owner_id;
         DROP INDEX pending_turns_by_runtime_owner;
         ALTER TABLE turns DROP COLUMN runtime_owner_id;
         DROP TABLE runtime_owners;
         DROP TABLE runtime_coordination;
         DROP TABLE cron_runs;
         DROP TABLE cron_jobs;
         UPDATE schema_version SET version=1;",
    )
    .unwrap();
    drop(db);

    let migrated = Store::open(&path).unwrap();
    assert_eq!(migrated.list_dialogs().unwrap()[0].id, dialog);
    assert_eq!(
        rusqlite::Connection::open(&path)
            .unwrap()
            .query_row("SELECT version FROM schema_version", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        5
    );
    migrated.create_job(recurring(dialog)).unwrap();
}

fn recurring(dialog: deepseek_cli::domain::DialogId) -> JobCreate {
    JobCreate {
        source_dialog_id: dialog,
        name: "morning report".into(),
        schedule: ScheduleSpec::parse_cron("0 9 * * 1-5", Moscow).unwrap(),
        prompt: "Prepare the report".into(),
    }
}

#[test]
fn create_starts_active_pending() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    assert_eq!(job.desired_state, JobDesiredState::Active);
    assert_eq!(job.sync_state, JobSyncState::Pending);
    assert_eq!(job.source_dialog_id, Some(dialog));
    assert_eq!(store.get_job(job.id).unwrap(), job);
}

#[test]
fn invalid_public_schedule_values_never_mutate_jobs() {
    let (_dir, store, dialog) = setup();
    let invalid_cron = ScheduleSpec::Cron {
        expression: "0 9 * * *\n/bin/evil".into(),
        timezone: Moscow,
    };
    assert!(matches!(
        store.create_job(JobCreate {
            source_dialog_id: dialog,
            name: "invalid".into(),
            schedule: invalid_cron,
            prompt: "must not persist".into(),
        }),
        Err(StoreError::InvalidMetadata)
    ));
    assert!(store.list_jobs().unwrap().is_empty());

    let original = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "normalized".into(),
            schedule: ScheduleSpec::Cron {
                expression: "0 09 * * 1-5".into(),
                timezone: Moscow,
            },
            prompt: "valid prompt".into(),
        })
        .unwrap();
    assert_eq!(original.schedule.expression(), Some("0 9 * * 1-5"));
    let invalid_once = ScheduleSpec::OnceAt {
        at: Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 1).unwrap(),
        timezone: Moscow,
    };
    assert!(matches!(
        store.update_job(
            original.id,
            "poisoned".into(),
            invalid_once,
            "poisoned prompt".into(),
        ),
        Err(StoreError::InvalidMetadata)
    ));
    assert_eq!(store.get_job(original.id).unwrap(), original);
}

#[test]
fn disabled_or_deleted_job_never_claims_even_with_stale_line() {
    let (_dir, store, dialog) = setup();
    let disabled = store.create_job(recurring(dialog)).unwrap();
    store.mark_job_sync_applied(disabled.id).unwrap();
    store
        .set_job_desired_state(disabled.id, JobDesiredState::Disabled)
        .unwrap();
    assert_eq!(
        store.claim_run(disabled.id, Utc::now()).unwrap(),
        RunClaim::Inactive
    );

    let deleted = store.create_job(recurring(dialog)).unwrap();
    store.mark_job_sync_applied(deleted.id).unwrap();
    store
        .set_job_desired_state(deleted.id, JobDesiredState::Deleted)
        .unwrap();
    assert_eq!(
        store.claim_run(deleted.id, Utc::now()).unwrap(),
        RunClaim::Inactive
    );
    assert_eq!(store.list_jobs().unwrap().len(), 2, "delete is a tombstone");
}

#[test]
fn pending_or_failed_sync_job_never_claims() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    assert_eq!(
        store.claim_run(job.id, Utc::now()).unwrap(),
        RunClaim::Inactive
    );
    store.mark_job_sync_failed(job.id).unwrap();
    assert_eq!(
        store.claim_run(job.id, Utc::now()).unwrap(),
        RunClaim::Inactive
    );
}

#[test]
fn overlap_is_recorded_skipped() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 0, 0).unwrap();
    let first = store.claim_run(job.id, now).unwrap();
    assert!(matches!(first, RunClaim::Claimed(_)));
    let second = store.claim_run(job.id, now).unwrap();
    assert!(matches!(second, RunClaim::Skipped(_)));
    let runs = store.list_runs(job.id).unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].status, CronRunStatus::Pending);
    assert_eq!(runs[1].status, CronRunStatus::Skipped);
}

#[test]
fn claimed_run_carries_the_atomic_job_prompt_snapshot() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = store.claim_run(job.id, Utc::now()).unwrap() else {
        panic!("expected claim")
    };
    store
        .update_job(
            job.id,
            "changed".into(),
            ScheduleSpec::parse_cron("5 10 * * *", Moscow).unwrap(),
            "changed prompt".into(),
        )
        .unwrap();
    assert_eq!(claim.job.prompt, "Prepare the report");
    assert_eq!(claim.run.job_id, claim.job.id);
}

#[test]
fn once_claim_disables_before_provider_work() {
    let (_dir, store, dialog) = setup();
    let at = Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 0).unwrap();
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "once".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "Do it once".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let claim = store.claim_run(job.id, at).unwrap();
    assert!(matches!(claim, RunClaim::Claimed(_)));
    let stored = store.get_job(job.id).unwrap();
    assert_eq!(stored.desired_state, JobDesiredState::Disabled);
    assert_eq!(stored.sync_state, JobSyncState::Pending);
}

#[test]
fn missed_once_job_is_never_run_late_or_next_year() {
    let (_dir, store, dialog) = setup();
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "once".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "Do it once".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let late = Utc.with_ymd_and_hms(2026, 9, 26, 7, 31, 0).unwrap();
    assert_eq!(store.mark_missed_once_jobs(late).unwrap(), 1);
    assert_eq!(store.mark_missed_once_jobs(late).unwrap(), 0);
    assert_eq!(store.claim_run(job.id, late).unwrap(), RunClaim::Inactive);
    assert_eq!(
        store
            .claim_run(job.id, Utc.with_ymd_and_hms(2027, 9, 26, 7, 30, 0).unwrap())
            .unwrap(),
        RunClaim::Inactive
    );
    assert_eq!(
        store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Missed
    );
}

#[test]
fn once_job_is_never_run_on_the_same_calendar_date_a_year_early() {
    let (_dir, store, dialog) = setup();
    let at = Utc.with_ymd_and_hms(2027, 9, 26, 7, 30, 0).unwrap();
    let job = store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "future once".into(),
            schedule: ScheduleSpec::parse_once_at("2027-09-26T10:30", Moscow).unwrap(),
            prompt: "Do it next year".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let early = Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 0).unwrap();
    assert_eq!(store.claim_run(job.id, early).unwrap(), RunClaim::Inactive);
    assert!(store.list_runs(job.id).unwrap().is_empty());
    assert_eq!(
        store.get_job(job.id).unwrap().desired_state,
        JobDesiredState::Active
    );
    assert!(matches!(
        store.claim_run(job.id, at).unwrap(),
        RunClaim::Claimed(_)
    ));
}

#[test]
fn cron_tool_owner_requires_existing_run() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = store.claim_run(job.id, Utc::now()).unwrap() else {
        panic!("expected claimed run")
    };
    let start = |owner| ToolRunStart {
        owner,
        call_id: "call_1".into(),
        server_name: "telegram".into(),
        tool_name: "get_messages".into(),
        read_only: true,
        arguments: "{}".into(),
    };
    store
        .start_tool_run(start(ToolOwner::CronRun(claim.run.id)))
        .unwrap();
    assert!(matches!(
        store.start_tool_run(start(ToolOwner::CronRun(
            deepseek_cli::domain::RunId::new(999).unwrap()
        ))),
        Err(StoreError::InvalidOwner)
    ));
    assert_eq!(
        store.list_tool_runs().unwrap()[0].owner,
        ToolOwner::CronRun(claim.run.id)
    );
}

#[test]
fn finishing_run_is_idempotent_only_for_identical_outcome() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = store.claim_run(job.id, Utc::now()).unwrap() else {
        panic!("expected claim")
    };
    let finish = CronRunFinish::completed("done");
    store.finish_run(claim.run.id, finish.clone()).unwrap();
    store.finish_run(claim.run.id, finish).unwrap();
    assert!(matches!(
        store.finish_run(
            claim.run.id,
            CronRunFinish::failed(SafeErrorCode::ProviderError)
        ),
        Err(StoreError::Conflict)
    ));
}

#[test]
fn cron_run_cannot_finish_while_its_tool_audit_is_pending() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = store.claim_run(job.id, Utc::now()).unwrap() else {
        panic!("expected claim")
    };
    let audit = store
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::CronRun(claim.run.id),
            call_id: "pending".into(),
            server_name: "fixture".into(),
            tool_name: "read".into(),
            read_only: true,
            arguments: "{}".into(),
        })
        .unwrap();

    assert_eq!(
        store.finish_run(claim.run.id, CronRunFinish::completed("too early")),
        Err(StoreError::Busy)
    );
    store
        .finish_tool_run(audit, ToolRunFinish::completed())
        .unwrap();
    store
        .finish_run(claim.run.id, CronRunFinish::completed("done"))
        .unwrap();
}

#[test]
fn live_job_blocks_dialog_delete_but_deleted_tombstone_loses_link() {
    let (_dir, store, dialog) = setup();
    let job = store.create_job(recurring(dialog)).unwrap();
    assert!(matches!(store.delete_dialog(dialog), Err(StoreError::Busy)));
    store
        .set_job_desired_state(job.id, JobDesiredState::Deleted)
        .unwrap();
    store.delete_dialog(dialog).unwrap();
    assert_eq!(store.get_job(job.id).unwrap().source_dialog_id, None);
}
