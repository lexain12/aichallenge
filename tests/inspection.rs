mod common;

use std::io::Write;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use chrono_tz::Europe::Moscow;
use deepseek_cli::domain::{JobDesiredState, ToolOwner};
use deepseek_cli::inspection::{
    InspectQuery, InspectionCancellation, InspectionError, InspectionService,
};
use deepseek_cli::scheduler::ScheduleSpec;
use deepseek_cli::store::{
    CronRunFinish, JobCreate, RunClaim, SafeErrorCode, Store, ToolRunFinish, ToolRunStart,
};
use serde_json::Value;

fn all_items(service: &InspectionService, query: InspectQuery) -> Vec<Value> {
    let mut snapshot = service.snapshot(query).unwrap();
    let mut items = Vec::new();
    while let Some(mut page) = snapshot.next_page().unwrap() {
        items.append(&mut page.items);
        if page.complete {
            assert!(page.complete);
            break;
        }
    }
    snapshot.close().unwrap();
    items
}

struct ShortWriteRecorder {
    bytes: Vec<u8>,
    largest_write: usize,
}

impl Write for ShortWriteRecorder {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.largest_write = self.largest_write.max(bytes.len());
        let accepted = bytes.len().min(31);
        self.bytes.extend_from_slice(&bytes[..accepted]);
        Ok(accepted)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn dump_export_and_history_cover_all_logical_state_in_stable_order() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let first = store.create_dialog("first").unwrap();
    let second = store.create_dialog("second").unwrap();

    let completed = store.begin_turn(first.id, "question").unwrap();
    let audit = store
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::InteractiveTurn(completed.turn_id),
            call_id: "call-1".into(),
            server_name: "telegram".into(),
            tool_name: "lookup".into(),
            read_only: true,
            arguments: "{\"chat_id\":\"private marker\"}".into(),
        })
        .unwrap();
    store
        .finish_tool_run(audit, ToolRunFinish::completed())
        .unwrap();
    store.complete_turn(completed.turn_id, "answer").unwrap();
    let failed = store.begin_turn(second.id, "will fail").unwrap();
    store
        .fail_turn(failed.turn_id, SafeErrorCode::ProviderError)
        .unwrap();

    let active = store
        .create_job(JobCreate {
            source_dialog_id: first.id,
            name: "active".into(),
            schedule: ScheduleSpec::parse_cron("0 9 * * *", Moscow).unwrap(),
            prompt: "daily task".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(active.id).unwrap();
    let RunClaim::Claimed(claim) = store
        .claim_run(
            active.id,
            Utc.with_ymd_and_hms(2026, 9, 26, 6, 0, 0).unwrap(),
        )
        .unwrap()
    else {
        panic!("expected claimed run")
    };
    store
        .finish_run(claim.run.id, CronRunFinish::completed("done"))
        .unwrap();

    let missed = store
        .create_job(JobCreate {
            source_dialog_id: second.id,
            name: "missed".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "one-time task".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(missed.id).unwrap();
    store
        .mark_missed_once_jobs(Utc.with_ymd_and_hms(2026, 9, 26, 7, 31, 0).unwrap())
        .unwrap();
    let deleted = store
        .create_job(JobCreate {
            source_dialog_id: second.id,
            name: "deleted".into(),
            schedule: ScheduleSpec::parse_cron("15 8 * * *", Moscow).unwrap(),
            prompt: "deleted task".into(),
        })
        .unwrap();
    store
        .set_job_desired_state(deleted.id, JobDesiredState::Deleted)
        .unwrap();

    let service = InspectionService::with_page_size(store.clone(), 2).unwrap();
    let dump = all_items(&service, InspectQuery::Dump);
    assert_eq!(
        dump.iter()
            .map(|item| item["record_type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "dialog", "dialog", "turn", "turn", "message", "message", "message", "job", "job",
            "job", "run", "run", "tool_run"
        ]
    );
    assert_eq!(dump, all_items(&service, InspectQuery::Dump));

    let history = all_items(&service, InspectQuery::History(Some(first.id)));
    assert!(
        history
            .iter()
            .any(|item| item["record_type"] == "service_event")
    );
    assert_eq!(
        history
            .iter()
            .filter(|item| item["record_type"] == "message")
            .count(),
        2
    );
    assert!(
        history
            .iter()
            .filter(|item| item["record_type"] == "message")
            .all(|item| item.get("job_name").is_none())
    );

    let mut first_export = Vec::new();
    let summary = service.write_export(&mut first_export).unwrap();
    assert_eq!(summary.total_bytes as usize, first_export.len());
    assert_eq!(summary.records, 13);
    assert_eq!(summary.sha256.len(), 64);
    let mut second_export = Vec::new();
    service.write_export(&mut second_export).unwrap();
    assert_eq!(first_export, second_export);
    let records = String::from_utf8(first_export).unwrap();
    assert!(
        records
            .lines()
            .next()
            .unwrap()
            .contains("logical_export_v1")
    );
    assert_eq!(records.lines().count(), 14);
}

#[test]
fn export_is_streamed_and_has_no_secret_or_raw_payload_fields() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let dialog = store.create_dialog("safe").unwrap();
    let turn = store.begin_turn(dialog.id, "ordinary message").unwrap();
    let tool = store
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::InteractiveTurn(turn.turn_id),
            call_id: "opaque-call".into(),
            server_name: "remote".into(),
            tool_name: "read".into(),
            read_only: true,
            arguments: "{\"secret\":\"private marker\"}".into(),
        })
        .unwrap();
    store
        .finish_tool_run(tool, ToolRunFinish::failed(SafeErrorCode::ToolError))
        .unwrap();
    store
        .complete_turn(turn.turn_id, "ordinary answer")
        .unwrap();
    let service = InspectionService::new(store);
    let audit = all_items(&service, InspectQuery::Audit(dialog.id));
    assert_eq!(audit.len(), 1);
    assert_eq!(
        audit[0]["arguments"],
        serde_json::json!({"secret":"private marker"})
    );
    assert!(audit[0].get("call_id").is_none());
    assert!(
        !format!(
            "{:?}",
            service.inspect(InspectQuery::Audit(dialog.id)).unwrap()
        )
        .contains("private marker")
    );

    let dump = all_items(&service, InspectQuery::Dump);
    assert!(dump.iter().all(|item| item.get("arguments").is_none()));

    let mut writer = ShortWriteRecorder {
        bytes: Vec::new(),
        largest_write: 0,
    };
    service.write_export(&mut writer).unwrap();
    assert!(writer.largest_write <= 262_144);
    let export = String::from_utf8(writer.bytes).unwrap();
    for forbidden in [
        "api_key",
        "bearer_token",
        "mcp_url",
        "provider_body",
        "raw_error",
        "arguments",
        "tool_result",
    ] {
        assert!(!export.contains(forbidden), "leaked field: {forbidden}");
    }
    assert!(export.contains("opaque-call"));
    assert!(export.contains("tool_error"));
}

#[test]
fn audit_is_scoped_to_one_dialog_and_includes_its_cron_runs() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let first = store.create_dialog("first").unwrap();
    let second = store.create_dialog("second").unwrap();

    let first_turn = store.begin_turn(first.id, "first question").unwrap();
    let first_tool = store
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::InteractiveTurn(first_turn.turn_id),
            call_id: "first-interactive".into(),
            server_name: "telegram".into(),
            tool_name: "read_chat".into(),
            read_only: true,
            arguments: "{}".into(),
        })
        .unwrap();
    store
        .finish_tool_run(first_tool, ToolRunFinish::completed())
        .unwrap();
    store
        .complete_turn(first_turn.turn_id, "first answer")
        .unwrap();

    let second_turn = store.begin_turn(second.id, "second question").unwrap();
    let second_tool = store
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::InteractiveTurn(second_turn.turn_id),
            call_id: "second-interactive".into(),
            server_name: "telegram".into(),
            tool_name: "read_chat".into(),
            read_only: true,
            arguments: "{}".into(),
        })
        .unwrap();
    store
        .finish_tool_run(second_tool, ToolRunFinish::completed())
        .unwrap();
    store
        .complete_turn(second_turn.turn_id, "second answer")
        .unwrap();

    let job = store
        .create_job(JobCreate {
            source_dialog_id: first.id,
            name: "first cron".into(),
            schedule: ScheduleSpec::parse_cron("0 9 * * *", Moscow).unwrap(),
            prompt: "cron prompt".into(),
        })
        .unwrap();
    store.mark_job_sync_applied(job.id).unwrap();
    let RunClaim::Claimed(claim) = store
        .claim_run(job.id, Utc.with_ymd_and_hms(2026, 9, 28, 6, 0, 0).unwrap())
        .unwrap()
    else {
        panic!("expected claimed run")
    };
    let cron_tool = store
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::CronRun(claim.run.id),
            call_id: "first-cron".into(),
            server_name: "telegram".into(),
            tool_name: "list_chats".into(),
            read_only: true,
            arguments: "{}".into(),
        })
        .unwrap();
    store
        .finish_tool_run(cron_tool, ToolRunFinish::completed())
        .unwrap();
    store
        .finish_run(claim.run.id, CronRunFinish::completed("done"))
        .unwrap();

    let audit = all_items(
        &InspectionService::new(store),
        InspectQuery::Audit(first.id),
    );
    assert_eq!(
        audit
            .iter()
            .map(|record| record["id"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![first_tool, cron_tool]
    );
    assert!(
        audit
            .iter()
            .all(|record| record["id"].as_i64() != Some(second_tool))
    );
}

#[test]
fn inspection_pages_are_bounded_and_cursor_order_is_deterministic() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    for index in 0..7 {
        store.create_dialog(&format!("dialog-{index}")).unwrap();
    }
    let service = InspectionService::with_page_size(store, 3).unwrap();
    let mut snapshot = service.snapshot(InspectQuery::Dialogs).unwrap();
    let first = snapshot.next_page().unwrap().unwrap();
    assert_eq!(first.items.len(), 3);
    assert!(!first.complete);
    let second = snapshot.next_page().unwrap().unwrap();
    assert_eq!(second.items.len(), 3);
    assert!(!second.complete);
    let third = snapshot.next_page().unwrap().unwrap();
    assert_eq!(third.items.len(), 1);
    assert!(third.complete);
    assert!(snapshot.next_page().unwrap().is_none());
    snapshot.close().unwrap();
}

#[test]
fn inspection_snapshot_is_consistent_across_pages_during_concurrent_mutation() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let original = (0..5)
        .map(|index| store.create_dialog(&format!("original-{index}")).unwrap())
        .collect::<Vec<_>>();
    let service = InspectionService::with_page_size(store.clone(), 2).unwrap();
    let mut snapshot = service.snapshot(InspectQuery::Dialogs).unwrap();

    let first = snapshot.next_page().unwrap().unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item["title"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["original-0", "original-1"]
    );

    store.delete_dialog(original[0].id).unwrap();
    store.create_dialog("inserted-after-snapshot").unwrap();

    let mut remaining = Vec::new();
    while let Some(page) = snapshot.next_page().unwrap() {
        remaining.extend(page.items);
    }
    snapshot.close().unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|item| item["title"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["original-2", "original-3", "original-4"]
    );

    let fresh = all_items(&service, InspectQuery::Dialogs);
    assert_eq!(
        fresh
            .iter()
            .map(|item| item["title"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "original-1",
            "original-2",
            "original-3",
            "original-4",
            "inserted-after-snapshot"
        ]
    );
}

#[test]
fn stalled_snapshot_worker_releases_wal_at_the_hard_deadline() {
    let dir = common::private_tempdir();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    for index in 0..5 {
        store.create_dialog(&format!("before-{index}")).unwrap();
    }
    let service =
        InspectionService::with_snapshot_timeout(store.clone(), 2, Duration::from_millis(75))
            .unwrap();
    let mut snapshot = service.snapshot(InspectQuery::Dialogs).unwrap();
    assert_eq!(snapshot.next_page().unwrap().unwrap().items.len(), 2);
    assert_eq!(service.active_snapshots(), 1);

    store.create_dialog("new-wal-frame").unwrap();
    let checkpoint = rusqlite::Connection::open(&path).unwrap();

    assert!(service.wait_for_snapshot_idle(Duration::from_secs(5)));
    assert_eq!(
        service.active_snapshots(),
        0,
        "worker must roll back even when the caller never requests another page"
    );
    let busy_after: i64 = checkpoint
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
        .unwrap();
    assert_eq!(busy_after, 0, "expired worker must no longer pin the WAL");
    assert_eq!(
        snapshot.next_page().unwrap_err(),
        deepseek_cli::inspection::InspectionError::SnapshotExpired
    );
}

#[test]
fn deadline_interrupted_page_is_reported_as_snapshot_expired() {
    let dir = common::private_tempdir();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    let setup = rusqlite::Connection::open(&path).unwrap();
    setup
        .execute_batch(
            "PRAGMA foreign_keys=OFF;
             DROP TABLE dialogs;
             CREATE TABLE inspection_slow_source(n INTEGER PRIMARY KEY);
             WITH digits(d) AS (
               VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
             )
             INSERT INTO inspection_slow_source(n)
             SELECT hundreds.d * 100 + tens.d * 10 + ones.d
             FROM digits hundreds, digits tens, digits ones;
             CREATE VIEW dialogs AS
             SELECT a.n * 1000000 + b.n * 1000 + c.n AS id,
                    'slow' AS title,
                    '2026-09-26T00:00:00Z' AS created_at,
                    '2026-09-26T00:00:00Z' AS updated_at
             FROM inspection_slow_source a
             CROSS JOIN inspection_slow_source b
             CROSS JOIN inspection_slow_source c;",
        )
        .unwrap();
    drop(setup);

    let service =
        InspectionService::with_snapshot_timeout(store, 2, Duration::from_millis(30)).unwrap();
    let mut snapshot = service.snapshot(InspectQuery::Dialogs).unwrap();
    let started = Instant::now();
    assert_eq!(
        snapshot.next_page().unwrap_err(),
        InspectionError::SnapshotExpired
    );
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
fn cancellation_interrupts_a_running_sqlite_page_and_joins_the_worker() {
    let dir = common::private_tempdir();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    let setup = rusqlite::Connection::open(&path).unwrap();
    setup
        .execute_batch(
            "PRAGMA foreign_keys=OFF;
             DROP TABLE dialogs;
             CREATE TABLE inspection_slow_source(n INTEGER PRIMARY KEY);
             WITH digits(d) AS (
               VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
             )
             INSERT INTO inspection_slow_source(n)
             SELECT hundreds.d * 100 + tens.d * 10 + ones.d
             FROM digits hundreds, digits tens, digits ones;
             CREATE VIEW dialogs AS
             SELECT a.n * 1000000 + b.n * 1000 + c.n AS id,
                    'slow' AS title,
                    '2026-09-26T00:00:00Z' AS created_at,
                    '2026-09-26T00:00:00Z' AS updated_at
             FROM inspection_slow_source a
             CROSS JOIN inspection_slow_source b
             CROSS JOIN inspection_slow_source c;",
        )
        .unwrap();
    drop(setup);

    let service = InspectionService::new(store);
    let cancellation = InspectionCancellation::new();
    let mut snapshot = service
        .snapshot_with_cancellation(InspectQuery::Dialogs, cancellation.clone())
        .unwrap();
    let query = std::thread::spawn(move || snapshot.next_page());
    std::thread::sleep(Duration::from_millis(25));
    let started = Instant::now();
    cancellation.cancel();

    assert_eq!(
        query.join().unwrap().unwrap_err(),
        InspectionError::SnapshotExpired
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(service.active_snapshots(), 0);
}

#[test]
fn locked_database_startup_expires_and_releases_the_worker_before_unlock() {
    let dir = common::private_tempdir();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    let lock = rusqlite::Connection::open(&path).unwrap();
    lock.pragma_update(None, "journal_mode", "DELETE").unwrap();
    lock.execute_batch("BEGIN EXCLUSIVE").unwrap();

    let service =
        InspectionService::with_snapshot_timeout(store.clone(), 2, Duration::from_millis(120))
            .unwrap();
    let worker_service = service.clone();
    let start_sequence = service.snapshot_start_sequence();
    let startup = std::thread::spawn(move || worker_service.snapshot(InspectQuery::Dialogs));

    assert!(service.wait_for_snapshot_start(start_sequence, Duration::from_secs(5)));
    let result = startup.join().unwrap();
    assert!(
        matches!(result, Err(InspectionError::SnapshotExpired)),
        "locked startup must fail closed with snapshot_expired"
    );
    assert!(service.wait_for_snapshot_idle(Duration::from_secs(5)));
    assert_eq!(
        service.active_snapshots(),
        0,
        "worker must terminate while the exclusive lock is still held"
    );

    lock.execute_batch("ROLLBACK").unwrap();
    lock.pragma_update(None, "journal_mode", "WAL").unwrap();
    store.create_dialog("after-expired-startup").unwrap();
    let checkpoint = rusqlite::Connection::open(&path).unwrap();
    let busy: i64 = checkpoint
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
        .unwrap();
    assert_eq!(busy, 0, "expired startup must not create a later WAL pin");
}
