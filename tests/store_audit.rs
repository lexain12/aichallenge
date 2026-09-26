mod common;

use deepseek_cli::domain::{RunId, ToolOwner, ToolRunStatus, TurnId};
use deepseek_cli::runtime::ProcessLease;
use deepseek_cli::store::{SafeErrorCode, Store, StoreError, ToolRunFinish, ToolRunStart};
use rusqlite::Connection;

fn setup() -> (tempfile::TempDir, Store, Connection, ToolOwner) {
    let dir = common::private_tempdir();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    let d = store.create_dialog("audit").unwrap();
    let turn = store.begin_turn(d.id, "question").unwrap();
    let db = Connection::open(path).unwrap();
    (dir, store, db, ToolOwner::InteractiveTurn(turn.turn_id))
}

fn start(owner: ToolOwner, read_only: bool) -> ToolRunStart {
    ToolRunStart {
        owner,
        call_id: "call_1".into(),
        server_name: "telegram".into(),
        tool_name: "get_messages".into(),
        read_only,
    }
}

fn complete_work(store: &Store, owner: ToolOwner, audit_id: i64) {
    store
        .finish_tool_run(audit_id, ToolRunFinish::completed())
        .unwrap();
    let ToolOwner::InteractiveTurn(turn_id) = owner else {
        panic!("expected interactive test owner")
    };
    store.complete_turn(turn_id, "answer").unwrap();
}

#[test]
fn tool_run_stores_route_status_and_read_only_but_no_arguments() {
    let (_dir, store, db, owner) = setup();
    let id = store.start_tool_run(start(owner, true)).unwrap();
    store
        .finish_tool_run(id, ToolRunFinish::completed())
        .unwrap();
    let rows = store.list_tool_runs().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].owner, owner);
    assert_eq!(rows[0].call_id, "call_1");
    assert_eq!(rows[0].server_name, "telegram");
    assert_eq!(rows[0].tool_name, "get_messages");
    assert!(rows[0].read_only);
    assert_eq!(rows[0].status, ToolRunStatus::Completed);
    assert_eq!(rows[0].safe_error_code, None);
    chrono::DateTime::parse_from_rfc3339(&rows[0].started_at).unwrap();
    chrono::DateTime::parse_from_rfc3339(rows[0].finished_at.as_ref().unwrap()).unwrap();
    let columns: Vec<String> = db
        .prepare("PRAGMA table_info(tool_runs)")
        .unwrap()
        .query_map([], |r| r.get(1))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        columns,
        [
            "id",
            "owner_kind",
            "owner_id",
            "call_id",
            "server_name",
            "tool_name",
            "read_only",
            "status",
            "safe_error_code",
            "started_at",
            "finished_at",
            "runtime_owner_id"
        ]
    );
    for forbidden in ["argument", "hash", "result", "url", "raw_error"] {
        assert!(columns.iter().all(|c| !c.contains(forbidden)));
    }
}

#[test]
fn write_recovery_becomes_uncertain() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let lease = ProcessLease::acquire(&store).unwrap();
    let owned = lease.owned_store(&store).unwrap();
    let dialog = store.create_dialog("audit").unwrap();
    let turn = owned.begin_turn(dialog.id, "question").unwrap();
    owned
        .start_tool_run(start(ToolOwner::InteractiveTurn(turn.turn_id), false))
        .unwrap();
    drop(owned);
    drop(lease);
    let recovered = ProcessLease::acquire(&store).unwrap();
    assert_eq!(recovered.recovery_report().tool_runs, 1);
    let row = &store.list_tool_runs().unwrap()[0];
    assert_eq!(row.status, ToolRunStatus::Uncertain);
    assert_eq!(row.safe_error_code, Some(SafeErrorCode::ProcessInterrupted));
    assert!(row.finished_at.is_some());
    drop(recovered);
    assert_eq!(
        ProcessLease::acquire(&store)
            .unwrap()
            .recovery_report()
            .tool_runs,
        0
    );
}

#[test]
fn read_only_recovery_becomes_failed() {
    let dir = common::private_tempdir();
    let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
    let lease = ProcessLease::acquire(&store).unwrap();
    let owned = lease.owned_store(&store).unwrap();
    let dialog = store.create_dialog("audit").unwrap();
    let turn = owned.begin_turn(dialog.id, "question").unwrap();
    let id = owned
        .start_tool_run(start(ToolOwner::InteractiveTurn(turn.turn_id), true))
        .unwrap();
    drop(owned);
    drop(lease);
    let recovered = ProcessLease::acquire(&store).unwrap();
    assert_eq!(recovered.recovery_report().tool_runs, 1);
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Failed
    );
    assert!(matches!(
        recovered_store(&store, &recovered).finish_tool_run(id, ToolRunFinish::completed()),
        Err(StoreError::Conflict)
    ));
}

fn recovered_store(store: &Store, lease: &ProcessLease) -> Store {
    lease.owned_store(store).unwrap()
}

#[test]
fn parent_cannot_be_terminal_while_owned_tool_audit_is_pending() {
    let directory = common::private_tempdir();
    let store = Store::open(directory.path().join("agent.sqlite")).unwrap();
    let lease = ProcessLease::acquire(&store).unwrap();
    let owned = lease.owned_store(&store).unwrap();
    let dialog = store.create_dialog("audit fence").unwrap();
    let turn = owned.begin_turn(dialog.id, "question").unwrap();
    let audit = owned
        .start_tool_run(start(ToolOwner::InteractiveTurn(turn.turn_id), true))
        .unwrap();

    assert_eq!(
        owned.complete_turn(turn.turn_id, "answer"),
        Err(StoreError::Busy)
    );
    assert_eq!(
        owned.fail_turn(turn.turn_id, SafeErrorCode::InternalError),
        Err(StoreError::Busy)
    );
    owned
        .finish_tool_run(audit, ToolRunFinish::completed())
        .unwrap();
    owned.complete_turn(turn.turn_id, "answer").unwrap();
}

#[test]
fn v3_migration_backfills_pending_tool_runtime_owner_from_parent() {
    let directory = common::private_tempdir();
    let path = directory.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    let lease = ProcessLease::acquire(&store).unwrap();
    let owned = lease.owned_store(&store).unwrap();
    let dialog = store.create_dialog("migration").unwrap();
    let turn = owned.begin_turn(dialog.id, "question").unwrap();
    owned
        .start_tool_run(start(ToolOwner::InteractiveTurn(turn.turn_id), true))
        .unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute_batch(
        "DROP INDEX pending_tools_by_runtime_owner;
         ALTER TABLE tool_runs DROP COLUMN runtime_owner_id;
         UPDATE schema_version SET version=3;",
    )
    .unwrap();
    drop(db);

    Store::open(&path).unwrap();
    let backfilled: String = Connection::open(&path)
        .unwrap()
        .query_row("SELECT runtime_owner_id FROM tool_runs", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(backfilled, lease.owner_id());
}

#[test]
fn dialog_delete_is_transactional() {
    let (_dir, store, db, owner) = setup();
    let audit_id = store.start_tool_run(start(owner, true)).unwrap();
    complete_work(&store, owner, audit_id);
    let d = store.list_dialogs().unwrap()[0].id;
    db.execute_batch("CREATE TRIGGER reject_delete BEFORE DELETE ON dialogs BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(store.delete_dialog(d).is_err());
    for (table, expected) in [
        ("dialogs", 1),
        ("turns", 1),
        ("messages", 2),
        ("tool_runs", 1),
    ] {
        assert_eq!(
            db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            expected
        );
    }
    db.execute_batch("DROP TRIGGER reject_delete").unwrap();
    store.delete_dialog(d).unwrap();
    for table in ["dialogs", "turns", "messages", "tool_runs"] {
        assert_eq!(
            db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}

#[test]
fn active_turn_blocks_dialog_deletion_and_preserves_pending_work() {
    let (_dir, store, db, owner) = setup();
    let mut write = start(owner, false);
    write.tool_name = "send_message".into();
    let audit_id = store.start_tool_run(write).unwrap();
    let dialogs_before = store.list_dialogs().unwrap();
    let audits_before = store.list_tool_runs().unwrap();
    let dialog_id = dialogs_before[0].id;
    let ToolOwner::InteractiveTurn(turn_id) = owner else {
        panic!("expected interactive test owner")
    };

    assert!(matches!(
        store.clone().delete_dialog(dialog_id),
        Err(StoreError::Busy)
    ));

    assert_eq!(store.list_dialogs().unwrap(), dialogs_before);
    assert_eq!(store.list_tool_runs().unwrap(), audits_before);
    assert_eq!(audits_before[0].id, audit_id);
    assert_eq!(audits_before[0].status, ToolRunStatus::Pending);
    assert_eq!(
        db.query_row(
            "SELECT status FROM turns WHERE id=? AND dialog_id=?",
            [turn_id.get(), dialog_id.get()],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "pending"
    );
    assert_eq!(
        db.query_row(
            "SELECT role,content FROM messages WHERE turn_id=? AND dialog_id=?",
            [turn_id.get(), dialog_id.get()],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        )
        .unwrap(),
        ("user".into(), "question".into())
    );
    for table in ["dialogs", "turns", "messages", "tool_runs"] {
        assert_eq!(
            db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    // The first session can still record uncertainty after the rejected delete.
    store
        .finish_tool_run(audit_id, ToolRunFinish::uncertain(SafeErrorCode::TimedOut))
        .unwrap();
}

#[test]
fn owner_validation_and_call_uniqueness_are_transactional() {
    let (_dir, store, _, owner) = setup();
    assert!(matches!(
        store.start_tool_run(start(
            ToolOwner::InteractiveTurn(TurnId::new(999).unwrap()),
            true
        )),
        Err(StoreError::InvalidOwner)
    ));
    assert!(matches!(
        store.start_tool_run(start(ToolOwner::CronRun(RunId::new(1).unwrap()), true)),
        Err(StoreError::InvalidOwner)
    ));
    store.start_tool_run(start(owner, true)).unwrap();
    assert!(matches!(
        store.start_tool_run(start(owner, false)),
        Err(StoreError::Conflict)
    ));
    let d = store.create_dialog("another").unwrap();
    let t = store.begin_turn(d.id, "question").unwrap();
    store
        .start_tool_run(start(ToolOwner::InteractiveTurn(t.turn_id), true))
        .unwrap();
    assert_eq!(store.list_tool_runs().unwrap().len(), 2);
}

#[test]
fn finalization_is_idempotent_only_for_identical_terminal_metadata() {
    let (_dir, store, _, owner) = setup();
    let id = store.start_tool_run(start(owner, true)).unwrap();
    let finish = ToolRunFinish::failed(SafeErrorCode::ToolError);
    store.finish_tool_run(id, finish.clone()).unwrap();
    let before = store.list_tool_runs().unwrap();
    store.finish_tool_run(id, finish).unwrap();
    assert_eq!(store.list_tool_runs().unwrap(), before);
    assert!(matches!(
        store.finish_tool_run(id, ToolRunFinish::failed(SafeErrorCode::InternalError)),
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        store.finish_tool_run(id, ToolRunFinish::uncertain(SafeErrorCode::ToolError)),
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        store.finish_tool_run(999, ToolRunFinish::completed()),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn ignored_audit_writes_and_dialog_deletion_are_conflicts() {
    let (_dir, store, db, owner) = setup();
    db.execute_batch(
        "CREATE TRIGGER ignore_audit BEFORE INSERT ON tool_runs BEGIN SELECT RAISE(IGNORE); END;",
    )
    .unwrap();
    assert!(matches!(
        store.start_tool_run(start(owner, true)),
        Err(StoreError::Conflict)
    ));
    db.execute_batch("DROP TRIGGER ignore_audit").unwrap();
    let id = store.start_tool_run(start(owner, true)).unwrap();
    db.execute_batch(
        "CREATE TRIGGER ignore_finish BEFORE UPDATE ON tool_runs BEGIN SELECT RAISE(IGNORE); END;",
    )
    .unwrap();
    assert!(matches!(
        store.finish_tool_run(id, ToolRunFinish::completed()),
        Err(StoreError::Conflict)
    ));
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending
    );
    db.execute_batch("DROP TRIGGER ignore_finish").unwrap();
    complete_work(&store, owner, id);
    db.execute_batch(
        "CREATE TRIGGER ignore_delete BEFORE DELETE ON dialogs BEGIN SELECT RAISE(IGNORE); END;",
    )
    .unwrap();
    let d = store.list_dialogs().unwrap()[0].id;
    assert!(matches!(store.delete_dialog(d), Err(StoreError::Conflict)));
    assert_eq!(store.list_tool_runs().unwrap().len(), 1);
}

#[test]
fn ignored_audit_deletion_cannot_leave_orphaned_owners() {
    let (_dir, store, db, owner) = setup();
    let audit_id = store.start_tool_run(start(owner, true)).unwrap();
    complete_work(&store, owner, audit_id);
    let d = store.list_dialogs().unwrap()[0].id;
    db.execute_batch("CREATE TRIGGER ignore_audit_delete BEFORE DELETE ON tool_runs BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(matches!(store.delete_dialog(d), Err(StoreError::Conflict)));
    assert_eq!(store.list_dialogs().unwrap().len(), 1);
    assert_eq!(store.list_tool_runs().unwrap().len(), 1);
}

#[test]
fn audit_rejects_unstructured_metadata_and_schema_rejects_unsafe_errors() {
    let (_dir, store, db, owner) = setup();
    for value in [
        "https://example.org/token",
        "raw error with secret",
        "",
        &"x".repeat(257),
    ] {
        let mut input = start(owner, true);
        input.tool_name = value.into();
        assert!(matches!(
            store.start_tool_run(input),
            Err(StoreError::InvalidMetadata)
        ));
    }
    let id = store.start_tool_run(start(owner, true)).unwrap();
    assert!(db.execute("UPDATE tool_runs SET status='failed',safe_error_code='secret',finished_at='now' WHERE id=?",[id]).is_err());
    assert!(
        db.execute("UPDATE tool_runs SET status='completed' WHERE id=?", [id])
            .is_err()
    );
    assert!(
        db.execute("UPDATE tool_runs SET read_only=2 WHERE id=?", [id])
            .is_err()
    );
}
