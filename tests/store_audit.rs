use deepseek_cli::domain::{RunId, ToolOwner, ToolRunStatus, TurnId};
use deepseek_cli::store::{SafeErrorCode, Store, StoreError, ToolRunFinish, ToolRunStart};
use rusqlite::Connection;

fn setup() -> (tempfile::TempDir, Store, Connection, ToolOwner) {
    let dir = tempfile::tempdir().unwrap();
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
            "finished_at"
        ]
    );
    for forbidden in ["argument", "hash", "result", "url", "raw_error"] {
        assert!(columns.iter().all(|c| !c.contains(forbidden)));
    }
}

#[test]
fn write_recovery_becomes_uncertain() {
    let (_dir, store, _, owner) = setup();
    store.start_tool_run(start(owner, false)).unwrap();
    assert_eq!(store.recover_pending_tool_runs().unwrap(), 1);
    let row = &store.list_tool_runs().unwrap()[0];
    assert_eq!(row.status, ToolRunStatus::Uncertain);
    assert_eq!(row.safe_error_code, Some(SafeErrorCode::ProcessInterrupted));
    assert!(row.finished_at.is_some());
    assert_eq!(store.recover_pending_tool_runs().unwrap(), 0);
}

#[test]
fn read_only_recovery_becomes_failed() {
    let (_dir, store, _, owner) = setup();
    let id = store.start_tool_run(start(owner, true)).unwrap();
    store.recover_pending_tool_runs().unwrap();
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Failed
    );
    assert!(matches!(
        store.finish_tool_run(id, ToolRunFinish::completed()),
        Err(StoreError::Conflict)
    ));
}

#[test]
fn dialog_delete_is_transactional() {
    let (_dir, store, db, owner) = setup();
    store.start_tool_run(start(owner, true)).unwrap();
    let d = store.list_dialogs().unwrap()[0].id;
    db.execute_batch("CREATE TRIGGER reject_delete BEFORE DELETE ON dialogs BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(store.delete_dialog(d).is_err());
    for table in ["dialogs", "turns", "messages", "tool_runs"] {
        assert_eq!(
            db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
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
    store.start_tool_run(start(owner, true)).unwrap();
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
