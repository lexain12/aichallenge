use deepseek_cli::{
    chat::Role,
    dialog::DialogStore,
    memory::RequestScope,
    tool_audit::{
        ToolExecutionErrorCode, ToolExecutionFinish, ToolExecutionStart, ToolExecutionStatus,
    },
};

fn start(store: &mut DialogStore, dialog: i64, message: i64, call: &str, args: &str) -> i64 {
    store
        .start_tool_execution(ToolExecutionStart {
            dialog_id: dialog,
            input_message_id: message,
            tool_call_id: call,
            server_name: "telegram",
            tool_name: "send_message",
            arguments_json: args,
        })
        .unwrap()
}

#[test]
fn audit_stores_only_metadata_and_survives_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("audit.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let (dialog, message) = store
        .start_dialog_with_message_id("System", "user request")
        .unwrap();
    let id = start(
        &mut store,
        dialog,
        message,
        "call-1",
        r#"{"text":"PRIVATE_PAYLOAD_MARKER_8943","chat":"me"}"#,
    );
    let row = store.tool_execution(id).unwrap().unwrap();
    assert_eq!(row.status, ToolExecutionStatus::Started);
    assert!(!row.is_error);
    assert!(row.error_code.is_none());
    assert!(row.finished_at.is_none());
    assert!(!row.started_at.is_empty());
    assert_eq!(row.arguments_hash.len(), 64);
    assert!(
        row.arguments_hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    store
        .finish_tool_execution(id, ToolExecutionFinish::succeeded())
        .unwrap();
    drop(store);
    let reopened = DialogStore::open(&path).unwrap();
    let row = reopened.tool_execution(id).unwrap().unwrap();
    assert_eq!((row.dialog_id, row.input_message_id), (dialog, message));
    assert_eq!(
        (
            row.tool_call_id.as_str(),
            row.server_name.as_str(),
            row.tool_name.as_str()
        ),
        ("call-1", "telegram", "send_message")
    );
    assert_eq!(row.status, ToolExecutionStatus::Succeeded);
    assert!(row.finished_at.is_some());
    drop(reopened);
    let connection = rusqlite::Connection::open(&path).unwrap();
    let columns: Vec<String> = connection
        .prepare("PRAGMA table_info(tool_executions)")
        .unwrap()
        .query_map([], |r| r.get(1))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        columns,
        [
            "id",
            "dialog_id",
            "input_message_id",
            "tool_call_id",
            "server_name",
            "tool_name",
            "arguments_hash",
            "status",
            "is_error",
            "error_code",
            "started_at",
            "finished_at"
        ]
    );
    drop(connection);
    assert!(
        !std::fs::read(path)
            .unwrap()
            .windows(b"PRIVATE_PAYLOAD_MARKER_8943".len())
            .any(|w| w == b"PRIVATE_PAYLOAD_MARKER_8943")
    );
}

#[test]
fn hash_canonicalizes_nested_objects_but_preserves_arrays_and_types() {
    let mut store = DialogStore::open(std::path::Path::new(":memory:")).unwrap();
    let (d, m) = store
        .start_dialog_with_message_id("System", "Question")
        .unwrap();
    let cases = [
        r#"{"b":[{"z":2,"a":1},true],"a":null}"#,
        r#"{ "a":null,"b":[{"a":1,"z":2},true]}"#,
        r#"{"a":null,"b":[true,{"a":1,"z":2}]}"#,
        r#"{"a":null,"b":[{"a":"1","z":2},true]}"#,
        "{}",
    ];
    let hashes: Vec<_> = cases
        .iter()
        .enumerate()
        .map(|(i, args)| {
            let id = start(&mut store, d, m, &format!("call-{i}"), args);
            store.tool_execution(id).unwrap().unwrap().arguments_hash
        })
        .collect();
    assert_eq!(hashes[0], hashes[1]);
    assert_ne!(hashes[0], hashes[2]);
    assert_ne!(hashes[0], hashes[3]);
    assert_eq!(
        hashes[4],
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
}

#[test]
fn invalid_arguments_never_insert_or_leak_into_errors() {
    let mut store = DialogStore::open(std::path::Path::new(":memory:")).unwrap();
    let (d, m) = store
        .start_dialog_with_message_id("System", "Question")
        .unwrap();
    for args in [
        "PRIVATE_BAD_ARGS",
        "[]",
        "null",
        "1",
        r#""PRIVATE_BAD_ARGS""#,
    ] {
        let error = store
            .start_tool_execution(ToolExecutionStart {
                dialog_id: d,
                input_message_id: m,
                tool_call_id: "call",
                server_name: "telegram",
                tool_name: "send_message",
                arguments_json: args,
            })
            .unwrap_err();
        assert!(!format!("{error:?} {error}").contains("PRIVATE_BAD_ARGS"));
    }
    assert!(store.tool_executions(d).unwrap().is_empty());
    start(&mut store, d, m, "call", "{}");
}

#[test]
fn finalization_is_one_way_and_identical_retries_preserve_the_row() {
    let mut store = DialogStore::open(std::path::Path::new(":memory:")).unwrap();
    let (d, m) = store
        .start_dialog_with_message_id("System", "Question")
        .unwrap();
    for (i, finish, status) in [
        (
            0,
            ToolExecutionFinish::succeeded(),
            ToolExecutionStatus::Succeeded,
        ),
        (
            1,
            ToolExecutionFinish::failed(ToolExecutionErrorCode::new("mcp_tool_error").unwrap()),
            ToolExecutionStatus::Failed,
        ),
        (
            2,
            ToolExecutionFinish::uncertain(ToolExecutionErrorCode::new("timeout").unwrap()),
            ToolExecutionStatus::Uncertain,
        ),
    ] {
        let id = start(&mut store, d, m, &format!("call-{i}"), "{}");
        store.finish_tool_execution(id, finish.clone()).unwrap();
        let before = store.tool_execution(id).unwrap().unwrap();
        assert_eq!(before.status, status);
        assert_eq!(before.is_error, i != 0);
        assert_eq!(
            before.error_code.as_ref().map(|code| code.as_str()),
            match i {
                0 => None,
                1 => Some("mcp_tool_error"),
                _ => Some("timeout"),
            }
        );
        store.finish_tool_execution(id, finish).unwrap();
        assert_eq!(store.tool_execution(id).unwrap().unwrap(), before);
        let changed = if i == 0 {
            ToolExecutionFinish::failed(ToolExecutionErrorCode::new("transport").unwrap())
        } else {
            ToolExecutionFinish::succeeded()
        };
        assert!(store.finish_tool_execution(id, changed).is_err());
        assert_eq!(store.tool_execution(id).unwrap().unwrap(), before);
    }
    assert!(
        store
            .finish_tool_execution(9999, ToolExecutionFinish::succeeded())
            .is_err()
    );
    assert!(store.tool_execution(9999).unwrap().is_none());
}

#[test]
fn error_codes_accept_bounded_machine_codes_and_reject_details() {
    for code in [
        "ambiguous_chat",
        "delivery_unknown",
        "blocked_by_invariant",
        "remote-error-42",
    ] {
        assert_eq!(ToolExecutionErrorCode::new(code).unwrap().as_str(), code);
    }
    assert!(ToolExecutionErrorCode::new(&"a".repeat(64)).is_ok());
    for code in [
        "",
        "PRIVATE_ERROR_DETAIL",
        "failed because private message",
        "error\nprivate",
        "https://secret",
        "ошибка",
        &"a".repeat(65),
    ] {
        let error = ToolExecutionErrorCode::new(code).unwrap_err();
        assert!(!format!("{error:?} {error}").contains("PRIVATE_ERROR_DETAIL"));
    }
    let mut store = DialogStore::open(std::path::Path::new(":memory:")).unwrap();
    let (d, m) = store
        .start_dialog_with_message_id("System", "Question")
        .unwrap();
    let id = start(&mut store, d, m, "call", "{}");
    store
        .finish_tool_execution(
            id,
            ToolExecutionFinish::uncertain(
                ToolExecutionErrorCode::new("delivery_unknown").unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(
        store
            .tool_execution(id)
            .unwrap()
            .unwrap()
            .error_code
            .unwrap()
            .as_str(),
        "delivery_unknown"
    );
}

#[test]
fn linkage_requires_own_user_message_and_call_is_unique_per_input() {
    let mut store = DialogStore::open(std::path::Path::new(":memory:")).unwrap();
    let (d, m) = store
        .start_dialog_with_message_id("System", "Question")
        .unwrap();
    let (other, other_message) = store
        .start_dialog_with_message_id("System", "Other")
        .unwrap();
    let answer = store
        .append_message_with_id(d, 1, Role::Assistant, "Answer")
        .unwrap();
    for (dialog, message) in [
        (d, other_message),
        (other, m),
        (999, m),
        (d, 999),
        (d, answer),
    ] {
        assert!(
            store
                .start_tool_execution(ToolExecutionStart {
                    dialog_id: dialog,
                    input_message_id: message,
                    tool_call_id: "call",
                    server_name: "telegram",
                    tool_name: "send_message",
                    arguments_json: "{}"
                })
                .is_err()
        );
    }
    start(&mut store, d, m, "call", "{}");
    assert!(
        store
            .start_tool_execution(ToolExecutionStart {
                dialog_id: d,
                input_message_id: m,
                tool_call_id: "call",
                server_name: "other",
                tool_name: "other",
                arguments_json: "{}"
            })
            .is_err()
    );
    let next = store
        .append_message_with_id(d, 2, Role::User, "Next")
        .unwrap();
    start(&mut store, d, next, "call", "{}");
    assert!(
        store
            .append_message_with_id(d, 2, Role::User, "Stale")
            .is_err()
    );
    assert_eq!(store.load(d).unwrap().messages.len(), 3);
    assert_eq!(store.tool_executions(d).unwrap().len(), 2);
}

#[test]
fn scoped_start_returns_the_inserted_message_and_branch_does_not_copy_audits() {
    let mut store = DialogStore::open(std::path::Path::new(":memory:")).unwrap();
    let scope = RequestScope::new("alice", "task").unwrap();
    let (d, m) = store
        .start_dialog_in_scope_with_message_id(&scope, "System", "Question")
        .unwrap();
    assert_eq!(store.load(d).unwrap().scope, scope);
    let id = start(&mut store, d, m, "call", "{}");
    let branch = store.fork_dialog(d, 1).unwrap().new_dialog_id;
    assert!(store.tool_executions(branch).unwrap().is_empty());
    assert_eq!(store.tool_executions(d).unwrap()[0].id, id);
}

#[test]
fn legacy_database_migrates_idempotently_and_enforces_ownership_in_sql() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("legacy.sqlite3");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE dialogs (id INTEGER PRIMARY KEY AUTOINCREMENT, system_prompt TEXT NOT NULL, title TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', last_message_id INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, dialog_id INTEGER NOT NULL REFERENCES dialogs(id), role TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT '');
    INSERT INTO dialogs(id,system_prompt,title) VALUES(1,'System','Question'),(2,'System','Other');
    INSERT INTO messages(id,dialog_id,role,content) VALUES(1,1,'user','Question'),(2,2,'user','Other');").unwrap();
    drop(db);
    let mut store = DialogStore::open(&path).unwrap();
    let id = start(&mut store, 1, 1, "legacy-call", "{}");
    drop(store);
    let store = DialogStore::open(&path).unwrap();
    assert_eq!(store.load(1).unwrap().messages[0].content(), "Question");
    assert_eq!(
        store.tool_execution(id).unwrap().unwrap().input_message_id,
        1
    );
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON").unwrap();
    assert!(
        db.execute(
            "UPDATE tool_executions SET input_message_id=2 WHERE id=?1",
            [id]
        )
        .is_err()
    );
    assert!(
        db.execute("UPDATE tool_executions SET dialog_id=2 WHERE id=?1", [id])
            .is_err()
    );
}

#[test]
fn ignored_message_insertion_never_returns_a_stale_id_or_partial_dialog() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("atomic.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let (dialog, _) = store
        .start_dialog_with_message_id("System", "Question")
        .unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER ignore_message BEFORE INSERT ON messages BEGIN SELECT RAISE(IGNORE); END;",
    )
    .unwrap();
    assert!(
        store
            .start_dialog_with_message_id("System", "Ignored")
            .is_err()
    );
    assert_eq!(store.list().unwrap().len(), 1);
    assert!(
        store
            .append_message_with_id(dialog, 1, Role::User, "Ignored")
            .is_err()
    );
    assert_eq!(store.load(dialog).unwrap().messages.len(), 1);
}
