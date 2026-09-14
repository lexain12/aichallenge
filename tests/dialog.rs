use deepseek_cli::chat::Role;
use deepseek_cli::client::TokenUsage;
use deepseek_cli::dialog::{DialogStore, StoreError};

#[test]
fn reopens_messages_in_order_and_lists_latest_activity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    assert!(store.latest_id().unwrap().is_none());
    let first = store
        .start_dialog("Original system", "First question")
        .unwrap();
    let second = store.start_dialog("Other system", "Second dialog").unwrap();
    assert_eq!(store.latest_id().unwrap(), Some(second));
    store
        .append_message(first, 1, Role::Assistant, "First answer")
        .unwrap();
    drop(store);

    let store = DialogStore::open(&path).unwrap();
    assert_eq!(store.latest_id().unwrap(), Some(first));
    let dialog = store.load(first).unwrap();
    assert_eq!(dialog.system_prompt, "Original system");
    assert_eq!(
        dialog
            .messages
            .iter()
            .map(|m| (m.role(), m.content()))
            .collect::<Vec<_>>(),
        [
            (Role::User, "First question"),
            (Role::Assistant, "First answer")
        ]
    );
    let list = store.list().unwrap();
    assert_eq!(
        list.iter()
            .map(|d| (d.id, d.message_count))
            .collect::<Vec<_>>(),
        [(first, 2), (second, 1)]
    );
    assert_eq!(list[0].title, "First question");
}

#[test]
fn stale_writer_cannot_append_or_change_latest_dialog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut first = DialogStore::open(&path).unwrap();
    let mut second = DialogStore::open(&path).unwrap();
    let id = first.start_dialog("System", "Question").unwrap();
    first
        .append_message(id, 1, Role::Assistant, "Answer")
        .unwrap();
    let newest = first.start_dialog("System", "New dialog").unwrap();
    assert!(matches!(
        second.append_message(id, 1, Role::Assistant, "Stale answer"),
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(first.load(id).unwrap().messages.len(), 2);
    assert_eq!(first.latest_id().unwrap(), Some(newest));
    assert!(matches!(first.load(9999), Err(StoreError::NotFound(9999))));
}

#[test]
fn upgrades_day7_database_and_writes_answer_usage_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE dialogs (id INTEGER PRIMARY KEY AUTOINCREMENT, system_prompt TEXT NOT NULL, title TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', last_message_id INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, dialog_id INTEGER NOT NULL REFERENCES dialogs(id), role TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT '');
        INSERT INTO dialogs (id, system_prompt, title, last_message_id) VALUES (1, 'Old system', 'Old question', 2);
        INSERT INTO messages (dialog_id, role, content) VALUES (1, 'user', 'Old question'), (1, 'assistant', 'Old answer');").unwrap();
    let mut store = DialogStore::open(&path).unwrap();
    let old = store.load(1).unwrap();
    assert_eq!(old.messages[1].content(), "Old answer");
    assert_eq!(old.messages[1].usage(), None);
    store
        .append_message(1, 2, Role::User, "New question")
        .unwrap();
    let usage: TokenUsage =
        serde_json::from_str(r#"{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}"#)
            .unwrap();
    connection.execute_batch("CREATE TRIGGER reject_usage BEFORE INSERT ON message_usage BEGIN SELECT RAISE(ABORT, 'usage write failed'); END;").unwrap();
    assert!(
        store
            .append_answer(1, 3, "New answer", Some(usage))
            .is_err()
    );
    assert_eq!(store.load(1).unwrap().messages.len(), 3);
    connection
        .execute_batch("DROP TRIGGER reject_usage;")
        .unwrap();
    store
        .append_answer(1, 3, "New answer", Some(usage))
        .unwrap();
    drop(store);
    let store = DialogStore::open(&path).unwrap();
    assert_eq!(store.load(1).unwrap().messages[3].usage(), Some(usage));
}
