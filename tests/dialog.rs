use std::path::Path;

use deepseek_cli::chat::Role;
use deepseek_cli::client::TokenUsage;
use deepseek_cli::context::ContextSummary;
use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::facts::Facts;

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

fn create_day8_database_with_four_messages(path: &Path) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE dialogs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                system_prompt TEXT NOT NULL,
                title TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT '',
                last_message_id INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE message_usage (
                message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                usage_json TEXT NOT NULL
            );
            INSERT INTO dialogs (id, system_prompt, title, last_message_id)
                VALUES (1, 'System', 'u1', 4);
            INSERT INTO messages (dialog_id, role, content) VALUES
                (1, 'user', 'u1'),
                (1, 'assistant', 'a1'),
                (1, 'user', 'u2'),
                (1, 'assistant', 'a2');",
        )
        .unwrap();
}

#[test]
fn upgrades_day8_database_and_replaces_one_summary_while_accumulating_usage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    create_day8_database_with_four_messages(&path);

    let mut store = DialogStore::open(&path).unwrap();
    let first = store
        .replace_context(
            1,
            4,
            ContextSummary::new("first", 2),
            Some(TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 2,
                total_tokens: 12,
                completion_tokens_details: None,
            }),
        )
        .unwrap();
    assert_eq!(first.compaction_usage().call_count(), 1);

    let second = store
        .replace_context(1, 4, ContextSummary::new("second", 3), None)
        .unwrap();
    assert_eq!(second.summary().unwrap().content(), "second");
    assert_eq!(second.summary().unwrap().covered_message_count(), 3);
    assert_eq!(second.compaction_usage().call_count(), 2);
    assert_eq!(second.compaction_usage().total_tokens(), 12);
    assert_eq!(second.compaction_usage().missing_usage_count(), 1);

    let connection = rusqlite::Connection::open(&path).unwrap();
    let rows: i64 = connection
        .query_row("SELECT count(*) FROM dialog_context", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(store.load(1).unwrap().context, second);
    assert_eq!(store.load(1).unwrap().messages.len(), 4);
}

#[test]
fn stale_or_failed_context_replacement_keeps_previous_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    create_day8_database_with_four_messages(&path);
    let mut store = DialogStore::open(&path).unwrap();
    let first = store
        .replace_context(1, 4, ContextSummary::new("first", 2), None)
        .unwrap();

    assert!(matches!(
        store.replace_context(1, 3, ContextSummary::new("stale", 3), None),
        Err(StoreError::Conflict(1))
    ));
    assert_eq!(store.load(1).unwrap().context, first);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_context BEFORE UPDATE OF summary ON dialog_context
             BEGIN SELECT RAISE(ABORT, 'context write failed'); END;",
        )
        .unwrap();
    assert!(
        store
            .replace_context(1, 4, ContextSummary::new("rejected", 3), None)
            .is_err()
    );
    assert_eq!(store.load(1).unwrap().context, first);
}

#[test]
fn replaces_and_restores_facts_with_cumulative_usage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    create_day8_database_with_four_messages(&path);
    let mut store = DialogStore::open(&path).unwrap();
    let usage = TokenUsage {
        prompt_tokens: 10,
        completion_tokens: 2,
        total_tokens: 12,
        completion_tokens_details: None,
    };

    let first = store
        .replace_facts(
            1,
            4,
            Facts::from([("goal".into(), "ship CLI".into())]),
            Some(usage),
        )
        .unwrap();
    assert_eq!(first.covered_message_count(), 4);
    assert_eq!(first.update_usage().call_count(), 1);

    store
        .append_message(1, 4, Role::User, "deadline Monday")
        .unwrap();
    let second = store
        .replace_facts(
            1,
            5,
            Facts::from([
                ("goal".into(), "ship CLI".into()),
                ("deadline".into(), "Monday".into()),
            ]),
            None,
        )
        .unwrap();
    drop(store);

    let restored = DialogStore::open(&path).unwrap().load(1).unwrap().facts;
    assert_eq!(restored, second);
    assert_eq!(restored.facts()["deadline"], "Monday");
    assert_eq!(restored.update_usage().call_count(), 2);
    assert_eq!(restored.update_usage().total_tokens(), 12);
    assert_eq!(restored.update_usage().missing_usage_count(), 1);
}

#[test]
fn stale_failed_or_malformed_facts_never_replace_valid_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    create_day8_database_with_four_messages(&path);
    let mut store = DialogStore::open(&path).unwrap();
    let first = store
        .replace_facts(1, 4, Facts::from([("goal".into(), "ship".into())]), None)
        .unwrap();

    assert!(matches!(
        store.replace_facts(1, 3, Facts::new(), None),
        Err(StoreError::Conflict(1))
    ));
    assert_eq!(store.load(1).unwrap().facts, first);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_facts BEFORE UPDATE OF facts_json ON dialog_facts
             BEGIN SELECT RAISE(ABORT, 'facts write failed'); END;",
        )
        .unwrap();
    assert!(store.replace_facts(1, 4, Facts::new(), None).is_err());
    assert_eq!(store.load(1).unwrap().facts, first);
    connection
        .execute_batch("DROP TRIGGER reject_facts;")
        .unwrap();
    connection
        .execute(
            "UPDATE dialog_facts SET facts_json = '[1]' WHERE dialog_id = 1",
            [],
        )
        .unwrap();
    assert!(matches!(store.load(1), Err(StoreError::InvalidFacts(_))));
}
