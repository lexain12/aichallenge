use std::path::Path;

use deepseek_cli::chat::Role;
use deepseek_cli::client::TokenUsage;
use deepseek_cli::context::ContextSummary;
use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::facts::Facts;
use deepseek_cli::memory::{
    DEFAULT_TASK_ID, DEFAULT_USER_ID, DurableMemoryScope, MemoryAddress, MemoryError,
    MemoryRepository, RequestScope,
};

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

#[test]
fn fork_copies_checkpoint_state_and_branches_continue_independently() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let original = store.start_dialog("System", "u1").unwrap();
    let usage = TokenUsage {
        prompt_tokens: 4,
        completion_tokens: 2,
        total_tokens: 6,
        completion_tokens_details: None,
    };
    store.append_answer(original, 1, "a1", Some(usage)).unwrap();
    store.append_message(original, 2, Role::User, "u2").unwrap();
    store.append_answer(original, 3, "a2", None).unwrap();
    store
        .replace_context(original, 4, ContextSummary::new("summary", 2), Some(usage))
        .unwrap();
    store
        .replace_facts(
            original,
            4,
            Facts::from([("goal".into(), "branch safely".into())]),
            Some(usage),
        )
        .unwrap();

    let fork = store.fork_dialog(original, 4).unwrap();

    assert_eq!(fork.original_dialog_id, original);
    assert_ne!(fork.new_dialog_id, original);
    assert_eq!(fork.checkpoint_message_count, 4);
    let left = store.load(original).unwrap();
    let right = store.load(fork.new_dialog_id).unwrap();
    assert_eq!(right.messages, left.messages);
    assert_eq!(right.messages[1].usage(), Some(usage));
    assert_eq!(right.context, left.context);
    assert_eq!(right.facts, left.facts);
    assert_eq!(left.branch.as_ref().unwrap().branch_group_id, original);
    assert_eq!(right.branch.as_ref().unwrap().branch_group_id, original);
    assert_eq!(
        right.branch.as_ref().unwrap().parent_dialog_id,
        Some(original)
    );

    store
        .append_message(original, 4, Role::User, "left only")
        .unwrap();
    store
        .append_message(fork.new_dialog_id, 4, Role::User, "right only")
        .unwrap();
    let left = store.load(original).unwrap();
    let right = store.load(fork.new_dialog_id).unwrap();
    assert_eq!(left.messages.last().unwrap().content(), "left only");
    assert_eq!(right.messages.last().unwrap().content(), "right only");

    let selected = store
        .load_branch_member(original, fork.new_dialog_id)
        .unwrap();
    assert_eq!(selected.id, fork.new_dialog_id);
}

#[test]
fn failed_or_stale_fork_rolls_back_and_unrelated_dialog_cannot_be_selected() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let original = store.start_dialog("System", "u1").unwrap();
    let unrelated = store.start_dialog("System", "other").unwrap();

    assert!(matches!(
        store.fork_dialog(original, 2),
        Err(StoreError::Conflict(found)) if found == original
    ));
    assert_eq!(store.list().unwrap().len(), 2);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER reject_branch_copy BEFORE INSERT ON messages
             WHEN NEW.dialog_id NOT IN ({original}, {unrelated})
             BEGIN SELECT RAISE(ABORT, 'copy failed'); END;"
        ))
        .unwrap();
    assert!(store.fork_dialog(original, 1).is_err());
    assert_eq!(store.list().unwrap().len(), 2);
    connection
        .execute_batch("DROP TRIGGER reject_branch_copy;")
        .unwrap();

    let fork = store.fork_dialog(original, 1).unwrap();
    assert!(matches!(
        store.load_branch_member(original, unrelated),
        Err(StoreError::UnrelatedBranch { current, target })
            if current == original && target == unrelated
    ));
    assert!(matches!(
        store.load_branch_member(original, 9999),
        Err(StoreError::NotFound(9999))
    ));
    assert_eq!(store.list().unwrap().len(), 3);
    assert_eq!(store.load(fork.new_dialog_id).unwrap().messages.len(), 1);
}

#[test]
fn dialog_scope_is_persisted_listed_and_copied_to_branches() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    let id = store
        .start_dialog_in_scope(&scope, "System", "Question")
        .unwrap();
    let fork = store.fork_dialog(id, 1).unwrap();

    assert_eq!(store.load(id).unwrap().scope.user_id(), "alice");
    assert_eq!(store.load(id).unwrap().scope.task_id(), "bot");
    assert_eq!(
        store.load(fork.new_dialog_id).unwrap().scope,
        store.load(id).unwrap().scope
    );
    assert_eq!(store.list().unwrap()[0].scope.user_id(), "alice");
}

#[test]
fn legacy_dialogs_are_migrated_to_default_scope() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    create_day8_database_with_four_messages(&path);

    let store = DialogStore::open(&path).unwrap();
    let scope = store.load(1).unwrap().scope;

    assert_eq!(scope.user_id(), DEFAULT_USER_ID);
    assert_eq!(scope.task_id(), DEFAULT_TASK_ID);
}

#[test]
fn transcript_hides_only_controller_inputs_while_stage_protocol_keeps_them() {
    use deepseek_cli::workflow::StageRunId;
    use deepseek_cli::workflow_store::{ProtocolSource, WorkflowRepository};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let id = store.start_dialog("System", "legacy user").unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch(
        r#"PRAGMA foreign_keys = ON;
        BEGIN;
        INSERT INTO messages (id, dialog_id, role, content) VALUES
        (2, 1, 'user', 'human input'), (3, 1, 'assistant', 'first answer'),
        (4, 1, 'user', 'hidden controller'), (5, 1, 'assistant', 'second answer');
        INSERT INTO workflow_tasks (id, dialog_id, ordinal, phase, status, goal, plan_json, checkpoint_json, version)
        VALUES (1, 1, 1, 'planning', 'active', 'Build it',
        '{"revision":0,"steps":[],"acceptance_criteria":[]}', '{"summary":"","decisions":[],"open_issues":[]}', 0);
        INSERT INTO task_stage_runs (id, workflow_task_id, phase, sequence) VALUES (1, 1, 'planning', 1);
        UPDATE workflow_tasks SET current_stage_run_id = 1;
        INSERT INTO dialog_workflow_state VALUES (1, 1);
        INSERT INTO message_task_stages VALUES (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1), (5, 1, 1);
        INSERT INTO workflow_inputs (dialog_id, message_id, source, intent_json, outcome) VALUES (1, 2, 'human', '{}', 'accepted');
        INSERT INTO response_processing (id, assistant_message_id, checker_name, expected_version, status) VALUES (1, 3, 'continuation', 0, 'completed');
        INSERT INTO workflow_inputs (dialog_id, message_id, source, checker_name, model_name, triggering_assistant_message_id, intent_json, outcome, processing_id)
        VALUES (1, 4, 'controller', 'continuation', 'model', 3, '{}', 'accepted', 1);
        COMMIT;"#
    ).unwrap();
    let dialog = store.load(id).unwrap();
    assert_eq!(
        dialog
            .messages
            .iter()
            .map(|message| message.content())
            .collect::<Vec<_>>(),
        [
            "legacy user",
            "human input",
            "first answer",
            "second answer"
        ]
    );
    let protocol = store.load_stage_messages(StageRunId(1)).unwrap();
    assert_eq!(
        protocol
            .iter()
            .map(|message| message.message_id)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    assert_eq!(protocol[3].source, ProtocolSource::Controller);
    assert_eq!(protocol[3].message.role(), Role::User);
    assert_eq!(store.list().unwrap()[0].title, "legacy user");
}

#[test]
fn durable_memory_is_isolated_by_user_and_task() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let alice_bot = RequestScope::new("alice", "bot").unwrap();
    let alice_other = RequestScope::new("alice", "other").unwrap();
    let bob_bot = RequestScope::new("bob", "bot").unwrap();

    store
        .upsert_memory(
            &alice_bot.address(DurableMemoryScope::User),
            "language",
            "Russian",
        )
        .unwrap();
    store
        .upsert_memory(
            &alice_bot.address(DurableMemoryScope::Task),
            "stack",
            "Rust",
        )
        .unwrap();

    let first = store.load_memory(&alice_bot).unwrap();
    assert_eq!(first.user_entries()["language"], "Russian");
    assert_eq!(first.task_entries()["stack"], "Rust");
    assert_eq!(
        store.load_memory(&alice_other).unwrap().user_entries()["language"],
        "Russian"
    );
    assert!(
        store
            .load_memory(&alice_other)
            .unwrap()
            .task_entries()
            .is_empty()
    );
    assert!(
        store
            .load_memory(&bob_bot)
            .unwrap()
            .user_entries()
            .is_empty()
    );
    assert!(
        store
            .load_memory(&bob_bot)
            .unwrap()
            .task_entries()
            .is_empty()
    );
}

#[test]
fn upsert_replaces_and_delete_reports_whether_a_key_existed() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    let address = scope.address(DurableMemoryScope::User);

    store
        .upsert_memory(&address, "language", "English")
        .unwrap();
    store
        .upsert_memory(&address, "language", "Russian")
        .unwrap();
    assert_eq!(
        store.load_memory(&scope).unwrap().user_entries()["language"],
        "Russian"
    );
    assert!(store.delete_memory(&address, "language").unwrap());
    assert!(!store.delete_memory(&address, "language").unwrap());
}

fn blank_memory_addresses() -> Vec<(MemoryAddress, MemoryError)> {
    vec![
        (
            MemoryAddress::User { user_id: "".into() },
            MemoryError::BlankUserId,
        ),
        (
            MemoryAddress::User {
                user_id: " \t ".into(),
            },
            MemoryError::BlankUserId,
        ),
        (
            MemoryAddress::Task {
                user_id: "".into(),
                task_id: "bot".into(),
            },
            MemoryError::BlankUserId,
        ),
        (
            MemoryAddress::Task {
                user_id: " \t ".into(),
                task_id: "bot".into(),
            },
            MemoryError::BlankUserId,
        ),
        (
            MemoryAddress::Task {
                user_id: "alice".into(),
                task_id: "".into(),
            },
            MemoryError::BlankTaskId,
        ),
        (
            MemoryAddress::Task {
                user_id: "alice".into(),
                task_id: " \t ".into(),
            },
            MemoryError::BlankTaskId,
        ),
    ]
}

#[test]
fn memory_address_upsert_rejects_blank_identifiers_before_sql() {
    // Catch accepting public enum variants that no valid RequestScope can reach.
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    for (address, expected) in blank_memory_addresses() {
        let result = store.upsert_memory(&address, "key", "value");
        assert!(
            matches!(&result, Err(StoreError::InvalidMemory(error)) if *error == expected),
            "{address:?}: {result:?}"
        );
    }
    let connection = rusqlite::Connection::open(database).unwrap();
    let rows: i64 = connection
        .query_row("SELECT count(*) FROM memory_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0);
}

#[test]
fn memory_address_delete_rejects_blank_identifiers_before_sql() {
    // Catch treating an invalid address as a successful missing-key lookup.
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    for (address, expected) in blank_memory_addresses() {
        let result = store.delete_memory(&address, "key");
        assert!(
            matches!(&result, Err(StoreError::InvalidMemory(error)) if *error == expected),
            "{address:?}: {result:?}"
        );
    }
}

#[test]
fn memory_address_upsert_normalizes_padding_to_the_existing_entry() {
    // Catch unreachable duplicate rows when callers construct padded enum variants.
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    store
        .upsert_memory(
            &scope.address(DurableMemoryScope::User),
            "language",
            "English",
        )
        .unwrap();
    store
        .upsert_memory(&scope.address(DurableMemoryScope::Task), "stack", "Go")
        .unwrap();
    store
        .upsert_memory(
            &MemoryAddress::User {
                user_id: " alice ".into(),
            },
            "language",
            "Russian",
        )
        .unwrap();
    store
        .upsert_memory(
            &MemoryAddress::Task {
                user_id: " alice ".into(),
                task_id: " bot ".into(),
            },
            "stack",
            "Rust",
        )
        .unwrap();

    let snapshot = store.load_memory(&scope).unwrap();
    assert_eq!(
        snapshot.user_entries(),
        &[("language".into(), "Russian".into())].into()
    );
    assert_eq!(
        snapshot.task_entries(),
        &[("stack".into(), "Rust".into())].into()
    );
}

#[test]
fn memory_address_delete_normalizes_padding_to_the_existing_entry() {
    // Catch padded delete addresses silently leaving canonical entries intact.
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    store
        .upsert_memory(
            &scope.address(DurableMemoryScope::User),
            "language",
            "Russian",
        )
        .unwrap();
    store
        .upsert_memory(&scope.address(DurableMemoryScope::Task), "stack", "Rust")
        .unwrap();
    assert!(
        store
            .delete_memory(
                &MemoryAddress::User {
                    user_id: " alice ".into()
                },
                "language"
            )
            .unwrap()
    );
    assert!(
        store
            .delete_memory(
                &MemoryAddress::Task {
                    user_id: " alice ".into(),
                    task_id: " bot ".into()
                },
                "stack"
            )
            .unwrap()
    );

    let snapshot = store.load_memory(&scope).unwrap();
    assert!(snapshot.user_entries().is_empty());
    assert!(snapshot.task_entries().is_empty());
}
