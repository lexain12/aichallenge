use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::workflow::{StageRunId, TaskPhase, TaskStatus};
use deepseek_cli::workflow_store::{ProcessingStatus, ProtocolSource, WorkflowRepository};
use rusqlite::Connection;

#[test]
fn opening_a_legacy_database_adds_workflow_tables_without_reclassifying_messages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch(
        "CREATE TABLE dialogs (id INTEGER PRIMARY KEY AUTOINCREMENT, system_prompt TEXT NOT NULL,
         title TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', last_message_id INTEGER NOT NULL DEFAULT 0);
         CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
         role TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT '');
         INSERT INTO dialogs (id, system_prompt, title, last_message_id) VALUES (1, 'System', 'u1', 4);
         INSERT INTO messages (dialog_id, role, content) VALUES
         (1, 'user', 'u1'), (1, 'assistant', 'a1'), (1, 'user', 'u2'), (1, 'assistant', 'a2');"
    ).unwrap();
    for _ in 0..2 {
        let store = DialogStore::open(&path).unwrap();
        assert_eq!(store.load(1).unwrap().messages.len(), 4);
        assert!(store.load_workflow(1).unwrap().current_task.is_none());
        assert!(
            store
                .load_stage_messages(StageRunId(999))
                .unwrap()
                .is_empty()
        );
        for table in [
            "workflow_tasks",
            "dialog_workflow_state",
            "task_stage_runs",
            "task_stage_context",
            "workflow_inputs",
            "message_task_stages",
            "response_processing",
            "task_transitions",
        ] {
            let rows: i64 = connection
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(rows, 0, "legacy messages must stay unmanaged: {table}");
        }
        assert!(
            !connection
                .prepare("PRAGMA foreign_key_check")
                .unwrap()
                .exists([])
                .unwrap()
        );
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    store: DialogStore,
    connection: Connection,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("workflow.sqlite3");
        let mut store = DialogStore::open(&path).unwrap();
        assert_eq!(store.start_dialog("System", "build it").unwrap(), 1);
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(
            r#"PRAGMA foreign_keys = ON;
            BEGIN;
            INSERT INTO workflow_tasks (id, dialog_id, ordinal, phase, status, goal, plan_json, checkpoint_json, version)
            VALUES (1, 1, 1, 'planning', 'paused', 'Build a CLI',
            '{"revision":1,"steps":[{"id":"design","description":"Design it","status":"pending"}],"acceptance_criteria":["Tests pass"]}',
            '{"summary":"Agreed scope","decisions":["Use SQLite"],"open_issues":[]}', 3);
            INSERT INTO task_stage_runs (id, workflow_task_id, phase, sequence) VALUES (1, 1, 'planning', 1);
            UPDATE workflow_tasks SET current_stage_run_id = 1, current_step_id = 'design', expected_action = 'Review plan' WHERE id = 1;
            INSERT INTO dialog_workflow_state VALUES (1, 1);
            INSERT INTO message_task_stages VALUES (1, 1, 1);
            COMMIT;"#
        ).unwrap();
        Self {
            _directory: directory,
            store,
            connection,
        }
    }
}

#[test]
fn restores_the_selected_projection_without_resuming_or_changing_it() {
    let fixture = Fixture::new();
    let task = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(task.id.0, 1);
    assert_eq!(task.dialog_id, 1);
    assert_eq!(task.ordinal, 1);
    assert_eq!(task.phase, TaskPhase::Planning);
    assert_eq!(task.status, TaskStatus::Paused);
    assert_eq!(task.goal, "Build a CLI");
    assert_eq!(task.plan.revision, 1);
    assert_eq!(task.plan.steps[0].id, "design");
    assert_eq!(task.plan.acceptance_criteria, ["Tests pass"]);
    assert_eq!(task.current_step_id.as_deref(), Some("design"));
    assert_eq!(task.expected_action.as_deref(), Some("Review plan"));
    assert_eq!(task.checkpoint.summary, "Agreed scope");
    assert_eq!(task.checkpoint.decisions, ["Use SQLite"]);
    assert_eq!(task.current_stage_run_id, StageRunId(1));
    assert_eq!(task.current_stage_sequence, 1);
    assert_eq!(task.incoming_handoff_id, None);
    assert_eq!(task.version, 3);
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap(),
        task
    );
    assert!(matches!(
        fixture.store.load_workflow(999),
        Err(StoreError::NotFound(999))
    ));
}

#[test]
fn restoration_rejects_corrupt_workflow_rows() {
    for corruption in [
        "UPDATE workflow_tasks SET phase = 'unknown'",
        "UPDATE workflow_tasks SET status = 'unknown'",
        "UPDATE workflow_tasks SET version = -1",
        "UPDATE workflow_tasks SET version = 'invalid'",
        "UPDATE workflow_tasks SET ordinal = -1",
        "UPDATE workflow_tasks SET ordinal = 4294967296",
        "UPDATE dialog_workflow_state SET current_task_id = 999",
        "UPDATE workflow_tasks SET dialog_id = 999",
        "UPDATE workflow_tasks SET current_stage_run_id = NULL",
        "UPDATE workflow_tasks SET current_stage_run_id = 999",
        "UPDATE task_stage_runs SET workflow_task_id = 999",
        "UPDATE task_stage_runs SET finished_at = '9999-01-01'",
        "UPDATE task_stage_runs SET phase = 'execution'",
        "UPDATE task_stage_runs SET sequence = -1",
        "UPDATE task_stage_runs SET sequence = 4294967296",
        "UPDATE workflow_tasks SET plan_json = '{'",
        "UPDATE workflow_tasks SET plan_json = '{\"revision\":-1,\"steps\":[],\"acceptance_criteria\":[]}'",
        "UPDATE workflow_tasks SET checkpoint_json = '{\"summary\":\"s\",\"decisions\":[],\"open_issues\":[],\"extra\":true}'",
        "UPDATE workflow_tasks SET goal = '   '",
        "UPDATE workflow_tasks SET current_step_id = 'missing'",
        "UPDATE workflow_tasks SET expected_action = ''",
        "UPDATE workflow_tasks SET incoming_handoff_id = 999",
        "UPDATE workflow_tasks SET phase = 'done'; UPDATE task_stage_runs SET phase = 'done'",
    ] {
        let fixture = Fixture::new();
        fixture
            .connection
            .execute_batch("PRAGMA foreign_keys = OFF; PRAGMA ignore_check_constraints = ON;")
            .unwrap();
        fixture.connection.execute_batch(corruption).unwrap();
        let result = fixture.store.load_workflow(1);
        assert!(
            matches!(result, Err(StoreError::InvalidWorkflow(_))),
            "{corruption}: {result:?}"
        );
    }
}

#[test]
fn schema_enforces_one_unfinished_task_but_allows_completed_history() {
    let fixture = Fixture::new();
    let insert = "INSERT INTO workflow_tasks (dialog_id, ordinal, phase, status, goal, plan_json, checkpoint_json, version)
                  SELECT dialog_id, 2, phase, status, goal, plan_json, checkpoint_json, version FROM workflow_tasks WHERE id = 1";
    assert!(fixture.connection.execute(insert, []).is_err());
    fixture
        .connection
        .execute(
            "UPDATE workflow_tasks SET phase = 'done', status = 'active'",
            [],
        )
        .unwrap();
    fixture.connection.execute(insert, []).unwrap();
}

#[test]
fn stage_protocol_preserves_order_usage_and_provenance_without_other_stages() {
    let fixture = Fixture::new();
    fixture.connection.execute_batch(
        r#"INSERT INTO messages (id, dialog_id, role, content) VALUES
        (2, 1, 'user', 'human'), (3, 1, 'assistant', 'answer'),
        (4, 1, 'user', 'controller'), (5, 1, 'assistant', 'next answer'),
        (6, 1, 'user', 'other stage'), (7, 1, 'user', 'unmapped');
        INSERT INTO task_stage_runs (id, workflow_task_id, phase, sequence) VALUES (2, 1, 'execution', 2);
        INSERT INTO message_task_stages VALUES (2, 1, 1), (3, 1, 1), (4, 1, 1), (5, 1, 1), (6, 1, 2);
        INSERT INTO message_usage VALUES (3, '{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}');
        INSERT INTO response_processing (id, assistant_message_id, checker_name, expected_version, status) VALUES (1, 3, 'continuation', 3, 'completed');
        INSERT INTO workflow_inputs (dialog_id, message_id, source, intent_json, outcome) VALUES (1, 2, 'human', '{}', 'accepted');
        INSERT INTO workflow_inputs (dialog_id, message_id, source, checker_name, model_name, triggering_assistant_message_id, intent_json, outcome, processing_id)
        VALUES (1, 4, 'controller', 'continuation', 'checker-model', 3, '{}', 'accepted', 1);"#
    ).unwrap();
    let messages = fixture.store.load_stage_messages(StageRunId(1)).unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|row| (row.message_id, row.message.content(), row.source))
            .collect::<Vec<_>>(),
        [
            (1, "build it", ProtocolSource::Human),
            (2, "human", ProtocolSource::Human),
            (3, "answer", ProtocolSource::Assistant),
            (4, "controller", ProtocolSource::Controller),
            (5, "next answer", ProtocolSource::Assistant),
        ]
    );
    assert_eq!(messages[2].message.usage().unwrap().total_tokens, 15);
    assert_eq!(
        fixture.store.load_stage_messages(StageRunId(2)).unwrap()[0]
            .message
            .content(),
        "other stage"
    );
}

#[test]
fn stage_protocol_rejects_corrupt_roles_sources_and_cross_task_or_dialog_mappings() {
    for corruption in [
        "UPDATE messages SET role = 'unknown'",
        "UPDATE messages SET dialog_id = 999",
        "UPDATE message_task_stages SET workflow_task_id = 999",
        "UPDATE workflow_inputs SET source = 'unknown'",
        "UPDATE workflow_inputs SET dialog_id = 999",
        "INSERT INTO message_usage VALUES (1, 'not json')",
    ] {
        let fixture = Fixture::new();
        fixture.connection.execute_batch(
            "INSERT INTO workflow_inputs (dialog_id, message_id, source, intent_json, outcome) VALUES (1, 1, 'human', '{}', 'accepted');
             PRAGMA foreign_keys = OFF; PRAGMA ignore_check_constraints = ON;"
        ).unwrap();
        fixture.connection.execute_batch(corruption).unwrap();
        let result = fixture.store.load_stage_messages(StageRunId(1));
        assert!(
            matches!(result, Err(StoreError::InvalidWorkflow(_))),
            "{corruption}: {result:?}"
        );
    }
}

fn seed_processing(fixture: &Fixture) {
    fixture.connection.execute_batch(
        r#"INSERT INTO dialogs (id, system_prompt, title) VALUES (2, 'System', 'Other');
        INSERT INTO workflow_tasks (id, dialog_id, ordinal, phase, status, goal, plan_json, checkpoint_json, version)
        SELECT 2, 1, 2, 'done', 'active', goal, plan_json, checkpoint_json, 3 FROM workflow_tasks WHERE id = 1;
        INSERT INTO task_stage_runs (id, workflow_task_id, phase, sequence) VALUES (2, 2, 'done', 1);
        INSERT INTO messages (id, dialog_id, role, content) VALUES (2, 1, 'assistant', 'answer'), (3, 1, 'assistant', 'old task'), (4, 2, 'assistant', 'other dialog');
        INSERT INTO message_task_stages VALUES (2, 1, 1), (3, 2, 2);
        INSERT INTO response_processing (assistant_message_id, checker_name, expected_version, status, attempts, result_json) VALUES
        (2, 'pending', 3, 'pending', 0, NULL),
        (2, 'processing', 3, 'processing', 1, NULL),
        (2, 'retry', 3, 'failed', 1, '{"decision":"await_user"}'),
        (2, 'exhausted', 3, 'failed', 2, NULL),
        (2, 'exhausted_pending', 3, 'pending', 2, NULL),
        (2, 'exhausted_processing', 3, 'processing', 2, NULL),
        (2, 'completed', 3, 'completed', 1, NULL),
        (2, 'stale_pending', 2, 'pending', 0, '{"audit":true}'),
        (2, 'stale_processing', 2, 'processing', 1, NULL),
        (2, 'stale_failed', 2, 'failed', 1, NULL),
        (2, 'stale_completed', 2, 'completed', 1, NULL),
        (2, 'future', 4, 'pending', 0, NULL),
        (3, 'old_task', 3, 'pending', 0, NULL),
        (3, 'old_task_stale', 2, 'pending', 0, NULL),
        (4, 'other_dialog', 3, 'pending', 0, NULL);"#
    ).unwrap();
}

#[test]
fn pending_processing_is_bounded_to_the_selected_task_and_exact_current_version() {
    let fixture = Fixture::new();
    seed_processing(&fixture);
    let pending = fixture.store.load_pending_processing(1).unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|row| (
                row.id,
                row.assistant_message_id,
                row.checker_name.as_str(),
                row.expected_version,
                row.status,
                row.attempts
            ))
            .collect::<Vec<_>>(),
        [
            (1, 2, "pending", 3, ProcessingStatus::Pending, 0),
            (2, 2, "processing", 3, ProcessingStatus::Processing, 1),
            (3, 2, "retry", 3, ProcessingStatus::Failed, 1),
        ]
    );
    assert_eq!(
        pending[2].result_json,
        Some(serde_json::json!({"decision":"await_user"}))
    );
    assert_eq!(pending[0].last_error, None);
    assert!(fixture.store.load_pending_processing(2).unwrap().is_empty());
}

#[test]
fn pending_processing_rejects_corrupt_numbers_and_json() {
    for corruption in [
        "UPDATE response_processing SET attempts = -1 WHERE id = 1",
        "UPDATE response_processing SET result_json = '{' WHERE id = 1",
        "UPDATE response_processing SET checker_name = '' WHERE id = 1",
        "UPDATE messages SET role = 'user' WHERE id = 2",
        "UPDATE messages SET dialog_id = 2 WHERE id = 2",
        "UPDATE message_task_stages SET stage_run_id = 2 WHERE message_id = 2",
    ] {
        let fixture = Fixture::new();
        seed_processing(&fixture);
        fixture
            .connection
            .execute_batch("PRAGMA ignore_check_constraints = ON;")
            .unwrap();
        fixture.connection.execute_batch(corruption).unwrap();
        let result = fixture.store.load_pending_processing(1);
        assert!(
            matches!(result, Err(StoreError::InvalidWorkflow(_))),
            "{corruption}: {result:?}"
        );
    }
}

#[test]
fn stale_closure_is_version_guarded_terminal_scoped_and_preserves_audit_result() {
    let mut fixture = Fixture::new();
    seed_processing(&fixture);
    assert!(matches!(
        fixture.store.close_stale_processing(1, 4),
        Err(StoreError::Conflict(1))
    ));
    assert_eq!(fixture.store.close_stale_processing(1, 3).unwrap(), 3);
    for id in [8, 9, 10] {
        let row = fixture
            .connection
            .query_row(
                "SELECT status, attempts, last_error FROM response_processing WHERE id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, ("failed".into(), 2, "stale task version".into()));
    }
    let result: String = fixture
        .connection
        .query_row(
            "SELECT result_json FROM response_processing WHERE id = 8",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(result, r#"{"audit":true}"#);
    for (id, status) in [
        (1, "pending"),
        (11, "completed"),
        (12, "pending"),
        (14, "pending"),
        (15, "pending"),
    ] {
        assert_eq!(
            fixture
                .connection
                .query_row(
                    "SELECT status FROM response_processing WHERE id = ?1",
                    [id],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            status
        );
    }
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap()
            .version,
        3
    );
}

#[test]
fn migration_rejects_dangling_foreign_keys_and_rolls_back_workflow_schema() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("corrupt.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch(
        "PRAGMA foreign_keys = OFF;
         CREATE TABLE dialogs (id INTEGER PRIMARY KEY AUTOINCREMENT, system_prompt TEXT NOT NULL,
         title TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', last_message_id INTEGER NOT NULL DEFAULT 0);
         CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
         role TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT '');
         INSERT INTO messages (dialog_id, role, content) VALUES (999, 'user', 'orphan');"
    ).unwrap();
    assert!(DialogStore::open(&path).is_err());
    assert!(
        !connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'workflow_tasks')",
                [],
                |row| row.get::<_, bool>(0)
            )
            .unwrap()
    );
}
