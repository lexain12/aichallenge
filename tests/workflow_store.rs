use deepseek_cli::client::TokenUsage;
use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::memory::RequestScope;
use deepseek_cli::workflow::{PlanAppend, PlanStepStatus, StepStatusUpdate, TaskStatePatch};
use deepseek_cli::workflow::{StageRunId, TaskPhase, TaskStatus};
use deepseek_cli::workflow::{WorkflowInput, WorkflowInputSource, WorkflowIntent, WorkflowTaskId};
use deepseek_cli::workflow_store::{AcceptedInputEffect, AnswerCommit, InputCommit};
use deepseek_cli::workflow_store::{ControllerInputCommit, ProcessingLeaseMode, ProcessingResult};
use deepseek_cli::workflow_store::{ProcessingStatus, ProtocolSource, WorkflowRepository};
use rusqlite::Connection;

fn patch(version: u64) -> TaskStatePatch {
    TaskStatePatch {
        expected_version: version,
        plan_append: PlanAppend::default(),
        step_updates: vec![],
        current_step_id: None,
        expected_action: Some("Next action".into()),
        checkpoint: None,
    }
}

#[test]
fn stale_patch_is_a_workflow_conflict_without_completing_processing() {
    let (mut fixture, processing, _) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch(2)),
        Err(StoreError::WorkflowConflict(1))
    ));
    assert_eq!(
        fixture.store.load_pending_processing(1).unwrap()[0].status,
        ProcessingStatus::Processing
    );
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
fn lost_initial_stage_update_is_a_conflict_and_rolls_back_creation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ignored.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let connection = Connection::open(path).unwrap();
    connection.execute_batch("CREATE TRIGGER ignore_stage BEFORE UPDATE ON workflow_tasks BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(matches!(
        store.start_dialog_with_workflow_task(&RequestScope::default(), "System", "Build"),
        Err(StoreError::WorkflowConflict(_))
    ));
    assert_eq!(count(&connection, "dialogs"), 0);
    assert_eq!(count(&connection, "workflow_tasks"), 0);
}

#[test]
fn sqlite_uniqueness_conflict_is_mapped_and_does_not_leave_an_input() {
    let mut fixture = Fixture::new();
    fixture
        .connection
        .execute("DELETE FROM dialog_workflow_state", [])
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .create_task_with_human_input(1, "another", "another", 0),
        Err(StoreError::WorkflowConflict(1))
    ));
    assert_eq!(count(&fixture.connection, "messages"), 1);
    assert_eq!(count(&fixture.connection, "workflow_tasks"), 1);
}

fn pending_fixture() -> (Fixture, i64, i64) {
    let mut fixture = Fixture::new();
    fixture
        .connection
        .execute("UPDATE workflow_tasks SET status='active'", [])
        .unwrap();
    let answer = fixture
        .store
        .append_answer_for_processing(answer_command(3))
        .unwrap();
    (fixture, answer.processing_id, answer.message_id)
}

fn controller_command<'a>(
    processing: i64,
    assistant: i64,
    intent: &'a WorkflowIntent,
    patch: &'a TaskStatePatch,
) -> ControllerInputCommit<'a> {
    ControllerInputCommit {
        processing_id: processing,
        task_id: WorkflowTaskId(1),
        stage_run_id: StageRunId(1),
        expected_version: 3,
        checker: "continuation",
        model: "checker-model",
        triggering_assistant_message_id: assistant,
        instruction: "Do next",
        intent,
        confidence: 0.9,
        accepted_patch: patch,
    }
}

#[test]
fn leases_distinguish_normal_processing_from_crash_recovery_and_bound_attempts() {
    let (mut fixture, processing, assistant) = pending_fixture();
    assert!(matches!(
        fixture
            .store
            .lease_processing(processing, 2, ProcessingLeaseMode::Normal),
        Err(StoreError::WorkflowConflict(1))
    ));
    let first = fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            first.processing_id,
            first.assistant_message_id,
            first.expected_version,
            first.attempts
        ),
        (processing, assistant, 3, 1)
    );
    let mut other = DialogStore::open(&fixture._directory.path().join("workflow.sqlite3")).unwrap();
    assert!(
        other
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap()
            .is_none()
    );
    let recovered = other
        .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.attempts, 2);
    assert!(
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
            .unwrap()
            .is_none()
    );
    other
        .fail_processing(processing, 3, "checker unavailable")
        .unwrap();
    assert!(
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap()
            .is_none()
    );
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
fn failure_preserves_answer_and_task_and_allows_one_retry() {
    let (mut fixture, processing, _) = pending_fixture();
    assert!(
        fixture
            .store
            .fail_processing(processing, 3, "error")
            .is_err()
    );
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    for invalid in ["".to_owned(), "x".repeat(8193)] {
        assert!(
            fixture
                .store
                .fail_processing(processing, 3, &invalid)
                .is_err()
        );
    }
    fixture
        .store
        .fail_processing(processing, 3, "checker unavailable")
        .unwrap();
    let pending = fixture.store.load_pending_processing(1).unwrap();
    assert_eq!(pending[0].status, ProcessingStatus::Failed);
    assert_eq!(
        pending[0].last_error.as_deref(),
        Some("checker unavailable")
    );
    assert_eq!(
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap()
            .unwrap()
            .attempts,
        2
    );
    assert_eq!(fixture.store.load(1).unwrap().messages.len(), 2);
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
fn await_user_validates_patch_and_completes_idempotently() {
    let (mut fixture, processing, _) = pending_fixture();
    let mut patch = patch(3);
    assert!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch)
            .is_err()
    );
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    patch.step_updates.push(StepStatusUpdate {
        step_id: "unknown".into(),
        status: PlanStepStatus::Completed,
        evidence: vec!["Observed test pass".into()],
    });
    assert!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch)
            .is_err()
    );
    patch.step_updates.clear();
    let result = fixture
        .store
        .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch)
        .unwrap();
    assert_eq!(result, ProcessingResult::AwaitUser { task_version: 4 });
    assert_eq!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch)
            .unwrap(),
        result
    );
    assert!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(2), StageRunId(1), 3, &patch)
            .is_err()
    );
    assert!(
        fixture
            .store
            .fail_processing(processing, 3, "late failure")
            .is_err()
    );
    assert!(
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
            .unwrap()
            .is_none()
    );
    let task = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(task.version, 4);
    assert_eq!(task.expected_action.as_deref(), Some("Next action"));
    assert_eq!(count(&fixture.connection, "messages"), 2);
    let stored: String = fixture
        .connection
        .query_row("SELECT result_json FROM response_processing", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        serde_json::from_str::<ProcessingResult>(&stored).unwrap(),
        result
    );
}

#[test]
fn await_user_empty_patch_preserves_version_and_failure_rolls_back_patch() {
    let (mut fixture, processing, _) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    fixture.connection.execute_batch("CREATE TRIGGER fail BEFORE UPDATE ON response_processing WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch(3))
            .is_err()
    );
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
    fixture
        .connection
        .execute_batch("DROP TRIGGER fail")
        .unwrap();
    let mut patch = patch(3);
    patch.expected_action = None;
    assert_eq!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch)
            .unwrap(),
        ProcessingResult::AwaitUser { task_version: 3 }
    );
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
fn stale_processing_and_closed_or_paused_stages_cannot_apply_effects() {
    for corruption in [
        "UPDATE workflow_tasks SET version=4",
        "UPDATE workflow_tasks SET status='paused'",
        "UPDATE task_stage_runs SET finished_at='9999-01-01'",
    ] {
        let (mut fixture, processing, _) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        fixture.connection.execute_batch(corruption).unwrap();
        assert!(
            fixture
                .store
                .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch(3))
                .is_err()
        );
        assert!(
            fixture
                .store
                .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
                .is_err()
        );
        assert_eq!(count(&fixture.connection, "messages"), 2);
    }
}

#[test]
fn controller_completion_is_hidden_atomic_idempotent_and_single_version_increment() {
    for nonempty in [true, false] {
        let (mut fixture, processing, assistant) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        let intent = WorkflowIntent::human_continue("Do next").unwrap();
        let mut patch = patch(3);
        if !nonempty {
            patch.expected_action = None;
        }
        let result = fixture
            .store
            .commit_controller_decision(controller_command(processing, assistant, &intent, &patch))
            .unwrap();
        let ProcessingResult::ControllerInput {
            message_id,
            workflow_input_id,
            task_version,
        } = result
        else {
            panic!("wrong result")
        };
        assert_eq!(task_version, 4);
        assert_eq!(
            fixture
                .store
                .commit_controller_decision(controller_command(
                    processing, assistant, &intent, &patch
                ))
                .unwrap(),
            result
        );
        assert_eq!(
            fixture
                .store
                .load_workflow(1)
                .unwrap()
                .current_task
                .unwrap()
                .version,
            4
        );
        assert_eq!(fixture.store.load(1).unwrap().messages.len(), 2);
        let protocol = fixture.store.load_stage_messages(StageRunId(1)).unwrap();
        assert_eq!(protocol.len(), 3);
        assert_eq!(protocol[2].message_id, message_id);
        assert_eq!(protocol[2].message.content(), "Do next");
        assert_eq!(protocol[2].source, ProtocolSource::Controller);
        let audit: (i64, i64, String, String, i64) = fixture.connection.query_row("SELECT id, processing_id, checker_name, model_name, triggering_assistant_message_id FROM workflow_inputs", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))).unwrap();
        assert_eq!(
            audit,
            (
                workflow_input_id,
                processing,
                "continuation".into(),
                "checker-model".into(),
                assistant
            )
        );
        assert_eq!(fixture.store.list().unwrap()[0].title, "build it");
        assert!(
            fixture
                .store
                .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, &patch)
                .is_err()
        );
        let mut changed = controller_command(processing, assistant, &intent, &patch);
        changed.model = "other-model";
        assert!(fixture.store.commit_controller_decision(changed).is_err());
    }
}

#[test]
fn controller_failure_rolls_back_patch_hidden_input_and_completion() {
    for (table, operation) in [
        ("workflow_tasks", "UPDATE"),
        ("messages", "INSERT"),
        ("workflow_inputs", "INSERT"),
        ("message_task_stages", "INSERT"),
        ("response_processing", "UPDATE"),
        ("dialogs", "UPDATE"),
    ] {
        let (mut fixture, processing, assistant) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        fixture.connection.execute_batch(&format!("CREATE TRIGGER fail BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END;")).unwrap();
        let intent = WorkflowIntent::human_continue("Do next").unwrap();
        assert!(
            fixture
                .store
                .commit_controller_decision(controller_command(
                    processing,
                    assistant,
                    &intent,
                    &patch(3)
                ))
                .is_err()
        );
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
        assert_eq!(count(&fixture.connection, "messages"), 2);
        assert_eq!(count(&fixture.connection, "workflow_inputs"), 0);
        assert_eq!(count(&fixture.connection, "message_task_stages"), 2);
        assert_eq!(
            fixture.store.load_pending_processing(1).unwrap()[0].status,
            ProcessingStatus::Processing
        );
    }
}

#[test]
fn controller_cannot_forge_provenance_intent_or_patch() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let intent = WorkflowIntent::human_continue("Do next").unwrap();
    let patch = patch(3);
    for corruption in 0..9 {
        let mut command = controller_command(processing, assistant, &intent, &patch);
        match corruption {
            0 => command.checker = "different",
            1 => command.model = " ",
            2 => command.triggering_assistant_message_id = 1,
            3 => command.stage_run_id = StageRunId(2),
            4 => command.task_id = WorkflowTaskId(2),
            5 => command.expected_version = 2,
            6 => command.confidence = f32::NAN,
            7 => command.instruction = " ",
            _ => command.instruction = "different instruction",
        }
        assert!(
            fixture.store.commit_controller_decision(command).is_err(),
            "case {corruption}"
        );
    }
    for intent in [
        WorkflowIntent::StartNewTask {
            goal: "other".into(),
        },
        WorkflowIntent::ReplanCurrent {
            change_request: "other".into(),
        },
    ] {
        assert!(
            fixture
                .store
                .commit_controller_decision(controller_command(
                    processing, assistant, &intent, &patch
                ))
                .is_err()
        );
    }
    let mut bad_patch = patch.clone();
    bad_patch.expected_version = 2;
    assert!(
        fixture
            .store
            .commit_controller_decision(controller_command(
                processing, assistant, &intent, &bad_patch
            ))
            .is_err()
    );
    assert_eq!(count(&fixture.connection, "messages"), 2);
    assert_eq!(count(&fixture.connection, "workflow_inputs"), 0);
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

fn human(text: &str) -> WorkflowInput {
    WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::human_continue(text).unwrap(),
    }
}

fn input_command(input: &WorkflowInput, version: Option<u64>) -> InputCommit<'_> {
    InputCommit {
        dialog_id: 1,
        input,
        protocol_text: "continue",
        confidence: Some(0.9),
        expected_version: version,
    }
}

fn answer_command(version: u64) -> AnswerCommit<'static> {
    AnswerCommit {
        dialog_id: 1,
        task_id: WorkflowTaskId(1),
        stage_run_id: StageRunId(1),
        expected_version: version,
        content: "Complete answer",
        usage: Some(TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 5,
            total_tokens: 15,
            completion_tokens_details: None,
        }),
    }
}

#[test]
fn human_resume_and_continue_keep_stage_and_advance_versions_once() {
    let mut fixture = Fixture::new();
    let input = human("continue");
    let result = fixture
        .store
        .append_input(
            input_command(&input, Some(3)),
            AcceptedInputEffect::ResumeSameStage,
        )
        .unwrap();
    let resumed = result.task.unwrap();
    assert_eq!(resumed.status, TaskStatus::Active);
    assert_eq!(resumed.current_stage_run_id, StageRunId(1));
    assert_eq!(resumed.version, 4);
    assert!(result.workflow_input_id.is_some());
    let result = fixture
        .store
        .append_input(
            input_command(&input, Some(4)),
            AcceptedInputEffect::ContinueSameStage,
        )
        .unwrap();
    assert_eq!(result.task.unwrap().version, 5);
    assert_eq!(count(&fixture.connection, "task_stage_runs"), 1);
    assert_eq!(
        fixture
            .store
            .load_stage_messages(StageRunId(1))
            .unwrap()
            .len(),
        3
    );
    assert_eq!(fixture.store.load(1).unwrap().messages.len(), 3);
}

#[test]
fn reject_records_visible_unmapped_audit_without_resuming() {
    let mut fixture = Fixture::new();
    let input = WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::StartNewTask {
            goal: "other task".into(),
        },
    };
    let result = fixture
        .store
        .append_input(
            input_command(&input, Some(3)),
            AcceptedInputEffect::Reject {
                reason: "unfinished task".into(),
            },
        )
        .unwrap();
    assert_eq!(result.task.unwrap().status, TaskStatus::Paused);
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
    assert_eq!(fixture.store.load(1).unwrap().messages.len(), 2);
    assert_eq!(
        fixture
            .store
            .load_stage_messages(StageRunId(1))
            .unwrap()
            .len(),
        1
    );
    let audit: (String, String) = fixture
        .connection
        .query_row(
            "SELECT outcome, rejection_reason FROM workflow_inputs",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(audit, ("rejected".into(), "unfinished task".into()));
}

#[test]
fn unmanaged_input_is_legacy_style_and_requires_the_done_snapshot() {
    let mut fixture = Fixture::new();
    let mut input = human("continue");
    input.intent = WorkflowIntent::Continue {
        instruction: String::new(),
    };
    assert!(
        fixture
            .store
            .append_input(
                input_command(&input, Some(3)),
                AcceptedInputEffect::RouteUnmanaged
            )
            .is_err()
    );
    fixture.connection.execute_batch("UPDATE workflow_tasks SET phase='done', status='active'; UPDATE task_stage_runs SET phase='done';").unwrap();
    assert!(
        fixture
            .store
            .append_input(
                input_command(&input, None),
                AcceptedInputEffect::RouteUnmanaged
            )
            .is_err()
    );
    let result = fixture
        .store
        .append_input(
            input_command(&input, Some(3)),
            AcceptedInputEffect::RouteUnmanaged,
        )
        .unwrap();
    assert_eq!(result.workflow_input_id, None);
    assert_eq!(result.task.unwrap().version, 3);
    assert_eq!(count(&fixture.connection, "workflow_inputs"), 0);
    assert_eq!(count(&fixture.connection, "message_task_stages"), 1);
    assert_eq!(fixture.store.load(1).unwrap().messages.len(), 2);
}

#[test]
fn invalid_input_effects_sources_versions_and_blank_text_write_nothing() {
    let mut fixture = Fixture::new();
    let input = human("continue");
    for expected in [None, Some(2), Some(4)] {
        assert!(
            fixture
                .store
                .append_input(
                    input_command(&input, expected),
                    AcceptedInputEffect::ResumeSameStage
                )
                .is_err()
        );
    }
    assert!(
        fixture
            .store
            .append_input(
                input_command(&input, Some(3)),
                AcceptedInputEffect::ContinueSameStage
            )
            .is_err()
    );
    for intent in [
        WorkflowIntent::ReplanCurrent {
            change_request: "change".into(),
        },
        WorkflowIntent::StartNewTask {
            goal: "other".into(),
        },
    ] {
        let input = WorkflowInput {
            source: WorkflowInputSource::Human,
            intent,
        };
        assert!(
            fixture
                .store
                .append_input(
                    input_command(&input, Some(3)),
                    AcceptedInputEffect::ResumeSameStage
                )
                .is_err()
        );
    }
    let controller = WorkflowInput {
        source: WorkflowInputSource::Controller {
            checker: "continuation".into(),
            model: "model".into(),
            triggering_assistant_message_id: 1,
        },
        intent: input.intent.clone(),
    };
    for effect in [
        AcceptedInputEffect::ResumeSameStage,
        AcceptedInputEffect::Reject {
            reason: "rejected".into(),
        },
        AcceptedInputEffect::RouteUnmanaged,
    ] {
        assert!(
            fixture
                .store
                .append_input(input_command(&controller, Some(3)), effect)
                .is_err()
        );
    }
    let mut command = input_command(&input, Some(3));
    command.protocol_text = " \n ";
    assert!(
        fixture
            .store
            .append_input(command, AcceptedInputEffect::ResumeSameStage)
            .is_err()
    );
    for confidence in [f32::NAN, -0.1, 1.1] {
        let mut command = input_command(&input, Some(3));
        command.confidence = Some(confidence);
        assert!(
            fixture
                .store
                .append_input(command, AcceptedInputEffect::ResumeSameStage)
                .is_err()
        );
    }
    assert_eq!(count(&fixture.connection, "messages"), 1);
    assert_eq!(count(&fixture.connection, "workflow_inputs"), 0);
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
fn input_failure_rolls_back_message_audit_mapping_and_resume() {
    for (table, operation) in [
        ("messages", "INSERT"),
        ("workflow_inputs", "INSERT"),
        ("message_task_stages", "INSERT"),
        ("workflow_tasks", "UPDATE"),
        ("dialogs", "UPDATE"),
    ] {
        let mut fixture = Fixture::new();
        fixture.connection.execute_batch(&format!("CREATE TRIGGER fail BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END;")).unwrap();
        assert!(
            fixture
                .store
                .append_input(
                    input_command(&human("continue"), Some(3)),
                    AcceptedInputEffect::ResumeSameStage
                )
                .is_err()
        );
        assert_eq!(count(&fixture.connection, "messages"), 1);
        assert_eq!(count(&fixture.connection, "workflow_inputs"), 0);
        assert_eq!(count(&fixture.connection, "message_task_stages"), 1);
        let task = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        assert_eq!((task.status, task.version), (TaskStatus::Paused, 3));
    }
}

#[test]
fn answer_usage_mapping_and_pending_processing_commit_together() {
    let mut fixture = Fixture::new();
    fixture
        .connection
        .execute("UPDATE workflow_tasks SET status='active'", [])
        .unwrap();
    let result = fixture
        .store
        .append_answer_for_processing(answer_command(3))
        .unwrap();
    let pending = fixture.store.load_pending_processing(1).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, result.processing_id);
    assert_eq!(pending[0].assistant_message_id, result.message_id);
    assert_eq!(pending[0].status, ProcessingStatus::Pending);
    assert_eq!(pending[0].checker_name, "continuation");
    assert_eq!(pending[0].attempts, 0);
    assert_eq!(pending[0].expected_version, 3);
    let messages = fixture.store.load_stage_messages(StageRunId(1)).unwrap();
    assert_eq!(messages[1].message.content(), "Complete answer");
    assert_eq!(messages[1].message.usage().unwrap().total_tokens, 15);
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
fn answer_failure_rolls_back_complete_answer_and_usage() {
    for table in [
        "message_usage",
        "message_task_stages",
        "response_processing",
    ] {
        let mut fixture = Fixture::new();
        fixture
            .connection
            .execute("UPDATE workflow_tasks SET status='active'", [])
            .unwrap();
        fixture.connection.execute_batch(&format!("CREATE TRIGGER fail BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END;")).unwrap();
        assert!(
            fixture
                .store
                .append_answer_for_processing(answer_command(3))
                .is_err()
        );
        assert_eq!(count(&fixture.connection, "messages"), 1);
        assert_eq!(count(&fixture.connection, "message_usage"), 0);
        assert_eq!(count(&fixture.connection, "response_processing"), 0);
        assert_eq!(count(&fixture.connection, "message_task_stages"), 1);
    }
}

#[test]
fn answer_requires_current_version_identity_and_active_open_stage() {
    for corruption in [
        "",
        "UPDATE workflow_tasks SET status='active', version=4",
        "UPDATE workflow_tasks SET status='active'; UPDATE task_stage_runs SET finished_at='9999-01-01'",
        "UPDATE workflow_tasks SET status='active', phase='done'; UPDATE task_stage_runs SET phase='done'",
    ] {
        let mut fixture = Fixture::new();
        fixture.connection.execute_batch(corruption).unwrap();
        assert!(
            fixture
                .store
                .append_answer_for_processing(answer_command(3))
                .is_err()
        );
        assert_eq!(count(&fixture.connection, "messages"), 1);
    }
    let mut fixture = Fixture::new();
    fixture
        .connection
        .execute("UPDATE workflow_tasks SET status='active'", [])
        .unwrap();
    for bad_task in [true, false] {
        let mut command = answer_command(3);
        if bad_task {
            command.task_id = WorkflowTaskId(2);
        } else {
            command.stage_run_id = StageRunId(2);
        }
        assert!(matches!(
            fixture.store.append_answer_for_processing(command),
            Err(StoreError::WorkflowConflict(1))
        ));
    }
    assert_eq!(count(&fixture.connection, "messages"), 1);
}

fn count(connection: &Connection, table: &str) -> i64 {
    connection
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

#[test]
fn new_dialog_scope_task_stage_and_first_message_commit_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("new.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let started = store
        .start_dialog_with_workflow_task(
            &RequestScope::new("alice", "memory-space").unwrap(),
            "System",
            "Build a parser",
        )
        .unwrap();
    let loaded = store.load(started.dialog_id).unwrap();
    assert_eq!(loaded.messages.len(), 1);
    assert_eq!(loaded.scope.task_id(), "memory-space");
    assert_eq!(
        store
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap(),
        started.task
    );
    let messages = store.load_stage_messages(started.stage_run_id).unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message.content(), "Build a parser");
    assert_eq!(messages[0].source, ProtocolSource::Human);
    assert_eq!(started.task.ordinal, 1);
    assert_eq!(started.task.version, 0);
}

#[test]
fn failure_at_any_creation_write_rolls_back_every_row() {
    for (table, operation) in [
        ("dialogs", "INSERT"),
        ("dialog_scopes", "INSERT"),
        ("workflow_tasks", "INSERT"),
        ("task_stage_runs", "INSERT"),
        ("workflow_tasks", "UPDATE"),
        ("messages", "INSERT"),
        ("workflow_inputs", "INSERT"),
        ("message_task_stages", "INSERT"),
        ("dialog_workflow_state", "INSERT"),
        ("dialogs", "UPDATE"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollback.sqlite3");
        let mut store = DialogStore::open(&path).unwrap();
        let connection = Connection::open(path).unwrap();
        connection.execute_batch(&format!("CREATE TRIGGER fail BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END;")).unwrap();
        assert!(
            store
                .start_dialog_with_workflow_task(
                    &RequestScope::default(),
                    "System",
                    "Build a parser"
                )
                .is_err(),
            "{table} {operation}"
        );
        for checked in [
            "dialogs",
            "dialog_scopes",
            "workflow_tasks",
            "task_stage_runs",
            "messages",
            "workflow_inputs",
            "message_task_stages",
            "dialog_workflow_state",
        ] {
            assert_eq!(
                count(&connection, checked),
                0,
                "{table} {operation}: {checked}"
            );
        }
    }
}

#[test]
fn existing_dialog_creation_rejects_stale_versions_and_competing_unfinished_tasks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shared.sqlite3");
    let mut first = DialogStore::open(&path).unwrap();
    let dialog = first.start_dialog("System", "legacy").unwrap();
    let mut second = DialogStore::open(&path).unwrap();
    let created = first
        .create_task_with_human_input(dialog, "one", "one", 0)
        .unwrap();
    assert!(matches!(
        second.create_task_with_human_input(dialog, "two", "two", 0),
        Err(StoreError::WorkflowConflict(_))
    ));
    let connection = Connection::open(path).unwrap();
    connection.execute_batch("UPDATE workflow_tasks SET phase='done', version=4; UPDATE task_stage_runs SET phase='done';").unwrap();
    assert!(matches!(
        second.create_task_with_human_input(dialog, "two", "two", 0),
        Err(StoreError::WorkflowConflict(_))
    ));
    let next = second
        .create_task_with_human_input(dialog, "two", "two", 4)
        .unwrap();
    assert_eq!(next.task.ordinal, 2);
    assert_ne!(next.task.id, created.task.id);
    assert_eq!(count(&connection, "workflow_tasks"), 2);
    assert_eq!(count(&connection, "messages"), 3);
}

#[test]
fn blank_initial_input_or_goal_writes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blank.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let connection = Connection::open(path).unwrap();
    assert!(
        store
            .start_dialog_with_workflow_task(&RequestScope::default(), "System", " \n\t")
            .is_err()
    );
    assert_eq!(count(&connection, "dialogs"), 0);
    let dialog = store.start_dialog("System", "legacy").unwrap();
    for (text, goal) in [(" ", "goal"), ("text", " ")] {
        assert!(
            store
                .create_task_with_human_input(dialog, text, goal, 0)
                .is_err()
        );
    }
    assert_eq!(count(&connection, "messages"), 1);
    assert_eq!(count(&connection, "workflow_tasks"), 0);
}

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
