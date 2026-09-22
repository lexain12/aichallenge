use deepseek_cli::client::TokenUsage;
use deepseek_cli::context::ContextSummary;
use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::memory::RequestScope;
use deepseek_cli::workflow::{
    PatchContext, StageChangeAuthorization, StateMachine, TransitionEvent, WorkflowTaskState,
};
use deepseek_cli::workflow::{PlanAppend, PlanStepStatus, StepStatusUpdate, TaskStatePatch};
use deepseek_cli::workflow::{StageRunId, TaskPhase, TaskStatus};
use deepseek_cli::workflow::{WorkflowInput, WorkflowInputSource, WorkflowIntent, WorkflowTaskId};
use deepseek_cli::workflow_context::StageReductionState;
use deepseek_cli::workflow_model::HandoffPayload;
use deepseek_cli::workflow_store::TransitionCommit;
use deepseek_cli::workflow_store::UnmanagedAnswerCommit;
use deepseek_cli::workflow_store::{
    AcceptedInputEffect, AnswerCommit, ExpectedCurrentTask, InputCommit,
};
use deepseek_cli::workflow_store::{
    ControllerInputCommit, FailProcessingCommit, ProcessingLeaseMode, ProcessingResult,
};
use deepseek_cli::workflow_store::{
    PauseOutcome, ProcessingStatus, ProtocolSource, WorkflowRepository,
};
use rusqlite::Connection;

// Break caught: moving the large state behind indirection must still decode
// existing audit JSON and replay it with exactly the same serialized shape.
#[test]
fn transition_processing_result_preserves_existing_json_shape() {
    let stored = serde_json::json!({
        "decision":"transition", "transition_id":7,"input_message_id":8,"workflow_input_id":9,
        "target_state": {
            "id":1,"dialog_id":2,"ordinal":1,"phase":"execution","status":"active",
            "goal":"Build parser", "plan":{"revision":1,"steps":[{"id":"build","description":"Build parser","status":"pending"}],"acceptance_criteria":["tests pass"]},
            "current_step_id":"build","expected_action":"Implement parser",
            "checkpoint":{"summary":"Design accepted","decisions":[],"open_issues":[]},
            "current_stage_run_id":4,"current_stage_sequence":2,"incoming_handoff_id":7,"version":3
        }
    });
    let result: ProcessingResult = serde_json::from_value(stored.clone()).unwrap();
    let ProcessingResult::Transition {
        ref target_state,
        transition_id,
        ..
    } = result
    else {
        panic!("stored transition must decode to a typed replay result");
    };
    assert_eq!(transition_id, 7);
    assert_eq!(target_state.phase, TaskPhase::Execution);
    assert_eq!(target_state.current_stage_run_id, StageRunId(4));
    assert_eq!(target_state.version, 3);
    assert_eq!(serde_json::to_value(result).unwrap(), stored);
    // Old controller completions remain readable as audit, while new ones
    // carry an explicit semantic binding without changing the existing keys.
    for raw in [
        r#"{"decision":"controller_input","message_id":8,"workflow_input_id":9,"task_version":4}"#,
        r#"{"decision":"controller_input","message_id":8,"workflow_input_id":9,"task_version":4,"patch_fingerprint":"{\"expected_version\":3,\"plan_append\":{\"steps\":[],\"acceptance_criteria\":[]},\"step_updates\":[],\"current_step_id\":null,\"expected_action\":\"Next action\",\"checkpoint\":null}"}"#,
    ] {
        let result: ProcessingResult = serde_json::from_str(raw).unwrap();
        assert_eq!(serde_json::to_string(&result).unwrap(), raw);
    }
}

// Break caught: recovery must reconstruct the accepted typed input and truncate stage history at its answer.
#[test]
fn processing_context_is_bound_to_dialog_and_original_answer() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("context.sqlite3")).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "original goal")
        .unwrap();
    let answer = store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: started.dialog_id,
            task_id: started.task.id,
            stage_run_id: started.stage_run_id,
            expected_version: 0,
            content: "saved answer",
            usage: None,
        })
        .unwrap();
    let work = store
        .load_processing_context(started.dialog_id, answer.processing_id)
        .unwrap();
    assert_eq!(work.response, "saved answer");
    assert_eq!(
        work.context.triggering_input.intent,
        WorkflowIntent::StartNewTask {
            goal: "original goal".into()
        }
    );
    assert_eq!(work.context.stage_messages.len(), 2);
    assert_eq!(work.checker_name, "continuation");
    assert!(
        store
            .load_processing_context(started.dialog_id + 1, answer.processing_id)
            .is_err()
    );
    assert!(
        store
            .load_processing_result(started.dialog_id + 1, answer.processing_id)
            .is_err()
    );
    assert_eq!(
        store
            .load_processing_result(started.dialog_id, answer.processing_id)
            .unwrap(),
        None
    );
    store
        .lease_processing(answer.processing_id, 0, ProcessingLeaseMode::Normal)
        .unwrap();
    let result = store
        .commit_await_user(
            answer.processing_id,
            started.task.id,
            started.stage_run_id,
            0,
            1,
            &TaskStatePatch {
                expected_version: 0,
                plan_append: Default::default(),
                step_updates: vec![],
                current_step_id: None,
                expected_action: Some("review".into()),
                checkpoint: None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .load_processing_result(started.dialog_id, answer.processing_id)
            .unwrap(),
        Some(result)
    );
    assert!(
        store
            .load_processing_context(started.dialog_id, answer.processing_id)
            .is_err(),
        "old-version job cannot be checked against the new version"
    );
}

// Break caught: restart can finish advisory work on Paused without enabling normal controller effects.
#[test]
fn recovery_only_completion_and_failure_preserve_paused_status_and_exact_attempt() {
    for fail in [false, true] {
        let (mut fixture, processing, assistant) = pending_fixture();
        fixture
            .connection
            .execute("UPDATE workflow_tasks SET status='paused' WHERE id=1", [])
            .unwrap();
        assert!(
            fixture
                .store
                .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
                .is_err()
        );
        let lease = fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
            .unwrap()
            .unwrap();
        let accepted = patch(3);
        assert!(
            fixture
                .store
                .commit_await_user(
                    processing,
                    WorkflowTaskId(1),
                    StageRunId(1),
                    3,
                    lease.attempts,
                    &accepted
                )
                .is_err()
        );
        if fail {
            assert!(
                fixture
                    .store
                    .fail_processing(failure_command(
                        processing,
                        assistant,
                        lease.attempts,
                        "checker failed"
                    ))
                    .is_err()
            );
            fixture
                .store
                .fail_processing_with_mode(
                    failure_command(processing, assistant, lease.attempts, "checker failed"),
                    ProcessingLeaseMode::Recovery,
                )
                .unwrap();
        } else {
            assert!(
                fixture
                    .store
                    .commit_await_user_with_mode(
                        processing,
                        WorkflowTaskId(1),
                        StageRunId(1),
                        3,
                        2,
                        &accepted,
                        ProcessingLeaseMode::Recovery
                    )
                    .is_err()
            );
            fixture
                .store
                .commit_await_user_with_mode(
                    processing,
                    WorkflowTaskId(1),
                    StageRunId(1),
                    3,
                    lease.attempts,
                    &accepted,
                    ProcessingLeaseMode::Recovery,
                )
                .unwrap();
        }
        let state = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        assert_eq!(state.status, TaskStatus::Paused);
        assert_eq!(state.current_stage_run_id, StageRunId(1));
        assert_eq!(state.version, if fail { 3 } else { 4 });
    }
}

fn unmanaged_answer_fixture() -> (Fixture, i64) {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let intent = WorkflowIntent::human_continue("Do next").unwrap();
    fixture
        .store
        .commit_controller_decision(controller_command(
            processing,
            assistant,
            &intent,
            &patch(3),
        ))
        .unwrap();
    fixture
        .connection
        .execute_batch(
            "UPDATE workflow_tasks SET phase='done'; UPDATE task_stage_runs SET phase='done';",
        )
        .unwrap();
    let input = WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::human_continue("unmanaged question").unwrap(),
    };
    let saved = fixture
        .store
        .append_input(
            InputCommit {
                dialog_id: 1,
                input: &input,
                protocol_text: "unmanaged question",
                confidence: None,
                expected_current_task: ExpectedCurrentTask::Present {
                    task_id: WorkflowTaskId(1),
                    version: 4,
                },
            },
            AcceptedInputEffect::RouteUnmanaged,
        )
        .unwrap();
    (fixture, saved.message_id)
}

// Break caught: unmapped answers must count hidden history correctly and never create a job.
#[test]
fn unmanaged_answer_is_atomic_unmapped_and_cannot_be_repeated() {
    let (mut fixture, input) = unmanaged_answer_fixture();
    let before = fixture.store.load_workflow(1).unwrap().current_task;
    let rows = count(&fixture.connection, "messages");
    let mappings = count(&fixture.connection, "message_task_stages");
    let jobs = count(&fixture.connection, "response_processing");
    let usage = TokenUsage {
        prompt_tokens: 4,
        completion_tokens: 2,
        total_tokens: 6,
        completion_tokens_details: None,
    };
    let saved = fixture
        .store
        .append_unmanaged_answer(UnmanagedAnswerCommit {
            dialog_id: 1,
            input_message_id: input,
            expected_current_task: ExpectedCurrentTask::Present {
                task_id: WorkflowTaskId(1),
                version: 4,
            },
            content: "unmanaged answer",
            usage: Some(usage),
        })
        .unwrap();
    assert!(saved > input);
    assert_eq!(count(&fixture.connection, "messages"), rows + 1);
    assert_eq!(count(&fixture.connection, "message_task_stages"), mappings);
    assert_eq!(count(&fixture.connection, "response_processing"), jobs);
    assert_eq!(fixture.store.load_workflow(1).unwrap().current_task, before);
    assert_eq!(
        fixture
            .store
            .load(1)
            .unwrap()
            .messages
            .last()
            .unwrap()
            .usage(),
        Some(usage)
    );
    assert!(
        fixture
            .store
            .append_unmanaged_answer(UnmanagedAnswerCommit {
                dialog_id: 1,
                input_message_id: input,
                expected_current_task: ExpectedCurrentTask::Present {
                    task_id: WorkflowTaskId(1),
                    version: 4
                },
                content: "duplicate",
                usage: None
            })
            .is_err()
    );
    assert_eq!(count(&fixture.connection, "messages"), rows + 1);
}

// Break caught: a stale/unmapped-looking input must not append to another task or transcript turn.
#[test]
fn unmanaged_answer_rejects_changed_task_nonhuman_mapped_and_nonlatest_inputs() {
    for mutation in [
        "UPDATE workflow_tasks SET version=5",
        "UPDATE workflow_tasks SET phase='planning'; UPDATE task_stage_runs SET phase='planning'",
        "UPDATE messages SET role='assistant' WHERE id=(SELECT max(id) FROM messages)",
        "INSERT INTO workflow_inputs (dialog_id,message_id,source,intent_json,outcome) SELECT 1,max(id),'human','{\"type\":\"continue\",\"instruction\":\"x\"}','rejected' FROM messages",
        "INSERT INTO message_task_stages SELECT max(id),1,1 FROM messages",
        "INSERT INTO messages (dialog_id,role,content) VALUES (1,'user','racing input')",
        "DELETE FROM dialog_workflow_state",
    ] {
        let (mut fixture, input) = unmanaged_answer_fixture();
        fixture.connection.execute_batch(mutation).unwrap();
        let before = count(&fixture.connection, "messages");
        assert!(
            fixture
                .store
                .append_unmanaged_answer(UnmanagedAnswerCommit {
                    dialog_id: 1,
                    input_message_id: input,
                    expected_current_task: ExpectedCurrentTask::Present {
                        task_id: WorkflowTaskId(1),
                        version: 4
                    },
                    content: "lost answer",
                    usage: None
                })
                .is_err(),
            "{mutation}"
        );
        assert_eq!(count(&fixture.connection, "messages"), before, "{mutation}");
    }
    for expected in [
        ExpectedCurrentTask::Absent,
        ExpectedCurrentTask::Present {
            task_id: WorkflowTaskId(999),
            version: 4,
        },
    ] {
        let (mut fixture, input) = unmanaged_answer_fixture();
        assert!(
            fixture
                .store
                .append_unmanaged_answer(UnmanagedAnswerCommit {
                    dialog_id: 1,
                    input_message_id: input,
                    expected_current_task: expected,
                    content: "lost answer",
                    usage: None
                })
                .is_err()
        );
    }
}

// Break caught: an ignored or failed insert/update must not leave a partial unmanaged answer.
#[test]
fn unmanaged_answer_rolls_back_each_required_write_and_rejects_blank_content() {
    for (table, operation) in [
        ("messages", "INSERT"),
        ("message_usage", "INSERT"),
        ("dialogs", "UPDATE"),
    ] {
        for action in ["ABORT, 'injected'", "IGNORE"] {
            let (mut fixture, input) = unmanaged_answer_fixture();
            let before = count(&fixture.connection, "messages");
            fixture.connection.execute_batch(&format!("CREATE TRIGGER fail_unmanaged BEFORE {operation} ON {table} BEGIN SELECT RAISE({action}); END;")).unwrap();
            assert!(
                fixture
                    .store
                    .append_unmanaged_answer(UnmanagedAnswerCommit {
                        dialog_id: 1,
                        input_message_id: input,
                        expected_current_task: ExpectedCurrentTask::Present {
                            task_id: WorkflowTaskId(1),
                            version: 4
                        },
                        content: "lost answer",
                        usage: Some(TokenUsage {
                            prompt_tokens: 1,
                            completion_tokens: 1,
                            total_tokens: 2,
                            completion_tokens_details: None
                        })
                    })
                    .is_err(),
                "{table} {action}"
            );
            assert_eq!(
                count(&fixture.connection, "messages"),
                before,
                "{table} {action}"
            );
        }
    }
    let (mut fixture, input) = unmanaged_answer_fixture();
    assert!(
        fixture
            .store
            .append_unmanaged_answer(UnmanagedAnswerCommit {
                dialog_id: 1,
                input_message_id: input,
                expected_current_task: ExpectedCurrentTask::Present {
                    task_id: WorkflowTaskId(1),
                    version: 4
                },
                content: " \n ",
                usage: None
            })
            .is_err()
    );
}

#[test]
fn stage_reductions_restore_independently_accumulate_usage_and_leave_paused_task_unchanged() {
    let mut fixture = Fixture::new();
    let before = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(
        fixture.store.load_stage_reductions(StageRunId(1)).unwrap(),
        StageReductionState::default()
    );
    let context = fixture
        .store
        .replace_stage_context(
            StageRunId(1),
            3,
            1,
            ContextSummary::new("stage summary", 1),
            Some(TokenUsage {
                prompt_tokens: 4,
                completion_tokens: 2,
                total_tokens: 6,
                completion_tokens_details: None,
            }),
        )
        .unwrap();
    assert_eq!(context.compaction_usage().total_tokens(), 6);
    let facts = fixture
        .store
        .replace_stage_facts(
            StageRunId(1),
            3,
            1,
            [("goal".into(), "ship".into())].into(),
            None,
        )
        .unwrap();
    assert_eq!(facts.covered_message_count(), 1);
    let context = fixture
        .store
        .replace_stage_context(
            StageRunId(1),
            3,
            1,
            ContextSummary::new("replacement", 1),
            None,
        )
        .unwrap();
    assert_eq!(context.compaction_usage().call_count(), 2);
    assert_eq!(context.compaction_usage().missing_usage_count(), 1);
    let reopened = DialogStore::open(&fixture._directory.path().join("workflow.sqlite3")).unwrap();
    assert_eq!(
        reopened.load_stage_reductions(StageRunId(1)).unwrap(),
        StageReductionState { context, facts }
    );
    assert_eq!(
        reopened.load_workflow(1).unwrap().current_task.unwrap(),
        before
    );
    assert_eq!(count(&fixture.connection, "dialog_context"), 0);
    assert_eq!(count(&fixture.connection, "dialog_facts"), 0);
}

#[test]
fn stage_reduction_writes_reject_stale_version_count_closed_done_and_unselected_stages() {
    for corruption in [
        "UPDATE workflow_tasks SET version=4",
        "DELETE FROM message_task_stages",
        "UPDATE task_stage_runs SET finished_at='9999-01-01'",
        "UPDATE workflow_tasks SET phase='done'; UPDATE task_stage_runs SET phase='done'",
        "DELETE FROM dialog_workflow_state",
    ] {
        let mut fixture = Fixture::new();
        fixture.connection.execute_batch(corruption).unwrap();
        assert!(
            fixture
                .store
                .replace_stage_context(StageRunId(1), 3, 1, ContextSummary::new("stale", 1), None)
                .is_err(),
            "{corruption}"
        );
        assert!(
            fixture
                .store
                .replace_stage_facts(StageRunId(1), 3, 1, Default::default(), None)
                .is_err(),
            "{corruption}"
        );
        assert_eq!(count(&fixture.connection, "task_stage_context"), 0);
    }
}

#[test]
fn stage_reductions_reject_invalid_boundaries_corrupt_json_and_ignored_writes() {
    let mut fixture = Fixture::new();
    for boundary in [0, 2] {
        assert!(
            fixture
                .store
                .replace_stage_context(
                    StageRunId(1),
                    3,
                    1,
                    ContextSummary::new("bad", boundary),
                    None
                )
                .is_err()
        );
    }
    assert!(
        fixture
            .store
            .load_stage_reductions(StageRunId(999))
            .is_err()
    );
    fixture.connection.execute_batch("CREATE TRIGGER ignore_context BEFORE INSERT ON task_stage_context BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(
        fixture
            .store
            .replace_stage_context(StageRunId(1), 3, 1, ContextSummary::new("lost", 1), None)
            .is_err()
    );
    assert!(
        fixture
            .store
            .replace_stage_facts(StageRunId(1), 3, 1, Default::default(), None)
            .is_err()
    );
    assert_eq!(count(&fixture.connection, "task_stage_context"), 0);
    fixture
        .connection
        .execute_batch("DROP TRIGGER ignore_context;")
        .unwrap();
    fixture
        .store
        .replace_stage_context(StageRunId(1), 3, 1, ContextSummary::new("saved", 1), None)
        .unwrap();
    let saved = fixture.store.load_stage_reductions(StageRunId(1)).unwrap();
    fixture.connection.execute_batch("CREATE TRIGGER ignore_context BEFORE UPDATE ON task_stage_context BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(
        fixture
            .store
            .replace_stage_context(StageRunId(1), 3, 1, ContextSummary::new("lost", 1), None)
            .is_err()
    );
    assert!(
        fixture
            .store
            .replace_stage_facts(StageRunId(1), 3, 1, Default::default(), None)
            .is_err()
    );
    assert_eq!(
        fixture.store.load_stage_reductions(StageRunId(1)).unwrap(),
        saved
    );
    fixture
        .connection
        .execute_batch(
            "DROP TRIGGER ignore_context; UPDATE task_stage_context SET facts_json='{}';",
        )
        .unwrap();
    assert!(fixture.store.load_stage_reductions(StageRunId(1)).is_err());
    assert!(
        fixture
            .store
            .replace_stage_facts(StageRunId(1), 3, 1, Default::default(), None)
            .is_err()
    );
}

#[test]
fn stage_facts_coverage_excludes_controller_but_write_guard_counts_it() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let intent = WorkflowIntent::human_continue("Do next").unwrap();
    fixture
        .store
        .commit_controller_decision(controller_command(
            processing,
            assistant,
            &intent,
            &patch(3),
        ))
        .unwrap();
    assert_eq!(
        fixture
            .store
            .load_stage_messages(StageRunId(1))
            .unwrap()
            .len(),
        3
    );
    assert!(
        fixture
            .store
            .replace_stage_facts(StageRunId(1), 4, 2, Default::default(), None)
            .is_err()
    );
    let facts = fixture
        .store
        .replace_stage_facts(
            StageRunId(1),
            4,
            3,
            [("goal".into(), "build".into())].into(),
            None,
        )
        .unwrap();
    assert_eq!(facts.covered_message_count(), 2);
}

#[test]
fn stage_transition_resets_reductions_retains_audit_and_rejects_outgoing_writes() {
    let mut fixture = Fixture::new();
    fixture
        .store
        .replace_stage_context(
            StageRunId(1),
            3,
            1,
            ContextSummary::new("old summary", 1),
            None,
        )
        .unwrap();
    fixture
        .store
        .replace_stage_facts(
            StageRunId(1),
            3,
            1,
            [("old".into(), "fact".into())].into(),
            None,
        )
        .unwrap();
    let old = fixture.store.load_stage_reductions(StageRunId(1)).unwrap();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::PlanningCompleted);
    let auth = authorize(&source, &input, None);
    let result = fixture
        .store
        .commit_stage_change(transition_command(&source, &input, &auth, &handoff()))
        .unwrap();
    assert_eq!(
        fixture
            .store
            .load_stage_reductions(result.target_state.current_stage_run_id)
            .unwrap(),
        StageReductionState::default()
    );
    assert_eq!(
        fixture.store.load_stage_reductions(StageRunId(1)).unwrap(),
        old
    );
    // Even the new task version and matching old count cannot authorize old-stage writes.
    assert!(
        fixture
            .store
            .replace_stage_context(
                StageRunId(1),
                result.target_state.version,
                1,
                ContextSummary::new("stale", 1),
                None
            )
            .is_err()
    );
    assert!(
        fixture
            .store
            .replace_stage_facts(
                StageRunId(1),
                result.target_state.version,
                1,
                Default::default(),
                None
            )
            .is_err()
    );
    assert_eq!(
        fixture.store.load_stage_reductions(StageRunId(1)).unwrap(),
        old
    );
}

#[test]
fn malformed_stage_context_is_an_error_and_cannot_be_overwritten_by_facts() {
    let mut fixture = Fixture::new();
    fixture.connection.execute("INSERT INTO task_stage_context(stage_run_id,context_json,facts_json) VALUES (1,'{',?1)", [serde_json::to_string(&deepseek_cli::facts::FactsState::default()).unwrap()]).unwrap();
    assert!(fixture.store.load_stage_reductions(StageRunId(1)).is_err());
    assert!(
        fixture
            .store
            .replace_stage_context(StageRunId(1), 3, 1, ContextSummary::new("new", 1), None)
            .is_err()
    );
    assert!(
        fixture
            .store
            .replace_stage_facts(StageRunId(1), 3, 1, Default::default(), None)
            .is_err()
    );
    let stored: String = fixture
        .connection
        .query_row("SELECT context_json FROM task_stage_context", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(stored, "{");
}

fn handoff() -> HandoffPayload {
    HandoffPayload {
        summary: "Implementation complete".into(),
        completed_step_ids: vec![],
        next_step_id: None,
        expected_action: Some("Run validation".into()),
        plan_changes: vec![],
        decisions: vec!["Keep scope".into()],
        open_issues: vec![],
    }
}

fn transition_input(event: TransitionEvent) -> WorkflowInput {
    WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::ProposeTransition {
            event,
            evidence: vec!["Tests pass => 12 tests passed".into()],
        },
    }
}

fn authorize(
    task: &WorkflowTaskState,
    input: &WorkflowInput,
    patch: Option<&TaskStatePatch>,
) -> StageChangeAuthorization {
    let preview = patch
        .map(|p| task.preview_patch(p, PatchContext::Normal).unwrap())
        .unwrap_or_else(|| task.clone());
    match &input.intent {
        WorkflowIntent::ProposeTransition { event, evidence } => {
            StageChangeAuthorization::Transition(
                StateMachine::authorize(&preview, *event, evidence).unwrap(),
            )
        }
        WorkflowIntent::ReplanCurrent { change_request } => StageChangeAuthorization::Replan(
            StateMachine::authorize_replan(&preview, &input.source, change_request.clone())
                .unwrap(),
        ),
        _ => panic!("not a stage change"),
    }
}

fn transition_command<'a>(
    task: &'a WorkflowTaskState,
    input: &'a WorkflowInput,
    auth: &'a StageChangeAuthorization,
    handoff: &'a HandoffPayload,
) -> TransitionCommit<'a> {
    TransitionCommit {
        dialog_id: task.dialog_id,
        source_task: task,
        authorization: auth,
        triggering_input: input,
        protocol_text: "start validation",
        confidence: Some(0.9),
        accepted_patch: None,
        handoff,
        processing_id: None,
        processing_attempt: None,
    }
}

#[test]
fn transition_closes_old_stage_and_projects_handoff_in_one_commit() {
    let mut fixture = Fixture::new();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::PlanningCompleted);
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    let result = fixture
        .store
        .commit_stage_change(transition_command(&source, &input, &auth, &payload))
        .unwrap();
    let current = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(result.target_state, current);
    assert_eq!(
        (current.phase, current.status, current.version),
        (TaskPhase::Execution, TaskStatus::Active, 4)
    );
    assert_eq!(current.current_stage_sequence, 2);
    assert_ne!(current.current_stage_run_id, source.current_stage_run_id);
    assert_eq!(current.incoming_handoff_id, Some(result.transition_id));
    assert_eq!(current.checkpoint.summary, "Implementation complete");
    assert_eq!(current.current_step_id, None);
    let messages = fixture
        .store
        .load_stage_messages(current.current_stage_run_id)
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(
        (messages[0].message_id, messages[0].source),
        (result.input_message_id, ProtocolSource::Human)
    );
    assert_eq!(fixture.store.load(1).unwrap().messages.len(), 2);
    assert!(
        fixture
            .connection
            .query_row(
                "SELECT finished_at IS NOT NULL FROM task_stage_runs WHERE id=1",
                [],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    assert_eq!(count(&fixture.connection, "task_stage_context"), 1);
    assert!(
        !fixture
            .connection
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap()
    );
}

#[test]
fn human_replay_rejects_changed_semantic_source_after_later_evolution() {
    let mut fixture = Fixture::new();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::PlanningCompleted);
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    let original = fixture
        .store
        .commit_stage_change(transition_command(&source, &input, &auth, &payload))
        .unwrap();
    let replan = WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::ReplanCurrent {
            change_request: "Later change".into(),
        },
    };
    let replan_auth = authorize(&original.target_state, &replan, None);
    fixture
        .store
        .commit_stage_change(transition_command(
            &original.target_state,
            &replan,
            &replan_auth,
            &payload,
        ))
        .unwrap();
    for field in 0..7 {
        let mut altered = source.clone();
        match field {
            0 => altered.goal = "Fabricated goal".into(),
            1 => altered.plan.steps[0].description = "Fabricated plan".into(),
            2 => altered.ordinal += 1,
            3 => altered.status = TaskStatus::Active,
            4 => altered.current_step_id = None,
            5 => altered.expected_action = Some("Fabricated action".into()),
            _ => altered.checkpoint.summary = "Fabricated checkpoint".into(),
        }
        assert!(
            matches!(
                fixture
                    .store
                    .commit_stage_change(transition_command(&altered, &input, &auth, &payload)),
                Err(StoreError::WorkflowConflict(1))
            ),
            "field {field}"
        );
    }
    assert_eq!(
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &payload))
            .unwrap(),
        original
    );
    assert_eq!(count(&fixture.connection, "task_transitions"), 2);
    assert_eq!(count(&fixture.connection, "messages"), 3);
}

#[test]
fn transition_source_binding_migrates_old_databases_idempotently_and_rejects_legacy_replay() {
    let mut fixture = Fixture::new();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::PlanningCompleted);
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    fixture
        .store
        .commit_stage_change(transition_command(&source, &input, &auth, &payload))
        .unwrap();
    let has_column: bool = fixture.connection.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('task_transitions') WHERE name='source_fingerprint')", [], |r| r.get(0)).unwrap();
    if has_column {
        fixture
            .connection
            .execute_batch("ALTER TABLE task_transitions DROP COLUMN source_fingerprint")
            .unwrap();
    }
    let path = fixture._directory.path().join("workflow.sqlite3");
    fixture.store = DialogStore::open(&path).unwrap();
    fixture.store = DialogStore::open(&path).unwrap();
    assert!(fixture.connection.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('task_transitions') WHERE name='source_fingerprint')", [], |r| r.get::<_, bool>(0)).unwrap(), "migration must install source binding");
    assert!(matches!(
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &payload)),
        Err(StoreError::WorkflowConflict(1))
    ));
    assert_eq!(count(&fixture.connection, "task_transitions"), 1);
}

#[test]
fn ignored_fork_messages_roll_back_without_mapping_an_unrelated_message() {
    for mapped in [false, true] {
        let mut fixture = Fixture::new();
        if !mapped {
            fixture
                .connection
                .execute("DELETE FROM message_task_stages", [])
                .unwrap();
        }
        let unrelated = fixture.store.start_dialog("Other", "unrelated").unwrap();
        fixture
            .store
            .append_answer(unrelated, 1, "unrelated answer", None)
            .unwrap();
        let source = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        let before_mappings = count(&fixture.connection, "message_task_stages");
        fixture.connection.execute_batch("CREATE TRIGGER ignore_fork_message BEFORE INSERT ON messages WHEN NEW.dialog_id>2 BEGIN SELECT RAISE(IGNORE); END;").unwrap();
        assert!(fixture.store.fork_dialog(1, 1).is_err(), "mapped={mapped}");
        assert_eq!(count(&fixture.connection, "dialogs"), 2);
        assert_eq!(count(&fixture.connection, "dialog_branches"), 0);
        assert_eq!(count(&fixture.connection, "messages"), 3);
        assert_eq!(
            count(&fixture.connection, "message_task_stages"),
            before_mappings
        );
        assert_eq!(
            fixture
                .connection
                .query_row(
                    "SELECT count(*) FROM message_task_stages WHERE message_id=3",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .store
                .load_workflow(1)
                .unwrap()
                .current_task
                .unwrap(),
            source
        );
        assert_eq!(fixture.store.load(unrelated).unwrap().messages.len(), 2);
    }
}

#[test]
fn human_transition_replay_returns_original_after_later_replan() {
    let mut fixture = Fixture::new();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::PlanningCompleted);
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    let original = fixture
        .store
        .commit_stage_change(transition_command(&source, &input, &auth, &payload))
        .unwrap();
    let replan = WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::ReplanCurrent {
            change_request: "Change approach".into(),
        },
    };
    let replan_auth = authorize(&original.target_state, &replan, None);
    let replanned = fixture
        .store
        .commit_stage_change(transition_command(
            &original.target_state,
            &replan,
            &replan_auth,
            &payload,
        ))
        .unwrap();
    assert_eq!(
        (
            replanned.target_state.phase,
            replanned.target_state.plan.revision,
            replanned.target_state.version
        ),
        (TaskPhase::Planning, 2, 5)
    );
    assert!(
        replanned
            .target_state
            .checkpoint
            .open_issues
            .contains(&"Change approach".into())
    );
    assert_eq!(
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &payload))
            .unwrap(),
        original
    );
    let mut changed = payload.clone();
    changed.summary = "different".into();
    assert!(matches!(
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &changed)),
        Err(StoreError::WorkflowConflict(1))
    ));
    assert_eq!(count(&fixture.connection, "task_transitions"), 2);
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap(),
        replanned.target_state
    );
}

#[test]
fn replan_always_creates_a_new_planning_run_including_planning_and_done() {
    for phase in ["planning", "execution", "validation", "done"] {
        let mut fixture = Fixture::new();
        fixture
            .connection
            .execute(
                "UPDATE workflow_tasks SET phase=?1, status='active'",
                [phase],
            )
            .unwrap();
        fixture
            .connection
            .execute("UPDATE task_stage_runs SET phase=?1", [phase])
            .unwrap();
        let source = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        let input = WorkflowInput {
            source: WorkflowInputSource::Human,
            intent: WorkflowIntent::ReplanCurrent {
                change_request: "Revise approach".into(),
            },
        };
        let auth = authorize(&source, &input, None);
        let result = fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &handoff()))
            .unwrap();
        assert_eq!(
            (
                result.target_state.id,
                result.target_state.phase,
                result.target_state.version,
                result.target_state.plan.revision
            ),
            (source.id, TaskPhase::Planning, 4, 2)
        );
        assert_ne!(
            result.target_state.current_stage_run_id,
            source.current_stage_run_id
        );
        assert_eq!(
            fixture
                .connection
                .query_row("SELECT event FROM task_transitions", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            "replan_requested"
        );
    }
}

#[test]
fn transition_rejects_stale_or_forged_authorization_and_human_patch_without_writes() {
    let mut fixture = Fixture::new();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::PlanningCompleted);
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    let patch = patch(3);
    let mut command = transition_command(&source, &input, &auth, &payload);
    command.accepted_patch = Some(&patch);
    assert!(fixture.store.commit_stage_change(command).is_err());
    let mut forged = auth.clone();
    if let StageChangeAuthorization::Transition(ref mut auth) = forged {
        auth.to_phase = TaskPhase::Done;
    }
    assert!(
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &forged, &payload))
            .is_err()
    );
    fixture
        .connection
        .execute("UPDATE workflow_tasks SET version=4", [])
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &payload)),
        Err(StoreError::WorkflowConflict(1))
    ));
    assert_eq!(count(&fixture.connection, "messages"), 1);
    assert_eq!(count(&fixture.connection, "task_transitions"), 0);
}

#[test]
fn every_created_stage_initializes_empty_reduction_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("context.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "System", "build")
        .unwrap();
    let connection = Connection::open(path).unwrap();
    let (context, facts): (String, String) = connection
        .query_row(
            "SELECT context_json, facts_json FROM task_stage_context WHERE stage_run_id=?1",
            [started.stage_run_id.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&context).unwrap(),
        serde_json::json!({"summary":null,"compaction_usage":{"call_count":0,"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"missing_usage_count":0}})
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&facts).unwrap(),
        serde_json::json!({"facts":{},"covered_message_count":0,"update_usage":{"call_count":0,"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"missing_usage_count":0}})
    );
}

#[test]
fn workflow_branch_deep_copies_history_provenance_processing_and_replay_ids() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let mut input = transition_input(TransitionEvent::PlanningCompleted);
    input.source = WorkflowInputSource::Controller {
        checker: "continuation".into(),
        model: "checker-model".into(),
        triggering_assistant_message_id: assistant,
    };
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    let mut command = transition_command(&source, &input, &auth, &payload);
    command.processing_id = Some(processing);
    command.processing_attempt = Some(1);
    let original = fixture.store.commit_stage_change(command).unwrap();
    let context = r#"{"summary":{"content":"branch summary","covered_message_count":1},"compaction_usage":{"call_count":1,"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,"missing_usage_count":0}}"#;
    fixture
        .connection
        .execute("UPDATE task_stage_context SET context_json=?1", [context])
        .unwrap();
    let branch = fixture.store.fork_dialog(1, 3).unwrap().new_dialog_id;
    let copy = fixture
        .store
        .load_workflow(branch)
        .unwrap()
        .current_task
        .expect("branch must retain workflow");
    assert_ne!(copy.id, original.target_state.id);
    assert_ne!(
        copy.current_stage_run_id,
        original.target_state.current_stage_run_id
    );
    assert_ne!(
        copy.incoming_handoff_id,
        original.target_state.incoming_handoff_id
    );
    assert_eq!(copy.checkpoint, original.target_state.checkpoint);
    assert_eq!(
        fixture.store.load(branch).unwrap().messages,
        fixture.store.load(1).unwrap().messages
    );
    assert_eq!(
        fixture
            .store
            .load_stage_messages(copy.current_stage_run_id)
            .unwrap()[0]
            .source,
        ProtocolSource::Controller
    );
    assert_eq!(
        fixture
            .connection
            .query_row(
                "SELECT context_json FROM task_stage_context WHERE stage_run_id=?1",
                [copy.current_stage_run_id.0],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        context
    );
    let (copied_processing, copied_assistant, raw): (i64, i64, String) = fixture.connection.query_row("SELECT p.id,p.assistant_message_id,p.result_json FROM response_processing p JOIN messages m ON m.id=p.assistant_message_id WHERE m.dialog_id=?1", [branch], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_ne!(copied_processing, processing);
    assert_ne!(copied_assistant, assistant);
    let ProcessingResult::Transition {
        transition_id,
        input_message_id,
        workflow_input_id,
        target_state,
    } = serde_json::from_str(&raw).unwrap()
    else {
        panic!("transition result")
    };
    assert_eq!(*target_state, copy);
    assert_eq!(Some(transition_id), copy.incoming_handoff_id);
    assert_ne!(input_message_id, original.input_message_id);
    let linked: (i64, i64, i64, i64) = fixture.connection.query_row("SELECT i.message_id,i.processing_id,i.triggering_assistant_message_id,i.dialog_id FROM workflow_inputs i WHERE i.id=?1", [workflow_input_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(
        linked,
        (
            input_message_id,
            copied_processing,
            copied_assistant,
            branch
        )
    );
    let old_stage: i64 = fixture
        .connection
        .query_row(
            "SELECT from_stage_run_id FROM task_transitions WHERE id=?1",
            [transition_id],
            |r| r.get(0),
        )
        .unwrap();
    let mut copy_source = source.clone();
    copy_source.id = copy.id;
    copy_source.dialog_id = branch;
    copy_source.current_stage_run_id = StageRunId(old_stage);
    let mut copy_input = input.clone();
    if let WorkflowInputSource::Controller {
        triggering_assistant_message_id,
        ..
    } = &mut copy_input.source
    {
        *triggering_assistant_message_id = copied_assistant;
    }
    let mut replay = transition_command(&copy_source, &copy_input, &auth, &payload);
    replay.processing_id = Some(copied_processing);
    replay.processing_attempt = Some(1);
    assert_eq!(
        fixture
            .store
            .commit_stage_change(replay)
            .unwrap()
            .target_state,
        copy
    );
    let replan = WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::ReplanCurrent {
            change_request: "Branch only".into(),
        },
    };
    let replan_auth = authorize(&copy, &replan, None);
    fixture
        .store
        .commit_stage_change(transition_command(&copy, &replan, &replan_auth, &payload))
        .unwrap();
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap(),
        original.target_state
    );
    assert!(
        !fixture
            .connection
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap()
    );
}

#[test]
fn workflow_branch_failure_rolls_back_dialog_and_every_workflow_copy() {
    for table in [
        "workflow_tasks",
        "task_stage_runs",
        "message_task_stages",
        "response_processing",
        "workflow_inputs",
        "task_transitions",
        "task_stage_context",
        "dialog_workflow_state",
    ] {
        let (mut fixture, _, _) = pending_fixture();
        let source = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        let input = transition_input(TransitionEvent::PlanningCompleted);
        let auth = authorize(&source, &input, None);
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &handoff()))
            .unwrap();
        let tables = [
            "dialogs",
            "messages",
            "dialog_branches",
            "workflow_tasks",
            "task_stage_runs",
            "message_task_stages",
            "response_processing",
            "workflow_inputs",
            "task_transitions",
            "task_stage_context",
            "dialog_workflow_state",
        ];
        let counts: Vec<_> = tables
            .iter()
            .map(|t| count(&fixture.connection, t))
            .collect();
        fixture.connection.execute_batch(&format!("CREATE TRIGGER fail BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'injected'); END;")).unwrap();
        assert!(fixture.store.fork_dialog(1, 3).is_err(), "{table}");
        for (t, expected) in tables.iter().zip(counts) {
            assert_eq!(count(&fixture.connection, t), expected, "{table}: {t}");
        }
    }
}

#[test]
fn transition_failure_at_every_write_rolls_back_all_effects() {
    for (table, operation) in [
        ("messages", "INSERT"),
        ("workflow_inputs", "INSERT"),
        ("task_stage_runs", "UPDATE"),
        ("task_stage_runs", "INSERT"),
        ("task_transitions", "INSERT"),
        ("message_task_stages", "INSERT"),
        ("workflow_tasks", "UPDATE"),
        ("dialog_workflow_state", "UPDATE"),
        ("task_stage_context", "INSERT"),
        ("dialogs", "UPDATE"),
    ] {
        let mut fixture = Fixture::new();
        let source = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        let input = transition_input(TransitionEvent::PlanningCompleted);
        let auth = authorize(&source, &input, None);
        fixture.connection.execute_batch(&format!("CREATE TRIGGER fail BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END;")).unwrap();
        assert!(
            fixture
                .store
                .commit_stage_change(transition_command(&source, &input, &auth, &handoff()))
                .is_err(),
            "{table} {operation}"
        );
        assert_eq!(
            fixture
                .store
                .load_workflow(1)
                .unwrap()
                .current_task
                .unwrap(),
            source
        );
        for (checked, expected) in [
            ("messages", 1),
            ("workflow_inputs", 0),
            ("task_stage_runs", 1),
            ("task_transitions", 0),
            ("task_stage_context", 0),
            ("message_task_stages", 1),
        ] {
            assert_eq!(
                count(&fixture.connection, checked),
                expected,
                "{table} {operation}: {checked}"
            );
        }
    }
}

#[test]
fn ignored_transition_writes_cannot_commit_partial_state() {
    for (table, operation) in [
        ("messages", "INSERT"),
        ("workflow_inputs", "INSERT"),
        ("task_stage_runs", "UPDATE"),
        ("task_stage_runs", "INSERT"),
        ("task_transitions", "INSERT"),
        ("message_task_stages", "INSERT"),
        ("workflow_tasks", "UPDATE"),
        ("dialog_workflow_state", "UPDATE"),
        ("task_stage_context", "INSERT"),
        ("dialogs", "UPDATE"),
    ] {
        let mut fixture = Fixture::new();
        let source = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        let input = transition_input(TransitionEvent::PlanningCompleted);
        let auth = authorize(&source, &input, None);
        fixture.connection.execute_batch(&format!("CREATE TRIGGER ignore_write BEFORE {operation} ON {table} BEGIN SELECT RAISE(IGNORE); END;")).unwrap();
        assert!(
            fixture
                .store
                .commit_stage_change(transition_command(&source, &input, &auth, &handoff()))
                .is_err(),
            "{table} {operation}"
        );
        assert_eq!(
            fixture
                .store
                .load_workflow(1)
                .unwrap()
                .current_task
                .unwrap(),
            source
        );
        assert_eq!(count(&fixture.connection, "messages"), 1);
        assert_eq!(count(&fixture.connection, "task_transitions"), 0);
    }
}

#[test]
fn ignored_branch_mapping_context_or_pointer_rolls_back_copy() {
    for table in [
        "task_stage_context",
        "message_task_stages",
        "dialog_workflow_state",
    ] {
        let mut fixture = Fixture::new();
        let source = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        let input = transition_input(TransitionEvent::PlanningCompleted);
        let auth = authorize(&source, &input, None);
        fixture
            .store
            .commit_stage_change(transition_command(&source, &input, &auth, &handoff()))
            .unwrap();
        fixture.connection.execute_batch(&format!("CREATE TRIGGER ignore_write BEFORE INSERT ON {table} BEGIN SELECT RAISE(IGNORE); END;")).unwrap();
        assert!(fixture.store.fork_dialog(1, 2).is_err(), "{table}");
        assert_eq!(count(&fixture.connection, "dialogs"), 1);
    }
}

#[test]
fn terminal_done_stage_rejects_work_and_allows_only_next_human_task() {
    let mut fixture = Fixture::new();
    fixture.connection.execute_batch("UPDATE workflow_tasks SET phase='validation',status='active'; UPDATE task_stage_runs SET phase='validation';").unwrap();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    for status in ["active", "paused"] {
        fixture
            .connection
            .execute("UPDATE workflow_tasks SET status=?1", [status])
            .unwrap();
        assert!(
            fixture
                .store
                .create_task_with_human_input(
                    1,
                    "next",
                    "next",
                    ExpectedCurrentTask::Present {
                        task_id: source.id,
                        version: 3
                    }
                )
                .is_err()
        );
    }
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::ValidationPassed);
    let auth = authorize(&source, &input, None);
    let done = fixture
        .store
        .commit_stage_change(transition_command(&source, &input, &auth, &handoff()))
        .unwrap()
        .target_state;
    assert_eq!(
        (done.phase, done.status, done.current_stage_sequence),
        (TaskPhase::Done, TaskStatus::Active, 2)
    );
    assert_eq!(
        fixture
            .connection
            .query_row(
                "SELECT phase FROM task_stage_runs WHERE id=?1",
                [done.current_stage_run_id.0],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "done"
    );
    assert!(
        fixture
            .store
            .append_answer_for_processing(AnswerCommit {
                dialog_id: 1,
                task_id: done.id,
                stage_run_id: done.current_stage_run_id,
                expected_version: done.version,
                content: "must not work",
                usage: None
            })
            .is_err()
    );
    let controller = WorkflowInput {
        source: WorkflowInputSource::Controller {
            checker: "continuation".into(),
            model: "checker".into(),
            triggering_assistant_message_id: 1,
        },
        intent: WorkflowIntent::StartNewTask {
            goal: "forbidden".into(),
        },
    };
    assert!(
        fixture
            .store
            .append_input(
                InputCommit {
                    dialog_id: 1,
                    input: &controller,
                    protocol_text: "forbidden",
                    confidence: Some(1.0),
                    expected_current_task: ExpectedCurrentTask::Present {
                        task_id: done.id,
                        version: done.version
                    }
                },
                AcceptedInputEffect::ContinueSameStage
            )
            .is_err()
    );
    let next = fixture
        .store
        .create_task_with_human_input(
            1,
            "next",
            "next",
            ExpectedCurrentTask::Present {
                task_id: done.id,
                version: done.version,
            },
        )
        .unwrap();
    assert_eq!(
        (next.task.ordinal, next.task.phase),
        (2, TaskPhase::Planning)
    );
    assert_ne!(next.task.id, done.id);
    let branch = fixture.store.fork_dialog(1, 3).unwrap().new_dialog_id;
    assert_eq!(
        fixture
            .connection
            .query_row(
                "SELECT count(*) FROM workflow_tasks WHERE dialog_id=?1",
                [branch],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    assert_eq!(
        fixture
            .store
            .load_workflow(branch)
            .unwrap()
            .current_task
            .unwrap()
            .ordinal,
        2
    );
    assert!(
        !fixture
            .connection
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap()
    );
}

#[test]
fn controller_transition_failure_preserves_leased_answer_patch_and_source() {
    for operation in ["ABORT, 'injected'", "IGNORE"] {
        let (mut fixture, processing, assistant) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        let source = fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap();
        let mut input = transition_input(TransitionEvent::PlanningCompleted);
        input.source = WorkflowInputSource::Controller {
            checker: "continuation".into(),
            model: "checker-model".into(),
            triggering_assistant_message_id: assistant,
        };
        let auth = authorize(&source, &input, None);
        let patch = patch(3);
        let payload = handoff();
        fixture.connection.execute_batch(&format!("CREATE TRIGGER fail_completion BEFORE UPDATE ON response_processing BEGIN SELECT RAISE({operation}); END;")).unwrap();
        let mut command = transition_command(&source, &input, &auth, &payload);
        command.accepted_patch = Some(&patch);
        command.processing_id = Some(processing);
        command.processing_attempt = Some(1);
        assert!(fixture.store.commit_stage_change(command).is_err());
        assert_eq!(
            fixture
                .store
                .load_workflow(1)
                .unwrap()
                .current_task
                .unwrap(),
            source
        );
        assert_eq!(
            fixture.store.load_pending_processing(1).unwrap()[0].status,
            ProcessingStatus::Processing
        );
        assert_eq!(count(&fixture.connection, "messages"), 2);
        assert_eq!(count(&fixture.connection, "task_transitions"), 0);
        assert_eq!(count(&fixture.connection, "task_stage_runs"), 1);
        assert_eq!(count(&fixture.connection, "workflow_inputs"), 0);
    }
}

#[test]
fn controller_transition_rejects_forged_provenance_and_stale_processing() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let input = transition_input(TransitionEvent::PlanningCompleted);
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    for corruption in 0..7 {
        let mut input = input.clone();
        input.source = WorkflowInputSource::Controller {
            checker: if corruption == 0 {
                "wrong"
            } else {
                "continuation"
            }
            .into(),
            model: if corruption == 1 {
                " "
            } else {
                "checker-model"
            }
            .into(),
            triggering_assistant_message_id: if corruption == 2 { 1 } else { assistant },
        };
        let mut command = transition_command(&source, &input, &auth, &payload);
        command.processing_id = if corruption == 3 {
            None
        } else {
            Some(processing)
        };
        command.processing_attempt = Some(1);
        if corruption == 4 {
            command.confidence = Some(f32::NAN);
        }
        if corruption == 5 {
            command.protocol_text = " ";
        }
        if corruption == 6 {
            fixture
                .connection
                .execute("UPDATE workflow_tasks SET version=4", [])
                .unwrap();
        }
        assert!(
            fixture.store.commit_stage_change(command).is_err(),
            "case {corruption}"
        );
    }
    assert_eq!(count(&fixture.connection, "messages"), 2);
    assert_eq!(count(&fixture.connection, "task_transitions"), 0);
}

#[test]
fn branch_remaps_same_stage_controller_result_and_pending_processing_independently() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let intent = WorkflowIntent::human_continue("Do next").unwrap();
    let accepted_patch = patch(3);
    fixture
        .store
        .commit_controller_decision(controller_command(
            processing,
            assistant,
            &intent,
            &accepted_patch,
        ))
        .unwrap();
    let answer = fixture
        .store
        .append_answer_for_processing(answer_command(4))
        .unwrap();
    let branch = fixture.store.fork_dialog(1, 4).unwrap().new_dialog_id;
    let copy = fixture
        .store
        .load_workflow(branch)
        .unwrap()
        .current_task
        .unwrap();
    let pending = fixture.store.load_pending_processing(branch).unwrap();
    assert_eq!(pending.len(), 1);
    assert_ne!(pending[0].id, answer.processing_id);
    assert_ne!(pending[0].assistant_message_id, answer.message_id);
    let (copy_processing,copy_assistant,raw):(i64,i64,String)=fixture.connection.query_row("SELECT p.id,p.assistant_message_id,p.result_json FROM response_processing p JOIN messages m ON m.id=p.assistant_message_id WHERE m.dialog_id=?1 AND p.status='completed'",[branch],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    let mut replay = controller_command(copy_processing, copy_assistant, &intent, &accepted_patch);
    replay.task_id = copy.id;
    replay.stage_run_id = copy.current_stage_run_id;
    assert_eq!(
        fixture.store.commit_controller_decision(replay).unwrap(),
        serde_json::from_str::<ProcessingResult>(&raw).unwrap()
    );
    fixture
        .store
        .lease_processing(pending[0].id, 4, ProcessingLeaseMode::Normal)
        .unwrap();
    fixture
        .store
        .commit_await_user(
            pending[0].id,
            copy.id,
            copy.current_stage_run_id,
            4,
            1,
            &patch(4),
        )
        .unwrap();
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
    assert_eq!(
        fixture
            .store
            .load_workflow(branch)
            .unwrap()
            .current_task
            .unwrap()
            .version,
        5
    );
    assert_eq!(
        fixture.store.load_pending_processing(1).unwrap()[0].status,
        ProcessingStatus::Pending
    );
    assert!(
        !fixture
            .connection
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap()
    );
}

#[test]
fn patch_and_transition_increment_version_once_and_complete_processing() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture.connection.execute_batch("UPDATE workflow_tasks SET phase='execution'; UPDATE task_stage_runs SET phase='execution';").unwrap();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let mut input = transition_input(TransitionEvent::ExecutionCompleted);
    input.source = WorkflowInputSource::Controller {
        checker: "continuation".into(),
        model: "checker-model".into(),
        triggering_assistant_message_id: assistant,
    };
    let mut patch = patch(3);
    patch.step_updates.push(StepStatusUpdate {
        step_id: "design".into(),
        status: PlanStepStatus::Completed,
        evidence: vec!["Implementation saved".into()],
    });
    let auth = authorize(&source, &input, Some(&patch));
    let payload = handoff();
    let mut command = transition_command(&source, &input, &auth, &payload);
    command.processing_id = Some(processing);
    command.processing_attempt = Some(1);
    command.accepted_patch = Some(&patch);
    let result = fixture.store.commit_stage_change(command).unwrap();
    assert_eq!(
        (result.target_state.phase, result.target_state.version),
        (TaskPhase::Validation, 4)
    );
    assert_eq!(
        result.target_state.plan.steps[0].status,
        PlanStepStatus::Completed
    );
    assert_eq!(fixture.store.load(1).unwrap().messages.len(), 2);
    assert_eq!(
        fixture
            .store
            .load_stage_messages(result.target_state.current_stage_run_id)
            .unwrap()[0]
            .source,
        ProtocolSource::Controller
    );
    assert_eq!(
        fixture
            .connection
            .query_row(
                "SELECT status FROM response_processing WHERE id=?1",
                [processing],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "completed"
    );
    let mut command = transition_command(&source, &input, &auth, &payload);
    command.processing_id = Some(processing);
    command.processing_attempt = Some(1);
    command.accepted_patch = Some(&patch);
    assert_eq!(fixture.store.commit_stage_change(command).unwrap(), result);
    assert_eq!(count(&fixture.connection, "task_transitions"), 1);
    assert_eq!(count(&fixture.connection, "messages"), 3);
}

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
        fixture.store.commit_await_user(
            processing,
            WorkflowTaskId(1),
            StageRunId(1),
            3,
            1,
            &patch(2)
        ),
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
        fixture.store.create_task_with_human_input(
            1,
            "another",
            "another",
            ExpectedCurrentTask::Absent
        ),
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
        expected_attempt: 1,
        model: "checker-model",
        triggering_assistant_message_id: assistant,
        instruction: "Do next",
        intent,
        confidence: 0.9,
        accepted_patch: patch,
    }
}

// Break caught: exact controller replay must bind every accepted patch field,
// including across branch ID remapping, and old unbound rows fail closed.
fn assert_controller_replay_patch_binding(legacy: bool) {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let intent = WorkflowIntent::human_continue("Do next").unwrap();
    let accepted = patch(3);
    let result = fixture
        .store
        .commit_controller_decision(controller_command(
            processing, assistant, &intent, &accepted,
        ))
        .unwrap();
    assert_eq!(
        fixture
            .store
            .commit_controller_decision(controller_command(
                processing, assistant, &intent, &accepted
            ))
            .unwrap(),
        result
    );
    if legacy {
        let mut raw = serde_json::to_value(&result).unwrap();
        raw.as_object_mut().unwrap().remove("patch_fingerprint");
        fixture
            .connection
            .execute(
                "UPDATE response_processing SET result_json=?1 WHERE id=?2",
                rusqlite::params![raw.to_string(), processing],
            )
            .unwrap();
    }
    let branch = fixture.store.fork_dialog(1, 3).unwrap().new_dialog_id;
    let copied_task = fixture
        .store
        .load_workflow(branch)
        .unwrap()
        .current_task
        .unwrap();
    let (copied_processing, copied_assistant): (i64, i64) = fixture.connection.query_row(
            "SELECT p.id,p.assistant_message_id FROM response_processing p JOIN messages m ON m.id=p.assistant_message_id WHERE m.dialog_id=?1", [branch], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    let before_task = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    for (pid, aid, task) in [
        (processing, assistant, &before_task),
        (copied_processing, copied_assistant, &copied_task),
    ] {
        let before = processing_record(&fixture.connection, pid);
        let mut same = controller_command(pid, aid, &intent, &accepted);
        same.task_id = task.id;
        same.stage_run_id = task.current_stage_run_id;
        let replay = fixture.store.commit_controller_decision(same);
        if legacy {
            assert!(replay.is_err(), "unbound legacy replay must fail closed");
        } else {
            assert!(replay.is_ok());
        }
        let mut changed = accepted.clone();
        changed.expected_action = Some("DIFFERENT proposed effect".into());
        let mut command = controller_command(pid, aid, &intent, &changed);
        command.task_id = task.id;
        command.stage_run_id = task.current_stage_run_id;
        assert!(
            fixture.store.commit_controller_decision(command).is_err(),
            "different patch must not replay successfully"
        );
        assert_eq!(processing_record(&fixture.connection, pid), before);
        assert_eq!(
            fixture
                .store
                .load_workflow(task.dialog_id)
                .unwrap()
                .current_task
                .unwrap(),
            *task
        );
        assert_eq!(
            fixture.store.load(task.dialog_id).unwrap().messages.len(),
            2
        );
    }
    assert_eq!(count(&fixture.connection, "messages"), 6);
}

#[test]
fn final_controller_replay_binds_patch_in_original_and_branch() {
    assert_controller_replay_patch_binding(false);
}

#[test]
fn final_legacy_controller_replay_fails_closed_and_preserves_branch_audit() {
    assert_controller_replay_patch_binding(true);
}

// Break caught: cleanup must fence the selected task/stage/version/status and
// exact attempt, preserve other terminal states, and roll back a failed batch.
#[test]
fn final_exhausted_processing_closure_is_scoped_atomic_and_fenced() {
    let (mut fixture, processing, assistant) = pending_fixture();
    let task = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    assert_eq!(fixture.store.close_exhausted_processing(&task).unwrap(), 0);
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
        .unwrap();
    let before = processing_record(&fixture.connection, processing);
    for field in ["task", "stage", "version", "status"] {
        let mut stale = task.clone();
        match field {
            "task" => stale.id = WorkflowTaskId(99),
            "stage" => stale.current_stage_run_id = StageRunId(99),
            "version" => stale.version += 1,
            "status" => stale.status = TaskStatus::Paused,
            _ => unreachable!(),
        }
        assert!(
            fixture.store.close_exhausted_processing(&stale).is_err(),
            "{field}"
        );
        assert_eq!(processing_record(&fixture.connection, processing), before);
    }
    let answer = fixture
        .store
        .append_answer_for_processing(answer_command(3))
        .unwrap();
    fixture
        .store
        .lease_processing(answer.processing_id, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    fixture
        .store
        .lease_processing(answer.processing_id, 3, ProcessingLeaseMode::Recovery)
        .unwrap();
    fixture
        .connection
        .execute_batch(&format!(
            "CREATE TRIGGER fail_exhausted BEFORE UPDATE ON response_processing
        WHEN NEW.id={} AND NEW.status='failed' BEGIN SELECT RAISE(ABORT,'injected'); END;",
            answer.processing_id
        ))
        .unwrap();
    assert!(fixture.store.close_exhausted_processing(&task).is_err());
    assert_eq!(processing_record(&fixture.connection, processing), before);
    assert_eq!(
        processing_record(&fixture.connection, answer.processing_id).0,
        "processing"
    );
    fixture
        .connection
        .execute_batch("DROP TRIGGER fail_exhausted")
        .unwrap();
    fixture
        .connection
        .execute(
            "UPDATE response_processing SET attempts=3 WHERE id=?1",
            [processing],
        )
        .unwrap();
    assert_eq!(fixture.store.close_exhausted_processing(&task).unwrap(), 2);
    assert_eq!(
        processing_record(&fixture.connection, processing),
        (
            "failed".into(),
            3,
            Some("recovery attempts exhausted".into()),
            None
        )
    );
    assert_eq!(fixture.store.close_exhausted_processing(&task).unwrap(), 0);
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap(),
        task
    );
    assert!(
        fixture
            .store
            .fail_processing(failure_command(processing, assistant, 2, "late worker"))
            .is_err()
    );
    for status in ["completed", "failed"] {
        fixture
            .connection
            .execute(
                "UPDATE response_processing SET status=?1,last_error='keep diagnostic' WHERE id=?2",
                rusqlite::params![status, processing],
            )
            .unwrap();
        let before = processing_record(&fixture.connection, processing);
        assert_eq!(fixture.store.close_exhausted_processing(&task).unwrap(), 0);
        assert_eq!(processing_record(&fixture.connection, processing), before);
    }
}

fn failure_command(
    processing: i64,
    assistant: i64,
    expected_attempt: u32,
    diagnostic: &str,
) -> FailProcessingCommit<'_> {
    FailProcessingCommit {
        processing_id: processing,
        dialog_id: 1,
        task_id: WorkflowTaskId(1),
        stage_run_id: StageRunId(1),
        expected_version: 3,
        expected_attempt,
        triggering_assistant_message_id: assistant,
        checker: "continuation",
        diagnostic,
    }
}

fn processing_record(
    connection: &Connection,
    processing: i64,
) -> (String, u32, Option<String>, Option<String>) {
    connection
        .query_row(
            "SELECT status,attempts,last_error,result_json FROM response_processing WHERE id=?1",
            [processing],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
}

// Break caught: an attempt-one checker must not complete a job already leased by attempt two.
#[test]
fn recovered_lease_fences_stale_await_user_completion() {
    let (mut fixture, processing, _) = pending_fixture();
    let first = fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap()
        .unwrap();
    let second = fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
        .unwrap()
        .unwrap();
    assert_eq!((first.attempts, second.attempts), (1, 2));
    assert!(
        fixture
            .store
            .commit_await_user(
                processing,
                WorkflowTaskId(1),
                StageRunId(1),
                3,
                1,
                &patch(3)
            )
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
    assert_eq!(
        processing_record(&fixture.connection, processing),
        ("processing".into(), 2, None, None)
    );
    let result = fixture
        .store
        .commit_await_user(
            processing,
            WorkflowTaskId(1),
            StageRunId(1),
            3,
            second.attempts,
            &patch(3),
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .commit_await_user(
                processing,
                WorkflowTaskId(1),
                StageRunId(1),
                3,
                first.attempts,
                &patch(3)
            )
            .unwrap(),
        result
    );
}

// Break caught: a recovered processing lease must fence stale controller continuation effects.
#[test]
fn recovered_lease_fences_stale_controller_completion() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
        .unwrap();
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
    assert_eq!(count(&fixture.connection, "messages"), 2);
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
    assert_eq!(
        processing_record(&fixture.connection, processing),
        ("processing".into(), 2, None, None)
    );
    let patch = patch(3);
    let mut current = controller_command(processing, assistant, &intent, &patch);
    current.expected_attempt = 2;
    let result = fixture.store.commit_controller_decision(current).unwrap();
    assert_eq!(
        fixture
            .store
            .commit_controller_decision(controller_command(processing, assistant, &intent, &patch))
            .unwrap(),
        result
    );
}

// Break caught: a recovered processing lease must fence stale controller stage transitions.
#[test]
fn recovered_lease_fences_stale_controller_transition() {
    let (mut fixture, processing, assistant) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
        .unwrap();
    let source = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let mut input = transition_input(TransitionEvent::PlanningCompleted);
    input.source = WorkflowInputSource::Controller {
        checker: "continuation".into(),
        model: "checker-model".into(),
        triggering_assistant_message_id: assistant,
    };
    let auth = authorize(&source, &input, None);
    let payload = handoff();
    let mut command = transition_command(&source, &input, &auth, &payload);
    command.processing_id = Some(processing);
    command.processing_attempt = Some(1);
    assert!(fixture.store.commit_stage_change(command).is_err());
    assert_eq!(count(&fixture.connection, "messages"), 2);
    assert_eq!(count(&fixture.connection, "task_transitions"), 0);
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap(),
        source
    );
    assert_eq!(
        processing_record(&fixture.connection, processing),
        ("processing".into(), 2, None, None)
    );
    let mut current = transition_command(&source, &input, &auth, &payload);
    current.processing_id = Some(processing);
    current.processing_attempt = Some(2);
    let result = fixture.store.commit_stage_change(current).unwrap();
    let mut replay = transition_command(&source, &input, &auth, &payload);
    replay.processing_id = Some(processing);
    replay.processing_attempt = Some(1);
    assert_eq!(fixture.store.commit_stage_change(replay).unwrap(), result);
}

// Break caught: failure commands must bind every origin field, including the exact lease attempt.
#[test]
fn failure_rejects_forged_origin_and_stale_attempt_without_mutation() {
    for mismatch in [
        "processing",
        "dialog",
        "task",
        "stage",
        "version",
        "assistant",
        "checker",
        "attempt",
        "recovered",
    ] {
        let (mut fixture, processing, assistant) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        if mismatch == "recovered" {
            fixture
                .store
                .lease_processing(processing, 3, ProcessingLeaseMode::Recovery)
                .unwrap();
        }
        let before = processing_record(&fixture.connection, processing);
        let mut command = failure_command(processing, assistant, 1, "checker rejected");
        match mismatch {
            "processing" => command.processing_id += 100,
            "dialog" => command.dialog_id += 1,
            "task" => command.task_id = WorkflowTaskId(2),
            "stage" => command.stage_run_id = StageRunId(2),
            "version" => command.expected_version += 1,
            "assistant" => command.triggering_assistant_message_id = 1,
            "checker" => command.checker = "other checker",
            "attempt" => command.expected_attempt = 0,
            "recovered" => {}
            _ => unreachable!(),
        }
        assert!(
            fixture.store.fail_processing(command).is_err(),
            "{mismatch}"
        );
        assert_eq!(
            processing_record(&fixture.connection, processing),
            before,
            "{mismatch}"
        );
        assert_eq!(count(&fixture.connection, "messages"), 2);
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
}

// Break caught: a matching command cannot bypass durable assistant ownership or current-stage checks.
#[test]
fn failure_rejects_corrupt_assistant_ownership_and_stale_state() {
    for mutation in [
        "UPDATE messages SET role='user' WHERE role='assistant'",
        "UPDATE messages SET dialog_id=2 WHERE role='assistant'",
        "DELETE FROM message_task_stages WHERE message_id=(SELECT assistant_message_id FROM response_processing)",
        "UPDATE message_task_stages SET workflow_task_id=2 WHERE message_id=(SELECT assistant_message_id FROM response_processing)",
        "UPDATE message_task_stages SET stage_run_id=2 WHERE message_id=(SELECT assistant_message_id FROM response_processing)",
        "UPDATE workflow_tasks SET version=4 WHERE id=1",
        "UPDATE workflow_tasks SET status='paused' WHERE id=1",
        "UPDATE task_stage_runs SET finished_at='9999-01-01' WHERE id=1",
    ] {
        let (mut fixture, processing, assistant) = pending_fixture();
        fixture
            .store
            .start_dialog_with_workflow_task(&RequestScope::default(), "system", "unrelated task")
            .unwrap();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        fixture.connection.execute_batch(mutation).unwrap();
        let before = processing_record(&fixture.connection, processing);
        assert!(
            fixture
                .store
                .fail_processing(failure_command(
                    processing,
                    assistant,
                    1,
                    "checker rejected"
                ))
                .is_err(),
            "{mutation}"
        );
        assert_eq!(
            processing_record(&fixture.connection, processing),
            before,
            "{mutation}"
        );
        assert_eq!(count(&fixture.connection, "messages"), 3);
    }
}

// Break caught: a completed AwaitUser result must not authenticate a different checker patch.
#[test]
fn await_user_replay_rejects_a_different_patch() {
    let (mut fixture, processing, _) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    fixture
        .store
        .commit_await_user(
            processing,
            WorkflowTaskId(1),
            StageRunId(1),
            3,
            1,
            &patch(3),
        )
        .unwrap();
    let mut changed = patch(3);
    changed.expected_action = Some("different accepted action".into());
    assert!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, 1, &changed)
            .is_err()
    );
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap()
            .expected_action
            .as_deref(),
        Some("Next action")
    );
}

// Break caught: the immutable AwaitUser patch binding must survive branch copying unchanged.
#[test]
fn await_user_branch_preserves_exact_patch_replay_identity() {
    let (mut fixture, processing, _) = pending_fixture();
    fixture
        .store
        .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
        .unwrap();
    let accepted = patch(3);
    let result = fixture
        .store
        .commit_await_user(
            processing,
            WorkflowTaskId(1),
            StageRunId(1),
            3,
            1,
            &accepted,
        )
        .unwrap();
    let ProcessingResult::AwaitUser {
        patch_fingerprint, ..
    } = &result
    else {
        panic!("not AwaitUser")
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(patch_fingerprint).unwrap(),
        serde_json::json!({
            "expected_version":3,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],
            "current_step_id":null,"expected_action":"Next action","checkpoint":null,
        })
    );
    let branch = fixture.store.fork_dialog(1, 2).unwrap().new_dialog_id;
    let task = fixture
        .store
        .load_workflow(branch)
        .unwrap()
        .current_task
        .unwrap();
    let (copy_processing, copied_json): (i64, String) = fixture.connection.query_row("SELECT p.id,p.result_json FROM response_processing p JOIN messages m ON m.id=p.assistant_message_id WHERE m.dialog_id=?1", [branch], |row| Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_ne!(copy_processing, processing);
    assert_eq!(
        serde_json::from_str::<ProcessingResult>(&copied_json).unwrap(),
        result
    );
    assert_eq!(
        fixture
            .store
            .commit_await_user(
                copy_processing,
                task.id,
                task.current_stage_run_id,
                3,
                1,
                &accepted
            )
            .unwrap(),
        result
    );
    let mut changed = accepted;
    changed.checkpoint = Some(deepseek_cli::workflow::StageCheckpoint {
        summary: "different replay patch".into(),
        decisions: vec![],
        open_issues: vec![],
    });
    assert!(
        fixture
            .store
            .commit_await_user(
                copy_processing,
                task.id,
                task.current_stage_run_id,
                3,
                1,
                &changed
            )
            .is_err()
    );
    assert_eq!(
        fixture
            .store
            .load_workflow(branch)
            .unwrap()
            .current_task
            .unwrap(),
        task
    );
}

// Break caught: branching must preserve pre-fingerprint AwaitUser audit without enabling its replay.
#[test]
fn legacy_await_user_branch_preserves_raw_audit_but_replay_stays_rejected() {
    for (raw, nonempty_patch) in [
        (
            " {\"task_version\":3, \"decision\":\"await_user\"}\n",
            false,
        ),
        ("\n{\"decision\":\"await_user\", \"task_version\":4} ", true),
    ] {
        let (mut fixture, processing, _) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        let mut accepted = patch(3);
        if !nonempty_patch {
            accepted.expected_action = None;
        }
        fixture
            .store
            .commit_await_user(
                processing,
                WorkflowTaskId(1),
                StageRunId(1),
                3,
                1,
                &accepted,
            )
            .unwrap();
        fixture
            .connection
            .execute(
                "UPDATE response_processing SET result_json=?1 WHERE id=?2",
                rusqlite::params![raw, processing],
            )
            .unwrap();
        let original_record = processing_record(&fixture.connection, processing);
        let branch = fixture.store.fork_dialog(1, 2).unwrap().new_dialog_id;
        let task = fixture
            .store
            .load_workflow(branch)
            .unwrap()
            .current_task
            .unwrap();
        let copied_processing: i64 = fixture.connection.query_row("SELECT p.id FROM response_processing p JOIN messages m ON m.id=p.assistant_message_id WHERE m.dialog_id=?1", [branch], |row| row.get(0)).unwrap();
        assert_ne!(copied_processing, processing);
        assert_eq!(
            processing_record(&fixture.connection, copied_processing),
            original_record
        );
        assert_eq!(original_record.3.as_deref(), Some(raw));
        assert_eq!(
            fixture.store.load(branch).unwrap().messages,
            fixture.store.load(1).unwrap().messages
        );
        for (id, task_id, stage_id) in [
            (processing, WorkflowTaskId(1), StageRunId(1)),
            (copied_processing, task.id, task.current_stage_run_id),
        ] {
            assert!(matches!(
                fixture
                    .store
                    .commit_await_user(id, task_id, stage_id, 3, 1, &accepted),
                Err(StoreError::InvalidWorkflow(_) | StoreError::WorkflowConflict(_))
            ));
            assert_eq!(processing_record(&fixture.connection, id), original_record);
        }
        assert_eq!(
            fixture
                .store
                .load_workflow(branch)
                .unwrap()
                .current_task
                .unwrap(),
            task
        );
        assert_eq!(count(&fixture.connection, "dialogs"), 2);
        assert_eq!(count(&fixture.connection, "messages"), 4);
    }
}

// Break caught: the branch-only legacy exception must not admit malformed or unknown result shapes.
#[test]
fn legacy_await_user_branch_rejects_malformed_unknown_or_invalid_results() {
    for raw in [
        r#"{"decision":"await_user","task_version":4,"unknown":true}"#,
        r#"{"decision":"await_user","task_version":4,"patch_fingerprint":null}"#,
        r#"{"decision":"await_user"}"#,
        r#"{"decision":"await_user","task_version":-1}"#,
        r#"{"decision":"await_user","task_version":4.0}"#,
        r#"{"decision":"await_user","task_version":"4"}"#,
        r#"{"decision":"await_user","task_version":2}"#,
        r#"{"decision":"await_user","task_version":5}"#,
        r#"{"decision":"await_user","task_version":18446744073709551615}"#,
        r#"{"decision":"await_user","task_version":4,"task_version":4}"#,
        r#"{"decision":"unknown","task_version":4}"#,
        r#"{"decision":"controller_input","task_version":4}"#,
        r#"{"decision":"transition","task_version":4}"#,
        "not JSON",
    ] {
        let (mut fixture, processing, _) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        fixture
            .store
            .commit_await_user(
                processing,
                WorkflowTaskId(1),
                StageRunId(1),
                3,
                1,
                &patch(3),
            )
            .unwrap();
        fixture
            .connection
            .execute(
                "UPDATE response_processing SET result_json=?1 WHERE id=?2",
                rusqlite::params![raw, processing],
            )
            .unwrap();
        let before = processing_record(&fixture.connection, processing);
        assert!(
            matches!(
                fixture.store.fork_dialog(1, 2),
                Err(StoreError::InvalidWorkflow(_))
            ),
            "{raw}"
        );
        assert_eq!(processing_record(&fixture.connection, processing), before);
        assert_eq!(count(&fixture.connection, "dialogs"), 1);
        assert_eq!(count(&fixture.connection, "messages"), 2);
        assert_eq!(count(&fixture.connection, "response_processing"), 1);
    }
}

// Break caught: legacy or corrupt completion JSON must never authorize unbound patch replay.
#[test]
fn await_user_replay_rejects_missing_or_invalid_patch_binding() {
    for binding in [None, Some("{}"), Some("not JSON")] {
        let (mut fixture, processing, _) = pending_fixture();
        fixture
            .store
            .lease_processing(processing, 3, ProcessingLeaseMode::Normal)
            .unwrap();
        fixture
            .store
            .commit_await_user(
                processing,
                WorkflowTaskId(1),
                StageRunId(1),
                3,
                1,
                &patch(3),
            )
            .unwrap();
        let (_, _, _, raw) = processing_record(&fixture.connection, processing);
        let mut raw: serde_json::Value = serde_json::from_str(&raw.unwrap()).unwrap();
        match binding {
            Some(value) => {
                raw["patch_fingerprint"] = value.into();
            }
            None => {
                raw.as_object_mut().unwrap().remove("patch_fingerprint");
            }
        }
        fixture
            .connection
            .execute(
                "UPDATE response_processing SET result_json=?1 WHERE id=?2",
                rusqlite::params![raw.to_string(), processing],
            )
            .unwrap();
        let before = processing_record(&fixture.connection, processing);
        assert!(
            fixture
                .store
                .commit_await_user(
                    processing,
                    WorkflowTaskId(1),
                    StageRunId(1),
                    3,
                    1,
                    &patch(3)
                )
                .is_err()
        );
        assert_eq!(processing_record(&fixture.connection, processing), before);
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
        .fail_processing(failure_command(
            processing,
            assistant,
            2,
            "checker unavailable",
        ))
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
    let (mut fixture, processing, assistant) = pending_fixture();
    assert!(
        fixture
            .store
            .fail_processing(failure_command(processing, assistant, 1, "error"))
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
                .fail_processing(failure_command(processing, assistant, 1, &invalid))
                .is_err()
        );
    }
    fixture
        .store
        .fail_processing(failure_command(
            processing,
            assistant,
            1,
            "checker unavailable",
        ))
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
    let (mut fixture, processing, assistant) = pending_fixture();
    let mut patch = patch(3);
    assert!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, 1, &patch)
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
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, 1, &patch)
            .is_err()
    );
    patch.step_updates.clear();
    let result = fixture
        .store
        .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, 1, &patch)
        .unwrap();
    assert!(
        matches!(&result, ProcessingResult::AwaitUser { task_version: 4, patch_fingerprint } if !patch_fingerprint.is_empty())
    );
    assert_eq!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, 1, &patch)
            .unwrap(),
        result
    );
    assert!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(2), StageRunId(1), 3, 1, &patch)
            .is_err()
    );
    assert!(
        fixture
            .store
            .fail_processing(failure_command(processing, assistant, 1, "late failure"))
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
            .commit_await_user(
                processing,
                WorkflowTaskId(1),
                StageRunId(1),
                3,
                1,
                &patch(3)
            )
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
    assert!(matches!(
        fixture
            .store
            .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, 1, &patch)
            .unwrap(),
        ProcessingResult::AwaitUser {
            task_version: 3,
            ..
        }
    ));
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
                .commit_await_user(
                    processing,
                    WorkflowTaskId(1),
                    StageRunId(1),
                    3,
                    1,
                    &patch(3)
                )
                .is_err()
        );
        let recovered =
            fixture
                .store
                .lease_processing(processing, 3, ProcessingLeaseMode::Recovery);
        if corruption.contains("status='paused'") {
            assert_eq!(recovered.unwrap().unwrap().attempts, 2);
        } else {
            assert!(recovered.is_err());
        }
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
            ..
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
                .commit_await_user(processing, WorkflowTaskId(1), StageRunId(1), 3, 1, &patch)
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

#[test]
fn stale_human_input_cannot_target_replacement_task_with_the_same_version() {
    let mut fixture = Fixture::new();
    let observed = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let mut other = DialogStore::open(&fixture._directory.path().join("workflow.sqlite3")).unwrap();
    fixture.connection.execute_batch("UPDATE workflow_tasks SET phase='done', status='active', version=4; UPDATE task_stage_runs SET phase='done';").unwrap();
    let replacement = other
        .create_task_with_human_input(
            1,
            "replacement",
            "replacement",
            ExpectedCurrentTask::Present {
                task_id: observed.id,
                version: 4,
            },
        )
        .unwrap();
    fixture
        .connection
        .execute(
            "UPDATE workflow_tasks SET version=3 WHERE id=?1",
            [replacement.task.id.0],
        )
        .unwrap();
    let message_count = count(&fixture.connection, "messages");
    let input_count = count(&fixture.connection, "workflow_inputs");
    let mapping_count = count(&fixture.connection, "message_task_stages");
    let current = other.load_workflow(1).unwrap().current_task.unwrap();
    assert_ne!(current.id, observed.id);
    assert_eq!(current.version, observed.version);
    let input = human("continue");
    let mut command = input_command(&input, Some(observed.version));
    command.expected_current_task = ExpectedCurrentTask::Present {
        task_id: observed.id,
        version: observed.version,
    };
    assert!(matches!(
        fixture
            .store
            .append_input(command, AcceptedInputEffect::ContinueSameStage),
        Err(StoreError::WorkflowConflict(1))
    ));
    assert_eq!(count(&fixture.connection, "messages"), message_count);
    assert_eq!(count(&fixture.connection, "workflow_inputs"), input_count);
    assert_eq!(
        count(&fixture.connection, "message_task_stages"),
        mapping_count
    );
    assert_eq!(
        other.load_workflow(1).unwrap().current_task.unwrap(),
        current
    );
}

#[test]
fn stale_creation_cannot_replace_a_different_done_task_with_the_same_version() {
    let mut fixture = Fixture::new();
    fixture.connection.execute_batch("UPDATE workflow_tasks SET phase='done', status='active'; UPDATE task_stage_runs SET phase='done';").unwrap();
    let observed = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let mut other = DialogStore::open(&fixture._directory.path().join("workflow.sqlite3")).unwrap();
    let expected = ExpectedCurrentTask::Present {
        task_id: observed.id,
        version: observed.version,
    };
    let replacement = other
        .create_task_with_human_input(1, "replacement", "replacement", expected)
        .unwrap();
    fixture
        .connection
        .execute(
            "UPDATE workflow_tasks SET phase='done', version=3 WHERE id=?1",
            [replacement.task.id.0],
        )
        .unwrap();
    fixture
        .connection
        .execute(
            "UPDATE task_stage_runs SET phase='done' WHERE id=?1",
            [replacement.stage_run_id.0],
        )
        .unwrap();
    let current = other.load_workflow(1).unwrap().current_task.unwrap();
    assert_ne!(current.id, observed.id);
    assert_eq!(current.version, observed.version);
    let counts = [
        "workflow_tasks",
        "task_stage_runs",
        "messages",
        "workflow_inputs",
        "message_task_stages",
    ]
    .map(|table| count(&fixture.connection, table));
    assert!(matches!(
        fixture
            .store
            .create_task_with_human_input(1, "stale", "stale", expected),
        Err(StoreError::WorkflowConflict(1))
    ));
    assert_eq!(
        [
            "workflow_tasks",
            "task_stage_runs",
            "messages",
            "workflow_inputs",
            "message_task_stages"
        ]
        .map(|table| count(&fixture.connection, table)),
        counts
    );
    assert_eq!(
        other.load_workflow(1).unwrap().current_task.unwrap(),
        current
    );
}

fn input_command(input: &WorkflowInput, version: Option<u64>) -> InputCommit<'_> {
    InputCommit {
        dialog_id: 1,
        input,
        protocol_text: "continue",
        confidence: Some(0.9),
        expected_current_task: version.map_or(ExpectedCurrentTask::Absent, |version| {
            ExpectedCurrentTask::Present {
                task_id: WorkflowTaskId(1),
                version,
            }
        }),
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

// Break caught: an interrupt may change only activity status, version, and timestamp.
#[test]
fn pausing_and_human_resume_preserve_the_complete_stage_projection() {
    let mut fixture = Fixture::new();
    fixture
        .connection
        .execute_batch(
            "UPDATE workflow_tasks SET phase='execution', status='active';
             UPDATE task_stage_runs SET phase='execution';",
        )
        .unwrap();
    let before = fixture
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    let stage_count = count(&fixture.connection, "task_stage_runs");
    let transition_count = count(&fixture.connection, "task_transitions");

    let PauseOutcome::Paused(paused) = fixture.store.pause_current_task(1).unwrap() else {
        panic!("active unfinished task must be paused");
    };
    let mut expected = before.clone().pause().unwrap();
    expected.version += 1;
    assert_eq!(*paused, expected);
    assert_eq!(
        fixture
            .store
            .load_workflow(1)
            .unwrap()
            .current_task
            .unwrap(),
        expected
    );
    assert_eq!(count(&fixture.connection, "task_stage_runs"), stage_count);
    assert_eq!(
        count(&fixture.connection, "task_transitions"),
        transition_count
    );

    let input = human("continue");
    let resumed = fixture
        .store
        .append_input(
            InputCommit {
                dialog_id: 1,
                input: &input,
                protocol_text: "continue",
                confidence: Some(0.9),
                expected_current_task: ExpectedCurrentTask::Present {
                    task_id: paused.id,
                    version: paused.version,
                },
            },
            AcceptedInputEffect::ResumeSameStage,
        )
        .unwrap()
        .task
        .unwrap();
    assert_eq!(resumed.status, TaskStatus::Active);
    assert_eq!(resumed.id, before.id);
    assert_eq!(resumed.current_stage_run_id, before.current_stage_run_id);
    assert_eq!(resumed.version, before.version + 2);
    assert_eq!(count(&fixture.connection, "task_stage_runs"), stage_count);
}

// Break caught: no-task, done, repeated, and ignored updates must never masquerade as a pause.
#[test]
fn pausing_returns_typed_non_mutating_outcomes_and_fences_the_update() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("empty.sqlite3");
    let mut empty = DialogStore::open(&path).unwrap();
    let dialog_id = empty.start_dialog("System", "legacy").unwrap();
    assert_eq!(
        empty.pause_current_task(dialog_id).unwrap(),
        PauseOutcome::NoTask
    );

    let mut paused = Fixture::new();
    let paused_before = paused.store.load_workflow(1).unwrap().current_task.unwrap();
    assert_eq!(
        paused.store.pause_current_task(1).unwrap(),
        PauseOutcome::AlreadyPaused
    );
    assert_eq!(
        paused.store.load_workflow(1).unwrap().current_task.unwrap(),
        paused_before
    );

    let mut done = Fixture::new();
    done.connection
        .execute_batch(
            "UPDATE workflow_tasks SET phase='done', status='active';
             UPDATE task_stage_runs SET phase='done';",
        )
        .unwrap();
    let done_before = done.store.load_workflow(1).unwrap().current_task.unwrap();
    assert_eq!(
        done.store.pause_current_task(1).unwrap(),
        PauseOutcome::AlreadyDone
    );
    assert_eq!(
        done.store.load_workflow(1).unwrap().current_task.unwrap(),
        done_before
    );

    let mut ignored = Fixture::new();
    ignored
        .connection
        .execute("UPDATE workflow_tasks SET status='active'", [])
        .unwrap();
    ignored
        .connection
        .execute_batch(
            "CREATE TRIGGER ignore_pause BEFORE UPDATE ON workflow_tasks
             WHEN NEW.status='paused' BEGIN SELECT RAISE(IGNORE); END;",
        )
        .unwrap();
    assert!(matches!(
        ignored.store.pause_current_task(1),
        Err(StoreError::WorkflowConflict(1))
    ));
    let unchanged = ignored
        .store
        .load_workflow(1)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(
        (unchanged.status, unchanged.version),
        (TaskStatus::Active, 3)
    );
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
    assert!(matches!(
        first.create_task_with_human_input(
            dialog,
            "one",
            "one",
            ExpectedCurrentTask::Present {
                task_id: WorkflowTaskId(1),
                version: 0
            }
        ),
        Err(StoreError::WorkflowConflict(_))
    ));
    let created = first
        .create_task_with_human_input(dialog, "one", "one", ExpectedCurrentTask::Absent)
        .unwrap();
    assert!(matches!(
        second.create_task_with_human_input(
            dialog,
            "two",
            "two",
            ExpectedCurrentTask::Present {
                task_id: created.task.id,
                version: 0
            }
        ),
        Err(StoreError::WorkflowConflict(_))
    ));
    let connection = Connection::open(path).unwrap();
    connection.execute_batch("UPDATE workflow_tasks SET phase='done', version=4; UPDATE task_stage_runs SET phase='done';").unwrap();
    assert!(matches!(
        second.create_task_with_human_input(
            dialog,
            "two",
            "two",
            ExpectedCurrentTask::Present {
                task_id: created.task.id,
                version: 0
            }
        ),
        Err(StoreError::WorkflowConflict(_))
    ));
    let next = second
        .create_task_with_human_input(
            dialog,
            "two",
            "two",
            ExpectedCurrentTask::Present {
                task_id: created.task.id,
                version: 4,
            },
        )
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
                .create_task_with_human_input(dialog, text, goal, ExpectedCurrentTask::Absent)
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
