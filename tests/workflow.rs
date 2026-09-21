use deepseek_cli::workflow::{
    PatchContext, PlanAppend, PlanStep, PlanStepStatus, StageCheckpoint, StageRunId, StateMachine,
    StepStatusUpdate, TaskPhase, TaskPlan, TaskStatePatch, TaskStatus, TransitionEvent,
    WorkflowError, WorkflowInputSource, WorkflowIntent, WorkflowTaskId, WorkflowTaskState,
    render_task_state,
};

fn state(phase: TaskPhase) -> WorkflowTaskState {
    WorkflowTaskState {
        id: WorkflowTaskId(1),
        dialog_id: 2,
        ordinal: 1,
        phase,
        status: TaskStatus::Active,
        goal: "Ship the workflow domain".into(),
        plan: TaskPlan {
            revision: 0,
            steps: vec![
                PlanStep {
                    id: "implement".into(),
                    description: "Implement the state machine".into(),
                    status: PlanStepStatus::Pending,
                },
                PlanStep {
                    id: "validate".into(),
                    description: "Run the focused test suite".into(),
                    status: PlanStepStatus::Pending,
                },
            ],
            acceptance_criteria: vec!["focused tests pass".into()],
        },
        current_step_id: Some("implement".into()),
        expected_action: Some("Implement the reducer".into()),
        checkpoint: StageCheckpoint {
            summary: "Work is ready".into(),
            decisions: vec!["Use a reducer".into()],
            open_issues: vec![],
        },
        current_stage_run_id: StageRunId(3),
        current_stage_sequence: 1,
        incoming_handoff_id: None,
        version: 4,
    }
}

fn completed_execution_state() -> WorkflowTaskState {
    let mut task = state(TaskPhase::Execution);
    for step in &mut task.plan.steps {
        step.status = PlanStepStatus::Completed;
    }
    task
}

fn done_state() -> WorkflowTaskState {
    state(TaskPhase::Done)
}

#[test]
fn only_declared_phase_transitions_have_targets() {
    let cases = [
        (
            TaskPhase::Planning,
            TransitionEvent::PlanningCompleted,
            TaskPhase::Execution,
        ),
        (
            TaskPhase::Execution,
            TransitionEvent::ExecutionCompleted,
            TaskPhase::Validation,
        ),
        (
            TaskPhase::Validation,
            TransitionEvent::ValidationPassed,
            TaskPhase::Done,
        ),
        (
            TaskPhase::Validation,
            TransitionEvent::ValidationFailed,
            TaskPhase::Execution,
        ),
    ];
    for (from, event, to) in cases {
        assert_eq!(
            deepseek_cli::workflow::target_phase(from, event).unwrap(),
            to
        );
    }
    for (from, event) in [
        (TaskPhase::Planning, TransitionEvent::ExecutionCompleted),
        (TaskPhase::Planning, TransitionEvent::ValidationPassed),
        (TaskPhase::Execution, TransitionEvent::ValidationPassed),
        (TaskPhase::Done, TransitionEvent::PlanningCompleted),
    ] {
        assert!(matches!(
            deepseek_cli::workflow::target_phase(from, event),
            Err(WorkflowError::IllegalTransition { .. })
        ));
    }
}

#[test]
fn transition_authorization_enforces_phase_specific_guards() {
    let planning = state(TaskPhase::Planning);
    let authorization =
        StateMachine::authorize(&planning, TransitionEvent::PlanningCompleted, &[]).unwrap();
    assert_eq!(authorization.to_phase, TaskPhase::Execution);
    assert_eq!(authorization.source_version, 4);

    let empty_plan = WorkflowTaskState {
        plan: TaskPlan {
            revision: 0,
            steps: vec![],
            acceptance_criteria: vec![],
        },
        ..planning.clone()
    };
    assert!(StateMachine::authorize(&empty_plan, TransitionEvent::PlanningCompleted, &[]).is_err());

    assert!(
        StateMachine::authorize(
            &state(TaskPhase::Execution),
            TransitionEvent::ExecutionCompleted,
            &[],
        )
        .is_err()
    );
    assert!(
        StateMachine::authorize(
            &completed_execution_state(),
            TransitionEvent::ExecutionCompleted,
            &[],
        )
        .is_ok()
    );

    let validation = state(TaskPhase::Validation);
    assert!(StateMachine::authorize(&validation, TransitionEvent::ValidationPassed, &[]).is_err());
    assert!(
        StateMachine::authorize(
            &validation,
            TransitionEvent::ValidationPassed,
            &["different evidence".into()],
        )
        .is_err()
    );
    assert!(
        StateMachine::authorize(
            &validation,
            TransitionEvent::ValidationPassed,
            &["focused tests pass".into()],
        )
        .is_ok()
    );
}

#[test]
fn source_status_and_dialog_guards_are_local_and_exhaustive() {
    let active = state(TaskPhase::Execution);
    let paused = active.clone().pause().unwrap();
    assert_eq!(paused.phase, active.phase);
    assert_eq!(paused.current_stage_run_id, active.current_stage_run_id);
    assert_eq!(paused.status, TaskStatus::Paused);
    assert!(
        StateMachine::validate_source(
            &WorkflowInputSource::Controller {
                checker: "c".into(),
                model: "m".into(),
                triggering_assistant_message_id: 9,
            },
            &WorkflowIntent::StartNewTask {
                goal: "other".into()
            },
        )
        .is_err()
    );
    assert!(StateMachine::validate_new_task(Some(&active)).is_err());
    assert!(StateMachine::validate_new_task(Some(&done_state())).is_ok());
    assert!(done_state().pause().is_err());
    for text in ["", "   ", "\n\t"] {
        assert!(WorkflowIntent::human_continue(text).is_err());
    }
}

#[test]
fn only_humans_can_authorize_a_replan_from_every_phase() {
    let done = done_state();
    let authorization = StateMachine::authorize_replan(
        &done,
        &WorkflowInputSource::Human,
        "Support the repaired deployment".into(),
    )
    .unwrap();
    assert_eq!(authorization.from_phase, TaskPhase::Done);
    assert_eq!(authorization.to_phase, TaskPhase::Planning);
    assert_eq!(authorization.next_plan_revision, 1);
    assert_eq!(
        authorization.change_request,
        "Support the repaired deployment"
    );
    assert!(
        StateMachine::authorize_replan(
            &done,
            &WorkflowInputSource::Controller {
                checker: "c".into(),
                model: "m".into(),
                triggering_assistant_message_id: 9,
            },
            "bypass the human".into(),
        )
        .is_err()
    );
}

#[test]
fn patches_are_versioned_monotonic_and_context_guarded() {
    let planning = state(TaskPhase::Planning);
    let patch = TaskStatePatch {
        expected_version: 4,
        plan_append: PlanAppend {
            steps: vec![PlanStep {
                id: "document".into(),
                description: "Document the state machine".into(),
                status: PlanStepStatus::Pending,
            }],
            acceptance_criteria: vec!["documentation exists".into()],
        },
        step_updates: vec![StepStatusUpdate {
            step_id: "implement".into(),
            status: PlanStepStatus::Completed,
            evidence: vec!["tests prove reducer behavior".into()],
        }],
        current_step_id: Some("document".into()),
        expected_action: Some("Write docs".into()),
        checkpoint: None,
    };
    let preview = planning
        .preview_patch(&patch, PatchContext::Normal)
        .unwrap();
    assert_eq!(preview.version, 4);
    assert_eq!(preview.plan.revision, 1);
    assert_eq!(preview.plan.steps.len(), 3);
    assert_eq!(preview.current_step_id.as_deref(), Some("document"));
    let applied = planning.apply_patch(&patch, PatchContext::Normal).unwrap();
    assert_eq!(applied.version, 5);
    assert_eq!(applied.plan.revision, 1);

    assert!(
        planning
            .preview_patch(
                &TaskStatePatch {
                    expected_version: 3,
                    ..patch.clone()
                },
                PatchContext::Normal,
            )
            .is_err()
    );
    assert!(
        state(TaskPhase::Execution)
            .preview_patch(&patch, PatchContext::Normal)
            .is_err()
    );
    assert!(
        state(TaskPhase::Validation)
            .preview_patch(&patch, PatchContext::ValidationRepair)
            .is_ok()
    );
    assert!(
        state(TaskPhase::Execution)
            .preview_patch(&patch, PatchContext::ValidationRepair)
            .is_err()
    );
    assert!(
        planning
            .preview_patch(
                &TaskStatePatch {
                    plan_append: PlanAppend::default(),
                    step_updates: vec![StepStatusUpdate {
                        step_id: "implement".into(),
                        status: PlanStepStatus::Completed,
                        evidence: vec![],
                    }],
                    current_step_id: None,
                    expected_action: None,
                    checkpoint: None,
                    expected_version: 4,
                },
                PatchContext::Normal,
            )
            .is_err()
    );
    assert!(
        applied
            .preview_patch(
                &TaskStatePatch {
                    plan_append: PlanAppend::default(),
                    step_updates: vec![StepStatusUpdate {
                        step_id: "implement".into(),
                        status: PlanStepStatus::InProgress,
                        evidence: vec![],
                    }],
                    current_step_id: None,
                    expected_action: None,
                    checkpoint: None,
                    expected_version: 5,
                },
                PatchContext::Normal,
            )
            .is_err()
    );
}

#[test]
fn rendering_is_deterministic_and_excludes_internal_metadata() {
    let rendered = render_task_state(&state(TaskPhase::Execution));
    assert_eq!(
        rendered,
        r#"{
  "workflow_task": 1,
  "phase": "execution",
  "status": "active",
  "goal": "Ship the workflow domain",
  "plan": {
    "revision": 0,
    "steps": [
      {
        "id": "implement",
        "description": "Implement the state machine",
        "status": "pending"
      },
      {
        "id": "validate",
        "description": "Run the focused test suite",
        "status": "pending"
      }
    ],
    "acceptance_criteria": [
      "focused tests pass"
    ]
  },
  "current_step_id": "implement",
  "expected_action": "Implement the reducer",
  "checkpoint": {
    "summary": "Work is ready",
    "decisions": [
      "Use a reducer"
    ],
    "open_issues": []
  }
}"#,
    );
    for forbidden in [
        "dialog_id",
        "incoming_handoff_id",
        "version",
        "current_stage_run_id",
    ] {
        assert!(!rendered.contains(forbidden));
    }
}
