use deepseek_cli::workflow::{GoalProposal, authorize_goal_approval, authorize_goal_reopen};
use deepseek_cli::workflow::{
    MAX_ACCEPTANCE_CRITERIA, MAX_ACCEPTANCE_CRITERION_CHARS, PatchContext, PlanAppend, PlanStep,
    PlanStepStatus, StageCheckpoint, StageRunId, StateMachine, StepStatusUpdate, TaskPhase,
    TaskPlan, TaskStatePatch, TaskStatus, TransitionEvent, WorkflowError, WorkflowInputSource,
    WorkflowIntent, WorkflowTaskId, WorkflowTaskState, render_task_state,
};

fn state(phase: TaskPhase) -> WorkflowTaskState {
    WorkflowTaskState {
        id: WorkflowTaskId(1),
        dialog_id: 2,
        ordinal: 1,
        phase,
        status: TaskStatus::Active,
        goal: "Ship the workflow domain".into(),
        goal_revision: 1,
        goal_proposal: None,
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

fn empty_patch(expected_version: u64) -> TaskStatePatch {
    TaskStatePatch {
        expected_version,
        plan_append: PlanAppend::default(),
        step_updates: vec![],
        current_step_id: None,
        expected_action: None,
        checkpoint: None,
    }
}

#[test]
fn goal_definition_cannot_skip_planning() {
    let task = WorkflowTaskState::new(
        WorkflowTaskId(1),
        2,
        1,
        "Build parser".into(),
        StageRunId(3),
    )
    .unwrap();
    assert_eq!(task.phase, TaskPhase::GoalDefinition);
    assert!(task.plan.steps.is_empty());
    for event in [
        TransitionEvent::PlanningCompleted,
        TransitionEvent::ExecutionCompleted,
        TransitionEvent::ValidationPassed,
        TransitionEvent::ValidationFailed,
    ] {
        assert!(StateMachine::authorize(&task, event, &[]).is_err());
    }
}

#[test]
fn phase_transition_matrix_is_exhaustive() {
    let phases = [
        TaskPhase::Planning,
        TaskPhase::Execution,
        TaskPhase::Validation,
        TaskPhase::Done,
    ];
    let events = [
        TransitionEvent::PlanningCompleted,
        TransitionEvent::ExecutionCompleted,
        TransitionEvent::ValidationPassed,
        TransitionEvent::ValidationFailed,
    ];
    for from in phases {
        for event in events {
            let expected = match (from, event) {
                (TaskPhase::Planning, TransitionEvent::PlanningCompleted) => {
                    Some(TaskPhase::Execution)
                }
                (TaskPhase::Execution, TransitionEvent::ExecutionCompleted) => {
                    Some(TaskPhase::Validation)
                }
                (TaskPhase::Validation, TransitionEvent::ValidationPassed) => Some(TaskPhase::Done),
                (TaskPhase::Validation, TransitionEvent::ValidationFailed) => {
                    Some(TaskPhase::Execution)
                }
                _ => None,
            };
            match expected {
                Some(to) => assert_eq!(deepseek_cli::workflow::target_phase(from, event), Ok(to)),
                None => assert!(matches!(
                    deepseek_cli::workflow::target_phase(from, event),
                    Err(WorkflowError::IllegalTransition { .. })
                )),
            }
        }
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
        current_step_id: None,
        ..planning.clone()
    };
    assert_eq!(
        StateMachine::authorize(&empty_plan, TransitionEvent::PlanningCompleted, &[]),
        Err(WorkflowError::PlanningRequirementsIncomplete)
    );

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
            &["focused tests pass => observed".into()],
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
fn only_humans_can_authorize_a_replan_from_approved_unfinished_phases() {
    for phase in [
        TaskPhase::Planning,
        TaskPhase::Execution,
        TaskPhase::Validation,
    ] {
        let task = state(phase);
        let authorization = StateMachine::authorize_replan(
            &task,
            &WorkflowInputSource::Human,
            "Support the repaired deployment".into(),
        )
        .unwrap();
        assert_eq!(authorization.from_phase, phase);
        assert_eq!(authorization.to_phase, TaskPhase::Planning);
        assert_eq!(authorization.source_version, task.version);
        assert_eq!(authorization.next_plan_revision, task.plan.revision + 1);
        assert_eq!(
            authorization.change_request,
            "Support the repaired deployment"
        );
    }
    assert!(
        StateMachine::authorize_replan(
            &state(TaskPhase::Done),
            &WorkflowInputSource::Human,
            "again".into()
        )
        .is_err()
    );
    let done = done_state();
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
fn only_human_can_approve_current_visible_goal_proposal() {
    let mut task =
        WorkflowTaskState::new(WorkflowTaskId(1), 2, 1, "draft".into(), StageRunId(3)).unwrap();
    assert!(authorize_goal_approval(&task, &WorkflowInputSource::Human).is_err());
    task.goal_proposal = Some(GoalProposal {
        text: "Build parser".into(),
        assistant_message_id: 7,
        stage_run_id: StageRunId(3),
    });
    let authorization = authorize_goal_approval(&task, &WorkflowInputSource::Human).unwrap();
    assert_eq!(authorization.goal_text, "Build parser");
    assert_eq!(authorization.assistant_message_id, 7);
    assert_eq!(authorization.source_stage_run_id, StageRunId(3));
    assert!(
        authorize_goal_approval(
            &task,
            &WorkflowInputSource::Controller {
                checker: "c".into(),
                model: "m".into(),
                triggering_assistant_message_id: 7
            }
        )
        .is_err()
    );
    task.current_stage_run_id = StageRunId(4);
    assert!(authorize_goal_approval(&task, &WorkflowInputSource::Human).is_err());
}

#[test]
fn reopening_goal_requires_human_and_unfinished_approved_phase() {
    for phase in [
        TaskPhase::Planning,
        TaskPhase::Execution,
        TaskPhase::Validation,
    ] {
        let task = state(phase);
        let authorization =
            authorize_goal_reopen(&task, &WorkflowInputSource::Human, "Change goal".into())
                .unwrap();
        assert_eq!(authorization.source_stage_run_id, task.current_stage_run_id);
        assert_eq!(authorization.next_plan_revision, task.plan.revision + 1);
    }
    assert!(
        authorize_goal_reopen(
            &state(TaskPhase::Done),
            &WorkflowInputSource::Human,
            "Change goal".into()
        )
        .is_err()
    );
}

#[test]
fn pause_then_resume_preserves_every_other_state_field() {
    let active = state(TaskPhase::Validation);
    assert_eq!(active.clone().pause().unwrap().resume().unwrap(), active);
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
fn patch_rejects_completed_appends_and_duplicate_or_unknown_step_references() {
    let planning = state(TaskPhase::Planning);
    let completed_append = TaskStatePatch {
        plan_append: PlanAppend {
            steps: vec![PlanStep {
                id: "evidence-bypass".into(),
                description: "Must not begin complete".into(),
                status: PlanStepStatus::Completed,
            }],
            acceptance_criteria: vec![],
        },
        ..empty_patch(planning.version)
    };
    assert!(matches!(
        planning.preview_patch(&completed_append, PatchContext::Normal),
        Err(WorkflowError::AppendedStepMustStartPending(id)) if id == "evidence-bypass"
    ));

    let duplicate_updates = TaskStatePatch {
        step_updates: vec![
            StepStatusUpdate {
                step_id: "implement".into(),
                status: PlanStepStatus::InProgress,
                evidence: vec![],
            },
            StepStatusUpdate {
                step_id: "implement".into(),
                status: PlanStepStatus::Blocked,
                evidence: vec![],
            },
        ],
        ..empty_patch(planning.version)
    };
    assert_eq!(
        planning.preview_patch(&duplicate_updates, PatchContext::Normal),
        Err(WorkflowError::DuplicateStepUpdate("implement".into()))
    );

    let unknown_update = TaskStatePatch {
        step_updates: vec![StepStatusUpdate {
            step_id: "missing".into(),
            status: PlanStepStatus::InProgress,
            evidence: vec![],
        }],
        ..empty_patch(planning.version)
    };
    assert_eq!(
        planning.preview_patch(&unknown_update, PatchContext::Normal),
        Err(WorkflowError::UnknownStepId("missing".into()))
    );

    let trimmed_duplicate_criterion = TaskStatePatch {
        plan_append: PlanAppend {
            steps: vec![],
            acceptance_criteria: vec![" focused tests pass ".into()],
        },
        ..empty_patch(planning.version)
    };
    assert_eq!(
        planning.preview_patch(&trimmed_duplicate_criterion, PatchContext::Normal),
        Err(WorkflowError::DuplicateAcceptanceCriterion(
            " focused tests pass ".into()
        ))
    );
}

#[test]
fn validation_evidence_covers_the_maximum_criteria_without_ambiguity() {
    let mut validation = state(TaskPhase::Validation);
    let criteria: Vec<_> = (0..MAX_ACCEPTANCE_CRITERIA)
        .map(|index| {
            format!(
                "{index:02}-{}",
                "x".repeat(MAX_ACCEPTANCE_CRITERION_CHARS - 3)
            )
        })
        .collect();
    validation.plan.acceptance_criteria = criteria.clone();
    let evidence: Vec<_> = criteria
        .iter()
        .map(|criterion| format!("  {criterion} => observed result  "))
        .collect();
    assert!(
        StateMachine::authorize(&validation, TransitionEvent::ValidationPassed, &evidence,).is_ok()
    );

    validation.plan.acceptance_criteria = (0..(MAX_ACCEPTANCE_CRITERIA + 1))
        .map(|index| format!("criterion-{index}"))
        .collect();
    assert!(matches!(
        validation.validate(),
        Err(WorkflowError::TooManyItems {
            field: "acceptance criteria",
            ..
        })
    ));

    validation.plan.acceptance_criteria = vec!["x".repeat(MAX_ACCEPTANCE_CRITERION_CHARS + 1)];
    assert!(matches!(
        validation.validate(),
        Err(WorkflowError::StringTooLong {
            field: "acceptance criterion",
            ..
        })
    ));
}

#[test]
fn validation_pass_rejects_missing_duplicate_malformed_and_blank_result_coverage() {
    let validation = state(TaskPhase::Validation);
    let cases = [
        (vec![], WorkflowError::ValidationEvidenceRequired),
        (
            vec![
                "focused tests pass => observed".into(),
                "focused tests pass => again".into(),
            ],
            WorkflowError::DuplicateCriterionCoverage("focused tests pass".into()),
        ),
        (
            vec!["focused tests pass observed".into()],
            WorkflowError::MalformedValidationEvidence("focused tests pass observed".into()),
        ),
        (
            vec!["focused tests pass =>   ".into()],
            WorkflowError::MalformedValidationEvidence("focused tests pass =>".into()),
        ),
    ];
    for (evidence, expected) in cases {
        assert_eq!(
            StateMachine::authorize(&validation, TransitionEvent::ValidationPassed, &evidence),
            Err(expected)
        );
    }

    let mut two_criteria = validation;
    two_criteria
        .plan
        .acceptance_criteria
        .push("second criterion".into());
    assert_eq!(
        StateMachine::authorize(
            &two_criteria,
            TransitionEvent::ValidationPassed,
            &["focused tests pass => observed".into()],
        ),
        Err(WorkflowError::MissingCriterionCoverage(
            "second criterion".into()
        ))
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
