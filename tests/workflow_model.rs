use deepseek_cli::chat::{Message, Role};
use deepseek_cli::client::{DeepSeekClient, TokenUsage};
use deepseek_cli::config::Config;
use deepseek_cli::workflow_model::{
    CheckContext, CheckerMode, ContinuationChecker, HandoffBuilder, HumanInputInterpreter,
    ResponseChecker, parse_handoff, project_handoff,
};
use deepseek_cli::workflow_model::{
    CompletionModel, DeepSeekCompletionModel, ModelError, ModelFuture, ModelRequest, ModelResponse,
};
use std::sync::{Arc, Mutex};

fn handoff_json() -> serde_json::Value {
    serde_json::json!({"summary":"Ready for next stage", "completed_step_ids":[], "next_step_id":null,
      "expected_action":"Check search", "plan_changes":[], "decisions":[], "open_issues":[]})
}

fn authorized(state: &WorkflowTaskState, event: TransitionEvent) -> StageChangeAuthorization {
    StageChangeAuthorization::Transition(
        StateMachine::authorize(state, event, &["search works => tested".into()]).unwrap(),
    )
}

#[test]
fn handoff_projects_only_authorized_phase_and_existing_work() {
    let mut state = task(TaskPhase::Execution);
    state.plan.steps[0].status = PlanStepStatus::Completed;
    let auth = authorized(&state, TransitionEvent::ExecutionCompleted);
    let mut json = handoff_json();
    json["completed_step_ids"] = serde_json::json!(["implement"]);
    let payload = parse_handoff(&json.to_string(), &state, &auth).unwrap();
    let projected = project_handoff(&state, &auth, &payload).unwrap();
    assert_eq!(projected.phase, TaskPhase::Validation);
    assert_eq!(projected.current_step_id, None);
    assert_eq!(projected.version, 0);
    assert_eq!(projected.current_stage_run_id, StageRunId(1));
    assert_eq!(projected.plan, state.plan);
    assert_eq!(projected.checkpoint.summary, "Ready for next stage");
    assert_eq!(state.phase, TaskPhase::Execution);
}

#[test]
fn handoff_rejects_unearned_completion_unknown_and_duplicate_ids_and_control_fields() {
    let state = task(TaskPhase::Planning);
    let auth = authorized(&state, TransitionEvent::PlanningCompleted);
    for (field, value) in [
        ("completed_step_ids", serde_json::json!(["implement"])),
        ("completed_step_ids", serde_json::json!(["unknown"])),
        ("next_step_id", serde_json::json!("unknown")),
        (
            "plan_changes",
            serde_json::json!([{"id":"repair","description":"repair","status":"pending"}]),
        ),
        ("to_phase", serde_json::json!("done")),
        ("from_phase", serde_json::json!("execution")),
        ("event", serde_json::json!("validation_passed")),
        ("version", serde_json::json!(99)),
        ("summary", serde_json::json!(" ")),
        ("decisions", serde_json::json!([" "])),
        ("open_issues", serde_json::json!(vec!["issue"; 129])),
    ] {
        let mut body = handoff_json();
        body[field] = value;
        assert!(
            parse_handoff(&body.to_string(), &state, &auth).is_err(),
            "accepted {body}"
        );
    }
    let mut completed = state.clone();
    completed.plan.steps[0].status = PlanStepStatus::Completed;
    let auth = authorized(&completed, TransitionEvent::PlanningCompleted);
    for (field, value) in [
        (
            "completed_step_ids",
            serde_json::json!(["implement", "implement"]),
        ),
        ("next_step_id", serde_json::json!("implement")),
    ] {
        let mut body = handoff_json();
        body[field] = value;
        assert!(parse_handoff(&body.to_string(), &completed, &auth).is_err());
    }
}

#[test]
fn handoff_repair_is_pending_unique_bounded_and_only_for_validation_failure() {
    let mut state = task(TaskPhase::Validation);
    state.plan.steps[0].status = PlanStepStatus::Completed;
    let auth = authorized(&state, TransitionEvent::ValidationFailed);
    let repair = serde_json::json!({"id":"repair","description":"Fix search","status":"pending"});
    let mut body = handoff_json();
    body["plan_changes"] = serde_json::json!([repair]);
    body["next_step_id"] = serde_json::json!("repair");
    let payload = parse_handoff(&body.to_string(), &state, &auth).unwrap();
    let projected = project_handoff(&state, &auth, &payload).unwrap();
    assert_eq!(projected.phase, TaskPhase::Execution);
    assert_eq!(projected.current_step_id.as_deref(), Some("repair"));
    assert_eq!(projected.plan.revision, 1);
    assert_eq!(projected.plan.steps[0].status, PlanStepStatus::Completed);
    assert_eq!(projected.plan.steps.len(), 2);
    for changes in [
        serde_json::json!([repair, repair]),
        serde_json::json!([{"id":"implement","description":"x","status":"pending"}]),
        serde_json::json!([{"id":"repair","description":"x","status":"completed"}]),
    ] {
        body["plan_changes"] = changes;
        assert!(parse_handoff(&body.to_string(), &state, &auth).is_err());
    }
}

#[test]
fn handoff_replan_preserves_goal_plan_and_human_request() {
    let mut state = task(TaskPhase::Execution);
    state.plan.revision = 4;
    let auth = StageChangeAuthorization::Replan(
        StateMachine::authorize_replan(
            &state,
            &WorkflowInputSource::Human,
            "Support offline mode".into(),
        )
        .unwrap(),
    );
    let payload = parse_handoff(&handoff_json().to_string(), &state, &auth).unwrap();
    let projected = project_handoff(&state, &auth, &payload).unwrap();
    assert_eq!(projected.phase, TaskPhase::Planning);
    assert_eq!(projected.goal, "build search");
    assert_eq!(projected.plan.steps, state.plan.steps);
    assert_eq!(
        projected.plan.acceptance_criteria,
        state.plan.acceptance_criteria
    );
    assert_eq!(projected.plan.revision, 5);
    assert!(
        projected
            .checkpoint
            .open_issues
            .iter()
            .any(|s| s.contains("Support offline mode"))
    );
    assert!(
        projected
            .expected_action
            .unwrap()
            .to_lowercase()
            .contains("plan")
    );
}

#[test]
fn replan_accepts_maximum_human_request_and_resumes_a_paused_task() {
    let mut state = task(TaskPhase::Execution);
    state.status = TaskStatus::Paused;
    let request = "я".repeat(8192);
    let auth = StageChangeAuthorization::Replan(
        StateMachine::authorize_replan(&state, &WorkflowInputSource::Human, request.clone())
            .unwrap(),
    );
    let payload = parse_handoff(&handoff_json().to_string(), &state, &auth).unwrap();
    let projected = project_handoff(&state, &auth, &payload).unwrap();
    assert_eq!(projected.status, TaskStatus::Active);
    assert!(projected.checkpoint.open_issues.contains(&request));
    assert!(projected.expected_action.unwrap().chars().count() <= 8192);
}

#[test]
fn checker_handles_patch_before_transition_and_allows_repairs_only_on_failed_validation() {
    let mut body = check_json();
    body["patch"]["step_updates"] = serde_json::json!([{"step_id":"implement","status":"completed","evidence":["implemented and tested"]}]);
    body["decision"] = serde_json::json!({"type":"emit_transition","event":"execution_completed","evidence":[],"confidence":1});
    assert!(parse_continuation_check(&body.to_string(), &task(TaskPhase::Execution)).is_ok());
    let mut body = check_json();
    body["patch"]["plan_append"]["steps"] =
        serde_json::json!([{"id":"repair","description":"fix","status":"pending"}]);
    assert!(parse_continuation_check(&body.to_string(), &task(TaskPhase::Validation)).is_err());
    body["decision"] = serde_json::json!({"type":"emit_transition","event":"validation_failed","evidence":["search fails"],"confidence":1});
    assert!(parse_continuation_check(&body.to_string(), &task(TaskPhase::Validation)).is_ok());
}

#[test]
fn checker_rejects_unknown_nested_fields_statuses_oversize_and_fenced_output() {
    let state = task(TaskPhase::Planning);
    let mut body = check_json();
    body["patch"]["plan_append"]["steps"] =
        serde_json::json!([{"id":"s","description":"x","status":"pending","extra":1}]);
    assert!(parse_continuation_check(&body.to_string(), &state).is_err());
    body["patch"]["plan_append"]["steps"] =
        serde_json::json!([{"id":"s","description":"x","status":"finished"}]);
    assert!(parse_continuation_check(&body.to_string(), &state).is_err());
    let valid = check_json().to_string();
    for raw in [
        format!("```json\n{valid}\n```"),
        format!("{valid} {valid}"),
        format!("{valid}{}", " ".repeat(65537 - valid.len())),
    ] {
        assert!(parse_continuation_check(&raw, &state).is_err());
    }
}

#[test]
fn handoff_checks_byte_limit_and_stale_authorization_even_for_direct_projection() {
    let state = task(TaskPhase::Planning);
    let auth = authorized(&state, TransitionEvent::PlanningCompleted);
    let valid = handoff_json().to_string();
    assert!(
        parse_handoff(
            &format!("{valid}{}", " ".repeat(65536 - valid.len())),
            &state,
            &auth
        )
        .is_ok()
    );
    assert!(
        parse_handoff(
            &format!("{valid}{}", " ".repeat(65537 - valid.len())),
            &state,
            &auth
        )
        .is_err()
    );
    assert!(parse_handoff(&format!("```json\n{valid}\n```"), &state, &auth).is_err());
    let mut payload = parse_handoff(&valid, &state, &auth).unwrap();
    payload.summary = "🦀".repeat(8193);
    assert!(project_handoff(&state, &auth, &payload).is_err());
    let payload = parse_handoff(&valid, &state, &auth).unwrap();
    let mut stale = state.clone();
    stale.version = 1;
    assert!(project_handoff(&stale, &auth, &payload).is_err());
}

struct RecordingModel {
    content: Option<String>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl CompletionModel for RecordingModel {
    fn name(&self) -> &str {
        "recording"
    }
    fn complete(&self, request: ModelRequest) -> ModelFuture<'_> {
        self.requests.lock().unwrap().push(request);
        Box::pin(async move {
            match &self.content {
                Some(content) => Ok(ModelResponse {
                    content: content.clone(),
                    usage: Some(TokenUsage {
                        total_tokens: 17,
                        ..TokenUsage::default()
                    }),
                }),
                None => Err(ModelError::BlankModelName),
            }
        })
    }
}

fn recording(content: Option<String>) -> Arc<RecordingModel> {
    Arc::new(RecordingModel {
        content,
        requests: Mutex::new(Vec::new()),
    })
}
fn workflow_config() -> deepseek_cli::config::WorkflowConfig {
    Config::from_toml("api_key='key'\n[context]\nstrategy='summary'", None)
        .unwrap()
        .workflow()
        .clone()
}
fn input() -> WorkflowInput {
    WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::Continue {
            instruction: "human trigger".into(),
        },
    }
}

#[tokio::test]
async fn interpreter_falls_back_on_api_malformed_and_low_confidence_and_keeps_usage() {
    for content in [None,Some("bad json".into()),Some(r#"{"confidence":0.1,"intent":{"type":"replan_current","change_request":"unsafe inferred change"}}"#.into())] {
        let model=recording(content.clone());let interpreter=HumanInputInterpreter::new(model.clone(),&workflow_config());
        let result=interpreter.interpret("raw human",Some(&task(TaskPhase::Execution))).await.unwrap();
        assert!(matches!(result,HumanInterpretation::Managed{intent:WorkflowIntent::Continue{instruction},usage,..} if instruction=="raw human" && usage.map(|u|u.total_tokens)==content.as_ref().map(|_|17)));
        let requests=model.requests.lock().unwrap();let prompt:serde_json::Value=serde_json::from_str(requests[0].messages[1].content()).unwrap();
        assert_eq!(prompt["human_text"],"raw human");assert_eq!(prompt["current_state"]["phase"],"execution");
        assert!(prompt.get("stage_messages").is_none());
    }
}

#[tokio::test]
async fn interpreter_instructs_model_to_refine_unapproved_goal_instead_of_starting_task() {
    let model = recording(None);
    let interpreter = HumanInputInterpreter::new(model.clone(), &workflow_config());
    interpreter
        .interpret(
            "Хочу сделать echo сервер на rust",
            Some(&task(TaskPhase::GoalDefinition)),
        )
        .await
        .unwrap();

    let requests = model.requests.lock().unwrap();
    let policy = requests[0].messages[0].content();
    assert!(
        policy.contains("A new desired outcome in goal_definition is continue, not start_new_task"),
        "interpreter policy does not distinguish a draft goal from a separate task: {policy}"
    );
}

// Break caught: the real adapter rejects whitespace, but its billed usage must survive checker errors and human fallback.
#[tokio::test]
async fn real_adapter_blank_completion_retains_checker_and_interpreter_usage() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    let chunk = serde_json::json!({"choices":[{"delta":{"content":" \n\t "},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":12,"completion_tokens":5,"total_tokens":17}});
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n")),
        )
        .mount(&server)
        .await;
    let config = Config::from_toml(
        &format!(
            "api_key='key'\nbase_url='{}'\n[context]\nstrategy='summary'",
            server.uri()
        ),
        None,
    )
    .unwrap();
    let model = Arc::new(
        DeepSeekCompletionModel::new(DeepSeekClient::new(&config).unwrap(), "real-service".into())
            .unwrap(),
    );
    let state = task(TaskPhase::Execution);
    let context = CheckContext {
        task: state.clone(),
        stage_messages: vec![],
        triggering_input: input(),
    };
    let checker = ContinuationChecker::new(model.clone(), config.workflow());
    let error = checker
        .check(&context, "complete answer")
        .await
        .unwrap_err();
    assert_eq!(error.usage().map(|usage| usage.total_tokens), Some(17));
    let interpreter = HumanInputInterpreter::new(model, config.workflow());
    assert!(
        matches!(interpreter.interpret("raw human", Some(&state)).await.unwrap(),
        HumanInterpretation::Managed { intent: WorkflowIntent::Continue { instruction }, usage: Some(usage), confidence }
        if instruction == "raw human" && confidence == 0.0 && usage.total_tokens == 17)
    );
    assert_eq!(context.task, state);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn interpreter_uses_high_confidence_intent_and_validates_human_before_calling() {
    let model = recording(Some(
        r#"{"confidence":0.99,"intent":{"type":"replan_current","change_request":"offline"}}"#
            .into(),
    ));
    let interpreter = HumanInputInterpreter::new(model.clone(), &workflow_config());
    assert!(interpreter.interpret("  ", None).await.is_err());
    assert!(model.requests.lock().unwrap().is_empty());
    assert!(
        matches!(interpreter.interpret("offline",Some(&task(TaskPhase::Execution))).await.unwrap(),HumanInterpretation::Managed{intent:WorkflowIntent::ReplanCurrent{change_request},..} if change_request=="offline")
    );
}

#[tokio::test]
async fn advisory_checker_sends_complete_answer_and_returns_validated_patch_and_usage() {
    let model = recording(Some(check_json().to_string()));
    let checker: Box<dyn ResponseChecker> =
        Box::new(ContinuationChecker::new(model.clone(), &workflow_config()));
    assert_eq!(checker.mode(), CheckerMode::Advisory);
    let context = CheckContext {
        task: task(TaskPhase::Planning),
        stage_messages: vec![Message::for_request(Role::User, "stage marker")],
        triggering_input: input(),
    };
    let answer = "я".repeat(9000);
    let result = checker.check(&context, &answer).await.unwrap();
    assert!(matches!(result.decision, ControllerDecision::AwaitUser));
    assert_eq!(result.usage.unwrap().total_tokens, 17);
    let requests = model.requests.lock().unwrap();
    let prompt: serde_json::Value =
        serde_json::from_str(requests[0].messages[1].content()).unwrap();
    assert_eq!(prompt["assistant_response"], answer);
    assert_eq!(prompt["current_version"], 0);
    assert_eq!(prompt["stage_messages"][0]["content"], "stage marker");
    assert_eq!(
        prompt["triggering_input"],
        serde_json::to_value(input()).unwrap()
    );
}

#[tokio::test]
async fn checker_rejects_invalid_output_without_effects_and_handoff_builder_keeps_usage() {
    let mut body = check_json();
    body["patch"]["expected_version"] = serde_json::json!(9);
    let checker = ContinuationChecker::new(recording(Some(body.to_string())), &workflow_config());
    let context = CheckContext {
        task: task(TaskPhase::Planning),
        stage_messages: vec![],
        triggering_input: input(),
    };
    assert!(checker.check(&context, "answer").await.is_err());
    assert_eq!(context.task.version, 0);
    let model = recording(Some(handoff_json().to_string()));
    let builder = HandoffBuilder::new(model.clone(), &workflow_config());
    let auth = authorized(&context.task, TransitionEvent::PlanningCompleted);
    let result = builder
        .build(&auth, &context.task, &[], &input())
        .await
        .unwrap();
    assert_eq!(result.usage.unwrap().total_tokens, 17);
    assert_eq!(result.payload.summary, "Ready for next stage");
    let requests = model.requests.lock().unwrap();
    assert_eq!(
        requests[0].max_tokens,
        workflow_config().handoff_max_tokens()
    );
}
use deepseek_cli::workflow::*;
use deepseek_cli::workflow_model::{
    ControllerDecision, HumanInterpretation, human_fallback, parse_continuation_check,
    parse_human_interpretation,
};

fn task(phase: TaskPhase) -> WorkflowTaskState {
    let mut task = WorkflowTaskState::new(
        WorkflowTaskId(1),
        1,
        1,
        "build search".into(),
        StageRunId(1),
    )
    .unwrap();
    task.phase = phase;
    if phase == TaskPhase::GoalDefinition {
        return task;
    }
    task.goal_revision = 1;
    task.plan.steps = vec![PlanStep {
        id: "implement".into(),
        description: "Build search".into(),
        status: PlanStepStatus::Pending,
    }];
    task.plan.acceptance_criteria = vec!["search works".into()];
    task
}

#[test]
fn parser_accepts_typed_goal_decisions() {
    let approved =
        parse_human_interpretation(r#"{"confidence":0.99,"intent":{"type":"approve_goal"}}"#)
            .unwrap();
    assert!(matches!(
        approved,
        HumanInterpretation::Managed {
            intent: WorkflowIntent::ApproveGoal,
            ..
        }
    ));
    let reopened = parse_human_interpretation(
        r#"{"confidence":0.99,"intent":{"type":"reopen_goal","change_request":"Add offline mode"}}"#,
    ).unwrap();
    assert!(
        matches!(reopened, HumanInterpretation::Managed { intent: WorkflowIntent::ReopenGoal { change_request }, .. } if change_request == "Add offline mode")
    );
}

#[tokio::test]
async fn approval_without_current_proposal_falls_back_to_discussion() {
    let model = recording(Some(
        r#"{"confidence":0.99,"intent":{"type":"approve_goal"}}"#.into(),
    ));
    let interpreter = HumanInputInterpreter::new(model, &workflow_config());
    let result = interpreter
        .interpret("да", Some(&task(TaskPhase::GoalDefinition)))
        .await
        .unwrap();
    assert!(
        matches!(result, HumanInterpretation::Managed { intent: WorkflowIntent::Continue { instruction }, .. } if instruction == "да")
    );
}

#[tokio::test]
async fn conditional_yes_with_active_proposal_remains_discussion() {
    let mut current = task(TaskPhase::GoalDefinition);
    current.goal_proposal = Some(GoalProposal {
        text: "Сделать CLI с авторизацией".into(),
        assistant_message_id: 11,
        stage_run_id: current.current_stage_run_id,
    });
    let model = recording(Some(
        r#"{"confidence":0.99,"intent":{"type":"continue","instruction":"Убрать авторизацию из цели"}}"#.into(),
    ));
    let interpreter = HumanInputInterpreter::new(model, &workflow_config());
    let result = interpreter
        .interpret("да, но без авторизации", Some(&current))
        .await
        .unwrap();
    assert!(matches!(
        result,
        HumanInterpretation::Managed {
            intent: WorkflowIntent::Continue { .. },
            ..
        }
    ));
}

fn check_json() -> serde_json::Value {
    serde_json::json!({
        "patch": { "expected_version": 0, "plan_append": {"steps": [], "acceptance_criteria": []},
          "step_updates": [], "current_step_id": null, "expected_action": null, "checkpoint": null },
        "decision": {"type": "await_user"}
    })
}

#[test]
fn interpreter_accepts_exactly_the_four_intents() {
    for (intent, kind) in [
        (
            serde_json::json!({"type":"continue","instruction":"keep implementing"}),
            "continue",
        ),
        (
            serde_json::json!({"type":"start_new_task","goal":"build search"}),
            "start_new_task",
        ),
        (
            serde_json::json!({"type":"replan_current","change_request":"offline mode"}),
            "replan_current",
        ),
        (
            serde_json::json!({"type":"propose_transition","event":"execution_completed","evidence":["tests pass"]}),
            "propose_transition",
        ),
    ] {
        let parsed = parse_human_interpretation(
            &serde_json::json!({"confidence":0.95,"intent":intent}).to_string(),
        )
        .unwrap();
        let HumanInterpretation::Managed { intent, .. } = parsed else {
            panic!("unmanaged parsed output")
        };
        assert_eq!(intent.kind(), kind);
    }
}

#[test]
fn human_parser_rejects_non_strict_json_and_invalid_fields() {
    for json in [
        "```json\n{}\n```",
        "{} {}",
        r#"{"confidence":0.9,"extra":1,"intent":{"type":"continue","instruction":"x"}}"#,
        r#"{"confidence":0.9,"intent":{"type":"continue","instruction":"x","extra":1}}"#,
        r#"{"confidence":0.9,"intent":{"type":"continue","instruction":"   "}}"#,
        r#"{"confidence":0.9,"intent":{"type":"pause"}}"#,
        r#"{"confidence":0.9,"intent":{"type":"propose_transition","event":"finished","evidence":[]}}"#,
        r#"{"confidence":1.01,"intent":{"type":"continue","instruction":"x"}}"#,
        r#"{"confidence":-0.1,"intent":{"type":"continue","instruction":"x"}}"#,
        r#"{"confidence":null,"intent":{"type":"continue","instruction":"x"}}"#,
        r#"{"confidence":0.1,"confidence":0.9,"intent":{"type":"continue","instruction":"x"}}"#,
    ] {
        assert!(parse_human_interpretation(json).is_err(), "accepted {json}");
    }
}

#[test]
fn human_parser_counts_unicode_scalars_and_enforces_evidence_bounds() {
    for (count, valid) in [(8192, true), (8193, false)] {
        let json = serde_json::json!({"confidence":1.0,"intent":{"type":"continue","instruction":"🦀".repeat(count)}});
        assert_eq!(parse_human_interpretation(&json.to_string()).is_ok(), valid);
    }
    for (items, valid) in [
        (vec!["x".to_owned(); 32], true),
        (vec!["x".to_owned(); 33], false),
        (vec!["я".repeat(2048)], true),
        (vec!["я".repeat(2049)], false),
        (vec![" ".into()], false),
    ] {
        let json = serde_json::json!({"confidence":0.9,"intent":{"type":"propose_transition","event":"execution_completed","evidence":items}});
        assert_eq!(parse_human_interpretation(&json.to_string()).is_ok(), valid);
    }
    let valid = r#"{"confidence":1,"intent":{"type":"continue","instruction":"x"}}"#;
    assert!(
        parse_human_interpretation(&format!("{}{}", valid, " ".repeat(65536 - valid.len())))
            .is_ok()
    );
    assert!(
        parse_human_interpretation(&format!("{}{}", valid, " ".repeat(65537 - valid.len())))
            .is_err()
    );
}

#[test]
fn fallback_preserves_human_text_without_inventing_new_task_on_completed_state() {
    assert!(human_fallback(" \n ", None).is_err());
    assert!(
        matches!(human_fallback(" goal ",None).unwrap(), HumanInterpretation::Managed { intent: WorkflowIntent::StartNewTask { goal }, .. } if goal == "goal")
    );
    assert!(
        matches!(human_fallback(" new task ",Some(&task(TaskPhase::Execution))).unwrap(), HumanInterpretation::Managed { intent: WorkflowIntent::Continue { instruction }, .. } if instruction == "new task")
    );
    assert!(matches!(
        human_fallback("new task", Some(&task(TaskPhase::Done))).unwrap(),
        HumanInterpretation::Unmanaged { .. }
    ));
    assert!(human_fallback(&"a".repeat(8193), None).is_err());
}

#[test]
fn checker_validates_patch_against_domain_and_rejects_unknown_fields() {
    let state = task(TaskPhase::Planning);
    let valid = check_json();
    assert!(matches!(
        parse_continuation_check(&valid.to_string(), &state)
            .unwrap()
            .decision,
        ControllerDecision::AwaitUser
    ));
    for (pointer, value) in [
        ("/patch/expected_version", serde_json::json!(1)),
        ("/patch/current_step_id", serde_json::json!("missing")),
        (
            "/patch/step_updates",
            serde_json::json!([{"step_id":"implement","status":"completed","evidence":[]}]),
        ),
        (
            "/patch/step_updates",
            serde_json::json!([{"step_id":"implement","status":"blocked","evidence":[]},{"step_id":"implement","status":"pending","evidence":[]}]),
        ),
        (
            "/decision",
            serde_json::json!({"type":"start_new_task","goal":"other"}),
        ),
        (
            "/decision",
            serde_json::json!({"type":"continue","instruction":" ","confidence":1}),
        ),
        (
            "/decision",
            serde_json::json!({"type":"continue","instruction":"go","confidence":2}),
        ),
    ] {
        let mut body = valid.clone();
        *body.pointer_mut(pointer).unwrap() = value;
        assert!(
            parse_continuation_check(&body.to_string(), &state).is_err(),
            "accepted {body}"
        );
    }
    for pointer in ["", "/patch", "/patch/plan_append", "/decision"] {
        let mut body = valid.clone();
        body.pointer_mut(pointer).unwrap()["unexpected"] = serde_json::json!(1);
        assert!(parse_continuation_check(&body.to_string(), &state).is_err());
    }
}

#[test]
fn checker_enforces_resulting_plan_and_criterion_limits() {
    let state = task(TaskPhase::Planning);
    for (field, value, valid) in [
        ("steps",serde_json::json!((0..255).map(|i|serde_json::json!({"id":format!("s{i}"),"description":"x","status":"pending"})).collect::<Vec<_>>()),true),
        ("steps",serde_json::json!((0..256).map(|i|serde_json::json!({"id":format!("s{i}"),"description":"x","status":"pending"})).collect::<Vec<_>>()),false),
        ("steps",serde_json::json!([{"id":"implement","description":"x","status":"pending"}]),false),
        ("acceptance_criteria",serde_json::json!((0..31).map(|i|format!("criterion {i}")).collect::<Vec<_>>()),true),
        ("acceptance_criteria",serde_json::json!((0..32).map(|i|format!("criterion {i}")).collect::<Vec<_>>()),false),
        ("acceptance_criteria",serde_json::json!(["я".repeat(1024)]),true),
        ("acceptance_criteria",serde_json::json!(["я".repeat(1025)]),false),
    ] {
        let mut body=check_json();body["patch"]["plan_append"][field]=value;
        assert_eq!(parse_continuation_check(&body.to_string(),&state).is_ok(),valid);
    }
}

#[test]
fn validation_passed_evidence_is_checked_against_each_criterion() {
    let state = task(TaskPhase::Validation);
    for (evidence, valid) in [
        (vec!["search works => integration test passed"], true),
        (vec!["tests pass"], false),
        (vec!["search works => "], false),
        (vec!["search works => yes", "search works => twice"], false),
    ] {
        let mut body = check_json();
        body["decision"] = serde_json::json!({"type":"emit_transition","event":"validation_passed","confidence":1,"evidence":evidence});
        assert_eq!(
            parse_continuation_check(&body.to_string(), &state).is_ok(),
            valid
        );
    }
}

struct FakeCompletionModel {
    name: String,
    content: String,
    usage: TokenUsage,
}

impl FakeCompletionModel {
    fn one(name: &str, content: &str, total_tokens: u64) -> Self {
        Self {
            name: name.to_owned(),
            content: content.to_owned(),
            usage: TokenUsage {
                total_tokens,
                ..TokenUsage::default()
            },
        }
    }
}

impl CompletionModel for FakeCompletionModel {
    fn name(&self) -> &str {
        &self.name
    }

    fn complete(&self, _request: ModelRequest) -> ModelFuture<'_> {
        let response = ModelResponse {
            content: self.content.clone(),
            usage: Some(self.usage),
        };
        Box::pin(async move { Ok::<_, ModelError>(response) })
    }
}

#[tokio::test]
async fn completion_model_can_be_replaced_without_http() {
    let model = FakeCompletionModel::one("checker-a", r#"{"decision":"await_user"}"#, 7);
    let response = model
        .complete(ModelRequest {
            messages: vec![Message::for_request(Role::User, "inspect")],
            max_tokens: 32,
        })
        .await
        .unwrap();

    assert_eq!(model.name(), "checker-a");
    assert_eq!(response.content, r#"{"decision":"await_user"}"#);
    assert_eq!(response.usage.unwrap().total_tokens, 7);
}

#[test]
fn deepseek_completion_model_rejects_blank_model_name() {
    let config =
        Config::from_toml("api_key = \"key\"\n[context]\nstrategy = \"summary\"", None).unwrap();
    let client = DeepSeekClient::new(&config).unwrap();

    assert!(DeepSeekCompletionModel::new(client, "   ".to_owned()).is_err());
}
