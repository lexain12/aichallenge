use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use deepseek_cli::agent::AgentEvent;
use deepseek_cli::chat::ChatHistory;
use deepseek_cli::client::{ClientError, DeepSeekClient, TokenUsage};
use deepseek_cli::config::Config;
use deepseek_cli::context::ContextSummary;
use deepseek_cli::dialog::DialogStore;
use deepseek_cli::memory::{DurableMemoryScope, MemoryRepository, RequestScope};
use deepseek_cli::profile::ProfileRepository;
use deepseek_cli::workflow::{
    TaskPhase, TaskStatePatch, TaskStatus, TransitionEvent, WorkflowInput, WorkflowInputSource,
    WorkflowIntent, WorkflowTaskState,
};
use deepseek_cli::workflow_engine::{
    InputHandlingContext, RoutingOutcome, WorkflowEngine, WorkflowInputHandler, WorkflowModels,
    WorkflowSession, WorkflowTurnEvent,
};
use deepseek_cli::workflow_model::{
    CompletionModel, HandoffBuilder, ModelError, ModelFuture, ModelRequest, ModelResponse,
};
use deepseek_cli::workflow_store::{
    AnswerCommit, ControllerInputCommit, ProcessingLeaseMode, ProcessingStatus, WorkflowRepository,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

#[derive(Default)]
struct FakeModel {
    responses: Mutex<VecDeque<Result<String, ModelError>>>,
    requests: Mutex<Vec<ModelRequest>>,
    before_reply: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl FakeModel {
    fn reply(&self, value: Value) {
        self.responses
            .lock()
            .unwrap()
            .push_back(Ok(value.to_string()));
    }

    fn calls(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl CompletionModel for FakeModel {
    fn name(&self) -> &str {
        "fake-service"
    }

    fn complete(&self, request: ModelRequest) -> ModelFuture<'_> {
        self.requests.lock().unwrap().push(request);
        let content = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected service call");
        if let Some(action) = self.before_reply.lock().unwrap().take() {
            action();
        }
        Box::pin(async move {
            Ok(ModelResponse {
                content: content?,
                usage: Some(usage()),
            })
        })
    }
}

fn usage() -> TokenUsage {
    TokenUsage {
        prompt_tokens: 2,
        completion_tokens: 1,
        total_tokens: 3,
        completion_tokens_details: None,
    }
}

fn handoff() -> Value {
    json!({"summary":"projected checkpoint", "completed_step_ids":[], "next_step_id":null,
        "expected_action":"Inspect the current result", "plan_changes":[], "decisions":[], "open_issues":[]})
}

fn interpretation(intent: Value) -> Value {
    json!({"confidence":0.95,"intent":intent})
}

fn ordinary_response(answer: &str, done: bool) -> ResponseTemplate {
    let chunk =
        json!({"choices":[{"delta":{"content":answer},"finish_reason":"stop"}],"usage":usage()});
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!(
            "data: {chunk}\n\n{}",
            if done { "data: [DONE]\n\n" } else { "" }
        ))
}

struct Fixture {
    _directory: tempfile::TempDir,
    connection: Connection,
    store: DialogStore,
    server: MockServer,
    client: DeepSeekClient,
    config: Config,
    interpreter: Arc<FakeModel>,
    checker: Arc<FakeModel>,
    handoff: Arc<FakeModel>,
    dialog_id: Option<i64>,
    scope: RequestScope,
    history: ChatHistory,
    last_usage: Option<TokenUsage>,
}

impl Fixture {
    fn strategy(&mut self, strategy: &str, threshold: u64, keep: usize) {
        self.config=Config::from_toml(&format!("api_key='test-key'\nbase_url='{}'\nsystem_prompt='BASE'\n[context]\nstrategy='{strategy}'\ncompact_after_prompt_tokens={threshold}\nkeep_last_messages={keep}",self.server.uri()),None).unwrap();
    }
    async fn new(phase: Option<TaskPhase>, status: TaskStatus) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("workflow.sqlite3");
        let mut store = DialogStore::open(&database).unwrap();
        let connection = Connection::open(&database).unwrap();
        let server = MockServer::start().await;
        let config = Config::from_toml(&format!(
            "api_key='test-key'\nbase_url='{}'\nsystem_prompt='BASE'\n[context]\nstrategy='summary'\ncompact_after_prompt_tokens=10000\nkeep_last_messages=20", server.uri()), None).unwrap();
        let mut dialog_id = None;
        let history = ChatHistory::new("BASE".to_owned());
        let mut scope = RequestScope::default();
        if let Some(phase) = phase {
            let started = store
                .start_dialog_with_workflow_task(&scope, "BASE", "old-stage-only-marker")
                .unwrap();
            dialog_id = Some(started.dialog_id);
            scope = scope.with_dialog_id(dialog_id);
            let phase = serde_json::to_value(phase)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
            let status = serde_json::to_value(status)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
            let plan = json!({"revision":1,"steps":[{"id":"build","description":"build result","status":"completed"}], "acceptance_criteria":["tests pass"]});
            connection.execute("UPDATE workflow_tasks SET phase=?1,status=?2,plan_json=?3,goal='current task goal'", params![phase,status,plan.to_string()]).unwrap();
            connection
                .execute("UPDATE task_stage_runs SET phase=?1", [phase])
                .unwrap();
        }
        Self {
            _directory: directory,
            connection,
            store,
            client: DeepSeekClient::new(&config).unwrap(),
            server,
            config,
            interpreter: Arc::default(),
            checker: Arc::default(),
            handoff: Arc::default(),
            dialog_id,
            scope,
            history,
            last_usage: None,
        }
    }

    fn current(&self) -> Option<WorkflowTaskState> {
        self.dialog_id
            .and_then(|id| self.store.load_workflow(id).unwrap().current_task)
    }

    fn count(&self, table: &str) -> i64 {
        self.connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn models(&self) -> WorkflowModels {
        WorkflowModels {
            interpreter: self.interpreter.clone(),
            checker: self.checker.clone(),
            handoff: self.handoff.clone(),
        }
    }

    async fn ordinary(&self, response: ResponseTemplate) {
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(response)
            .mount(&self.server)
            .await;
    }

    async fn run<F>(
        &mut self,
        prompt: &str,
        callback: F,
    ) -> Result<
        deepseek_cli::workflow_engine::WorkflowTurnResult,
        deepseek_cli::workflow_engine::WorkflowEngineError,
    >
    where
        F: FnMut(AgentEvent<'_>) -> std::io::Result<()>,
    {
        let models = self.models();
        WorkflowEngine::new(
            &self.client,
            self.config.context(),
            self.config.workflow(),
            &models,
            WorkflowSession {
                store: &mut self.store,
                dialog_id: &mut self.dialog_id,
                scope: &mut self.scope,
                history: &mut self.history,
                last_usage: &mut self.last_usage,
            },
        )
        .run_human_input(prompt, callback)
        .await
    }
}

// Break caught: routing must persist before ordinary generation and reject every forbidden branch.
#[tokio::test]
async fn human_routing_matrix_preserves_phase_status_and_stage_boundaries() {
    struct Case {
        name: &'static str,
        phase: Option<TaskPhase>,
        paused: bool,
        intent: Value,
        kind: &'static str,
        calls: usize,
        target: TaskPhase,
        new_stage: bool,
    }
    use TaskPhase::*;
    let cases = [
        Case {
            name: "first task",
            phase: None,
            paused: false,
            intent: Value::Null,
            kind: "managed",
            calls: 1,
            target: Planning,
            new_stage: true,
        },
        Case {
            name: "unfinished start",
            phase: Some(Execution),
            paused: false,
            intent: json!({"type":"start_new_task","goal":"other task"}),
            kind: "rejected",
            calls: 0,
            target: Execution,
            new_stage: false,
        },
        Case {
            name: "paused continue",
            phase: Some(Execution),
            paused: true,
            intent: json!({"type":"continue","instruction":"continue"}),
            kind: "managed",
            calls: 1,
            target: Execution,
            new_stage: false,
        },
        Case {
            name: "paused transition",
            phase: Some(Execution),
            paused: true,
            intent: json!({"type":"propose_transition","event":"execution_completed","evidence":["build green"]}),
            kind: "managed",
            calls: 1,
            target: Validation,
            new_stage: true,
        },
        Case {
            name: "paused start",
            phase: Some(Execution),
            paused: true,
            intent: json!({"type":"start_new_task","goal":"other task"}),
            kind: "rejected",
            calls: 0,
            target: Execution,
            new_stage: false,
        },
        Case {
            name: "validation done",
            phase: Some(Validation),
            paused: false,
            intent: json!({"type":"propose_transition","event":"validation_passed","evidence":["tests pass => observed 12 passing tests"]}),
            kind: "managed",
            calls: 0,
            target: Done,
            new_stage: true,
        },
        Case {
            name: "done fallback",
            phase: Some(Done),
            paused: false,
            intent: Value::Null,
            kind: "unmanaged",
            calls: 1,
            target: Done,
            new_stage: false,
        },
        Case {
            name: "done new task",
            phase: Some(Done),
            paused: false,
            intent: json!({"type":"start_new_task","goal":"second goal"}),
            kind: "managed",
            calls: 1,
            target: Planning,
            new_stage: true,
        },
        Case {
            name: "illegal skipped stage",
            phase: Some(Planning),
            paused: false,
            intent: json!({"type":"propose_transition","event":"execution_completed","evidence":[]}),
            kind: "rejected",
            calls: 0,
            target: Planning,
            new_stage: false,
        },
        Case {
            name: "human replan",
            phase: Some(Planning),
            paused: false,
            intent: json!({"type":"replan_current","change_request":"revise design"}),
            kind: "managed",
            calls: 1,
            target: Planning,
            new_stage: true,
        },
        Case {
            name: "planning start",
            phase: Some(Planning),
            paused: false,
            intent: json!({"type":"start_new_task","goal":"other task"}),
            kind: "rejected",
            calls: 0,
            target: Planning,
            new_stage: false,
        },
        Case {
            name: "validation start",
            phase: Some(Validation),
            paused: false,
            intent: json!({"type":"start_new_task","goal":"other task"}),
            kind: "rejected",
            calls: 0,
            target: Validation,
            new_stage: false,
        },
        Case {
            name: "planning complete",
            phase: Some(Planning),
            paused: false,
            intent: json!({"type":"propose_transition","event":"planning_completed","evidence":[]}),
            kind: "managed",
            calls: 1,
            target: Execution,
            new_stage: true,
        },
        Case {
            name: "validation repair",
            phase: Some(Validation),
            paused: false,
            intent: json!({"type":"propose_transition","event":"validation_failed","evidence":["one test fails"]}),
            kind: "managed",
            calls: 1,
            target: Execution,
            new_stage: true,
        },
        Case {
            name: "paused replan",
            phase: Some(Execution),
            paused: true,
            intent: json!({"type":"replan_current","change_request":"revise design"}),
            kind: "managed",
            calls: 1,
            target: Planning,
            new_stage: true,
        },
        Case {
            name: "done replan",
            phase: Some(Done),
            paused: false,
            intent: json!({"type":"replan_current","change_request":"revise design"}),
            kind: "managed",
            calls: 1,
            target: Planning,
            new_stage: true,
        },
        Case {
            name: "done continue",
            phase: Some(Done),
            paused: false,
            intent: json!({"type":"continue","instruction":"explain"}),
            kind: "unmanaged",
            calls: 1,
            target: Done,
            new_stage: false,
        },
        Case {
            name: "done transition forbidden",
            phase: Some(Done),
            paused: false,
            intent: json!({"type":"propose_transition","event":"planning_completed","evidence":[]}),
            kind: "rejected",
            calls: 0,
            target: Done,
            new_stage: false,
        },
    ];
    for case in cases {
        let mut f = Fixture::new(
            case.phase,
            if case.paused {
                TaskStatus::Paused
            } else {
                TaskStatus::Active
            },
        )
        .await;
        let before = f.current();
        if case.phase.is_some() {
            f.interpreter.reply(if case.intent.is_null() {
                json!({"malformed":true})
            } else {
                interpretation(case.intent)
            });
        }
        f.handoff.reply(handoff());
        f.ordinary(ordinary_response("answer", true)).await;
        let mut rejected = false;
        let result = f
            .run("raw human instruction", |event| {
                if matches!(
                    event,
                    AgentEvent::Workflow(WorkflowTurnEvent::InputRejected { .. })
                ) {
                    rejected = true;
                }
                Ok(())
            })
            .await
            .unwrap();
        let kind = match result.routing {
            RoutingOutcome::Managed { .. } => "managed",
            RoutingOutcome::Unmanaged { .. } => "unmanaged",
            RoutingOutcome::Rejected { .. } => "rejected",
        };
        assert_eq!(kind, case.kind, "{}", case.name);
        assert_eq!(
            f.server.received_requests().await.unwrap().len(),
            case.calls,
            "{}",
            case.name
        );
        assert_eq!(
            f.interpreter.calls(),
            usize::from(case.phase.is_some()),
            "{}",
            case.name
        );
        assert_eq!(f.checker.calls(), 0, "Task 8 does not process answers");
        let state = f.current().unwrap();
        assert_eq!(state.phase, case.target, "{}", case.name);
        assert_eq!(
            state.status,
            if case.paused && case.kind == "rejected" {
                TaskStatus::Paused
            } else {
                TaskStatus::Active
            },
            "{}",
            case.name
        );
        if let Some(before) = before {
            assert_eq!(
                state.current_stage_run_id != before.current_stage_run_id,
                case.new_stage,
                "{}",
                case.name
            );
        }
        assert_eq!(rejected, case.kind == "rejected", "{}", case.name);
        assert_eq!(
            f.history.messages().last().unwrap().content(),
            if case.calls == 1 {
                "answer"
            } else {
                "raw human instruction"
            }
        );
        if case.kind == "rejected" {
            assert_eq!(f.count("message_task_stages"), 1);
            let outcome: String = f
                .connection
                .query_row(
                    "SELECT outcome FROM workflow_inputs ORDER BY id DESC LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(outcome, "rejected");
            assert_eq!(f.handoff.calls(), 0);
        }
    }
}

// Break caught: a human transition must be committed before context is assembled.
#[tokio::test]
async fn human_transition_is_applied_before_the_ordinary_request() {
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    f.interpreter.reply(interpretation(json!({"type":"propose_transition","event":"execution_completed","evidence":["build green"]})));
    f.handoff.reply(handoff());
    f.ordinary(ordinary_response("validation result", true))
        .await;
    let mut response_phase = None;
    let result = f
        .run("implementation done; test it", |event| {
            if let AgentEvent::Workflow(WorkflowTurnEvent::ResponseStarted { phase, .. }) = event {
                response_phase = Some(phase);
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(response_phase, Some(TaskPhase::Validation));
    assert_eq!(result.answer.as_deref(), Some("validation result"));
    let requests = f.server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    let messages = body["messages"].as_array().unwrap();
    let text = messages
        .iter()
        .map(|m| m["content"].as_str().unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("\"phase\": \"validation\""));
    assert!(text.contains("projected checkpoint"));
    assert!(!text.contains("old-stage-only-marker"));
    assert_eq!(
        messages
            .iter()
            .filter(|m| m["content"] == "implementation done; test it")
            .count(),
        1
    );
    assert_eq!(f.count("response_processing"), 1);
    let pending = f
        .store
        .load_pending_processing(f.dialog_id.unwrap())
        .unwrap();
    assert_eq!(pending[0].status, ProcessingStatus::Pending);
    assert_eq!(
        pending[0].assistant_message_id,
        result.persisted_answer.unwrap().message_id
    );
    let handoff_requests = f.handoff.requests.lock().unwrap();
    let context: Value = serde_json::from_str(handoff_requests[0].messages[1].content()).unwrap();
    assert_eq!(
        context["stage_messages"][0]["content"],
        "old-stage-only-marker"
    );
}

// Break caught: controller source restrictions must precede both handoff and persistence.
#[tokio::test]
async fn controller_cannot_start_or_replan_a_task() {
    for intent in [
        WorkflowIntent::StartNewTask {
            goal: "other task".into(),
        },
        WorkflowIntent::ReplanCurrent {
            change_request: "rewrite goal".into(),
        },
    ] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let before = f.current();
        let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
        let snapshot = f.store.load_workflow(f.dialog_id.unwrap()).unwrap();
        let result = WorkflowInputHandler {
            store: &mut f.store,
            handoff_builder: &builder,
        }
        .handle(
            f.dialog_id.unwrap(),
            snapshot,
            WorkflowInput {
                source: WorkflowInputSource::Controller {
                    checker: "continuation".into(),
                    model: "checker".into(),
                    triggering_assistant_message_id: 1,
                },
                intent,
            },
            "synthetic instruction",
            Some(0.99),
            InputHandlingContext::default(),
        )
        .await
        .unwrap();
        assert!(matches!(result, RoutingOutcome::Rejected { .. }));
        assert_eq!(f.current(), before);
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.count("workflow_inputs"), 1);
        assert_eq!(f.handoff.calls(), 0);
        assert_eq!(f.interpreter.calls(), 0);
        assert!(f.server.received_requests().await.unwrap().is_empty());
    }
}

fn empty_patch(version: u64) -> TaskStatePatch {
    TaskStatePatch {
        expected_version: version,
        plan_append: Default::default(),
        step_updates: vec![],
        current_step_id: None,
        expected_action: None,
        checkpoint: None,
    }
}

// Break caught: accepted controller routes must bind checker provenance and atomically finish its job.
#[tokio::test]
async fn controller_continue_and_transition_share_guarded_routing_and_hide_the_input() {
    for transition in [false, true] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let task = f.current().unwrap();
        let answer = f
            .store
            .append_answer_for_processing(AnswerCommit {
                dialog_id: task.dialog_id,
                task_id: task.id,
                stage_run_id: task.current_stage_run_id,
                expected_version: task.version,
                content: "finished build",
                usage: Some(usage()),
            })
            .unwrap();
        f.store
            .lease_processing(
                answer.processing_id,
                task.version,
                ProcessingLeaseMode::Normal,
            )
            .unwrap();
        let patch = empty_patch(task.version);
        let input = WorkflowInput {
            source: WorkflowInputSource::Controller {
                checker: "continuation".into(),
                model: "checker-b".into(),
                triggering_assistant_message_id: answer.message_id,
            },
            intent: if transition {
                WorkflowIntent::ProposeTransition {
                    event: TransitionEvent::ExecutionCompleted,
                    evidence: vec!["build green".into()],
                }
            } else {
                WorkflowIntent::Continue {
                    instruction: "hidden instruction".into(),
                }
            },
        };
        f.handoff.reply(handoff());
        let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
        let snapshot = f.store.load_workflow(task.dialog_id).unwrap();
        let route = WorkflowInputHandler {
            store: &mut f.store,
            handoff_builder: &builder,
        }
        .handle(
            task.dialog_id,
            snapshot,
            input,
            "hidden instruction",
            Some(0.95),
            InputHandlingContext {
                processing_id: Some(answer.processing_id),
                accepted_patch: Some(&patch),
            },
        )
        .await
        .unwrap();
        let RoutingOutcome::Managed { state, .. } = route else {
            panic!("controller route rejected")
        };
        assert_eq!(state.version, task.version + 1);
        assert_eq!(
            state.phase,
            if transition {
                TaskPhase::Validation
            } else {
                TaskPhase::Execution
            }
        );
        assert_eq!(
            state.current_stage_run_id != task.current_stage_run_id,
            transition
        );
        assert!(
            f.store
                .load_stage_messages(state.current_stage_run_id)
                .unwrap()
                .iter()
                .any(|row| row.message.content() == "hidden instruction")
        );
        assert!(
            !f.store
                .load(task.dialog_id)
                .unwrap()
                .messages
                .iter()
                .any(|row| row.content() == "hidden instruction")
        );
        let metadata:(String,String,String,i64,String)=f.connection.query_row("SELECT source,checker_name,model_name,triggering_assistant_message_id,outcome FROM workflow_inputs ORDER BY id DESC LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
        assert_eq!(
            metadata,
            (
                "controller".into(),
                "continuation".into(),
                "checker-b".into(),
                answer.message_id,
                "accepted".into()
            )
        );
        let status: String = f
            .connection
            .query_row(
                "SELECT status FROM response_processing WHERE id=?1",
                [answer.processing_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(f.handoff.calls(), usize::from(transition));
    }
}

// Break caught: a failed handoff must never assign the human trigger to the outgoing stage.
#[tokio::test]
async fn human_handoff_failure_records_rejection_and_keeps_the_old_stage() {
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Paused).await;
    let before = f.current();
    f.interpreter.reply(interpretation(
        json!({"type":"propose_transition","event":"execution_completed","evidence":[]}),
    ));
    f.handoff
        .reply(json!({"invalid":"provider payload must not enter diagnostics"}));
    let result = f.run("test it", |_| Ok(())).await.unwrap();
    assert!(matches!(result.routing, RoutingOutcome::Rejected { .. }));
    assert_eq!(f.current(), before);
    assert_eq!(f.count("messages"), 2);
    assert_eq!(f.count("message_task_stages"), 1);
    assert_eq!(f.count("task_transitions"), 0);
    assert_eq!(f.count("response_processing"), 0);
    assert_eq!(f.history.messages().last().unwrap().content(), "test it");
    assert!(f.server.received_requests().await.unwrap().is_empty());
}

// Break caught: partial, blank, API-error, and unwritable output may not create an assistant/job.
#[tokio::test]
async fn failed_ordinary_turn_keeps_only_the_committed_input() {
    for failure in ["incomplete", "blank", "api", "output"] {
        let mut f = Fixture::new(None, TaskStatus::Active).await;
        f.ordinary(match failure {
            "incomplete" => ordinary_response("partial answer", false),
            "blank" => ordinary_response("  ", true),
            "api" => ResponseTemplate::new(500),
            _ => ordinary_response("complete answer", true),
        })
        .await;
        let result = f
            .run("first task", |event| {
                if failure == "output" && matches!(event, AgentEvent::Text(_)) {
                    Err(std::io::Error::other("output unavailable"))
                } else {
                    Ok(())
                }
            })
            .await;
        assert!(result.is_err(), "{failure}");
        let state = f.current().unwrap();
        assert_eq!(state.version, 0);
        assert_eq!(state.phase, TaskPhase::Planning);
        assert_eq!(f.count("messages"), 1, "{failure}");
        assert_eq!(f.count("response_processing"), 0, "{failure}");
        assert_eq!(f.history.messages().len(), 1);
        assert_eq!(f.scope.dialog_id(), f.dialog_id);
    }
}

// Break caught: fact extraction must use only human evidence from this stage, before ordinary generation.
#[tokio::test]
async fn stage_facts_refresh_excludes_controller_and_dialog_wide_reductions() {
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    f.strategy("sticky_facts", 10000, 10);
    let task = f.current().unwrap();
    let answer = f
        .store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: task.dialog_id,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            content: "assistant-only-marker",
            usage: None,
        })
        .unwrap();
    f.store
        .lease_processing(
            answer.processing_id,
            task.version,
            ProcessingLeaseMode::Normal,
        )
        .unwrap();
    let intent = WorkflowIntent::human_continue("controller-only-marker").unwrap();
    f.store
        .commit_controller_decision(ControllerInputCommit {
            processing_id: answer.processing_id,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            checker: "continuation",
            model: "checker-b",
            triggering_assistant_message_id: answer.message_id,
            instruction: "controller-only-marker",
            intent: &intent,
            confidence: 0.95,
            accepted_patch: &empty_patch(task.version),
        })
        .unwrap();
    f.connection
        .execute_batch("DROP TABLE dialog_context; DROP TABLE dialog_facts;")
        .unwrap();
    f.interpreter.reply(interpretation(
        json!({"type":"continue","instruction":"I use Rust"}),
    ));
    Mock::given(body_string_contains("Update the key-value memory"))
        .respond_with(ordinary_response("{\"language\":\"Rust\"}", true))
        .mount(&f.server)
        .await;
    Mock::given(body_string_contains("BASE"))
        .respond_with(ordinary_response("ordinary answer", true))
        .mount(&f.server)
        .await;
    let result = f.run("I use Rust", |_| Ok(())).await.unwrap();
    assert_eq!(result.answer.as_deref(), Some("ordinary answer"));
    let requests = f.server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let facts: Value = requests[0].body_json().unwrap();
    let facts_text = facts["messages"].to_string();
    assert!(facts_text.contains("I use Rust"));
    assert!(!facts_text.contains("controller-only-marker"));
    assert!(!facts_text.contains("assistant-only-marker"));
    let ordinary: Value = requests[1].body_json().unwrap();
    assert!(ordinary["messages"].to_string().contains("language"));
    assert!(
        ordinary["messages"]
            .to_string()
            .contains("controller-only-marker")
    );
    let reductions = f
        .store
        .load_stage_reductions(task.current_stage_run_id)
        .unwrap();
    assert_eq!(
        reductions.facts.facts().get("language").map(String::as_str),
        Some("Rust")
    );
    assert_eq!(reductions.facts.covered_message_count(), 3);
    assert_eq!(reductions.facts.update_usage().total_tokens(), 3);
}

// Break caught: a failed pre-answer service may not produce an ordinary answer or processing job.
#[tokio::test]
async fn stage_facts_failure_stops_before_ordinary_but_keeps_input() {
    for response in [
        ResponseTemplate::new(500),
        ordinary_response("invalid facts", true),
    ] {
        let mut f = Fixture::new(None, TaskStatus::Active).await;
        f.strategy("sticky_facts", 10000, 10);
        f.ordinary(response).await;
        assert!(f.run("task with facts", |_| Ok(())).await.is_err());
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.count("response_processing"), 0);
        assert_eq!(f.server.received_requests().await.unwrap().len(), 1);
    }
}

// Break caught: post-answer compaction must see committed answer/job and never roll them back on failure.
#[tokio::test]
async fn stage_summary_runs_after_atomic_answer_commit_and_fails_open() {
    for failure in [false, true] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        f.strategy("summary", 1, 2);
        f.interpreter.reply(interpretation(
            json!({"type":"continue","instruction":"continue"}),
        ));
        Mock::given(body_string_contains("BASE"))
            .respond_with(ordinary_response("ordinary answer", true))
            .mount(&f.server)
            .await;
        let database = f._directory.path().join("workflow.sqlite3");
        Mock::given(body_string_contains("Create a faithful cumulative summary"))
            .respond_with(move |_: &Request| {
                let connection = Connection::open(&database).unwrap();
                let jobs: i64 = connection
                    .query_row("SELECT count(*) FROM response_processing", [], |r| r.get(0))
                    .unwrap();
                let answers: i64 = connection
                    .query_row(
                        "SELECT count(*) FROM messages WHERE role='assistant'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(
                    (jobs, answers),
                    (1, 1),
                    "answer and job must exist before compaction HTTP"
                );
                if failure {
                    ResponseTemplate::new(500)
                } else {
                    ordinary_response("current-stage-summary", true)
                }
            })
            .mount(&f.server)
            .await;
        let mut warning = false;
        let result = f
            .run("continue", |event| {
                if matches!(event, AgentEvent::CompactionFailed { .. }) {
                    warning = true;
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(result.answer.as_deref(), Some("ordinary answer"));
        assert_eq!(f.count("response_processing"), 1);
        assert_eq!(f.server.received_requests().await.unwrap().len(), 2);
        assert_eq!(warning, failure);
        let reductions = f
            .store
            .load_stage_reductions(f.current().unwrap().current_stage_run_id)
            .unwrap();
        if failure {
            assert!(reductions.context.summary().is_none());
        } else {
            assert_eq!(
                reductions.context.summary().unwrap().content(),
                "current-stage-summary"
            );
            assert_eq!(
                reductions
                    .context
                    .summary()
                    .unwrap()
                    .covered_message_count(),
                1
            );
        }
        assert_eq!(f.count("dialog_context"), 0);
    }
}

// Break caught: completed-task fallback must not revive old protocol or create managed assistant metadata.
#[tokio::test]
async fn unmanaged_completed_task_answer_uses_only_new_input_and_inherited_blocks() {
    let mut f = Fixture::new(Some(TaskPhase::Done), TaskStatus::Active).await;
    let before = f.current();
    f.store
        .replace_profile(f.scope.user_id(), "PROFILE MARKER")
        .unwrap();
    f.store
        .upsert_memory(
            &f.scope.address(DurableMemoryScope::User),
            "language",
            "USER MEMORY MARKER",
        )
        .unwrap();
    f.store
        .upsert_memory(
            &f.scope.address(DurableMemoryScope::Task),
            "project",
            "TASK MEMORY MARKER",
        )
        .unwrap();
    f.interpreter
        .responses
        .lock()
        .unwrap()
        .push_back(Err(ModelError::Client(ClientError::Stream(
            "interpreter unavailable".into(),
        ))));
    f.ordinary(ordinary_response("unmanaged answer", true))
        .await;
    let result = f.run("one unrelated question", |_| Ok(())).await.unwrap();
    assert!(matches!(result.routing, RoutingOutcome::Unmanaged { .. }));
    assert!(result.persisted_answer.is_none());
    assert_eq!(f.current(), before);
    assert_eq!(f.count("messages"), 3);
    assert_eq!(f.count("workflow_inputs"), 1);
    assert_eq!(f.count("message_task_stages"), 1);
    assert_eq!(f.count("response_processing"), 0);
    let requests = f.server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 5);
    assert_eq!(messages[0]["content"], "BASE");
    assert!(
        messages[1]["content"]
            .as_str()
            .unwrap()
            .contains("PROFILE MARKER")
    );
    assert!(
        messages[2]["content"]
            .as_str()
            .unwrap()
            .contains("USER MEMORY MARKER")
    );
    assert!(
        messages[3]["content"]
            .as_str()
            .unwrap()
            .contains("TASK MEMORY MARKER")
    );
    assert_eq!(messages[4]["content"], "one unrelated question");
}

// Break caught: low confidence and service failure cannot start a second task or change phase.
#[tokio::test]
async fn interpreter_failure_and_low_confidence_use_the_safe_fallbacks() {
    for phase in [
        TaskPhase::Planning,
        TaskPhase::Execution,
        TaskPhase::Validation,
        TaskPhase::Done,
    ] {
        for api_failure in [false, true] {
            let mut f = Fixture::new(Some(phase), TaskStatus::Active).await;
            let before = f.current().unwrap();
            if api_failure {
                f.interpreter
                    .responses
                    .lock()
                    .unwrap()
                    .push_back(Err(ModelError::Client(ClientError::Stream(
                        "unavailable".into(),
                    ))));
            } else {
                f.interpreter.reply(json!({"confidence":0.1,"intent":{"type":"start_new_task","goal":"unwanted task"}}));
            }
            f.ordinary(ordinary_response("answer", true)).await;
            f.run("original human text", |_| Ok(())).await.unwrap();
            let after = f.current().unwrap();
            assert_eq!(after.id, before.id);
            assert_eq!(after.phase, phase);
            assert_eq!(after.current_stage_run_id, before.current_stage_run_id);
            assert_eq!(
                after.version,
                before.version + u64::from(phase != TaskPhase::Done)
            );
            assert_eq!(f.count("workflow_tasks"), 1);
            assert_eq!(f.handoff.calls(), 0);
        }
    }
}

// Break caught: stale interpreter/handoff output must lose the guarded write with no leaked input or answer.
#[tokio::test]
async fn racing_state_change_discards_interpreter_or_handoff_effects() {
    for during_handoff in [false, true] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let original = f.current().unwrap();
        f.interpreter.reply(interpretation(if during_handoff {
            json!({"type":"propose_transition","event":"execution_completed","evidence":[]})
        } else {
            json!({"type":"continue","instruction":"continue"})
        }));
        f.handoff.reply(handoff());
        let database = f._directory.path().join("workflow.sqlite3");
        let hook = Box::new(move || {
            Connection::open(database)
                .unwrap()
                .execute("UPDATE workflow_tasks SET version=version+1", [])
                .unwrap();
        });
        if during_handoff {
            *f.handoff.before_reply.lock().unwrap() = Some(hook);
        } else {
            *f.interpreter.before_reply.lock().unwrap() = Some(hook);
        }
        assert!(f.run("stale instruction", |_| Ok(())).await.is_err());
        assert_eq!(f.current().unwrap().phase, original.phase);
        assert_eq!(f.current().unwrap().version, original.version + 1);
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.count("task_transitions"), 0);
        assert!(f.server.received_requests().await.unwrap().is_empty());
    }
}

// Break caught: missing controller processing/patch and paused/done controllers must never emit protocol.
#[tokio::test]
async fn controller_requires_active_task_and_explicit_matching_processing_patch() {
    for (phase, status, missing_patch, stale_patch) in [
        (TaskPhase::Execution, TaskStatus::Paused, false, false),
        (TaskPhase::Done, TaskStatus::Active, false, false),
        (TaskPhase::Execution, TaskStatus::Active, true, false),
        (TaskPhase::Execution, TaskStatus::Active, false, true),
    ] {
        let mut f = Fixture::new(Some(phase), status).await;
        let task = f.current().unwrap();
        let patch = empty_patch(task.version + u64::from(stale_patch));
        let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
        let snapshot = f.store.load_workflow(task.dialog_id).unwrap();
        let route = WorkflowInputHandler {
            store: &mut f.store,
            handoff_builder: &builder,
        }
        .handle(
            task.dialog_id,
            snapshot,
            WorkflowInput {
                source: WorkflowInputSource::Controller {
                    checker: "continuation".into(),
                    model: "checker".into(),
                    triggering_assistant_message_id: 1,
                },
                intent: WorkflowIntent::human_continue("hidden").unwrap(),
            },
            "hidden",
            Some(0.95),
            InputHandlingContext {
                processing_id: Some(1),
                accepted_patch: if missing_patch { None } else { Some(&patch) },
            },
        )
        .await;
        assert!(matches!(
            route,
            Err(_) | Ok(RoutingOutcome::Rejected { .. })
        ));
        assert_eq!(f.current().unwrap(), task);
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.handoff.calls(), 0);
    }
}

// Break caught: controller handoff failure must fail its leased processing without writing a hidden input.
#[tokio::test]
async fn controller_handoff_failure_preserves_answer_and_fails_processing() {
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    let task = f.current().unwrap();
    let answer = f
        .store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: task.dialog_id,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            content: "saved answer",
            usage: None,
        })
        .unwrap();
    f.store
        .lease_processing(
            answer.processing_id,
            task.version,
            ProcessingLeaseMode::Normal,
        )
        .unwrap();
    f.handoff.reply(json!({"bad":"handoff"}));
    let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
    let snapshot = f.store.load_workflow(task.dialog_id).unwrap();
    let patch = empty_patch(task.version);
    let route = WorkflowInputHandler {
        store: &mut f.store,
        handoff_builder: &builder,
    }
    .handle(
        task.dialog_id,
        snapshot,
        WorkflowInput {
            source: WorkflowInputSource::Controller {
                checker: "continuation".into(),
                model: "checker".into(),
                triggering_assistant_message_id: answer.message_id,
            },
            intent: WorkflowIntent::ProposeTransition {
                event: TransitionEvent::ExecutionCompleted,
                evidence: vec![],
            },
        },
        "validate",
        Some(0.95),
        InputHandlingContext {
            processing_id: Some(answer.processing_id),
            accepted_patch: Some(&patch),
        },
    )
    .await
    .unwrap();
    assert!(matches!(route, RoutingOutcome::Rejected { .. }));
    assert_eq!(f.current().unwrap(), task);
    assert_eq!(f.count("messages"), 2);
    assert_eq!(f.count("workflow_inputs"), 1);
    assert_eq!(
        f.store.load_pending_processing(task.dialog_id).unwrap()[0].status,
        ProcessingStatus::Failed
    );
}

// Break caught: blank input, input-write failure, and answer-job failure may not leave partial durable effects.
#[tokio::test]
async fn local_validation_and_store_failures_never_commit_partial_turns() {
    let mut blank = Fixture::new(None, TaskStatus::Active).await;
    assert!(blank.run(" \n ", |_| Ok(())).await.is_err());
    assert_eq!(blank.count("dialogs"), 0);
    assert!(blank.server.received_requests().await.unwrap().is_empty());
    for table in ["workflow_inputs", "response_processing"] {
        let mut f = Fixture::new(None, TaskStatus::Active).await;
        f.connection.execute_batch(&format!("CREATE TRIGGER fail_write BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'injected'); END;")).unwrap();
        f.ordinary(ordinary_response("complete answer", true)).await;
        assert!(f.run("human input", |_| Ok(())).await.is_err());
        assert_eq!(
            f.count("messages"),
            i64::from(table == "response_processing")
        );
        assert_eq!(f.count("response_processing"), 0);
        assert_eq!(
            f.history.messages().len(),
            usize::from(table == "response_processing")
        );
    }
}

// Break caught: upgrading a legacy dialog must start a tagged stage without importing its history or reductions.
#[tokio::test]
async fn legacy_dialog_starts_its_first_task_without_interpreter_or_old_context() {
    let mut f = Fixture::new(None, TaskStatus::Active).await;
    let id = f.store.start_dialog("BASE", "LEGACY USER MARKER").unwrap();
    f.store
        .append_answer(id, 1, "LEGACY ANSWER MARKER", None)
        .unwrap();
    f.store
        .replace_context(id, 2, ContextSummary::new("LEGACY SUMMARY MARKER", 1), None)
        .unwrap();
    f.store
        .replace_facts(
            id,
            2,
            [("legacy".into(), "LEGACY FACTS MARKER".into())].into(),
            None,
        )
        .unwrap();
    f.dialog_id = Some(id);
    f.scope = f.scope.with_dialog_id(Some(id));
    f.ordinary(ordinary_response("managed answer", true)).await;
    f.run("new managed goal", |_| Ok(())).await.unwrap();
    assert_eq!(f.interpreter.calls(), 0);
    assert_eq!(f.count("workflow_tasks"), 1);
    assert_eq!(f.count("messages"), 4);
    assert_eq!(f.count("message_task_stages"), 2);
    let requests = f.server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert!(!body["messages"].to_string().contains("LEGACY"));
    assert_eq!(body["messages"].as_array().unwrap().len(), 3);
    assert_eq!(body["messages"][2]["content"], "new managed goal");
}
