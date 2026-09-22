use std::collections::VecDeque;
use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use deepseek_cli::agent::AgentEvent;
use deepseek_cli::chat::ChatHistory;
use deepseek_cli::client::{ClientError, DeepSeekClient, TokenUsage};
use deepseek_cli::config::Config;
use deepseek_cli::context::ContextSummary;
use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::memory::{DurableMemoryScope, MemoryRepository, RequestScope};
use deepseek_cli::profile::ProfileRepository;
use deepseek_cli::workflow::{
    PlanStepStatus, StepStatusUpdate, TaskPhase, TaskStatePatch, TaskStatus, TransitionEvent,
    WorkflowInput, WorkflowInputSource, WorkflowIntent, WorkflowTaskState,
};
use deepseek_cli::workflow_engine::{
    AutonomyBudget, AutonomyStopReason, InputHandlingContext, PipelineOutcome, ResponsePipeline,
    RoutingOutcome, StateFingerprint, WorkflowEngine, WorkflowEngineError, WorkflowInputHandler,
    WorkflowModels, WorkflowSession, WorkflowTurnEvent,
};
use deepseek_cli::workflow_model::{
    CheckContext, CheckFuture, CheckerMode, CompletionModel, ContinuationCheckResult,
    ControllerDecision, DeepSeekCompletionModel, HandoffBuilder, ModelError, ModelFuture,
    ModelRequest, ModelResponse, ResponseChecker,
};
use deepseek_cli::workflow_store::{
    AcceptedInputEffect, AnswerCommit, ControllerInputCommit, ExpectedCurrentTask, InputCommit,
    PauseOutcome, ProcessingLeaseMode, ProcessingStatus, WorkflowRepository,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

#[derive(Default)]
struct FakeModel {
    responses: Mutex<VecDeque<Result<String, ModelError>>>,
    requests: Mutex<Vec<ModelRequest>>,
    before_reply: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    missing_usage: bool,
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
            .unwrap_or_else(|| Err(ClientError::EmptyAnswer.into()));
        if let Some(action) = self.before_reply.lock().unwrap().take() {
            action();
        }
        Box::pin(async move {
            Ok(ModelResponse {
                content: content?,
                usage: (!self.missing_usage).then(usage),
            })
        })
    }
}

#[derive(Default)]
struct PendingModel {
    started: AtomicBool,
}

impl CompletionModel for PendingModel {
    fn name(&self) -> &str {
        "pending-service"
    }

    fn complete(&self, _request: ModelRequest) -> ModelFuture<'_> {
        self.started.store(true, Ordering::SeqCst);
        Box::pin(async move {
            pending::<()>().await;
            unreachable!("pending model is cancelled by the test")
        })
    }
}

#[derive(Clone)]
struct StartedResponder {
    started: Arc<AtomicBool>,
    response: ResponseTemplate,
}

impl Respond for StartedResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        self.started.store(true, Ordering::SeqCst);
        self.response.clone()
    }
}

async fn wait_until_started(started: &AtomicBool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("service call should start");
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

fn response_without_usage(answer: &str) -> ResponseTemplate {
    let chunk = json!({"choices":[{"delta":{"content":answer},"finish_reason":"stop"}]});
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
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
    fn pending_answer(&mut self) -> deepseek_cli::workflow_store::PersistedAnswer {
        let task = self.current().unwrap();
        self.store
            .append_answer_for_processing(AnswerCommit {
                dialog_id: task.dialog_id,
                task_id: task.id,
                stage_run_id: task.current_stage_run_id,
                expected_version: task.version,
                content: "saved answer",
                usage: Some(usage()),
            })
            .unwrap()
    }

    async fn process(
        &mut self,
        id: i64,
        budget: &mut AutonomyBudget,
        mode: ProcessingLeaseMode,
        pipeline: Option<ResponsePipeline>,
    ) -> deepseek_cli::workflow_engine::ProcessingOutcome {
        let models = self.models();
        let engine = WorkflowEngine::new(
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
        );
        let mut engine = if let Some(pipeline) = pipeline {
            engine.with_pipeline(pipeline)
        } else {
            engine
        };
        engine.process_answer(id, budget, mode).await.unwrap()
    }
    fn limits(&mut self, turns: u32, tokens: u64) {
        self.config = Config::from_toml(&format!("api_key='test-key'\nbase_url='{}'\nsystem_prompt='BASE'\n[workflow]\nmax_autonomous_turns={turns}\nmax_autonomous_tokens={tokens}\n[context]\nstrategy='summary'", self.server.uri()), None).unwrap();
    }

    async fn recover(&mut self) -> Vec<deepseek_cli::workflow_engine::RecoveredProcessing> {
        let models = self.models();
        let id = self.dialog_id.unwrap();
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
        .recover_pending_processing(id)
        .await
        .unwrap()
    }
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
        self.run_with_models(prompt, &models, callback).await
    }

    async fn run_with_models<F>(
        &mut self,
        prompt: &str,
        models: &WorkflowModels,
        callback: F,
    ) -> Result<deepseek_cli::workflow_engine::WorkflowTurnResult, WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> std::io::Result<()>,
    {
        WorkflowEngine::new(
            &self.client,
            self.config.context(),
            self.config.workflow(),
            models,
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

fn checked(version: u64, action: Option<&str>, decision: Value) -> Value {
    let mut patch = empty_patch(version);
    patch.expected_action = action.map(str::to_owned);
    json!({"patch":patch,"decision":decision})
}

fn continue_decision() -> Value {
    json!({"type":"continue","instruction":"HIDDEN next instruction","confidence":0.95})
}

// Break caught: dropping any service-boundary future may preserve earlier commits, but no partial effect.
#[tokio::test]
async fn cancellation_at_interpreter_handoff_ordinary_and_recovery_boundaries_is_atomic() {
    {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let before = f.current().unwrap();
        let blocker = Arc::new(PendingModel::default());
        let models = WorkflowModels {
            interpreter: blocker.clone(),
            checker: f.checker.clone(),
            handoff: f.handoff.clone(),
        };
        let mut run = Box::pin(f.run_with_models("continue", &models, |_| Ok(())));
        tokio::select! {
            result = run.as_mut() => panic!("interpreter unexpectedly completed: {result:?}"),
            _ = wait_until_started(&blocker.started) => {}
        }
        drop(run);
        assert_eq!(f.current().unwrap(), before);
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.count("workflow_inputs"), 1);
        let PauseOutcome::Paused(paused) = f.store.pause_current_task(before.dialog_id).unwrap()
        else {
            panic!("active task must pause after interpreter cancellation");
        };
        assert_eq!(paused.status, TaskStatus::Paused);
        assert_eq!(paused.version, before.version + 1);
        assert_eq!(paused.current_stage_run_id, before.current_stage_run_id);
    }

    {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let before = f.current().unwrap();
        f.interpreter.reply(interpretation(json!({
            "type":"propose_transition",
            "event":"execution_completed",
            "evidence":["build green"]
        })));
        let blocker = Arc::new(PendingModel::default());
        let models = WorkflowModels {
            interpreter: f.interpreter.clone(),
            checker: f.checker.clone(),
            handoff: blocker.clone(),
        };
        let mut run = Box::pin(f.run_with_models("validate it", &models, |_| Ok(())));
        tokio::select! {
            result = run.as_mut() => panic!("handoff unexpectedly completed: {result:?}"),
            _ = wait_until_started(&blocker.started) => {}
        }
        drop(run);
        assert_eq!(f.current().unwrap(), before);
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.count("task_transitions"), 0);
        let PauseOutcome::Paused(paused) = f.store.pause_current_task(before.dialog_id).unwrap()
        else {
            panic!("active task must pause after handoff cancellation");
        };
        assert_eq!(paused.version, before.version + 1);
        assert_eq!(paused.current_stage_run_id, before.current_stage_run_id);
    }

    {
        let mut f = Fixture::new(None, TaskStatus::Active).await;
        let request_started = Arc::new(AtomicBool::new(false));
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(StartedResponder {
                started: request_started.clone(),
                response: ordinary_response("partial", true)
                    .set_delay(std::time::Duration::from_secs(30)),
            })
            .mount(&f.server)
            .await;
        let mut run = Box::pin(f.run("first task", |_| Ok(())));
        tokio::select! {
            result = run.as_mut() => panic!("ordinary request unexpectedly completed: {result:?}"),
            _ = wait_until_started(&request_started) => {}
        }
        drop(run);
        let before_pause = f.current().expect("input creates its legitimate task");
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.count("response_processing"), 0);
        let PauseOutcome::Paused(paused) =
            f.store.pause_current_task(before_pause.dialog_id).unwrap()
        else {
            panic!("active task must pause after ordinary cancellation");
        };
        assert_eq!(paused.version, before_pause.version + 1);
        assert_eq!(
            paused.current_stage_run_id,
            before_pause.current_stage_run_id
        );
        assert_eq!(f.count("response_processing"), 0);
    }

    {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let before = f.current().unwrap();
        let answer = f.pending_answer();
        let blocker = Arc::new(PendingModel::default());
        let models = WorkflowModels {
            interpreter: f.interpreter.clone(),
            checker: blocker.clone(),
            handoff: f.handoff.clone(),
        };
        let dialog_id = f.dialog_id.unwrap();
        let mut engine = WorkflowEngine::new(
            &f.client,
            f.config.context(),
            f.config.workflow(),
            &models,
            WorkflowSession {
                store: &mut f.store,
                dialog_id: &mut f.dialog_id,
                scope: &mut f.scope,
                history: &mut f.history,
                last_usage: &mut f.last_usage,
            },
        );
        let mut recovery = Box::pin(engine.recover_pending_processing(dialog_id));
        tokio::select! {
            result = recovery.as_mut() => panic!("recovery checker unexpectedly completed: {result:?}"),
            _ = wait_until_started(&blocker.started) => {}
        }
        drop(recovery);
        drop(engine);
        let processing = f.store.load_pending_processing(dialog_id).unwrap();
        assert_eq!(processing.len(), 1);
        assert_eq!(processing[0].id, answer.processing_id);
        assert_eq!(processing[0].status, ProcessingStatus::Processing);
        assert_eq!(f.current().unwrap(), before);
        assert_eq!(f.count("workflow_inputs"), 1);
        let PauseOutcome::Paused(paused) = f.store.pause_current_task(dialog_id).unwrap() else {
            panic!("active task must pause after recovery cancellation");
        };
        assert_eq!(paused.version, before.version + 1);
        assert_eq!(paused.current_stage_run_id, before.current_stage_run_id);
        assert_eq!(f.count("workflow_inputs"), 1);
    }
}

// Break caught: ordinary answers must be durable before checker work, and await_user applies one patch.
#[tokio::test]
async fn await_user_applies_one_patch_and_ends_the_loop() {
    let mut f = Fixture::new(None, TaskStatus::Active).await;
    f.ordinary(ordinary_response("plan drafted", true)).await;
    f.checker.reply(checked(
        0,
        Some("ask for approval"),
        json!({"type":"await_user"}),
    ));
    let database = f._directory.path().join("workflow.sqlite3");
    *f.checker.before_reply.lock().unwrap() = Some(Box::new(move || {
        let db = Connection::open(database).unwrap();
        let row: (String, String, i64) = db.query_row("SELECT m.content,p.status,p.attempts FROM response_processing p JOIN messages m ON m.id=p.assistant_message_id", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(row, ("plan drafted".into(), "processing".into(), 1));
    }));
    let result = f.run("draft a plan", |_| Ok(())).await.unwrap();
    assert_eq!(result.stop_reason, AutonomyStopReason::AwaitUser);
    assert_eq!(result.autonomous_turns, 0);
    assert_eq!(result.tokens, 6);
    assert_eq!(
        result.final_state.unwrap().expected_action.as_deref(),
        Some("ask for approval")
    );
    assert_eq!(f.current().unwrap().version, 1);
    assert!(
        f.store
            .load_pending_processing(f.dialog_id.unwrap())
            .unwrap()
            .is_empty()
    );
}

// Break caught: controller continuation must be persisted, hidden from transcript, and used by the next call.
#[tokio::test]
async fn continue_persists_hidden_input_then_runs_another_ordinary_turn() {
    let mut f = Fixture::new(None, TaskStatus::Active).await;
    f.ordinary(ordinary_response("ordinary answer", true)).await;
    f.checker
        .reply(checked(0, Some("part two"), continue_decision()));
    f.checker
        .reply(checked(1, None, json!({"type":"await_user"})));
    let mut events = vec![];
    let result = f
        .run("implement", |event| {
            if let AgentEvent::Workflow(WorkflowTurnEvent::AutonomousTurnStarted {
                number,
                phase,
            }) = event
            {
                events.push((number, phase));
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(result.autonomous_turns, 1);
    assert_eq!(result.tokens, 12);
    assert_eq!(events, [(1, TaskPhase::Planning)]);
    let requests = f.server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].body_json::<Value>().unwrap()["messages"]
            .to_string()
            .contains("HIDDEN next instruction")
    );
    assert_eq!(f.count("workflow_inputs"), 2);
    assert_eq!(f.history.messages().len(), 3);
    assert!(
        !f.store
            .load(f.dialog_id.unwrap())
            .unwrap()
            .messages
            .iter()
            .any(|m| m.content().contains("HIDDEN"))
    );
}

// Break caught: the prospective boundary must reject turn three before its hidden input is saved.
#[tokio::test]
async fn turn_limit_allows_exactly_two_autonomous_ordinary_turns() {
    let mut f = Fixture::new(None, TaskStatus::Active).await;
    f.limits(2, 1000);
    f.ordinary(ordinary_response("answer", true)).await;
    for version in 0..3 {
        f.checker.reply(checked(version, None, continue_decision()));
    }
    let result = f.run("implement", |_| Ok(())).await.unwrap();
    assert_eq!(result.stop_reason, AutonomyStopReason::TurnLimit);
    assert_eq!(result.autonomous_turns, 2);
    assert_eq!(result.tokens, 18);
    assert_eq!(f.server.received_requests().await.unwrap().len(), 3);
    assert_eq!(f.count("workflow_inputs"), 3);
}

// Break caught: usage gaps and exhausted tokens must never create hidden controller work.
#[tokio::test]
async fn missing_usage_and_exact_token_limit_preserve_answer_without_controller_input() {
    for missing in [false, true] {
        let mut f = Fixture::new(None, TaskStatus::Active).await;
        f.limits(8, 6);
        f.checker = Arc::new(FakeModel {
            missing_usage: missing,
            ..Default::default()
        });
        f.checker
            .reply(checked(0, Some("unsafe to schedule"), continue_decision()));
        f.ordinary(ordinary_response("complete answer", true)).await;
        let result = f.run("implement", |_| Ok(())).await.unwrap();
        assert_eq!(
            result.stop_reason,
            if missing {
                AutonomyStopReason::MissingUsage
            } else {
                AutonomyStopReason::TokenLimit
            }
        );
        assert_eq!(result.tokens, if missing { 3 } else { 6 });
        assert_eq!(f.count("workflow_inputs"), 1);
        assert_eq!(result.answer.as_deref(), Some("complete answer"));
        assert_eq!(f.current().unwrap().version, 1);
        assert_eq!(
            f.current().unwrap().expected_action.as_deref(),
            Some("unsafe to schedule")
        );
    }
}

// Break caught: auxiliary providers must not silently bypass missing-usage protection.
#[tokio::test]
async fn absent_facts_or_summary_usage_stops_continuation_after_the_completed_answer() {
    for strategy in ["sticky_facts", "summary"] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        f.strategy(strategy, 1, 2);
        f.interpreter.reply(interpretation(
            json!({"type":"continue","instruction":"continue"}),
        ));
        f.checker
            .reply(checked(1, Some("review"), continue_decision()));
        Mock::given(body_string_contains("BASE"))
            .respond_with(ordinary_response("saved answer", true))
            .mount(&f.server)
            .await;
        let (marker, response) = if strategy == "summary" {
            ("Create a faithful cumulative summary", "summary")
        } else {
            ("Update the key-value memory", "{\"language\":\"Rust\"}")
        };
        Mock::given(body_string_contains(marker))
            .respond_with(response_without_usage(response))
            .mount(&f.server)
            .await;
        let result = f.run("continue", |_| Ok(())).await.unwrap();
        assert_eq!(result.stop_reason, AutonomyStopReason::MissingUsage);
        assert_eq!(result.tokens, 9);
        assert!(!result.usage_complete);
        assert_eq!(result.answer.as_deref(), Some("saved answer"));
        assert_eq!(f.count("workflow_inputs"), 2);
        assert_eq!(f.server.received_requests().await.unwrap().len(), 2);
    }
}

// Break caught: advisory policy failures must leave both the streamed answer and state intact.
#[tokio::test]
async fn unsafe_checker_outcomes_preserve_answer_and_state() {
    for outcome in ["malformed", "api", "confidence", "invalid-patch"] {
        let mut f = Fixture::new(None, TaskStatus::Active).await;
        f.ordinary(ordinary_response("ordinary answer", true)).await;
        match outcome {
            "malformed" => f.checker.reply(json!({"bad":"JSON shape"})),
            "api" => {}
            "confidence" => f.checker.reply(checked(
                0,
                Some("do not apply"),
                json!({"type":"continue","instruction":"next","confidence":0.1}),
            )),
            _ => {
                let mut value = checked(0, None, continue_decision());
                value["patch"]["current_step_id"] = json!("unknown");
                f.checker.reply(value);
            }
        }
        let result = f.run("implement", |_| Ok(())).await.unwrap();
        assert_eq!(
            result.stop_reason,
            if outcome == "confidence" {
                AutonomyStopReason::LowConfidence
            } else {
                AutonomyStopReason::CheckerFailed
            }
        );
        assert_eq!(f.current().unwrap().version, 0);
        assert_eq!(f.count("workflow_inputs"), 1);
        assert_eq!(result.answer.as_deref(), Some("ordinary answer"));
        let job = &f
            .store
            .load_pending_processing(f.dialog_id.unwrap())
            .unwrap()[0];
        assert_eq!((job.status, job.attempts), (ProcessingStatus::Failed, 1));
    }
}

struct ProposedChecker {
    name: &'static str,
    mode: CheckerMode,
    patch: TaskStatePatch,
    decision: ControllerDecision,
    seen: Arc<Mutex<Vec<WorkflowTaskState>>>,
}
impl ResponseChecker for ProposedChecker {
    fn name(&self) -> &str {
        self.name
    }
    fn mode(&self) -> CheckerMode {
        self.mode
    }
    fn check<'a>(&'a self, context: &'a CheckContext, _: &'a str) -> CheckFuture<'a> {
        self.seen.lock().unwrap().push(context.task.clone());
        Box::pin(async move {
            Ok(ContinuationCheckResult {
                patch: self.patch.clone(),
                decision: self.decision.clone(),
                usage: Some(usage()),
                raw_output: None,
                output_chars: 0,
            })
        })
    }
}

// Break caught: applying a checker patch early would leak it to later checkers and accept incompatible effects.
#[tokio::test]
async fn pipeline_collects_all_proposals_on_one_snapshot_and_rejects_conflicts() {
    let mut f = Fixture::new(Some(TaskPhase::Planning), TaskStatus::Active).await;
    let state = f.current().unwrap();
    for decision_conflict in [false, true] {
        let seen = Arc::new(Mutex::new(vec![]));
        let checkers: Vec<Arc<dyn ResponseChecker>> = (0..2)
            .map(|i| {
                let mut patch = empty_patch(state.version);
                patch.expected_action = Some(
                    if i == 0 || decision_conflict {
                        "first"
                    } else {
                        "second"
                    }
                    .into(),
                );
                Arc::new(ProposedChecker {
                    name: if i == 0 { "a" } else { "b" },
                    mode: CheckerMode::Advisory,
                    patch,
                    decision: if i == 1 && decision_conflict {
                        ControllerDecision::Continue {
                            instruction: "next".into(),
                            confidence: 0.95,
                        }
                    } else {
                        ControllerDecision::AwaitUser
                    },
                    seen: seen.clone(),
                }) as Arc<dyn ResponseChecker>
            })
            .collect();
        let pipeline = ResponsePipeline::new(checkers).unwrap();
        let mut budget = AutonomyBudget::new(f.config.workflow());
        let outcome = pipeline
            .collect(
                &CheckContext {
                    task: state.clone(),
                    stage_messages: vec![],
                    triggering_input: WorkflowInput {
                        source: WorkflowInputSource::Human,
                        intent: WorkflowIntent::human_continue("human").unwrap(),
                    },
                },
                "answer",
                &mut budget,
                0.8,
            )
            .await;
        assert!(matches!(outcome, PipelineOutcome::Conflict { .. }));
        assert_eq!(*seen.lock().unwrap(), [state.clone(), state.clone()]);
        assert_eq!(budget.tokens(), 6);
        assert_eq!(f.current().unwrap(), state);
        let job = f.pending_answer();
        let outcome = f
            .process(
                job.processing_id,
                &mut budget,
                ProcessingLeaseMode::Normal,
                Some(pipeline),
            )
            .await;
        assert!(matches!(
            outcome,
            deepseek_cli::workflow_engine::ProcessingOutcome::Stop(
                AutonomyStopReason::CheckerFailed
            )
        ));
        assert_eq!(f.current().unwrap(), state);
        assert_eq!(f.count("workflow_inputs"), 1);
        assert!(
            f.store
                .load(f.dialog_id.unwrap())
                .unwrap()
                .messages
                .iter()
                .any(|message| message.content() == "saved answer")
        );
    }
}

#[tokio::test]
async fn blocking_checker_is_rejected_before_streaming_can_start() {
    let checker = ProposedChecker {
        name: "blocking",
        mode: CheckerMode::Blocking,
        patch: empty_patch(0),
        decision: ControllerDecision::AwaitUser,
        seen: Arc::default(),
    };
    assert!(matches!(
        ResponsePipeline::new(vec![Arc::new(checker)]),
        Err(WorkflowEngineError::BlockingCheckerRequiresBufferedDelivery)
    ));
}

#[tokio::test]
async fn budget_rejects_the_same_fingerprint_before_a_second_reservation() {
    let f = Fixture::new(Some(TaskPhase::Planning), TaskStatus::Active).await;
    let mut budget = AutonomyBudget::new(f.config.workflow());
    let fingerprint = StateFingerprint::from(&f.current().unwrap());
    budget.reserve_turn(fingerprint.clone()).unwrap();
    assert_eq!(
        budget.reserve_turn(fingerprint),
        Err(AutonomyStopReason::RepeatedState)
    );
    assert_eq!(budget.turns(), 1);
}

#[tokio::test]
async fn repeated_next_fingerprint_stops_before_hidden_input_commit() {
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    let job = f.pending_answer();
    f.checker.reply(checked(0, None, continue_decision()));
    let mut budget = AutonomyBudget::new(f.config.workflow());
    let mut already_scheduled = f.current().unwrap();
    already_scheduled.version = 1;
    budget
        .reserve_turn(StateFingerprint::from(&already_scheduled))
        .unwrap();
    let outcome = f
        .process(
            job.processing_id,
            &mut budget,
            ProcessingLeaseMode::Normal,
            None,
        )
        .await;
    assert!(matches!(
        outcome,
        deepseek_cli::workflow_engine::ProcessingOutcome::Stop(AutonomyStopReason::RepeatedState)
    ));
    assert_eq!(f.count("workflow_inputs"), 1);
    assert_eq!(f.current().unwrap().version, 0);
}

#[tokio::test]
async fn a_new_human_input_gets_a_fresh_autonomy_budget() {
    let mut f = Fixture::new(None, TaskStatus::Active).await;
    f.limits(1, 1000);
    f.ordinary(ordinary_response("answer", true)).await;
    for version in 0..4 {
        f.checker.reply(checked(version, None, continue_decision()));
    }
    let first = f.run("first human", |_| Ok(())).await.unwrap();
    assert_eq!(first.stop_reason, AutonomyStopReason::TurnLimit);
    assert_eq!(first.autonomous_turns, 1);
    f.interpreter.reply(interpretation(
        json!({"type":"continue","instruction":"second human"}),
    ));
    let second = f.run("second human", |_| Ok(())).await.unwrap();
    assert_eq!(second.stop_reason, AutonomyStopReason::TurnLimit);
    assert_eq!(second.autonomous_turns, 1);
    assert_eq!(second.tokens, 15);
    assert_eq!(f.server.received_requests().await.unwrap().len(), 4);
}

// Break caught: a controller stage change must use the shared handoff/commit boundary, accounting every call.
#[tokio::test]
async fn emitted_transition_uses_handoff_model_and_shared_handler() {
    for done in [false, true] {
        let phase = if done {
            TaskPhase::Validation
        } else {
            TaskPhase::Execution
        };
        let mut f = Fixture::new(Some(phase), TaskStatus::Active).await;
        f.interpreter.reply(interpretation(
            json!({"type":"continue","instruction":"continue"}),
        ));
        f.ordinary(ordinary_response("ordinary answer", true)).await;
        let event = if done {
            "validation_passed"
        } else {
            "execution_completed"
        };
        f.checker.reply(checked(1, None, json!({"type":"emit_transition","event":event,"evidence":["tests pass => 12 tests passed"],"confidence":0.95})));
        f.checker
            .reply(checked(2, None, json!({"type":"await_user"})));
        f.handoff.reply(handoff());
        let result = f.run("continue", |_| Ok(())).await.unwrap();
        assert_eq!(
            result.stop_reason,
            if done {
                AutonomyStopReason::Done
            } else {
                AutonomyStopReason::AwaitUser
            }
        );
        assert_eq!(result.tokens, if done { 12 } else { 18 });
        assert_eq!(result.autonomous_turns, if done { 0 } else { 1 });
        assert_eq!(
            f.current().unwrap().phase,
            if done {
                TaskPhase::Done
            } else {
                TaskPhase::Validation
            }
        );
        assert_eq!(f.count("task_transitions"), 1);
        assert_eq!(f.count("workflow_tasks"), 1);
        assert_eq!(f.handoff.calls(), 1);
        let requests = f.server.received_requests().await.unwrap();
        assert_eq!(requests.len(), if done { 1 } else { 2 });
        if !done {
            assert!(
                !requests[1].body_json::<Value>().unwrap()["messages"]
                    .to_string()
                    .contains("old-stage-only-marker")
            );
        }
    }
}

// Break caught: handoff spending/failure must be checked before any transition or hidden controller input.
#[tokio::test]
async fn handoff_failure_or_budget_exhaustion_keeps_the_outgoing_stage() {
    for mode in ["failure", "tokens", "missing"] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        f.limits(8, if mode == "tokens" { 12 } else { 1000 });
        f.interpreter.reply(interpretation(
            json!({"type":"continue","instruction":"continue"}),
        ));
        f.ordinary(ordinary_response("saved answer", true)).await;
        f.checker.reply(checked(1, Some("review"), json!({"type":"emit_transition","event":"execution_completed","evidence":["build passed"],"confidence":0.95})));
        if mode == "missing" {
            f.handoff = Arc::new(FakeModel {
                missing_usage: true,
                ..Default::default()
            });
        }
        if mode != "failure" {
            f.handoff.reply(handoff());
        }
        let result = f.run("continue", |_| Ok(())).await.unwrap();
        assert_eq!(
            result.stop_reason,
            match mode {
                "failure" => AutonomyStopReason::TransitionFailed,
                "tokens" => AutonomyStopReason::TokenLimit,
                _ => AutonomyStopReason::MissingUsage,
            }
        );
        assert_eq!(f.current().unwrap().phase, TaskPhase::Execution);
        assert_eq!(f.count("task_transitions"), 0);
        assert_eq!(f.count("workflow_inputs"), 2);
        assert_eq!(result.answer.as_deref(), Some("saved answer"));
        assert_eq!(result.tokens, if mode == "tokens" { 12 } else { 9 });
    }
}

// Break caught: restoration must run advisory work without handoff, transition, hidden input, or ordinary work.
#[tokio::test]
async fn recovery_retries_once_then_stops_and_success_never_resumes_autonomously() {
    for decision in [
        continue_decision(),
        json!({"type":"emit_transition","event":"execution_completed","evidence":["observed build"],"confidence":0.95}),
    ] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let task = f.current().unwrap();
        f.store
            .append_answer_for_processing(AnswerCommit {
                dialog_id: task.dialog_id,
                task_id: task.id,
                stage_run_id: task.current_stage_run_id,
                expected_version: task.version,
                content: "saved answer",
                usage: Some(usage()),
            })
            .unwrap();
        f.checker
            .reply(checked(0, Some("review saved work"), decision));
        let recovered = f.recover().await;
        assert_eq!(
            recovered[0].stop_reason,
            AutonomyStopReason::AwaitUserAfterRestart
        );
        assert_eq!(
            f.current().unwrap().expected_action.as_deref(),
            Some("review saved work")
        );
        assert_eq!(f.current().unwrap().phase, TaskPhase::Execution);
        assert_eq!(f.handoff.calls(), 0);
        assert_eq!(f.count("workflow_inputs"), 1);
        assert_eq!(f.server.received_requests().await.unwrap().len(), 0);
        assert!(f.recover().await.is_empty());
    }
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    let task = f.current().unwrap();
    f.store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: task.dialog_id,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            content: "saved",
            usage: Some(usage()),
        })
        .unwrap();
    assert_eq!(
        f.recover().await[0].stop_reason,
        AutonomyStopReason::CheckerFailed
    );
    assert_eq!(
        f.store.load_pending_processing(task.dialog_id).unwrap()[0].attempts,
        1
    );
    assert_eq!(
        f.recover().await[0].stop_reason,
        AutonomyStopReason::CheckerFailed
    );
    assert!(f.recover().await.is_empty());
    let row: (String, u32) = f
        .connection
        .query_row("SELECT status,attempts FROM response_processing", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(row, ("failed".into(), 2));
}

// Break caught: completed processing replay must return durable outcomes without rechecking or scheduling work.
#[tokio::test]
async fn completed_processing_replay_never_duplicates_patch_controller_or_transition() {
    use deepseek_cli::workflow_engine::ProcessingOutcome;
    for kind in ["await", "continue", "transition"] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let job = f.pending_answer();
        let decision = match kind {
            "await" => json!({"type":"await_user"}),
            "continue" => continue_decision(),
            _ => {
                json!({"type":"emit_transition","event":"execution_completed","evidence":["build passed"],"confidence":0.95})
            }
        };
        f.checker.reply(checked(0, Some("review"), decision));
        f.handoff.reply(handoff());
        let mut budget = AutonomyBudget::new(f.config.workflow());
        f.process(
            job.processing_id,
            &mut budget,
            ProcessingLeaseMode::Normal,
            None,
        )
        .await;
        let state = f.current();
        let rows = (f.count("messages"), f.count("task_transitions"));
        let stored = f
            .store
            .load_processing_result(f.dialog_id.unwrap(), job.processing_id)
            .unwrap()
            .unwrap();
        let replay = f
            .process(
                job.processing_id,
                &mut budget,
                ProcessingLeaseMode::Normal,
                None,
            )
            .await;
        assert!(matches!(replay, ProcessingOutcome::Completed(result) if result == stored));
        assert_eq!(f.checker.calls(), 1);
        assert_eq!(f.current(), state);
        assert_eq!((f.count("messages"), f.count("task_transitions")), rows);
    }
}

// Break caught: a stale checker must lose both version and attempt races with no controller effect.
#[tokio::test]
async fn checker_races_reload_state_and_never_emit_controller_work() {
    for race in ["version", "attempt"] {
        let mut f = Fixture::new(None, TaskStatus::Active).await;
        f.ordinary(ordinary_response("saved answer", true)).await;
        f.checker
            .reply(checked(0, Some("stale proposal"), continue_decision()));
        let database = f._directory.path().join("workflow.sqlite3");
        *f.checker.before_reply.lock().unwrap() = Some(Box::new(move || {
            if race == "version" {
                Connection::open(database)
                    .unwrap()
                    .execute(
                        "UPDATE workflow_tasks SET version=1,expected_action='competing session'",
                        [],
                    )
                    .unwrap();
            } else {
                DialogStore::open(&database)
                    .unwrap()
                    .lease_processing(1, 0, ProcessingLeaseMode::Recovery)
                    .unwrap();
            }
        }));
        let result = f.run("implement", |_| Ok(())).await.unwrap();
        assert_eq!(result.stop_reason, AutonomyStopReason::CheckerFailed);
        assert_eq!(f.count("workflow_inputs"), 1);
        assert_eq!(f.count("task_transitions"), 0);
        if race == "version" {
            assert_eq!(
                result.final_state.unwrap().expected_action.as_deref(),
                Some("competing session")
            );
        }
        assert_eq!(result.answer.as_deref(), Some("saved answer"));
    }
}

// Break caught: recovery must close stale jobs before checker calls and after a recovered patch advances version.
#[tokio::test]
async fn recovery_closes_stale_jobs_and_preserves_paused_stage() {
    for paused in [false, true] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let first = f.pending_answer();
        let second = f.pending_answer();
        let stage = f.current().unwrap().current_stage_run_id;
        if paused {
            f.connection
                .execute("UPDATE workflow_tasks SET status='paused'", [])
                .unwrap();
        }
        f.checker.reply(checked(
            0,
            Some("review recovered patch"),
            continue_decision(),
        ));
        assert_eq!(f.recover().await.len(), 1);
        assert_eq!(f.checker.calls(), 1);
        assert_eq!(
            f.current().unwrap().status,
            if paused {
                TaskStatus::Paused
            } else {
                TaskStatus::Active
            }
        );
        assert_eq!(f.current().unwrap().current_stage_run_id, stage);
        let first_status: String = f
            .connection
            .query_row(
                "SELECT status FROM response_processing WHERE id=?1",
                [first.processing_id],
                |r| r.get(0),
            )
            .unwrap();
        let stale: (String, u32, String) = f
            .connection
            .query_row(
                "SELECT status,attempts,last_error FROM response_processing WHERE id=?1",
                [second.processing_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(first_status, "completed");
        assert_eq!(stale, ("failed".into(), 2, "stale task version".into()));
        assert_eq!(f.count("workflow_inputs"), 1);
    }
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    f.pending_answer();
    f.connection
        .execute(
            "UPDATE response_processing SET result_json=?1",
            [checked(0, None, continue_decision()).to_string()],
        )
        .unwrap();
    f.connection
        .execute("UPDATE workflow_tasks SET version=1", [])
        .unwrap();
    assert!(f.recover().await.is_empty());
    assert_eq!(f.checker.calls(), 0);
    assert_eq!(f.count("workflow_inputs"), 1);
}

#[tokio::test]
async fn recovery_reclaims_crash_left_processing_and_paused_failures_are_bounded() {
    for succeeds in [false, true] {
        let mut f = Fixture::new(Some(TaskPhase::Planning), TaskStatus::Active).await;
        let job = f.pending_answer();
        f.store
            .lease_processing(job.processing_id, 0, ProcessingLeaseMode::Normal)
            .unwrap();
        f.connection
            .execute("UPDATE workflow_tasks SET status='paused'", [])
            .unwrap();
        if succeeds {
            f.checker
                .reply(checked(0, None, json!({"type":"await_user"})));
        }
        let result = f.recover().await;
        assert_eq!(
            result[0].stop_reason,
            if succeeds {
                AutonomyStopReason::AwaitUserAfterRestart
            } else {
                AutonomyStopReason::CheckerFailed
            }
        );
        assert_eq!(f.current().unwrap().status, TaskStatus::Paused);
        assert!(f.recover().await.is_empty());
        let attempts: u32 = f
            .connection
            .query_row("SELECT attempts FROM response_processing", [], |r| r.get(0))
            .unwrap();
        assert_eq!(attempts, 2);
    }
}

#[tokio::test]
async fn validation_repair_patch_is_not_applied_without_its_transition() {
    for recovery in [false, true] {
        let mut f = Fixture::new(Some(TaskPhase::Validation), TaskStatus::Active).await;
        let job = f.pending_answer();
        let mut proposal = checked(
            0,
            Some("repair"),
            json!({"type":"emit_transition","event":"validation_failed","evidence":["test failed"],"confidence":0.95}),
        );
        proposal["patch"]["plan_append"]["steps"] =
            json!([{"id":"fix","description":"repair failure","status":"pending"}]);
        f.checker.reply(proposal);
        let before = f.current();
        if recovery {
            assert_eq!(
                f.recover().await[0].stop_reason,
                AutonomyStopReason::AwaitUserAfterRestart
            );
        } else {
            f.limits(1, 1);
            let mut budget = AutonomyBudget::new(f.config.workflow());
            let result = f
                .process(
                    job.processing_id,
                    &mut budget,
                    ProcessingLeaseMode::Normal,
                    None,
                )
                .await;
            assert!(matches!(
                result,
                deepseek_cli::workflow_engine::ProcessingOutcome::Stop(
                    AutonomyStopReason::TokenLimit
                )
            ));
        }
        assert_eq!(f.current(), before);
        assert_eq!(f.handoff.calls(), 0);
        assert_eq!(f.count("workflow_inputs"), 1);
        assert!(
            f.store
                .load_pending_processing(f.dialog_id.unwrap())
                .unwrap()
                .is_empty()
        );
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
        assert_eq!(
            f.checker.calls(),
            if case.kind == "managed" {
                case.calls
            } else {
                0
            }
        );
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
    assert_eq!(pending[0].status, ProcessingStatus::Failed);
    assert_eq!(pending[0].attempts, 1);
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

// Break caught: a replayed controller message must never be paired with a newer task snapshot.
async fn assert_controller_continue_rejects_stale_route(historical_replay: bool) {
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    let task = f.current().unwrap();
    let snapshot = f.store.load_workflow(task.dialog_id).unwrap();
    let answer = f
        .store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: task.dialog_id,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            content: "completed work",
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
    let patch = empty_patch(task.version);
    let input = WorkflowInput {
        source: WorkflowInputSource::Controller {
            checker: "continuation".into(),
            model: "checker".into(),
            triggering_assistant_message_id: answer.message_id,
        },
        intent: WorkflowIntent::human_continue("hidden follow-up").unwrap(),
    };
    if historical_replay {
        f.store
            .commit_controller_decision(ControllerInputCommit {
                processing_id: answer.processing_id,
                expected_attempt: 1,
                task_id: task.id,
                stage_run_id: task.current_stage_run_id,
                expected_version: task.version,
                checker: "continuation",
                model: "checker",
                triggering_assistant_message_id: answer.message_id,
                instruction: "hidden follow-up",
                intent: &input.intent,
                confidence: 0.95,
                accepted_patch: &patch,
            })
            .unwrap();
        let newer = WorkflowInput {
            source: WorkflowInputSource::Human,
            intent: WorkflowIntent::human_continue("new human instruction").unwrap(),
        };
        f.store
            .append_input(
                InputCommit {
                    dialog_id: task.dialog_id,
                    input: &newer,
                    protocol_text: "new human instruction",
                    confidence: None,
                    expected_current_task: ExpectedCurrentTask::Present {
                        task_id: task.id,
                        version: task.version + 1,
                    },
                },
                AcceptedInputEffect::ContinueSameStage,
            )
            .unwrap();
    } else {
        // Mutate at the durable completion boundary, before the handler reloads the task.
        f.connection.execute_batch("CREATE TRIGGER interleaved_task_change AFTER UPDATE OF status ON response_processing WHEN NEW.status='completed' BEGIN UPDATE workflow_tasks SET version=version+1; END;").unwrap();
    }
    let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
    let result = WorkflowInputHandler {
        store: &mut f.store,
        handoff_builder: &builder,
    }
    .handle(
        task.dialog_id,
        snapshot,
        input,
        "hidden follow-up",
        Some(0.95),
        InputHandlingContext {
            processing_id: Some(answer.processing_id),
            processing_attempt: Some(1),
            accepted_patch: Some(&patch),
        },
    )
    .await;
    assert!(
        matches!(result, Err(WorkflowEngineError::Store(StoreError::WorkflowConflict(id))) if id == task.dialog_id),
        "historical={historical_replay}: {result:?}"
    );
    assert_eq!(f.current().unwrap().version, task.version + 2);
    assert_eq!(f.count("messages"), if historical_replay { 4 } else { 3 });
    assert_eq!(f.count("response_processing"), 1);
    assert!(f.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn controller_continue_rejects_historical_replay() {
    assert_controller_continue_rejects_stale_route(true).await;
}

#[tokio::test]
async fn controller_continue_rejects_interleaving_mutation() {
    assert_controller_continue_rejects_stale_route(false).await;
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
                processing_attempt: Some(1),
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
            expected_attempt: 1,
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
    f.checker
        .reply(checked(2, None, json!({"type":"await_user"})));
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
    assert_eq!(
        result.tokens, 12,
        "interpreter, facts, ordinary, checker all consume budget"
    );
    assert!(result.usage_complete);
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
        f.checker
            .reply(checked(1, None, json!({"type":"await_user"})));
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
        assert_eq!(result.tokens, if failure { 9 } else { 12 });
        assert_eq!(result.usage_complete, !failure);
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

// Break caught: a service adapter rejecting a blank answer must not turn reported usage into MissingUsage.
#[tokio::test]
async fn real_adapter_failed_checker_interpreter_and_handoff_keep_budget_usage() {
    use wiremock::matchers::body_partial_json;
    for service in ["checker", "interpreter", "handoff"] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        f.interpreter.reply(interpretation(
            json!({"type":"continue","instruction":"continue"}),
        ));
        f.checker.reply(checked(1, None, if service == "handoff" {
            json!({"type":"emit_transition","event":"execution_completed","evidence":["observed build"],"confidence":0.95})
        } else { json!({"type":"await_user"}) }));
        f.ordinary(ordinary_response("saved answer", true)).await;
        let chunk = json!({"choices":[{"delta":{"content":" \t\n "},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":12,"completion_tokens":5,"total_tokens":17}});
        Mock::given(body_partial_json(json!({"model":"real-service"})))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n")),
            )
            .with_priority(1)
            .mount(&f.server)
            .await;
        let model = Arc::new(
            DeepSeekCompletionModel::new(f.client.clone(), "real-service".into()).unwrap(),
        );
        let mut models = f.models();
        match service {
            "checker" => models.checker = model,
            "interpreter" => models.interpreter = model,
            _ => models.handoff = model,
        }
        let result = f
            .run_with_models("continue", &models, |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(
            result.tokens,
            if service == "handoff" { 26 } else { 23 },
            "{service}"
        );
        assert!(result.usage_complete, "{service}");
        assert_eq!(
            result.stop_reason,
            match service {
                "checker" => AutonomyStopReason::CheckerFailed,
                "interpreter" => AutonomyStopReason::AwaitUser,
                _ => AutonomyStopReason::TransitionFailed,
            }
        );
        assert_eq!(result.answer.as_deref(), Some("saved answer"));
        assert_eq!(f.current().unwrap().phase, TaskPhase::Execution);
        assert_eq!(f.count("workflow_inputs"), 2);
        assert_eq!(f.count("task_transitions"), 0);
        assert_eq!(f.server.received_requests().await.unwrap().len(), 2);
    }
}

// Break caught: summary failure must retain its known usage while failing open to the saved ordinary answer.
#[tokio::test]
async fn failed_summary_keeps_reported_usage_in_the_human_budget() {
    for complete in [false, true] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        f.strategy("summary", 1, 2);
        f.interpreter.reply(interpretation(
            json!({"type":"continue","instruction":"continue"}),
        ));
        f.checker
            .reply(checked(1, None, json!({"type":"await_user"})));
        Mock::given(body_string_contains("BASE"))
            .respond_with(ordinary_response("saved answer", true))
            .mount(&f.server)
            .await;
        let chunk = json!({"choices":[{"delta":{"content":" \n\t "},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":12,"completion_tokens":5,"total_tokens":17}});
        Mock::given(body_string_contains("Create a faithful cumulative summary"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!(
                        "data: {chunk}\n\n{}",
                        if complete { "data: [DONE]\n\n" } else { "" }
                    )),
            )
            .mount(&f.server)
            .await;
        let mut failed = false;
        let result = f
            .run("continue", |event| {
                if matches!(event, AgentEvent::CompactionFailed { .. }) {
                    failed = true;
                }
                Ok(())
            })
            .await
            .unwrap();
        assert!(failed);
        assert_eq!(result.tokens, 26);
        assert!(result.usage_complete);
        assert_eq!(result.stop_reason, AutonomyStopReason::AwaitUser);
        assert_eq!(result.answer.as_deref(), Some("saved answer"));
        assert!(
            f.store
                .load_stage_reductions(f.current().unwrap().current_stage_run_id)
                .unwrap()
                .context
                .summary()
                .is_none()
        );
        assert_eq!(f.server.received_requests().await.unwrap().len(), 2);
    }
}

// Break caught: optional summary must not spend beyond the scheduling boundary; the mandatory checker still runs.
#[tokio::test]
async fn summary_is_skipped_at_token_equality_ordinary_overshoot_and_missing_usage() {
    for (case, limit, ordinary_tokens, expected_tokens) in [
        ("equality", 6, 3, 9),
        ("overshoot", 10, 11, 17),
        ("missing", 1000, 3, 6),
    ] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        f.config = Config::from_toml(&format!("api_key='test-key'\nbase_url='{}'\nsystem_prompt='BASE'\n[workflow]\nmax_autonomous_tokens={limit}\n[context]\nstrategy='summary'\ncompact_after_prompt_tokens=1\nkeep_last_messages=2", f.server.uri()), None).unwrap();
        if case == "missing" {
            f.interpreter = Arc::new(FakeModel {
                missing_usage: true,
                ..Default::default()
            });
        }
        f.interpreter.reply(interpretation(
            json!({"type":"continue","instruction":"continue"}),
        ));
        f.checker.reply(checked(1, None, continue_decision()));
        let chunk = json!({"choices":[{"delta":{"content":"saved answer"},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":ordinary_tokens - 1,"completion_tokens":1,"total_tokens":ordinary_tokens}});
        Mock::given(body_string_contains("BASE"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n")),
            )
            .mount(&f.server)
            .await;
        Mock::given(body_string_contains("Create a faithful cumulative summary"))
            .respond_with(ordinary_response("summary must not run", true))
            .mount(&f.server)
            .await;
        let mut compaction_started = 0;
        let result = f
            .run("continue", |event| {
                if matches!(event, AgentEvent::CompactionStarted { .. }) {
                    compaction_started += 1;
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(compaction_started, 0, "{case}");
        assert_eq!(
            f.server.received_requests().await.unwrap().len(),
            1,
            "{case}"
        );
        assert_eq!(
            f.checker.calls(),
            1,
            "mandatory checker must still finish processing"
        );
        assert_eq!(result.tokens, expected_tokens, "{case}");
        assert_eq!(result.usage_complete, case != "missing");
        assert_eq!(
            result.stop_reason,
            if case == "missing" {
                AutonomyStopReason::MissingUsage
            } else {
                AutonomyStopReason::TokenLimit
            }
        );
        assert_eq!(result.answer.as_deref(), Some("saved answer"));
        assert_eq!(f.count("workflow_inputs"), 2);
        assert!(
            f.store
                .load_stage_reductions(f.current().unwrap().current_stage_run_id)
                .unwrap()
                .context
                .summary()
                .is_none()
        );
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
                processing_attempt: Some(1),
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

// Break caught: every human context field must be absent, and every controller context field present.
#[tokio::test]
async fn input_context_requires_complete_controller_lease_and_no_human_lease() {
    for (human, has_id, has_attempt, has_patch) in [
        (true, true, false, false),
        (true, false, true, false),
        (true, false, false, true),
        (false, false, true, true),
        (false, true, false, true),
        (false, true, true, false),
    ] {
        let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
        let task = f.current().unwrap();
        let patch = empty_patch(task.version);
        let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
        let snapshot = f.store.load_workflow(task.dialog_id).unwrap();
        let result = WorkflowInputHandler {
            store: &mut f.store,
            handoff_builder: &builder,
        }
        .handle(
            task.dialog_id,
            snapshot,
            WorkflowInput {
                source: if human {
                    WorkflowInputSource::Human
                } else {
                    WorkflowInputSource::Controller {
                        checker: "continuation".into(),
                        model: "checker".into(),
                        triggering_assistant_message_id: 1,
                    }
                },
                intent: WorkflowIntent::human_continue("continue").unwrap(),
            },
            "continue",
            Some(0.95),
            InputHandlingContext {
                processing_id: has_id.then_some(1),
                processing_attempt: has_attempt.then_some(1),
                accepted_patch: has_patch.then_some(&patch),
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(WorkflowEngineError::InvalidInputContext(_))
        ));
        assert_eq!(f.current().unwrap(), task);
        assert_eq!(f.count("messages"), 1);
        assert_eq!(f.handoff.calls(), 0);
    }
}

// Break caught: the handler must forward the captured attempt, never default to or reload an attempt.
#[tokio::test]
async fn controller_handler_fences_and_forwards_the_exact_leased_attempt() {
    for route in ["continue", "transition", "reject"] {
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
        f.store
            .lease_processing(
                answer.processing_id,
                task.version,
                ProcessingLeaseMode::Recovery,
            )
            .unwrap();
        let input = WorkflowInput {
            source: WorkflowInputSource::Controller {
                checker: "continuation".into(),
                model: "checker".into(),
                triggering_assistant_message_id: answer.message_id,
            },
            intent: match route {
                "continue" => WorkflowIntent::human_continue("hidden instruction").unwrap(),
                "transition" => WorkflowIntent::ProposeTransition {
                    event: TransitionEvent::ExecutionCompleted,
                    evidence: vec![],
                },
                "reject" => WorkflowIntent::ProposeTransition {
                    event: TransitionEvent::ValidationPassed,
                    evidence: vec![],
                },
                _ => unreachable!(),
            },
        };
        let patch = empty_patch(task.version);
        f.handoff.reply(handoff());
        f.handoff.reply(handoff());
        let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
        for attempt in [1, 2] {
            let snapshot = f.store.load_workflow(task.dialog_id).unwrap();
            let result = WorkflowInputHandler {
                store: &mut f.store,
                handoff_builder: &builder,
            }
            .handle(
                task.dialog_id,
                snapshot,
                input.clone(),
                "hidden instruction",
                Some(0.95),
                InputHandlingContext {
                    processing_id: Some(answer.processing_id),
                    processing_attempt: Some(attempt),
                    accepted_patch: Some(&patch),
                },
            )
            .await;
            if attempt == 1 {
                assert!(
                    matches!(
                        result,
                        Err(WorkflowEngineError::Store(StoreError::WorkflowConflict(_)))
                    ),
                    "{route}: {result:?}"
                );
                assert_eq!(f.current().unwrap(), task);
                assert_eq!(f.count("messages"), 2);
            } else {
                assert!(
                    matches!(result, Ok(RoutingOutcome::Managed { .. }))
                        || (route == "reject"
                            && matches!(result, Ok(RoutingOutcome::Rejected { .. })))
                );
            }
        }
        let (status, attempts): (String, u32) = f
            .connection
            .query_row(
                "SELECT status,attempts FROM response_processing WHERE id=?1",
                [answer.processing_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            status,
            if route == "reject" {
                "failed"
            } else {
                "completed"
            }
        );
        assert_eq!(attempts, 2);
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
            processing_attempt: Some(1),
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

// Break caught: rejecting input from one dialog must not fail an unrelated dialog's leased job.
#[tokio::test]
async fn controller_rejection_cannot_fail_another_dialogs_processing() {
    let mut f = Fixture::new(Some(TaskPhase::Execution), TaskStatus::Active).await;
    let source = f.current().unwrap();
    let other = f
        .store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "unrelated task")
        .unwrap();
    let answer = f
        .store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: other.dialog_id,
            task_id: other.task.id,
            stage_run_id: other.stage_run_id,
            expected_version: other.task.version,
            content: "unrelated answer",
            usage: None,
        })
        .unwrap();
    f.store
        .lease_processing(
            answer.processing_id,
            other.task.version,
            ProcessingLeaseMode::Normal,
        )
        .unwrap();
    let snapshot = f.store.load_workflow(source.dialog_id).unwrap();
    let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
    let patch = empty_patch(source.version);
    let result = WorkflowInputHandler {
        store: &mut f.store,
        handoff_builder: &builder,
    }
    .handle(
        source.dialog_id,
        snapshot,
        WorkflowInput {
            source: WorkflowInputSource::Controller {
                checker: "continuation".into(),
                model: "checker".into(),
                triggering_assistant_message_id: answer.message_id,
            },
            intent: WorkflowIntent::ProposeTransition {
                event: TransitionEvent::ValidationPassed,
                evidence: vec![],
            },
        },
        "invalid transition",
        Some(0.95),
        InputHandlingContext {
            processing_id: Some(answer.processing_id),
            processing_attempt: Some(1),
            accepted_patch: Some(&patch),
        },
    )
    .await;
    assert!(result.is_err(), "{result:?}");
    let job = f
        .store
        .load_pending_processing(other.dialog_id)
        .unwrap()
        .remove(0);
    assert_eq!(job.status, ProcessingStatus::Processing);
    assert!(job.last_error.is_none());
    assert_eq!(f.current().unwrap(), source);
    assert_eq!(f.count("messages"), 3);
    assert_eq!(f.handoff.calls(), 0);
}

// Break caught: FSM and handoff errors may contain sensitive input, which must stay out of diagnostics.
async fn assert_human_rejection_diagnostic_is_sanitized(failure: &str) {
    const SECRET: &str = "SECRET_HUMAN_PAYLOAD_5fe8";
    let mut f = Fixture::new(
        Some(if failure == "handoff" {
            TaskPhase::Execution
        } else {
            TaskPhase::Validation
        }),
        TaskStatus::Active,
    )
    .await;
    if failure == "criterion" {
        let mut plan = f.current().unwrap().plan;
        plan.acceptance_criteria.push(SECRET.into());
        f.connection
            .execute(
                "UPDATE workflow_tasks SET plan_json=?1",
                [serde_json::to_string(&plan).unwrap()],
            )
            .unwrap();
    }
    let evidence = if failure == "evidence" {
        vec![SECRET]
    } else {
        vec!["tests pass => observed pass"]
    };
    let event = if failure == "handoff" {
        "execution_completed"
    } else {
        "validation_passed"
    };
    f.interpreter.reply(interpretation(
        json!({"type":"propose_transition","event":event,"evidence":evidence}),
    ));
    f.handoff.reply(json!({SECRET: SECRET}));
    let mut events = vec![];
    let result = f
        .run(SECRET, |event| {
            if let AgentEvent::Workflow(WorkflowTurnEvent::InputRejected { reason }) = event {
                events.push(reason.clone());
            }
            Ok(())
        })
        .await
        .unwrap();
    let RoutingOutcome::Rejected { reason, .. } = result.routing else {
        panic!("expected rejection")
    };
    assert!(!reason.contains(SECRET), "{failure}: {reason}");
    assert!(reason.len() <= 80);
    assert_eq!(events, vec![reason.clone()]);
    let stored: String = f
        .connection
        .query_row(
            "SELECT rejection_reason FROM workflow_inputs ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, reason);
    if failure == "evidence" {
        let audit: String = f
            .connection
            .query_row(
                "SELECT intent_json FROM workflow_inputs ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            audit.contains(SECRET),
            "typed intent remains the explicit audit payload"
        );
    }
    assert!(f.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn human_rejection_diagnostics_hide_criterion() {
    assert_human_rejection_diagnostic_is_sanitized("criterion").await;
}

#[tokio::test]
async fn human_rejection_diagnostics_hide_evidence() {
    assert_human_rejection_diagnostic_is_sanitized("evidence").await;
}

#[tokio::test]
async fn human_rejection_diagnostics_hide_handoff_fields() {
    assert_human_rejection_diagnostic_is_sanitized("handoff").await;
}

// Break caught: controller guard/patch/handoff failures must store only a bounded diagnostic category.
async fn assert_controller_rejection_diagnostic_is_sanitized(failure: &str) {
    const SECRET: &str = "SECRET_CONTROLLER_PAYLOAD_2f85";
    let mut f = Fixture::new(
        Some(if failure == "handoff" {
            TaskPhase::Execution
        } else {
            TaskPhase::Validation
        }),
        TaskStatus::Active,
    )
    .await;
    if failure == "criterion" {
        let mut plan = f.current().unwrap().plan;
        plan.acceptance_criteria.push(SECRET.into());
        f.connection
            .execute(
                "UPDATE workflow_tasks SET plan_json=?1",
                [serde_json::to_string(&plan).unwrap()],
            )
            .unwrap();
    }
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
    let mut patch = empty_patch(task.version);
    if failure == "patch" || failure == "continue_patch" {
        patch.step_updates.push(StepStatusUpdate {
            step_id: SECRET.into(),
            status: PlanStepStatus::Completed,
            evidence: vec![SECRET.into()],
        });
    }
    let input = WorkflowInput {
        source: WorkflowInputSource::Controller {
            checker: "continuation".into(),
            model: "checker".into(),
            triggering_assistant_message_id: answer.message_id,
        },
        intent: if failure == "continue_patch" {
            WorkflowIntent::human_continue(SECRET).unwrap()
        } else {
            WorkflowIntent::ProposeTransition {
                event: if failure == "handoff" {
                    TransitionEvent::ExecutionCompleted
                } else {
                    TransitionEvent::ValidationPassed
                },
                evidence: if failure == "evidence" {
                    vec![SECRET.into()]
                } else {
                    vec!["tests pass => observed pass".into()]
                },
            }
        },
    };
    f.handoff.reply(json!({SECRET: SECRET}));
    let snapshot = f.store.load_workflow(task.dialog_id).unwrap();
    let builder = HandoffBuilder::new(f.handoff.clone(), f.config.workflow());
    let route = WorkflowInputHandler {
        store: &mut f.store,
        handoff_builder: &builder,
    }
    .handle(
        task.dialog_id,
        snapshot,
        input,
        SECRET,
        Some(0.95),
        InputHandlingContext {
            processing_id: Some(answer.processing_id),
            processing_attempt: Some(1),
            accepted_patch: Some(&patch),
        },
    )
    .await
    .unwrap();
    let RoutingOutcome::Rejected { reason, .. } = route else {
        panic!("expected rejection")
    };
    assert!(!reason.contains(SECRET), "{failure}: {reason}");
    assert!(reason.len() <= 80);
    let job = f
        .store
        .load_pending_processing(task.dialog_id)
        .unwrap()
        .remove(0);
    assert_eq!(job.status, ProcessingStatus::Failed);
    assert_eq!(job.last_error.as_deref(), Some(reason.as_str()));
    assert_eq!(f.count("messages"), 2);
    assert_eq!(f.count("workflow_inputs"), 1);
    assert_eq!(f.current().unwrap(), task);
}

#[tokio::test]
async fn controller_rejection_diagnostics_hide_criterion() {
    assert_controller_rejection_diagnostic_is_sanitized("criterion").await;
}

#[tokio::test]
async fn controller_rejection_diagnostics_hide_evidence() {
    assert_controller_rejection_diagnostic_is_sanitized("evidence").await;
}

#[tokio::test]
async fn controller_rejection_diagnostics_hide_patch_payload() {
    assert_controller_rejection_diagnostic_is_sanitized("patch").await;
}

#[tokio::test]
async fn controller_rejection_diagnostics_hide_continue_patch_payload() {
    assert_controller_rejection_diagnostic_is_sanitized("continue_patch").await;
}

#[tokio::test]
async fn controller_rejection_diagnostics_hide_handoff_fields() {
    assert_controller_rejection_diagnostic_is_sanitized("handoff").await;
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
