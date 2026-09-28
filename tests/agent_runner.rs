mod common;

use std::{
    collections::VecDeque,
    future::pending,
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use deepseek_cli::{
    agent_runner::{
        AgentError, AgentEvent, AgentInput, AgentRunner, InteractiveService, ToolEventCode,
    },
    domain::{DialogId, ToolOwner, ToolRunStatus},
    provider::{
        AssistantTurn, ModelToolCall, ModelToolDefinition, Provider, ProviderError, ProviderFuture,
        ProviderMessage, TokenUsage,
    },
    store::{MessageRole, SafeErrorCode, Store},
    tools::{ToolExecutionResult, ToolExecutor, ToolFuture, ToolRoute},
};
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const REQUEST_LIMIT: usize = 512 * 1024;
const MESSAGE_LIMIT: usize = 256 * 1024;

#[derive(Clone)]
enum ProviderReply {
    Turn(AssistantTurn),
    Error(&'static str),
}

struct FakeProvider {
    replies: Mutex<VecDeque<ProviderReply>>,
    requests: Mutex<Vec<Vec<Value>>>,
    entered: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
}

impl FakeProvider {
    fn new(replies: impl IntoIterator<Item = ProviderReply>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
            entered: None,
            release: None,
        })
    }

    fn blocking(reply: ProviderReply, entered: Arc<Notify>, release: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(VecDeque::from([reply])),
            requests: Mutex::new(Vec::new()),
            entered: Some(entered),
            release: Some(release),
        })
    }

    fn requests(&self) -> Vec<Vec<Value>> {
        self.requests.lock().unwrap().clone()
    }
}

impl Provider for FakeProvider {
    fn stream_turn<'a>(
        &'a self,
        messages: &'a [ProviderMessage],
        _tools: &'a [ModelToolDefinition],
        text_sink: &'a mut (dyn FnMut(&str) -> io::Result<()> + Send),
    ) -> ProviderFuture<'a> {
        let messages = messages
            .iter()
            .map(|message| serde_json::to_value(message).unwrap())
            .collect::<Vec<_>>();
        self.requests.lock().unwrap().push(messages);
        let reply = self.replies.lock().unwrap().pop_front().unwrap();
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            if let Some(entered) = entered {
                entered.notify_one();
            }
            if let Some(release) = release {
                release.notified().await;
            }
            match reply {
                ProviderReply::Turn(turn) => {
                    if let AssistantTurn::FinalText { content, .. } = &turn {
                        text_sink(content).map_err(|_| ProviderError::safe("output"))?;
                    }
                    Ok(turn)
                }
                ProviderReply::Error(code) => Err(ProviderError::safe(code)),
            }
        })
    }

    fn serialized_request_len(
        &self,
        messages: &[ProviderMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<usize, ProviderError> {
        let tools = tools
            .iter()
            .map(|tool| {
                json!({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.parameters,
                    }
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&json!({"messages": messages, "tools": tools}))
            .map(|request| request.len())
            .map_err(|_| ProviderError::safe("serialization"))
    }
}

#[derive(Clone)]
enum ToolBehavior {
    Immediate(ToolExecutionResult),
    Pending,
}

struct FakeTools {
    definitions: Vec<ModelToolDefinition>,
    behavior: ToolBehavior,
    calls: Mutex<Vec<String>>,
    active: Mutex<usize>,
    max_active: Mutex<usize>,
    entered: Option<Arc<Notify>>,
}

impl FakeTools {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            definitions: Vec::new(),
            behavior: ToolBehavior::Immediate(success("unused")),
            calls: Mutex::new(Vec::new()),
            active: Mutex::new(0),
            max_active: Mutex::new(0),
            entered: None,
        })
    }

    fn with_definitions(
        definitions: Vec<ModelToolDefinition>,
        behavior: ToolBehavior,
        entered: Option<Arc<Notify>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            definitions,
            behavior,
            calls: Mutex::new(Vec::new()),
            active: Mutex::new(0),
            max_active: Mutex::new(0),
            entered,
        })
    }
}

struct ActiveGuard<'a>(&'a Mutex<usize>);

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        *self.0.lock().unwrap() -= 1;
    }
}

impl ToolExecutor for FakeTools {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        self.definitions
            .iter()
            .find(|definition| definition.name == name)
            .map(|definition| ToolRoute {
                server_name: "fixture",
                tool_name: definition.name.as_str(),
            })
    }

    fn is_read_only(&self, name: &str) -> Option<bool> {
        self.definitions
            .iter()
            .find(|definition| definition.name == name)
            .map(|definition| definition.read_only)
    }

    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a> {
        self.calls.lock().unwrap().push(call.name.clone());
        let mut active = self.active.lock().unwrap();
        *active += 1;
        let current = *active;
        drop(active);
        let mut max_active = self.max_active.lock().unwrap();
        *max_active = (*max_active).max(current);
        drop(max_active);
        if let Some(entered) = &self.entered {
            entered.notify_one();
        }
        let guard = ActiveGuard(&self.active);
        let behavior = self.behavior.clone();
        Box::pin(async move {
            let _guard = guard;
            match behavior {
                ToolBehavior::Immediate(result) => Ok(result),
                ToolBehavior::Pending => pending().await,
            }
        })
    }
}

struct Fixture {
    _directory: TempDir,
    path: std::path::PathBuf,
    store: Store,
    dialog_id: DialogId,
}

fn fixture() -> Fixture {
    let directory = common::private_tempdir();
    let path = directory.path().join("agent.sqlite3");
    let store = Store::open(&path).unwrap();
    let dialog_id = store.create_dialog("dialog").unwrap().id;
    Fixture {
        _directory: directory,
        path,
        store,
        dialog_id,
    }
}

fn runner(store: &Store, provider: Arc<dyn Provider>, tools: Arc<dyn ToolExecutor>) -> AgentRunner {
    AgentRunner::new(
        provider,
        tools,
        store.clone(),
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
    )
}

fn service(store: &Store, runner: AgentRunner) -> InteractiveService {
    InteractiveService::new(store.clone(), runner, "SYSTEM")
}

fn final_text(content: &str) -> ProviderReply {
    ProviderReply::Turn(AssistantTurn::FinalText {
        content: content.into(),
        usage: Some(TokenUsage {
            prompt_tokens: 2,
            completion_tokens: 3,
            total_tokens: 5,
            ..TokenUsage::default()
        }),
    })
}

fn definition(name: &str, read_only: bool) -> ModelToolDefinition {
    ModelToolDefinition {
        name: name.into(),
        description: Some("fixture tool".into()),
        parameters: json!({"type":"object"}).as_object().unwrap().clone(),
        read_only,
    }
}

fn call(id: &str, name: &str) -> ModelToolCall {
    ModelToolCall {
        id: id.into(),
        name: name.into(),
        arguments: "{}".into(),
    }
}

#[test]
fn model_tool_call_debug_redacts_arguments() {
    let mut value = call("opaque", "read");
    value.arguments = "{\"secret\":\"private marker\"}".into();
    assert!(!format!("{value:?}").contains("private marker"));
}

fn tool_turn(calls: Vec<ModelToolCall>) -> ProviderReply {
    ProviderReply::Turn(AssistantTurn::ToolCalls {
        content: None,
        calls,
        usage: None,
    })
}

fn success(content: &str) -> ToolExecutionResult {
    ToolExecutionResult {
        content: content.into(),
        is_error: false,
        error_code: None,
        delivery_uncertain: false,
    }
}

fn sink() -> impl FnMut(AgentEvent) -> io::Result<()> + Send {
    |_| Ok(())
}

fn roles_and_content(messages: &[Value]) -> Vec<(&str, &str)> {
    messages
        .iter()
        .map(|message| {
            (
                message["role"].as_str().unwrap(),
                message["content"].as_str().unwrap_or(""),
            )
        })
        .collect()
}

fn turn_rows(path: &std::path::Path) -> Vec<(String, Option<String>)> {
    let db = Connection::open(path).unwrap();
    let mut statement = db
        .prepare("SELECT status,safe_error_code FROM turns ORDER BY id")
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

#[tokio::test]
async fn request_contains_system_all_completed_messages_and_new_user_only() {
    let f = fixture();
    let old = f.store.begin_turn(f.dialog_id, "old user").unwrap();
    f.store.complete_turn(old.turn_id, "old answer").unwrap();
    let provider = FakeProvider::new([final_text("new answer")]);
    let service = service(
        &f.store,
        runner(&f.store, provider.clone(), FakeTools::empty()),
    );

    service
        .send_message(
            f.dialog_id,
            "new user",
            CancellationToken::new(),
            &mut sink(),
        )
        .await
        .unwrap();

    assert_eq!(
        roles_and_content(&provider.requests()[0]),
        vec![
            ("system", "SYSTEM"),
            ("user", "old user"),
            ("assistant", "old answer"),
            ("user", "new user"),
        ]
    );
}

#[tokio::test]
async fn failed_and_interrupted_messages_never_replay() {
    let f = fixture();
    let failed = f.store.begin_turn(f.dialog_id, "failed secret").unwrap();
    f.store
        .fail_turn(failed.turn_id, SafeErrorCode::ProviderError)
        .unwrap();
    let interrupted = f
        .store
        .begin_turn(f.dialog_id, "interrupted secret")
        .unwrap();
    f.store
        .interrupt_turn(interrupted.turn_id, SafeErrorCode::Interrupted)
        .unwrap();
    let provider = FakeProvider::new([final_text("ok")]);

    service(
        &f.store,
        runner(&f.store, provider.clone(), FakeTools::empty()),
    )
    .send_message(
        f.dialog_id,
        "current",
        CancellationToken::new(),
        &mut sink(),
    )
    .await
    .unwrap();

    assert_eq!(
        roles_and_content(&provider.requests()[0]),
        vec![("system", "SYSTEM"), ("user", "current")]
    );
}

#[tokio::test]
async fn tool_messages_exist_only_during_current_loop() {
    let f = fixture();
    let mut private_call = call("call-1", "read");
    private_call.arguments = "{\"chat_id\":\"private marker\"}".into();
    let provider = FakeProvider::new([
        tool_turn(vec![private_call]),
        final_text("first answer"),
        final_text("second answer"),
    ]);
    let tools = FakeTools::with_definitions(
        vec![definition("read", true)],
        ToolBehavior::Immediate(success("tool result")),
        None,
    );
    let service = service(&f.store, runner(&f.store, provider.clone(), tools));

    service
        .send_message(f.dialog_id, "first", CancellationToken::new(), &mut sink())
        .await
        .unwrap();
    service
        .send_message(f.dialog_id, "second", CancellationToken::new(), &mut sink())
        .await
        .unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        f.store.list_tool_runs().unwrap()[0].arguments.as_deref(),
        Some("{\"chat_id\":\"private marker\"}")
    );
    assert_eq!(requests[1][2]["role"], "assistant");
    assert_eq!(requests[1][3]["role"], "tool");
    assert_eq!(
        roles_and_content(&requests[2]),
        vec![
            ("system", "SYSTEM"),
            ("user", "first"),
            ("assistant", "first answer"),
            ("user", "second"),
        ]
    );
}

#[tokio::test]
async fn every_provider_dispatch_enforces_512_kib() {
    let f = fixture();
    let provider = FakeProvider::new([
        tool_turn(vec![call("call-1", "read")]),
        final_text("must not dispatch"),
    ]);
    let tools = FakeTools::with_definitions(
        vec![definition("read", true)],
        ToolBehavior::Immediate(success(&"x".repeat(600_000))),
        None,
    );
    let service = service(&f.store, runner(&f.store, provider.clone(), tools));

    let error = service
        .send_message(f.dialog_id, "go", CancellationToken::new(), &mut sink())
        .await
        .unwrap_err();

    assert_eq!(error, AgentError::ContextTooLong);
    assert_eq!(provider.requests().len(), 1);

    let huge_schema = json!({"type":"object","description":"x".repeat(600_000)})
        .as_object()
        .unwrap()
        .clone();
    let definitions = vec![ModelToolDefinition {
        name: "huge".into(),
        description: None,
        parameters: huge_schema,
        read_only: true,
    }];
    let provider = FakeProvider::new([final_text("must not dispatch")]);
    let tools = FakeTools::with_definitions(
        definitions,
        ToolBehavior::Immediate(success("unused")),
        None,
    );
    let error = runner(&f.store, provider.clone(), tools)
        .run(
            AgentInput {
                owner: ToolOwner::InteractiveTurn(
                    f.store.begin_turn(f.dialog_id, "another").unwrap().turn_id,
                ),
                system_prompt: "SYSTEM".into(),
                history: Vec::new(),
                prompt: "another".into(),
            },
            CancellationToken::new(),
            &mut sink(),
        )
        .await
        .unwrap_err();
    assert_eq!(error, AgentError::ContextTooLong);
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn oversized_provider_answer_is_not_persisted() {
    let f = fixture();
    let provider = FakeProvider::new([final_text(&"x".repeat(MESSAGE_LIMIT + 1))]);
    let error = service(&f.store, runner(&f.store, provider, FakeTools::empty()))
        .send_message(
            f.dialog_id,
            "question",
            CancellationToken::new(),
            &mut sink(),
        )
        .await
        .unwrap_err();

    assert_eq!(error, AgentError::ContentTooLong);
    assert!(f.store.completed_messages(f.dialog_id).unwrap().is_empty());
    assert_eq!(
        turn_rows(&f.path),
        vec![("failed".into(), Some("internal_error".into()))]
    );
}

#[tokio::test]
async fn oversized_interactive_input_is_rejected_before_turn_persistence() {
    let f = fixture();
    let provider = FakeProvider::new([final_text("must not run")]);
    let runner = AgentRunner::new(
        provider.clone(),
        FakeTools::empty(),
        f.store.clone(),
        8,
        REQUEST_LIMIT,
        1024,
    );

    let error = service(&f.store, runner)
        .send_message(
            f.dialog_id,
            &"x".repeat(1025),
            CancellationToken::new(),
            &mut sink(),
        )
        .await
        .unwrap_err();

    assert_eq!(error, AgentError::ContentTooLong);
    assert!(turn_rows(&f.path).is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn provider_context_error_recommends_new_dialog_without_deleting_history() {
    let f = fixture();
    let old = f.store.begin_turn(f.dialog_id, "old user").unwrap();
    f.store.complete_turn(old.turn_id, "old answer").unwrap();
    let provider = FakeProvider::new([ProviderReply::Error("context_length")]);

    let error = service(&f.store, runner(&f.store, provider, FakeTools::empty()))
        .send_message(
            f.dialog_id,
            "too much",
            CancellationToken::new(),
            &mut sink(),
        )
        .await
        .unwrap_err();

    assert_eq!(error, AgentError::ContextTooLong);
    assert!(error.recommends_new_dialog());
    let history = f.store.completed_messages(f.dialog_id).unwrap();
    assert_eq!(
        history
            .iter()
            .map(|message| (&message.role, message.content.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (&MessageRole::User, "old user"),
            (&MessageRole::Assistant, "old answer"),
        ]
    );
    assert_eq!(f.store.list_dialogs().unwrap().len(), 1);
}

#[tokio::test]
async fn user_is_durable_before_provider_call() {
    let f = fixture();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = FakeProvider::blocking(final_text("answer"), entered.clone(), release.clone());
    let service = service(&f.store, runner(&f.store, provider, FakeTools::empty()));
    let dialog_id = f.dialog_id;
    let task = tokio::spawn(async move {
        service
            .send_message(dialog_id, "durable", CancellationToken::new(), &mut sink())
            .await
    });
    entered.notified().await;

    let db = Connection::open(&f.path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT t.status,m.role,m.content FROM turns t JOIN messages m ON m.turn_id=t.id",
            [],
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?
            )),
        )
        .unwrap(),
        ("pending".into(), "user".into(), "durable".into())
    );
    release.notify_one();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn final_answer_and_completed_status_commit_together() {
    let f = fixture();
    let db = Connection::open(&f.path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER reject_answer BEFORE INSERT ON messages WHEN NEW.role='assistant' BEGIN SELECT RAISE(ABORT, 'no answer'); END;",
    )
    .unwrap();
    drop(db);
    let provider = FakeProvider::new([final_text("answer")]);

    assert!(
        service(&f.store, runner(&f.store, provider, FakeTools::empty()))
            .send_message(
                f.dialog_id,
                "question",
                CancellationToken::new(),
                &mut sink()
            )
            .await
            .is_err()
    );

    let db = Connection::open(&f.path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM messages WHERE role='assistant'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_ne!(
        db.query_row("SELECT status FROM turns", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "completed"
    );
}

#[tokio::test]
async fn output_failure_persists_no_assistant() {
    let f = fixture();
    let provider = FakeProvider::new([final_text("streamed answer")]);
    let mut failing_sink = |event| match event {
        AgentEvent::TextDelta { .. } => Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
        _ => Ok(()),
    };

    let error = service(&f.store, runner(&f.store, provider, FakeTools::empty()))
        .send_message(
            f.dialog_id,
            "question",
            CancellationToken::new(),
            &mut failing_sink,
        )
        .await
        .unwrap_err();

    assert_eq!(error, AgentError::Output);
    assert!(f.store.completed_messages(f.dialog_id).unwrap().is_empty());
}

#[tokio::test]
async fn tool_calls_run_sequentially_and_emit_events() {
    let f = fixture();
    let provider = FakeProvider::new([
        tool_turn(vec![call("one", "read"), call("two", "read")]),
        final_text("done"),
    ]);
    let tools = FakeTools::with_definitions(
        vec![definition("read", true)],
        ToolBehavior::Immediate(success("ok")),
        None,
    );
    let mut events = Vec::new();
    let mut event_sink = |event| {
        events.push(event);
        Ok(())
    };

    service(&f.store, runner(&f.store, provider, tools.clone()))
        .send_message(
            f.dialog_id,
            "question",
            CancellationToken::new(),
            &mut event_sink,
        )
        .await
        .unwrap();

    assert_eq!(*tools.max_active.lock().unwrap(), 1);
    assert_eq!(&*tools.calls.lock().unwrap(), &["read", "read"]);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::ToolStarted { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                AgentEvent::ToolFinished {
                    code: ToolEventCode::Completed,
                    ..
                }
            ))
            .count(),
        2
    );
    assert!(
        f.store
            .list_tool_runs()
            .unwrap()
            .iter()
            .all(|run| run.status == ToolRunStatus::Completed)
    );
}

#[tokio::test]
async fn cancellation_interrupts_a_locked_tool_audit_without_a_late_insert() {
    let f = fixture();
    let turn = f.store.begin_turn(f.dialog_id, "question").unwrap();
    let provider = FakeProvider::new([
        tool_turn(vec![call("locked-call", "read")]),
        final_text("unused"),
    ]);
    let tools = FakeTools::with_definitions(
        vec![definition("read", true)],
        ToolBehavior::Immediate(success("unused")),
        None,
    );
    let runner = runner(&f.store, provider, tools);
    let mut blocker = Connection::open(&f.path).unwrap();
    let transaction = blocker
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let cancellation = CancellationToken::new();
    let cancel_from_thread = cancellation.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(40));
        cancel_from_thread.cancel();
    });
    let started = Instant::now();

    let error = runner
        .run(
            AgentInput {
                owner: ToolOwner::InteractiveTurn(turn.turn_id),
                system_prompt: "SYSTEM".into(),
                history: Vec::new(),
                prompt: "question".into(),
            },
            cancellation,
            &mut sink(),
        )
        .await
        .unwrap_err();
    assert_eq!(error, AgentError::Interrupted);
    assert!(started.elapsed() < Duration::from_millis(500));
    drop(transaction);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(f.store.list_tool_runs().unwrap().is_empty());
}

async fn cancelled_tool(read_only: bool) -> (Fixture, Arc<FakeTools>, AgentError) {
    let f = fixture();
    let name = if read_only { "read" } else { "write" };
    let provider = FakeProvider::new([tool_turn(vec![call("call-1", name)])]);
    let entered = Arc::new(Notify::new());
    let tools = FakeTools::with_definitions(
        vec![definition(name, read_only)],
        ToolBehavior::Pending,
        Some(entered.clone()),
    );
    let service = service(&f.store, runner(&f.store, provider, tools.clone()));
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let dialog_id = f.dialog_id;
    let task = tokio::spawn(async move {
        service
            .send_message(dialog_id, "question", cancellation, &mut sink())
            .await
            .unwrap_err()
    });
    entered.notified().await;
    cancel.cancel();
    let error = task.await.unwrap();
    (f, tools, error)
}

#[tokio::test]
async fn cancelled_read_call_is_failed() {
    let (f, tools, error) = cancelled_tool(true).await;
    assert_eq!(error, AgentError::Interrupted);
    assert_eq!(tools.calls.lock().unwrap().len(), 1);
    let audits = f.store.list_tool_runs().unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].status, ToolRunStatus::Failed);
    assert_eq!(audits[0].safe_error_code, Some(SafeErrorCode::Interrupted));
}

#[tokio::test]
async fn cancelled_write_call_is_uncertain_and_not_retried() {
    let (f, tools, error) = cancelled_tool(false).await;
    assert_eq!(error, AgentError::Interrupted);
    assert_eq!(tools.calls.lock().unwrap().len(), 1);
    let audits = f.store.list_tool_runs().unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].status, ToolRunStatus::Uncertain);
    assert_eq!(audits[0].safe_error_code, Some(SafeErrorCode::Interrupted));
}

#[test]
fn agent_errors_never_include_provider_or_tool_payloads() {
    for error in [
        AgentError::Provider,
        AgentError::ContextTooLong,
        AgentError::Tool,
        AgentError::Output,
    ] {
        assert!(error.to_string().len() < 64);
        assert!(!error.to_string().contains("payload"));
    }
}
