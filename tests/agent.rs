use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use deepseek_cli::agent::{Agent, AgentError, AgentEvent};
use deepseek_cli::client::{ClientError, DeepSeekClient};
use deepseek_cli::config::Config;
use deepseek_cli::dialog::DialogStore;
use deepseek_cli::invariants::InvariantRepository;
use deepseek_cli::memory::{DurableMemoryScope, RequestScope};
use deepseek_cli::profile::ProfileRepository;
use deepseek_cli::tool_audit::ToolExecutionStatus;
use deepseek_cli::tool_calling::{
    ModelToolCall, ModelToolDefinition, ToolExecutionError, ToolExecutionResult, ToolExecutor,
    ToolFuture,
};
use deepseek_cli::workflow::{TaskPhase, TaskStatus};
use deepseek_cli::workflow_engine::{
    AutonomyStopReason, WorkflowEngineError, WorkflowModels, WorkflowTurnEvent,
};
use deepseek_cli::workflow_model::{CompletionModel, ModelFuture, ModelRequest, ModelResponse};
use deepseek_cli::workflow_store::{
    AnswerCommit, PauseOutcome, ProcessingStatus, WorkflowRepository,
};
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn config(server: &MockServer) -> Config {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"summary\"\n",
        server.uri()
    )
    .unwrap();
    Config::load(file.path(), None).unwrap()
}

struct FakeToolExecutor {
    definitions: Vec<ModelToolDefinition>,
    calls: Mutex<Vec<ModelToolCall>>,
    result: Result<ToolExecutionResult, ToolExecutionError>,
    hang: bool,
    audit_db: Option<std::path::PathBuf>,
}

impl FakeToolExecutor {
    fn new(read_only: bool, result: Result<ToolExecutionResult, ToolExecutionError>) -> Self {
        Self {
            definitions: vec![ModelToolDefinition {
                name: "telegram__read_chat".into(),
                description: None,
                parameters: serde_json::from_value(json!({"type":"object"})).unwrap(),
                read_only,
            }],
            calls: Mutex::new(vec![]),
            result,
            hang: false,
            audit_db: None,
        }
    }
    fn success() -> Self {
        Self::new(
            true,
            Ok(ToolExecutionResult {
                content: "SECRET_RESULT".into(),
                is_error: false,
                error_code: None,
                delivery_uncertain: false,
            }),
        )
    }
}

impl ToolExecutor for FakeToolExecutor {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }
    fn is_read_only(&self, name: &str) -> Option<bool> {
        self.definitions
            .iter()
            .find(|d| d.name == name)
            .map(|d| d.read_only)
    }
    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async move {
            if let Some(db) = &self.audit_db {
                let status: String = rusqlite::Connection::open(db)
                    .unwrap()
                    .query_row(
                        "SELECT status FROM tool_executions WHERE tool_call_id=?1",
                        [&call.id],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(status, "started", "audit must commit before dispatch");
            }
            self.calls.lock().unwrap().push(call.clone());
            if self.hang {
                std::future::pending::<()>().await;
            }
            self.result.clone()
        })
    }
}

fn tool_response(calls: &[(&str, &str, &str)]) -> ResponseTemplate {
    let calls: Vec<_> = calls.iter().enumerate().map(|(index, (id, name, arguments))| json!({"index":index,"id":id,"type":"function","function":{"name":name,"arguments":arguments}})).collect();
    ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string(format!("data: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"delta":{"tool_calls":calls},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}})))
}

#[tokio::test]
async fn legacy_agent_tool_executes_then_persists_only_final_text() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            tool_response(&[("call_1", "telegram__read_chat", r#"{"chat":"SECRET_ARG"}"#)]),
            sse("Done", 7, 2, 9),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("tools.sqlite3");
    let mut executor = FakeToolExecutor::success();
    executor.audit_db = Some(db.clone());
    let executor = Arc::new(executor);
    let mut agent = Agent::with_store(&config(&server), DialogStore::open(&db).unwrap())
        .unwrap()
        .with_tool_executor(executor.clone());
    let mut events = vec![];
    let answer = agent
        .run_streaming("read saved messages", |event| {
            match event {
                AgentEvent::ToolStarted { call_id, name } => {
                    events.push(format!("{call_id}:{name}:started"))
                }
                AgentEvent::ToolFinished {
                    call_id,
                    name,
                    status,
                    code,
                } => events.push(format!("{call_id}:{name}:{status:?}:{code:?}")),
                _ => {}
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(answer, "Done");
    assert_eq!(executor.calls.lock().unwrap().len(), 1);
    assert_eq!(agent.last_usage().unwrap().total_tokens, 14);
    let store = DialogStore::open(&db).unwrap();
    let saved = store.load(agent.dialog_id().unwrap()).unwrap();
    assert_eq!(saved.messages.len(), 2);
    assert_eq!(saved.messages[1].content(), "Done");
    assert_eq!(saved.messages[1].usage().unwrap().total_tokens, 14);
    let audit = store.tool_executions(saved.id).unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].status, ToolExecutionStatus::Succeeded);
    assert_eq!(audit[0].tool_name, "read_chat");
    let input_id: i64 = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row("SELECT id FROM messages WHERE role='user'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(audit[0].input_message_id, input_id);
    assert_eq!(
        events,
        [
            "call_1:telegram__read_chat:started",
            "call_1:telegram__read_chat:Succeeded:None"
        ]
    );
    let requests = server.received_requests().await.unwrap();
    let next: Value = requests[1].body_json().unwrap();
    assert_eq!(next["messages"][2]["tool_calls"][0]["id"], "call_1");
    assert_eq!(
        next["messages"][3],
        json!({"role":"tool","tool_call_id":"call_1","content":"SECRET_RESULT"})
    );
}

#[tokio::test]
async fn tool_timed_out_write_is_not_retried_and_is_audited_uncertain() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            tool_response(&[("write_1", "telegram__read_chat", "{}")]),
            sse("Delivery unknown", 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("tools.sqlite3");
    let executor = Arc::new(FakeToolExecutor::new(
        false,
        Err(ToolExecutionError::Timeout),
    ));
    let mut agent = Agent::with_store(&config(&server), DialogStore::open(&db).unwrap())
        .unwrap()
        .with_tool_executor(executor.clone());
    assert_eq!(
        agent.run_with_prompt("send").await.unwrap(),
        "Delivery unknown"
    );
    assert_eq!(executor.calls.lock().unwrap().len(), 1);
    let audit = DialogStore::open(&db)
        .unwrap()
        .tool_executions(agent.dialog_id().unwrap())
        .unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].status, ToolExecutionStatus::Uncertain);
    assert_eq!(
        audit[0].error_code.as_ref().unwrap().as_str(),
        "delivery_unknown"
    );
    let requests = server.received_requests().await.unwrap();
    let next: Value = requests[1].body_json().unwrap();
    assert!(
        next["messages"][3]["content"]
            .as_str()
            .unwrap()
            .contains("delivery_unknown")
    );
}

#[tokio::test]
async fn tool_round_limit_uses_config_and_stops_before_second_execution() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            tool_response(&[("c1", "telegram__read_chat", "{}")]),
            tool_response(&[("c2", "telegram__read_chat", "{}")]),
            sse("wrong", 1, 1, 2),
        ],
    )
    .await;
    let config = Config::from_toml(&format!("api_key='key'\nbase_url='{}'\n[workflow]\nenabled=false\n[context]\nstrategy='summary'\n[mcp]\nmax_tool_rounds=1", server.uri()), None).unwrap();
    let executor = Arc::new(FakeToolExecutor::success());
    let mut agent = Agent::new(&config)
        .unwrap()
        .with_tool_executor(executor.clone());
    assert!(matches!(
        agent.run_with_prompt("test").await,
        Err(AgentError::ToolLoop(
            deepseek_cli::tool_calling::ToolLoopError::RoundLimitExceeded
        ))
    ));
    assert_eq!(executor.calls.lock().unwrap().len(), 1);
    assert!(agent.history().messages().is_empty());
    assert_eq!(agent.last_usage().unwrap().total_tokens, 10);
}

#[tokio::test]
async fn tool_errors_are_safe_audited_and_returned_to_the_model() {
    for (read_only, result, status, code) in [
        (
            true,
            Err(ToolExecutionError::Timeout),
            ToolExecutionStatus::Failed,
            "timeout",
        ),
        (
            true,
            Err(ToolExecutionError::Transport),
            ToolExecutionStatus::Failed,
            "transport",
        ),
        (
            false,
            Err(ToolExecutionError::Transport),
            ToolExecutionStatus::Uncertain,
            "delivery_unknown",
        ),
        (
            true,
            Err(ToolExecutionError::UnknownTool),
            ToolExecutionStatus::Failed,
            "unknown_tool",
        ),
        (
            true,
            Err(ToolExecutionError::InvalidArguments),
            ToolExecutionStatus::Failed,
            "invalid_arguments",
        ),
        (
            true,
            Ok(ToolExecutionResult {
                content: "SECRET_BODY".into(),
                is_error: true,
                error_code: Some("SECRET_CODE".into()),
                delivery_uncertain: false,
            }),
            ToolExecutionStatus::Failed,
            "tool_error",
        ),
        (
            true,
            Ok(ToolExecutionResult {
                content: "SECRET_BODY".into(),
                is_error: true,
                error_code: Some("mcp_tool_error".into()),
                delivery_uncertain: false,
            }),
            ToolExecutionStatus::Failed,
            "mcp_tool_error",
        ),
    ] {
        let server = MockServer::start().await;
        mount_sequence(
            &server,
            [
                tool_response(&[("call", "telegram__read_chat", "{}")]),
                sse("Done", 2, 1, 3),
            ],
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("tools.sqlite3");
        let executor = Arc::new(FakeToolExecutor::new(read_only, result));
        let mut agent = Agent::with_store(&config(&server), DialogStore::open(&db).unwrap())
            .unwrap()
            .with_tool_executor(executor.clone());
        let mut finished = None;
        agent
            .run_streaming("test", |event| {
                if let AgentEvent::ToolFinished { status, code, .. } = event {
                    finished = Some((status, code.map(str::to_owned)));
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(finished, Some((status, Some(code.into()))));
        assert_eq!(executor.calls.lock().unwrap().len(), 1);
        let audit = DialogStore::open(&db)
            .unwrap()
            .tool_executions(agent.dialog_id().unwrap())
            .unwrap();
        assert_eq!(audit[0].status, status);
        assert_eq!(audit[0].error_code.as_ref().unwrap().as_str(), code);
        let requests = server.received_requests().await.unwrap();
        let next: Value = requests[1].body_json().unwrap();
        assert_eq!(
            next["messages"][3]["content"],
            json!({"error": code}).to_string()
        );
    }
}

#[tokio::test]
async fn tool_calls_are_ordered_and_resumed_audit_uses_current_input() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            tool_response(&[
                ("one", "telegram__read_chat", r#"{"n":1}"#),
                ("two", "telegram__read_chat", r#"{"n":2}"#),
            ]),
            sse("Done", 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("tools.sqlite3");
    let mut store = DialogStore::open(&db).unwrap();
    let (id, old_input) = store
        .start_dialog_with_message_id("system", "earlier")
        .unwrap();
    store.append_answer(id, 1, "earlier answer", None).unwrap();
    let executor = Arc::new(FakeToolExecutor::success());
    let mut agent = Agent::from_dialog(&config(&server), store, id)
        .unwrap()
        .with_tool_executor(executor.clone());
    agent.run_with_prompt("now").await.unwrap();
    let calls = executor.calls.lock().unwrap().clone();
    assert_eq!(
        calls.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
        ["one", "two"]
    );
    assert_eq!(calls[1].arguments, r#"{"n":2}"#);
    let store = DialogStore::open(&db).unwrap();
    let audit = store.tool_executions(id).unwrap();
    assert_eq!(audit.len(), 2);
    assert_ne!(audit[0].input_message_id, old_input);
    let conn = rusqlite::Connection::open(&db).unwrap();
    let content: String = conn
        .query_row(
            "SELECT content FROM messages WHERE id=?1",
            [audit[0].input_message_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(content, "now");
    assert_eq!(audit[0].input_message_id, audit[1].input_message_id);
    let requests = server.received_requests().await.unwrap();
    let next: Value = requests[1].body_json().unwrap();
    assert_eq!(next["messages"][5]["tool_call_id"], "one");
    assert_eq!(next["messages"][6]["tool_call_id"], "two");
    assert_eq!(store.load(id).unwrap().messages.len(), 4);
}

#[tokio::test]
async fn tool_unknown_and_invalid_calls_fail_closed_before_execution_or_audit() {
    for (name, arguments) in [
        ("SECRET_TOOL", "{}"),
        ("telegram__read_chat", "SECRET_ARGUMENT"),
        ("telegram__read_chat", "[]"),
    ] {
        let server = MockServer::start().await;
        mount_sequence(&server, [tool_response(&[("call", name, arguments)])]).await;
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("tools.sqlite3");
        let executor = Arc::new(FakeToolExecutor::success());
        let mut agent = Agent::with_store(&config(&server), DialogStore::open(&db).unwrap())
            .unwrap()
            .with_tool_executor(executor.clone());
        let error = agent.run_with_prompt("test").await.unwrap_err();
        assert!(!error.to_string().contains("SECRET"));
        assert!(!error.operator_message().contains("SECRET"));
        assert!(executor.calls.lock().unwrap().is_empty());
        let store = DialogStore::open(&db).unwrap();
        assert!(
            store
                .tool_executions(agent.dialog_id().unwrap())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .load(agent.dialog_id().unwrap())
                .unwrap()
                .messages
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn tool_output_failure_preserves_persistent_input_and_in_memory_atomicity() {
    for persistent in [false, true] {
        let server = MockServer::start().await;
        mount_sequence(
            &server,
            [
                tool_response(&[("call", "telegram__read_chat", "{}")]),
                sse("Done", 2, 1, 3),
            ],
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("tools.sqlite3");
        let mut agent = if persistent {
            Agent::with_store(&config(&server), DialogStore::open(&db).unwrap()).unwrap()
        } else {
            Agent::from_client(DeepSeekClient::new(&config(&server)).unwrap(), "system")
        }
        .with_tool_executor(Arc::new(FakeToolExecutor::success()));
        let error = agent
            .run_streaming("test", |event| {
                if matches!(event, AgentEvent::Text(_)) {
                    return Err(std::io::Error::other("output closed"));
                }
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, AgentError::Client(ClientError::Output(_))));
        assert_eq!(agent.history().messages().len(), usize::from(persistent));
        assert_eq!(agent.last_usage().unwrap().total_tokens, 8);
        if persistent {
            let store = DialogStore::open(&db).unwrap();
            assert_eq!(
                store
                    .load(agent.dialog_id().unwrap())
                    .unwrap()
                    .messages
                    .len(),
                1
            );
            assert_eq!(
                store.tool_executions(agent.dialog_id().unwrap()).unwrap()[0].status,
                ToolExecutionStatus::Succeeded
            );
        }
    }
}

#[tokio::test]
async fn tool_finished_callback_failure_stops_remaining_calls() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [tool_response(&[
            ("one", "telegram__read_chat", "{}"),
            ("two", "telegram__read_chat", "{}"),
        ])],
    )
    .await;
    let executor = Arc::new(FakeToolExecutor::success());
    let mut agent = Agent::new(&config(&server))
        .unwrap()
        .with_tool_executor(executor.clone());
    assert!(
        agent
            .run_streaming("test", |event| {
                if matches!(event, AgentEvent::ToolFinished { .. }) {
                    Err(std::io::Error::other("closed"))
                } else {
                    Ok(())
                }
            })
            .await
            .is_err()
    );
    assert_eq!(executor.calls.lock().unwrap().len(), 1);
    assert!(agent.history().messages().is_empty());
}

#[tokio::test]
async fn tool_cancellation_does_not_commit_an_answer_or_repeat_a_dispatched_write() {
    for persistent in [false, true] {
        let server = MockServer::start().await;
        mount_sequence(
            &server,
            [tool_response(&[("write", "telegram__read_chat", "{}")])],
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("tools.sqlite3");
        let mut executor = FakeToolExecutor::new(false, Err(ToolExecutionError::Timeout));
        executor.hang = true;
        if persistent {
            executor.audit_db = Some(db.clone());
        }
        let executor = Arc::new(executor);
        let mut agent = if persistent {
            Agent::with_store(&config(&server), DialogStore::open(&db).unwrap()).unwrap()
        } else {
            Agent::new(&config(&server)).unwrap()
        }
        .with_tool_executor(executor.clone());
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                agent.run_with_prompt("send")
            )
            .await
            .is_err()
        );
        assert_eq!(executor.calls.lock().unwrap().len(), 1);
        assert_eq!(agent.history().messages().len(), usize::from(persistent));
        if persistent {
            let store = DialogStore::open(&db).unwrap();
            assert_eq!(
                store
                    .load(agent.dialog_id().unwrap())
                    .unwrap()
                    .messages
                    .len(),
                1
            );
            let audit = store.tool_executions(agent.dialog_id().unwrap()).unwrap();
            assert_eq!(audit.len(), 1);
            assert_eq!(audit[0].status, ToolExecutionStatus::Uncertain);
            assert_eq!(
                audit[0].error_code.as_ref().unwrap().as_str(),
                "delivery_unknown"
            );
        }
    }
}

#[tokio::test]
async fn tool_read_cancellation_is_audited_failed() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [tool_response(&[("read", "telegram__read_chat", "{}")])],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("tools.sqlite3");
    let mut executor = FakeToolExecutor::success();
    executor.hang = true;
    let executor = Arc::new(executor);
    let mut agent = Agent::with_store(&config(&server), DialogStore::open(&db).unwrap())
        .unwrap()
        .with_tool_executor(executor.clone());
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            agent.run_with_prompt("read")
        )
        .await
        .is_err()
    );
    assert_eq!(executor.calls.lock().unwrap().len(), 1);
    let audit = DialogStore::open(&db)
        .unwrap()
        .tool_executions(agent.dialog_id().unwrap())
        .unwrap();
    assert_eq!(audit[0].status, ToolExecutionStatus::Failed);
    assert_eq!(audit[0].error_code.as_ref().unwrap().as_str(), "cancelled");
}

#[tokio::test]
async fn tool_cancellation_cleanup_database_error_does_not_panic() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [tool_response(&[("write", "telegram__read_chat", "{}")])],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("tools.sqlite3");
    let store = DialogStore::open(&db).unwrap();
    rusqlite::Connection::open(&db).unwrap().execute_batch(
        "CREATE TRIGGER fail_audit_cleanup BEFORE UPDATE ON tool_executions BEGIN SELECT RAISE(ABORT, 'SECRET_CLEANUP_FAILURE'); END;"
    ).unwrap();
    let mut executor = FakeToolExecutor::new(false, Err(ToolExecutionError::Timeout));
    executor.hang = true;
    let executor = Arc::new(executor);
    let mut agent = Agent::with_store(&config(&server), store)
        .unwrap()
        .with_tool_executor(executor.clone());
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            agent.run_with_prompt("send")
        )
        .await
        .is_err()
    );
    assert_eq!(executor.calls.lock().unwrap().len(), 1);
    assert_eq!(agent.history().messages().len(), 1);
    let store = DialogStore::open(&db).unwrap();
    assert_eq!(
        store.tool_executions(agent.dialog_id().unwrap()).unwrap()[0].status,
        ToolExecutionStatus::Started
    );
}

#[tokio::test]
async fn tool_intermediate_text_is_not_emitted_and_failed_final_round_retains_usage() {
    let server = MockServer::start().await;
    let intermediate = ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string(format!("data: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"delta":{"content":"SECRET_INTERMEDIATE", "tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"telegram__read_chat","arguments":"{}"}}]},"finish_reason":"tool_calls"}], "usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,"completion_tokens_details":{"reasoning_tokens":1}}})));
    let truncated = ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string(format!("data: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"delta":{"content":"PARTIAL"},"finish_reason":"length"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9,"completion_tokens_details":{"reasoning_tokens":2}}})));
    mount_sequence(&server, [intermediate, truncated]).await;
    let mut agent = Agent::new(&config(&server))
        .unwrap()
        .with_tool_executor(Arc::new(FakeToolExecutor::success()));
    let mut output = String::new();
    let error = agent
        .run_streaming("test", |event| {
            if let AgentEvent::Text(text) = event {
                output.push_str(text);
            }
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(error, AgentError::Client(_)));
    assert!(output.is_empty());
    assert!(agent.history().messages().is_empty());
    let usage = agent.last_usage().unwrap();
    assert_eq!(usage.total_tokens, 14);
    assert_eq!(
        usage.completion_tokens_details.unwrap().reasoning_tokens,
        Some(3)
    );
}

// Historical checker tests start with an already-approved goal so they can
// exercise planning/execution rather than the new goal-definition gate.
fn approved_planning_store(path: &Path) -> (DialogStore, i64) {
    let mut store = DialogStore::open(path).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "approved goal")
        .unwrap();
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch("UPDATE workflow_tasks SET phase='planning', goal_revision=1; UPDATE task_stage_runs SET phase='planning';")
        .unwrap();
    (store, started.dialog_id)
}

fn continue_interpretation() -> String {
    json!({"confidence":0.95,"intent":{"type":"continue","instruction":"continue planning"}})
        .to_string()
}

#[test]
fn config_invariants_override_scoped_rules_without_copying_them_to_sqlite() {
    let config = Config::from_toml(
        "api_key='key'\n[context]\nstrategy='summary'\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
        None,
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let scope = RequestScope::new("alice", "parser").unwrap();
    let mut store = DialogStore::open(&path).unwrap();
    store
        .upsert_invariant(&scope, "STACK", "Use Go only")
        .unwrap();
    store
        .upsert_invariant(&scope, "ARCH", "Keep the monolith")
        .unwrap();
    let mut agent = Agent::with_store_for_scope(&config, store, scope.clone()).unwrap();
    let rules = agent.invariants().unwrap();
    assert_eq!(rules.rules().len(), 2);
    assert_eq!(rules.rule("STACK").unwrap().text, "Use Rust only");
    assert_eq!(rules.rule("ARCH").unwrap().text, "Keep the monolith");
    assert!(agent.upsert_invariant("STACK", "Use Go only").is_err());
    assert!(agent.delete_invariant("STACK").is_err());
    drop(agent);
    let store = DialogStore::open(&path).unwrap();
    assert_eq!(
        store
            .load_invariants(&scope)
            .unwrap()
            .rule("STACK")
            .unwrap()
            .text,
        "Use Go only"
    );
}

#[tokio::test]
async fn config_only_invariant_blocks_new_task_before_ordinary_generation() {
    let server = MockServer::start().await;
    let config = Config::from_toml(
        &format!(
            "api_key='key'\nbase_url='{}'\n[context]\nstrategy='summary'\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
            server.uri()
        ),
        None,
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let service = Arc::new(AgentWorkflowModel::default());
    service.responses.lock().unwrap().push_back(
        json!({"type":"deny","violations":[{"id":"STACK","reason":"Go conflicts with Rust"}]})
            .to_string(),
    );
    let mut agent = Agent::with_store(&config, store)
        .unwrap()
        .with_workflow_models(injected_models(&service));
    let answer = agent
        .run_with_prompt("Build the backend in Go")
        .await
        .unwrap();
    assert!(answer.contains("STACK"));
    assert!(answer.contains("Use Rust only"));
    assert!(agent.dialog_id().is_none());
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn invariant_input_rejection_logs_safe_checker_result() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("checks.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='key'\nbase_url='{}'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=false\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
            server.uri(), log_path
        ),
        None,
    )
    .unwrap();
    let service = Arc::new(AgentWorkflowModel::default());
    service.responses.lock().unwrap().push_back(
        json!({"type":"deny","violations":[{"id":"STACK","reason":"SECRET_REASON"}]}).to_string(),
    );
    let store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let mut agent = Agent::with_store(&config, store)
        .unwrap()
        .with_workflow_models(injected_models(&service));

    let answer = agent
        .run_with_prompt("Build a backend in Go")
        .await
        .unwrap();

    assert!(answer.contains("STACK"));
    let log = std::fs::read_to_string(log_path).unwrap();
    let checks: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| {
            event["event"] == "workflow" && event["details"]["mode"] == "blocking"
        })
        .collect();
    assert_eq!(checks.len(), 1, "{log}");
    let details = &checks[0]["details"];
    assert_eq!(details["component"], "invariants");
    assert_eq!(details["check_target"], "start_new_task");
    assert_eq!(details["outcome"], "denied");
    assert_eq!(details["violation_ids"], json!(["STACK"]));
    assert!(details.get("payload").is_none());
    assert!(!log.contains("SECRET_REASON"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn invariant_response_rejection_logs_ordered_checks_without_running_advisory() {
    let server = MockServer::start().await;
    mount_sequence(&server, [sse("Go answer", 2, 1, 3)]).await;
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("checks.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='key'\nbase_url='{}'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
            server.uri(), log_path
        ),
        None,
    )
    .unwrap();
    let service = Arc::new(AgentWorkflowModel::default());
    service.responses.lock().unwrap().extend([
        json!({"type":"allow"}).to_string(),
        json!({"type":"deny","violations":[{"id":"STACK","reason":"Go conflicts with Rust"}]})
            .to_string(),
    ]);
    let store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let mut agent = Agent::with_store(&config, store)
        .unwrap()
        .with_workflow_models(injected_models(&service));

    let answer = agent.run_with_prompt("Try Go").await.unwrap();

    assert!(answer.contains("Go conflicts with Rust"));
    let log = std::fs::read_to_string(log_path).unwrap();
    let checks: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| {
            event["event"] == "workflow" && event["details"]["mode"] == "blocking"
        })
        .collect();
    assert_eq!(checks.len(), 2, "{log}");
    assert_eq!(checks[0]["details"]["check_target"], "start_new_task");
    assert_eq!(checks[0]["details"]["outcome"], "allowed");
    assert_eq!(checks[1]["details"]["check_target"], "candidate_response");
    assert_eq!(checks[1]["details"]["outcome"], "denied");
    assert_eq!(checks[1]["details"]["violation_ids"], json!(["STACK"]));
    assert!(
        checks[1]["details"]["payload"]
            .to_string()
            .contains("Go conflicts with Rust")
    );
    assert!(!log.contains("\"component\":\"continuation\""));
    assert_eq!(service.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn rejected_task_start_log_explains_unfinished_task_and_interpreted_intent() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("checks.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='key'\nbase_url='{}'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=false",
            server.uri(), log_path
        ),
        None,
    )
    .unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "Build a CLI")
        .unwrap();
    let service = Arc::new(AgentWorkflowModel::default());
    service.responses.lock().unwrap().push_back(
        json!({"confidence":0.95,"intent":{"type":"start_new_task","goal":"Other goal"}})
            .to_string(),
    );
    let mut agent = Agent::from_dialog(&config, store, started.dialog_id)
        .unwrap()
        .with_workflow_models(injected_models(&service));

    agent
        .run_workflow_streaming("Start another task", |_| Ok(()))
        .await
        .unwrap();

    let log = std::fs::read_to_string(log_path).unwrap();
    let router: Value = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| {
            event["event"] == "workflow"
                && event["details"]["component"] == "input_router"
                && event["details"]["outcome"] == "rejected"
        })
        .expect("rejected router diagnostic");
    let details = &router["details"];
    assert_eq!(details["input_intent"], "start_new_task");
    assert_eq!(details["input_phase"], "goal_definition");
    assert_eq!(details["input_status"], "active");
    assert_eq!(
        details["rejection_reason"],
        "dialog already has an unfinished task"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn config_invariants_cannot_be_bypassed_by_in_memory_agent() {
    let config = Config::from_toml(
        "api_key='key'\n[context]\nstrategy='summary'\n[[invariants]]\nid='STACK'\ntext='Use Rust only'",
        None,
    )
    .unwrap();
    let mut agent = Agent::new(&config).unwrap();
    assert!(matches!(
        agent.run_with_prompt("Use Go").await,
        Err(AgentError::InvariantRequiresStore)
    ));
}

#[test]
fn debug_snapshot_reads_paused_task_without_resuming_or_changing_it() {
    let config = Config::from_toml("api_key='key'\n[context]\nstrategy='summary'", None).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "Build a CLI")
        .unwrap();
    let PauseOutcome::Paused(paused) = store.pause_current_task(started.dialog_id).unwrap() else {
        panic!("task should pause");
    };
    let agent = Agent::from_dialog(&config, store, started.dialog_id).unwrap();
    let snapshot = serde_json::to_value(agent.debug_snapshot().unwrap()).unwrap();
    assert_eq!(snapshot["workflow"]["phase"], "goal_definition");
    assert_eq!(snapshot["workflow"]["status"], "paused");
    assert_eq!(snapshot["workflow"]["goal"], "Build a CLI");
    assert_eq!(snapshot["workflow"]["version"], paused.version);
    assert_eq!(snapshot["processing"], Value::Null);
    assert_eq!(
        agent.workflow_status().unwrap().unwrap().status,
        TaskStatus::Paused
    );
    assert_eq!(
        DialogStore::open(&path)
            .unwrap()
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap(),
        *paused
    );
}

#[test]
fn debug_snapshot_identifies_active_profile_without_exposing_its_text() {
    let config = Config::from_toml("api_key='key'\n[context]\nstrategy='summary'", None).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    store
        .replace_profile("alice", "ALICE_PRIVATE_PROFILE")
        .unwrap();
    store.replace_profile("bob", "BOB_PRIVATE_PROFILE").unwrap();

    for user_id in ["alice", "bob", "default"] {
        let agent = Agent::with_store_for_scope(
            &config,
            DialogStore::open(&path).unwrap(),
            RequestScope::new(user_id, "project").unwrap(),
        )
        .unwrap();
        let snapshot = serde_json::to_value(agent.debug_snapshot().unwrap()).unwrap();
        assert_eq!(snapshot["active_profile"]["user_id"], user_id);
        assert_eq!(
            snapshot["active_profile"]["configured"],
            user_id != "default"
        );
        assert_eq!(
            snapshot["active_profile"]["updated_at"].is_string(),
            user_id != "default"
        );
        assert!(!snapshot.to_string().contains("PRIVATE_PROFILE"));
    }
}

// Break caught: disabling workflow must not silently bypass durable project rules.
#[tokio::test]
async fn legacy_persistent_mode_refuses_to_run_with_invariants() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    store
        .upsert_invariant(&RequestScope::default(), "STACK", "Use Rust only")
        .unwrap();
    let mut agent = Agent::with_store(&config(&server), store).unwrap();
    assert!(matches!(
        agent.run_with_prompt("use Go").await,
        Err(AgentError::InvariantsRequireWorkflow)
    ));
    assert!(server.received_requests().await.unwrap().is_empty());
}

fn managed_config(server: &MockServer) -> Config {
    Config::from_toml(&format!("api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\nsystem_prompt='MANAGED BASE'\n[workflow]\ninterpreter_model='interpreter-model'\nchecker_model='checker-model'\nhandoff_model='handoff-model'\n[context]\nstrategy='summary'",server.uri()),None).unwrap()
}

#[derive(Default)]
struct AgentWorkflowModel {
    responses: Mutex<VecDeque<String>>,
    requests: Mutex<Vec<ModelRequest>>,
    usage: Option<deepseek_cli::client::TokenUsage>,
}

impl CompletionModel for AgentWorkflowModel {
    fn name(&self) -> &str {
        "agent-test-model"
    }
    fn complete(&self, request: ModelRequest) -> ModelFuture<'_> {
        self.requests.lock().unwrap().push(request);
        let content = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| "unavailable checker".into());
        let usage = self.usage;
        Box::pin(async move { Ok(ModelResponse { content, usage }) })
    }
}

fn injected_models(service: &Arc<AgentWorkflowModel>) -> WorkflowModels {
    WorkflowModels {
        interpreter: service.clone(),
        checker: service.clone(),
        handoff: service.clone(),
    }
}

fn await_check(version: u64) -> String {
    json!({"patch":{"expected_version":version,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},"decision":{"type":"await_user"}}).to_string()
}

// Break caught: the Agent loses its executor when it constructs a workflow engine for a new turn.
#[tokio::test]
async fn workflow_tool_agent_reuses_executor_across_resumed_turns() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            tool_response(&[("first", "telegram__read_chat", "{}")]),
            response("First final", true),
            tool_response(&[("second", "telegram__read_chat", "{}")]),
            response("Second final", true),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("workflow-tools.sqlite3");
    let (store, id) = approved_planning_store(&db);
    let service = Arc::new(AgentWorkflowModel {
        usage: Some(deepseek_cli::client::TokenUsage {
            prompt_tokens: 2,
            completion_tokens: 1,
            total_tokens: 3,
            completion_tokens_details: None,
        }),
        ..Default::default()
    });
    service.responses.lock().unwrap().extend([
        continue_interpretation(),
        await_check(1),
        continue_interpretation(),
        await_check(3),
    ]);
    let tools = Arc::new(FakeToolExecutor {
        audit_db: Some(db.clone()),
        ..FakeToolExecutor::success()
    });
    let mut agent = Agent::from_dialog(&managed_config(&server), store, id)
        .unwrap()
        .with_workflow_models(injected_models(&service))
        .with_tool_executor(tools.clone());
    assert_eq!(
        agent.run_with_prompt("read once").await.unwrap(),
        "First final"
    );
    assert_eq!(agent.last_usage().unwrap().total_tokens, 8);
    assert_eq!(
        agent.run_with_prompt("read again").await.unwrap(),
        "Second final"
    );
    assert_eq!(agent.last_usage().unwrap().total_tokens, 8);
    assert_eq!(
        tools
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.id.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    let store = DialogStore::open(&db).unwrap();
    let audits = store.tool_executions(id).unwrap();
    assert_eq!(
        audits
            .iter()
            .map(|row| row.input_message_id)
            .collect::<Vec<_>>(),
        [2, 4]
    );
    assert!(
        audits
            .iter()
            .all(|row| row.status == ToolExecutionStatus::Succeeded)
    );
    let requests = server.received_requests().await.unwrap();
    let next: Value = requests[2].body_json().unwrap();
    assert!(!next["messages"].to_string().contains("SECRET_RESULT"));
    assert!(
        next["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|message| message["role"] != "tool")
    );
}

// Break caught: workflow audit errors stop being fatal at the Agent/CLI boundary.
#[tokio::test]
async fn workflow_tool_agent_audit_failures_remain_fatal_and_safe() {
    for finish in [false, true] {
        let server = MockServer::start().await;
        mount_sequence(
            &server,
            [tool_response(&[("call", "telegram__read_chat", "{}")])],
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("audit-error.sqlite3");
        let (store, id) = approved_planning_store(&db);
        rusqlite::Connection::open(&db).unwrap().execute_batch(&format!(
            "CREATE TRIGGER reject_audit BEFORE {} ON tool_executions BEGIN SELECT RAISE(ABORT, 'PRIVATE_DATABASE_ERROR'); END;",
            if finish { "UPDATE" } else { "INSERT" }
        )).unwrap();
        let service = Arc::new(AgentWorkflowModel {
            usage: Some(deepseek_cli::client::TokenUsage {
                prompt_tokens: 2,
                completion_tokens: 1,
                total_tokens: 3,
                completion_tokens_details: None,
            }),
            ..Default::default()
        });
        service
            .responses
            .lock()
            .unwrap()
            .push_back(continue_interpretation());
        let tools = Arc::new(FakeToolExecutor::success());
        let mut agent = Agent::from_dialog(&managed_config(&server), store, id)
            .unwrap()
            .with_workflow_models(injected_models(&service))
            .with_tool_executor(tools.clone());
        let error = agent.run_with_prompt("read").await.unwrap_err();
        assert!(matches!(error, AgentError::ToolAudit(_)), "{error:?}");
        assert_eq!(error.operator_message(), "tool audit failed");
        assert_eq!(tools.calls.lock().unwrap().len(), usize::from(finish));
        assert_eq!(agent.history().messages().len(), 2);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

fn hidden_controller_dialog(store: &mut DialogStore) -> i64 {
    use deepseek_cli::workflow::{TaskStatePatch, WorkflowIntent};
    use deepseek_cli::workflow_store::{ControllerInputCommit, ProcessingLeaseMode};
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "human goal")
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
    store
        .lease_processing(answer.processing_id, 0, ProcessingLeaseMode::Normal)
        .unwrap();
    let intent = WorkflowIntent::human_continue("HIDDEN_CONTROLLER_BOUNDARY").unwrap();
    store
        .commit_controller_decision(ControllerInputCommit {
            processing_id: answer.processing_id,
            task_id: started.task.id,
            stage_run_id: started.stage_run_id,
            expected_version: 0,
            expected_attempt: 1,
            checker: "continuation",
            model: "checker",
            triggering_assistant_message_id: answer.message_id,
            instruction: "HIDDEN_CONTROLLER_BOUNDARY",
            intent: &intent,
            confidence: 0.95,
            accepted_patch: &TaskStatePatch {
                expected_version: 0,
                plan_append: Default::default(),
                step_updates: vec![],
                current_step_id: None,
                expected_action: None,
                checkpoint: None,
            },
        })
        .unwrap();
    started.dialog_id
}

// Break caught: hidden protocol rows must not turn a valid restored legacy
// session into a stale writer, or weaken real stale-writer detection.
#[tokio::test]
async fn final_disabled_workflow_uses_raw_checkpoint_for_turns_reductions_and_branches() {
    for strategy in ["summary", "sticky_facts", "branching"] {
        let server = MockServer::start().await;
        let mut responses = Vec::new();
        if strategy == "sticky_facts" {
            responses.push(sse(r#"{"language":"Rust"}"#, 2, 1, 3));
        }
        responses.push(sse("legacy answer", 10, 2, 12));
        if strategy == "summary" {
            responses.push(sse("visible summary", 2, 1, 3));
        }
        mount_sequence(&server, responses).await;
        let config = Config::from_toml(&format!(
            "api_key='test-key'\nbase_url='{}'\n[workflow]\nenabled=false\n[context]\nstrategy='{strategy}'\ncompact_after_prompt_tokens=1\nkeep_last_messages=1", server.uri()), None).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("raw-checkpoint.sqlite3");
        let mut store = DialogStore::open(&database).unwrap();
        let id = hidden_controller_dialog(&mut store);
        let mut agent = Agent::from_dialog(&config, store, id).unwrap();
        if strategy == "branching" {
            let fork = agent.branch_dialog().unwrap();
            assert_eq!(fork.checkpoint_message_count, 3);
            agent.switch_branch(fork.new_dialog_id).unwrap();
        }
        let id = agent.dialog_id().unwrap();
        let mut stale =
            Agent::from_dialog(&config, DialogStore::open(&database).unwrap(), id).unwrap();
        assert_eq!(
            agent.run_with_prompt("continue legacy").await.unwrap(),
            "legacy answer"
        );
        assert_eq!(agent.history().messages().len(), 4);
        let store = DialogStore::open(&database).unwrap();
        assert_eq!(store.raw_message_count(id).unwrap(), 5);
        let loaded = store.load(id).unwrap();
        assert_eq!(loaded.messages.len(), 4);
        if strategy == "sticky_facts" {
            assert_eq!(loaded.facts.covered_message_count(), 3);
            assert_eq!(loaded.facts.facts()["language"], "Rust");
        }
        if strategy == "summary" {
            assert_eq!(loaded.context.summary().unwrap().covered_message_count(), 3);
        }
        let requests = server.received_requests().await.unwrap();
        assert!(requests.iter().all(|request| {
            !String::from_utf8_lossy(&request.body).contains("HIDDEN_CONTROLLER_BOUNDARY")
        }));
        assert!(matches!(
            stale.run_with_prompt("stale input").await,
            Err(AgentError::Store(
                deepseek_cli::dialog::StoreError::Conflict(_)
            ))
        ));
        if strategy == "branching" {
            assert!(matches!(
                stale.branch_dialog(),
                Err(AgentError::Store(
                    deepseek_cli::dialog::StoreError::Conflict(_)
                ))
            ));
        }
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            requests.len()
        );
        assert_eq!(store.raw_message_count(id).unwrap(), 5);
    }
}

// Break caught: managed stats must read current-stage reductions and usage,
// even with obsolete dialog reductions and earlier-stage messages present.
#[tokio::test]
async fn final_managed_context_stats_use_only_current_stage_reductions() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("stage-stats.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let id = hidden_controller_dialog(&mut store);
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "UPDATE workflow_tasks SET phase='planning', goal_revision=1;
        UPDATE task_stage_runs SET finished_at=started_at;
        INSERT INTO task_stage_runs(workflow_task_id,phase,sequence) VALUES(1,'planning',2);
        UPDATE workflow_tasks SET current_stage_run_id=2;",
        )
        .unwrap();
    let task = store.load_workflow(id).unwrap().current_task.unwrap();
    let input = deepseek_cli::workflow::WorkflowInput {
        source: deepseek_cli::workflow::WorkflowInputSource::Human,
        intent: deepseek_cli::workflow::WorkflowIntent::human_continue("current input").unwrap(),
    };
    let persisted = store
        .append_input(
            deepseek_cli::workflow_store::InputCommit {
                dialog_id: id,
                input: &input,
                protocol_text: "current input",
                confidence: None,
                expected_current_task: deepseek_cli::workflow_store::ExpectedCurrentTask::Present {
                    task_id: task.id,
                    version: task.version,
                },
            },
            deepseek_cli::workflow_store::AcceptedInputEffect::ContinueSameStage,
        )
        .unwrap();
    let task = persisted.task.unwrap();
    let usage = deepseek_cli::client::TokenUsage {
        prompt_tokens: 7,
        completion_tokens: 3,
        total_tokens: 10,
        completion_tokens_details: None,
    };
    store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: id,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            content: "current answer",
            usage: Some(usage),
        })
        .unwrap();
    store
        .replace_stage_facts(
            task.current_stage_run_id,
            task.version,
            2,
            [("language".into(), "Rust".into())].into(),
            Some(usage),
        )
        .unwrap();
    store
        .replace_context(
            id,
            5,
            deepseek_cli::context::ContextSummary::new("OBSOLETE", 1),
            None,
        )
        .unwrap();
    let config = Config::from_toml(&format!("api_key='key'\nbase_url='{}'\n[context]\nstrategy='sticky_facts'\nkeep_last_messages=8",server.uri()),None).unwrap();
    let before: i64 = connection
        .query_row("PRAGMA data_version", [], |r| r.get(0))
        .unwrap();
    let agent = Agent::from_dialog(&config, store, id).unwrap();
    let stats = agent.context_stats().unwrap();
    assert_eq!(stats.facts_count, 1);
    assert_eq!(stats.facts_covered_message_count, 2);
    assert_eq!(stats.facts_usage.call_count(), 1);
    assert_eq!(stats.facts_usage.total_tokens(), 10);
    assert_eq!(stats.full_message_count, 4);
    assert_eq!(stats.selected_message_count, 2);
    assert_eq!(stats.raw_message_count, 2);
    assert_eq!(stats.ordinary_usage.call_count(), 1);
    assert_eq!(stats.ordinary_usage.total_tokens(), 10);
    assert_eq!(stats.compaction_usage.call_count(), 0);
    assert_eq!(stats.stage_message_count, Some(2));
    assert_eq!(agent.context_stats().unwrap(), stats);
    assert_eq!(
        connection
            .query_row::<i64, _, _>("PRAGMA data_version", [], |r| r.get(0))
            .unwrap(),
        before
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

// Break caught: a second crash cannot strand current-version processing forever.
#[tokio::test]
async fn final_exhausted_crash_processing_terminalizes_without_model_work() {
    use deepseek_cli::workflow_store::ProcessingLeaseMode;
    for paused in [false, true] {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("exhausted.sqlite3");
        let mut store = DialogStore::open(&database).unwrap();
        let started = store
            .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "goal")
            .unwrap();
        if paused {
            store.pause_current_task(started.dialog_id).unwrap();
        }
        let connection = rusqlite::Connection::open(&database).unwrap();
        if paused {
            connection
                .execute("UPDATE workflow_tasks SET status='active'", [])
                .unwrap();
        }
        let task = store
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap();
        let answer = store
            .append_answer_for_processing(AnswerCommit {
                dialog_id: started.dialog_id,
                task_id: task.id,
                stage_run_id: task.current_stage_run_id,
                expected_version: task.version,
                content: "saved answer",
                usage: None,
            })
            .unwrap();
        store
            .lease_processing(
                answer.processing_id,
                task.version,
                ProcessingLeaseMode::Normal,
            )
            .unwrap();
        store
            .lease_processing(
                answer.processing_id,
                task.version,
                ProcessingLeaseMode::Recovery,
            )
            .unwrap();
        if paused {
            connection
                .execute("UPDATE workflow_tasks SET status='paused'", [])
                .unwrap();
        }
        let before = store
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap();
        let models = Arc::new(AgentWorkflowModel::default());
        let mut agent = Agent::from_dialog(&managed_config(&server), store, started.dialog_id)
            .unwrap()
            .with_workflow_models(injected_models(&models));
        agent.recover_workflow_processing().await.unwrap();
        assert_eq!(
            agent.workflow_status().unwrap().unwrap().processing,
            Some(ProcessingStatus::Failed)
        );
        let row: (String, u32, String) = connection
            .query_row(
                "SELECT status,attempts,last_error FROM response_processing",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            ("failed".into(), 2, "recovery attempts exhausted".into())
        );
        assert!(
            agent
                .recover_workflow_processing()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            DialogStore::open(&database)
                .unwrap()
                .load_workflow(started.dialog_id)
                .unwrap()
                .current_task
                .unwrap(),
            before
        );
        assert_eq!(agent.history().messages().len(), 2);
        assert!(models.requests.lock().unwrap().is_empty());
        assert!(server.received_requests().await.unwrap().is_empty());
    }
}

// Break caught: the workflow diagnostic sink is held across awaits, so its
// trait-object bounds must not make otherwise Send public Agent futures local.
#[test]
fn public_agent_workflow_futures_remain_send() {
    fn assert_send<T: Send>(_: T) {}

    let config = Config::from_toml(
        "api_key='test-key'\nbase_url='http://127.0.0.1:9'\n[workflow]\n[context]\nstrategy='summary'",
        None,
    )
    .unwrap();
    let client = DeepSeekClient::new(&config).unwrap();

    let mut agent = Agent::from_client(client.clone(), "BASE");
    assert_send(agent.run_with_prompt("test"));

    let mut agent = Agent::from_client(client.clone(), "BASE");
    assert_send(agent.run_streaming("test", |_| Ok(())));

    let mut agent = Agent::from_client(client.clone(), "BASE");
    assert_send(agent.run_workflow_streaming("test", |_| Ok(())));

    let mut agent = Agent::from_client(client.clone(), "BASE");
    assert_send(agent.recover_workflow_processing());

    let mut agent = Agent::from_client(client, "BASE");
    assert_send(agent.recover_workflow_processing_streaming(|_| Ok(())));
}

// Break caught: status must observe the committed snapshot plus the newest
// processing row without leasing or otherwise advancing either one.
#[tokio::test]
async fn workflow_status_is_a_read_only_durable_projection() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("workflow-status.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "saved goal")
        .unwrap();
    store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: started.dialog_id,
            task_id: started.task.id,
            stage_run_id: started.stage_run_id,
            expected_version: started.task.version,
            content: "candidate answer",
            usage: None,
        })
        .unwrap();
    let before = store
        .load_workflow(started.dialog_id)
        .unwrap()
        .current_task
        .unwrap();
    drop(store);

    let agent = Agent::from_dialog(
        &managed_config(&server),
        DialogStore::open(&database).unwrap(),
        started.dialog_id,
    )
    .unwrap();
    let status = agent.workflow_status().unwrap().unwrap();

    assert_eq!(status.task_id, before.id);
    assert_eq!(status.ordinal, 1);
    assert_eq!(status.phase, TaskPhase::GoalDefinition);
    assert_eq!(status.goal, "saved goal");
    assert_eq!(status.goal_revision, 0);
    assert_eq!(status.goal_proposal_message_id, None);
    assert_eq!(status.status, TaskStatus::Active);
    assert_eq!(status.plan_revision, 0);
    assert_eq!(status.current_step_id, None);
    assert_eq!(status.expected_action, None);
    assert_eq!(status.stage_sequence, 1);
    assert_eq!(status.processing, Some(ProcessingStatus::Pending));
    assert!(server.received_requests().await.unwrap().is_empty());
    drop(agent);

    let store = DialogStore::open(&database).unwrap();
    assert_eq!(
        store
            .load_workflow(started.dialog_id)
            .unwrap()
            .current_task
            .unwrap(),
        before
    );
    assert_eq!(
        store.load_pending_processing(started.dialog_id).unwrap()[0].status,
        ProcessingStatus::Pending
    );
    drop(store);
    let disabled_agent = Agent::from_dialog(
        &config(&server),
        DialogStore::open(&database).unwrap(),
        started.dialog_id,
    )
    .unwrap();
    assert_eq!(
        disabled_agent.workflow_status().unwrap().unwrap().task_id,
        before.id
    );
}

// Break caught: UI consumers need typed, payload-free workflow failure and
// stop events instead of inferring lifecycle state from hidden controller data.
#[tokio::test]
async fn workflow_events_report_sanitized_processing_failure_and_stop_reason() {
    let server = MockServer::start().await;
    mount_sequence(&server, [sse("visible answer", 2, 1, 3)]).await;
    let service = Arc::new(AgentWorkflowModel::default());
    service
        .responses
        .lock()
        .unwrap()
        .push_back(continue_interpretation());
    service
        .responses
        .lock()
        .unwrap()
        .push_back("RAW_CHECKER_SECRET_11".into());
    let directory = tempfile::tempdir().unwrap();
    let (store, dialog_id) =
        approved_planning_store(&directory.path().join("workflow-events.sqlite3"));
    let mut agent = Agent::from_dialog(&managed_config(&server), store, dialog_id)
        .unwrap()
        .with_workflow_models(injected_models(&service));
    let mut events = Vec::new();

    agent
        .run_streaming("start task", |event| {
            if let AgentEvent::Workflow(event) = event {
                events.push(event);
            }
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(
        events,
        vec![
            WorkflowTurnEvent::ResponseStarted {
                autonomous_turn: 0,
                phase: TaskPhase::Planning,
            },
            WorkflowTurnEvent::ProcessingFailed {
                checker: "continuation".into(),
                error: "workflow checker failed".into(),
            },
            WorkflowTurnEvent::Stopped {
                reason: AutonomyStopReason::CheckerFailed,
            },
        ]
    );
    assert!(!format!("{events:?}").contains("RAW_CHECKER_SECRET_11"));
}

// Break caught: managed turns must emit useful workflow metadata in production,
// while default diagnostics remain free of every human/model/controller payload.
#[tokio::test]
async fn managed_workflow_writes_metadata_only_debug_event_by_default() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("ORDINARY_SECRET_ONE", 2, 1, 3),
            sse("ORDINARY_SECRET_TWO", 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("workflow-debug.sqlite3");
    let log_path = directory.path().join("workflow-debug.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\nsystem_prompt='BASE_SECRET'\n[workflow]\ninterpreter_model='interpreter-model'\nchecker_model='checker-model'\nhandoff_model='handoff-model'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=false",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let marker = "CONTROLLER_SECRET_11";
    let service = Arc::new(AgentWorkflowModel::default());
    service
        .responses
        .lock()
        .unwrap()
        .push_back(continue_interpretation());
    service.responses.lock().unwrap().push_back(
        json!({
            "patch": {"expected_version":1,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
            "decision": {"type":"continue","instruction":marker,"confidence":0.95}
        })
        .to_string(),
    );
    service.responses.lock().unwrap().push_back(await_check(2));
    let (store, dialog_id) = approved_planning_store(&database);
    let mut agent = Agent::from_dialog(&config, store, dialog_id)
        .unwrap()
        .with_workflow_models(injected_models(&service));

    agent.run_with_prompt("HUMAN_SECRET_11").await.unwrap();

    let log = std::fs::read_to_string(log_path).unwrap();
    let values: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| event["event"] == "workflow")
        .collect();
    let ordinary = values
        .iter()
        .find(|value| value["details"]["component"] == "ordinary")
        .unwrap();
    let checker = values
        .iter()
        .find(|value| value["details"]["component"] == "continuation")
        .unwrap();
    assert_eq!(ordinary["details"]["model"], "ordinary-model");
    assert_eq!(ordinary["details"]["autonomous_tokens"], 3);
    assert_eq!(checker["details"]["source"], "controller");
    assert_eq!(checker["details"]["model"], "agent-test-model");
    assert_eq!(checker["details"]["processing_status"], "completed");
    assert_eq!(checker["details"]["accepted"], false);
    for secret in [
        "HUMAN_SECRET_11",
        "ORDINARY_SECRET_ONE",
        "ORDINARY_SECRET_TWO",
        marker,
        "BASE_SECRET",
        "test-key",
    ] {
        assert!(!log.contains(secret), "workflow debug log leaked {secret}");
    }
    for value in values {
        assert!(value["details"].get("payload").is_none());
    }
}

#[tokio::test]
async fn goal_proposal_log_exposes_only_binding_id_without_payload_opt_in() {
    let server = MockServer::start().await;
    mount_sequence(&server, [sse("Предлагаемая цель: СЕКРЕТНАЯ_ЦЕЛЬ", 2, 1, 3)]).await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("goal-log.sqlite3");
    let log_path = directory.path().join("goal-log.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\n[workflow]\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=false",
            server.uri(), log_path
        ),
        None,
    )
    .unwrap();
    let mut agent = Agent::with_store(&config, DialogStore::open(&database).unwrap()).unwrap();
    agent
        .run_with_prompt("ОБСУДИМ_СЕКРЕТНУЮ_ЦЕЛЬ")
        .await
        .unwrap();
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(log.contains("goal_proposal_message_id"), "{log}");
    assert!(log.contains("\"goal_parse_result\":\"valid\""), "{log}");
    assert!(!log.contains("СЕКРЕТНАЯ_ЦЕЛЬ"), "{log}");
    assert!(!log.contains("ОБСУДИМ_СЕКРЕТНУЮ_ЦЕЛЬ"), "{log}");
}

// Break caught: an invalid checker transition is a rejected controller
// proposal, not an accepted human decision inferred from endpoint phases.
#[tokio::test]
async fn rejected_checker_transition_logs_the_real_failed_decision() {
    let server = MockServer::start().await;
    let proposed = json!({
        "patch": {"expected_version":1,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
        "decision": {"type":"emit_transition","event":"execution_completed","evidence":[],"confidence":0.95}
    })
    .to_string();
    mount_sequence(
        &server,
        [
            sse(&continue_interpretation(), 2, 1, 3),
            sse("visible answer", 2, 1, 3),
            sse(&proposed, 4, 2, 6),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("rejected-diagnostic.sqlite3");
    let log_path = directory.path().join("rejected-diagnostic.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\ninterpreter_model='interpreter-model'\nchecker_model='checker-model'\nhandoff_model='handoff-model'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=false",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let (store, dialog_id) = approved_planning_store(&database);
    let mut agent = Agent::from_dialog(&config, store, dialog_id).unwrap();

    agent.run_with_prompt("start task").await.unwrap();

    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| event["event"] == "workflow")
        .collect();
    let checker = events
        .iter()
        .find(|event| event["details"]["component"] == "continuation")
        .expect("checker decision diagnostic");
    let details = &checker["details"];
    assert_eq!(details["source"], "controller");
    assert_eq!(details["model"], "checker-model");
    assert_eq!(details["mode"], "advisory");
    assert_eq!(details["input_version"], 1);
    assert_eq!(details["output_version"], 1);
    assert_eq!(details["proposed_event"], "execution_completed");
    assert_eq!(details["accepted"], false);
    assert_eq!(details["outcome"], "failed");
    assert_eq!(details["processing_status"], "failed");
    assert!(details["processing_id"].as_i64().unwrap() > 0);
    assert_eq!(details["usage"]["total_tokens"], 6);
    assert!(details.get("payload").is_none());
}

// Break caught: an accepted transition has two real model decisions (checker
// and handoff), each with its own response and the same committed transition.
#[tokio::test]
async fn accepted_transition_logs_matching_checker_and_handoff_invocations() {
    let server = MockServer::start().await;
    let checker = json!({
        "patch": {
            "expected_version":1,
            "plan_append": {
                "steps":[{"id":"s1","description":"implement it","status":"pending"}],
                "acceptance_criteria":["it works"]
            },
            "step_updates":[],
            "current_step_id":"s1",
            "expected_action":"implement s1",
            "checkpoint":null
        },
        "decision": {"type":"emit_transition","event":"planning_completed","evidence":[],"confidence":0.95}
    })
    .to_string();
    let handoff = json!({
        "summary":"plan approved",
        "completed_step_ids":[],
        "next_step_id":"s1",
        "expected_action":"implement s1",
        "plan_changes":[],
        "decisions":[],
        "open_issues":[]
    })
    .to_string();
    let follow_up_interpreter = json!({
        "confidence": 0.95,
        "intent": {"type":"continue","instruction":"continue execution"}
    })
    .to_string();
    mount_sequence(
        &server,
        [
            sse(&continue_interpretation(), 2, 1, 3),
            sse("planning answer", 2, 1, 3),
            sse(&checker, 3, 2, 5),
            sse(&handoff, 4, 2, 6),
            sse("execution answer", 2, 1, 3),
            sse(&await_check(1), 2, 1, 3),
            sse(&follow_up_interpreter, 3, 2, 5),
            sse("follow-up answer", 2, 1, 3),
            sse(&await_check(1), 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("transition-diagnostic.sqlite3");
    let log_path = directory.path().join("transition-diagnostic.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\ninterpreter_model='interpreter-model'\nchecker_model='checker-model'\nhandoff_model='handoff-model'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let (store, dialog_id) = approved_planning_store(&database);
    let mut agent = Agent::from_dialog(&config, store, dialog_id).unwrap();

    agent.run_with_prompt("start task").await.unwrap();
    agent.run_with_prompt("more detail").await.unwrap();

    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| event["event"] == "workflow")
        .collect();
    let transition_checker = events
        .iter()
        .find(|event| {
            event["details"]["component"] == "continuation"
                && event["details"]["proposed_event"] == "planning_completed"
        })
        .expect("transition checker event");
    let handoff_event = events
        .iter()
        .find(|event| event["details"]["component"] == "handoff_builder")
        .expect("handoff event");
    assert_eq!(transition_checker["details"]["accepted"], true);
    assert_eq!(transition_checker["details"]["output_version"], 1);
    assert_eq!(
        transition_checker["details"]["processing_status"],
        "processing"
    );
    assert_eq!(
        transition_checker["details"]["payload"]["checker_output"],
        checker
    );
    assert_eq!(handoff_event["details"]["source"], "controller");
    assert_eq!(handoff_event["details"]["model"], "handoff-model");
    assert_eq!(handoff_event["details"]["accepted"], true);
    assert_eq!(handoff_event["details"]["payload"]["handoff"], handoff);
    assert_eq!(transition_checker["details"]["transition_id"], Value::Null);
    assert!(handoff_event["details"]["transition_id"].as_i64().unwrap() > 0);
    let interpreter = events
        .iter()
        .find(|event| event["details"]["component"] == "human_input_interpreter")
        .expect("follow-up interpreter event");
    assert_eq!(interpreter["details"]["proposed_event"], Value::Null);
    assert_eq!(interpreter["details"]["transition_id"], Value::Null);
}

// Break caught: a valid handoff response is a completed provider invocation
// even when its usage exhausts the autonomy budget before the transition can
// commit. Its exact opt-in response and usage must remain observable.
#[tokio::test]
async fn handoff_completed_before_token_rejection_is_logged_without_transition() {
    let server = MockServer::start().await;
    let checker = json!({
        "patch": {
            "expected_version":1,
            "plan_append": {
                "steps":[{"id":"s1","description":"implement it","status":"pending"}],
                "acceptance_criteria":["it works"]
            },
            "step_updates":[],
            "current_step_id":"s1",
            "expected_action":"implement s1",
            "checkpoint":null
        },
        "decision": {"type":"emit_transition","event":"planning_completed","evidence":[],"confidence":0.95}
    })
    .to_string();
    let handoff = json!({
        "summary":"valid but over budget",
        "completed_step_ids":[],
        "next_step_id":"s1",
        "expected_action":"implement s1",
        "plan_changes":[],
        "decisions":[],
        "open_issues":[]
    })
    .to_string();
    mount_sequence(
        &server,
        [
            sse(&continue_interpretation(), 0, 0, 0),
            sse("planning answer", 1, 0, 1),
            sse(&checker, 1, 0, 1),
            sse(&handoff, 1, 1, 2),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("handoff-budget.sqlite3");
    let log_path = directory.path().join("handoff-budget.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\nchecker_model='checker-model'\nhandoff_model='handoff-model'\nmax_autonomous_tokens=3\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let (store, dialog_id) = approved_planning_store(&database);
    let mut agent = Agent::from_dialog(&config, store, dialog_id).unwrap();

    assert_eq!(
        agent.run_with_prompt("start task").await.unwrap(),
        "planning answer"
    );
    let status = agent.workflow_status().unwrap().unwrap();
    assert_eq!(status.phase, TaskPhase::Planning);
    assert_eq!(status.stage_sequence, 1);

    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| event["event"] == "workflow")
        .collect();
    let handoff_event = events
        .iter()
        .find(|event| event["details"]["component"] == "handoff_builder")
        .expect("completed handoff invocation diagnostic");
    assert_eq!(handoff_event["details"]["model"], "handoff-model");
    assert_eq!(handoff_event["details"]["usage"]["total_tokens"], 2);
    assert_eq!(handoff_event["details"]["accepted"], false);
    assert_eq!(handoff_event["details"]["outcome"], "rejected");
    assert_eq!(handoff_event["details"]["transition_id"], Value::Null);
    assert_eq!(handoff_event["details"]["payload"]["handoff"], handoff);
}

// Break caught: when payload logging is explicitly enabled, a failed checker
// event owns the raw response that failed; it must not reuse a persisted intent.
#[tokio::test]
async fn failed_checker_payload_is_bound_to_its_own_invocation() {
    let server = MockServer::start().await;
    let raw_checker = json!({
        "patch": {"expected_version":1,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
        "decision": {"type":"emit_transition","event":"execution_completed","evidence":[],"confidence":0.95}
    })
    .to_string();
    mount_sequence(
        &server,
        [
            sse(&continue_interpretation(), 2, 1, 3),
            sse("visible answer", 2, 1, 3),
            sse(&raw_checker, 4, 2, 6),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("failed-checker-raw.sqlite3");
    let log_path = directory.path().join("failed-checker-raw.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\nchecker_model='checker-model'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let (store, dialog_id) = approved_planning_store(&database);
    let mut agent = Agent::from_dialog(&config, store, dialog_id).unwrap();

    agent.run_with_prompt("start task").await.unwrap();

    let checker = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["details"]["component"] == "continuation")
        .expect("failed checker event");
    assert_eq!(checker["details"]["accepted"], false);
    assert_eq!(checker["details"]["outcome"], "failed");
    assert_eq!(checker["details"]["payload"]["checker_output"], raw_checker);
    assert!(checker["details"]["payload"]["interpreter_output"].is_null());
}

// Break caught: checker output validation completes before its failed-status
// transaction. A later persistence failure must not erase the checker response.
#[tokio::test]
async fn checker_diagnostic_precedes_failure_persistence_error() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse(&continue_interpretation(), 2, 1, 3),
            sse("visible answer", 2, 1, 3),
            sse("CHECKER_INVALID_RAW", 3, 2, 5),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("checker-persistence.sqlite3");
    let log_path = directory.path().join("checker-persistence.jsonl");
    let (store, dialog_id) = approved_planning_store(&database);
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_checker_failure BEFORE UPDATE ON response_processing
             WHEN NEW.status='failed' BEGIN SELECT RAISE(ABORT,'injected'); END;",
        )
        .unwrap();
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\nchecker_model='checker-model'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let mut agent = Agent::from_dialog(&config, store, dialog_id).unwrap();

    assert!(matches!(
        agent.run_with_prompt("start task").await,
        Err(AgentError::Workflow(WorkflowEngineError::Store(_)))
    ));
    let checker = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["details"]["component"] == "continuation")
        .expect("completed checker invocation diagnostic");
    assert_eq!(checker["details"]["accepted"], false);
    assert_eq!(checker["details"]["outcome"], "failed");
    assert_eq!(checker["details"]["usage"]["total_tokens"], 5);
    assert_eq!(
        checker["details"]["payload"]["checker_output"],
        "CHECKER_INVALID_RAW"
    );
}

// Break caught: raw provider diagnostics are available only inside the opt-in
// payload envelope; the returned operator error remains body-free.
#[tokio::test]
async fn provider_error_body_is_opt_in_payload_only() {
    let server = MockServer::start().await;
    let marker = "OPT_IN_PROVIDER_BODY_SECRET";
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(400).set_body_string(marker))
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("provider-error-raw.sqlite3");
    let log_path = directory.path().join("provider-error-raw.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let mut agent = Agent::with_store(&config, DialogStore::open(&database).unwrap()).unwrap();

    let error = agent.run_with_prompt("start task").await.unwrap_err();

    assert!(!error.to_string().contains(marker));
    let operator = error.operator_message();
    assert!(!operator.contains(marker), "{operator}");
    assert_eq!(
        operator,
        "provider failure · component: ordinary · kind: http · status: 400"
    );
    let ordinary = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["details"]["component"] == "ordinary")
        .expect("ordinary failure event");
    assert!(
        ordinary["details"]["payload"]["provider_error"]
            .as_str()
            .unwrap()
            .contains(marker)
    );
    let mut metadata_only = ordinary.clone();
    metadata_only["details"]
        .as_object_mut()
        .unwrap()
        .remove("payload");
    assert!(!metadata_only.to_string().contains(marker));
}

// Break caught: a facts provider call has completed even when its body fails
// JSON validation. The failure metadata is always logged, while the exact raw
// response is present only under the explicit payload opt-in.
#[tokio::test]
async fn malformed_facts_output_is_logged_at_the_provider_boundary() {
    for log_payloads in [false, true] {
        let server = MockServer::start().await;
        let marker = format!("MALFORMED_FACTS_RAW_SECRET_{log_payloads}");
        mount_sequence(&server, [sse(&marker, 3, 2, 5)]).await;
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("malformed-facts.sqlite3");
        let log_path = directory.path().join("malformed-facts.jsonl");
        let config = Config::from_toml(
            &format!(
                "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\n[context]\nstrategy='sticky_facts'\nfacts_max_tokens=64\n[debug]\nlog_path={:?}\nlog_payloads={log_payloads}",
                server.uri(),
                log_path
            ),
            None,
        )
        .unwrap();
        let mut agent = Agent::with_store(&config, DialogStore::open(&database).unwrap()).unwrap();

        let error = agent.run_with_prompt("remember this").await.unwrap_err();
        assert!(!error.operator_message().contains(&marker));

        let events: Vec<Value> = std::fs::read_to_string(&log_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .filter(|event: &Value| event["event"] == "workflow")
            .collect();
        let facts = events
            .iter()
            .find(|event| event["details"]["component"] == "facts")
            .expect("malformed facts diagnostic");
        assert_eq!(facts["details"]["accepted"], false);
        assert_eq!(facts["details"]["outcome"], "failed");
        assert_eq!(facts["details"]["error_kind"], "invalid_output");
        assert_eq!(facts["details"]["usage"]["total_tokens"], 5);
        if log_payloads {
            assert_eq!(facts["details"]["payload"]["model_output"], marker);
        } else {
            assert!(facts["details"].get("payload").is_none(), "{facts:#?}");
            assert!(
                !std::fs::read_to_string(&log_path)
                    .unwrap()
                    .contains(&marker)
            );
        }
    }
}

// Break caught: opting into payload logging must expose persisted workflow
// audit payloads only below `payload`, never as top-level metadata.
#[tokio::test]
async fn managed_workflow_nests_controller_and_checker_payloads_when_enabled() {
    let server = MockServer::start().await;
    let marker = "OPT_IN_CONTROLLER_SECRET_11";
    let first_checker = json!({
        "patch": {"expected_version":1,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
        "decision": {"type":"continue","instruction":marker,"confidence":0.95}
    })
    .to_string();
    let second_checker = json!({
        "patch": {"expected_version":2,"plan_append":{"steps":[],"acceptance_criteria":[]},"step_updates":[],"current_step_id":null,"expected_action":null,"checkpoint":null},
        "decision": {"type":"await_user"}
    })
    .to_string();
    mount_sequence(
        &server,
        [
            sse(&continue_interpretation(), 2, 1, 3),
            sse("first answer", 2, 1, 3),
            sse(&first_checker, 2, 1, 3),
            sse("second answer", 2, 1, 3),
            sse(&second_checker, 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("workflow-debug-full.sqlite3");
    let log_path = directory.path().join("workflow-debug-full.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let (store, dialog_id) = approved_planning_store(&database);
    let mut agent = Agent::from_dialog(&config, store, dialog_id).unwrap();

    agent.run_with_prompt("human prompt").await.unwrap();

    let values: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| event["event"] == "workflow")
        .collect();
    let checkers: Vec<&Value> = values
        .iter()
        .filter(|event| event["details"]["component"] == "continuation")
        .collect();
    assert_eq!(checkers.len(), 2, "{values:#?}");
    assert_eq!(
        checkers[0]["details"]["payload"]["checker_output"],
        first_checker
    );
    assert_eq!(
        checkers[0]["details"]["payload"]["controller_instruction"],
        marker
    );
    assert_eq!(
        checkers[1]["details"]["payload"]["checker_output"],
        second_checker
    );
    assert!(
        checkers[1]["details"]["payload"]["controller_instruction"].is_null(),
        "{:#?}",
        checkers[1]
    );
    let ordinary: Vec<&Value> = values
        .iter()
        .filter(|event| event["details"]["component"] == "ordinary")
        .collect();
    assert_eq!(ordinary.len(), 2, "{values:#?}");
    assert_eq!(ordinary[0]["details"]["processing_status"], "pending");
    assert_eq!(ordinary[1]["details"]["processing_status"], "pending");
    assert!(ordinary[0]["details"]["processing_id"].as_i64().unwrap() > 0);
    assert!(ordinary[1]["details"]["processing_id"].as_i64().unwrap() > 0);
    assert_eq!(
        ordinary[0]["details"]["payload"]["model_output"],
        "first answer"
    );
    assert_eq!(
        ordinary[1]["details"]["payload"]["model_output"],
        "second answer"
    );
    for value in &values {
        if value["details"]["component"] == "human_input_interpreter" {
            continue;
        }
        assert!(
            value["details"]["payload"]["interpreter_output"].is_null(),
            "another invocation reused interpreter output: {value:#?}"
        );
        let mut metadata_only = value.clone();
        metadata_only["details"]
            .as_object_mut()
            .unwrap()
            .remove("payload");
        assert!(!metadata_only.to_string().contains(marker));
    }
}

// Break caught: interpreter_output must be the response from this exact model
// invocation, not the durable normalized intent written later by routing.
#[tokio::test]
async fn workflow_payload_logging_captures_matching_interpreter_response() {
    let server = MockServer::start().await;
    let raw_interpreter = json!({
        "confidence": 0.95,
        "intent": {"type":"continue","instruction":"interpreted follow-up"}
    })
    .to_string();
    mount_sequence(
        &server,
        [
            sse("first answer", 2, 1, 3),
            sse(&raw_interpreter, 3, 2, 5),
            sse("second answer", 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("interpreter-raw.sqlite3");
    let log_path = directory.path().join("interpreter-raw.jsonl");
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\ninterpreter_model='interpreter-model'\nchecker_model='checker-model'\nhandoff_model='handoff-model'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let mut agent = Agent::with_store(&config, DialogStore::open(&database).unwrap()).unwrap();

    agent.run_with_prompt("start task").await.unwrap();
    agent.run_with_prompt("follow up").await.unwrap();

    let values: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &Value| event["event"] == "workflow")
        .collect();
    let interpreters: Vec<&Value> = values
        .iter()
        .filter(|event| event["details"]["component"] == "human_input_interpreter")
        .collect();
    assert_eq!(interpreters.len(), 1, "{values:#?}");
    assert_eq!(interpreters[0]["details"]["model"], "interpreter-model");
    assert_eq!(interpreters[0]["details"]["usage"]["total_tokens"], 5);
    assert_eq!(
        interpreters[0]["details"]["payload"]["interpreter_output"],
        raw_interpreter
    );
}

// Break caught: interpretation completes before the accepted human input is
// persisted. A later SQLite failure must not erase that completed invocation.
#[tokio::test]
async fn interpreter_diagnostic_precedes_routing_persistence_failure() {
    let server = MockServer::start().await;
    let raw_interpreter = json!({
        "confidence":0.95,
        "intent":{"type":"continue","instruction":"continue safely"}
    })
    .to_string();
    mount_sequence(&server, [sse(&raw_interpreter, 3, 2, 5)]).await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("interpreter-persistence.sqlite3");
    let log_path = directory.path().join("interpreter-persistence.jsonl");
    let mut store = DialogStore::open(&database).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&RequestScope::default(), "BASE", "saved goal")
        .unwrap();
    drop(store);
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_followup BEFORE INSERT ON messages
             WHEN NEW.role='user' BEGIN SELECT RAISE(ABORT,'injected'); END;",
        )
        .unwrap();
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\ninterpreter_model='interpreter-model'\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let mut agent = Agent::from_dialog(
        &config,
        DialogStore::open(&database).unwrap(),
        started.dialog_id,
    )
    .unwrap();

    assert!(matches!(
        agent.run_with_prompt("follow up").await,
        Err(AgentError::Workflow(WorkflowEngineError::Store(_)))
    ));
    let interpreter = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["details"]["component"] == "human_input_interpreter")
        .expect("completed interpreter diagnostic");
    assert_eq!(interpreter["details"]["accepted"], true);
    assert_eq!(interpreter["details"]["usage"]["total_tokens"], 5);
    assert_eq!(
        interpreter["details"]["payload"]["interpreter_output"],
        raw_interpreter
    );
}

// Break caught: an ordinary response is a completed provider invocation even
// if the subsequent assistant-message transaction fails.
#[tokio::test]
async fn ordinary_diagnostic_precedes_answer_persistence_failure() {
    let server = MockServer::start().await;
    mount_sequence(&server, [sse("ORDINARY_PERSISTENCE_RAW", 3, 2, 5)]).await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("ordinary-persistence.sqlite3");
    let log_path = directory.path().join("ordinary-persistence.jsonl");
    let store = DialogStore::open(&database).unwrap();
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_answer BEFORE INSERT ON messages
             WHEN NEW.role='assistant' BEGIN SELECT RAISE(ABORT,'injected'); END;",
        )
        .unwrap();
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\n[context]\nstrategy='summary'\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let mut agent = Agent::with_store(&config, store).unwrap();

    assert!(matches!(
        agent.run_with_prompt("start task").await,
        Err(AgentError::Workflow(WorkflowEngineError::Store(_)))
    ));
    let ordinary = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["details"]["component"] == "ordinary")
        .expect("completed ordinary invocation diagnostic");
    assert_eq!(ordinary["details"]["accepted"], false);
    assert_eq!(ordinary["details"]["outcome"], "failed");
    assert_eq!(ordinary["details"]["error_kind"], "persistence");
    assert_eq!(ordinary["details"]["usage"]["total_tokens"], 5);
    assert_eq!(
        ordinary["details"]["payload"]["model_output"],
        "ORDINARY_PERSISTENCE_RAW"
    );
}

// Break caught: a completed summary response must remain observable when the
// later stage-context update fails; compaction still fails open for the answer.
#[tokio::test]
async fn compaction_diagnostic_precedes_summary_persistence_failure() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("visible answer", 8, 1, 9),
            sse("SUMMARY_PERSISTENCE_RAW", 3, 2, 5),
            sse(&await_check(0), 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("summary-persistence.sqlite3");
    let log_path = directory.path().join("summary-persistence.jsonl");
    let store = DialogStore::open(&database).unwrap();
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_summary BEFORE UPDATE ON task_stage_context
             BEGIN SELECT RAISE(ABORT,'injected'); END;",
        )
        .unwrap();
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\nmodel='ordinary-model'\n[workflow]\nchecker_model='checker-model'\n[context]\nstrategy='summary'\ncompact_after_prompt_tokens=1\nkeep_last_messages=1\nsummary_max_tokens=64\n[debug]\nlog_path={:?}\nlog_payloads=true",
            server.uri(),
            log_path
        ),
        None,
    )
    .unwrap();
    let mut agent = Agent::with_store(&config, store).unwrap();

    assert_eq!(
        agent.run_with_prompt("start task").await.unwrap(),
        "visible answer"
    );
    let compaction = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["details"]["component"] == "compaction")
        .expect("completed compaction invocation diagnostic");
    assert_eq!(compaction["details"]["accepted"], false);
    assert_eq!(compaction["details"]["outcome"], "failed");
    assert_eq!(compaction["details"]["error_kind"], "persistence");
    assert_eq!(compaction["details"]["usage"]["total_tokens"], 5);
    assert_eq!(
        compaction["details"]["payload"]["model_output"],
        "SUMMARY_PERSISTENCE_RAW"
    );
}

// Break caught: restoring an Agent must expose advisory recovery without running the ordinary model.
struct InterruptedAcceptanceChecker {
    started: std::sync::atomic::AtomicBool,
}

impl CompletionModel for InterruptedAcceptanceChecker {
    fn name(&self) -> &str {
        "interrupted-acceptance-checker"
    }

    fn complete(&self, _request: ModelRequest) -> ModelFuture<'_> {
        self.started
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
}

// Break caught: a crash after the durable answer must neither duplicate that
// answer/job nor autonomously resume; later human continue must retain the plan and step.
#[tokio::test]
async fn acceptance_restart_recovers_once_then_human_continues_the_stored_execution_step() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("PLANNING_RAW_BEFORE_CRASH", 2, 1, 3),
            sse("DURABLE_EXECUTION_ANSWER", 2, 1, 3),
            sse("RESUMED_EXECUTION_ANSWER", 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("restart-acceptance.sqlite3");
    let config = managed_config(&server);
    let service = Arc::new(AgentWorkflowModel::default());
    let mut plan: Value = serde_json::from_str(&await_check(1)).unwrap();
    plan["patch"]["plan_append"] = json!({"steps":[{"id":"stored-build-step","description":"Implement persisted design","status":"pending"}],"acceptance_criteria":["tests pass"]});
    plan["patch"]["current_step_id"] = json!("stored-build-step");
    service.responses.lock().unwrap().extend([
        continue_interpretation(),
        plan.to_string(),
        json!({"confidence":0.95,"intent":{"type":"propose_transition","event":"planning_completed","evidence":[]}}).to_string(),
        json!({"summary":"STORED_DESIGN_CHECKPOINT","completed_step_ids":[],"next_step_id":"stored-build-step","expected_action":"Implement persisted design","plan_changes":[],"decisions":[],"open_issues":[]}).to_string(),
    ]);
    let (store, dialog_id) = approved_planning_store(&database);
    let mut agent = Agent::from_dialog(&config, store, dialog_id)
        .unwrap()
        .with_workflow_models(injected_models(&service));
    agent.run_with_prompt("design a parser").await.unwrap();
    let id = agent.dialog_id().unwrap();
    let blocker = Arc::new(InterruptedAcceptanceChecker {
        started: false.into(),
    });
    agent = agent.with_workflow_models(WorkflowModels {
        interpreter: service.clone(),
        checker: blocker.clone(),
        handoff: service.clone(),
    });
    let mut turn = Box::pin(agent.run_with_prompt("plan accepted; execute"));
    tokio::select! {
        result = turn.as_mut() => panic!("checker must still be pending: {result:?}"),
        _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !blocker.started.load(std::sync::atomic::Ordering::SeqCst) { tokio::task::yield_now().await; }
        }) => assert!(blocker.started.load(std::sync::atomic::Ordering::SeqCst)),
    }
    drop(turn);
    drop(agent);
    let store = DialogStore::open(&database).unwrap();
    let before = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(before.phase, TaskPhase::Execution);
    assert_eq!(before.current_step_id.as_deref(), Some("stored-build-step"));
    assert_eq!(before.version, 3);
    let pending = store.load_pending_processing(id).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, ProcessingStatus::Processing);
    assert_eq!(pending[0].attempts, 1);
    assert_eq!(
        store
            .load(id)
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.content() == "DURABLE_EXECUTION_ANSWER")
            .count(),
        1
    );

    let recovery = Arc::new(AgentWorkflowModel {
        usage: Some(deepseek_cli::client::TokenUsage {
            prompt_tokens: 2,
            completion_tokens: 1,
            total_tokens: 3,
            completion_tokens_details: None,
        }),
        ..Default::default()
    });
    let mut proposed: Value = serde_json::from_str(&await_check(3)).unwrap();
    proposed["patch"]["expected_action"] = json!("Resume stored build");
    proposed["decision"] =
        json!({"type":"continue","instruction":"MUST_NOT_AUTONOMOUSLY_RESUME","confidence":0.95});
    recovery.responses.lock().unwrap().extend([
        proposed.to_string(),
        json!({"confidence":0.95,"intent":{"type":"continue","instruction":"continue"}})
            .to_string(),
        await_check(5),
    ]);
    let mut resumed = Agent::from_dialog(&config, store, id)
        .unwrap()
        .with_workflow_models(injected_models(&recovery));
    assert_eq!(recovery.requests.lock().unwrap().len(), 0);
    let results = resumed.recover_workflow_processing().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].stop_reason,
        AutonomyStopReason::AwaitUserAfterRestart
    );
    assert!(
        resumed
            .recover_workflow_processing()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(recovery.requests.lock().unwrap().len(), 1);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    let store = DialogStore::open(&database).unwrap();
    let recovered = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(recovered.current_stage_run_id, before.current_stage_run_id);
    assert_eq!(recovered.plan, before.plan);
    assert_eq!(recovered.version, 4);
    let connection = rusqlite::Connection::open(&database).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM workflow_inputs WHERE source='controller'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT attempts FROM response_processing WHERE id=?1",
                [pending[0].id],
                |row| row.get::<_, u32>(0)
            )
            .unwrap(),
        2
    );

    assert_eq!(
        resumed.run_with_prompt("continue").await.unwrap(),
        "RESUMED_EXECUTION_ANSWER"
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    let body: Value = requests[2].body_json().unwrap();
    let text = body["messages"].to_string();
    for marker in [
        "stored-build-step",
        "Implement persisted design",
        "STORED_DESIGN_CHECKPOINT",
        "Resume stored build",
        "DURABLE_EXECUTION_ANSWER",
    ] {
        assert!(text.contains(marker), "{text}");
    }
    assert!(!text.contains("PLANNING_RAW_BEFORE_CRASH"));
    assert!(!text.contains("MUST_NOT_AUTONOMOUSLY_RESUME"));
    assert_eq!(
        body["messages"].as_array().unwrap().last().unwrap()["content"],
        "continue"
    );
    let after = store.load_workflow(id).unwrap().current_task.unwrap();
    assert_eq!(after.id, before.id);
    assert_eq!(after.plan, before.plan);
    assert_eq!(after.current_stage_run_id, before.current_stage_run_id);
    assert_eq!(after.current_step_id, before.current_step_id);
    assert_eq!(after.version, 5);
    assert_eq!(recovery.requests.lock().unwrap().len(), 3);
    assert_eq!(
        store
            .load(id)
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.content() == "DURABLE_EXECUTION_ANSWER")
            .count(),
        1
    );
}

#[tokio::test]
async fn resumed_agent_recovers_advisory_work_without_autonomous_resume() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("recovery.sqlite3")).unwrap();
    let started = store
        .start_dialog_with_workflow_task(&Default::default(), "BASE", "saved goal")
        .unwrap();
    store
        .append_answer_for_processing(deepseek_cli::workflow_store::AnswerCommit {
            dialog_id: started.dialog_id,
            task_id: started.task.id,
            stage_run_id: started.stage_run_id,
            expected_version: 0,
            content: "saved answer",
            usage: None,
        })
        .unwrap();
    let service = Arc::new(AgentWorkflowModel::default());
    service.responses.lock().unwrap().push_back(await_check(0));
    let mut agent = Agent::from_dialog(&managed_config(&server), store, started.dialog_id)
        .unwrap()
        .with_workflow_models(injected_models(&service));
    let recovered = agent.recover_workflow_processing().await.unwrap();
    assert_eq!(
        recovered[0].stop_reason,
        deepseek_cli::workflow_engine::AutonomyStopReason::AwaitUserAfterRestart
    );
    assert!(
        agent
            .recover_workflow_processing()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(agent.history().messages().len(), 2);
    assert!(server.received_requests().await.unwrap().is_empty());
}

// Break caught: workflow-enabled persistent agents must create a tagged task/input and pending answer job.
#[tokio::test]
async fn managed_agent_persists_task_and_uses_injected_interpreter_before_the_next_answer() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [sse("first answer", 2, 1, 3), sse("second answer", 2, 1, 3)],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let service = Arc::new(AgentWorkflowModel::default());
    service.responses.lock().unwrap().push_back(
        json!({"confidence":0.95,"intent":{"type":"continue","instruction":"continue plan"}})
            .to_string(),
    );
    let mut agent = Agent::with_store(
        &managed_config(&server),
        DialogStore::open(&database).unwrap(),
    )
    .unwrap()
    .with_workflow_models(injected_models(&service));
    agent.run_with_prompt("create a parser").await.unwrap();
    assert_eq!(
        service.requests.lock().unwrap().len(),
        0,
        "goal definition waits for human input without an advisory checker"
    );
    let observer = DialogStore::open(&database).unwrap();
    let first = observer
        .load_workflow(agent.dialog_id().unwrap())
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(first.phase, TaskPhase::GoalDefinition);
    assert_eq!(
        observer
            .load_pending_processing(first.dialog_id)
            .unwrap()
            .len(),
        0
    );
    agent.run_with_prompt("continue plan").await.unwrap();
    {
        let requests = service.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "the next human input is interpreted");
        let interpreted: Value =
            serde_json::from_str(requests[0].messages.last().unwrap().content()).unwrap();
        assert_eq!(interpreted["human_text"], "continue plan");
        assert!(
            !requests[0]
                .messages
                .last()
                .unwrap()
                .content()
                .contains("first answer")
        );
    }
    let task = observer
        .load_workflow(first.dialog_id)
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(task.version, 1);
    assert_eq!(agent.history().messages().len(), 4);
    assert_eq!(
        observer.load(first.dialog_id).unwrap().messages,
        agent.history().messages()
    );
    let ordinary = server.received_requests().await.unwrap();
    assert_eq!(ordinary.len(), 2);
    let body: Value = ordinary[1].body_json().unwrap();
    let text = body["messages"].to_string();
    assert!(text.contains("workflow") || text.contains("current_step_id"));
    assert_eq!(
        body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["content"] == "continue plan")
            .count(),
        1
    );
}

// Break caught: constructors must honor each configured service model and route a transition before ordinary HTTP.
#[tokio::test]
async fn managed_agent_uses_configured_service_models_and_restored_stage_context() {
    let server = MockServer::start().await;
    let interpretation=json!({"confidence":0.95,"intent":{"type":"propose_transition","event":"execution_completed","evidence":["build green"]}}).to_string();
    let handoff=json!({"summary":"CURRENT CHECKPOINT","completed_step_ids":[],"next_step_id":null,"expected_action":"validate","plan_changes":[],"decisions":[],"open_issues":[]}).to_string();
    mount_sequence(
        &server,
        [
            sse("old execution answer", 2, 1, 3),
            sse(&interpretation, 2, 1, 3),
            sse(&handoff, 2, 1, 3),
            sse("validation answer", 2, 1, 3),
            sse(&await_check(2), 2, 1, 3),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let config = managed_config(&server);
    let mut agent = Agent::with_store(&config, DialogStore::open(&database).unwrap()).unwrap();
    agent.run_with_prompt("execution-only input").await.unwrap();
    let id = agent.dialog_id().unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection.execute_batch("UPDATE workflow_tasks SET phase='execution',goal='current task goal',goal_revision=1; UPDATE task_stage_runs SET phase='execution';").unwrap();
    drop(agent);
    let mut agent = Agent::from_dialog(&config, DialogStore::open(&database).unwrap(), id).unwrap();
    assert_eq!(
        agent
            .run_with_prompt("implementation done; test it")
            .await
            .unwrap(),
        "validation answer"
    );
    let requests = server.received_requests().await.unwrap();
    let bodies: Vec<Value> = requests.iter().map(|r| r.body_json().unwrap()).collect();
    assert_eq!(
        bodies
            .iter()
            .map(|b| b["model"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "ordinary-model",
            "interpreter-model",
            "handoff-model",
            "ordinary-model",
            "checker-model"
        ]
    );
    let ordinary = bodies[3]["messages"].to_string();
    assert!(ordinary.contains("validation"));
    assert!(ordinary.contains("CURRENT CHECKPOINT"));
    assert!(!ordinary.contains("execution-only input"));
    assert!(!ordinary.contains("old execution answer"));
}

// Break caught: enabling workflow cannot change in-memory pair-commit semantics or call service models.
#[tokio::test]
async fn workflow_enabled_in_memory_agents_still_use_the_legacy_path() {
    let server = MockServer::start().await;
    mount(&server, response("answer", true)).await;
    let service = Arc::new(AgentWorkflowModel::default());
    let mut agent = Agent::new(&managed_config(&server))
        .unwrap()
        .with_workflow_models(injected_models(&service));
    agent.run_with_prompt("one").await.unwrap();
    agent.run_with_prompt("two").await.unwrap();
    assert!(agent.dialog_id().is_none());
    assert!(service.requests.lock().unwrap().is_empty());
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[1].body_json().unwrap();
    assert_eq!(body["messages"].as_array().unwrap().len(), 4);
    assert_eq!(body["messages"][1]["content"], "one");
}

// Break caught: dropping an in-flight first turn cannot lose the newly persisted dialog/task identity.
#[tokio::test]
async fn cancelled_managed_first_turn_keeps_session_identity_and_input() {
    let server = MockServer::start().await;
    mount(
        &server,
        response("late answer", true).set_delay(std::time::Duration::from_secs(2)),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(
        &managed_config(&server),
        DialogStore::open(&database).unwrap(),
    )
    .unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(40),
            agent.run_with_prompt("create task")
        )
        .await
        .is_err()
    );
    let store = DialogStore::open(&database).unwrap();
    let task = store
        .load_workflow(agent.dialog_id().unwrap())
        .unwrap()
        .current_task
        .unwrap();
    assert_eq!(task.status, TaskStatus::Active);
    assert_eq!(agent.scope().dialog_id(), Some(task.dialog_id));
    assert_eq!(agent.history().messages().len(), 1);
    assert_eq!(store.load(task.dialog_id).unwrap().messages.len(), 1);
    assert!(
        store
            .load_pending_processing(task.dialog_id)
            .unwrap()
            .is_empty()
    );
}

// Break caught: a managed fork checkpoint counts hidden protocol, while transcript replay stays visible-only.
#[tokio::test]
async fn restored_managed_dialog_with_hidden_controller_input_can_branch() {
    use deepseek_cli::workflow::{TaskStatePatch, WorkflowIntent};
    use deepseek_cli::workflow_store::{ControllerInputCommit, ProcessingLeaseMode};
    let server = MockServer::start().await;
    mount(&server, response("saved answer", true)).await;
    let config = Config::from_toml(
        &format!(
            "api_key='test-key'\nbase_url='{}'\n[context]\nstrategy='branching'",
            server.uri()
        ),
        None,
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let (mut store, id) = approved_planning_store(&database);
    let task = store.load_workflow(id).unwrap().current_task.unwrap();
    store
        .append_answer_for_processing(AnswerCommit {
            dialog_id: id,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            content: "saved answer",
            usage: None,
        })
        .unwrap();
    let processing = store.load_pending_processing(id).unwrap().remove(0);
    let lease = store
        .lease_processing(processing.id, task.version, ProcessingLeaseMode::Normal)
        .unwrap()
        .unwrap();
    let intent = WorkflowIntent::human_continue("HIDDEN CONTROLLER").unwrap();
    store
        .commit_controller_decision(ControllerInputCommit {
            processing_id: processing.id,
            expected_attempt: lease.attempts,
            task_id: task.id,
            stage_run_id: task.current_stage_run_id,
            expected_version: task.version,
            checker: "continuation",
            model: "checker",
            triggering_assistant_message_id: processing.assistant_message_id,
            instruction: "HIDDEN CONTROLLER",
            intent: &intent,
            confidence: 0.95,
            accepted_patch: &TaskStatePatch {
                expected_version: task.version,
                plan_append: Default::default(),
                step_updates: vec![],
                current_step_id: None,
                expected_action: None,
                checkpoint: None,
            },
        })
        .unwrap();
    let mut restored = Agent::from_dialog(&config, store, id).unwrap();
    assert_eq!(restored.history().messages().len(), 2);
    let fork = restored.branch_dialog().unwrap();
    assert_eq!(fork.checkpoint_message_count, 3);
    let store = DialogStore::open(&database).unwrap();
    let branch = store
        .load_workflow(fork.new_dialog_id)
        .unwrap()
        .current_task
        .unwrap();
    assert_ne!(branch.id, task.id);
    assert_eq!(
        store
            .load_stage_messages(branch.current_stage_run_id)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(store.load(fork.new_dialog_id).unwrap().messages.len(), 2);
    restored.switch_branch(fork.new_dialog_id).unwrap();
    assert!(
        restored
            .history()
            .messages()
            .iter()
            .all(|m| m.content() != "HIDDEN CONTROLLER")
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

fn agent(server: &MockServer) -> Agent {
    Agent::new(&config(server)).unwrap()
}

fn compression_config(server: &MockServer, threshold: u64, keep: usize) -> Config {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"summary\"\ncompact_after_prompt_tokens = {threshold}\nkeep_last_messages = {keep}\nsummary_max_tokens = 64\n",
        server.uri(),
    )
    .unwrap();
    Config::load(file.path(), None).unwrap()
}

fn sticky_config(server: &MockServer, keep: usize) -> Config {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"sticky_facts\"\nkeep_last_messages = {keep}\nfacts_max_tokens = 64\n",
        server.uri(),
    )
    .unwrap();
    Config::load(file.path(), None).unwrap()
}

fn branching_config(server: &MockServer) -> Config {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"branching\"\n",
        server.uri(),
    )
    .unwrap();
    Config::load(file.path(), None).unwrap()
}

#[derive(Clone)]
struct SequenceResponder {
    responses: Arc<Mutex<VecDeque<ResponseTemplate>>>,
}

impl Respond for SequenceResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra request")
    }
}

async fn mount_sequence(
    server: &MockServer,
    responses: impl IntoIterator<Item = ResponseTemplate>,
) {
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        })
        .mount(server)
        .await;
}

fn sse(
    answer: &str,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
) -> ResponseTemplate {
    let chunk = json!({
        "choices": [{"delta": {"content": answer}, "finish_reason": "stop"}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens
        }
    });
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Seen {
    CompactionStarted,
    CompactionCompleted,
    CompactionFailed,
    Other,
}

fn seen(event: AgentEvent<'_>) -> Seen {
    match event {
        AgentEvent::CompactionStarted { .. } => Seen::CompactionStarted,
        AgentEvent::CompactionCompleted { .. } => Seen::CompactionCompleted,
        AgentEvent::CompactionFailed { .. } => Seen::CompactionFailed,
        AgentEvent::Text(_)
        | AgentEvent::ToolStarted { .. }
        | AgentEvent::ToolFinished { .. }
        | AgentEvent::Usage(_)
        | AgentEvent::FactsUpdateStarted { .. }
        | AgentEvent::FactsUpdateCompleted { .. }
        | AgentEvent::FactsUpdateFailed { .. }
        | AgentEvent::DebugLogFailed { .. }
        | AgentEvent::Workflow(_) => Seen::Other,
    }
}

#[tokio::test]
async fn ordinary_request_includes_user_then_task_memory() {
    let server = MockServer::start().await;
    mount(&server, response("Answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    let mut agent = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap(),
        scope,
    )
    .unwrap();
    agent
        .remember(DurableMemoryScope::User, "language", "Russian")
        .unwrap();
    agent
        .remember(DurableMemoryScope::Task, "stack", "Rust")
        .unwrap();

    agent
        .run_with_prompt("What context do you have?")
        .await
        .unwrap();

    let request = &server.received_requests().await.unwrap()[0];
    let body: Value = request.body_json().unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert!(messages[1]["content"].as_str().unwrap().contains("Russian"));
    assert!(messages[2]["content"].as_str().unwrap().contains("Rust"));
    assert_eq!(
        messages.last().unwrap()["content"],
        "What context do you have?"
    );
}

#[tokio::test]
async fn different_users_automatically_receive_only_their_profiles() {
    let server = MockServer::start().await;
    mount(&server, response("Answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut alice = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&path).unwrap(),
        RequestScope::new("alice", "mobile").unwrap(),
    )
    .unwrap();
    alice
        .replace_profile("ALICE_PROFILE prefers Android")
        .unwrap();
    alice
        .remember(DurableMemoryScope::User, "language", "Kotlin")
        .unwrap();
    alice
        .remember(DurableMemoryScope::Task, "ui", "Compose")
        .unwrap();
    let mut bob = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&path).unwrap(),
        RequestScope::new("bob", "mobile").unwrap(),
    )
    .unwrap();
    bob.replace_profile("BOB_PROFILE prefers Flutter").unwrap();

    alice.run_with_prompt("Design an app").await.unwrap();
    bob.run_with_prompt("Design an app").await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let alice_request: Value = requests[0].body_json().unwrap();
    let alice_messages = alice_request["messages"].as_array().unwrap();
    assert!(
        alice_messages[1]["content"]
            .as_str()
            .unwrap()
            .contains("ALICE_PROFILE")
    );
    assert!(
        alice_messages[2]["content"]
            .as_str()
            .unwrap()
            .contains("Kotlin")
    );
    assert!(
        alice_messages[3]["content"]
            .as_str()
            .unwrap()
            .contains("Compose")
    );
    assert!(
        !serde_json::to_string(alice_messages)
            .unwrap()
            .contains("BOB_PROFILE")
    );
    let bob_request: Value = requests[1].body_json().unwrap();
    let bob_serialized = serde_json::to_string(&bob_request["messages"]).unwrap();
    assert!(bob_serialized.contains("BOB_PROFILE"));
    assert!(!bob_serialized.contains("ALICE_PROFILE"));
}

#[tokio::test]
async fn profile_is_reloaded_each_request_and_excluded_from_compaction() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("answer one", 2, 1, 3),
            sse("answer two", 4, 2, 6),
            sse("summary", 7, 2, 9),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(
        &compression_config(&server, 3, 2),
        DialogStore::open(&path).unwrap(),
    )
    .unwrap();
    agent.replace_profile("PROFILE_ONE").unwrap();
    agent.run_with_prompt("First").await.unwrap();
    let mut writer = Agent::with_store(
        &compression_config(&server, 3, 2),
        DialogStore::open(&path).unwrap(),
    )
    .unwrap();
    writer.replace_profile("PROFILE_TWO").unwrap();
    agent.run_with_prompt("Second").await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    let first = serde_json::to_string(&requests[0].body_json::<Value>().unwrap()).unwrap();
    let second = serde_json::to_string(&requests[1].body_json::<Value>().unwrap()).unwrap();
    let compaction = serde_json::to_string(&requests[2].body_json::<Value>().unwrap()).unwrap();
    assert!(first.contains("PROFILE_ONE"));
    assert!(!first.contains("PROFILE_TWO"));
    assert!(second.contains("PROFILE_TWO"));
    assert!(!second.contains("PROFILE_ONE"));
    assert!(!compaction.contains("PROFILE_ONE"));
    assert!(!compaction.contains("PROFILE_TWO"));
}

#[tokio::test]
async fn in_memory_agents_reject_profile_operations() {
    let server = MockServer::start().await;
    let mut agent = agent(&server);

    assert!(matches!(
        agent.profile(),
        Err(AgentError::ProfileRequiresStore)
    ));
    assert!(matches!(
        agent.replace_profile("Be concise."),
        Err(AgentError::ProfileRequiresStore)
    ));
    assert!(matches!(
        agent.clear_profile(),
        Err(AgentError::ProfileRequiresStore)
    ));
}

#[tokio::test]
async fn profile_read_failure_preserves_input_and_prevents_sticky_facts_http() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(
        &sticky_config(&server, 3),
        DialogStore::open(&path).unwrap(),
    )
    .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "ALTER TABLE user_profiles RENAME TO unavailable_profiles",
            [],
        )
        .unwrap();

    assert!(matches!(
        agent.run_with_prompt("Pending").await,
        Err(AgentError::Store(_))
    ));
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(agent.history().messages().len(), 1);
    assert_eq!(agent.history().messages()[0].content(), "Pending");

    connection
        .execute(
            "ALTER TABLE unavailable_profiles RENAME TO user_profiles",
            [],
        )
        .unwrap();
    mount_sequence(&server, [sse("{}", 3, 1, 4), sse("Recovered", 3, 1, 4)]).await;
    agent.run_with_prompt("Retry").await.unwrap();
    assert_eq!(agent.history().messages().len(), 3);
}

#[tokio::test]
async fn restored_and_new_dialogs_observe_only_their_addressed_memory() {
    let server = MockServer::start().await;
    mount(&server, response("Answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let scope = RequestScope::new("alice", "bot").unwrap();
    let mut first = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&path).unwrap(),
        scope.clone(),
    )
    .unwrap();
    first
        .remember(DurableMemoryScope::User, "language", "Russian")
        .unwrap();
    first
        .remember(DurableMemoryScope::Task, "stack", "Rust")
        .unwrap();
    first.run_with_prompt("First dialog").await.unwrap();
    let id = first.dialog_id().unwrap();
    assert_eq!(first.scope(), &scope.with_dialog_id(Some(id)));
    drop(first);

    let restored =
        Agent::from_dialog(&config(&server), DialogStore::open(&path).unwrap(), id).unwrap();
    assert_eq!(restored.scope(), &scope.with_dialog_id(Some(id)));
    assert_eq!(
        restored.memory_snapshot().unwrap().task_entries()["stack"],
        "Rust"
    );

    let other = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&path).unwrap(),
        RequestScope::new("alice", "other").unwrap(),
    )
    .unwrap();
    assert_eq!(
        other.memory_snapshot().unwrap().user_entries()["language"],
        "Russian"
    );
    assert!(other.memory_snapshot().unwrap().task_entries().is_empty());
    let other_user = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&path).unwrap(),
        RequestScope::new("bob", "bot").unwrap(),
    )
    .unwrap();
    assert!(
        other_user
            .memory_snapshot()
            .unwrap()
            .user_entries()
            .is_empty()
    );
    assert!(
        other_user
            .memory_snapshot()
            .unwrap()
            .task_entries()
            .is_empty()
    );
}

#[tokio::test]
async fn compaction_excludes_durable_memory() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("answer one", 2, 1, 3),
            sse("answer two", 4, 2, 6),
            sse("summary", 7, 2, 9),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let mut agent = Agent::with_store_for_scope(
        &compression_config(&server, 3, 2),
        DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap(),
        RequestScope::new("alice", "bot").unwrap(),
    )
    .unwrap();
    agent
        .remember(DurableMemoryScope::User, "private", "user secret")
        .unwrap();
    agent
        .remember(DurableMemoryScope::Task, "decision", "task decision")
        .unwrap();
    agent.run_with_prompt("First").await.unwrap();
    agent.run_with_prompt("Second").await.unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    let compaction: Value = requests[2].body_json().unwrap();
    let serialized = serde_json::to_string(&compaction["messages"]).unwrap();
    assert!(!serialized.contains("user secret"));
    assert!(!serialized.contains("task decision"));
}

#[tokio::test]
async fn durable_memory_metadata_appears_only_in_ordinary_requests() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("answer one", 2, 1, 3),
            sse("answer two", 4, 2, 6),
            sse("summary", 7, 2, 9),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("context.jsonl");
    let mut file = NamedTempFile::new().unwrap();
    write!(file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"summary\"\ncompact_after_prompt_tokens = 3\nkeep_last_messages = 2\n[debug]\nlog_path = \"{}\"\nlog_payloads = false\n",
        server.uri(), log_path.display(),
    ).unwrap();
    let mut agent = Agent::with_store(
        &Config::load(file.path(), None).unwrap(),
        DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap(),
    )
    .unwrap();
    agent
        .remember(DurableMemoryScope::User, "private", "user secret")
        .unwrap();
    agent
        .remember(DurableMemoryScope::Task, "decision", "task decision")
        .unwrap();
    agent.run_with_prompt("First").await.unwrap();
    agent.run_with_prompt("Second").await.unwrap();
    let log = std::fs::read_to_string(log_path).unwrap();
    let events: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let requests: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "request_prepared")
        .collect();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[0]["system_blocks"],
        json!([
            {"name":"base", "scope":"application", "compaction":"exclude"},
            {"name":"user_memory", "scope":"user", "compaction":"exclude"},
            {"name":"task_memory", "scope":"task", "compaction":"exclude"},
        ])
    );
    assert_eq!(requests[2]["kind"], "compaction");
    assert_eq!(
        requests[2]["system_blocks"],
        json!([
            {"name":"summary_compactor", "scope":"application", "compaction":"exclude"},
        ])
    );
    assert!(!log.contains("user secret"));
    assert!(!log.contains("task decision"));
}

#[tokio::test]
async fn sticky_facts_follow_durable_memory_in_the_ordinary_request() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [sse(r#"{"goal":"ship"}"#, 3, 1, 4), sse("Answer", 5, 1, 6)],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let mut agent = Agent::with_store(
        &sticky_config(&server, 3),
        DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap(),
    )
    .unwrap();
    agent
        .remember(DurableMemoryScope::User, "language", "Russian")
        .unwrap();
    agent
        .remember(DurableMemoryScope::Task, "stack", "Rust")
        .unwrap();
    agent.run_with_prompt("Ship it").await.unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let body: Value = requests[1].body_json().unwrap();
    assert!(
        body["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains("Russian")
    );
    assert!(
        body["messages"][2]["content"]
            .as_str()
            .unwrap()
            .contains("Rust")
    );
    assert!(
        body["messages"][3]["content"]
            .as_str()
            .unwrap()
            .contains(r#"{"goal":"ship"}"#)
    );
    assert_eq!(body["messages"][4]["content"], "Ship it");
}

#[tokio::test]
async fn memory_is_reloaded_and_forgetting_updates_the_next_request() {
    let server = MockServer::start().await;
    mount(&server, response("Answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(&config(&server), DialogStore::open(&path).unwrap()).unwrap();
    let mut writer =
        Agent::with_store(&config(&server), DialogStore::open(&path).unwrap()).unwrap();
    writer
        .remember(DurableMemoryScope::Task, "stack", "Rust")
        .unwrap();
    agent.run_with_prompt("First").await.unwrap();
    writer
        .remember(DurableMemoryScope::Task, "stack", "Go")
        .unwrap();
    agent.run_with_prompt("Second").await.unwrap();
    assert!(agent.forget(DurableMemoryScope::Task, "stack").unwrap());
    assert!(!agent.forget(DurableMemoryScope::Task, "stack").unwrap());
    agent.run_with_prompt("Third").await.unwrap();
    let requests = server.received_requests().await.unwrap();
    let first: Value = requests[0].body_json().unwrap();
    let second: Value = requests[1].body_json().unwrap();
    let third: Value = requests[2].body_json().unwrap();
    assert!(
        first["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains("Rust")
    );
    assert!(
        second["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains("Go")
    );
    assert_eq!(third["messages"][1]["role"], "user");
}

#[tokio::test]
async fn in_memory_agents_reject_durable_memory_operations() {
    let server = MockServer::start().await;
    let mut agent = agent(&server);
    assert!(matches!(
        agent.remember(DurableMemoryScope::User, "key", "value"),
        Err(AgentError::MemoryRequiresStore)
    ));
    assert!(matches!(
        agent.forget(DurableMemoryScope::Task, "key"),
        Err(AgentError::MemoryRequiresStore)
    ));
    assert!(matches!(
        agent.memory_snapshot(),
        Err(AgentError::MemoryRequiresStore)
    ));
}

#[tokio::test]
async fn clear_and_branch_switch_preserve_the_persisted_memory_scope() {
    let server = MockServer::start().await;
    mount(&server, response("Answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let scope = RequestScope::new("alice", "bot").unwrap();
    let mut agent = Agent::with_store_for_scope(
        &branching_config(&server),
        DialogStore::open(&path).unwrap(),
        scope.clone(),
    )
    .unwrap();
    agent
        .remember(DurableMemoryScope::Task, "stack", "Rust")
        .unwrap();
    agent.run_with_prompt("First").await.unwrap();
    let fork = agent.branch_dialog().unwrap();
    agent.switch_branch(fork.new_dialog_id).unwrap();
    assert_eq!(
        agent.scope(),
        &scope.with_dialog_id(Some(fork.new_dialog_id))
    );
    assert_eq!(
        agent.memory_snapshot().unwrap().task_entries()["stack"],
        "Rust"
    );
    agent.clear_history();
    assert_eq!(agent.scope(), &scope);
    assert_eq!(agent.dialog_id(), None);
    agent.run_with_prompt("Fresh dialog").await.unwrap();
    let saved = DialogStore::open(&path)
        .unwrap()
        .load(agent.dialog_id().unwrap())
        .unwrap();
    assert_eq!(saved.scope.user_id(), "alice");
    assert_eq!(saved.scope.task_id(), "bot");
    assert_eq!(
        agent.memory_snapshot().unwrap().task_entries()["stack"],
        "Rust"
    );
}

#[tokio::test]
async fn memory_read_failure_preserves_input_and_prevents_sticky_facts_http() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(
        &sticky_config(&server, 3),
        DialogStore::open(&path).unwrap(),
    )
    .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "ALTER TABLE memory_entries RENAME TO unavailable_memory",
            [],
        )
        .unwrap();
    assert!(matches!(
        agent.run_with_prompt("Pending").await,
        Err(AgentError::Store(_))
    ));
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(agent.history().messages().len(), 1);
    assert_eq!(agent.history().messages()[0].content(), "Pending");
    connection
        .execute(
            "ALTER TABLE unavailable_memory RENAME TO memory_entries",
            [],
        )
        .unwrap();
    let saved = DialogStore::open(&path)
        .unwrap()
        .load(agent.dialog_id().unwrap())
        .unwrap();
    assert_eq!(saved.messages, agent.history().messages());
    mount_sequence(&server, [sse("{}", 3, 1, 4), sse("Recovered", 3, 1, 4)]).await;
    agent.run_with_prompt("Retry").await.unwrap();
    assert_eq!(agent.history().messages().len(), 3);
}

#[tokio::test]
async fn persists_input_before_answer_and_restores_original_system_and_messages() {
    let server = MockServer::start().await;
    mount(&server, response("Hello", true)).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let store = DialogStore::open(&path).unwrap();
    let mut agent = Agent::with_store(&config(&server), store).unwrap();
    agent
        .run_streaming("First", |_| {
            let reader = DialogStore::open(&path).unwrap();
            let saved = reader.load(reader.latest_id().unwrap().unwrap()).unwrap();
            assert_eq!(saved.messages.len(), 1);
            assert_eq!(saved.messages[0].content(), "First");
            Ok(())
        })
        .await
        .unwrap();
    let id = agent.dialog_id().unwrap();
    drop(agent);
    let mut changed = NamedTempFile::new().unwrap();
    write!(
        changed,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Changed system\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"summary\"\n",
        server.uri()
    )
    .unwrap();
    let changed = Config::load(changed.path(), None).unwrap();
    let mut restored = Agent::from_dialog(&changed, DialogStore::open(&path).unwrap(), id).unwrap();
    restored.run_with_prompt("Continue").await.unwrap();
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[1].body_json().unwrap();
    assert_eq!(
        body["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "First"},
            {"role": "assistant", "content": "Hello"},
            {"role": "user", "content": "Continue"}
        ])
    );
    assert_eq!(
        DialogStore::open(&path)
            .unwrap()
            .load(id)
            .unwrap()
            .messages
            .len(),
        4
    );
}

#[tokio::test]
async fn durable_failed_input_survives_restart_and_clear_keeps_old_dialog() {
    let server = MockServer::start().await;
    mount(&server, response("Partial", false)).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(&config(&server), DialogStore::open(&path).unwrap()).unwrap();
    assert!(agent.run_with_prompt("Keep this question").await.is_err());
    let id = agent.dialog_id().unwrap();
    drop(agent);
    let mut restored =
        Agent::from_dialog(&config(&server), DialogStore::open(&path).unwrap(), id).unwrap();
    assert_eq!(restored.history().messages().len(), 1);
    assert_eq!(restored.history().turn_count(), 0);
    server.reset().await;
    mount(&server, response("Answer", true)).await;
    restored.run_with_prompt("Please answer it").await.unwrap();
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(
        body["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Keep this question"},
            {"role": "user", "content": "Please answer it"}
        ])
    );
    restored.clear_history();
    assert!(restored.dialog_id().is_none());
    restored.run_with_prompt("New dialog").await.unwrap();
    assert_ne!(restored.dialog_id(), Some(id));
    let store = DialogStore::open(&path).unwrap();
    assert_eq!(store.load(id).unwrap().messages.len(), 3);
    assert_eq!(store.list().unwrap().len(), 2);
}

#[tokio::test]
async fn database_failure_prevents_api_call() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let store = DialogStore::open(&path).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_message BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT, 'test write failure'); END;").unwrap();
    let mut agent = Agent::with_store(&config(&server), store).unwrap();
    assert!(matches!(
        agent.run_with_prompt("Question").await,
        Err(AgentError::Store(_))
    ));
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(DialogStore::open(&path).unwrap().list().unwrap().is_empty());
    assert!(agent.history().messages().is_empty());
}

#[tokio::test]
async fn answer_storage_failure_is_reported_without_claiming_it_was_saved() {
    let server = MockServer::start().await;
    mount(&server, response("Hello", true)).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let store = DialogStore::open(&path).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_answer BEFORE INSERT ON messages WHEN NEW.role = 'assistant' BEGIN SELECT RAISE(ABORT, 'test write failure'); END;").unwrap();
    let mut agent = Agent::with_store(&config(&server), store).unwrap();
    let mut text = String::new();
    let result = agent
        .run_streaming("Question", |event| {
            if let AgentEvent::Text(fragment) = event {
                text.push_str(fragment);
            }
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(AgentError::Store(_))));
    assert_eq!(text, "Hello");
    assert_eq!(agent.history().messages().len(), 1);
    let saved = DialogStore::open(&path)
        .unwrap()
        .load(agent.dialog_id().unwrap())
        .unwrap();
    assert_eq!(saved.messages.len(), 1);
    assert_eq!(saved.messages[0].content(), "Question");
}

fn response(text: &str, done: bool) -> ResponseTemplate {
    let chunk = json!({"choices": [{"delta": {"content": text}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3}});
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!(
            "data: {chunk}\n\n{}",
            if done { "data: [DONE]\n\n" } else { "" }
        ))
}

async fn mount(server: &MockServer, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(response)
        .mount(server)
        .await;
}

#[tokio::test]
async fn remembers_turns_and_clear_preserves_system_prompt() {
    let server = MockServer::start().await;
    mount(&server, response("Hello", true)).await;
    let mut agent = agent(&server).with_prompt("Start");
    assert_eq!(agent.run().await.unwrap(), "Hello");
    agent.run_with_prompt("Continue").await.unwrap();
    assert_eq!(agent.history().turn_count(), 2);
    agent.clear_history();
    agent.run().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    let bodies: Vec<Value> = requests.iter().map(|r| r.body_json().unwrap()).collect();
    assert_eq!(
        bodies[1]["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Start"},
            {"role": "assistant", "content": "Hello"},
            {"role": "user", "content": "Continue"}
        ])
    );
    assert_eq!(bodies[2]["messages"], bodies[0]["messages"]);
}

#[tokio::test]
async fn agents_do_not_share_history_and_forward_stream_events() {
    let server = MockServer::start().await;
    mount(&server, response("Hello", true)).await;
    let mut first = agent(&server);
    let mut second = agent(&server);
    let mut text = String::new();
    let mut usage = None;
    first
        .run_streaming("First", |event| {
            match event {
                AgentEvent::Text(fragment) => text.push_str(fragment),
                AgentEvent::Usage(value) => usage = Some(value),
                AgentEvent::CompactionStarted { .. }
                | AgentEvent::CompactionCompleted { .. }
                | AgentEvent::CompactionFailed { .. }
                | AgentEvent::FactsUpdateStarted { .. }
                | AgentEvent::FactsUpdateCompleted { .. }
                | AgentEvent::FactsUpdateFailed { .. }
                | AgentEvent::DebugLogFailed { .. }
                | AgentEvent::Workflow(_)
                | AgentEvent::ToolStarted { .. }
                | AgentEvent::ToolFinished { .. } => {}
            }
            Ok(())
        })
        .await
        .unwrap();
    second.run_with_prompt("Second").await.unwrap();
    assert_eq!(text, "Hello");
    assert_eq!(usage.unwrap().total_tokens, 3);
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[1].body_json().unwrap();
    assert_eq!(
        body["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Second"}
        ])
    );
}

#[tokio::test]
async fn failed_or_empty_answers_do_not_pollute_the_next_request() {
    let server = MockServer::start().await;
    let mut agent = agent(&server);
    mount(&server, response("Hello", true)).await;
    agent.run_with_prompt("Good").await.unwrap();
    for failed in [
        ResponseTemplate::new(500),
        response("Partial", false),
        response("  ", true),
    ] {
        server.reset().await;
        mount(&server, failed).await;
        assert!(agent.run_with_prompt("Bad").await.is_err());
        assert_eq!(agent.history().turn_count(), 1);
    }
    server.reset().await;
    mount(&server, response("Hello", true)).await;
    agent.run_with_prompt("Retry").await.unwrap();
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(
        body["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Good"},
            {"role": "assistant", "content": "Hello"},
            {"role": "user", "content": "Retry"}
        ])
    );
}

#[tokio::test]
async fn missing_or_blank_prompt_never_calls_api() {
    let server = MockServer::start().await;
    let mut agent = agent(&server);
    assert!(matches!(agent.run().await, Err(AgentError::MissingPrompt)));
    assert!(matches!(
        agent.run_with_prompt("  ").await,
        Err(AgentError::EmptyPrompt)
    ));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn output_failure_does_not_commit_a_turn() {
    let server = MockServer::start().await;
    mount(&server, response("Hello", true)).await;
    let mut agent = agent(&server);
    let result = agent
        .run_streaming("Hello", |_| Err(std::io::Error::other("closed")))
        .await;
    assert!(matches!(
        result,
        Err(AgentError::Client(ClientError::Output(_)))
    ));
    assert_eq!(agent.history().turn_count(), 0);
}

#[tokio::test]
async fn usage_is_replaced_per_call_persisted_and_never_sent_as_context() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let body = "data: {\"choices\":[{\"delta\":{\"content\":\"Answer\"}}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20,\"total_tokens\":120}}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":25,\"total_tokens\":125,\"completion_tokens_details\":{\"reasoning_tokens\":5}}}\n\ndata: [DONE]\n\n";
    mount(
        &server,
        ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string(body),
    )
    .await;
    let mut agent = Agent::with_store(&config(&server), DialogStore::open(&path).unwrap()).unwrap();
    agent.run_with_prompt("Question").await.unwrap();
    let usage = agent.last_usage().unwrap();
    assert_eq!(
        (
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens
        ),
        (100, 25, 125)
    );
    assert_eq!(
        usage.completion_tokens_details.unwrap().reasoning_tokens,
        Some(5)
    );
    let id = agent.dialog_id().unwrap();
    drop(agent);
    let mut agent =
        Agent::from_dialog(&config(&server), DialogStore::open(&path).unwrap(), id).unwrap();
    assert_eq!(agent.last_usage(), Some(usage));
    assert_eq!(agent.history().messages()[1].usage(), Some(usage));
    server.reset().await;
    mount(
        &server,
        ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string(
                "data: {\"choices\":[{\"delta\":{\"content\":\"No usage\"}}]}\n\ndata: [DONE]\n\n",
            ),
    )
    .await;
    agent.run_with_prompt("Next").await.unwrap();
    assert_eq!(agent.last_usage(), None);
    let requests = server.received_requests().await.unwrap();
    let body: Value = requests[0].body_json().unwrap();
    assert_eq!(
        body["messages"],
        json!([
            {"role":"system","content":"Be concise."},
            {"role":"user","content":"Question"},
            {"role":"assistant","content":"Answer"},
            {"role":"user","content":"Next"}
        ])
    );
    drop(agent);
    let mut agent =
        Agent::from_dialog(&config(&server), DialogStore::open(&path).unwrap(), id).unwrap();
    assert_eq!(agent.last_usage(), None);
    agent.clear_history();
    assert_eq!(agent.last_usage(), None);
}

#[tokio::test]
async fn failed_request_usage_is_not_saved_or_reused_by_next_attempt() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(&config(&server), DialogStore::open(&path).unwrap()).unwrap();
    mount(&server, ResponseTemplate::new(200).insert_header("content-type", "text/event-stream")
        .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"Partial\"},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\ndata: [DONE]\n\n")).await;
    assert!(matches!(
        agent.run_with_prompt("Question").await,
        Err(AgentError::Client(ClientError::Truncated))
    ));
    assert_eq!(agent.last_usage().unwrap().total_tokens, 15);
    let restored = Agent::from_dialog(
        &config(&server),
        DialogStore::open(&path).unwrap(),
        agent.dialog_id().unwrap(),
    )
    .unwrap();
    assert_eq!(restored.last_usage(), None);
    assert_eq!(restored.history().messages().len(), 1);
    server.reset().await;
    mount(&server, ResponseTemplate::new(500)).await;
    assert!(agent.run_with_prompt("Retry").await.is_err());
    assert_eq!(agent.last_usage(), None);
    server.reset().await;
    mount(&server, response("Success", true)).await;
    agent.run_with_prompt("Retry again").await.unwrap();
    assert_eq!(agent.last_usage().unwrap().total_tokens, 3);
    agent.clear_history();
    assert_eq!(agent.last_usage(), None);
}

#[tokio::test]
async fn crossing_request_is_saved_then_compacted_and_next_request_uses_summary_tail() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("answer one", 2, 1, 3),
            sse("answer two", 4, 2, 6),
            sse("summary one", 7, 2, 9),
            sse("answer three", 2, 2, 4),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialog.sqlite3");
    let mut agent = Agent::with_store(
        &compression_config(&server, 3, 2),
        DialogStore::open(&database).unwrap(),
    )
    .unwrap();

    agent.run_with_prompt("u1").await.unwrap();
    let mut events = Vec::new();
    agent
        .run_streaming("u2", |event| {
            events.push(seen(event));
            Ok(())
        })
        .await
        .unwrap();
    agent.run_with_prompt("u3").await.unwrap();

    assert!(events.contains(&Seen::CompactionStarted));
    assert!(events.contains(&Seen::CompactionCompleted));
    assert_eq!(agent.history().messages().len(), 6);
    let stats = agent.context_stats().unwrap();
    assert_eq!(stats.covered_message_count, 2);
    assert_eq!(stats.raw_message_count, 4);
    assert_eq!(stats.ordinary_usage.total_tokens(), 13);
    assert_eq!(stats.compaction_usage.total_tokens(), 9);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4);
    let summary: Value = requests[2].body_json().unwrap();
    assert_eq!(summary["messages"].as_array().unwrap().len(), 2);
    assert!(
        summary["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains("user: u1\nassistant: answer one")
    );
    let follow_up: Value = requests[3].body_json().unwrap();
    assert_eq!(
        follow_up["messages"],
        json!([
            {"role":"system","content":"Be concise."},
            {"role":"system","content":"Summary of earlier conversation:\nsummary one"},
            {"role":"user","content":"u2"},
            {"role":"assistant","content":"answer two"},
            {"role":"user","content":"u3"}
        ])
    );
}

#[tokio::test]
async fn repeated_compaction_sends_previous_summary_with_only_new_prefix() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("a1", 2, 1, 3),
            sse("a2", 4, 1, 5),
            sse("summary one", 5, 2, 7),
            sse("a3", 4, 1, 5),
            sse("summary two", 6, 2, 8),
        ],
    )
    .await;
    let mut agent = Agent::new(&compression_config(&server, 3, 2)).unwrap();

    agent.run_with_prompt("u1").await.unwrap();
    agent.run_with_prompt("u2").await.unwrap();
    agent.run_with_prompt("u3").await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 5);
    let second_summary: Value = requests[4].body_json().unwrap();
    let input = second_summary["messages"][1]["content"].as_str().unwrap();
    assert!(
        input.contains("Context block summary:\nSummary of earlier conversation:\nsummary one")
    );
    assert!(input.contains("New messages:\nuser: u2\nassistant: a2"));
    assert!(!input.contains("user: u1"));
    assert!(!input.contains("user: u3"));
    assert_eq!(agent.context_stats().unwrap().covered_message_count, 4);
    assert_eq!(
        agent.context_stats().unwrap().compaction_usage.call_count(),
        2
    );
}

#[tokio::test]
async fn resumed_retention_increase_keeps_the_previous_summary_in_compaction() {
    // Catch skipping the summarized prefix when the pre-turn request omitted its summary.
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("a1", 2, 1, 3),
            sse("a2", 4, 1, 5),
            sse("summary one", 5, 2, 7),
            sse("a3", 4, 1, 5),
            sse("summary two", 6, 2, 8),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(
        &compression_config(&server, 3, 2),
        DialogStore::open(&database).unwrap(),
    )
    .unwrap();
    agent
        .remember(DurableMemoryScope::User, "private", "user secret")
        .unwrap();
    agent
        .remember(DurableMemoryScope::Task, "decision", "task decision")
        .unwrap();
    agent.run_with_prompt("u1").await.unwrap();
    agent.run_with_prompt("u2").await.unwrap();
    let id = agent.dialog_id().unwrap();
    assert_eq!(agent.history().messages().len(), 4);
    assert_eq!(agent.context_stats().unwrap().covered_message_count, 2);
    drop(agent);

    let log_path = directory.path().join("resumed.jsonl");
    let resumed_config = Config::from_toml(
        &format!(
            "api_key = \"test-key\"\nbase_url = {:?}\n[workflow]\nenabled = false\n[context]\nstrategy = \"summary\"\ncompact_after_prompt_tokens = 3\nkeep_last_messages = 3\n[debug]\nlog_path = {:?}\n",
            server.uri(), log_path.to_str().unwrap(),
        ),
        None,
    ).unwrap();
    let mut resumed =
        Agent::from_dialog(&resumed_config, DialogStore::open(&database).unwrap(), id).unwrap();
    resumed.run_with_prompt("u3").await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 5);
    let ordinary: Value = requests[3].body_json().unwrap();
    assert_eq!(
        &ordinary["messages"].as_array().unwrap()[3..],
        &json!([
            {"role":"user","content":"u1"},
            {"role":"assistant","content":"a1"},
            {"role":"user","content":"u2"},
            {"role":"assistant","content":"a2"},
            {"role":"user","content":"u3"},
        ])
        .as_array()
        .unwrap()[..],
    );
    let compaction: Value = requests[4].body_json().unwrap();
    assert_eq!(
        compaction["messages"][1]["content"],
        "Context block summary:\nSummary of earlier conversation:\nsummary one\n\nNew messages:\nuser: u2\n",
    );
    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let compaction_log = events
        .iter()
        .find(|event| event["kind"] == "compaction")
        .unwrap();
    assert_eq!(
        compaction_log["system_block_names"],
        json!(["summary_compactor", "summary"])
    );
    assert_eq!(compaction_log["summary_boundary"], 2);
    let stored = DialogStore::open(&database).unwrap().load(id).unwrap();
    let summary = stored.context.summary().unwrap();
    assert_eq!(summary.content(), "summary two");
    assert_eq!(summary.covered_message_count(), 3);
    assert_eq!(stored.messages.len(), 6);
}

#[tokio::test]
async fn failed_compaction_keeps_full_history_and_does_not_fail_user_answer() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("answer", 4, 1, 5),
            ResponseTemplate::new(500),
            sse("next answer", 2, 1, 3),
        ],
    )
    .await;
    let mut agent = Agent::new(&compression_config(&server, 3, 1)).unwrap();
    let mut events = Vec::new();

    let answer = agent
        .run_streaming("first", |event| {
            events.push(seen(event));
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(answer, "answer");
    assert_eq!(agent.context_stats().unwrap().covered_message_count, 0);
    assert_eq!(agent.history().messages().len(), 2);
    assert!(events.contains(&Seen::CompactionFailed));
    agent.run_with_prompt("second").await.unwrap();
    let requests = server.received_requests().await.unwrap();
    let retry: Value = requests[2].body_json().unwrap();
    assert_eq!(
        retry["messages"],
        json!([
            {"role":"system","content":"Be concise."},
            {"role":"user","content":"first"},
            {"role":"assistant","content":"answer"},
            {"role":"user","content":"second"}
        ])
    );
}

#[tokio::test]
async fn sticky_facts_are_updated_persisted_and_sent_before_the_answer() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse(r#"{"goal":"prepare specification"}"#, 5, 2, 7),
            sse("Understood", 8, 2, 10),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(
        &sticky_config(&server, 3),
        DialogStore::open(&path).unwrap(),
    )
    .unwrap();

    agent
        .run_with_prompt("We need a specification")
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let facts_body: Value = requests[0].body_json().unwrap();
    assert_eq!(facts_body["temperature"], 0.0);
    assert_eq!(facts_body["max_tokens"], 64);
    assert!(
        facts_body["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains("We need a specification")
    );
    let chat_body: Value = requests[1].body_json().unwrap();
    assert_eq!(
        chat_body["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "system", "content": "Facts (JSON key-value memory):\n{\"goal\":\"prepare specification\"}"},
            {"role": "user", "content": "We need a specification"}
        ])
    );
    let stored = DialogStore::open(&path)
        .unwrap()
        .load(agent.dialog_id().unwrap())
        .unwrap();
    assert_eq!(stored.facts.facts()["goal"], "prepare specification");
    assert_eq!(stored.facts.covered_message_count(), 1);
    assert_eq!(stored.facts.update_usage().total_tokens(), 7);
    let stats = agent.context_stats().unwrap();
    assert_eq!(
        stats.strategy,
        deepseek_cli::config::ContextStrategy::StickyFacts
    );
    assert_eq!(stats.selected_message_count, 2);
    assert_eq!(stats.facts_count, 1);
    assert_eq!(stats.facts_covered_message_count, 1);
    assert_eq!(stats.facts_usage.total_tokens(), 7);
}

#[tokio::test]
async fn sticky_facts_failure_recovers_all_uncovered_user_messages() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("not json", 3, 1, 4),
            sse("First answer", 4, 2, 6),
            sse(r#"{"deadline":"Monday"}"#, 8, 2, 10),
            sse("Second answer", 9, 2, 11),
        ],
    )
    .await;
    let mut agent = Agent::new(&sticky_config(&server, 3)).unwrap();

    agent.run_with_prompt("deadline Friday").await.unwrap();
    agent
        .run_with_prompt("cancel Friday; deadline Monday")
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4);
    let first_chat: Value = requests[1].body_json().unwrap();
    assert_eq!(first_chat["messages"].as_array().unwrap().len(), 2);
    let catch_up: Value = requests[2].body_json().unwrap();
    let update_input = catch_up["messages"][1]["content"].as_str().unwrap();
    assert!(update_input.contains("deadline Friday"));
    assert!(update_input.contains("deadline Monday"));
    assert!(!update_input.contains("First answer"));
    let second_chat: Value = requests[3].body_json().unwrap();
    assert_eq!(
        second_chat["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "system", "content": "Facts (JSON key-value memory):\n{\"deadline\":\"Monday\"}"},
            {"role": "user", "content": "deadline Friday"},
            {"role": "assistant", "content": "First answer"},
            {"role": "user", "content": "cancel Friday; deadline Monday"}
        ])
    );
}

#[tokio::test]
async fn in_memory_facts_are_not_committed_when_the_answer_fails() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse(r#"{"goal":"ship"}"#, 3, 1, 4),
            ResponseTemplate::new(500),
        ],
    )
    .await;
    let mut agent = Agent::new(&sticky_config(&server, 3)).unwrap();

    assert!(agent.run_with_prompt("ship it").await.is_err());

    assert!(agent.history().messages().is_empty());
    assert!(agent.facts_state().facts().is_empty());
    assert_eq!(agent.facts_state().covered_message_count(), 0);
}

#[tokio::test]
async fn branch_and_switch_continue_two_complete_histories_independently() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("Shared answer", 3, 2, 5),
            sse("Left answer", 7, 2, 9),
            sse("Right answer", 7, 2, 9),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dialogs.sqlite3");
    let mut agent = Agent::with_store(
        &branching_config(&server),
        DialogStore::open(&path).unwrap(),
    )
    .unwrap();

    agent.run_with_prompt("Shared question").await.unwrap();
    let original = agent.dialog_id().unwrap();
    let fork = agent.branch_dialog().unwrap();
    assert_eq!(agent.dialog_id(), Some(original));
    agent.run_with_prompt("Left continuation").await.unwrap();
    agent.switch_branch(fork.new_dialog_id).unwrap();
    assert_eq!(agent.dialog_id(), Some(fork.new_dialog_id));
    agent.run_with_prompt("Right continuation").await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    let left: Value = requests[1].body_json().unwrap();
    assert_eq!(
        left["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Shared question"},
            {"role": "assistant", "content": "Shared answer"},
            {"role": "user", "content": "Left continuation"}
        ])
    );
    let right: Value = requests[2].body_json().unwrap();
    assert_eq!(
        right["messages"],
        json!([
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Shared question"},
            {"role": "assistant", "content": "Shared answer"},
            {"role": "user", "content": "Right continuation"}
        ])
    );
    let store = DialogStore::open(&path).unwrap();
    assert_eq!(store.load(original).unwrap().messages.len(), 4);
    assert_eq!(store.load(fork.new_dialog_id).unwrap().messages.len(), 4);
}

#[tokio::test]
async fn branch_operations_reject_wrong_strategy_or_missing_persistent_dialog() {
    let server = MockServer::start().await;
    let mut summary = Agent::new(&config(&server)).unwrap();
    assert!(matches!(
        summary.branch_dialog(),
        Err(AgentError::BranchingStrategyRequired)
    ));
    assert!(matches!(
        summary.switch_branch(1),
        Err(AgentError::BranchingStrategyRequired)
    ));

    let mut in_memory = Agent::new(&branching_config(&server)).unwrap();
    assert!(matches!(
        in_memory.branch_dialog(),
        Err(AgentError::NoPersistentDialog)
    ));
    assert!(matches!(
        in_memory.switch_branch(1),
        Err(AgentError::NoPersistentDialog)
    ));
}

#[tokio::test]
async fn sticky_facts_writes_separate_debug_events_without_payloads() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [sse(r#"{"goal":"ship"}"#, 3, 1, 4), sse("Answer", 5, 1, 6)],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("context.jsonl");
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"sticky_facts\"\n[debug]\nlog_path = \"{}\"\nlog_payloads = false\n",
        server.uri(),
        log_path.display(),
    )
    .unwrap();
    let mut agent = Agent::new(&Config::load(file.path(), None).unwrap()).unwrap();

    agent.run_with_prompt("private goal").await.unwrap();

    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        events.iter().any(|event| {
            event["event"] == "request_prepared" && event["kind"] == "facts_update"
        })
    );
    assert!(
        events
            .iter()
            .any(|event| event["event"] == "facts_update_started")
    );
    assert!(
        events
            .iter()
            .any(|event| event["event"] == "facts_update_completed")
    );
    let text = serde_json::to_string(&events).unwrap();
    assert!(!text.contains("private goal"));
    assert!(!text.contains("ship"));
    assert!(!text.contains("test-key"));
}

#[tokio::test]
async fn branch_creation_and_switch_are_recorded_in_debug_log() {
    let server = MockServer::start().await;
    mount(&server, response("Shared answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let log_path = directory.path().join("context.jsonl");
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\n[workflow]\nenabled = false\n[context]\nstrategy = \"branching\"\n[debug]\nlog_path = \"{}\"\n",
        server.uri(),
        log_path.display(),
    )
    .unwrap();
    let mut agent = Agent::with_store(
        &Config::load(file.path(), None).unwrap(),
        DialogStore::open(&database).unwrap(),
    )
    .unwrap();
    agent.run_with_prompt("Shared question").await.unwrap();

    let fork = agent.branch_dialog().unwrap();
    agent.switch_branch(fork.new_dialog_id).unwrap();

    let events: Vec<Value> = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events.iter().any(|event| {
        event["event"] == "branch_created"
            && event["details"]["original_dialog_id"] == fork.original_dialog_id
            && event["details"]["new_dialog_id"] == fork.new_dialog_id
    }));
    assert!(events.iter().any(|event| {
        event["event"] == "branch_switched" && event["details"]["dialog_id"] == fork.new_dialog_id
    }));
}
