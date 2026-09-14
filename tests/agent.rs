use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, Mutex};

use deepseek_cli::agent::{Agent, AgentError, AgentEvent};
use deepseek_cli::client::ClientError;
use deepseek_cli::config::Config;
use deepseek_cli::dialog::DialogStore;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn config(server: &MockServer) -> Config {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n",
        server.uri()
    )
    .unwrap();
    Config::load(file.path(), None).unwrap()
}

fn agent(server: &MockServer) -> Agent {
    Agent::new(&config(server)).unwrap()
}

fn compression_config(server: &MockServer, threshold: u64, keep: usize) -> Config {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n[context]\nenabled = true\ncompact_after_prompt_tokens = {threshold}\nkeep_last_messages = {keep}\nsummary_max_tokens = 64\n",
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
        AgentEvent::Text(_) | AgentEvent::Usage(_) | AgentEvent::DebugLogFailed { .. } => {
            Seen::Other
        }
    }
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
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Changed system\"\n",
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
                | AgentEvent::DebugLogFailed { .. } => {}
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
    let stats = agent.context_stats();
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
    assert!(input.contains("Previous summary:\nsummary one"));
    assert!(input.contains("New messages:\nuser: u2\nassistant: a2"));
    assert!(!input.contains("user: u1"));
    assert!(!input.contains("user: u3"));
    assert_eq!(agent.context_stats().covered_message_count, 4);
    assert_eq!(agent.context_stats().compaction_usage.call_count(), 2);
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
    assert_eq!(agent.context_stats().covered_message_count, 0);
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
