use std::io::Write;

use deepseek_cli::agent::{Agent, AgentError};
use deepseek_cli::client::{ClientError, StreamEvent};
use deepseek_cli::config::Config;
use deepseek_cli::dialog::DialogStore;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
            if let StreamEvent::Text(fragment) = event {
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
                StreamEvent::Text(fragment) => text.push_str(fragment),
                StreamEvent::Usage(value) => usage = Some(value),
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
