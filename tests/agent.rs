use std::io::Write;

use deepseek_cli::agent::{Agent, AgentError};
use deepseek_cli::client::{ClientError, StreamEvent};
use deepseek_cli::config::Config;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn agent(server: &MockServer) -> Agent {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n",
        server.uri()
    )
    .unwrap();
    Agent::new(&Config::load(file.path(), None).unwrap()).unwrap()
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
