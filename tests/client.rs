use std::io::{self, Write};

use deepseek_cli::chat::ChatHistory;
use deepseek_cli::client::DeepSeekClient;
use deepseek_cli::config::Config;
use serde_json::json;
use tempfile::NamedTempFile;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config_for(server: &MockServer, api_key: &str) -> Config {
    let mut file = NamedTempFile::new().expect("create temporary config");
    write!(
        file,
        r#"
api_key = "{api_key}"
base_url = "{}"
model = "test-model"
system_prompt = "Be concise."
temperature = 0.5
max_tokens = 128
timeout_seconds = 5
"#,
        server.uri()
    )
    .expect("write temporary config");
    Config::load(file.path(), None).expect("load test config")
}

fn request_messages() -> Vec<deepseek_cli::chat::Message> {
    ChatHistory::new("Be concise.".into()).request_messages("Hello")
}

fn successful_sse() -> String {
    [
        r#"data: {"id":"chat-1","object":"chat.completion.chunk","created":1,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"},"finish_reason":null}]}"#,
        r#"data: {"id":"chat-1","object":"chat.completion.chunk","created":1,"model":"test-model","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}"#,
        "data: [DONE]",
    ]
    .join("\n\n")
        + "\n\n"
}

#[tokio::test]
async fn streams_content_and_sends_expected_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .and(header("authorization", "Bearer test-key"))
        .and(body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": "Be concise."},
                {"role": "user", "content": "Hello"}
            ],
            "temperature": 0.5,
            "max_tokens": 128,
            "stream": true
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(successful_sse()),
        )
        .expect(1)
        .mount(&server)
        .await;
    let config = config_for(&server, "test-key");
    let client = DeepSeekClient::new(&config).expect("create API client");
    let mut fragments = Vec::new();

    let answer = client
        .stream_chat(&request_messages(), |text| {
            fragments.push(text.to_owned());
            Ok(())
        })
        .await
        .expect("stream response");

    assert_eq!(fragments, ["Hello", " world"]);
    assert_eq!(answer, "Hello world");
}

#[tokio::test]
async fn ignores_metadata_and_reasoning_only_chunks() {
    let server = MockServer::start().await;
    let body = [
        r#"data: {"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning_content":"private reasoning"},"finish_reason":null}]}"#,
        r#"data: {"choices":[{"index":0,"delta":{"content":null},"finish_reason":null}]}"#,
        r#"data: {"choices":[{"index":0,"delta":{"content":"Answer"},"finish_reason":"stop"}]}"#,
        "data: [DONE]",
    ]
    .join("\n\n")
        + "\n\n";
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body),
        )
        .mount(&server)
        .await;
    let config = config_for(&server, "test-key");
    let client = DeepSeekClient::new(&config).expect("create API client");
    let mut fragments = Vec::new();

    let answer = client
        .stream_chat(&request_messages(), |text| {
            fragments.push(text.to_owned());
            Ok(())
        })
        .await
        .expect("stream response");

    assert_eq!(fragments, ["Answer"]);
    assert_eq!(answer, "Answer");
}

#[tokio::test]
async fn rejects_invalid_json_event() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string("data: {invalid-json}\n\ndata: [DONE]\n\n"),
        )
        .mount(&server)
        .await;
    let config = config_for(&server, "test-key");
    let client = DeepSeekClient::new(&config).expect("create API client");

    let error = client
        .stream_chat(&request_messages(), |_| Ok(()))
        .await
        .expect_err("invalid event must fail")
        .to_string();

    assert!(error.contains("JSON"), "unexpected error: {error}");
}

#[tokio::test]
async fn rejects_stream_closed_without_done_event() {
    let server = MockServer::start().await;
    let body =
        r#"data: {"choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}\n\n"#;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body),
        )
        .mount(&server)
        .await;
    let config = config_for(&server, "test-key");
    let client = DeepSeekClient::new(&config).expect("create API client");

    let error = client
        .stream_chat(&request_messages(), |_| Ok(()))
        .await
        .expect_err("incomplete stream must fail")
        .to_string();

    assert!(error.contains("[DONE]"), "unexpected error: {error}");
}

#[tokio::test]
async fn bounds_and_redacts_api_error_body() {
    let server = MockServer::start().await;
    let secret = "secret-api-key";
    let response_body = format!("invalid key {secret}:{}THE-END", "x".repeat(5000));
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_string(response_body))
        .mount(&server)
        .await;
    let config = config_for(&server, secret);
    let client = DeepSeekClient::new(&config).expect("create API client");

    let error = client
        .stream_chat(&request_messages(), |_| Ok(()))
        .await
        .expect_err("HTTP failure must be returned")
        .to_string();

    assert!(error.contains("401"), "unexpected error: {error}");
    assert!(error.contains("[REDACTED]"), "unexpected error: {error}");
    assert!(!error.contains(secret));
    assert!(!error.contains("THE-END"));
    assert!(error.len() < 4300, "error body was not bounded");
}

#[tokio::test]
async fn propagates_output_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(successful_sse()),
        )
        .mount(&server)
        .await;
    let config = config_for(&server, "test-key");
    let client = DeepSeekClient::new(&config).expect("create API client");

    let error = client
        .stream_chat(&request_messages(), |_| {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "output closed"))
        })
        .await
        .expect_err("output failure must be returned")
        .to_string();

    assert!(error.contains("output closed"), "unexpected error: {error}");
}
