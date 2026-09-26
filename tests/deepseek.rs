use std::io::{self, Write};

use deepseek_cli::provider::{
    AssistantTurn, DeepSeekProvider, ModelToolCall, ModelToolDefinition, Provider, ProviderMessage,
};
use deepseek_cli::settings::ServerSettings;
use serde_json::json;
use tempfile::NamedTempFile;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn provider(server: &MockServer, key: &str) -> DeepSeekProvider {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "[provider]\napi_key = '{key}'\nbase_url = '{}'\nmodel = 'test-model'\nmax_tokens = 128\ntimeout_seconds = 5\n",
        server.uri()
    )
    .unwrap();
    let settings = ServerSettings::load(file.path(), None).unwrap();
    DeepSeekProvider::new(settings.provider()).unwrap()
}

fn sse(deltas: Vec<serde_json::Value>, done: bool) -> String {
    let mut body = deltas
        .into_iter()
        .map(|delta| {
            format!(
                "data: {}\n\n",
                json!({"choices":[{"index":0,"delta":delta}]})
            )
        })
        .collect::<String>();
    body.push_str("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\n");
    body.push_str("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n");
    if done {
        body.push_str("data: [DONE]\n\n");
    }
    body
}

async fn mock_stream(server: &MockServer, body: String) {
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body),
        )
        .mount(server)
        .await;
}

fn tools() -> Vec<ModelToolDefinition> {
    vec![ModelToolDefinition {
        name: "telegram__read_chat".into(),
        description: Some("Read chat".into()),
        parameters: json!({"type":"object","properties":{"chat_id":{"type":"string"}}})
            .as_object()
            .unwrap()
            .clone(),
        read_only: true,
    }]
}

// Catches dropping fragments, leaking reasoning, and a non-object-safe provider interface.
#[tokio::test]
async fn streams_fragmented_text() {
    let server = MockServer::start().await;
    mock_stream(
        &server,
        sse(
            vec![
                json!({"role":"assistant","reasoning_content":"private","content":"Hel"}),
                json!({"content":"lo"}),
            ],
            true,
        ),
    )
    .await;
    let concrete = provider(&server, "test-key");
    let provider: &dyn Provider = &concrete;
    let messages = [
        ProviderMessage::system("Be concise"),
        ProviderMessage::user("Hi"),
    ];
    let mut fragments = Vec::new();
    let turn = provider
        .stream_turn(&messages, &[], &mut |text| {
            fragments.push(text.to_owned());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(fragments, ["Hel", "lo"]);
    assert!(matches!(turn, AssistantTurn::FinalText { content, .. } if content == "Hello"));
}

// Catches assembly by arrival order, premature JSON validation, or lost message fields.
#[tokio::test]
async fn assembles_fragmented_tool_calls_by_index() {
    let server = MockServer::start().await;
    mock_stream(
        &server,
        sse(
            vec![
                json!({"tool_calls":[{"index":9,"id":"call_","type":"function","function":{"name":"telegram__send_","arguments":"{\"text\":"}}]}),
                json!({"tool_calls":[{"index":2,"id":"call_","type":"function","function":{"name":"telegram__","arguments":"{\"chat_id\":\""}},{"index":9,"id":"send","function":{"name":"message","arguments":"\"hi\"}"}}]}),
                json!({"tool_calls":[{"index":2,"id":"read","function":{"name":"read_chat","arguments":"7\"}"}}]}),
            ],
            true,
        ),
    )
    .await;
    let concrete = provider(&server, "test-key");
    let provider: &dyn Provider = &concrete;
    let calls = [ModelToolCall {
        id: "previous".into(),
        name: "telegram__read_chat".into(),
        arguments: "{}".into(),
    }];
    let messages = [
        ProviderMessage::assistant("Checking"),
        ProviderMessage::assistant_tool_calls(None, &calls),
        ProviderMessage::tool_result("previous", "ok"),
    ];
    let turn = provider
        .stream_turn(&messages, &tools(), &mut |_| Ok(()))
        .await
        .unwrap();
    let AssistantTurn::ToolCalls {
        content,
        calls,
        usage,
    } = turn
    else {
        panic!("expected tool calls");
    };
    assert_eq!(content, None);
    assert_eq!(calls[0].id, "call_read");
    assert_eq!(calls[0].name, "telegram__read_chat");
    assert_eq!(calls[0].arguments, r#"{"chat_id":"7"}"#);
    assert_eq!(calls[1].id, "call_send");
    assert_eq!(calls[1].name, "telegram__send_message");
    assert_eq!(calls[1].arguments, r#"{"text":"hi"}"#);
    assert_eq!(usage.unwrap().total_tokens, 5);
    let request = server.received_requests().await.unwrap().remove(0);
    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(
        body["messages"][0],
        json!({"role":"assistant","content":"Checking"})
    );
    assert_eq!(body["messages"][1]["tool_calls"][0]["id"], "previous");
    assert_eq!(
        body["messages"][2],
        json!({"role":"tool","content":"ok","tool_call_id":"previous"})
    );
    assert_eq!(body["tools"][0]["function"]["name"], "telegram__read_chat");
    assert!(body["tools"][0]["function"].get("read_only").is_none());
}

// Catches accepting a duplicate ID, absent fields, malformed arguments, or redeclaration.
#[tokio::test]
async fn rejects_duplicate_or_malformed_call_ids() {
    let cases = vec![
        vec![
            json!({"index":0,"id":"same","function":{"name":"read","arguments":"{}"}}),
            json!({"index":1,"id":"same","function":{"name":"write","arguments":"{}"}}),
        ],
        vec![json!({"index":0,"function":{"name":"read","arguments":"{}"}})],
        vec![json!({"index":0,"id":" ","function":{"name":"read","arguments":"{}"}})],
        vec![json!({"index":0,"id":"id","function":{"name":"read","arguments":"not json"}})],
        vec![
            json!({"index":0,"id":"id","type":"other","function":{"name":"read","arguments":"{}"}}),
        ],
        vec![
            json!({"index":0,"id":"a","type":"function","function":{"name":"read","arguments":"{}"}}),
            json!({"index":0,"type":"function"}),
        ],
    ];
    for calls in cases {
        let server = MockServer::start().await;
        let deltas = calls
            .into_iter()
            .map(|call| json!({"tool_calls":[call]}))
            .collect();
        mock_stream(&server, sse(deltas, true)).await;
        let error = provider(&server, "test-key")
            .stream_turn(&[], &[], &mut |_| Ok(()))
            .await
            .unwrap_err();
        assert_eq!(error.safe_code(), "invalid_tool_call");
        assert!(!format!("{error:?}").contains("not json"));
    }
}

// Catches finalizing partial output before [DONE] or accepting a length finish.
#[tokio::test]
async fn rejects_truncated_and_incomplete_streams() {
    let server = MockServer::start().await;
    mock_stream(&server, sse(vec![json!({"content":"partial"})], false)).await;
    let error = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "incomplete_stream");

    let server = MockServer::start().await;
    mock_stream(
        &server,
        "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n".into(),
    )
    .await;
    let error = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "truncated");
}

// Catches secret leakage at the body limit and unbounded operator diagnostics.
#[tokio::test]
async fn bounds_and_redacts_error_body() {
    let server = MockServer::start().await;
    let key = "secret-api-key";
    let body = format!("{}{}after-key", "x".repeat(4090), key);
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_string(body))
        .mount(&server)
        .await;
    let error = provider(&server, key)
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "http");
    assert_eq!(error.operator_metadata().status, Some(401));
    for public in [error.to_string(), format!("{error:?}")] {
        assert!(!public.contains(key));
        assert!(!public.contains("after-key"));
    }
    let diagnostic = error.raw_diagnostic();
    assert!(diagnostic.contains("[REDACTED]"));
    assert!(!diagnostic.contains(key));
    assert!(!diagnostic.contains("after-key"));
    assert!(diagnostic.len() < 4300);
}

// Catches accidentally sending an empty tools array or local tool metadata.
#[tokio::test]
async fn never_sends_tools_when_catalog_is_empty() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer test-key"))
        .and(body_json(json!({
            "model":"test-model", "messages":[{"role":"user","content":"Hi"}],
            "max_tokens":128, "stream":true,
            "stream_options":{"include_usage":true}
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(vec![json!({"content":"ok"})], true)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let concrete = provider(&server, "test-key");
    let turn = concrete
        .stream_turn(&[ProviderMessage::user("Hi")], &[], &mut |_| Ok(()))
        .await
        .unwrap();
    assert!(matches!(turn, AssistantTurn::FinalText { content, .. } if content == "ok"));
    server.verify().await;
}

// Catches summing snapshots or ignoring usage chunks with no choices.
#[tokio::test]
async fn collects_provider_usage() {
    let server = MockServer::start().await;
    mock_stream(&server, sse(vec![json!({"content":"ok"})], true)).await;
    let turn = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap();
    assert!(
        matches!(turn, AssistantTurn::FinalText { usage: Some(usage), .. } if usage.prompt_tokens == 3 && usage.completion_tokens == 2 && usage.total_tokens == 5)
    );
}

// Catches treating a failed text sink as a successful model turn.
#[tokio::test]
async fn propagates_text_sink_failure() {
    let server = MockServer::start().await;
    mock_stream(&server, sse(vec![json!({"content":"ok"})], true)).await;
    let error = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "secret output"))
        })
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "output");
    assert!(!error.to_string().contains("secret output"));
}
