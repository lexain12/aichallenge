use std::io::{self, Write};
use std::time::Duration;

use deepseek_cli::provider::{
    AssistantTurn, DeepSeekProvider, ModelToolCall, ModelToolDefinition, Provider, ProviderMessage,
};
use deepseek_cli::settings::ServerSettings;
use serde_json::json;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
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

#[tokio::test]
async fn proxy_routes_https_provider_request_through_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination_port = destination.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("provider did not connect to the configured proxy")
            .unwrap();
        let mut stream = BufReader::new(stream);
        let mut request_line = String::new();
        stream.read_line(&mut request_line).await.unwrap();
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        request_line
    });

    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "[provider]\napi_key = 'test-key'\nbase_url = 'https://127.0.0.1:{destination_port}'\nproxy_url = 'http://127.0.0.1:{proxy_port}'\ntimeout_seconds = 2\n"
    )
    .unwrap();
    let settings = ServerSettings::load(file.path(), None).unwrap();
    let provider = DeepSeekProvider::new(settings.provider()).unwrap();
    let error = provider
        .stream_turn(&[ProviderMessage::user("test")], &[], &mut |_| Ok(()))
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "request");
    assert_eq!(
        proxy.await.unwrap(),
        format!("CONNECT 127.0.0.1:{destination_port} HTTP/1.1\r\n")
    );
}

fn sse(deltas: Vec<serde_json::Value>, finish_reason: Option<&str>, done: bool) -> String {
    let mut body = deltas
        .into_iter()
        .map(|delta| {
            format!(
                "data: {}\n\n",
                json!({"choices":[{"index":0,"delta":delta}]})
            )
        })
        .collect::<String>();
    if let Some(reason) = finish_reason {
        body.push_str(&format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":{},"finish_reason":reason}]})
        ));
    }
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
            Some("stop"),
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
            Some("tool_calls"),
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
        mock_stream(&server, sse(deltas, Some("tool_calls"), true)).await;
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
    mock_stream(
        &server,
        sse(vec![json!({"content":"partial"})], Some("stop"), false),
    )
    .await;
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

// Catches HTTP body leakage through any protocol-visible error channel.
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
        assert!(!public.contains("xxxxxxxx"));
    }
    let metadata = format!("{:?}", error.operator_metadata());
    assert!(!metadata.contains(key));
    assert!(!metadata.contains("after-key"));
    assert!(!metadata.contains("xxxxxxxx"));
}

#[tokio::test]
async fn classifies_only_explicit_context_length_http_errors() {
    let context_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "type": "invalid_request_error",
                "code": "context_length_exceeded",
                "message": "maximum context length exceeded"
            }
        })))
        .mount(&context_server)
        .await;
    let context_error = provider(&context_server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap_err();
    assert_eq!(context_error.safe_code(), "context_too_long");
    assert_eq!(context_error.operator_metadata().status, Some(400));

    let invalid_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_tool_schema",
                "message": "tool schema is invalid"
            }
        })))
        .mount(&invalid_server)
        .await;
    let invalid_error = provider(&invalid_server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap_err();
    assert_eq!(invalid_error.safe_code(), "http");
    assert_eq!(invalid_error.operator_metadata().status, Some(400));
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
                .set_body_string(sse(vec![json!({"content":"ok"})], Some("stop"), true)),
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
    mock_stream(
        &server,
        sse(vec![json!({"content":"ok"})], Some("stop"), true),
    )
    .await;
    let turn = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap();
    assert!(
        matches!(turn, AssistantTurn::FinalText { usage: Some(usage), .. } if usage.prompt_tokens == 3 && usage.completion_tokens == 2 && usage.total_tokens == 5)
    );
}

// Catches dispatching a valid JSON scalar or array as function arguments.
#[tokio::test]
async fn rejects_non_object_tool_arguments() {
    for arguments in ["[]", "42", "null", "\"text\""] {
        let server = MockServer::start().await;
        mock_stream(
            &server,
            sse(
                vec![json!({"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read","arguments":arguments}}]})],
                Some("tool_calls"),
                true,
            ),
        )
        .await;
        let error = provider(&server, "test-key")
            .stream_turn(&[], &[], &mut |_| Ok(()))
            .await
            .unwrap_err();
        assert_eq!(error.safe_code(), "invalid_tool_call", "{arguments}");
    }
}

// Catches treating [DONE] alone as proof of a complete assistant turn.
#[tokio::test]
async fn rejects_done_without_terminal_finish_reason() {
    let server = MockServer::start().await;
    mock_stream(&server, sse(vec![json!({"content":"partial"})], None, true)).await;
    let error = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "incomplete_stream");
    assert_eq!(error.usage().unwrap().total_tokens, 5);
}

// Catches accepting safety-filtered, resource-limited, or unknown finishes.
#[tokio::test]
async fn rejects_unsupported_finish_reasons() {
    for reason in ["content_filter", "resource_exhausted", "unknown"] {
        let server = MockServer::start().await;
        mock_stream(
            &server,
            sse(vec![json!({"content":"partial"})], Some(reason), true),
        )
        .await;
        let error = provider(&server, "test-key")
            .stream_turn(&[], &[], &mut |_| Ok(()))
            .await
            .unwrap_err();
        assert_eq!(error.safe_code(), "invalid_stream", "{reason}");
    }
}

// Catches a terminal reason that contradicts the actual response mode.
#[tokio::test]
async fn rejects_finish_reason_content_mode_mismatch() {
    let cases = [
        (
            json!({"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read","arguments":"{}"}}]}),
            "stop",
        ),
        (json!({"content":"answer"}), "tool_calls"),
    ];
    for (delta, reason) in cases {
        let server = MockServer::start().await;
        mock_stream(&server, sse(vec![delta], Some(reason), true)).await;
        let error = provider(&server, "test-key")
            .stream_turn(&[], &[], &mut |_| Ok(()))
            .await
            .unwrap_err();
        assert_eq!(error.safe_code(), "invalid_stream", "{reason}");
    }
}

// Catches inadvertently rejecting legitimate terminal stop and tool_calls.
#[tokio::test]
async fn accepts_valid_stop_and_tool_calls_finish_reasons() {
    let server = MockServer::start().await;
    mock_stream(
        &server,
        sse(vec![json!({"content":"answer"})], Some("stop"), true),
    )
    .await;
    let turn = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap();
    assert!(matches!(turn, AssistantTurn::FinalText { content, .. } if content == "answer"));

    let server = MockServer::start().await;
    mock_stream(
        &server,
        sse(
            vec![json!({"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read","arguments":"{}"}}]})],
            Some("tool_calls"),
            true,
        ),
    )
    .await;
    let turn = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| Ok(()))
        .await
        .unwrap();
    assert!(
        matches!(turn, AssistantTurn::ToolCalls { calls, .. } if calls.len() == 1 && calls[0].id == "call_1")
    );
}

// Catches treating a failed text sink as a successful model turn.
#[tokio::test]
async fn propagates_text_sink_failure() {
    let server = MockServer::start().await;
    mock_stream(
        &server,
        sse(vec![json!({"content":"ok"})], Some("stop"), true),
    )
    .await;
    let error = provider(&server, "test-key")
        .stream_turn(&[], &[], &mut |_| {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "secret output"))
        })
        .await
        .unwrap_err();
    assert_eq!(error.safe_code(), "output");
    assert!(!error.to_string().contains("secret output"));
}
