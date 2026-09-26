use deepseek_cli::{
    provider::ModelToolCall,
    settings::{McpSettings, ServerSettings},
    tools::mcp::{McpRegistry, McpRegistryError},
    tools::{ToolExecutionError, ToolExecutor},
};
use serde_json::{Value, json};

fn call(arguments: &str) -> ModelToolCall {
    ModelToolCall {
        id: "call-1".into(),
        name: "telegram__read_chat".into(),
        arguments: arguments.into(),
    }
}

#[tokio::test]
async fn configured_unavailable_server_fails_registry_startup() {
    let server = wiremock::MockServer::start().await;
    let config = http_settings("unavailable", &server.uri(), 1, Some("secret-token"));
    let error = McpRegistry::connect(&config).await.err().unwrap();
    assert!(matches!(&error, McpRegistryError::ConnectFailed));
    for formatted in [error.to_string(), format!("{error:?}")] {
        assert!(!formatted.contains("secret-token"));
        assert!(!formatted.contains(&server.uri()));
    }
}

#[tokio::test]
async fn http_adapter_discovers_all_pages_and_reuses_initialized_client() {
    use wiremock::{Mock, Request, ResponseTemplate, matchers::method};

    let server = wiremock::MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(|request: &Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let result = match body["method"].as_str().unwrap() {
                "initialize" => json!({"protocolVersion":"2025-03-26", "capabilities":{"tools":{}}, "serverInfo":{"name":"fixture", "version":"1"}}),
                "notifications/initialized" => return ResponseTemplate::new(202),
                "tools/list" if body["params"]["cursor"] == "next" => json!({"tools":[{"name":"second", "inputSchema":{}}]}),
                "tools/list" => json!({"tools":[{"name":"read_chat", "inputSchema":{}}], "nextCursor":"next"}),
                "tools/call" => json!({"content":[{"type":"text", "text":"adapter result"}]}),
                other => panic!("Unexpected MCP method: {other}"),
            };
            ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0", "id":body["id"], "result":result}))
        })
        .mount(&server)
        .await;
    let config = http_settings("telegram", &server.uri(), 2, Some("private-bearer-token"));
    let registry = McpRegistry::connect(&config).await.unwrap();
    assert_eq!(
        registry
            .definitions()
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["telegram__read_chat", "telegram__second"]
    );
    for _ in 0..2 {
        let result = registry.call(&call(r#"{"chat_id":"7"}"#)).await.unwrap();
        assert_eq!(result.content, "adapter result");
    }
    let requests = server.received_requests().await.unwrap();
    for request in &requests {
        assert_eq!(
            request.headers.get("authorization").unwrap(),
            "Bearer private-bearer-token"
        );
        assert!(!request.url.as_str().contains("private-bearer-token"));
    }
    let bodies: Vec<Value> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .map(|request| serde_json::from_slice(&request.body).unwrap())
        .collect();
    assert_eq!(
        bodies
            .iter()
            .filter(|body| body["method"] == "initialize")
            .count(),
        1
    );
    assert_eq!(
        bodies
            .iter()
            .filter(|body| body["method"] == "tools/list")
            .count(),
        2,
        "discovery occurs once, reading both pages"
    );
    let calls: Vec<_> = bodies
        .iter()
        .filter(|body| body["method"] == "tools/call")
        .collect();
    assert_eq!(calls.len(), 2);
    for call in calls {
        assert_eq!(call["params"]["name"], "read_chat");
        assert_eq!(call["params"]["arguments"], json!({"chat_id":"7"}));
    }
}

#[tokio::test]
async fn expired_http_session_does_not_reinitialize_or_replay_tool_call() {
    use wiremock::{Mock, Request, ResponseTemplate, matchers::method};

    let server = wiremock::MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(405))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(|request: &Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let result = match body["method"].as_str().unwrap() {
                "initialize" => json!({"protocolVersion":"2025-03-26", "capabilities":{"tools":{}}, "serverInfo":{"name":"fixture", "version":"1"}}),
                "notifications/initialized" => return ResponseTemplate::new(202),
                "tools/list" => json!({"tools":[{"name":"write_chat", "inputSchema":{}, "annotations":{"readOnlyHint":false}}]}),
                "tools/call" => return ResponseTemplate::new(404).set_body_string("private session failure details"),
                other => panic!("Unexpected MCP method: {other}"),
            };
            ResponseTemplate::new(200)
                .insert_header("mcp-session-id", "fixture-session")
                .set_body_json(json!({"jsonrpc":"2.0", "id":body["id"], "result":result}))
        })
        .mount(&server)
        .await;
    let config = http_settings("telegram", &server.uri(), 2, None);
    let registry = McpRegistry::connect(&config).await.unwrap();
    let error = registry
        .call(&ModelToolCall {
            id: "write-1".into(),
            name: "telegram__write_chat".into(),
            arguments: r#"{"text":"private message"}"#.into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error, ToolExecutionError::Transport);
    assert!(!error.to_string().contains("private"));
    assert!(!error.to_string().contains(&server.uri()));

    let requests = server.received_requests().await.unwrap();
    let bodies: Vec<Value> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .map(|request| serde_json::from_slice(&request.body).unwrap())
        .collect();
    let initializations = bodies
        .iter()
        .filter(|body| body["method"] == "initialize")
        .count();
    let calls = bodies
        .iter()
        .filter(|body| body["method"] == "tools/call")
        .count();
    assert_eq!(
        (initializations, calls),
        (1, 1),
        "expired sessions must not reinitialize or replay a write"
    );
}

#[tokio::test]
async fn http_307_redirect_does_not_replay_tool_call_to_another_origin() {
    assert_tool_redirect_is_not_followed(307).await;
}

#[tokio::test]
async fn http_308_redirect_does_not_replay_tool_call_to_another_origin() {
    assert_tool_redirect_is_not_followed(308).await;
}

async fn assert_tool_redirect_is_not_followed(status: u16) {
    use wiremock::{Mock, Request, ResponseTemplate, matchers::method};

    let source = wiremock::MockServer::start().await;
    let target = wiremock::MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("private target failure"))
        .mount(&target)
        .await;
    let target_url = format!("{}/capture", target.uri());
    Mock::given(method("POST"))
        .respond_with(move |request: &Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let result = match body["method"].as_str().unwrap() {
                "initialize" => json!({"protocolVersion":"2025-03-26", "capabilities":{"tools":{}}, "serverInfo":{"name":"fixture", "version":"1"}}),
                "notifications/initialized" => return ResponseTemplate::new(202),
                "tools/list" => json!({"tools":[{"name":"write_chat", "inputSchema":{}, "annotations":{"readOnlyHint":false}}]}),
                "tools/call" => return ResponseTemplate::new(status)
                    .insert_header("Location", target_url.as_str())
                    .set_body_string("private redirect details"),
                other => panic!("Unexpected MCP method: {other}"),
            };
            ResponseTemplate::new(200)
                .set_body_json(json!({"jsonrpc":"2.0", "id":body["id"], "result":result}))
        })
        .mount(&source)
        .await;
    let config = http_settings("telegram", &source.uri(), 2, None);
    let registry = McpRegistry::connect(&config).await.unwrap();
    let error = registry
        .call(&ModelToolCall {
            id: "write-redirect".into(),
            name: "telegram__write_chat".into(),
            arguments: r#"{"text":"private message"}"#.into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error, ToolExecutionError::Transport);
    assert!(!error.to_string().contains("private"));
    assert!(!error.to_string().contains(&source.uri()));
    assert!(!error.to_string().contains(&target.uri()));

    let source_requests = source.received_requests().await.unwrap();
    let source_calls = source_requests
        .iter()
        .filter(|request| request.method == "POST")
        .map(|request| serde_json::from_slice::<Value>(&request.body).unwrap())
        .filter(|body| body["method"] == "tools/call")
        .count();
    let target_requests = target.received_requests().await.unwrap();
    assert_eq!(
        (source_calls, target_requests.len()),
        (1, 0),
        "HTTP {status} must not replay a tool call to the redirect target"
    );
}

fn http_settings(name: &str, origin: &str, timeout: u64, token: Option<&str>) -> McpSettings {
    let file = tempfile::NamedTempFile::new().unwrap();
    let token = token
        .map(|token| format!("bearer_token = {token:?}"))
        .unwrap_or_default();
    std::fs::write(
        file.path(),
        format!(
            r#"
[mcp]
connect_timeout_seconds = {timeout}
call_timeout_seconds = {timeout}
[[mcp.servers]]
name = "{name}"
url = "{origin}/mcp"
{token}
"#
        ),
    )
    .unwrap();
    ServerSettings::load(file.path(), Some("fixture-key".into()))
        .unwrap()
        .mcp()
        .clone()
}
