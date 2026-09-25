use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};

use deepseek_cli::{
    config::{McpConfig, McpServerConfig},
    mcp::{McpClient, McpClientError, McpFuture, McpRegistry, McpRegistryError},
    tool_calling::{ModelToolCall, ToolExecutionError, ToolExecutor},
};
use rmcp::model::{CallToolResult, Tool};
use serde_json::{Map, Value, json};

type RecordedCalls = Arc<Mutex<Vec<(String, Map<String, Value>)>>>;

#[derive(Clone)]
struct FakeMcpClient {
    tools: Vec<Tool>,
    result: CallToolResult,
    calls: RecordedCalls,
    fail_list: bool,
    hang_list: bool,
    fail_call: bool,
    hang_call: bool,
}

impl FakeMcpClient {
    fn new(tools: Value) -> Self {
        Self {
            tools: serde_json::from_value(tools).unwrap(),
            result: serde_json::from_value(json!({"content": [{"type":"text", "text":"ok"}]}))
                .unwrap(),
            calls: Default::default(),
            fail_list: false,
            hang_list: false,
            fail_call: false,
            hang_call: false,
        }
    }

    fn one() -> Self {
        Self::new(
            json!([{"name":"read_chat", "description":"Read a chat", "inputSchema":{"type":"object", "properties":{"chat":{"type":"string"}}}, "annotations":{"readOnlyHint":true}}]),
        )
    }
}

impl McpClient for FakeMcpClient {
    fn list_tools(&self) -> McpFuture<'_, Vec<Tool>> {
        Box::pin(async move {
            if self.hang_list {
                pending::<()>().await;
            }
            if self.fail_list {
                return Err(McpClientError);
            }
            Ok(self.tools.clone())
        })
    }

    fn call_tool<'a>(
        &'a self,
        name: &'a str,
        arguments: Map<String, Value>,
    ) -> McpFuture<'a, CallToolResult> {
        Box::pin(async move {
            self.calls.lock().unwrap().push((name.into(), arguments));
            if self.hang_call {
                pending::<()>().await;
            }
            if self.fail_call {
                return Err(McpClientError);
            }
            Ok(self.result.clone())
        })
    }
}

async fn registry(fake: FakeMcpClient) -> McpRegistry {
    McpRegistry::from_clients(
        vec![("telegram".into(), Box::new(fake))],
        Duration::from_millis(20),
        Duration::from_millis(20),
    )
    .await
    .unwrap()
}

fn call(arguments: &str) -> ModelToolCall {
    ModelToolCall {
        id: "call-1".into(),
        name: "telegram__read_chat".into(),
        arguments: arguments.into(),
    }
}

#[tokio::test]
async fn discovers_namespaced_definitions_and_defaults_missing_read_only_to_false() {
    let fake = FakeMcpClient::new(json!([
        {"name":"read_chat","description":"Read a chat","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}},
        {"name":"write_chat","inputSchema":{"type":"object"}},
        {"name":"other","inputSchema":{},"annotations":{}}
    ]));
    let registry = registry(fake).await;
    let definitions = registry.definitions();
    assert_eq!(definitions.len(), 3);
    assert_eq!(definitions[0].name, "telegram__read_chat");
    assert_eq!(definitions[0].description.as_deref(), Some("Read a chat"));
    assert_eq!(
        definitions[0].parameters,
        json!({"type":"object"}).as_object().unwrap().clone()
    );
    assert!(definitions[0].read_only);
    assert_eq!(registry.is_read_only("telegram__write_chat"), Some(false));
    assert_eq!(registry.is_read_only("telegram__other"), Some(false));
    assert_eq!(registry.is_read_only("unknown"), None);
}

#[tokio::test]
async fn dispatches_namespaced_name_as_original_name() {
    let fake = FakeMcpClient::one();
    let registry = registry(fake.clone()).await;
    registry.call(&call(r#"{"chat":"me"}"#)).await.unwrap();
    registry.call(&call("{}")).await.unwrap();
    assert_eq!(
        *fake.calls.lock().unwrap(),
        vec![
            (
                "read_chat".into(),
                json!({"chat":"me"}).as_object().unwrap().clone()
            ),
            ("read_chat".into(), Map::new()),
        ]
    );
}

#[tokio::test]
async fn identical_original_names_on_different_servers_route_independently() {
    let first = FakeMcpClient::one();
    let second = FakeMcpClient::one();
    let registry = McpRegistry::from_clients(
        vec![
            ("one".into(), Box::new(first.clone())),
            ("two".into(), Box::new(second.clone())),
        ],
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let mut request = call("{}");
    request.name = "two__read_chat".into();
    registry.call(&request).await.unwrap();
    assert!(first.calls.lock().unwrap().is_empty());
    assert_eq!(second.calls.lock().unwrap()[0].0, "read_chat");
}

#[tokio::test]
async fn structured_content_wins_and_preserves_server_error_flag() {
    let mut fake = FakeMcpClient::one();
    fake.result = serde_json::from_value(json!({"structuredContent":{"answer":42}, "content":[{"type":"image", "data":"eA==", "mimeType":"image/png"}], "isError":true})).unwrap();
    let result = registry(fake).await.call(&call("{}")).await.unwrap();
    assert_eq!(result.content, r#"{"answer":42}"#);
    assert!(result.is_error);
    assert_eq!(result.error_code.as_deref(), Some("mcp_tool_error"));
    assert!(!result.delivery_uncertain);
}

#[tokio::test]
async fn text_content_is_joined_in_order() {
    let mut fake = FakeMcpClient::one();
    fake.result = serde_json::from_value(
        json!({"content":[{"type":"text", "text":"first"}, {"type":"text", "text":"second"}]}),
    )
    .unwrap();
    let result = registry(fake).await.call(&call("{}")).await.unwrap();
    assert_eq!(result.content, "first\nsecond");
    assert!(!result.is_error);
    assert_eq!(result.error_code, None);
}

#[tokio::test]
async fn unsupported_and_mixed_content_are_explicit_tool_errors() {
    for content in [
        json!([{"type":"image", "data":"eA==", "mimeType":"image/png"}]),
        json!([{"type":"text", "text":"partial"}, {"type":"image", "data":"eA==", "mimeType":"image/png"}]),
    ] {
        let mut fake = FakeMcpClient::one();
        fake.result = serde_json::from_value(json!({"content":content})).unwrap();
        let result = registry(fake).await.call(&call("{}")).await.unwrap();
        assert!(result.is_error);
        assert_eq!(result.error_code.as_deref(), Some("unsupported_content"));
        assert!(!result.content.is_empty());
        assert_ne!(result.content, "partial");
    }
}

#[tokio::test]
async fn invalid_provider_name_fails_registry_startup() {
    for name in [
        "has.dot".into(),
        "has space".into(),
        "тест".into(),
        "x".repeat(55),
        String::new(),
    ] {
        let fake = FakeMcpClient::new(json!([{"name":name, "inputSchema":{}}]));
        let result = McpRegistry::from_clients(
            vec![("telegram".into(), Box::new(fake))],
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await;
        assert!(matches!(result, Err(McpRegistryError::InvalidToolName(_))));
    }
}

#[tokio::test]
async fn provider_name_at_64_character_boundary_is_accepted() {
    let fake = FakeMcpClient::new(json!([{"name":"x".repeat(54), "inputSchema":{}}]));
    assert_eq!(registry(fake).await.definitions()[0].name.len(), 64);
}

#[tokio::test]
async fn namespaced_collision_fails_registry_startup() {
    let fake = FakeMcpClient::new(
        json!([{"name":"same", "inputSchema":{}}, {"name":"same", "inputSchema":{}}]),
    );
    let result = McpRegistry::from_clients(
        vec![("server".into(), Box::new(fake))],
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .await;
    assert!(matches!(result, Err(McpRegistryError::NameCollision(_))));
}

#[tokio::test]
async fn configured_unavailable_server_fails_registry_startup() {
    let server = wiremock::MockServer::start().await;
    let config = McpConfig {
        connect_timeout: Duration::from_secs(1),
        call_timeout: Duration::from_secs(1),
        max_tool_rounds: 1,
        servers: vec![McpServerConfig {
            name: "unavailable".into(),
            url: format!("{}/mcp", server.uri()).parse().unwrap(),
        }],
    };
    assert!(
        matches!(McpRegistry::connect(&config).await, Err(McpRegistryError::ConnectFailed(name)) if name == "unavailable")
    );
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
    let config = McpConfig {
        connect_timeout: Duration::from_secs(2),
        call_timeout: Duration::from_secs(2),
        max_tool_rounds: 1,
        servers: vec![McpServerConfig {
            name: "telegram".into(),
            url: format!("{}/mcp", server.uri()).parse().unwrap(),
        }],
    };
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
        let result = registry.call(&call(r#"{"chat":"me"}"#)).await.unwrap();
        assert_eq!(result.content, "adapter result");
    }
    let requests = server.received_requests().await.unwrap();
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
    let calls: Vec<_> = bodies
        .iter()
        .filter(|body| body["method"] == "tools/call")
        .collect();
    assert_eq!(calls.len(), 2);
    for call in calls {
        assert_eq!(call["params"]["name"], "read_chat");
        assert_eq!(call["params"]["arguments"], json!({"chat":"me"}));
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
    let config = McpConfig {
        connect_timeout: Duration::from_secs(2),
        call_timeout: Duration::from_secs(2),
        max_tool_rounds: 1,
        servers: vec![McpServerConfig {
            name: "telegram".into(),
            url: format!("{}/mcp", server.uri()).parse().unwrap(),
        }],
    };
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
async fn discovery_failure_or_timeout_fails_startup() {
    for hang in [false, true] {
        let mut fake = FakeMcpClient::one();
        fake.fail_list = !hang;
        fake.hang_list = hang;
        let result = McpRegistry::from_clients(
            vec![("server".into(), Box::new(fake))],
            Duration::from_millis(10),
            Duration::from_secs(1),
        )
        .await;
        if hang {
            assert!(matches!(result, Err(McpRegistryError::ListTimeout(_))));
        } else {
            assert!(matches!(result, Err(McpRegistryError::ListFailed(_))));
        }
    }
}

#[tokio::test]
async fn invalid_arguments_are_rejected_before_dispatch() {
    let fake = FakeMcpClient::one();
    let registry = registry(fake.clone()).await;
    for arguments in ["not json", "[]", "null", "42", "\"string\""] {
        assert_eq!(
            registry.call(&call(arguments)).await.unwrap_err(),
            ToolExecutionError::InvalidArguments
        );
    }
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unknown_tool_is_rejected_before_dispatch() {
    let fake = FakeMcpClient::one();
    let registry = registry(fake.clone()).await;
    let mut request = call("{}");
    request.name = "unknown".into();
    assert_eq!(
        registry.call(&request).await.unwrap_err(),
        ToolExecutionError::UnknownTool
    );
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn call_timeout_is_typed_and_does_not_retry() {
    let mut fake = FakeMcpClient::one();
    fake.hang_call = true;
    let registry = registry(fake.clone()).await;
    assert_eq!(
        registry.call(&call("{}")).await.unwrap_err(),
        ToolExecutionError::Timeout
    );
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn transport_failure_is_typed_and_does_not_retry() {
    let mut fake = FakeMcpClient::one();
    fake.fail_call = true;
    let registry = registry(fake.clone()).await;
    assert_eq!(
        registry.call(&call("{}")).await.unwrap_err(),
        ToolExecutionError::Transport
    );
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
}
