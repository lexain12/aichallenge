use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    provider::ModelToolCall,
    tools::mcp::{McpClient, McpClientError, McpFuture, McpRegistry, McpRegistryError},
    tools::{ToolExecutionError, ToolExecutor},
};
use rmcp::model::{CallToolResult, Tool};
use serde_json::{Map, Value, json};

type RecordedCalls = Arc<Mutex<Vec<(String, Map<String, Value>)>>>;

#[tokio::test]
async fn remote_names_never_reach_registry_error_formatting() {
    for name in ["SECRET_MARKER".repeat(10_000), "SECRET_MARKER".into()] {
        let fake = FakeMcpClient::new(
            json!([{"name": name, "inputSchema":{}}, {"name": name, "inputSchema":{}}]),
        );
        let error = McpRegistry::from_clients(
            vec![("server".into(), Box::new(fake))],
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
        .err()
        .unwrap();
        for formatted in [error.to_string(), format!("{error:?}")] {
            assert!(formatted.len() < 64);
            assert!(!formatted.contains("SECRET_MARKER"));
            assert!(matches!(
                formatted.as_str(),
                "mcp_invalid_tool_name" | "mcp_name_collision"
            ));
        }
    }
}

#[test]
fn aliases_never_reach_composite_error_formatting() {
    use crate::tools::CompositeToolExecutor;
    for (name, has_route) in [
        ("SECRET_MARKER".repeat(10_000), false),
        ("SECRET_MARKER".into(), true),
    ] {
        let definition = crate::provider::ModelToolDefinition {
            name,
            description: None,
            parameters: Map::new(),
            read_only: true,
        };
        let executor: Arc<dyn ToolExecutor> = Arc::new(OpaqueExecutor {
            definitions: vec![definition],
            has_route,
        });
        let error = CompositeToolExecutor::new(vec![executor.clone(), executor])
            .err()
            .unwrap();
        for formatted in [error.to_string(), format!("{error:?}")] {
            assert!(formatted.len() < 64);
            assert!(!formatted.contains("SECRET_MARKER"));
            assert!(matches!(
                formatted.as_str(),
                "tool_catalog_missing_route" | "tool_catalog_name_collision"
            ));
        }
    }
}

struct OpaqueExecutor {
    definitions: Vec<crate::provider::ModelToolDefinition>,
    has_route: bool,
}

impl ToolExecutor for OpaqueExecutor {
    fn definitions(&self) -> &[crate::provider::ModelToolDefinition] {
        &self.definitions
    }
    fn route(&self, name: &str) -> Option<crate::tools::ToolRoute<'_>> {
        (self.has_route
            && self
                .definitions
                .iter()
                .any(|definition| definition.name == name))
        .then_some(crate::tools::ToolRoute {
            server_name: "local",
            tool_name: "actual_operation",
        })
    }
    fn is_read_only(&self, _: &str) -> Option<bool> {
        Some(true)
    }
    fn call<'a>(&'a self, call: &'a ModelToolCall) -> crate::tools::ToolFuture<'a> {
        Box::pin(async move {
            assert_eq!(call.name, "totally_unrelated_alias");
            Ok(crate::tools::ToolExecutionResult {
                content: "opaque result".into(),
                is_error: false,
                error_code: None,
                delivery_uncertain: false,
            })
        })
    }
}

#[tokio::test]
async fn composite_never_derives_identity_from_provider_alias() {
    use crate::tools::{CompositeToolExecutor, ToolCatalogError, ToolRoute};
    let definition = crate::provider::ModelToolDefinition {
        name: "totally_unrelated_alias".into(),
        description: None,
        parameters: Map::new(),
        read_only: true,
    };
    let catalog = CompositeToolExecutor::new(vec![Arc::new(OpaqueExecutor {
        definitions: vec![definition.clone()],
        has_route: true,
    })])
    .unwrap();
    assert_eq!(
        catalog.route("totally_unrelated_alias"),
        Some(ToolRoute {
            server_name: "local",
            tool_name: "actual_operation"
        })
    );
    assert_eq!(catalog.is_read_only("totally_unrelated_alias"), Some(true));
    assert_eq!(
        catalog
            .call(&ModelToolCall {
                id: "id".into(),
                name: definition.name.clone(),
                arguments: "{}".into()
            })
            .await
            .unwrap()
            .content,
        "opaque result"
    );
    assert!(matches!(
        CompositeToolExecutor::new(vec![Arc::new(OpaqueExecutor {
            definitions: vec![definition],
            has_route: false
        })]),
        Err(ToolCatalogError::MissingRoute)
    ));
}

#[tokio::test]
async fn composite_preserves_opaque_routes_and_rejects_duplicate_aliases() {
    use crate::tools::{CompositeToolExecutor, ToolCatalogError, ToolRoute};
    let fake = FakeMcpClient::new(json!([{"name":"read__nested", "inputSchema":{}}]));
    let first: Arc<dyn ToolExecutor> = Arc::new(registry(fake.clone()).await);
    let second: Arc<dyn ToolExecutor> = Arc::new(registry(FakeMcpClient::one()).await);
    let catalog = CompositeToolExecutor::new(vec![first.clone(), second.clone()]).unwrap();
    assert_eq!(
        catalog.route("telegram__read__nested"),
        Some(ToolRoute {
            server_name: "telegram",
            tool_name: "read__nested"
        })
    );
    let request = ModelToolCall {
        id: "opaque".into(),
        name: "telegram__read__nested".into(),
        arguments: "{}".into(),
    };
    assert_eq!(catalog.call(&request).await.unwrap().content, "ok");
    assert_eq!(fake.calls.lock().unwrap()[0].0, "read__nested");
    assert!(matches!(
        CompositeToolExecutor::new(vec![second.clone(), second]),
        Err(ToolCatalogError::NameCollision)
    ));
    assert_eq!(catalog.route("missing"), None);
    assert_eq!(catalog.is_read_only("missing"), None);
    assert_eq!(
        catalog
            .call(&ModelToolCall {
                name: "missing".into(),
                ..request
            })
            .await
            .unwrap_err(),
        ToolExecutionError::UnknownTool
    );
}

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
            json!([{"name":"read_chat", "description":"Read a chat", "inputSchema":{"type":"object", "properties":{"chat_id":{"type":"string"}}}, "annotations":{"readOnlyHint":true}}]),
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
    registry.call(&call(r#"{"chat_id":"7"}"#)).await.unwrap();
    registry.call(&call("{}")).await.unwrap();
    assert_eq!(
        *fake.calls.lock().unwrap(),
        vec![
            (
                "read_chat".into(),
                json!({"chat_id":"7"}).as_object().unwrap().clone()
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
async fn unrecognized_structured_error_is_sanitized_before_content_conversion() {
    let mut fake = FakeMcpClient::one();
    fake.result = serde_json::from_value(json!({"structuredContent":{"answer":42}, "content":[{"type":"image", "data":"eA==", "mimeType":"image/png"}], "isError":true})).unwrap();
    let result = registry(fake).await.call(&call("{}")).await.unwrap();
    assert_eq!(result.content, r#"{"error":"mcp_tool_error"}"#);
    assert!(result.is_error);
    assert_eq!(result.error_code.as_deref(), Some("mcp_tool_error"));
    assert!(!result.delivery_uncertain);
}

#[tokio::test]
async fn allowlisted_rate_limit_on_a_read_reaches_the_model_as_a_safe_code() {
    let mut fake = FakeMcpClient::one();
    fake.result = serde_json::from_value(json!({
        "isError": true,
        "content": [{
            "type": "text",
            "text": "Error executing tool read_chat: {\"mcp_error\":{\"version\":1,\"code\":\"rate_limited\"}}"
        }]
    }))
    .unwrap();

    let result = registry(fake).await.call(&call("{}")).await.unwrap();

    assert!(result.is_error);
    assert!(!result.delivery_uncertain);
    assert_eq!(result.error_code.as_deref(), Some("rate_limited"));
    assert_eq!(result.content, r#"{"error":"rate_limited"}"#);
}

#[tokio::test]
async fn allowlisted_telegram_read_errors_reach_the_model_as_safe_codes() {
    for code in ["chat_not_found", "telegram_unauthorized"] {
        let mut fake = FakeMcpClient::one();
        fake.result = serde_json::from_value(json!({
            "isError": true,
            "content": [{
                "type": "text",
                "text": format!(
                    "Error executing tool read_chat: {{\"mcp_error\":{{\"version\":1,\"code\":\"{code}\"}}}}"
                )
            }]
        }))
        .unwrap();

        let result = registry(fake).await.call(&call("{}")).await.unwrap();

        assert!(result.is_error);
        assert!(!result.delivery_uncertain);
        assert_eq!(result.error_code.as_deref(), Some(code));
        assert_eq!(result.content, json!({"error": code}).to_string());
    }
}

#[tokio::test]
async fn server_delivery_unknown_marks_a_write_uncertain_without_replaying_it() {
    let mut fake = FakeMcpClient::new(json!([
        {"name":"send_message","inputSchema":{},"annotations":{"readOnlyHint":false}}
    ]));
    fake.result = serde_json::from_str(include_str!(
        "../../../tests/fixtures/mcp_delivery_unknown.json"
    ))
    .unwrap();
    let observed = fake.clone();
    let registry = registry(fake).await;
    let result = registry
        .call(&ModelToolCall {
            id: "uncertain-write".into(),
            name: "telegram__send_message".into(),
            arguments: "{}".into(),
        })
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.delivery_uncertain);
    assert_eq!(result.error_code.as_deref(), Some("delivery_unknown"));
    assert_eq!(result.content, r#"{"error":"delivery_unknown"}"#);
    assert_eq!(observed.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn claimed_delivery_unknown_on_a_read_remains_a_safe_generic_failure() {
    let mut fake = FakeMcpClient::one();
    fake.result = serde_json::from_value(json!({
        "isError":true,
        "structuredContent":{"mcp_error":{"version":1,"code":"delivery_unknown"}},
        "content":[]
    }))
    .unwrap();
    let result = registry(fake).await.call(&call("{}")).await.unwrap();
    assert!(result.is_error);
    assert!(!result.delivery_uncertain);
    assert_eq!(result.error_code.as_deref(), Some("mcp_tool_error"));
    assert_eq!(result.content, r#"{"error":"mcp_tool_error"}"#);
}

#[tokio::test]
async fn untrusted_mcp_errors_never_expose_bodies_or_claim_uncertainty() {
    for response in [
        json!({"isError":true,"content":[{"type":"text","text":"private error details"}]}),
        json!({"isError":true,"content":[{"type":"text","text":"delivery_unknown private error details"}]}),
        json!({"isError":true,"structuredContent":{"mcp_error":{"version":2,"code":"delivery_unknown"}},"content":[]}),
        json!({"isError":true,"structuredContent":{"mcp_error":{"version":1,"code":"private_secret"}},"content":[]}),
        json!({"isError":true,"structuredContent":{"mcp_error":{"version":1,"code":"delivery_unknown","message":"private"}},"content":[]}),
        json!({"isError":true,"structuredContent":{"mcp_error":{"version":1,"code":"delivery_unknown"},"extra":"private"},"content":[]}),
        json!({"isError":true,"structuredContent":{"mcp_error":{"code":"delivery_unknown"}},"content":[]}),
        json!({"isError":true,"content":[{"type":"text","text":"Error executing tool another: {\"mcp_error\":{\"version\":1,\"code\":\"delivery_unknown\"}}"}]}),
        json!({"isError":true,"content":[{"type":"text","text":"{\"mcp_error\":{\"version\":1,\"version\":1,\"code\":\"delivery_unknown\"}}"}]}),
        json!({"isError":true,"content":[{"type":"text","text":"{\"mcp_error\":{\"version\":1,\"code\":\"delivery_unknown\"}}"},{"type":"text","text":"private details"}]}),
    ] {
        let mut fake = FakeMcpClient::new(json!([{"name":"send_message","inputSchema":{}}]));
        fake.result = serde_json::from_value(response).unwrap();
        let result = registry(fake)
            .await
            .call(&ModelToolCall {
                id: "write".into(),
                name: "telegram__send_message".into(),
                arguments: "{}".into(),
            })
            .await
            .unwrap();
        assert!(result.is_error);
        assert!(!result.delivery_uncertain);
        assert_eq!(result.error_code.as_deref(), Some("mcp_tool_error"));
        assert_eq!(result.content, r#"{"error":"mcp_tool_error"}"#);
    }
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
        assert!(matches!(result, Err(McpRegistryError::InvalidToolName)));
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
    assert!(matches!(result, Err(McpRegistryError::NameCollision)));
}

#[tokio::test]
async fn discovery_failure_or_timeout_fails_startup() {
    for hang in [false, true] {
        let mut fake = FakeMcpClient::one();
        fake.fail_list = !hang;
        fake.hang_list = hang;
        let result = tokio::time::timeout(
            Duration::from_millis(250),
            McpRegistry::from_clients(
                vec![("server".into(), Box::new(fake))],
                Duration::from_millis(10),
                Duration::from_secs(5),
            ),
        )
        .await
        .expect("discovery must use its own short timeout, not the call timeout");
        if hang {
            assert!(matches!(result, Err(McpRegistryError::ListTimeout)));
        } else {
            assert!(matches!(result, Err(McpRegistryError::ListFailed)));
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
    let registry = McpRegistry::from_clients(
        vec![("telegram".into(), Box::new(fake.clone()))],
        Duration::from_secs(5),
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(250), registry.call(&call("{}")))
            .await
            .expect("tool call must use its own short timeout, not the discovery timeout")
            .unwrap_err(),
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
