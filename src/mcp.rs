//! Immutable discovery and dispatch for configured Streamable HTTP MCP servers.

use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use rmcp::{
    RoleClient, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ClientConfig, ContentBlock, Tool,
    },
    service::RunningService,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::time::timeout;

use crate::{
    config::McpConfig,
    tool_calling::{
        ModelToolCall, ModelToolDefinition, ToolExecutionError, ToolExecutionResult, ToolExecutor,
        ToolFuture, ToolRoute,
    },
};

/// The adapter boundary never exposes potentially sensitive remote diagnostics.
#[derive(Clone, Copy, Debug, Error)]
#[error("MCP client request failed")]
pub struct McpClientError;

pub type McpFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, McpClientError>> + Send + 'a>>;

/// An initialized client; implementations must return the complete tool catalog.
pub trait McpClient: Send + Sync {
    fn list_tools(&self) -> McpFuture<'_, Vec<Tool>>;
    fn call_tool<'a>(
        &'a self,
        name: &'a str,
        arguments: Map<String, Value>,
    ) -> McpFuture<'a, CallToolResult>;
}

struct RmcpClient {
    service: RunningService<RoleClient, ClientConfig>,
}

impl McpClient for RmcpClient {
    fn list_tools(&self) -> McpFuture<'_, Vec<Tool>> {
        Box::pin(async {
            self.service
                .list_all_tools()
                .await
                .map_err(|_| McpClientError)
        })
    }

    fn call_tool<'a>(
        &'a self,
        name: &'a str,
        arguments: Map<String, Value>,
    ) -> McpFuture<'a, CallToolResult> {
        Box::pin(async move {
            // Do not let rmcp's high-level MRTR helper reissue a write call.
            match self
                .service
                .call_tool_once(
                    CallToolRequestParams::new(name.to_owned()).with_arguments(arguments),
                )
                .await
                .map_err(|_| McpClientError)?
            {
                CallToolResponse::Complete(result) => Ok(result),
                _ => Err(McpClientError),
            }
        })
    }
}

#[derive(Debug, Error)]
pub enum McpRegistryError {
    #[error("MCP server {0} failed to connect")]
    ConnectFailed(String),
    #[error("MCP server {0} connection timed out")]
    ConnectTimeout(String),
    #[error("MCP server {0} tool discovery failed")]
    ListFailed(String),
    #[error("MCP server {0} tool discovery timed out")]
    ListTimeout(String),
    #[error("MCP tool has an invalid provider name: {0}")]
    InvalidToolName(String),
    #[error("MCP tool name collision: {0}")]
    NameCollision(String),
}

struct Route {
    client_index: usize,
    server_name: String,
    original_name: String,
    read_only: bool,
}

pub struct McpRegistry {
    clients: Vec<Box<dyn McpClient>>,
    definitions: Vec<ModelToolDefinition>,
    routes: HashMap<String, Route>,
    call_timeout: Duration,
}

impl McpRegistry {
    pub async fn connect(config: &McpConfig) -> Result<Self, McpRegistryError> {
        let mut clients: Vec<(String, Box<dyn McpClient>)> = Vec::new();
        for server in &config.servers {
            // Session recovery replays ordinary POSTs, which may duplicate writes.
            // Keep rmcp's no-redirect HTTP client so redirects cannot replay calls.
            let transport = StreamableHttpClientTransport::from_config(
                StreamableHttpClientTransportConfig::with_uri(server.url.as_str())
                    .reinit_on_expired_session(false),
            );
            let service = timeout(
                config.connect_timeout,
                ClientConfig::default().serve(transport),
            )
            .await
            .map_err(|_| McpRegistryError::ConnectTimeout(server.name.clone()))?
            .map_err(|_| McpRegistryError::ConnectFailed(server.name.clone()))?;
            clients.push((server.name.clone(), Box::new(RmcpClient { service })));
        }
        Self::from_clients(clients, config.connect_timeout, config.call_timeout).await
    }

    /// Build from initialized clients, with a separate deadline for each catalog.
    pub async fn from_clients(
        clients: Vec<(String, Box<dyn McpClient>)>,
        list_timeout: Duration,
        call_timeout: Duration,
    ) -> Result<Self, McpRegistryError> {
        let mut registry = Self {
            clients: Vec::new(),
            definitions: Vec::new(),
            routes: HashMap::new(),
            call_timeout,
        };
        for (server_name, client) in clients {
            let tools = timeout(list_timeout, client.list_tools())
                .await
                .map_err(|_| McpRegistryError::ListTimeout(server_name.clone()))?
                .map_err(|_| McpRegistryError::ListFailed(server_name.clone()))?;
            let client_index = registry.clients.len();
            for tool in tools {
                let name = format!("{server_name}__{}", tool.name);
                if tool.name.is_empty()
                    || name.len() > 64
                    || !name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                {
                    return Err(McpRegistryError::InvalidToolName(name));
                }
                if registry.routes.contains_key(&name) {
                    return Err(McpRegistryError::NameCollision(name));
                }
                let read_only = tool
                    .annotations
                    .as_ref()
                    .and_then(|annotations| annotations.read_only_hint)
                    .unwrap_or(false);
                registry.routes.insert(
                    name.clone(),
                    Route {
                        client_index,
                        server_name: server_name.clone(),
                        original_name: tool.name.into_owned(),
                        read_only,
                    },
                );
                registry.definitions.push(ModelToolDefinition {
                    name,
                    description: tool.description.map(|description| description.into_owned()),
                    parameters: (*tool.input_schema).clone(),
                    read_only,
                });
            }
            registry.clients.push(client);
        }
        Ok(registry)
    }
}

impl ToolExecutor for McpRegistry {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        self.routes.get(name).map(|route| ToolRoute {
            server_name: &route.server_name,
            tool_name: &route.original_name,
        })
    }

    fn is_read_only(&self, name: &str) -> Option<bool> {
        self.routes.get(name).map(|route| route.read_only)
    }

    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async move {
            let route = self
                .routes
                .get(&call.name)
                .ok_or(ToolExecutionError::UnknownTool)?;
            let arguments: Map<String, Value> = serde_json::from_str(&call.arguments)
                .map_err(|_| ToolExecutionError::InvalidArguments)?;
            let result = timeout(
                self.call_timeout,
                self.clients[route.client_index].call_tool(&route.original_name, arguments),
            )
            .await
            .map_err(|_| ToolExecutionError::Timeout)?
            .map_err(|_| ToolExecutionError::Transport)?;
            Ok(convert_result(result))
        })
    }
}

fn convert_result(result: CallToolResult) -> ToolExecutionResult {
    let is_error = result.is_error.unwrap_or(false);
    let content = if let Some(structured) = result.structured_content {
        structured.to_string()
    } else {
        let mut text = Vec::new();
        for content in result.content {
            match content {
                ContentBlock::Text(content) => text.push(content.text),
                _ => {
                    return ToolExecutionResult {
                        content: "MCP tool returned unsupported non-text content".into(),
                        is_error: true,
                        error_code: Some("unsupported_content".into()),
                        delivery_uncertain: false,
                    };
                }
            }
        }
        text.join("\n")
    };
    ToolExecutionResult {
        content,
        is_error,
        error_code: is_error.then(|| "mcp_tool_error".into()),
        delivery_uncertain: false,
    }
}
