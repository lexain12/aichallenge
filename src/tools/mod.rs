//! Provider-neutral tool execution and immutable composite catalog.
pub mod conversation;
pub mod mcp;
pub use conversation::{
    ConversationStep, DEFAULT_MAX_TOOL_ROUNDS, ToolConversation, ToolLoopError, ToolResultMessage,
};

use crate::provider::{ModelToolCall, ModelToolDefinition};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolExecutionResult {
    pub content: String,
    pub is_error: bool,
    pub error_code: Option<String>,
    pub delivery_uncertain: bool,
}

/// Safe errors deliberately exclude arguments, URLs, and remote error bodies.
/// A timeout or transport failure can occur after dispatch; callers must use
/// the tool's read-only classification when deciding whether delivery is uncertain.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ToolExecutionError {
    #[error("Unknown tool")]
    UnknownTool,
    #[error("Tool arguments must be a valid JSON object")]
    InvalidArguments,
    #[error("Tool call timed out")]
    Timeout,
    #[error("Tool transport or protocol failed")]
    Transport,
}

pub type ToolFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ToolExecutionResult, ToolExecutionError>> + Send + 'a>>;

/// Exact dispatch identity. Provider aliases are opaque and must not be parsed
/// to recover the server or original tool name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolRoute<'a> {
    pub server_name: &'a str,
    pub tool_name: &'a str,
}

pub trait ToolExecutor: Send + Sync {
    fn definitions(&self) -> &[ModelToolDefinition];
    fn route(&self, name: &str) -> Option<ToolRoute<'_>>;
    fn is_read_only(&self, name: &str) -> Option<bool>;
    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a>;
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ToolCatalogError {
    #[error("Tool provider name collision: {0}")]
    NameCollision(String),
    #[error("Tool is missing a dispatch route: {0}")]
    MissingRoute(String),
}

struct CompositeRoute {
    executor_index: usize,
    server_name: String,
    tool_name: String,
    read_only: bool,
}

/// Snapshot definitions and dispatch identities once; aliases stay opaque.
pub struct CompositeToolExecutor {
    executors: Vec<Arc<dyn ToolExecutor>>,
    definitions: Vec<ModelToolDefinition>,
    routes: HashMap<String, CompositeRoute>,
}

impl CompositeToolExecutor {
    pub fn new(executors: Vec<Arc<dyn ToolExecutor>>) -> Result<Self, ToolCatalogError> {
        let mut definitions = Vec::new();
        let mut routes = HashMap::new();
        for (executor_index, executor) in executors.iter().enumerate() {
            for definition in executor.definitions() {
                if routes.contains_key(&definition.name) {
                    return Err(ToolCatalogError::NameCollision(definition.name.clone()));
                }
                let route = executor
                    .route(&definition.name)
                    .ok_or_else(|| ToolCatalogError::MissingRoute(definition.name.clone()))?;
                routes.insert(
                    definition.name.clone(),
                    CompositeRoute {
                        executor_index,
                        server_name: route.server_name.to_owned(),
                        tool_name: route.tool_name.to_owned(),
                        read_only: definition.read_only,
                    },
                );
                definitions.push(definition.clone());
            }
        }
        Ok(Self {
            executors,
            definitions,
            routes,
        })
    }
}

impl ToolExecutor for CompositeToolExecutor {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }
    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        self.routes.get(name).map(|route| ToolRoute {
            server_name: &route.server_name,
            tool_name: &route.tool_name,
        })
    }
    fn is_read_only(&self, name: &str) -> Option<bool> {
        self.routes.get(name).map(|route| route.read_only)
    }
    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a> {
        match self.routes.get(&call.name) {
            Some(route) => self.executors[route.executor_index].call(call),
            None => Box::pin(async { Err(ToolExecutionError::UnknownTool) }),
        }
    }
}
