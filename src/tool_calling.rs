//! Provider- and service-neutral tool definitions and execution contracts.

use std::{future::Future, pin::Pin};

use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct ModelToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub parameters: Map<String, Value>,
    pub read_only: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

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

pub trait ToolExecutor: Send + Sync {
    fn definitions(&self) -> &[ModelToolDefinition];
    fn is_read_only(&self, name: &str) -> Option<bool>;
    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a>;
}
