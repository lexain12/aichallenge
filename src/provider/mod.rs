//! Minimal, provider-neutral chat turn types and streaming interface.

mod deepseek;

use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use deepseek::DeepSeekProvider;

#[derive(Clone, Debug, Serialize)]
pub struct ProviderMessage {
    role: ProviderRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ProviderToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum ProviderRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize)]
struct ProviderToolCall {
    id: String,
    r#type: &'static str,
    function: ProviderFunctionCall,
}

#[derive(Clone, Debug, Serialize)]
struct ProviderFunctionCall {
    name: String,
    arguments: String,
}

impl ProviderMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self::text(ProviderRole::System, content)
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::text(ProviderRole::User, content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::text(ProviderRole::Assistant, content)
    }

    fn text(role: ProviderRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: Some(content.into()),
            tool_calls: None,
            tool_call_id: None,
        }
    }

    pub fn assistant_tool_calls(content: Option<String>, calls: &[ModelToolCall]) -> Self {
        Self {
            role: ProviderRole::Assistant,
            content,
            tool_calls: Some(
                calls
                    .iter()
                    .map(|call| ProviderToolCall {
                        id: call.id.clone(),
                        r#type: "function",
                        function: ProviderFunctionCall {
                            name: call.name.clone(),
                            arguments: call.arguments.clone(),
                        },
                    })
                    .collect(),
            ),
            tool_call_id: None,
        }
    }

    pub fn tool_result(id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: ProviderRole::Tool,
            content: Some(content.into()),
            tool_calls: None,
            tool_call_id: Some(id.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModelToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub parameters: Map<String, Value>,
    pub read_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// Provider-reported token usage for one request.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens_details: Option<CompletionTokenDetails>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct CompletionTokenDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AssistantTurn {
    FinalText {
        content: String,
        usage: Option<TokenUsage>,
    },
    ToolCalls {
        content: Option<String>,
        calls: Vec<ModelToolCall>,
        usage: Option<TokenUsage>,
    },
}

pub type ProviderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AssistantTurn, ProviderError>> + Send + 'a>>;

pub trait Provider: Send + Sync {
    fn stream_turn<'a>(
        &'a self,
        messages: &'a [ProviderMessage],
        tools: &'a [ModelToolDefinition],
        text_sink: &'a mut (dyn FnMut(&str) -> io::Result<()> + Send),
    ) -> ProviderFuture<'a>;

    /// Returns the exact serialized wire-request size. Providers that do not
    /// implement this boundary fail closed before any network dispatch.
    fn serialized_request_len(
        &self,
        _messages: &[ProviderMessage],
        _tools: &[ModelToolDefinition],
    ) -> Result<usize, ProviderError> {
        Err(ProviderError::new("request_size_unavailable"))
    }
}

pub(crate) fn serialized_len(value: &impl Serialize) -> Result<usize, ProviderError> {
    #[derive(Default)]
    struct Counter(usize);

    impl Write for Counter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(buffer.len())
                .ok_or_else(|| io::Error::other("serialized request length overflow"))?;
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter::default();
    serde_json::to_writer(&mut counter, value).map_err(|_| ProviderError::new("serialization"))?;
    Ok(counter.0)
}

/// Safe error for protocol callers. The HTTP body is kept only for explicit
/// operator diagnostics; even Debug does not show it.
///
/// Protocol callers cannot access the HTTP body:
///
/// ```compile_fail
/// use deepseek_cli::provider::ProviderError;
/// fn leak(error: &ProviderError) {
///     let _ = error.raw_diagnostic();
/// }
/// ```
pub struct ProviderError {
    code: &'static str,
    status: Option<u16>,
    diagnostic: Option<String>,
    usage: Option<TokenUsage>,
}

impl ProviderError {
    /// Construct a provider-neutral, diagnostic-free machine error. Public
    /// provider implementations can fail without gaining access to raw HTTP
    /// diagnostics, and callers still map this code to a closed local set.
    pub fn safe(code: &'static str) -> Self {
        Self::new(code)
    }

    pub(crate) fn new(code: &'static str) -> Self {
        Self {
            code,
            status: None,
            diagnostic: None,
            usage: None,
        }
    }

    pub(crate) fn http(status: u16, diagnostic: String) -> Self {
        Self {
            code: "http",
            status: Some(status),
            diagnostic: Some(diagnostic),
            usage: None,
        }
    }

    pub(crate) fn with_usage(mut self, usage: Option<TokenUsage>) -> Self {
        self.usage = usage;
        self
    }

    pub fn safe_code(&self) -> &'static str {
        self.code
    }

    pub fn usage(&self) -> Option<TokenUsage> {
        self.usage
    }

    pub fn operator_metadata(&self) -> ProviderErrorMetadata {
        ProviderErrorMetadata {
            kind: self.code,
            status: self.status,
        }
    }

    #[allow(dead_code)] // Reserved for operator logging inside the VM crate.
    pub(crate) fn raw_diagnostic(&self) -> String {
        match (&self.status, &self.diagnostic) {
            (Some(status), Some(body)) => format!("DeepSeek API returned HTTP {status}: {body}"),
            _ => self.to_string(),
        }
    }
}

impl fmt::Debug for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderError")
            .field("code", &self.code)
            .field("status", &self.status)
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "provider failure: {}", self.code)
    }
}

impl std::error::Error for ProviderError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderErrorMetadata {
    pub kind: &'static str,
    pub status: Option<u16>,
}
