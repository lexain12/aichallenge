use std::collections::{BTreeMap, HashSet};
use std::io;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::settings::ProviderSettings;

use super::{
    AssistantTurn, ModelToolCall, ModelToolDefinition, Provider, ProviderError, ProviderFuture,
    ProviderMessage, TokenUsage, serialized_len,
};

const MAX_ERROR_BODY_BYTES: usize = 4096;
const REDACTED: &str = "[REDACTED]";

#[derive(Clone)]
pub struct DeepSeekProvider {
    http: reqwest::Client,
    endpoint: Url,
    api_key: String,
    model: String,
    max_tokens: u32,
}

impl DeepSeekProvider {
    pub fn new(settings: &ProviderSettings) -> Result<Self, ProviderError> {
        let endpoint = Url::parse(&format!(
            "{}/chat/completions",
            settings.base_url().as_str().trim_end_matches('/')
        ))
        .map_err(|_| ProviderError::new("configuration"))?;
        let http = reqwest::Client::builder()
            .timeout(settings.timeout())
            .build()
            .map_err(|_| ProviderError::new("configuration"))?;
        Ok(Self {
            http,
            endpoint,
            api_key: settings.api_key().to_owned(),
            model: settings.model().to_owned(),
            max_tokens: settings.max_tokens(),
        })
    }

    async fn stream(
        &self,
        messages: &[ProviderMessage],
        tools: &[ModelToolDefinition],
        text_sink: &mut (dyn FnMut(&str) -> io::Result<()> + Send),
    ) -> Result<AssistantTurn, ProviderError> {
        let tools: Vec<_> = tools.iter().map(ProviderToolDefinition::from).collect();
        let response = self
            .http
            .post(self.endpoint.clone())
            .bearer_auth(&self.api_key)
            .json(&ChatRequest {
                model: &self.model,
                messages,
                max_tokens: self.max_tokens,
                stream: true,
                stream_options: StreamOptions {
                    include_usage: true,
                },
                tools: &tools,
            })
            .send()
            .await
            .map_err(|_| ProviderError::new("request"))?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = read_error_body(response, &self.api_key).await?;
            return Err(if is_context_length_error(&body) {
                ProviderError::http_with_code("context_too_long", status, body)
            } else {
                ProviderError::http(status, body)
            });
        }

        let mut events = response.bytes_stream().eventsource();
        let mut answer = String::new();
        let mut usage = None;
        let mut partial_calls = BTreeMap::<u32, PartialToolCall>::new();
        let mut finish_reason = None;
        let mut saw_done = false;
        while let Some(event) = events.next().await {
            let event = event.map_err(|_| ProviderError::new("stream").with_usage(usage))?;
            if event.data == "[DONE]" {
                saw_done = true;
                break;
            }
            let chunk: StreamChunk = serde_json::from_str(&event.data)
                .map_err(|_| ProviderError::new("invalid_stream").with_usage(usage))?;
            if let Some(value) = chunk.usage {
                usage = Some(value);
            }
            for choice in chunk.choices {
                if finish_reason.is_some() {
                    return Err(ProviderError::new("invalid_stream").with_usage(usage));
                }
                if let Some(content) = choice.delta.content
                    && !content.is_empty()
                {
                    text_sink(&content)
                        .map_err(|_| ProviderError::new("output").with_usage(usage))?;
                    answer.push_str(&content);
                }
                for delta in choice.delta.tool_calls.unwrap_or_default() {
                    partial_calls
                        .entry(delta.index)
                        .or_default()
                        .merge(delta)
                        .map_err(|error| error.with_usage(usage))?;
                }
                finish_reason = match choice.finish_reason.as_deref() {
                    None => None,
                    Some("stop") => Some(FinishReason::Stop),
                    Some("tool_calls") => Some(FinishReason::ToolCalls),
                    Some("length") => {
                        return Err(ProviderError::new("truncated").with_usage(usage));
                    }
                    Some(_) => {
                        return Err(ProviderError::new("invalid_stream").with_usage(usage));
                    }
                };
            }
        }

        if !saw_done {
            return Err(ProviderError::new("incomplete_stream").with_usage(usage));
        }
        match (finish_reason, partial_calls.is_empty()) {
            (None, _) => {
                return Err(ProviderError::new("incomplete_stream").with_usage(usage));
            }
            (Some(FinishReason::Stop), false) | (Some(FinishReason::ToolCalls), true) => {
                return Err(ProviderError::new("invalid_stream").with_usage(usage));
            }
            _ => {}
        }
        if partial_calls.is_empty() {
            if answer.trim().is_empty() {
                return Err(ProviderError::new("empty_response").with_usage(usage));
            }
            return Ok(AssistantTurn::FinalText {
                content: answer,
                usage,
            });
        }
        let calls = partial_calls
            .into_values()
            .map(PartialToolCall::finish)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.with_usage(usage))?;
        let mut ids = HashSet::new();
        if calls.iter().any(|call| !ids.insert(&call.id)) {
            return Err(ProviderError::new("invalid_tool_call").with_usage(usage));
        }
        Ok(AssistantTurn::ToolCalls {
            content: (!answer.is_empty()).then_some(answer),
            calls,
            usage,
        })
    }
}

fn is_context_length_error(body: &str) -> bool {
    let parsed = serde_json::from_str::<serde_json::Value>(body).ok();
    let explicit_code = parsed.as_ref().and_then(|value| {
        value
            .pointer("/error/code")
            .or_else(|| value.pointer("/code"))
            .and_then(serde_json::Value::as_str)
    });
    if matches!(
        explicit_code,
        Some("context_length_exceeded" | "context_too_long" | "max_context_length_exceeded")
    ) {
        return true;
    }
    let message = parsed
        .as_ref()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.pointer("/message"))
        })
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    message.contains("maximum context length") || message.contains("context length exceeded")
}

#[derive(Clone, Copy)]
enum FinishReason {
    Stop,
    ToolCalls,
}

impl Provider for DeepSeekProvider {
    fn stream_turn<'a>(
        &'a self,
        messages: &'a [ProviderMessage],
        tools: &'a [ModelToolDefinition],
        text_sink: &'a mut (dyn FnMut(&str) -> io::Result<()> + Send),
    ) -> ProviderFuture<'a> {
        Box::pin(self.stream(messages, tools, text_sink))
    }

    fn serialized_request_len(
        &self,
        messages: &[ProviderMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<usize, ProviderError> {
        let tools: Vec<_> = tools.iter().map(ProviderToolDefinition::from).collect();
        serialized_len(&ChatRequest {
            model: &self.model,
            messages,
            max_tokens: self.max_tokens,
            stream: true,
            stream_options: StreamOptions {
                include_usage: true,
            },
            tools: &tools,
        })
    }
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: &'a [ProviderMessage],
    max_tokens: u32,
    stream: bool,
    stream_options: StreamOptions,
    #[serde(skip_serializing_if = "<[ProviderToolDefinition<'_>]>::is_empty")]
    tools: &'a [ProviderToolDefinition<'a>],
}

#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

#[derive(Serialize)]
struct ProviderToolDefinition<'a> {
    r#type: &'static str,
    function: ProviderFunctionDefinition<'a>,
}

#[derive(Serialize)]
struct ProviderFunctionDefinition<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    parameters: &'a serde_json::Map<String, serde_json::Value>,
}

impl<'a> From<&'a ModelToolDefinition> for ProviderToolDefinition<'a> {
    fn from(tool: &'a ModelToolDefinition) -> Self {
        Self {
            r#type: "function",
            function: ProviderFunctionDefinition {
                name: &tool.name,
                description: tool.description.as_deref(),
                parameters: &tool.parameters,
            },
        }
    }
}

#[derive(Deserialize)]
struct StreamChunk {
    usage: Option<TokenUsage>,
    #[serde(default)]
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    finish_reason: Option<String>,
    #[serde(default)]
    delta: Delta,
}

#[derive(Default, Deserialize)]
struct Delta {
    content: Option<String>,
    tool_calls: Option<Vec<ToolCallDelta>>,
}

#[derive(Deserialize)]
struct ToolCallDelta {
    index: u32,
    id: Option<String>,
    r#type: Option<String>,
    function: Option<FunctionDelta>,
}

#[derive(Deserialize)]
struct FunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Default)]
struct PartialToolCall {
    id: String,
    name: String,
    arguments: String,
    declared: bool,
}

impl PartialToolCall {
    fn merge(&mut self, delta: ToolCallDelta) -> Result<(), ProviderError> {
        if let Some(kind) = delta.r#type {
            if kind != "function" || self.declared {
                return Err(ProviderError::new("invalid_tool_call"));
            }
            self.declared = true;
        }
        if let Some(id) = delta.id {
            self.id.push_str(&id);
        }
        if let Some(function) = delta.function {
            if let Some(name) = function.name {
                self.name.push_str(&name);
            }
            if let Some(arguments) = function.arguments {
                self.arguments.push_str(&arguments);
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<ModelToolCall, ProviderError> {
        if self.id.trim().is_empty()
            || self.name.trim().is_empty()
            || !serde_json::from_str::<serde_json::Value>(&self.arguments)
                .is_ok_and(|arguments| arguments.is_object())
        {
            return Err(ProviderError::new("invalid_tool_call"));
        }
        Ok(ModelToolCall {
            id: self.id,
            name: self.name,
            arguments: self.arguments,
        })
    }
}

async fn read_error_body(
    response: reqwest::Response,
    api_key: &str,
) -> Result<String, ProviderError> {
    // Read across the display limit so a key starting at its edge is redacted.
    let read_limit = MAX_ERROR_BODY_BYTES.saturating_add(api_key.len().saturating_sub(1));
    let mut bytes = Vec::with_capacity(read_limit);
    let mut stream = response.bytes_stream();
    while bytes.len() < read_limit {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk.map_err(|_| ProviderError::new("request"))?;
        let remaining = read_limit - bytes.len();
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    let mut body = String::from_utf8_lossy(&bytes).replace(api_key, REDACTED);
    if body.len() > MAX_ERROR_BODY_BYTES {
        let mut end = floor_char_boundary(&body, MAX_ERROR_BODY_BYTES);
        if let Some((start, _)) = body
            .match_indices(REDACTED)
            .find(|(start, _)| *start < end && start + REDACTED.len() > end)
        {
            end = start + REDACTED.len();
        }
        body.truncate(end);
    }
    Ok(body)
}

fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn error_body_is_bounded_and_key_crossing_limit_is_redacted() {
        let server = MockServer::start().await;
        let key = "secret-api-key";
        let body = format!("{}{}after-key", "x".repeat(4090), key);
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string(body))
            .mount(&server)
            .await;
        let response = reqwest::Client::new()
            .post(server.uri())
            .send()
            .await
            .unwrap();
        let body = read_error_body(response, key).await.unwrap();
        let diagnostic = ProviderError::http(401, body).raw_diagnostic();
        assert!(diagnostic.contains(REDACTED));
        assert!(!diagnostic.contains(key));
        assert!(!diagnostic.contains("after-key"));
        assert!(diagnostic.len() < 4300);
    }
}
