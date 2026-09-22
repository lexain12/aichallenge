use std::io;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::chat::Message;
use crate::config::Config;

const MAX_ERROR_BODY_BYTES: usize = 4096;
const REDACTED: &str = "[REDACTED]";

/// Direct HTTP client for DeepSeek Chat Completions.
#[derive(Clone)]
pub struct DeepSeekClient {
    http: reqwest::Client,
    endpoint: Url,
    api_key: String,
    model: String,
    temperature: f64,
    max_tokens: u32,
    disable_thinking: bool,
    thinking: Option<String>,
    top_p: Option<f64>,
    stop: Vec<String>,
    include_usage: bool,
}

impl DeepSeekClient {
    pub fn new(config: &Config) -> Result<Self, ClientError> {
        let endpoint = Url::parse(&format!(
            "{}/chat/completions",
            config.base_url().as_str().trim_end_matches('/')
        ))
        .map_err(ClientError::Endpoint)?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds()))
            .build()
            .map_err(ClientError::Build)?;

        Ok(Self {
            http,
            endpoint,
            api_key: config.api_key().to_owned(),
            model: config.model().to_owned(),
            temperature: config.temperature(),
            max_tokens: config.max_tokens(),
            disable_thinking: false,
            thinking: config.thinking().map(str::to_owned),
            top_p: config.top_p(),
            stop: config.stop().to_vec(),
            include_usage: config.include_usage(),
        })
    }

    /// Keep the reasoning experiment in the same non-thinking API mode.
    pub fn without_thinking(mut self) -> Self {
        self.disable_thinking = true;
        self
    }

    pub fn model_name(&self) -> &str {
        &self.model
    }

    pub async fn stream_chat<F>(
        &self,
        messages: &[Message],
        mut on_text: F,
    ) -> Result<String, ClientError>
    where
        F: FnMut(&str) -> io::Result<()>,
    {
        self.stream_chat_events(messages, |event| match event {
            StreamEvent::Text(text) => on_text(text),
            StreamEvent::Usage(_) => Ok(()),
        })
        .await
    }

    pub async fn stream_chat_events<F>(
        &self,
        messages: &[Message],
        on_event: F,
    ) -> Result<String, ClientError>
    where
        F: FnMut(StreamEvent<'_>) -> io::Result<()>,
    {
        self.stream_chat_events_with_options(
            messages,
            RequestOptions {
                model: &self.model,
                temperature: self.temperature,
                max_tokens: self.max_tokens,
                thinking: if self.disable_thinking {
                    Some(Thinking { r#type: "disabled" })
                } else {
                    self.thinking
                        .as_deref()
                        .map(|value| Thinking { r#type: value })
                },
                top_p: self.top_p,
                stop: &self.stop,
            },
            on_event,
        )
        .await
    }

    pub async fn summarize(
        &self,
        messages: &[Message],
        max_tokens: u32,
    ) -> Result<SummaryResult, ClientError> {
        self.deterministic_service_call(&self.model, messages, max_tokens)
            .await
    }

    pub async fn update_facts(
        &self,
        messages: &[Message],
        max_tokens: u32,
    ) -> Result<SummaryResult, ClientError> {
        self.deterministic_service_call(&self.model, messages, max_tokens)
            .await
    }

    pub async fn complete(
        &self,
        model: &str,
        messages: &[Message],
        max_tokens: u32,
    ) -> Result<SummaryResult, ClientError> {
        self.deterministic_service_call(model, messages, max_tokens)
            .await
    }

    async fn deterministic_service_call(
        &self,
        model: &str,
        messages: &[Message],
        max_tokens: u32,
    ) -> Result<SummaryResult, ClientError> {
        let mut usage = None;
        let empty_stop: &[String] = &[];
        let answer = self
            .stream_chat_events_with_options(
                messages,
                RequestOptions {
                    model,
                    temperature: 0.0,
                    max_tokens,
                    thinking: Some(Thinking { r#type: "disabled" }),
                    top_p: None,
                    stop: empty_stop,
                },
                |event| {
                    if let StreamEvent::Usage(value) = event {
                        usage = Some(value);
                    }
                    Ok(())
                },
            )
            .await;
        answer
            .and_then(|answer| {
                if answer.trim().is_empty() {
                    Err(ClientError::EmptyAnswer)
                } else {
                    Ok(SummaryResult { answer, usage })
                }
            })
            .map_err(|source| match usage {
                Some(usage) => ClientError::WithUsage {
                    source: Box::new(source),
                    usage,
                },
                None => source,
            })
    }

    async fn stream_chat_events_with_options<F>(
        &self,
        messages: &[Message],
        options: RequestOptions<'_>,
        mut on_event: F,
    ) -> Result<String, ClientError>
    where
        F: FnMut(StreamEvent<'_>) -> io::Result<()>,
    {
        let response = self
            .http
            .post(self.endpoint.clone())
            .bearer_auth(&self.api_key)
            .json(&ChatRequest {
                model: options.model,
                messages,
                temperature: options.temperature,
                max_tokens: options.max_tokens,
                stream: true,
                stream_options: self.include_usage.then_some(StreamOptions {
                    include_usage: true,
                }),
                thinking: options.thinking,
                top_p: options.top_p,
                stop: options.stop,
            })
            .send()
            .await
            .map_err(ClientError::Request)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = read_error_body(response, &self.api_key).await?;
            return Err(ClientError::Api { status, body });
        }

        let mut events = response.bytes_stream().eventsource();
        let mut answer = String::new();
        let mut saw_done = false;
        while let Some(event) = events.next().await {
            let event = event.map_err(|error| ClientError::Stream(error.to_string()))?;
            if event.data == "[DONE]" {
                saw_done = true;
                break;
            }

            let chunk: StreamChunk =
                serde_json::from_str(&event.data).map_err(ClientError::Json)?;
            if let Some(usage) = chunk.usage {
                on_event(StreamEvent::Usage(usage)).map_err(ClientError::Output)?;
            }
            for choice in chunk.choices {
                let truncated = choice.finish_reason.as_deref() == Some("length");
                if let Some(content) = choice.delta.content
                    && !content.is_empty()
                {
                    on_event(StreamEvent::Text(&content)).map_err(ClientError::Output)?;
                    answer.push_str(&content);
                }
                if truncated {
                    return Err(ClientError::Truncated);
                }
            }
        }

        if saw_done {
            Ok(answer)
        } else {
            Err(ClientError::IncompleteStream)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SummaryResult {
    answer: String,
    usage: Option<TokenUsage>,
}

impl SummaryResult {
    pub fn answer(&self) -> &str {
        &self.answer
    }

    pub fn usage(&self) -> Option<TokenUsage> {
        self.usage
    }
}

struct RequestOptions<'a> {
    model: &'a str,
    temperature: f64,
    max_tokens: u32,
    thinking: Option<Thinking<'a>>,
    top_p: Option<f64>,
    stop: &'a [String],
}

async fn read_error_body(
    response: reqwest::Response,
    api_key: &str,
) -> Result<String, ClientError> {
    // Read enough overlap to recognize a key that starts just before the
    // visible limit. Redaction happens before the body is bounded.
    let read_limit = MAX_ERROR_BODY_BYTES.saturating_add(api_key.len().saturating_sub(1));
    let mut bytes = Vec::with_capacity(read_limit);
    let mut stream = response.bytes_stream();
    while bytes.len() < read_limit {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk.map_err(ClientError::Request)?;
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

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: &'a [Message],
    temperature: f64,
    max_tokens: u32,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<StreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f64>,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    stop: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<Thinking<'a>>,
}

#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

/// Provider-reported usage for one request; not an estimate from text length.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens_details: Option<CompletionTokenDetails>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct CompletionTokenDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
}

pub enum StreamEvent<'a> {
    Text(&'a str),
    Usage(TokenUsage),
}

#[derive(Serialize)]
struct Thinking<'a> {
    r#type: &'a str,
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
}

#[derive(Debug, Error)]
pub enum ClientError {
    /// Deterministic service calls retain provider usage even when completion fails.
    #[error("{source}")]
    WithUsage {
        #[source]
        source: Box<ClientError>,
        usage: TokenUsage,
    },
    #[error("ответ обрезан лимитом токенов; увеличьте max_tokens")]
    Truncated,
    #[error("модель вернула пустой ответ")]
    EmptyAnswer,
    #[error("failed to build HTTP client")]
    Build(#[source] reqwest::Error),
    #[error("failed to construct API endpoint")]
    Endpoint(#[source] url::ParseError),
    #[error("DeepSeek request failed")]
    Request(#[source] reqwest::Error),
    #[error("DeepSeek API returned HTTP {status}")]
    Api { status: StatusCode, body: String },
    #[error("DeepSeek stream failed: {0}")]
    Stream(String),
    #[error("invalid JSON in DeepSeek stream")]
    Json(#[source] serde_json::Error),
    #[error("failed to write streamed output: {0}")]
    Output(#[source] io::Error),
    #[error("DeepSeek stream closed before the [DONE] event")]
    IncompleteStream,
}

impl ClientError {
    pub fn usage(&self) -> Option<TokenUsage> {
        match self {
            Self::WithUsage { usage, .. } => Some(*usage),
            _ => None,
        }
    }

    pub fn operator_metadata(&self) -> ProviderErrorMetadata {
        let error = match self {
            Self::WithUsage { source, .. } => source.as_ref(),
            error => error,
        };
        match error {
            Self::Api { status, .. } => ProviderErrorMetadata {
                kind: "http",
                status: Some(status.as_u16()),
            },
            Self::Request(_) => ProviderErrorMetadata {
                kind: "request",
                status: None,
            },
            Self::Stream(_) => ProviderErrorMetadata {
                kind: "stream",
                status: None,
            },
            Self::Json(_) => ProviderErrorMetadata {
                kind: "invalid_stream",
                status: None,
            },
            Self::IncompleteStream => ProviderErrorMetadata {
                kind: "incomplete_stream",
                status: None,
            },
            Self::Truncated => ProviderErrorMetadata {
                kind: "truncated",
                status: None,
            },
            Self::EmptyAnswer => ProviderErrorMetadata {
                kind: "empty_response",
                status: None,
            },
            Self::Output(_) => ProviderErrorMetadata {
                kind: "output",
                status: None,
            },
            Self::Build(_) | Self::Endpoint(_) => ProviderErrorMetadata {
                kind: "configuration",
                status: None,
            },
            Self::WithUsage { .. } => unreachable!("nested usage error was unwrapped"),
        }
    }

    pub fn operator_message(&self, component: &str) -> String {
        let metadata = self.operator_metadata();
        match metadata.status {
            Some(status) => format!(
                "provider failure · component: {component} · kind: {} · status: {status}",
                metadata.kind
            ),
            None => format!(
                "provider failure · component: {component} · kind: {}",
                metadata.kind
            ),
        }
    }

    pub fn raw_diagnostic(&self) -> String {
        match self {
            Self::WithUsage { source, .. } => source.raw_diagnostic(),
            Self::Api { status, body } => {
                format!("DeepSeek API returned HTTP {status}: {body}")
            }
            _ => self.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderErrorMetadata {
    pub kind: &'static str,
    pub status: Option<u16>,
}
