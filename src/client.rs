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

/// Direct HTTP client for DeepSeek Chat Completions.
pub struct DeepSeekClient {
    http: reqwest::Client,
    endpoint: Url,
    api_key: String,
    model: String,
    temperature: f64,
    max_tokens: u32,
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
        })
    }

    pub async fn stream_chat<F>(
        &self,
        messages: &[Message],
        mut on_text: F,
    ) -> Result<String, ClientError>
    where
        F: FnMut(&str) -> io::Result<()>,
    {
        let response = self
            .http
            .post(self.endpoint.clone())
            .bearer_auth(&self.api_key)
            .json(&ChatRequest {
                model: &self.model,
                messages,
                temperature: self.temperature,
                max_tokens: self.max_tokens,
                stream: true,
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
            for choice in chunk.choices {
                if let Some(content) = choice.delta.content
                    && !content.is_empty()
                {
                    on_text(&content).map_err(ClientError::Output)?;
                    answer.push_str(&content);
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

async fn read_error_body(
    response: reqwest::Response,
    api_key: &str,
) -> Result<String, ClientError> {
    let mut bytes = Vec::with_capacity(MAX_ERROR_BODY_BYTES);
    let mut stream = response.bytes_stream();
    while bytes.len() < MAX_ERROR_BODY_BYTES {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk.map_err(ClientError::Request)?;
        let remaining = MAX_ERROR_BODY_BYTES - bytes.len();
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }

    let body = String::from_utf8_lossy(&bytes);
    Ok(body.replace(api_key, "[REDACTED]"))
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: &'a [Message],
    temperature: f64,
    max_tokens: u32,
    stream: bool,
}

#[derive(Deserialize)]
struct StreamChunk {
    #[serde(default)]
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    delta: Delta,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<String>,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to build HTTP client")]
    Build(#[source] reqwest::Error),
    #[error("failed to construct API endpoint")]
    Endpoint(#[source] url::ParseError),
    #[error("DeepSeek request failed")]
    Request(#[source] reqwest::Error),
    #[error("DeepSeek API returned HTTP {status}: {body}")]
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
