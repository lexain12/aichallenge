use std::io;

use thiserror::Error;

use crate::chat::ChatHistory;
use crate::client::{ClientError, DeepSeekClient, StreamEvent};
use crate::config::Config;

/// An API client and its independent, in-memory conversation.
/// Only complete, nonempty answers are committed to history.
pub struct Agent {
    client: DeepSeekClient,
    history: ChatHistory,
    prompt: Option<String>,
}

impl Agent {
    pub fn new(config: &Config) -> Result<Self, ClientError> {
        Ok(Self::from_client(
            DeepSeekClient::new(config)?,
            config.system_prompt(),
        ))
    }

    /// Reuse a configured HTTP client while starting a fresh conversation.
    pub fn from_client(client: DeepSeekClient, system_prompt: &str) -> Self {
        Self {
            client,
            history: ChatHistory::new(system_prompt.to_owned()),
            prompt: None,
        }
    }

    /// Set the default user prompt for `run`; this is separate from the system prompt.
    pub fn with_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt = Some(prompt.into());
        self
    }

    /// Send the default prompt as a new turn, including existing history.
    pub async fn run(&mut self) -> Result<String, AgentError> {
        let prompt = self.prompt.clone().ok_or(AgentError::MissingPrompt)?;
        self.run_with_prompt(&prompt).await
    }

    /// Send a new user message without replacing the default prompt.
    pub async fn run_with_prompt(&mut self, prompt: &str) -> Result<String, AgentError> {
        self.run_streaming(prompt, |_| Ok(())).await
    }

    /// Forward text and usage events to the caller, then commit the completed turn.
    /// Errors (including output errors) and cancellation leave history unchanged.
    pub async fn run_streaming<F>(
        &mut self,
        prompt: &str,
        on_event: F,
    ) -> Result<String, AgentError>
    where
        F: FnMut(StreamEvent<'_>) -> io::Result<()>,
    {
        if prompt.trim().is_empty() {
            return Err(AgentError::EmptyPrompt);
        }
        let request = self.history.request_messages(prompt);
        let answer = self.client.stream_chat_events(&request, on_event).await?;
        if answer.trim().is_empty() {
            return Err(ClientError::EmptyAnswer.into());
        }
        self.history.commit_turn(prompt.to_owned(), answer.clone());
        Ok(answer)
    }

    pub fn history(&self) -> &ChatHistory {
        &self.history
    }

    /// Clear dialog turns while retaining the system and default user prompts.
    pub fn clear_history(&mut self) {
        self.history.clear();
    }
}

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("no default prompt configured; use with_prompt or run_with_prompt")]
    MissingPrompt,
    #[error("prompt must not be empty")]
    EmptyPrompt,
    #[error(transparent)]
    Client(#[from] ClientError),
}
