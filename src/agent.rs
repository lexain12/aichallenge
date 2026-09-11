use std::io;

use thiserror::Error;

use crate::chat::{ChatHistory, Role};
use crate::client::{ClientError, DeepSeekClient, StreamEvent};
use crate::config::Config;
use crate::dialog::{DialogStore, StoreError};

/// An API client and its independent conversation, optionally backed by SQLite.
pub struct Agent {
    client: DeepSeekClient,
    history: ChatHistory,
    prompt: Option<String>,
    store: Option<DialogStore>,
    dialog_id: Option<i64>,
}

impl Agent {
    /// Start a persistent dialog lazily, when the first prompt is sent.
    pub fn with_store(config: &Config, store: DialogStore) -> Result<Self, ClientError> {
        let mut agent = Self::new(config)?;
        agent.store = Some(store);
        Ok(agent)
    }

    /// Restore the original system prompt and every saved message.
    /// API credentials and model settings come from the current configuration.
    pub fn from_dialog(config: &Config, store: DialogStore, id: i64) -> Result<Self, AgentError> {
        let dialog = store.load(id)?;
        let mut agent = Self::with_store(config, store)?;
        agent.history = ChatHistory::from_messages(dialog.system_prompt, dialog.messages);
        agent.dialog_id = Some(dialog.id);
        Ok(agent)
    }

    pub fn dialog_id(&self) -> Option<i64> {
        self.dialog_id
    }

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
            store: None,
            dialog_id: None,
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

    /// Forward text and usage events, saving each complete message separately.
    /// Persistent agents commit input before HTTP starts; it survives request
    /// errors and cancellation. In-memory agents commit only successful pairs.
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
        if let Some(store) = &mut self.store {
            match self.dialog_id {
                Some(id) => {
                    store.append_message(id, self.history.messages().len(), Role::User, prompt)?
                }
                None => {
                    self.dialog_id = Some(store.start_dialog(self.history.system_prompt(), prompt)?)
                }
            }
            self.history.push(Role::User, prompt.to_owned());
        }
        let answer = self.client.stream_chat_events(&request, on_event).await?;
        if answer.trim().is_empty() {
            return Err(ClientError::EmptyAnswer.into());
        }
        if let Some(store) = &mut self.store {
            let id = self.dialog_id.expect("persistent input created a dialog");
            store.append_message(id, self.history.messages().len(), Role::Assistant, &answer)?;
            self.history.push(Role::Assistant, answer.clone());
        } else {
            self.history.commit_turn(prompt.to_owned(), answer.clone());
        }
        Ok(answer)
    }

    pub fn history(&self) -> &ChatHistory {
        &self.history
    }

    /// Start a fresh conversation, retaining prompts and keeping old dialogs on disk.
    pub fn clear_history(&mut self) {
        self.history.clear();
        self.dialog_id = None;
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
    #[error(transparent)]
    Store(#[from] StoreError),
}
