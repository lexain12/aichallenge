use std::io;

use thiserror::Error;

use crate::chat::{ChatHistory, Role};
use crate::client::{ClientError, DeepSeekClient, StreamEvent, TokenUsage};
use crate::config::{Config, ContextConfig};
use crate::context::{
    ContextState, ContextStats, ContextSummary, build_request_messages, plan_compaction, stats,
};
use crate::debug_log::DebugLog;
use crate::dialog::{DialogStore, StoreError};

/// An API client and its independent conversation, optionally backed by SQLite.
pub struct Agent {
    client: DeepSeekClient,
    history: ChatHistory,
    prompt: Option<String>,
    store: Option<DialogStore>,
    dialog_id: Option<i64>,
    last_usage: Option<TokenUsage>,
    context_config: ContextConfig,
    context_state: ContextState,
    debug_log: DebugLog,
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
        agent.last_usage = dialog.messages.last().and_then(|message| message.usage());
        agent.context_state = dialog.context;
        agent.history = ChatHistory::from_messages(dialog.system_prompt, dialog.messages);
        agent.dialog_id = Some(dialog.id);
        Ok(agent)
    }

    pub fn dialog_id(&self) -> Option<i64> {
        self.dialog_id
    }

    /// Statistics for the latest request, not a sum over the conversation.
    pub fn last_usage(&self) -> Option<TokenUsage> {
        self.last_usage
    }

    pub fn new(config: &Config) -> Result<Self, ClientError> {
        Ok(Self {
            client: DeepSeekClient::new(config)?,
            history: ChatHistory::new(config.system_prompt().to_owned()),
            prompt: None,
            store: None,
            dialog_id: None,
            last_usage: None,
            context_config: config.context().clone(),
            context_state: ContextState::default(),
            debug_log: DebugLog::from_config(config.debug(), config.api_key()),
        })
    }

    /// Reuse a configured HTTP client while starting a fresh conversation.
    pub fn from_client(client: DeepSeekClient, system_prompt: &str) -> Self {
        Self {
            client,
            history: ChatHistory::new(system_prompt.to_owned()),
            prompt: None,
            store: None,
            dialog_id: None,
            last_usage: None,
            context_config: ContextConfig::disabled(),
            context_state: ContextState::default(),
            debug_log: DebugLog::new(None, false, ""),
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

    /// Forward agent events, saving each complete message separately.
    /// Persistent agents commit input before HTTP starts; it survives request
    /// errors and cancellation. In-memory agents commit only successful pairs.
    pub async fn run_streaming<F>(
        &mut self,
        prompt: &str,
        mut on_event: F,
    ) -> Result<String, AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        self.last_usage = None;
        if prompt.trim().is_empty() {
            return Err(AgentError::EmptyPrompt);
        }
        let request = build_request_messages(
            &self.history,
            &self.context_state,
            self.context_config.enabled(),
            self.context_config.keep_last_messages(),
            prompt,
        );
        let boundary = self.context_stats().covered_message_count;
        if let Some(error) = self.debug_log.log_request("chat", &request, boundary) {
            emit_event(&mut on_event, AgentEvent::DebugLogFailed { error })?;
        }
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
        let mut usage = None;
        let result = self
            .client
            .stream_chat_events(&request, |event| {
                if let StreamEvent::Usage(value) = &event {
                    usage = Some(*value);
                }
                match event {
                    StreamEvent::Text(text) => on_event(AgentEvent::Text(text)),
                    StreamEvent::Usage(value) => on_event(AgentEvent::Usage(value)),
                }
            })
            .await;
        self.last_usage = usage;
        let answer = result?;
        if answer.trim().is_empty() {
            return Err(ClientError::EmptyAnswer.into());
        }
        if let Some(store) = &mut self.store {
            let id = self.dialog_id.expect("persistent input created a dialog");
            store.append_answer(id, self.history.messages().len(), &answer, usage)?;
        } else {
            self.history.push(Role::User, prompt.to_owned());
        }
        self.history.push_answer(answer.clone(), usage);
        if let Some(error) = self
            .debug_log
            .log_event("chat_completed", serde_json::json!({"usage": usage}))
        {
            emit_event(&mut on_event, AgentEvent::DebugLogFailed { error })?;
        }
        self.maybe_compact(usage, &mut on_event).await?;
        Ok(answer)
    }

    pub fn history(&self) -> &ChatHistory {
        &self.history
    }

    pub fn context_stats(&self) -> ContextStats {
        stats(
            &self.history,
            &self.context_state,
            self.context_config.enabled(),
            self.context_config.keep_last_messages(),
        )
    }

    /// Start a fresh conversation, retaining prompts and keeping old dialogs on disk.
    pub fn clear_history(&mut self) {
        self.history.clear();
        self.dialog_id = None;
        self.last_usage = None;
        self.context_state = ContextState::default();
    }

    async fn maybe_compact<F>(
        &mut self,
        usage: Option<TokenUsage>,
        on_event: &mut F,
    ) -> Result<(), AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        let Some(usage) = usage else {
            return Ok(());
        };
        if !self.context_config.enabled()
            || usage.prompt_tokens < self.context_config.compact_after_prompt_tokens()
        {
            return Ok(());
        }
        let Some(plan) = plan_compaction(
            &self.history,
            &self.context_state,
            self.context_config.keep_last_messages(),
        ) else {
            return Ok(());
        };
        let covered_message_count = plan.covered_message_count();
        let kept_message_count = self.history.messages().len() - covered_message_count;
        emit_event(
            on_event,
            AgentEvent::CompactionStarted {
                threshold: self.context_config.compact_after_prompt_tokens(),
                covered_message_count,
                kept_message_count,
            },
        )?;
        if let Some(error) = self.debug_log.log_event(
            "compaction_started",
            serde_json::json!({
                "threshold": self.context_config.compact_after_prompt_tokens(),
                "covered_message_count": covered_message_count,
                "kept_message_count": kept_message_count,
                "new_message_count": plan.new_message_count(),
            }),
        ) {
            emit_event(on_event, AgentEvent::DebugLogFailed { error })?;
        }
        let previous_boundary = self.context_stats().covered_message_count;
        if let Some(error) =
            self.debug_log
                .log_request("compaction", plan.request_messages(), previous_boundary)
        {
            emit_event(on_event, AgentEvent::DebugLogFailed { error })?;
        }

        let result = match self
            .client
            .summarize(
                plan.request_messages(),
                self.context_config.summary_max_tokens(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let error = error.to_string();
                if let Some(log_error) = self
                    .debug_log
                    .log_event("compaction_failed", serde_json::json!({"error": error}))
                {
                    emit_event(on_event, AgentEvent::DebugLogFailed { error: log_error })?;
                }
                emit_event(on_event, AgentEvent::CompactionFailed { error })?;
                return Ok(());
            }
        };
        let summary = ContextSummary::new(result.answer().to_owned(), covered_message_count);
        self.context_state = if let Some(store) = &mut self.store {
            let id = self
                .dialog_id
                .expect("completed persistent turn has a dialog");
            store.replace_context(id, self.history.messages().len(), summary, result.usage())?
        } else {
            let mut state = self.context_state.clone();
            state.replace_summary(summary, result.usage());
            state
        };
        if let Some(error) = self.debug_log.log_event(
            "compaction_completed",
            serde_json::json!({
                "covered_message_count": covered_message_count,
                "usage": result.usage(),
            }),
        ) {
            emit_event(on_event, AgentEvent::DebugLogFailed { error })?;
        }
        emit_event(
            on_event,
            AgentEvent::CompactionCompleted {
                covered_message_count,
                usage: result.usage(),
            },
        )?;
        Ok(())
    }
}

fn emit_event<F>(on_event: &mut F, event: AgentEvent<'_>) -> Result<(), AgentError>
where
    F: FnMut(AgentEvent<'_>) -> io::Result<()>,
{
    on_event(event).map_err(ClientError::Output)?;
    Ok(())
}

pub enum AgentEvent<'a> {
    Text(&'a str),
    Usage(TokenUsage),
    CompactionStarted {
        threshold: u64,
        covered_message_count: usize,
        kept_message_count: usize,
    },
    CompactionCompleted {
        covered_message_count: usize,
        usage: Option<TokenUsage>,
    },
    CompactionFailed {
        error: String,
    },
    DebugLogFailed {
        error: String,
    },
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
