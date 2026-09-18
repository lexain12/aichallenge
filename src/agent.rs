use std::io;

use thiserror::Error;

use crate::chat::{ChatHistory, Message, Role};
use crate::client::{ClientError, DeepSeekClient, StreamEvent, TokenUsage};
use crate::config::{Config, ContextConfig, ContextStrategy};
use crate::context::{
    ContextState, ContextStats, ContextSummary, plan_compaction, prepare_request, stats,
};
use crate::debug_log::{DebugLog, RequestMetadata};
use crate::dialog::{BranchInfo, DialogStore, ForkResult, StoreError};
use crate::facts::{FactsState, parse_facts_json, plan_facts_update};
use crate::memory::{
    ContextError, ContextProvider, DurableMemoryScope, MemoryRepository, MemorySnapshot,
    RequestScope,
};
use crate::system_context::{CompactionPolicy, ContextScope, SystemBlockMetadata, SystemContext};

/// An API client and its independent conversation, optionally backed by SQLite.
pub struct Agent {
    client: DeepSeekClient,
    history: ChatHistory,
    prompt: Option<String>,
    store: Option<DialogStore>,
    dialog_id: Option<i64>,
    scope: RequestScope,
    last_usage: Option<TokenUsage>,
    context_config: ContextConfig,
    context_state: ContextState,
    facts_state: FactsState,
    branch_info: Option<BranchInfo>,
    debug_log: DebugLog,
}

impl Agent {
    /// Start a persistent dialog lazily, when the first prompt is sent.
    pub fn with_store(config: &Config, store: DialogStore) -> Result<Self, ClientError> {
        Self::with_store_for_scope(config, store, RequestScope::default())
    }

    /// Start a fresh persistent dialog within the addressed user and task.
    pub fn with_store_for_scope(
        config: &Config,
        store: DialogStore,
        scope: RequestScope,
    ) -> Result<Self, ClientError> {
        let mut agent = Self::new(config)?;
        agent.store = Some(store);
        agent.scope = scope.with_dialog_id(None);
        Ok(agent)
    }

    /// Restore the original system prompt and every saved message.
    /// API credentials and model settings come from the current configuration.
    pub fn from_dialog(config: &Config, store: DialogStore, id: i64) -> Result<Self, AgentError> {
        let dialog = store.load(id)?;
        let mut agent = Self::with_store(config, store)?;
        agent.last_usage = dialog.messages.last().and_then(|message| message.usage());
        agent.context_state = dialog.context;
        agent.facts_state = dialog.facts;
        agent.branch_info = dialog.branch;
        agent.history = ChatHistory::from_messages(dialog.system_prompt, dialog.messages);
        agent.dialog_id = Some(dialog.id);
        agent.scope = dialog.scope.with_dialog_id(Some(dialog.id));
        Ok(agent)
    }

    pub fn dialog_id(&self) -> Option<i64> {
        self.dialog_id
    }

    pub fn scope(&self) -> &RequestScope {
        &self.scope
    }

    pub fn remember(
        &mut self,
        layer: DurableMemoryScope,
        key: &str,
        value: &str,
    ) -> Result<(), AgentError> {
        self.store
            .as_mut()
            .ok_or(AgentError::MemoryRequiresStore)?
            .upsert_memory(&self.scope.address(layer), key, value)?;
        Ok(())
    }

    pub fn forget(&mut self, layer: DurableMemoryScope, key: &str) -> Result<bool, AgentError> {
        Ok(self
            .store
            .as_mut()
            .ok_or(AgentError::MemoryRequiresStore)?
            .delete_memory(&self.scope.address(layer), key)?)
    }

    pub fn memory_snapshot(&self) -> Result<MemorySnapshot, AgentError> {
        Ok(self
            .store
            .as_ref()
            .ok_or(AgentError::MemoryRequiresStore)?
            .load_memory(&self.scope)?)
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
            scope: RequestScope::default(),
            last_usage: None,
            context_config: config.context().clone(),
            context_state: ContextState::default(),
            facts_state: FactsState::default(),
            branch_info: None,
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
            scope: RequestScope::default(),
            last_usage: None,
            context_config: ContextConfig::full_history(),
            context_state: ContextState::default(),
            facts_state: FactsState::default(),
            branch_info: None,
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
        let persistent = self.store.is_some();
        if let Some(store) = &mut self.store {
            match self.dialog_id {
                Some(id) => {
                    store.append_message(id, self.history.messages().len(), Role::User, prompt)?
                }
                None => {
                    let id = store.start_dialog_in_scope(
                        &self.scope,
                        self.history.system_prompt(),
                        prompt,
                    )?;
                    self.dialog_id = Some(id);
                    self.scope = self.scope.with_dialog_id(Some(id));
                }
            }
        }
        let memory_blocks = if persistent {
            match self
                .memory_snapshot()
                .and_then(|snapshot| snapshot.blocks(&self.scope).map_err(AgentError::Context))
            {
                Ok(blocks) => blocks,
                Err(error) => {
                    self.history.push(Role::User, prompt.to_owned());
                    return Err(error);
                }
            }
        } else {
            Vec::new()
        };
        let mut candidate_messages = self.history.messages().to_vec();
        candidate_messages.push(Message::new(Role::User, prompt.to_owned()));
        let facts_result = self
            .maybe_update_facts(&candidate_messages, &mut on_event)
            .await;
        let candidate_facts = match facts_result {
            Ok(state) => state,
            Err(error) => {
                if persistent {
                    self.history.push(Role::User, prompt.to_owned());
                }
                return Err(error);
            }
        };
        let mut additional_blocks = memory_blocks;
        if self.context_config.strategy() == ContextStrategy::StickyFacts {
            additional_blocks.extend(candidate_facts.system_block());
        }
        let prepared = prepare_request(
            &self.history,
            &self.context_state,
            &self.context_config,
            prompt,
            &additional_blocks,
        );
        let boundary = prepared.summary_boundary();
        if persistent {
            self.history.push(Role::User, prompt.to_owned());
        }
        let facts_boundary = if self.context_config.strategy() == ContextStrategy::StickyFacts {
            candidate_facts.covered_message_count()
        } else {
            0
        };
        let request_metadata = RequestMetadata::new(
            self.context_config.strategy(),
            prepared.system_block_metadata(),
            prepared.selected_message_count(),
            boundary,
            facts_boundary,
        );
        if let Some(error) =
            self.debug_log
                .log_request("chat", prepared.messages(), &request_metadata)
        {
            emit_event(&mut on_event, AgentEvent::DebugLogFailed { error })?;
        }
        let mut usage = None;
        let result = self
            .client
            .stream_chat_events(prepared.messages(), |event| {
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
            self.facts_state = candidate_facts;
        }
        self.history.push_answer(answer.clone(), usage);
        if let Some(error) = self
            .debug_log
            .log_event("chat_completed", serde_json::json!({"usage": usage}))
        {
            emit_event(&mut on_event, AgentEvent::DebugLogFailed { error })?;
        }
        self.maybe_compact(prepared.system_context(), usage, &mut on_event)
            .await?;
        Ok(answer)
    }

    pub fn history(&self) -> &ChatHistory {
        &self.history
    }

    pub fn facts_state(&self) -> &FactsState {
        &self.facts_state
    }

    pub fn branch_info(&self) -> Option<&BranchInfo> {
        self.branch_info.as_ref()
    }

    pub fn branch_dialog(&mut self) -> Result<ForkResult, AgentError> {
        if self.context_config.strategy() != ContextStrategy::Branching {
            return Err(AgentError::BranchingStrategyRequired);
        }
        let id = self.dialog_id.ok_or(AgentError::NoPersistentDialog)?;
        let store = self.store.as_mut().ok_or(AgentError::NoPersistentDialog)?;
        let fork = store.fork_dialog(id, self.history.messages().len())?;
        self.branch_info = Some(BranchInfo {
            dialog_id: id,
            branch_group_id: fork.branch_group_id,
            parent_dialog_id: self
                .branch_info
                .as_ref()
                .and_then(|branch| branch.parent_dialog_id),
            checkpoint_message_count: self
                .branch_info
                .as_ref()
                .map_or(fork.checkpoint_message_count, |branch| {
                    branch.checkpoint_message_count
                }),
        });
        let _ = self.debug_log.log_event(
            "branch_created",
            serde_json::json!({
                "original_dialog_id": fork.original_dialog_id,
                "new_dialog_id": fork.new_dialog_id,
                "branch_group_id": fork.branch_group_id,
                "checkpoint_message_count": fork.checkpoint_message_count,
            }),
        );
        Ok(fork)
    }

    pub fn switch_branch(&mut self, target_id: i64) -> Result<(), AgentError> {
        if self.context_config.strategy() != ContextStrategy::Branching {
            return Err(AgentError::BranchingStrategyRequired);
        }
        let current_id = self.dialog_id.ok_or(AgentError::NoPersistentDialog)?;
        let store = self.store.as_ref().ok_or(AgentError::NoPersistentDialog)?;
        let dialog = store.load_branch_member(current_id, target_id)?;
        let last_usage = dialog.messages.last().and_then(|message| message.usage());
        self.history = ChatHistory::from_messages(dialog.system_prompt, dialog.messages);
        self.context_state = dialog.context;
        self.facts_state = dialog.facts;
        self.branch_info = dialog.branch;
        self.dialog_id = Some(dialog.id);
        self.scope = dialog.scope.with_dialog_id(Some(dialog.id));
        self.last_usage = last_usage;
        let _ = self.debug_log.log_event(
            "branch_switched",
            serde_json::json!({
                "dialog_id": target_id,
                "branch_group_id": self
                    .branch_info
                    .as_ref()
                    .map(|branch| branch.branch_group_id),
            }),
        );
        Ok(())
    }

    pub fn context_stats(&self) -> ContextStats {
        stats(
            &self.history,
            &self.context_state,
            &self.facts_state,
            &self.context_config,
            self.dialog_id,
            self.branch_info
                .as_ref()
                .map(|branch| branch.branch_group_id),
        )
    }

    /// Start a fresh conversation, retaining prompts and keeping old dialogs on disk.
    pub fn clear_history(&mut self) {
        self.history.clear();
        self.dialog_id = None;
        self.scope = self.scope.with_dialog_id(None);
        self.last_usage = None;
        self.context_state = ContextState::default();
        self.facts_state = FactsState::default();
        self.branch_info = None;
    }

    async fn maybe_update_facts<F>(
        &mut self,
        candidate_messages: &[Message],
        on_event: &mut F,
    ) -> Result<FactsState, AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        if self.context_config.strategy() != ContextStrategy::StickyFacts {
            return Ok(self.facts_state.clone());
        }
        let Some(plan) = plan_facts_update(candidate_messages, &self.facts_state) else {
            return Ok(self.facts_state.clone());
        };
        emit_event(
            on_event,
            AgentEvent::FactsUpdateStarted {
                previous_boundary: self.facts_state.covered_message_count(),
                target_boundary: plan.covered_message_count(),
            },
        )?;
        if let Some(error) = self.debug_log.log_event(
            "facts_update_started",
            serde_json::json!({
                "previous_boundary": self.facts_state.covered_message_count(),
                "target_boundary": plan.covered_message_count(),
            }),
        ) {
            emit_event(on_event, AgentEvent::DebugLogFailed { error })?;
        }
        let request_metadata = RequestMetadata::new(
            ContextStrategy::StickyFacts,
            vec![SystemBlockMetadata {
                name: "facts_updater".to_owned(),
                scope: ContextScope::Application,
                compaction: CompactionPolicy::Exclude,
            }],
            candidate_messages
                .len()
                .saturating_sub(self.facts_state.covered_message_count()),
            0,
            self.facts_state.covered_message_count(),
        );
        if let Some(error) =
            self.debug_log
                .log_request("facts_update", plan.request_messages(), &request_metadata)
        {
            emit_event(on_event, AgentEvent::DebugLogFailed { error })?;
        }
        let result = match self
            .client
            .update_facts(
                plan.request_messages(),
                self.context_config.facts_max_tokens(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let error = error.to_string();
                if let Some(log_error) = self
                    .debug_log
                    .log_event("facts_update_failed", serde_json::json!({"error": error}))
                {
                    emit_event(on_event, AgentEvent::DebugLogFailed { error: log_error })?;
                }
                emit_event(on_event, AgentEvent::FactsUpdateFailed { error })?;
                return Ok(self.facts_state.clone());
            }
        };
        let facts = match parse_facts_json(result.answer()) {
            Ok(facts) => facts,
            Err(error) => {
                let error = error.to_string();
                if let Some(log_error) = self
                    .debug_log
                    .log_event("facts_update_failed", serde_json::json!({"error": error}))
                {
                    emit_event(on_event, AgentEvent::DebugLogFailed { error: log_error })?;
                }
                emit_event(on_event, AgentEvent::FactsUpdateFailed { error })?;
                return Ok(self.facts_state.clone());
            }
        };
        let covered_message_count = plan.covered_message_count();
        let state = if let Some(store) = &mut self.store {
            let id = self
                .dialog_id
                .expect("persistent input created a dialog before facts update");
            store.replace_facts(id, candidate_messages.len(), facts, result.usage())?
        } else {
            self.facts_state
                .clone()
                .updated(facts, covered_message_count, result.usage())
        };
        if self.store.is_some() {
            self.facts_state = state.clone();
        }
        if let Some(error) = self.debug_log.log_event(
            "facts_update_completed",
            serde_json::json!({
                "covered_message_count": covered_message_count,
                "usage": result.usage(),
            }),
        ) {
            emit_event(on_event, AgentEvent::DebugLogFailed { error })?;
        }
        emit_event(
            on_event,
            AgentEvent::FactsUpdateCompleted {
                covered_message_count,
                usage: result.usage(),
            },
        )?;
        Ok(state)
    }

    async fn maybe_compact<F>(
        &mut self,
        system_context: &SystemContext,
        usage: Option<TokenUsage>,
        on_event: &mut F,
    ) -> Result<(), AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        let Some(usage) = usage else {
            return Ok(());
        };
        if self.context_config.strategy() != ContextStrategy::Summary
            || usage.prompt_tokens < self.context_config.compact_after_prompt_tokens()
        {
            return Ok(());
        }
        let Some(plan) = plan_compaction(
            &self.history,
            &self.context_state,
            self.context_config.keep_last_messages(),
            system_context,
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
        let mut system_blocks = vec![SystemBlockMetadata {
            name: "summary_compactor".to_owned(),
            scope: ContextScope::Application,
            compaction: CompactionPolicy::Exclude,
        }];
        system_blocks.extend(
            system_context
                .compaction_blocks()
                .into_iter()
                .map(|block| block.metadata()),
        );
        let request_metadata = RequestMetadata::new(
            ContextStrategy::Summary,
            system_blocks,
            plan.request_messages().len(),
            previous_boundary,
            0,
        );
        if let Some(error) =
            self.debug_log
                .log_request("compaction", plan.request_messages(), &request_metadata)
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
    FactsUpdateStarted {
        previous_boundary: usize,
        target_boundary: usize,
    },
    FactsUpdateCompleted {
        covered_message_count: usize,
        usage: Option<TokenUsage>,
    },
    FactsUpdateFailed {
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
    #[error("branch commands require context strategy = branching")]
    BranchingStrategyRequired,
    #[error("branch commands require a persisted active dialog")]
    NoPersistentDialog,
    #[error("durable memory requires a persistent store")]
    MemoryRequiresStore,
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Store(#[from] StoreError),
}
