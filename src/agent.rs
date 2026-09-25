use std::io;
use std::sync::Arc;

use serde::Serialize;
use thiserror::Error;

use crate::chat::{ChatHistory, Message, ProviderMessage, Role};
use crate::client::{ClientError, DeepSeekClient, StreamEvent, TokenUsage};
use crate::config::{Config, ContextConfig, ContextStrategy, WorkflowConfig};
use crate::context::{
    ContextState, ContextStats, ContextSummary, plan_compaction, prepare_request, stats,
};
use crate::debug_log::{DebugLog, RequestMetadata, WorkflowDebugEvent};
use crate::dialog::{BranchInfo, DialogStore, ForkResult, StoreError};
use crate::facts::{FactsState, parse_facts_json, plan_facts_update};
use crate::invariants::{InvariantRepository, InvariantRule, InvariantSet};
use crate::memory::{
    ContextError, ContextProvider, DurableMemoryScope, MemoryRepository, MemorySnapshot,
    RequestScope,
};
use crate::profile::{ProfileRepository, UserProfile};
use crate::system_context::{CompactionPolicy, ContextScope, SystemBlock, SystemBlockMetadata};
use crate::tool_audit::{
    ToolAuditError, ToolExecutionErrorCode, ToolExecutionFinish, ToolExecutionStart,
    ToolExecutionStatus,
};
use crate::tool_calling::{
    ConversationStep, ModelToolCall, ToolConversation, ToolExecutionError, ToolExecutor,
    ToolLoopError, ToolResultMessage,
};
use crate::workflow::{TaskPhase, TaskStatus, WorkflowTaskId, WorkflowTaskState};
use crate::workflow_engine::{
    RecoveredProcessing, WorkflowEngine, WorkflowEngineError, WorkflowModels, WorkflowSession,
};
use crate::workflow_model::DeepSeekCompletionModel;
use crate::workflow_store::{PauseOutcome, ProcessingStatus, WorkflowRepository};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowStatus {
    pub task_id: WorkflowTaskId,
    pub ordinal: u32,
    pub phase: TaskPhase,
    pub status: TaskStatus,
    pub goal: String,
    pub goal_revision: u32,
    pub goal_proposal_message_id: Option<i64>,
    pub plan_revision: u32,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub stage_sequence: u32,
    pub processing: Option<ProcessingStatus>,
}

#[derive(Serialize)]
pub struct AgentDebugSnapshot {
    scope: DebugScope,
    active_profile: DebugActiveProfile,
    workflow: Option<WorkflowTaskState>,
    processing: Option<ProcessingStatus>,
    context: ContextStats,
    invariants: Vec<InvariantRule>,
}

#[derive(Serialize)]
struct DebugScope {
    user_id: String,
    task_id: String,
    dialog_id: Option<i64>,
}

#[derive(Serialize)]
struct DebugActiveProfile {
    user_id: String,
    configured: bool,
    updated_at: Option<String>,
}

/// Finalize a dispatched call even when its owning async turn is dropped.
/// Keep the store borrowed so cleanup is synchronous and cannot outlive it.
struct ToolAuditGuard<'a> {
    store: &'a mut DialogStore,
    id: i64,
    cancellation: Option<ToolExecutionFinish>,
}

impl<'a> ToolAuditGuard<'a> {
    fn start(
        store: &'a mut DialogStore,
        start: ToolExecutionStart<'_>,
        read_only: bool,
    ) -> Result<Self, ToolAuditError> {
        let cancellation = if read_only {
            ToolExecutionFinish::failed(ToolExecutionErrorCode::new("cancelled")?)
        } else {
            ToolExecutionFinish::uncertain(ToolExecutionErrorCode::new("delivery_unknown")?)
        };
        let id = store.start_tool_execution(start)?;
        Ok(Self {
            store,
            id,
            cancellation: Some(cancellation),
        })
    }

    fn finish(&mut self, finish: ToolExecutionFinish) -> Result<(), ToolAuditError> {
        self.store.finish_tool_execution(self.id, finish)?;
        self.cancellation = None;
        Ok(())
    }
}

impl Drop for ToolAuditGuard<'_> {
    fn drop(&mut self) {
        if let Some(finish) = self.cancellation.take() {
            // Drop has no error channel: never panic or log database/remote data.
            let _ = self.store.finish_tool_execution(self.id, finish);
        }
    }
}

/// An API client and its independent conversation, optionally backed by SQLite.
pub struct Agent {
    client: DeepSeekClient,
    history: ChatHistory,
    persisted_message_count: usize,
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
    workflow_config: Option<WorkflowConfig>,
    workflow_models: Option<WorkflowModels>,
    requires_invariant_store: bool,
    tool_executor: Option<Arc<dyn ToolExecutor>>,
    max_tool_rounds: usize,
}

impl Agent {
    /// Start a persistent dialog lazily, when the first prompt is sent.
    pub fn with_store(config: &Config, store: DialogStore) -> Result<Self, ClientError> {
        Self::with_store_for_scope(config, store, RequestScope::default())
    }

    /// Start a fresh persistent dialog within the addressed user and task.
    pub fn with_store_for_scope(
        config: &Config,
        mut store: DialogStore,
        scope: RequestScope,
    ) -> Result<Self, ClientError> {
        let mut agent = Self::new(config)?;
        store.set_config_invariants(config.invariants().to_vec());
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
        agent.persisted_message_count = dialog.raw_message_count;
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

    pub fn profile(&self) -> Result<Option<UserProfile>, AgentError> {
        Ok(self
            .store
            .as_ref()
            .ok_or(AgentError::ProfileRequiresStore)?
            .load_profile(self.scope.user_id())?)
    }

    pub fn replace_profile(&mut self, markdown: &str) -> Result<(), AgentError> {
        self.store
            .as_mut()
            .ok_or(AgentError::ProfileRequiresStore)?
            .replace_profile(self.scope.user_id(), markdown)?;
        Ok(())
    }

    pub fn clear_profile(&mut self) -> Result<bool, AgentError> {
        Ok(self
            .store
            .as_mut()
            .ok_or(AgentError::ProfileRequiresStore)?
            .delete_profile(self.scope.user_id())?)
    }

    pub fn invariants(&self) -> Result<InvariantSet, AgentError> {
        Ok(self
            .store
            .as_ref()
            .ok_or(AgentError::InvariantRequiresStore)?
            .load_invariants(&self.scope)?)
    }

    pub fn upsert_invariant(&mut self, id: &str, text: &str) -> Result<(), AgentError> {
        self.store
            .as_mut()
            .ok_or(AgentError::InvariantRequiresStore)?
            .upsert_invariant(&self.scope, id, text)?;
        Ok(())
    }

    pub fn delete_invariant(&mut self, id: &str) -> Result<bool, AgentError> {
        Ok(self
            .store
            .as_mut()
            .ok_or(AgentError::InvariantRequiresStore)?
            .delete_invariant(&self.scope, id)?)
    }

    /// Statistics for the latest request, not a sum over the conversation.
    pub fn last_usage(&self) -> Option<TokenUsage> {
        self.last_usage
    }

    pub fn new(config: &Config) -> Result<Self, ClientError> {
        let client = DeepSeekClient::new(config)?;
        let adapter = |model: &str| {
            Arc::new(
                DeepSeekCompletionModel::new(client.clone(), model.to_owned())
                    .expect("workflow model names are validated by Config"),
            )
        };
        let workflow_models = WorkflowModels {
            interpreter: adapter(config.workflow().interpreter_model()),
            checker: adapter(config.workflow().checker_model()),
            handoff: adapter(config.workflow().handoff_model()),
        };
        Ok(Self {
            client,
            history: ChatHistory::new(config.system_prompt().to_owned()),
            persisted_message_count: 0,
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
            workflow_config: Some(config.workflow().clone()),
            workflow_models: Some(workflow_models),
            requires_invariant_store: !config.invariants().is_empty(),
            tool_executor: None,
            max_tool_rounds: config.mcp().max_tool_rounds as usize,
        })
    }

    /// Reuse a configured HTTP client while starting a fresh conversation.
    pub fn from_client(client: DeepSeekClient, system_prompt: &str) -> Self {
        Self {
            client,
            history: ChatHistory::new(system_prompt.to_owned()),
            persisted_message_count: 0,
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
            workflow_config: None,
            workflow_models: None,
            requires_invariant_store: false,
            tool_executor: None,
            max_tool_rounds: crate::tool_calling::DEFAULT_MAX_TOOL_ROUNDS,
        }
    }

    pub fn with_workflow_models(mut self, models: WorkflowModels) -> Self {
        self.workflow_models = Some(models);
        self
    }

    /// Enable tools for legacy turns; managed workflow turns keep their own pipeline.
    pub fn with_tool_executor(self, executor: Arc<dyn ToolExecutor>) -> Self {
        self.with_optional_tool_executor(Some(executor))
    }

    pub fn with_optional_tool_executor(mut self, executor: Option<Arc<dyn ToolExecutor>>) -> Self {
        self.tool_executor = executor;
        self
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
        on_event: F,
    ) -> Result<String, AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        if self.store.is_none() && self.requires_invariant_store {
            return Err(AgentError::InvariantRequiresStore);
        }
        if self.store.is_some()
            && !self
                .workflow_config
                .as_ref()
                .is_some_and(WorkflowConfig::enabled)
            && !self.invariants()?.is_empty()
        {
            return Err(AgentError::InvariantsRequireWorkflow);
        }
        if self.store.is_some()
            && self
                .workflow_config
                .as_ref()
                .is_some_and(WorkflowConfig::enabled)
        {
            self.run_workflow_streaming(prompt, on_event).await
        } else {
            self.run_legacy_streaming(prompt, on_event).await
        }
    }

    pub async fn recover_workflow_processing(
        &mut self,
    ) -> Result<Vec<RecoveredProcessing>, AgentError> {
        self.recover_workflow_processing_streaming(|_| Ok(())).await
    }

    pub async fn recover_workflow_processing_streaming<F>(
        &mut self,
        mut on_event: F,
    ) -> Result<Vec<RecoveredProcessing>, AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        let Some(config) = self
            .workflow_config
            .as_ref()
            .filter(|config| config.enabled())
            .cloned()
        else {
            return Ok(vec![]);
        };
        let (Some(dialog_id), Some(store)) = (self.dialog_id, self.store.as_mut()) else {
            return Ok(vec![]);
        };
        let models = self
            .workflow_models
            .as_ref()
            .ok_or(AgentError::WorkflowUnavailable)?
            .clone();
        let diagnostics_enabled = self.debug_log.is_active();
        let capture_payloads = self.debug_log.payloads_enabled();
        let mut debug_warnings = Vec::new();
        let result = {
            let mut engine = WorkflowEngine::new(
                &self.client,
                &self.context_config,
                &config,
                &models,
                WorkflowSession {
                    store,
                    dialog_id: &mut self.dialog_id,
                    scope: &mut self.scope,
                    history: &mut self.history,
                    persisted_message_count: &mut self.persisted_message_count,
                    last_usage: &mut self.last_usage,
                },
            );
            if diagnostics_enabled {
                let debug_log = &mut self.debug_log;
                let warnings = &mut debug_warnings;
                let mut log_diagnostic = move |event: WorkflowDebugEvent| {
                    if let Some(error) =
                        debug_log.log_workflow(&event.metadata, event.payload.as_ref())
                    {
                        warnings.push(error);
                    }
                    debug_log.payloads_enabled()
                };
                engine
                    .with_diagnostics(&mut log_diagnostic, capture_payloads)
                    .recover_pending_processing(dialog_id)
                    .await
            } else {
                engine.recover_pending_processing(dialog_id).await
            }
        };
        for error in debug_warnings {
            // Diagnostics are best-effort and must not change recovery state.
            let _ = emit_event(&mut on_event, AgentEvent::DebugLogFailed { error });
        }
        Ok(result?)
    }

    /// Persist a pause only for an already-selected active workflow task.
    /// This method never creates a dialog, task, stage, or protocol message.
    pub fn pause_current_workflow(&mut self) -> Result<PauseOutcome, AgentError> {
        if !self
            .workflow_config
            .as_ref()
            .is_some_and(WorkflowConfig::enabled)
        {
            return Ok(PauseOutcome::NoTask);
        }
        let (Some(dialog_id), Some(store)) = (self.dialog_id, self.store.as_mut()) else {
            return Ok(PauseOutcome::NoTask);
        };
        Ok(store.pause_current_task(dialog_id)?)
    }

    /// Reload the compact committed workflow projection without leasing,
    /// resuming, recovering, or otherwise mutating workflow state.
    pub fn workflow_status(&self) -> Result<Option<WorkflowStatus>, AgentError> {
        let (Some(dialog_id), Some(store)) = (self.dialog_id, self.store.as_ref()) else {
            return Ok(None);
        };
        let snapshot = store.load_workflow_status(dialog_id)?;
        Ok(snapshot.current_task.map(|task| WorkflowStatus {
            task_id: task.id,
            ordinal: task.ordinal,
            phase: task.phase,
            status: task.status,
            goal: task.goal,
            goal_revision: task.goal_revision,
            goal_proposal_message_id: task
                .goal_proposal
                .as_ref()
                .map(|proposal| proposal.assistant_message_id),
            plan_revision: task.plan.revision,
            current_step_id: task.current_step_id,
            expected_action: task.expected_action,
            stage_sequence: task.current_stage_sequence,
            processing: snapshot.processing,
        }))
    }

    /// Read-only diagnostic projection; does not include raw protocol or dialog messages.
    pub fn debug_snapshot(&self) -> Result<AgentDebugSnapshot, AgentError> {
        if self.store.is_none() && self.requires_invariant_store {
            return Err(AgentError::InvariantRequiresStore);
        }
        let (workflow, processing) = match (self.store.as_ref(), self.dialog_id) {
            (Some(store), Some(dialog_id)) => {
                let snapshot = store.load_workflow_status(dialog_id)?;
                (snapshot.current_task, snapshot.processing)
            }
            _ => (None, None),
        };
        let invariants = match self.store.as_ref() {
            Some(store) => store.load_invariants(&self.scope)?.rules().to_vec(),
            None => Vec::new(),
        };
        let profile = match self.store.as_ref() {
            Some(store) => store.load_profile(self.scope.user_id())?,
            None => None,
        };
        Ok(AgentDebugSnapshot {
            scope: DebugScope {
                user_id: self.scope.user_id().to_owned(),
                task_id: self.scope.task_id().to_owned(),
                dialog_id: self.dialog_id,
            },
            active_profile: DebugActiveProfile {
                user_id: self.scope.user_id().to_owned(),
                configured: profile.is_some(),
                updated_at: profile.map(|profile| profile.updated_at().to_owned()),
            },
            workflow,
            processing,
            context: self.context_stats()?,
            invariants,
        })
    }

    pub async fn run_workflow_streaming<F>(
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
        let config = self
            .workflow_config
            .as_ref()
            .filter(|config| config.enabled())
            .ok_or(AgentError::WorkflowUnavailable)?
            .clone();
        let models = self
            .workflow_models
            .as_ref()
            .ok_or(AgentError::WorkflowUnavailable)?
            .clone();
        let diagnostics_enabled = self.debug_log.is_active();
        let capture_payloads = self.debug_log.payloads_enabled();
        let mut debug_warnings = Vec::new();
        let result = {
            let store = self.store.as_mut().ok_or(AgentError::WorkflowUnavailable)?;
            let mut engine = WorkflowEngine::new(
                &self.client,
                &self.context_config,
                &config,
                &models,
                WorkflowSession {
                    store,
                    dialog_id: &mut self.dialog_id,
                    scope: &mut self.scope,
                    history: &mut self.history,
                    persisted_message_count: &mut self.persisted_message_count,
                    last_usage: &mut self.last_usage,
                },
            );
            if diagnostics_enabled {
                let debug_log = &mut self.debug_log;
                let warnings = &mut debug_warnings;
                let mut log_diagnostic = move |event: WorkflowDebugEvent| {
                    if let Some(error) =
                        debug_log.log_workflow(&event.metadata, event.payload.as_ref())
                    {
                        warnings.push(error);
                    }
                    debug_log.payloads_enabled()
                };
                engine
                    .with_diagnostics(&mut log_diagnostic, capture_payloads)
                    .run_human_input(prompt, &mut on_event)
                    .await
            } else {
                engine.run_human_input(prompt, &mut on_event).await
            }
        };
        for error in debug_warnings {
            // Logging and warning rendering are best-effort after state effects.
            let _ = emit_event(&mut on_event, AgentEvent::DebugLogFailed { error });
        }
        let result = result?;
        Ok(result.answer.unwrap_or_default())
    }

    async fn run_legacy_streaming<F>(
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
        let mut input_message_id = None;
        if let Some(store) = &mut self.store {
            match self.dialog_id {
                Some(id) => {
                    input_message_id = Some(store.append_message_with_id(
                        id,
                        self.persisted_message_count,
                        Role::User,
                        prompt,
                    )?);
                }
                None => {
                    let (id, message_id) = store.start_dialog_in_scope_with_message_id(
                        &self.scope,
                        self.history.system_prompt(),
                        prompt,
                    )?;
                    self.dialog_id = Some(id);
                    self.scope = self.scope.with_dialog_id(Some(id));
                    input_message_id = Some(message_id);
                }
            }
            self.persisted_message_count += 1;
        }
        let mut additional_blocks = if persistent {
            let context = (|| {
                let mut blocks = Vec::new();
                if let Some(profile) = self.profile()? {
                    blocks.extend(profile.blocks(&self.scope)?);
                }
                blocks.extend(self.memory_snapshot()?.blocks(&self.scope)?);
                Ok::<_, AgentError>(blocks)
            })();
            match context {
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
        let answer = if let Some(executor) = self.tool_executor.clone() {
            self.run_tool_conversation(
                prepared.messages(),
                input_message_id,
                executor.as_ref(),
                &mut on_event,
            )
            .await?
        } else {
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
            result?
        };
        let usage = self.last_usage;
        if answer.trim().is_empty() {
            return Err(ClientError::EmptyAnswer.into());
        }
        if let Some(store) = &mut self.store {
            let id = self.dialog_id.expect("persistent input created a dialog");
            store.append_answer(id, self.persisted_message_count, &answer, usage)?;
            self.persisted_message_count += 1;
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
        self.maybe_compact(&additional_blocks, usage, &mut on_event)
            .await?;
        Ok(answer)
    }

    async fn run_tool_conversation<F>(
        &mut self,
        messages: &[Message],
        input_message_id: Option<i64>,
        executor: &dyn ToolExecutor,
        on_event: &mut F,
    ) -> Result<String, AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        let mut conversation = ToolConversation::new(
            messages.iter().map(ProviderMessage::from).collect(),
            executor.definitions().to_vec(),
            self.max_tool_rounds,
        );
        loop {
            let turn = self
                .client
                .stream_assistant_turn(conversation.messages(), conversation.definitions())
                .await;
            let usage = match &turn {
                Ok(
                    crate::client::AssistantTurn::FinalText { usage, .. }
                    | crate::client::AssistantTurn::ToolCalls { usage, .. },
                ) => *usage,
                Err(error) => error.usage(),
            };
            if let Some(usage) = usage {
                self.last_usage = Some(add_tool_usage(self.last_usage.unwrap_or_default(), usage));
                emit_event(on_event, AgentEvent::Usage(self.last_usage.unwrap()))?;
            }
            match conversation.accept_assistant_turn(turn?)? {
                ConversationStep::Complete { content, .. } => {
                    emit_event(on_event, AgentEvent::Text(&content))?;
                    return Ok(content);
                }
                ConversationStep::Execute {
                    assistant_message,
                    calls,
                    ..
                } => {
                    let mut results = Vec::with_capacity(calls.len());
                    for call in calls {
                        results.push(
                            self.execute_tool(&call, input_message_id, executor, on_event)
                                .await?,
                        );
                    }
                    conversation.accept_tool_results(assistant_message, results)?;
                }
            }
        }
    }

    async fn execute_tool<F>(
        &mut self,
        call: &ModelToolCall,
        input_message_id: Option<i64>,
        executor: &dyn ToolExecutor,
        on_event: &mut F,
    ) -> Result<ToolResultMessage, AgentError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        // A failed output callback must stop before dispatching another tool.
        emit_event(
            on_event,
            AgentEvent::ToolStarted {
                call_id: &call.id,
                name: &call.name,
            },
        )?;
        let read_only = executor.is_read_only(&call.name) == Some(true);
        let mut audit = if let (Some(store), Some(input_message_id), Some(dialog_id)) =
            (self.store.as_mut(), input_message_id, self.dialog_id)
        {
            let (server_name, tool_name) = call.name.split_once("__").unwrap_or(("", &call.name));
            Some(ToolAuditGuard::start(
                store,
                ToolExecutionStart {
                    dialog_id,
                    input_message_id,
                    tool_call_id: &call.id,
                    server_name,
                    tool_name,
                    arguments_json: &call.arguments,
                },
                read_only,
            )?)
        } else {
            None
        };
        let result = executor.call(call).await;
        let uncertain = !read_only
            && (matches!(
                result,
                Err(ToolExecutionError::Timeout | ToolExecutionError::Transport)
            ) || result
                .as_ref()
                .is_ok_and(|output| output.delivery_uncertain));
        let code = if uncertain {
            Some("delivery_unknown")
        } else {
            match &result {
                Err(ToolExecutionError::UnknownTool) => Some("unknown_tool"),
                Err(ToolExecutionError::InvalidArguments) => Some("invalid_arguments"),
                Err(ToolExecutionError::Timeout) => Some("timeout"),
                Err(ToolExecutionError::Transport) => Some("transport"),
                Ok(output) if output.is_error || output.delivery_uncertain => {
                    Some(match output.error_code.as_deref() {
                        Some("unsupported_content") => "unsupported_content",
                        Some("mcp_tool_error") => "mcp_tool_error",
                        _ => "tool_error",
                    })
                }
                Ok(_) => None,
            }
        };
        let (status, finish) = match code {
            Some(code) if uncertain => (
                ToolExecutionStatus::Uncertain,
                ToolExecutionFinish::uncertain(ToolExecutionErrorCode::new(code)?),
            ),
            Some(code) => (
                ToolExecutionStatus::Failed,
                ToolExecutionFinish::failed(ToolExecutionErrorCode::new(code)?),
            ),
            None => (
                ToolExecutionStatus::Succeeded,
                ToolExecutionFinish::succeeded(),
            ),
        };
        if let Some(audit) = audit.as_mut() {
            audit.finish(finish)?;
        }
        emit_event(
            on_event,
            AgentEvent::ToolFinished {
                call_id: &call.id,
                name: &call.name,
                status,
                code,
            },
        )?;
        Ok(match (code, result) {
            (Some(code), _) => {
                ToolResultMessage::error(&call.id, serde_json::json!({"error":code}).to_string())
            }
            (None, Ok(output)) => ToolResultMessage::success(&call.id, output.content),
            (None, Err(_)) => unreachable!("executor errors always have a safe code"),
        })
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
        let fork = store.fork_dialog(id, self.persisted_message_count)?;
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
        self.persisted_message_count = dialog.raw_message_count;
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

    pub fn context_stats(&self) -> Result<ContextStats, AgentError> {
        let branch_group_id = self
            .branch_info
            .as_ref()
            .map(|branch| branch.branch_group_id);
        if self
            .workflow_config
            .as_ref()
            .is_some_and(WorkflowConfig::enabled)
            && let (Some(store), Some(dialog_id)) = (self.store.as_ref(), self.dialog_id)
            && let Some(snapshot) = store.load_workflow_context(dialog_id)?
        {
            let history = crate::workflow_context::stage_history(&snapshot.stage_messages);
            let mut result = stats(
                &history,
                &snapshot.reductions.context,
                &snapshot.reductions.facts,
                &self.context_config,
                self.dialog_id,
                branch_group_id,
            );
            result.stage_message_count = Some(history.messages().len());
            result.full_message_count = snapshot.transcript_message_count;
            return Ok(result);
        }
        Ok(stats(
            &self.history,
            &self.context_state,
            &self.facts_state,
            &self.context_config,
            self.dialog_id,
            branch_group_id,
        ))
    }

    /// Start a fresh conversation, retaining prompts and keeping old dialogs on disk.
    pub fn clear_history(&mut self) {
        self.history.clear();
        self.persisted_message_count = 0;
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
                let metadata = error.operator_metadata();
                let safe = error.operator_message("facts");
                let raw = self
                    .debug_log
                    .payloads_enabled()
                    .then(|| error.raw_diagnostic());
                if let Some(log_error) = self.debug_log.log_failure(
                    "facts_update_failed",
                    serde_json::json!({
                        "component": "facts",
                        "kind": metadata.kind,
                        "http_status": metadata.status,
                    }),
                    raw.as_deref(),
                ) {
                    emit_event(on_event, AgentEvent::DebugLogFailed { error: log_error })?;
                }
                emit_event(on_event, AgentEvent::FactsUpdateFailed { error: safe })?;
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
            store.replace_facts(id, self.persisted_message_count, facts, result.usage())?
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
        additional_system_blocks: &[SystemBlock],
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
            additional_system_blocks,
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
        let mut system_blocks = vec![SystemBlockMetadata {
            name: "summary_compactor".to_owned(),
            scope: ContextScope::Application,
            compaction: CompactionPolicy::Exclude,
        }];
        system_blocks.extend_from_slice(plan.system_block_metadata());
        let request_metadata = RequestMetadata::new(
            ContextStrategy::Summary,
            system_blocks,
            plan.request_messages().len(),
            plan.previous_boundary(),
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
                let metadata = error.operator_metadata();
                let safe = error.operator_message("compaction");
                let raw = self
                    .debug_log
                    .payloads_enabled()
                    .then(|| error.raw_diagnostic());
                if let Some(log_error) = self.debug_log.log_failure(
                    "compaction_failed",
                    serde_json::json!({
                        "component": "compaction",
                        "kind": metadata.kind,
                        "http_status": metadata.status,
                    }),
                    raw.as_deref(),
                ) {
                    emit_event(on_event, AgentEvent::DebugLogFailed { error: log_error })?;
                }
                emit_event(on_event, AgentEvent::CompactionFailed { error: safe })?;
                return Ok(());
            }
        };
        let summary = ContextSummary::new(result.answer().to_owned(), covered_message_count);
        self.context_state = if let Some(store) = &mut self.store {
            let id = self
                .dialog_id
                .expect("completed persistent turn has a dialog");
            store.replace_context(id, self.persisted_message_count, summary, result.usage())?
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

fn add_tool_usage(mut total: TokenUsage, incoming: TokenUsage) -> TokenUsage {
    total.prompt_tokens = total.prompt_tokens.saturating_add(incoming.prompt_tokens);
    total.completion_tokens = total
        .completion_tokens
        .saturating_add(incoming.completion_tokens);
    total.total_tokens = total.total_tokens.saturating_add(incoming.total_tokens);
    let prior = total
        .completion_tokens_details
        .and_then(|details| details.reasoning_tokens);
    let next = incoming
        .completion_tokens_details
        .and_then(|details| details.reasoning_tokens);
    if prior.is_some() || next.is_some() {
        total.completion_tokens_details = Some(crate::client::CompletionTokenDetails {
            reasoning_tokens: Some(
                prior
                    .unwrap_or_default()
                    .saturating_add(next.unwrap_or_default()),
            ),
        });
    }
    total
}

pub enum AgentEvent<'a> {
    ToolStarted {
        call_id: &'a str,
        name: &'a str,
    },
    ToolFinished {
        call_id: &'a str,
        name: &'a str,
        status: ToolExecutionStatus,
        code: Option<&'a str>,
    },
    Workflow(crate::workflow_engine::WorkflowTurnEvent),
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
    #[error("tool conversation failed")]
    ToolLoop(#[from] ToolLoopError),
    #[error("tool audit failed")]
    ToolAudit(#[from] ToolAuditError),
    #[error("workflow requires enabled configuration and a persistent store")]
    WorkflowUnavailable,
    #[error(transparent)]
    Workflow(#[from] WorkflowEngineError),
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
    #[error("user profiles require a persistent store")]
    ProfileRequiresStore,
    #[error("invariants require a persistent store")]
    InvariantRequiresStore,
    #[error("project invariants require workflow.enabled = true")]
    InvariantsRequireWorkflow,
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl AgentError {
    pub fn operator_message(&self) -> String {
        match self {
            Self::Workflow(error) => error.operator_message(),
            Self::Client(error) => error.operator_message("chat"),
            _ => self.to_string(),
        }
    }
}
