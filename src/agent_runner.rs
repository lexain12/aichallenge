//! Bounded full-history turns shared by interactive and scheduled execution.

use std::{io, sync::Arc, time::Duration};

use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{DialogId, JobId, RunId, ToolOwner},
    provider::{ModelToolCall, Provider, ProviderMessage, TokenUsage},
    scheduler::{CronClock, CronRunReconciler, ScheduleSpec, SystemCronClock},
    store::{
        CronRunFinish, MessageRole, RunClaim, SafeErrorCode, Store, StoreError, StoredMessage,
        ToolRunFinish, ToolRunStart,
    },
    tools::{
        ConversationStep, ToolConversation, ToolExecutionError, ToolExecutionResult, ToolExecutor,
        ToolLoopError, ToolResultMessage,
    },
};

#[derive(Clone, Debug)]
pub struct AgentInput {
    pub owner: ToolOwner,
    pub system_prompt: String,
    pub history: Vec<StoredMessage>,
    pub prompt: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CronRunOutcome {
    Inactive,
    Skipped(RunId),
    Completed(RunId),
}

/// Safe cron-run failures. These values are suitable for stderr and never
/// contain provider diagnostics, prompts, tool arguments, paths, or secrets.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CronRunError {
    #[error("store_error")]
    Store,
    #[error("provider_error")]
    Provider,
    #[error("context_too_long")]
    ContextTooLong,
    #[error("tool_error")]
    Tool,
    #[error("tool_round_limit")]
    ToolRoundLimit,
    #[error("interrupted")]
    Interrupted,
    #[error("timed_out")]
    TimedOut,
    #[error("internal_error")]
    Internal,
}

pub struct CronAgentService {
    store: Store,
    runner: AgentRunner,
    system_prompt: String,
    timeout: Duration,
    reconciler: Option<Arc<dyn CronRunReconciler>>,
    clock: Arc<dyn CronClock>,
}

impl CronAgentService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Store,
        provider: Arc<dyn Provider>,
        mcp: Arc<dyn ToolExecutor>,
        system_prompt: impl Into<String>,
        max_tool_rounds: usize,
        max_provider_request_bytes: usize,
        max_message_bytes: usize,
        timeout: Duration,
        reconciler: Option<Arc<dyn CronRunReconciler>>,
    ) -> Self {
        let runner = AgentRunner::new(
            provider,
            mcp,
            store.clone(),
            max_tool_rounds,
            max_provider_request_bytes,
            max_message_bytes,
        );
        Self {
            store,
            runner,
            system_prompt: system_prompt.into(),
            timeout,
            reconciler,
            clock: Arc::new(SystemCronClock),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn CronClock>) -> Self {
        self.clock = clock;
        self
    }

    pub async fn run_job(
        &self,
        job_id: JobId,
        cancellation: CancellationToken,
    ) -> Result<CronRunOutcome, CronRunError> {
        if cancellation.is_cancelled() {
            return Err(CronRunError::Interrupted);
        }
        let now = self.clock.now();
        let claim = self
            .store
            .claim_run(job_id, now)
            .map_err(|_| CronRunError::Store)?;
        let claim = match claim {
            RunClaim::Inactive => return Ok(CronRunOutcome::Inactive),
            RunClaim::Skipped(run) => return Ok(CronRunOutcome::Skipped(run.id)),
            RunClaim::Claimed(claim) => claim,
        };

        let run_id = claim.run.id;
        let deadline = tokio::time::Instant::now() + self.timeout;

        // The claim transaction has already disabled a once-at job and made
        // its desired state pending. Removing the stale line is best effort;
        // execution remains at-most-once even when crontab is unavailable.
        if matches!(claim.job.schedule, ScheduleSpec::OnceAt { .. })
            && let Some(reconciler) = &self.reconciler
        {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.store.finish_run(
                        run_id,
                        CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                    ).map_err(|_| CronRunError::Store)?;
                    return Err(CronRunError::Interrupted);
                }
                _ = tokio::time::sleep_until(deadline) => {
                    self.store.finish_run(run_id, CronRunFinish::timed_out())
                        .map_err(|_| CronRunError::Store)?;
                    return Err(CronRunError::TimedOut);
                }
                _ = reconciler.reconcile(now) => {}
            }
        }

        let run_cancellation = CancellationToken::new();
        let mut sink = |_event| Ok(());
        let execution = self.runner.run(
            AgentInput {
                owner: ToolOwner::CronRun(run_id),
                system_prompt: self.system_prompt.clone(),
                history: Vec::new(),
                prompt: claim.job.prompt,
            },
            run_cancellation.clone(),
            &mut sink,
        );
        tokio::pin!(execution);
        let deadline_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(deadline_sleep);

        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                run_cancellation.cancel();
                let _ = execution.await;
                self.store.finish_run(
                    run_id,
                    CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                ).map_err(|_| CronRunError::Store)?;
                return Err(CronRunError::Interrupted);
            }
            _ = &mut deadline_sleep => {
                run_cancellation.cancel();
                let _ = execution.await;
                self.store.finish_run(run_id, CronRunFinish::timed_out())
                    .map_err(|_| CronRunError::Store)?;
                return Err(CronRunError::TimedOut);
            }
            result = &mut execution => result,
        };

        match result {
            Ok(outcome) => {
                self.store
                    .finish_run(run_id, CronRunFinish::completed(outcome.answer))
                    .map_err(|_| CronRunError::Store)?;
                Ok(CronRunOutcome::Completed(run_id))
            }
            Err(AgentError::Interrupted) => {
                self.store
                    .finish_run(
                        run_id,
                        CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                    )
                    .map_err(|_| CronRunError::Store)?;
                Err(CronRunError::Interrupted)
            }
            Err(error) => {
                let (run_error, code) = cron_error(error);
                self.store
                    .finish_run(run_id, CronRunFinish::failed(code))
                    .map_err(|_| CronRunError::Store)?;
                Err(run_error)
            }
        }
    }
}

fn cron_error(error: AgentError) -> (CronRunError, SafeErrorCode) {
    match error {
        AgentError::Provider => (CronRunError::Provider, SafeErrorCode::ProviderError),
        AgentError::ContextTooLong => (CronRunError::ContextTooLong, SafeErrorCode::ContextTooLong),
        AgentError::Tool => (CronRunError::Tool, SafeErrorCode::ToolError),
        AgentError::ToolRoundLimit => (CronRunError::ToolRoundLimit, SafeErrorCode::ToolRoundLimit),
        AgentError::Interrupted => (CronRunError::Interrupted, SafeErrorCode::Interrupted),
        AgentError::Store(_) | AgentError::ContentTooLong | AgentError::Output => {
            (CronRunError::Internal, SafeErrorCode::InternalError)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolEventCode {
    Completed,
    Failed,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentEvent {
    TextDelta { text: String },
    ToolStarted { name: String },
    ToolFinished { name: String, code: ToolEventCode },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentOutcome {
    pub answer: String,
    pub usage: TokenUsage,
}

pub type AgentEventSink<'a> = dyn FnMut(AgentEvent) -> io::Result<()> + Send + 'a;

/// All variants and their formatting are deliberately bounded and contain no
/// provider body, tool arguments, tool result, URL, prompt, or credential.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AgentError {
    #[error("store_error")]
    Store(StoreError),
    #[error("provider_error")]
    Provider,
    #[error("context_too_long")]
    ContextTooLong,
    #[error("content_too_long")]
    ContentTooLong,
    #[error("tool_error")]
    Tool,
    #[error("tool_round_limit")]
    ToolRoundLimit,
    #[error("interrupted")]
    Interrupted,
    #[error("output_error")]
    Output,
}

impl AgentError {
    pub fn recommends_new_dialog(self) -> bool {
        self == Self::ContextTooLong
    }

    fn persisted_code(self) -> SafeErrorCode {
        match self {
            Self::Provider => SafeErrorCode::ProviderError,
            Self::ContextTooLong => SafeErrorCode::ContextTooLong,
            Self::Tool | Self::ToolRoundLimit => {
                if self == Self::ToolRoundLimit {
                    SafeErrorCode::ToolRoundLimit
                } else {
                    SafeErrorCode::ToolError
                }
            }
            Self::Interrupted => SafeErrorCode::Interrupted,
            Self::Store(_) | Self::ContentTooLong | Self::Output => SafeErrorCode::InternalError,
        }
    }
}

impl From<StoreError> for AgentError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

pub struct AgentRunner {
    provider: Arc<dyn Provider>,
    tools: Arc<dyn ToolExecutor>,
    store: Store,
    max_tool_rounds: usize,
    max_provider_request_bytes: usize,
    max_message_bytes: usize,
}

impl AgentRunner {
    pub fn new(
        provider: Arc<dyn Provider>,
        tools: Arc<dyn ToolExecutor>,
        store: Store,
        max_tool_rounds: usize,
        max_provider_request_bytes: usize,
        max_message_bytes: usize,
    ) -> Self {
        Self {
            provider,
            tools,
            store,
            max_tool_rounds,
            max_provider_request_bytes,
            max_message_bytes,
        }
    }

    pub async fn run(
        &self,
        input: AgentInput,
        cancellation: CancellationToken,
        event_sink: &mut AgentEventSink<'_>,
    ) -> Result<AgentOutcome, AgentError> {
        if input.prompt.len() > self.max_message_bytes {
            return Err(AgentError::ContentTooLong);
        }
        let mut messages = Vec::with_capacity(input.history.len() + 2);
        messages.push(ProviderMessage::system(input.system_prompt));
        messages.extend(input.history.into_iter().map(|message| match message.role {
            MessageRole::User => ProviderMessage::user(message.content),
            MessageRole::Assistant => ProviderMessage::assistant(message.content),
        }));
        messages.push(ProviderMessage::user(input.prompt));
        let mut conversation = ToolConversation::new(
            messages,
            self.tools.definitions().to_vec(),
            self.max_tool_rounds,
        );

        loop {
            if cancellation.is_cancelled() {
                return Err(AgentError::Interrupted);
            }
            self.enforce_request_limit(&conversation)?;

            let mut streamed_bytes = 0usize;
            let mut streamed_too_long = false;
            let turn_result = {
                let mut text_sink = |text: &str| {
                    streamed_bytes = streamed_bytes.saturating_add(text.len());
                    if streamed_bytes > self.max_message_bytes {
                        streamed_too_long = true;
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "content_too_long",
                        ));
                    }
                    event_sink(AgentEvent::TextDelta {
                        text: text.to_owned(),
                    })
                };
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(AgentError::Interrupted),
                    result = self.provider.stream_turn(
                        conversation.messages(),
                        conversation.definitions(),
                        &mut text_sink,
                    ) => result,
                }
            };
            if streamed_too_long {
                return Err(AgentError::ContentTooLong);
            }
            let turn = turn_result.map_err(map_provider_error)?;
            let step = conversation
                .accept_assistant_turn(turn)
                .map_err(map_tool_loop_error)?;
            match step {
                ConversationStep::Complete { content, usage } => {
                    if content.len() > self.max_message_bytes {
                        return Err(AgentError::ContentTooLong);
                    }
                    return Ok(AgentOutcome {
                        answer: content,
                        usage,
                    });
                }
                ConversationStep::Execute {
                    assistant_message,
                    calls,
                    ..
                } => {
                    let results = self
                        .execute_tools(input.owner, calls, &cancellation, event_sink)
                        .await?;
                    conversation
                        .accept_tool_results(assistant_message, results)
                        .map_err(map_tool_loop_error)?;
                }
            }
        }
    }

    fn enforce_request_limit(&self, conversation: &ToolConversation) -> Result<(), AgentError> {
        let size = self
            .provider
            .serialized_request_len(conversation.messages(), conversation.definitions())
            .map_err(map_provider_error)?;
        if size > self.max_provider_request_bytes {
            Err(AgentError::ContextTooLong)
        } else {
            Ok(())
        }
    }

    async fn execute_tools(
        &self,
        owner: ToolOwner,
        calls: Vec<ModelToolCall>,
        cancellation: &CancellationToken,
        event_sink: &mut AgentEventSink<'_>,
    ) -> Result<Vec<ToolResultMessage>, AgentError> {
        let mut results = Vec::with_capacity(calls.len());
        for call in calls {
            if cancellation.is_cancelled() {
                return Err(AgentError::Interrupted);
            }
            let route = self.tools.route(&call.name).ok_or(AgentError::Tool)?;
            let read_only = self
                .tools
                .is_read_only(&call.name)
                .ok_or(AgentError::Tool)?;
            let audit_id = self.store.start_tool_run(ToolRunStart {
                owner,
                call_id: call.id.clone(),
                server_name: route.server_name.to_owned(),
                tool_name: route.tool_name.to_owned(),
                read_only,
            })?;
            if event_sink(AgentEvent::ToolStarted {
                name: call.name.clone(),
            })
            .is_err()
            {
                self.store.finish_tool_run(
                    audit_id,
                    ToolRunFinish::failed(SafeErrorCode::InternalError),
                )?;
                return Err(AgentError::Output);
            }

            let execution = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    let finish = if read_only {
                        ToolRunFinish::failed(SafeErrorCode::Interrupted)
                    } else {
                        ToolRunFinish::uncertain(SafeErrorCode::Interrupted)
                    };
                    self.store.finish_tool_run(audit_id, finish)?;
                    return Err(AgentError::Interrupted);
                }
                result = self.tools.call(&call) => result,
            };
            let (result, finish, event_code) = classify_tool_result(execution, read_only, &call.id);
            self.store.finish_tool_run(audit_id, finish)?;
            if event_sink(AgentEvent::ToolFinished {
                name: call.name,
                code: event_code,
            })
            .is_err()
            {
                return Err(AgentError::Output);
            }
            results.push(result);
        }
        Ok(results)
    }
}

pub struct InteractiveService {
    store: Store,
    runner: AgentRunner,
    system_prompt: String,
}

impl InteractiveService {
    pub fn new(store: Store, runner: AgentRunner, system_prompt: impl Into<String>) -> Self {
        Self {
            store,
            runner,
            system_prompt: system_prompt.into(),
        }
    }

    pub async fn send_message(
        &self,
        dialog_id: DialogId,
        content: &str,
        cancellation: CancellationToken,
        event_sink: &mut AgentEventSink<'_>,
    ) -> Result<AgentOutcome, AgentError> {
        let turn = self.store.begin_turn(dialog_id, content)?;
        let history = match self.store.completed_messages(dialog_id) {
            Ok(history) => history,
            Err(error) => {
                let original = AgentError::Store(error);
                self.finish_failed(turn.turn_id, original)?;
                return Err(original);
            }
        };
        let result = self
            .runner
            .run(
                AgentInput {
                    owner: ToolOwner::InteractiveTurn(turn.turn_id),
                    system_prompt: self.system_prompt.clone(),
                    history,
                    prompt: content.to_owned(),
                },
                cancellation,
                event_sink,
            )
            .await;
        match result {
            Ok(outcome) => {
                if let Err(error) = self.store.complete_turn(turn.turn_id, &outcome.answer) {
                    let original = AgentError::Store(error);
                    self.finish_failed(turn.turn_id, original)?;
                    return Err(original);
                }
                Ok(outcome)
            }
            Err(error) => {
                self.finish_failed(turn.turn_id, error)?;
                Err(error)
            }
        }
    }

    fn finish_failed(
        &self,
        turn_id: crate::domain::TurnId,
        error: AgentError,
    ) -> Result<(), AgentError> {
        if error == AgentError::Interrupted {
            self.store
                .interrupt_turn(turn_id, SafeErrorCode::Interrupted)?;
        } else {
            self.store.fail_turn(turn_id, error.persisted_code())?;
        }
        Ok(())
    }
}

fn map_provider_error(error: crate::provider::ProviderError) -> AgentError {
    match error.safe_code() {
        "output" => AgentError::Output,
        "context_length" | "context_too_long" => AgentError::ContextTooLong,
        _ => AgentError::Provider,
    }
}

fn map_tool_loop_error(error: ToolLoopError) -> AgentError {
    match error {
        ToolLoopError::RoundLimitExceeded => AgentError::ToolRoundLimit,
        _ => AgentError::Tool,
    }
}

fn classify_tool_result(
    result: Result<ToolExecutionResult, ToolExecutionError>,
    read_only: bool,
    call_id: &str,
) -> (ToolResultMessage, ToolRunFinish, ToolEventCode) {
    match result {
        Ok(result) if !result.is_error => (
            ToolResultMessage::success(call_id, result.content),
            ToolRunFinish::completed(),
            ToolEventCode::Completed,
        ),
        Ok(result) => {
            let code = allowlisted_tool_code(result.error_code.as_deref());
            let uncertain = result.delivery_uncertain && !read_only;
            let finish = if uncertain {
                ToolRunFinish::uncertain(SafeErrorCode::ToolError)
            } else {
                ToolRunFinish::failed(SafeErrorCode::ToolError)
            };
            (
                ToolResultMessage::error(call_id, json_error(code)),
                finish,
                if uncertain {
                    ToolEventCode::Uncertain
                } else {
                    ToolEventCode::Failed
                },
            )
        }
        Err(error) => {
            let safe_code = match error {
                ToolExecutionError::Timeout => "timed_out",
                ToolExecutionError::UnknownTool
                | ToolExecutionError::InvalidArguments
                | ToolExecutionError::Transport => "tool_error",
            };
            let persisted = if error == ToolExecutionError::Timeout {
                SafeErrorCode::TimedOut
            } else {
                SafeErrorCode::ToolError
            };
            let uncertain = !read_only
                && matches!(
                    error,
                    ToolExecutionError::Timeout | ToolExecutionError::Transport
                );
            (
                ToolResultMessage::error(call_id, json_error(safe_code)),
                if uncertain {
                    ToolRunFinish::uncertain(persisted)
                } else {
                    ToolRunFinish::failed(persisted)
                },
                if uncertain {
                    ToolEventCode::Uncertain
                } else {
                    ToolEventCode::Failed
                },
            )
        }
    }
}

fn allowlisted_tool_code(code: Option<&str>) -> &'static str {
    match code {
        Some("chat_not_found") => "chat_not_found",
        Some("delivery_unknown") => "delivery_unknown",
        Some("rate_limited") => "rate_limited",
        Some("telegram_unauthorized") => "telegram_unauthorized",
        Some("unsupported_content") => "unsupported_content",
        Some("mcp_tool_error") => "mcp_tool_error",
        _ => "tool_error",
    }
}

fn json_error(code: &str) -> String {
    format!(r#"{{"error":"{code}"}}"#)
}
