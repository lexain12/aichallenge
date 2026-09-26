//! Bounded full-history turns shared by interactive and scheduled execution.

use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

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
        self.run_job_until(job_id, cancellation, Instant::now() + self.timeout)
            .await
    }

    pub async fn run_job_until(
        &self,
        job_id: JobId,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<CronRunOutcome, CronRunError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(CronRunError::TimedOut);
        }
        // Keep a meaningful part of even very short budgets for the durable
        // terminal write. Production timeouts cap this reservation at 100ms.
        let reserve = std::cmp::min(Duration::from_millis(100), remaining / 2);
        let execution_deadline = deadline.checked_sub(reserve).unwrap_or(deadline);
        if cancellation.is_cancelled() {
            return Err(CronRunError::Interrupted);
        }
        let claim_cancellation = CancellationToken::new();
        let claim_store = self.store.clone();
        let claim_clock = self.clock.clone();
        let blocking_cancellation = claim_cancellation.clone();
        let claim_task = tokio::task::spawn_blocking(move || {
            claim_store.claim_run_with_deadline(
                job_id,
                execution_deadline,
                &blocking_cancellation,
                || claim_clock.now(),
            )
        });
        tokio::pin!(claim_task);
        let claim = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                claim_cancellation.cancel();
                self.finish_late_claim(
                    claim_task.await,
                    CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                    deadline,
                ).await?;
                return Err(CronRunError::Interrupted);
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(execution_deadline)) => {
                claim_cancellation.cancel();
                self.finish_late_claim(
                    claim_task.await,
                    CronRunFinish::timed_out(),
                    deadline,
                ).await?;
                return Err(CronRunError::TimedOut);
            }
            result = &mut claim_task => result
                .map_err(|_| CronRunError::Store)?
                .map_err(|_| {
                    if Instant::now() >= execution_deadline {
                        CronRunError::TimedOut
                    } else {
                        CronRunError::Store
                    }
                })?,
        };
        let claim = match claim {
            RunClaim::Inactive => return Ok(CronRunOutcome::Inactive),
            RunClaim::Skipped(run) => return Ok(CronRunOutcome::Skipped(run.id)),
            RunClaim::Claimed(claim) => claim,
        };

        let run_id = claim.run.id;

        // The claim transaction has already disabled a once-at job and made
        // its desired state pending. Removing the stale line is best effort;
        // execution remains at-most-once even when crontab is unavailable.
        if matches!(claim.job.schedule, ScheduleSpec::OnceAt { .. })
            && let Some(reconciler) = &self.reconciler
        {
            let reconcile_time = self.clock.now();
            let _ = reconciler
                .reconcile(reconcile_time, execution_deadline, cancellation.clone())
                .await;
            if cancellation.is_cancelled() {
                self.finish_run_bounded(
                    run_id,
                    CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                    finalization_deadline(deadline),
                    &CancellationToken::new(),
                )
                .await?;
                return Err(CronRunError::Interrupted);
            }
            if Instant::now() >= execution_deadline {
                self.finish_run_bounded(
                    run_id,
                    CronRunFinish::timed_out(),
                    deadline,
                    &CancellationToken::new(),
                )
                .await?;
                return Err(CronRunError::TimedOut);
            }
        }

        let run_cancellation = CancellationToken::new();
        let mut sink = |_event| Ok(());
        let execution = self.runner.run_until(
            AgentInput {
                owner: ToolOwner::CronRun(run_id),
                system_prompt: self.system_prompt.clone(),
                history: Vec::new(),
                prompt: claim.job.prompt,
            },
            run_cancellation.clone(),
            &mut sink,
            execution_deadline,
            deadline,
        );
        tokio::pin!(execution);
        let deadline_sleep =
            tokio::time::sleep_until(tokio::time::Instant::from_std(execution_deadline));
        tokio::pin!(deadline_sleep);

        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                run_cancellation.cancel();
                let _ = execution.await;
                self.finish_run_bounded(
                    run_id,
                    CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                    finalization_deadline(deadline),
                    &CancellationToken::new(),
                ).await?;
                return Err(CronRunError::Interrupted);
            }
            _ = &mut deadline_sleep => {
                run_cancellation.cancel();
                let _ = execution.await;
                self.finish_run_bounded(
                    run_id,
                    CronRunFinish::timed_out(),
                    deadline,
                    &CancellationToken::new(),
                ).await?;
                return Err(CronRunError::TimedOut);
            }
            result = &mut execution => result,
        };

        match result {
            Ok(outcome) => {
                let finished = self
                    .finish_run_bounded(
                        run_id,
                        CronRunFinish::completed(outcome.answer),
                        deadline,
                        &cancellation,
                    )
                    .await;
                if finished == Err(CronRunError::Interrupted) {
                    let _ = self
                        .finish_run_bounded(
                            run_id,
                            CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                            finalization_deadline(deadline),
                            &CancellationToken::new(),
                        )
                        .await;
                    return Err(CronRunError::Interrupted);
                }
                finished?;
                Ok(CronRunOutcome::Completed(run_id))
            }
            Err(AgentError::Interrupted) => {
                self.finish_run_bounded(
                    run_id,
                    CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                    finalization_deadline(deadline),
                    &CancellationToken::new(),
                )
                .await?;
                Err(CronRunError::Interrupted)
            }
            Err(error) => {
                let (run_error, code) = cron_error(error);
                let finished = self
                    .finish_run_bounded(
                        run_id,
                        CronRunFinish::failed(code),
                        deadline,
                        &cancellation,
                    )
                    .await;
                if finished == Err(CronRunError::Interrupted) {
                    let _ = self
                        .finish_run_bounded(
                            run_id,
                            CronRunFinish::interrupted(SafeErrorCode::Interrupted),
                            finalization_deadline(deadline),
                            &CancellationToken::new(),
                        )
                        .await;
                    return Err(CronRunError::Interrupted);
                }
                finished?;
                Err(run_error)
            }
        }
    }

    async fn finish_run_bounded(
        &self,
        run_id: RunId,
        finish: CronRunFinish,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), CronRunError> {
        let store = self.store.clone();
        let worker_cancellation = CancellationToken::new();
        let blocking_cancellation = worker_cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            store.finish_run_with_deadline(run_id, finish, deadline, &blocking_cancellation)
        });
        tokio::pin!(worker);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                worker_cancellation.cancel();
                match worker.await {
                    Ok(Ok(())) => Ok(()),
                    _ => Err(CronRunError::Interrupted),
                }
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                worker_cancellation.cancel();
                let _ = worker.await;
                Err(CronRunError::Store)
            }
            result = &mut worker => result
                .map_err(|_| CronRunError::Store)?
                .map_err(|_| CronRunError::Store),
        }
    }

    async fn finish_late_claim(
        &self,
        result: Result<Result<RunClaim, StoreError>, tokio::task::JoinError>,
        finish: CronRunFinish,
        deadline: Instant,
    ) -> Result<(), CronRunError> {
        if let Ok(Ok(RunClaim::Claimed(claim))) = result {
            self.finish_run_bounded(
                claim.run.id,
                finish,
                finalization_deadline(deadline),
                &CancellationToken::new(),
            )
            .await?;
        }
        Ok(())
    }
}

fn finalization_deadline(deadline: Instant) -> Instant {
    deadline.min(Instant::now() + Duration::from_millis(100))
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
        self.run_inner(input, cancellation, event_sink, None, None)
            .await
    }

    async fn run_until(
        &self,
        input: AgentInput,
        cancellation: CancellationToken,
        event_sink: &mut AgentEventSink<'_>,
        execution_deadline: Instant,
        finalization_deadline: Instant,
    ) -> Result<AgentOutcome, AgentError> {
        self.run_inner(
            input,
            cancellation,
            event_sink,
            Some(execution_deadline),
            Some(finalization_deadline),
        )
        .await
    }

    async fn run_inner(
        &self,
        input: AgentInput,
        cancellation: CancellationToken,
        event_sink: &mut AgentEventSink<'_>,
        execution_deadline: Option<Instant>,
        finalization_deadline: Option<Instant>,
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
                        .execute_tools(
                            input.owner,
                            calls,
                            &cancellation,
                            event_sink,
                            execution_deadline,
                            finalization_deadline,
                        )
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
        execution_deadline: Option<Instant>,
        finalization_deadline: Option<Instant>,
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
            let audit_id = self
                .start_tool_audit(
                    ToolRunStart {
                        owner,
                        call_id: call.id.clone(),
                        server_name: route.server_name.to_owned(),
                        tool_name: route.tool_name.to_owned(),
                        read_only,
                    },
                    cancellation,
                    execution_deadline,
                    finalization_deadline,
                )
                .await?;
            if event_sink(AgentEvent::ToolStarted {
                name: call.name.clone(),
            })
            .is_err()
            {
                self.finish_tool_audit(
                    audit_id,
                    ToolRunFinish::failed(SafeErrorCode::InternalError),
                    &CancellationToken::new(),
                    finalization_deadline,
                )
                .await?;
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
                    self.finish_tool_audit(
                        audit_id,
                        finish,
                        &CancellationToken::new(),
                        bounded_audit_cleanup_deadline(finalization_deadline),
                    ).await?;
                    return Err(AgentError::Interrupted);
                }
                result = self.tools.call(&call) => result,
            };
            let (result, finish, event_code) = classify_tool_result(execution, read_only, &call.id);
            let finish_result = self
                .finish_tool_audit(
                    audit_id,
                    finish.clone(),
                    cancellation,
                    finalization_deadline,
                )
                .await;
            if matches!(finish_result, Err(AgentError::Interrupted)) && cancellation.is_cancelled()
            {
                // A signal may arrive after the tool returns while SQLite is
                // persisting its audit. Stop and join that worker, then make
                // one short shielded terminal attempt before owner recovery.
                self.finish_tool_audit(
                    audit_id,
                    finish,
                    &CancellationToken::new(),
                    bounded_audit_cleanup_deadline(finalization_deadline),
                )
                .await?;
                return Err(AgentError::Interrupted);
            }
            finish_result?;
            if cancellation.is_cancelled() {
                return Err(AgentError::Interrupted);
            }
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

    async fn start_tool_audit(
        &self,
        start: ToolRunStart,
        cancellation: &CancellationToken,
        execution_deadline: Option<Instant>,
        finalization_deadline: Option<Instant>,
    ) -> Result<i64, AgentError> {
        let deadline =
            execution_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(5));
        let store = self.store.clone();
        let worker_cancellation = CancellationToken::new();
        let blocking_cancellation = worker_cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            store.start_tool_run_with_deadline(start, deadline, &blocking_cancellation)
        });
        tokio::pin!(worker);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                worker_cancellation.cancel();
                if let Ok(Ok(id)) = worker.await {
                    let finish = ToolRunFinish::failed(SafeErrorCode::Interrupted);
                    self.finish_tool_audit(
                        id,
                        finish,
                        &CancellationToken::new(),
                        bounded_audit_cleanup_deadline(finalization_deadline),
                    ).await?;
                }
                Err(AgentError::Interrupted)
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                worker_cancellation.cancel();
                if let Ok(Ok(id)) = worker.await {
                    // Dispatch has not happened yet, so even a write is known
                    // not to have been delivered and is safe to mark failed.
                    let finish = ToolRunFinish::failed(SafeErrorCode::TimedOut);
                    self.finish_tool_audit(
                        id,
                        finish,
                        &CancellationToken::new(),
                        finalization_deadline,
                    ).await?;
                }
                Err(AgentError::Store(StoreError::Busy))
            }
            result = &mut worker => result.map_err(|_| AgentError::Store(StoreError::Database))?.map_err(AgentError::Store),
        }
    }

    async fn finish_tool_audit(
        &self,
        id: i64,
        finish: ToolRunFinish,
        cancellation: &CancellationToken,
        deadline: Option<Instant>,
    ) -> Result<(), AgentError> {
        let deadline = deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(5));
        let store = self.store.clone();
        let worker_cancellation = CancellationToken::new();
        let blocking_cancellation = worker_cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            store.finish_tool_run_with_deadline(id, finish, deadline, &blocking_cancellation)
        });
        tokio::pin!(worker);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                worker_cancellation.cancel();
                match worker.await {
                    Ok(Ok(())) => Ok(()),
                    _ => Err(AgentError::Interrupted),
                }
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                worker_cancellation.cancel();
                match worker.await {
                    Ok(Ok(())) => Ok(()),
                    _ => Err(AgentError::Store(StoreError::Busy)),
                }
            }
            result = &mut worker => result.map_err(|_| AgentError::Store(StoreError::Database))?.map_err(AgentError::Store),
        }
    }
}

fn bounded_audit_cleanup_deadline(deadline: Option<Instant>) -> Option<Instant> {
    let bounded = Instant::now() + Duration::from_millis(100);
    Some(deadline.map_or(bounded, |deadline| deadline.min(bounded)))
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
