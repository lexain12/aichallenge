//! One authenticated stdio session. All stdout bytes are versioned NDJSON.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Write},
    sync::{Arc, Mutex},
};

use base64::Engine as _;
use chrono::Utc;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    agent_runner::{AgentError, AgentEvent, AgentInput, AgentRunner, ToolEventCode},
    domain::{ConfirmationId, DialogId, RequestId, ToolOwner},
    inspection::{
        InspectQuery, InspectionCancellation, InspectionError, InspectionService, LogicalExportV1,
    },
    protocol::{
        ConfirmationAction, ConfirmationScheduleKind, EXPORT_CHUNK_BYTES, InspectKind,
        NdjsonReader, NdjsonWriter, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode,
        RequestEnvelope, ScheduleConfirmationPreview, ServerEnvelope, ServerEvent,
    },
    provider::Provider,
    scheduler::CronSynchronizer,
    settings::ServerSettings,
    store::{SafeErrorCode, Store, StoreError},
    tools::{
        CompositeToolExecutor, ToolExecutor,
        scheduler::{
            ConfirmationBroker, ConfirmationClock, ConfirmationError, ConfirmationFuture,
            ConfirmationRequest, ScheduleAction, SchedulePreview, SchedulerToolExecutor,
        },
        time::TimeToolExecutor,
    },
};

const EVENT_QUEUE: usize = 64;
const MAX_ACTIVE_TURNS: usize = 8;
const MAX_BACKGROUND_REQUESTS: usize = 8;
const FRAGMENT_BYTES: usize = 64 * 1024;
const USED_CONFIRMATIONS: usize = 1024;
const TERMINAL_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Clone)]
pub struct ServerDependencies {
    pub settings: Arc<ServerSettings>,
    pub store: Store,
    pub provider: Arc<dyn Provider>,
    pub mcp: Arc<dyn ToolExecutor>,
    pub synchronizer: Arc<CronSynchronizer>,
    pub inspection: InspectionService,
}

#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum ServerError {
    #[error("protocol_error")]
    Protocol,
    #[error("store_error")]
    Store,
    #[error("inspection_error")]
    Inspection,
    #[error("tool_catalog_error")]
    ToolCatalog,
}

impl From<ProtocolError> for ServerError {
    fn from(_: ProtocolError) -> Self {
        Self::Protocol
    }
}
impl From<StoreError> for ServerError {
    fn from(_: StoreError) -> Self {
        Self::Store
    }
}
impl From<InspectionError> for ServerError {
    fn from(_: InspectionError) -> Self {
        Self::Inspection
    }
}

struct PendingConfirmation {
    request_id: RequestId,
    action_hash: [u8; 32],
    expires_at: chrono::DateTime<Utc>,
    answer: oneshot::Sender<ConfirmationResolution>,
}

struct ConfirmationResolution {
    accepted: bool,
    request_id: RequestId,
    action_hash: [u8; 32],
}

#[derive(Clone)]
struct ConfirmationPrompt {
    request_id: RequestId,
    request: ConfirmationRequest,
}

struct ConfirmationState {
    pending: HashMap<ConfirmationId, PendingConfirmation>,
    used: HashSet<ConfirmationId>,
    used_order: VecDeque<ConfirmationId>,
}

/// A broker is created per stdio session and never shared with another client.
pub struct SessionConfirmationBroker {
    state: Mutex<ConfirmationState>,
    prompts: mpsc::Sender<ConfirmationPrompt>,
    clock: Arc<dyn ConfirmationClock>,
    timeout: std::time::Duration,
}

struct UtcClock;
impl ConfirmationClock for UtcClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        Utc::now()
    }
}

impl SessionConfirmationBroker {
    #[cfg(test)]
    fn pair_with_clock(
        clock: Arc<dyn ConfirmationClock>,
    ) -> (Arc<Self>, mpsc::Receiver<ConfirmationPrompt>) {
        Self::pair_with_clock_and_timeout(clock, std::time::Duration::from_secs(5 * 60))
    }

    fn pair_with_clock_and_timeout(
        clock: Arc<dyn ConfirmationClock>,
        timeout: std::time::Duration,
    ) -> (Arc<Self>, mpsc::Receiver<ConfirmationPrompt>) {
        let (prompts, receiver) = mpsc::channel(EVENT_QUEUE);
        (
            Arc::new(Self {
                state: Mutex::new(ConfirmationState {
                    pending: HashMap::new(),
                    used: HashSet::new(),
                    used_order: VecDeque::new(),
                }),
                prompts,
                clock,
                timeout,
            }),
            receiver,
        )
    }

    pub fn resolve(
        &self,
        request_id: RequestId,
        confirmation_id: ConfirmationId,
        accepted: bool,
    ) -> Result<(), ConfirmationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ConfirmationError::Unavailable)?;
        if state.used.contains(&confirmation_id) {
            return Err(ConfirmationError::AlreadyUsed);
        }
        let pending = state
            .pending
            .get(&confirmation_id)
            .ok_or(ConfirmationError::WrongSession)?;
        if pending.request_id != request_id {
            return Err(ConfirmationError::WrongRequest);
        }
        if self.clock.now() >= pending.expires_at {
            let pending = state.pending.remove(&confirmation_id).expect("present");
            remember_used(&mut state, confirmation_id);
            let _ = pending.answer.send(ConfirmationResolution {
                accepted: false,
                request_id,
                action_hash: pending.action_hash,
            });
            return Err(ConfirmationError::Expired);
        }
        let pending = state.pending.remove(&confirmation_id).expect("present");
        remember_used(&mut state, confirmation_id);
        pending
            .answer
            .send(ConfirmationResolution {
                accepted,
                request_id,
                action_hash: pending.action_hash,
            })
            .map_err(|_| ConfirmationError::Unavailable)
    }

    pub fn clear(&self) {
        if let Ok(mut state) = self.state.lock() {
            for (_, pending) in state.pending.drain() {
                let _ = pending.answer.send(ConfirmationResolution {
                    accepted: false,
                    request_id: pending.request_id,
                    action_hash: pending.action_hash,
                });
            }
        }
    }

    fn expire_pending(&self, confirmation_id: ConfirmationId) -> Result<bool, ConfirmationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ConfirmationError::Unavailable)?;
        let Some(pending) = state.pending.remove(&confirmation_id) else {
            return Ok(false);
        };
        remember_used(&mut state, confirmation_id);
        drop(pending);
        Ok(true)
    }
}

fn remember_used(state: &mut ConfirmationState, id: ConfirmationId) {
    state.used.insert(id);
    state.used_order.push_back(id);
    if state.used_order.len() > USED_CONFIRMATIONS
        && let Some(old) = state.used_order.pop_front()
    {
        state.used.remove(&old);
    }
}

impl ConfirmationBroker for SessionConfirmationBroker {
    fn confirm<'a>(&'a self, request: ConfirmationRequest) -> ConfirmationFuture<'a> {
        Box::pin(async move {
            if self.clock.now() >= request.expires_at {
                return Err(ConfirmationError::Expired);
            }
            let until_request_expiry = (request.expires_at - self.clock.now())
                .to_std()
                .map_err(|_| ConfirmationError::Expired)?;
            let timeout = self.timeout.min(until_request_expiry);
            let deadline = tokio::time::Instant::now() + timeout;
            let (answer, response) = oneshot::channel();
            {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ConfirmationError::Unavailable)?;
                if state.pending.contains_key(&request.id) || state.used.contains(&request.id) {
                    return Err(ConfirmationError::AlreadyUsed);
                }
                state.pending.insert(
                    request.id,
                    PendingConfirmation {
                        request_id: request.request_id,
                        action_hash: request.action_hash(),
                        expires_at: request.expires_at,
                        answer,
                    },
                );
            }
            let prompt = ConfirmationPrompt {
                request_id: request.request_id,
                request: request.clone(),
            };
            tokio::select! {
                result = self.prompts.send(prompt) => {
                    if result.is_err() {
                        self.clear();
                        return Err(ConfirmationError::Unavailable);
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    if self.expire_pending(request.id)? {
                        return Err(ConfirmationError::Expired);
                    }
                }
            }
            let mut response = Box::pin(response);
            let resolution = match tokio::time::timeout_at(deadline, &mut response).await {
                Ok(response) => response.map_err(|_| ConfirmationError::Unavailable)?,
                Err(_) => {
                    if self.expire_pending(request.id)? {
                        return Err(ConfirmationError::Expired);
                    }
                    response.await.map_err(|_| ConfirmationError::Unavailable)?
                }
            };
            if resolution.request_id != request.request_id {
                return Err(ConfirmationError::WrongRequest);
            }
            if resolution.action_hash != request.action_hash() {
                return Err(ConfirmationError::ActionChanged);
            }
            if self.clock.now() >= request.expires_at {
                return Err(ConfirmationError::Expired);
            }
            if resolution.accepted {
                Ok(())
            } else {
                Err(ConfirmationError::Rejected)
            }
        })
    }
}

pub struct StdioServer {
    dependencies: ServerDependencies,
    confirmation_timeout: std::time::Duration,
    maximum_confirmation_timeout: std::time::Duration,
}

impl StdioServer {
    pub fn new(dependencies: ServerDependencies) -> Self {
        let confirmation_timeout = dependencies.settings.scheduler().confirmation_timeout();
        Self {
            dependencies,
            confirmation_timeout,
            maximum_confirmation_timeout: confirmation_timeout,
        }
    }

    /// Tightens the maximum confirmation wait. Production defaults to five
    /// minutes; shorter values are useful for fail-fast deployments and tests.
    pub fn with_confirmation_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<Self, ServerError> {
        if timeout.is_zero() || timeout > self.maximum_confirmation_timeout {
            return Err(ServerError::Protocol);
        }
        self.confirmation_timeout = timeout;
        Ok(self)
    }

    pub async fn serve<R, W>(&self, reader: R, writer: W) -> Result<(), ServerError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        self.serve_with_cancellation(reader, writer, CancellationToken::new())
            .await
    }

    pub async fn serve_with_cancellation<R, W>(
        &self,
        reader: R,
        writer: W,
        cancellation: CancellationToken,
    ) -> Result<(), ServerError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (input_tx, mut input_rx) = mpsc::channel::<InputEvent>(EVENT_QUEUE);
        let reader_task = tokio::spawn(read_requests(reader, input_tx));
        let (output_tx, output_rx) = mpsc::channel::<OutputCommand>(EVENT_QUEUE);
        let (writer_failure_tx, mut writer_failure_rx) = mpsc::channel(1);
        let writer_task = tokio::spawn(write_events(writer, output_rx, writer_failure_tx));
        let (events_tx, mut events_rx) = mpsc::channel::<InternalEvent>(EVENT_QUEUE);
        let (broker, mut prompts_rx) = SessionConfirmationBroker::pair_with_clock_and_timeout(
            Arc::new(UtcClock),
            self.confirmation_timeout,
        );
        let mut active: HashMap<DialogId, ActiveTurn> = HashMap::new();
        let mut background: Vec<BackgroundTask> = Vec::new();

        queue_event(&output_tx, RequestId::new(), ServerEvent::Hello, None)?;
        let result = loop {
            tokio::select! {
                input = input_rx.recv() => match input {
                    Some(InputEvent::Request(request)) => {
                        if let Err(error) = self.handle_request(request, &output_tx, &events_tx, &broker, &mut active, &mut background).await {
                            break Err(error);
                        }
                    }
                    Some(InputEvent::ProtocolError(code)) => {
                        let (ack, delivered) = oneshot::channel();
                        if queue_event(
                            &output_tx,
                            RequestId::new(),
                            ServerEvent::ProtocolError { code },
                            Some(ack),
                        ).is_err() {
                            break Err(ServerError::Protocol);
                        }
                        match tokio::time::timeout(TERMINAL_DRAIN_TIMEOUT, delivered).await {
                            Ok(Ok(Ok(()))) => break Ok(()),
                            _ => break Err(ServerError::Protocol),
                        }
                    }
                    Some(InputEvent::Eof) | None => break Ok(()),
                },
                Some(event) = events_rx.recv() => {
                    if let Some(dialog_id) = event.done { active.remove(&dialog_id); }
                    if let Err(error) = queue_event(&output_tx, event.request_id, event.event, event.ack) {
                        break Err(error);
                    }
                }
                Some(prompt) = prompts_rx.recv() => {
                    let preview = wire_confirmation_preview(prompt.request.preview)?;
                    if let Err(error) = queue_event(&output_tx, prompt.request_id, ServerEvent::ConfirmationRequired {
                        confirmation_id: prompt.request.id,
                        preview,
                    }, None) {
                        break Err(error);
                    }
                }
                Some(()) = writer_failure_rx.recv() => break Err(ServerError::Protocol),
                _ = cancellation.cancelled() => break Ok(()),
            }
        };

        for turn in active.values() {
            turn.cancellation.cancel();
        }
        broker.clear();
        for task in &background {
            task.cancellation.cancel();
        }
        drop(events_rx);
        drop(output_tx);
        reader_task.abort();
        writer_task.abort();
        let _ = reader_task.await;
        let _ = writer_task.await;
        for (_, turn) in active {
            let _ = turn.task.await;
        }
        for task in background {
            let _ = task.task.await;
        }
        result
    }

    async fn handle_request(
        &self,
        envelope: RequestEnvelope,
        output: &mpsc::Sender<OutputCommand>,
        events: &mpsc::Sender<InternalEvent>,
        broker: &Arc<SessionConfirmationBroker>,
        active: &mut HashMap<DialogId, ActiveTurn>,
        background: &mut Vec<BackgroundTask>,
    ) -> Result<(), ServerError> {
        use crate::protocol::ClientRequest::*;
        reap_background(background).await;
        let request_id = envelope.request_id;
        match envelope.request {
            ListDialogs => self.start_dialog_list(request_id, events, background, output)?,
            CreateDialog { title } => {
                if !valid_title(&title, self.dependencies.settings.max_message_bytes()) {
                    queue_error(output, request_id, ProtocolErrorCode::ContentTooLong)?;
                } else {
                    match self.dependencies.store.create_dialog(&title) {
                        Ok(dialog) => queue_event(
                            output,
                            request_id,
                            ServerEvent::DialogOpened {
                                dialog_id: dialog.id,
                                title: dialog.title,
                            },
                            None,
                        )?,
                        Err(_) => {
                            queue_error(output, request_id, ProtocolErrorCode::InternalError)?
                        }
                    }
                }
            }
            OpenDialog { dialog_id } => match self.dependencies.store.get_dialog(dialog_id) {
                Ok(dialog) => queue_event(
                    output,
                    request_id,
                    ServerEvent::DialogOpened {
                        dialog_id,
                        title: dialog.title,
                    },
                    None,
                )?,
                Err(StoreError::NotFound) => {
                    queue_error(output, request_id, ProtocolErrorCode::InvalidRequest)?
                }
                Err(_) => queue_error(output, request_id, ProtocolErrorCode::InternalError)?,
            },
            RenameDialog { dialog_id, title } => {
                if !valid_title(&title, self.dependencies.settings.max_message_bytes()) {
                    queue_error(output, request_id, ProtocolErrorCode::ContentTooLong)?;
                } else if self
                    .dependencies
                    .store
                    .rename_dialog(dialog_id, &title)
                    .is_err()
                {
                    queue_error(output, request_id, ProtocolErrorCode::InternalError)?;
                } else {
                    queue_event(
                        output,
                        request_id,
                        ServerEvent::DialogOpened { dialog_id, title },
                        None,
                    )?;
                }
            }
            DeleteDialog { dialog_id } => {
                if self.dependencies.store.delete_dialog(dialog_id).is_err() {
                    queue_error(output, request_id, ProtocolErrorCode::InternalError)?;
                } else {
                    self.start_dialog_list(request_id, events, background, output)?;
                }
            }
            SetDialogPermission {
                dialog_id,
                confirmation_required,
            } => match self
                .dependencies
                .store
                .set_cron_confirmation_required(dialog_id, confirmation_required)
            {
                Ok(_) => queue_event(
                    output,
                    request_id,
                    ServerEvent::DialogPermissionChanged {
                        dialog_id,
                        confirmation_required,
                    },
                    None,
                )?,
                Err(StoreError::NotFound) => {
                    queue_error(output, request_id, ProtocolErrorCode::InvalidRequest)?
                }
                Err(_) => queue_error(output, request_id, ProtocolErrorCode::InternalError)?,
            },
            SendMessage { dialog_id, message } => {
                if message.len() > self.dependencies.settings.max_message_bytes() {
                    queue_error(output, request_id, ProtocolErrorCode::ContentTooLong)?;
                    return Ok(());
                }
                let at_capacity = active.len() >= MAX_ACTIVE_TURNS;
                match active.entry(dialog_id) {
                    std::collections::hash_map::Entry::Occupied(_) => {
                        queue_event(
                            output,
                            request_id,
                            ServerEvent::TurnFailed {
                                code: ProtocolErrorCode::InternalError,
                            },
                            None,
                        )?;
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        if at_capacity {
                            queue_event(
                                output,
                                request_id,
                                ServerEvent::TurnFailed {
                                    code: ProtocolErrorCode::InternalError,
                                },
                                None,
                            )?;
                            return Ok(());
                        }
                        queue_event(
                            output,
                            request_id,
                            ServerEvent::ResponseStarted { dialog_id },
                            None,
                        )?;
                        let cancellation = CancellationToken::new();
                        match self.spawn_turn(
                            dialog_id,
                            request_id,
                            message,
                            cancellation.clone(),
                            events.clone(),
                            broker.clone(),
                        ) {
                            Ok(task) => {
                                entry.insert(ActiveTurn { cancellation, task });
                            }
                            Err(_) => {
                                queue_event(
                                    output,
                                    request_id,
                                    ServerEvent::TurnFailed {
                                        code: ProtocolErrorCode::InternalError,
                                    },
                                    None,
                                )?;
                            }
                        }
                    }
                }
            }
            ConfirmAction {
                confirmation_id,
                originating_request_id,
            } => {
                if broker
                    .resolve(originating_request_id, confirmation_id, true)
                    .is_err()
                {
                    queue_error(output, request_id, ProtocolErrorCode::InvalidRequest)?;
                } else {
                    queue_event(
                        output,
                        request_id,
                        ServerEvent::ConfirmationResolved {
                            confirmation_id,
                            accepted: true,
                        },
                        None,
                    )?;
                }
            }
            CancelAction {
                confirmation_id,
                originating_request_id,
            } => {
                if broker
                    .resolve(originating_request_id, confirmation_id, false)
                    .is_err()
                {
                    queue_error(output, request_id, ProtocolErrorCode::InvalidRequest)?;
                } else {
                    queue_event(
                        output,
                        request_id,
                        ServerEvent::ConfirmationResolved {
                            confirmation_id,
                            accepted: false,
                        },
                        None,
                    )?;
                }
            }
            Inspect { kind } => {
                if active_background(background) >= MAX_BACKGROUND_REQUESTS {
                    queue_error(output, request_id, ProtocolErrorCode::InternalError)?;
                } else {
                    background.push(start_inspection(
                        self.dependencies.inspection.clone(),
                        request_id,
                        kind,
                        events.clone(),
                    ));
                }
            }
            Export => {
                if active_background(background) >= MAX_BACKGROUND_REQUESTS {
                    queue_error(output, request_id, ProtocolErrorCode::InternalError)?;
                } else {
                    background.push(start_export(
                        self.dependencies.inspection.clone(),
                        request_id,
                        events.clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn start_dialog_list(
        &self,
        request_id: RequestId,
        events: &mpsc::Sender<InternalEvent>,
        background: &mut Vec<BackgroundTask>,
        output: &mpsc::Sender<OutputCommand>,
    ) -> Result<(), ServerError> {
        if active_background(background) >= MAX_BACKGROUND_REQUESTS {
            return queue_error(output, request_id, ProtocolErrorCode::InternalError);
        }
        background.push(start_dialog_list(
            self.dependencies.inspection.clone(),
            request_id,
            events.clone(),
        ));
        Ok(())
    }

    fn spawn_turn(
        &self,
        dialog_id: DialogId,
        request_id: RequestId,
        message: String,
        cancellation: CancellationToken,
        events: mpsc::Sender<InternalEvent>,
        broker: Arc<SessionConfirmationBroker>,
    ) -> Result<JoinHandle<()>, ServerError> {
        let scheduler: Arc<dyn ToolExecutor> = Arc::new(
            SchedulerToolExecutor::new_configured(
                self.dependencies.store.clone(),
                self.dependencies.synchronizer.clone(),
                broker,
                dialog_id,
                request_id,
                self.dependencies.settings.scheduler().timezone(),
                self.dependencies.settings.max_message_bytes(),
                self.dependencies
                    .settings
                    .scheduler()
                    .confirmation_timeout(),
            )
            .map_err(|_| ServerError::Protocol)?,
        );
        let time: Arc<dyn ToolExecutor> = Arc::new(TimeToolExecutor::new(
            self.dependencies.settings.scheduler().timezone(),
        ));
        let tools =
            CompositeToolExecutor::new(vec![self.dependencies.mcp.clone(), scheduler, time])
                .map_err(|_| ServerError::ToolCatalog)?;
        let runner = AgentRunner::new(
            self.dependencies.provider.clone(),
            Arc::new(tools),
            self.dependencies.store.clone(),
            self.dependencies.settings.mcp().max_tool_rounds() as usize,
            self.dependencies.settings.max_provider_request_bytes(),
            self.dependencies.settings.max_message_bytes(),
        );
        let execution = TurnExecution {
            store: self.dependencies.store.clone(),
            runner,
            max_message_bytes: self.dependencies.settings.max_message_bytes(),
            system_prompt: self
                .dependencies
                .settings
                .interactive_system_prompt()
                .to_owned(),
        };
        Ok(tokio::spawn(async move {
            let event_tx = events.clone();
            let acknowledgements = Arc::new(Mutex::new(Vec::new()));
            let sink_acknowledgements = acknowledgements.clone();
            let mut sink = move |event| {
                let event = match event {
                    AgentEvent::TextDelta { text } => ServerEvent::TextDelta { text },
                    AgentEvent::ToolStarted { name } => ServerEvent::ToolStarted { name },
                    AgentEvent::ToolFinished { name, code } => ServerEvent::ToolFinished {
                        name,
                        code: match code {
                            ToolEventCode::Completed => ProtocolErrorCode::Ok,
                            ToolEventCode::Failed | ToolEventCode::Uncertain => {
                                ProtocolErrorCode::InternalError
                            }
                        },
                    },
                };
                let (ack, response) = oneshot::channel();
                event_tx
                    .try_send(InternalEvent {
                        request_id,
                        event,
                        done: None,
                        ack: Some(ack),
                    })
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::WouldBlock, "output_backpressure")
                    })?;
                sink_acknowledgements
                    .lock()
                    .map_err(|_| io::Error::other("output_error"))?
                    .push(response);
                Ok(())
            };
            let outcome = execution
                .run(
                    dialog_id,
                    &message,
                    cancellation.clone(),
                    &mut sink,
                    acknowledgements,
                )
                .await;
            let final_event = match outcome {
                Ok(prepared) => {
                    let (ack, delivered) = oneshot::channel();
                    if events
                        .send(InternalEvent {
                            request_id,
                            event: ServerEvent::TurnPrepared {
                                answer: prepared.answer.clone(),
                            },
                            done: None,
                            ack: Some(ack),
                        })
                        .await
                        .is_err()
                        || delivered.await.ok() != Some(Ok(()))
                    {
                        let _ = execution.abandon(prepared.turn_id, cancellation.is_cancelled());
                        return;
                    }
                    match execution.commit(prepared.turn_id, &prepared.answer) {
                        Ok(()) => ServerEvent::TurnCompleted {
                            answer: prepared.answer,
                        },
                        Err(_) => ServerEvent::TurnFailed {
                            code: ProtocolErrorCode::InternalError,
                        },
                    }
                }
                Err(_) => ServerEvent::TurnFailed {
                    code: ProtocolErrorCode::InternalError,
                },
            };
            let _ = events
                .send(InternalEvent {
                    request_id,
                    event: final_event,
                    done: Some(dialog_id),
                    ack: None,
                })
                .await;
        }))
    }
}

struct ActiveTurn {
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

struct BackgroundTask {
    cancellation: InspectionCancellation,
    task: JoinHandle<()>,
}

enum InputEvent {
    Request(RequestEnvelope),
    ProtocolError(ProtocolErrorCode),
    Eof,
}

struct OutputCommand {
    envelope: ServerEnvelope,
    ack: Option<oneshot::Sender<Result<(), ()>>>,
}

struct InternalEvent {
    request_id: RequestId,
    event: ServerEvent,
    done: Option<DialogId>,
    ack: Option<oneshot::Sender<Result<(), ()>>>,
}

async fn read_requests<R>(reader: R, input: mpsc::Sender<InputEvent>)
where
    R: AsyncRead + Unpin,
{
    let mut reader = NdjsonReader::new(reader);
    loop {
        let event = match reader.read_request().await {
            Ok(Some(request)) => InputEvent::Request(request),
            Ok(None) => InputEvent::Eof,
            Err(error) => InputEvent::ProtocolError(error.code()),
        };
        let terminal = !matches!(event, InputEvent::Request(_));
        if input.send(event).await.is_err() || terminal {
            return;
        }
    }
}

async fn write_events<W>(
    writer: W,
    mut output: mpsc::Receiver<OutputCommand>,
    failure: mpsc::Sender<()>,
) where
    W: AsyncWrite + Unpin,
{
    let mut writer = NdjsonWriter::new(writer);
    while let Some(command) = output.recv().await {
        let result = writer.write_event(&command.envelope).await;
        let acknowledged = if result.is_ok() { Ok(()) } else { Err(()) };
        if let Some(ack) = command.ack {
            let _ = ack.send(acknowledged);
        }
        if result.is_err() {
            let _ = failure.try_send(());
            return;
        }
    }
}

fn queue_event(
    output: &mpsc::Sender<OutputCommand>,
    request_id: RequestId,
    event: ServerEvent,
    ack: Option<oneshot::Sender<Result<(), ()>>>,
) -> Result<(), ServerError> {
    let command = OutputCommand {
        envelope: ServerEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            event,
        },
        ack,
    };
    output.try_send(command).map_err(|error| {
        let command = match error {
            mpsc::error::TrySendError::Full(command)
            | mpsc::error::TrySendError::Closed(command) => command,
        };
        if let Some(ack) = command.ack {
            let _ = ack.send(Err(()));
        }
        ServerError::Protocol
    })
}

fn queue_error(
    output: &mpsc::Sender<OutputCommand>,
    request_id: RequestId,
    code: ProtocolErrorCode,
) -> Result<(), ServerError> {
    queue_event(
        output,
        request_id,
        ServerEvent::ProtocolError { code },
        None,
    )
}

fn active_background(tasks: &[BackgroundTask]) -> usize {
    tasks.iter().filter(|task| !task.task.is_finished()).count()
}

async fn reap_background(tasks: &mut Vec<BackgroundTask>) {
    let mut index = 0;
    while index < tasks.len() {
        if tasks[index].task.is_finished() {
            let task = tasks.swap_remove(index);
            let _ = task.task.await;
        } else {
            index += 1;
        }
    }
}

type EventAcknowledgements = Arc<Mutex<Vec<oneshot::Receiver<Result<(), ()>>>>>;

struct TurnExecution {
    store: Store,
    runner: AgentRunner,
    system_prompt: String,
    max_message_bytes: usize,
}

struct PreparedTurn {
    turn_id: crate::domain::TurnId,
    answer: String,
}

impl TurnExecution {
    async fn run(
        &self,
        dialog_id: DialogId,
        content: &str,
        cancellation: CancellationToken,
        sink: &mut crate::agent_runner::AgentEventSink<'_>,
        acknowledgements: EventAcknowledgements,
    ) -> Result<PreparedTurn, AgentError> {
        if content.len() > self.max_message_bytes {
            return Err(AgentError::ContentTooLong);
        }
        let turn = self.store.begin_turn(dialog_id, content)?;
        let history = match self.store.completed_messages(dialog_id) {
            Ok(history) => history,
            Err(error) => {
                let _ = self
                    .store
                    .fail_turn(turn.turn_id, SafeErrorCode::InternalError);
                return Err(AgentError::Store(error));
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
                sink,
            )
            .await;
        let receivers = acknowledgements
            .lock()
            .map_err(|_| AgentError::Output)?
            .drain(..)
            .collect::<Vec<_>>();
        for receiver in receivers {
            if receiver.await.ok() != Some(Ok(())) {
                self.store
                    .fail_turn(turn.turn_id, SafeErrorCode::InternalError)?;
                return Err(AgentError::Output);
            }
        }
        match result {
            Ok(outcome) => Ok(PreparedTurn {
                turn_id: turn.turn_id,
                answer: outcome.answer,
            }),
            Err(error) => {
                if error == AgentError::Interrupted {
                    self.store
                        .interrupt_turn(turn.turn_id, SafeErrorCode::Interrupted)?;
                } else {
                    self.store
                        .fail_turn(turn.turn_id, agent_error_code(error))?;
                }
                Err(error)
            }
        }
    }

    fn commit(&self, turn_id: crate::domain::TurnId, answer: &str) -> Result<(), AgentError> {
        if let Err(error) = self.store.complete_turn(turn_id, answer) {
            let _ = self.store.fail_turn(turn_id, SafeErrorCode::InternalError);
            return Err(AgentError::Store(error));
        }
        Ok(())
    }

    fn abandon(&self, turn_id: crate::domain::TurnId, interrupted: bool) -> Result<(), AgentError> {
        if interrupted {
            self.store
                .interrupt_turn(turn_id, SafeErrorCode::Interrupted)?;
        } else {
            self.store
                .fail_turn(turn_id, SafeErrorCode::InternalError)?;
        }
        Ok(())
    }
}

fn agent_error_code(error: AgentError) -> SafeErrorCode {
    match error {
        AgentError::Provider => SafeErrorCode::ProviderError,
        AgentError::ContextTooLong => SafeErrorCode::ContextTooLong,
        AgentError::ToolRoundLimit => SafeErrorCode::ToolRoundLimit,
        AgentError::Interrupted => SafeErrorCode::Interrupted,
        AgentError::Tool => SafeErrorCode::ToolError,
        AgentError::Store(_) | AgentError::ContentTooLong | AgentError::Output => {
            SafeErrorCode::InternalError
        }
    }
}

fn valid_title(title: &str, max: usize) -> bool {
    !title.trim().is_empty() && title.len() <= max
}

fn wire_confirmation_preview(
    preview: SchedulePreview,
) -> Result<ScheduleConfirmationPreview, ServerError> {
    let action = match preview.action {
        ScheduleAction::Create => ConfirmationAction::Create,
        ScheduleAction::Update => ConfirmationAction::Update,
        ScheduleAction::Enable => ConfirmationAction::Enable,
        ScheduleAction::Disable => ConfirmationAction::Disable,
        ScheduleAction::Delete => ConfirmationAction::Delete,
    };
    let schedule_kind = match preview.schedule_kind.as_str() {
        "cron" => ConfirmationScheduleKind::Cron,
        "once_at" => ConfirmationScheduleKind::OnceAt,
        _ => return Err(ServerError::Protocol),
    };
    Ok(ScheduleConfirmationPreview {
        action,
        job_id: preview.job_id,
        name: preview.name,
        schedule_kind,
        schedule_value: preview.schedule_value,
        timezone: preview.timezone,
        task: preview.prompt,
    })
}

fn inspect_query(kind: &InspectKind) -> InspectQuery {
    match kind {
        InspectKind::Dialogs => InspectQuery::Dialogs,
        InspectKind::History { dialog_id } => InspectQuery::History(*dialog_id),
        InspectKind::Jobs => InspectQuery::Jobs,
        InspectKind::Job { job_id } => InspectQuery::Job(*job_id),
        InspectKind::Runs { job_id } => InspectQuery::Runs(*job_id),
        InspectKind::Audit { dialog_id } => InspectQuery::Audit(*dialog_id),
        InspectKind::Dump => InspectQuery::Dump,
    }
}

fn start_dialog_list(
    service: InspectionService,
    request_id: RequestId,
    events: mpsc::Sender<InternalEvent>,
) -> BackgroundTask {
    let cancellation = InspectionCancellation::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::task::spawn_blocking(move || {
        let snapshot = match service.snapshot_with_page_size_and_cancellation(
            InspectQuery::Dialogs,
            1,
            task_cancellation.clone(),
        ) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                send_protocol_error(&events, request_id, &task_cancellation);
                return;
            }
        };
        let mut snapshot = Some(snapshot);
        let mut sequence = 0_u64;
        loop {
            if task_cancellation.is_cancelled() {
                if let Some(snapshot) = snapshot.take() {
                    let _ = snapshot.close();
                }
                return;
            }
            let page = match snapshot.as_mut().expect("snapshot present").next_page() {
                Ok(Some(page)) => page,
                Ok(None) => {
                    let _ = send_internal_acknowledged(
                        &events,
                        request_id,
                        ServerEvent::DialogList {
                            sequence,
                            dialogs: Vec::new(),
                            complete: true,
                        },
                        &task_cancellation,
                    );
                    return;
                }
                Err(_) => {
                    send_protocol_error(&events, request_id, &task_cancellation);
                    return;
                }
            };
            let mut dialogs = Vec::with_capacity(page.items.len());
            for item in page.items {
                let Some(id) = item
                    .get("id")
                    .and_then(serde_json::Value::as_i64)
                    .and_then(|id| DialogId::new(id).ok())
                else {
                    if let Some(snapshot) = snapshot.take() {
                        let _ = snapshot.close();
                    }
                    send_protocol_error(&events, request_id, &task_cancellation);
                    return;
                };
                let Some(title) = item.get("title").and_then(serde_json::Value::as_str) else {
                    if let Some(snapshot) = snapshot.take() {
                        let _ = snapshot.close();
                    }
                    send_protocol_error(&events, request_id, &task_cancellation);
                    return;
                };
                dialogs.push(crate::protocol::DialogSummary {
                    id,
                    title: title.to_owned(),
                });
            }
            if page.complete && snapshot.take().expect("snapshot present").close().is_err() {
                send_protocol_error(&events, request_id, &task_cancellation);
                return;
            }
            if dialogs.is_empty() {
                if !send_internal_acknowledged(
                    &events,
                    request_id,
                    ServerEvent::DialogList {
                        sequence,
                        dialogs,
                        complete: page.complete,
                    },
                    &task_cancellation,
                ) {
                    if let Some(snapshot) = snapshot.take() {
                        let _ = snapshot.close();
                    }
                    return;
                }
                sequence += 1;
            } else {
                let count = dialogs.len();
                for (index, dialog) in dialogs.into_iter().enumerate() {
                    if !send_internal_acknowledged(
                        &events,
                        request_id,
                        ServerEvent::DialogList {
                            sequence,
                            dialogs: vec![dialog],
                            complete: page.complete && index + 1 == count,
                        },
                        &task_cancellation,
                    ) {
                        if let Some(snapshot) = snapshot.take() {
                            let _ = snapshot.close();
                        }
                        return;
                    }
                    sequence += 1;
                }
            }
            if page.complete {
                return;
            }
        }
    });
    BackgroundTask { cancellation, task }
}

fn start_inspection(
    service: InspectionService,
    request_id: RequestId,
    kind: InspectKind,
    events: mpsc::Sender<InternalEvent>,
) -> BackgroundTask {
    let cancellation = InspectionCancellation::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::task::spawn_blocking(move || {
        if task_cancellation.is_cancelled() {
            return;
        }
        let mut snapshot = match service
            .snapshot_with_cancellation(inspect_query(&kind), task_cancellation.clone())
        {
            Ok(value) => value,
            Err(_) => {
                send_protocol_error(&events, request_id, &task_cancellation);
                return;
            }
        };
        let mut sequence = 0u64;
        let mut record_sequence = 0u64;
        loop {
            if task_cancellation.is_cancelled() {
                let _ = snapshot.close();
                return;
            }
            match snapshot.next_page() {
                Ok(Some(page)) => {
                    for item in page.items {
                        let bytes = match serde_json::to_vec(&item) {
                            Ok(bytes) => bytes,
                            Err(_) => {
                                let _ = snapshot.close();
                                send_protocol_error(&events, request_id, &task_cancellation);
                                return;
                            }
                        };
                        let chunks = bytes.chunks(FRAGMENT_BYTES).collect::<Vec<_>>();
                        for (fragment_sequence, chunk) in chunks.iter().enumerate() {
                            let fragment = json!({
                                "record_fragment": {
                                    "record_sequence": record_sequence,
                                    "fragment_sequence": fragment_sequence,
                                    "complete": fragment_sequence + 1 == chunks.len(),
                                    "encoding": "base64",
                                    "data": base64::engine::general_purpose::STANDARD.encode(chunk),
                                }
                            });
                            if !send_internal(
                                &events,
                                request_id,
                                ServerEvent::InspectionResult {
                                    kind: kind.clone(),
                                    sequence,
                                    items: vec![fragment],
                                    complete: false,
                                },
                                &task_cancellation,
                            ) {
                                let _ = snapshot.close();
                                return;
                            }
                            sequence += 1;
                        }
                        record_sequence += 1;
                    }
                    if page.complete {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    let _ = snapshot.close();
                    send_protocol_error(&events, request_id, &task_cancellation);
                    return;
                }
            }
        }
        if snapshot.close().is_err() {
            send_protocol_error(&events, request_id, &task_cancellation);
            return;
        }
        let _ = send_internal(
            &events,
            request_id,
            ServerEvent::InspectionResult {
                kind,
                sequence,
                items: Vec::new(),
                complete: true,
            },
            &task_cancellation,
        );
    });
    BackgroundTask { cancellation, task }
}

fn send_internal(
    events: &mpsc::Sender<InternalEvent>,
    request_id: RequestId,
    event: ServerEvent,
    cancellation: &InspectionCancellation,
) -> bool {
    send_internal_event(events, request_id, event, None, cancellation)
}

fn send_internal_acknowledged(
    events: &mpsc::Sender<InternalEvent>,
    request_id: RequestId,
    event: ServerEvent,
    cancellation: &InspectionCancellation,
) -> bool {
    let (ack, mut delivered) = oneshot::channel();
    if !send_internal_event(events, request_id, event, Some(ack), cancellation) {
        return false;
    }
    loop {
        if cancellation.is_cancelled() {
            return false;
        }
        match delivered.try_recv() {
            Ok(Ok(())) => return true,
            Ok(Err(())) | Err(oneshot::error::TryRecvError::Closed) => return false,
            Err(oneshot::error::TryRecvError::Empty) => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
}

fn send_internal_event(
    events: &mpsc::Sender<InternalEvent>,
    request_id: RequestId,
    event: ServerEvent,
    ack: Option<oneshot::Sender<Result<(), ()>>>,
    cancellation: &InspectionCancellation,
) -> bool {
    let mut pending = InternalEvent {
        request_id,
        event,
        done: None,
        ack,
    };
    loop {
        if cancellation.is_cancelled() {
            return false;
        }
        match events.try_send(pending) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(event)) => {
                pending = event;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
}

fn send_protocol_error(
    events: &mpsc::Sender<InternalEvent>,
    request_id: RequestId,
    cancellation: &InspectionCancellation,
) {
    if !cancellation.is_cancelled() {
        let _ = send_internal(
            events,
            request_id,
            ServerEvent::ProtocolError {
                code: ProtocolErrorCode::InternalError,
            },
            cancellation,
        );
    }
}

struct ChunkWriter {
    request_id: RequestId,
    events: mpsc::Sender<InternalEvent>,
    sequence: u64,
    buffer: Vec<u8>,
    cancellation: InspectionCancellation,
    total_bytes: u64,
    digest: Sha256,
}

impl ChunkWriter {
    fn flush_chunks(&mut self, all: bool) -> io::Result<()> {
        while self.buffer.len() >= EXPORT_CHUNK_BYTES || (all && !self.buffer.is_empty()) {
            if self.cancellation.is_cancelled() {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
            }
            let length = self.buffer.len().min(EXPORT_CHUNK_BYTES);
            let remainder = self.buffer.split_off(length);
            let chunk = std::mem::replace(&mut self.buffer, remainder);
            let event = ServerEvent::ExportChunk {
                sequence: self.sequence,
                data_base64: base64::engine::general_purpose::STANDARD.encode(chunk),
            };
            if !send_internal(&self.events, self.request_id, event, &self.cancellation) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "session_closed"));
            }
            self.sequence += 1;
        }
        Ok(())
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.total_bytes = self
            .total_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("export_too_large"))?;
        self.digest.update(bytes);
        self.buffer.extend_from_slice(bytes);
        self.flush_chunks(false)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_chunks(true)
    }
}

fn start_export(
    service: InspectionService,
    request_id: RequestId,
    events: mpsc::Sender<InternalEvent>,
) -> BackgroundTask {
    let cancellation = InspectionCancellation::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::task::spawn_blocking(move || {
        let mut writer = ChunkWriter {
            request_id,
            events: events.clone(),
            sequence: 0,
            buffer: Vec::with_capacity(EXPORT_CHUNK_BYTES),
            cancellation: task_cancellation.clone(),
            total_bytes: 0,
            digest: Sha256::new(),
        };
        let mut snapshot = match service
            .snapshot_with_cancellation(InspectQuery::Dump, task_cancellation.clone())
        {
            Ok(snapshot) => snapshot,
            Err(_) => {
                send_protocol_error(&events, request_id, &task_cancellation);
                return;
            }
        };
        let result = (|| -> Result<u64, ()> {
            write_export_line(
                &mut writer,
                &LogicalExportV1::Header {
                    format: "logical_export_v1".into(),
                    version: 1,
                },
            )?;
            let mut records = 0u64;
            loop {
                if task_cancellation.is_cancelled() {
                    return Err(());
                }
                match snapshot.next_page().map_err(|_| ())? {
                    Some(page) => {
                        for record in page.items {
                            write_export_line(&mut writer, &LogicalExportV1::Record { record })?;
                            records += 1;
                        }
                        if page.complete {
                            break;
                        }
                    }
                    None => break,
                }
            }
            Ok(records)
        })();
        let close = snapshot.close();
        if result.is_err() || close.is_err() || writer.flush().is_err() {
            send_protocol_error(&events, request_id, &task_cancellation);
            return;
        }
        let total_bytes = writer.total_bytes;
        let sha256 = format!("{:x}", writer.digest.finalize());
        let _ = send_internal(
            &events,
            request_id,
            ServerEvent::ExportCompleted {
                total_bytes,
                sha256,
            },
            &task_cancellation,
        );
    });
    BackgroundTask { cancellation, task }
}

fn write_export_line(writer: &mut ChunkWriter, record: &LogicalExportV1) -> Result<(), ()> {
    serde_json::to_writer(&mut *writer, record).map_err(|_| ())?;
    writer.write_all(b"\n").map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        provider::ModelToolCall,
        scheduler::{CrontabBackend, CrontabFuture},
    };
    use chrono::{DateTime, Duration, TimeZone};
    use chrono_tz::Europe::Moscow;
    use std::{path::PathBuf, sync::Mutex};

    fn private_tempdir() -> tempfile::TempDir {
        let temporary_root = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        let directory = tempfile::tempdir_in(temporary_root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        directory
    }

    #[derive(Clone)]
    struct MutableClock(Arc<Mutex<DateTime<Utc>>>);
    impl MutableClock {
        fn advance(&self, duration: Duration) {
            *self.0.lock().unwrap() += duration;
        }
    }
    impl ConfirmationClock for MutableClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    struct Backend;
    impl CrontabBackend for Backend {
        fn preflight(&self) -> CrontabFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
        fn list(&self) -> CrontabFuture<'_, String> {
            Box::pin(async { Ok(String::new()) })
        }
        fn validate<'a>(&'a self, _: &'a str) -> CrontabFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
        fn install<'a>(&'a self, _: &'a str) -> CrontabFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    fn create_call(prompt: &str) -> ModelToolCall {
        ModelToolCall {
            id: "call".into(),
            name: "cron__create".into(),
            arguments: json!({
                "name":"job", "schedule":{"kind":"cron","expression":"0 9 * * *"},
                "timezone":"Europe/Moscow", "prompt":prompt
            })
            .to_string(),
        }
    }

    #[tokio::test]
    async fn confirmation_is_request_session_hash_expiry_bound_and_single_use() {
        let dir = private_tempdir();
        let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
        let dialog = store.create_dialog("dialog").unwrap().id;
        let synchronizer = Arc::new(
            CronSynchronizer::new(
                store.clone(),
                Arc::new(Backend),
                dir.path().join("cron.lock"),
                PathBuf::from("/opt/light-agent/bin/light-agent"),
            )
            .unwrap(),
        );
        let clock = Arc::new(MutableClock(Arc::new(Mutex::new(
            Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap(),
        ))));
        let request_id = RequestId::new();
        let (broker, mut prompts) = SessionConfirmationBroker::pair_with_clock(clock.clone());
        let (other_session, _) = SessionConfirmationBroker::pair_with_clock(clock.clone());
        let executor = Arc::new(SchedulerToolExecutor::new_with_clock(
            store.clone(),
            synchronizer.clone(),
            broker.clone(),
            dialog,
            request_id,
            clock.clone(),
        ));
        let task = tokio::spawn(async move { executor.call(&create_call("first")).await });
        let prompt = prompts.recv().await.unwrap();
        assert_eq!(
            broker.resolve(RequestId::new(), prompt.request.id, true),
            Err(ConfirmationError::WrongRequest)
        );
        assert_eq!(
            other_session.resolve(request_id, prompt.request.id, true),
            Err(ConfirmationError::WrongSession)
        );
        assert_eq!(broker.resolve(request_id, prompt.request.id, true), Ok(()));
        assert_eq!(
            broker.resolve(request_id, prompt.request.id, true),
            Err(ConfirmationError::AlreadyUsed)
        );
        assert!(!task.await.unwrap().unwrap().is_error);

        let executor = Arc::new(SchedulerToolExecutor::new_with_clock(
            store.clone(),
            synchronizer,
            broker.clone(),
            dialog,
            request_id,
            clock.clone(),
        ));
        let task = tokio::spawn(async move { executor.call(&create_call("expired")).await });
        let prompt = prompts.recv().await.unwrap();
        clock.advance(Duration::minutes(6));
        assert_eq!(
            broker.resolve(request_id, prompt.request.id, true),
            Err(ConfirmationError::Expired)
        );
        assert!(task.await.unwrap().unwrap().is_error);
        assert_eq!(store.list_jobs().unwrap().len(), 1);
        assert_eq!(store.list_jobs().unwrap()[0].schedule.timezone(), Moscow);
    }

    #[tokio::test]
    async fn confirmation_action_hash_tampering_fails_closed() {
        let dir = private_tempdir();
        let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
        let dialog = store.create_dialog("dialog").unwrap().id;
        let synchronizer = Arc::new(
            CronSynchronizer::new(
                store.clone(),
                Arc::new(Backend),
                dir.path().join("cron.lock"),
                PathBuf::from("/opt/light-agent/bin/light-agent"),
            )
            .unwrap(),
        );
        let clock = Arc::new(MutableClock(Arc::new(Mutex::new(
            Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap(),
        ))));
        let request_id = RequestId::new();
        let (broker, mut prompts) = SessionConfirmationBroker::pair_with_clock(clock.clone());
        let executor = Arc::new(SchedulerToolExecutor::new_with_clock(
            store.clone(),
            synchronizer,
            broker.clone(),
            dialog,
            request_id,
            clock,
        ));
        let task = tokio::spawn(async move { executor.call(&create_call("tamper")).await });
        let prompt = prompts.recv().await.unwrap();
        broker
            .state
            .lock()
            .unwrap()
            .pending
            .get_mut(&prompt.request.id)
            .unwrap()
            .action_hash = [0; 32];
        assert_eq!(broker.resolve(request_id, prompt.request.id, true), Ok(()));
        assert!(task.await.unwrap().unwrap().is_error);
        assert!(store.list_jobs().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unanswered_confirmation_expires_without_a_real_five_minute_wait() {
        let dir = private_tempdir();
        let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
        let dialog = store.create_dialog("dialog").unwrap().id;
        let synchronizer = Arc::new(
            CronSynchronizer::new(
                store.clone(),
                Arc::new(Backend),
                dir.path().join("cron.lock"),
                PathBuf::from("/opt/light-agent/bin/light-agent"),
            )
            .unwrap(),
        );
        let clock = Arc::new(MutableClock(Arc::new(Mutex::new(
            Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap(),
        ))));
        let request_id = RequestId::new();
        let (broker, mut prompts) = SessionConfirmationBroker::pair_with_clock_and_timeout(
            clock.clone(),
            std::time::Duration::from_millis(20),
        );
        let executor = Arc::new(SchedulerToolExecutor::new_with_clock(
            store.clone(),
            synchronizer,
            broker.clone(),
            dialog,
            request_id,
            clock,
        ));
        let task = tokio::spawn(async move { executor.call(&create_call("expires")).await });
        let prompt = prompts.recv().await.unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_millis(250), task)
            .await
            .expect("unanswered confirmation must expire")
            .unwrap()
            .unwrap();
        assert!(result.is_error);
        assert!(store.list_jobs().unwrap().is_empty());
        assert_eq!(
            broker.resolve(request_id, prompt.request.id, true),
            Err(ConfirmationError::AlreadyUsed)
        );
    }

    #[tokio::test]
    async fn confirmation_and_expiry_race_has_one_single_use_winner() {
        let dir = private_tempdir();
        let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
        let dialog = store.create_dialog("dialog").unwrap().id;
        let synchronizer = Arc::new(
            CronSynchronizer::new(
                store.clone(),
                Arc::new(Backend),
                dir.path().join("cron.lock"),
                PathBuf::from("/opt/light-agent/bin/light-agent"),
            )
            .unwrap(),
        );
        let clock = Arc::new(MutableClock(Arc::new(Mutex::new(
            Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap(),
        ))));
        let request_id = RequestId::new();
        let (broker, mut prompts) = SessionConfirmationBroker::pair_with_clock_and_timeout(
            clock.clone(),
            std::time::Duration::from_millis(20),
        );
        let executor = Arc::new(SchedulerToolExecutor::new_with_clock(
            store.clone(),
            synchronizer,
            broker.clone(),
            dialog,
            request_id,
            clock,
        ));
        let task = tokio::spawn(async move { executor.call(&create_call("race")).await });
        let prompt = prompts.recv().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let resolution = broker.resolve(request_id, prompt.request.id, true);
        let result = task.await.unwrap().unwrap();

        match resolution {
            Ok(()) => {
                assert!(!result.is_error);
                assert_eq!(store.list_jobs().unwrap().len(), 1);
            }
            Err(ConfirmationError::AlreadyUsed | ConfirmationError::Expired) => {
                assert!(result.is_error);
                assert!(store.list_jobs().unwrap().is_empty());
            }
            other => panic!("unexpected race result: {other:?}"),
        }
        assert_eq!(
            broker.resolve(request_id, prompt.request.id, true),
            Err(ConfirmationError::AlreadyUsed)
        );
    }
}
