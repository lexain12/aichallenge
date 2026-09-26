//! One authenticated stdio session. All stdout bytes are versioned NDJSON.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Write},
    sync::{Arc, Mutex},
};

use base64::Engine as _;
use chrono::Utc;
use serde_json::json;
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
    inspection::{InspectQuery, InspectionError, InspectionService},
    protocol::{
        EXPORT_CHUNK_BYTES, InspectKind, NdjsonReader, NdjsonWriter, PROTOCOL_VERSION,
        ProtocolError, ProtocolErrorCode, RequestEnvelope, ServerEnvelope, ServerEvent,
    },
    provider::Provider,
    scheduler::CronSynchronizer,
    settings::ServerSettings,
    store::{SafeErrorCode, Store, StoreError},
    tools::{
        CompositeToolExecutor, ToolExecutor,
        scheduler::{
            ConfirmationBroker, ConfirmationClock, ConfirmationError, ConfirmationFuture,
            ConfirmationRequest, ScheduleAction, SchedulerToolExecutor,
        },
    },
};

const EVENT_QUEUE: usize = 64;
const MAX_ACTIVE_TURNS: usize = 8;
const MAX_BACKGROUND_REQUESTS: usize = 8;
const FRAGMENT_BYTES: usize = 64 * 1024;
const USED_CONFIRMATIONS: usize = 1024;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    pub tool_runs: usize,
    pub turns: usize,
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
}

struct UtcClock;
impl ConfirmationClock for UtcClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        Utc::now()
    }
}

impl SessionConfirmationBroker {
    fn pair() -> (Arc<Self>, mpsc::Receiver<ConfirmationPrompt>) {
        Self::pair_with_clock(Arc::new(UtcClock))
    }

    fn pair_with_clock(
        clock: Arc<dyn ConfirmationClock>,
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
            if self
                .prompts
                .send(ConfirmationPrompt {
                    request_id: request.request_id,
                    request: request.clone(),
                })
                .await
                .is_err()
            {
                self.clear();
                return Err(ConfirmationError::Unavailable);
            }
            let resolution = response.await.map_err(|_| ConfirmationError::Unavailable)?;
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
}

impl StdioServer {
    pub fn new(dependencies: ServerDependencies) -> Self {
        Self { dependencies }
    }

    /// Must be called exactly once by the process owner, before accepting sessions.
    pub fn recover_startup(store: &Store) -> Result<RecoveryReport, ServerError> {
        Ok(RecoveryReport {
            tool_runs: store.recover_pending_tool_runs()?,
            turns: store.recover_pending_turns()?,
        })
    }

    pub async fn serve<R, W>(&self, reader: R, writer: W) -> Result<(), ServerError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut input = NdjsonReader::new(reader);
        let mut output = NdjsonWriter::new(writer);
        let (events_tx, mut events_rx) = mpsc::channel::<InternalEvent>(EVENT_QUEUE);
        let (broker, mut prompts_rx) = SessionConfirmationBroker::pair();
        let mut active: HashMap<DialogId, ActiveTurn> = HashMap::new();
        let mut background: Vec<JoinHandle<()>> = Vec::new();

        write(&mut output, RequestId::new(), ServerEvent::Hello).await?;
        let result: Result<(), ServerError> = async {
        loop {
            tokio::select! {
                request = input.read_request() => match request {
                    Ok(Some(request)) => self.handle_request(request, &mut output, &events_tx, &broker, &mut active, &mut background).await?,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = write(&mut output, RequestId::new(), ServerEvent::ProtocolError { code: error.code() }).await;
                        break;
                    }
                },
                Some(event) = events_rx.recv() => {
                    if let Some(dialog_id) = event.done { active.remove(&dialog_id); }
                    let result = write(&mut output, event.request_id, event.event).await;
                    let acknowledged = if result.is_ok() { Ok(()) } else { Err(()) };
                    if let Some(ack) = event.ack { let _ = ack.send(acknowledged); }
                    result?;
                }
                Some(prompt) = prompts_rx.recv() => {
                    let description = action_description(prompt.request.preview.action).to_owned();
                    write(&mut output, prompt.request_id, ServerEvent::ConfirmationRequired {
                        confirmation_id: prompt.request.id,
                        description,
                        prompt: prompt.request.preview.prompt,
                    }).await?;
                }
            }
        }
        Ok(())
        }.await;

        for turn in active.values() {
            turn.cancellation.cancel();
        }
        broker.clear();
        for task in background {
            task.abort();
        }
        drop(events_rx);
        for (_, turn) in active {
            let _ = turn.task.await;
        }
        result
    }

    async fn handle_request<W: AsyncWrite + Unpin>(
        &self,
        envelope: RequestEnvelope,
        output: &mut NdjsonWriter<W>,
        events: &mpsc::Sender<InternalEvent>,
        broker: &Arc<SessionConfirmationBroker>,
        active: &mut HashMap<DialogId, ActiveTurn>,
        background: &mut Vec<JoinHandle<()>>,
    ) -> Result<(), ServerError> {
        use crate::protocol::ClientRequest::*;
        background.retain(|task| !task.is_finished());
        let request_id = envelope.request_id;
        match envelope.request {
            ListDialogs => self.write_dialogs(output, request_id).await?,
            CreateDialog { title } => {
                if !valid_title(&title, self.dependencies.settings.max_message_bytes()) {
                    write_error(output, request_id, ProtocolErrorCode::ContentTooLong).await?;
                } else {
                    match self.dependencies.store.create_dialog(&title) {
                        Ok(dialog) => {
                            write(
                                output,
                                request_id,
                                ServerEvent::DialogOpened {
                                    dialog_id: dialog.id,
                                    title: dialog.title,
                                },
                            )
                            .await?
                        }
                        Err(_) => {
                            write_error(output, request_id, ProtocolErrorCode::InternalError)
                                .await?
                        }
                    }
                }
            }
            OpenDialog { dialog_id } => {
                match self
                    .dependencies
                    .store
                    .list_dialogs()?
                    .into_iter()
                    .find(|d| d.id == dialog_id)
                {
                    Some(dialog) => {
                        write(
                            output,
                            request_id,
                            ServerEvent::DialogOpened {
                                dialog_id,
                                title: dialog.title,
                            },
                        )
                        .await?
                    }
                    None => {
                        write_error(output, request_id, ProtocolErrorCode::InvalidRequest).await?
                    }
                }
            }
            RenameDialog { dialog_id, title } => {
                if !valid_title(&title, self.dependencies.settings.max_message_bytes()) {
                    write_error(output, request_id, ProtocolErrorCode::ContentTooLong).await?;
                } else if self
                    .dependencies
                    .store
                    .rename_dialog(dialog_id, &title)
                    .is_err()
                {
                    write_error(output, request_id, ProtocolErrorCode::InternalError).await?;
                } else {
                    write(
                        output,
                        request_id,
                        ServerEvent::DialogOpened { dialog_id, title },
                    )
                    .await?;
                }
            }
            DeleteDialog { dialog_id } => {
                if self.dependencies.store.delete_dialog(dialog_id).is_err() {
                    write_error(output, request_id, ProtocolErrorCode::InternalError).await?;
                } else {
                    self.write_dialogs(output, request_id).await?;
                }
            }
            SendMessage { dialog_id, message } => {
                let at_capacity = active.len() >= MAX_ACTIVE_TURNS;
                match active.entry(dialog_id) {
                    std::collections::hash_map::Entry::Occupied(_) => {
                        write(
                            output,
                            request_id,
                            ServerEvent::TurnFailed {
                                code: ProtocolErrorCode::InternalError,
                            },
                        )
                        .await?;
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        if at_capacity {
                            write(
                                output,
                                request_id,
                                ServerEvent::TurnFailed {
                                    code: ProtocolErrorCode::InternalError,
                                },
                            )
                            .await?;
                            return Ok(());
                        }
                        write(
                            output,
                            request_id,
                            ServerEvent::ResponseStarted { dialog_id },
                        )
                        .await?;
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
                                write(
                                    output,
                                    request_id,
                                    ServerEvent::TurnFailed {
                                        code: ProtocolErrorCode::InternalError,
                                    },
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
            ConfirmAction { confirmation_id } => {
                if broker.resolve(request_id, confirmation_id, true).is_err() {
                    write_error(output, request_id, ProtocolErrorCode::InvalidRequest).await?;
                }
            }
            CancelAction { confirmation_id } => {
                if broker.resolve(request_id, confirmation_id, false).is_err() {
                    write_error(output, request_id, ProtocolErrorCode::InvalidRequest).await?;
                }
            }
            Inspect { kind } => {
                if background.len() >= MAX_BACKGROUND_REQUESTS {
                    write_error(output, request_id, ProtocolErrorCode::InternalError).await?;
                } else {
                    background.push(spawn_inspection(
                        self.dependencies.inspection.clone(),
                        request_id,
                        kind,
                        events.clone(),
                    ));
                }
            }
            Export => {
                if background.len() >= MAX_BACKGROUND_REQUESTS {
                    write_error(output, request_id, ProtocolErrorCode::InternalError).await?;
                } else {
                    background.push(spawn_export(
                        self.dependencies.inspection.clone(),
                        request_id,
                        events.clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn write_dialogs<W: AsyncWrite + Unpin>(
        &self,
        output: &mut NdjsonWriter<W>,
        request_id: RequestId,
    ) -> Result<(), ServerError> {
        let dialogs = self
            .dependencies
            .store
            .list_dialogs()?
            .into_iter()
            .map(|d| crate::protocol::DialogSummary {
                id: d.id,
                title: d.title,
            })
            .collect();
        write(
            output,
            request_id,
            ServerEvent::DialogList {
                sequence: 0,
                dialogs,
                complete: true,
            },
        )
        .await
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
        let scheduler: Arc<dyn ToolExecutor> = Arc::new(SchedulerToolExecutor::new(
            self.dependencies.store.clone(),
            self.dependencies.synchronizer.clone(),
            broker,
            dialog_id,
            request_id,
        ));
        let tools = CompositeToolExecutor::new(vec![self.dependencies.mcp.clone(), scheduler])
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
            let final_event = match execution
                .run(
                    dialog_id,
                    &message,
                    cancellation,
                    &mut sink,
                    acknowledgements,
                )
                .await
            {
                Ok(answer) => ServerEvent::TurnCompleted { answer },
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
struct InternalEvent {
    request_id: RequestId,
    event: ServerEvent,
    done: Option<DialogId>,
    ack: Option<oneshot::Sender<Result<(), ()>>>,
}

type EventAcknowledgements = Arc<Mutex<Vec<oneshot::Receiver<Result<(), ()>>>>>;

struct TurnExecution {
    store: Store,
    runner: AgentRunner,
    system_prompt: String,
}

impl TurnExecution {
    async fn run(
        &self,
        dialog_id: DialogId,
        content: &str,
        cancellation: CancellationToken,
        sink: &mut crate::agent_runner::AgentEventSink<'_>,
        acknowledgements: EventAcknowledgements,
    ) -> Result<String, AgentError> {
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
            Ok(outcome) => {
                if let Err(error) = self.store.complete_turn(turn.turn_id, &outcome.answer) {
                    let _ = self
                        .store
                        .fail_turn(turn.turn_id, SafeErrorCode::InternalError);
                    return Err(AgentError::Store(error));
                }
                Ok(outcome.answer)
            }
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

async fn write<W: AsyncWrite + Unpin>(
    writer: &mut NdjsonWriter<W>,
    request_id: RequestId,
    event: ServerEvent,
) -> Result<(), ServerError> {
    writer
        .write_event(&ServerEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            event,
        })
        .await
        .map_err(Into::into)
}

async fn write_error<W: AsyncWrite + Unpin>(
    writer: &mut NdjsonWriter<W>,
    request_id: RequestId,
    code: ProtocolErrorCode,
) -> Result<(), ServerError> {
    write(writer, request_id, ServerEvent::ProtocolError { code }).await
}

fn valid_title(title: &str, max: usize) -> bool {
    !title.trim().is_empty() && title.len() <= max
}

fn action_description(action: ScheduleAction) -> &'static str {
    match action {
        ScheduleAction::Create => "create scheduled job",
        ScheduleAction::Update => "update scheduled job",
        ScheduleAction::Enable => "enable scheduled job",
        ScheduleAction::Disable => "disable scheduled job",
        ScheduleAction::Delete => "delete scheduled job",
    }
}

fn inspect_query(kind: &InspectKind) -> InspectQuery {
    match kind {
        InspectKind::Dialogs => InspectQuery::Dialogs,
        InspectKind::History { dialog_id } => InspectQuery::History(*dialog_id),
        InspectKind::Jobs => InspectQuery::Jobs,
        InspectKind::Job { job_id } => InspectQuery::Job(*job_id),
        InspectKind::Runs { job_id } => InspectQuery::Runs(*job_id),
        InspectKind::Audit => InspectQuery::Audit,
        InspectKind::Dump => InspectQuery::Dump,
    }
}

fn spawn_inspection(
    service: InspectionService,
    request_id: RequestId,
    kind: InspectKind,
    events: mpsc::Sender<InternalEvent>,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut snapshot = match service.snapshot(inspect_query(&kind)) {
            Ok(value) => value,
            Err(_) => {
                send_internal(
                    &events,
                    request_id,
                    ServerEvent::ProtocolError {
                        code: ProtocolErrorCode::InternalError,
                    },
                );
                return;
            }
        };
        let mut sequence = 0u64;
        let mut record_sequence = 0u64;
        loop {
            match snapshot.next_page() {
                Ok(Some(page)) => {
                    for item in page.items {
                        let bytes = match serde_json::to_vec(&item) {
                            Ok(bytes) => bytes,
                            Err(_) => return,
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
                            ) {
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
                Err(_) => return,
            }
        }
        let _ = snapshot.close();
        let _ = send_internal(
            &events,
            request_id,
            ServerEvent::InspectionResult {
                kind,
                sequence,
                items: Vec::new(),
                complete: true,
            },
        );
    })
}

fn send_internal(
    events: &mpsc::Sender<InternalEvent>,
    request_id: RequestId,
    event: ServerEvent,
) -> bool {
    events
        .blocking_send(InternalEvent {
            request_id,
            event,
            done: None,
            ack: None,
        })
        .is_ok()
}

struct ChunkWriter {
    request_id: RequestId,
    events: mpsc::Sender<InternalEvent>,
    sequence: u64,
    buffer: Vec<u8>,
}

impl ChunkWriter {
    fn flush_chunks(&mut self, all: bool) -> io::Result<()> {
        while self.buffer.len() >= EXPORT_CHUNK_BYTES || (all && !self.buffer.is_empty()) {
            let length = self.buffer.len().min(EXPORT_CHUNK_BYTES);
            let remainder = self.buffer.split_off(length);
            let chunk = std::mem::replace(&mut self.buffer, remainder);
            let event = ServerEvent::ExportChunk {
                sequence: self.sequence,
                data_base64: base64::engine::general_purpose::STANDARD.encode(chunk),
            };
            self.events
                .blocking_send(InternalEvent {
                    request_id: self.request_id,
                    event,
                    done: None,
                    ack: None,
                })
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "session_closed"))?;
            self.sequence += 1;
        }
        Ok(())
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        self.flush_chunks(false)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_chunks(true)
    }
}

fn spawn_export(
    service: InspectionService,
    request_id: RequestId,
    events: mpsc::Sender<InternalEvent>,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut writer = ChunkWriter {
            request_id,
            events: events.clone(),
            sequence: 0,
            buffer: Vec::with_capacity(EXPORT_CHUNK_BYTES),
        };
        let summary = match service.write_export(&mut writer) {
            Ok(value) => value,
            Err(_) => {
                let _ = send_internal(
                    &events,
                    request_id,
                    ServerEvent::ProtocolError {
                        code: ProtocolErrorCode::InternalError,
                    },
                );
                return;
            }
        };
        if writer.flush().is_err() {
            return;
        }
        send_internal(
            &events,
            request_id,
            ServerEvent::ExportCompleted {
                total_bytes: summary.total_bytes,
                sha256: summary.sha256,
            },
        );
    })
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
        let dir = tempfile::tempdir().unwrap();
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
        let dir = tempfile::tempdir().unwrap();
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
}
