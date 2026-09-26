//! Interactive terminal coordination and safe local export handling.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::domain::{ConfirmationId, DialogId, JobId, RequestId};
use crate::protocol::{
    ClientRequest, EXPORT_CHUNK_BYTES, InspectKind, MAX_CONTENT_BYTES, PROTOCOL_VERSION,
    ProtocolErrorCode, RequestEnvelope, ServerEnvelope, ServerEvent,
};
use crate::remote_client::RemoteSession;

pub use crate::remote_client::ClientError;

const INPUT_QUEUE: usize = 1;
const MAX_LABEL_BYTES: usize = 512;
const MAX_INSPECTION_RECORD_BYTES: usize = MAX_CONTENT_BYTES * 6 + 65_536;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientAction {
    Send(String),
    Exit,
    ListDialogs,
    NewDialog(String),
    OpenDialog(DialogId),
    RenameDialog(DialogId, String),
    DeleteDialog(DialogId),
    History(Option<DialogId>),
    Jobs,
    Job(JobId),
    Runs(JobId),
    Audit,
    Dump,
    Export(PathBuf),
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ClientInputError {
    #[error("invalid_command")]
    InvalidCommand,
    #[error("invalid_dialog_id")]
    InvalidDialogId,
    #[error("invalid_job_id")]
    InvalidJobId,
    #[error("invalid_export_path")]
    InvalidExportPath,
}

pub fn parse_terminal_input(input: &str) -> Result<ClientAction, ClientInputError> {
    let input = input.strip_suffix('\r').unwrap_or(input);
    if input.is_empty() || input.trim().is_empty() {
        return Err(ClientInputError::InvalidCommand);
    }
    if !input.starts_with('/') {
        if input.len() > MAX_CONTENT_BYTES {
            return Err(ClientInputError::InvalidCommand);
        }
        return Ok(ClientAction::Send(input.to_owned()));
    }
    let (command, rest) = input.split_once(' ').unwrap_or((input, ""));
    let no_arguments = || {
        if rest.is_empty() {
            Ok(())
        } else {
            Err(ClientInputError::InvalidCommand)
        }
    };
    match command {
        "/exit" => {
            no_arguments()?;
            Ok(ClientAction::Exit)
        }
        "/dialogs" => {
            no_arguments()?;
            Ok(ClientAction::ListDialogs)
        }
        "/new" => Ok(ClientAction::NewDialog(required_text(rest)?)),
        "/open" => Ok(ClientAction::OpenDialog(parse_dialog_id(rest)?)),
        "/rename" => {
            let (id, title) = rest
                .split_once(' ')
                .ok_or(ClientInputError::InvalidCommand)?;
            Ok(ClientAction::RenameDialog(
                parse_dialog_id(id)?,
                required_text(title)?,
            ))
        }
        "/delete" => Ok(ClientAction::DeleteDialog(parse_dialog_id(rest)?)),
        "/history" if rest.is_empty() => Ok(ClientAction::History(None)),
        "/history" if !rest.contains(char::is_whitespace) => {
            Ok(ClientAction::History(Some(parse_dialog_id(rest)?)))
        }
        "/jobs" => {
            no_arguments()?;
            Ok(ClientAction::Jobs)
        }
        "/job" => Ok(ClientAction::Job(parse_job_id(rest)?)),
        "/runs" => Ok(ClientAction::Runs(parse_job_id(rest)?)),
        "/audit" => {
            no_arguments()?;
            Ok(ClientAction::Audit)
        }
        "/dump" => {
            no_arguments()?;
            Ok(ClientAction::Dump)
        }
        "/export" => {
            let value = required_text(rest)?;
            if value.chars().any(char::is_control) {
                return Err(ClientInputError::InvalidExportPath);
            }
            let path = PathBuf::from(value);
            if path.file_name().is_none()
                || path.components().any(|part| part == Component::ParentDir)
            {
                return Err(ClientInputError::InvalidExportPath);
            }
            Ok(ClientAction::Export(path))
        }
        _ => Err(ClientInputError::InvalidCommand),
    }
}

fn required_text(value: &str) -> Result<String, ClientInputError> {
    if value.is_empty() || value.trim().is_empty() || value.len() > MAX_CONTENT_BYTES {
        Err(ClientInputError::InvalidCommand)
    } else {
        Ok(value.to_owned())
    }
}

fn parse_dialog_id(value: &str) -> Result<DialogId, ClientInputError> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(ClientInputError::InvalidDialogId);
    }
    value
        .parse::<i64>()
        .map_err(|_| ClientInputError::InvalidDialogId)
        .and_then(|id| DialogId::new(id).map_err(|_| ClientInputError::InvalidDialogId))
}

fn parse_job_id(value: &str) -> Result<JobId, ClientInputError> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(ClientInputError::InvalidJobId);
    }
    JobId::from_str(value).map_err(|_| ClientInputError::InvalidJobId)
}

pub fn confirmation_accepts(input: &str) -> bool {
    matches!(input, "y" | "Y")
}

pub fn render_confirmation_preview(description: &str, prompt: &str) -> String {
    format!(
        "Confirm {}:\n{}\n[y/N] ",
        escape_terminal_bounded(description, MAX_LABEL_BYTES),
        escape_terminal(prompt)
    )
}

fn escape_terminal(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() {
        match character {
            '\n' => escaped.push('\n'),
            '\t' => escaped.push('\t'),
            character if character.is_control() => {
                escaped.extend(character.escape_default());
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn escape_terminal_bounded(value: &str, maximum: usize) -> String {
    let mut result = String::new();
    for character in value.chars() {
        let escaped = character.escape_default().to_string();
        if result.len() + escaped.len() > maximum {
            result.push_str("...");
            break;
        }
        result.push_str(&escaped);
    }
    result
}

pub struct AtomicExport {
    destination: PathBuf,
    temporary: PathBuf,
    file: Option<File>,
    sequence: u64,
    total_bytes: u64,
    digest: Sha256,
    committed: bool,
}

impl AtomicExport {
    pub fn start(destination: impl AsRef<Path>) -> Result<Self, ClientError> {
        let destination = destination.as_ref().to_owned();
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let name = destination
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or(ClientError::Export)?;
        let temporary = parent.join(format!(
            ".{name}.light-agent-export-{}.part",
            uuid::Uuid::new_v4()
        ));
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&temporary).map_err(|_| ClientError::Export)?;
        Ok(Self {
            destination,
            temporary,
            file: Some(file),
            sequence: 0,
            total_bytes: 0,
            digest: Sha256::new(),
            committed: false,
        })
    }

    pub fn push_chunk(&mut self, sequence: u64, encoded: &str) -> Result<(), ClientError> {
        if sequence != self.sequence {
            return Err(ClientError::Protocol);
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| ClientError::Protocol)?;
        if bytes.len() > EXPORT_CHUNK_BYTES {
            return Err(ClientError::Protocol);
        }
        let total = self
            .total_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(ClientError::Export)?;
        self.file
            .as_mut()
            .ok_or(ClientError::Export)?
            .write_all(&bytes)
            .map_err(|_| ClientError::Export)?;
        self.digest.update(&bytes);
        self.total_bytes = total;
        self.sequence += 1;
        Ok(())
    }

    pub fn complete(mut self, total_bytes: u64, sha256: &str) -> Result<(), ClientError> {
        let actual = format!("{:x}", self.digest.clone().finalize());
        if total_bytes != self.total_bytes || sha256 != actual {
            return Err(ClientError::Protocol);
        }
        let mut file = self.file.take().ok_or(ClientError::Export)?;
        file.flush().map_err(|_| ClientError::Export)?;
        file.sync_all().map_err(|_| ClientError::Export)?;
        drop(file);
        std::fs::rename(&self.temporary, &self.destination).map_err(|_| ClientError::Export)?;
        if let Some(parent) = self.destination.parent()
            && let Ok(directory) = File::open(parent)
        {
            directory.sync_all().map_err(|_| ClientError::Export)?;
        }
        self.committed = true;
        Ok(())
    }
}

impl Drop for AtomicExport {
    fn drop(&mut self) {
        if !self.committed {
            self.file.take();
            let _ = std::fs::remove_file(&self.temporary);
        }
    }
}

pub struct TerminalClient;

impl TerminalClient {
    pub async fn run<R, W, I, O, E>(
        mut session: RemoteSession<R, W>,
        input: I,
        mut output: O,
        mut error: E,
    ) -> Result<(), ClientError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        I: AsyncRead + Unpin + Send + 'static,
        O: AsyncWrite + Unpin,
        E: AsyncWrite + Unpin,
    {
        let hello = session.event().await?.ok_or(ClientError::TransportClosed)?;
        if !matches!(hello.event, ServerEvent::Hello) {
            return Err(ClientError::Protocol);
        }
        output
            .write_all(b"connected\n")
            .await
            .map_err(|_| ClientError::Output)?;

        let (input_tx, mut input_rx) = mpsc::channel(INPUT_QUEUE);
        let input_task = tokio::spawn(read_terminal_input(input, input_tx));
        let mut state = ClientState::default();
        let result = loop {
            tokio::select! {
                input = input_rx.recv() => {
                    match input {
                        Some(TerminalInput::Line(line)) => {
                            if let Some(confirmation) = state.confirmation.take() {
                                let accepted = confirmation_accepts(&line);
                                let request = if accepted {
                                    ClientRequest::ConfirmAction { confirmation_id: confirmation.confirmation_id }
                                } else {
                                    ClientRequest::CancelAction { confirmation_id: confirmation.confirmation_id }
                                };
                                if let Err(failure) = send_request(&mut session, confirmation.request_id, request).await {
                                    break Err(failure);
                                }
                            } else {
                                match parse_terminal_input(&line) {
                                    Ok(ClientAction::Exit) => break Ok(()),
                                    Ok(action) => {
                                        if let Err(failure) = submit_action(&mut session, &mut state, action).await {
                                            if failure == ClientError::NoActiveDialog || failure == ClientError::Input {
                                                render_error(&mut error, failure).await?;
                                            } else {
                                                break Err(failure);
                                            }
                                        }
                                    }
                                    Err(_) => render_error(&mut error, ClientError::Input).await?,
                                }
                            }
                        }
                        Some(TerminalInput::Eof) | None => {
                            if let Some(confirmation) = state.confirmation.take() {
                                let request = ClientRequest::CancelAction { confirmation_id: confirmation.confirmation_id };
                                let _ = send_request(&mut session, confirmation.request_id, request).await;
                            }
                            break Ok(());
                        }
                        Some(TerminalInput::Error) => break Err(ClientError::Input),
                    }
                }
                event = session.event() => {
                    match event {
                        Ok(Some(envelope)) => {
                            if let Err(failure) = handle_event(&mut state, envelope, &mut output, &mut error).await {
                                break Err(failure);
                            }
                        }
                        Ok(None) => break Err(ClientError::TransportClosed),
                        Err(failure) => break Err(failure),
                    }
                }
            }
        };

        input_task.abort();
        let _ = input_task.await;
        state.pending.clear();
        let _ = session.shutdown().await;
        let process = session.finish().await;
        match (result, process) {
            (Ok(()), Ok(())) => Ok(()),
            (_, Err(failure)) => Err(failure),
            (Err(failure), _) => Err(failure),
        }
    }
}

enum TerminalInput {
    Line(String),
    Eof,
    Error,
}

async fn read_terminal_input<I>(input: I, output: mpsc::Sender<TerminalInput>)
where
    I: AsyncRead + Unpin,
{
    let mut input = BufReader::new(input);
    let mut line = Vec::new();
    loop {
        match read_bounded_terminal_line(&mut input, &mut line).await {
            Ok(None) => {
                let _ = output.send(TerminalInput::Eof).await;
                return;
            }
            Ok(Some(line)) => {
                if output.send(TerminalInput::Line(line)).await.is_err() {
                    return;
                }
            }
            Err(()) => {
                let _ = output.send(TerminalInput::Error).await;
                return;
            }
        }
    }
}

async fn read_bounded_terminal_line<R>(
    input: &mut R,
    line: &mut Vec<u8>,
) -> Result<Option<String>, ()>
where
    R: AsyncBufRead + Unpin,
{
    line.clear();
    loop {
        let available = input.fill_buf().await.map_err(|_| ())?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            if line.len() > MAX_CONTENT_BYTES {
                return Err(());
            }
            return String::from_utf8(line.clone()).map(Some).map_err(|_| ());
        }
        let count = available
            .iter()
            .position(|&byte| byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if count > MAX_CONTENT_BYTES + 1 - line.len() {
            return Err(());
        }
        let complete = available[count - 1] == b'\n';
        line.extend_from_slice(&available[..count]);
        input.consume(count);
        if complete {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return String::from_utf8(line.clone()).map(Some).map_err(|_| ());
        }
    }
}

#[derive(Default)]
struct ClientState {
    active_dialog: Option<DialogId>,
    pending: HashMap<RequestId, PendingRequest>,
    confirmation: Option<PendingConfirmation>,
}

struct PendingConfirmation {
    request_id: RequestId,
    confirmation_id: ConfirmationId,
}

enum PendingRequest {
    Dialogs {
        sequence: u64,
        deleted: Option<DialogId>,
    },
    DialogMutation {
        activate: bool,
    },
    Turn {
        dialog_id: DialogId,
        prepared: bool,
    },
    Inspection(InspectionAssembler),
    Export(AtomicExport),
}

async fn submit_action<R, W>(
    session: &mut RemoteSession<R, W>,
    state: &mut ClientState,
    action: ClientAction,
) -> Result<(), ClientError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let request_id = RequestId::new();
    let (request, pending) = match action {
        ClientAction::Send(message) => {
            let dialog_id = state.active_dialog.ok_or(ClientError::NoActiveDialog)?;
            (
                ClientRequest::SendMessage { dialog_id, message },
                PendingRequest::Turn {
                    dialog_id,
                    prepared: false,
                },
            )
        }
        ClientAction::ListDialogs => (
            ClientRequest::ListDialogs,
            PendingRequest::Dialogs {
                sequence: 0,
                deleted: None,
            },
        ),
        ClientAction::NewDialog(title) => (
            ClientRequest::CreateDialog { title },
            PendingRequest::DialogMutation { activate: true },
        ),
        ClientAction::OpenDialog(dialog_id) => (
            ClientRequest::OpenDialog { dialog_id },
            PendingRequest::DialogMutation { activate: true },
        ),
        ClientAction::RenameDialog(dialog_id, title) => (
            ClientRequest::RenameDialog { dialog_id, title },
            PendingRequest::DialogMutation { activate: false },
        ),
        ClientAction::DeleteDialog(dialog_id) => (
            ClientRequest::DeleteDialog { dialog_id },
            PendingRequest::Dialogs {
                sequence: 0,
                deleted: Some(dialog_id),
            },
        ),
        ClientAction::History(dialog_id) => inspection_request(InspectKind::History { dialog_id }),
        ClientAction::Jobs => inspection_request(InspectKind::Jobs),
        ClientAction::Job(job_id) => inspection_request(InspectKind::Job { job_id }),
        ClientAction::Runs(job_id) => inspection_request(InspectKind::Runs { job_id }),
        ClientAction::Audit => inspection_request(InspectKind::Audit),
        ClientAction::Dump => inspection_request(InspectKind::Dump),
        ClientAction::Export(path) => (
            ClientRequest::Export,
            PendingRequest::Export(AtomicExport::start(path)?),
        ),
        ClientAction::Exit => return Ok(()),
    };
    send_request(session, request_id, request).await?;
    state.pending.insert(request_id, pending);
    Ok(())
}

fn inspection_request(kind: InspectKind) -> (ClientRequest, PendingRequest) {
    (
        ClientRequest::Inspect { kind: kind.clone() },
        PendingRequest::Inspection(InspectionAssembler::new(kind)),
    )
}

async fn send_request<R, W>(
    session: &mut RemoteSession<R, W>,
    request_id: RequestId,
    request: ClientRequest,
) -> Result<(), ClientError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    session
        .send(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            request,
        })
        .await
}

async fn handle_event<O, E>(
    state: &mut ClientState,
    envelope: ServerEnvelope,
    output: &mut O,
    error: &mut E,
) -> Result<(), ClientError>
where
    O: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    let request_id = envelope.request_id;
    if matches!(envelope.event, ServerEvent::Hello) {
        return Err(ClientError::Protocol);
    }
    let pending = state
        .pending
        .get_mut(&request_id)
        .ok_or(ClientError::Protocol)?;
    let mut finished = false;
    match (&mut *pending, envelope.event) {
        (
            PendingRequest::Dialogs { sequence, deleted },
            ServerEvent::DialogList {
                sequence: actual,
                dialogs,
                complete,
            },
        ) => {
            if actual != *sequence {
                return Err(ClientError::Protocol);
            }
            *sequence += 1;
            for dialog in dialogs {
                let line = format!(
                    "{} {}\n",
                    dialog.id,
                    escape_terminal_bounded(&dialog.title, MAX_LABEL_BYTES)
                );
                output
                    .write_all(line.as_bytes())
                    .await
                    .map_err(|_| ClientError::Output)?;
            }
            if deleted.is_some_and(|deleted| Some(deleted) == state.active_dialog) {
                state.active_dialog = None;
            }
            finished = complete;
        }
        (
            PendingRequest::DialogMutation { activate },
            ServerEvent::DialogOpened { dialog_id, title },
        ) => {
            if *activate {
                state.active_dialog = Some(dialog_id);
            }
            let verb = if *activate { "opened" } else { "renamed" };
            let line = format!(
                "{verb} {} {}\n",
                dialog_id,
                escape_terminal_bounded(&title, MAX_LABEL_BYTES)
            );
            output
                .write_all(line.as_bytes())
                .await
                .map_err(|_| ClientError::Output)?;
            finished = true;
        }
        (
            PendingRequest::Turn {
                dialog_id: expected,
                ..
            },
            ServerEvent::ResponseStarted { dialog_id },
        ) if dialog_id == *expected => {}
        (PendingRequest::Turn { .. }, ServerEvent::TextDelta { text }) => {
            output
                .write_all(text.as_bytes())
                .await
                .map_err(|_| ClientError::Output)?;
            output.flush().await.map_err(|_| ClientError::Output)?;
        }
        (PendingRequest::Turn { .. }, ServerEvent::ToolStarted { name }) => {
            let line = format!(
                "tool started: {}\n",
                escape_terminal_bounded(&name, MAX_LABEL_BYTES)
            );
            error
                .write_all(line.as_bytes())
                .await
                .map_err(|_| ClientError::Output)?;
        }
        (PendingRequest::Turn { .. }, ServerEvent::ToolFinished { name, code }) => {
            let line = format!(
                "tool finished: {} {}\n",
                escape_terminal_bounded(&name, MAX_LABEL_BYTES),
                safe_code(code)
            );
            error
                .write_all(line.as_bytes())
                .await
                .map_err(|_| ClientError::Output)?;
        }
        (
            PendingRequest::Turn { .. },
            ServerEvent::ConfirmationRequired {
                confirmation_id,
                description,
                prompt,
            },
        ) => {
            if state.confirmation.is_some() {
                return Err(ClientError::Protocol);
            }
            let preview = render_confirmation_preview(&description, &prompt);
            output
                .write_all(preview.as_bytes())
                .await
                .map_err(|_| ClientError::Output)?;
            output.flush().await.map_err(|_| ClientError::Output)?;
            state.confirmation = Some(PendingConfirmation {
                request_id,
                confirmation_id,
            });
        }
        (PendingRequest::Turn { prepared, .. }, ServerEvent::TurnPrepared { .. }) => {
            if *prepared {
                return Err(ClientError::Protocol);
            }
            *prepared = true;
            error
                .write_all(b"answer delivered; awaiting durable commit\n")
                .await
                .map_err(|_| ClientError::Output)?;
        }
        (PendingRequest::Turn { prepared, .. }, ServerEvent::TurnCompleted { .. }) => {
            if !*prepared {
                return Err(ClientError::Protocol);
            }
            output
                .write_all(b"\nturn completed\n")
                .await
                .map_err(|_| ClientError::Output)?;
            finished = true;
        }
        (PendingRequest::Turn { .. }, ServerEvent::TurnFailed { code }) => {
            let line = format!("turn failed: {}\n", safe_code(code));
            error
                .write_all(line.as_bytes())
                .await
                .map_err(|_| ClientError::Output)?;
            finished = true;
        }
        (
            PendingRequest::Inspection(assembler),
            ServerEvent::InspectionResult {
                kind,
                sequence,
                items,
                complete,
            },
        ) => {
            let records = assembler.push(kind, sequence, items, complete)?;
            for record in records {
                let mut line = serde_json::to_vec(&record).map_err(|_| ClientError::Protocol)?;
                line.push(b'\n');
                output
                    .write_all(&line)
                    .await
                    .map_err(|_| ClientError::Output)?;
            }
            finished = complete;
        }
        (
            PendingRequest::Export(export),
            ServerEvent::ExportChunk {
                sequence,
                data_base64,
            },
        ) => {
            export.push_chunk(sequence, &data_base64)?;
        }
        (
            PendingRequest::Export(_),
            ServerEvent::ExportCompleted {
                total_bytes,
                sha256,
            },
        ) => {
            let PendingRequest::Export(export) = state
                .pending
                .remove(&request_id)
                .ok_or(ClientError::Protocol)?
            else {
                unreachable!()
            };
            export.complete(total_bytes, &sha256)?;
            output
                .write_all(b"export completed\n")
                .await
                .map_err(|_| ClientError::Output)?;
            return Ok(());
        }
        (_, ServerEvent::ProtocolError { code }) => {
            let line = format!("request failed: {}\n", safe_code(code));
            error
                .write_all(line.as_bytes())
                .await
                .map_err(|_| ClientError::Output)?;
            finished = true;
        }
        _ => return Err(ClientError::Protocol),
    }
    if finished {
        if state
            .confirmation
            .as_ref()
            .is_some_and(|confirmation| confirmation.request_id == request_id)
        {
            state.confirmation = None;
        }
        state.pending.remove(&request_id);
    }
    Ok(())
}

async fn render_error<E: AsyncWrite + Unpin>(
    error: &mut E,
    failure: ClientError,
) -> Result<(), ClientError> {
    let line = format!("{failure}\n");
    error
        .write_all(line.as_bytes())
        .await
        .map_err(|_| ClientError::Output)
}

fn safe_code(code: ProtocolErrorCode) -> &'static str {
    match code {
        ProtocolErrorCode::Ok => "ok",
        ProtocolErrorCode::InvalidRequest => "invalid_request",
        ProtocolErrorCode::InvalidEvent => "invalid_event",
        ProtocolErrorCode::UnsupportedVersion => "unsupported_version",
        ProtocolErrorCode::LineTooLong => "line_too_long",
        ProtocolErrorCode::ContentTooLong => "content_too_long",
        ProtocolErrorCode::ChunkTooLong => "chunk_too_long",
        ProtocolErrorCode::IoError => "io_error",
        ProtocolErrorCode::InternalError => "internal_error",
    }
}

struct InspectionAssembler {
    kind: InspectKind,
    sequence: u64,
    record_sequence: u64,
    fragment_sequence: u64,
    record: Vec<u8>,
}

impl InspectionAssembler {
    fn new(kind: InspectKind) -> Self {
        Self {
            kind,
            sequence: 0,
            record_sequence: 0,
            fragment_sequence: 0,
            record: Vec::new(),
        }
    }

    fn push(
        &mut self,
        kind: InspectKind,
        sequence: u64,
        items: Vec<Value>,
        complete: bool,
    ) -> Result<Vec<Value>, ClientError> {
        if kind != self.kind || sequence != self.sequence {
            return Err(ClientError::Protocol);
        }
        self.sequence += 1;
        if complete {
            if !items.is_empty() || !self.record.is_empty() {
                return Err(ClientError::Protocol);
            }
            return Ok(Vec::new());
        }
        if items.len() != 1 {
            return Err(ClientError::Protocol);
        }
        let fragment: FragmentEnvelope = serde_json::from_value(items.into_iter().next().unwrap())
            .map_err(|_| ClientError::Protocol)?;
        let fragment = fragment.record_fragment;
        if fragment.record_sequence != self.record_sequence
            || fragment.fragment_sequence != self.fragment_sequence
            || fragment.encoding != "base64"
        {
            return Err(ClientError::Protocol);
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(fragment.data)
            .map_err(|_| ClientError::Protocol)?;
        if self.record.len() + bytes.len() > MAX_INSPECTION_RECORD_BYTES {
            return Err(ClientError::Protocol);
        }
        self.record.extend_from_slice(&bytes);
        self.fragment_sequence += 1;
        if !fragment.complete {
            return Ok(Vec::new());
        }
        let record = serde_json::from_slice(&self.record).map_err(|_| ClientError::Protocol)?;
        self.record.clear();
        self.record_sequence += 1;
        self.fragment_sequence = 0;
        Ok(vec![record])
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FragmentEnvelope {
    record_fragment: RecordFragment,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordFragment {
    record_sequence: u64,
    fragment_sequence: u64,
    complete: bool,
    encoding: String,
    data: String,
}
