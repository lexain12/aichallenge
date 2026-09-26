//! Versioned, bounded NDJSON wire types for the SSH stdio session.

use std::io::{self, Write};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::domain::{ConfirmationId, DialogId, JobId, RequestId};

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_LINE_BYTES: usize = 1_048_576;
pub const MAX_CONTENT_BYTES: usize = 262_144;
pub const EXPORT_CHUNK_BYTES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    Ok,
    InvalidRequest,
    InvalidEvent,
    UnsupportedVersion,
    LineTooLong,
    ContentTooLong,
    ChunkTooLong,
    IoError,
    InternalError,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ProtocolError {
    #[error("invalid_request")]
    InvalidRequest,
    #[error("invalid_event")]
    InvalidEvent,
    #[error("unsupported_version")]
    UnsupportedVersion,
    #[error("line_too_long")]
    LineTooLong,
    #[error("content_too_long")]
    ContentTooLong,
    #[error("chunk_too_long")]
    ChunkTooLong,
    #[error("io_error")]
    IoError,
}

impl ProtocolError {
    pub fn code(&self) -> ProtocolErrorCode {
        match self {
            Self::InvalidRequest => ProtocolErrorCode::InvalidRequest,
            Self::InvalidEvent => ProtocolErrorCode::InvalidEvent,
            Self::UnsupportedVersion => ProtocolErrorCode::UnsupportedVersion,
            Self::LineTooLong => ProtocolErrorCode::LineTooLong,
            Self::ContentTooLong => ProtocolErrorCode::ContentTooLong,
            Self::ChunkTooLong => ProtocolErrorCode::ChunkTooLong,
            Self::IoError => ProtocolErrorCode::IoError,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestEnvelope {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub request: ClientRequest,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerEnvelope {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub event: ServerEvent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientRequest {
    ListDialogs,
    CreateDialog {
        title: String,
    },
    OpenDialog {
        dialog_id: DialogId,
    },
    RenameDialog {
        dialog_id: DialogId,
        title: String,
    },
    DeleteDialog {
        dialog_id: DialogId,
    },
    SendMessage {
        dialog_id: DialogId,
        message: String,
    },
    ConfirmAction {
        confirmation_id: ConfirmationId,
    },
    CancelAction {
        confirmation_id: ConfirmationId,
    },
    Inspect {
        kind: InspectKind,
    },
    Export,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InspectKind {
    Dialogs,
    History {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dialog_id: Option<DialogId>,
    },
    Jobs,
    Job {
        job_id: JobId,
    },
    Runs {
        job_id: JobId,
    },
    Audit,
    Dump,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionPayload {
    pub kind: InspectKind,
    pub data: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogSummary {
    pub id: DialogId,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerEvent {
    Hello,
    DialogList {
        sequence: u64,
        dialogs: Vec<DialogSummary>,
        complete: bool,
    },
    DialogOpened {
        dialog_id: DialogId,
        title: String,
    },
    ResponseStarted {
        dialog_id: DialogId,
    },
    TextDelta {
        text: String,
    },
    ToolStarted {
        name: String,
    },
    ToolFinished {
        name: String,
        code: ProtocolErrorCode,
    },
    ConfirmationRequired {
        confirmation_id: ConfirmationId,
        description: String,
        prompt: String,
    },
    /// The full answer has reached the transport, but the durable turn has not
    /// yet been committed. Clients must wait for `turn_completed`.
    TurnPrepared {
        answer: String,
    },
    TurnCompleted {
        answer: String,
    },
    TurnFailed {
        code: ProtocolErrorCode,
    },
    InspectionResult {
        kind: InspectKind,
        sequence: u64,
        items: Vec<Value>,
        complete: bool,
    },
    ExportChunk {
        sequence: u64,
        data_base64: String,
    },
    ExportCompleted {
        total_bytes: u64,
        sha256: String,
    },
    ProtocolError {
        code: ProtocolErrorCode,
    },
}

impl RequestEnvelope {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion);
        }
        match &self.request {
            ClientRequest::SendMessage { message, .. } if message.len() > MAX_CONTENT_BYTES => {
                Err(ProtocolError::ContentTooLong)
            }
            _ => Ok(()),
        }
    }
}

impl ServerEnvelope {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion);
        }
        match &self.event {
            ServerEvent::TextDelta { text } if text.len() > MAX_CONTENT_BYTES => {
                Err(ProtocolError::ContentTooLong)
            }
            ServerEvent::ConfirmationRequired { prompt, .. }
                if prompt.len() > MAX_CONTENT_BYTES =>
            {
                Err(ProtocolError::ContentTooLong)
            }
            ServerEvent::TurnPrepared { answer } | ServerEvent::TurnCompleted { answer }
                if answer.len() > MAX_CONTENT_BYTES =>
            {
                Err(ProtocolError::ContentTooLong)
            }
            ServerEvent::ExportChunk { data_base64, .. } => {
                let max_encoded = EXPORT_CHUNK_BYTES.div_ceil(3) * 4;
                if data_base64.len() > max_encoded {
                    return Err(ProtocolError::ChunkTooLong);
                }
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(data_base64)
                    .map_err(|_| ProtocolError::InvalidEvent)?;
                if bytes.len() > EXPORT_CHUNK_BYTES {
                    Err(ProtocolError::ChunkTooLong)
                } else {
                    Ok(())
                }
            }
            ServerEvent::ExportCompleted { sha256, .. }
                if sha256.len() != 64
                    || !sha256
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) =>
            {
                Err(ProtocolError::InvalidEvent)
            }
            _ => Ok(()),
        }
    }
}

/// Keeps at most one complete wire line in memory. A protocol failure closes the session.
pub struct NdjsonReader<R> {
    input: BufReader<R>,
    line: Vec<u8>,
}

impl<R: AsyncRead + Unpin> NdjsonReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            input: BufReader::new(reader),
            line: Vec::new(),
        }
    }

    pub fn buffer_capacity(&self) -> usize {
        self.line.capacity()
    }

    async fn read_line(&mut self) -> Result<Option<&[u8]>, ProtocolError> {
        self.line.clear();
        loop {
            let available = self
                .input
                .fill_buf()
                .await
                .map_err(|_| ProtocolError::IoError)?;
            if available.is_empty() {
                return if self.line.is_empty() {
                    Ok(None)
                } else {
                    Err(ProtocolError::InvalidRequest)
                };
            }
            let count = available
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(available.len(), |i| i + 1);
            if count > MAX_LINE_BYTES - self.line.len() {
                return Err(ProtocolError::LineTooLong);
            }
            let complete = available[count - 1] == b'\n';
            self.line.extend_from_slice(&available[..count]);
            self.input.consume(count);
            if complete {
                self.line.pop();
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                return Ok(Some(&self.line));
            }
        }
    }

    pub async fn read_request(&mut self) -> Result<Option<RequestEnvelope>, ProtocolError> {
        let Some(line) = self.read_line().await? else {
            return Ok(None);
        };
        let request: RequestEnvelope =
            serde_json::from_slice(line).map_err(|_| ProtocolError::InvalidRequest)?;
        request.validate()?;
        let original: Value =
            serde_json::from_slice(line).map_err(|_| ProtocolError::InvalidRequest)?;
        if serde_json::to_value(&request).map_err(|_| ProtocolError::InvalidRequest)? != original {
            return Err(ProtocolError::InvalidRequest);
        }
        Ok(Some(request))
    }

    pub async fn read_event(&mut self) -> Result<Option<ServerEnvelope>, ProtocolError> {
        let Some(line) = self.read_line().await? else {
            return Ok(None);
        };
        let event: ServerEnvelope =
            serde_json::from_slice(line).map_err(|_| ProtocolError::InvalidEvent)?;
        event.validate()?;
        let original: Value =
            serde_json::from_slice(line).map_err(|_| ProtocolError::InvalidEvent)?;
        if serde_json::to_value(&event).map_err(|_| ProtocolError::InvalidEvent)? != original {
            return Err(ProtocolError::InvalidEvent);
        }
        Ok(Some(event))
    }
}

pub struct NdjsonWriter<W> {
    output: W,
}

impl<W: AsyncWrite + Unpin> NdjsonWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { output: writer }
    }

    pub async fn write_event(&mut self, event: &ServerEnvelope) -> Result<(), ProtocolError> {
        event.validate()?;
        let bytes = serialize_bounded(event)?;
        self.output
            .write_all(&bytes)
            .await
            .map_err(|_| ProtocolError::IoError)?;
        self.output
            .write_all(b"\n")
            .await
            .map_err(|_| ProtocolError::IoError)?;
        self.output
            .flush()
            .await
            .map_err(|_| ProtocolError::IoError)
    }

    pub async fn write_request(&mut self, request: &RequestEnvelope) -> Result<(), ProtocolError> {
        request.validate()?;
        let bytes = serialize_bounded(request)?;
        self.output
            .write_all(&bytes)
            .await
            .map_err(|_| ProtocolError::IoError)?;
        self.output
            .write_all(b"\n")
            .await
            .map_err(|_| ProtocolError::IoError)?;
        self.output
            .flush()
            .await
            .map_err(|_| ProtocolError::IoError)
    }

    /// Closes only this protocol direction while allowing the peer's remaining
    /// events to be drained. This matters for split duplex/SSH streams.
    pub async fn shutdown(&mut self) -> Result<(), ProtocolError> {
        self.output
            .shutdown()
            .await
            .map_err(|_| ProtocolError::IoError)
    }
}

fn serialize_bounded<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let mut buffer = BoundedBuffer(Vec::new());
    serde_json::to_writer(&mut buffer, value).map_err(|_| ProtocolError::LineTooLong)?;
    Ok(buffer.0)
}

struct BoundedBuffer(Vec<u8>);

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_LINE_BYTES - 1 - self.0.len() {
            return Err(io::Error::other("line too long"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
