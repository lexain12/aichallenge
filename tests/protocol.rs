use base64::Engine as _;
use deepseek_cli::domain::{ConfirmationId, DialogId, JobId, RequestId};
use deepseek_cli::protocol::{
    ClientRequest, DialogSummary, EXPORT_CHUNK_BYTES, InspectKind, MAX_CONTENT_BYTES,
    MAX_LINE_BYTES, NdjsonReader, NdjsonWriter, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode,
    RequestEnvelope, ServerEnvelope, ServerEvent,
};
use serde_json::json;
use tokio::io::AsyncReadExt;

fn dialog_id() -> DialogId {
    DialogId::new(1).unwrap()
}

fn request_id() -> RequestId {
    "9cf50f62-386f-42d4-8fe5-2b95f7c43283".parse().unwrap()
}

fn confirmation_id() -> ConfirmationId {
    "e5ef2ca9-ded6-4a2d-a096-36563ec6fc3d".parse().unwrap()
}

fn job_id() -> JobId {
    "34479b6c-1a81-43b0-a514-c11743d09afa".parse().unwrap()
}

#[test]
fn round_trips_every_request_and_event() {
    let requests = [
        ClientRequest::ListDialogs,
        ClientRequest::CreateDialog {
            title: "Work".into(),
        },
        ClientRequest::OpenDialog {
            dialog_id: dialog_id(),
        },
        ClientRequest::RenameDialog {
            dialog_id: dialog_id(),
            title: "New".into(),
        },
        ClientRequest::DeleteDialog {
            dialog_id: dialog_id(),
        },
        ClientRequest::SendMessage {
            dialog_id: dialog_id(),
            message: "Hi".into(),
        },
        ClientRequest::ConfirmAction {
            confirmation_id: confirmation_id(),
        },
        ClientRequest::CancelAction {
            confirmation_id: confirmation_id(),
        },
        ClientRequest::Inspect {
            kind: InspectKind::Job { job_id: job_id() },
        },
        ClientRequest::Export,
    ];
    for request in requests {
        let envelope = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id(),
            request,
        };
        let bytes = serde_json::to_vec(&envelope).unwrap();
        let decoded: RequestEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, envelope);
    }

    let events = [
        ServerEvent::Hello,
        ServerEvent::DialogList {
            sequence: 0,
            dialogs: vec![DialogSummary {
                id: dialog_id(),
                title: "Work".into(),
            }],
            complete: true,
        },
        ServerEvent::DialogOpened {
            dialog_id: dialog_id(),
            title: "Work".into(),
        },
        ServerEvent::ResponseStarted {
            dialog_id: dialog_id(),
        },
        ServerEvent::TextDelta {
            text: "Hello".into(),
        },
        ServerEvent::ToolStarted {
            name: "example__read".into(),
        },
        ServerEvent::ToolFinished {
            name: "example__read".into(),
            code: ProtocolErrorCode::Ok,
        },
        ServerEvent::ConfirmationRequired {
            confirmation_id: confirmation_id(),
            description: "Run daily".into(),
            prompt: "Summarize".into(),
        },
        ServerEvent::TurnPrepared {
            answer: "Hello".into(),
        },
        ServerEvent::TurnCompleted {
            answer: "Hello".into(),
        },
        ServerEvent::TurnFailed {
            code: ProtocolErrorCode::InternalError,
        },
        ServerEvent::InspectionResult {
            kind: InspectKind::Dialogs,
            sequence: 0,
            items: vec![json!({"id": 1})],
            complete: true,
        },
        ServerEvent::ExportChunk {
            sequence: 0,
            data_base64: "aGk=".into(),
        },
        ServerEvent::ExportCompleted {
            total_bytes: 2,
            sha256: "8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4".into(),
        },
        ServerEvent::ProtocolError {
            code: ProtocolErrorCode::InvalidRequest,
        },
    ];
    for event in events {
        let envelope = ServerEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id(),
            event,
        };
        let bytes = serde_json::to_vec(&envelope).unwrap();
        let decoded: ServerEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, envelope);
    }
}

#[tokio::test]
async fn dialog_list_pages_cover_more_than_one_mib_and_open_is_metadata_only() {
    let dialogs: Vec<_> = (1..=2_200)
        .map(|id| DialogSummary {
            id: DialogId::new(id).unwrap(),
            title: "d".repeat(512),
        })
        .collect();
    assert!(serde_json::to_vec(&dialogs).unwrap().len() > MAX_LINE_BYTES);

    let (mut read, write) = tokio::io::duplex(MAX_LINE_BYTES * 2);
    let mut writer = NdjsonWriter::new(write);
    for (sequence, page) in dialogs.chunks(500).enumerate() {
        writer
            .write_event(&ServerEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: request_id(),
                event: ServerEvent::DialogList {
                    sequence: sequence as u64,
                    dialogs: page.to_vec(),
                    complete: (sequence + 1) * 500 >= dialogs.len(),
                },
            })
            .await
            .unwrap();
    }
    writer
        .write_event(&ServerEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id(),
            event: ServerEvent::DialogOpened {
                dialog_id: dialog_id(),
                title: "Work".into(),
            },
        })
        .await
        .unwrap();
    drop(writer);
    let mut output = Vec::new();
    read.read_to_end(&mut output).await.unwrap();
    let lines: Vec<_> = output
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(lines.len(), 6);
    for (sequence, line) in lines[..5].iter().enumerate() {
        assert!(line.len() < MAX_LINE_BYTES);
        let page: ServerEnvelope = serde_json::from_slice(line).unwrap();
        match page.event {
            ServerEvent::DialogList {
                sequence: actual,
                dialogs: page_dialogs,
                complete,
            } => {
                assert_eq!(actual, sequence as u64);
                assert_eq!(
                    page_dialogs,
                    dialogs[sequence * 500..((sequence + 1) * 500).min(dialogs.len())]
                );
                assert_eq!(complete, sequence == 4);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    let opened: serde_json::Value = serde_json::from_slice(lines[5]).unwrap();
    assert_eq!(
        opened["event"],
        json!({"type": "dialog_opened", "dialog_id": 1, "title": "Work"})
    );
}

#[tokio::test]
async fn inspection_pages_cover_more_than_one_mib() {
    let items: Vec<_> = (0..5)
        .map(|id| json!({"id": id, "content": "x".repeat(240_000)}))
        .collect();
    assert!(serde_json::to_vec(&items).unwrap().len() > MAX_LINE_BYTES);
    let kind = InspectKind::History {
        dialog_id: Some(dialog_id()),
    };
    let (mut read, write) = tokio::io::duplex(MAX_LINE_BYTES * 2);
    let mut writer = NdjsonWriter::new(write);
    for (sequence, item) in items.iter().enumerate() {
        writer
            .write_event(&ServerEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: request_id(),
                event: ServerEvent::InspectionResult {
                    kind: kind.clone(),
                    sequence: sequence as u64,
                    items: vec![item.clone()],
                    complete: sequence + 1 == items.len(),
                },
            })
            .await
            .unwrap();
    }
    drop(writer);
    let mut output = Vec::new();
    read.read_to_end(&mut output).await.unwrap();
    let lines: Vec<_> = output
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(lines.len(), items.len());
    for (sequence, line) in lines.iter().enumerate() {
        assert!(line.len() < MAX_LINE_BYTES);
        let page: ServerEnvelope = serde_json::from_slice(line).unwrap();
        match page.event {
            ServerEvent::InspectionResult {
                kind: actual_kind,
                sequence: actual_sequence,
                items: page_items,
                complete,
            } => {
                assert_eq!(actual_kind, kind);
                assert_eq!(actual_sequence, sequence as u64);
                assert_eq!(page_items, vec![items[sequence].clone()]);
                assert_eq!(complete, sequence + 1 == items.len());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}

#[tokio::test]
async fn rejects_unknown_fields_and_types() {
    let cases = [
        json!({"protocol_version": 1, "request_id": request_id(), "request": {"type": "list_dialogs"}, "ignored": true}),
        json!({"protocol_version": 1, "request_id": request_id(), "request": {"type": "send_message", "dialog_id": 1, "message": "Hi", "path": "/tmp/x"}}),
        json!({"protocol_version": 1, "request_id": request_id(), "request": {"type": "export", "path": "/tmp/x"}}),
        json!({"protocol_version": 1, "request_id": request_id(), "request": {"type": "unknown"}}),
        json!({"protocol_version": "1", "request_id": request_id(), "request": {"type": "list_dialogs"}}),
        json!({"protocol_version": 1, "request_id": request_id(), "request": {"type": "open_dialog", "dialog_id": 0}}),
    ];
    for value in cases {
        let line = format!("{value}\n");
        let error = NdjsonReader::new(line.as_bytes())
            .read_request()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ProtocolErrorCode::InvalidRequest);
    }
}

#[tokio::test]
async fn rejects_version_mismatch_before_dispatch() {
    let line = format!(
        "{}\n",
        json!({"protocol_version": 2, "request_id": request_id(), "request": {"type": "list_dialogs"}})
    );
    let error = NdjsonReader::new(line.as_bytes())
        .read_request()
        .await
        .unwrap_err();
    assert_eq!(error.code(), ProtocolErrorCode::UnsupportedVersion);
}

#[tokio::test]
async fn rejects_line_over_one_mib_without_allocating_past_limit() {
    let mut line = vec![b'x'; MAX_LINE_BYTES + 1];
    line.push(b'\n');
    let mut reader = NdjsonReader::new(line.as_slice());
    let error = reader.read_request().await.unwrap_err();
    assert_eq!(error.code(), ProtocolErrorCode::LineTooLong);
    assert!(reader.buffer_capacity() <= MAX_LINE_BYTES);
}

#[tokio::test]
async fn rejects_message_and_prompt_over_256_kib() {
    let long = "x".repeat(MAX_CONTENT_BYTES + 1);
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request_id(),
        request: ClientRequest::SendMessage {
            dialog_id: dialog_id(),
            message: long.clone(),
        },
    };
    let line = format!("{}\n", serde_json::to_string(&request).unwrap());
    let error = NdjsonReader::new(line.as_bytes())
        .read_request()
        .await
        .unwrap_err();
    assert_eq!(error.code(), ProtocolErrorCode::ContentTooLong);

    let (mut read, write) = tokio::io::duplex(MAX_LINE_BYTES + 1);
    let mut writer = NdjsonWriter::new(write);
    let event = ServerEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request_id(),
        event: ServerEvent::ConfirmationRequired {
            confirmation_id: confirmation_id(),
            description: "create job".into(),
            prompt: long,
        },
    };
    let error = writer.write_event(&event).await.unwrap_err();
    assert_eq!(error.code(), ProtocolErrorCode::ContentTooLong);
    drop(writer);
    let mut output = Vec::new();
    read.read_to_end(&mut output).await.unwrap();
    assert!(output.is_empty());
}

#[tokio::test]
async fn export_chunks_are_at_most_64_kib() {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(vec![0x7f; EXPORT_CHUNK_BYTES + 1]);
    let (mut read, write) = tokio::io::duplex(MAX_LINE_BYTES + 1);
    let mut writer = NdjsonWriter::new(write);
    let oversized = ServerEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request_id(),
        event: ServerEvent::ExportChunk {
            sequence: 0,
            data_base64: encoded,
        },
    };
    let error = writer.write_event(&oversized).await.unwrap_err();
    assert_eq!(error.code(), ProtocolErrorCode::ChunkTooLong);
    let exact = ServerEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request_id(),
        event: ServerEvent::ExportChunk {
            sequence: 0,
            data_base64: base64::engine::general_purpose::STANDARD
                .encode(vec![0x7f; EXPORT_CHUNK_BYTES]),
        },
    };
    writer.write_event(&exact).await.unwrap();
    drop(writer);
    let mut bytes = Vec::new();
    read.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes.iter().filter(|&&b| b == b'\n').count(), 1);
    assert!(bytes.ends_with(b"\n"));
    let parsed: ServerEnvelope = serde_json::from_slice(&bytes[..bytes.len() - 1]).unwrap();
    assert_eq!(parsed, exact);
}

#[test]
fn error_responses_contain_only_a_bounded_machine_code() {
    let envelope = ServerEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request_id(),
        event: ServerEvent::ProtocolError {
            code: ProtocolErrorCode::InvalidRequest,
        },
    };
    let value = serde_json::to_value(envelope).unwrap();
    assert_eq!(
        value["event"],
        json!({"type": "protocol_error", "code": "invalid_request"})
    );
    assert_eq!(ProtocolError::InvalidRequest.to_string(), "invalid_request");
}
