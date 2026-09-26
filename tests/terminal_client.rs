use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use base64::Engine as _;
use deepseek_cli::domain::{ConfirmationId, DialogId, RequestId};
use deepseek_cli::protocol::{
    ClientRequest, InspectKind, NdjsonReader, NdjsonWriter, PROTOCOL_VERSION, ServerEnvelope,
    ServerEvent,
};
use deepseek_cli::remote_client::RemoteSession;
use deepseek_cli::terminal_client::{
    AtomicExport, ClientAction, ClientError, TerminalClient, confirmation_accepts,
    parse_terminal_input, render_confirmation_preview,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Clone, Default)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl SharedWriter {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl AsyncWrite for SharedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn envelope(request_id: RequestId, event: ServerEvent) -> ServerEnvelope {
    ServerEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id,
        event,
    }
}

#[test]
fn terminal_commands_parse_strictly() {
    let job = "123e4567-e89b-12d3-a456-426614174000";
    assert_eq!(
        parse_terminal_input("hello").unwrap(),
        ClientAction::Send("hello".into())
    );
    assert_eq!(
        parse_terminal_input("/dialogs").unwrap(),
        ClientAction::ListDialogs
    );
    assert_eq!(
        parse_terminal_input("/new one two").unwrap(),
        ClientAction::NewDialog("one two".into())
    );
    assert_eq!(
        parse_terminal_input("/open 7").unwrap(),
        ClientAction::OpenDialog(DialogId::new(7).unwrap())
    );
    assert_eq!(
        parse_terminal_input("/rename 7 renamed dialog").unwrap(),
        ClientAction::RenameDialog(DialogId::new(7).unwrap(), "renamed dialog".into())
    );
    assert_eq!(
        parse_terminal_input("/delete 7").unwrap(),
        ClientAction::DeleteDialog(DialogId::new(7).unwrap())
    );
    assert_eq!(
        parse_terminal_input("/history").unwrap(),
        ClientAction::History(None)
    );
    assert_eq!(
        parse_terminal_input("/history 7").unwrap(),
        ClientAction::History(Some(DialogId::new(7).unwrap()))
    );
    assert_eq!(parse_terminal_input("/jobs").unwrap(), ClientAction::Jobs);
    assert!(matches!(
        parse_terminal_input(&format!("/job {job}")).unwrap(),
        ClientAction::Job(_)
    ));
    assert!(matches!(
        parse_terminal_input(&format!("/runs {job}")).unwrap(),
        ClientAction::Runs(_)
    ));
    assert_eq!(parse_terminal_input("/audit").unwrap(), ClientAction::Audit);
    assert_eq!(parse_terminal_input("/dump").unwrap(), ClientAction::Dump);
    assert_eq!(
        parse_terminal_input("/export /tmp/state.jsonl").unwrap(),
        ClientAction::Export(PathBuf::from("/tmp/state.jsonl"))
    );
    assert_eq!(parse_terminal_input("/exit").unwrap(), ClientAction::Exit);

    for invalid in [
        "",
        "   ",
        "/dialogs extra",
        "/open",
        "/open 0",
        "/history 1 extra",
        "/jobs extra",
        "/job nope",
        "/runs",
        "/audit extra",
        "/dump extra",
        "/export",
        "/exit now",
        "/unknown",
    ] {
        assert!(
            parse_terminal_input(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
}

#[test]
fn confirmation_preview_is_complete_escaped_and_default_rejects() {
    let prompt =
        "name: morning\nschedule: 0 9 * * *\ntimezone: Europe/Moscow\nprompt: line one\nline two";
    let rendered = render_confirmation_preview("create\u{1b}[31m", prompt);
    assert!(rendered.contains("create\\u{1b}[31m"));
    assert!(rendered.contains("name: morning"));
    assert!(rendered.contains("schedule: 0 9 * * *"));
    assert!(rendered.contains("timezone: Europe/Moscow"));
    assert!(rendered.contains("prompt: line one"));
    assert!(rendered.contains("line two"));
    assert!(rendered.ends_with("[y/N] "));
    assert!(confirmation_accepts("y"));
    assert!(confirmation_accepts("Y"));
    for rejected in ["", "n", "N", "yes", " y", "y "] {
        assert!(!confirmation_accepts(rejected), "accepted {rejected:?}");
    }
}

#[test]
fn atomic_export_preserves_old_destination_until_verified_completion() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("state.jsonl");
    std::fs::write(&destination, b"old").unwrap();

    {
        let mut export = AtomicExport::start(&destination).unwrap();
        export
            .push_chunk(
                0,
                &base64::engine::general_purpose::STANDARD.encode(b"partial"),
            )
            .unwrap();
    }
    assert_eq!(std::fs::read(&destination).unwrap(), b"old");
    assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".light-agent-export-")
    }));

    let bytes = b"complete export\n";
    let digest = format!("{:x}", Sha256::digest(bytes));
    let mut export = AtomicExport::start(&destination).unwrap();
    export
        .push_chunk(0, &base64::engine::general_purpose::STANDARD.encode(bytes))
        .unwrap();
    export.complete(bytes.len() as u64, &digest).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), bytes);

    let mut export = AtomicExport::start(&destination).unwrap();
    export
        .push_chunk(
            0,
            &base64::engine::general_purpose::STANDARD.encode(b"replacement"),
        )
        .unwrap();
    assert_eq!(
        export.complete(11, &"0".repeat(64)).unwrap_err(),
        ClientError::Protocol
    );
    assert_eq!(std::fs::read(&destination).unwrap(), bytes);

    let mut export = AtomicExport::start(&destination).unwrap();
    assert_eq!(
        export
            .push_chunk(1, &base64::engine::general_purpose::STANDARD.encode(b"bad"))
            .unwrap_err(),
        ClientError::Protocol
    );
    drop(export);
    assert_eq!(std::fs::read(&destination).unwrap(), bytes);
}

#[tokio::test]
async fn prepared_turn_is_not_reported_complete_and_events_remain_correlated() {
    let (client_wire, server_wire) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_wire);
    let session = RemoteSession::from_streams(client_read, client_write);
    let (mut user, client_input) = tokio::io::duplex(1024);
    let output = SharedWriter::default();
    let errors = SharedWriter::default();
    let output_view = output.clone();
    let errors_view = errors.clone();
    let client = tokio::spawn(TerminalClient::run(session, client_input, output, errors));
    let (server_read, server_write) = tokio::io::split(server_wire);
    let mut requests = NdjsonReader::new(server_read);
    let mut events = NdjsonWriter::new(server_write);
    let hello_id = RequestId::new();
    events
        .write_event(&envelope(hello_id, ServerEvent::Hello))
        .await
        .unwrap();

    user.write_all(b"/open 1\n").await.unwrap();
    let opened = requests.read_request().await.unwrap().unwrap();
    assert!(matches!(opened.request, ClientRequest::OpenDialog { .. }));
    events
        .write_event(&envelope(
            opened.request_id,
            ServerEvent::DialogOpened {
                dialog_id: DialogId::new(1).unwrap(),
                title: "dialog".into(),
            },
        ))
        .await
        .unwrap();

    user.write_all(b"hello\n").await.unwrap();
    let sent = requests.read_request().await.unwrap().unwrap();
    assert!(matches!(sent.request, ClientRequest::SendMessage { .. }));
    events
        .write_event(&envelope(
            sent.request_id,
            ServerEvent::ResponseStarted {
                dialog_id: DialogId::new(1).unwrap(),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            sent.request_id,
            ServerEvent::TextDelta {
                text: "answer".into(),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            sent.request_id,
            ServerEvent::TurnPrepared {
                answer: "answer".into(),
            },
        ))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(!output_view.text().contains("turn completed"));
    assert!(errors_view.text().contains("awaiting durable commit"));

    events
        .write_event(&envelope(
            sent.request_id,
            ServerEvent::TurnCompleted {
                answer: "answer".into(),
            },
        ))
        .await
        .unwrap();
    user.write_all(b"/exit\n").await.unwrap();
    user.shutdown().await.unwrap();
    drop(events);
    assert_eq!(client.await.unwrap(), Ok(()));
    assert!(output_view.text().contains("turn completed"));
}

#[tokio::test]
async fn confirmation_and_fragmented_inspection_use_originating_request() {
    let (client_wire, server_wire) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_wire);
    let session = RemoteSession::from_streams(client_read, client_write);
    let (mut user, client_input) = tokio::io::duplex(4096);
    let output = SharedWriter::default();
    let output_view = output.clone();
    let client = tokio::spawn(TerminalClient::run(
        session,
        client_input,
        output,
        SharedWriter::default(),
    ));
    let (server_read, server_write) = tokio::io::split(server_wire);
    let mut requests = NdjsonReader::new(server_read);
    let mut events = NdjsonWriter::new(server_write);
    events
        .write_event(&envelope(RequestId::new(), ServerEvent::Hello))
        .await
        .unwrap();

    user.write_all(b"/open 1\n").await.unwrap();
    let opened = requests.read_request().await.unwrap().unwrap();
    events
        .write_event(&envelope(
            opened.request_id,
            ServerEvent::DialogOpened {
                dialog_id: DialogId::new(1).unwrap(),
                title: "d".into(),
            },
        ))
        .await
        .unwrap();
    user.write_all(b"schedule it\n").await.unwrap();
    let sent = requests.read_request().await.unwrap().unwrap();
    events
        .write_event(&envelope(
            sent.request_id,
            ServerEvent::ResponseStarted {
                dialog_id: DialogId::new(1).unwrap(),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            sent.request_id,
            ServerEvent::ConfirmationRequired {
                confirmation_id: ConfirmationId::new(),
                description: "create scheduled job".into(),
                prompt: "name: x\nschedule: 0 9 * * *\ntimezone: Europe/Moscow\nprompt: do it"
                    .into(),
            },
        ))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    user.write_all(b"no\n").await.unwrap();
    let cancelled = requests.read_request().await.unwrap().unwrap();
    assert_eq!(cancelled.request_id, sent.request_id);
    assert!(matches!(
        cancelled.request,
        ClientRequest::CancelAction { .. }
    ));
    events
        .write_event(&envelope(
            sent.request_id,
            ServerEvent::TurnFailed {
                code: deepseek_cli::protocol::ProtocolErrorCode::InternalError,
            },
        ))
        .await
        .unwrap();

    user.write_all(b"/dump\n").await.unwrap();
    let dump = requests.read_request().await.unwrap().unwrap();
    assert!(matches!(
        dump.request,
        ClientRequest::Inspect {
            kind: InspectKind::Dump
        }
    ));
    let record = br#"{"record_type":"dialog","id":1}"#;
    for (sequence, bytes, complete) in [(0, &record[..12], false), (1, &record[12..], true)] {
        let fragment = serde_json::json!({"record_fragment": {"record_sequence": 0, "fragment_sequence": sequence, "complete": complete, "encoding": "base64", "data": base64::engine::general_purpose::STANDARD.encode(bytes)}});
        events
            .write_event(&envelope(
                dump.request_id,
                ServerEvent::InspectionResult {
                    kind: InspectKind::Dump,
                    sequence,
                    items: vec![fragment],
                    complete: false,
                },
            ))
            .await
            .unwrap();
    }
    events
        .write_event(&envelope(
            dump.request_id,
            ServerEvent::InspectionResult {
                kind: InspectKind::Dump,
                sequence: 2,
                items: vec![],
                complete: true,
            },
        ))
        .await
        .unwrap();
    user.write_all(b"/exit\n").await.unwrap();
    user.shutdown().await.unwrap();
    drop(events);
    assert_eq!(client.await.unwrap(), Ok(()));
    assert!(output_view.text().contains("\"record_type\":\"dialog\""));
    assert!(output_view.text().contains("[y/N]"));
}

#[tokio::test]
async fn export_path_stays_local_and_wire_contains_only_export_request() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("export.jsonl");
    let command = format!("/export {}\n", destination.display());
    let (client_wire, server_wire) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_wire);
    let session = RemoteSession::from_streams(client_read, client_write);
    let (mut user, client_input) = tokio::io::duplex(4096);
    let client = tokio::spawn(TerminalClient::run(
        session,
        client_input,
        SharedWriter::default(),
        SharedWriter::default(),
    ));
    let (server_read, server_write) = tokio::io::split(server_wire);
    let mut requests = NdjsonReader::new(server_read);
    let mut events = NdjsonWriter::new(server_write);
    events
        .write_event(&envelope(RequestId::new(), ServerEvent::Hello))
        .await
        .unwrap();

    user.write_all(command.as_bytes()).await.unwrap();
    let request = requests.read_request().await.unwrap().unwrap();
    assert_eq!(request.request, ClientRequest::Export);
    assert!(
        !serde_json::to_string(&request)
            .unwrap()
            .contains(&destination.display().to_string())
    );
    let bytes = b"export bytes\n";
    events
        .write_event(&envelope(
            request.request_id,
            ServerEvent::ExportChunk {
                sequence: 0,
                data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            request.request_id,
            ServerEvent::ExportCompleted {
                total_bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
            },
        ))
        .await
        .unwrap();
    user.write_all(b"/exit\n").await.unwrap();
    user.shutdown().await.unwrap();
    drop(events);
    assert_eq!(client.await.unwrap(), Ok(()));
    assert_eq!(std::fs::read(destination).unwrap(), bytes);
}

#[tokio::test]
async fn malformed_export_stream_removes_only_its_temp_and_preserves_destination() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("export.jsonl");
    std::fs::write(&destination, b"old export").unwrap();
    let command = format!("/export {}\n", destination.display());
    let (client_wire, server_wire) = tokio::io::duplex(64 * 1024);
    let (client_read, client_write) = tokio::io::split(client_wire);
    let session = RemoteSession::from_streams(client_read, client_write);
    let (mut user, client_input) = tokio::io::duplex(4096);
    let client = tokio::spawn(TerminalClient::run(
        session,
        client_input,
        SharedWriter::default(),
        SharedWriter::default(),
    ));
    let (server_read, server_write) = tokio::io::split(server_wire);
    let mut requests = NdjsonReader::new(server_read);
    let mut events = NdjsonWriter::new(server_write);
    events
        .write_event(&envelope(RequestId::new(), ServerEvent::Hello))
        .await
        .unwrap();
    user.write_all(command.as_bytes()).await.unwrap();
    let request = requests.read_request().await.unwrap().unwrap();
    events
        .write_event(&envelope(
            request.request_id,
            ServerEvent::ExportChunk {
                sequence: 1,
                data_base64: base64::engine::general_purpose::STANDARD.encode(b"bad"),
            },
        ))
        .await
        .unwrap();
    assert_eq!(client.await.unwrap(), Err(ClientError::Protocol));
    assert_eq!(std::fs::read(&destination).unwrap(), b"old export");
    assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".light-agent-export-")
    }));
}

#[tokio::test]
async fn unexpected_ssh_stdout_eof_returns_one_safe_transport_error() {
    let (client_wire, server_wire) = tokio::io::duplex(4096);
    let (client_read, client_write) = tokio::io::split(client_wire);
    let session = RemoteSession::from_streams(client_read, client_write);
    let (_user, client_input) = tokio::io::duplex(64);
    let client = tokio::spawn(TerminalClient::run(
        session,
        client_input,
        SharedWriter::default(),
        SharedWriter::default(),
    ));
    let (_server_read, server_write) = tokio::io::split(server_wire);
    let mut events = NdjsonWriter::new(server_write);
    events
        .write_event(&envelope(RequestId::new(), ServerEvent::Hello))
        .await
        .unwrap();
    events.shutdown().await.unwrap();
    assert_eq!(client.await.unwrap(), Err(ClientError::TransportClosed));
}
