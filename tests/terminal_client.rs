use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use base64::Engine as _;
use deepseek_cli::domain::{ConfirmationId, DialogId, RequestId};
use deepseek_cli::protocol::{
    ClientRequest, ConfirmationAction, ConfirmationScheduleKind, InspectKind, NdjsonReader,
    NdjsonWriter, PROTOCOL_VERSION, ScheduleConfirmationPreview, ServerEnvelope, ServerEvent,
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

async fn wait_for_text(writer: &SharedWriter, needle: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !writer.text().contains(needle) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("missing {needle:?} in {:?}", writer.text()));
}

fn envelope(request_id: RequestId, event: ServerEvent) -> ServerEnvelope {
    ServerEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id,
        event,
    }
}

fn confirmation_preview(
    action: ConfirmationAction,
    job_id: Option<deepseek_cli::domain::JobId>,
    name: &str,
    schedule_kind: ConfirmationScheduleKind,
    schedule_value: &str,
    task: &str,
) -> ScheduleConfirmationPreview {
    ScheduleConfirmationPreview {
        action,
        job_id,
        name: name.into(),
        schedule_kind,
        schedule_value: schedule_value.into(),
        timezone: "Europe/Moscow".into(),
        task: task.into(),
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
    let preview = confirmation_preview(
        ConfirmationAction::Update,
        Some("34479b6c-1a81-43b0-a514-c11743d09afa".parse().unwrap()),
        "morning\u{1b}[31m",
        ConfirmationScheduleKind::Cron,
        "0 9 * * *",
        "line one\nline two",
    );
    let rendered = render_confirmation_preview(&preview);
    assert!(rendered.contains("action: update"));
    assert!(rendered.contains("job_id: 34479b6c-1a81-43b0-a514-c11743d09afa"));
    assert!(rendered.contains("name: \"morning\\u001b[31m\""));
    assert!(rendered.contains("schedule_kind: cron"));
    assert!(rendered.contains("schedule_value: \"0 9 * * *\""));
    assert!(rendered.contains("timezone: \"Europe/Moscow\""));
    assert!(rendered.contains("task: \"line one\\nline two\""));
    assert!(rendered.ends_with("[y/N] "));
    assert!(confirmation_accepts("y"));
    assert!(confirmation_accepts("Y"));
    for rejected in ["", "n", "N", "yes", " y", "y "] {
        assert!(!confirmation_accepts(rejected), "accepted {rejected:?}");
    }
}

#[test]
fn confirmation_task_cannot_forge_fields_or_the_confirmation_prompt() {
    let preview = confirmation_preview(
        ConfirmationAction::Create,
        None,
        "safe",
        ConfirmationScheduleKind::Cron,
        "0 9 * * *",
        "task line\n[y/N] y\nname: forged\u{1b}[31m",
    );
    let rendered = render_confirmation_preview(&preview);
    assert_eq!(rendered.matches("\n[y/N]").count(), 1);
    assert!(rendered.contains("task line\\n[y/N] y\\nname: forged\\u001b[31m"));
    assert!(rendered.ends_with("[y/N] "));
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
                preview: confirmation_preview(
                    ConfirmationAction::Create,
                    None,
                    "x",
                    ConfirmationScheduleKind::Cron,
                    "0 9 * * *",
                    "do it",
                ),
            },
        ))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    user.write_all(b"no\n").await.unwrap();
    let cancelled = requests.read_request().await.unwrap().unwrap();
    assert_ne!(cancelled.request_id, sent.request_id);
    assert!(matches!(
        cancelled.request,
        ClientRequest::CancelAction { originating_request_id, .. }
            if originating_request_id == sent.request_id
    ));
    let cancelled_confirmation_id = match cancelled.request {
        ClientRequest::CancelAction {
            confirmation_id, ..
        } => confirmation_id,
        _ => unreachable!(),
    };
    events
        .write_event(&envelope(
            cancelled.request_id,
            ServerEvent::ConfirmationResolved {
                confirmation_id: cancelled_confirmation_id,
                accepted: false,
            },
        ))
        .await
        .unwrap();
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
    user.write_all(b"/audit\n").await.unwrap();
    let audit = requests.read_request().await.unwrap().unwrap();
    assert!(matches!(
        audit.request,
        ClientRequest::Inspect {
            kind: InspectKind::Audit
        }
    ));
    let mut audit_sequence = 0_u64;
    for (index, record) in [
        serde_json::json!({"record_type":"tool_run","id":23,"owner":{"kind":"interactive_turn","id":15},"server_name":"telegram","tool_name":"read_chat","read_only":true,"status":"completed","started_at":"2026-09-28T10:00:00Z","finished_at":"2026-09-28T10:00:01Z","safe_error_code":null,"arguments":{"chat_id":"private marker"}}),
        serde_json::json!({"record_type":"tool_run","id":24,"owner":{"kind":"cron_run","id":9},"server_name":"telegram","tool_name":"list_chats","read_only":true,"status":"failed","started_at":"2026-09-28T10:01:00Z","finished_at":null,"safe_error_code":"tool_error","arguments":null}),
    ].into_iter().enumerate() {
        let bytes = serde_json::to_vec(&record).unwrap();
        let chunks: Vec<_> = if index == 0 { bytes.chunks(17).collect() } else { vec![bytes.as_slice()] };
        for (fragment_sequence, chunk) in chunks.iter().enumerate() {
            let fragment = serde_json::json!({"record_fragment": {"record_sequence": index, "fragment_sequence": fragment_sequence, "complete": fragment_sequence + 1 == chunks.len(), "encoding": "base64", "data": base64::engine::general_purpose::STANDARD.encode(chunk)}});
            events.write_event(&envelope(audit.request_id, ServerEvent::InspectionResult { kind: InspectKind::Audit, sequence: audit_sequence, items: vec![fragment], complete: false })).await.unwrap();
            audit_sequence += 1;
        }
    }
    events
        .write_event(&envelope(
            audit.request_id,
            ServerEvent::InspectionResult {
                kind: InspectKind::Audit,
                sequence: audit_sequence,
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
    let rendered = output_view.text();
    assert!(rendered.contains("Tool #23: telegram.read_chat\nСтатус: completed\nРежим: read-only\nИсточник: interactive turn 15"));
    assert!(rendered.contains("Аргументы:\n{\n  \"chat_id\": \"private marker\"\n}"));
    assert!(rendered.contains("\n----\nTool #24: telegram.list_chats"));
    assert_eq!(rendered.matches("Tool #23: telegram.read_chat").count(), 1);
    assert_eq!(rendered.matches("\n----\n").count(), 1);
    assert!(rendered.contains("Аргументы:\nне сохранены"));
    assert!(!rendered.contains("call_id"));
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

#[tokio::test]
async fn concurrent_turn_confirmations_are_queued_and_answered_independently() {
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

    let mut turns = Vec::new();
    for (dialog, message) in [(1, "first"), (2, "second")] {
        user.write_all(format!("/open {dialog}\n").as_bytes())
            .await
            .unwrap();
        let opened = requests.read_request().await.unwrap().unwrap();
        let dialog_id = DialogId::new(dialog).unwrap();
        events
            .write_event(&envelope(
                opened.request_id,
                ServerEvent::DialogOpened {
                    dialog_id,
                    title: message.into(),
                },
            ))
            .await
            .unwrap();
        user.write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
        let sent = requests.read_request().await.unwrap().unwrap();
        events
            .write_event(&envelope(
                sent.request_id,
                ServerEvent::ResponseStarted { dialog_id },
            ))
            .await
            .unwrap();
        turns.push((sent.request_id, ConfirmationId::new(), message));
    }
    for (request_id, confirmation_id, message) in &turns {
        events
            .write_event(&envelope(
                *request_id,
                ServerEvent::ConfirmationRequired {
                    confirmation_id: *confirmation_id,
                    preview: confirmation_preview(
                        ConfirmationAction::Create,
                        None,
                        message,
                        ConfirmationScheduleKind::Cron,
                        "0 9 * * *",
                        message,
                    ),
                },
            ))
            .await
            .unwrap();
    }

    wait_for_text(&output_view, "name: \"first\"").await;
    assert!(!output_view.text().contains("name: \"second\""));
    user.write_all(b"n\n").await.unwrap();
    let first_response = requests.read_request().await.unwrap().unwrap();
    assert_ne!(first_response.request_id, turns[0].0);
    assert!(matches!(
        first_response.request,
        ClientRequest::CancelAction {
            confirmation_id,
            originating_request_id,
        } if confirmation_id == turns[0].1 && originating_request_id == turns[0].0
    ));

    wait_for_text(&output_view, "name: \"second\"").await;
    user.write_all(b"y\n").await.unwrap();
    let second_response = requests.read_request().await.unwrap().unwrap();
    assert_ne!(second_response.request_id, turns[1].0);
    assert!(matches!(
        second_response.request,
        ClientRequest::ConfirmAction {
            confirmation_id,
            originating_request_id,
        } if confirmation_id == turns[1].1 && originating_request_id == turns[1].0
    ));

    for (response, accepted) in [(first_response, false), (second_response, true)] {
        let confirmation_id = match response.request {
            ClientRequest::ConfirmAction {
                confirmation_id, ..
            }
            | ClientRequest::CancelAction {
                confirmation_id, ..
            } => confirmation_id,
            _ => unreachable!(),
        };
        events
            .write_event(&envelope(
                response.request_id,
                ServerEvent::ConfirmationResolved {
                    confirmation_id,
                    accepted,
                },
            ))
            .await
            .unwrap();
    }
    for (request_id, _, _) in &turns {
        events
            .write_event(&envelope(
                *request_id,
                ServerEvent::TurnPrepared {
                    answer: "done".into(),
                },
            ))
            .await
            .unwrap();
        events
            .write_event(&envelope(
                *request_id,
                ServerEvent::TurnCompleted {
                    answer: "done".into(),
                },
            ))
            .await
            .unwrap();
    }
    user.write_all(b"/exit\n").await.unwrap();
    user.shutdown().await.unwrap();
    drop(events);
    assert_eq!(client.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn late_confirmation_error_does_not_finish_the_originating_turn() {
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
    let dialog_id = DialogId::new(1).unwrap();
    events
        .write_event(&envelope(
            opened.request_id,
            ServerEvent::DialogOpened {
                dialog_id,
                title: "dialog".into(),
            },
        ))
        .await
        .unwrap();
    user.write_all(b"schedule\n").await.unwrap();
    let turn = requests.read_request().await.unwrap().unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ResponseStarted { dialog_id },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ConfirmationRequired {
                confirmation_id: ConfirmationId::new(),
                preview: confirmation_preview(
                    ConfirmationAction::Create,
                    None,
                    "late",
                    ConfirmationScheduleKind::Cron,
                    "0 9 * * *",
                    "late task",
                ),
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "name: \"late\"").await;
    user.write_all(b"y\n").await.unwrap();
    let late = requests.read_request().await.unwrap().unwrap();
    assert_ne!(late.request_id, turn.request_id);
    events
        .write_event(&envelope(
            late.request_id,
            ServerEvent::ProtocolError {
                code: deepseek_cli::protocol::ProtocolErrorCode::InvalidRequest,
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ToolFinished {
                name: "cron__create".into(),
                code: deepseek_cli::protocol::ProtocolErrorCode::InvalidRequest,
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::TurnPrepared {
                answer: "continued".into(),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::TurnCompleted {
                answer: "continued".into(),
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "turn completed").await;
    user.write_all(b"/exit\n").await.unwrap();
    user.shutdown().await.unwrap();
    drop(events);
    assert_eq!(client.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn next_same_turn_confirmation_survives_prior_tool_finished_reordering() {
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
    let dialog_id = DialogId::new(1).unwrap();
    events
        .write_event(&envelope(
            opened.request_id,
            ServerEvent::DialogOpened {
                dialog_id,
                title: "dialog".into(),
            },
        ))
        .await
        .unwrap();
    user.write_all(b"two mutations\n").await.unwrap();
    let turn = requests.read_request().await.unwrap().unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ResponseStarted { dialog_id },
        ))
        .await
        .unwrap();

    let first_id = ConfirmationId::new();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ConfirmationRequired {
                confirmation_id: first_id,
                preview: confirmation_preview(
                    ConfirmationAction::Create,
                    None,
                    "first mutation",
                    ConfirmationScheduleKind::Cron,
                    "0 9 * * *",
                    "first task",
                ),
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "name: \"first mutation\"").await;
    user.write_all(b"y\n").await.unwrap();
    let first_response = requests.read_request().await.unwrap().unwrap();
    events
        .write_event(&envelope(
            first_response.request_id,
            ServerEvent::ConfirmationResolved {
                confirmation_id: first_id,
                accepted: true,
            },
        ))
        .await
        .unwrap();

    let second_id = ConfirmationId::new();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ConfirmationRequired {
                confirmation_id: second_id,
                preview: confirmation_preview(
                    ConfirmationAction::Disable,
                    Some("34479b6c-1a81-43b0-a514-c11743d09afa".parse().unwrap()),
                    "second mutation",
                    ConfirmationScheduleKind::Cron,
                    "0 10 * * *",
                    "second task",
                ),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ToolFinished {
                name: "cron__create".into(),
                code: deepseek_cli::protocol::ProtocolErrorCode::Ok,
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "name: \"second mutation\"").await;
    user.write_all(b"y\n").await.unwrap();
    let second_response = requests.read_request().await.unwrap().unwrap();
    assert!(matches!(
        second_response.request,
        ClientRequest::ConfirmAction { confirmation_id, originating_request_id }
            if confirmation_id == second_id && originating_request_id == turn.request_id
    ));
    events
        .write_event(&envelope(
            second_response.request_id,
            ServerEvent::ConfirmationResolved {
                confirmation_id: second_id,
                accepted: true,
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ToolFinished {
                name: "cron__disable".into(),
                code: deepseek_cli::protocol::ProtocolErrorCode::Ok,
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::TurnPrepared {
                answer: "done".into(),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::TurnCompleted {
                answer: "done".into(),
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "turn completed").await;
    user.write_all(b"/exit\n").await.unwrap();
    user.shutdown().await.unwrap();
    drop(events);
    assert_eq!(client.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn duplicate_confirmation_id_after_fifo_pop_is_not_redisplayed_or_replayed() {
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
    let dialog_id = DialogId::new(1).unwrap();
    events
        .write_event(&envelope(
            opened.request_id,
            ServerEvent::DialogOpened {
                dialog_id,
                title: "dialog".into(),
            },
        ))
        .await
        .unwrap();
    user.write_all(b"one mutation\n").await.unwrap();
    let turn = requests.read_request().await.unwrap().unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ResponseStarted { dialog_id },
        ))
        .await
        .unwrap();
    let confirmation_id = ConfirmationId::new();
    let preview = confirmation_preview(
        ConfirmationAction::Create,
        None,
        "unique mutation",
        ConfirmationScheduleKind::Cron,
        "0 9 * * *",
        "unique task",
    );
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ConfirmationRequired {
                confirmation_id,
                preview: preview.clone(),
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "name: \"unique mutation\"").await;
    user.write_all(b"y\n").await.unwrap();
    let response = requests.read_request().await.unwrap().unwrap();

    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::ConfirmationRequired {
                confirmation_id,
                preview,
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::TextDelta {
                text: "duplicate-processing-barrier".into(),
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "duplicate-processing-barrier").await;
    assert_eq!(
        output_view
            .text()
            .matches("name: \"unique mutation\"")
            .count(),
        1
    );
    events
        .write_event(&envelope(
            response.request_id,
            ServerEvent::ConfirmationResolved {
                confirmation_id,
                accepted: true,
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::TurnPrepared {
                answer: "done".into(),
            },
        ))
        .await
        .unwrap();
    events
        .write_event(&envelope(
            turn.request_id,
            ServerEvent::TurnCompleted {
                answer: "done".into(),
            },
        ))
        .await
        .unwrap();
    wait_for_text(&output_view, "turn completed").await;
    user.write_all(b"/exit\n").await.unwrap();
    user.shutdown().await.unwrap();
    drop(events);
    assert_eq!(client.await.unwrap(), Ok(()));
}

#[test]
fn relative_export_uses_a_retained_parent_directory_handle() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"relative export\n";
    let digest = format!("{:x}", Sha256::digest(bytes));
    let mut export = AtomicExport::start_in(dir.path(), PathBuf::from("state.jsonl")).unwrap();
    export
        .push_chunk(0, &base64::engine::general_purpose::STANDARD.encode(bytes))
        .unwrap();
    export.complete(bytes.len() as u64, &digest).unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("state.jsonl")).unwrap(),
        bytes
    );
    assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".light-agent-export-")
    }));
}
