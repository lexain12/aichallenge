//! Opt-in live acceptance against the configured SSH alias.
//!
//! These tests never run in the deterministic suite. They intentionally keep
//! prompts and answers out of diagnostics.

use std::path::PathBuf;
use std::time::Duration;

use deepseek_cli::domain::{ConfirmationId, DialogId, RequestId};
use deepseek_cli::protocol::{
    ClientRequest, PROTOCOL_VERSION, RequestEnvelope, ServerEnvelope, ServerEvent,
};
use deepseek_cli::remote_client::{RemoteSession, SshTransport};
use deepseek_cli::settings::ClientSettings;
use tokio::process::{ChildStdin, ChildStdout};

type LiveSession = RemoteSession<ChildStdout, ChildStdin>;

fn require_flag(name: &str) {
    assert_eq!(
        std::env::var(name).ok().as_deref(),
        Some("1"),
        "{name}=1 is required"
    );
}

async fn connect() -> LiveSession {
    require_flag("LIGHT_AGENT_LIVE");
    let path = std::env::var_os("LIGHT_AGENT_CLIENT_CONFIG")
        .map(PathBuf::from)
        .expect("LIGHT_AGENT_CLIENT_CONFIG is required");
    let settings = ClientSettings::load(&path).expect("invalid live client configuration");
    let mut session = SshTransport::connect(&settings)
        .await
        .expect("live SSH connection failed");
    let hello = timed_event(&mut session).await;
    assert!(matches!(hello.event, ServerEvent::Hello), "missing hello");
    session
}

async fn timed_event(session: &mut LiveSession) -> ServerEnvelope {
    tokio::time::timeout(Duration::from_secs(180), session.event())
        .await
        .expect("live event timed out")
        .expect("live protocol failed")
        .expect("live transport closed")
}

async fn send(session: &mut LiveSession, request: ClientRequest) -> RequestId {
    let request_id = RequestId::new();
    session
        .send(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            request,
        })
        .await
        .expect("live request failed");
    request_id
}

async fn create_dialog(session: &mut LiveSession, title: &str) -> DialogId {
    let request_id = send(
        session,
        ClientRequest::CreateDialog {
            title: title.to_owned(),
        },
    )
    .await;
    loop {
        let event = timed_event(session).await;
        if event.request_id == request_id {
            match event.event {
                ServerEvent::DialogOpened { dialog_id, .. } => return dialog_id,
                ServerEvent::ProtocolError { .. } => panic!("create dialog rejected"),
                _ => {}
            }
        }
    }
}

async fn completed_turn(
    session: &mut LiveSession,
    dialog_id: DialogId,
    message: &str,
) -> Vec<String> {
    let request_id = send(
        session,
        ClientRequest::SendMessage {
            dialog_id,
            message: message.to_owned(),
        },
    )
    .await;
    let mut tools = Vec::new();
    loop {
        let event = timed_event(session).await;
        if event.request_id != request_id {
            continue;
        }
        match event.event {
            ServerEvent::ToolStarted { name } => tools.push(name),
            ServerEvent::TurnCompleted { answer } => {
                assert!(!answer.trim().is_empty(), "empty durable answer");
                return tools;
            }
            ServerEvent::TurnFailed { .. } | ServerEvent::ProtocolError { .. } => {
                panic!("live turn failed")
            }
            _ => {}
        }
    }
}

async fn delete_dialog(session: &mut LiveSession, dialog_id: DialogId) {
    let request_id = send(session, ClientRequest::DeleteDialog { dialog_id }).await;
    loop {
        let event = timed_event(session).await;
        if event.request_id == request_id {
            match event.event {
                ServerEvent::DialogList { complete: true, .. } => return,
                ServerEvent::ProtocolError { .. } => panic!("delete dialog rejected"),
                _ => {}
            }
        }
    }
}

async fn confirm_and_complete(session: &mut LiveSession, originating_request_id: RequestId) {
    let confirmation_id: ConfirmationId = loop {
        let event = timed_event(session).await;
        if event.request_id == originating_request_id {
            match event.event {
                ServerEvent::ConfirmationRequired {
                    confirmation_id, ..
                } => break confirmation_id,
                ServerEvent::TurnFailed { .. } | ServerEvent::ProtocolError { .. } => {
                    panic!("scheduled mutation proposal failed")
                }
                _ => {}
            }
        }
    };
    send(
        session,
        ClientRequest::ConfirmAction {
            confirmation_id,
            originating_request_id,
        },
    )
    .await;
    loop {
        let event = timed_event(session).await;
        if event.request_id == originating_request_id {
            match event.event {
                ServerEvent::TurnCompleted { answer } => {
                    assert!(!answer.trim().is_empty(), "empty durable answer");
                    return;
                }
                ServerEvent::TurnFailed { .. } | ServerEvent::ProtocolError { .. } => {
                    panic!("confirmed scheduled mutation failed")
                }
                _ => {}
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires LIGHT_AGENT_LIVE=1 and an authorized live client config"]
async fn live_deepseek_no_tools() {
    let mut session = connect().await;
    let dialog = create_dialog(&mut session, "day18-live-no-tools").await;
    let tools = completed_turn(
        &mut session,
        dialog,
        "Reply with one short greeting. Do not call any tool.",
    )
    .await;
    assert!(tools.is_empty(), "unexpected tool call");
    delete_dialog(&mut session, dialog).await;
}

#[tokio::test]
#[ignore = "requires LIGHT_AGENT_LIVE=1 and an authorized live client config"]
async fn live_ssh_two_dialog_roundtrip() {
    let mut session = connect().await;
    let first = create_dialog(&mut session, "day18-live-first").await;
    let second = create_dialog(&mut session, "day18-live-second").await;
    completed_turn(
        &mut session,
        first,
        "Reply only: first dialog is reachable.",
    )
    .await;
    completed_turn(
        &mut session,
        second,
        "Reply only: second dialog is reachable.",
    )
    .await;
    delete_dialog(&mut session, first).await;
    delete_dialog(&mut session, second).await;
}

#[tokio::test]
#[ignore = "requires LIGHT_AGENT_LIVE=1 and loopback Telegram MCP on the VM"]
async fn live_read_only_telegram_mcp() {
    let mut session = connect().await;
    let dialog = create_dialog(&mut session, "day18-live-telegram-readonly").await;
    let tools = completed_turn(
        &mut session,
        dialog,
        "Use only read-only Telegram tools to list chats and inspect one chat. Do not send or mutate anything. Return only a generic success/failure status without message contents.",
    )
    .await;
    assert!(!tools.is_empty(), "Telegram MCP was not called");
    assert!(
        tools.iter().all(|name| matches!(
            name.as_str(),
            "telegram__list_chats" | "telegram__read_chat"
        )),
        "non-read-only tool was called"
    );
    delete_dialog(&mut session, dialog).await;
}

#[tokio::test]
#[ignore = "requires LIGHT_AGENT_LIVE=1, LIGHT_AGENT_CRON_LIVE=1, and explicit VM crontab authorization"]
async fn live_confirmed_cron_roundtrip() {
    require_flag("LIGHT_AGENT_CRON_LIVE");
    let mut session = connect().await;
    let dialog = create_dialog(&mut session, "day18-live-cron").await;
    let job_name = format!("day18-live-check-{}", std::process::id());
    let originating_request_id = send(
        &mut session,
        ClientRequest::SendMessage {
            dialog_id: dialog,
            message: format!(
                "Create a cron job named {job_name} for 05:17 Europe/Moscow whose task is to reply with a health status."
            ),
        },
    )
    .await;
    confirm_and_complete(&mut session, originating_request_id).await;
    let delete_request_id = send(
        &mut session,
        ClientRequest::SendMessage {
            dialog_id: dialog,
            message: format!("Delete the scheduled job named {job_name}."),
        },
    )
    .await;
    confirm_and_complete(&mut session, delete_request_id).await;
    delete_dialog(&mut session, dialog).await;
}

#[tokio::test]
#[ignore = "requires LIGHT_AGENT_LIVE=1 after an operator-authorized VM reboot"]
async fn live_reboot_recovery() {
    let dialog = std::env::var("LIGHT_AGENT_REBOOT_DIALOG_ID")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .and_then(|value| DialogId::new(value).ok())
        .expect("LIGHT_AGENT_REBOOT_DIALOG_ID is required");
    let mut session = connect().await;
    let request_id = send(
        &mut session,
        ClientRequest::OpenDialog { dialog_id: dialog },
    )
    .await;
    loop {
        let event = timed_event(&mut session).await;
        if event.request_id == request_id {
            match event.event {
                ServerEvent::DialogOpened { dialog_id, .. } if dialog_id == dialog => break,
                ServerEvent::ProtocolError { .. } => panic!("persisted dialog unavailable"),
                _ => {}
            }
        }
    }
}
