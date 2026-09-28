mod common;

use base64::Engine as _;
use chrono_tz::Europe::Moscow;
use deepseek_cli::{
    domain::{RequestId, ToolOwner, ToolRunStatus},
    inspection::InspectionService,
    protocol::{
        ClientRequest, ConfirmationAction, ConfirmationScheduleKind, InspectKind, MAX_LINE_BYTES,
        NdjsonReader, NdjsonWriter, PROTOCOL_VERSION, RequestEnvelope, ServerEvent,
    },
    provider::{
        AssistantTurn, ModelToolCall, ModelToolDefinition, Provider, ProviderError, ProviderFuture,
        ProviderMessage, TokenUsage,
    },
    runtime::ProcessLease,
    scheduler::{CronSynchronizer, CrontabBackend, CrontabFuture, ScheduleSpec},
    server::{ServerDependencies, StdioServer},
    settings::ServerSettings,
    store::{JobCreate, SafeErrorCode, Store, ToolRunStart},
    terminal_client::render_confirmation_preview,
    tools::{ToolExecutionError, ToolExecutor, ToolFuture, ToolRoute},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    io,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Waker,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncWrite, DuplexStream, ReadHalf, WriteHalf, duplex, split},
    sync::Notify,
    task::JoinHandle,
};

#[derive(Clone)]
enum Reply {
    Final(String),
    Wait(Arc<Notify>, Arc<Notify>, String),
    ToolCall(ModelToolCall),
}
struct FakeProvider {
    replies: Mutex<VecDeque<Reply>>,
    tools_seen: Mutex<Vec<Vec<String>>>,
}
impl FakeProvider {
    fn new(replies: impl IntoIterator<Item = Reply>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into_iter().collect()),
            tools_seen: Mutex::new(Vec::new()),
        })
    }

    fn tools_seen(&self) -> Vec<Vec<String>> {
        self.tools_seen.lock().unwrap().clone()
    }
}
impl Provider for FakeProvider {
    fn stream_turn<'a>(
        &'a self,
        _messages: &'a [ProviderMessage],
        tools: &'a [ModelToolDefinition],
        sink: &'a mut (dyn FnMut(&str) -> io::Result<()> + Send),
    ) -> ProviderFuture<'a> {
        self.tools_seen
            .lock()
            .unwrap()
            .push(tools.iter().map(|tool| tool.name.clone()).collect());
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Reply::Final("ok".into()));
        Box::pin(async move {
            if let Reply::ToolCall(call) = reply {
                return Ok(AssistantTurn::ToolCalls {
                    content: None,
                    calls: vec![call],
                    usage: None,
                });
            }
            let text = match reply {
                Reply::Final(text) => text,
                Reply::Wait(entered, release, text) => {
                    entered.notify_one();
                    release.notified().await;
                    text
                }
                Reply::ToolCall(_) => unreachable!(),
            };
            sink(&text).map_err(|_| ProviderError::safe("output"))?;
            Ok(AssistantTurn::FinalText {
                content: text,
                usage: Some(TokenUsage::default()),
            })
        })
    }
    fn serialized_request_len(
        &self,
        messages: &[ProviderMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<usize, ProviderError> {
        serde_json::to_vec(&json!({"messages": messages, "tool_count": tools.len()}))
            .map(|v| v.len())
            .map_err(|_| ProviderError::safe("serialization"))
    }
}
struct EmptyTools;
impl ToolExecutor for EmptyTools {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &[]
    }
    fn route(&self, _: &str) -> Option<ToolRoute<'_>> {
        None
    }
    fn is_read_only(&self, _: &str) -> Option<bool> {
        None
    }
    fn call<'a>(&'a self, _: &'a deepseek_cli::provider::ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async { Err(ToolExecutionError::UnknownTool) })
    }
}
struct PendingWriteTools {
    definitions: Vec<ModelToolDefinition>,
    calls: Arc<Mutex<usize>>,
}
impl PendingWriteTools {
    fn new() -> (Arc<Self>, Arc<Mutex<usize>>) {
        let calls = Arc::new(Mutex::new(0));
        (
            Arc::new(Self {
                definitions: vec![ModelToolDefinition {
                    name: "mcp__write".into(),
                    description: None,
                    parameters: json!({"type":"object"}).as_object().unwrap().clone(),
                    read_only: false,
                }],
                calls: calls.clone(),
            }),
            calls,
        )
    }
}
impl ToolExecutor for PendingWriteTools {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }
    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        (name == "mcp__write").then_some(ToolRoute {
            server_name: "mcp",
            tool_name: "write",
        })
    }
    fn is_read_only(&self, name: &str) -> Option<bool> {
        (name == "mcp__write").then_some(false)
    }
    fn call<'a>(&'a self, _: &'a ModelToolCall) -> ToolFuture<'a> {
        *self.calls.lock().unwrap() += 1;
        Box::pin(std::future::pending())
    }
}
struct FakeCron {
    installed: Arc<Mutex<Vec<String>>>,
}
impl CrontabBackend for FakeCron {
    fn preflight(&self) -> CrontabFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn list(&self) -> CrontabFuture<'_, String> {
        Box::pin(async { Ok(String::new()) })
    }
    fn validate<'a>(&'a self, _: &'a str) -> CrontabFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn install<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()> {
        self.installed.lock().unwrap().push(candidate.to_owned());
        Box::pin(async { Ok(()) })
    }
}
struct Fixture {
    _dir: TempDir,
    store: Store,
    inspection: InspectionService,
    installed_crontabs: Arc<Mutex<Vec<String>>>,
    server: StdioServer,
}
impl Fixture {
    fn new(provider: Arc<dyn Provider>) -> Self {
        Self::with_tools(provider, Arc::new(EmptyTools))
    }
    fn with_tools(provider: Arc<dyn Provider>, mcp: Arc<dyn ToolExecutor>) -> Self {
        Self::with_config(provider, mcp, "", "")
    }
    fn with_config(
        provider: Arc<dyn Provider>,
        mcp: Arc<dyn ToolExecutor>,
        scheduler: &str,
        limits: &str,
    ) -> Self {
        let dir = common::private_tempdir();
        let db = dir.path().join("agent.sqlite");
        let store = Store::open(&db).unwrap();
        let config = dir.path().join("server.toml");
        std::fs::write(&config, format!("[provider]\napi_key = \"test\"\n[database]\npath = \"{}\"\n[scheduler]\nlock_path = \"{}\"\nbinary_path = \"/opt/light-agent/bin/light-agent\"\ncrontab_binary = \"/usr/bin/crontab\"\n{scheduler}\n[limits]\n{limits}\n", db.display(), dir.path().join("cron.lock").display())).unwrap();
        let settings = Arc::new(ServerSettings::load(&config, None).unwrap());
        let installed_crontabs = Arc::new(Mutex::new(Vec::new()));
        let synchronizer = Arc::new(
            CronSynchronizer::new(
                store.clone(),
                Arc::new(FakeCron {
                    installed: installed_crontabs.clone(),
                }),
                dir.path().join("cron.lock"),
                PathBuf::from("/opt/light-agent/bin/light-agent"),
            )
            .unwrap(),
        );
        let inspection = InspectionService::with_page_size(store.clone(), 1).unwrap();
        let server = StdioServer::new(ServerDependencies {
            settings,
            store: store.clone(),
            provider,
            mcp,
            synchronizer,
            inspection: inspection.clone(),
        });
        Self {
            _dir: dir,
            store,
            inspection,
            installed_crontabs,
            server,
        }
    }
}
struct Session {
    requests: NdjsonWriter<WriteHalf<DuplexStream>>,
    events: NdjsonReader<ReadHalf<DuplexStream>>,
    _task: JoinHandle<Result<(), deepseek_cli::server::ServerError>>,
}
impl Session {
    async fn start(server: StdioServer) -> Self {
        let (client, remote) = duplex(2 * MAX_LINE_BYTES);
        let (client_read, client_write) = split(client);
        let (remote_read, remote_write) = split(remote);
        let task = tokio::spawn(async move { server.serve(remote_read, remote_write).await });
        Self {
            requests: NdjsonWriter::new(client_write),
            events: NdjsonReader::new(client_read),
            _task: task,
        }
    }
    async fn send(&mut self, request_id: RequestId, request: ClientRequest) {
        self.requests
            .write_request(&RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id,
                request,
            })
            .await
            .unwrap();
    }
    async fn event(&mut self) -> deepseek_cli::protocol::ServerEnvelope {
        self.events.read_event().await.unwrap().unwrap()
    }
}
fn req() -> RequestId {
    RequestId::new()
}

async fn confirm_scheduled_turn(
    session: &mut Session,
    dialog_id: deepseek_cli::domain::DialogId,
    expected_timezone: &str,
) {
    let origin = req();
    session
        .send(
            origin,
            ClientRequest::SendMessage {
                dialog_id,
                message: "schedule it".into(),
            },
        )
        .await;
    let confirmation_id = loop {
        let event = session.event().await;
        assert_eq!(event.request_id, origin);
        if let ServerEvent::ConfirmationRequired {
            confirmation_id,
            preview,
        } = event.event
        {
            assert_eq!(preview.timezone, expected_timezone);
            assert!(
                render_confirmation_preview(&preview)
                    .contains(&format!("timezone: {expected_timezone:?}"))
            );
            break confirmation_id;
        }
    };
    let response = req();
    session
        .send(
            response,
            ClientRequest::ConfirmAction {
                confirmation_id,
                originating_request_id: origin,
            },
        )
        .await;
    loop {
        let event = session.event().await;
        if event.request_id == response {
            assert!(matches!(
                event.event,
                ServerEvent::ConfirmationResolved { accepted: true, .. }
            ));
        } else if event.request_id == origin
            && matches!(event.event, ServerEvent::TurnCompleted { .. })
        {
            break;
        }
    }
}

#[tokio::test]
async fn hello_precedes_requests() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let mut session = Session::start(fixture.server).await;
    session.send(req(), ClientRequest::ListDialogs).await;
    assert!(matches!(session.event().await.event, ServerEvent::Hello));
    assert!(matches!(
        session.event().await.event,
        ServerEvent::DialogList { complete: true, .. }
    ));
}

#[tokio::test]
async fn interactive_catalog_advertises_and_dispatches_current_time() {
    let provider = FakeProvider::new([
        Reply::ToolCall(ModelToolCall {
            id: "time-call".into(),
            name: "time__get_current_time".into(),
            arguments: "{}".into(),
        }),
        Reply::Final("done".into()),
    ]);
    let fixture = Fixture::new(provider.clone());
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    session
        .send(
            req(),
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "What time is it?".into(),
            },
        )
        .await;
    loop {
        match session.event().await.event {
            ServerEvent::TurnCompleted { .. } => break,
            ServerEvent::TurnFailed { .. } => panic!("time tool failed to dispatch"),
            _ => {}
        }
    }
    let catalogs = provider.tools_seen();
    assert!(catalogs[0].contains(&"time__get_current_time".to_owned()));
    assert!(catalogs[0].contains(&"cron__create".to_owned()));
    let runs = store.list_tool_runs().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(
        (runs[0].server_name.as_str(), runs[0].tool_name.as_str()),
        ("time", "get_current_time")
    );
    assert!(runs[0].read_only);
    assert_eq!(runs[0].status, ToolRunStatus::Completed);
}

#[tokio::test]
async fn dialog_crud_round_trip() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    session
        .send(
            req(),
            ClientRequest::CreateDialog {
                title: "one".into(),
            },
        )
        .await;
    let dialog_id = match session.event().await.event {
        ServerEvent::DialogOpened { dialog_id, title } => {
            assert_eq!(title, "one");
            dialog_id
        }
        other => panic!("{other:?}"),
    };
    session
        .send(
            req(),
            ClientRequest::RenameDialog {
                dialog_id,
                title: "two".into(),
            },
        )
        .await;
    assert!(
        matches!(session.event().await.event, ServerEvent::DialogOpened { title, .. } if title == "two")
    );
    session
        .send(req(), ClientRequest::DeleteDialog { dialog_id })
        .await;
    assert!(
        matches!(session.event().await.event, ServerEvent::DialogList { dialogs, complete: true, .. } if dialogs.is_empty())
    );
}

#[tokio::test]
async fn dialog_permission_update_is_persisted_only_for_the_target_dialog() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let first = fixture.store.create_dialog("first").unwrap();
    let second = fixture.store.create_dialog("second").unwrap();
    let store = fixture.store.clone();
    let mut session = Session::start(fixture.server).await;
    session.event().await;

    session
        .send(
            req(),
            ClientRequest::SetDialogPermission {
                dialog_id: first.id,
                confirmation_required: false,
            },
        )
        .await;
    assert_eq!(
        session.event().await.event,
        ServerEvent::DialogPermissionChanged {
            dialog_id: first.id,
            confirmation_required: false,
        }
    );
    assert!(
        !store
            .get_dialog(first.id)
            .unwrap()
            .cron_confirmation_required
    );
    assert!(
        store
            .get_dialog(second.id)
            .unwrap()
            .cron_confirmation_required
    );
}

#[tokio::test]
async fn send_streams_events_and_commits_answer() {
    let fixture = Fixture::new(FakeProvider::new([Reply::Final("answer".into())]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    session
        .send(
            req(),
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "question".into(),
            },
        )
        .await;
    assert!(
        matches!(session.event().await.event, ServerEvent::ResponseStarted { dialog_id } if dialog_id == dialog)
    );
    assert!(
        matches!(session.event().await.event, ServerEvent::TextDelta { text } if text == "answer")
    );
    assert!(
        matches!(session.event().await.event, ServerEvent::TurnPrepared { answer } if answer == "answer")
    );
    assert!(
        matches!(session.event().await.event, ServerEvent::TurnCompleted { answer } if answer == "answer")
    );
    assert_eq!(
        store
            .completed_messages(dialog)
            .unwrap()
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        ["question", "answer"]
    );
}

#[tokio::test]
async fn configured_message_limit_rejects_send_before_persistence() {
    let provider = FakeProvider::new([Reply::Final("must not run".into())]);
    let fixture = Fixture::with_config(
        provider.clone(),
        Arc::new(EmptyTools),
        "",
        "message_bytes = 1024",
    );
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let database_path = fixture._dir.path().join("agent.sqlite");
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    let request_id = req();
    session
        .send(
            request_id,
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "x".repeat(1025),
            },
        )
        .await;

    assert!(matches!(
        session.event().await.event,
        ServerEvent::ProtocolError {
            code: deepseek_cli::protocol::ProtocolErrorCode::ContentTooLong
        }
    ));
    assert!(store.completed_messages(dialog).unwrap().is_empty());
    let connection = rusqlite::Connection::open(database_path).unwrap();
    let turns: i64 = connection
        .query_row("SELECT count(*) FROM turns", [], |row| row.get(0))
        .unwrap();
    assert_eq!(turns, 0);
    assert_eq!(provider.replies.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn inspection_works_while_turn_is_busy() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fixture = Fixture::new(FakeProvider::new([Reply::Wait(
        entered.clone(),
        release.clone(),
        "done".into(),
    )]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    session
        .send(
            req(),
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "wait".into(),
            },
        )
        .await;
    session.event().await;
    entered.notified().await;
    let inspect = req();
    session
        .send(
            inspect,
            ClientRequest::Inspect {
                kind: InspectKind::Dialogs,
            },
        )
        .await;
    loop {
        let event = session.event().await;
        if event.request_id == inspect
            && matches!(
                event.event,
                ServerEvent::InspectionResult { complete: true, .. }
            )
        {
            break;
        }
    }
    release.notify_waiters();
}

#[tokio::test]
async fn second_writer_gets_busy() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fixture = Fixture::new(FakeProvider::new([Reply::Wait(
        entered.clone(),
        release.clone(),
        "done".into(),
    )]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    session
        .send(
            req(),
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "first".into(),
            },
        )
        .await;
    session.event().await;
    entered.notified().await;
    let second = req();
    session
        .send(
            second,
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "second".into(),
            },
        )
        .await;
    let event = session.event().await;
    assert_eq!(event.request_id, second);
    assert!(matches!(event.event, ServerEvent::TurnFailed { .. }));
    release.notify_waiters();
}

#[tokio::test]
async fn confirmation_routes_only_to_originating_request() {
    let call = ModelToolCall {
        id: "schedule".into(),
        name: "cron__create".into(),
        arguments: json!({
            "name":"report",
            "schedule":{"kind":"cron","expression":"0 9 * * *"},
            "timezone":"Europe/Moscow",
            "prompt":"prepare report"
        })
        .to_string(),
    };
    let fixture = Fixture::new(FakeProvider::new([
        Reply::ToolCall(call),
        Reply::Final("scheduled".into()),
    ]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    let origin = req();
    session
        .send(
            origin,
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "schedule it".into(),
            },
        )
        .await;
    let mut confirmation_id = None;
    let mut tool_started = false;
    while confirmation_id.is_none() || !tool_started {
        let event = session.event().await;
        assert_eq!(event.request_id, origin);
        match event.event {
            ServerEvent::ConfirmationRequired {
                confirmation_id: id,
                preview,
                ..
            } => {
                assert_eq!(preview.task, "prepare report");
                assert_eq!(preview.action, ConfirmationAction::Create);
                assert_eq!(preview.job_id, None);
                assert_eq!(preview.name, "report");
                assert_eq!(preview.schedule_kind, ConfirmationScheduleKind::Cron);
                assert_eq!(preview.schedule_value, "0 9 * * *");
                assert_eq!(preview.timezone, "Europe/Moscow");
                let rendered = render_confirmation_preview(&preview);
                for expected in [
                    "action: create",
                    "job_id: new",
                    "name: \"report\"",
                    "schedule_kind: cron",
                    "schedule_value: \"0 9 * * *\"",
                    "timezone: \"Europe/Moscow\"",
                    "task: \"prepare report\"",
                ] {
                    assert!(rendered.contains(expected), "missing {expected:?}");
                }
                confirmation_id = Some(id);
            }
            ServerEvent::ToolStarted { .. } => tool_started = true,
            ServerEvent::ResponseStarted { .. } => {}
            other => panic!("{other:?}"),
        }
    }
    let confirmation_id = confirmation_id.unwrap();
    let wrong = req();
    session
        .send(
            wrong,
            ClientRequest::ConfirmAction {
                confirmation_id,
                originating_request_id: wrong,
            },
        )
        .await;
    let rejected = session.event().await;
    assert_eq!(rejected.request_id, wrong);
    assert!(matches!(rejected.event, ServerEvent::ProtocolError { .. }));
    assert!(store.list_jobs().unwrap().is_empty());
    let accepted = req();
    session
        .send(
            accepted,
            ClientRequest::ConfirmAction {
                confirmation_id,
                originating_request_id: origin,
            },
        )
        .await;
    loop {
        let event = session.event().await;
        if event.request_id == accepted {
            assert!(matches!(
                event.event,
                ServerEvent::ConfirmationResolved { accepted: true, .. }
            ));
        } else if matches!(event.event, ServerEvent::TurnCompleted { .. }) {
            break;
        }
    }
    assert_eq!(store.list_jobs().unwrap().len(), 1);
    let replay = req();
    session
        .send(
            replay,
            ClientRequest::ConfirmAction {
                confirmation_id,
                originating_request_id: origin,
            },
        )
        .await;
    let replay_event = session.event().await;
    assert_eq!(replay_event.request_id, replay);
    assert!(matches!(
        replay_event.event,
        ServerEvent::ProtocolError { .. }
    ));
}

#[tokio::test]
async fn configured_scheduler_timezone_defaults_and_explicit_override_agree_end_to_end() {
    let provider = FakeProvider::new([
        Reply::ToolCall(ModelToolCall {
            id: "default-utc".into(),
            name: "cron__create".into(),
            arguments: json!({
                "name":"UTC report",
                "schedule":{"kind":"cron","expression":"0 9 * * *"},
                "prompt":"utc task"
            })
            .to_string(),
        }),
        Reply::Final("scheduled UTC".into()),
        Reply::ToolCall(ModelToolCall {
            id: "explicit-moscow".into(),
            name: "cron__create".into(),
            arguments: json!({
                "name":"Moscow report",
                "schedule":{"kind":"cron","expression":"30 9 * * *"},
                "timezone":"Europe/Moscow",
                "prompt":"moscow task"
            })
            .to_string(),
        }),
        Reply::Final("scheduled Moscow".into()),
    ]);
    let fixture = Fixture::with_config(
        provider,
        Arc::new(EmptyTools),
        "timezone = \"UTC\"\nconfirmation_timeout_minutes = 2",
        "message_bytes = 1024",
    );
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let installed = fixture.installed_crontabs.clone();
    let mut session = Session::start(fixture.server).await;
    session.event().await;

    confirm_scheduled_turn(&mut session, dialog, "UTC").await;
    confirm_scheduled_turn(&mut session, dialog, "Europe/Moscow").await;

    let jobs = store.list_jobs().unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].schedule.timezone().to_string(), "UTC");
    assert_eq!(jobs[1].schedule.timezone().to_string(), "Europe/Moscow");
    let rendered = installed.lock().unwrap().last().unwrap().clone();
    assert!(rendered.contains("CRON_TZ=UTC"));
    assert!(rendered.contains("CRON_TZ=Europe/Moscow"));
}

#[tokio::test]
async fn existing_job_confirmation_wire_renders_the_complete_normalized_snapshot() {
    let provider = FakeProvider::new([]);
    let fixture = Fixture::new(provider.clone());
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let job = fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "existing report".into(),
            schedule: ScheduleSpec::parse_cron("5 10 * * 1-5", Moscow).unwrap(),
            prompt: "full existing task\nsecond line".into(),
        })
        .unwrap();
    let call = ModelToolCall {
        id: "disable".into(),
        name: "cron__disable".into(),
        arguments: json!({"job_id":job.id}).to_string(),
    };
    provider
        .replies
        .lock()
        .unwrap()
        .extend([Reply::ToolCall(call), Reply::Final("not changed".into())]);
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    let origin = req();
    session
        .send(
            origin,
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "disable it".into(),
            },
        )
        .await;
    let (confirmation_id, rendered) = loop {
        let event = session.event().await;
        assert_eq!(event.request_id, origin);
        if let ServerEvent::ConfirmationRequired {
            confirmation_id,
            preview,
        } = event.event
        {
            assert_eq!(preview.action, ConfirmationAction::Disable);
            assert_eq!(preview.job_id, Some(job.id));
            assert_eq!(preview.name, "existing report");
            assert_eq!(preview.schedule_kind, ConfirmationScheduleKind::Cron);
            assert_eq!(preview.schedule_value, "5 10 * * 1-5");
            assert_eq!(preview.timezone, "Europe/Moscow");
            assert_eq!(preview.task, "full existing task\nsecond line");
            break (confirmation_id, render_confirmation_preview(&preview));
        }
    };
    for expected in [
        "action: disable",
        &format!("job_id: {}", job.id),
        "name: \"existing report\"",
        "schedule_kind: cron",
        "schedule_value: \"5 10 * * 1-5\"",
        "timezone: \"Europe/Moscow\"",
        "task: \"full existing task\\nsecond line\"",
    ] {
        assert!(rendered.contains(expected), "missing {expected:?}");
    }
    let response = req();
    session
        .send(
            response,
            ClientRequest::CancelAction {
                confirmation_id,
                originating_request_id: origin,
            },
        )
        .await;
    loop {
        let event = session.event().await;
        if event.request_id == response {
            assert!(matches!(
                event.event,
                ServerEvent::ConfirmationResolved {
                    accepted: false,
                    ..
                }
            ));
        } else if matches!(event.event, ServerEvent::TurnCompleted { .. }) {
            break;
        }
    }
    assert_eq!(
        fixture.store.get_job(job.id).unwrap().desired_state,
        deepseek_cli::domain::JobDesiredState::Active
    );
}

#[tokio::test]
async fn unanswered_confirmation_expires_and_finalizes_audited_turn() {
    let call = ModelToolCall {
        id: "schedule-expired".into(),
        name: "cron__create".into(),
        arguments: json!({
            "name":"report",
            "schedule":{"kind":"cron","expression":"0 9 * * *"},
            "timezone":"Europe/Moscow",
            "prompt":"prepare report"
        })
        .to_string(),
    };
    let fixture = Fixture::new(FakeProvider::new([
        Reply::ToolCall(call),
        Reply::Final("not scheduled".into()),
    ]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let server = fixture
        .server
        .with_confirmation_timeout(Duration::from_millis(20))
        .unwrap();
    let mut session = Session::start(server).await;
    session.event().await;
    let origin = req();
    session
        .send(
            origin,
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "schedule it".into(),
            },
        )
        .await;
    let mut confirmation_id = None;
    let mut saw_failed_tool = false;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), session.event())
            .await
            .expect("confirmation timeout must finish the turn");
        assert_eq!(event.request_id, origin);
        match event.event {
            ServerEvent::ConfirmationRequired {
                confirmation_id: id,
                ..
            } => confirmation_id = Some(id),
            ServerEvent::ToolFinished { .. } => saw_failed_tool = true,
            ServerEvent::TurnCompleted { answer } => {
                assert_eq!(answer, "not scheduled");
                break;
            }
            ServerEvent::ResponseStarted { .. }
            | ServerEvent::ToolStarted { .. }
            | ServerEvent::TextDelta { .. }
            | ServerEvent::TurnPrepared { .. } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }
    let confirmation_id = confirmation_id.expect("confirmation prompt");
    assert!(saw_failed_tool);
    let runs = store.list_tool_runs().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, ToolRunStatus::Failed);
    assert_eq!(runs[0].safe_error_code, Some(SafeErrorCode::ToolError));
    assert!(store.list_jobs().unwrap().is_empty());
    assert_eq!(
        store
            .completed_messages(dialog)
            .unwrap()
            .into_iter()
            .map(|message| message.content)
            .collect::<Vec<_>>(),
        ["schedule it", "not scheduled"]
    );

    let late = req();
    session
        .send(
            late,
            ClientRequest::ConfirmAction {
                confirmation_id,
                originating_request_id: origin,
            },
        )
        .await;
    let replay = session.event().await;
    assert_eq!(replay.request_id, late);
    assert!(matches!(replay.event, ServerEvent::ProtocolError { .. }));
}

#[tokio::test]
async fn export_is_sequence_numbered_base64_with_final_sha256() {
    let fixture = Fixture::new(FakeProvider::new([]));
    fixture.store.create_dialog("chat").unwrap();
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    let id = req();
    session.send(id, ClientRequest::Export).await;
    let mut bytes = Vec::new();
    let mut expected = 0;
    loop {
        let envelope = session.event().await;
        assert_eq!(envelope.request_id, id);
        match envelope.event {
            ServerEvent::ExportChunk {
                sequence,
                data_base64,
            } => {
                assert_eq!(sequence, expected);
                expected += 1;
                bytes.extend(
                    base64::engine::general_purpose::STANDARD
                        .decode(data_base64)
                        .unwrap(),
                );
            }
            ServerEvent::ExportCompleted {
                total_bytes,
                sha256,
            } => {
                assert_eq!(total_bytes as usize, bytes.len());
                assert_eq!(sha256, format!("{:x}", Sha256::digest(&bytes)));
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains("logical_export_v1")
    );
}

#[tokio::test]
async fn eof_cancels_inflight_write_marks_uncertain_and_does_not_retry() {
    let (tools, calls) = PendingWriteTools::new();
    let provider = FakeProvider::new([Reply::ToolCall(ModelToolCall {
        id: "write".into(),
        name: "mcp__write".into(),
        arguments: "{}".into(),
    })]);
    let fixture = Fixture::with_tools(provider, tools);
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    session
        .send(
            req(),
            ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "write".into(),
            },
        )
        .await;
    assert!(matches!(
        session.event().await.event,
        ServerEvent::ResponseStarted { .. }
    ));
    assert!(matches!(
        session.event().await.event,
        ServerEvent::ToolStarted { .. }
    ));
    session.requests.shutdown().await.unwrap();
    let Session {
        requests: _,
        events: _,
        _task: task,
    } = session;
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(
        store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Uncertain
    );
    assert!(store.completed_messages(dialog).unwrap().is_empty());
}

#[test]
fn restart_recovers_pending_turn_and_tools() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let turn = fixture.store.begin_turn(dialog, "request").unwrap();
    fixture
        .store
        .start_tool_run(ToolRunStart {
            owner: ToolOwner::InteractiveTurn(turn.turn_id),
            call_id: "read".into(),
            server_name: "mcp".into(),
            tool_name: "get".into(),
            read_only: true,
            arguments: "{}".into(),
        })
        .unwrap();
    let lease = ProcessLease::acquire(&fixture.store).unwrap();
    assert!(lease.recovery_report().performed);
    let runs = fixture.store.list_tool_runs().unwrap();
    let tool = &runs[0];
    assert_eq!(tool.status, ToolRunStatus::Failed);
    assert_eq!(
        tool.safe_error_code,
        Some(SafeErrorCode::ProcessInterrupted)
    );
}

#[tokio::test]
async fn delete_dialog_with_live_job_is_rejected_but_inspection_still_works() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: dialog,
            name: "job".into(),
            schedule: ScheduleSpec::parse_cron("0 9 * * *", Moscow).unwrap(),
            prompt: "work".into(),
        })
        .unwrap();
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    session
        .send(req(), ClientRequest::DeleteDialog { dialog_id: dialog })
        .await;
    assert!(matches!(
        session.event().await.event,
        ServerEvent::ProtocolError { .. }
    ));
    let inspect = req();
    session
        .send(
            inspect,
            ClientRequest::Inspect {
                kind: InspectKind::Jobs,
            },
        )
        .await;
    loop {
        let event = session.event().await;
        if event.request_id == inspect
            && matches!(
                event.event,
                ServerEvent::InspectionResult { complete: true, .. }
            )
        {
            break;
        }
    }
}

#[tokio::test]
async fn inspection_fragments_one_large_field_and_keeps_every_frame_bounded() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let title = "x".repeat(MAX_LINE_BYTES + 4096);
    fixture.store.create_dialog(&title).unwrap();
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    let id = req();
    session
        .send(
            id,
            ClientRequest::Inspect {
                kind: InspectKind::Dialogs,
            },
        )
        .await;
    let mut record = Vec::new();
    let mut sequence = 0;
    loop {
        let envelope = session.event().await;
        let wire = serde_json::to_vec(&envelope).unwrap();
        assert!(wire.len() < MAX_LINE_BYTES);
        match envelope.event {
            ServerEvent::InspectionResult {
                sequence: actual,
                items,
                complete: false,
                ..
            } => {
                assert_eq!(actual, sequence);
                sequence += 1;
                let data = items[0]["record_fragment"]["data"].as_str().unwrap();
                record.extend(
                    base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .unwrap(),
                );
            }
            ServerEvent::InspectionResult { complete: true, .. } => break,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&record).unwrap()["title"],
        title
    );
}

#[test]
fn protocol_server_never_accesses_raw_provider_diagnostics() {
    assert!(!include_str!("../src/server.rs").contains("raw_diagnostic"));
}

#[tokio::test]
async fn output_sink_failure_does_not_persist_assistant() {
    let fixture = Fixture::new(FakeProvider::new([Reply::Final("must-not-save".into())]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let (client, remote) = duplex(MAX_LINE_BYTES);
    let (_client_read, client_write) = split(client);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, FailAfterLines::new(2)).await });
    let mut requests = NdjsonWriter::new(client_write);
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: req(),
            request: ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "question".into(),
            },
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    drop(requests);
    assert!(store.completed_messages(dialog).unwrap().is_empty());
}

#[tokio::test]
async fn terminal_delivery_failure_does_not_commit_assistant_or_turn() {
    let fixture = Fixture::new(FakeProvider::new([Reply::Final("must-not-save".into())]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let (client, remote) = duplex(MAX_LINE_BYTES);
    let (_client_read, client_write) = split(client);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, FailAfterLines::new(3)).await });
    let mut requests = NdjsonWriter::new(client_write);
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: req(),
            request: ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "question".into(),
            },
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    drop(requests);
    assert!(store.completed_messages(dialog).unwrap().is_empty());
}

#[tokio::test]
async fn dialog_list_pages_multiple_maximum_titles_below_wire_limit() {
    let fixture = Fixture::new(FakeProvider::new([]));
    for index in 0..5 {
        fixture
            .store
            .create_dialog(&format!("{index}{}", "x".repeat(256 * 1024 - 1)))
            .unwrap();
    }
    let mut session = Session::start(fixture.server).await;
    session.event().await;
    let id = req();
    session.send(id, ClientRequest::ListDialogs).await;
    let mut sequence = 0;
    let mut count = 0;
    loop {
        let envelope = session.event().await;
        assert_eq!(envelope.request_id, id);
        assert!(serde_json::to_vec(&envelope).unwrap().len() < MAX_LINE_BYTES);
        match envelope.event {
            ServerEvent::DialogList {
                sequence: actual,
                dialogs,
                complete,
            } => {
                assert_eq!(actual, sequence);
                sequence += 1;
                count += dialogs.len();
                if complete {
                    break;
                }
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(count, 5);
    assert!(sequence > 1);
}

#[tokio::test]
async fn dialog_list_is_one_stable_snapshot_during_concurrent_crud() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let mut original = Vec::new();
    for index in 0..12 {
        let title = format!("{index:03}-{}", "x".repeat(256 * 1024 - 4));
        let dialog = fixture.store.create_dialog(&title).unwrap();
        original.push((dialog.id, title));
    }
    let store = fixture.store.clone();
    let inspection = fixture.inspection.clone();
    let capture = Arc::new(Mutex::new(Vec::new()));
    let blocked = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let waker = Arc::new(Mutex::new(None));
    let writer = ReleasableGateWriter::new(
        2,
        capture.clone(),
        blocked.clone(),
        released.clone(),
        waker.clone(),
    );
    let (client, remote) = duplex(MAX_LINE_BYTES);
    let (_client_read, client_write) = split(client);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, writer).await });
    let mut requests = NdjsonWriter::new(client_write);
    let request_id = req();
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            request: ClientRequest::ListDialogs,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !blocked.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        inspection.active_snapshots(),
        1,
        "DialogList must retain one read snapshot while streaming"
    );

    store.delete_dialog(original[8].0).unwrap();
    store
        .rename_dialog(original[9].0, "renamed-after-first-page")
        .unwrap();
    store.create_dialog("inserted-after-first-page").unwrap();
    released.store(true, Ordering::Release);
    if let Some(waker) = waker.lock().unwrap().take() {
        waker.wake();
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let bytes = capture.lock().unwrap().clone();
            let complete = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .filter_map(|line| {
                    serde_json::from_slice::<deepseek_cli::protocol::ServerEnvelope>(line).ok()
                })
                .any(|event| {
                    event.request_id == request_id
                        && matches!(event.event, ServerEvent::DialogList { complete: true, .. })
                });
            if complete {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dialog list must always terminate with complete=true");
    requests.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(inspection.active_snapshots(), 0);

    let bytes = capture.lock().unwrap().clone();
    let mut actual = Vec::new();
    let mut sequence = 0_u64;
    let mut saw_complete = false;
    for event in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<deepseek_cli::protocol::ServerEnvelope>(line).unwrap())
        .filter(|event| event.request_id == request_id)
    {
        match event.event {
            ServerEvent::DialogList {
                sequence: actual_sequence,
                dialogs,
                complete,
            } => {
                assert_eq!(actual_sequence, sequence);
                sequence += 1;
                actual.extend(dialogs.into_iter().map(|dialog| (dialog.id, dialog.title)));
                saw_complete |= complete;
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(saw_complete);
    assert_eq!(actual, original);
}

#[tokio::test]
async fn eof_joins_a_blocked_dialog_list_snapshot() {
    let fixture = Fixture::new(FakeProvider::new([]));
    for index in 0..12 {
        fixture
            .store
            .create_dialog(&format!("{index:03}-{}", "x".repeat(256 * 1024 - 4)))
            .unwrap();
    }
    let inspection = fixture.inspection.clone();
    let writer = GateWriter::new(
        2,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicBool::new(false)),
    );
    let (client, remote) = duplex(MAX_LINE_BYTES);
    let (_client_read, client_write) = split(client);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, writer).await });
    let mut requests = NdjsonWriter::new(client_write);
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: req(),
            request: ClientRequest::ListDialogs,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while inspection.active_snapshots() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    requests.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(inspection.active_snapshots(), 0);
}

#[tokio::test]
async fn malformed_request_error_is_drained_before_session_shutdown() {
    let fixture = Fixture::new(FakeProvider::new([]));
    let capture = Arc::new(Mutex::new(Vec::new()));
    let released = Arc::new(AtomicBool::new(false));
    let blocked = Arc::new(AtomicBool::new(false));
    let waker = Arc::new(Mutex::new(None));
    let writer = ReleasableGateWriter::new(
        1,
        capture.clone(),
        blocked.clone(),
        released.clone(),
        waker.clone(),
    );
    let (mut client, remote) = duplex(MAX_LINE_BYTES);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, writer).await });
    use tokio::io::AsyncWriteExt as _;
    client.write_all(b"not-json\n").await.unwrap();
    client.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !blocked.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !task.is_finished(),
        "server returned before the terminal protocol error was written"
    );

    released.store(true, Ordering::Release);
    if let Some(waker) = waker.lock().unwrap().take() {
        waker.wake();
    }
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let events = capture.lock().unwrap().clone();
    let frames = events
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<deepseek_cli::protocol::ServerEnvelope>(line).unwrap())
        .collect::<Vec<_>>();
    assert!(matches!(
        frames.last().unwrap().event,
        ServerEvent::ProtocolError { .. }
    ));
}

#[tokio::test]
async fn eof_interrupts_running_sqlite_for_inspection_and_export() {
    for request in [
        ClientRequest::Inspect {
            kind: InspectKind::Dialogs,
        },
        ClientRequest::Export,
    ] {
        let fixture = Fixture::new(FakeProvider::new([]));
        let path = fixture._dir.path().join("agent.sqlite");
        let setup = rusqlite::Connection::open(path).unwrap();
        setup
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 DROP TABLE dialogs;
                 CREATE TABLE inspection_slow_source(n INTEGER PRIMARY KEY);
                 WITH digits(d) AS (
                   VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
                 )
                 INSERT INTO inspection_slow_source(n)
                 SELECT hundreds.d * 100 + tens.d * 10 + ones.d
                 FROM digits hundreds, digits tens, digits ones;
                 CREATE VIEW dialogs AS
                 SELECT a.n * 1000000 + b.n * 1000 + c.n AS id,
                        'slow' AS title,
                        '2026-09-26T00:00:00Z' AS created_at,
                        '2026-09-26T00:00:00Z' AS updated_at
                 FROM inspection_slow_source a
                 CROSS JOIN inspection_slow_source b
                 CROSS JOIN inspection_slow_source c;",
            )
            .unwrap();
        drop(setup);
        let inspection = fixture.inspection.clone();
        let (client, remote) = duplex(MAX_LINE_BYTES);
        let (_client_read, client_write) = split(client);
        let (remote_read, remote_write) = split(remote);
        let server = fixture.server;
        let task = tokio::spawn(async move { server.serve(remote_read, remote_write).await });
        let mut requests = NdjsonWriter::new(client_write);
        requests
            .write_request(&RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: req(),
                request,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while inspection.active_snapshots() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let started = Instant::now();
        requests.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .expect("EOF must interrupt the running SQLite query")
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(inspection.active_snapshots(), 0);
    }
}

#[tokio::test]
async fn blocked_stdout_still_processes_confirmation_and_eof() {
    let call = ModelToolCall {
        id: "schedule".into(),
        name: "cron__create".into(),
        arguments: json!({
            "name":"report", "schedule":{"kind":"cron","expression":"0 9 * * *"},
            "timezone":"Europe/Moscow", "prompt":"prepare report"
        })
        .to_string(),
    };
    let fixture = Fixture::new(FakeProvider::new([
        Reply::ToolCall(call),
        Reply::Final("scheduled".into()),
    ]));
    let dialog = fixture.store.create_dialog("chat").unwrap().id;
    let store = fixture.store.clone();
    let capture = Arc::new(Mutex::new(Vec::new()));
    let blocked = Arc::new(AtomicBool::new(false));
    let writer = GateWriter::new(4, capture.clone(), blocked.clone());
    let (client, remote) = duplex(MAX_LINE_BYTES);
    let (_client_read, client_write) = split(client);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, writer).await });
    let mut requests = NdjsonWriter::new(client_write);
    let origin = req();
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: origin,
            request: ClientRequest::SendMessage {
                dialog_id: dialog,
                message: "schedule it".into(),
            },
        })
        .await
        .unwrap();
    let confirmation_id = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let bytes = capture.lock().unwrap().clone();
            for line in bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                if let Ok(envelope) =
                    serde_json::from_slice::<deepseek_cli::protocol::ServerEnvelope>(line)
                    && let ServerEvent::ConfirmationRequired {
                        confirmation_id, ..
                    } = envelope.event
                {
                    return confirmation_id;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let wrong = req();
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: wrong,
            request: ClientRequest::ConfirmAction {
                confirmation_id,
                originating_request_id: wrong,
            },
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !blocked.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: req(),
            request: ClientRequest::CancelAction {
                confirmation_id: deepseek_cli::domain::ConfirmationId::new(),
                originating_request_id: origin,
            },
        })
        .await
        .unwrap();
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: req(),
            request: ClientRequest::ConfirmAction {
                confirmation_id,
                originating_request_id: origin,
            },
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while store.list_jobs().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    requests.shutdown().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn eof_cooperatively_joins_blocking_inspection_and_releases_snapshot() {
    let fixture = Fixture::new(FakeProvider::new([]));
    for index in 0..80 {
        fixture
            .store
            .create_dialog(&format!("{index}{}", "x".repeat(256 * 1024)))
            .unwrap();
    }
    let inspection = fixture.inspection.clone();
    let writer = GateWriter::new(
        1,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicBool::new(false)),
    );
    let (client, remote) = duplex(MAX_LINE_BYTES);
    let (_client_read, client_write) = split(client);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, writer).await });
    let mut requests = NdjsonWriter::new(client_write);
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: req(),
            request: ClientRequest::Inspect {
                kind: InspectKind::Dialogs,
            },
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while inspection.active_snapshots() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    requests.shutdown().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(inspection.active_snapshots(), 0);
}

#[tokio::test]
async fn eof_cooperatively_joins_blocking_export_and_releases_snapshot() {
    let fixture = Fixture::new(FakeProvider::new([]));
    for index in 0..10 {
        fixture
            .store
            .create_dialog(&format!("{index}{}", "x".repeat(256 * 1024)))
            .unwrap();
    }
    let inspection = fixture.inspection.clone();
    let writer = GateWriter::new(
        1,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicBool::new(false)),
    );
    let (client, remote) = duplex(MAX_LINE_BYTES);
    let (_client_read, client_write) = split(client);
    let (remote_read, _remote_write) = split(remote);
    let server = fixture.server;
    let task = tokio::spawn(async move { server.serve(remote_read, writer).await });
    let mut requests = NdjsonWriter::new(client_write);
    requests
        .write_request(&RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: req(),
            request: ClientRequest::Export,
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while inspection.active_snapshots() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    requests.shutdown().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(inspection.active_snapshots(), 0);
}

struct FailAfterLines {
    accepted_lines: usize,
    limit: usize,
}

impl FailAfterLines {
    fn new(limit: usize) -> Self {
        Self {
            accepted_lines: 0,
            limit,
        }
    }
}

impl AsyncWrite for FailAfterLines {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        if self.accepted_lines >= self.limit {
            return std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "closed",
            )));
        }
        self.accepted_lines += bytes.iter().filter(|byte| **byte == b'\n').count();
        std::task::Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

struct GateWriter {
    line_limit: usize,
    complete_lines: usize,
    capture: Arc<Mutex<Vec<u8>>>,
    blocked: Arc<AtomicBool>,
}

struct ReleasableGateWriter {
    line_limit: usize,
    complete_lines: usize,
    capture: Arc<Mutex<Vec<u8>>>,
    blocked: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl ReleasableGateWriter {
    fn new(
        line_limit: usize,
        capture: Arc<Mutex<Vec<u8>>>,
        blocked: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
        waker: Arc<Mutex<Option<Waker>>>,
    ) -> Self {
        Self {
            line_limit,
            complete_lines: 0,
            capture,
            blocked,
            released,
            waker,
        }
    }
}

impl AsyncWrite for ReleasableGateWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        if self.complete_lines >= self.line_limit && !self.released.load(Ordering::Acquire) {
            self.blocked.store(true, Ordering::Release);
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            return std::task::Poll::Pending;
        }
        self.capture.lock().unwrap().extend_from_slice(bytes);
        self.complete_lines += bytes.iter().filter(|byte| **byte == b'\n').count();
        std::task::Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl GateWriter {
    fn new(line_limit: usize, capture: Arc<Mutex<Vec<u8>>>, blocked: Arc<AtomicBool>) -> Self {
        Self {
            line_limit,
            complete_lines: 0,
            capture,
            blocked,
        }
    }
}

impl AsyncWrite for GateWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        if self.complete_lines >= self.line_limit {
            self.blocked.store(true, Ordering::Release);
            return std::task::Poll::Pending;
        }
        self.capture.lock().unwrap().extend_from_slice(bytes);
        self.complete_lines += bytes.iter().filter(|byte| **byte == b'\n').count();
        std::task::Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
