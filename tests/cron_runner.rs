mod common;

use std::collections::VecDeque;
use std::future::pending;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::{America::New_York, Europe::Moscow};
use deepseek_cli::agent_runner::{CronAgentService, CronRunError, CronRunOutcome};
use deepseek_cli::domain::{
    CronRunStatus, JobDesiredState, JobSyncState, ToolOwner, ToolRunStatus,
};
use deepseek_cli::provider::{
    AssistantTurn, ModelToolCall, ModelToolDefinition, Provider, ProviderError, ProviderFuture,
    ProviderMessage, TokenUsage,
};
use deepseek_cli::runtime::ProcessLease;
use deepseek_cli::scheduler::{
    CronClock, CronReconcileFuture, CronRunReconciler, CronSynchronizer, ScheduleSpec,
    SystemCrontabBackend,
};
use deepseek_cli::store::{JobCreate, Store};
use deepseek_cli::tools::{ToolExecutionResult, ToolExecutor, ToolFuture, ToolRoute};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const REQUEST_LIMIT: usize = 512 * 1024;
const MESSAGE_LIMIT: usize = 256 * 1024;

#[derive(Clone)]
enum Reply {
    Turn(AssistantTurn),
    Pending,
}

struct FakeProvider {
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<(Vec<Value>, Vec<String>)>>,
    entered: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
}

impl FakeProvider {
    fn new(replies: impl IntoIterator<Item = Reply>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
            entered: None,
            release: None,
        })
    }

    fn blocking(entered: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(VecDeque::from([Reply::Pending])),
            requests: Mutex::new(Vec::new()),
            entered: Some(entered),
            release: None,
        })
    }

    fn gated(reply: Reply, entered: Arc<Notify>, release: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(VecDeque::from([reply])),
            requests: Mutex::new(Vec::new()),
            entered: Some(entered),
            release: Some(release),
        })
    }

    fn requests(&self) -> Vec<(Vec<Value>, Vec<String>)> {
        self.requests.lock().unwrap().clone()
    }
}

impl Provider for FakeProvider {
    fn stream_turn<'a>(
        &'a self,
        messages: &'a [ProviderMessage],
        tools: &'a [ModelToolDefinition],
        text_sink: &'a mut (dyn FnMut(&str) -> io::Result<()> + Send),
    ) -> ProviderFuture<'a> {
        self.requests.lock().unwrap().push((
            messages
                .iter()
                .map(|message| serde_json::to_value(message).unwrap())
                .collect(),
            tools.iter().map(|tool| tool.name.clone()).collect(),
        ));
        let reply = self.replies.lock().unwrap().pop_front().unwrap();
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            if let Some(entered) = entered {
                entered.notify_one();
            }
            if let Some(release) = release {
                release.notified().await;
            }
            match reply {
                Reply::Turn(turn) => {
                    if let AssistantTurn::FinalText { content, .. } = &turn {
                        text_sink(content).map_err(|_| ProviderError::safe("output"))?;
                    }
                    Ok(turn)
                }
                Reply::Pending => pending().await,
            }
        })
    }

    fn serialized_request_len(
        &self,
        messages: &[ProviderMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<usize, ProviderError> {
        let tools = tools
            .iter()
            .map(|tool| {
                json!({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&json!({"messages":messages,"tools":tools}))
            .map(|bytes| bytes.len())
            .map_err(|_| ProviderError::safe("serialization"))
    }
}

struct FakeTools {
    definitions: Vec<ModelToolDefinition>,
}

impl FakeTools {
    fn mcp() -> Arc<Self> {
        Arc::new(Self {
            definitions: vec![ModelToolDefinition {
                name: "telegram__send".into(),
                description: Some("send".into()),
                parameters: json!({"type":"object"}).as_object().unwrap().clone(),
                read_only: false,
            }],
        })
    }
}

impl ToolExecutor for FakeTools {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }
    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        (name == "telegram__send").then_some(ToolRoute {
            server_name: "telegram",
            tool_name: "send",
        })
    }
    fn is_read_only(&self, name: &str) -> Option<bool> {
        (name == "telegram__send").then_some(false)
    }
    fn call<'a>(&'a self, _call: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async {
            Ok(ToolExecutionResult {
                content: "sent".into(),
                is_error: false,
                error_code: None,
                delivery_uncertain: false,
            })
        })
    }
}

struct PendingTools {
    definitions: Vec<ModelToolDefinition>,
    entered: Arc<Notify>,
}

impl PendingTools {
    fn new(entered: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            definitions: FakeTools::mcp().definitions.clone(),
            entered,
        })
    }
}

impl ToolExecutor for PendingTools {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        (name == "telegram__send").then_some(ToolRoute {
            server_name: "telegram",
            tool_name: "send",
        })
    }

    fn is_read_only(&self, name: &str) -> Option<bool> {
        (name == "telegram__send").then_some(false)
    }

    fn call<'a>(&'a self, _call: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async move {
            self.entered.notify_one();
            pending().await
        })
    }
}

struct GatedTools {
    definitions: Vec<ModelToolDefinition>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    returned: Arc<Notify>,
}

impl GatedTools {
    fn new(entered: Arc<Notify>, release: Arc<Notify>, returned: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            definitions: FakeTools::mcp().definitions.clone(),
            entered,
            release,
            returned,
        })
    }
}

impl ToolExecutor for GatedTools {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        (name == "telegram__send").then_some(ToolRoute {
            server_name: "telegram",
            tool_name: "send",
        })
    }

    fn is_read_only(&self, name: &str) -> Option<bool> {
        (name == "telegram__send").then_some(false)
    }

    fn call<'a>(&'a self, _call: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            self.returned.notify_one();
            Ok(ToolExecutionResult {
                content: "sent".into(),
                is_error: false,
                error_code: None,
                delivery_uncertain: false,
            })
        })
    }
}

#[derive(Clone)]
struct FixedClock(DateTime<Utc>);

impl CronClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

#[derive(Clone)]
struct AdjustableClock(Arc<Mutex<DateTime<Utc>>>);

impl CronClock for AdjustableClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

struct FailedReconciler;

impl CronRunReconciler for FailedReconciler {
    fn reconcile<'a>(
        &'a self,
        _now: DateTime<Utc>,
        _deadline: Instant,
        _cancellation: CancellationToken,
    ) -> CronReconcileFuture<'a> {
        Box::pin(async { Err(()) })
    }
}

struct PendingReconciler;

impl CronRunReconciler for PendingReconciler {
    fn reconcile<'a>(
        &'a self,
        _now: DateTime<Utc>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> CronReconcileFuture<'a> {
        Box::pin(async move {
            tokio::select! {
                _ = cancellation.cancelled() => Err(()),
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Err(()),
                _ = pending::<()>() => Ok(()),
            }
        })
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    store: Store,
    dialog_id: deepseek_cli::domain::DialogId,
}

fn fixture() -> Fixture {
    let directory = common::private_tempdir();
    let store = Store::open(directory.path().join("agent.sqlite3")).unwrap();
    let dialog_id = store.create_dialog("source").unwrap().id;
    Fixture {
        _directory: directory,
        store,
        dialog_id,
    }
}

fn final_text(value: &str) -> Reply {
    Reply::Turn(AssistantTurn::FinalText {
        content: value.into(),
        usage: Some(TokenUsage::default()),
    })
}

fn recurring(fixture: &Fixture, timezone: chrono_tz::Tz) -> deepseek_cli::store::CronJob {
    let job = fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: fixture.dialog_id,
            name: "report".into(),
            schedule: ScheduleSpec::parse_cron("30 1 * * *", timezone).unwrap(),
            prompt: "SAVED JOB PROMPT".into(),
        })
        .unwrap();
    fixture.store.mark_job_sync_applied(job.id).unwrap();
    fixture.store.get_job(job.id).unwrap()
}

fn service(
    fixture: &Fixture,
    provider: Arc<dyn Provider>,
    clock: DateTime<Utc>,
    timeout: Duration,
) -> CronAgentService {
    CronAgentService::new(
        fixture.store.clone(),
        provider,
        FakeTools::mcp(),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        timeout,
        Some(Arc::new(FailedReconciler)),
    )
    .with_clock(Arc::new(FixedClock(clock)))
}

#[tokio::test]
async fn cron_request_contains_only_cron_system_prompt_and_saved_prompt() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    fixture
        .store
        .complete_turn(
            fixture
                .store
                .begin_turn(fixture.dialog_id, "HISTORY SECRET")
                .unwrap()
                .turn_id,
            "HISTORY ANSWER",
        )
        .unwrap();
    let provider = FakeProvider::new([final_text("done")]);
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    service(&fixture, provider.clone(), now, Duration::from_secs(2))
        .run_job(job.id, CancellationToken::new())
        .await
        .unwrap();

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].0,
        vec![
            json!({"role":"system","content":"CRON SYSTEM"}),
            json!({"role":"user","content":"SAVED JOB PROMPT"}),
        ]
    );
}

#[tokio::test]
async fn cron_catalog_has_mcp_but_no_cron_tools() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let provider = FakeProvider::new([final_text("done")]);
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    service(&fixture, provider.clone(), now, Duration::from_secs(2))
        .run_job(job.id, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(provider.requests()[0].1, vec!["telegram__send"]);
}

#[tokio::test]
async fn result_is_stored_as_run_not_message_and_tool_owner_is_cron_run() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let provider = FakeProvider::new([
        Reply::Turn(AssistantTurn::ToolCalls {
            content: None,
            calls: vec![ModelToolCall {
                id: "call_1".into(),
                name: "telegram__send".into(),
                arguments: "{}".into(),
            }],
            usage: None,
        }),
        final_text("stored result"),
    ]);
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let outcome = service(&fixture, provider, now, Duration::from_secs(2))
        .run_job(job.id, CancellationToken::new())
        .await
        .unwrap();
    let CronRunOutcome::Completed(run_id) = outcome else {
        panic!("expected completed")
    };
    let runs = fixture.store.list_runs(job.id).unwrap();
    assert_eq!(runs[0].status, CronRunStatus::Completed);
    assert_eq!(runs[0].result.as_deref(), Some("stored result"));
    assert!(
        fixture
            .store
            .completed_messages(fixture.dialog_id)
            .unwrap()
            .is_empty()
    );
    let tool_runs = fixture.store.list_tool_runs().unwrap();
    assert_eq!(tool_runs.len(), 1);
    assert_eq!(tool_runs[0].owner, ToolOwner::CronRun(run_id));
}

#[tokio::test]
async fn run_timeout_is_durable() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let provider = FakeProvider::new([Reply::Pending]);
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let error = service(&fixture, provider, now, Duration::from_millis(20))
        .run_job(job.id, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error, CronRunError::TimedOut);
    let runs = fixture.store.list_runs(job.id).unwrap();
    assert_eq!(runs[0].status, CronRunStatus::TimedOut);
}

#[tokio::test]
async fn timeout_during_active_write_tool_terminalizes_audit_before_run() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let provider = FakeProvider::new([Reply::Turn(AssistantTurn::ToolCalls {
        content: None,
        calls: vec![ModelToolCall {
            id: "call_timeout".into(),
            name: "telegram__send".into(),
            arguments: "{}".into(),
        }],
        usage: None,
    })]);
    let entered = Arc::new(Notify::new());
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let service = CronAgentService::new(
        fixture.store.clone(),
        provider,
        PendingTools::new(entered.clone()),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_millis(300),
        Some(Arc::new(FailedReconciler)),
    )
    .with_clock(Arc::new(FixedClock(now)));
    let task = tokio::spawn(async move { service.run_job(job.id, CancellationToken::new()).await });
    entered.notified().await;

    assert_eq!(task.await.unwrap().unwrap_err(), CronRunError::TimedOut);
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::TimedOut
    );
    let tools = fixture.store.list_tool_runs().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].status, ToolRunStatus::Uncertain);
    assert!(tools[0].finished_at.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_during_locked_active_tool_audit_is_bounded_and_recoverable() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let provider = FakeProvider::new([Reply::Turn(AssistantTurn::ToolCalls {
        content: None,
        calls: vec![ModelToolCall {
            id: "call_cancel".into(),
            name: "telegram__send".into(),
            arguments: "{}".into(),
        }],
        usage: None,
    })]);
    let entered = Arc::new(Notify::new());
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let service = CronAgentService::new(
        fixture.store.clone(),
        provider,
        PendingTools::new(entered.clone()),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_secs(5),
        Some(Arc::new(FailedReconciler)),
    )
    .with_clock(Arc::new(FixedClock(now)));
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task = tokio::spawn(async move { service.run_job(job.id, task_cancellation).await });
    entered.notified().await;
    let blocker =
        rusqlite::Connection::open(fixture._directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let cancelled_at = Instant::now();
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_millis(500), &mut task)
        .await
        .expect("active tool audit ignored cancellation")
        .unwrap();
    assert_eq!(result.unwrap_err(), CronRunError::Store);
    assert!(cancelled_at.elapsed() < Duration::from_millis(500));
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Pending
    );
    assert_eq!(
        fixture.store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending
    );

    drop(blocker);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        fixture.store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending,
        "cancelled worker mutated after returning"
    );
    let recovery = ProcessLease::acquire(&fixture.store).unwrap();
    assert_eq!(recovery.recovery_report().tool_runs, 1);
    assert_eq!(recovery.recovery_report().cron_runs, 1);
    assert_eq!(
        fixture.store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Uncertain
    );
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Interrupted
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_interrupts_locked_audit_after_tool_has_returned() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let provider = FakeProvider::new([Reply::Turn(AssistantTurn::ToolCalls {
        content: None,
        calls: vec![ModelToolCall {
            id: "call_returned".into(),
            name: "telegram__send".into(),
            arguments: "{}".into(),
        }],
        usage: None,
    })]);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let returned = Arc::new(Notify::new());
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let service = CronAgentService::new(
        fixture.store.clone(),
        provider,
        GatedTools::new(entered.clone(), release.clone(), returned.clone()),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_secs(5),
        Some(Arc::new(FailedReconciler)),
    )
    .with_clock(Arc::new(FixedClock(now)));
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task = tokio::spawn(async move { service.run_job(job.id, task_cancellation).await });
    entered.notified().await;
    let blocker =
        rusqlite::Connection::open(fixture._directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    release.notify_one();
    returned.notified().await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    let cancelled_at = Instant::now();
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_millis(500), &mut task)
        .await
        .expect("audit finish ignored cancellation after the tool returned")
        .unwrap();
    assert_eq!(result.unwrap_err(), CronRunError::Store);
    assert!(cancelled_at.elapsed() < Duration::from_millis(500));
    assert_eq!(
        fixture.store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending
    );

    drop(blocker);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        fixture.store.list_tool_runs().unwrap()[0].status,
        ToolRunStatus::Pending,
        "cancelled audit worker performed a late write"
    );
    let recovery = ProcessLease::acquire(&fixture.store).unwrap();
    assert_eq!(recovery.recovery_report().tool_runs, 1);
    assert_eq!(recovery.recovery_report().cron_runs, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locked_database_cannot_push_claim_past_the_whole_run_deadline() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let blocker =
        rusqlite::Connection::open(fixture._directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let provider = FakeProvider::new([final_text("must not dispatch")]);
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let started = std::time::Instant::now();
    let result = service(&fixture, provider.clone(), now, Duration::from_millis(80))
        .run_job(job.id, CancellationToken::new())
        .await;
    assert_eq!(result.unwrap_err(), CronRunError::TimedOut);
    assert!(started.elapsed() < Duration::from_millis(500));
    drop(blocker);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fixture.store.list_runs(job.id).unwrap().is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_interrupts_a_locked_claim_without_late_mutation() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let blocker =
        rusqlite::Connection::open(fixture._directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let provider = FakeProvider::new([final_text("must not dispatch")]);
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let service = service(&fixture, provider.clone(), now, Duration::from_secs(5));
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task = tokio::spawn(async move { service.run_job(job.id, task_cancellation).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cancelled_at = std::time::Instant::now();
    cancellation.cancel();

    let result = tokio::time::timeout(Duration::from_millis(300), &mut task).await;
    if result.is_err() {
        drop(blocker);
        let _ = task.await;
        panic!("locked claim ignored cancellation");
    }
    assert_eq!(
        result.unwrap().unwrap().unwrap_err(),
        CronRunError::Interrupted
    );
    assert!(cancelled_at.elapsed() < Duration::from_millis(300));
    drop(blocker);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fixture.store.list_runs(job.id).unwrap().is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_finalization_is_bounded_and_next_exclusive_startup_recovers() {
    let fixture = fixture();
    let lease = ProcessLease::acquire(&fixture.store).unwrap();
    let job = recurring(&fixture, Moscow);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = FakeProvider::gated(final_text("done"), entered.clone(), release.clone());
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let owned_store = lease.owned_store(&fixture.store).unwrap();
    let runner = CronAgentService::new(
        owned_store,
        provider,
        FakeTools::mcp(),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_millis(160),
        Some(Arc::new(FailedReconciler)),
    )
    .with_clock(Arc::new(FixedClock(now)));
    let started = std::time::Instant::now();
    let task = tokio::spawn(async move { runner.run_job(job.id, CancellationToken::new()).await });
    entered.notified().await;
    let blocker =
        rusqlite::Connection::open(fixture._directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    release.notify_one();
    assert_eq!(task.await.unwrap().unwrap_err(), CronRunError::Store);
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Pending
    );
    drop(blocker);
    drop(lease);

    let recovered = ProcessLease::acquire(&fixture.store).unwrap();
    assert_eq!(recovered.recovery_report().cron_runs, 1);
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Interrupted
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_interrupts_locked_finalization_without_late_mutation() {
    let fixture = fixture();
    let lease = ProcessLease::acquire(&fixture.store).unwrap();
    let job = recurring(&fixture, Moscow);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = FakeProvider::gated(final_text("done"), entered.clone(), release.clone());
    let owned_store = lease.owned_store(&fixture.store).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let runner = CronAgentService::new(
        owned_store,
        provider,
        FakeTools::mcp(),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_secs(5),
        Some(Arc::new(FailedReconciler)),
    )
    .with_clock(Arc::new(FixedClock(now)));
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task = tokio::spawn(async move { runner.run_job(job.id, task_cancellation).await });
    entered.notified().await;
    let blocker =
        rusqlite::Connection::open(fixture._directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    release.notify_one();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cancelled_at = std::time::Instant::now();
    cancellation.cancel();

    let result = tokio::time::timeout(Duration::from_millis(300), &mut task).await;
    if result.is_err() {
        drop(blocker);
        let _ = task.await;
        panic!("locked finalization ignored cancellation");
    }
    assert_eq!(
        result.unwrap().unwrap().unwrap_err(),
        CronRunError::Interrupted
    );
    assert!(cancelled_at.elapsed() < Duration::from_millis(300));
    drop(blocker);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Pending
    );
    drop(lease);
    let recovered = ProcessLease::acquire(&fixture.store).unwrap();
    assert_eq!(recovered.recovery_report().cron_runs, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn once_at_time_is_sampled_after_the_write_lock_is_acquired() {
    let fixture = fixture();
    let at = Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 0).unwrap();
    let job = fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: fixture.dialog_id,
            name: "once".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "once prompt".into(),
        })
        .unwrap();
    fixture.store.mark_job_sync_applied(job.id).unwrap();
    let blocker =
        rusqlite::Connection::open(fixture._directory.path().join("agent.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let provider = FakeProvider::new([final_text("done")]);
    let value = Arc::new(Mutex::new(at - chrono::Duration::minutes(1)));
    let clock = AdjustableClock(value.clone());
    let runner = CronAgentService::new(
        fixture.store.clone(),
        provider.clone(),
        FakeTools::mcp(),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_secs(2),
        Some(Arc::new(FailedReconciler)),
    )
    .with_clock(Arc::new(clock));
    let task = tokio::spawn(async move { runner.run_job(job.id, CancellationToken::new()).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    *value.lock().unwrap() = at;
    drop(blocker);
    assert!(matches!(
        task.await.unwrap().unwrap(),
        CronRunOutcome::Completed(_)
    ));
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn once_at_reconciliation_is_inside_the_run_wall_timeout() {
    let fixture = fixture();
    let at = Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 0).unwrap();
    let job = fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: fixture.dialog_id,
            name: "once".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "once prompt".into(),
        })
        .unwrap();
    fixture.store.mark_job_sync_applied(job.id).unwrap();
    let provider = FakeProvider::new([final_text("must not dispatch")]);
    let service = CronAgentService::new(
        fixture.store.clone(),
        provider.clone(),
        FakeTools::mcp(),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_millis(200),
        Some(Arc::new(PendingReconciler)),
    )
    .with_clock(Arc::new(FixedClock(at)));
    assert_eq!(
        service
            .run_job(job.id, CancellationToken::new())
            .await
            .unwrap_err(),
        CronRunError::TimedOut
    );
    assert!(provider.requests().is_empty());
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::TimedOut
    );
}

#[cfg(unix)]
#[tokio::test]
async fn reconciliation_timeout_kills_the_crontab_preflight_process() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = fixture();
    let at = Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 0).unwrap();
    let job = fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: fixture.dialog_id,
            name: "once".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "once prompt".into(),
        })
        .unwrap();
    fixture.store.mark_job_sync_applied(job.id).unwrap();

    let directory = common::private_tempdir();
    let executable = directory.path().join("fake-crontab");
    let escaped_marker = directory.path().join("preflight-completed");
    std::fs::write(
        &executable,
        format!(
            "#!/bin/sh\nsleep 1\ntouch '{}'\nprintf 'cronie 1.7.2\\n'\n",
            escaped_marker.display()
        ),
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&executable, permissions).unwrap();
    let backend = Arc::new(SystemCrontabBackend::new(executable).unwrap());
    let synchronizer = Arc::new(
        CronSynchronizer::new(
            fixture.store.clone(),
            backend,
            fixture._directory.path().join("cron.lock"),
            "/opt/light-agent/bin/light-agent".into(),
        )
        .unwrap(),
    );
    let reconciler: Arc<dyn CronRunReconciler> = synchronizer;
    let provider = FakeProvider::new([final_text("must not dispatch")]);
    let service = CronAgentService::new(
        fixture.store.clone(),
        provider,
        FakeTools::mcp(),
        "CRON SYSTEM",
        8,
        REQUEST_LIMIT,
        MESSAGE_LIMIT,
        Duration::from_millis(200),
        Some(reconciler),
    )
    .with_clock(Arc::new(FixedClock(at)));

    assert_eq!(
        service
            .run_job(job.id, CancellationToken::new())
            .await
            .unwrap_err(),
        CronRunError::TimedOut
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(
        !escaped_marker.exists(),
        "timed-out crontab preflight survived the run"
    );
}

#[tokio::test]
async fn overlap_records_skipped_without_provider_call() {
    let fixture = fixture();
    let job = recurring(&fixture, Moscow);
    let entered = Arc::new(Notify::new());
    let provider = FakeProvider::blocking(entered.clone());
    let now = Utc.with_ymd_and_hms(2026, 9, 26, 6, 30, 0).unwrap();
    let first_cancel = CancellationToken::new();
    let first_service = service(&fixture, provider.clone(), now, Duration::from_secs(2));
    let first_token = first_cancel.clone();
    let first = tokio::spawn(async move { first_service.run_job(job.id, first_token).await });
    entered.notified().await;

    let second = service(&fixture, provider.clone(), now, Duration::from_secs(2))
        .run_job(job.id, CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(second, CronRunOutcome::Skipped(_)));
    assert_eq!(provider.requests().len(), 1);
    first_cancel.cancel();
    assert_eq!(first.await.unwrap().unwrap_err(), CronRunError::Interrupted);
    let statuses = fixture
        .store
        .list_runs(job.id)
        .unwrap()
        .into_iter()
        .map(|run| run.status)
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![CronRunStatus::Interrupted, CronRunStatus::Skipped]
    );
}

#[tokio::test]
async fn once_at_claim_disables_before_agent_starts() {
    let fixture = fixture();
    let at = Utc.with_ymd_and_hms(2026, 9, 26, 7, 30, 0).unwrap();
    let job = fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: fixture.dialog_id,
            name: "once".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "once prompt".into(),
        })
        .unwrap();
    fixture.store.mark_job_sync_applied(job.id).unwrap();
    let entered = Arc::new(Notify::new());
    let provider = FakeProvider::blocking(entered.clone());
    let cancellation = CancellationToken::new();
    let runner = service(&fixture, provider, at, Duration::from_secs(2));
    let child_token = cancellation.clone();
    let task = tokio::spawn(async move { runner.run_job(job.id, child_token).await });
    entered.notified().await;
    let stored = fixture.store.get_job(job.id).unwrap();
    assert_eq!(stored.desired_state, JobDesiredState::Disabled);
    assert_eq!(stored.sync_state, JobSyncState::Pending);
    cancellation.cancel();
    assert_eq!(task.await.unwrap().unwrap_err(), CronRunError::Interrupted);
}

#[tokio::test]
async fn moscow_missed_once_at_is_never_late_run() {
    let fixture = fixture();
    let job = fixture
        .store
        .create_job(JobCreate {
            source_dialog_id: fixture.dialog_id,
            name: "missed".into(),
            schedule: ScheduleSpec::parse_once_at("2026-09-26T10:30", Moscow).unwrap(),
            prompt: "missed prompt".into(),
        })
        .unwrap();
    fixture.store.mark_job_sync_applied(job.id).unwrap();
    let provider = FakeProvider::new([]);
    let late = Utc.with_ymd_and_hms(2026, 9, 26, 7, 31, 0).unwrap();
    let outcome = service(&fixture, provider.clone(), late, Duration::from_secs(2))
        .run_job(job.id, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome, CronRunOutcome::Inactive);
    assert!(provider.requests().is_empty());
    assert_eq!(
        fixture.store.list_runs(job.id).unwrap()[0].status,
        CronRunStatus::Missed
    );
}

#[tokio::test]
async fn dst_fallback_allows_only_one_active_claim_for_the_job() {
    let fixture = fixture();
    let job = recurring(&fixture, New_York);
    let entered = Arc::new(Notify::new());
    let provider = FakeProvider::blocking(entered.clone());
    let first_time = Utc.with_ymd_and_hms(2026, 11, 1, 5, 30, 0).unwrap();
    let second_time = Utc.with_ymd_and_hms(2026, 11, 1, 6, 30, 0).unwrap();
    let cancel = CancellationToken::new();
    let runner = service(
        &fixture,
        provider.clone(),
        first_time,
        Duration::from_secs(2),
    );
    let token = cancel.clone();
    let first = tokio::spawn(async move { runner.run_job(job.id, token).await });
    entered.notified().await;
    let second = service(
        &fixture,
        provider.clone(),
        second_time,
        Duration::from_secs(2),
    )
    .run_job(job.id, CancellationToken::new())
    .await
    .unwrap();
    assert!(matches!(second, CronRunOutcome::Skipped(_)));
    assert_eq!(provider.requests().len(), 1);
    cancel.cancel();
    let _ = first.await;
}
