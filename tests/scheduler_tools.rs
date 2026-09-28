mod common;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::UTC;
use deepseek_cli::domain::{DialogId, JobDesiredState, JobId, JobSyncState, RequestId};
use deepseek_cli::provider::ModelToolCall;
use deepseek_cli::scheduler::{CronSynchronizer, CrontabBackend, CrontabFuture, SchedulerError};
use deepseek_cli::store::Store;
use deepseek_cli::tools::scheduler::{
    ConfirmationBroker, ConfirmationClock, ConfirmationError, ConfirmationFuture,
    ConfirmationRequest, SchedulerToolExecutor,
};
use deepseek_cli::tools::{CompositeToolExecutor, ToolExecutor};
use serde_json::{Value, json};
use tokio::sync::{Notify, oneshot};

const REQUEST_A: &str = "123e4567-e89b-42d3-a456-426614174000";
const REQUEST_B: &str = "123e4567-e89b-42d3-a456-426614174001";

fn request(value: &str) -> RequestId {
    RequestId::from_str(value).unwrap()
}

fn call(name: &str, arguments: Value) -> ModelToolCall {
    ModelToolCall {
        id: "call-1".into(),
        name: name.into(),
        arguments: arguments.to_string(),
    }
}

#[derive(Clone)]
struct FixedClock(DateTime<Utc>);

impl ConfirmationClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

#[derive(Clone)]
struct MutableClock(Arc<Mutex<DateTime<Utc>>>);

impl MutableClock {
    fn new(value: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }

    fn advance(&self, duration: Duration) {
        let mut value = self.0.lock().unwrap();
        *value += duration;
    }
}

impl ConfirmationClock for MutableClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

#[derive(Clone)]
struct FakeBackend {
    fail_install: bool,
    installed: Arc<Mutex<Vec<String>>>,
}

impl FakeBackend {
    fn success() -> Self {
        Self {
            fail_install: false,
            installed: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn failing() -> Self {
        Self {
            fail_install: true,
            installed: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl CrontabBackend for FakeBackend {
    fn list(&self) -> CrontabFuture<'_, String> {
        Box::pin(async { Ok(String::new()) })
    }

    fn validate<'a>(&'a self, _candidate: &'a str) -> CrontabFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn install<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()> {
        let fail = self.fail_install;
        let installed = self.installed.clone();
        let candidate = candidate.to_owned();
        Box::pin(async move {
            if fail {
                Err(SchedulerError::Backend)
            } else {
                installed.lock().unwrap().push(candidate);
                Ok(())
            }
        })
    }

    fn preflight(&self) -> CrontabFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct ImmediateBroker {
    result: ConfirmationErrorOrOk,
    requests: Arc<Mutex<Vec<ConfirmationRequest>>>,
}

#[derive(Clone, Copy)]
enum ConfirmationErrorOrOk {
    Ok,
    Error(ConfirmationError),
}

impl ImmediateBroker {
    fn accepting() -> Arc<Self> {
        Arc::new(Self {
            result: ConfirmationErrorOrOk::Ok,
            requests: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn rejecting(error: ConfirmationError) -> Arc<Self> {
        Arc::new(Self {
            result: ConfirmationErrorOrOk::Error(error),
            requests: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

impl ConfirmationBroker for ImmediateBroker {
    fn confirm<'a>(&'a self, request: ConfirmationRequest) -> ConfirmationFuture<'a> {
        self.requests.lock().unwrap().push(request);
        let result = self.result;
        Box::pin(async move {
            match result {
                ConfirmationErrorOrOk::Ok => Ok(()),
                ConfirmationErrorOrOk::Error(error) => Err(error),
            }
        })
    }
}

struct BlockingBroker {
    request: Arc<Mutex<Option<ConfirmationRequest>>>,
    seen: Arc<Notify>,
    response: Mutex<Option<oneshot::Receiver<Result<(), ConfirmationError>>>>,
}

impl ConfirmationBroker for BlockingBroker {
    fn confirm<'a>(&'a self, request: ConfirmationRequest) -> ConfirmationFuture<'a> {
        *self.request.lock().unwrap() = Some(request);
        self.seen.notify_waiters();
        let response = self.response.lock().unwrap().take().unwrap();
        Box::pin(async move {
            response
                .await
                .unwrap_or(Err(ConfirmationError::Unavailable))
        })
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    store: Store,
    source_dialog_id: DialogId,
    backend: Arc<FakeBackend>,
    synchronizer: Arc<CronSynchronizer>,
}

impl Fixture {
    fn new(backend: FakeBackend) -> Self {
        let dir = common::private_tempdir();
        let store = Store::open(dir.path().join("agent.sqlite")).unwrap();
        let source_dialog_id = store.create_dialog("source").unwrap().id;
        let backend = Arc::new(backend);
        let synchronizer = Arc::new(
            CronSynchronizer::new(
                store.clone(),
                backend.clone(),
                dir.path().join("cron.lock"),
                PathBuf::from("/opt/light-agent/bin/light-agent"),
            )
            .unwrap(),
        );
        Self {
            _dir: dir,
            store,
            source_dialog_id,
            backend,
            synchronizer,
        }
    }

    fn executor(
        &self,
        broker: Arc<dyn ConfirmationBroker>,
        request_id: RequestId,
    ) -> SchedulerToolExecutor {
        SchedulerToolExecutor::new_with_clock(
            self.store.clone(),
            self.synchronizer.clone(),
            broker,
            self.source_dialog_id,
            request_id,
            Arc::new(FixedClock(
                Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap(),
            )),
        )
    }
}

fn create_arguments(prompt: &str) -> Value {
    json!({
        "name": "  Morning report  ",
        "schedule": {"kind": "cron", "expression": "0 09 * * 1-5"},
        "timezone": "Europe/Moscow",
        "prompt": prompt
    })
}

fn parse_result(result: &deepseek_cli::tools::ToolExecutionResult) -> Value {
    serde_json::from_str(&result.content).unwrap()
}

#[test]
fn schemas_are_closed_and_session_fields_are_not_model_arguments() {
    let fixture = Fixture::new(FakeBackend::success());
    let executor = fixture.executor(ImmediateBroker::accepting(), request(REQUEST_A));
    let names = executor
        .definitions()
        .iter()
        .map(|definition| definition.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "cron__create",
            "cron__list",
            "cron__get",
            "cron__update",
            "cron__enable",
            "cron__disable",
            "cron__delete"
        ]
    );
    for definition in executor.definitions() {
        let schema = Value::Object(definition.parameters.clone());
        assert_eq!(schema["type"], "object", "{}", definition.name);
        assert_eq!(schema["additionalProperties"], false, "{}", definition.name);
        assert!(schema["properties"].get("source_dialog_id").is_none());
        if let Some(schedule) = schema["properties"].get("schedule") {
            for alternative in schedule["oneOf"].as_array().unwrap() {
                assert_eq!(alternative["additionalProperties"], false);
            }
        }
    }
    assert!(executor.is_read_only("cron__list").unwrap());
    assert!(executor.is_read_only("cron__get").unwrap());
    for name in [
        "cron__create",
        "cron__update",
        "cron__enable",
        "cron__disable",
        "cron__delete",
    ] {
        assert!(!executor.is_read_only(name).unwrap());
    }
}

#[tokio::test]
async fn configured_defaults_bound_prompt_schema_and_confirmation_expiry() {
    let fixture = Fixture::new(FakeBackend::success());
    let broker = ImmediateBroker::accepting();
    let issued_at = Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap();
    let executor = SchedulerToolExecutor::new_with_clock_and_config(
        fixture.store.clone(),
        fixture.synchronizer.clone(),
        broker.clone(),
        fixture.source_dialog_id,
        request(REQUEST_A),
        Arc::new(FixedClock(issued_at)),
        UTC,
        1024,
        std::time::Duration::from_secs(120),
    )
    .unwrap();

    for tool in ["cron__create", "cron__update"] {
        let prompt_limit = executor
            .definitions()
            .iter()
            .find(|definition| definition.name == tool)
            .unwrap()
            .parameters["properties"]["prompt"]["maxLength"]
            .as_u64();
        assert_eq!(prompt_limit, Some(1024));
    }
    let timezone_default = executor
        .definitions()
        .iter()
        .find(|definition| definition.name == "cron__create")
        .unwrap()
        .parameters["properties"]["timezone"]["default"]
        .as_str();
    assert_eq!(timezone_default, Some("UTC"));

    executor
        .call(&call(
            "cron__create",
            json!({
                "name":"UTC report",
                "schedule":{"kind":"cron","expression":"0 9 * * *"},
                "prompt":"report"
            }),
        ))
        .await
        .unwrap();
    let request = broker.requests.lock().unwrap()[0].clone();
    assert_eq!(request.preview.timezone, "UTC");
    assert_eq!(request.expires_at, issued_at + Duration::minutes(2));
    assert_eq!(
        fixture.store.list_jobs().unwrap()[0].schedule.timezone(),
        UTC
    );

    let before = fixture.store.list_jobs().unwrap().len();
    let installs_before = fixture.backend.installed.lock().unwrap().len();
    assert_eq!(
        executor
            .call(&call(
                "cron__create",
                json!({
                    "name":"too large",
                    "schedule":{"kind":"cron","expression":"0 9 * * *"},
                    "timezone":"Europe/Moscow",
                    "prompt":"x".repeat(1025)
                }),
            ))
            .await
            .unwrap_err(),
        deepseek_cli::tools::ToolExecutionError::InvalidArguments
    );
    assert_eq!(fixture.store.list_jobs().unwrap().len(), before);
    assert_eq!(
        fixture.backend.installed.lock().unwrap().len(),
        installs_before
    );

    let existing = fixture.store.list_jobs().unwrap()[0].clone();
    assert_eq!(
        executor
            .call(&call(
                "cron__update",
                json!({
                    "job_id":existing.id,
                    "name":"oversized update",
                    "schedule":{"kind":"cron","expression":"30 10 * * *"},
                    "prompt":"x".repeat(1025)
                }),
            ))
            .await
            .unwrap_err(),
        deepseek_cli::tools::ToolExecutionError::InvalidArguments
    );
    assert_eq!(fixture.store.get_job(existing.id).unwrap(), existing);
    assert_eq!(
        fixture.backend.installed.lock().unwrap().len(),
        installs_before
    );
}

#[test]
fn configured_executor_rejects_unrepresentable_confirmation_duration() {
    let fixture = Fixture::new(FakeBackend::success());
    assert!(
        SchedulerToolExecutor::new_with_clock_and_config(
            fixture.store.clone(),
            fixture.synchronizer.clone(),
            ImmediateBroker::accepting(),
            fixture.source_dialog_id,
            request(REQUEST_A),
            Arc::new(FixedClock(
                Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap(),
            )),
            UTC,
            1024,
            std::time::Duration::MAX,
        )
        .is_err()
    );
}

#[tokio::test]
async fn preview_contains_complete_prompt_and_normalized_values() {
    let fixture = Fixture::new(FakeBackend::success());
    let broker = ImmediateBroker::rejecting(ConfirmationError::Rejected);
    let executor = fixture.executor(broker.clone(), request(REQUEST_A));
    let prompt = "Read MCP data and send the complete morning report.";
    let result = executor
        .call(&call("cron__create", create_arguments(prompt)))
        .await
        .unwrap();
    assert!(result.is_error);
    let requests = broker.requests.lock().unwrap();
    let preview = &requests[0].preview;
    assert_eq!(preview.name, "Morning report");
    assert_eq!(preview.schedule_kind, "cron");
    assert_eq!(preview.schedule_value, "0 9 * * 1-5");
    assert_eq!(preview.timezone, "Europe/Moscow");
    assert_eq!(preview.prompt, prompt);
    assert_eq!(requests[0].request_id, request(REQUEST_A));
    assert_eq!(
        requests[0].expires_at,
        Utc.with_ymd_and_hms(2026, 9, 26, 10, 5, 0).unwrap()
    );
    assert!(fixture.store.list_jobs().unwrap().is_empty());
}

#[tokio::test]
async fn cron_list_and_get_are_read_only_without_confirmation_or_sync() {
    let fixture = Fixture::new(FakeBackend::success());
    let broker = ImmediateBroker::accepting();
    let create = fixture.executor(broker.clone(), request(REQUEST_A));
    let created = create
        .call(&call(
            "cron__create",
            create_arguments("Only the task text"),
        ))
        .await
        .unwrap();
    let job_id = parse_result(&created)["job"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let confirmation_count = broker.requests.lock().unwrap().len();
    let install_count = fixture.backend.installed.lock().unwrap().len();

    let listed = create.call(&call("cron__list", json!({}))).await.unwrap();
    let fetched = create
        .call(&call("cron__get", json!({"job_id": job_id})))
        .await
        .unwrap();
    assert_eq!(parse_result(&listed)["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(parse_result(&fetched)["job"]["name"], "Morning report");
    assert_eq!(broker.requests.lock().unwrap().len(), confirmation_count);
    assert_eq!(
        fixture.backend.installed.lock().unwrap().len(),
        install_count
    );
}

#[tokio::test]
async fn mutation_waits_for_matching_confirmation() {
    let fixture = Fixture::new(FakeBackend::success());
    let (response_tx, response_rx) = oneshot::channel();
    let broker = Arc::new(BlockingBroker {
        request: Arc::new(Mutex::new(None)),
        seen: Arc::new(Notify::new()),
        response: Mutex::new(Some(response_rx)),
    });
    let notified = broker.seen.notified();
    let executor = Arc::new(fixture.executor(broker.clone(), request(REQUEST_A)));
    let task = tokio::spawn(async move {
        executor
            .call(&call("cron__create", create_arguments("task prompt")))
            .await
    });
    notified.await;
    assert!(fixture.store.list_jobs().unwrap().is_empty());
    assert!(fixture.backend.installed.lock().unwrap().is_empty());
    response_tx.send(Ok(())).unwrap();
    assert!(!task.await.unwrap().unwrap().is_error);
    assert_eq!(fixture.store.list_jobs().unwrap().len(), 1);
    assert_eq!(fixture.backend.installed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn dialog_permission_off_applies_mutation_without_requesting_confirmation() {
    let fixture = Fixture::new(FakeBackend::success());
    fixture
        .store
        .set_cron_confirmation_required(fixture.source_dialog_id, false)
        .unwrap();
    let broker = ImmediateBroker::rejecting(ConfirmationError::Rejected);
    let executor = fixture.executor(broker.clone(), request(REQUEST_A));

    let result = executor
        .call(&call(
            "cron__create",
            create_arguments("task without confirmation"),
        ))
        .await
        .unwrap();

    assert!(!result.is_error);
    assert!(broker.requests.lock().unwrap().is_empty());
    assert_eq!(fixture.store.list_jobs().unwrap().len(), 1);
    assert_eq!(fixture.backend.installed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn rejection_changes_no_state() {
    assert_confirmation_error_changes_no_state(ConfirmationError::Rejected).await;
}

#[tokio::test]
async fn broker_already_used_rejection_changes_no_state() {
    assert_confirmation_error_changes_no_state(ConfirmationError::AlreadyUsed).await;
}

#[tokio::test]
async fn confirmation_expires_at_five_minutes() {
    let fixture = Fixture::new(FakeBackend::success());
    let (response_tx, response_rx) = oneshot::channel();
    let broker = Arc::new(BlockingBroker {
        request: Arc::new(Mutex::new(None)),
        seen: Arc::new(Notify::new()),
        response: Mutex::new(Some(response_rx)),
    });
    let clock = MutableClock::new(Utc.with_ymd_and_hms(2026, 9, 26, 10, 0, 0).unwrap());
    let executor = Arc::new(SchedulerToolExecutor::new_with_clock(
        fixture.store.clone(),
        fixture.synchronizer.clone(),
        broker.clone(),
        fixture.source_dialog_id,
        request(REQUEST_A),
        Arc::new(clock.clone()),
    ));
    let notified = broker.seen.notified();
    let task = tokio::spawn(async move {
        executor
            .call(&call("cron__create", create_arguments("task prompt")))
            .await
    });
    notified.await;
    let expires_at = broker.request.lock().unwrap().as_ref().unwrap().expires_at;
    assert_eq!(
        expires_at,
        Utc.with_ymd_and_hms(2026, 9, 26, 10, 5, 0).unwrap()
    );
    clock.advance(Duration::minutes(5));
    response_tx.send(Ok(())).unwrap();
    let result = task.await.unwrap().unwrap();
    assert!(result.is_error);
    assert_eq!(result.error_code.as_deref(), Some("confirmation_rejected"));
    assert!(fixture.store.list_jobs().unwrap().is_empty());
    assert!(fixture.backend.installed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn broker_request_or_session_rejection_changes_no_state() {
    for error in [
        ConfirmationError::WrongRequest,
        ConfirmationError::WrongSession,
    ] {
        assert_confirmation_error_changes_no_state(error).await;
    }
}

#[tokio::test]
async fn stale_state_preview_fails_before_mutation_or_crontab_sync() {
    let fixture = Fixture::new(FakeBackend::success());
    let creator = fixture.executor(ImmediateBroker::accepting(), request(REQUEST_A));
    let created = creator
        .call(&call("cron__create", create_arguments("original task")))
        .await
        .unwrap();
    let job_id = JobId::from_str(parse_result(&created)["job"]["id"].as_str().unwrap()).unwrap();
    let original = fixture.store.get_job(job_id).unwrap();
    let install_count = fixture.backend.installed.lock().unwrap().len();

    let (response_tx, response_rx) = oneshot::channel();
    let broker = Arc::new(BlockingBroker {
        request: Arc::new(Mutex::new(None)),
        seen: Arc::new(Notify::new()),
        response: Mutex::new(Some(response_rx)),
    });
    let notified = broker.seen.notified();
    let executor = Arc::new(fixture.executor(broker.clone(), request(REQUEST_B)));
    let task = tokio::spawn(async move {
        executor
            .call(&call("cron__disable", json!({"job_id":job_id})))
            .await
    });
    notified.await;
    fixture
        .store
        .update_job(
            job_id,
            "Concurrent edit".into(),
            original.schedule,
            "new task from another request".into(),
        )
        .unwrap();
    response_tx.send(Ok(())).unwrap();

    let result = task.await.unwrap().unwrap();
    assert!(result.is_error);
    assert_eq!(result.error_code.as_deref(), Some("job_changed"));
    let current = fixture.store.get_job(job_id).unwrap();
    assert_eq!(current.name, "Concurrent edit");
    assert_eq!(current.desired_state, JobDesiredState::Active);
    assert_eq!(
        fixture.backend.installed.lock().unwrap().len(),
        install_count
    );
}

#[tokio::test]
async fn stale_update_preview_fails_before_overwrite_or_crontab_sync() {
    let fixture = Fixture::new(FakeBackend::success());
    let creator = fixture.executor(ImmediateBroker::accepting(), request(REQUEST_A));
    let created = creator
        .call(&call("cron__create", create_arguments("original task")))
        .await
        .unwrap();
    let job_id = JobId::from_str(parse_result(&created)["job"]["id"].as_str().unwrap()).unwrap();
    let install_count = fixture.backend.installed.lock().unwrap().len();

    let (response_tx, response_rx) = oneshot::channel();
    let broker = Arc::new(BlockingBroker {
        request: Arc::new(Mutex::new(None)),
        seen: Arc::new(Notify::new()),
        response: Mutex::new(Some(response_rx)),
    });
    let notified = broker.seen.notified();
    let executor = Arc::new(fixture.executor(broker.clone(), request(REQUEST_B)));
    let task = tokio::spawn(async move {
        executor
            .call(&call(
                "cron__update",
                json!({
                    "job_id":job_id,
                    "name":"Confirmed replacement",
                    "prompt":"confirmed replacement task",
                    "timezone":"Europe/Moscow",
                    "schedule":{"kind":"cron","expression":"15 10 * * *"}
                }),
            ))
            .await
    });
    notified.await;
    fixture
        .store
        .set_job_desired_state(job_id, JobDesiredState::Disabled)
        .unwrap();
    response_tx.send(Ok(())).unwrap();

    let result = task.await.unwrap().unwrap();
    assert!(result.is_error);
    assert_eq!(result.error_code.as_deref(), Some("job_changed"));
    let current = fixture.store.get_job(job_id).unwrap();
    assert_eq!(current.name, "Morning report");
    assert_eq!(current.prompt, "original task");
    assert_eq!(current.desired_state, JobDesiredState::Disabled);
    assert_eq!(
        fixture.backend.installed.lock().unwrap().len(),
        install_count
    );
}

async fn assert_confirmation_error_changes_no_state(error: ConfirmationError) {
    let fixture = Fixture::new(FakeBackend::success());
    let broker = ImmediateBroker::rejecting(error);
    let executor = fixture.executor(broker, request(REQUEST_B));
    let result = executor
        .call(&call("cron__create", create_arguments("private prompt")))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(result.error_code.as_deref(), Some("confirmation_rejected"));
    assert!(fixture.store.list_jobs().unwrap().is_empty());
    assert!(fixture.backend.installed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn altered_action_hash_fails_closed() {
    struct HashBroker(Mutex<Option<[u8; 32]>>);
    impl ConfirmationBroker for HashBroker {
        fn confirm<'a>(&'a self, request: ConfirmationRequest) -> ConfirmationFuture<'a> {
            let mut expected = self.0.lock().unwrap();
            let result = match *expected {
                None => {
                    *expected = Some(request.action_hash());
                    Err(ConfirmationError::Rejected)
                }
                Some(hash) if hash == request.action_hash() => Ok(()),
                Some(_) => Err(ConfirmationError::ActionChanged),
            };
            Box::pin(async move { result })
        }
    }

    let fixture = Fixture::new(FakeBackend::success());
    let broker = Arc::new(HashBroker(Mutex::new(None)));
    let executor = fixture.executor(broker, request(REQUEST_A));
    let first = executor
        .call(&call("cron__create", create_arguments("first prompt")))
        .await
        .unwrap();
    assert!(first.is_error);
    let altered = executor
        .call(&call("cron__create", create_arguments("altered prompt")))
        .await
        .unwrap();
    assert!(altered.is_error);
    assert!(fixture.store.list_jobs().unwrap().is_empty());
    assert!(fixture.backend.installed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn strict_bounded_arguments_reject_shell_fields_before_confirmation() {
    let fixture = Fixture::new(FakeBackend::success());
    let broker = ImmediateBroker::accepting();
    let executor = fixture.executor(broker.clone(), request(REQUEST_A));
    for arguments in [
        json!({
            "name":"job", "prompt":"task", "timezone":"Europe/Moscow",
            "schedule":{"kind":"cron","expression":"0 9 * * *"},
            "command":"rm -rf /"
        }),
        json!({
            "name":"job", "prompt":"task", "timezone":"Europe/Moscow",
            "schedule":{"kind":"cron","expression":"0 9 * * *"},
            "source_dialog_id":fixture.source_dialog_id
        }),
        json!({
            "name":"job", "prompt":"task", "timezone":"Europe/Moscow",
            "schedule":{"kind":"cron","expression":"0 9 * * *", "shell":"id"}
        }),
        json!({
            "name":"job", "prompt":"x".repeat(262_145), "timezone":"Europe/Moscow",
            "schedule":{"kind":"cron","expression":"0 9 * * *"}
        }),
    ] {
        assert_eq!(
            executor
                .call(&call("cron__create", arguments))
                .await
                .unwrap_err(),
            deepseek_cli::tools::ToolExecutionError::InvalidArguments
        );
    }
    assert!(broker.requests.lock().unwrap().is_empty());
    assert!(fixture.store.list_jobs().unwrap().is_empty());
}

#[tokio::test]
async fn successful_mutations_return_installed_state() {
    let fixture = Fixture::new(FakeBackend::success());
    let executor = fixture.executor(ImmediateBroker::accepting(), request(REQUEST_A));
    let created = executor
        .call(&call("cron__create", create_arguments("task")))
        .await
        .unwrap();
    assert!(!created.is_error);
    assert_eq!(parse_result(&created)["status"], "installed");
    let job_id = parse_result(&created)["job"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let disabled = executor
        .call(&call("cron__disable", json!({"job_id":job_id})))
        .await
        .unwrap();
    assert_eq!(parse_result(&disabled)["job"]["desired_state"], "disabled");
    let enabled = executor
        .call(&call("cron__enable", json!({"job_id":job_id})))
        .await
        .unwrap();
    assert_eq!(parse_result(&enabled)["job"]["desired_state"], "active");
    let updated = executor
        .call(&call(
            "cron__update",
            json!({
                "job_id":job_id,
                "name":"Updated",
                "prompt":"updated task",
                "timezone":"Europe/Moscow",
                "schedule":{"kind":"once_at","at":"2026-09-27T11:45"}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(parse_result(&updated)["job"]["name"], "Updated");
    let deleted = executor
        .call(&call("cron__delete", json!({"job_id":job_id})))
        .await
        .unwrap();
    assert_eq!(parse_result(&deleted)["job"]["desired_state"], "deleted");
}

#[tokio::test]
async fn backend_failure_returns_saved_not_installed_and_keeps_failed_job_visible() {
    let fixture = Fixture::new(FakeBackend::failing());
    let executor = fixture.executor(ImmediateBroker::accepting(), request(REQUEST_A));
    let result = executor
        .call(&call("cron__create", create_arguments("task")))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(result.error_code.as_deref(), Some("saved_not_installed"));
    assert_eq!(parse_result(&result)["status"], "saved_not_installed");
    let jobs = fixture.store.list_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].sync_state, JobSyncState::Failed);
    assert_eq!(jobs[0].desired_state, JobDesiredState::Active);
}

#[test]
fn native_tools_compose_without_parsing_or_weakening_other_routes() {
    struct OpaqueExecutor {
        definitions: Vec<deepseek_cli::provider::ModelToolDefinition>,
    }
    impl ToolExecutor for OpaqueExecutor {
        fn definitions(&self) -> &[deepseek_cli::provider::ModelToolDefinition] {
            &self.definitions
        }
        fn route(&self, name: &str) -> Option<deepseek_cli::tools::ToolRoute<'_>> {
            (name == "opaque-alias").then_some(deepseek_cli::tools::ToolRoute {
                server_name: "server/opaque",
                tool_name: "original::tool",
            })
        }
        fn is_read_only(&self, name: &str) -> Option<bool> {
            (name == "opaque-alias").then_some(true)
        }
        fn call<'a>(
            &'a self,
            _call: &'a ModelToolCall,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            deepseek_cli::tools::ToolExecutionResult,
                            deepseek_cli::tools::ToolExecutionError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async { unreachable!() })
        }
    }
    let fixture = Fixture::new(FakeBackend::success());
    let native = fixture.executor(ImmediateBroker::accepting(), request(REQUEST_A));
    let opaque = OpaqueExecutor {
        definitions: vec![deepseek_cli::provider::ModelToolDefinition {
            name: "opaque-alias".into(),
            description: None,
            parameters: json!({"type":"object"}).as_object().unwrap().clone(),
            read_only: true,
        }],
    };
    let composite = CompositeToolExecutor::new(vec![Arc::new(opaque), Arc::new(native)]).unwrap();
    assert_eq!(
        composite.route("opaque-alias").unwrap(),
        deepseek_cli::tools::ToolRoute {
            server_name: "server/opaque",
            tool_name: "original::tool"
        }
    );
    assert_eq!(
        composite.route("cron__create").unwrap(),
        deepseek_cli::tools::ToolRoute {
            server_name: "cron",
            tool_name: "create"
        }
    );
}
