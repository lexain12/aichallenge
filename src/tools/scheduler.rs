//! Session-bound native scheduler tools with a fail-closed confirmation boundary.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use chrono_tz::{Europe::Moscow, Tz};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::domain::{ConfirmationId, DialogId, JobDesiredState, JobId, RequestId};
use crate::provider::{ModelToolCall, ModelToolDefinition};
use crate::scheduler::{CronSynchronizer, ScheduleSpec, SyncReport};
use crate::store::{CronJob, JobCreate, Store};
use crate::tools::{ToolExecutionError, ToolExecutionResult, ToolExecutor, ToolFuture, ToolRoute};

const CONFIRMATION_MINUTES: i64 = 5;
const MAX_NAME_BYTES: usize = 256;
const MAX_PROMPT_BYTES: usize = 262_144;
const MAX_ARGUMENT_BYTES: usize = 524_288;

/// Exact user-visible schedule state. The prompt is deliberately present for
/// confirmation, but custom Debug output keeps it out of incidental logs.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct SchedulePreview {
    pub action: ScheduleAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<JobId>,
    pub name: String,
    pub schedule: String,
    pub timezone: String,
    pub prompt: String,
}

impl fmt::Debug for SchedulePreview {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchedulePreview")
            .field("action", &self.action)
            .field("arguments", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleAction {
    Create,
    Update,
    Enable,
    Disable,
    Delete,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConfirmationRequest {
    pub id: ConfirmationId,
    pub request_id: RequestId,
    pub preview: SchedulePreview,
    pub expires_at: DateTime<Utc>,
    action_hash: [u8; 32],
}

impl ConfirmationRequest {
    /// Hash of the exact normalized action, including its request and source
    /// dialog bindings. Session brokers compare this value before resolving.
    pub fn action_hash(&self) -> [u8; 32] {
        self.action_hash
    }
}

impl fmt::Debug for ConfirmationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfirmationRequest")
            .field("id", &self.id)
            .field("request_id", &self.request_id)
            .field("preview", &self.preview)
            .field("expires_at", &self.expires_at)
            .field("action_hash", &"[redacted]")
            .finish()
    }
}

/// Safe confirmation outcomes. None carries user text or identifiers.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ConfirmationError {
    #[error("confirmation_rejected")]
    Rejected,
    #[error("confirmation_expired")]
    Expired,
    #[error("confirmation_already_used")]
    AlreadyUsed,
    #[error("confirmation_wrong_request")]
    WrongRequest,
    #[error("confirmation_wrong_session")]
    WrongSession,
    #[error("confirmation_action_changed")]
    ActionChanged,
    #[error("confirmation_unavailable")]
    Unavailable,
}

pub type ConfirmationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ConfirmationError>> + Send + 'a>>;

/// One broker instance belongs to one live SSH session. Implementations must
/// consume each ID once and compare request ID, expiry, and action hash.
pub trait ConfirmationBroker: Send + Sync {
    fn confirm<'a>(&'a self, request: ConfirmationRequest) -> ConfirmationFuture<'a>;
}

pub trait ConfirmationClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

struct SystemClock;

impl ConfirmationClock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum CanonicalAction {
    Create {
        source_dialog_id: DialogId,
        name: String,
        schedule: ScheduleSpec,
        prompt: String,
    },
    Update {
        source_dialog_id: DialogId,
        expected: CronJob,
        name: String,
        schedule: ScheduleSpec,
        prompt: String,
    },
    SetState {
        source_dialog_id: DialogId,
        expected: CronJob,
        state: JobDesiredState,
        preview: SchedulePreview,
    },
}

impl CanonicalAction {
    fn preview(&self) -> SchedulePreview {
        match self {
            Self::Create {
                name,
                schedule,
                prompt,
                ..
            } => preview_for(ScheduleAction::Create, None, name, schedule, prompt),
            Self::Update {
                expected,
                name,
                schedule,
                prompt,
                ..
            } => preview_for(
                ScheduleAction::Update,
                Some(expected.id),
                name,
                schedule,
                prompt,
            ),
            Self::SetState { preview, .. } => preview.clone(),
        }
    }
}

pub struct SchedulerToolExecutor {
    store: Store,
    synchronizer: Arc<CronSynchronizer>,
    broker: Arc<dyn ConfirmationBroker>,
    source_dialog_id: DialogId,
    request_id: RequestId,
    clock: Arc<dyn ConfirmationClock>,
    definitions: Vec<ModelToolDefinition>,
}

impl SchedulerToolExecutor {
    pub fn new(
        store: Store,
        synchronizer: Arc<CronSynchronizer>,
        broker: Arc<dyn ConfirmationBroker>,
        source_dialog_id: DialogId,
        request_id: RequestId,
    ) -> Self {
        Self::new_with_clock(
            store,
            synchronizer,
            broker,
            source_dialog_id,
            request_id,
            Arc::new(SystemClock),
        )
    }

    pub fn new_with_clock(
        store: Store,
        synchronizer: Arc<CronSynchronizer>,
        broker: Arc<dyn ConfirmationBroker>,
        source_dialog_id: DialogId,
        request_id: RequestId,
        clock: Arc<dyn ConfirmationClock>,
    ) -> Self {
        Self {
            store,
            synchronizer,
            broker,
            source_dialog_id,
            request_id,
            clock,
            definitions: definitions(),
        }
    }

    fn parse_create(&self, arguments: &str) -> Result<CanonicalAction, ToolExecutionError> {
        let arguments: CreateArguments = parse_arguments(arguments)?;
        let name = normalize_name(arguments.name)?;
        validate_prompt(&arguments.prompt)?;
        let schedule = arguments.schedule.normalize(arguments.timezone)?;
        Ok(CanonicalAction::Create {
            source_dialog_id: self.source_dialog_id,
            name,
            schedule,
            prompt: arguments.prompt,
        })
    }

    fn parse_update(&self, arguments: &str) -> Result<CanonicalAction, ToolExecutionError> {
        let arguments: UpdateArguments = parse_arguments(arguments)?;
        let expected = self.require_existing_job(arguments.job_id)?;
        if expected.desired_state == JobDesiredState::Deleted {
            return Err(ToolExecutionError::InvalidArguments);
        }
        let name = normalize_name(arguments.name)?;
        validate_prompt(&arguments.prompt)?;
        let schedule = arguments.schedule.normalize(arguments.timezone)?;
        Ok(CanonicalAction::Update {
            source_dialog_id: self.source_dialog_id,
            expected,
            name,
            schedule,
            prompt: arguments.prompt,
        })
    }

    fn state_action(
        &self,
        arguments: &str,
        action: ScheduleAction,
        state: JobDesiredState,
    ) -> Result<CanonicalAction, ToolExecutionError> {
        let arguments: JobArguments = parse_arguments(arguments)?;
        let job = self.require_existing_job(arguments.job_id)?;
        if job.desired_state == JobDesiredState::Deleted {
            return Err(ToolExecutionError::InvalidArguments);
        }
        let preview = preview_for(action, Some(job.id), &job.name, &job.schedule, &job.prompt);
        Ok(CanonicalAction::SetState {
            source_dialog_id: self.source_dialog_id,
            expected: job,
            state,
            preview,
        })
    }

    fn require_existing_job(&self, job_id: JobId) -> Result<CronJob, ToolExecutionError> {
        self.store
            .get_job(job_id)
            .map_err(|_| ToolExecutionError::InvalidArguments)
    }

    async fn confirm_and_apply(
        &self,
        action: CanonicalAction,
    ) -> Result<ToolExecutionResult, ToolExecutionError> {
        let preview = action.preview();
        let issued_at = self.clock.now();
        let expires_at = issued_at + Duration::minutes(CONFIRMATION_MINUTES);
        let request = ConfirmationRequest {
            id: ConfirmationId::new(),
            request_id: self.request_id,
            action_hash: action_hash(self.request_id, &action)?,
            preview,
            expires_at,
        };
        if self.broker.confirm(request).await.is_err() || self.clock.now() >= expires_at {
            return Ok(safe_error("confirmation_rejected"));
        }

        let (mutated, snapshot_bound) = match action {
            CanonicalAction::Create {
                source_dialog_id,
                name,
                schedule,
                prompt,
            } => (
                self.store.create_job(JobCreate {
                    source_dialog_id,
                    name,
                    schedule,
                    prompt,
                }),
                false,
            ),
            CanonicalAction::Update {
                expected,
                name,
                schedule,
                prompt,
                ..
            } => (
                self.store
                    .update_job_if_unchanged(&expected, name, schedule, prompt),
                true,
            ),
            CanonicalAction::SetState {
                expected, state, ..
            } => (
                self.store
                    .set_job_desired_state_if_unchanged(&expected, state),
                true,
            ),
        };
        let job = match mutated {
            Ok(job) => job,
            Err(crate::store::StoreError::Conflict | crate::store::StoreError::NotFound)
                if snapshot_bound =>
            {
                return Ok(safe_error("job_changed"));
            }
            Err(_) => return Ok(safe_error("store_error")),
        };
        match self.synchronizer.sync().await {
            Ok(SyncReport::Installed { .. }) => match self.store.get_job(job.id) {
                Ok(job) => Ok(success("installed", &job)),
                Err(_) => Ok(safe_error("store_error")),
            },
            Ok(SyncReport::SavedNotInstalled { .. }) => match self.store.get_job(job.id) {
                Ok(job) => Ok(sync_failure(&job)),
                Err(_) => Ok(safe_error("store_error")),
            },
            Err(_) => {
                if self.store.mark_job_sync_failed(job.id).is_err() {
                    return Ok(safe_error("store_error"));
                }
                match self.store.get_job(job.id) {
                    Ok(job) => Ok(sync_failure(&job)),
                    Err(_) => Ok(safe_error("store_error")),
                }
            }
        }
    }

    fn list(&self, arguments: &str) -> Result<ToolExecutionResult, ToolExecutionError> {
        let _: EmptyArguments = parse_arguments(arguments)?;
        match self.store.list_jobs() {
            Ok(jobs) => Ok(ToolExecutionResult {
                content: json!({"jobs": jobs}).to_string(),
                is_error: false,
                error_code: None,
                delivery_uncertain: false,
            }),
            Err(_) => Ok(safe_error("store_error")),
        }
    }

    fn get(&self, arguments: &str) -> Result<ToolExecutionResult, ToolExecutionError> {
        let arguments: JobArguments = parse_arguments(arguments)?;
        match self.store.get_job(arguments.job_id) {
            Ok(job) => Ok(success("read", &job)),
            Err(_) => Ok(safe_error("job_not_found")),
        }
    }
}

impl ToolExecutor for SchedulerToolExecutor {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        native_name(name).map(|tool_name| ToolRoute {
            server_name: "cron",
            tool_name,
        })
    }

    fn is_read_only(&self, name: &str) -> Option<bool> {
        native_name(name).map(|tool_name| matches!(tool_name, "list" | "get"))
    }

    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async move {
            match call.name.as_str() {
                "cron__list" => self.list(&call.arguments),
                "cron__get" => self.get(&call.arguments),
                "cron__create" => {
                    let action = self.parse_create(&call.arguments)?;
                    self.confirm_and_apply(action).await
                }
                "cron__update" => {
                    let action = self.parse_update(&call.arguments)?;
                    self.confirm_and_apply(action).await
                }
                "cron__enable" => {
                    let action = self.state_action(
                        &call.arguments,
                        ScheduleAction::Enable,
                        JobDesiredState::Active,
                    )?;
                    self.confirm_and_apply(action).await
                }
                "cron__disable" => {
                    let action = self.state_action(
                        &call.arguments,
                        ScheduleAction::Disable,
                        JobDesiredState::Disabled,
                    )?;
                    self.confirm_and_apply(action).await
                }
                "cron__delete" => {
                    let action = self.state_action(
                        &call.arguments,
                        ScheduleAction::Delete,
                        JobDesiredState::Deleted,
                    )?;
                    self.confirm_and_apply(action).await
                }
                _ => Err(ToolExecutionError::UnknownTool),
            }
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyArguments {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobArguments {
    job_id: JobId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateArguments {
    name: String,
    schedule: InputSchedule,
    #[serde(default)]
    timezone: Option<String>,
    prompt: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateArguments {
    job_id: JobId,
    name: String,
    schedule: InputSchedule,
    #[serde(default)]
    timezone: Option<String>,
    prompt: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum InputSchedule {
    Cron { expression: String },
    OnceAt { at: String },
}

impl InputSchedule {
    fn normalize(self, timezone: Option<String>) -> Result<ScheduleSpec, ToolExecutionError> {
        let timezone = match timezone {
            Some(timezone)
                if !timezone.is_empty()
                    && timezone.len() <= 64
                    && timezone.is_ascii()
                    && !timezone.contains(['\0', '\r', '\n']) =>
            {
                timezone
                    .parse::<Tz>()
                    .map_err(|_| ToolExecutionError::InvalidArguments)?
            }
            Some(_) => return Err(ToolExecutionError::InvalidArguments),
            None => Moscow,
        };
        match self {
            Self::Cron { expression } => ScheduleSpec::parse_cron(&expression, timezone),
            Self::OnceAt { at } => ScheduleSpec::parse_once_at(&at, timezone),
        }
        .map_err(|_| ToolExecutionError::InvalidArguments)
    }
}

fn parse_arguments<T: for<'de> Deserialize<'de>>(arguments: &str) -> Result<T, ToolExecutionError> {
    if arguments.len() > MAX_ARGUMENT_BYTES {
        return Err(ToolExecutionError::InvalidArguments);
    }
    serde_json::from_str(arguments).map_err(|_| ToolExecutionError::InvalidArguments)
}

fn normalize_name(name: String) -> Result<String, ToolExecutionError> {
    let name = name.trim().to_owned();
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.chars().any(char::is_control) {
        return Err(ToolExecutionError::InvalidArguments);
    }
    Ok(name)
}

fn validate_prompt(prompt: &str) -> Result<(), ToolExecutionError> {
    if prompt.is_empty() || prompt.len() > MAX_PROMPT_BYTES || prompt.contains('\0') {
        return Err(ToolExecutionError::InvalidArguments);
    }
    Ok(())
}

fn preview_for(
    action: ScheduleAction,
    job_id: Option<JobId>,
    name: &str,
    schedule: &ScheduleSpec,
    prompt: &str,
) -> SchedulePreview {
    let schedule_text = match schedule {
        ScheduleSpec::Cron { expression, .. } => expression.clone(),
        ScheduleSpec::OnceAt { at, timezone } => at
            .with_timezone(timezone)
            .format("%Y-%m-%dT%H:%M")
            .to_string(),
    };
    SchedulePreview {
        action,
        job_id,
        name: name.to_owned(),
        schedule: schedule_text,
        timezone: schedule.timezone().to_string(),
        prompt: prompt.to_owned(),
    }
}

fn action_hash(
    request_id: RequestId,
    action: &CanonicalAction,
) -> Result<[u8; 32], ToolExecutionError> {
    let mut hasher = Sha256::new();
    hasher.update(request_id.to_string().as_bytes());
    hasher.update([0]);
    let bytes = serde_json::to_vec(action).map_err(|_| ToolExecutionError::InvalidArguments)?;
    hasher.update(bytes);
    Ok(hasher.finalize().into())
}

fn success(status: &str, job: &CronJob) -> ToolExecutionResult {
    ToolExecutionResult {
        content: json!({"status": status, "job": job}).to_string(),
        is_error: false,
        error_code: None,
        delivery_uncertain: false,
    }
}

fn sync_failure(job: &CronJob) -> ToolExecutionResult {
    ToolExecutionResult {
        content: json!({"status":"saved_not_installed", "job":job}).to_string(),
        is_error: true,
        error_code: Some("saved_not_installed".into()),
        delivery_uncertain: false,
    }
}

fn safe_error(code: &'static str) -> ToolExecutionResult {
    ToolExecutionResult {
        content: json!({"error":code}).to_string(),
        is_error: true,
        error_code: Some(code.into()),
        delivery_uncertain: false,
    }
}

fn native_name(name: &str) -> Option<&'static str> {
    match name {
        "cron__create" => Some("create"),
        "cron__list" => Some("list"),
        "cron__get" => Some("get"),
        "cron__update" => Some("update"),
        "cron__enable" => Some("enable"),
        "cron__disable" => Some("disable"),
        "cron__delete" => Some("delete"),
        _ => None,
    }
}

fn definitions() -> Vec<ModelToolDefinition> {
    let schedule = json!({
        "oneOf": [
            {
                "type":"object",
                "additionalProperties":false,
                "properties": {
                    "kind":{"const":"cron"},
                    "expression":{"type":"string","maxLength":128}
                },
                "required":["kind","expression"]
            },
            {
                "type":"object",
                "additionalProperties":false,
                "properties": {
                    "kind":{"const":"once_at"},
                    "at":{"type":"string","maxLength":16}
                },
                "required":["kind","at"]
            }
        ]
    });
    let create = closed_schema(
        json!({
            "name":{"type":"string","maxLength":MAX_NAME_BYTES},
            "schedule":schedule,
            "timezone":{"type":"string","maxLength":64,"default":"Europe/Moscow"},
            "prompt":{"type":"string","maxLength":MAX_PROMPT_BYTES}
        }),
        json!(["name", "schedule", "prompt"]),
    );
    let mut update_properties = create["properties"].clone();
    update_properties
        .as_object_mut()
        .unwrap()
        .insert("job_id".into(), job_id_schema());
    let update = closed_schema(
        update_properties,
        json!(["job_id", "name", "schedule", "prompt"]),
    );
    let by_id = closed_schema(json!({"job_id":job_id_schema()}), json!(["job_id"]));
    vec![
        definition(
            "cron__create",
            "Create a scheduled agent task",
            create,
            false,
        ),
        definition(
            "cron__list",
            "List scheduled agent tasks",
            closed_schema(json!({}), json!([])),
            true,
        ),
        definition(
            "cron__get",
            "Read one scheduled agent task",
            by_id.clone(),
            true,
        ),
        definition(
            "cron__update",
            "Replace a scheduled agent task",
            update,
            false,
        ),
        definition(
            "cron__enable",
            "Enable a scheduled agent task",
            by_id.clone(),
            false,
        ),
        definition(
            "cron__disable",
            "Disable a scheduled agent task",
            by_id.clone(),
            false,
        ),
        definition(
            "cron__delete",
            "Delete a scheduled agent task",
            by_id,
            false,
        ),
    ]
}

fn job_id_schema() -> Value {
    json!({"type":"string","format":"uuid"})
}

fn closed_schema(properties: Value, required: Value) -> Map<String, Value> {
    json!({
        "type":"object",
        "additionalProperties":false,
        "properties":properties,
        "required":required
    })
    .as_object()
    .unwrap()
    .clone()
}

fn definition(
    name: &str,
    description: &str,
    parameters: Map<String, Value>,
    read_only: bool,
) -> ModelToolDefinition {
    ModelToolDefinition {
        name: name.into(),
        description: Some(description.into()),
        parameters,
        read_only,
    }
}
