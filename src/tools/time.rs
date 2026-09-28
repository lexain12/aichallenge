//! Read-only current time in the scheduler's configured timezone.

use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use chrono_tz::Tz;
use serde_json::{Value, json};

use crate::provider::{ModelToolCall, ModelToolDefinition};
use crate::tools::{ToolExecutionError, ToolExecutionResult, ToolExecutor, ToolFuture, ToolRoute};

const NAME: &str = "time__get_current_time";
const MAX_ARGUMENT_BYTES: usize = 524_288;

pub trait TimeClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

struct SystemClock;

impl TimeClock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct TimeToolExecutor {
    timezone: Tz,
    clock: Arc<dyn TimeClock>,
    definitions: Vec<ModelToolDefinition>,
}

impl TimeToolExecutor {
    pub fn new(timezone: Tz) -> Self {
        Self::with_clock(timezone, Arc::new(SystemClock))
    }

    pub fn with_clock(timezone: Tz, clock: Arc<dyn TimeClock>) -> Self {
        Self {
            timezone,
            clock,
            definitions: vec![ModelToolDefinition {
                name: NAME.into(),
                description: Some("Read the current UTC time and scheduler-local time".into()),
                parameters: json!({
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {},
                    "required": []
                })
                .as_object()
                .unwrap()
                .clone(),
                read_only: true,
            }],
        }
    }
}

impl ToolExecutor for TimeToolExecutor {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        (name == NAME).then_some(ToolRoute {
            server_name: "time",
            tool_name: "get_current_time",
        })
    }

    fn is_read_only(&self, name: &str) -> Option<bool> {
        (name == NAME).then_some(true)
    }

    fn call<'a>(&'a self, call: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async move {
            if call.name != NAME {
                return Err(ToolExecutionError::UnknownTool);
            }
            if call.arguments.len() > MAX_ARGUMENT_BYTES {
                return Err(ToolExecutionError::InvalidArguments);
            }
            let arguments: Value = serde_json::from_str(&call.arguments)
                .map_err(|_| ToolExecutionError::InvalidArguments)?;
            if !matches!(arguments, Value::Object(ref fields) if fields.is_empty()) {
                return Err(ToolExecutionError::InvalidArguments);
            }
            let utc = self.clock.now();
            let local = utc.with_timezone(&self.timezone);
            let content = json!({
                "utc": utc.to_rfc3339_opts(SecondsFormat::AutoSi, true),
                "local": local.to_rfc3339_opts(SecondsFormat::AutoSi, false),
                "local_minute": local.format("%Y-%m-%dT%H:%M").to_string(),
                "timezone": self.timezone.to_string(),
            });
            Ok(ToolExecutionResult {
                content: content.to_string(),
                is_error: false,
                error_code: None,
                delivery_uncertain: false,
            })
        })
    }
}
