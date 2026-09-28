use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::{America::New_York, Europe::Moscow};
use deepseek_cli::provider::{ModelToolCall, ModelToolDefinition};
use deepseek_cli::tools::time::{TimeClock, TimeToolExecutor};
use deepseek_cli::tools::{
    ToolExecutionError, ToolExecutor, ToolFuture, ToolRoute, cron_runtime_catalog,
};
use serde_json::{Value, json};

struct FixedClock(DateTime<Utc>);

struct McpStub {
    definitions: Vec<ModelToolDefinition>,
}

impl McpStub {
    fn new() -> Self {
        Self {
            definitions: vec![ModelToolDefinition {
                name: "telegram__read".into(),
                description: None,
                parameters: json!({"type":"object"}).as_object().unwrap().clone(),
                read_only: true,
            }],
        }
    }
}

impl ToolExecutor for McpStub {
    fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }
    fn route(&self, name: &str) -> Option<ToolRoute<'_>> {
        (name == "telegram__read").then_some(ToolRoute {
            server_name: "telegram",
            tool_name: "read",
        })
    }
    fn is_read_only(&self, name: &str) -> Option<bool> {
        (name == "telegram__read").then_some(true)
    }
    fn call<'a>(&'a self, _: &'a ModelToolCall) -> ToolFuture<'a> {
        Box::pin(async { Err(ToolExecutionError::UnknownTool) })
    }
}

impl TimeClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

fn call(arguments: &str) -> ModelToolCall {
    ModelToolCall {
        id: "clock-call".into(),
        name: "time__get_current_time".into(),
        arguments: arguments.into(),
    }
}

#[tokio::test]
async fn reports_utc_and_configured_local_time_across_day_boundary() {
    let instant = Utc.with_ymd_and_hms(2026, 9, 26, 22, 15, 30).unwrap();
    let executor = TimeToolExecutor::with_clock(Moscow, Arc::new(FixedClock(instant)));
    let result = executor.call(&call("{}")).await.unwrap();
    assert!(!result.is_error);
    assert_eq!(
        serde_json::from_str::<Value>(&result.content).unwrap(),
        json!({
            "utc": "2026-09-26T22:15:30Z",
            "local": "2026-09-27T01:15:30+03:00",
            "local_minute": "2026-09-27T01:15",
            "timezone": "Europe/Moscow"
        })
    );
}

#[tokio::test]
async fn local_offset_tracks_configured_timezone_dst() {
    let instant = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let executor = TimeToolExecutor::with_clock(New_York, Arc::new(FixedClock(instant)));
    let result = executor.call(&call("{}")).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&result.content).unwrap(),
        json!({
            "utc": "2026-07-01T12:00:00Z",
            "local": "2026-07-01T08:00:00-04:00",
            "local_minute": "2026-07-01T08:00",
            "timezone": "America/New_York"
        })
    );
}

#[tokio::test]
async fn rejects_any_arguments_except_an_empty_json_object() {
    let instant = Utc.with_ymd_and_hms(2026, 9, 26, 22, 15, 30).unwrap();
    let executor = TimeToolExecutor::with_clock(Moscow, Arc::new(FixedClock(instant)));
    for invalid in ["", "not json", "null", "[]", "42", "{\"extra\":true}"] {
        assert_eq!(
            executor.call(&call(invalid)).await,
            Err(ToolExecutionError::InvalidArguments),
            "arguments: {invalid:?}"
        );
    }
}

#[test]
fn definition_is_closed_read_only_and_routed_to_time() {
    let executor = TimeToolExecutor::new(Moscow);
    let definitions = executor.definitions();
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, "time__get_current_time");
    assert!(definitions[0].read_only);
    assert_eq!(definitions[0].parameters["type"], "object");
    assert_eq!(definitions[0].parameters["additionalProperties"], false);
    assert_eq!(definitions[0].parameters["properties"], json!({}));
    assert_eq!(definitions[0].parameters["required"], json!([]));
    let route = executor.route("time__get_current_time").unwrap();
    assert_eq!(
        (route.server_name, route.tool_name),
        ("time", "get_current_time")
    );
    assert_eq!(executor.is_read_only("time__get_current_time"), Some(true));
    assert!(executor.route("time__unknown").is_none());
}

#[tokio::test]
async fn cron_runtime_catalog_dispatches_time_without_scheduler_mutations() {
    let mcp: Arc<dyn ToolExecutor> = Arc::new(McpStub::new());
    let catalog = cron_runtime_catalog(mcp, Moscow).unwrap();
    let names: Vec<_> = catalog
        .definitions()
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    assert_eq!(names, ["telegram__read", "time__get_current_time"]);
    assert_eq!(catalog.is_read_only("time__get_current_time"), Some(true));
    assert!(catalog.route("cron__create").is_none());
    let result = catalog.call(&call("{}")).await.unwrap();
    let output: Value = serde_json::from_str(&result.content).unwrap();
    assert_eq!(output["timezone"], "Europe/Moscow");
    assert!(output["utc"].as_str().unwrap().ends_with('Z'));
}
