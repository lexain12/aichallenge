use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::chat::{Message, Role};
use crate::client::TokenUsage;
use crate::config::{ContextStrategy, DebugConfig};
use crate::system_context::SystemBlockMetadata;

#[derive(Clone, Debug, serde::Serialize)]
pub struct RequestMetadata {
    strategy: ContextStrategy,
    system_blocks: Vec<SystemBlockMetadata>,
    selected_message_count: usize,
    summary_boundary: usize,
    facts_boundary: usize,
}

impl RequestMetadata {
    pub fn new(
        strategy: ContextStrategy,
        system_blocks: Vec<SystemBlockMetadata>,
        selected_message_count: usize,
        summary_boundary: usize,
        facts_boundary: usize,
    ) -> Self {
        Self {
            strategy,
            system_blocks,
            selected_message_count,
            summary_boundary,
            facts_boundary,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct WorkflowDebugMetadata {
    pub source: String,
    pub component: String,
    pub model: String,
    pub mode: String,
    pub input_version: u64,
    pub output_version: Option<u64>,
    pub proposed_event: Option<String>,
    pub accepted: bool,
    pub outcome: String,
    pub autonomous_turn: u32,
    pub autonomous_tokens: u64,
    pub stage_run_id: i64,
    pub transition_id: Option<i64>,
    pub processing_id: Option<i64>,
    pub processing_status: String,
    pub error_kind: Option<String>,
    pub http_status: Option<u16>,
    pub usage: Option<TokenUsage>,
    pub input_chars: usize,
    pub output_chars: usize,
    pub stage_message_count: usize,
    pub plan_step_count: usize,
    pub checkpoint_item_count: usize,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct WorkflowDebugPayload {
    pub interpreter_output: Option<String>,
    pub checker_output: Option<String>,
    pub plan: Option<String>,
    pub checkpoint: Option<String>,
    pub controller_instruction: Option<String>,
    pub handoff: Option<String>,
    pub model_prompt: Option<String>,
    pub model_output: Option<String>,
    pub provider_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct WorkflowDebugEvent {
    pub metadata: WorkflowDebugMetadata,
    pub payload: Option<WorkflowDebugPayload>,
}

pub struct DebugLog {
    writer: Option<BufWriter<Box<dyn Write + Send>>>,
    log_payloads: bool,
    api_key: String,
    pending_warning: Option<String>,
}

impl DebugLog {
    pub fn is_active(&self) -> bool {
        self.writer.is_some() || self.pending_warning.is_some()
    }

    pub fn payloads_enabled(&self) -> bool {
        self.writer.is_some() && self.log_payloads
    }

    pub fn new(path: Option<PathBuf>, log_payloads: bool, api_key: &str) -> Self {
        let (writer, pending_warning) = match path {
            None => (None, None),
            Some(path) => match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => (
                    Some(BufWriter::new(Box::new(file) as Box<dyn Write + Send>)),
                    None,
                ),
                Err(error) => (
                    None,
                    Some(format!(
                        "debug log disabled: failed to open {}: {error}",
                        path.display()
                    )),
                ),
            },
        };
        Self {
            writer,
            log_payloads,
            api_key: api_key.to_owned(),
            pending_warning,
        }
    }

    pub fn from_config(config: &DebugConfig, api_key: &str) -> Self {
        Self::new(
            config.log_path().map(PathBuf::from),
            config.log_payloads(),
            api_key,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_writer_for_test(
        writer: impl Write + Send + 'static,
        log_payloads: bool,
        api_key: &str,
    ) -> Self {
        Self {
            writer: Some(BufWriter::new(Box::new(writer))),
            log_payloads,
            api_key: api_key.to_owned(),
            pending_warning: None,
        }
    }

    pub fn log_request(
        &mut self,
        kind: &'static str,
        messages: &[Message],
        context: &RequestMetadata,
    ) -> Option<String> {
        if self.writer.is_none() {
            return self.pending_warning.take();
        }
        let metadata: Vec<_> = messages
            .iter()
            .map(|message| {
                json!({
                    "role": role_name(message.role()),
                    "content_chars": message.content().chars().count(),
                })
            })
            .collect();
        let names: Vec<_> = context
            .system_blocks
            .iter()
            .map(|block| block.name.as_str())
            .collect();
        let mut value = json!({
            "event": "request_prepared",
            "timestamp_unix_ms": timestamp_unix_ms(),
            "kind": kind,
            "strategy": context.strategy,
            "system_block_names": names,
            "system_blocks": context.system_blocks,
            "selected_message_count": context.selected_message_count,
            "summary_boundary": context.summary_boundary,
            "facts_boundary": context.facts_boundary,
            "message_count": messages.len(),
            "message_metadata": metadata,
        });
        if self.log_payloads {
            value["messages"] = json!(messages);
        }
        self.write_value(value)
    }

    pub fn log_event(&mut self, event: &'static str, details: Value) -> Option<String> {
        if self.writer.is_none() {
            return self.pending_warning.take();
        }
        self.write_value(json!({
            "event": event,
            "timestamp_unix_ms": timestamp_unix_ms(),
            "details": details,
        }))
    }

    pub fn log_workflow(
        &mut self,
        metadata: &WorkflowDebugMetadata,
        payload: Option<&WorkflowDebugPayload>,
    ) -> Option<String> {
        if self.writer.is_none() {
            return self.pending_warning.take();
        }
        let mut details = match serde_json::to_value(metadata) {
            Ok(value) => value,
            Err(error) => return self.disable(format!("serialization failed: {error}")),
        };
        if self.payloads_enabled()
            && let Some(payload) = payload
        {
            details["payload"] = match serde_json::to_value(payload) {
                Ok(value) => value,
                Err(error) => return self.disable(format!("serialization failed: {error}")),
            };
        }
        self.write_value(json!({
            "event": "workflow",
            "timestamp_unix_ms": timestamp_unix_ms(),
            "details": details,
        }))
    }

    pub fn log_failure(
        &mut self,
        event: &'static str,
        details: Value,
        provider_error: Option<&str>,
    ) -> Option<String> {
        if self.writer.is_none() {
            return self.pending_warning.take();
        }
        let mut value = json!({
            "event": event,
            "timestamp_unix_ms": timestamp_unix_ms(),
            "details": details,
        });
        if self.payloads_enabled()
            && let Some(provider_error) = provider_error
        {
            value["details"]["payload"] = json!({"provider_error": provider_error});
        }
        self.write_value(value)
    }

    fn write_value(&mut self, value: Value) -> Option<String> {
        if let Some(warning) = self.pending_warning.take() {
            return Some(warning);
        }
        let writer = self.writer.as_mut()?;
        let mut line = match serde_json::to_string(&value) {
            Ok(line) => line,
            Err(error) => return self.disable(format!("serialization failed: {error}")),
        };
        if !self.api_key.is_empty() {
            line = line.replace(&self.api_key, "[REDACTED]");
        }
        if let Err(error) = writeln!(writer, "{line}").and_then(|_| writer.flush()) {
            return self.disable(format!("write failed: {error}"));
        }
        None
    }

    fn disable(&mut self, reason: String) -> Option<String> {
        self.writer = None;
        Some(format!("debug log disabled: {reason}"))
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

fn timestamp_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
