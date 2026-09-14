use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::chat::{Message, Role};
use crate::config::DebugConfig;

pub struct DebugLog {
    writer: Option<BufWriter<File>>,
    log_payloads: bool,
    api_key: String,
    pending_warning: Option<String>,
}

impl DebugLog {
    pub fn new(path: Option<PathBuf>, log_payloads: bool, api_key: &str) -> Self {
        let (writer, pending_warning) = match path {
            None => (None, None),
            Some(path) => match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => (Some(BufWriter::new(file)), None),
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

    pub fn log_request(
        &mut self,
        kind: &'static str,
        messages: &[Message],
        summary_boundary: usize,
    ) -> Option<String> {
        let metadata: Vec<_> = messages
            .iter()
            .map(|message| {
                json!({
                    "role": role_name(message.role()),
                    "content_chars": message.content().chars().count(),
                })
            })
            .collect();
        let mut value = json!({
            "event": "request_prepared",
            "timestamp_unix_ms": timestamp_unix_ms(),
            "kind": kind,
            "summary_boundary": summary_boundary,
            "message_count": messages.len(),
            "message_metadata": metadata,
        });
        if self.log_payloads {
            value["messages"] = json!(messages);
        }
        self.write_value(value)
    }

    pub fn log_event(&mut self, event: &'static str, details: Value) -> Option<String> {
        self.write_value(json!({
            "event": event,
            "timestamp_unix_ms": timestamp_unix_ms(),
            "details": details,
        }))
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
