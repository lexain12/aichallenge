//! Durable tool metadata. Arguments are canonicalized and hashed in memory only;
//! result bodies, message contents, transcripts, and error details have no storage API.

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::dialog::DialogStore;

pub struct ToolExecutionStart<'a> {
    pub dialog_id: i64,
    pub input_message_id: i64,
    pub tool_call_id: &'a str,
    pub server_name: &'a str,
    /// Original server tool name, before any model-facing name encoding.
    pub tool_name: &'a str,
    pub arguments_json: &'a str,
}

/// A bounded machine code, never an error description or remote body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolExecutionErrorCode(String);

impl ToolExecutionErrorCode {
    pub fn new(code: &str) -> Result<Self, ToolAuditError> {
        if code.is_empty()
            || code.len() > 64
            || !code
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        {
            return Err(ToolAuditError::InvalidErrorCode);
        }
        Ok(Self(code.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolExecutionStatus {
    Started,
    Succeeded,
    Failed,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolExecutionFinalStatus {
    Succeeded,
    Failed,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolExecutionFinish {
    status: ToolExecutionFinalStatus,
    error_code: Option<ToolExecutionErrorCode>,
}

impl ToolExecutionFinish {
    pub fn succeeded() -> Self {
        Self {
            status: ToolExecutionFinalStatus::Succeeded,
            error_code: None,
        }
    }
    pub fn failed(code: ToolExecutionErrorCode) -> Self {
        Self {
            status: ToolExecutionFinalStatus::Failed,
            error_code: Some(code),
        }
    }
    pub fn uncertain(code: ToolExecutionErrorCode) -> Self {
        Self {
            status: ToolExecutionFinalStatus::Uncertain,
            error_code: Some(code),
        }
    }

    fn status_str(&self) -> &'static str {
        match self.status {
            ToolExecutionFinalStatus::Succeeded => "succeeded",
            ToolExecutionFinalStatus::Failed => "failed",
            ToolExecutionFinalStatus::Uncertain => "uncertain",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolExecution {
    pub id: i64,
    pub dialog_id: i64,
    pub input_message_id: i64,
    pub tool_call_id: String,
    pub server_name: String,
    pub tool_name: String,
    pub arguments_hash: String,
    pub status: ToolExecutionStatus,
    pub is_error: bool,
    pub error_code: Option<ToolExecutionErrorCode>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Error)]
pub enum ToolAuditError {
    #[error("tool arguments must be a valid JSON object")]
    InvalidArguments,
    #[error(
        "tool error code must contain 1..64 lowercase ASCII letters, digits, underscores or hyphens"
    )]
    InvalidErrorCode,
    #[error("tool input must be an existing user message in the same dialog")]
    InvalidInputMessage,
    #[error("tool execution was not found")]
    NotFound,
    #[error("tool execution is already finalized with different metadata")]
    AlreadyFinalized,
    #[error("invalid stored tool metadata")]
    InvalidMetadata,
    #[error("tool audit database error: {0}")]
    Database(#[from] rusqlite::Error),
}

pub(crate) fn migrate(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS messages_audit_ownership ON messages(dialog_id, id);
         CREATE TABLE IF NOT EXISTS tool_executions (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
             input_message_id INTEGER NOT NULL,
             tool_call_id TEXT NOT NULL,
             server_name TEXT NOT NULL,
             tool_name TEXT NOT NULL,
             arguments_hash TEXT NOT NULL CHECK(length(arguments_hash) = 64 AND arguments_hash NOT GLOB '*[^0-9a-f]*'),
             status TEXT NOT NULL CHECK(status IN ('started','succeeded','failed','uncertain')),
             is_error INTEGER NOT NULL DEFAULT 0 CHECK(is_error IN (0,1)),
             error_code TEXT CHECK(error_code IS NULL OR (length(error_code) BETWEEN 1 AND 64 AND error_code NOT GLOB '*[^a-z0-9_-]*')),
             started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
             finished_at TEXT,
             FOREIGN KEY(dialog_id, input_message_id) REFERENCES messages(dialog_id, id),
             UNIQUE(dialog_id, input_message_id, tool_call_id),
             CHECK((status = 'started' AND finished_at IS NULL AND is_error = 0 AND error_code IS NULL)
                OR (status = 'succeeded' AND finished_at IS NOT NULL AND is_error = 0 AND error_code IS NULL)
                OR (status IN ('failed','uncertain') AND finished_at IS NOT NULL AND is_error = 1 AND error_code IS NOT NULL))
         );"
    )
}

fn arguments_hash(json: &str) -> Result<String, ToolAuditError> {
    let mut value: Value =
        serde_json::from_str(json).map_err(|_| ToolAuditError::InvalidArguments)?;
    if !value.is_object() {
        return Err(ToolAuditError::InvalidArguments);
    }
    value.sort_all_objects();
    let canonical = serde_json::to_vec(&value).map_err(|_| ToolAuditError::InvalidArguments)?;
    Ok(format!("{:x}", Sha256::digest(canonical)))
}

impl DialogStore {
    pub fn start_tool_execution(
        &mut self,
        start: ToolExecutionStart<'_>,
    ) -> Result<i64, ToolAuditError> {
        let hash = arguments_hash(start.arguments_json)?;
        let inserted = self.connection.execute(
            "INSERT INTO tool_executions (dialog_id,input_message_id,tool_call_id,server_name,tool_name,arguments_hash,status)
             SELECT ?1,?2,?3,?4,?5,?6,'started'
             WHERE EXISTS(SELECT 1 FROM messages WHERE dialog_id=?1 AND id=?2 AND role='user')",
            params![start.dialog_id,start.input_message_id,start.tool_call_id,start.server_name,start.tool_name,hash],
        )?;
        if inserted != 1 {
            return Err(ToolAuditError::InvalidInputMessage);
        }
        Ok(self.connection.last_insert_rowid())
    }

    /// Only started rows can transition. An identical retry succeeds without
    /// rewriting the original finish timestamp; a conflicting retry fails.
    pub fn finish_tool_execution(
        &mut self,
        id: i64,
        finish: ToolExecutionFinish,
    ) -> Result<(), ToolAuditError> {
        let is_error = finish.status != ToolExecutionFinalStatus::Succeeded;
        let updated = self.connection.execute(
            "UPDATE tool_executions SET status=?2,is_error=?3,error_code=?4,finished_at=strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id=?1 AND status='started'",
            params![id,finish.status_str(),is_error,finish.error_code.as_ref().map(ToolExecutionErrorCode::as_str)],
        )?;
        if updated == 1 {
            return Ok(());
        }
        let row = self.tool_execution(id)?.ok_or(ToolAuditError::NotFound)?;
        let expected = match finish.status {
            ToolExecutionFinalStatus::Succeeded => ToolExecutionStatus::Succeeded,
            ToolExecutionFinalStatus::Failed => ToolExecutionStatus::Failed,
            ToolExecutionFinalStatus::Uncertain => ToolExecutionStatus::Uncertain,
        };
        if row.status == expected && row.is_error == is_error && row.error_code == finish.error_code
        {
            Ok(())
        } else {
            Err(ToolAuditError::AlreadyFinalized)
        }
    }

    pub fn tool_execution(&self, id: i64) -> Result<Option<ToolExecution>, ToolAuditError> {
        self.connection.query_row(
            "SELECT id,dialog_id,input_message_id,tool_call_id,server_name,tool_name,arguments_hash,status,is_error,error_code,started_at,finished_at FROM tool_executions WHERE id=?1",[id],decode_row,
        ).optional()?.transpose()
    }

    pub fn tool_executions(&self, dialog_id: i64) -> Result<Vec<ToolExecution>, ToolAuditError> {
        let mut statement = self.connection.prepare("SELECT id,dialog_id,input_message_id,tool_call_id,server_name,tool_name,arguments_hash,status,is_error,error_code,started_at,finished_at FROM tool_executions WHERE dialog_id=?1 ORDER BY id")?;
        statement
            .query_map([dialog_id], decode_row)?
            .map(|row| row?)
            .collect()
    }
}

fn decode_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<ToolExecution, ToolAuditError>> {
    let status: String = row.get(7)?;
    let status = match status.as_str() {
        "started" => ToolExecutionStatus::Started,
        "succeeded" => ToolExecutionStatus::Succeeded,
        "failed" => ToolExecutionStatus::Failed,
        "uncertain" => ToolExecutionStatus::Uncertain,
        _ => return Ok(Err(ToolAuditError::InvalidMetadata)),
    };
    let code: Option<String> = row.get(9)?;
    let error_code = match code.as_deref().map(ToolExecutionErrorCode::new).transpose() {
        Ok(code) => code,
        Err(error) => return Ok(Err(error)),
    };
    Ok(Ok(ToolExecution {
        id: row.get(0)?,
        dialog_id: row.get(1)?,
        input_message_id: row.get(2)?,
        tool_call_id: row.get(3)?,
        server_name: row.get(4)?,
        tool_name: row.get(5)?,
        arguments_hash: row.get(6)?,
        status,
        is_error: row.get(8)?,
        error_code,
        started_at: row.get(10)?,
        finished_at: row.get(11)?,
    }))
}
