use std::time::{Duration, Instant};

use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{SafeErrorCode, Store, StoreError, execute_one, now};
use crate::domain::{RunId, ToolOwner, ToolRunStatus, TurnId};

/// Tool arguments are stored for explicit audit inspection. Results and raw
/// error descriptions are never stored here.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolRunStart {
    pub owner: ToolOwner,
    pub call_id: String,
    pub server_name: String,
    pub tool_name: String,
    pub read_only: bool,
    pub arguments: String,
}

impl std::fmt::Debug for ToolRunStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolRunStart")
            .field("owner", &self.owner)
            .field("call_id", &self.call_id)
            .field("server_name", &self.server_name)
            .field("tool_name", &self.tool_name)
            .field("read_only", &self.read_only)
            .field("arguments", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolRunFinish {
    status: ToolRunStatus,
    safe_error_code: Option<SafeErrorCode>,
}

impl ToolRunFinish {
    pub fn completed() -> Self {
        Self {
            status: ToolRunStatus::Completed,
            safe_error_code: None,
        }
    }
    pub fn failed(code: SafeErrorCode) -> Self {
        Self {
            status: ToolRunStatus::Failed,
            safe_error_code: Some(code),
        }
    }
    pub fn uncertain(code: SafeErrorCode) -> Self {
        Self {
            status: ToolRunStatus::Uncertain,
            safe_error_code: Some(code),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ToolRun {
    pub id: i64,
    pub owner: ToolOwner,
    pub call_id: String,
    pub server_name: String,
    pub tool_name: String,
    pub read_only: bool,
    pub arguments: Option<String>,
    pub status: ToolRunStatus,
    pub safe_error_code: Option<SafeErrorCode>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

impl std::fmt::Debug for ToolRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolRun")
            .field("id", &self.id)
            .field("owner", &self.owner)
            .field("call_id", &self.call_id)
            .field("server_name", &self.server_name)
            .field("tool_name", &self.tool_name)
            .field("read_only", &self.read_only)
            .field("status", &self.status)
            .field("safe_error_code", &self.safe_error_code)
            .field("started_at", &self.started_at)
            .field("finished_at", &self.finished_at)
            .field("arguments", &"[redacted]")
            .finish()
    }
}

impl Store {
    pub fn start_tool_run(&self, start: ToolRunStart) -> Result<i64, StoreError> {
        self.start_tool_run_with_deadline(
            start,
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
    }

    pub(crate) fn start_tool_run_with_deadline(
        &self,
        start: ToolRunStart,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<i64, StoreError> {
        for name in [&start.call_id, &start.server_name, &start.tool_name] {
            if name.is_empty()
                || name.len() > 256
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
            {
                return Err(StoreError::InvalidMetadata);
            }
        }
        if start.arguments.len() > 524_288
            || !serde_json::from_str::<serde_json::Value>(&start.arguments)
                .is_ok_and(|value| value.is_object())
        {
            return Err(StoreError::InvalidMetadata);
        }
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            let (owner_kind, owner_id, runtime_owner_id) = match start.owner {
                ToolOwner::InteractiveTurn(id) => {
                    let (status, runtime_owner_id) = tx
                        .query_row(
                            "SELECT status,runtime_owner_id FROM turns WHERE id=?",
                            [id.get()],
                            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
                        )
                        .optional()?
                        .ok_or(StoreError::InvalidOwner)?;
                    if status != "pending" {
                        return Err(StoreError::InvalidOwner);
                    }
                    self.require_runtime_owner(tx, runtime_owner_id.as_deref())?;
                    ("interactive_turn", id.get(), runtime_owner_id)
                }
                ToolOwner::CronRun(id) => {
                    let (status, runtime_owner_id) = tx
                        .query_row(
                            "SELECT status,runtime_owner_id FROM cron_runs WHERE id=?",
                            [id.get()],
                            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
                        )
                        .optional()?
                        .ok_or(StoreError::InvalidOwner)?;
                    if status != "pending" {
                        return Err(StoreError::InvalidOwner);
                    }
                    self.require_runtime_owner(tx, runtime_owner_id.as_deref())?;
                    ("cron_run", id.get(), runtime_owner_id)
                }
            };
            execute_one(
                tx,
                "INSERT INTO tool_runs(owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,started_at,runtime_owner_id,arguments) VALUES(?,?,?,?,?,?,'pending',?,?,?)",
                params![
                    owner_kind,
                    owner_id,
                    start.call_id,
                    start.server_name,
                    start.tool_name,
                    start.read_only,
                    now(),
                    runtime_owner_id,
                    start.arguments,
                ],
            )?;
            Ok(tx.last_insert_rowid())
        })
    }

    pub fn finish_tool_run(&self, id: i64, finish: ToolRunFinish) -> Result<(), StoreError> {
        self.finish_tool_run_with_deadline(
            id,
            finish,
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
    }

    pub(crate) fn finish_tool_run_with_deadline(
        &self,
        id: i64,
        finish: ToolRunFinish,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), StoreError> {
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            let (old_status, old_code, runtime_owner_id) = tx
                .query_row(
                    "SELECT status,safe_error_code,runtime_owner_id FROM tool_runs WHERE id=?",
                    [id],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, Option<String>>(1)?,
                            r.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
                .optional()?
                .ok_or(StoreError::NotFound)?;
            let status = match finish.status {
                ToolRunStatus::Completed => "completed",
                ToolRunStatus::Failed => "failed",
                ToolRunStatus::Uncertain => "uncertain",
                ToolRunStatus::Pending => unreachable!(),
            };
            let code = finish.safe_error_code.map(SafeErrorCode::as_str);
            if old_status != "pending" {
                return if old_status == status && old_code.as_deref() == code {
                    Ok(())
                } else {
                    Err(StoreError::Conflict)
                };
            }
            self.require_runtime_owner(tx, runtime_owner_id.as_deref())?;
            execute_one(
                tx,
                "UPDATE tool_runs SET status=?,safe_error_code=?,finished_at=?,runtime_owner_id=NULL WHERE id=?",
                params![status, code, now(), id],
            )
        })
    }

    pub fn list_tool_runs(&self) -> Result<Vec<ToolRun>, StoreError> {
        let db = self.connection()?;
        let mut statement = db.prepare("SELECT id,owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,safe_error_code,started_at,finished_at,arguments FROM tool_runs ORDER BY id")?;
        Ok(statement
            .query_map([], |r| {
                let owner = match r.get::<_, String>(1)?.as_str() {
                    "interactive_turn" => ToolOwner::InteractiveTurn(
                        TurnId::new(r.get(2)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    ),
                    "cron_run" => ToolOwner::CronRun(
                        RunId::new(r.get(2)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    ),
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                let status = match r.get::<_, String>(7)?.as_str() {
                    "pending" => ToolRunStatus::Pending,
                    "completed" => ToolRunStatus::Completed,
                    "failed" => ToolRunStatus::Failed,
                    "uncertain" => ToolRunStatus::Uncertain,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                Ok(ToolRun {
                    id: r.get(0)?,
                    owner,
                    call_id: r.get(3)?,
                    server_name: r.get(4)?,
                    tool_name: r.get(5)?,
                    read_only: r.get(6)?,
                    status,
                    safe_error_code: r
                        .get::<_, Option<String>>(8)?
                        .as_deref()
                        .map(SafeErrorCode::parse)
                        .transpose()?,
                    started_at: r.get(9)?,
                    finished_at: r.get(10)?,
                    arguments: r.get(11)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
}
