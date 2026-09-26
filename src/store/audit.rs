use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use super::{SafeErrorCode, Store, StoreError, execute_one, now};
use crate::domain::{RunId, ToolOwner, ToolRunStatus, TurnId};

/// Only route identifiers and classification enter this API, never arguments,
/// hashes, result bodies, URLs, or error descriptions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolRunStart {
    pub owner: ToolOwner,
    pub call_id: String,
    pub server_name: String,
    pub tool_name: String,
    pub read_only: bool,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ToolRun {
    pub id: i64,
    pub owner: ToolOwner,
    pub call_id: String,
    pub server_name: String,
    pub tool_name: String,
    pub read_only: bool,
    pub status: ToolRunStatus,
    pub safe_error_code: Option<SafeErrorCode>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

impl Store {
    pub fn start_tool_run(&self, start: ToolRunStart) -> Result<i64, StoreError> {
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
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (owner_kind, owner_id) = match start.owner {
            ToolOwner::InteractiveTurn(id) => {
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM turns WHERE id=?)",
                    [id.get()],
                    |r| r.get(0),
                )?;
                if !exists {
                    return Err(StoreError::InvalidOwner);
                }
                ("interactive_turn", id.get())
            }
            ToolOwner::CronRun(id) => {
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM cron_runs WHERE id=?)",
                    [id.get()],
                    |r| r.get(0),
                )?;
                if !exists {
                    return Err(StoreError::InvalidOwner);
                }
                ("cron_run", id.get())
            }
        };
        execute_one(
            &tx,
            "INSERT INTO tool_runs(owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,started_at) VALUES(?,?,?,?,?,?,'pending',?)",
            params![
                owner_kind,
                owner_id,
                start.call_id,
                start.server_name,
                start.tool_name,
                start.read_only,
                now()
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    pub fn finish_tool_run(&self, id: i64, finish: ToolRunFinish) -> Result<(), StoreError> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (old_status, old_code) = tx
            .query_row(
                "SELECT status,safe_error_code FROM tool_runs WHERE id=?",
                [id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
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
        execute_one(
            &tx,
            "UPDATE tool_runs SET status=?,safe_error_code=?,finished_at=? WHERE id=?",
            params![status, code, now(), id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Only recover abandoned work after excluding live sessions/processes.
    /// A mutating call may already have taken effect remotely: never retry it
    /// automatically or classify it as definitely failed.
    pub fn recover_pending_tool_runs(&self) -> Result<usize, StoreError> {
        let db = self.connection()?;
        Ok(db.execute("UPDATE tool_runs SET status=CASE WHEN read_only=1 THEN 'failed' ELSE 'uncertain' END,safe_error_code='process_interrupted',finished_at=? WHERE status='pending'", [now()])?)
    }

    pub fn list_tool_runs(&self) -> Result<Vec<ToolRun>, StoreError> {
        let db = self.connection()?;
        let mut statement = db.prepare("SELECT id,owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,safe_error_code,started_at,finished_at FROM tool_runs ORDER BY id")?;
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
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
}
