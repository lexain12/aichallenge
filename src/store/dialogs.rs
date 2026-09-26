use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use super::{SafeErrorCode, Store, StoreError, dialog_exists, execute_one, now};
use crate::domain::{DialogId, TurnId, TurnStatus};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Dialog {
    pub id: DialogId,
    pub title: String,
    pub created_at: String,
    pub updated_at: String,
}

pub type DialogSummary = Dialog;
const MAX_DIALOG_PAGE_SIZE: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredMessage {
    pub id: i64,
    pub dialog_id: DialogId,
    pub turn_id: TurnId,
    pub role: MessageRole,
    pub content: String,
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnStart {
    pub turn_id: TurnId,
    pub user_message_id: i64,
}

impl Store {
    pub fn create_dialog(&self, title: &str) -> Result<Dialog, StoreError> {
        let db = self.connection()?;
        let timestamp = now();
        execute_one(
            &db,
            "INSERT INTO dialogs(title,created_at,updated_at) VALUES(?1,?2,?2)",
            params![title, timestamp],
        )?;
        Ok(Dialog {
            id: DialogId::new(db.last_insert_rowid()).map_err(|_| StoreError::Database)?,
            title: title.into(),
            created_at: timestamp.clone(),
            updated_at: timestamp,
        })
    }

    pub fn list_dialogs(&self) -> Result<Vec<DialogSummary>, StoreError> {
        let db = self.connection()?;
        let mut statement =
            db.prepare("SELECT id,title,created_at,updated_at FROM dialogs ORDER BY id")?;
        Ok(statement
            .query_map([], |r| {
                Ok(Dialog {
                    id: DialogId::new(r.get(0)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    title: r.get(1)?,
                    created_at: r.get(2)?,
                    updated_at: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_dialogs_page(
        &self,
        after: Option<DialogId>,
        limit: usize,
    ) -> Result<Vec<DialogSummary>, StoreError> {
        if limit == 0 || limit > MAX_DIALOG_PAGE_SIZE {
            return Err(StoreError::InvalidMetadata);
        }
        let limit = i64::try_from(limit).map_err(|_| StoreError::InvalidMetadata)?;
        let after = after.map(DialogId::get).unwrap_or(0);
        let db = self.connection()?;
        let mut statement = db.prepare(
            "SELECT id,title,created_at,updated_at FROM dialogs
             WHERE id > ?1 ORDER BY id LIMIT ?2",
        )?;
        Ok(statement
            .query_map(params![after, limit], |row| {
                Ok(Dialog {
                    id: DialogId::new(row.get(0)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    title: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get_dialog(&self, id: DialogId) -> Result<DialogSummary, StoreError> {
        let db = self.connection()?;
        db.query_row(
            "SELECT id,title,created_at,updated_at FROM dialogs WHERE id=?1",
            [id.get()],
            |row| {
                Ok(Dialog {
                    id: DialogId::new(row.get(0)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    title: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            },
        )
        .optional()?
        .ok_or(StoreError::NotFound)
    }

    pub fn rename_dialog(&self, id: DialogId, title: &str) -> Result<(), StoreError> {
        let db = self.connection()?;
        if db.execute(
            "UPDATE dialogs SET title=?2,updated_at=?3 WHERE id=?1",
            params![id.get(), title, now()],
        )? == 0
        {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }

    pub fn delete_dialog(&self, id: DialogId) -> Result<(), StoreError> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        dialog_exists(&tx, id)?;
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM turns WHERE dialog_id=? AND status='pending')",
            [id.get()],
            |r| r.get(0),
        )?;
        if pending {
            return Err(StoreError::Busy);
        }
        let live_job: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM cron_jobs WHERE source_dialog_id=? AND desired_state!='deleted')",
            [id.get()],
            |row| row.get(0),
        )?;
        if live_job {
            return Err(StoreError::Busy);
        }
        // Polymorphic audit owners cannot use a normal foreign key. Delete
        // interactive audit records before cascading turns/messages atomically.
        let expected_audits: i64 = tx.query_row("SELECT count(*) FROM tool_runs WHERE owner_kind='interactive_turn' AND owner_id IN (SELECT id FROM turns WHERE dialog_id=?)", [id.get()], |r| r.get(0))?;
        let removed_audits = tx.execute("DELETE FROM tool_runs WHERE owner_kind='interactive_turn' AND owner_id IN (SELECT id FROM turns WHERE dialog_id=?)", [id.get()])?;
        if i64::try_from(removed_audits).ok() != Some(expected_audits) {
            return Err(StoreError::Conflict);
        }
        execute_one(&tx, "DELETE FROM dialogs WHERE id=?", [id.get()])?;
        tx.commit()?;
        Ok(())
    }

    pub fn begin_turn(&self, id: DialogId, input: &str) -> Result<TurnStart, StoreError> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.require_runtime_owner_for_creation(&tx)?;
        dialog_exists(&tx, id)?;
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM turns WHERE dialog_id=? AND status='pending')",
            [id.get()],
            |r| r.get(0),
        )?;
        if pending {
            return Err(StoreError::Busy);
        }
        let timestamp = now();
        execute_one(
            &tx,
            "INSERT INTO turns(dialog_id,status,started_at,runtime_owner_id) VALUES(?,'pending',?,?)",
            params![id.get(), timestamp, self.runtime_owner_id()],
        )?;
        let turn_id = TurnId::new(tx.last_insert_rowid()).map_err(|_| StoreError::Database)?;
        execute_one(
            &tx,
            "INSERT INTO messages(dialog_id,turn_id,role,content,created_at) VALUES(?,?,'user',?,?)",
            params![id.get(), turn_id.get(), input, timestamp],
        )?;
        let user_message_id = tx.last_insert_rowid();
        execute_one(
            &tx,
            "UPDATE dialogs SET updated_at=? WHERE id=?",
            params![timestamp, id.get()],
        )?;
        tx.commit()?;
        Ok(TurnStart {
            turn_id,
            user_message_id,
        })
    }

    pub fn completed_messages(&self, id: DialogId) -> Result<Vec<StoredMessage>, StoreError> {
        let mut db = self.connection()?;
        // One read snapshot prevents deletion between the existence check and
        // history query from returning a spurious empty successful history.
        let tx = db.transaction()?;
        dialog_exists(&tx, id)?;
        let messages = {
            let mut statement = tx.prepare("SELECT m.id,m.dialog_id,m.turn_id,m.role,m.content,m.created_at FROM messages m JOIN turns t ON t.id=m.turn_id AND t.dialog_id=m.dialog_id WHERE m.dialog_id=? AND t.status='completed' ORDER BY t.id,m.id")?;
            statement
                .query_map([id.get()], |r| {
                    Ok(StoredMessage {
                        id: r.get(0)?,
                        dialog_id: DialogId::new(r.get(1)?)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                        turn_id: TurnId::new(r.get(2)?)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                        role: match r.get::<_, String>(3)?.as_str() {
                            "user" => MessageRole::User,
                            "assistant" => MessageRole::Assistant,
                            _ => return Err(rusqlite::Error::InvalidQuery),
                        },
                        content: r.get(4)?,
                        created_at: r.get(5)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        tx.commit()?;
        Ok(messages)
    }

    pub fn complete_turn(&self, id: TurnId, answer: &str) -> Result<(), StoreError> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (dialog_id, status, runtime_owner_id) = tx
            .query_row(
                "SELECT dialog_id,status,runtime_owner_id FROM turns WHERE id=?",
                [id.get()],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or(StoreError::NotFound)?;
        if status != "pending" {
            let old_answer: Option<String> = tx
                .query_row(
                    "SELECT content FROM messages WHERE turn_id=? AND role='assistant'",
                    [id.get()],
                    |r| r.get(0),
                )
                .optional()?;
            return if status == "completed" && old_answer.as_deref() == Some(answer) {
                Ok(())
            } else {
                Err(StoreError::Conflict)
            };
        }
        self.require_runtime_owner(&tx, runtime_owner_id.as_deref())?;
        self.reject_pending_tools(&tx, "interactive_turn", id.get())?;
        let timestamp = now();
        execute_one(
            &tx,
            "INSERT INTO messages(dialog_id,turn_id,role,content,created_at) VALUES(?,?,'assistant',?,?)",
            params![dialog_id, id.get(), answer, timestamp],
        )?;
        execute_one(
            &tx,
            "UPDATE turns SET status='completed',finished_at=?,runtime_owner_id=NULL WHERE id=?",
            params![timestamp, id.get()],
        )?;
        execute_one(
            &tx,
            "UPDATE dialogs SET updated_at=? WHERE id=?",
            params![timestamp, dialog_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn fail_turn(&self, id: TurnId, code: SafeErrorCode) -> Result<(), StoreError> {
        self.end_turn(id, TurnStatus::Failed, code)
    }

    pub fn interrupt_turn(&self, id: TurnId, code: SafeErrorCode) -> Result<(), StoreError> {
        self.end_turn(id, TurnStatus::Interrupted, code)
    }

    fn end_turn(
        &self,
        id: TurnId,
        status: TurnStatus,
        code: SafeErrorCode,
    ) -> Result<(), StoreError> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (old_status, old_code, dialog_id, runtime_owner_id) = tx
            .query_row(
                "SELECT status,safe_error_code,dialog_id,runtime_owner_id FROM turns WHERE id=?",
                [id.get()],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or(StoreError::NotFound)?;
        let status = match status {
            TurnStatus::Failed => "failed",
            TurnStatus::Interrupted => "interrupted",
            _ => unreachable!(),
        };
        if old_status != "pending" {
            return if old_status == status && old_code.as_deref() == Some(code.as_str()) {
                Ok(())
            } else {
                Err(StoreError::Conflict)
            };
        }
        self.require_runtime_owner(&tx, runtime_owner_id.as_deref())?;
        self.reject_pending_tools(&tx, "interactive_turn", id.get())?;
        let timestamp = now();
        execute_one(
            &tx,
            "UPDATE turns SET status=?,safe_error_code=?,finished_at=?,runtime_owner_id=NULL WHERE id=?",
            params![status, code.as_str(), timestamp, id.get()],
        )?;
        execute_one(
            &tx,
            "UPDATE dialogs SET updated_at=? WHERE id=?",
            params![timestamp, dialog_id],
        )?;
        tx.commit()?;
        Ok(())
    }
}
