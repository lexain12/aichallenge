use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use thiserror::Error;

use crate::chat::{Message, Role};

pub struct DialogStore {
    connection: Connection,
}

pub struct StoredDialog {
    pub id: i64,
    pub system_prompt: String,
    pub messages: Vec<Message>,
}

pub struct DialogSummary {
    pub id: i64,
    pub title: String,
    pub updated_at: String,
    pub message_count: i64,
}

impl DialogStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS dialogs (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 system_prompt TEXT NOT NULL,
                 title TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                 last_message_id INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS messages (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
                 role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
                 content TEXT NOT NULL,
                 created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE INDEX IF NOT EXISTS messages_by_dialog ON messages(dialog_id, id);",
        )?;
        Ok(Self { connection })
    }

    /// Create a dialog and its first user message in one durable transaction.
    pub fn start_dialog(&mut self, system_prompt: &str, prompt: &str) -> Result<i64, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let title: String = prompt.chars().take(60).collect();
        tx.execute(
            "INSERT INTO dialogs (system_prompt, title) VALUES (?1, ?2)",
            params![system_prompt, title],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO messages (dialog_id, role, content) VALUES (?1, 'user', ?2)",
            params![id, prompt],
        )?;
        tx.execute(
            "UPDATE dialogs SET last_message_id = ?1 WHERE id = ?2",
            params![tx.last_insert_rowid(), id],
        )?;
        tx.commit()?;
        Ok(id)
    }

    /// Reject stale sessions instead of silently mixing independently generated replies.
    pub fn append_message(
        &mut self,
        id: i64,
        expected_count: usize,
        role: Role,
        content: &str,
    ) -> Result<(), StoreError> {
        let role = match role {
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::System => return Err(StoreError::InvalidRole),
        };
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).ok() != Some(expected_count) {
            return Err(StoreError::Conflict(id));
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM dialogs WHERE id = ?1)",
            [id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound(id));
        }
        tx.execute(
            "INSERT INTO messages (dialog_id, role, content) VALUES (?1, ?2, ?3)",
            params![id, role, content],
        )?;
        tx.execute("UPDATE dialogs SET last_message_id = ?1, updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?2", params![tx.last_insert_rowid(), id])?;
        tx.commit()?;
        Ok(())
    }

    pub fn load(&self, id: i64) -> Result<StoredDialog, StoreError> {
        // Read metadata and messages from a single SQLite snapshot.
        let tx = self.connection.unchecked_transaction()?;
        let system_prompt = tx
            .query_row(
                "SELECT system_prompt FROM dialogs WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(StoreError::NotFound(id))?;
        let messages = {
            let mut statement =
                tx.prepare("SELECT role, content FROM messages WHERE dialog_id = ?1 ORDER BY id")?;
            statement
                .query_map([id], |row| {
                    let role: String = row.get(0)?;
                    let role = match role.as_str() {
                        "user" => Role::User,
                        "assistant" => Role::Assistant,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    };
                    Ok(Message::new(role, row.get(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        tx.commit()?;
        Ok(StoredDialog {
            id,
            system_prompt,
            messages,
        })
    }

    pub fn latest_id(&self) -> Result<Option<i64>, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT id FROM dialogs ORDER BY last_message_id DESC, id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn list(&self) -> Result<Vec<DialogSummary>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT d.id, d.title, d.updated_at, (SELECT count(*) FROM messages m WHERE m.dialog_id = d.id)
             FROM dialogs d ORDER BY d.last_message_id DESC, d.id DESC",
        )?;
        Ok(statement
            .query_map([], |row| {
                Ok(DialogSummary {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    updated_at: row.get(2)?,
                    message_count: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("dialog database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("dialog {0} was not found")]
    NotFound(i64),
    #[error("dialog {0} changed in another session; restart with --resume {0}")]
    Conflict(i64),
    #[error("system instructions belong to the dialog, not its message list")]
    InvalidRole,
}
