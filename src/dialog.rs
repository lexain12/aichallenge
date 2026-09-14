use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use thiserror::Error;

use crate::chat::{Message, Role};
use crate::client::TokenUsage;
use crate::context::{ContextState, ContextSummary, UsageTotals};
use crate::facts::{Facts, FactsState};

pub struct DialogStore {
    connection: Connection,
}

pub struct StoredDialog {
    pub id: i64,
    pub system_prompt: String,
    pub messages: Vec<Message>,
    pub context: ContextState,
    pub facts: FactsState,
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
             CREATE INDEX IF NOT EXISTS messages_by_dialog ON messages(dialog_id, id);
             CREATE TABLE IF NOT EXISTS message_usage (
                 message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                 usage_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS dialog_context (
                 dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
                 summary TEXT NOT NULL,
                 covered_message_count INTEGER NOT NULL CHECK (covered_message_count > 0),
                 compaction_count INTEGER NOT NULL DEFAULT 0,
                 known_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                 known_completion_tokens INTEGER NOT NULL DEFAULT 0,
                 known_total_tokens INTEGER NOT NULL DEFAULT 0,
                 missing_usage_count INTEGER NOT NULL DEFAULT 0,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE TABLE IF NOT EXISTS dialog_facts (
                 dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
                 facts_json TEXT NOT NULL,
                 covered_message_count INTEGER NOT NULL CHECK (covered_message_count > 0),
                 update_count INTEGER NOT NULL DEFAULT 0,
                 known_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                 known_completion_tokens INTEGER NOT NULL DEFAULT 0,
                 known_total_tokens INTEGER NOT NULL DEFAULT 0,
                 missing_usage_count INTEGER NOT NULL DEFAULT 0,
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );",
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
        self.append_with_usage(id, expected_count, role, content, None)
    }

    /// The answer and its provider statistics either both commit or neither does.
    pub fn append_answer(
        &mut self,
        id: i64,
        expected_count: usize,
        content: &str,
        usage: Option<TokenUsage>,
    ) -> Result<(), StoreError> {
        self.append_with_usage(id, expected_count, Role::Assistant, content, usage)
    }

    fn append_with_usage(
        &mut self,
        id: i64,
        expected_count: usize,
        role: Role,
        content: &str,
        usage: Option<TokenUsage>,
    ) -> Result<(), StoreError> {
        let usage_json = usage
            .map(|value| serde_json::to_string(&value))
            .transpose()?;
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
        let message_id = tx.last_insert_rowid();
        if let Some(usage_json) = usage_json {
            tx.execute(
                "INSERT INTO message_usage (message_id, usage_json) VALUES (?1, ?2)",
                params![message_id, usage_json],
            )?;
        }
        tx.execute("UPDATE dialogs SET last_message_id = ?1, updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?2", params![message_id, id])?;
        tx.commit()?;
        Ok(())
    }

    pub fn replace_context(
        &mut self,
        id: i64,
        expected_message_count: usize,
        summary: ContextSummary,
        usage: Option<TokenUsage>,
    ) -> Result<ContextState, StoreError> {
        if summary.covered_message_count() == 0
            || summary.covered_message_count() > expected_message_count
        {
            return Err(StoreError::InvalidContext(
                "summary boundary must cover an existing non-empty prefix",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).ok() != Some(expected_message_count) {
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

        let stored = tx
            .query_row(
                "SELECT summary, covered_message_count, compaction_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_context WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let mut state = decode_context(stored)?;
        state.replace_summary(summary, usage);
        let current = state
            .summary()
            .expect("replacement always installs a summary");
        let totals = state.compaction_usage();
        tx.execute(
            "INSERT INTO dialog_context (
                 dialog_id, summary, covered_message_count, compaction_count,
                 known_prompt_tokens, known_completion_tokens,
                 known_total_tokens, missing_usage_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(dialog_id) DO UPDATE SET
                 summary = excluded.summary,
                 covered_message_count = excluded.covered_message_count,
                 compaction_count = excluded.compaction_count,
                 known_prompt_tokens = excluded.known_prompt_tokens,
                 known_completion_tokens = excluded.known_completion_tokens,
                 known_total_tokens = excluded.known_total_tokens,
                 missing_usage_count = excluded.missing_usage_count,
                 updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
            params![
                id,
                current.content(),
                to_i64(current.covered_message_count())?,
                to_i64(totals.call_count())?,
                to_i64(totals.prompt_tokens())?,
                to_i64(totals.completion_tokens())?,
                to_i64(totals.total_tokens())?,
                to_i64(totals.missing_usage_count())?,
            ],
        )?;
        tx.commit()?;
        Ok(state)
    }

    pub fn replace_facts(
        &mut self,
        id: i64,
        expected_message_count: usize,
        facts: Facts,
        usage: Option<TokenUsage>,
    ) -> Result<FactsState, StoreError> {
        if expected_message_count == 0 {
            return Err(StoreError::InvalidFacts(
                "facts boundary must cover an existing non-empty prefix",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE dialog_id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).ok() != Some(expected_message_count) {
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
        let stored = tx
            .query_row(
                "SELECT facts_json, covered_message_count, update_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_facts WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let state = decode_facts(stored, expected_message_count)?.updated(
            facts,
            expected_message_count,
            usage,
        );
        let facts_json = serde_json::to_string(state.facts())?;
        let totals = state.update_usage();
        tx.execute(
            "INSERT INTO dialog_facts (
                 dialog_id, facts_json, covered_message_count, update_count,
                 known_prompt_tokens, known_completion_tokens,
                 known_total_tokens, missing_usage_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(dialog_id) DO UPDATE SET
                 facts_json = excluded.facts_json,
                 covered_message_count = excluded.covered_message_count,
                 update_count = excluded.update_count,
                 known_prompt_tokens = excluded.known_prompt_tokens,
                 known_completion_tokens = excluded.known_completion_tokens,
                 known_total_tokens = excluded.known_total_tokens,
                 missing_usage_count = excluded.missing_usage_count,
                 updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
            params![
                id,
                facts_json,
                to_i64(state.covered_message_count())?,
                to_i64(totals.call_count())?,
                to_i64(totals.prompt_tokens())?,
                to_i64(totals.completion_tokens())?,
                to_i64(totals.total_tokens())?,
                to_i64(totals.missing_usage_count())?,
            ],
        )?;
        tx.commit()?;
        Ok(state)
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
                tx.prepare("SELECT m.role, m.content, u.usage_json FROM messages m LEFT JOIN message_usage u ON u.message_id = m.id WHERE m.dialog_id = ?1 ORDER BY m.id")?;
            statement
                .query_map([id], |row| {
                    let role: String = row.get(0)?;
                    let role = match role.as_str() {
                        "user" => Role::User,
                        "assistant" => Role::Assistant,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    };
                    let usage_json: Option<String> = row.get(2)?;
                    let usage = usage_json
                        .map(|value| serde_json::from_str(&value))
                        .transpose()
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?;
                    Ok(Message::new(role, row.get(1)?).with_usage(usage))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let stored_context = tx
            .query_row(
                "SELECT summary, covered_message_count, compaction_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_context WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let context = decode_context(stored_context)?;
        let stored_facts = tx
            .query_row(
                "SELECT facts_json, covered_message_count, update_count,
                        known_prompt_tokens, known_completion_tokens,
                        known_total_tokens, missing_usage_count
                 FROM dialog_facts WHERE dialog_id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let facts = decode_facts(stored_facts, messages.len())?;
        tx.commit()?;
        Ok(StoredDialog {
            id,
            system_prompt,
            messages,
            context,
            facts,
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

type StoredContextRow = (String, i64, i64, i64, i64, i64, i64);
type StoredFactsRow = (String, i64, i64, i64, i64, i64, i64);

fn decode_context(row: Option<StoredContextRow>) -> Result<ContextState, StoreError> {
    let Some((summary, covered, calls, prompt, completion, total, missing)) = row else {
        return Ok(ContextState::default());
    };
    let covered = usize::try_from(covered)
        .map_err(|_| StoreError::InvalidContext("negative summary boundary"))?;
    let values = [calls, prompt, completion, total, missing];
    if values.iter().any(|value| *value < 0) {
        return Err(StoreError::InvalidContext("negative context metric"));
    }
    Ok(ContextState::restored(
        Some(ContextSummary::new(summary, covered)),
        UsageTotals::from_parts(
            calls as u64,
            prompt as u64,
            completion as u64,
            total as u64,
            missing as u64,
        ),
    ))
}

fn decode_facts(
    row: Option<StoredFactsRow>,
    message_count: usize,
) -> Result<FactsState, StoreError> {
    let Some((facts_json, covered, calls, prompt, completion, total, missing)) = row else {
        return Ok(FactsState::default());
    };
    let facts: Facts = serde_json::from_str(&facts_json)
        .map_err(|_| StoreError::InvalidFacts("facts_json must be a string-to-string object"))?;
    let covered = usize::try_from(covered)
        .map_err(|_| StoreError::InvalidFacts("negative facts boundary"))?;
    if covered == 0 || covered > message_count {
        return Err(StoreError::InvalidFacts(
            "facts boundary exceeds dialog history",
        ));
    }
    let values = [calls, prompt, completion, total, missing];
    if values.iter().any(|value| *value < 0) {
        return Err(StoreError::InvalidFacts("negative facts metric"));
    }
    Ok(FactsState::restored(
        facts,
        covered,
        UsageTotals::from_parts(
            calls as u64,
            prompt as u64,
            completion as u64,
            total as u64,
            missing as u64,
        ),
    ))
}

fn to_i64(value: impl TryInto<i64>) -> Result<i64, StoreError> {
    value
        .try_into()
        .map_err(|_| StoreError::InvalidContext("context metric exceeds SQLite INTEGER"))
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("invalid token statistics: {0}")]
    Usage(#[from] serde_json::Error),
    #[error("dialog database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("dialog {0} was not found")]
    NotFound(i64),
    #[error("dialog {0} changed in another session; restart with --resume {0}")]
    Conflict(i64),
    #[error("system instructions belong to the dialog, not its message list")]
    InvalidRole,
    #[error("invalid dialog context: {0}")]
    InvalidContext(&'static str),
    #[error("invalid dialog facts: {0}")]
    InvalidFacts(&'static str),
}
