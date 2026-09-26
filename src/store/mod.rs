//! Day 18 persistence. Each operation owns a short-lived connection; no network
//! or provider work may be performed inside a store transaction.

mod audit;
mod dialogs;

pub use audit::{ToolRun, ToolRunFinish, ToolRunStart};
pub use dialogs::{Dialog, DialogSummary, MessageRole, StoredMessage, TurnStart};

use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Closed local codes: remote error strings cannot become persisted metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafeErrorCode {
    InternalError,
    ProviderError,
    ToolError,
    Interrupted,
    ProcessInterrupted,
    TimedOut,
    ContextTooLong,
    ToolRoundLimit,
}

impl SafeErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InternalError => "internal_error",
            Self::ProviderError => "provider_error",
            Self::ToolError => "tool_error",
            Self::Interrupted => "interrupted",
            Self::ProcessInterrupted => "process_interrupted",
            Self::TimedOut => "timed_out",
            Self::ContextTooLong => "context_too_long",
            Self::ToolRoundLimit => "tool_round_limit",
        }
    }

    pub(crate) fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "internal_error" => Ok(Self::InternalError),
            "provider_error" => Ok(Self::ProviderError),
            "tool_error" => Ok(Self::ToolError),
            "interrupted" => Ok(Self::Interrupted),
            "process_interrupted" => Ok(Self::ProcessInterrupted),
            "timed_out" => Ok(Self::TimedOut),
            "context_too_long" => Ok(Self::ContextTooLong),
            "tool_round_limit" => Ok(Self::ToolRoundLimit),
            _ => Err(rusqlite::Error::InvalidQuery),
        }
    }
}

/// Errors deliberately carry no SQLite details, paths, prompts, or remote text.
#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum StoreError {
    #[error("store_busy")]
    Busy,
    #[error("not_found")]
    NotFound,
    #[error("conflict")]
    Conflict,
    #[error("invalid_owner")]
    InvalidOwner,
    #[error("invalid_metadata")]
    InvalidMetadata,
    #[error("invalid_database_path")]
    InvalidPath,
    #[error("unsupported_schema")]
    UnsupportedSchema,
    #[error("database_error")]
    Database,
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        match error.sqlite_error_code() {
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                Self::Busy
            }
            Some(rusqlite::ErrorCode::ConstraintViolation) => Self::Conflict,
            _ => Self::Database,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Store {
    path: PathBuf,
}

impl Store {
    /// Opens a file-backed Day 18 database. Legacy DialogStore files are not
    /// migrated. Relative paths are anchored now so subsequent cwd changes do
    /// not redirect operations to another database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(StoreError::InvalidPath);
        }
        let path = if path.is_absolute() {
            path.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|_| StoreError::InvalidPath)?
                .join(path)
        };
        let store = Self { path };
        let mut db = store.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version_table: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version')",
            [], |r| r.get(0),
        )?;
        if version_table {
            let mut statement = tx.prepare("SELECT version FROM schema_version")?;
            let versions = statement
                .query_map([], |r| r.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if versions != [1] {
                return Err(StoreError::UnsupportedSchema);
            }
        } else {
            let existing: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%')",
                [],
                |r| r.get(0),
            )?;
            if existing {
                return Err(StoreError::UnsupportedSchema);
            }
            tx.execute_batch(SCHEMA_V1)?;
        }
        tx.commit()?;
        Ok(store)
    }

    pub(crate) fn connection(&self) -> Result<Connection, StoreError> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        Ok(connection)
    }
}

pub(crate) fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub(crate) fn execute_one(
    db: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<(), StoreError> {
    if db.execute(sql, params)? != 1 {
        return Err(StoreError::Conflict);
    }
    Ok(())
}

pub(crate) fn dialog_exists(
    db: &Connection,
    id: crate::domain::DialogId,
) -> Result<(), StoreError> {
    db.query_row("SELECT id FROM dialogs WHERE id=?", [id.get()], |_| Ok(()))
        .optional()?
        .ok_or(StoreError::NotFound)
}

const SCHEMA_V1: &str = "
CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version VALUES (1);
CREATE TABLE dialogs (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
    title TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE turns (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
    dialog_id INTEGER NOT NULL REFERENCES dialogs(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK(status IN ('pending','completed','failed','interrupted')),
    safe_error_code TEXT CHECK(safe_error_code IS NULL OR safe_error_code IN
        ('internal_error','provider_error','tool_error','interrupted','process_interrupted','timed_out','context_too_long','tool_round_limit')),
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at TEXT,
    UNIQUE(id,dialog_id),
    CHECK((status='pending' AND finished_at IS NULL AND safe_error_code IS NULL)
       OR (status='completed' AND finished_at IS NOT NULL AND safe_error_code IS NULL)
       OR (status IN ('failed','interrupted') AND finished_at IS NOT NULL AND safe_error_code IS NOT NULL))
);
CREATE UNIQUE INDEX one_pending_turn_per_dialog ON turns(dialog_id) WHERE status='pending';
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
    dialog_id INTEGER NOT NULL REFERENCES dialogs(id) ON DELETE CASCADE,
    turn_id INTEGER NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('user','assistant')),
    content TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY(turn_id,dialog_id) REFERENCES turns(id,dialog_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX one_user_per_turn ON messages(turn_id) WHERE role='user';
CREATE UNIQUE INDEX one_assistant_per_turn ON messages(turn_id) WHERE role='assistant';
CREATE INDEX messages_by_dialog_turn ON messages(dialog_id,turn_id,id);
CREATE TABLE tool_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('interactive_turn','cron_run')),
    owner_id INTEGER NOT NULL CHECK(owner_id > 0),
    call_id TEXT NOT NULL CHECK(length(call_id) BETWEEN 1 AND 256 AND call_id NOT GLOB '*[^A-Za-z0-9_.:-]*'),
    server_name TEXT NOT NULL CHECK(length(server_name) BETWEEN 1 AND 256 AND server_name NOT GLOB '*[^A-Za-z0-9_.:-]*'),
    tool_name TEXT NOT NULL CHECK(length(tool_name) BETWEEN 1 AND 256 AND tool_name NOT GLOB '*[^A-Za-z0-9_.:-]*'),
    read_only INTEGER NOT NULL CHECK(read_only IN (0,1)),
    status TEXT NOT NULL CHECK(status IN ('pending','completed','failed','uncertain')),
    safe_error_code TEXT CHECK(safe_error_code IS NULL OR safe_error_code IN
        ('internal_error','provider_error','tool_error','interrupted','process_interrupted','timed_out','context_too_long','tool_round_limit')),
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at TEXT,
    UNIQUE(owner_kind,owner_id,call_id),
    CHECK((status='pending' AND finished_at IS NULL AND safe_error_code IS NULL)
       OR (status='completed' AND finished_at IS NOT NULL AND safe_error_code IS NULL)
       OR (status IN ('failed','uncertain') AND finished_at IS NOT NULL AND safe_error_code IS NOT NULL))
);
";
