//! Day 18 persistence. Each operation owns a short-lived connection; no network
//! or provider work may be performed inside a store transaction.

mod audit;
mod dialogs;
mod jobs;
mod runtime;

pub use audit::{ToolRun, ToolRunFinish, ToolRunStart};
pub use dialogs::{Dialog, DialogSummary, MessageRole, StoredMessage, TurnStart};
pub use jobs::{CronJob, CronRun, CronRunClaim, CronRunFinish, JobCreate, RunClaim, ServiceEvent};
pub(crate) use runtime::RuntimeOwnerRecord;

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::runtime::RuntimeOwnerCapability;

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
            Some(rusqlite::ErrorCode::OperationInterrupted) => Self::Busy,
            Some(rusqlite::ErrorCode::ConstraintViolation) => Self::Conflict,
            _ => Self::Database,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Store {
    path: PathBuf,
    identity: DatabaseIdentity,
    trusted_directory: std::sync::Arc<TrustedDirectory>,
    runtime_owner: Option<std::sync::Arc<RuntimeOwnerCapability>>,
}

#[derive(Debug)]
struct TrustedDirectory {
    path: PathBuf,
    file: std::fs::File,
    identity: DatabaseIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseIdentity {
    pub device: u64,
    pub inode: u64,
}

impl Store {
    /// Opens a file-backed Day 18 database. Legacy DialogStore files are not
    /// migrated. Relative paths are anchored now so subsequent cwd changes do
    /// not redirect operations to another database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_deadline(
            path,
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
    }

    pub fn open_with_deadline(
        path: impl AsRef<Path>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, StoreError> {
        check_deadline(deadline, cancellation)?;
        let (path, identity, trusted_directory) =
            prepare_database_path(path.as_ref(), deadline, cancellation)?;
        let store = Self {
            path,
            identity,
            trusted_directory: std::sync::Arc::new(trusted_directory),
            runtime_owner: None,
        };
        store.with_immediate_transaction(deadline, cancellation, |tx| {
            let version_table: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version')",
                [], |r| r.get(0),
            )?;
            if version_table {
                let mut statement = tx.prepare("SELECT version FROM schema_version")?;
                let versions = statement
                    .query_map([], |r| r.get::<_, i64>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                match versions.as_slice() {
                    [1] => {
                        tx.execute_batch(SCHEMA_V2)?;
                        tx.execute_batch(SCHEMA_V3)?;
                        tx.execute_batch(SCHEMA_V4)?;
                    }
                    [2] => {
                        tx.execute_batch(SCHEMA_V3)?;
                        tx.execute_batch(SCHEMA_V4)?;
                    }
                    [3] => tx.execute_batch(SCHEMA_V4)?,
                    [4] => {}
                    _ => return Err(StoreError::UnsupportedSchema),
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
                tx.execute_batch(SCHEMA_V2)?;
                tx.execute_batch(SCHEMA_V3)?;
                tx.execute_batch(SCHEMA_V4)?;
            }
            Ok(())
        })?;
        Ok(store)
    }

    pub(crate) fn connection(&self) -> Result<Connection, StoreError> {
        self.connection_with_busy_timeout(Duration::from_secs(5))
    }

    pub(crate) fn connection_with_busy_timeout(
        &self,
        busy_timeout: Duration,
    ) -> Result<Connection, StoreError> {
        self.validate_storage_boundary()?;
        let connection =
            Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        // Recheck before any writable pragma. Opening without CREATE prevents
        // a rename race from manufacturing a replacement database here.
        self.validate_storage_boundary()?;
        connection.busy_timeout(busy_timeout.max(Duration::from_millis(1)))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        self.validate_storage_boundary()?;
        Ok(connection)
    }

    pub(crate) fn with_immediate_transaction<T>(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
        operation: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        const BUSY_SLICE: Duration = Duration::from_millis(10);
        let mut operation = Some(operation);
        loop {
            if cancellation.is_cancelled() || Instant::now() >= deadline {
                return Err(StoreError::Busy);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let mut db = match self.connection_with_busy_timeout(remaining.min(BUSY_SLICE)) {
                Ok(db) => db,
                Err(StoreError::Busy) => continue,
                Err(error) => return Err(error),
            };
            let progress_cancellation = cancellation.clone();
            db.progress_handler(
                1_000,
                Some(move || progress_cancellation.is_cancelled() || Instant::now() >= deadline),
            )?;
            let tx = match db.transaction_with_behavior(TransactionBehavior::Immediate) {
                Ok(tx) => tx,
                Err(error)
                    if matches!(
                        error.sqlite_error_code(),
                        Some(
                            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                        )
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if cancellation.is_cancelled() || Instant::now() >= deadline {
                return Err(StoreError::Busy);
            }
            let result = operation.take().expect("operation runs once")(&tx)?;
            if cancellation.is_cancelled() || Instant::now() >= deadline {
                return Err(StoreError::Busy);
            }
            tx.commit()?;
            return Ok(result);
        }
    }

    pub(crate) fn with_runtime_capability_until(
        &self,
        capability: std::sync::Arc<RuntimeOwnerCapability>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, StoreError> {
        let owner_id = capability.owner_id();
        uuid::Uuid::parse_str(owner_id).map_err(|_| StoreError::InvalidOwner)?;
        let exists: bool = self.with_immediate_transaction(deadline, cancellation, |tx| {
            Ok(tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_owners WHERE owner_id=?)",
                [owner_id],
                |row| row.get(0),
            )?)
        })?;
        if !exists {
            return Err(StoreError::InvalidOwner);
        }
        let mut store = self.clone();
        store.runtime_owner = Some(capability);
        Ok(store)
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn database_directory_path(&self) -> &Path {
        &self.trusted_directory.path
    }

    pub(crate) fn database_identity(&self) -> DatabaseIdentity {
        self.identity
    }

    pub(crate) fn validate_storage_boundary(&self) -> Result<(), StoreError> {
        // rusqlite does not expose an fd-bound VFS here. The exact-mode 0700
        // parent is therefore the security boundary: another Unix user cannot
        // rename entries between these checks, while a same-uid adversary is
        // deliberately out of scope because it can already inspect this
        // process and its provider credentials. Retaining the directory fd
        // still detects path replacement for the lifetime of every Store.
        validate_trusted_directory_identity(&self.trusted_directory)?;
        validate_database_identity(&self.path, self.identity)
    }

    pub(crate) fn runtime_owner_id(&self) -> Option<&str> {
        self.runtime_owner
            .as_deref()
            .map(RuntimeOwnerCapability::owner_id)
    }

    pub(crate) fn require_runtime_owner_for_creation(
        &self,
        db: &Connection,
    ) -> Result<(), StoreError> {
        if self.runtime_owner.is_some() {
            return Ok(());
        }
        let initialized: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_coordination WHERE singleton=1)",
            [],
            |row| row.get(0),
        )?;
        if initialized {
            Err(StoreError::InvalidOwner)
        } else {
            Ok(())
        }
    }

    pub(crate) fn require_runtime_owner(
        &self,
        db: &Connection,
        active_owner: Option<&str>,
    ) -> Result<(), StoreError> {
        match (self.runtime_owner_id(), active_owner) {
            (Some(expected), Some(actual)) if expected == actual => Ok(()),
            (Some(_), _) => Err(StoreError::InvalidOwner),
            (None, Some(_)) => Err(StoreError::InvalidOwner),
            (None, None) => self.require_runtime_owner_for_creation(db),
        }
    }

    pub(crate) fn reject_pending_tools(
        &self,
        db: &Connection,
        owner_kind: &str,
        owner_id: i64,
    ) -> Result<(), StoreError> {
        let pending: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM tool_runs WHERE owner_kind=? AND owner_id=? AND status='pending')",
            rusqlite::params![owner_kind, owner_id],
            |row| row.get(0),
        )?;
        if pending {
            Err(StoreError::Busy)
        } else {
            Ok(())
        }
    }
}

fn check_deadline(deadline: Instant, cancellation: &CancellationToken) -> Result<(), StoreError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(StoreError::Busy)
    } else {
        Ok(())
    }
}

fn prepare_database_path(
    path: &Path,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(PathBuf, DatabaseIdentity, TrustedDirectory), StoreError> {
    check_deadline(deadline, cancellation)?;
    if path.as_os_str().is_empty() || path == Path::new(":memory:") {
        return Err(StoreError::InvalidPath);
    }
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|_| StoreError::InvalidPath)?
            .join(path)
    };
    let file_name = absolute.file_name().ok_or(StoreError::InvalidPath)?;
    let parent = absolute.parent().ok_or(StoreError::InvalidPath)?;
    let parent = fs::canonicalize(parent).map_err(|_| StoreError::InvalidPath)?;
    let trusted_directory = open_trusted_directory(&parent)?;
    let canonical_candidate = parent.join(file_name);

    match fs::symlink_metadata(&canonical_candidate) {
        Ok(metadata) => validate_database_metadata(&metadata)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            check_deadline(deadline, cancellation)?;
            let mut options = OpenOptions::new();
            options.read(true).write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            options
                .open(&canonical_candidate)
                .map_err(|_| StoreError::InvalidPath)?;
        }
        Err(_) => return Err(StoreError::InvalidPath),
    }
    check_deadline(deadline, cancellation)?;
    let path = fs::canonicalize(&canonical_candidate).map_err(|_| StoreError::InvalidPath)?;
    if path.parent() != Some(parent.as_path()) {
        return Err(StoreError::InvalidPath);
    }
    let metadata = fs::symlink_metadata(&path).map_err(|_| StoreError::InvalidPath)?;
    validate_database_metadata(&metadata)?;
    Ok((path, metadata_identity(&metadata), trusted_directory))
}

fn validate_database_identity(path: &Path, expected: DatabaseIdentity) -> Result<(), StoreError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| StoreError::InvalidPath)?;
    validate_database_metadata(&metadata)?;
    if metadata_identity(&metadata) != expected {
        return Err(StoreError::InvalidPath);
    }
    Ok(())
}

#[cfg(unix)]
fn open_trusted_directory(path: &Path) -> Result<TrustedDirectory, StoreError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path).map_err(|_| StoreError::InvalidPath)?;
    let metadata = file.metadata().map_err(|_| StoreError::InvalidPath)?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o7777 != 0o700
    {
        return Err(StoreError::InvalidPath);
    }
    let path_metadata = fs::symlink_metadata(path).map_err(|_| StoreError::InvalidPath)?;
    if metadata_identity(&path_metadata) != metadata_identity(&metadata) {
        return Err(StoreError::InvalidPath);
    }
    Ok(TrustedDirectory {
        path: path.to_owned(),
        identity: metadata_identity(&metadata),
        file,
    })
}

#[cfg(not(unix))]
fn open_trusted_directory(_path: &Path) -> Result<TrustedDirectory, StoreError> {
    Err(StoreError::InvalidPath)
}

#[cfg(unix)]
fn validate_trusted_directory_identity(directory: &TrustedDirectory) -> Result<(), StoreError> {
    let fd_metadata = directory
        .file
        .metadata()
        .map_err(|_| StoreError::InvalidPath)?;
    if !fd_metadata.is_dir()
        || fd_metadata.uid() != unsafe { libc::geteuid() }
        || fd_metadata.mode() & 0o7777 != 0o700
        || metadata_identity(&fd_metadata) != directory.identity
    {
        return Err(StoreError::InvalidPath);
    }
    let path_metadata =
        fs::symlink_metadata(&directory.path).map_err(|_| StoreError::InvalidPath)?;
    if metadata_identity(&path_metadata) != directory.identity {
        return Err(StoreError::InvalidPath);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_trusted_directory_identity(_directory: &TrustedDirectory) -> Result<(), StoreError> {
    Err(StoreError::InvalidPath)
}

#[cfg(unix)]
fn validate_database_metadata(metadata: &fs::Metadata) -> Result<(), StoreError> {
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.mode() & 0o7777 != 0o600
    {
        return Err(StoreError::InvalidPath);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_database_metadata(metadata: &fs::Metadata) -> Result<(), StoreError> {
    metadata
        .is_file()
        .then_some(())
        .ok_or(StoreError::InvalidPath)
}

#[cfg(unix)]
fn metadata_identity(metadata: &fs::Metadata) -> DatabaseIdentity {
    DatabaseIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn metadata_identity(_metadata: &fs::Metadata) -> DatabaseIdentity {
    DatabaseIdentity {
        device: 0,
        inode: 0,
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

const SCHEMA_V2: &str = "
CREATE TABLE cron_jobs (
    id TEXT PRIMARY KEY CHECK(length(id)=36),
    source_dialog_id INTEGER REFERENCES dialogs(id) ON DELETE SET NULL,
    name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 256),
    schedule_kind TEXT NOT NULL CHECK(schedule_kind IN ('cron','once_at')),
    schedule_value TEXT NOT NULL,
    timezone TEXT NOT NULL,
    prompt TEXT NOT NULL CHECK(length(prompt) BETWEEN 1 AND 262144),
    desired_state TEXT NOT NULL CHECK(desired_state IN ('active','disabled','deleted')),
    sync_state TEXT NOT NULL CHECK(sync_state IN ('pending','applied','failed')),
    safe_sync_error_code TEXT CHECK(safe_sync_error_code IS NULL OR safe_sync_error_code IN
        ('internal_error','provider_error','tool_error','interrupted','process_interrupted','timed_out','context_too_long','tool_round_limit')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK(source_dialog_id IS NOT NULL OR desired_state='deleted'),
    CHECK((sync_state='failed' AND safe_sync_error_code IS NOT NULL)
       OR (sync_state!='failed' AND safe_sync_error_code IS NULL))
);
CREATE INDEX cron_jobs_by_source_dialog ON cron_jobs(source_dialog_id);
CREATE TABLE cron_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id > 0),
    job_id TEXT NOT NULL REFERENCES cron_jobs(id),
    scheduled_for TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','completed','failed','interrupted','timed_out','skipped','missed')),
    result TEXT,
    safe_error_code TEXT CHECK(safe_error_code IS NULL OR safe_error_code IN
        ('internal_error','provider_error','tool_error','interrupted','process_interrupted','timed_out','context_too_long','tool_round_limit')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    CHECK((status='pending' AND result IS NULL AND safe_error_code IS NULL AND finished_at IS NULL)
       OR (status='completed' AND result IS NOT NULL AND safe_error_code IS NULL AND finished_at IS NOT NULL)
       OR (status IN ('failed','interrupted','timed_out') AND result IS NULL AND safe_error_code IS NOT NULL AND finished_at IS NOT NULL)
       OR (status IN ('skipped','missed') AND result IS NULL AND safe_error_code IS NULL AND finished_at IS NOT NULL))
);
CREATE UNIQUE INDEX one_pending_run_per_job ON cron_runs(job_id) WHERE status='pending';
CREATE UNIQUE INDEX one_execution_per_job_minute ON cron_runs(job_id,scheduled_for) WHERE status!='skipped';
CREATE INDEX cron_runs_by_job ON cron_runs(job_id,id);
UPDATE schema_version SET version=2;
";

const SCHEMA_V3: &str = "
CREATE TABLE runtime_coordination (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    lock_name TEXT NOT NULL UNIQUE,
    lock_device INTEGER NOT NULL,
    lock_inode INTEGER NOT NULL
);
CREATE TABLE runtime_owners (
    owner_id TEXT PRIMARY KEY CHECK(length(owner_id)=36),
    lock_name TEXT NOT NULL UNIQUE,
    lock_device INTEGER NOT NULL,
    lock_inode INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
ALTER TABLE turns ADD COLUMN runtime_owner_id TEXT REFERENCES runtime_owners(owner_id);
ALTER TABLE cron_runs ADD COLUMN runtime_owner_id TEXT REFERENCES runtime_owners(owner_id);
CREATE INDEX pending_turns_by_runtime_owner ON turns(runtime_owner_id) WHERE status='pending';
CREATE INDEX pending_runs_by_runtime_owner ON cron_runs(runtime_owner_id) WHERE status='pending';
UPDATE schema_version SET version=3;
";

const SCHEMA_V4: &str = "
ALTER TABLE tool_runs ADD COLUMN runtime_owner_id TEXT REFERENCES runtime_owners(owner_id);
UPDATE tool_runs SET runtime_owner_id=CASE owner_kind
    WHEN 'interactive_turn' THEN (SELECT runtime_owner_id FROM turns WHERE id=tool_runs.owner_id)
    WHEN 'cron_run' THEN (SELECT runtime_owner_id FROM cron_runs WHERE id=tool_runs.owner_id)
END WHERE status='pending';
CREATE INDEX pending_tools_by_runtime_owner ON tool_runs(runtime_owner_id) WHERE status='pending';
UPDATE schema_version SET version=4;
";
