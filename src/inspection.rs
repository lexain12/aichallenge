//! Bounded logical inspection, streaming exports, and a restricted local SQL shell.

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, RecvTimeoutError, SyncSender, TrySendError},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::Engine as _;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::domain::{DialogId, JobId};
use crate::store::{Store, StoreError};

const DEFAULT_PAGE_SIZE: usize = 128;
const MAX_PAGE_SIZE: usize = 1024;
const MAX_SNAPSHOT_DURATION: Duration = Duration::from_secs(30);
const HARD_MAX_SQL_BYTES: usize = 1_048_576;
const HARD_MAX_ROWS: usize = 10_000;
const HARD_MAX_COLUMNS: usize = 256;
const HARD_MAX_CELL_BYTES: usize = 1_048_576;
const HARD_MAX_OUTPUT_BYTES: usize = 67_108_864;
const HARD_MAX_QUERY_TIME: Duration = Duration::from_secs(30);
const SQLITE_BUSY_POLL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectQuery {
    Dialogs,
    History(Option<DialogId>),
    Jobs,
    Job(JobId),
    Runs(JobId),
    Audit,
    Dump,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorScope {
    Dialogs,
    History(Option<DialogId>),
    Jobs,
    Job(JobId),
    Runs(JobId),
    Audit,
    Dump,
}

impl From<&InspectQuery> for CursorScope {
    fn from(value: &InspectQuery) -> Self {
        match value {
            InspectQuery::Dialogs => Self::Dialogs,
            InspectQuery::History(id) => Self::History(*id),
            InspectQuery::Jobs => Self::Jobs,
            InspectQuery::Job(id) => Self::Job(*id),
            InspectQuery::Runs(id) => Self::Runs(*id),
            InspectQuery::Audit => Self::Audit,
            InspectQuery::Dump => Self::Dump,
        }
    }
}

/// An opaque continuation token. It is meaningful only to the service that
/// produced it and only for the same inspection query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InspectionCursor {
    offset: u64,
    scope: CursorScope,
}

#[derive(Clone, PartialEq)]
pub struct InspectionResult {
    pub items: Vec<Value>,
    pub next_cursor: Option<InspectionCursor>,
    pub complete: bool,
}

impl std::fmt::Debug for InspectionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectionResult")
            .field("item_count", &self.items.len())
            .field("next_cursor", &self.next_cursor)
            .field("complete", &self.complete)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct InspectionService {
    store: Store,
    page_size: usize,
    snapshot_timeout: Duration,
    snapshot_tracker: Arc<SnapshotTracker>,
}

#[derive(Debug, Default)]
struct SnapshotTracker {
    state: Mutex<SnapshotTrackerState>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug, Default)]
struct SnapshotTrackerState {
    active: usize,
    started: u64,
}

impl SnapshotTracker {
    fn start(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active += 1;
            state.started = state.started.saturating_add(1);
            self.changed.notify_all();
        }
    }

    fn finish(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active = state.active.saturating_sub(1);
            self.changed.notify_all();
        }
    }

    fn snapshot(&self) -> SnapshotTrackerState {
        self.state.lock().map(|state| *state).unwrap_or_default()
    }

    fn wait_for(
        &self,
        timeout: Duration,
        condition: impl Fn(&SnapshotTrackerState) -> bool,
    ) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        let Ok((state, _)) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !condition(state))
        else {
            return false;
        };
        condition(&state)
    }
}

/// A bounded-lifetime read transaction used to stream every page from one
/// consistent SQLite snapshot. Call [`Self::close`] as soon as the response
/// stream is complete; dropping it also rolls the read transaction back.
pub struct InspectionSnapshot {
    commands: Option<SyncSender<SnapshotCommand>>,
    worker: Option<JoinHandle<()>>,
    complete: bool,
    deadline: Instant,
    cancellation: InspectionCancellation,
}

/// Cloneable cooperative cancellation shared by the snapshot caller and its
/// dedicated SQLite worker.
#[derive(Clone, Debug, Default)]
pub struct InspectionCancellation {
    cancelled: Arc<AtomicBool>,
    workers: Arc<Mutex<Vec<SyncSender<SnapshotCommand>>>>,
}

impl InspectionCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(workers) = self.workers.lock() {
            for worker in workers.iter() {
                let _ = worker.try_send(SnapshotCommand::Cancel);
            }
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn register(&self, worker: SyncSender<SnapshotCommand>) {
        if let Ok(mut workers) = self.workers.lock() {
            workers.push(worker);
        }
    }
}

enum SnapshotCommand {
    Next(SyncSender<Result<InspectionResult, InspectionError>>),
    Close(SyncSender<Result<(), InspectionError>>),
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportSummary {
    pub total_bytes: u64,
    pub records: u64,
    pub sha256: String,
}

/// One line of the stable JSONL export format.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalExportV1 {
    Header { format: String, version: u16 },
    Record { record: Value },
}

#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum InspectionError {
    #[error("store_error")]
    Store,
    #[error("invalid_page_size")]
    InvalidPageSize,
    #[error("invalid_cursor")]
    InvalidCursor,
    #[error("serialization_error")]
    Serialization,
    #[error("io_error")]
    Io,
    #[error("invalid_database_path")]
    InvalidDatabasePath,
    #[error("database_open_error")]
    DatabaseOpen,
    #[error("rejected_query")]
    RejectedQuery,
    #[error("multiple_statements")]
    MultipleStatements,
    #[error("parameters_not_allowed")]
    ParametersNotAllowed,
    #[error("query_error")]
    Query,
    #[error("input_too_large")]
    InputTooLarge,
    #[error("too_many_columns")]
    TooManyColumns,
    #[error("cell_too_large")]
    CellTooLarge,
    #[error("query_timed_out")]
    QueryTimedOut,
    #[error("resource_limit")]
    ResourceLimit,
    #[error("output_too_large")]
    OutputTooLarge,
    #[error("snapshot_expired")]
    SnapshotExpired,
}

impl From<StoreError> for InspectionError {
    fn from(_: StoreError) -> Self {
        Self::Store
    }
}

impl InspectionService {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            page_size: DEFAULT_PAGE_SIZE,
            snapshot_timeout: MAX_SNAPSHOT_DURATION,
            snapshot_tracker: Arc::new(SnapshotTracker::default()),
        }
    }

    pub fn with_page_size(store: Store, page_size: usize) -> Result<Self, InspectionError> {
        if page_size == 0 || page_size > MAX_PAGE_SIZE {
            return Err(InspectionError::InvalidPageSize);
        }
        Ok(Self {
            store,
            page_size,
            snapshot_timeout: MAX_SNAPSHOT_DURATION,
            snapshot_tracker: Arc::new(SnapshotTracker::default()),
        })
    }

    pub fn with_snapshot_timeout(
        store: Store,
        page_size: usize,
        snapshot_timeout: Duration,
    ) -> Result<Self, InspectionError> {
        if page_size == 0
            || page_size > MAX_PAGE_SIZE
            || snapshot_timeout.is_zero()
            || snapshot_timeout > MAX_SNAPSHOT_DURATION
        {
            return Err(InspectionError::ResourceLimit);
        }
        Ok(Self {
            store,
            page_size,
            snapshot_timeout,
            snapshot_tracker: Arc::new(SnapshotTracker::default()),
        })
    }

    /// Number of currently owned inspection workers, including connection
    /// startup. This is operational metadata only; it exposes no database or
    /// query content.
    pub fn active_snapshots(&self) -> usize {
        self.snapshot_tracker.snapshot().active
    }

    /// Monotonic worker-start sequence for bounded lifecycle coordination.
    pub fn snapshot_start_sequence(&self) -> u64 {
        self.snapshot_tracker.snapshot().started
    }

    /// Waits for a worker started after `sequence`. The timeout is only a
    /// deadlock guard; the condition variable supplies the readiness signal.
    pub fn wait_for_snapshot_start(&self, sequence: u64, timeout: Duration) -> bool {
        self.snapshot_tracker
            .wait_for(timeout, |state| state.started > sequence)
    }

    /// Waits until every owned snapshot worker has released its connection.
    pub fn wait_for_snapshot_idle(&self, timeout: Duration) -> bool {
        self.snapshot_tracker
            .wait_for(timeout, |state| state.active == 0)
    }

    pub fn inspect(&self, query: InspectQuery) -> Result<InspectionResult, InspectionError> {
        let mut snapshot = self.snapshot(query)?;
        let result = snapshot
            .next_page()?
            .ok_or(InspectionError::InvalidCursor)?;
        snapshot.close()?;
        Ok(result)
    }

    pub fn snapshot(&self, query: InspectQuery) -> Result<InspectionSnapshot, InspectionError> {
        self.snapshot_with_cancellation(query, InspectionCancellation::new())
    }

    pub fn snapshot_with_cancellation(
        &self,
        query: InspectQuery,
        cancellation: InspectionCancellation,
    ) -> Result<InspectionSnapshot, InspectionError> {
        self.snapshot_with_page_size_and_cancellation(query, self.page_size, cancellation)
    }

    pub(crate) fn snapshot_with_page_size_and_cancellation(
        &self,
        query: InspectQuery,
        page_size: usize,
        cancellation: InspectionCancellation,
    ) -> Result<InspectionSnapshot, InspectionError> {
        if page_size == 0 || page_size > MAX_PAGE_SIZE {
            return Err(InspectionError::InvalidPageSize);
        }
        let (commands, command_rx) = mpsc::sync_channel(1);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let store = self.store.clone();
        let snapshot_tracker = self.snapshot_tracker.clone();
        let deadline = Instant::now() + self.snapshot_timeout;
        let worker_cancellation = cancellation.clone();
        cancellation.register(commands.clone());
        snapshot_tracker.start();
        let worker = match thread::Builder::new()
            .name("light-agent-inspection".into())
            .spawn(move || {
                run_snapshot_worker(SnapshotWorker {
                    store,
                    query,
                    page_size,
                    deadline,
                    command_rx,
                    ready: ready_tx,
                    snapshot_tracker,
                    cancellation: worker_cancellation,
                })
            }) {
            Ok(worker) => worker,
            Err(_) => {
                self.snapshot_tracker.finish();
                return Err(InspectionError::Store);
            }
        };
        loop {
            if cancellation.is_cancelled() {
                drop(commands);
                let _ = worker.join();
                return Err(InspectionError::SnapshotExpired);
            }
            let startup_remaining = deadline.saturating_duration_since(Instant::now());
            if startup_remaining.is_zero() {
                drop(commands);
                let _ = worker.join();
                return Err(InspectionError::SnapshotExpired);
            }
            match ready_rx.recv_timeout(startup_remaining) {
                Ok(Ok(())) => break,
                Ok(Err(error)) => {
                    let _ = worker.join();
                    return Err(error);
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    let _ = worker.join();
                    return Err(InspectionError::SnapshotExpired);
                }
            }
        }
        Ok(InspectionSnapshot {
            commands: Some(commands),
            worker: Some(worker),
            complete: false,
            deadline,
            cancellation,
        })
    }

    /// Writes versioned JSONL directly to the supplied local stream. No path
    /// or remote destination is accepted at this boundary.
    pub fn write_export<W: Write>(&self, writer: &mut W) -> Result<ExportSummary, InspectionError> {
        let mut snapshot = self.snapshot(InspectQuery::Dump)?;
        let mut writer = DigestWriter::new(writer);
        write_json_line(
            &mut writer,
            &LogicalExportV1::Header {
                format: "logical_export_v1".into(),
                version: 1,
            },
        )?;
        let mut records = 0_u64;
        while let Some(page) = snapshot.next_page()? {
            for record in page.items {
                write_json_line(&mut writer, &LogicalExportV1::Record { record })?;
                records += 1;
            }
        }
        snapshot.close()?;
        writer.flush().map_err(|_| InspectionError::Io)?;
        let (total_bytes, sha256) = writer.finish();
        Ok(ExportSummary {
            total_bytes,
            records,
            sha256,
        })
    }
}

impl InspectionSnapshot {
    pub fn cancellation(&self) -> InspectionCancellation {
        self.cancellation.clone()
    }

    pub fn next_page(&mut self) -> Result<Option<InspectionResult>, InspectionError> {
        if self.complete {
            return Ok(None);
        }
        let remaining = match self.remaining() {
            Ok(remaining) => remaining,
            Err(error) => {
                self.cancellation.cancel();
                self.complete = true;
                self.commands.take();
                self.join_worker()?;
                return Err(error);
            }
        };
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.commands
            .as_ref()
            .ok_or(InspectionError::SnapshotExpired)?
            .try_send(SnapshotCommand::Next(reply_tx))
            .map_err(|_| InspectionError::SnapshotExpired)?;
        let result = loop {
            if self.cancellation.is_cancelled() {
                self.complete = true;
                self.commands.take();
                self.join_worker()?;
                return Err(InspectionError::SnapshotExpired);
            }
            match reply_rx.recv_timeout(remaining) {
                Ok(Ok(result)) => break result,
                Ok(Err(error)) => {
                    self.complete = true;
                    self.commands.take();
                    self.join_worker()?;
                    return Err(error);
                }
                Err(RecvTimeoutError::Timeout) => {
                    if self.remaining().is_err() {
                        self.cancellation.cancel();
                        self.complete = true;
                        self.commands.take();
                        self.join_worker()?;
                        return Err(InspectionError::SnapshotExpired);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.complete = true;
                    self.commands.take();
                    self.join_worker()?;
                    return Err(InspectionError::SnapshotExpired);
                }
            }
        };
        self.complete = result.complete;
        if self.complete {
            self.commands.take();
            self.join_worker()?;
        }
        Ok(Some(result))
    }

    pub fn close(mut self) -> Result<(), InspectionError> {
        if !self.complete {
            if let Some(commands) = self.commands.take() {
                let (reply_tx, reply_rx) = mpsc::sync_channel(1);
                match commands.try_send(SnapshotCommand::Close(reply_tx)) {
                    Ok(()) => {
                        let remaining = self.deadline.saturating_duration_since(Instant::now());
                        match reply_rx.recv_timeout(remaining) {
                            Ok(result) => result?,
                            Err(_) => return Err(InspectionError::SnapshotExpired),
                        }
                    }
                    Err(TrySendError::Disconnected(_)) => {}
                    Err(TrySendError::Full(_)) => return Err(InspectionError::Store),
                }
            }
            self.complete = true;
        }
        self.join_worker()
    }

    fn remaining(&self) -> Result<Duration, InspectionError> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(InspectionError::SnapshotExpired)
        } else {
            Ok(remaining)
        }
    }

    fn join_worker(&mut self) -> Result<(), InspectionError> {
        self.worker
            .take()
            .map(|worker| worker.join().map_err(|_| InspectionError::Store))
            .unwrap_or(Ok(()))
    }
}

impl Drop for InspectionSnapshot {
    fn drop(&mut self) {
        if !self.complete || self.worker.is_some() {
            self.cancellation.cancel();
        }
        if let Some(commands) = self.commands.take() {
            let (reply_tx, _reply_rx) = mpsc::sync_channel(1);
            let _ = commands.try_send(SnapshotCommand::Close(reply_tx));
        }
        // Dropping JoinHandle detaches the worker. It still owns the only
        // receiver and must either observe Close/disconnect or hit its hard
        // recv_timeout deadline, so SQLite ownership cannot escape forever.
        self.worker.take();
    }
}

struct SnapshotWorker {
    store: Store,
    query: InspectQuery,
    page_size: usize,
    deadline: Instant,
    command_rx: mpsc::Receiver<SnapshotCommand>,
    ready: SyncSender<Result<(), InspectionError>>,
    snapshot_tracker: Arc<SnapshotTracker>,
    cancellation: InspectionCancellation,
}

fn run_snapshot_worker(worker: SnapshotWorker) {
    let SnapshotWorker {
        store,
        query,
        page_size,
        deadline,
        command_rx: commands,
        ready,
        snapshot_tracker,
        cancellation,
    } = worker;
    let _active_guard = ActiveSnapshotGuard(snapshot_tracker);
    let db = match open_snapshot_connection(&store, deadline, &cancellation) {
        Ok(db) => db,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let progress_cancellation = cancellation.clone();
    if db
        .progress_handler(
            1_000,
            Some(move || progress_cancellation.is_cancelled() || Instant::now() >= deadline),
        )
        .is_err()
    {
        let _ = db.execute_batch("ROLLBACK");
        let _ = ready.send(Err(InspectionError::Store));
        return;
    }
    if ready.send(Ok(())).is_err() {
        let _ = db.execute_batch("ROLLBACK");
        return;
    }

    let mut offset = 0_u64;
    loop {
        if cancellation.is_cancelled() {
            let _ = db.execute_batch("ROLLBACK");
            return;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let _ = db.execute_batch("ROLLBACK");
            return;
        }
        match commands.recv_timeout(remaining) {
            Ok(SnapshotCommand::Next(reply)) => {
                let result = read_snapshot_page(
                    &db,
                    &query,
                    page_size,
                    &mut offset,
                    deadline,
                    &cancellation,
                );
                let terminal = match &result {
                    Ok(page) => page.complete,
                    Err(_) => true,
                };
                if terminal {
                    let _ = db.execute_batch("ROLLBACK");
                }
                let _ = reply.send(result);
                if terminal {
                    return;
                }
            }
            Ok(SnapshotCommand::Close(reply)) => {
                let result = map_db(db.execute_batch("ROLLBACK"));
                let _ = reply.send(result);
                return;
            }
            Ok(SnapshotCommand::Cancel) => {
                let _ = db.execute_batch("ROLLBACK");
                return;
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                let _ = db.execute_batch("ROLLBACK");
                return;
            }
        }
    }
}

struct ActiveSnapshotGuard(Arc<SnapshotTracker>);

impl Drop for ActiveSnapshotGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}

fn open_snapshot_connection(
    store: &Store,
    deadline: Instant,
    cancellation: &InspectionCancellation,
) -> Result<Connection, InspectionError> {
    snapshot_remaining(deadline, cancellation)?;
    let db = map_snapshot_db(
        deadline,
        cancellation,
        Connection::open_with_flags(
            store.database_path(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ),
    )?;

    configure_snapshot_connection(&db, deadline, cancellation)?;
    // Validate a real read before BEGIN. If database startup is locked until
    // the deadline, the worker must exit without opening a late transaction.
    read_schema_version_until_deadline(&db, deadline, cancellation)?;
    snapshot_remaining(deadline, cancellation)?;
    map_snapshot_db(
        deadline,
        cancellation,
        db.execute_batch("BEGIN DEFERRED TRANSACTION"),
    )?;
    if let Err(error) = snapshot_remaining(deadline, cancellation) {
        let _ = db.execute_batch("ROLLBACK");
        return Err(error);
    }
    // BEGIN DEFERRED alone does not establish a read snapshot. This read does,
    // so mutations after snapshot() returns cannot enter later pages.
    if let Err(error) = read_schema_version_until_deadline(&db, deadline, cancellation) {
        let _ = db.execute_batch("ROLLBACK");
        return Err(error);
    }
    Ok(db)
}

fn configure_snapshot_connection(
    db: &Connection,
    deadline: Instant,
    cancellation: &InspectionCancellation,
) -> Result<(), InspectionError> {
    let remaining = snapshot_remaining(deadline, cancellation)?;
    map_snapshot_db(deadline, cancellation, db.busy_timeout(remaining))?;
    snapshot_remaining(deadline, cancellation)?;
    map_snapshot_db(
        deadline,
        cancellation,
        db.pragma_update(None, "query_only", true),
    )?;
    snapshot_remaining(deadline, cancellation)?;
    Ok(())
}

fn read_schema_version_until_deadline(
    db: &Connection,
    deadline: Instant,
    cancellation: &InspectionCancellation,
) -> Result<(), InspectionError> {
    loop {
        let remaining = snapshot_remaining(deadline, cancellation)?;
        map_snapshot_db(
            deadline,
            cancellation,
            db.busy_timeout(remaining.min(SQLITE_BUSY_POLL)),
        )?;
        let result = db.query_row("SELECT version FROM schema_version LIMIT 1", [], |_| Ok(()));
        match result {
            Ok(()) => return snapshot_remaining(deadline, cancellation).map(|_| ()),
            Err(error)
                if matches!(
                    error.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
                ) =>
            {
                snapshot_remaining(deadline, cancellation)?;
            }
            Err(error) => return map_snapshot_db(deadline, cancellation, Err(error)),
        }
    }
}

fn deadline_remaining(deadline: Instant) -> Result<Duration, InspectionError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(InspectionError::SnapshotExpired)
    } else {
        Ok(remaining)
    }
}

fn snapshot_remaining(
    deadline: Instant,
    cancellation: &InspectionCancellation,
) -> Result<Duration, InspectionError> {
    if cancellation.is_cancelled() {
        Err(InspectionError::SnapshotExpired)
    } else {
        deadline_remaining(deadline)
    }
}

fn map_snapshot_db<T>(
    deadline: Instant,
    cancellation: &InspectionCancellation,
    result: rusqlite::Result<T>,
) -> Result<T, InspectionError> {
    match result {
        Ok(_value) if cancellation.is_cancelled() || Instant::now() >= deadline => {
            Err(InspectionError::SnapshotExpired)
        }
        Ok(value) => Ok(value),
        Err(_) if cancellation.is_cancelled() || Instant::now() >= deadline => {
            Err(InspectionError::SnapshotExpired)
        }
        Err(_) => Err(InspectionError::Store),
    }
}

fn map_snapshot_query<T>(
    deadline: Instant,
    cancellation: &InspectionCancellation,
    result: Result<T, InspectionError>,
) -> Result<T, InspectionError> {
    match result {
        Ok(_value) if cancellation.is_cancelled() || Instant::now() >= deadline => {
            Err(InspectionError::SnapshotExpired)
        }
        Ok(value) => Ok(value),
        Err(_) if cancellation.is_cancelled() || Instant::now() >= deadline => {
            Err(InspectionError::SnapshotExpired)
        }
        Err(error) => Err(error),
    }
}

fn read_snapshot_page(
    db: &Connection,
    query: &InspectQuery,
    page_size: usize,
    offset: &mut u64,
    deadline: Instant,
    cancellation: &InspectionCancellation,
) -> Result<InspectionResult, InspectionError> {
    let remaining = snapshot_remaining(deadline, cancellation)?;
    map_snapshot_db(deadline, cancellation, db.busy_timeout(remaining))?;
    let scope = CursorScope::from(query);
    let mut items = map_snapshot_query(
        deadline,
        cancellation,
        (|| match query.clone() {
            InspectQuery::Dialogs => query_dialogs(db, *offset, page_size + 1),
            InspectQuery::History(dialog_id) => {
                query_history(db, dialog_id, *offset, page_size + 1)
            }
            InspectQuery::Jobs => query_jobs(db, *offset, page_size + 1),
            InspectQuery::Job(job_id) => {
                let rows = query_one_job(db, job_id, *offset)?;
                if *offset == 0 && rows.is_empty() {
                    return Err(StoreError::NotFound.into());
                }
                Ok(rows)
            }
            InspectQuery::Runs(job_id) => query_runs(db, job_id, *offset, page_size + 1),
            InspectQuery::Audit => query_audit(db, *offset, page_size + 1),
            InspectQuery::Dump => query_dump(db, *offset, page_size + 1),
        })(),
    )?;
    let has_more = items.len() > page_size;
    if has_more {
        items.truncate(page_size);
    }
    *offset = offset
        .checked_add(items.len() as u64)
        .ok_or(InspectionError::InvalidCursor)?;
    Ok(InspectionResult {
        items,
        next_cursor: has_more.then_some(InspectionCursor {
            offset: *offset,
            scope,
        }),
        complete: !has_more,
    })
}

fn write_json_line<W: Write, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), InspectionError> {
    serde_json::to_writer(&mut *writer, value).map_err(|_| InspectionError::Serialization)?;
    writer.write_all(b"\n").map_err(|_| InspectionError::Io)
}

fn write_bounded_json_line<W: Write, T: Serialize>(
    writer: &mut W,
    value: &T,
    total: &mut usize,
    maximum: usize,
) -> Result<(), InspectionError> {
    let mut counter = CountingWriter::default();
    serde_json::to_writer(&mut counter, value).map_err(|_| InspectionError::Serialization)?;
    let required = counter
        .bytes
        .checked_add(1)
        .and_then(|line| total.checked_add(line))
        .ok_or(InspectionError::OutputTooLarge)?;
    if required > maximum {
        return Err(InspectionError::OutputTooLarge);
    }
    serde_json::to_writer(&mut *writer, value).map_err(|_| InspectionError::Io)?;
    writer.write_all(b"\n").map_err(|_| InspectionError::Io)?;
    *total = required;
    Ok(())
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("count_overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct DigestWriter<'a, W> {
    inner: &'a mut W,
    digest: Sha256,
    total: u64,
}

impl<'a, W> DigestWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            total: 0,
        }
    }

    fn finish(self) -> (u64, String) {
        (self.total, format!("{:x}", self.digest.finalize()))
    }
}

impl<W: Write> Write for DigestWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.total = self.total.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn map_db<T>(result: rusqlite::Result<T>) -> Result<T, InspectionError> {
    result.map_err(|_| InspectionError::Store)
}

fn limit_offset(limit: usize, offset: u64) -> Result<(i64, i64), InspectionError> {
    let limit = i64::try_from(limit).map_err(|_| InspectionError::InvalidPageSize)?;
    let offset = i64::try_from(offset).map_err(|_| InspectionError::InvalidCursor)?;
    Ok((limit, offset))
}

fn query_dialogs(
    db: &Connection,
    offset: u64,
    limit: usize,
) -> Result<Vec<Value>, InspectionError> {
    let (limit, offset) = limit_offset(limit, offset)?;
    let mut statement = map_db(db.prepare(
        "SELECT id,title,created_at,updated_at FROM dialogs ORDER BY id LIMIT ?1 OFFSET ?2",
    ))?;
    let rows = map_db(statement.query_map(params![limit, offset], |row| {
        Ok(json!({
            "record_type": "dialog",
            "id": row.get::<_, i64>(0)?,
            "title": row.get::<_, String>(1)?,
            "created_at": row.get::<_, String>(2)?,
            "updated_at": row.get::<_, String>(3)?,
        }))
    }))?;
    map_db(rows.collect())
}

fn query_history(
    db: &Connection,
    dialog_id: Option<DialogId>,
    offset: u64,
    limit: usize,
) -> Result<Vec<Value>, InspectionError> {
    if let Some(id) = dialog_id {
        let exists: bool = map_db(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM dialogs WHERE id=?1)",
            [id.get()],
            |row| row.get(0),
        ))?;
        if !exists {
            return Err(StoreError::NotFound.into());
        }
    }
    let (limit, offset) = limit_offset(limit, offset)?;
    let id = dialog_id.map(DialogId::get);
    let mut statement = map_db(db.prepare(
        "SELECT kind,id FROM (
           SELECT 'turn' kind,t.id id,t.started_at sort_at,0 rank FROM turns t
             WHERE (?1 IS NULL OR t.dialog_id=?1)
           UNION ALL
           SELECT 'message',m.id,m.created_at,1 FROM messages m
             WHERE (?1 IS NULL OR m.dialog_id=?1)
           UNION ALL
           SELECT 'service_event',r.id,r.started_at,2 FROM cron_runs r
             JOIN cron_jobs j ON j.id=r.job_id
             WHERE (?1 IS NULL OR j.source_dialog_id=?1)
         ) ORDER BY sort_at,rank,id LIMIT ?2 OFFSET ?3",
    ))?;
    let keys = map_db(statement.query_map(params![id, limit, offset], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }))?;
    let keys = map_db(keys.collect::<rusqlite::Result<Vec<_>>>())?;
    keys.into_iter()
        .map(|(kind, id)| match kind.as_str() {
            "turn" => load_turn(db, id),
            "message" => load_message(db, id),
            "service_event" => load_service_event(db, id),
            _ => Err(InspectionError::Store),
        })
        .collect()
}

fn query_jobs(db: &Connection, offset: u64, limit: usize) -> Result<Vec<Value>, InspectionError> {
    let (limit, offset) = limit_offset(limit, offset)?;
    let mut statement =
        map_db(db.prepare("SELECT id FROM cron_jobs ORDER BY created_at,id LIMIT ?1 OFFSET ?2"))?;
    let ids = map_db(statement.query_map(params![limit, offset], |row| row.get::<_, String>(0)))?;
    map_db(ids.collect::<rusqlite::Result<Vec<_>>>())?
        .into_iter()
        .map(|id| load_job(db, &id))
        .collect()
}

fn query_one_job(
    db: &Connection,
    job_id: JobId,
    offset: u64,
) -> Result<Vec<Value>, InspectionError> {
    if offset > 0 {
        return Ok(Vec::new());
    }
    let id = job_id.to_string();
    let exists: bool = map_db(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM cron_jobs WHERE id=?1)",
        [&id],
        |row| row.get(0),
    ))?;
    if exists {
        Ok(vec![load_job(db, &id)?])
    } else {
        Ok(Vec::new())
    }
}

fn query_runs(
    db: &Connection,
    job_id: JobId,
    offset: u64,
    limit: usize,
) -> Result<Vec<Value>, InspectionError> {
    let exists: bool = map_db(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM cron_jobs WHERE id=?1)",
        [job_id.to_string()],
        |row| row.get(0),
    ))?;
    if !exists {
        return Err(StoreError::NotFound.into());
    }
    let (limit, offset) = limit_offset(limit, offset)?;
    let mut statement = map_db(
        db.prepare("SELECT id FROM cron_runs WHERE job_id=?1 ORDER BY id LIMIT ?2 OFFSET ?3"),
    )?;
    let ids = map_db(
        statement.query_map(params![job_id.to_string(), limit, offset], |row| {
            row.get::<_, i64>(0)
        }),
    )?;
    map_db(ids.collect::<rusqlite::Result<Vec<_>>>())?
        .into_iter()
        .map(|id| load_run(db, id))
        .collect()
}

fn query_audit(db: &Connection, offset: u64, limit: usize) -> Result<Vec<Value>, InspectionError> {
    let (limit, offset) = limit_offset(limit, offset)?;
    let mut statement =
        map_db(db.prepare("SELECT id FROM tool_runs ORDER BY id LIMIT ?1 OFFSET ?2"))?;
    let ids = map_db(statement.query_map(params![limit, offset], |row| row.get::<_, i64>(0)))?;
    map_db(ids.collect::<rusqlite::Result<Vec<_>>>())?
        .into_iter()
        .map(|id| load_tool_run(db, id, true))
        .collect()
}

fn query_dump(db: &Connection, offset: u64, limit: usize) -> Result<Vec<Value>, InspectionError> {
    let (limit, offset) = limit_offset(limit, offset)?;
    let mut statement = map_db(db.prepare(
        "SELECT kind,numeric_id,text_id FROM (
           SELECT 'dialog' kind,id numeric_id,NULL text_id,0 rank FROM dialogs
           UNION ALL SELECT 'turn',id,NULL,1 FROM turns
           UNION ALL SELECT 'message',id,NULL,2 FROM messages
           UNION ALL SELECT 'job',NULL,id,3 FROM cron_jobs
           UNION ALL SELECT 'run',id,NULL,4 FROM cron_runs
           UNION ALL SELECT 'tool_run',id,NULL,5 FROM tool_runs
         ) ORDER BY rank,numeric_id,text_id LIMIT ?1 OFFSET ?2",
    ))?;
    let keys = map_db(statement.query_map(params![limit, offset], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<i64>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    }))?;
    let keys = map_db(keys.collect::<rusqlite::Result<Vec<_>>>())?;
    keys.into_iter()
        .map(|(kind, numeric_id, text_id)| match kind.as_str() {
            "dialog" => load_dialog(db, numeric_id.ok_or(InspectionError::Store)?),
            "turn" => load_turn(db, numeric_id.ok_or(InspectionError::Store)?),
            "message" => load_message(db, numeric_id.ok_or(InspectionError::Store)?),
            "job" => load_job(db, text_id.as_deref().ok_or(InspectionError::Store)?),
            "run" => load_run(db, numeric_id.ok_or(InspectionError::Store)?),
            "tool_run" => load_tool_run(db, numeric_id.ok_or(InspectionError::Store)?, false),
            _ => Err(InspectionError::Store),
        })
        .collect()
}

fn load_dialog(db: &Connection, id: i64) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT id,title,created_at,updated_at FROM dialogs WHERE id=?1",
        [id],
        |row| {
            Ok(json!({
                "record_type": "dialog",
                "id": row.get::<_, i64>(0)?,
                "title": row.get::<_, String>(1)?,
                "created_at": row.get::<_, String>(2)?,
                "updated_at": row.get::<_, String>(3)?,
            }))
        },
    ))
}

fn load_turn(db: &Connection, id: i64) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT id,dialog_id,status,safe_error_code,started_at,finished_at FROM turns WHERE id=?1",
        [id],
        |row| {
            Ok(json!({
                "record_type": "turn",
                "id": row.get::<_, i64>(0)?,
                "dialog_id": row.get::<_, i64>(1)?,
                "status": row.get::<_, String>(2)?,
                "safe_error_code": row.get::<_, Option<String>>(3)?,
                "started_at": row.get::<_, String>(4)?,
                "finished_at": row.get::<_, Option<String>>(5)?,
            }))
        },
    ))
}

fn load_message(db: &Connection, id: i64) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT id,dialog_id,turn_id,role,content,created_at FROM messages WHERE id=?1",
        [id],
        |row| {
            Ok(json!({
                "record_type": "message",
                "id": row.get::<_, i64>(0)?,
                "dialog_id": row.get::<_, i64>(1)?,
                "turn_id": row.get::<_, i64>(2)?,
                "role": row.get::<_, String>(3)?,
                "content": row.get::<_, String>(4)?,
                "created_at": row.get::<_, String>(5)?,
            }))
        },
    ))
}

fn load_job(db: &Connection, id: &str) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT id,source_dialog_id,name,schedule_kind,schedule_value,timezone,prompt,desired_state,sync_state,safe_sync_error_code,created_at,updated_at FROM cron_jobs WHERE id=?1",
        [id],
        |row| {
            Ok(json!({
                "record_type": "job",
                "id": row.get::<_, String>(0)?,
                "source_dialog_id": row.get::<_, Option<i64>>(1)?,
                "name": row.get::<_, String>(2)?,
                "schedule": {
                    "kind": row.get::<_, String>(3)?,
                    "value": row.get::<_, String>(4)?,
                    "timezone": row.get::<_, String>(5)?,
                },
                "prompt": row.get::<_, String>(6)?,
                "desired_state": row.get::<_, String>(7)?,
                "sync_state": row.get::<_, String>(8)?,
                "safe_sync_error_code": row.get::<_, Option<String>>(9)?,
                "created_at": row.get::<_, String>(10)?,
                "updated_at": row.get::<_, String>(11)?,
            }))
        },
    ))
}

fn load_run(db: &Connection, id: i64) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT id,job_id,scheduled_for,status,result,safe_error_code,started_at,finished_at FROM cron_runs WHERE id=?1",
        [id],
        |row| {
            Ok(json!({
                "record_type": "run",
                "id": row.get::<_, i64>(0)?,
                "job_id": row.get::<_, String>(1)?,
                "scheduled_for": row.get::<_, String>(2)?,
                "status": row.get::<_, String>(3)?,
                "result": row.get::<_, Option<String>>(4)?,
                "safe_error_code": row.get::<_, Option<String>>(5)?,
                "started_at": row.get::<_, String>(6)?,
                "finished_at": row.get::<_, Option<String>>(7)?,
            }))
        },
    ))
}

fn load_tool_run(db: &Connection, id: i64, for_audit: bool) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT id,owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,safe_error_code,started_at,finished_at,arguments FROM tool_runs WHERE id=?1",
        [id],
        |row| {
            let mut record = json!({
                "record_type": "tool_run",
                "id": row.get::<_, i64>(0)?,
                "owner": {
                    "kind": row.get::<_, String>(1)?,
                    "id": row.get::<_, i64>(2)?,
                },
                "call_id": row.get::<_, String>(3)?,
                "server_name": row.get::<_, String>(4)?,
                "tool_name": row.get::<_, String>(5)?,
                "read_only": row.get::<_, bool>(6)?,
                "status": row.get::<_, String>(7)?,
                "safe_error_code": row.get::<_, Option<String>>(8)?,
                "started_at": row.get::<_, String>(9)?,
                "finished_at": row.get::<_, Option<String>>(10)?,
            });
            if for_audit {
                record.as_object_mut().unwrap().remove("call_id");
                let arguments: Option<String> = row.get(11)?;
                let arguments = arguments
                    .map(|raw| serde_json::from_str::<Value>(&raw).map_err(|_| rusqlite::Error::InvalidQuery))
                    .transpose()?;
                record["arguments"] = arguments.map_or(Value::Null, |value| value);
            }
            Ok(record)
        },
    ))
}

fn load_service_event(db: &Connection, id: i64) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT r.id,r.job_id,j.name,j.source_dialog_id,r.scheduled_for,r.status,r.result,r.safe_error_code,r.started_at,r.finished_at FROM cron_runs r JOIN cron_jobs j ON j.id=r.job_id WHERE r.id=?1",
        [id],
        |row| {
            Ok(json!({
                "record_type": "service_event",
                "run_id": row.get::<_, i64>(0)?,
                "job_id": row.get::<_, String>(1)?,
                "job_name": row.get::<_, String>(2)?,
                "dialog_id": row.get::<_, Option<i64>>(3)?,
                "scheduled_for": row.get::<_, String>(4)?,
                "status": row.get::<_, String>(5)?,
                "result": row.get::<_, Option<String>>(6)?,
                "safe_error_code": row.get::<_, Option<String>>(7)?,
                "started_at": row.get::<_, String>(8)?,
                "finished_at": row.get::<_, Option<String>>(9)?,
            }))
        },
    ))
}

#[derive(Clone, Copy, Debug)]
pub struct DbShellLimits {
    pub max_sql_bytes: usize,
    pub max_rows: usize,
    pub max_columns: usize,
    pub max_cell_bytes: usize,
    pub max_output_bytes: usize,
    pub max_query_time: Duration,
}

impl Default for DbShellLimits {
    fn default() -> Self {
        Self {
            max_sql_bytes: 65_536,
            max_rows: 1_000,
            max_columns: 128,
            max_cell_bytes: 262_144,
            max_output_bytes: 8_388_608,
            max_query_time: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadonlyDbShell {
    path: PathBuf,
    limits: DbShellLimits,
}

impl ReadonlyDbShell {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            limits: DbShellLimits::default(),
        }
    }

    pub fn with_limits(
        path: impl AsRef<Path>,
        limits: DbShellLimits,
    ) -> Result<Self, InspectionError> {
        if limits.max_sql_bytes == 0
            || limits.max_rows == 0
            || limits.max_columns == 0
            || limits.max_cell_bytes == 0
            || limits.max_output_bytes == 0
            || limits.max_query_time.is_zero()
            || limits.max_sql_bytes > HARD_MAX_SQL_BYTES
            || limits.max_rows > HARD_MAX_ROWS
            || limits.max_columns > HARD_MAX_COLUMNS
            || limits.max_cell_bytes > HARD_MAX_CELL_BYTES
            || limits.max_output_bytes > HARD_MAX_OUTPUT_BYTES
            || limits.max_query_time > HARD_MAX_QUERY_TIME
        {
            return Err(InspectionError::ResourceLimit);
        }
        Ok(Self {
            path: path.as_ref().to_owned(),
            limits,
        })
    }

    pub fn run<R: BufRead, W: Write>(
        &self,
        mut input: R,
        mut output: W,
    ) -> Result<(), InspectionError> {
        if self.path.as_os_str().is_empty() || self.path == Path::new(":memory:") {
            return Err(InspectionError::InvalidDatabasePath);
        }
        let db = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|_| InspectionError::DatabaseOpen)?;
        db.pragma_update(None, "query_only", true)
            .map_err(|_| InspectionError::DatabaseOpen)?;
        db.busy_timeout(self.limits.max_query_time)
            .map_err(|_| InspectionError::DatabaseOpen)?;
        // Force the trusted Day 18 schema to be parsed before lowering limits;
        // otherwise a deliberately tiny user result limit can reject SQLite's
        // own schema rows rather than the operator query.
        db.query_row("SELECT version FROM schema_version LIMIT 1", [], |_| Ok(()))
            .map_err(|_| InspectionError::DatabaseOpen)?;
        self.apply_native_limits(&db)?;
        db.authorizer(Some(authorize_readonly))
            .map_err(|_| InspectionError::DatabaseOpen)?;

        let mut output_bytes = 0_usize;
        while let Some(line) = read_bounded_line(&mut input, self.limits.max_sql_bytes)? {
            let sql = line.trim();
            if sql.is_empty() {
                continue;
            }
            validate_sql(sql)?;
            self.execute_one(&db, sql, &mut output, &mut output_bytes)?;
        }
        Ok(())
    }

    fn apply_native_limits(&self, db: &Connection) -> Result<(), InspectionError> {
        let length = i32::try_from(self.limits.max_cell_bytes)
            .map_err(|_| InspectionError::ResourceLimit)?;
        let sql_length =
            i32::try_from(self.limits.max_sql_bytes).map_err(|_| InspectionError::ResourceLimit)?;
        let columns =
            i32::try_from(self.limits.max_columns).map_err(|_| InspectionError::ResourceLimit)?;
        for (kind, value) in [
            (Limit::SQLITE_LIMIT_LENGTH, length),
            (Limit::SQLITE_LIMIT_SQL_LENGTH, sql_length),
            (Limit::SQLITE_LIMIT_COLUMN, columns),
            (Limit::SQLITE_LIMIT_ATTACHED, 0),
            (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 0),
        ] {
            db.set_limit(kind, value)
                .map_err(|_| InspectionError::DatabaseOpen)?;
        }
        Ok(())
    }

    fn execute_one<W: Write>(
        &self,
        db: &Connection,
        sql: &str,
        output: &mut W,
        output_bytes: &mut usize,
    ) -> Result<(), InspectionError> {
        let mut statement = match db.prepare(sql) {
            Ok(statement) => statement,
            Err(rusqlite::Error::MultipleStatement) => {
                return Err(InspectionError::MultipleStatements);
            }
            Err(error)
                if error.sqlite_error_code()
                    == Some(rusqlite::ErrorCode::AuthorizationForStatementDenied) =>
            {
                return Err(InspectionError::RejectedQuery);
            }
            Err(error) if is_resource_limit(&error) => return Err(InspectionError::ResourceLimit),
            Err(_) => return Err(InspectionError::Query),
        };
        if !statement.readonly() {
            return Err(InspectionError::RejectedQuery);
        }
        if statement.parameter_count() != 0 {
            return Err(InspectionError::ParametersNotAllowed);
        }
        let column_count = statement.column_count();
        if column_count > self.limits.max_columns {
            return Err(InspectionError::TooManyColumns);
        }
        let columns = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        write_bounded_json_line(
            output,
            &json!({ "columns": columns }),
            output_bytes,
            self.limits.max_output_bytes,
        )?;

        let timed_out = Arc::new(AtomicBool::new(false));
        let timed_out_hook = timed_out.clone();
        let started = Instant::now();
        let limit = self.limits.max_query_time;
        db.progress_handler(
            100,
            Some(move || {
                let expired = started.elapsed() >= limit;
                if expired {
                    timed_out_hook.store(true, Ordering::Relaxed);
                }
                expired
            }),
        )
        .map_err(|_| InspectionError::Query)?;

        let result = (|| {
            let mut rows = statement.query([]).map_err(map_query_error)?;
            let mut count = 0_usize;
            let mut truncated = false;
            loop {
                let row = match rows.next() {
                    Ok(Some(row)) => row,
                    Ok(None) => break,
                    Err(_) if timed_out.load(Ordering::Relaxed) => {
                        return Err(InspectionError::QueryTimedOut);
                    }
                    Err(error) => return Err(map_query_error(error)),
                };
                if count == self.limits.max_rows {
                    truncated = true;
                    break;
                }
                let mut cells = Vec::with_capacity(column_count);
                for index in 0..column_count {
                    let value = row.get_ref(index).map_err(map_query_error)?;
                    cells.push(cell_value(value, self.limits.max_cell_bytes)?);
                }
                write_bounded_json_line(
                    output,
                    &json!({ "row": cells }),
                    output_bytes,
                    self.limits.max_output_bytes,
                )?;
                count += 1;
            }
            write_bounded_json_line(
                output,
                &json!({ "rows": count, "truncated": truncated }),
                output_bytes,
                self.limits.max_output_bytes,
            )?;
            Ok(())
        })();
        drop(statement);
        db.progress_handler(0, None::<fn() -> bool>)
            .map_err(|_| InspectionError::Query)?;
        result
    }
}

fn authorize_readonly(context: AuthContext<'_>) -> Authorization {
    match context.action {
        AuthAction::Select | AuthAction::Read { .. } | AuthAction::Recursive => {
            Authorization::Allow
        }
        AuthAction::Pragma { pragma_name, .. } if safe_pragma_name(pragma_name) => {
            Authorization::Allow
        }
        AuthAction::Function { function_name } if safe_function(function_name) => {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }
}

fn is_resource_limit(error: &rusqlite::Error) -> bool {
    error.sqlite_error_code() == Some(rusqlite::ErrorCode::TooBig)
}

fn map_query_error(error: rusqlite::Error) -> InspectionError {
    if is_resource_limit(&error) {
        InspectionError::ResourceLimit
    } else {
        InspectionError::Query
    }
}

fn safe_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "abs"
            | "avg"
            | "coalesce"
            | "count"
            | "date"
            | "datetime"
            | "group_concat"
            | "hex"
            | "ifnull"
            | "instr"
            | "json"
            | "json_array"
            | "json_extract"
            | "json_object"
            | "julianday"
            | "length"
            | "likely"
            | "lower"
            | "ltrim"
            | "max"
            | "min"
            | "nullif"
            | "printf"
            | "quote"
            | "replace"
            | "round"
            | "rtrim"
            | "strftime"
            | "substr"
            | "substring"
            | "sum"
            | "time"
            | "total"
            | "trim"
            | "typeof"
            | "unicode"
            | "unixepoch"
            | "unlikely"
            | "upper"
            | "zeroblob"
    )
}

fn safe_pragma_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "database_list"
            | "foreign_key_list"
            | "index_info"
            | "index_list"
            | "query_only"
            | "schema_version"
            | "table_info"
            | "table_xinfo"
            | "user_version"
    )
}

fn validate_sql(sql: &str) -> Result<(), InspectionError> {
    let normalized = strip_optional_final_semicolon(sql)?;
    let first = normalized
        .split_whitespace()
        .next()
        .ok_or(InspectionError::RejectedQuery)?
        .to_ascii_uppercase();
    match first.as_str() {
        "SELECT" | "WITH" | "EXPLAIN" => Ok(()),
        "PRAGMA" if validate_pragma(normalized) => Ok(()),
        _ => Err(InspectionError::RejectedQuery),
    }
}

fn strip_optional_final_semicolon(sql: &str) -> Result<&str, InspectionError> {
    let bytes = sql.as_bytes();
    let mut quote = None;
    let mut semicolon = None;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(end) = quote {
            if byte == end {
                if end != b']' && bytes.get(index + 1) == Some(&end) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'\'' | b'"' | b'`' => quote = Some(byte),
            b'[' => quote = Some(b']'),
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                return Err(InspectionError::RejectedQuery);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                return Err(InspectionError::RejectedQuery);
            }
            b';' if semicolon.replace(index).is_some() => {
                return Err(InspectionError::MultipleStatements);
            }
            _ => {}
        }
        index += 1;
    }
    if quote.is_some() {
        return Err(InspectionError::Query);
    }
    if let Some(index) = semicolon {
        if !sql[index + 1..].trim().is_empty() {
            return Err(InspectionError::MultipleStatements);
        }
        Ok(sql[..index].trim_end())
    } else {
        Ok(sql)
    }
}

fn validate_pragma(sql: &str) -> bool {
    let Some(keyword_end) = sql.find(char::is_whitespace) else {
        return false;
    };
    if !sql[..keyword_end].eq_ignore_ascii_case("pragma") {
        return false;
    }
    let rest = sql[keyword_end..].trim();
    if rest.contains('=') || rest.contains(char::is_whitespace) {
        return false;
    }
    let (name, argument) = if let Some(open) = rest.find('(') {
        if !rest.ends_with(')') {
            return false;
        }
        (&rest[..open], Some(&rest[open + 1..rest.len() - 1]))
    } else {
        (rest, None)
    };
    let name = name.rsplit('.').next().unwrap_or(name);
    if !safe_pragma_name(name) {
        return false;
    }
    match name.to_ascii_lowercase().as_str() {
        "table_info" | "table_xinfo" | "index_list" | "index_info" | "foreign_key_list" => {
            argument.is_some_and(valid_identifier)
        }
        _ => argument.is_none(),
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Option<String>, InspectionError> {
    let mut bytes = Vec::new();
    loop {
        let buffer = reader.fill_buf().map_err(|_| InspectionError::Io)?;
        if buffer.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(buffer.len(), |index| index + 1);
        if bytes.len().saturating_add(take) > max_bytes {
            reader.consume(take);
            while newline.is_none() {
                let buffer = reader.fill_buf().map_err(|_| InspectionError::Io)?;
                if buffer.is_empty() {
                    break;
                }
                let next = buffer.iter().position(|byte| *byte == b'\n');
                let take = next.map_or(buffer.len(), |index| index + 1);
                reader.consume(take);
                if next.is_some() {
                    break;
                }
            }
            return Err(InspectionError::InputTooLarge);
        }
        bytes.extend_from_slice(&buffer[..take]);
        reader.consume(take);
        if newline.is_some() {
            break;
        }
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| InspectionError::RejectedQuery)
}

fn cell_value(value: ValueRef<'_>, max_bytes: usize) -> Result<Value, InspectionError> {
    match value {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(value) => Ok(json!(value)),
        ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or(InspectionError::Query),
        ValueRef::Text(value) => {
            if value.len() > max_bytes {
                return Err(InspectionError::CellTooLarge);
            }
            let value = std::str::from_utf8(value).map_err(|_| InspectionError::Query)?;
            Ok(json!(value))
        }
        ValueRef::Blob(value) => {
            if value.len() > max_bytes {
                return Err(InspectionError::CellTooLarge);
            }
            Ok(json!({
                "blob_base64": base64::engine::general_purpose::STANDARD.encode(value)
            }))
        }
    }
}

#[cfg(test)]
mod snapshot_lifecycle_tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn expired_next_page_joins_finished_worker_before_returning() {
        let temporary_root = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        let directory = tempfile::tempdir_in(temporary_root).unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = Store::open(directory.path().join("agent.sqlite3")).unwrap();
        for index in 0..3 {
            store.create_dialog(&format!("dialog-{index}")).unwrap();
        }
        let service =
            InspectionService::with_snapshot_timeout(store, 1, Duration::from_millis(20)).unwrap();
        let mut snapshot = service.snapshot(InspectQuery::Dialogs).unwrap();
        assert!(!snapshot.next_page().unwrap().unwrap().complete);

        assert!(service.wait_for_snapshot_idle(Duration::from_secs(5)));
        assert_eq!(
            snapshot.next_page().unwrap_err(),
            InspectionError::SnapshotExpired
        );
        assert!(
            snapshot.worker.is_none(),
            "expired next_page must join instead of retaining a detached handle"
        );
        assert_eq!(service.active_snapshots(), 0);
    }
}
