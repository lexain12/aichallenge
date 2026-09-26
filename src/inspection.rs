//! Bounded logical inspection, streaming exports, and a restricted local SQL shell.

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use base64::Engine as _;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
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

#[derive(Clone, Debug, PartialEq)]
pub struct InspectionResult {
    pub items: Vec<Value>,
    pub next_cursor: Option<InspectionCursor>,
    pub complete: bool,
}

#[derive(Clone, Debug)]
pub struct InspectionService {
    store: Store,
    page_size: usize,
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
        }
    }

    pub fn with_page_size(store: Store, page_size: usize) -> Result<Self, InspectionError> {
        if page_size == 0 || page_size > MAX_PAGE_SIZE {
            return Err(InspectionError::InvalidPageSize);
        }
        Ok(Self { store, page_size })
    }

    pub fn inspect(&self, query: InspectQuery) -> Result<InspectionResult, InspectionError> {
        self.inspect_at(query, 0)
    }

    pub fn inspect_page(
        &self,
        query: InspectQuery,
        cursor: InspectionCursor,
    ) -> Result<InspectionResult, InspectionError> {
        if cursor.scope != CursorScope::from(&query) {
            return Err(InspectionError::InvalidCursor);
        }
        self.inspect_at(query, cursor.offset)
    }

    fn inspect_at(
        &self,
        query: InspectQuery,
        offset: u64,
    ) -> Result<InspectionResult, InspectionError> {
        let db = self.store.connection()?;
        let scope = CursorScope::from(&query);
        let mut items = match query {
            InspectQuery::Dialogs => query_dialogs(&db, offset, self.page_size + 1)?,
            InspectQuery::History(dialog_id) => {
                query_history(&db, dialog_id, offset, self.page_size + 1)?
            }
            InspectQuery::Jobs => query_jobs(&db, offset, self.page_size + 1)?,
            InspectQuery::Job(job_id) => {
                let rows = query_one_job(&db, job_id, offset)?;
                if offset == 0 && rows.is_empty() {
                    return Err(StoreError::NotFound.into());
                }
                rows
            }
            InspectQuery::Runs(job_id) => query_runs(&db, job_id, offset, self.page_size + 1)?,
            InspectQuery::Audit => query_audit(&db, offset, self.page_size + 1)?,
            InspectQuery::Dump => query_dump(&db, offset, self.page_size + 1)?,
        };
        let has_more = items.len() > self.page_size;
        if has_more {
            items.truncate(self.page_size);
        }
        let next_cursor = has_more.then_some(InspectionCursor {
            offset: offset
                .checked_add(items.len() as u64)
                .ok_or(InspectionError::InvalidCursor)?,
            scope,
        });
        Ok(InspectionResult {
            items,
            next_cursor,
            complete: !has_more,
        })
    }

    /// Writes versioned JSONL directly to the supplied local stream. No path
    /// or remote destination is accepted at this boundary.
    pub fn write_export<W: Write>(&self, writer: &mut W) -> Result<ExportSummary, InspectionError> {
        let mut db = self.store.connection()?;
        let snapshot = map_db(db.transaction())?;
        let mut writer = DigestWriter::new(writer);
        write_json_line(
            &mut writer,
            &LogicalExportV1::Header {
                format: "logical_export_v1".into(),
                version: 1,
            },
        )?;
        let mut records = 0_u64;
        let mut offset = 0_u64;
        loop {
            let mut page = query_dump(&snapshot, offset, self.page_size + 1)?;
            let has_more = page.len() > self.page_size;
            if has_more {
                page.truncate(self.page_size);
            }
            for record in page {
                write_json_line(&mut writer, &LogicalExportV1::Record { record })?;
                records += 1;
            }
            if !has_more {
                break;
            }
            offset = offset
                .checked_add(self.page_size as u64)
                .ok_or(InspectionError::InvalidCursor)?;
        }
        map_db(snapshot.commit())?;
        writer.flush().map_err(|_| InspectionError::Io)?;
        let (total_bytes, sha256) = writer.finish();
        Ok(ExportSummary {
            total_bytes,
            records,
            sha256,
        })
    }
}

fn write_json_line<W: Write, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), InspectionError> {
    serde_json::to_writer(&mut *writer, value).map_err(|_| InspectionError::Serialization)?;
    writer.write_all(b"\n").map_err(|_| InspectionError::Io)
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
        .map(|id| load_tool_run(db, id))
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
            "tool_run" => load_tool_run(db, numeric_id.ok_or(InspectionError::Store)?),
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

fn load_tool_run(db: &Connection, id: i64) -> Result<Value, InspectionError> {
    map_db(db.query_row(
        "SELECT id,owner_kind,owner_id,call_id,server_name,tool_name,read_only,status,safe_error_code,started_at,finished_at FROM tool_runs WHERE id=?1",
        [id],
        |row| {
            Ok(json!({
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
            }))
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
    pub max_query_time: Duration,
}

impl Default for DbShellLimits {
    fn default() -> Self {
        Self {
            max_sql_bytes: 65_536,
            max_rows: 1_000,
            max_columns: 128,
            max_cell_bytes: 262_144,
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
            || limits.max_query_time.is_zero()
        {
            return Err(InspectionError::RejectedQuery);
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
        db.authorizer(Some(authorize_readonly))
            .map_err(|_| InspectionError::DatabaseOpen)?;

        while let Some(line) = read_bounded_line(&mut input, self.limits.max_sql_bytes)? {
            let sql = line.trim();
            if sql.is_empty() {
                continue;
            }
            validate_sql(sql)?;
            self.execute_one(&db, sql, &mut output)?;
        }
        Ok(())
    }

    fn execute_one<W: Write>(
        &self,
        db: &Connection,
        sql: &str,
        output: &mut W,
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
        write_json_line(output, &json!({ "columns": columns }))?;

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
            let mut rows = statement.query([]).map_err(|_| InspectionError::Query)?;
            let mut count = 0_usize;
            let mut truncated = false;
            loop {
                let row = match rows.next() {
                    Ok(Some(row)) => row,
                    Ok(None) => break,
                    Err(_) if timed_out.load(Ordering::Relaxed) => {
                        return Err(InspectionError::QueryTimedOut);
                    }
                    Err(_) => return Err(InspectionError::Query),
                };
                if count == self.limits.max_rows {
                    truncated = true;
                    break;
                }
                let mut cells = Vec::with_capacity(column_count);
                for index in 0..column_count {
                    let value = row.get_ref(index).map_err(|_| InspectionError::Query)?;
                    cells.push(cell_value(value, self.limits.max_cell_bytes)?);
                }
                write_json_line(output, &json!({ "row": cells }))?;
                count += 1;
            }
            write_json_line(output, &json!({ "rows": count, "truncated": truncated }))?;
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
