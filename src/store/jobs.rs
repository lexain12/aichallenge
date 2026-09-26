use std::str::FromStr;
use std::time::{Duration, Instant};

use chrono::{DateTime, Timelike, Utc};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{SafeErrorCode, Store, StoreError, dialog_exists, execute_one, now};
use crate::domain::{CronRunStatus, DialogId, JobDesiredState, JobId, JobSyncState, RunId};
use crate::scheduler::ScheduleSpec;

const MAX_JOB_TEXT_BYTES: usize = 262_144;
const MAX_JOB_NAME_BYTES: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobCreate {
    pub source_dialog_id: DialogId,
    pub name: String,
    pub schedule: ScheduleSpec,
    pub prompt: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronJob {
    pub id: JobId,
    pub source_dialog_id: Option<DialogId>,
    pub name: String,
    pub schedule: ScheduleSpec,
    pub prompt: String,
    pub desired_state: JobDesiredState,
    pub sync_state: JobSyncState,
    pub safe_sync_error_code: Option<SafeErrorCode>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CronRun {
    pub id: RunId,
    pub job_id: JobId,
    pub scheduled_for: String,
    pub status: CronRunStatus,
    pub result: Option<String>,
    pub safe_error_code: Option<SafeErrorCode>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunClaim {
    Claimed(CronRunClaim),
    Skipped(CronRun),
    Inactive,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronRunClaim {
    pub run: CronRun,
    pub job: CronJob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronRunFinish {
    status: CronRunStatus,
    result: Option<String>,
    safe_error_code: Option<SafeErrorCode>,
}

impl CronRunFinish {
    pub fn completed(result: impl Into<String>) -> Self {
        Self {
            status: CronRunStatus::Completed,
            result: Some(result.into()),
            safe_error_code: None,
        }
    }

    pub fn failed(code: SafeErrorCode) -> Self {
        Self::error(CronRunStatus::Failed, code)
    }

    pub fn interrupted(code: SafeErrorCode) -> Self {
        Self::error(CronRunStatus::Interrupted, code)
    }

    pub fn timed_out() -> Self {
        Self::error(CronRunStatus::TimedOut, SafeErrorCode::TimedOut)
    }

    fn error(status: CronRunStatus, safe_error_code: SafeErrorCode) -> Self {
        Self {
            status,
            result: None,
            safe_error_code: Some(safe_error_code),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceEvent {
    pub run: CronRun,
    pub job_name: String,
}

impl Store {
    pub fn create_job(&self, create: JobCreate) -> Result<CronJob, StoreError> {
        validate_job_text(&create.name, MAX_JOB_NAME_BYTES)?;
        validate_job_text(&create.prompt, MAX_JOB_TEXT_BYTES)?;
        let schedule = create
            .schedule
            .validate_and_normalize()
            .map_err(|_| StoreError::InvalidMetadata)?;
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        dialog_exists(&tx, create.source_dialog_id)?;
        let id = JobId::new();
        let timestamp = now();
        let (kind, value, timezone) = schedule.kind_and_value();
        execute_one(
            &tx,
            "INSERT INTO cron_jobs(id,source_dialog_id,name,schedule_kind,schedule_value,timezone,prompt,desired_state,sync_state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'active','pending',?,?)",
            params![
                id.to_string(),
                create.source_dialog_id.get(),
                create.name,
                kind,
                value,
                timezone,
                create.prompt,
                timestamp,
                timestamp
            ],
        )?;
        tx.commit()?;
        self.get_job(id)
    }

    pub fn update_job(
        &self,
        id: JobId,
        name: String,
        schedule: ScheduleSpec,
        prompt: String,
    ) -> Result<CronJob, StoreError> {
        validate_job_text(&name, MAX_JOB_NAME_BYTES)?;
        validate_job_text(&prompt, MAX_JOB_TEXT_BYTES)?;
        let schedule = schedule
            .validate_and_normalize()
            .map_err(|_| StoreError::InvalidMetadata)?;
        let (kind, value, timezone) = schedule.kind_and_value();
        let db = self.connection()?;
        let changed = db.execute(
            "UPDATE cron_jobs SET name=?,schedule_kind=?,schedule_value=?,timezone=?,prompt=?,desired_state='active',sync_state='pending',safe_sync_error_code=NULL,updated_at=? WHERE id=? AND desired_state!='deleted'",
            params![name, kind, value, timezone, prompt, now(), id.to_string()],
        )?;
        if changed == 0 {
            return Err(StoreError::NotFound);
        }
        self.get_job(id)
    }

    /// Atomically applies an update only while every persisted field still
    /// matches the snapshot shown to the confirmer.
    pub fn update_job_if_unchanged(
        &self,
        expected: &CronJob,
        name: String,
        schedule: ScheduleSpec,
        prompt: String,
    ) -> Result<CronJob, StoreError> {
        validate_job_text(&name, MAX_JOB_NAME_BYTES)?;
        validate_job_text(&prompt, MAX_JOB_TEXT_BYTES)?;
        let schedule = schedule
            .validate_and_normalize()
            .map_err(|_| StoreError::InvalidMetadata)?;
        let (kind, value, timezone) = schedule.kind_and_value();
        let (expected_kind, expected_value, expected_timezone) = expected.schedule.kind_and_value();
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE cron_jobs SET name=?,schedule_kind=?,schedule_value=?,timezone=?,prompt=?,desired_state='active',sync_state='pending',safe_sync_error_code=NULL,updated_at=? WHERE id=? AND source_dialog_id IS ? AND name=? AND schedule_kind=? AND schedule_value=? AND timezone=? AND prompt=? AND desired_state=? AND sync_state=? AND safe_sync_error_code IS ? AND created_at=? AND updated_at=?",
            params![
                name,
                kind,
                value,
                timezone,
                prompt,
                now(),
                expected.id.to_string(),
                expected.source_dialog_id.map(DialogId::get),
                expected.name,
                expected_kind,
                expected_value,
                expected_timezone,
                expected.prompt,
                desired_state_str(expected.desired_state),
                sync_state_str(expected.sync_state),
                expected.safe_sync_error_code.map(SafeErrorCode::as_str),
                expected.created_at,
                expected.updated_at,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict);
        }
        tx.commit()?;
        self.get_job(expected.id)
    }

    pub fn set_job_desired_state(
        &self,
        id: JobId,
        state: JobDesiredState,
    ) -> Result<CronJob, StoreError> {
        let db = self.connection()?;
        let changed = db.execute(
            "UPDATE cron_jobs SET desired_state=?,sync_state='pending',safe_sync_error_code=NULL,updated_at=? WHERE id=? AND desired_state!='deleted'",
            params![desired_state_str(state), now(), id.to_string()],
        )?;
        if changed == 0 {
            let existing = db
                .query_row(
                    "SELECT desired_state FROM cron_jobs WHERE id=?",
                    [id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            return if existing.as_deref() == Some("deleted") && state == JobDesiredState::Deleted {
                self.get_job(id)
            } else {
                Err(StoreError::NotFound)
            };
        }
        self.get_job(id)
    }

    /// Atomically applies a desired-state change only while the row still
    /// equals the exact snapshot used for its confirmation preview.
    pub fn set_job_desired_state_if_unchanged(
        &self,
        expected: &CronJob,
        state: JobDesiredState,
    ) -> Result<CronJob, StoreError> {
        let (expected_kind, expected_value, expected_timezone) = expected.schedule.kind_and_value();
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE cron_jobs SET desired_state=?,sync_state='pending',safe_sync_error_code=NULL,updated_at=? WHERE id=? AND source_dialog_id IS ? AND name=? AND schedule_kind=? AND schedule_value=? AND timezone=? AND prompt=? AND desired_state=? AND sync_state=? AND safe_sync_error_code IS ? AND created_at=? AND updated_at=?",
            params![
                desired_state_str(state),
                now(),
                expected.id.to_string(),
                expected.source_dialog_id.map(DialogId::get),
                expected.name,
                expected_kind,
                expected_value,
                expected_timezone,
                expected.prompt,
                desired_state_str(expected.desired_state),
                sync_state_str(expected.sync_state),
                expected.safe_sync_error_code.map(SafeErrorCode::as_str),
                expected.created_at,
                expected.updated_at,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict);
        }
        tx.commit()?;
        self.get_job(expected.id)
    }

    pub fn get_job(&self, id: JobId) -> Result<CronJob, StoreError> {
        let db = self.connection()?;
        db.query_row(
            "SELECT id,source_dialog_id,name,schedule_kind,schedule_value,timezone,prompt,desired_state,sync_state,safe_sync_error_code,created_at,updated_at FROM cron_jobs WHERE id=?",
            [id.to_string()],
            decode_job,
        )
        .optional()?
        .ok_or(StoreError::NotFound)
    }

    pub fn list_jobs(&self) -> Result<Vec<CronJob>, StoreError> {
        let db = self.connection()?;
        let mut statement = db.prepare("SELECT id,source_dialog_id,name,schedule_kind,schedule_value,timezone,prompt,desired_state,sync_state,safe_sync_error_code,created_at,updated_at FROM cron_jobs ORDER BY created_at,id")?;
        Ok(statement
            .query_map([], decode_job)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_runs(&self, job_id: JobId) -> Result<Vec<CronRun>, StoreError> {
        let db = self.connection()?;
        let mut statement = db.prepare("SELECT id,job_id,scheduled_for,status,result,safe_error_code,started_at,finished_at FROM cron_runs WHERE job_id=? ORDER BY id")?;
        Ok(statement
            .query_map([job_id.to_string()], decode_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_all_runs(&self) -> Result<Vec<CronRun>, StoreError> {
        let db = self.connection()?;
        let mut statement = db.prepare("SELECT id,job_id,scheduled_for,status,result,safe_error_code,started_at,finished_at FROM cron_runs ORDER BY id")?;
        Ok(statement
            .query_map([], decode_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn service_events(&self, dialog_id: DialogId) -> Result<Vec<ServiceEvent>, StoreError> {
        let db = self.connection()?;
        dialog_exists(&db, dialog_id)?;
        let mut statement = db.prepare("SELECT r.id,r.job_id,r.scheduled_for,r.status,r.result,r.safe_error_code,r.started_at,r.finished_at,j.name FROM cron_runs r JOIN cron_jobs j ON j.id=r.job_id WHERE j.source_dialog_id=? ORDER BY r.id")?;
        Ok(statement
            .query_map([dialog_id.get()], |row| {
                Ok(ServiceEvent {
                    run: decode_run_columns(row)?,
                    job_name: row.get(8)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn claim_run(
        &self,
        job_id: JobId,
        current_time: DateTime<Utc>,
    ) -> Result<RunClaim, StoreError> {
        self.claim_run_inner(
            job_id,
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
            || current_time,
        )
    }

    pub(crate) fn claim_run_with_deadline(
        &self,
        job_id: JobId,
        deadline: Instant,
        cancellation: &CancellationToken,
        now: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<RunClaim, StoreError> {
        self.claim_run_inner(job_id, deadline, cancellation, now)
    }

    fn claim_run_inner(
        &self,
        job_id: JobId,
        deadline: Instant,
        cancellation: &CancellationToken,
        now: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<RunClaim, StoreError> {
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            self.require_runtime_owner_for_creation(tx)?;
            // Sample only while holding the write transaction. A once-at job
            // is classified against the instant at which it can commit.
            let current_time = now();
            let job = tx
                .query_row(
                    "SELECT id,source_dialog_id,name,schedule_kind,schedule_value,timezone,prompt,desired_state,sync_state,safe_sync_error_code,created_at,updated_at FROM cron_jobs WHERE id=?",
                    [job_id.to_string()],
                    decode_job,
                )
                .optional()?
                .ok_or(StoreError::NotFound)?;
            if job.desired_state != JobDesiredState::Active
                || job.sync_state != JobSyncState::Applied
            {
                return Ok(RunClaim::Inactive);
            }
            if let ScheduleSpec::OnceAt { at, .. } = job.schedule {
                let current_minute = minute_start(current_time);
                if current_minute < at {
                    return Ok(RunClaim::Inactive);
                }
                if at < current_minute {
                    insert_terminal_run(tx, job_id, at, CronRunStatus::Missed)?;
                    disable_once(tx, job_id)?;
                    return Ok(RunClaim::Inactive);
                }
            }
            let scheduled_for = match job.schedule {
                ScheduleSpec::OnceAt { at, .. } => at,
                ScheduleSpec::Cron { .. } => minute_start(current_time),
            };
            let timestamp = current_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO cron_runs(job_id,scheduled_for,status,started_at,runtime_owner_id) VALUES(?,?,'pending',?,?)",
                params![job_id.to_string(), scheduled_for.to_rfc3339_opts(chrono::SecondsFormat::Secs, true), timestamp, self.runtime_owner_id()],
            )?;
            if inserted == 0 {
                tx.execute(
                    "INSERT INTO cron_runs(job_id,scheduled_for,status,started_at,finished_at) VALUES(?,?,'skipped',?,?)",
                    params![
                        job_id.to_string(),
                        scheduled_for.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        timestamp,
                        timestamp
                    ],
                )?;
                let run = load_run(tx, tx.last_insert_rowid())?;
                return Ok(RunClaim::Skipped(run));
            }
            let run_id = tx.last_insert_rowid();
            if matches!(job.schedule, ScheduleSpec::OnceAt { .. }) {
                disable_once(tx, job_id)?;
            }
            let run = load_run(tx, run_id)?;
            Ok(RunClaim::Claimed(CronRunClaim { run, job }))
        })
    }

    pub fn finish_run(&self, id: RunId, finish: CronRunFinish) -> Result<(), StoreError> {
        self.finish_run_inner(
            id,
            finish,
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
    }

    pub(crate) fn finish_run_with_deadline(
        &self,
        id: RunId,
        finish: CronRunFinish,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), StoreError> {
        self.finish_run_inner(id, finish, deadline, cancellation)
    }

    fn finish_run_inner(
        &self,
        id: RunId,
        finish: CronRunFinish,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), StoreError> {
        if finish
            .result
            .as_ref()
            .is_some_and(|result| result.len() > MAX_JOB_TEXT_BYTES)
        {
            return Err(StoreError::InvalidMetadata);
        }
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            let (old_status, old_result, old_code, runtime_owner_id) = tx
                .query_row(
                    "SELECT status,result,safe_error_code,runtime_owner_id FROM cron_runs WHERE id=?",
                    [id.get()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .optional()?
                .ok_or(StoreError::NotFound)?;
            let status = run_status_str(finish.status);
            let code = finish.safe_error_code.map(SafeErrorCode::as_str);
            if old_status != "pending" {
                return if old_status == status
                    && old_result == finish.result
                    && old_code.as_deref() == code
                {
                    Ok(())
                } else {
                    Err(StoreError::Conflict)
                };
            }
            self.require_runtime_owner(tx, runtime_owner_id.as_deref())?;
            execute_one(
                tx,
                "UPDATE cron_runs SET status=?,result=?,safe_error_code=?,finished_at=?,runtime_owner_id=NULL WHERE id=?",
                params![status, finish.result, code, now(), id.get()],
            )
        })
    }

    pub fn mark_missed_once_jobs(&self, current_time: DateTime<Utc>) -> Result<usize, StoreError> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let candidates = {
            let mut statement = tx.prepare("SELECT id,schedule_value FROM cron_jobs WHERE schedule_kind='once_at' AND desired_state='active'")?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut changed = 0;
        for (id, at) in candidates {
            let at = DateTime::parse_from_rfc3339(&at)
                .map_err(|_| StoreError::Database)?
                .with_timezone(&Utc);
            if at < minute_start(current_time) {
                let job_id = JobId::from_str(&id).map_err(|_| StoreError::Database)?;
                insert_terminal_run(&tx, job_id, at, CronRunStatus::Missed)?;
                disable_once(&tx, job_id)?;
                changed += 1;
            }
        }
        tx.commit()?;
        Ok(changed)
    }

    pub fn mark_job_sync_applied(&self, id: JobId) -> Result<(), StoreError> {
        self.mark_job_sync(id, JobSyncState::Applied)
    }

    pub fn mark_job_sync_failed(&self, id: JobId) -> Result<(), StoreError> {
        self.mark_job_sync(id, JobSyncState::Failed)
    }

    fn mark_job_sync(&self, id: JobId, state: JobSyncState) -> Result<(), StoreError> {
        let db = self.connection()?;
        let (state, code) = match state {
            JobSyncState::Applied => ("applied", None),
            JobSyncState::Failed => ("failed", Some(SafeErrorCode::InternalError.as_str())),
            JobSyncState::Pending => return Err(StoreError::InvalidMetadata),
        };
        if db.execute(
            "UPDATE cron_jobs SET sync_state=?,safe_sync_error_code=? WHERE id=?",
            params![state, code, id.to_string()],
        )? == 0
        {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }

    pub(crate) fn jobs_for_sync(&self) -> Result<Vec<CronJob>, StoreError> {
        self.list_jobs()
    }

    pub(crate) fn mark_sync_snapshot_applied(
        &self,
        snapshot: &[CronJob],
    ) -> Result<(), StoreError> {
        self.mark_sync_snapshot(snapshot, JobSyncState::Applied)
    }

    pub(crate) fn mark_sync_snapshot_failed(&self, snapshot: &[CronJob]) -> Result<(), StoreError> {
        self.mark_sync_snapshot(snapshot, JobSyncState::Failed)
    }

    fn mark_sync_snapshot(
        &self,
        snapshot: &[CronJob],
        state: JobSyncState,
    ) -> Result<(), StoreError> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for job in snapshot {
            if state == JobSyncState::Failed && job.sync_state == JobSyncState::Applied {
                continue;
            }
            let (kind, value, timezone) = job.schedule.kind_and_value();
            let (sync_state, code) = match state {
                JobSyncState::Applied => ("applied", None),
                JobSyncState::Failed => ("failed", Some(SafeErrorCode::InternalError.as_str())),
                JobSyncState::Pending => unreachable!(),
            };
            let changed = tx.execute(
                "UPDATE cron_jobs SET sync_state=?,safe_sync_error_code=? WHERE id=? AND name=? AND schedule_kind=? AND schedule_value=? AND timezone=? AND prompt=? AND desired_state=? AND updated_at=?",
                params![sync_state, code, job.id.to_string(), job.name, kind, value, timezone, job.prompt, desired_state_str(job.desired_state), job.updated_at],
            )?;
            if changed == 0 {
                let current = tx
                    .query_row(
                        "SELECT id,source_dialog_id,name,schedule_kind,schedule_value,timezone,prompt,desired_state,sync_state,safe_sync_error_code,created_at,updated_at FROM cron_jobs WHERE id=?",
                        [job.id.to_string()],
                        decode_job,
                    )
                    .optional()?;
                if current.as_ref().is_none_or(|current| current == job) {
                    return Err(StoreError::Conflict);
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
}

fn validate_job_text(value: &str, maximum: usize) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > maximum || value.contains('\0') {
        return Err(StoreError::InvalidMetadata);
    }
    Ok(())
}

fn minute_start(value: DateTime<Utc>) -> DateTime<Utc> {
    value.with_second(0).unwrap().with_nanosecond(0).unwrap()
}

fn disable_once(tx: &Transaction<'_>, id: JobId) -> Result<(), StoreError> {
    execute_one(
        tx,
        "UPDATE cron_jobs SET desired_state='disabled',sync_state='pending',safe_sync_error_code=NULL,updated_at=? WHERE id=? AND desired_state='active'",
        params![now(), id.to_string()],
    )
}

fn insert_terminal_run(
    tx: &Transaction<'_>,
    job_id: JobId,
    scheduled_for: DateTime<Utc>,
    status: CronRunStatus,
) -> Result<(), StoreError> {
    let timestamp = now();
    execute_one(
        tx,
        "INSERT INTO cron_runs(job_id,scheduled_for,status,started_at,finished_at) VALUES(?,?,?,?,?)",
        params![
            job_id.to_string(),
            scheduled_for.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            run_status_str(status),
            timestamp,
            timestamp
        ],
    )
}

fn load_run(tx: &Transaction<'_>, id: i64) -> Result<CronRun, StoreError> {
    Ok(tx.query_row(
        "SELECT id,job_id,scheduled_for,status,result,safe_error_code,started_at,finished_at FROM cron_runs WHERE id=?",
        [id],
        decode_run,
    )?)
}

fn decode_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<CronJob> {
    Ok(CronJob {
        id: JobId::from_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        source_dialog_id: row
            .get::<_, Option<i64>>(1)?
            .map(|id| DialogId::new(id).map_err(|_| rusqlite::Error::InvalidQuery))
            .transpose()?,
        name: row.get(2)?,
        schedule: ScheduleSpec::from_columns(
            &row.get::<_, String>(3)?,
            &row.get::<_, String>(4)?,
            &row.get::<_, String>(5)?,
        )?,
        prompt: row.get(6)?,
        desired_state: parse_desired_state(&row.get::<_, String>(7)?)?,
        sync_state: parse_sync_state(&row.get::<_, String>(8)?)?,
        safe_sync_error_code: row
            .get::<_, Option<String>>(9)?
            .as_deref()
            .map(SafeErrorCode::parse)
            .transpose()?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn decode_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<CronRun> {
    decode_run_columns(row)
}

fn decode_run_columns(row: &rusqlite::Row<'_>) -> rusqlite::Result<CronRun> {
    Ok(CronRun {
        id: RunId::new(row.get(0)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
        job_id: JobId::from_str(&row.get::<_, String>(1)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        scheduled_for: row.get(2)?,
        status: parse_run_status(&row.get::<_, String>(3)?)?,
        result: row.get(4)?,
        safe_error_code: row
            .get::<_, Option<String>>(5)?
            .as_deref()
            .map(SafeErrorCode::parse)
            .transpose()?,
        started_at: row.get(6)?,
        finished_at: row.get(7)?,
    })
}

fn desired_state_str(value: JobDesiredState) -> &'static str {
    match value {
        JobDesiredState::Active => "active",
        JobDesiredState::Disabled => "disabled",
        JobDesiredState::Deleted => "deleted",
    }
}

fn sync_state_str(value: JobSyncState) -> &'static str {
    match value {
        JobSyncState::Pending => "pending",
        JobSyncState::Applied => "applied",
        JobSyncState::Failed => "failed",
    }
}

fn parse_desired_state(value: &str) -> rusqlite::Result<JobDesiredState> {
    match value {
        "active" => Ok(JobDesiredState::Active),
        "disabled" => Ok(JobDesiredState::Disabled),
        "deleted" => Ok(JobDesiredState::Deleted),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn parse_sync_state(value: &str) -> rusqlite::Result<JobSyncState> {
    match value {
        "pending" => Ok(JobSyncState::Pending),
        "applied" => Ok(JobSyncState::Applied),
        "failed" => Ok(JobSyncState::Failed),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn run_status_str(value: CronRunStatus) -> &'static str {
    match value {
        CronRunStatus::Pending => "pending",
        CronRunStatus::Completed => "completed",
        CronRunStatus::Failed => "failed",
        CronRunStatus::Interrupted => "interrupted",
        CronRunStatus::TimedOut => "timed_out",
        CronRunStatus::Skipped => "skipped",
        CronRunStatus::Missed => "missed",
    }
}

fn parse_run_status(value: &str) -> rusqlite::Result<CronRunStatus> {
    match value {
        "pending" => Ok(CronRunStatus::Pending),
        "completed" => Ok(CronRunStatus::Completed),
        "failed" => Ok(CronRunStatus::Failed),
        "interrupted" => Ok(CronRunStatus::Interrupted),
        "timed_out" => Ok(CronRunStatus::TimedOut),
        "skipped" => Ok(CronRunStatus::Skipped),
        "missed" => Ok(CronRunStatus::Missed),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}
