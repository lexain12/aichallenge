//! Validated schedules and fail-closed reconciliation of the managed crontab block.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;

use chrono::{DateTime, Datelike, LocalResult, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use fs2::FileExt;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::store::{CronJob, Store, StoreError};

pub const MANAGED_START: &str = "# BEGIN LIGHT-AGENT MANAGED JOBS";
pub const MANAGED_END: &str = "# END LIGHT-AGENT MANAGED JOBS";
const MAX_CRON_EXPRESSION_BYTES: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleSpec {
    Cron { expression: String, timezone: Tz },
    OnceAt { at: DateTime<Utc>, timezone: Tz },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum UncheckedScheduleSpec {
    Cron { expression: String, timezone: Tz },
    OnceAt { at: DateTime<Utc>, timezone: Tz },
}

impl<'de> Deserialize<'de> for ScheduleSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let unchecked = UncheckedScheduleSpec::deserialize(deserializer)?;
        let schedule = match unchecked {
            UncheckedScheduleSpec::Cron {
                expression,
                timezone,
            } => Self::Cron {
                expression,
                timezone,
            },
            UncheckedScheduleSpec::OnceAt { at, timezone } => Self::OnceAt { at, timezone },
        };
        schedule
            .validate_and_normalize()
            .map_err(serde::de::Error::custom)
    }
}

impl ScheduleSpec {
    pub fn parse_cron(expression: &str, timezone: Tz) -> Result<Self, SchedulerError> {
        if expression.is_empty()
            || expression.len() > MAX_CRON_EXPRESSION_BYTES
            || !expression.is_ascii()
            || expression.contains(['\r', '\n', '\t'])
        {
            return Err(SchedulerError::InvalidSchedule);
        }
        let fields: Vec<_> = expression.split(' ').collect();
        if fields.len() != 5 || fields.iter().any(|field| field.is_empty()) {
            return Err(SchedulerError::InvalidSchedule);
        }
        let limits = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)];
        let normalized = fields
            .iter()
            .zip(limits)
            .map(|(field, limits)| normalize_field(field, limits))
            .collect::<Result<Vec<_>, _>>()?
            .join(" ");
        Ok(Self::Cron {
            expression: normalized,
            timezone,
        })
    }

    pub fn parse_once_at(value: &str, timezone: Tz) -> Result<Self, SchedulerError> {
        if value.len() != 16 || !value.is_ascii() {
            return Err(SchedulerError::InvalidSchedule);
        }
        let local = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M")
            .map_err(|_| SchedulerError::InvalidSchedule)?;
        let at = match timezone.from_local_datetime(&local) {
            LocalResult::Single(value) => value.with_timezone(&Utc),
            LocalResult::Ambiguous(_, _) | LocalResult::None => {
                return Err(SchedulerError::InvalidSchedule);
            }
        };
        Self::OnceAt { at, timezone }.validate_and_normalize()
    }

    pub fn validate_and_normalize(&self) -> Result<Self, SchedulerError> {
        match self {
            Self::Cron {
                expression,
                timezone,
            } => Self::parse_cron(expression, *timezone),
            Self::OnceAt { at, timezone } => {
                if at.second() != 0 || at.nanosecond() != 0 {
                    return Err(SchedulerError::InvalidSchedule);
                }
                let local = at.with_timezone(timezone).naive_local();
                match timezone.from_local_datetime(&local) {
                    LocalResult::Single(round_trip) if round_trip.with_timezone(&Utc) == *at => {
                        Ok(self.clone())
                    }
                    LocalResult::Single(_) | LocalResult::Ambiguous(_, _) | LocalResult::None => {
                        Err(SchedulerError::InvalidSchedule)
                    }
                }
            }
        }
    }

    pub fn timezone(&self) -> Tz {
        match self {
            Self::Cron { timezone, .. } | Self::OnceAt { timezone, .. } => *timezone,
        }
    }

    pub fn at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::OnceAt { at, .. } => Some(*at),
            Self::Cron { .. } => None,
        }
    }

    pub fn expression(&self) -> Option<&str> {
        match self {
            Self::Cron { expression, .. } => Some(expression),
            Self::OnceAt { .. } => None,
        }
    }

    pub(crate) fn kind_and_value(&self) -> (&'static str, String, String) {
        match self {
            Self::Cron {
                expression,
                timezone,
            } => ("cron", expression.clone(), timezone.to_string()),
            Self::OnceAt { at, timezone } => (
                "once_at",
                at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                timezone.to_string(),
            ),
        }
    }

    pub(crate) fn from_columns(kind: &str, value: &str, timezone: &str) -> rusqlite::Result<Self> {
        let timezone = timezone
            .parse::<Tz>()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        match kind {
            "cron" => Self::parse_cron(value, timezone).map_err(|_| rusqlite::Error::InvalidQuery),
            "once_at" => {
                let at = DateTime::parse_from_rfc3339(value)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?
                    .with_timezone(&Utc);
                Self::OnceAt { at, timezone }
                    .validate_and_normalize()
                    .map_err(|_| rusqlite::Error::InvalidQuery)
            }
            _ => Err(rusqlite::Error::InvalidQuery),
        }
    }
}

fn normalize_field(field: &str, (minimum, maximum): (u32, u32)) -> Result<String, SchedulerError> {
    if field.is_empty()
        || !field
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'*' | b',' | b'-' | b'/'))
    {
        return Err(SchedulerError::InvalidSchedule);
    }
    field
        .split(',')
        .map(|part| normalize_part(part, minimum, maximum))
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join(","))
}

fn normalize_part(part: &str, minimum: u32, maximum: u32) -> Result<String, SchedulerError> {
    let mut step_split = part.split('/');
    let base = step_split.next().ok_or(SchedulerError::InvalidSchedule)?;
    let step = step_split.next();
    if step_split.next().is_some() || base.is_empty() {
        return Err(SchedulerError::InvalidSchedule);
    }
    let normalized_base = if base == "*" {
        "*".into()
    } else if let Some((start, end)) = base.split_once('-') {
        let start = parse_number(start, minimum, maximum)?;
        let end = parse_number(end, minimum, maximum)?;
        if start > end {
            return Err(SchedulerError::InvalidSchedule);
        }
        format!("{start}-{end}")
    } else {
        parse_number(base, minimum, maximum)?.to_string()
    };
    if let Some(step) = step {
        if base != "*" && !base.contains('-') {
            return Err(SchedulerError::InvalidSchedule);
        }
        let step = parse_number(step, 1, maximum)?;
        Ok(format!("{normalized_base}/{step}"))
    } else {
        Ok(normalized_base)
    }
}

fn parse_number(value: &str, minimum: u32, maximum: u32) -> Result<u32, SchedulerError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SchedulerError::InvalidSchedule);
    }
    let value = value
        .parse::<u32>()
        .map_err(|_| SchedulerError::InvalidSchedule)?;
    if !(minimum..=maximum).contains(&value) {
        return Err(SchedulerError::InvalidSchedule);
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SchedulerError {
    #[error("invalid_schedule")]
    InvalidSchedule,
    #[error("invalid_path")]
    InvalidPath,
    #[error("malformed_managed_block")]
    MalformedManagedBlock,
    #[error("unsupported_cron")]
    UnsupportedCron,
    #[error("cron_backend_error")]
    Backend,
    #[error("scheduler_busy")]
    Busy,
    #[error("store_error")]
    Store,
}

impl From<StoreError> for SchedulerError {
    fn from(_: StoreError) -> Self {
        Self::Store
    }
}

pub struct CronRenderer {
    binary_path: PathBuf,
}

impl CronRenderer {
    pub fn new(binary_path: PathBuf) -> Result<Self, SchedulerError> {
        validate_rendered_path(&binary_path)?;
        Ok(Self { binary_path })
    }

    pub fn render(&self, jobs: &[CronJob]) -> Result<String, SchedulerError> {
        let mut output = String::new();
        output.push_str(MANAGED_START);
        output.push('\n');
        for job in jobs {
            if job.desired_state != crate::domain::JobDesiredState::Active {
                continue;
            }
            let schedule = job.schedule.validate_and_normalize()?;
            output.push_str("CRON_TZ=");
            output.push_str(&schedule.timezone().to_string());
            output.push('\n');
            match &schedule {
                ScheduleSpec::Cron { expression, .. } => output.push_str(expression),
                ScheduleSpec::OnceAt { at, timezone } => {
                    let local = at.with_timezone(timezone);
                    output.push_str(&format!(
                        "{} {} {} {} *",
                        local.minute(),
                        local.hour(),
                        local.day(),
                        local.month()
                    ));
                }
            }
            output.push(' ');
            output.push_str(
                self.binary_path
                    .to_str()
                    .ok_or(SchedulerError::InvalidPath)?,
            );
            output.push_str(" run-job ");
            output.push_str(&job.id.to_string());
            output.push('\n');
        }
        output.push_str(MANAGED_END);
        output.push('\n');
        Ok(output)
    }

    pub fn merge(&self, existing: &str, jobs: &[CronJob]) -> Result<String, SchedulerError> {
        self.replace_managed_block(existing, jobs)
    }

    pub fn replace_managed_block(
        &self,
        existing: &str,
        jobs: &[CronJob],
    ) -> Result<String, SchedulerError> {
        let managed = self.render(jobs)?;
        replace_managed_text(existing, &managed)
    }
}

fn replace_managed_text(existing: &str, managed_block: &str) -> Result<String, SchedulerError> {
    let lines = line_ranges(existing);
    let starts: Vec<_> = lines
        .iter()
        .filter(|(_, _, body)| *body == MANAGED_START)
        .collect();
    let ends: Vec<_> = lines
        .iter()
        .filter(|(_, _, body)| *body == MANAGED_END)
        .collect();
    match (starts.as_slice(), ends.as_slice()) {
        ([], []) => {
            let mut result = existing.to_owned();
            if !result.is_empty() && !result.ends_with('\n') {
                result.push('\n');
            }
            result.push_str(managed_block);
            Ok(result)
        }
        ([(start, _, _)], [(_, end, _)]) if start < end => {
            let mut result = String::with_capacity(existing.len() + managed_block.len());
            result.push_str(&existing[..*start]);
            result.push_str(managed_block);
            result.push_str(&existing[*end..]);
            Ok(result)
        }
        _ => Err(SchedulerError::MalformedManagedBlock),
    }
}

fn line_ranges(input: &str) -> Vec<(usize, usize, &str)> {
    let mut result = Vec::new();
    let mut offset = 0;
    for line in input.split_inclusive('\n') {
        let end = offset + line.len();
        let body = line
            .strip_suffix('\n')
            .unwrap_or(line)
            .strip_suffix('\r')
            .unwrap_or_else(|| line.strip_suffix('\n').unwrap_or(line));
        result.push((offset, end, body));
        offset = end;
    }
    if input.is_empty() {
        return result;
    }
    result
}

pub type CrontabFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SchedulerError>> + Send + 'a>>;

/// Injectable wall clock for deterministic run claiming. Production uses
/// [`SystemCronClock`]; tests can pin DST and missed-run boundaries.
pub trait CronClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCronClock;

impl CronClock for SystemCronClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Best-effort post-claim reconciliation. Its intentionally opaque error
/// cannot expose backend diagnostics through the cron runner.
pub type CronReconcileFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>>;

pub trait CronRunReconciler: Send + Sync {
    fn reconcile<'a>(&'a self, now: DateTime<Utc>) -> CronReconcileFuture<'a>;
}

pub trait CrontabBackend: Send + Sync {
    fn list(&self) -> CrontabFuture<'_, String>;
    fn validate<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()>;
    fn install<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()>;
    fn preflight(&self) -> CrontabFuture<'_, ()>;
}

#[derive(Clone, Debug)]
pub struct SystemCrontabBackend {
    executable: PathBuf,
}

impl SystemCrontabBackend {
    pub fn new(executable: PathBuf) -> Result<Self, SchedulerError> {
        validate_executable_path(&executable)?;
        Ok(Self { executable })
    }

    async fn feed(
        &self,
        arguments: &[&str],
        input: &str,
    ) -> Result<std::process::Output, SchedulerError> {
        let mut child = Command::new(&self.executable)
            .args(arguments)
            .env("LC_ALL", "C")
            .env("LANG", "C")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| SchedulerError::Backend)?;
        child
            .stdin
            .take()
            .ok_or(SchedulerError::Backend)?
            .write_all(input.as_bytes())
            .await
            .map_err(|_| SchedulerError::Backend)?;
        child
            .wait_with_output()
            .await
            .map_err(|_| SchedulerError::Backend)
    }
}

impl CrontabBackend for SystemCrontabBackend {
    fn list(&self) -> CrontabFuture<'_, String> {
        Box::pin(async move {
            let output = Command::new(&self.executable)
                .arg("-l")
                .env("LC_ALL", "C")
                .env("LANG", "C")
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output()
                .await
                .map_err(|_| SchedulerError::Backend)?;
            if output.status.success() {
                String::from_utf8(output.stdout).map_err(|_| SchedulerError::Backend)
            } else if output.status.code() == Some(1)
                && String::from_utf8_lossy(&output.stderr)
                    .to_ascii_lowercase()
                    .contains("no crontab")
            {
                Ok(String::new())
            } else {
                Err(SchedulerError::Backend)
            }
        })
    }

    fn validate<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()> {
        Box::pin(async move {
            let output = self.feed(&["-T", "-"], candidate).await?;
            output
                .status
                .success()
                .then_some(())
                .ok_or(SchedulerError::Backend)
        })
    }

    fn install<'a>(&'a self, candidate: &'a str) -> CrontabFuture<'a, ()> {
        Box::pin(async move {
            let output = self.feed(&["-"], candidate).await?;
            output
                .status
                .success()
                .then_some(())
                .ok_or(SchedulerError::Backend)
        })
    }

    fn preflight(&self) -> CrontabFuture<'_, ()> {
        Box::pin(async move {
            let version = Command::new(&self.executable)
                .arg("-V")
                .env("LC_ALL", "C")
                .env("LANG", "C")
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output()
                .await
                .map_err(|_| SchedulerError::UnsupportedCron)?;
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&version.stdout),
                String::from_utf8_lossy(&version.stderr)
            );
            let identity = combined.trim().to_ascii_lowercase();
            let is_cronie = identity == "cronie"
                || identity.starts_with("cronie ")
                || identity.starts_with("crontab (cronie) ");
            if !version.status.success() || !is_cronie {
                return Err(SchedulerError::UnsupportedCron);
            }
            let probe = "CRON_TZ=UTC\n0 0 * * * /bin/true\n";
            let validated = self
                .feed(&["-T", "-"], probe)
                .await
                .map_err(|_| SchedulerError::UnsupportedCron)?;
            validated
                .status
                .success()
                .then_some(())
                .ok_or(SchedulerError::UnsupportedCron)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncReport {
    Installed { jobs: usize },
    SavedNotInstalled { jobs: usize },
}

pub struct CronSynchronizer {
    store: Store,
    backend: Arc<dyn CrontabBackend>,
    lock_path: PathBuf,
    renderer: CronRenderer,
}

impl CronSynchronizer {
    pub fn new(
        store: Store,
        backend: Arc<dyn CrontabBackend>,
        lock_path: PathBuf,
        binary_path: PathBuf,
    ) -> Result<Self, SchedulerError> {
        validate_lock_path(&lock_path)?;
        Ok(Self {
            store,
            backend,
            lock_path,
            renderer: CronRenderer::new(binary_path)?,
        })
    }

    pub fn sync(&self) -> CrontabFuture<'_, SyncReport> {
        self.sync_at(Utc::now())
    }

    pub fn sync_at(&self, current_time: DateTime<Utc>) -> CrontabFuture<'_, SyncReport> {
        Box::pin(async move {
            let lock = open_lock(&self.lock_path)?;
            lock.try_lock_exclusive()
                .map_err(|error| match error.kind() {
                    io::ErrorKind::WouldBlock => SchedulerError::Busy,
                    _ => SchedulerError::Backend,
                })?;
            self.backend.preflight().await?;
            self.store.mark_missed_once_jobs(current_time)?;
            let snapshot = self.store.jobs_for_sync()?;
            let active: Vec<_> = snapshot
                .iter()
                .filter(|job| job.desired_state == crate::domain::JobDesiredState::Active)
                .cloned()
                .collect();
            let existing = match self.backend.list().await {
                Ok(existing) => existing,
                Err(_) => {
                    self.store.mark_sync_snapshot_failed(&snapshot)?;
                    return Ok(SyncReport::SavedNotInstalled {
                        jobs: snapshot.len(),
                    });
                }
            };
            let candidate = match self.renderer.merge(&existing, &active) {
                Ok(candidate) => candidate,
                Err(error) => {
                    self.store.mark_sync_snapshot_failed(&snapshot)?;
                    return Err(error);
                }
            };
            if self.backend.validate(&candidate).await.is_err() {
                self.store.mark_sync_snapshot_failed(&snapshot)?;
                return Ok(SyncReport::SavedNotInstalled {
                    jobs: snapshot.len(),
                });
            }
            if self.backend.install(&candidate).await.is_err() {
                self.store.mark_sync_snapshot_failed(&snapshot)?;
                return Ok(SyncReport::SavedNotInstalled {
                    jobs: snapshot.len(),
                });
            }
            self.store.mark_sync_snapshot_applied(&snapshot)?;
            Ok(SyncReport::Installed { jobs: active.len() })
        })
    }
}

impl CronRunReconciler for CronSynchronizer {
    fn reconcile<'a>(&'a self, now: DateTime<Utc>) -> CronReconcileFuture<'a> {
        Box::pin(async move { self.sync_at(now).await.map(|_| ()).map_err(|_| ()) })
    }
}

fn open_lock(path: &Path) -> Result<File, SchedulerError> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|_| SchedulerError::Backend)
}

fn validate_lock_path(path: &Path) -> Result<(), SchedulerError> {
    if !path.is_absolute() || path.as_os_str().is_empty() {
        return Err(SchedulerError::InvalidPath);
    }
    let text = path.to_str().ok_or(SchedulerError::InvalidPath)?;
    if text.contains(['\0', '\r', '\n']) {
        return Err(SchedulerError::InvalidPath);
    }
    Ok(())
}

pub(crate) fn validate_rendered_path(path: &Path) -> Result<(), SchedulerError> {
    validate_executable_path(path)?;
    let text = path.to_str().ok_or(SchedulerError::InvalidPath)?;
    if !text
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'.'))
    {
        return Err(SchedulerError::InvalidPath);
    }
    Ok(())
}

pub(crate) fn validate_executable_path(path: &Path) -> Result<(), SchedulerError> {
    if !path.is_absolute() || path.as_os_str().is_empty() {
        return Err(SchedulerError::InvalidPath);
    }
    let text = path.to_str().ok_or(SchedulerError::InvalidPath)?;
    if text.contains(['\0', '\r', '\n']) {
        return Err(SchedulerError::InvalidPath);
    }
    Ok(())
}
