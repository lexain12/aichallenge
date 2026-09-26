//! Owner-aware process coordination and crash recovery.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fs2::FileExt;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use crate::store::{DatabaseIdentity, RuntimeOwnerRecord, Store};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StartupRecoveryReport {
    pub performed: bool,
    pub tool_runs: usize,
    pub turns: usize,
    pub cron_runs: usize,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RuntimeLeaseError {
    #[error("runtime_lease_error")]
    Lease,
    #[error("startup_recovery_error")]
    Recovery,
}

/// Every mutating runtime holds one exclusive owner lock. The coordinator
/// lock is held only while scanning dead owners and registering this owner.
pub struct ProcessLease {
    owner_id: String,
    _owner_file: File,
    report: StartupRecoveryReport,
}

impl ProcessLease {
    pub fn acquire(store: &Store) -> Result<Self, RuntimeLeaseError> {
        Self::acquire_with_deadline(
            store,
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )
    }

    pub fn acquire_with_deadline(
        store: &Store,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, RuntimeLeaseError> {
        ensure_active(deadline, cancellation)?;
        let coordinator_path = coordinator_path(store)?;
        ensure_active(deadline, cancellation)?;
        let coordinator = open_or_create_lock(&coordinator_path)?;
        lock_until(&coordinator, deadline, cancellation)?;
        verify_open_lock(&coordinator_path, &coordinator)?;
        let coordinator_identity = file_identity(&coordinator)?;
        let coordinator_name = coordinator_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(RuntimeLeaseError::Lease)?;
        let legacy = store
            .ensure_coordination_lock(
                coordinator_name,
                coordinator_identity,
                deadline,
                cancellation,
            )
            .map_err(|_| RuntimeLeaseError::Recovery)?;

        let mut report = StartupRecoveryReport {
            performed: true,
            ..StartupRecoveryReport::default()
        };
        report.tool_runs += legacy.tool_runs;
        report.turns += legacy.turns;
        report.cron_runs += legacy.cron_runs;

        for owner in store
            .runtime_owners(deadline, cancellation)
            .map_err(|_| RuntimeLeaseError::Recovery)?
        {
            let expected_name = owner_lock_name(store, &owner.owner_id)?;
            if owner.lock_name != expected_name {
                return Err(RuntimeLeaseError::Lease);
            }
            let owner_path = store
                .database_path()
                .parent()
                .ok_or(RuntimeLeaseError::Lease)?
                .join(&owner.lock_name);
            let owner_file = open_existing_lock(&owner_path)?;
            if file_identity(&owner_file)? != owner.lock_identity {
                return Err(RuntimeLeaseError::Lease);
            }
            verify_open_lock(&owner_path, &owner_file)?;
            match FileExt::try_lock_exclusive(&owner_file) {
                Ok(()) => {
                    let recovered = store
                        .recover_runtime_owner(&owner.owner_id, deadline, cancellation)
                        .map_err(|_| RuntimeLeaseError::Recovery)?;
                    report.tool_runs += recovered.tool_runs;
                    report.turns += recovered.turns;
                    report.cron_runs += recovered.cron_runs;
                    // The row is gone before unlinking, so a crash cannot
                    // leave an owner record whose authoritative lock vanished.
                    // Revalidate the path-to-fd identity immediately before
                    // best-effort removal; an orphaned UUID lock is harmless.
                    if verify_open_lock(&owner_path, &owner_file).is_ok() {
                        let _ = std::fs::remove_file(&owner_path);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => return Err(RuntimeLeaseError::Lease),
            }
        }

        ensure_active(deadline, cancellation)?;
        let (owner_id, owner_file, owner_record) =
            create_owner_lock(store, deadline, cancellation)?;
        store
            .register_runtime_owner(&owner_record, deadline, cancellation)
            .map_err(|_| RuntimeLeaseError::Recovery)?;
        drop(coordinator);
        Ok(Self {
            owner_id,
            _owner_file: owner_file,
            report,
        })
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub fn recovery_report(&self) -> StartupRecoveryReport {
        self.report
    }
}

fn coordinator_path(store: &Store) -> Result<PathBuf, RuntimeLeaseError> {
    let database = store.database_path();
    let mut name = database
        .file_name()
        .ok_or(RuntimeLeaseError::Lease)?
        .to_os_string();
    name.push(".runtime.lock");
    Ok(database.with_file_name(name))
}

fn owner_lock_name(store: &Store, owner_id: &str) -> Result<String, RuntimeLeaseError> {
    let parsed = uuid::Uuid::parse_str(owner_id).map_err(|_| RuntimeLeaseError::Lease)?;
    if parsed.hyphenated().to_string() != owner_id {
        return Err(RuntimeLeaseError::Lease);
    }
    let database = store
        .database_path()
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(RuntimeLeaseError::Lease)?;
    Ok(format!("{database}.runtime.owner.{owner_id}.lock"))
}

fn create_owner_lock(
    store: &Store,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(String, File, RuntimeOwnerRecord), RuntimeLeaseError> {
    let parent = store
        .database_path()
        .parent()
        .ok_or(RuntimeLeaseError::Lease)?;
    for _ in 0..4 {
        ensure_active(deadline, cancellation)?;
        let owner_id = uuid::Uuid::new_v4().hyphenated().to_string();
        let lock_name = owner_lock_name(store, &owner_id)?;
        let path = parent.join(&lock_name);
        match create_lock(&path) {
            Ok(file) => {
                FileExt::try_lock_exclusive(&file).map_err(|_| RuntimeLeaseError::Lease)?;
                verify_open_lock(&path, &file)?;
                let record = RuntimeOwnerRecord {
                    owner_id: owner_id.clone(),
                    lock_name,
                    lock_identity: file_identity(&file)?,
                };
                return Ok((owner_id, file, record));
            }
            Err(RuntimeLeaseError::Lease) if path.exists() => continue,
            Err(error) => return Err(error),
        }
    }
    Err(RuntimeLeaseError::Lease)
}

fn ensure_active(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), RuntimeLeaseError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(RuntimeLeaseError::Lease)
    } else {
        Ok(())
    }
}

fn lock_until(
    file: &File,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), RuntimeLeaseError> {
    const POLL: Duration = Duration::from_millis(5);
    loop {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(RuntimeLeaseError::Lease);
        }
        match FileExt::try_lock_exclusive(file) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()).min(POLL));
            }
            Err(_) => return Err(RuntimeLeaseError::Lease),
        }
    }
}

fn open_or_create_lock(path: &Path) -> Result<File, RuntimeLeaseError> {
    match open_existing_lock(path) {
        Ok(file) => Ok(file),
        Err(RuntimeLeaseError::Lease) if !path.exists() => match create_lock(path) {
            Ok(file) => Ok(file),
            Err(RuntimeLeaseError::Lease) => open_existing_lock(path),
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    }
}

fn create_lock(path: &Path) -> Result<File, RuntimeLeaseError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path).map_err(|_| RuntimeLeaseError::Lease)?;
    validate_lock_metadata(&file.metadata().map_err(|_| RuntimeLeaseError::Lease)?)?;
    Ok(file)
}

fn open_existing_lock(path: &Path) -> Result<File, RuntimeLeaseError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path).map_err(|_| RuntimeLeaseError::Lease)?;
    validate_lock_metadata(&file.metadata().map_err(|_| RuntimeLeaseError::Lease)?)?;
    Ok(file)
}

fn verify_open_lock(path: &Path, file: &File) -> Result<(), RuntimeLeaseError> {
    let path_metadata = std::fs::symlink_metadata(path).map_err(|_| RuntimeLeaseError::Lease)?;
    validate_lock_metadata(&path_metadata)?;
    if metadata_identity(&path_metadata)
        != metadata_identity(&file.metadata().map_err(|_| RuntimeLeaseError::Lease)?)
    {
        return Err(RuntimeLeaseError::Lease);
    }
    Ok(())
}

#[cfg(unix)]
fn validate_lock_metadata(metadata: &std::fs::Metadata) -> Result<(), RuntimeLeaseError> {
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
    {
        return Err(RuntimeLeaseError::Lease);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_lock_metadata(metadata: &std::fs::Metadata) -> Result<(), RuntimeLeaseError> {
    metadata
        .is_file()
        .then_some(())
        .ok_or(RuntimeLeaseError::Lease)
}

fn file_identity(file: &File) -> Result<DatabaseIdentity, RuntimeLeaseError> {
    let metadata = file.metadata().map_err(|_| RuntimeLeaseError::Lease)?;
    Ok(metadata_identity(&metadata))
}

#[cfg(unix)]
fn metadata_identity(metadata: &std::fs::Metadata) -> DatabaseIdentity {
    DatabaseIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn metadata_identity(_metadata: &std::fs::Metadata) -> DatabaseIdentity {
    DatabaseIdentity {
        device: 0,
        inode: 0,
    }
}
