//! Process-wide coordination for safe startup recovery.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

use fs2::FileExt;
use thiserror::Error;

use crate::store::Store;

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

/// A shared advisory lease held for the complete lifetime of every mutating
/// runtime process. The first process that can take the exclusive lease owns
/// global recovery and downgrades only after recovery commits.
pub struct ProcessLease {
    _file: File,
    report: StartupRecoveryReport,
}

impl ProcessLease {
    pub fn acquire(store: &Store) -> Result<Self, RuntimeLeaseError> {
        let path = lease_path(store)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| RuntimeLeaseError::Lease)?;

        let report = match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                let tool_runs = store
                    .recover_pending_tool_runs()
                    .map_err(|_| RuntimeLeaseError::Recovery)?;
                let turns = store
                    .recover_pending_turns()
                    .map_err(|_| RuntimeLeaseError::Recovery)?;
                let cron_runs = store
                    .recover_pending_cron_runs()
                    .map_err(|_| RuntimeLeaseError::Recovery)?;
                FileExt::lock_shared(&file).map_err(|_| RuntimeLeaseError::Lease)?;
                StartupRecoveryReport {
                    performed: true,
                    tool_runs,
                    turns,
                    cron_runs,
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                // This blocks behind an exclusive startup recovery, but joins
                // immediately when another healthy process already holds a
                // shared lifetime lease.
                FileExt::lock_shared(&file).map_err(|_| RuntimeLeaseError::Lease)?;
                StartupRecoveryReport::default()
            }
            Err(_) => return Err(RuntimeLeaseError::Lease),
        };
        Ok(Self {
            _file: file,
            report,
        })
    }

    pub fn recovery_report(&self) -> StartupRecoveryReport {
        self.report
    }
}

fn lease_path(store: &Store) -> Result<PathBuf, RuntimeLeaseError> {
    let database = store.database_path();
    let file_name = database
        .file_name()
        .ok_or(RuntimeLeaseError::Lease)?
        .to_os_string();
    let mut lease_name = file_name;
    lease_name.push(".runtime.lock");
    Ok(database.with_file_name(lease_name))
}
