use std::time::Instant;

use rusqlite::{OptionalExtension, Transaction, params};
use tokio_util::sync::CancellationToken;

use super::{DatabaseIdentity, Store, StoreError, now};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeOwnerRecord {
    pub owner_id: String,
    pub lock_name: String,
    pub lock_identity: DatabaseIdentity,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OwnerRecovery {
    pub tool_runs: usize,
    pub turns: usize,
    pub cron_runs: usize,
}

impl Store {
    pub(crate) fn ensure_coordination_lock(
        &self,
        lock_name: &str,
        identity: DatabaseIdentity,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OwnerRecovery, StoreError> {
        validate_lock_name(lock_name)?;
        let device = i64::try_from(identity.device).map_err(|_| StoreError::InvalidMetadata)?;
        let inode = i64::try_from(identity.inode).map_err(|_| StoreError::InvalidMetadata)?;
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            let current = tx
                .query_row(
                    "SELECT lock_name,lock_device,lock_inode FROM runtime_coordination WHERE singleton=1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()?;
            match current {
                Some((current_name, current_device, current_inode))
                    if current_name == lock_name
                        && current_device == device
                        && current_inode == inode =>
                {
                    recover_unowned_runtime_work(tx)
                }
                Some(_) => Err(StoreError::Conflict),
                None => {
                    tx.execute(
                        "INSERT INTO runtime_coordination(singleton,lock_name,lock_device,lock_inode) VALUES(1,?,?,?)",
                        params![lock_name, device, inode],
                    )?;
                    recover_unowned_runtime_work(tx)
                }
            }
        })
    }

    pub(crate) fn runtime_owners(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RuntimeOwnerRecord>, StoreError> {
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            let mut statement = tx.prepare(
                "SELECT owner_id,lock_name,lock_device,lock_inode FROM runtime_owners ORDER BY owner_id",
            )?;
            Ok(statement
                .query_map([], |row| {
                    let device = u64::try_from(row.get::<_, i64>(2)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?;
                    let inode = u64::try_from(row.get::<_, i64>(3)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?;
                    Ok(RuntimeOwnerRecord {
                        owner_id: row.get(0)?,
                        lock_name: row.get(1)?,
                        lock_identity: DatabaseIdentity { device, inode },
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub(crate) fn register_runtime_owner(
        &self,
        owner: &RuntimeOwnerRecord,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), StoreError> {
        uuid::Uuid::parse_str(&owner.owner_id).map_err(|_| StoreError::InvalidOwner)?;
        validate_lock_name(&owner.lock_name)?;
        let device =
            i64::try_from(owner.lock_identity.device).map_err(|_| StoreError::InvalidMetadata)?;
        let inode =
            i64::try_from(owner.lock_identity.inode).map_err(|_| StoreError::InvalidMetadata)?;
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            tx.execute(
                "INSERT INTO runtime_owners(owner_id,lock_name,lock_device,lock_inode,created_at) VALUES(?,?,?,?,?)",
                params![owner.owner_id, owner.lock_name, device, inode, now()],
            )?;
            Ok(())
        })
    }

    pub(crate) fn recover_runtime_owner(
        &self,
        owner_id: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OwnerRecovery, StoreError> {
        self.with_immediate_transaction(deadline, cancellation, |tx| {
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_owners WHERE owner_id=?)",
                [owner_id],
                |row| row.get(0),
            )?;
            if !exists {
                return Ok(OwnerRecovery::default());
            }
            let timestamp = now();
            let tool_runs = tx.execute(
                "UPDATE tool_runs
                 SET status=CASE WHEN read_only=1 THEN 'failed' ELSE 'uncertain' END,
                     safe_error_code='process_interrupted',finished_at=?1,runtime_owner_id=NULL
                 WHERE status='pending' AND (
                    runtime_owner_id=?2 OR (
                        runtime_owner_id IS NULL AND (
                            (owner_kind='interactive_turn' AND EXISTS(
                                SELECT 1 FROM turns
                                WHERE id=tool_runs.owner_id AND status='pending'
                                  AND runtime_owner_id=?2
                            )) OR
                            (owner_kind='cron_run' AND EXISTS(
                                SELECT 1 FROM cron_runs
                                WHERE id=tool_runs.owner_id AND status='pending'
                                  AND runtime_owner_id=?2
                            ))
                        )
                    )
                 )",
                params![timestamp, owner_id],
            )?;
            tx.execute(
                "UPDATE dialogs SET updated_at=?1 WHERE id IN
                    (SELECT dialog_id FROM turns WHERE status='pending' AND runtime_owner_id=?2)",
                params![timestamp, owner_id],
            )?;
            let turns = tx.execute(
                "UPDATE turns SET status='interrupted',safe_error_code='process_interrupted',
                    finished_at=?1,runtime_owner_id=NULL
                 WHERE status='pending' AND runtime_owner_id=?2",
                params![timestamp, owner_id],
            )?;
            let cron_runs = tx.execute(
                "UPDATE cron_runs SET status='interrupted',safe_error_code='process_interrupted',
                    finished_at=?1,runtime_owner_id=NULL
                 WHERE status='pending' AND runtime_owner_id=?2",
                params![timestamp, owner_id],
            )?;
            tx.execute(
                "UPDATE turns SET runtime_owner_id=NULL WHERE runtime_owner_id=?",
                [owner_id],
            )?;
            tx.execute(
                "UPDATE cron_runs SET runtime_owner_id=NULL WHERE runtime_owner_id=?",
                [owner_id],
            )?;
            if tx.execute("DELETE FROM runtime_owners WHERE owner_id=?", [owner_id])? != 1 {
                return Err(StoreError::Conflict);
            }
            Ok(OwnerRecovery {
                tool_runs,
                turns,
                cron_runs,
            })
        })
    }
}

fn recover_unowned_runtime_work(tx: &Transaction<'_>) -> Result<OwnerRecovery, StoreError> {
    let timestamp = now();
    let tool_runs = tx.execute(
        "UPDATE tool_runs
         SET status=CASE WHEN read_only=1 THEN 'failed' ELSE 'uncertain' END,
             safe_error_code='process_interrupted',finished_at=?1,runtime_owner_id=NULL
         WHERE status='pending' AND runtime_owner_id IS NULL
           AND NOT (
                owner_kind='interactive_turn' AND EXISTS(
                    SELECT 1 FROM turns
                    WHERE turns.id=tool_runs.owner_id AND turns.status='pending'
                      AND turns.runtime_owner_id IS NOT NULL
                )
           )
           AND NOT (
                owner_kind='cron_run' AND EXISTS(
                    SELECT 1 FROM cron_runs
                    WHERE cron_runs.id=tool_runs.owner_id AND cron_runs.status='pending'
                      AND cron_runs.runtime_owner_id IS NOT NULL
                )
           )",
        [&timestamp],
    )?;
    tx.execute(
        "UPDATE dialogs SET updated_at=? WHERE id IN
            (SELECT dialog_id FROM turns WHERE status='pending' AND runtime_owner_id IS NULL)",
        [&timestamp],
    )?;
    let turns = tx.execute(
        "UPDATE turns SET status='interrupted',safe_error_code='process_interrupted',finished_at=?
         WHERE status='pending' AND runtime_owner_id IS NULL",
        [&timestamp],
    )?;
    let cron_runs = tx.execute(
        "UPDATE cron_runs SET status='interrupted',safe_error_code='process_interrupted',finished_at=?
         WHERE status='pending' AND runtime_owner_id IS NULL",
        [&timestamp],
    )?;
    Ok(OwnerRecovery {
        tool_runs,
        turns,
        cron_runs,
    })
}

fn validate_lock_name(name: &str) -> Result<(), StoreError> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(StoreError::InvalidMetadata);
    }
    Ok(())
}
