use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::chat::{Message, Role};
use crate::dialog::{DialogStore, StoreError};
use crate::workflow::{StageRunId, TaskPhase, TaskStatus, WorkflowTaskId, WorkflowTaskState};

pub trait WorkflowRepository {
    fn load_workflow(&self, dialog_id: i64) -> Result<DialogWorkflowSnapshot, StoreError>;
    fn load_stage_messages(
        &self,
        stage_run_id: StageRunId,
    ) -> Result<Vec<StageProtocolMessage>, StoreError>;
    fn load_pending_processing(&self, dialog_id: i64)
    -> Result<Vec<PendingProcessing>, StoreError>;
    fn close_stale_processing(
        &mut self,
        dialog_id: i64,
        current_version: u64,
    ) -> Result<usize, StoreError>;
}

#[derive(Clone, Debug)]
pub struct DialogWorkflowSnapshot {
    pub current_task: Option<WorkflowTaskState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageProtocolMessage {
    pub message_id: i64,
    pub message: Message,
    pub source: ProtocolSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolSource {
    Human,
    Controller,
    Assistant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingProcessing {
    pub id: i64,
    pub assistant_message_id: i64,
    pub checker_name: String,
    pub expected_version: u64,
    pub status: ProcessingStatus,
    pub attempts: u32,
    pub result_json: Option<serde_json::Value>,
    pub last_error: Option<String>,
}

impl WorkflowRepository for DialogStore {
    fn load_workflow(&self, dialog_id: i64) -> Result<DialogWorkflowSnapshot, StoreError> {
        let tx = self.connection.unchecked_transaction()?;
        let snapshot = load_workflow(&tx, dialog_id)?;
        tx.commit()?;
        Ok(snapshot)
    }

    fn load_stage_messages(
        &self,
        stage_run_id: StageRunId,
    ) -> Result<Vec<StageProtocolMessage>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT m.id, m.role, m.content, u.usage_json, i.source,
                    m.dialog_id, t.dialog_id, ms.workflow_task_id, s.workflow_task_id, i.dialog_id
             FROM message_task_stages ms
             LEFT JOIN messages m ON m.id = ms.message_id
             LEFT JOIN task_stage_runs s ON s.id = ms.stage_run_id
             LEFT JOIN workflow_tasks t ON t.id = ms.workflow_task_id
             LEFT JOIN message_usage u ON u.message_id = m.id
             LEFT JOIN workflow_inputs i ON i.message_id = m.id
             WHERE ms.stage_run_id = ?1 ORDER BY m.id",
        )?;
        let mut rows = statement.query([stage_run_id.0])?;
        let mut messages = Vec::new();
        while let Some(row) = rows.next()? {
            let message_id = positive(integer(row, 0)?, "message id")?;
            let dialog_id = positive(integer(row, 5)?, "message dialog id")?;
            if dialog_id != integer(row, 6)?
                || integer(row, 7)? != integer(row, 8)?
                || field::<Option<i64>>(row, 9)?.is_some_and(|id| id != dialog_id)
            {
                return invalid("stage message ownership mismatch");
            }
            let role = match field::<String>(row, 1)?.as_str() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                _ => return invalid("unknown protocol role"),
            };
            let input_source: Option<String> = field(row, 4)?;
            if input_source
                .as_deref()
                .is_some_and(|source| source != "human" && source != "controller")
            {
                return invalid("unknown workflow input source");
            }
            let source = match (role, input_source.as_deref()) {
                (Role::Assistant, _) => ProtocolSource::Assistant,
                (_, Some("controller")) => ProtocolSource::Controller,
                _ => ProtocolSource::Human,
            };
            let usage = field::<Option<String>>(row, 3)?
                .map(|value| json(&value))
                .transpose()?;
            messages.push(StageProtocolMessage {
                message_id,
                message: Message::new(role, field(row, 2)?).with_usage(usage),
                source,
            });
        }
        Ok(messages)
    }

    fn load_pending_processing(
        &self,
        dialog_id: i64,
    ) -> Result<Vec<PendingProcessing>, StoreError> {
        let tx = self.connection.unchecked_transaction()?;
        let Some(task) = load_workflow(&tx, dialog_id)?.current_task else {
            return Ok(Vec::new());
        };
        let mut statement = tx.prepare(
            "SELECT p.id, p.assistant_message_id, p.checker_name, p.expected_version,
                    p.status, p.attempts, p.result_json, p.last_error,
                    m.role, m.dialog_id, s.workflow_task_id
             FROM response_processing p
             JOIN message_task_stages ms ON ms.message_id = p.assistant_message_id
             LEFT JOIN messages m ON m.id = p.assistant_message_id
             LEFT JOIN task_stage_runs s ON s.id = ms.stage_run_id
             WHERE ms.workflow_task_id = ?1 ORDER BY p.id",
        )?;
        let mut rows = statement.query([task.id.0])?;
        let mut pending = Vec::new();
        while let Some(row) = rows.next()? {
            if field::<String>(row, 8)? != "assistant"
                || integer(row, 9)? != dialog_id
                || integer(row, 10)? != task.id.0
            {
                return invalid("processing message ownership or role mismatch");
            }
            let checker_name: String = field(row, 2)?;
            if checker_name.trim().is_empty() {
                return invalid("blank processing checker name");
            }
            let processing = PendingProcessing {
                id: positive(integer(row, 0)?, "processing id")?,
                assistant_message_id: positive(integer(row, 1)?, "assistant message id")?,
                checker_name,
                expected_version: unsigned(integer(row, 3)?, "expected version")?,
                status: text_enum(row, 4)?,
                attempts: unsigned(integer(row, 5)?, "processing attempts")?,
                result_json: field::<Option<String>>(row, 6)?
                    .map(|value| json(&value))
                    .transpose()?,
                last_error: field(row, 7)?,
            };
            if processing.expected_version == task.version
                && processing.attempts < 2
                && processing.status != ProcessingStatus::Completed
            {
                pending.push(processing);
            }
        }
        Ok(pending)
    }

    fn close_stale_processing(
        &mut self,
        dialog_id: i64,
        current_version: u64,
    ) -> Result<usize, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = load_workflow(&tx, dialog_id)?
            .current_task
            .ok_or(StoreError::Conflict(dialog_id))?;
        if task.version != current_version {
            return Err(StoreError::Conflict(dialog_id));
        }
        let version = i64::try_from(current_version)
            .map_err(|_| StoreError::InvalidWorkflow("version exceeds SQLite INTEGER".into()))?;
        let changed = tx.execute(
            "UPDATE response_processing SET status = 'failed', attempts = max(attempts, 2),
                    last_error = 'stale task version', updated_at = strftime('%Y-%m-%d %H:%M:%f','now')
             WHERE expected_version < ?1 AND status <> 'completed'
               AND assistant_message_id IN (
                   SELECT ms.message_id FROM message_task_stages ms
                   JOIN messages m ON m.id = ms.message_id
                   JOIN task_stage_runs s ON s.id = ms.stage_run_id
                   WHERE ms.workflow_task_id = ?2 AND s.workflow_task_id = ?2
                     AND m.dialog_id = ?3 AND m.role = 'assistant')",
            params![version, task.id.0, dialog_id],
        )?;
        tx.commit()?;
        Ok(changed)
    }
}

fn load_workflow(
    connection: &Connection,
    dialog_id: i64,
) -> Result<DialogWorkflowSnapshot, StoreError> {
    if !connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM dialogs WHERE id = ?1)",
        [dialog_id],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(StoreError::NotFound(dialog_id));
    }
    let selected: Option<i64> = connection
        .query_row(
            "SELECT current_task_id FROM dialog_workflow_state WHERE dialog_id = ?1",
            [dialog_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            StoreError::InvalidWorkflow(format!("invalid current task pointer: {error}"))
        })?;
    let Some(task_id) = selected else {
        return Ok(DialogWorkflowSnapshot { current_task: None });
    };
    let mut statement = connection.prepare(
        "SELECT t.id, t.dialog_id, t.ordinal, t.phase, t.status, t.goal, t.plan_json,
                t.current_step_id, t.expected_action, t.checkpoint_json, t.current_stage_run_id,
                t.incoming_handoff_id, t.version, s.workflow_task_id, s.phase, s.sequence, s.finished_at,
                h.workflow_task_id, h.to_stage_run_id
         FROM workflow_tasks t
         LEFT JOIN task_stage_runs s ON s.id = t.current_stage_run_id
         LEFT JOIN task_transitions h ON h.id = t.incoming_handoff_id
         WHERE t.id = ?1",
    )?;
    let mut rows = statement.query([task_id])?;
    let row = rows
        .next()?
        .ok_or_else(|| StoreError::InvalidWorkflow("dangling current task pointer".into()))?;
    let phase: TaskPhase = text_enum(row, 3)?;
    let status: TaskStatus = text_enum(row, 4)?;
    let stage_id = integer(row, 10)?;
    let incoming_handoff_id: Option<i64> = field(row, 11)?;
    if integer(row, 1)? != dialog_id
        || integer(row, 13)? != task_id
        || text_enum::<TaskPhase>(row, 14)? != phase
        || field::<Option<String>>(row, 16)?.is_some()
    {
        return invalid("current task or stage ownership, phase, or open status mismatch");
    }
    if incoming_handoff_id.is_some()
        && (integer(row, 17)? != task_id || integer(row, 18)? != stage_id)
    {
        return invalid("incoming handoff does not target the current task and stage");
    }
    let task = WorkflowTaskState {
        id: WorkflowTaskId(integer(row, 0)?),
        dialog_id,
        ordinal: unsigned(integer(row, 2)?, "task ordinal")?,
        phase,
        status,
        goal: field(row, 5)?,
        plan: json(&field::<String>(row, 6)?)?,
        current_step_id: field(row, 7)?,
        expected_action: field(row, 8)?,
        checkpoint: json(&field::<String>(row, 9)?)?,
        current_stage_run_id: StageRunId(stage_id),
        current_stage_sequence: unsigned(integer(row, 15)?, "stage sequence")?,
        incoming_handoff_id,
        version: unsigned(integer(row, 12)?, "task version")?,
    };
    task.validate()
        .map_err(|error| StoreError::InvalidWorkflow(error.to_string()))?;
    Ok(DialogWorkflowSnapshot {
        current_task: Some(task),
    })
}

fn invalid<T>(reason: &str) -> Result<T, StoreError> {
    Err(StoreError::InvalidWorkflow(reason.into()))
}

fn field<T: rusqlite::types::FromSql>(row: &Row<'_>, index: usize) -> Result<T, StoreError> {
    row.get(index).map_err(|error| {
        StoreError::InvalidWorkflow(format!("invalid stored field {index}: {error}"))
    })
}

fn integer(row: &Row<'_>, index: usize) -> Result<i64, StoreError> {
    field(row, index)
}

fn unsigned<T: TryFrom<i64>>(value: i64, name: &str) -> Result<T, StoreError> {
    T::try_from(value).map_err(|_| StoreError::InvalidWorkflow(format!("invalid {name}")))
}

fn positive(value: i64, name: &str) -> Result<i64, StoreError> {
    if value <= 0 {
        return invalid(&format!("invalid {name}"));
    }
    Ok(value)
}

fn json<T: DeserializeOwned>(value: &str) -> Result<T, StoreError> {
    serde_json::from_str(value)
        .map_err(|error| StoreError::InvalidWorkflow(format!("invalid stored JSON: {error}")))
}

fn text_enum<T: DeserializeOwned>(row: &Row<'_>, index: usize) -> Result<T, StoreError> {
    serde_json::from_value(serde_json::Value::String(field(row, index)?))
        .map_err(|error| StoreError::InvalidWorkflow(format!("invalid stored enum: {error}")))
}

pub(crate) fn migrate(connection: &mut Connection) -> Result<(), StoreError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS workflow_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
            ordinal INTEGER NOT NULL CHECK (ordinal > 0),
            phase TEXT NOT NULL CHECK (phase IN ('planning','execution','validation','done')),
            status TEXT NOT NULL CHECK (status IN ('active','paused')),
            goal TEXT NOT NULL,
            plan_json TEXT NOT NULL,
            current_step_id TEXT,
            expected_action TEXT,
            checkpoint_json TEXT NOT NULL,
            current_stage_run_id INTEGER,
            incoming_handoff_id INTEGER,
            version INTEGER NOT NULL CHECK (version >= 0),
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
            UNIQUE(dialog_id, ordinal),
            FOREIGN KEY(current_stage_run_id) REFERENCES task_stage_runs(id),
            FOREIGN KEY(incoming_handoff_id) REFERENCES task_transitions(id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_unfinished_workflow_task_per_dialog
        ON workflow_tasks(dialog_id) WHERE phase <> 'done';
        CREATE TABLE IF NOT EXISTS dialog_workflow_state (
            dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
            current_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id)
        );
        CREATE TABLE IF NOT EXISTS task_stage_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id),
            phase TEXT NOT NULL CHECK (phase IN ('planning','execution','validation','done')),
            sequence INTEGER NOT NULL CHECK (sequence > 0),
            started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
            finished_at TEXT,
            CHECK (finished_at IS NULL OR finished_at >= started_at),
            UNIQUE(workflow_task_id, sequence)
        );
        CREATE TABLE IF NOT EXISTS task_stage_context (
            stage_run_id INTEGER PRIMARY KEY REFERENCES task_stage_runs(id),
            context_json TEXT NOT NULL,
            facts_json TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now'))
        );
        CREATE TABLE IF NOT EXISTS workflow_inputs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
            message_id INTEGER NOT NULL UNIQUE REFERENCES messages(id),
            source TEXT NOT NULL CHECK (source IN ('human','controller')),
            checker_name TEXT,
            model_name TEXT,
            triggering_assistant_message_id INTEGER REFERENCES messages(id),
            intent_json TEXT NOT NULL,
            confidence REAL,
            outcome TEXT NOT NULL CHECK (outcome IN ('accepted','rejected')),
            rejection_reason TEXT,
            processing_id INTEGER UNIQUE REFERENCES response_processing(id),
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
            CHECK (
                (source = 'human' AND checker_name IS NULL AND model_name IS NULL
                 AND processing_id IS NULL) OR
                (source = 'controller' AND checker_name IS NOT NULL AND model_name IS NOT NULL
                 AND triggering_assistant_message_id IS NOT NULL AND processing_id IS NOT NULL)
            )
        );
        CREATE TABLE IF NOT EXISTS message_task_stages (
            message_id INTEGER PRIMARY KEY REFERENCES messages(id),
            workflow_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id),
            stage_run_id INTEGER NOT NULL REFERENCES task_stage_runs(id)
        );
        CREATE TABLE IF NOT EXISTS response_processing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assistant_message_id INTEGER NOT NULL REFERENCES messages(id),
            checker_name TEXT NOT NULL,
            expected_version INTEGER NOT NULL CHECK (expected_version >= 0),
            status TEXT NOT NULL CHECK (status IN ('pending','processing','completed','failed')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            result_json TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
            UNIQUE(assistant_message_id, checker_name)
        );
        CREATE TABLE IF NOT EXISTS task_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id),
            from_stage_run_id INTEGER NOT NULL REFERENCES task_stage_runs(id),
            to_stage_run_id INTEGER NOT NULL REFERENCES task_stage_runs(id),
            workflow_input_id INTEGER NOT NULL UNIQUE REFERENCES workflow_inputs(id),
            event TEXT NOT NULL CHECK (event IN (
                'planning_completed','execution_completed','validation_passed','validation_failed',
                'replan_requested'
            )),
            source_version INTEGER NOT NULL CHECK (source_version >= 0),
            handoff_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now'))
        );",
    )?;
    if tx.prepare("PRAGMA foreign_key_check")?.exists([])? {
        return Err(StoreError::InvalidWorkflow(
            "foreign key check failed".into(),
        ));
    }
    tx.commit()?;
    Ok(())
}
