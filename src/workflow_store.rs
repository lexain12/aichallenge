use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::chat::{Message, Role};
use crate::client::TokenUsage;
use crate::dialog::{DialogStore, StoreError};
use crate::memory::RequestScope;
use crate::workflow::{
    PatchContext, StageRunId, StateMachine, TaskPhase, TaskStatePatch, TaskStatus, WorkflowInput,
    WorkflowInputSource, WorkflowIntent, WorkflowTaskId, WorkflowTaskState,
};

pub trait WorkflowRepository {
    fn lease_processing(
        &mut self,
        processing_id: i64,
        expected_version: u64,
        mode: ProcessingLeaseMode,
    ) -> Result<Option<ProcessingLease>, StoreError>;
    fn commit_await_user(
        &mut self,
        processing_id: i64,
        task_id: WorkflowTaskId,
        stage_run_id: StageRunId,
        expected_version: u64,
        patch: &TaskStatePatch,
    ) -> Result<ProcessingResult, StoreError>;
    fn commit_controller_decision(
        &mut self,
        command: ControllerInputCommit<'_>,
    ) -> Result<ProcessingResult, StoreError>;
    /// `sanitized_error` must be a safe diagnostic, never a raw provider response or payload.
    fn fail_processing(
        &mut self,
        processing_id: i64,
        expected_version: u64,
        sanitized_error: &str,
    ) -> Result<(), StoreError>;
    fn append_input(
        &mut self,
        command: InputCommit<'_>,
        effect: AcceptedInputEffect,
    ) -> Result<PersistedInput, StoreError>;
    fn append_answer_for_processing(
        &mut self,
        command: AnswerCommit<'_>,
    ) -> Result<PersistedAnswer, StoreError>;
    fn start_dialog_with_workflow_task(
        &mut self,
        scope: &RequestScope,
        system_prompt: &str,
        prompt: &str,
    ) -> Result<StartedWorkflow, StoreError>;
    fn create_task_with_human_input(
        &mut self,
        dialog_id: i64,
        protocol_text: &str,
        goal: &str,
        expected_version: u64,
    ) -> Result<StartedWorkflow, StoreError>;
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
pub struct StartedWorkflow {
    pub dialog_id: i64,
    pub task: WorkflowTaskState,
    pub stage_run_id: StageRunId,
    pub message_id: i64,
}

pub struct InputCommit<'a> {
    pub dialog_id: i64,
    pub input: &'a WorkflowInput,
    pub protocol_text: &'a str,
    pub confidence: Option<f32>,
    pub expected_version: Option<u64>,
}

#[derive(Clone, Debug)]
pub enum AcceptedInputEffect {
    ContinueSameStage,
    ResumeSameStage,
    RouteUnmanaged,
    Reject { reason: String },
}

#[derive(Clone, Debug)]
pub struct PersistedInput {
    pub message_id: i64,
    pub workflow_input_id: Option<i64>,
    pub task: Option<WorkflowTaskState>,
}

pub struct AnswerCommit<'a> {
    pub dialog_id: i64,
    pub task_id: WorkflowTaskId,
    pub stage_run_id: StageRunId,
    pub expected_version: u64,
    pub content: &'a str,
    pub usage: Option<TokenUsage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedAnswer {
    pub message_id: i64,
    pub processing_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessingLease {
    pub processing_id: i64,
    pub assistant_message_id: i64,
    pub expected_version: u64,
    pub attempts: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessingLeaseMode {
    Normal,
    Recovery,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessingResult {
    AwaitUser {
        task_version: u64,
    },
    ControllerInput {
        message_id: i64,
        workflow_input_id: i64,
        task_version: u64,
    },
}

pub struct ControllerInputCommit<'a> {
    pub processing_id: i64,
    pub task_id: WorkflowTaskId,
    pub stage_run_id: StageRunId,
    pub expected_version: u64,
    pub checker: &'a str,
    pub model: &'a str,
    pub triggering_assistant_message_id: i64,
    pub instruction: &'a str,
    pub intent: &'a WorkflowIntent,
    pub confidence: f32,
    pub accepted_patch: &'a TaskStatePatch,
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
    fn lease_processing(
        &mut self,
        processing_id: i64,
        expected_version: u64,
        mode: ProcessingLeaseMode,
    ) -> Result<Option<ProcessingLease>, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let processing = processing_row(&tx, processing_id)?;
        processing.check_identity(
            processing.task_id,
            processing.stage_run_id,
            expected_version,
        )?;
        if processing.status == ProcessingStatus::Completed || processing.attempts >= 2 {
            return Ok(None);
        }
        processing.current_task(&tx)?;
        if processing.status == ProcessingStatus::Processing && mode == ProcessingLeaseMode::Normal
        {
            return Ok(None);
        }
        let changed = tx.execute(
            "UPDATE response_processing SET status = 'processing', attempts = attempts + 1,
                    updated_at = strftime('%Y-%m-%d %H:%M:%f','now')
             WHERE id = ?1 AND expected_version = ?2 AND attempts < 2
               AND (status IN ('pending','failed') OR (status = 'processing' AND ?3))",
            params![
                processing_id,
                sqlite_version(expected_version)?,
                mode == ProcessingLeaseMode::Recovery
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::WorkflowConflict(processing.dialog_id));
        }
        tx.commit()?;
        Ok(Some(ProcessingLease {
            processing_id,
            assistant_message_id: processing.assistant_message_id,
            expected_version,
            attempts: processing.attempts + 1,
        }))
    }

    fn commit_await_user(
        &mut self,
        processing_id: i64,
        task_id: WorkflowTaskId,
        stage_run_id: StageRunId,
        expected_version: u64,
        patch: &TaskStatePatch,
    ) -> Result<ProcessingResult, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let processing = processing_row(&tx, processing_id)?;
        processing.check_identity(task_id, stage_run_id, expected_version)?;
        if patch.expected_version != expected_version {
            return Err(StoreError::WorkflowConflict(processing.dialog_id));
        }
        if let Some(result) = processing.completed_result()? {
            if !matches!(result, ProcessingResult::AwaitUser { .. }) {
                return Err(StoreError::WorkflowConflict(processing.dialog_id));
            }
            return Ok(result);
        }
        processing.require_leased()?;
        let task = processing.current_task(&tx)?;
        let projected = task
            .apply_patch(patch, PatchContext::Normal)
            .map_err(domain_error)?;
        save_task(&tx, &projected, expected_version)
            .map_err(|error| workflow_conflict(error, task.dialog_id))?;
        let result = ProcessingResult::AwaitUser {
            task_version: projected.version,
        };
        complete_processing(&tx, &processing, &result)?;
        tx.commit()?;
        Ok(result)
    }

    fn commit_controller_decision(
        &mut self,
        command: ControllerInputCommit<'_>,
    ) -> Result<ProcessingResult, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let processing = processing_row(&tx, command.processing_id)?;
        let result = commit_controller(&tx, &processing, &command)
            .map_err(|error| workflow_conflict(error, processing.dialog_id))?;
        tx.commit()?;
        Ok(result)
    }

    fn fail_processing(
        &mut self,
        processing_id: i64,
        expected_version: u64,
        sanitized_error: &str,
    ) -> Result<(), StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_protocol_text(sanitized_error)?;
        let processing = processing_row(&tx, processing_id)?;
        processing.check_identity(
            processing.task_id,
            processing.stage_run_id,
            expected_version,
        )?;
        processing.require_leased()?;
        processing.current_task(&tx)?;
        let changed = tx.execute(
            "UPDATE response_processing SET status = 'failed', last_error = ?1,
                    updated_at = strftime('%Y-%m-%d %H:%M:%f','now')
             WHERE id = ?2 AND status = 'processing' AND expected_version = ?3",
            params![
                sanitized_error,
                processing_id,
                sqlite_version(expected_version)?
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::WorkflowConflict(processing.dialog_id));
        }
        tx.commit()?;
        Ok(())
    }
    fn append_input(
        &mut self,
        command: InputCommit<'_>,
        effect: AcceptedInputEffect,
    ) -> Result<PersistedInput, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = append_input(&tx, &command, effect)
            .map_err(|error| workflow_conflict(error, command.dialog_id))?;
        tx.commit()?;
        Ok(result)
    }

    fn append_answer_for_processing(
        &mut self,
        command: AnswerCommit<'_>,
    ) -> Result<PersistedAnswer, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = append_answer(&tx, &command)
            .map_err(|error| workflow_conflict(error, command.dialog_id))?;
        tx.commit()?;
        Ok(result)
    }
    fn start_dialog_with_workflow_task(
        &mut self,
        scope: &RequestScope,
        system_prompt: &str,
        prompt: &str,
    ) -> Result<StartedWorkflow, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_protocol_text(prompt)?;
        tx.execute(
            "INSERT INTO dialogs (system_prompt, title) VALUES (?1, ?2)",
            params![system_prompt, prompt.chars().take(60).collect::<String>()],
        )?;
        let dialog_id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO dialog_scopes (dialog_id, user_id, task_id) VALUES (?1, ?2, ?3)",
            params![dialog_id, scope.user_id(), scope.task_id()],
        )?;
        let result = create_task(&tx, dialog_id, prompt, prompt, 0)
            .map_err(|error| workflow_conflict(error, dialog_id))?;
        tx.commit()?;
        Ok(result)
    }

    fn create_task_with_human_input(
        &mut self,
        dialog_id: i64,
        protocol_text: &str,
        goal: &str,
        expected_version: u64,
    ) -> Result<StartedWorkflow, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = create_task(&tx, dialog_id, protocol_text, goal, expected_version)
            .map_err(|error| workflow_conflict(error, dialog_id))?;
        tx.commit()?;
        Ok(result)
    }
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

fn create_task(
    connection: &Connection,
    dialog_id: i64,
    protocol_text: &str,
    goal: &str,
    expected_version: u64,
) -> Result<StartedWorkflow, StoreError> {
    validate_protocol_text(protocol_text)?;
    let current = load_workflow(connection, dialog_id)?.current_task;
    if current.as_ref().map_or(0, |task| task.version) != expected_version {
        return Err(StoreError::WorkflowConflict(dialog_id));
    }
    StateMachine::validate_new_task(current.as_ref())
        .map_err(|_| StoreError::WorkflowConflict(dialog_id))?;
    let ordinal: i64 = connection.query_row(
        "SELECT coalesce(max(ordinal), 0) + 1 FROM workflow_tasks WHERE dialog_id = ?1",
        [dialog_id],
        |row| row.get(0),
    )?;
    // Temporary positive IDs validate the initial projection before the circular rows exist.
    let mut task = WorkflowTaskState::new(
        WorkflowTaskId(1),
        dialog_id,
        unsigned(ordinal, "task ordinal")?,
        goal.to_owned(),
        StageRunId(1),
    )
    .map_err(domain_error)?;
    connection.execute(
        "INSERT INTO workflow_tasks (dialog_id, ordinal, phase, status, goal, plan_json, checkpoint_json, version)
         VALUES (?1, ?2, 'planning', 'active', ?3, ?4, ?5, 0)",
        params![dialog_id, ordinal, goal, serde_json::to_string(&task.plan)?, serde_json::to_string(&task.checkpoint)?],
    )?;
    task.id = WorkflowTaskId(connection.last_insert_rowid());
    connection.execute("INSERT INTO task_stage_runs (workflow_task_id, phase, sequence) VALUES (?1, 'planning', 1)", [task.id.0])?;
    task.current_stage_run_id = StageRunId(connection.last_insert_rowid());
    let changed = connection.execute(
        "UPDATE workflow_tasks SET current_stage_run_id = ?1 WHERE id = ?2",
        params![task.current_stage_run_id.0, task.id.0],
    )?;
    if changed != 1 {
        return Err(StoreError::WorkflowConflict(dialog_id));
    }
    let message_id = insert_message(connection, dialog_id, "user", protocol_text)?;
    let input = WorkflowInput {
        source: WorkflowInputSource::Human,
        intent: WorkflowIntent::StartNewTask {
            goal: goal.to_owned(),
        },
    };
    connection.execute(
        "INSERT INTO workflow_inputs (dialog_id, message_id, source, intent_json, outcome)
         VALUES (?1, ?2, 'human', ?3, 'accepted')",
        params![dialog_id, message_id, serde_json::to_string(&input.intent)?],
    )?;
    map_message(connection, message_id, &task)?;
    connection.execute(
        "INSERT INTO dialog_workflow_state (dialog_id, current_task_id) VALUES (?1, ?2)
         ON CONFLICT(dialog_id) DO UPDATE SET current_task_id = excluded.current_task_id",
        params![dialog_id, task.id.0],
    )?;
    touch_dialog(connection, dialog_id, message_id)?;
    Ok(StartedWorkflow {
        dialog_id,
        stage_run_id: task.current_stage_run_id,
        task,
        message_id,
    })
}

struct ProcessingRow {
    id: i64,
    assistant_message_id: i64,
    checker: String,
    expected_version: u64,
    status: ProcessingStatus,
    attempts: u32,
    result_json: Option<String>,
    dialog_id: i64,
    task_id: WorkflowTaskId,
    stage_run_id: StageRunId,
}

impl ProcessingRow {
    fn check_identity(
        &self,
        task_id: WorkflowTaskId,
        stage_run_id: StageRunId,
        version: u64,
    ) -> Result<(), StoreError> {
        if self.task_id != task_id
            || self.stage_run_id != stage_run_id
            || self.expected_version != version
        {
            return Err(StoreError::WorkflowConflict(self.dialog_id));
        }
        Ok(())
    }

    fn current_task(&self, connection: &Connection) -> Result<WorkflowTaskState, StoreError> {
        let task = current_task(
            connection,
            self.dialog_id,
            self.task_id,
            self.stage_run_id,
            self.expected_version,
        )?;
        active_task(&task)?;
        Ok(task)
    }

    fn require_leased(&self) -> Result<(), StoreError> {
        if self.status != ProcessingStatus::Processing || !(1..=2).contains(&self.attempts) {
            return Err(StoreError::WorkflowConflict(self.dialog_id));
        }
        Ok(())
    }

    fn completed_result(&self) -> Result<Option<ProcessingResult>, StoreError> {
        if self.status != ProcessingStatus::Completed {
            return Ok(None);
        }
        let result: ProcessingResult = json(self.result_json.as_deref().ok_or_else(|| {
            StoreError::InvalidWorkflow("completed processing has no result".into())
        })?)?;
        match &result {
            ProcessingResult::AwaitUser { task_version } => {
                if *task_version != self.expected_version
                    && *task_version != next_version(self.expected_version)?
                {
                    return invalid("invalid completion version");
                }
            }
            ProcessingResult::ControllerInput {
                message_id,
                workflow_input_id,
                task_version,
            } => {
                positive(*message_id, "controller message id")?;
                positive(*workflow_input_id, "controller input id")?;
                if *task_version != next_version(self.expected_version)? {
                    return invalid("invalid controller completion version");
                }
            }
        }
        Ok(Some(result))
    }
}

fn processing_row(connection: &Connection, id: i64) -> Result<ProcessingRow, StoreError> {
    positive(id, "processing id")?;
    let mut statement = connection.prepare(
        "SELECT p.assistant_message_id, p.checker_name, p.expected_version, p.status,
                p.attempts, p.result_json, m.dialog_id, ms.workflow_task_id,
                ms.stage_run_id, m.role, t.dialog_id, s.workflow_task_id
         FROM response_processing p
         LEFT JOIN messages m ON m.id = p.assistant_message_id
         LEFT JOIN message_task_stages ms ON ms.message_id = m.id
         LEFT JOIN workflow_tasks t ON t.id = ms.workflow_task_id
         LEFT JOIN task_stage_runs s ON s.id = ms.stage_run_id WHERE p.id = ?1",
    )?;
    let mut rows = statement.query([id])?;
    let row = rows
        .next()?
        .ok_or_else(|| StoreError::InvalidWorkflow("processing row missing".into()))?;
    if field::<String>(row, 9)? != "assistant"
        || integer(row, 6)? != integer(row, 10)?
        || integer(row, 7)? != integer(row, 11)?
    {
        return invalid("processing ownership or role mismatch");
    }
    let checker: String = field(row, 1)?;
    validate_protocol_text(&checker)?;
    Ok(ProcessingRow {
        id,
        assistant_message_id: positive(integer(row, 0)?, "assistant message id")?,
        checker,
        expected_version: unsigned(integer(row, 2)?, "processing version")?,
        status: text_enum(row, 3)?,
        attempts: unsigned(integer(row, 4)?, "processing attempts")?,
        result_json: field(row, 5)?,
        dialog_id: positive(integer(row, 6)?, "dialog id")?,
        task_id: WorkflowTaskId(positive(integer(row, 7)?, "task id")?),
        stage_run_id: StageRunId(positive(integer(row, 8)?, "stage id")?),
    })
}

fn complete_processing(
    connection: &Connection,
    processing: &ProcessingRow,
    result: &ProcessingResult,
) -> Result<(), StoreError> {
    let changed = connection.execute(
        "UPDATE response_processing SET status = 'completed', result_json = ?1, last_error = NULL,
                updated_at = strftime('%Y-%m-%d %H:%M:%f','now')
         WHERE id = ?2 AND status = 'processing' AND expected_version = ?3",
        params![
            serde_json::to_string(result)?,
            processing.id,
            sqlite_version(processing.expected_version)?
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::WorkflowConflict(processing.dialog_id));
    }
    Ok(())
}

fn commit_controller(
    connection: &Connection,
    processing: &ProcessingRow,
    command: &ControllerInputCommit<'_>,
) -> Result<ProcessingResult, StoreError> {
    processing.check_identity(
        command.task_id,
        command.stage_run_id,
        command.expected_version,
    )?;
    validate_protocol_text(command.instruction)?;
    validate_confidence(Some(command.confidence))?;
    let source = WorkflowInputSource::Controller {
        checker: command.checker.to_owned(),
        model: command.model.to_owned(),
        triggering_assistant_message_id: command.triggering_assistant_message_id,
    };
    StateMachine::validate_source(&source, command.intent).map_err(domain_error)?;
    if processing.assistant_message_id != command.triggering_assistant_message_id
        || processing.checker != command.checker
    {
        return Err(StoreError::WorkflowConflict(processing.dialog_id));
    }
    if !matches!(command.intent, WorkflowIntent::Continue { instruction } if instruction == command.instruction)
    {
        return invalid("same-stage controller completion requires matching Continue instruction");
    }
    if command.accepted_patch.expected_version != command.expected_version {
        return Err(StoreError::WorkflowConflict(processing.dialog_id));
    }
    if let Some(result) = processing.completed_result()? {
        let ProcessingResult::ControllerInput {
            message_id,
            workflow_input_id,
            ..
        } = result
        else {
            return Err(StoreError::WorkflowConflict(processing.dialog_id));
        };
        let matches: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_inputs i
             JOIN messages m ON m.id = i.message_id
             JOIN message_task_stages ms ON ms.message_id = m.id
             WHERE i.id = ?1 AND i.message_id = ?2 AND i.processing_id = ?3
               AND i.source = 'controller' AND i.dialog_id = ?4 AND m.dialog_id = ?4
               AND m.role = 'user' AND m.content = ?5 AND i.checker_name = ?6
               AND i.model_name = ?7 AND i.triggering_assistant_message_id = ?8
               AND i.intent_json = ?9 AND i.confidence = ?10 AND i.outcome = 'accepted'
               AND ms.workflow_task_id = ?11 AND ms.stage_run_id = ?12)",
            params![
                workflow_input_id,
                message_id,
                processing.id,
                processing.dialog_id,
                command.instruction,
                command.checker,
                command.model,
                command.triggering_assistant_message_id,
                serde_json::to_string(command.intent)?,
                command.confidence,
                command.task_id.0,
                command.stage_run_id.0
            ],
            |row| row.get(0),
        )?;
        if !matches {
            return Err(StoreError::WorkflowConflict(processing.dialog_id));
        }
        return Ok(result);
    }
    processing.require_leased()?;
    let task = processing.current_task(connection)?;
    let mut projected = task
        .preview_patch(command.accepted_patch, PatchContext::Normal)
        .map_err(domain_error)?;
    projected.version = next_version(task.version)?;
    save_task(connection, &projected, task.version)?;
    let message_id = insert_message(connection, task.dialog_id, "user", command.instruction)?;
    connection.execute(
        "INSERT INTO workflow_inputs (dialog_id, message_id, source, checker_name, model_name,
                triggering_assistant_message_id, intent_json, confidence, outcome, processing_id)
         VALUES (?1, ?2, 'controller', ?3, ?4, ?5, ?6, ?7, 'accepted', ?8)",
        params![
            task.dialog_id,
            message_id,
            command.checker,
            command.model,
            command.triggering_assistant_message_id,
            serde_json::to_string(command.intent)?,
            command.confidence,
            processing.id
        ],
    )?;
    let workflow_input_id = connection.last_insert_rowid();
    map_message(connection, message_id, &projected)?;
    let result = ProcessingResult::ControllerInput {
        message_id,
        workflow_input_id,
        task_version: projected.version,
    };
    complete_processing(connection, processing, &result)?;
    touch_dialog(connection, task.dialog_id, message_id)?;
    Ok(result)
}

fn append_input(
    connection: &Connection,
    command: &InputCommit<'_>,
    effect: AcceptedInputEffect,
) -> Result<PersistedInput, StoreError> {
    validate_protocol_text(command.protocol_text)?;
    validate_confidence(command.confidence)?;
    if command.input.source != WorkflowInputSource::Human {
        return invalid("controller inputs require a processing completion");
    }
    let mut task = load_workflow(connection, command.dialog_id)?.current_task;
    if task.as_ref().map(|task| task.version) != command.expected_version {
        return Err(StoreError::WorkflowConflict(command.dialog_id));
    }
    let rejection_reason = match &effect {
        AcceptedInputEffect::RouteUnmanaged => {
            if task
                .as_ref()
                .is_some_and(|task| task.phase != TaskPhase::Done)
            {
                return invalid("unfinished task cannot route unmanaged");
            }
            None
        }
        AcceptedInputEffect::Reject { reason } => {
            command.input.validate().map_err(domain_error)?;
            validate_protocol_text(reason)?;
            Some(reason.as_str())
        }
        AcceptedInputEffect::ContinueSameStage | AcceptedInputEffect::ResumeSameStage => {
            command.input.validate().map_err(domain_error)?;
            if !matches!(command.input.intent, WorkflowIntent::Continue { .. }) {
                return invalid("same-stage input requires Continue");
            }
            let state = task
                .as_mut()
                .ok_or(StoreError::WorkflowConflict(command.dialog_id))?;
            if state.phase == TaskPhase::Done {
                return invalid("completed task cannot continue");
            }
            let previous = state.version;
            if matches!(effect, AcceptedInputEffect::ResumeSameStage) {
                *state = state.clone().resume().map_err(domain_error)?;
            } else if state.status != TaskStatus::Active {
                return invalid("paused task requires human resume");
            }
            state.version = next_version(previous)?;
            save_task(connection, state, previous)?;
            None
        }
    };
    let message_id = insert_message(connection, command.dialog_id, "user", command.protocol_text)?;
    let workflow_input_id = if matches!(effect, AcceptedInputEffect::RouteUnmanaged) {
        None
    } else {
        connection.execute(
            "INSERT INTO workflow_inputs (dialog_id, message_id, source, intent_json, confidence,
                    outcome, rejection_reason) VALUES (?1, ?2, 'human', ?3, ?4, ?5, ?6)",
            params![
                command.dialog_id,
                message_id,
                serde_json::to_string(&command.input.intent)?,
                command.confidence,
                if rejection_reason.is_some() {
                    "rejected"
                } else {
                    "accepted"
                },
                rejection_reason
            ],
        )?;
        Some(connection.last_insert_rowid())
    };
    if matches!(
        effect,
        AcceptedInputEffect::ContinueSameStage | AcceptedInputEffect::ResumeSameStage
    ) {
        map_message(
            connection,
            message_id,
            task.as_ref().expect("managed input has a task"),
        )?;
    }
    touch_dialog(connection, command.dialog_id, message_id)?;
    Ok(PersistedInput {
        message_id,
        workflow_input_id,
        task,
    })
}

fn append_answer(
    connection: &Connection,
    command: &AnswerCommit<'_>,
) -> Result<PersistedAnswer, StoreError> {
    let task = current_task(
        connection,
        command.dialog_id,
        command.task_id,
        command.stage_run_id,
        command.expected_version,
    )?;
    active_task(&task)?;
    let message_id = insert_message(connection, command.dialog_id, "assistant", command.content)?;
    if let Some(usage) = &command.usage {
        connection.execute(
            "INSERT INTO message_usage (message_id, usage_json) VALUES (?1, ?2)",
            params![message_id, serde_json::to_string(usage)?],
        )?;
    }
    map_message(connection, message_id, &task)?;
    connection.execute(
        "INSERT INTO response_processing (assistant_message_id, checker_name, expected_version, status, attempts)
         VALUES (?1, 'continuation', ?2, 'pending', 0)",
        params![message_id, sqlite_version(command.expected_version)?],
    )?;
    let processing_id = connection.last_insert_rowid();
    touch_dialog(connection, command.dialog_id, message_id)?;
    Ok(PersistedAnswer {
        message_id,
        processing_id,
    })
}

fn validate_confidence(confidence: Option<f32>) -> Result<(), StoreError> {
    if confidence.is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)) {
        return invalid("invalid confidence");
    }
    Ok(())
}

fn sqlite_version(version: u64) -> Result<i64, StoreError> {
    i64::try_from(version)
        .map_err(|_| StoreError::InvalidWorkflow("version exceeds SQLite INTEGER".into()))
}

fn next_version(version: u64) -> Result<u64, StoreError> {
    let next = version
        .checked_add(1)
        .ok_or_else(|| StoreError::InvalidWorkflow("version overflow".into()))?;
    sqlite_version(next)?;
    Ok(next)
}

fn active_task(task: &WorkflowTaskState) -> Result<(), StoreError> {
    if task.status != TaskStatus::Active || task.phase == TaskPhase::Done {
        return invalid("processing requires an active unfinished task");
    }
    Ok(())
}

fn current_task(
    connection: &Connection,
    dialog_id: i64,
    task_id: WorkflowTaskId,
    stage_run_id: StageRunId,
    expected_version: u64,
) -> Result<WorkflowTaskState, StoreError> {
    let task = load_workflow(connection, dialog_id)?
        .current_task
        .ok_or(StoreError::WorkflowConflict(dialog_id))?;
    if task.id != task_id
        || task.current_stage_run_id != stage_run_id
        || task.version != expected_version
    {
        return Err(StoreError::WorkflowConflict(dialog_id));
    }
    Ok(task)
}

fn save_task(
    connection: &Connection,
    task: &WorkflowTaskState,
    expected_version: u64,
) -> Result<(), StoreError> {
    task.validate().map_err(domain_error)?;
    let changed = connection.execute(
        "UPDATE workflow_tasks SET status = ?1, plan_json = ?2, current_step_id = ?3,
                expected_action = ?4, checkpoint_json = ?5, version = ?6,
                updated_at = strftime('%Y-%m-%d %H:%M:%f','now')
         WHERE id = ?7 AND dialog_id = ?8 AND current_stage_run_id = ?9 AND version = ?10",
        params![
            if task.status == TaskStatus::Active {
                "active"
            } else {
                "paused"
            },
            serde_json::to_string(&task.plan)?,
            task.current_step_id,
            task.expected_action,
            serde_json::to_string(&task.checkpoint)?,
            sqlite_version(task.version)?,
            task.id.0,
            task.dialog_id,
            task.current_stage_run_id.0,
            sqlite_version(expected_version)?
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::WorkflowConflict(task.dialog_id));
    }
    Ok(())
}

fn domain_error(error: crate::workflow::WorkflowError) -> StoreError {
    StoreError::InvalidWorkflow(error.to_string())
}

fn validate_protocol_text(text: &str) -> Result<(), StoreError> {
    WorkflowIntent::human_continue(text)
        .map(|_| ())
        .map_err(domain_error)
}

fn insert_message(
    connection: &Connection,
    dialog_id: i64,
    role: &str,
    text: &str,
) -> Result<i64, StoreError> {
    connection.execute(
        "INSERT INTO messages (dialog_id, role, content) VALUES (?1, ?2, ?3)",
        params![dialog_id, role, text],
    )?;
    Ok(connection.last_insert_rowid())
}

fn map_message(
    connection: &Connection,
    message_id: i64,
    task: &WorkflowTaskState,
) -> Result<(), StoreError> {
    connection.execute("INSERT INTO message_task_stages (message_id, workflow_task_id, stage_run_id) VALUES (?1, ?2, ?3)", params![message_id, task.id.0, task.current_stage_run_id.0])?;
    Ok(())
}

fn touch_dialog(
    connection: &Connection,
    dialog_id: i64,
    message_id: i64,
) -> Result<(), StoreError> {
    if connection.execute("UPDATE dialogs SET last_message_id = ?1, updated_at = strftime('%Y-%m-%d %H:%M:%f','now') WHERE id = ?2", params![message_id, dialog_id])? != 1 {
        return Err(StoreError::WorkflowConflict(dialog_id));
    }
    Ok(())
}

fn workflow_conflict(error: StoreError, dialog_id: i64) -> StoreError {
    match error {
        StoreError::Database(rusqlite::Error::SqliteFailure(ref code, _))
            if matches!(
                code.extended_code,
                rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                    | rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
            ) =>
        {
            StoreError::WorkflowConflict(dialog_id)
        }
        other => other,
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
