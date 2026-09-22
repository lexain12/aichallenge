use std::collections::BTreeMap;

use rusqlite::{
    Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params, types::Value,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::chat::{Message, Role};
use crate::client::TokenUsage;
use crate::context::{ContextState, ContextSummary};
use crate::dialog::{DialogStore, StoreError};
use crate::facts::{Facts, FactsState};
use crate::memory::RequestScope;
use crate::workflow::{
    PatchContext, StageChangeAuthorization, StageRunId, StateMachine, TaskPhase, TaskStatePatch,
    TaskStatus, TransitionEvent, WorkflowInput, WorkflowInputSource, WorkflowIntent,
    WorkflowTaskId, WorkflowTaskState,
};
use crate::workflow_context::{StageReductionState, facts_candidates};
use crate::workflow_model::{HandoffPayload, project_handoff};

pub trait WorkflowRepository {
    fn load_stage_reductions(
        &self,
        stage_run_id: StageRunId,
    ) -> Result<StageReductionState, StoreError>;
    fn replace_stage_context(
        &mut self,
        stage_run_id: StageRunId,
        expected_task_version: u64,
        expected_stage_message_count: usize,
        summary: ContextSummary,
        usage: Option<TokenUsage>,
    ) -> Result<ContextState, StoreError>;
    fn replace_stage_facts(
        &mut self,
        stage_run_id: StageRunId,
        expected_task_version: u64,
        expected_stage_message_count: usize,
        facts: Facts,
        usage: Option<TokenUsage>,
    ) -> Result<FactsState, StoreError>;
    fn copy_workflow_branch(
        tx: &Transaction<'_>,
        source_dialog_id: i64,
        target_dialog_id: i64,
        message_id_map: &BTreeMap<i64, i64>,
    ) -> Result<(), StoreError>;
    fn commit_stage_change(
        &mut self,
        command: TransitionCommit<'_>,
    ) -> Result<PersistedTransition, StoreError>;
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
    fn append_unmanaged_answer(
        &mut self,
        command: UnmanagedAnswerCommit<'_>,
    ) -> Result<i64, StoreError>;
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
        expected_current_task: ExpectedCurrentTask,
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

pub struct TransitionCommit<'a> {
    pub dialog_id: i64,
    pub source_task: &'a WorkflowTaskState,
    pub authorization: &'a StageChangeAuthorization,
    pub triggering_input: &'a WorkflowInput,
    pub protocol_text: &'a str,
    pub confidence: Option<f32>,
    pub accepted_patch: Option<&'a TaskStatePatch>,
    pub handoff: &'a HandoffPayload,
    pub processing_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedTransition {
    pub transition_id: i64,
    pub input_message_id: i64,
    pub target_state: WorkflowTaskState,
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
    pub expected_current_task: ExpectedCurrentTask,
}

/// Snapshot identity used by commands that route through a dialog's selected task.
/// Versions are local to a task, so the task ID is part of the concurrency guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedCurrentTask {
    Absent,
    Present {
        task_id: WorkflowTaskId,
        version: u64,
    },
}

impl ExpectedCurrentTask {
    fn matches(self, current: Option<&WorkflowTaskState>) -> bool {
        match (self, current) {
            (Self::Absent, None) => true,
            (Self::Present { task_id, version }, Some(task)) => {
                task.id == task_id && task.version == version
            }
            _ => false,
        }
    }
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

pub struct UnmanagedAnswerCommit<'a> {
    pub dialog_id: i64,
    pub input_message_id: i64,
    pub expected_current_task: ExpectedCurrentTask,
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
    Transition {
        transition_id: i64,
        input_message_id: i64,
        workflow_input_id: i64,
        target_state: WorkflowTaskState,
    },
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
    fn load_stage_reductions(
        &self,
        stage_run_id: StageRunId,
    ) -> Result<StageReductionState, StoreError> {
        load_stage_reductions(&self.connection, stage_run_id)
    }

    fn replace_stage_context(
        &mut self,
        stage_run_id: StageRunId,
        expected_task_version: u64,
        expected_stage_message_count: usize,
        summary: ContextSummary,
        usage: Option<TokenUsage>,
    ) -> Result<ContextState, StoreError> {
        if summary.covered_message_count() == 0
            || summary.covered_message_count() > expected_stage_message_count
        {
            return Err(StoreError::InvalidContext(
                "summary boundary must cover an existing non-empty stage prefix",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (dialog_id, _) = guard_stage_reduction(
            &tx,
            stage_run_id,
            expected_task_version,
            expected_stage_message_count,
        )?;
        let mut state = load_stage_reductions(&tx, stage_run_id)?;
        state.context.replace_summary(summary, usage);
        require_one(tx.execute(
            "INSERT INTO task_stage_context (stage_run_id,context_json,facts_json) VALUES (?1,?2,?3)
             ON CONFLICT(stage_run_id) DO UPDATE SET context_json=excluded.context_json,
             updated_at=strftime('%Y-%m-%d %H:%M:%f','now')",
            params![stage_run_id.0, serde_json::to_string(&state.context)?, serde_json::to_string(&state.facts)?],
        )?, dialog_id)?;
        tx.commit()?;
        Ok(state.context)
    }

    fn replace_stage_facts(
        &mut self,
        stage_run_id: StageRunId,
        expected_task_version: u64,
        expected_stage_message_count: usize,
        facts: Facts,
        usage: Option<TokenUsage>,
    ) -> Result<FactsState, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (dialog_id, messages) = guard_stage_reduction(
            &tx,
            stage_run_id,
            expected_task_version,
            expected_stage_message_count,
        )?;
        let mut state = load_stage_reductions(&tx, stage_run_id)?;
        state.facts = state
            .facts
            .updated(facts, facts_candidates(&messages).len(), usage);
        require_one(tx.execute(
            "INSERT INTO task_stage_context (stage_run_id,context_json,facts_json) VALUES (?1,?2,?3)
             ON CONFLICT(stage_run_id) DO UPDATE SET facts_json=excluded.facts_json,
             updated_at=strftime('%Y-%m-%d %H:%M:%f','now')",
            params![stage_run_id.0, serde_json::to_string(&state.context)?, serde_json::to_string(&state.facts)?],
        )?, dialog_id)?;
        tx.commit()?;
        Ok(state.facts)
    }
    fn copy_workflow_branch(
        tx: &Transaction<'_>,
        source_dialog_id: i64,
        target_dialog_id: i64,
        message_id_map: &BTreeMap<i64, i64>,
    ) -> Result<(), StoreError> {
        copy_workflow_branch(tx, source_dialog_id, target_dialog_id, message_id_map)
    }
    fn commit_stage_change(
        &mut self,
        command: TransitionCommit<'_>,
    ) -> Result<PersistedTransition, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = commit_stage_change(&tx, &command)
            .map_err(|error| workflow_conflict(error, command.dialog_id))?;
        tx.commit()?;
        Ok(result)
    }
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

    fn append_unmanaged_answer(
        &mut self,
        command: UnmanagedAnswerCommit<'_>,
    ) -> Result<i64, StoreError> {
        if command.content.trim().is_empty() {
            return invalid("assistant answer must not be blank");
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = load_workflow(&tx, command.dialog_id)?.current_task;
        if !command.expected_current_task.matches(task.as_ref())
            || !task
                .as_ref()
                .is_some_and(|task| task.phase == TaskPhase::Done)
        {
            return Err(StoreError::WorkflowConflict(command.dialog_id));
        }
        let valid_input: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages m
             WHERE m.id=?1 AND m.dialog_id=?2 AND m.role='user'
               AND NOT EXISTS(SELECT 1 FROM workflow_inputs i WHERE i.message_id=m.id)
               AND NOT EXISTS(SELECT 1 FROM message_task_stages s WHERE s.message_id=m.id)
               AND m.id=(SELECT max(id) FROM messages WHERE dialog_id=?2))",
            params![command.input_message_id, command.dialog_id],
            |row| row.get(0),
        )?;
        if !valid_input {
            return Err(StoreError::WorkflowConflict(command.dialog_id));
        }
        let message_id = insert_message(&tx, command.dialog_id, "assistant", command.content)?;
        if let Some(usage) = command.usage {
            require_one(
                tx.execute(
                    "INSERT INTO message_usage (message_id,usage_json) VALUES (?1,?2)",
                    params![message_id, serde_json::to_string(&usage)?],
                )?,
                command.dialog_id,
            )?;
        }
        touch_dialog(&tx, command.dialog_id, message_id)?;
        tx.commit()?;
        Ok(message_id)
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
        let result = create_task(&tx, dialog_id, prompt, prompt, ExpectedCurrentTask::Absent)
            .map_err(|error| workflow_conflict(error, dialog_id))?;
        tx.commit()?;
        Ok(result)
    }

    fn create_task_with_human_input(
        &mut self,
        dialog_id: i64,
        protocol_text: &str,
        goal: &str,
        expected_current_task: ExpectedCurrentTask,
    ) -> Result<StartedWorkflow, StoreError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = create_task(&tx, dialog_id, protocol_text, goal, expected_current_task)
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
        load_stage_messages(&self.connection, stage_run_id)
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

type IdMap = BTreeMap<i64, i64>;

fn mapped_id(map: &IdMap, old: i64) -> Result<i64, StoreError> {
    map.get(&old).copied().ok_or_else(|| {
        StoreError::InvalidWorkflow(format!("branch reference {old} is outside copied history"))
    })
}

fn remap_field(
    row: &mut BTreeMap<String, Value>,
    name: &str,
    map: &IdMap,
) -> Result<(), StoreError> {
    match row.get_mut(name) {
        Some(Value::Integer(id)) => {
            *id = mapped_id(map, *id)?;
            Ok(())
        }
        Some(Value::Null) => Ok(()),
        _ => invalid("invalid branch reference"),
    }
}

// Table names, columns and filters are application-owned constants. Every reference
// is rewritten explicitly by the caller; missing mappings fail the whole fork.
fn copy_rows(
    connection: &Connection,
    table: &str,
    columns: &[&str],
    filter: &str,
    source_dialog: i64,
    mut rewrite: impl FnMut(&mut BTreeMap<String, Value>) -> Result<(), StoreError>,
) -> Result<IdMap, StoreError> {
    let rows = {
        let mut statement = connection.prepare(&format!(
            "SELECT id, {} FROM {table} WHERE {filter} ORDER BY id",
            columns.join(",")
        ))?;
        statement
            .query_map([source_dialog], |row| {
                let fields = columns
                    .iter()
                    .enumerate()
                    .map(|(i, name)| Ok(((*name).to_owned(), row.get::<_, Value>(i + 1)?)))
                    .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
                Ok((row.get::<_, i64>(0)?, fields))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let sql = format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        columns.join(","),
        vec!["?"; columns.len()].join(",")
    );
    let mut map = IdMap::new();
    for (old, mut row) in rows {
        rewrite(&mut row)?;
        if connection.execute(
            &sql,
            rusqlite::params_from_iter(columns.iter().map(|name| &row[*name])),
        )? != 1
        {
            return invalid("branch insertion did not insert one row");
        }
        map.insert(old, connection.last_insert_rowid());
    }
    Ok(map)
}

pub(crate) fn copy_workflow_branch(
    tx: &Transaction<'_>,
    source_dialog_id: i64,
    target_dialog_id: i64,
    message_id_map: &IdMap,
) -> Result<(), StoreError> {
    let tasks = copy_rows(
        tx,
        "workflow_tasks",
        &[
            "dialog_id",
            "ordinal",
            "phase",
            "status",
            "goal",
            "plan_json",
            "current_step_id",
            "expected_action",
            "checkpoint_json",
            "current_stage_run_id",
            "incoming_handoff_id",
            "version",
            "created_at",
            "updated_at",
        ],
        "dialog_id=?1",
        source_dialog_id,
        |row| {
            row.insert("dialog_id".into(), Value::Integer(target_dialog_id));
            row.insert("current_stage_run_id".into(), Value::Null);
            row.insert("incoming_handoff_id".into(), Value::Null);
            Ok(())
        },
    )?;
    let stages = copy_rows(
        tx,
        "task_stage_runs",
        &[
            "workflow_task_id",
            "phase",
            "sequence",
            "started_at",
            "finished_at",
        ],
        "workflow_task_id IN (SELECT id FROM workflow_tasks WHERE dialog_id=?1)",
        source_dialog_id,
        |row| remap_field(row, "workflow_task_id", &tasks),
    )?;
    for (old, new) in &stages {
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM task_stage_context WHERE stage_run_id=?1)",
            [old],
            |r| r.get(0),
        )?;
        if exists {
            require_one(tx.execute("INSERT INTO task_stage_context (stage_run_id,context_json,facts_json,updated_at) SELECT ?1,context_json,facts_json,updated_at FROM task_stage_context WHERE stage_run_id=?2", params![new,old])?, target_dialog_id)?;
        }
    }
    for (old, new) in message_id_map {
        let mapping: Option<(i64, i64)> = tx
            .query_row(
                "SELECT workflow_task_id,stage_run_id FROM message_task_stages WHERE message_id=?1",
                [old],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((task, stage)) = mapping {
            require_one(tx.execute("INSERT INTO message_task_stages (message_id,workflow_task_id,stage_run_id) VALUES (?1,?2,?3)", params![new,mapped_id(&tasks,task)?,mapped_id(&stages,stage)?])?,target_dialog_id)?;
        }
    }
    let processing = copy_rows(
        tx,
        "response_processing",
        &[
            "assistant_message_id",
            "checker_name",
            "expected_version",
            "status",
            "attempts",
            "result_json",
            "last_error",
            "created_at",
            "updated_at",
        ],
        "assistant_message_id IN (SELECT id FROM messages WHERE dialog_id=?1)",
        source_dialog_id,
        |row| {
            remap_field(row, "assistant_message_id", message_id_map)?;
            row.insert("result_json".into(), Value::Null);
            Ok(())
        },
    )?;
    let inputs = copy_rows(
        tx,
        "workflow_inputs",
        &[
            "dialog_id",
            "message_id",
            "source",
            "checker_name",
            "model_name",
            "triggering_assistant_message_id",
            "intent_json",
            "confidence",
            "outcome",
            "rejection_reason",
            "processing_id",
            "created_at",
        ],
        "dialog_id=?1",
        source_dialog_id,
        |row| {
            row.insert("dialog_id".into(), Value::Integer(target_dialog_id));
            remap_field(row, "message_id", message_id_map)?;
            remap_field(row, "triggering_assistant_message_id", message_id_map)?;
            remap_field(row, "processing_id", &processing)
        },
    )?;
    let transitions = copy_rows(
        tx,
        "task_transitions",
        &[
            "workflow_task_id",
            "from_stage_run_id",
            "to_stage_run_id",
            "workflow_input_id",
            "event",
            "source_version",
            "source_fingerprint",
            "handoff_json",
            "created_at",
        ],
        "workflow_task_id IN (SELECT id FROM workflow_tasks WHERE dialog_id=?1)",
        source_dialog_id,
        |row| {
            remap_field(row, "workflow_task_id", &tasks)?;
            remap_field(row, "from_stage_run_id", &stages)?;
            remap_field(row, "to_stage_run_id", &stages)?;
            remap_field(row, "workflow_input_id", &inputs)
        },
    )?;
    for (old, new) in &tasks {
        let (stage, handoff): (i64, Option<i64>) = tx.query_row(
            "SELECT current_stage_run_id,incoming_handoff_id FROM workflow_tasks WHERE id=?1",
            [old],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if tx.execute(
            "UPDATE workflow_tasks SET current_stage_run_id=?1,incoming_handoff_id=?2 WHERE id=?3",
            params![
                mapped_id(&stages, stage)?,
                handoff.map(|id| mapped_id(&transitions, id)).transpose()?,
                new
            ],
        )? != 1
        {
            return invalid("branch task update lost");
        }
    }
    for (old, new) in &processing {
        let (raw, status): (Option<String>, String) = tx.query_row(
            "SELECT result_json,status FROM response_processing WHERE id=?1",
            [old],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let copied = if let Some(raw) = raw {
            match serde_json::from_str::<ProcessingResult>(&raw) {
                Ok(mut result) => {
                    match &mut result {
                        ProcessingResult::AwaitUser { .. } => {}
                        ProcessingResult::ControllerInput {
                            message_id,
                            workflow_input_id,
                            ..
                        } => {
                            *message_id = mapped_id(message_id_map, *message_id)?;
                            *workflow_input_id = mapped_id(&inputs, *workflow_input_id)?;
                        }
                        ProcessingResult::Transition {
                            transition_id,
                            input_message_id,
                            workflow_input_id,
                            target_state,
                        } => {
                            *transition_id = mapped_id(&transitions, *transition_id)?;
                            *input_message_id = mapped_id(message_id_map, *input_message_id)?;
                            *workflow_input_id = mapped_id(&inputs, *workflow_input_id)?;
                            if target_state.dialog_id != source_dialog_id {
                                return invalid("transition replay belongs to another dialog");
                            }
                            target_state.dialog_id = target_dialog_id;
                            target_state.id = WorkflowTaskId(mapped_id(&tasks, target_state.id.0)?);
                            target_state.current_stage_run_id = StageRunId(mapped_id(
                                &stages,
                                target_state.current_stage_run_id.0,
                            )?);
                            target_state.incoming_handoff_id = target_state
                                .incoming_handoff_id
                                .map(|id| mapped_id(&transitions, id))
                                .transpose()?;
                        }
                    }
                    Some(serde_json::to_string(&result)?)
                }
                Err(_) if status != "completed" => {
                    json::<serde_json::Value>(&raw)?;
                    Some(raw)
                }
                Err(_) => return invalid("invalid completed processing result in branch"),
            }
        } else {
            None
        };
        if tx.execute(
            "UPDATE response_processing SET result_json=?1 WHERE id=?2",
            params![copied, new],
        )? != 1
        {
            return invalid("branch processing update lost");
        }
    }
    let selected: Option<i64> = tx
        .query_row(
            "SELECT current_task_id FROM dialog_workflow_state WHERE dialog_id=?1",
            [source_dialog_id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(selected) = selected {
        require_one(
            tx.execute(
                "INSERT INTO dialog_workflow_state (dialog_id,current_task_id) VALUES (?1,?2)",
                params![target_dialog_id, mapped_id(&tasks, selected)?],
            )?,
            target_dialog_id,
        )?;
        load_workflow(tx, target_dialog_id)?;
    }
    Ok(())
}

fn initialize_stage_context(connection: &Connection, stage: StageRunId) -> Result<(), StoreError> {
    let changed = connection.execute(
        "INSERT INTO task_stage_context (stage_run_id, context_json, facts_json) VALUES (?1, ?2, ?3)",
        params![stage.0, serde_json::to_string(&ContextState::default())?,
            serde_json::to_string(&FactsState::default())?],
    )?;
    if changed != 1 {
        return invalid("stage context insertion lost");
    }
    Ok(())
}

fn load_stage_messages(
    connection: &Connection,
    stage_run_id: StageRunId,
) -> Result<Vec<StageProtocolMessage>, StoreError> {
    let mut statement = connection.prepare(
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

fn load_stage_reductions(
    connection: &Connection,
    stage: StageRunId,
) -> Result<StageReductionState, StoreError> {
    let row: Option<(Option<String>, Option<String>)> = connection
        .query_row(
            "SELECT c.context_json,c.facts_json FROM task_stage_runs s
         LEFT JOIN task_stage_context c ON c.stage_run_id=s.id WHERE s.id=?1",
            [stage.0],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match row {
        Some((Some(context), Some(facts))) => Ok(StageReductionState {
            context: json(&context)?,
            facts: json(&facts)?,
        }),
        Some((None, None)) => Ok(StageReductionState::default()),
        _ => invalid("missing stage or incomplete stage reductions"),
    }
}

fn guard_stage_reduction(
    connection: &Connection,
    stage: StageRunId,
    expected_version: u64,
    expected_count: usize,
) -> Result<(i64, Vec<StageProtocolMessage>), StoreError> {
    let dialog_id: i64 = connection.query_row(
        "SELECT t.dialog_id FROM task_stage_runs s JOIN workflow_tasks t ON t.id=s.workflow_task_id WHERE s.id=?1",
        [stage.0], |row| row.get(0),
    ).optional()?.ok_or_else(|| StoreError::InvalidWorkflow("missing reduction stage".into()))?;
    let task = load_workflow(connection, dialog_id)?
        .current_task
        .ok_or(StoreError::WorkflowConflict(dialog_id))?;
    if task.current_stage_run_id != stage
        || task.version != expected_version
        || task.phase == TaskPhase::Done
    {
        return Err(StoreError::WorkflowConflict(dialog_id));
    }
    let messages = load_stage_messages(connection, stage)?;
    if messages.len() != expected_count {
        return Err(StoreError::WorkflowConflict(dialog_id));
    }
    Ok((dialog_id, messages))
}

fn transition_projection(
    command: &TransitionCommit<'_>,
    source: &WorkflowTaskState,
) -> Result<WorkflowTaskState, StoreError> {
    let preview = if let Some(patch) = command.accepted_patch {
        if patch.expected_version != source.version {
            return Err(StoreError::WorkflowConflict(command.dialog_id));
        }
        let context = if matches!(
            command.triggering_input.intent,
            WorkflowIntent::ProposeTransition {
                event: TransitionEvent::ValidationFailed,
                ..
            }
        ) {
            PatchContext::ValidationRepair
        } else {
            PatchContext::Normal
        };
        source.preview_patch(patch, context).map_err(domain_error)?
    } else {
        source.clone()
    };
    let authorization = match &command.triggering_input.intent {
        WorkflowIntent::ProposeTransition { event, evidence } => {
            StageChangeAuthorization::Transition(
                StateMachine::authorize(&preview, *event, evidence).map_err(domain_error)?,
            )
        }
        WorkflowIntent::ReplanCurrent { change_request } => StageChangeAuthorization::Replan(
            StateMachine::authorize_replan(
                &preview,
                &command.triggering_input.source,
                change_request.clone(),
            )
            .map_err(domain_error)?,
        ),
        _ => return invalid("stage change requires transition or replan input"),
    };
    if &authorization != command.authorization {
        return Err(StoreError::WorkflowConflict(command.dialog_id));
    }
    let mut target = project_handoff(&preview, &authorization, command.handoff)
        .map_err(|error| StoreError::InvalidWorkflow(error.to_string()))?;
    target.version = next_version(source.version)?;
    target.current_stage_sequence = source
        .current_stage_sequence
        .checked_add(1)
        .ok_or_else(|| StoreError::InvalidWorkflow("stage sequence overflow".into()))?;
    Ok(target)
}

fn transition_event(authorization: &StageChangeAuthorization) -> Result<String, StoreError> {
    match authorization {
        StageChangeAuthorization::Transition(auth) => enum_text(&auth.event),
        StageChangeAuthorization::Replan(_) => Ok("replan_requested".into()),
    }
}

// Bind replay to the semantic source, while allowing branch-local database IDs
// to be remapped. Stage identity/sequence and ownership are checked separately.
fn source_fingerprint(source: &WorkflowTaskState) -> Result<String, StoreError> {
    Ok(serde_json::to_string(&serde_json::json!({
        "ordinal": source.ordinal,
        "phase": source.phase,
        "status": source.status,
        "goal": source.goal,
        "plan": source.plan,
        "current_step_id": source.current_step_id,
        "expected_action": source.expected_action,
        "checkpoint": source.checkpoint,
        "version": source.version,
    }))?)
}

fn enum_text(value: &impl Serialize) -> Result<String, StoreError> {
    Ok(serde_json::to_value(value)?
        .as_str()
        .ok_or_else(|| StoreError::InvalidWorkflow("expected text enum".into()))?
        .to_owned())
}

fn commit_stage_change(
    connection: &Connection,
    command: &TransitionCommit<'_>,
) -> Result<PersistedTransition, StoreError> {
    validate_protocol_text(command.protocol_text)?;
    validate_confidence(command.confidence)?;
    command.triggering_input.validate().map_err(domain_error)?;
    let source = command.source_task;
    if source.dialog_id != command.dialog_id {
        return Err(StoreError::WorkflowConflict(command.dialog_id));
    }
    let (source_name, checker, model, assistant) = match &command.triggering_input.source {
        WorkflowInputSource::Human => {
            if command.processing_id.is_some() || command.accepted_patch.is_some() {
                return invalid("human stage changes cannot carry processing or checker patches");
            }
            ("human", None, None, None)
        }
        WorkflowInputSource::Controller {
            checker,
            model,
            triggering_assistant_message_id,
        } => {
            if command.processing_id.is_none() {
                return invalid("controller stage changes require processing");
            }
            active_task(source)?;
            (
                "controller",
                Some(checker.as_str()),
                Some(model.as_str()),
                Some(*triggering_assistant_message_id),
            )
        }
    };
    let processing = command
        .processing_id
        .map(|id| processing_row(connection, id))
        .transpose()?;
    let completed = if let Some(processing) = &processing {
        processing.check_identity(source.id, source.current_stage_run_id, source.version)?;
        if Some(processing.assistant_message_id) != assistant
            || Some(processing.checker.as_str()) != checker
            || processing.dialog_id != command.dialog_id
        {
            return Err(StoreError::WorkflowConflict(command.dialog_id));
        }
        processing.completed_result()?
    } else {
        None
    };
    let prior: Option<(i64, i64, i64, Option<String>)> = connection
        .query_row(
            "SELECT id, workflow_input_id, to_stage_run_id, source_fingerprint FROM task_transitions
         WHERE workflow_task_id=?1 AND from_stage_run_id=?2 AND source_version=?3",
            params![
                source.id.0,
                source.current_stage_run_id.0,
                sqlite_version(source.version)?
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if let Some((_, _, _, fingerprint)) = &prior {
        if fingerprint.as_deref() != Some(source_fingerprint(source)?.as_str()) {
            return Err(StoreError::WorkflowConflict(command.dialog_id));
        }
    }
    let mut target = transition_projection(command, source)?;
    if let Some((transition_id, workflow_input_id, stage_id, _)) = prior {
        target.current_stage_run_id = StageRunId(stage_id);
        target.incoming_handoff_id = Some(transition_id);
        let message: Option<i64> = connection
            .query_row(
                "SELECT i.message_id FROM task_transitions t
             JOIN workflow_inputs i ON i.id=t.workflow_input_id
             JOIN messages m ON m.id=i.message_id
             JOIN message_task_stages ms ON ms.message_id=m.id
             JOIN task_stage_runs old ON old.id=t.from_stage_run_id
             JOIN task_stage_runs new ON new.id=t.to_stage_run_id
             JOIN workflow_tasks wt ON wt.id=t.workflow_task_id
             WHERE t.id=?1 AND i.id=?2 AND t.event=?3 AND t.handoff_json=?4
               AND i.dialog_id=?5 AND m.dialog_id=?5 AND wt.dialog_id=?5
               AND m.role='user' AND m.content=?6 AND i.intent_json=?7 AND i.confidence IS ?8
               AND i.source=?9 AND i.checker_name IS ?10 AND i.model_name IS ?11
               AND i.triggering_assistant_message_id IS ?12 AND i.processing_id IS ?13
               AND i.outcome='accepted' AND ms.workflow_task_id=?14 AND ms.stage_run_id=?15
               AND old.workflow_task_id=?14 AND new.workflow_task_id=?14
               AND old.phase=?16 AND new.phase=?17 AND old.sequence=?18 AND new.sequence=?19
               AND old.finished_at IS NOT NULL",
                params![
                    transition_id,
                    workflow_input_id,
                    transition_event(command.authorization)?,
                    serde_json::to_string(command.handoff)?,
                    command.dialog_id,
                    command.protocol_text,
                    serde_json::to_string(&command.triggering_input.intent)?,
                    command.confidence,
                    source_name,
                    checker,
                    model,
                    assistant,
                    command.processing_id,
                    source.id.0,
                    stage_id,
                    enum_text(&source.phase)?,
                    enum_text(&target.phase)?,
                    source.current_stage_sequence,
                    target.current_stage_sequence
                ],
                |row| row.get(0),
            )
            .optional()?;
        let input_message_id = message.ok_or(StoreError::WorkflowConflict(command.dialog_id))?;
        if processing.is_some()
            && completed
                != Some(ProcessingResult::Transition {
                    transition_id,
                    input_message_id,
                    workflow_input_id,
                    target_state: target.clone(),
                })
        {
            return Err(StoreError::WorkflowConflict(command.dialog_id));
        }
        return Ok(PersistedTransition {
            transition_id,
            input_message_id,
            target_state: target,
        });
    }
    if completed.is_some() {
        return Err(StoreError::WorkflowConflict(command.dialog_id));
    }
    let current = current_task(
        connection,
        command.dialog_id,
        source.id,
        source.current_stage_run_id,
        source.version,
    )?;
    if &current != source {
        return Err(StoreError::WorkflowConflict(command.dialog_id));
    }
    target = transition_projection(command, &current)?;
    if let Some(processing) = &processing {
        processing.require_leased()?;
        processing.current_task(connection)?;
    }
    let input_message_id =
        insert_message(connection, command.dialog_id, "user", command.protocol_text)?;
    require_one(connection.execute(
        "INSERT INTO workflow_inputs (dialog_id,message_id,source,checker_name,model_name,triggering_assistant_message_id,intent_json,confidence,outcome,processing_id)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'accepted',?9)",
        params![command.dialog_id,input_message_id,source_name,checker,model,assistant,serde_json::to_string(&command.triggering_input.intent)?,command.confidence,command.processing_id],
    )?, command.dialog_id)?;
    let workflow_input_id = connection.last_insert_rowid();
    if connection.execute("UPDATE task_stage_runs SET finished_at=strftime('%Y-%m-%d %H:%M:%f','now') WHERE id=?1 AND workflow_task_id=?2 AND finished_at IS NULL", params![source.current_stage_run_id.0,source.id.0])? != 1 {
        return Err(StoreError::WorkflowConflict(command.dialog_id));
    }
    // The destination must exist before its non-null transition foreign key is inserted.
    require_one(
        connection.execute(
            "INSERT INTO task_stage_runs (workflow_task_id,phase,sequence) VALUES (?1,?2,?3)",
            params![
                source.id.0,
                enum_text(&target.phase)?,
                target.current_stage_sequence
            ],
        )?,
        command.dialog_id,
    )?;
    target.current_stage_run_id = StageRunId(connection.last_insert_rowid());
    require_one(connection.execute("INSERT INTO task_transitions (workflow_task_id,from_stage_run_id,to_stage_run_id,workflow_input_id,event,source_version,handoff_json,source_fingerprint) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![source.id.0,source.current_stage_run_id.0,target.current_stage_run_id.0,workflow_input_id,transition_event(command.authorization)?,sqlite_version(source.version)?,serde_json::to_string(command.handoff)?,source_fingerprint(source)?])?, command.dialog_id)?;
    let transition_id = connection.last_insert_rowid();
    target.incoming_handoff_id = Some(transition_id);
    target.validate().map_err(domain_error)?;
    map_message(connection, input_message_id, &target)?;
    if connection.execute(
        "UPDATE workflow_tasks SET phase=?1,status='active',plan_json=?2,current_step_id=?3,expected_action=?4,checkpoint_json=?5,current_stage_run_id=?6,incoming_handoff_id=?7,version=version+1,updated_at=strftime('%Y-%m-%d %H:%M:%f','now') WHERE id=?8 AND version=?9 AND current_stage_run_id=?10 AND dialog_id=?11",
        params![enum_text(&target.phase)?,serde_json::to_string(&target.plan)?,target.current_step_id,target.expected_action,serde_json::to_string(&target.checkpoint)?,target.current_stage_run_id.0,transition_id,source.id.0,sqlite_version(source.version)?,source.current_stage_run_id.0,command.dialog_id],
    )? != 1 { return Err(StoreError::WorkflowConflict(command.dialog_id)); }
    if connection.execute("UPDATE dialog_workflow_state SET current_task_id=?1 WHERE dialog_id=?2 AND current_task_id=?1", params![source.id.0,command.dialog_id])? != 1 {
        return Err(StoreError::WorkflowConflict(command.dialog_id));
    }
    initialize_stage_context(connection, target.current_stage_run_id)?;
    if let Some(processing) = &processing {
        complete_processing(
            connection,
            processing,
            &ProcessingResult::Transition {
                transition_id,
                input_message_id,
                workflow_input_id,
                target_state: target.clone(),
            },
        )?;
    }
    touch_dialog(connection, command.dialog_id, input_message_id)?;
    Ok(PersistedTransition {
        transition_id,
        input_message_id,
        target_state: target,
    })
}

fn create_task(
    connection: &Connection,
    dialog_id: i64,
    protocol_text: &str,
    goal: &str,
    expected_current_task: ExpectedCurrentTask,
) -> Result<StartedWorkflow, StoreError> {
    validate_protocol_text(protocol_text)?;
    let current = load_workflow(connection, dialog_id)?.current_task;
    if !expected_current_task.matches(current.as_ref()) {
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
    initialize_stage_context(connection, task.current_stage_run_id)?;
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
            ProcessingResult::Transition {
                transition_id,
                input_message_id,
                workflow_input_id,
                target_state,
            } => {
                positive(*transition_id, "transition id")?;
                positive(*input_message_id, "transition message id")?;
                positive(*workflow_input_id, "transition input id")?;
                target_state.validate().map_err(domain_error)?;
                if target_state.id != self.task_id
                    || target_state.dialog_id != self.dialog_id
                    || target_state.version != next_version(self.expected_version)?
                    || target_state.incoming_handoff_id != Some(*transition_id)
                {
                    return invalid("invalid transition completion");
                }
            }
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
    if !command.expected_current_task.matches(task.as_ref()) {
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
    require_one(
        connection.execute(
            "INSERT INTO messages (dialog_id, role, content) VALUES (?1, ?2, ?3)",
            params![dialog_id, role, text],
        )?,
        dialog_id,
    )?;
    Ok(connection.last_insert_rowid())
}

fn map_message(
    connection: &Connection,
    message_id: i64,
    task: &WorkflowTaskState,
) -> Result<(), StoreError> {
    require_one(connection.execute("INSERT INTO message_task_stages (message_id, workflow_task_id, stage_run_id) VALUES (?1, ?2, ?3)", params![message_id, task.id.0, task.current_stage_run_id.0])?, task.dialog_id)?;
    Ok(())
}

fn require_one(changed: usize, dialog_id: i64) -> Result<(), StoreError> {
    if changed != 1 {
        return Err(StoreError::WorkflowConflict(dialog_id));
    }
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
            source_fingerprint TEXT,
            handoff_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_transition_per_source_stage
        ON task_transitions(workflow_task_id, from_stage_run_id, source_version);",
    )?;
    let has_source_fingerprint: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('task_transitions') WHERE name='source_fingerprint')",
        [], |row| row.get(0),
    )?;
    if !has_source_fingerprint {
        // Historical rows cannot safely reconstruct their source: NULL is a
        // deliberate non-replayable sentinel, never inferred from current state.
        tx.execute_batch("ALTER TABLE task_transitions ADD COLUMN source_fingerprint TEXT")?;
    }
    if tx.prepare("PRAGMA foreign_key_check")?.exists([])? {
        return Err(StoreError::InvalidWorkflow(
            "foreign key check failed".into(),
        ));
    }
    tx.commit()?;
    Ok(())
}
