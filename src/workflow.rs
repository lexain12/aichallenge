use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};

pub const MAX_WORKFLOW_TEXT_CHARS: usize = 8_192;
pub const MAX_PLAN_STEPS: usize = 256;
pub const MAX_ACCEPTANCE_CRITERIA: usize = 32;
pub const MAX_ACCEPTANCE_CRITERION_CHARS: usize = 1_024;
pub const MAX_CHECKPOINT_ITEMS: usize = 128;
pub const MAX_EVIDENCE_ITEMS: usize = 32;
pub const MAX_EVIDENCE_ITEM_CHARS: usize = 2_048;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowTaskId(pub i64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StageRunId(pub i64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhase {
    Planning,
    Execution,
    Validation,
    Done,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Active,
    Paused,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPlan {
    pub revision: u32,
    pub steps: Vec<PlanStep>,
    pub acceptance_criteria: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub status: PlanStepStatus,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepStatus {
    Pending,
    InProgress,
    Completed,
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageCheckpoint {
    pub summary: String,
    pub decisions: Vec<String>,
    pub open_issues: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowTaskState {
    pub id: WorkflowTaskId,
    pub dialog_id: i64,
    pub ordinal: u32,
    pub phase: TaskPhase,
    pub status: TaskStatus,
    pub goal: String,
    pub plan: TaskPlan,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub checkpoint: StageCheckpoint,
    pub current_stage_run_id: StageRunId,
    pub current_stage_sequence: u32,
    pub incoming_handoff_id: Option<i64>,
    pub version: u64,
}

impl WorkflowTaskState {
    pub fn new(
        id: WorkflowTaskId,
        dialog_id: i64,
        ordinal: u32,
        goal: String,
        current_stage_run_id: StageRunId,
    ) -> Result<Self, WorkflowError> {
        let state = Self {
            id,
            dialog_id,
            ordinal,
            phase: TaskPhase::Planning,
            status: TaskStatus::Active,
            goal,
            plan: TaskPlan {
                revision: 0,
                steps: Vec::new(),
                acceptance_criteria: Vec::new(),
            },
            current_step_id: None,
            expected_action: None,
            checkpoint: StageCheckpoint {
                summary: String::new(),
                decisions: Vec::new(),
                open_issues: Vec::new(),
            },
            current_stage_run_id,
            current_stage_sequence: 1,
            incoming_handoff_id: None,
            version: 0,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), WorkflowError> {
        positive_id(self.id.0, "workflow task id")?;
        positive_id(self.dialog_id, "dialog id")?;
        if self.ordinal == 0 {
            return Err(WorkflowError::InvalidOrdinal);
        }
        positive_id(self.current_stage_run_id.0, "stage run id")?;
        if self.current_stage_sequence == 0 {
            return Err(WorkflowError::InvalidStageSequence);
        }
        if let Some(handoff_id) = self.incoming_handoff_id {
            positive_id(handoff_id, "incoming handoff id")?;
        }
        if self.phase == TaskPhase::Done && self.status == TaskStatus::Paused {
            return Err(WorkflowError::DoneTaskPaused);
        }
        validate_required_text(&self.goal, "goal", MAX_WORKFLOW_TEXT_CHARS)?;
        self.plan.validate()?;
        if let Some(step_id) = &self.current_step_id {
            validate_required_text(step_id, "current step id", MAX_WORKFLOW_TEXT_CHARS)?;
            if !self.plan.steps.iter().any(|step| &step.id == step_id) {
                return Err(WorkflowError::UnknownStepId(step_id.clone()));
            }
        }
        if let Some(action) = &self.expected_action {
            validate_required_text(action, "expected action", MAX_WORKFLOW_TEXT_CHARS)?;
        }
        self.checkpoint.validate()?;
        Ok(())
    }

    pub fn pause(mut self) -> Result<Self, WorkflowError> {
        self.validate()?;
        if self.phase == TaskPhase::Done {
            return Err(WorkflowError::CannotPauseDone);
        }
        if self.status == TaskStatus::Paused {
            return Err(WorkflowError::AlreadyPaused);
        }
        self.status = TaskStatus::Paused;
        Ok(self)
    }

    pub fn resume(mut self) -> Result<Self, WorkflowError> {
        self.validate()?;
        if self.phase == TaskPhase::Done {
            return Err(WorkflowError::CannotResumeDone);
        }
        if self.status == TaskStatus::Active {
            return Err(WorkflowError::AlreadyActive);
        }
        self.status = TaskStatus::Active;
        Ok(self)
    }

    pub fn preview_patch(
        &self,
        patch: &TaskStatePatch,
        context: PatchContext,
    ) -> Result<Self, WorkflowError> {
        self.validate()?;
        if patch.expected_version != self.version {
            return Err(WorkflowError::StaleVersion {
                expected: patch.expected_version,
                actual: self.version,
            });
        }
        patch.validate()?;

        let mut projected = self.clone();
        if !patch.plan_append.is_empty() {
            if !context.allows_plan_append(self.phase) {
                return Err(WorkflowError::PlanAdditionForbidden { phase: self.phase });
            }
            projected.append_plan(&patch.plan_append)?;
        }
        projected.apply_step_updates(&patch.step_updates)?;
        if let Some(current_step_id) = &patch.current_step_id {
            if !projected
                .plan
                .steps
                .iter()
                .any(|step| &step.id == current_step_id)
            {
                return Err(WorkflowError::UnknownStepId(current_step_id.clone()));
            }
            projected.current_step_id = Some(current_step_id.clone());
        }
        if let Some(expected_action) = &patch.expected_action {
            projected.expected_action = Some(expected_action.clone());
        }
        if let Some(checkpoint) = &patch.checkpoint {
            projected.checkpoint = checkpoint.clone();
        }
        projected.validate()?;
        Ok(projected)
    }

    pub fn apply_patch(
        &self,
        patch: &TaskStatePatch,
        context: PatchContext,
    ) -> Result<Self, WorkflowError> {
        let mut projected = self.preview_patch(patch, context)?;
        if !patch.is_empty() {
            projected.version = self
                .version
                .checked_add(1)
                .ok_or(WorkflowError::VersionOverflow)?;
        }
        Ok(projected)
    }

    pub fn fingerprint(&self) -> StateFingerprint {
        StateFingerprint::from(self)
    }

    fn append_plan(&mut self, append: &PlanAppend) -> Result<(), WorkflowError> {
        let mut step_ids: HashSet<&str> = self
            .plan
            .steps
            .iter()
            .map(|step| step.id.as_str())
            .collect();
        for step in &append.steps {
            step.validate()?;
            if step.status != PlanStepStatus::Pending {
                return Err(WorkflowError::AppendedStepMustStartPending(step.id.clone()));
            }
            if !step_ids.insert(step.id.as_str()) {
                return Err(WorkflowError::DuplicateStepId(step.id.clone()));
            }
        }
        if self.plan.steps.len() + append.steps.len() > MAX_PLAN_STEPS {
            return Err(WorkflowError::TooManyItems {
                field: "plan steps",
                max: MAX_PLAN_STEPS,
            });
        }

        let mut criteria: HashSet<&str> = self
            .plan
            .acceptance_criteria
            .iter()
            .map(|criterion| criterion.trim())
            .collect();
        for criterion in &append.acceptance_criteria {
            validate_acceptance_criterion(criterion)?;
            if !criteria.insert(criterion.trim()) {
                return Err(WorkflowError::DuplicateAcceptanceCriterion(
                    criterion.clone(),
                ));
            }
        }
        if self.plan.acceptance_criteria.len() + append.acceptance_criteria.len()
            > MAX_ACCEPTANCE_CRITERIA
        {
            return Err(WorkflowError::TooManyItems {
                field: "acceptance criteria",
                max: MAX_ACCEPTANCE_CRITERIA,
            });
        }

        self.plan.steps.extend(append.steps.iter().cloned());
        self.plan
            .acceptance_criteria
            .extend(append.acceptance_criteria.iter().cloned());
        self.plan.revision = self
            .plan
            .revision
            .checked_add(1)
            .ok_or(WorkflowError::PlanRevisionOverflow)?;
        Ok(())
    }

    fn apply_step_updates(&mut self, updates: &[StepStatusUpdate]) -> Result<(), WorkflowError> {
        let mut updated = HashSet::new();
        for update in updates {
            if !updated.insert(update.step_id.as_str()) {
                return Err(WorkflowError::DuplicateStepUpdate(update.step_id.clone()));
            }
            update.validate()?;
            let step = self
                .plan
                .steps
                .iter_mut()
                .find(|step| step.id == update.step_id)
                .ok_or_else(|| WorkflowError::UnknownStepId(update.step_id.clone()))?;
            if step.status == PlanStepStatus::Completed {
                return Err(WorkflowError::CompletedStepImmutable(
                    update.step_id.clone(),
                ));
            }
            step.status = update.status;
        }
        Ok(())
    }
}

impl TaskPlan {
    fn validate(&self) -> Result<(), WorkflowError> {
        if self.steps.len() > MAX_PLAN_STEPS {
            return Err(WorkflowError::TooManyItems {
                field: "plan steps",
                max: MAX_PLAN_STEPS,
            });
        }
        if self.acceptance_criteria.len() > MAX_ACCEPTANCE_CRITERIA {
            return Err(WorkflowError::TooManyItems {
                field: "acceptance criteria",
                max: MAX_ACCEPTANCE_CRITERIA,
            });
        }
        let mut step_ids = HashSet::new();
        for step in &self.steps {
            step.validate()?;
            if !step_ids.insert(step.id.as_str()) {
                return Err(WorkflowError::DuplicateStepId(step.id.clone()));
            }
        }
        let mut criteria = HashSet::new();
        for criterion in &self.acceptance_criteria {
            validate_acceptance_criterion(criterion)?;
            if !criteria.insert(criterion.trim()) {
                return Err(WorkflowError::DuplicateAcceptanceCriterion(
                    criterion.clone(),
                ));
            }
        }
        Ok(())
    }
}

impl PlanStep {
    fn validate(&self) -> Result<(), WorkflowError> {
        validate_required_text(&self.id, "plan step id", MAX_WORKFLOW_TEXT_CHARS)?;
        validate_required_text(
            &self.description,
            "plan step description",
            MAX_WORKFLOW_TEXT_CHARS,
        )
    }
}

impl StageCheckpoint {
    fn validate(&self) -> Result<(), WorkflowError> {
        validate_text(&self.summary, "checkpoint summary", MAX_WORKFLOW_TEXT_CHARS)?;
        validate_text_list(
            &self.decisions,
            "checkpoint decisions",
            MAX_CHECKPOINT_ITEMS,
        )?;
        validate_text_list(
            &self.open_issues,
            "checkpoint open issues",
            MAX_CHECKPOINT_ITEMS,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowInput {
    pub source: WorkflowInputSource,
    pub intent: WorkflowIntent,
}

impl WorkflowInput {
    pub fn validate(&self) -> Result<(), WorkflowError> {
        StateMachine::validate_source(&self.source, &self.intent)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WorkflowInputSource {
    Human,
    Controller {
        checker: String,
        model: String,
        triggering_assistant_message_id: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WorkflowIntent {
    Continue {
        instruction: String,
    },
    StartNewTask {
        goal: String,
    },
    ReplanCurrent {
        change_request: String,
    },
    ProposeTransition {
        event: TransitionEvent,
        evidence: Vec<String>,
    },
}

impl WorkflowIntent {
    pub fn human_continue(instruction: &str) -> Result<Self, WorkflowError> {
        validate_required_text(instruction, "instruction", MAX_WORKFLOW_TEXT_CHARS)?;
        Ok(Self::Continue {
            instruction: instruction.trim().to_owned(),
        })
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Continue { .. } => "continue",
            Self::StartNewTask { .. } => "start_new_task",
            Self::ReplanCurrent { .. } => "replan_current",
            Self::ProposeTransition { .. } => "propose_transition",
        }
    }

    fn validate(&self) -> Result<(), WorkflowError> {
        match self {
            Self::Continue { instruction } => {
                validate_required_text(instruction, "instruction", MAX_WORKFLOW_TEXT_CHARS)
            }
            Self::StartNewTask { goal } => {
                validate_required_text(goal, "goal", MAX_WORKFLOW_TEXT_CHARS)
            }
            Self::ReplanCurrent { change_request } => {
                validate_required_text(change_request, "change request", MAX_WORKFLOW_TEXT_CHARS)
            }
            Self::ProposeTransition { evidence, .. } => validate_evidence(evidence, false),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionEvent {
    PlanningCompleted,
    ExecutionCompleted,
    ValidationPassed,
    ValidationFailed,
}

pub fn target_phase(from: TaskPhase, event: TransitionEvent) -> Result<TaskPhase, WorkflowError> {
    match (from, event) {
        (TaskPhase::Planning, TransitionEvent::PlanningCompleted) => Ok(TaskPhase::Execution),
        (TaskPhase::Execution, TransitionEvent::ExecutionCompleted) => Ok(TaskPhase::Validation),
        (TaskPhase::Validation, TransitionEvent::ValidationPassed) => Ok(TaskPhase::Done),
        (TaskPhase::Validation, TransitionEvent::ValidationFailed) => Ok(TaskPhase::Execution),
        _ => Err(WorkflowError::IllegalTransition { from, event }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionAuthorization {
    pub from_phase: TaskPhase,
    pub to_phase: TaskPhase,
    pub event: TransitionEvent,
    pub source_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplanAuthorization {
    pub from_phase: TaskPhase,
    pub to_phase: TaskPhase,
    pub source_version: u64,
    pub next_plan_revision: u32,
    pub change_request: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StageChangeAuthorization {
    Transition(TransitionAuthorization),
    Replan(ReplanAuthorization),
}

pub struct StateMachine;

impl StateMachine {
    pub fn validate_source(
        source: &WorkflowInputSource,
        intent: &WorkflowIntent,
    ) -> Result<(), WorkflowError> {
        intent.validate()?;
        match source {
            WorkflowInputSource::Human => Ok(()),
            WorkflowInputSource::Controller {
                checker,
                model,
                triggering_assistant_message_id,
            } => {
                validate_required_text(checker, "checker", MAX_WORKFLOW_TEXT_CHARS)?;
                validate_required_text(model, "model", MAX_WORKFLOW_TEXT_CHARS)?;
                positive_id(
                    *triggering_assistant_message_id,
                    "triggering assistant message id",
                )?;
                match intent {
                    WorkflowIntent::Continue { .. } | WorkflowIntent::ProposeTransition { .. } => {
                        Ok(())
                    }
                    WorkflowIntent::StartNewTask { .. } | WorkflowIntent::ReplanCurrent { .. } => {
                        Err(WorkflowError::ControllerIntentForbidden {
                            intent: intent.kind(),
                        })
                    }
                }
            }
        }
    }

    pub fn validate_new_task(current: Option<&WorkflowTaskState>) -> Result<(), WorkflowError> {
        if let Some(current) = current {
            current.validate()?;
            if current.phase != TaskPhase::Done {
                return Err(WorkflowError::UnfinishedTaskExists);
            }
        }
        Ok(())
    }

    pub fn authorize(
        state: &WorkflowTaskState,
        event: TransitionEvent,
        evidence: &[String],
    ) -> Result<TransitionAuthorization, WorkflowError> {
        state.validate()?;
        let to_phase = target_phase(state.phase, event)?;
        match event {
            TransitionEvent::PlanningCompleted => {
                if state.plan.steps.is_empty() || state.plan.acceptance_criteria.is_empty() {
                    return Err(WorkflowError::PlanningRequirementsIncomplete);
                }
            }
            TransitionEvent::ExecutionCompleted => {
                if state
                    .plan
                    .steps
                    .iter()
                    .any(|step| step.status != PlanStepStatus::Completed)
                {
                    return Err(WorkflowError::IncompletePlan);
                }
            }
            TransitionEvent::ValidationPassed | TransitionEvent::ValidationFailed => {
                validate_evidence(evidence, true)?;
                if event == TransitionEvent::ValidationPassed {
                    validate_criterion_coverage(&state.plan.acceptance_criteria, evidence)?;
                }
            }
        }
        Ok(TransitionAuthorization {
            from_phase: state.phase,
            to_phase,
            event,
            source_version: state.version,
        })
    }

    pub fn authorize_replan(
        state: &WorkflowTaskState,
        source: &WorkflowInputSource,
        change_request: String,
    ) -> Result<ReplanAuthorization, WorkflowError> {
        state.validate()?;
        validate_required_text(&change_request, "change request", MAX_WORKFLOW_TEXT_CHARS)?;
        if source != &WorkflowInputSource::Human {
            return Err(WorkflowError::ReplanRequiresHuman);
        }
        Ok(ReplanAuthorization {
            from_phase: state.phase,
            to_phase: TaskPhase::Planning,
            source_version: state.version,
            next_plan_revision: state
                .plan
                .revision
                .checked_add(1)
                .ok_or(WorkflowError::PlanRevisionOverflow)?,
            change_request: change_request.trim().to_owned(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepStatusUpdate {
    pub step_id: String,
    pub status: PlanStepStatus,
    pub evidence: Vec<String>,
}

impl StepStatusUpdate {
    fn validate(&self) -> Result<(), WorkflowError> {
        validate_required_text(&self.step_id, "step update id", MAX_WORKFLOW_TEXT_CHARS)?;
        validate_evidence(&self.evidence, self.status == PlanStepStatus::Completed)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanAppend {
    pub steps: Vec<PlanStep>,
    pub acceptance_criteria: Vec<String>,
}

impl PlanAppend {
    fn is_empty(&self) -> bool {
        self.steps.is_empty() && self.acceptance_criteria.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStatePatch {
    pub expected_version: u64,
    pub plan_append: PlanAppend,
    pub step_updates: Vec<StepStatusUpdate>,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub checkpoint: Option<StageCheckpoint>,
}

impl TaskStatePatch {
    pub fn is_empty(&self) -> bool {
        self.plan_append.is_empty()
            && self.step_updates.is_empty()
            && self.current_step_id.is_none()
            && self.expected_action.is_none()
            && self.checkpoint.is_none()
    }

    fn validate(&self) -> Result<(), WorkflowError> {
        if self.step_updates.len() > MAX_PLAN_STEPS {
            return Err(WorkflowError::TooManyItems {
                field: "step updates",
                max: MAX_PLAN_STEPS,
            });
        }
        for step in &self.plan_append.steps {
            step.validate()?;
        }
        for criterion in &self.plan_append.acceptance_criteria {
            validate_acceptance_criterion(criterion)?;
        }
        if let Some(current_step_id) = &self.current_step_id {
            validate_required_text(current_step_id, "current step id", MAX_WORKFLOW_TEXT_CHARS)?;
        }
        if let Some(expected_action) = &self.expected_action {
            validate_required_text(expected_action, "expected action", MAX_WORKFLOW_TEXT_CHARS)?;
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PatchContext {
    Normal,
    ValidationRepair,
}

impl PatchContext {
    fn allows_plan_append(self, phase: TaskPhase) -> bool {
        matches!(
            (self, phase),
            (Self::Normal, TaskPhase::Planning) | (Self::ValidationRepair, TaskPhase::Validation)
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StateFingerprint {
    pub version: u64,
    pub phase: TaskPhase,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
}

impl From<&WorkflowTaskState> for StateFingerprint {
    fn from(state: &WorkflowTaskState) -> Self {
        Self {
            version: state.version,
            phase: state.phase,
            current_step_id: state.current_step_id.clone(),
            expected_action: state.expected_action.clone(),
        }
    }
}

pub fn render_task_state(state: &WorkflowTaskState) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "workflow_task": state.ordinal,
        "phase": state.phase,
        "status": state.status,
        "goal": state.goal,
        "plan": state.plan,
        "current_step_id": state.current_step_id,
        "expected_action": state.expected_action,
        "checkpoint": state.checkpoint,
    }))
    .expect("validated workflow state is serializable")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowError {
    IllegalTransition {
        from: TaskPhase,
        event: TransitionEvent,
    },
    InvalidIdentifier(&'static str),
    InvalidOrdinal,
    InvalidStageSequence,
    BlankField(&'static str),
    StringTooLong {
        field: &'static str,
        max: usize,
    },
    TooManyItems {
        field: &'static str,
        max: usize,
    },
    DuplicateStepId(String),
    DuplicateAcceptanceCriterion(String),
    DuplicateStepUpdate(String),
    UnknownStepId(String),
    AppendedStepMustStartPending(String),
    CompletedStepImmutable(String),
    CompletionEvidenceRequired,
    PlanningRequirementsIncomplete,
    IncompletePlan,
    ValidationEvidenceRequired,
    AcceptanceCriteriaNotMet,
    MissingCriterionCoverage(String),
    DuplicateCriterionCoverage(String),
    MalformedValidationEvidence(String),
    ControllerIntentForbidden {
        intent: &'static str,
    },
    ReplanRequiresHuman,
    UnfinishedTaskExists,
    CannotPauseDone,
    CannotResumeDone,
    DoneTaskPaused,
    AlreadyPaused,
    AlreadyActive,
    StaleVersion {
        expected: u64,
        actual: u64,
    },
    PlanAdditionForbidden {
        phase: TaskPhase,
    },
    PlanRevisionOverflow,
    VersionOverflow,
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IllegalTransition { from, event } => {
                write!(
                    formatter,
                    "illegal transition from {from:?} using {event:?}"
                )
            }
            Self::InvalidIdentifier(field) => write!(formatter, "{field} must be positive"),
            Self::InvalidOrdinal => write!(formatter, "task ordinal must be positive"),
            Self::InvalidStageSequence => write!(formatter, "stage sequence must be positive"),
            Self::BlankField(field) => write!(formatter, "{field} must not be blank"),
            Self::StringTooLong { field, max } => {
                write!(formatter, "{field} exceeds {max} characters")
            }
            Self::TooManyItems { field, max } => {
                write!(formatter, "too many {field}; maximum is {max}")
            }
            Self::DuplicateStepId(id) => write!(formatter, "duplicate plan step id: {id}"),
            Self::DuplicateAcceptanceCriterion(criterion) => {
                write!(formatter, "duplicate acceptance criterion: {criterion}")
            }
            Self::DuplicateStepUpdate(id) => write!(formatter, "duplicate update for step: {id}"),
            Self::UnknownStepId(id) => write!(formatter, "unknown plan step id: {id}"),
            Self::AppendedStepMustStartPending(id) => {
                write!(formatter, "appended plan step must start pending: {id}")
            }
            Self::CompletedStepImmutable(id) => {
                write!(formatter, "completed step cannot change: {id}")
            }
            Self::CompletionEvidenceRequired => {
                write!(formatter, "completing a step requires evidence")
            }
            Self::PlanningRequirementsIncomplete => {
                write!(formatter, "planning requires steps and acceptance criteria")
            }
            Self::IncompletePlan => write!(formatter, "all plan steps must be completed"),
            Self::ValidationEvidenceRequired => {
                write!(formatter, "validation transition requires evidence")
            }
            Self::AcceptanceCriteriaNotMet => {
                write!(formatter, "validation evidence misses acceptance criteria")
            }
            Self::MissingCriterionCoverage(criterion) => {
                write!(
                    formatter,
                    "validation evidence misses criterion: {criterion}"
                )
            }
            Self::DuplicateCriterionCoverage(criterion) => {
                write!(
                    formatter,
                    "validation evidence repeats criterion: {criterion}"
                )
            }
            Self::MalformedValidationEvidence(evidence) => {
                write!(formatter, "malformed validation evidence: {evidence}")
            }
            Self::ControllerIntentForbidden { intent } => {
                write!(formatter, "controller cannot submit {intent}")
            }
            Self::ReplanRequiresHuman => write!(formatter, "replan requires a human source"),
            Self::UnfinishedTaskExists => {
                write!(formatter, "dialog already has an unfinished task")
            }
            Self::CannotPauseDone => write!(formatter, "done task cannot be paused"),
            Self::CannotResumeDone => write!(formatter, "done task cannot be resumed"),
            Self::DoneTaskPaused => write!(formatter, "done task cannot be paused"),
            Self::AlreadyPaused => write!(formatter, "task is already paused"),
            Self::AlreadyActive => write!(formatter, "task is already active"),
            Self::StaleVersion { expected, actual } => {
                write!(
                    formatter,
                    "stale version {expected}; current version is {actual}"
                )
            }
            Self::PlanAdditionForbidden { phase } => {
                write!(formatter, "plan additions are forbidden during {phase:?}")
            }
            Self::PlanRevisionOverflow => write!(formatter, "plan revision overflow"),
            Self::VersionOverflow => write!(formatter, "state version overflow"),
        }
    }
}

impl std::error::Error for WorkflowError {}

fn positive_id(value: i64, field: &'static str) -> Result<(), WorkflowError> {
    if value > 0 {
        Ok(())
    } else {
        Err(WorkflowError::InvalidIdentifier(field))
    }
}

fn validate_required_text(
    value: &str,
    field: &'static str,
    max: usize,
) -> Result<(), WorkflowError> {
    if value.trim().is_empty() {
        return Err(WorkflowError::BlankField(field));
    }
    validate_text(value, field, max)
}

fn validate_text(value: &str, field: &'static str, max: usize) -> Result<(), WorkflowError> {
    if value.chars().count() > max {
        Err(WorkflowError::StringTooLong { field, max })
    } else {
        Ok(())
    }
}

fn validate_text_list(
    values: &[String],
    field: &'static str,
    max_items: usize,
) -> Result<(), WorkflowError> {
    if values.len() > max_items {
        return Err(WorkflowError::TooManyItems {
            field,
            max: max_items,
        });
    }
    for value in values {
        validate_required_text(value, field, MAX_WORKFLOW_TEXT_CHARS)?;
    }
    Ok(())
}

fn validate_acceptance_criterion(criterion: &str) -> Result<(), WorkflowError> {
    validate_required_text(
        criterion,
        "acceptance criterion",
        MAX_ACCEPTANCE_CRITERION_CHARS,
    )
}

fn validate_evidence(evidence: &[String], required: bool) -> Result<(), WorkflowError> {
    if required && evidence.is_empty() {
        return Err(WorkflowError::ValidationEvidenceRequired);
    }
    if evidence.len() > MAX_EVIDENCE_ITEMS {
        return Err(WorkflowError::TooManyItems {
            field: "evidence items",
            max: MAX_EVIDENCE_ITEMS,
        });
    }
    for item in evidence {
        validate_required_text(item, "evidence", MAX_EVIDENCE_ITEM_CHARS)?;
    }
    if !required && evidence.is_empty() {
        return Ok(());
    }
    Ok(())
}

fn validate_criterion_coverage(
    criteria: &[String],
    evidence: &[String],
) -> Result<(), WorkflowError> {
    let mut covered = HashSet::new();
    for item in evidence {
        let item = item.trim();
        let criterion = criteria
            .iter()
            .map(|criterion| criterion.trim())
            .filter_map(|criterion| {
                item.strip_prefix(criterion)
                    .and_then(|remainder| remainder.strip_prefix(" => "))
                    .filter(|result| !result.trim().is_empty())
                    .map(|_| criterion)
            })
            .max_by_key(|criterion| criterion.chars().count())
            .ok_or_else(|| WorkflowError::MalformedValidationEvidence(item.to_owned()))?;
        if !covered.insert(criterion) {
            return Err(WorkflowError::DuplicateCriterionCoverage(
                criterion.to_owned(),
            ));
        }
    }
    for criterion in criteria {
        let criterion = criterion.trim();
        if !covered.contains(criterion) {
            return Err(WorkflowError::MissingCriterionCoverage(
                criterion.to_owned(),
            ));
        }
    }
    Ok(())
}
