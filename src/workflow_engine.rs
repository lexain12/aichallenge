//! Shared workflow routing, advisory processing, and bounded autonomous turns.

use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use thiserror::Error;

use crate::agent::AgentEvent;
use crate::chat::{ChatHistory, Role};
use crate::client::{ClientError, DeepSeekClient, StreamEvent, TokenUsage};
use crate::config::{ContextConfig, ContextStrategy, WorkflowConfig};
use crate::context::{ContextState, ContextSummary, prepare_request};
use crate::debug_log::{WorkflowDebugEvent, WorkflowDebugMetadata, WorkflowDebugPayload};
use crate::dialog::{DialogStore, StoreError};
use crate::facts::{FactsError, parse_facts_json, plan_facts_update};
use crate::memory::{ContextError, ContextProvider, MemoryRepository, RequestScope};
use crate::profile::ProfileRepository;
use crate::system_context::SystemBlock;
use crate::workflow::{
    PatchContext, StageChangeAuthorization, StateMachine, TaskPhase, TaskStatePatch, TaskStatus,
    TransitionEvent, WorkflowError, WorkflowInput, WorkflowInputSource, WorkflowIntent,
    WorkflowTaskState,
};
use crate::workflow_context::{
    StageReductionState, WorkflowRequestInput, facts_candidates, plan_stage_compaction,
    prepare_workflow_request,
};
use crate::workflow_model::{
    CheckContext, CheckError, CheckerMode, CompletionModel, ContinuationChecker,
    ControllerDecision, HandoffBuildResult, HandoffBuilder, HumanInputInterpreter,
    HumanInterpretation, ModelPolicyError, ResponseChecker, parse_continuation_check,
    project_handoff,
};
use crate::workflow_store::{
    AcceptedInputEffect, AnswerCommit, ControllerInputCommit, DialogWorkflowSnapshot,
    ExpectedCurrentTask, FailProcessingCommit, InputCommit, PersistedAnswer, ProcessingLease,
    ProcessingLeaseMode, ProcessingResult, StageProtocolMessage, TransitionCommit,
    UnmanagedAnswerCommit, WorkflowRepository,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutonomyStopReason {
    AwaitUser,
    Done,
    TurnLimit,
    TokenLimit,
    RepeatedState,
    MissingUsage,
    LowConfidence,
    CheckerFailed,
    TransitionFailed,
    AwaitUserAfterRestart,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StateFingerprint {
    version: u64,
    phase: TaskPhase,
    current_step_id: Option<String>,
    expected_action: Option<String>,
}

impl From<&WorkflowTaskState> for StateFingerprint {
    fn from(task: &WorkflowTaskState) -> Self {
        Self {
            version: task.version,
            phase: task.phase,
            current_step_id: task.current_step_id.clone(),
            expected_action: task.expected_action.clone(),
        }
    }
}

pub struct AutonomyBudget {
    max_turns: u32,
    max_tokens: u64,
    turns: u32,
    tokens: u64,
    fingerprints: HashSet<StateFingerprint>,
    usage_complete: bool,
}

impl AutonomyBudget {
    pub fn new(config: &WorkflowConfig) -> Self {
        Self {
            max_turns: config.max_autonomous_turns(),
            max_tokens: config.max_autonomous_tokens(),
            turns: 0,
            tokens: 0,
            fingerprints: HashSet::new(),
            usage_complete: true,
        }
    }

    pub fn record_usage(&mut self, usage: Option<TokenUsage>) {
        match usage {
            Some(usage) => self.tokens = self.tokens.saturating_add(usage.total_tokens),
            None => self.usage_complete = false,
        }
    }

    pub fn tokens(&self) -> u64 {
        self.tokens
    }
    pub fn turns(&self) -> u32 {
        self.turns
    }
    pub fn usage_complete(&self) -> bool {
        self.usage_complete
    }

    fn allow_provider_call(&self) -> Result<(), AutonomyStopReason> {
        if !self.usage_complete {
            return Err(AutonomyStopReason::MissingUsage);
        }
        if self.tokens >= self.max_tokens {
            return Err(AutonomyStopReason::TokenLimit);
        }
        Ok(())
    }

    fn check_turn(&self, fingerprint: &StateFingerprint) -> Result<(), AutonomyStopReason> {
        self.allow_provider_call()?;
        if self.turns >= self.max_turns {
            return Err(AutonomyStopReason::TurnLimit);
        }
        if self.fingerprints.contains(fingerprint) {
            return Err(AutonomyStopReason::RepeatedState);
        }
        Ok(())
    }

    pub fn reserve_turn(
        &mut self,
        fingerprint: StateFingerprint,
    ) -> Result<(), AutonomyStopReason> {
        self.check_turn(&fingerprint)?;
        self.fingerprints.insert(fingerprint);
        self.turns += 1;
        Ok(())
    }
}

pub struct ResponsePipeline {
    checkers: Vec<Arc<dyn ResponseChecker>>,
}

#[derive(Debug)]
pub enum PipelineOutcome {
    AwaitUser {
        patch: TaskStatePatch,
    },
    Continue {
        patch: TaskStatePatch,
        instruction: String,
        confidence: f32,
    },
    Transition {
        patch: TaskStatePatch,
        event: TransitionEvent,
        evidence: Vec<String>,
        confidence: f32,
    },
    FailedOpen {
        error: String,
    },
    Conflict {
        checker_names: Vec<String>,
    },
    LowConfidence,
}

#[derive(Debug)]
pub struct PipelineResult {
    pub outcome: PipelineOutcome,
    observations: Vec<CheckerObservation>,
}

#[derive(Debug)]
struct CheckerObservation {
    checker: String,
    mode: CheckerMode,
    proposed_event: Option<TransitionEvent>,
    usage: Option<TokenUsage>,
    raw_output: Option<String>,
    provider_error: Option<String>,
    controller_instruction: Option<String>,
    error_kind: Option<&'static str>,
    http_status: Option<u16>,
    output_chars: usize,
}

impl ResponsePipeline {
    pub fn new(checkers: Vec<Arc<dyn ResponseChecker>>) -> Result<Self, WorkflowEngineError> {
        if checkers
            .iter()
            .any(|checker| checker.mode() == CheckerMode::Blocking)
        {
            return Err(WorkflowEngineError::BlockingCheckerRequiresBufferedDelivery);
        }
        Ok(Self { checkers })
    }

    pub async fn collect(
        &self,
        context: &CheckContext,
        response: &str,
        budget: &mut AutonomyBudget,
        min_confidence: f32,
    ) -> PipelineOutcome {
        self.collect_observed(context, response, budget, min_confidence, false, false)
            .await
            .outcome
    }

    async fn collect_observed(
        &self,
        context: &CheckContext,
        response: &str,
        budget: &mut AutonomyBudget,
        min_confidence: f32,
        observe: bool,
        capture_payloads: bool,
    ) -> PipelineResult {
        let mut proposals = Vec::new();
        let mut observations = Vec::new();
        let mut failed = false;
        let mut low_confidence = false;
        // Collect every checker against this one immutable snapshot before deciding effects.
        for checker in &self.checkers {
            match checker
                .check_observed(context, response, capture_payloads)
                .await
            {
                Ok(result) => {
                    budget.record_usage(result.usage);
                    let proposed_event = match &result.decision {
                        ControllerDecision::EmitTransition { event, .. } => Some(*event),
                        _ => None,
                    };
                    let controller_instruction = match &result.decision {
                        ControllerDecision::Continue { instruction, .. } if capture_payloads => {
                            Some(instruction.clone())
                        }
                        _ => None,
                    };
                    if observe {
                        observations.push(CheckerObservation {
                            checker: checker.name().to_owned(),
                            mode: checker.mode(),
                            proposed_event,
                            usage: result.usage,
                            raw_output: result.raw_output.clone(),
                            provider_error: None,
                            controller_instruction,
                            error_kind: None,
                            http_status: None,
                            output_chars: result.output_chars,
                        });
                    }
                    // Pluggable checkers get the same strict policy checks as model-backed ones.
                    let raw =
                        serde_json::json!({"patch": result.patch, "decision": result.decision})
                            .to_string();
                    match parse_continuation_check(&raw, &context.task) {
                        Ok(valid) => {
                            if matches!(&valid.decision, ControllerDecision::Continue { confidence, .. }
                                | ControllerDecision::EmitTransition { confidence, .. } if *confidence < min_confidence)
                            {
                                low_confidence = true;
                            }
                            proposals.push((checker.name().to_owned(), valid));
                        }
                        Err(_) => failed = true,
                    }
                }
                Err(error) => {
                    budget.record_usage(error.usage());
                    let (error_kind, http_status) = error.operator_metadata();
                    if observe {
                        observations.push(CheckerObservation {
                            checker: checker.name().to_owned(),
                            mode: checker.mode(),
                            proposed_event: error.proposed_event(),
                            usage: error.usage(),
                            raw_output: error.raw_output().map(str::to_owned),
                            provider_error: (capture_payloads && error.raw_output().is_none())
                                .then(|| error.raw_diagnostic()),
                            controller_instruction: None,
                            error_kind: Some(error_kind),
                            http_status,
                            output_chars: error.output_chars(),
                        });
                    }
                    failed = true;
                }
            }
        }
        if failed {
            return PipelineResult {
                outcome: PipelineOutcome::FailedOpen {
                    error: "workflow checker failed".into(),
                },
                observations,
            };
        }
        if low_confidence {
            return PipelineResult {
                outcome: PipelineOutcome::LowConfidence,
                observations,
            };
        }
        let mut patch = empty_patch(context.task.version);
        let mut decision = None;
        for (_, proposal) in &proposals {
            if (!patch.is_empty() && !proposal.patch.is_empty() && patch != proposal.patch)
                || decision
                    .as_ref()
                    .is_some_and(|prior| prior != &proposal.decision)
            {
                return PipelineResult {
                    outcome: PipelineOutcome::Conflict {
                        checker_names: proposals.iter().map(|(name, _)| name.clone()).collect(),
                    },
                    observations,
                };
            }
            if !proposal.patch.is_empty() {
                patch = proposal.patch.clone();
            }
            decision = Some(proposal.decision.clone());
        }
        let outcome = match decision.unwrap_or(ControllerDecision::AwaitUser) {
            ControllerDecision::AwaitUser => PipelineOutcome::AwaitUser { patch },
            ControllerDecision::Continue {
                instruction,
                confidence,
            } => PipelineOutcome::Continue {
                patch,
                instruction,
                confidence,
            },
            ControllerDecision::EmitTransition {
                event,
                evidence,
                confidence,
            } => PipelineOutcome::Transition {
                patch,
                event,
                evidence,
                confidence,
            },
        };
        PipelineResult {
            outcome,
            observations,
        }
    }
}

fn empty_patch(expected_version: u64) -> TaskStatePatch {
    TaskStatePatch {
        expected_version,
        plan_append: Default::default(),
        step_updates: vec![],
        current_step_id: None,
        expected_action: None,
        checkpoint: None,
    }
}

#[derive(Clone)]
pub struct WorkflowModels {
    pub interpreter: Arc<dyn CompletionModel>,
    pub checker: Arc<dyn CompletionModel>,
    pub handoff: Arc<dyn CompletionModel>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutingOutcome {
    Managed {
        input_message_id: i64,
        state: WorkflowTaskState,
    },
    Unmanaged {
        input_message_id: i64,
    },
    Rejected {
        reason: String,
        state: Option<WorkflowTaskState>,
    },
}

#[derive(Clone, Copy, Default)]
pub struct InputHandlingContext<'a> {
    pub processing_id: Option<i64>,
    pub processing_attempt: Option<u32>,
    pub accepted_patch: Option<&'a TaskStatePatch>,
}

pub struct WorkflowInputHandler<'a> {
    pub store: &'a mut DialogStore,
    pub handoff_builder: &'a HandoffBuilder,
}

impl WorkflowInputHandler<'_> {
    pub async fn handle(
        &mut self,
        dialog_id: i64,
        snapshot: DialogWorkflowSnapshot,
        input: WorkflowInput,
        protocol_text: &str,
        confidence: Option<f32>,
        context: InputHandlingContext<'_>,
    ) -> Result<RoutingOutcome, WorkflowEngineError> {
        self.handle_accounted(
            dialog_id,
            snapshot,
            input,
            protocol_text,
            confidence,
            context,
            None,
            None,
        )
        .await
    }

    // Keep the public routing boundary intact while carrying per-human-call accounting.
    #[allow(clippy::too_many_arguments)]
    async fn handle_accounted(
        &mut self,
        dialog_id: i64,
        snapshot: DialogWorkflowSnapshot,
        input: WorkflowInput,
        protocol_text: &str,
        confidence: Option<f32>,
        context: InputHandlingContext<'_>,
        mut budget: Option<&mut AutonomyBudget>,
        mut diagnostics: Option<&mut WorkflowDiagnostics<'_>>,
    ) -> Result<RoutingOutcome, WorkflowEngineError> {
        // Forbidden controller intents never reach a repository or a service call.
        if let Err(error) = StateMachine::validate_source(&input.source, &input.intent) {
            if input.source != WorkflowInputSource::Human {
                return Ok(RoutingOutcome::Rejected {
                    reason: "workflow input source rejected".into(),
                    state: snapshot.current_task,
                });
            }
            return Err(error.into());
        }
        WorkflowIntent::human_continue(protocol_text)?;
        if confidence.is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)) {
            return Err(WorkflowEngineError::InvalidInputContext(
                "invalid confidence",
            ));
        }
        let source = snapshot.current_task.as_ref();
        let expected = expected_task(source);
        if source.is_some_and(|task| task.dialog_id != dialog_id) {
            return Err(StoreError::WorkflowConflict(dialog_id).into());
        }
        let human = input.source == WorkflowInputSource::Human;
        if human {
            if context.processing_id.is_some()
                || context.processing_attempt.is_some()
                || context.accepted_patch.is_some()
            {
                return Err(WorkflowEngineError::InvalidInputContext(
                    "human input cannot carry checker processing",
                ));
            }
        } else {
            let Some(task) = source else {
                return Ok(RoutingOutcome::Rejected {
                    reason: "controller input requires an active task".into(),
                    state: None,
                });
            };
            if task.status != TaskStatus::Active || task.phase == TaskPhase::Done {
                return Ok(RoutingOutcome::Rejected {
                    reason: "controller input requires an active unfinished task".into(),
                    state: Some(task.clone()),
                });
            }
            let Some(patch) = context.accepted_patch else {
                return Err(WorkflowEngineError::InvalidInputContext(
                    "controller input requires an accepted checker patch",
                ));
            };
            if context.processing_id.is_none()
                || context.processing_attempt.is_none()
                || confidence.is_none()
            {
                return Err(WorkflowEngineError::InvalidInputContext(
                    "controller input requires processing and confidence",
                ));
            }
            if patch.expected_version != task.version {
                return Err(StoreError::WorkflowConflict(dialog_id).into());
            }
        }

        let command = InputCommit {
            dialog_id,
            input: &input,
            protocol_text,
            confidence,
            expected_current_task: expected,
        };
        let preview = if let (Some(task), Some(patch)) = (source, context.accepted_patch) {
            let patch_context = if matches!(
                input.intent,
                WorkflowIntent::ProposeTransition {
                    event: TransitionEvent::ValidationFailed,
                    ..
                }
            ) {
                PatchContext::ValidationRepair
            } else {
                PatchContext::Normal
            };
            match task.preview_patch(patch, patch_context) {
                Ok(preview) => Some(preview),
                Err(_) => {
                    return self.reject(
                        command,
                        source,
                        context,
                        "workflow checker patch rejected",
                    );
                }
            }
        } else {
            None
        };
        match &input.intent {
            WorkflowIntent::StartNewTask { goal } => {
                if StateMachine::validate_new_task(source).is_err() {
                    return self.reject(command, source, context, "workflow task start rejected");
                }
                let started = self.store.create_task_with_human_input(
                    dialog_id,
                    protocol_text,
                    goal,
                    expected,
                )?;
                Ok(RoutingOutcome::Managed {
                    input_message_id: started.message_id,
                    state: started.task,
                })
            }
            WorkflowIntent::Continue { .. }
                if human && source.is_some_and(|task| task.phase == TaskPhase::Done) =>
            {
                let saved = self
                    .store
                    .append_input(command, AcceptedInputEffect::RouteUnmanaged)?;
                Ok(RoutingOutcome::Unmanaged {
                    input_message_id: saved.message_id,
                })
            }
            WorkflowIntent::Continue { .. } => {
                let Some(task) = source else {
                    return self.reject(command, source, context, "no workflow task to continue");
                };
                if human {
                    let effect = if task.status == TaskStatus::Paused {
                        AcceptedInputEffect::ResumeSameStage
                    } else {
                        AcceptedInputEffect::ContinueSameStage
                    };
                    let saved = self.store.append_input(command, effect)?;
                    Ok(RoutingOutcome::Managed {
                        input_message_id: saved.message_id,
                        state: saved.task.expect("accepted continuation has a task"),
                    })
                } else {
                    let WorkflowInputSource::Controller {
                        checker,
                        model,
                        triggering_assistant_message_id,
                    } = &input.source
                    else {
                        unreachable!()
                    };
                    let result = self
                        .store
                        .commit_controller_decision(ControllerInputCommit {
                            processing_id: context
                                .processing_id
                                .expect("validated controller processing"),
                            task_id: task.id,
                            stage_run_id: task.current_stage_run_id,
                            expected_version: task.version,
                            expected_attempt: context
                                .processing_attempt
                                .expect("validated processing attempt"),
                            checker,
                            model,
                            triggering_assistant_message_id: *triggering_assistant_message_id,
                            instruction: protocol_text,
                            intent: &input.intent,
                            confidence: confidence.expect("validated confidence"),
                            accepted_patch: context
                                .accepted_patch
                                .expect("validated controller patch"),
                        })?;
                    let ProcessingResult::ControllerInput {
                        message_id,
                        task_version,
                        ..
                    } = result
                    else {
                        return Err(WorkflowEngineError::InvalidInputContext(
                            "unexpected controller processing result",
                        ));
                    };
                    let state = self
                        .store
                        .load_workflow(dialog_id)?
                        .current_task
                        .ok_or(StoreError::WorkflowConflict(dialog_id))?;
                    if state.id != task.id
                        || state.current_stage_run_id != task.current_stage_run_id
                        || state.version != task_version
                    {
                        return Err(StoreError::WorkflowConflict(dialog_id).into());
                    }
                    Ok(RoutingOutcome::Managed {
                        input_message_id: message_id,
                        state,
                    })
                }
            }
            WorkflowIntent::ProposeTransition { .. } | WorkflowIntent::ReplanCurrent { .. } => {
                let Some(task) = source else {
                    return self.reject(
                        command,
                        source,
                        context,
                        "no workflow task to change stage",
                    );
                };
                let preview = preview.as_ref().unwrap_or(task);
                let authorization = match &input.intent {
                    WorkflowIntent::ProposeTransition { event, evidence } => {
                        StateMachine::authorize(preview, *event, evidence)
                            .map(StageChangeAuthorization::Transition)
                    }
                    WorkflowIntent::ReplanCurrent { change_request } => {
                        StateMachine::authorize_replan(
                            preview,
                            &input.source,
                            change_request.clone(),
                        )
                        .map(StageChangeAuthorization::Replan)
                    }
                    _ => unreachable!(),
                };
                let authorization = match authorization {
                    Ok(authorization) => authorization,
                    Err(_) => {
                        return self.reject(
                            command,
                            source,
                            context,
                            "workflow stage change rejected",
                        );
                    }
                };
                let messages = self
                    .store
                    .load_stage_messages(task.current_stage_run_id)?
                    .into_iter()
                    .map(|row| row.message)
                    .collect::<Vec<_>>();
                let capture_payloads = diagnostics
                    .as_deref()
                    .is_some_and(|diagnostics| diagnostics.capture_payloads);
                let handoff = match self
                    .handoff_builder
                    .build_observed(&authorization, preview, &messages, &input, capture_payloads)
                    .await
                {
                    Ok(handoff) => handoff,
                    Err(error) => {
                        if let Some(budget) = budget.as_deref_mut() {
                            budget.record_usage(error.usage());
                        }
                        if let (Some(diagnostics), Some(budget)) =
                            (diagnostics.as_deref_mut(), budget.as_deref())
                        {
                            record_handoff_diagnostic(
                                diagnostics,
                                self.handoff_builder.model_name(),
                                &input,
                                task,
                                None,
                                &authorization,
                                context.processing_id,
                                "failed",
                                false,
                                "failed",
                                None,
                                Some(&error),
                                budget,
                                messages.len(),
                            );
                        }
                        return self.reject(command, source, context, "workflow handoff failed");
                    }
                };
                if let Some(budget) = budget.as_deref_mut() {
                    budget.record_usage(handoff.usage);
                    if !human {
                        let mut next =
                            match project_handoff(preview, &authorization, &handoff.payload) {
                                Ok(next) => next,
                                Err(error) => {
                                    if let Some(diagnostics) = diagnostics.as_deref_mut() {
                                        record_handoff_diagnostic(
                                            diagnostics,
                                            self.handoff_builder.model_name(),
                                            &input,
                                            task,
                                            None,
                                            &authorization,
                                            context.processing_id,
                                            "processing",
                                            false,
                                            "failed",
                                            Some(&handoff),
                                            None,
                                            budget,
                                            messages.len(),
                                        );
                                    }
                                    return Err(error.into());
                                }
                            };
                        next.version = task.version.saturating_add(1);
                        if let Err(reason) = budget.check_turn(&StateFingerprint::from(&next)) {
                            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                                record_handoff_diagnostic(
                                    diagnostics,
                                    self.handoff_builder.model_name(),
                                    &input,
                                    task,
                                    None,
                                    &authorization,
                                    context.processing_id,
                                    "processing",
                                    false,
                                    "rejected",
                                    Some(&handoff),
                                    None,
                                    budget,
                                    messages.len(),
                                );
                            }
                            return Err(WorkflowEngineError::AutonomyStopped(reason));
                        }
                    }
                }
                let saved = match self.store.commit_stage_change(TransitionCommit {
                    dialog_id,
                    source_task: task,
                    authorization: &authorization,
                    triggering_input: &input,
                    protocol_text,
                    confidence,
                    accepted_patch: context.accepted_patch,
                    handoff: &handoff.payload,
                    processing_id: context.processing_id,
                    processing_attempt: context.processing_attempt,
                }) {
                    Ok(saved) => saved,
                    Err(error) => {
                        if let (Some(diagnostics), Some(budget)) =
                            (diagnostics.as_deref_mut(), budget.as_deref())
                        {
                            record_handoff_diagnostic(
                                diagnostics,
                                self.handoff_builder.model_name(),
                                &input,
                                task,
                                None,
                                &authorization,
                                context.processing_id,
                                if human { "none" } else { "processing" },
                                false,
                                "failed",
                                Some(&handoff),
                                None,
                                budget,
                                messages.len(),
                            );
                        }
                        return Err(error.into());
                    }
                };
                if let (Some(diagnostics), Some(budget)) = (diagnostics, budget.as_deref()) {
                    record_handoff_diagnostic(
                        diagnostics,
                        self.handoff_builder.model_name(),
                        &input,
                        task,
                        Some(&saved.target_state),
                        &authorization,
                        context.processing_id,
                        if human { "none" } else { "completed" },
                        true,
                        "accepted",
                        Some(&handoff),
                        None,
                        budget,
                        messages.len(),
                    );
                }
                Ok(RoutingOutcome::Managed {
                    input_message_id: saved.input_message_id,
                    state: saved.target_state,
                })
            }
        }
    }

    fn reject(
        &mut self,
        command: InputCommit<'_>,
        source: Option<&WorkflowTaskState>,
        context: InputHandlingContext<'_>,
        reason: &'static str,
    ) -> Result<RoutingOutcome, WorkflowEngineError> {
        if command.input.source == WorkflowInputSource::Human {
            self.store.append_input(
                command,
                AcceptedInputEffect::Reject {
                    reason: reason.to_owned(),
                },
            )?;
        } else if let (Some(processing), Some(task)) = (context.processing_id, source) {
            let WorkflowInputSource::Controller {
                checker,
                triggering_assistant_message_id,
                ..
            } = &command.input.source
            else {
                unreachable!()
            };
            self.store.fail_processing(FailProcessingCommit {
                processing_id: processing,
                dialog_id: command.dialog_id,
                task_id: task.id,
                stage_run_id: task.current_stage_run_id,
                expected_version: task.version,
                expected_attempt: context
                    .processing_attempt
                    .expect("validated processing attempt"),
                triggering_assistant_message_id: *triggering_assistant_message_id,
                checker,
                diagnostic: reason,
            })?;
        }
        Ok(RoutingOutcome::Rejected {
            reason: reason.to_owned(),
            state: source.cloned(),
        })
    }
}

fn expected_task(task: Option<&WorkflowTaskState>) -> ExpectedCurrentTask {
    task.map_or(ExpectedCurrentTask::Absent, |task| {
        ExpectedCurrentTask::Present {
            task_id: task.id,
            version: task.version,
        }
    })
}

/// Borrowed session state is updated at each durable boundary, before the next await.
pub struct WorkflowSession<'a> {
    pub store: &'a mut DialogStore,
    pub dialog_id: &'a mut Option<i64>,
    pub scope: &'a mut RequestScope,
    pub history: &'a mut ChatHistory,
    pub persisted_message_count: &'a mut usize,
    pub last_usage: &'a mut Option<TokenUsage>,
}

pub struct WorkflowEngine<'a> {
    client: &'a DeepSeekClient,
    context_config: &'a ContextConfig,
    interpreter: HumanInputInterpreter,
    handoff_builder: HandoffBuilder,
    pipeline: ResponsePipeline,
    workflow_config: WorkflowConfig,
    interpreter_model: String,
    checker_model: String,
    diagnostics: Option<WorkflowDiagnostics<'a>>,
    session: WorkflowSession<'a>,
}

struct WorkflowDiagnostics<'a> {
    emit: &'a mut (dyn FnMut(WorkflowDebugEvent) -> bool + Send),
    capture_payloads: bool,
}

impl WorkflowDiagnostics<'_> {
    fn emit(&mut self, event: WorkflowDebugEvent) {
        let payloads_still_enabled = (self.emit)(event);
        self.capture_payloads &= payloads_still_enabled;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowTurnEvent {
    AutonomousTurnStarted {
        number: u32,
        phase: TaskPhase,
    },
    ResponseStarted {
        autonomous_turn: u32,
        phase: TaskPhase,
    },
    InputRejected {
        reason: String,
    },
    ProcessingFailed {
        checker: String,
        error: String,
    },
    Stopped {
        reason: AutonomyStopReason,
    },
}

#[derive(Debug)]
pub struct WorkflowTurnResult {
    pub routing: RoutingOutcome,
    pub answer: Option<String>,
    pub persisted_answer: Option<PersistedAnswer>,
    pub stop_reason: AutonomyStopReason,
    pub autonomous_turns: u32,
    pub tokens: u64,
    pub usage_complete: bool,
    pub final_state: Option<WorkflowTaskState>,
}

#[derive(Debug)]
pub struct RecoveredProcessing {
    pub processing_id: i64,
    pub stop_reason: AutonomyStopReason,
}

#[derive(Debug)]
pub enum ProcessingOutcome {
    Stop(AutonomyStopReason),
    Controller(Box<RoutingOutcome>),
    Completed(ProcessingResult),
}

struct OrdinaryTurn {
    answer: String,
    persisted_answer: Option<PersistedAnswer>,
}

#[derive(Clone)]
struct InputObservation {
    component: &'static str,
    model: String,
    mode: &'static str,
    input_state: Option<WorkflowTaskState>,
    proposed_event: Option<String>,
    usage: Option<TokenUsage>,
    raw_output: Option<String>,
    provider_error: Option<String>,
    error_kind: Option<&'static str>,
    http_status: Option<u16>,
    output_chars: usize,
    stage_message_count: usize,
    model_output_accepted: bool,
}

impl<'a> WorkflowEngine<'a> {
    pub fn new(
        client: &'a DeepSeekClient,
        context_config: &'a ContextConfig,
        workflow_config: &WorkflowConfig,
        models: &WorkflowModels,
        session: WorkflowSession<'a>,
    ) -> Self {
        Self {
            client,
            context_config,
            interpreter: HumanInputInterpreter::new(models.interpreter.clone(), workflow_config),
            handoff_builder: HandoffBuilder::new(models.handoff.clone(), workflow_config),
            pipeline: ResponsePipeline::new(vec![Arc::new(ContinuationChecker::new(
                models.checker.clone(),
                workflow_config,
            ))])
            .expect("continuation checker is advisory"),
            workflow_config: workflow_config.clone(),
            interpreter_model: models.interpreter.name().to_owned(),
            checker_model: models.checker.name().to_owned(),
            diagnostics: None,
            session,
        }
    }

    pub fn with_diagnostics(
        mut self,
        emit: &'a mut (dyn FnMut(WorkflowDebugEvent) -> bool + Send),
        capture_payloads: bool,
    ) -> Self {
        self.diagnostics = Some(WorkflowDiagnostics {
            emit,
            capture_payloads,
        });
        self
    }

    pub fn with_pipeline(mut self, pipeline: ResponsePipeline) -> Self {
        self.pipeline = pipeline;
        self
    }

    fn diagnostics_enabled(&self) -> bool {
        self.diagnostics.is_some()
    }

    fn capture_payloads(&self) -> bool {
        self.diagnostics
            .as_ref()
            .is_some_and(|diagnostics| diagnostics.capture_payloads)
    }

    fn payload_for_task(&self, task: Option<&WorkflowTaskState>) -> Option<WorkflowDebugPayload> {
        if !self.capture_payloads() {
            return None;
        }
        Some(WorkflowDebugPayload {
            plan: task.and_then(|task| serde_json::to_string(&task.plan).ok()),
            checkpoint: task.and_then(|task| serde_json::to_string(&task.checkpoint).ok()),
            ..WorkflowDebugPayload::default()
        })
    }

    fn record_diagnostic(
        &mut self,
        metadata: WorkflowDebugMetadata,
        payload: Option<WorkflowDebugPayload>,
    ) {
        if let Some(diagnostics) = self.diagnostics.as_mut() {
            diagnostics.emit(WorkflowDebugEvent { metadata, payload });
        }
    }

    fn record_input_observation(
        &mut self,
        observation: InputObservation,
        routing: &RoutingOutcome,
        prompt: &str,
        budget: &AutonomyBudget,
        accepted_override: Option<bool>,
    ) {
        if !self.diagnostics_enabled() {
            return;
        }
        let routed_output_state = match routing {
            RoutingOutcome::Managed { state, .. } => Some(state),
            RoutingOutcome::Rejected { state, .. } => state.as_ref(),
            RoutingOutcome::Unmanaged { .. } => observation.input_state.as_ref(),
        };
        let accepted =
            accepted_override.unwrap_or(!matches!(routing, RoutingOutcome::Rejected { .. }));
        let output_state = if accepted_override == Some(false) {
            observation.input_state.as_ref()
        } else {
            routed_output_state
        };
        let outcome = if !accepted {
            if observation.error_kind == Some("low_confidence") {
                "rejected"
            } else if observation.error_kind.is_some() {
                "failed"
            } else {
                "rejected"
            }
        } else if observation.error_kind.is_some() {
            "fallback"
        } else {
            "accepted"
        };
        let state_changed = match (observation.input_state.as_ref(), output_state) {
            (Some(input), Some(output)) => {
                input.version != output.version
                    || input.current_stage_run_id != output.current_stage_run_id
            }
            (None, Some(_)) => true,
            _ => false,
        };
        let transition_id = if accepted && state_changed && observation.proposed_event.is_some() {
            output_state.and_then(|task| task.incoming_handoff_id)
        } else {
            None
        };
        let mut payload = self.payload_for_task(observation.input_state.as_ref());
        if let Some(payload) = payload.as_mut() {
            payload.interpreter_output = observation.raw_output;
            payload.provider_error = observation.provider_error;
            payload.model_prompt = Some(prompt.to_owned());
        }
        let metadata = workflow_debug_metadata(
            "human",
            observation.component,
            &observation.model,
            observation.mode,
            observation.input_state.as_ref(),
            output_state,
            observation.proposed_event,
            accepted,
            outcome,
            budget,
            observation.input_state.as_ref().or(output_state),
            transition_id,
            None,
            "none",
            observation.error_kind,
            observation.http_status,
            observation.usage,
            prompt.chars().count(),
            observation.output_chars,
            observation.stage_message_count,
        );
        self.record_diagnostic(metadata, payload);
    }

    #[allow(clippy::too_many_arguments)]
    fn record_checker_observations(
        &mut self,
        observations: Vec<CheckerObservation>,
        task: &WorkflowTaskState,
        output_state: Option<&WorkflowTaskState>,
        processing_id: i64,
        processing_status: &str,
        accepted: bool,
        outcome: &str,
        response_chars: usize,
        stage_message_count: usize,
        budget: &AutonomyBudget,
    ) {
        if !self.diagnostics_enabled() {
            return;
        }
        for observation in observations {
            let state_changed = output_state.is_some_and(|output| {
                output.version != task.version
                    || output.current_stage_run_id != task.current_stage_run_id
            });
            let transition_id = if accepted && state_changed && observation.proposed_event.is_some()
            {
                output_state.and_then(|state| state.incoming_handoff_id)
            } else {
                None
            };
            let mut payload = self.payload_for_task(Some(task));
            if let Some(payload) = payload.as_mut() {
                payload.checker_output = observation.raw_output;
                payload.provider_error = observation.provider_error;
                payload.controller_instruction = observation.controller_instruction;
            }
            let metadata = workflow_debug_metadata(
                "controller",
                &observation.checker,
                &self.checker_model,
                checker_mode_name(observation.mode),
                Some(task),
                output_state.or(Some(task)),
                observation
                    .proposed_event
                    .map(|event| transition_event_name(event).to_owned()),
                accepted,
                outcome,
                budget,
                Some(task),
                transition_id,
                Some(processing_id),
                processing_status,
                observation.error_kind,
                observation.http_status,
                observation.usage,
                response_chars,
                observation.output_chars,
                stage_message_count,
            );
            self.record_diagnostic(metadata, payload);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_service_diagnostic(
        &mut self,
        component: &'static str,
        mode: &'static str,
        task: Option<&WorkflowTaskState>,
        budget: &AutonomyBudget,
        accepted: bool,
        outcome: &'static str,
        error: Option<&ClientError>,
        failure_kind: Option<&'static str>,
        usage: Option<TokenUsage>,
        input_chars: usize,
        output_chars: usize,
        stage_message_count: usize,
        processing_id: Option<i64>,
        processing_status: &'static str,
        model_prompt: Option<String>,
        model_output: Option<String>,
    ) {
        if !self.diagnostics_enabled() {
            return;
        }
        let error_metadata = error.map(ClientError::operator_metadata);
        let mut payload = self.payload_for_task(task);
        if let Some(payload) = payload.as_mut() {
            payload.model_prompt = model_prompt;
            payload.model_output = model_output;
            payload.provider_error = error.map(ClientError::raw_diagnostic);
        }
        let metadata = workflow_debug_metadata(
            if budget.turns == 0 {
                "human"
            } else {
                "controller"
            },
            component,
            self.client.model_name(),
            mode,
            task,
            task,
            None,
            accepted,
            outcome,
            budget,
            task,
            None,
            processing_id,
            processing_status,
            failure_kind.or_else(|| error_metadata.map(|metadata| metadata.kind)),
            error_metadata.and_then(|metadata| metadata.status),
            usage,
            input_chars,
            output_chars,
            stage_message_count,
        );
        self.record_diagnostic(metadata, payload);
    }

    pub async fn run_human_input<F>(
        &mut self,
        prompt: &str,
        mut on_event: F,
    ) -> Result<WorkflowTurnResult, WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        *self.session.last_usage = None;
        let mut budget = AutonomyBudget::new(&self.workflow_config);
        WorkflowIntent::human_continue(prompt)?;
        let (mut routing, input_observation) = if let Some(dialog_id) = *self.session.dialog_id {
            let snapshot = self.session.store.load_workflow(dialog_id)?;
            let input_state = snapshot.current_task.clone();
            let stage_message_count = if self.diagnostics_enabled() {
                input_state
                    .as_ref()
                    .map(|task| {
                        self.session
                            .store
                            .count_stage_messages(task.current_stage_run_id)
                    })
                    .transpose()?
                    .unwrap_or(0)
            } else {
                0
            };
            let capture_payloads = self.capture_payloads();
            let (intent, confidence, observation) = if let Some(task) = input_state.as_ref() {
                let observed = self
                    .interpreter
                    .interpret_observed(prompt, Some(task), capture_payloads)
                    .await?;
                let usage = match &observed.interpretation {
                    HumanInterpretation::Managed { usage, .. }
                    | HumanInterpretation::Unmanaged { usage } => *usage,
                };
                let (intent, confidence) = match observed.interpretation {
                    HumanInterpretation::Managed {
                        intent,
                        confidence,
                        usage,
                    } => {
                        budget.record_usage(usage);
                        (intent, Some(confidence))
                    }
                    HumanInterpretation::Unmanaged { usage } => {
                        budget.record_usage(usage);
                        (WorkflowIntent::human_continue(prompt)?, None)
                    }
                };
                let observation = InputObservation {
                    component: "human_input_interpreter",
                    model: self.interpreter_model.clone(),
                    mode: "advisory",
                    input_state: input_state.clone(),
                    proposed_event: observed.proposed_event,
                    usage,
                    raw_output: observed.raw_output,
                    provider_error: observed.provider_error,
                    error_kind: observed.failure_kind,
                    http_status: observed.http_status,
                    output_chars: observed.output_chars,
                    stage_message_count,
                    model_output_accepted: observed.model_output_accepted,
                };
                self.record_input_observation(
                    observation.clone(),
                    &RoutingOutcome::Managed {
                        input_message_id: 0,
                        state: task.clone(),
                    },
                    prompt,
                    &budget,
                    Some(observation.model_output_accepted),
                );
                (intent, confidence, observation)
            } else {
                let intent = WorkflowIntent::StartNewTask {
                    goal: prompt.trim().to_owned(),
                };
                let observation = InputObservation {
                    component: "input_router",
                    model: "local".into(),
                    mode: "deterministic",
                    input_state: None,
                    proposed_event: None,
                    usage: None,
                    raw_output: None,
                    provider_error: None,
                    error_kind: None,
                    http_status: None,
                    output_chars: 0,
                    stage_message_count: 0,
                    model_output_accepted: true,
                };
                (intent, None, observation)
            };
            let routing = WorkflowInputHandler {
                store: self.session.store,
                handoff_builder: &self.handoff_builder,
            }
            .handle_accounted(
                dialog_id,
                snapshot,
                WorkflowInput {
                    source: WorkflowInputSource::Human,
                    intent,
                },
                prompt,
                confidence,
                InputHandlingContext::default(),
                Some(&mut budget),
                self.diagnostics.as_mut(),
            )
            .await?;
            (routing, observation)
        } else {
            let started = self.session.store.start_dialog_with_workflow_task(
                self.session.scope,
                self.session.history.system_prompt(),
                prompt,
            )?;
            *self.session.dialog_id = Some(started.dialog_id);
            *self.session.scope = self.session.scope.with_dialog_id(Some(started.dialog_id));
            let routing = RoutingOutcome::Managed {
                input_message_id: started.message_id,
                state: started.task,
            };
            let observation = InputObservation {
                component: "input_router",
                model: "local".into(),
                mode: "deterministic",
                input_state: None,
                proposed_event: None,
                usage: None,
                raw_output: None,
                provider_error: None,
                error_kind: None,
                http_status: None,
                output_chars: 0,
                stage_message_count: 0,
                model_output_accepted: true,
            };
            (routing, observation)
        };
        if input_observation.component == "human_input_interpreter"
            && (!input_observation.model_output_accepted
                || matches!(&routing, RoutingOutcome::Rejected { .. }))
        {
            let local_observation = InputObservation {
                component: "input_router",
                model: "local".into(),
                mode: "deterministic",
                input_state: input_observation.input_state.clone(),
                proposed_event: matches!(&routing, RoutingOutcome::Rejected { .. })
                    .then(|| input_observation.proposed_event.clone())
                    .flatten(),
                usage: None,
                raw_output: None,
                provider_error: None,
                error_kind: None,
                http_status: None,
                output_chars: 0,
                stage_message_count: input_observation.stage_message_count,
                model_output_accepted: true,
            };
            self.record_input_observation(local_observation, &routing, prompt, &budget, None);
        } else if input_observation.component != "human_input_interpreter" {
            self.record_input_observation(input_observation, &routing, prompt, &budget, None);
        }
        *self.session.persisted_message_count += 1;
        self.session.history.push(Role::User, prompt.to_owned());
        match &routing {
            RoutingOutcome::Rejected { reason, .. } => {
                emit(
                    &mut on_event,
                    AgentEvent::Workflow(WorkflowTurnEvent::InputRejected {
                        reason: reason.clone(),
                    }),
                )?;
                return self.finish(
                    routing,
                    None,
                    None,
                    AutonomyStopReason::AwaitUser,
                    &budget,
                    &mut on_event,
                );
            }
            RoutingOutcome::Managed { state, .. } if state.phase == TaskPhase::Done => {
                return self.finish(
                    routing,
                    None,
                    None,
                    AutonomyStopReason::Done,
                    &budget,
                    &mut on_event,
                );
            }
            _ => {}
        }
        loop {
            let turn = self
                .run_one_ordinary_turn(&routing, prompt, &mut budget, &mut on_event)
                .await?;
            let Some(ref persisted) = turn.persisted_answer else {
                return self.finish(
                    routing,
                    Some(turn.answer),
                    None,
                    AutonomyStopReason::AwaitUser,
                    &budget,
                    &mut on_event,
                );
            };
            let processing = self
                .process_answer(
                    persisted.processing_id,
                    &mut budget,
                    ProcessingLeaseMode::Normal,
                )
                .await?;
            match processing {
                ProcessingOutcome::Stop(reason) => {
                    if let Some(error) = processing_failure_message(&reason) {
                        emit(
                            &mut on_event,
                            AgentEvent::Workflow(WorkflowTurnEvent::ProcessingFailed {
                                checker: "continuation".into(),
                                error: error.into(),
                            }),
                        )?;
                    }
                    return self.finish(
                        routing,
                        Some(turn.answer),
                        turn.persisted_answer,
                        reason,
                        &budget,
                        &mut on_event,
                    );
                }
                ProcessingOutcome::Completed(result) => {
                    let reason = if matches!(result, ProcessingResult::Transition { ref target_state, .. } if target_state.phase == TaskPhase::Done)
                    {
                        AutonomyStopReason::Done
                    } else {
                        AutonomyStopReason::AwaitUser
                    };
                    return self.finish(
                        routing,
                        Some(turn.answer),
                        turn.persisted_answer,
                        reason,
                        &budget,
                        &mut on_event,
                    );
                }
                ProcessingOutcome::Controller(next) => {
                    routing = *next;
                    if let RoutingOutcome::Managed { state, .. } = &routing {
                        if state.phase == TaskPhase::Done {
                            return self.finish(
                                routing,
                                Some(turn.answer),
                                turn.persisted_answer,
                                AutonomyStopReason::Done,
                                &budget,
                                &mut on_event,
                            );
                        }
                        // Scheduling is checked before the commit; reservation counts actual extra ordinary turns.
                        budget
                            .reserve_turn(StateFingerprint::from(state))
                            .map_err(WorkflowEngineError::AutonomyStopped)?;
                        emit(
                            &mut on_event,
                            AgentEvent::Workflow(WorkflowTurnEvent::AutonomousTurnStarted {
                                number: budget.turns,
                                phase: state.phase,
                            }),
                        )?;
                    }
                }
            }
        }
    }

    fn finish<F>(
        &self,
        routing: RoutingOutcome,
        answer: Option<String>,
        persisted_answer: Option<PersistedAnswer>,
        stop_reason: AutonomyStopReason,
        budget: &AutonomyBudget,
        on_event: &mut F,
    ) -> Result<WorkflowTurnResult, WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        emit(
            on_event,
            AgentEvent::Workflow(WorkflowTurnEvent::Stopped {
                reason: stop_reason.clone(),
            }),
        )?;
        let final_state = self
            .session
            .dialog_id
            .map(|id| self.session.store.load_workflow(id))
            .transpose()?
            .and_then(|snapshot| snapshot.current_task);
        Ok(WorkflowTurnResult {
            routing,
            answer,
            persisted_answer,
            stop_reason,
            autonomous_turns: budget.turns,
            tokens: budget.tokens,
            usage_complete: budget.usage_complete,
            final_state,
        })
    }

    /// Completed rows replay their stored result; they never dispatch controller work again.
    pub async fn process_answer(
        &mut self,
        processing_id: i64,
        budget: &mut AutonomyBudget,
        mode: ProcessingLeaseMode,
    ) -> Result<ProcessingOutcome, WorkflowEngineError> {
        match self.process_answer_inner(processing_id, budget, mode).await {
            Err(WorkflowEngineError::Store(
                StoreError::WorkflowConflict(_) | StoreError::Conflict(_),
            )) => {
                // Reload at the race boundary; finish/recovery also returns the selected durable state.
                if let Some(id) = *self.session.dialog_id {
                    self.session.store.load_workflow(id)?;
                }
                Ok(ProcessingOutcome::Stop(AutonomyStopReason::CheckerFailed))
            }
            result => result,
        }
    }

    async fn process_answer_inner(
        &mut self,
        processing_id: i64,
        budget: &mut AutonomyBudget,
        mode: ProcessingLeaseMode,
    ) -> Result<ProcessingOutcome, WorkflowEngineError> {
        let dialog_id = self
            .session
            .dialog_id
            .ok_or(WorkflowEngineError::InvalidInputContext(
                "processing requires a dialog",
            ))?;
        if let Some(result) = self
            .session
            .store
            .load_processing_result(dialog_id, processing_id)?
        {
            return Ok(ProcessingOutcome::Completed(result));
        }
        let work = self
            .session
            .store
            .load_processing_context(dialog_id, processing_id)?;
        let task = work.context.task.clone();
        let Some(lease) = self
            .session
            .store
            .lease_processing(processing_id, task.version, mode)?
        else {
            return Ok(ProcessingOutcome::Stop(AutonomyStopReason::CheckerFailed));
        };
        let stage_message_count = work.context.stage_messages.len();
        let pipeline = self
            .pipeline
            .collect_observed(
                &work.context,
                &work.response,
                budget,
                self.workflow_config.min_confidence(),
                self.diagnostics_enabled(),
                self.capture_payloads(),
            )
            .await;
        let mut observations = pipeline.observations;
        let (patch, intent, protocol, confidence) = match pipeline.outcome {
            PipelineOutcome::FailedOpen { .. } | PipelineOutcome::Conflict { .. } => {
                if let Err(error) = self.fail_advisory(
                    &task,
                    &lease,
                    &work.checker_name,
                    mode,
                    "workflow checker failed",
                ) {
                    self.record_checker_observations(
                        observations,
                        &task,
                        Some(&task),
                        processing_id,
                        "processing",
                        false,
                        "failed",
                        work.response.chars().count(),
                        stage_message_count,
                        budget,
                    );
                    return Err(error);
                }
                self.record_checker_observations(
                    observations,
                    &task,
                    Some(&task),
                    processing_id,
                    "failed",
                    false,
                    "failed",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                return Ok(ProcessingOutcome::Stop(AutonomyStopReason::CheckerFailed));
            }
            PipelineOutcome::LowConfidence => {
                if let Err(error) = self.fail_advisory(
                    &task,
                    &lease,
                    &work.checker_name,
                    mode,
                    "workflow checker confidence too low",
                ) {
                    self.record_checker_observations(
                        observations,
                        &task,
                        Some(&task),
                        processing_id,
                        "processing",
                        false,
                        "failed",
                        work.response.chars().count(),
                        stage_message_count,
                        budget,
                    );
                    return Err(error);
                }
                self.record_checker_observations(
                    observations,
                    &task,
                    Some(&task),
                    processing_id,
                    "failed",
                    false,
                    "rejected",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                return Ok(ProcessingOutcome::Stop(AutonomyStopReason::LowConfidence));
            }
            PipelineOutcome::AwaitUser { patch } => {
                let result = match self.complete_as_await(
                    &task,
                    &lease,
                    &patch,
                    mode,
                    if mode == ProcessingLeaseMode::Recovery {
                        AutonomyStopReason::AwaitUserAfterRestart
                    } else {
                        AutonomyStopReason::AwaitUser
                    },
                ) {
                    Ok(result) => result,
                    Err(error) => {
                        self.record_checker_observations(
                            observations,
                            &task,
                            Some(&task),
                            processing_id,
                            "processing",
                            false,
                            "failed",
                            work.response.chars().count(),
                            stage_message_count,
                            budget,
                        );
                        return Err(error);
                    }
                };
                let output_state = if self.diagnostics_enabled() {
                    self.session
                        .store
                        .load_workflow(dialog_id)
                        .ok()
                        .and_then(|snapshot| snapshot.current_task)
                } else {
                    None
                };
                self.record_checker_observations(
                    observations,
                    &task,
                    output_state.as_ref(),
                    processing_id,
                    "completed",
                    true,
                    "accepted",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                return Ok(result);
            }
            PipelineOutcome::Continue {
                patch,
                instruction,
                confidence,
            } => (
                patch,
                WorkflowIntent::Continue {
                    instruction: instruction.clone(),
                },
                instruction,
                confidence,
            ),
            PipelineOutcome::Transition {
                patch,
                event,
                evidence,
                confidence,
            } => (
                patch,
                WorkflowIntent::ProposeTransition { event, evidence },
                "Apply the approved workflow stage transition.".into(),
                confidence,
            ),
        };
        if mode == ProcessingLeaseMode::Recovery {
            let result = match self.complete_as_await(
                &task,
                &lease,
                &patch,
                mode,
                AutonomyStopReason::AwaitUserAfterRestart,
            ) {
                Ok(result) => result,
                Err(error) => {
                    self.record_checker_observations(
                        observations,
                        &task,
                        Some(&task),
                        processing_id,
                        "processing",
                        false,
                        "failed",
                        work.response.chars().count(),
                        stage_message_count,
                        budget,
                    );
                    return Err(error);
                }
            };
            let output_state = if self.diagnostics_enabled() {
                self.session
                    .store
                    .load_workflow(dialog_id)
                    .ok()
                    .and_then(|snapshot| snapshot.current_task)
            } else {
                None
            };
            self.record_checker_observations(
                observations,
                &task,
                output_state.as_ref(),
                processing_id,
                "completed",
                false,
                "rejected",
                work.response.chars().count(),
                stage_message_count,
                budget,
            );
            return Ok(result);
        }
        let checker_recorded_before_handoff =
            matches!(&intent, WorkflowIntent::ProposeTransition { .. });
        if checker_recorded_before_handoff {
            self.record_checker_observations(
                std::mem::take(&mut observations),
                &task,
                Some(&task),
                processing_id,
                "processing",
                true,
                "accepted",
                work.response.chars().count(),
                stage_message_count,
                budget,
            );
        }
        let patch_context = if matches!(
            intent,
            WorkflowIntent::ProposeTransition {
                event: TransitionEvent::ValidationFailed,
                ..
            }
        ) {
            PatchContext::ValidationRepair
        } else {
            PatchContext::Normal
        };
        let mut prospective = match task.preview_patch(&patch, patch_context) {
            Ok(prospective) => prospective,
            Err(error) => {
                self.record_checker_observations(
                    observations,
                    &task,
                    Some(&task),
                    processing_id,
                    "processing",
                    false,
                    "failed",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                return Err(error.into());
            }
        };
        prospective.version = task.version.saturating_add(1);
        if let Err(reason) = budget.check_turn(&StateFingerprint::from(&prospective)) {
            let result = match self.complete_as_await(&task, &lease, &patch, mode, reason) {
                Ok(result) => result,
                Err(error) => {
                    self.record_checker_observations(
                        observations,
                        &task,
                        Some(&task),
                        processing_id,
                        "processing",
                        false,
                        "failed",
                        work.response.chars().count(),
                        stage_message_count,
                        budget,
                    );
                    return Err(error);
                }
            };
            let output_state = if self.diagnostics_enabled() {
                self.session
                    .store
                    .load_workflow(dialog_id)
                    .ok()
                    .and_then(|snapshot| snapshot.current_task)
            } else {
                None
            };
            self.record_checker_observations(
                observations,
                &task,
                output_state.as_ref(),
                processing_id,
                "completed",
                false,
                "rejected",
                work.response.chars().count(),
                stage_message_count,
                budget,
            );
            return Ok(result);
        }
        let route = WorkflowInputHandler {
            store: self.session.store,
            handoff_builder: &self.handoff_builder,
        }
        .handle_accounted(
            dialog_id,
            DialogWorkflowSnapshot {
                current_task: Some(task.clone()),
            },
            WorkflowInput {
                source: WorkflowInputSource::Controller {
                    checker: work.checker_name.clone(),
                    model: self.checker_model.clone(),
                    triggering_assistant_message_id: lease.assistant_message_id,
                },
                intent,
            },
            &protocol,
            Some(confidence),
            InputHandlingContext {
                processing_id: Some(processing_id),
                processing_attempt: Some(lease.attempts),
                accepted_patch: Some(&patch),
            },
            Some(&mut *budget),
            self.diagnostics.as_mut(),
        )
        .await;
        match route {
            Ok(route @ RoutingOutcome::Rejected { .. }) => {
                let output_state = match &route {
                    RoutingOutcome::Rejected { state, .. } => state.as_ref(),
                    _ => unreachable!(),
                };
                self.record_checker_observations(
                    observations,
                    &task,
                    output_state,
                    processing_id,
                    "failed",
                    false,
                    "rejected",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                Ok(ProcessingOutcome::Stop(
                    AutonomyStopReason::TransitionFailed,
                ))
            }
            Ok(route) => {
                *self.session.persisted_message_count += 1;
                let output_state = match &route {
                    RoutingOutcome::Managed { state, .. } => Some(state),
                    RoutingOutcome::Unmanaged { .. } => Some(&task),
                    RoutingOutcome::Rejected { .. } => unreachable!(),
                };
                self.record_checker_observations(
                    observations,
                    &task,
                    output_state,
                    processing_id,
                    "completed",
                    true,
                    "accepted",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                Ok(ProcessingOutcome::Controller(Box::new(route)))
            }
            Err(WorkflowEngineError::AutonomyStopped(reason)) => {
                let result = match self.complete_as_await(&task, &lease, &patch, mode, reason) {
                    Ok(result) => result,
                    Err(error) => {
                        self.record_checker_observations(
                            observations,
                            &task,
                            Some(&task),
                            processing_id,
                            "processing",
                            false,
                            "failed",
                            work.response.chars().count(),
                            stage_message_count,
                            budget,
                        );
                        return Err(error);
                    }
                };
                let output_state = if self.diagnostics_enabled() {
                    self.session
                        .store
                        .load_workflow(dialog_id)
                        .ok()
                        .and_then(|snapshot| snapshot.current_task)
                } else {
                    None
                };
                self.record_checker_observations(
                    observations,
                    &task,
                    output_state.as_ref(),
                    processing_id,
                    "completed",
                    false,
                    "rejected",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                Ok(result)
            }
            Err(error) => {
                self.record_checker_observations(
                    observations,
                    &task,
                    Some(&task),
                    processing_id,
                    "failed",
                    false,
                    "failed",
                    work.response.chars().count(),
                    stage_message_count,
                    budget,
                );
                Err(error)
            }
        }
    }

    fn complete_as_await(
        &mut self,
        task: &WorkflowTaskState,
        lease: &ProcessingLease,
        patch: &TaskStatePatch,
        mode: ProcessingLeaseMode,
        reason: AutonomyStopReason,
    ) -> Result<ProcessingOutcome, WorkflowEngineError> {
        // Validation repair additions cannot be applied while remaining in validation.
        let empty = empty_patch(task.version);
        let patch = if task.preview_patch(patch, PatchContext::Normal).is_ok() {
            patch
        } else {
            &empty
        };
        self.session.store.commit_await_user_with_mode(
            lease.processing_id,
            task.id,
            task.current_stage_run_id,
            task.version,
            lease.attempts,
            patch,
            mode,
        )?;
        Ok(ProcessingOutcome::Stop(reason))
    }

    fn fail_advisory(
        &mut self,
        task: &WorkflowTaskState,
        lease: &ProcessingLease,
        checker: &str,
        mode: ProcessingLeaseMode,
        diagnostic: &'static str,
    ) -> Result<(), WorkflowEngineError> {
        self.session.store.fail_processing_with_mode(
            FailProcessingCommit {
                processing_id: lease.processing_id,
                dialog_id: task.dialog_id,
                task_id: task.id,
                stage_run_id: task.current_stage_run_id,
                expected_version: task.version,
                expected_attempt: lease.attempts,
                triggering_assistant_message_id: lease.assistant_message_id,
                checker,
                diagnostic,
            },
            mode,
        )?;
        Ok(())
    }

    pub async fn recover_pending_processing(
        &mut self,
        dialog_id: i64,
    ) -> Result<Vec<RecoveredProcessing>, WorkflowEngineError> {
        if *self.session.dialog_id != Some(dialog_id) {
            return Err(WorkflowEngineError::InvalidInputContext(
                "recovery dialog does not match session",
            ));
        }
        let Some(task) = self.session.store.load_workflow(dialog_id)?.current_task else {
            return Ok(vec![]);
        };
        self.session
            .store
            .close_stale_processing(dialog_id, task.version)?;
        self.session.store.close_exhausted_processing(&task)?;
        let pending = self.session.store.load_pending_processing(dialog_id)?;
        let mut recovered = Vec::new();
        let mut budget = AutonomyBudget::new(&self.workflow_config);
        for job in pending {
            // An earlier patch or another session may make the remaining snapshot stale.
            let Some(current) = self.session.store.load_workflow(dialog_id)?.current_task else {
                break;
            };
            self.session
                .store
                .close_stale_processing(dialog_id, current.version)?;
            if current.id != task.id || current.version != job.expected_version {
                continue;
            }
            let stop_reason = match self
                .process_answer(job.id, &mut budget, ProcessingLeaseMode::Recovery)
                .await?
            {
                ProcessingOutcome::Stop(reason) => reason,
                ProcessingOutcome::Completed(_) => AutonomyStopReason::AwaitUserAfterRestart,
                ProcessingOutcome::Controller(_) => {
                    unreachable!("recovery never dispatches controller input")
                }
            };
            recovered.push(RecoveredProcessing {
                processing_id: job.id,
                stop_reason,
            });
        }
        Ok(recovered)
    }

    async fn run_one_ordinary_turn<F>(
        &mut self,
        routing: &RoutingOutcome,
        prompt: &str,
        budget: &mut AutonomyBudget,
        on_event: &mut F,
    ) -> Result<OrdinaryTurn, WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        let dialog_id = self
            .session
            .dialog_id
            .expect("persisted input has a dialog");
        let inherited = self.inherited_blocks()?;
        let selected = self.session.store.load_workflow(dialog_id)?.current_task;
        let (prepared, diagnostic_state, stage_message_count) = match &routing {
            RoutingOutcome::Managed { state, .. } => {
                let messages = self
                    .session
                    .store
                    .load_stage_messages(state.current_stage_run_id)?;
                let mut reductions = self
                    .session
                    .store
                    .load_stage_reductions(state.current_stage_run_id)?;
                self.refresh_stage_facts(state, &messages, &mut reductions, budget, on_event)
                    .await?;
                let stage = prepare_workflow_request(WorkflowRequestInput {
                    base_prompt: self.session.history.system_prompt(),
                    inherited_blocks: inherited.clone(),
                    task: state,
                    stage_messages: &messages,
                    pending_input: None,
                    context_config: self.context_config,
                    context_state: &reductions.context,
                    facts_state: &reductions.facts,
                });
                emit(
                    on_event,
                    AgentEvent::Workflow(WorkflowTurnEvent::ResponseStarted {
                        autonomous_turn: budget.turns,
                        phase: state.phase,
                    }),
                )?;
                (stage.prepared, Some(state.clone()), messages.len())
            }
            RoutingOutcome::Unmanaged { .. } => (
                prepare_request(
                    &ChatHistory::new(self.session.history.system_prompt().to_owned()),
                    &ContextState::default(),
                    &ContextConfig::full_history(),
                    prompt,
                    &inherited,
                ),
                selected.clone(),
                0,
            ),
            RoutingOutcome::Rejected { .. } => unreachable!("rejected input has no ordinary turn"),
        };
        let model_prompt = self
            .capture_payloads()
            .then(|| serde_json::to_string(prepared.messages()).ok())
            .flatten();
        let input_chars = prepared
            .messages()
            .iter()
            .map(|message| message.content().chars().count())
            .sum();
        let mut usage = None;
        let result = self
            .client
            .stream_chat_events(prepared.messages(), |event| match event {
                StreamEvent::Text(text) => on_event(AgentEvent::Text(text)),
                StreamEvent::Usage(value) => {
                    usage = Some(value);
                    on_event(AgentEvent::Usage(value))
                }
            })
            .await;
        *self.session.last_usage = usage;
        budget.record_usage(usage);
        let answer = match result {
            Ok(answer) => answer,
            Err(error) => {
                self.record_service_diagnostic(
                    "ordinary",
                    "generation",
                    diagnostic_state.as_ref(),
                    budget,
                    false,
                    "failed",
                    Some(&error),
                    None,
                    usage.or(error.usage()),
                    input_chars,
                    0,
                    stage_message_count,
                    None,
                    "none",
                    model_prompt,
                    None,
                );
                return Err(WorkflowEngineError::provider("ordinary", error));
            }
        };
        if answer.trim().is_empty() {
            let error = ClientError::EmptyAnswer;
            self.record_service_diagnostic(
                "ordinary",
                "generation",
                diagnostic_state.as_ref(),
                budget,
                false,
                "failed",
                Some(&error),
                None,
                usage,
                input_chars,
                0,
                stage_message_count,
                None,
                "none",
                model_prompt,
                None,
            );
            return Err(WorkflowEngineError::provider("ordinary", error));
        }
        let output_chars = answer.chars().count();
        let model_output = self.capture_payloads().then(|| answer.clone());
        let persisted_answer = match &routing {
            RoutingOutcome::Managed { state, .. } => {
                match self
                    .session
                    .store
                    .append_answer_for_processing(AnswerCommit {
                        dialog_id,
                        task_id: state.id,
                        stage_run_id: state.current_stage_run_id,
                        expected_version: state.version,
                        content: &answer,
                        usage,
                    }) {
                    Ok(answer) => Some(answer),
                    Err(error) => {
                        self.record_service_diagnostic(
                            "ordinary",
                            "generation",
                            diagnostic_state.as_ref(),
                            budget,
                            false,
                            "failed",
                            None,
                            Some("persistence"),
                            usage,
                            input_chars,
                            output_chars,
                            stage_message_count,
                            None,
                            "none",
                            model_prompt,
                            model_output,
                        );
                        return Err(error.into());
                    }
                }
            }
            RoutingOutcome::Unmanaged { input_message_id } => {
                if let Err(error) =
                    self.session
                        .store
                        .append_unmanaged_answer(UnmanagedAnswerCommit {
                            dialog_id,
                            input_message_id: *input_message_id,
                            expected_current_task: expected_task(selected.as_ref()),
                            content: &answer,
                            usage,
                        })
                {
                    self.record_service_diagnostic(
                        "ordinary",
                        "generation",
                        diagnostic_state.as_ref(),
                        budget,
                        false,
                        "failed",
                        None,
                        Some("persistence"),
                        usage,
                        input_chars,
                        output_chars,
                        stage_message_count,
                        None,
                        "none",
                        model_prompt,
                        model_output,
                    );
                    return Err(error.into());
                }
                None
            }
            RoutingOutcome::Rejected { .. } => unreachable!(),
        };
        *self.session.persisted_message_count += 1;
        self.session.history.push_answer(answer.clone(), usage);
        self.record_service_diagnostic(
            "ordinary",
            "generation",
            diagnostic_state.as_ref(),
            budget,
            true,
            "accepted",
            None,
            None,
            usage,
            input_chars,
            output_chars,
            stage_message_count,
            persisted_answer.as_ref().map(|answer| answer.processing_id),
            if persisted_answer.is_some() {
                "pending"
            } else {
                "none"
            },
            model_prompt,
            model_output,
        );
        if let RoutingOutcome::Managed { state, .. } = &routing
            && let Err(error) = self
                .compact_stage(
                    state,
                    persisted_answer.as_ref().map(|answer| answer.processing_id),
                    usage,
                    &inherited,
                    budget,
                    on_event,
                )
                .await
        {
            // The answer and its processing job are already durable. Compaction is advisory.
            let _ = emit(
                on_event,
                AgentEvent::CompactionFailed {
                    error: error.operator_message(),
                },
            );
        }
        Ok(OrdinaryTurn {
            answer,
            persisted_answer,
        })
    }

    async fn refresh_stage_facts<F>(
        &mut self,
        task: &WorkflowTaskState,
        messages: &[StageProtocolMessage],
        reductions: &mut StageReductionState,
        budget: &mut AutonomyBudget,
        on_event: &mut F,
    ) -> Result<(), WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        if self.context_config.strategy() != ContextStrategy::StickyFacts {
            return Ok(());
        }
        let candidates = facts_candidates(messages);
        let Some(plan) = plan_facts_update(&candidates, &reductions.facts) else {
            return Ok(());
        };
        emit(
            on_event,
            AgentEvent::FactsUpdateStarted {
                previous_boundary: reductions.facts.covered_message_count(),
                target_boundary: plan.covered_message_count(),
            },
        )?;
        let model_prompt = self
            .capture_payloads()
            .then(|| serde_json::to_string(plan.request_messages()).ok())
            .flatten();
        let input_chars = plan
            .request_messages()
            .iter()
            .map(|message| message.content().chars().count())
            .sum();
        let result = match self
            .client
            .update_facts(
                plan.request_messages(),
                self.context_config.facts_max_tokens(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                budget.record_usage(error.usage());
                self.record_service_diagnostic(
                    "facts",
                    "advisory",
                    Some(task),
                    budget,
                    false,
                    "failed",
                    Some(&error),
                    None,
                    error.usage(),
                    input_chars,
                    0,
                    messages.len(),
                    None,
                    "none",
                    model_prompt,
                    None,
                );
                let error = WorkflowEngineError::provider("facts", error);
                emit(
                    on_event,
                    AgentEvent::FactsUpdateFailed {
                        error: error.operator_message(),
                    },
                )?;
                return Err(error);
            }
        };
        budget.record_usage(result.usage());
        let output_chars = result.answer().chars().count();
        let model_output = self.capture_payloads().then(|| result.answer().to_owned());
        let facts = match parse_facts_json(result.answer()) {
            Ok(facts) => facts,
            Err(error) => {
                self.record_service_diagnostic(
                    "facts",
                    "advisory",
                    Some(task),
                    budget,
                    false,
                    "failed",
                    None,
                    Some("invalid_output"),
                    result.usage(),
                    input_chars,
                    output_chars,
                    messages.len(),
                    None,
                    "none",
                    model_prompt,
                    model_output,
                );
                let error = WorkflowEngineError::from(error);
                emit(
                    on_event,
                    AgentEvent::FactsUpdateFailed {
                        error: error.operator_message(),
                    },
                )?;
                return Err(error);
            }
        };
        let facts = match self.session.store.replace_stage_facts(
            task.current_stage_run_id,
            task.version,
            messages.len(),
            facts,
            result.usage(),
        ) {
            Ok(facts) => facts,
            Err(error) => {
                self.record_service_diagnostic(
                    "facts",
                    "advisory",
                    Some(task),
                    budget,
                    false,
                    "failed",
                    None,
                    Some("persistence"),
                    result.usage(),
                    input_chars,
                    output_chars,
                    messages.len(),
                    None,
                    "none",
                    model_prompt,
                    model_output,
                );
                let error = WorkflowEngineError::from(error);
                emit(
                    on_event,
                    AgentEvent::FactsUpdateFailed {
                        error: error.operator_message(),
                    },
                )?;
                return Err(error);
            }
        };
        reductions.facts = facts;
        self.record_service_diagnostic(
            "facts",
            "advisory",
            Some(task),
            budget,
            true,
            "accepted",
            None,
            None,
            result.usage(),
            input_chars,
            output_chars,
            messages.len(),
            None,
            "none",
            model_prompt,
            model_output,
        );
        emit(
            on_event,
            AgentEvent::FactsUpdateCompleted {
                covered_message_count: reductions.facts.covered_message_count(),
                usage: result.usage(),
            },
        )?;
        Ok(())
    }

    async fn compact_stage<F>(
        &mut self,
        task: &WorkflowTaskState,
        processing_id: Option<i64>,
        usage: Option<TokenUsage>,
        inherited: &[SystemBlock],
        budget: &mut AutonomyBudget,
        on_event: &mut F,
    ) -> Result<(), WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        if self.context_config.strategy() != ContextStrategy::Summary
            || budget.allow_provider_call().is_err()
            || !usage.is_some_and(|usage| {
                usage.prompt_tokens >= self.context_config.compact_after_prompt_tokens()
            })
        {
            return Ok(());
        }
        let messages = self
            .session
            .store
            .load_stage_messages(task.current_stage_run_id)?;
        let reductions = self
            .session
            .store
            .load_stage_reductions(task.current_stage_run_id)?;
        let Some(plan) = plan_stage_compaction(
            &messages,
            &reductions.context,
            self.context_config.keep_last_messages(),
            inherited,
        ) else {
            return Ok(());
        };
        emit(
            on_event,
            AgentEvent::CompactionStarted {
                threshold: self.context_config.compact_after_prompt_tokens(),
                covered_message_count: plan.covered_message_count(),
                kept_message_count: messages.len() - plan.covered_message_count(),
            },
        )?;
        let model_prompt = self
            .capture_payloads()
            .then(|| serde_json::to_string(plan.request_messages()).ok())
            .flatten();
        let input_chars = plan
            .request_messages()
            .iter()
            .map(|message| message.content().chars().count())
            .sum();
        let result = match self
            .client
            .summarize(
                plan.request_messages(),
                self.context_config.summary_max_tokens(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                budget.record_usage(error.usage());
                self.record_service_diagnostic(
                    "compaction",
                    "advisory",
                    Some(task),
                    budget,
                    false,
                    "failed",
                    Some(&error),
                    None,
                    error.usage(),
                    input_chars,
                    0,
                    messages.len(),
                    processing_id,
                    if processing_id.is_some() {
                        "pending"
                    } else {
                        "none"
                    },
                    model_prompt,
                    None,
                );
                return Err(WorkflowEngineError::provider("compaction", error));
            }
        };
        budget.record_usage(result.usage());
        let output_chars = result.answer().chars().count();
        let model_output = self.capture_payloads().then(|| result.answer().to_owned());
        if let Err(error) = self.session.store.replace_stage_context(
            task.current_stage_run_id,
            task.version,
            messages.len(),
            ContextSummary::new(result.answer(), plan.covered_message_count()),
            result.usage(),
        ) {
            self.record_service_diagnostic(
                "compaction",
                "advisory",
                Some(task),
                budget,
                false,
                "failed",
                None,
                Some("persistence"),
                result.usage(),
                input_chars,
                output_chars,
                messages.len(),
                processing_id,
                if processing_id.is_some() {
                    "pending"
                } else {
                    "none"
                },
                model_prompt,
                model_output,
            );
            return Err(error.into());
        }
        self.record_service_diagnostic(
            "compaction",
            "advisory",
            Some(task),
            budget,
            true,
            "accepted",
            None,
            None,
            result.usage(),
            input_chars,
            output_chars,
            messages.len(),
            processing_id,
            if processing_id.is_some() {
                "pending"
            } else {
                "none"
            },
            model_prompt,
            model_output,
        );
        emit(
            on_event,
            AgentEvent::CompactionCompleted {
                covered_message_count: plan.covered_message_count(),
                usage: result.usage(),
            },
        )?;
        Ok(())
    }

    fn inherited_blocks(&self) -> Result<Vec<SystemBlock>, WorkflowEngineError> {
        let mut blocks = Vec::new();
        if let Some(profile) = self
            .session
            .store
            .load_profile(self.session.scope.user_id())?
        {
            blocks.extend(profile.blocks(self.session.scope)?);
        }
        blocks.extend(
            self.session
                .store
                .load_memory(self.session.scope)?
                .blocks(self.session.scope)?,
        );
        Ok(blocks)
    }
}

fn processing_failure_message(reason: &AutonomyStopReason) -> Option<&'static str> {
    match reason {
        AutonomyStopReason::CheckerFailed => Some("workflow checker failed"),
        AutonomyStopReason::LowConfidence => Some("workflow checker confidence too low"),
        AutonomyStopReason::TransitionFailed => Some("workflow transition failed"),
        _ => None,
    }
}

fn transition_event_name(event: TransitionEvent) -> &'static str {
    match event {
        TransitionEvent::PlanningCompleted => "planning_completed",
        TransitionEvent::ExecutionCompleted => "execution_completed",
        TransitionEvent::ValidationPassed => "validation_passed",
        TransitionEvent::ValidationFailed => "validation_failed",
    }
}

fn checker_mode_name(mode: CheckerMode) -> &'static str {
    match mode {
        CheckerMode::Advisory => "advisory",
        CheckerMode::Blocking => "blocking",
    }
}

#[allow(clippy::too_many_arguments)]
fn record_handoff_diagnostic(
    diagnostics: &mut WorkflowDiagnostics<'_>,
    model: &str,
    input: &WorkflowInput,
    input_state: &WorkflowTaskState,
    output_state: Option<&WorkflowTaskState>,
    authorization: &StageChangeAuthorization,
    processing_id: Option<i64>,
    processing_status: &str,
    accepted: bool,
    outcome: &str,
    handoff: Option<&HandoffBuildResult>,
    error: Option<&CheckError>,
    budget: &AutonomyBudget,
    stage_message_count: usize,
) {
    let proposed_event = match authorization {
        StageChangeAuthorization::Transition(authorization) => {
            Some(transition_event_name(authorization.event).to_owned())
        }
        StageChangeAuthorization::Replan(_) => Some("replan_requested".into()),
    };
    let error_metadata = error.map(CheckError::operator_metadata);
    let mut payload = diagnostics.capture_payloads.then(|| WorkflowDebugPayload {
        plan: serde_json::to_string(&input_state.plan).ok(),
        checkpoint: serde_json::to_string(&input_state.checkpoint).ok(),
        ..WorkflowDebugPayload::default()
    });
    if let Some(payload) = payload.as_mut() {
        payload.handoff = handoff
            .and_then(|handoff| handoff.raw_output.clone())
            .or_else(|| error.and_then(CheckError::raw_output).map(str::to_owned));
        payload.provider_error = error
            .filter(|error| error.raw_output().is_none())
            .map(CheckError::raw_diagnostic);
    }
    let source = match &input.source {
        WorkflowInputSource::Human => "human",
        WorkflowInputSource::Controller { .. } => "controller",
    };
    let metadata = workflow_debug_metadata(
        source,
        "handoff_builder",
        model,
        "blocking",
        Some(input_state),
        output_state.or(Some(input_state)),
        proposed_event,
        accepted,
        outcome,
        budget,
        Some(input_state),
        accepted
            .then(|| output_state.and_then(|state| state.incoming_handoff_id))
            .flatten(),
        processing_id,
        processing_status,
        error_metadata.map(|metadata| metadata.0),
        error_metadata.and_then(|metadata| metadata.1),
        handoff
            .and_then(|handoff| handoff.usage)
            .or_else(|| error.and_then(CheckError::usage)),
        0,
        handoff.map_or_else(
            || error.map_or(0, CheckError::output_chars),
            |handoff| handoff.output_chars,
        ),
        stage_message_count,
    );
    diagnostics.emit(WorkflowDebugEvent { metadata, payload });
}

#[allow(clippy::too_many_arguments)]
fn workflow_debug_metadata(
    source: &str,
    component: &str,
    model: &str,
    mode: &str,
    input_state: Option<&WorkflowTaskState>,
    output_state: Option<&WorkflowTaskState>,
    proposed_event: Option<String>,
    accepted: bool,
    outcome: &str,
    budget: &AutonomyBudget,
    stage_state: Option<&WorkflowTaskState>,
    transition_id: Option<i64>,
    processing_id: Option<i64>,
    processing_status: &str,
    error_kind: Option<&str>,
    http_status: Option<u16>,
    usage: Option<TokenUsage>,
    input_chars: usize,
    output_chars: usize,
    stage_message_count: usize,
) -> WorkflowDebugMetadata {
    let counts = output_state.or(input_state);
    WorkflowDebugMetadata {
        source: source.to_owned(),
        component: component.to_owned(),
        model: model.to_owned(),
        mode: mode.to_owned(),
        input_version: input_state.map_or(0, |task| task.version),
        output_version: output_state.map(|task| task.version),
        proposed_event,
        accepted,
        outcome: outcome.to_owned(),
        autonomous_turn: budget.turns,
        autonomous_tokens: budget.tokens,
        stage_run_id: stage_state.map_or(0, |task| task.current_stage_run_id.0),
        transition_id,
        processing_id,
        processing_status: processing_status.to_owned(),
        error_kind: error_kind.map(str::to_owned),
        http_status,
        usage,
        input_chars,
        output_chars,
        stage_message_count,
        plan_step_count: counts.map_or(0, |task| task.plan.steps.len()),
        checkpoint_item_count: counts.map_or(0, |task| {
            task.checkpoint.decisions.len()
                + task.checkpoint.open_issues.len()
                + usize::from(!task.checkpoint.summary.is_empty())
        }),
    }
}

fn emit<F>(on_event: &mut F, event: AgentEvent<'_>) -> Result<(), WorkflowEngineError>
where
    F: FnMut(AgentEvent<'_>) -> io::Result<()>,
{
    on_event(event).map_err(ClientError::Output)?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum WorkflowEngineError {
    #[error("blocking response checkers require buffered delivery")]
    BlockingCheckerRequiresBufferedDelivery,
    #[error("autonomous continuation stopped: {0:?}")]
    AutonomyStopped(AutonomyStopReason),
    #[error("invalid workflow input context: {0}")]
    InvalidInputContext(&'static str),
    #[error(transparent)]
    Workflow(#[from] WorkflowError),
    #[error(transparent)]
    Policy(#[from] ModelPolicyError),
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{source}")]
    Provider {
        component: &'static str,
        #[source]
        source: ClientError,
    },
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Facts(#[from] FactsError),
}

impl WorkflowEngineError {
    fn provider(component: &'static str, source: ClientError) -> Self {
        Self::Provider { component, source }
    }

    pub fn operator_message(&self) -> String {
        match self {
            Self::Provider { component, source } => source.operator_message(component),
            Self::Client(source) => source.operator_message("workflow"),
            Self::Store(_) => "workflow persistence failed".into(),
            _ => self.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::debug_log::DebugLog;
    use crate::workflow_model::DeepSeekCompletionModel;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct FailingWriter {
        attempts: Arc<AtomicUsize>,
    }

    impl io::Write for FailingWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("injected debug write failure"))
        }
    }

    struct PayloadCaptureSpy {
        observed: Arc<std::sync::Mutex<Vec<bool>>>,
    }

    impl ResponseChecker for PayloadCaptureSpy {
        fn name(&self) -> &str {
            "payload-capture-spy"
        }

        fn mode(&self) -> CheckerMode {
            CheckerMode::Advisory
        }

        fn check<'b>(
            &'b self,
            _context: &'b CheckContext,
            _response: &'b str,
        ) -> crate::workflow_model::CheckFuture<'b> {
            panic!("observed checker entry point must carry payload eligibility")
        }

        fn check_observed<'b>(
            &'b self,
            context: &'b CheckContext,
            _response: &'b str,
            capture_payloads: bool,
        ) -> crate::workflow_model::CheckFuture<'b> {
            self.observed.lock().unwrap().push(capture_payloads);
            Box::pin(async move {
                Ok(crate::workflow_model::ContinuationCheckResult {
                    patch: empty_patch(context.task.version),
                    decision: ControllerDecision::AwaitUser,
                    usage: Some(TokenUsage {
                        prompt_tokens: 1,
                        completion_tokens: 1,
                        total_tokens: 2,
                        completion_tokens_details: None,
                    }),
                    raw_output: capture_payloads.then(|| "CHECKER_RAW".into()),
                    output_chars: 11,
                })
            })
        }
    }

    // Break caught: an early debug write failure must disable raw capture at
    // later provider boundaries, not merely prevent the payload from reaching
    // a file after it has already been cloned or serialized.
    #[tokio::test]
    async fn diagnostic_write_failure_disables_later_payload_construction() {
        let server = MockServer::start().await;
        let usage = TokenUsage {
            prompt_tokens: 2,
            completion_tokens: 1,
            total_tokens: 3,
            completion_tokens_details: None,
        };
        let chunk = json!({
            "choices":[{"delta":{"content":"ordinary answer"},"finish_reason":"stop"}],
            "usage": usage
        });
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n")),
            )
            .mount(&server)
            .await;
        let config = Config::from_toml(
            &format!(
                "api_key='test-key'\nbase_url='{}'\n[workflow]\n[context]\nstrategy='summary'",
                server.uri()
            ),
            None,
        )
        .unwrap();
        let client = DeepSeekClient::new(&config).unwrap();
        let model =
            Arc::new(DeepSeekCompletionModel::new(client.clone(), "service".into()).unwrap());
        let models = WorkflowModels {
            interpreter: model.clone(),
            checker: model.clone(),
            handoff: model,
        };
        let directory = tempfile::tempdir().unwrap();
        let mut store = DialogStore::open(&directory.path().join("dynamic-log.sqlite3")).unwrap();
        let mut dialog_id = None;
        let mut scope = RequestScope::default();
        let mut history = ChatHistory::new("BASE".into());
        let mut persisted_message_count = 0;
        let mut last_usage = None;
        let write_attempts = Arc::new(AtomicUsize::new(0));
        let mut debug_log = DebugLog::from_writer_for_test(
            FailingWriter {
                attempts: write_attempts.clone(),
            },
            true,
            "test-key",
        );
        let initially_enabled = debug_log.payloads_enabled();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pipeline = ResponsePipeline::new(vec![Arc::new(PayloadCaptureSpy {
            observed: observed.clone(),
        })])
        .unwrap();
        let mut warnings = Vec::new();
        let mut log_diagnostic = |event: WorkflowDebugEvent| {
            if let Some(warning) = debug_log.log_workflow(&event.metadata, event.payload.as_ref()) {
                warnings.push(warning);
            }
            debug_log.payloads_enabled()
        };

        let result = WorkflowEngine::new(
            &client,
            config.context(),
            config.workflow(),
            &models,
            WorkflowSession {
                store: &mut store,
                dialog_id: &mut dialog_id,
                scope: &mut scope,
                history: &mut history,
                persisted_message_count: &mut persisted_message_count,
                last_usage: &mut last_usage,
            },
        )
        .with_pipeline(pipeline)
        .with_diagnostics(&mut log_diagnostic, initially_enabled)
        .run_human_input("start task", |_| Ok(()))
        .await
        .unwrap();

        assert_eq!(result.stop_reason, AutonomyStopReason::AwaitUser);
        assert_eq!(write_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(*observed.lock().unwrap(), [false]);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("debug log disabled"));
    }

    // Facts failure aborts the turn, so inspect the actual budget at this private boundary.
    #[tokio::test]
    async fn failed_stage_facts_retains_reported_usage_without_mutating_state() {
        for complete in [false, true] {
            for reported_usage in [
                None,
                Some(TokenUsage {
                    prompt_tokens: 12,
                    completion_tokens: 5,
                    total_tokens: 17,
                    completion_tokens_details: None,
                }),
            ] {
                let server = MockServer::start().await;
                let chunk = json!({"choices":[{"delta":{"content":"   "},"finish_reason":"stop"}],"usage":reported_usage});
                Mock::given(method("POST"))
                    .and(path("/chat/completions"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .insert_header("content-type", "text/event-stream")
                            .set_body_string(format!(
                                "data: {chunk}\n\n{}",
                                if complete { "data: [DONE]\n\n" } else { "" }
                            )),
                    )
                    .mount(&server)
                    .await;
                let config = Config::from_toml(
                    &format!(
                        "api_key='test-key'\nbase_url='{}'\n[context]\nstrategy='sticky_facts'",
                        server.uri()
                    ),
                    None,
                )
                .unwrap();
                let client = DeepSeekClient::new(&config).unwrap();
                let model = Arc::new(
                    DeepSeekCompletionModel::new(client.clone(), "service".into()).unwrap(),
                );
                let models = WorkflowModels {
                    interpreter: model.clone(),
                    checker: model.clone(),
                    handoff: model,
                };
                let directory = tempfile::tempdir().unwrap();
                let mut store = DialogStore::open(&directory.path().join("facts.sqlite3")).unwrap();
                let mut scope = RequestScope::default();
                let started = store
                    .start_dialog_with_workflow_task(&scope, "BASE", "human fact")
                    .unwrap();
                let mut dialog_id = Some(started.dialog_id);
                scope = scope.with_dialog_id(dialog_id);
                let task = store
                    .load_workflow(started.dialog_id)
                    .unwrap()
                    .current_task
                    .unwrap();
                let messages = store
                    .load_stage_messages(task.current_stage_run_id)
                    .unwrap();
                let mut reductions = store
                    .load_stage_reductions(task.current_stage_run_id)
                    .unwrap();
                let original_facts = reductions.facts.clone();
                let mut history = ChatHistory::new("BASE".into());
                let mut persisted_message_count =
                    store.raw_message_count(started.dialog_id).unwrap();
                let mut last_usage = None;
                let mut budget = AutonomyBudget::new(config.workflow());
                let mut engine = WorkflowEngine::new(
                    &client,
                    config.context(),
                    config.workflow(),
                    &models,
                    WorkflowSession {
                        store: &mut store,
                        dialog_id: &mut dialog_id,
                        scope: &mut scope,
                        history: &mut history,
                        persisted_message_count: &mut persisted_message_count,
                        last_usage: &mut last_usage,
                    },
                );
                let mut failed = false;
                let error = engine
                    .refresh_stage_facts(
                        &task,
                        &messages,
                        &mut reductions,
                        &mut budget,
                        &mut |event| {
                            failed |= matches!(event, AgentEvent::FactsUpdateFailed { .. });
                            Ok(())
                        },
                    )
                    .await
                    .unwrap_err();
                assert!(failed);
                let WorkflowEngineError::Provider {
                    component: "facts",
                    source: error,
                } = error
                else {
                    panic!("expected facts provider error")
                };
                assert_eq!(error.usage(), reported_usage);
                assert_eq!(
                    budget.tokens(),
                    reported_usage.map_or(0, |usage| usage.total_tokens)
                );
                assert_eq!(budget.usage_complete(), reported_usage.is_some());
                assert_eq!(reductions.facts, original_facts);
                assert_eq!(
                    store
                        .load_stage_reductions(task.current_stage_run_id)
                        .unwrap()
                        .facts,
                    original_facts
                );
                assert_eq!(
                    store.load_workflow(started.dialog_id).unwrap().current_task,
                    Some(task)
                );
                assert_eq!(server.received_requests().await.unwrap().len(), 1);
            }
        }
    }
}
