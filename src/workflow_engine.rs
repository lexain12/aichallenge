//! Shared workflow routing and one complete, durable ordinary turn.

use std::io;
use std::sync::Arc;

use thiserror::Error;

use crate::agent::AgentEvent;
use crate::chat::{ChatHistory, Role};
use crate::client::{ClientError, DeepSeekClient, StreamEvent, TokenUsage};
use crate::config::{ContextConfig, ContextStrategy, WorkflowConfig};
use crate::context::{ContextState, ContextSummary, prepare_request};
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
    CompletionModel, HandoffBuilder, HumanInputInterpreter, HumanInterpretation, ModelPolicyError,
};
use crate::workflow_store::{
    AcceptedInputEffect, AnswerCommit, ControllerInputCommit, DialogWorkflowSnapshot,
    ExpectedCurrentTask, InputCommit, PersistedAnswer, ProcessingResult, StageProtocolMessage,
    TransitionCommit, UnmanagedAnswerCommit, WorkflowRepository,
};

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
        // Forbidden controller intents never reach a repository or a service call.
        if let Err(error) = StateMachine::validate_source(&input.source, &input.intent) {
            if input.source != WorkflowInputSource::Human {
                return Ok(RoutingOutcome::Rejected {
                    reason: error.to_string(),
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
            if context.processing_id.is_some() || context.accepted_patch.is_some() {
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
            if context.processing_id.is_none() || confidence.is_none() {
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
        match &input.intent {
            WorkflowIntent::StartNewTask { goal } => {
                if let Err(error) = StateMachine::validate_new_task(source) {
                    return self.reject(command, source, context, &error.to_string());
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
                    let ProcessingResult::ControllerInput { message_id, .. } = result else {
                        return Err(WorkflowEngineError::InvalidInputContext(
                            "unexpected controller processing result",
                        ));
                    };
                    let state = self
                        .store
                        .load_workflow(dialog_id)?
                        .current_task
                        .ok_or(StoreError::WorkflowConflict(dialog_id))?;
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
                let preview = if let Some(patch) = context.accepted_patch {
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
                    task.preview_patch(patch, patch_context)?
                } else {
                    task.clone()
                };
                let authorization = match &input.intent {
                    WorkflowIntent::ProposeTransition { event, evidence } => {
                        StateMachine::authorize(&preview, *event, evidence)
                            .map(StageChangeAuthorization::Transition)
                    }
                    WorkflowIntent::ReplanCurrent { change_request } => {
                        StateMachine::authorize_replan(
                            &preview,
                            &input.source,
                            change_request.clone(),
                        )
                        .map(StageChangeAuthorization::Replan)
                    }
                    _ => unreachable!(),
                };
                let authorization = match authorization {
                    Ok(authorization) => authorization,
                    Err(error) => return self.reject(command, source, context, &error.to_string()),
                };
                let messages = self
                    .store
                    .load_stage_messages(task.current_stage_run_id)?
                    .into_iter()
                    .map(|row| row.message)
                    .collect::<Vec<_>>();
                let handoff = match self
                    .handoff_builder
                    .build(&authorization, &preview, &messages, &input)
                    .await
                {
                    Ok(handoff) => handoff,
                    Err(_) => {
                        return self.reject(command, source, context, "workflow handoff failed");
                    }
                };
                let saved = self.store.commit_stage_change(TransitionCommit {
                    dialog_id,
                    source_task: task,
                    authorization: &authorization,
                    triggering_input: &input,
                    protocol_text,
                    confidence,
                    accepted_patch: context.accepted_patch,
                    handoff: &handoff.payload,
                    processing_id: context.processing_id,
                })?;
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
        reason: &str,
    ) -> Result<RoutingOutcome, WorkflowEngineError> {
        if command.input.source == WorkflowInputSource::Human {
            self.store.append_input(
                command,
                AcceptedInputEffect::Reject {
                    reason: reason.to_owned(),
                },
            )?;
        } else if let (Some(processing), Some(task)) = (context.processing_id, source) {
            self.store
                .fail_processing(processing, task.version, reason)?;
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
    pub last_usage: &'a mut Option<TokenUsage>,
}

pub struct WorkflowEngine<'a> {
    client: &'a DeepSeekClient,
    context_config: &'a ContextConfig,
    interpreter: HumanInputInterpreter,
    handoff_builder: HandoffBuilder,
    session: WorkflowSession<'a>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowTurnEvent {
    ResponseStarted {
        autonomous_turn: u32,
        phase: TaskPhase,
    },
    InputRejected {
        reason: String,
    },
}

#[derive(Debug)]
pub struct WorkflowTurnResult {
    pub routing: RoutingOutcome,
    pub answer: Option<String>,
    pub persisted_answer: Option<PersistedAnswer>,
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
            session,
        }
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
        WorkflowIntent::human_continue(prompt)?;
        let routing = if let Some(dialog_id) = *self.session.dialog_id {
            let snapshot = self.session.store.load_workflow(dialog_id)?;
            let (intent, confidence) = if let Some(task) = snapshot.current_task.as_ref() {
                match self.interpreter.interpret(prompt, Some(task)).await? {
                    HumanInterpretation::Managed {
                        intent, confidence, ..
                    } => (intent, Some(confidence)),
                    HumanInterpretation::Unmanaged { .. } => {
                        (WorkflowIntent::human_continue(prompt)?, None)
                    }
                }
            } else {
                (
                    WorkflowIntent::StartNewTask {
                        goal: prompt.trim().to_owned(),
                    },
                    None,
                )
            };
            WorkflowInputHandler {
                store: self.session.store,
                handoff_builder: &self.handoff_builder,
            }
            .handle(
                dialog_id,
                snapshot,
                WorkflowInput {
                    source: WorkflowInputSource::Human,
                    intent,
                },
                prompt,
                confidence,
                InputHandlingContext::default(),
            )
            .await?
        } else {
            let started = self.session.store.start_dialog_with_workflow_task(
                self.session.scope,
                self.session.history.system_prompt(),
                prompt,
            )?;
            *self.session.dialog_id = Some(started.dialog_id);
            *self.session.scope = self.session.scope.with_dialog_id(Some(started.dialog_id));
            RoutingOutcome::Managed {
                input_message_id: started.message_id,
                state: started.task,
            }
        };
        self.session.history.push(Role::User, prompt.to_owned());
        match &routing {
            RoutingOutcome::Rejected { reason, .. } => {
                emit(
                    &mut on_event,
                    AgentEvent::Workflow(WorkflowTurnEvent::InputRejected {
                        reason: reason.clone(),
                    }),
                )?;
                Ok(WorkflowTurnResult {
                    routing,
                    answer: None,
                    persisted_answer: None,
                })
            }
            RoutingOutcome::Managed { state, .. } if state.phase == TaskPhase::Done => {
                Ok(WorkflowTurnResult {
                    routing,
                    answer: None,
                    persisted_answer: None,
                })
            }
            _ => {
                self.run_one_ordinary_turn(routing, prompt, &mut on_event)
                    .await
            }
        }
    }

    async fn run_one_ordinary_turn<F>(
        &mut self,
        routing: RoutingOutcome,
        prompt: &str,
        on_event: &mut F,
    ) -> Result<WorkflowTurnResult, WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        let dialog_id = self
            .session
            .dialog_id
            .expect("persisted input has a dialog");
        let inherited = self.inherited_blocks()?;
        let selected = self.session.store.load_workflow(dialog_id)?.current_task;
        let prepared = match &routing {
            RoutingOutcome::Managed { state, .. } => {
                let messages = self
                    .session
                    .store
                    .load_stage_messages(state.current_stage_run_id)?;
                let mut reductions = self
                    .session
                    .store
                    .load_stage_reductions(state.current_stage_run_id)?;
                self.refresh_stage_facts(state, &messages, &mut reductions, on_event)
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
                        autonomous_turn: 0,
                        phase: state.phase,
                    }),
                )?;
                stage.prepared
            }
            RoutingOutcome::Unmanaged { .. } => prepare_request(
                &ChatHistory::new(self.session.history.system_prompt().to_owned()),
                &ContextState::default(),
                &ContextConfig::full_history(),
                prompt,
                &inherited,
            ),
            RoutingOutcome::Rejected { .. } => unreachable!("rejected input has no ordinary turn"),
        };
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
        let answer = result?;
        if answer.trim().is_empty() {
            return Err(ClientError::EmptyAnswer.into());
        }
        let persisted_answer = match &routing {
            RoutingOutcome::Managed { state, .. } => Some(
                self.session
                    .store
                    .append_answer_for_processing(AnswerCommit {
                        dialog_id,
                        task_id: state.id,
                        stage_run_id: state.current_stage_run_id,
                        expected_version: state.version,
                        content: &answer,
                        usage,
                    })?,
            ),
            RoutingOutcome::Unmanaged { input_message_id } => {
                self.session
                    .store
                    .append_unmanaged_answer(UnmanagedAnswerCommit {
                        dialog_id,
                        input_message_id: *input_message_id,
                        expected_current_task: expected_task(selected.as_ref()),
                        content: &answer,
                        usage,
                    })?;
                None
            }
            RoutingOutcome::Rejected { .. } => unreachable!(),
        };
        self.session.history.push_answer(answer.clone(), usage);
        if let RoutingOutcome::Managed { state, .. } = &routing
            && let Err(error) = self.compact_stage(state, usage, &inherited, on_event).await
        {
            // The answer and its processing job are already durable. Compaction is advisory.
            let _ = emit(
                on_event,
                AgentEvent::CompactionFailed {
                    error: error.to_string(),
                },
            );
        }
        Ok(WorkflowTurnResult {
            routing,
            answer: Some(answer),
            persisted_answer,
        })
    }

    async fn refresh_stage_facts<F>(
        &mut self,
        task: &WorkflowTaskState,
        messages: &[StageProtocolMessage],
        reductions: &mut StageReductionState,
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
        let updated = async {
            let result = self
                .client
                .update_facts(
                    plan.request_messages(),
                    self.context_config.facts_max_tokens(),
                )
                .await?;
            let facts = parse_facts_json(result.answer())?;
            let state = self.session.store.replace_stage_facts(
                task.current_stage_run_id,
                task.version,
                messages.len(),
                facts,
                result.usage(),
            )?;
            Ok::<_, WorkflowEngineError>((state, result.usage()))
        }
        .await;
        match updated {
            Ok((facts, usage)) => {
                reductions.facts = facts;
                emit(
                    on_event,
                    AgentEvent::FactsUpdateCompleted {
                        covered_message_count: reductions.facts.covered_message_count(),
                        usage,
                    },
                )?;
                Ok(())
            }
            Err(error) => {
                emit(
                    on_event,
                    AgentEvent::FactsUpdateFailed {
                        error: error.to_string(),
                    },
                )?;
                Err(error)
            }
        }
    }

    async fn compact_stage<F>(
        &mut self,
        task: &WorkflowTaskState,
        usage: Option<TokenUsage>,
        inherited: &[SystemBlock],
        on_event: &mut F,
    ) -> Result<(), WorkflowEngineError>
    where
        F: FnMut(AgentEvent<'_>) -> io::Result<()>,
    {
        if self.context_config.strategy() != ContextStrategy::Summary
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
        let result = self
            .client
            .summarize(
                plan.request_messages(),
                self.context_config.summary_max_tokens(),
            )
            .await?;
        self.session.store.replace_stage_context(
            task.current_stage_run_id,
            task.version,
            messages.len(),
            ContextSummary::new(result.answer(), plan.covered_message_count()),
            result.usage(),
        )?;
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

fn emit<F>(on_event: &mut F, event: AgentEvent<'_>) -> Result<(), WorkflowEngineError>
where
    F: FnMut(AgentEvent<'_>) -> io::Result<()>,
{
    on_event(event).map_err(ClientError::Output)?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum WorkflowEngineError {
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
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Facts(#[from] FactsError),
}
