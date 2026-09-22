//! Request assembly and reduction inputs scoped to one workflow stage.

use crate::chat::{ChatHistory, Message};
use crate::config::{ContextConfig, ContextStrategy};
use crate::context::{
    CompactionPlan, ContextState, PreparedContext, plan_compaction, prepare_request_with_pending,
};
use crate::facts::FactsState;
use crate::system_context::{CompactionPolicy, ContextScope, SystemBlock};
use crate::workflow::{WorkflowTaskState, render_task_state};
use crate::workflow_store::{ProtocolSource, StageProtocolMessage};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StageReductionState {
    pub context: ContextState,
    pub facts: FactsState,
}

pub struct WorkflowRequestInput<'a> {
    pub base_prompt: &'a str,
    pub inherited_blocks: Vec<SystemBlock>,
    pub task: &'a WorkflowTaskState,
    /// Ordered rows loaded for `task.current_stage_run_id` by the repository.
    pub stage_messages: &'a [StageProtocolMessage],
    /// Supply only when the input has not already been persisted in the stage.
    pub pending_input: Option<&'a str>,
    pub context_config: &'a ContextConfig,
    pub context_state: &'a ContextState,
    pub facts_state: &'a FactsState,
}

pub struct WorkflowStageContext {
    pub prepared: PreparedContext,
    pub context_state: ContextState,
    pub facts_state: FactsState,
}

pub fn workflow_task_block(task: &WorkflowTaskState) -> SystemBlock {
    SystemBlock::new(
        "workflow_task",
        render_task_state(task),
        ContextScope::Task,
        CompactionPolicy::Exclude,
    )
}

pub fn prepare_workflow_request(input: WorkflowRequestInput<'_>) -> WorkflowStageContext {
    let history = ChatHistory::from_messages(
        input.base_prompt.to_owned(),
        input
            .stage_messages
            .iter()
            .map(|row| row.message.clone())
            .collect(),
    );
    let mut blocks = input.inherited_blocks;
    blocks.push(workflow_task_block(input.task));
    if input.context_config.strategy() == ContextStrategy::StickyFacts
        && let Some(facts) = input.facts_state.system_block()
    {
        blocks.push(facts);
    }
    WorkflowStageContext {
        prepared: prepare_request_with_pending(
            &history,
            input.context_state,
            input.context_config,
            input.pending_input,
            &blocks,
        ),
        context_state: input.context_state.clone(),
        facts_state: input.facts_state.clone(),
    }
}

pub fn stage_history(messages: &[StageProtocolMessage]) -> ChatHistory {
    ChatHistory::from_messages(
        String::new(),
        messages.iter().map(|row| row.message.clone()).collect(),
    )
}

pub fn plan_stage_compaction(
    messages: &[StageProtocolMessage],
    state: &ContextState,
    keep_last_messages: usize,
    additional_system_blocks: &[SystemBlock],
) -> Option<CompactionPlan> {
    plan_compaction(
        &stage_history(messages),
        state,
        keep_last_messages,
        additional_system_blocks,
    )
}

/// Fact boundaries use this filtered sequence, independently of raw protocol counts.
pub fn facts_candidates(messages: &[StageProtocolMessage]) -> Vec<Message> {
    messages
        .iter()
        .filter(|row| row.source != ProtocolSource::Controller)
        .map(|row| row.message.clone())
        .collect()
}
