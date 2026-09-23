use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

use crate::chat::{ChatHistory, Message, Role};
use crate::client::TokenUsage;
use crate::config::{ContextConfig, ContextStrategy};
use crate::facts::FactsState;
use crate::system_context::{
    CompactionPolicy, ContextScope, SystemBlock, SystemBlockMetadata, SystemContext,
};

const SUMMARY_CONTEXT_PREFIX: &str = "Summary of earlier conversation:\n";
const SUMMARY_SYSTEM_PROMPT: &str = "Create a faithful cumulative summary of the conversation context. Preserve facts, names, decisions, constraints, user preferences, unresolved questions, and exact technical identifiers. Distinguish user statements from assistant suggestions. Do not invent missing information. Return only the updated summary.";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContextSummary {
    content: String,
    covered_message_count: usize,
}

impl ContextSummary {
    pub fn new(content: impl Into<String>, covered_message_count: usize) -> Self {
        Self {
            content: content.into(),
            covered_message_count,
        }
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn covered_message_count(&self) -> usize {
        self.covered_message_count
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UsageTotals {
    call_count: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    missing_usage_count: u64,
}

impl UsageTotals {
    pub fn record(&mut self, usage: Option<TokenUsage>) {
        self.call_count = self.call_count.saturating_add(1);
        match usage {
            Some(usage) => {
                self.prompt_tokens = self.prompt_tokens.saturating_add(usage.prompt_tokens);
                self.completion_tokens = self
                    .completion_tokens
                    .saturating_add(usage.completion_tokens);
                self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
            }
            None => {
                self.missing_usage_count = self.missing_usage_count.saturating_add(1);
            }
        }
    }

    pub(crate) fn from_parts(
        call_count: u64,
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
        missing_usage_count: u64,
    ) -> Self {
        Self {
            call_count,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            missing_usage_count,
        }
    }

    pub fn call_count(&self) -> u64 {
        self.call_count
    }

    pub fn prompt_tokens(&self) -> u64 {
        self.prompt_tokens
    }

    pub fn completion_tokens(&self) -> u64 {
        self.completion_tokens
    }

    pub fn total_tokens(&self) -> u64 {
        self.total_tokens
    }

    pub fn missing_usage_count(&self) -> u64 {
        self.missing_usage_count
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContextState {
    summary: Option<ContextSummary>,
    compaction_usage: UsageTotals,
}

impl ContextState {
    pub fn with_summary(summary: ContextSummary) -> Self {
        Self {
            summary: Some(summary),
            compaction_usage: UsageTotals::default(),
        }
    }

    pub(crate) fn restored(summary: Option<ContextSummary>, compaction_usage: UsageTotals) -> Self {
        Self {
            summary,
            compaction_usage,
        }
    }

    pub(crate) fn replace_summary(&mut self, summary: ContextSummary, usage: Option<TokenUsage>) {
        self.summary = Some(summary);
        self.compaction_usage.record(usage);
    }

    pub fn summary(&self) -> Option<&ContextSummary> {
        self.summary.as_ref()
    }

    pub fn compaction_usage(&self) -> UsageTotals {
        self.compaction_usage
    }
}

pub struct CompactionPlan {
    request_messages: Vec<Message>,
    covered_message_count: usize,
    new_message_count: usize,
    system_block_metadata: Vec<SystemBlockMetadata>,
}

impl CompactionPlan {
    pub fn request_messages(&self) -> &[Message] {
        &self.request_messages
    }

    pub fn covered_message_count(&self) -> usize {
        self.covered_message_count
    }

    pub fn new_message_count(&self) -> usize {
        self.new_message_count
    }

    pub fn previous_boundary(&self) -> usize {
        self.covered_message_count - self.new_message_count
    }

    pub fn system_block_metadata(&self) -> &[SystemBlockMetadata] {
        &self.system_block_metadata
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ContextStats {
    pub strategy: ContextStrategy,
    pub full_message_count: usize,
    /// Present for a managed task; other context counts and usage are stage-local.
    pub stage_message_count: Option<usize>,
    pub covered_message_count: usize,
    pub raw_message_count: usize,
    pub selected_message_count: usize,
    pub facts_count: usize,
    pub facts_covered_message_count: usize,
    pub ordinary_usage: UsageTotals,
    pub compaction_usage: UsageTotals,
    pub facts_usage: UsageTotals,
    pub dialog_id: Option<i64>,
    pub branch_group_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistorySelection {
    Full,
    Last(usize),
    After(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedContext {
    messages: Vec<Message>,
    selected_message_count: usize,
    summary_boundary: usize,
    system_block_names: Vec<String>,
    system_context: SystemContext,
}

impl PreparedContext {
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    pub fn selected_message_count(&self) -> usize {
        self.selected_message_count
    }

    pub fn summary_boundary(&self) -> usize {
        self.summary_boundary
    }

    pub fn system_block_names(&self) -> &[String] {
        &self.system_block_names
    }

    pub fn system_context(&self) -> &SystemContext {
        &self.system_context
    }

    pub fn system_block_metadata(&self) -> Vec<SystemBlockMetadata> {
        self.system_context.metadata()
    }
}

pub fn assemble_request(
    system: &SystemContext,
    ordinary_messages: &[Message],
    selection: HistorySelection,
) -> Vec<Message> {
    let start = match selection {
        HistorySelection::Full => 0,
        HistorySelection::Last(count) => ordinary_messages.len().saturating_sub(count),
        HistorySelection::After(boundary) => boundary.min(ordinary_messages.len()),
    };
    let mut request = system.to_messages();
    request.extend(ordinary_messages[start..].iter().cloned());
    request
}

pub fn prepare_request(
    history: &ChatHistory,
    state: &ContextState,
    config: &ContextConfig,
    user_message: &str,
    additional_system_blocks: &[SystemBlock],
) -> PreparedContext {
    prepare_request_with_pending(
        history,
        state,
        config,
        Some(user_message),
        additional_system_blocks,
    )
}

pub(crate) fn prepare_request_with_pending(
    history: &ChatHistory,
    state: &ContextState,
    config: &ContextConfig,
    pending_input: Option<&str>,
    additional_system_blocks: &[SystemBlock],
) -> PreparedContext {
    let mut system = SystemContext::default();
    system.push(SystemBlock::new(
        "base",
        history.system_prompt(),
        ContextScope::Application,
        CompactionPolicy::Exclude,
    ));

    let (selection, summary_boundary) = match config.strategy() {
        ContextStrategy::Summary => {
            let summary = compatible_summary(history, state, true, config.keep_last_messages());
            if let Some(summary) = summary {
                system.push(summary_block(summary));
            }
            let boundary = summary.map_or(0, ContextSummary::covered_message_count);
            (HistorySelection::After(boundary), boundary)
        }
        ContextStrategy::SlidingWindow | ContextStrategy::StickyFacts => {
            (HistorySelection::Last(config.keep_last_messages()), 0)
        }
        ContextStrategy::Branching => (HistorySelection::Full, 0),
    };

    for block in additional_system_blocks {
        system.push(block.clone());
    }
    let system_block_names = system
        .metadata()
        .into_iter()
        .map(|block| block.name)
        .collect();
    let mut ordinary_messages = history.messages().to_vec();
    if let Some(input) = pending_input {
        ordinary_messages.push(Message::new(Role::User, input.to_owned()));
    }
    let selected_message_count = match selection {
        HistorySelection::Full => ordinary_messages.len(),
        HistorySelection::Last(count) => ordinary_messages.len().min(count),
        HistorySelection::After(boundary) => ordinary_messages.len().saturating_sub(boundary),
    };
    let messages = assemble_request(&system, &ordinary_messages, selection);

    PreparedContext {
        messages,
        selected_message_count,
        summary_boundary,
        system_block_names,
        system_context: system,
    }
}

pub fn build_request_messages(
    history: &ChatHistory,
    state: &ContextState,
    enabled: bool,
    keep_last_messages: usize,
    user_message: &str,
) -> Vec<Message> {
    let summary = compatible_summary(history, state, enabled, keep_last_messages);
    let boundary = summary.map_or(0, ContextSummary::covered_message_count);
    let mut request = Vec::with_capacity(history.messages().len() - boundary + 3);
    if !history.system_prompt().trim().is_empty() {
        request.push(Message::new(
            Role::System,
            history.system_prompt().to_owned(),
        ));
    }
    if let Some(summary) = summary {
        request.push(Message::new(
            Role::System,
            format!("{SUMMARY_CONTEXT_PREFIX}{}", summary.content()),
        ));
    }
    request.extend(history.messages()[boundary..].iter().cloned());
    request.push(Message::new(Role::User, user_message.to_owned()));
    request
}

pub fn plan_compaction(
    history: &ChatHistory,
    state: &ContextState,
    keep_last_messages: usize,
    additional_system_blocks: &[SystemBlock],
) -> Option<CompactionPlan> {
    let target = history.messages().len().checked_sub(keep_last_messages)?;
    if target == 0 {
        return None;
    }

    let previous = compatible_summary(history, state, true, keep_last_messages);
    let previous_boundary = previous.map_or(0, ContextSummary::covered_message_count);
    if target <= previous_boundary {
        return None;
    }

    // Select the summary block and boundary from the same post-turn snapshot.
    let mut system = SystemContext::default();
    if let Some(summary) = previous {
        system.push(summary_block(summary));
    }
    for block in additional_system_blocks {
        system.push(block.clone());
    }
    let admitted_blocks = system.compaction_blocks();
    let mut input = String::new();
    for block in &admitted_blocks {
        writeln!(
            input,
            "Context block {}:\n{}\n",
            block.name(),
            block.content()
        )
        .ok()?;
    }
    input.push_str("New messages:\n");
    for message in &history.messages()[previous_boundary..target] {
        let role = match message.role() {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
        };
        writeln!(input, "{role}: {}", message.content()).ok()?;
    }

    Some(CompactionPlan {
        request_messages: vec![
            Message::new(Role::System, SUMMARY_SYSTEM_PROMPT.to_owned()),
            Message::new(Role::User, input),
        ],
        covered_message_count: target,
        new_message_count: target - previous_boundary,
        system_block_metadata: admitted_blocks
            .iter()
            .map(|block| block.metadata())
            .collect(),
    })
}

fn summary_block(summary: &ContextSummary) -> SystemBlock {
    SystemBlock::new(
        "summary",
        format!("{SUMMARY_CONTEXT_PREFIX}{}", summary.content()),
        ContextScope::Conversation,
        CompactionPolicy::Include,
    )
}

pub fn stats(
    history: &ChatHistory,
    state: &ContextState,
    facts: &FactsState,
    config: &ContextConfig,
    dialog_id: Option<i64>,
    branch_group_id: Option<i64>,
) -> ContextStats {
    let mut ordinary_usage = UsageTotals::default();
    for message in history
        .messages()
        .iter()
        .filter(|message| message.role() == Role::Assistant)
    {
        ordinary_usage.record(message.usage());
    }
    let covered_message_count = if config.strategy() == ContextStrategy::Summary {
        compatible_summary(history, state, true, config.keep_last_messages())
            .map_or(0, ContextSummary::covered_message_count)
    } else {
        0
    };
    let raw_message_count = history.messages().len() - covered_message_count;
    let selected_message_count = match config.strategy() {
        ContextStrategy::Summary => raw_message_count,
        ContextStrategy::SlidingWindow | ContextStrategy::StickyFacts => {
            history.messages().len().min(config.keep_last_messages())
        }
        ContextStrategy::Branching => history.messages().len(),
    };
    let sticky = config.strategy() == ContextStrategy::StickyFacts;
    ContextStats {
        strategy: config.strategy(),
        full_message_count: history.messages().len(),
        stage_message_count: None,
        covered_message_count,
        raw_message_count,
        selected_message_count,
        facts_count: if sticky { facts.facts().len() } else { 0 },
        facts_covered_message_count: if sticky {
            facts.covered_message_count()
        } else {
            0
        },
        ordinary_usage,
        compaction_usage: state.compaction_usage(),
        facts_usage: facts.update_usage(),
        dialog_id,
        branch_group_id,
    }
}

fn compatible_summary<'a>(
    history: &ChatHistory,
    state: &'a ContextState,
    enabled: bool,
    keep_last_messages: usize,
) -> Option<&'a ContextSummary> {
    if !enabled {
        return None;
    }
    let max_covered = history.messages().len().saturating_sub(keep_last_messages);
    state
        .summary()
        .filter(|summary| summary.covered_message_count() <= max_covered)
}
