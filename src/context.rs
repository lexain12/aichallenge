use std::fmt::Write as _;

use crate::chat::{ChatHistory, Message, Role};
use crate::client::TokenUsage;

const SUMMARY_CONTEXT_PREFIX: &str = "Summary of earlier conversation:\n";
const SUMMARY_SYSTEM_PROMPT: &str = "Create a faithful cumulative summary of the conversation context. Preserve facts, names, decisions, constraints, user preferences, unresolved questions, and exact technical identifiers. Distinguish user statements from assistant suggestions. Do not invent missing information. Return only the updated summary.";

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContextStats {
    pub full_message_count: usize,
    pub covered_message_count: usize,
    pub raw_message_count: usize,
    pub ordinary_usage: UsageTotals,
    pub compaction_usage: UsageTotals,
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

    let mut input = String::new();
    if let Some(summary) = previous {
        writeln!(input, "Previous summary:\n{}\n", summary.content()).ok()?;
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
    })
}

pub fn stats(
    history: &ChatHistory,
    state: &ContextState,
    enabled: bool,
    keep_last_messages: usize,
) -> ContextStats {
    let mut ordinary_usage = UsageTotals::default();
    for message in history
        .messages()
        .iter()
        .filter(|message| message.role() == Role::Assistant)
    {
        ordinary_usage.record(message.usage());
    }
    let covered_message_count = compatible_summary(history, state, enabled, keep_last_messages)
        .map_or(0, ContextSummary::covered_message_count);
    ContextStats {
        full_message_count: history.messages().len(),
        covered_message_count,
        raw_message_count: history.messages().len() - covered_message_count,
        ordinary_usage,
        compaction_usage: state.compaction_usage(),
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
