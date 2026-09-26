//! Bounded transient conversation state for a provider tool loop.
use crate::provider::{
    AssistantTurn, CompletionTokenDetails, ModelToolCall, ModelToolDefinition, ProviderMessage,
    TokenUsage,
};
use serde_json::Value;
use std::collections::HashSet;
use thiserror::Error;

pub const DEFAULT_MAX_TOOL_ROUNDS: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolResultMessage {
    pub call_id: String,
    pub content: String,
    pub is_error: bool,
}

impl ToolResultMessage {
    pub fn success(call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            call_id: call_id.into(),
            content: content.into(),
            is_error: false,
        }
    }

    pub fn error(call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            call_id: call_id.into(),
            content: content.into(),
            is_error: true,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ConversationStep {
    Complete {
        content: String,
        usage: TokenUsage,
    },
    Execute {
        assistant_message: ProviderMessage,
        calls: Vec<ModelToolCall>,
        usage: TokenUsage,
    },
}

impl ConversationStep {
    pub fn assistant_message(&self) -> ProviderMessage {
        match self {
            Self::Execute {
                assistant_message, ..
            } => assistant_message.clone(),
            Self::Complete { content, .. } => ProviderMessage::assistant(content.clone()),
        }
    }
}

#[derive(Clone, Error, PartialEq, Eq)]
pub enum ToolLoopError {
    #[error("tool_already_complete")]
    AlreadyComplete,
    #[error("tool_incomplete_turn")]
    IncompleteTurn,
    #[error("tool_pending_tool_results")]
    PendingToolResults,
    #[error("tool_unexpected_tool_results")]
    UnexpectedToolResults,
    #[error("tool_round_limit_exceeded")]
    RoundLimitExceeded,
    #[error("tool_empty_tool_calls")]
    EmptyToolCalls,
    #[error("tool_empty_call_id")]
    EmptyCallId,
    #[error("tool_duplicate_call_id")]
    DuplicateCallId,
    #[error("tool_unknown")]
    UnknownTool,
    #[error("tool_invalid_arguments")]
    InvalidArguments,
    #[error("tool_assistant_message_mismatch")]
    AssistantMessageMismatch,
    #[error("tool_result_count_mismatch")]
    ResultCountMismatch,
    #[error("tool_result_call_id_mismatch")]
    ResultCallIdMismatch,
}

impl std::fmt::Debug for ToolLoopError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

struct PendingRound {
    assistant_message: ProviderMessage,
    call_ids: Vec<String>,
}

/// One user request's tool limits and the current ordinary turn's transcript.
pub struct ToolConversation {
    messages: Vec<ProviderMessage>,
    definitions: Vec<ModelToolDefinition>,
    max_rounds: usize,
    rounds: usize,
    seen_call_ids: HashSet<String>,
    usage: TokenUsage,
    pending: Option<PendingRound>,
    complete: bool,
}

impl ToolConversation {
    pub fn new(
        messages: Vec<ProviderMessage>,
        definitions: Vec<ModelToolDefinition>,
        max_rounds: usize,
    ) -> Self {
        Self {
            messages,
            definitions,
            max_rounds,
            rounds: 0,
            seen_call_ids: HashSet::new(),
            usage: TokenUsage::default(),
            pending: None,
            complete: false,
        }
    }

    pub fn with_default_limit(
        messages: Vec<ProviderMessage>,
        definitions: Vec<ModelToolDefinition>,
    ) -> Self {
        Self::new(messages, definitions, DEFAULT_MAX_TOOL_ROUNDS)
    }

    pub fn messages(&self) -> &[ProviderMessage] {
        &self.messages
    }

    pub fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    pub fn rounds(&self) -> usize {
        self.rounds
    }

    pub fn usage(&self) -> TokenUsage {
        self.usage
    }

    /// Start another ordinary answer within the same user request. Keep the
    /// consumed round allowance and call IDs while replacing transient context.
    pub fn begin_next_turn(&mut self, messages: Vec<ProviderMessage>) -> Result<(), ToolLoopError> {
        if !self.complete {
            return Err(ToolLoopError::IncompleteTurn);
        }
        self.messages = messages;
        self.usage = TokenUsage::default();
        self.complete = false;
        Ok(())
    }

    pub fn accept_assistant_turn(
        &mut self,
        turn: AssistantTurn,
    ) -> Result<ConversationStep, ToolLoopError> {
        if self.complete {
            return Err(ToolLoopError::AlreadyComplete);
        }
        if self.pending.is_some() {
            return Err(ToolLoopError::PendingToolResults);
        }

        match turn {
            AssistantTurn::FinalText { content, usage } => {
                self.record_usage(usage);
                self.messages
                    .push(ProviderMessage::assistant(content.clone()));
                self.complete = true;
                Ok(ConversationStep::Complete {
                    content,
                    usage: self.usage,
                })
            }
            AssistantTurn::ToolCalls {
                content,
                calls,
                usage,
            } => {
                if self.rounds >= self.max_rounds {
                    return Err(ToolLoopError::RoundLimitExceeded);
                }
                if calls.is_empty() {
                    return Err(ToolLoopError::EmptyToolCalls);
                }

                let mut next_ids = HashSet::new();
                for call in &calls {
                    if call.id.is_empty() {
                        return Err(ToolLoopError::EmptyCallId);
                    }
                    if self.seen_call_ids.contains(&call.id) || !next_ids.insert(call.id.clone()) {
                        return Err(ToolLoopError::DuplicateCallId);
                    }
                    if !self
                        .definitions
                        .iter()
                        .any(|definition| definition.name == call.name)
                    {
                        return Err(ToolLoopError::UnknownTool);
                    }
                    if !matches!(
                        serde_json::from_str::<Value>(&call.arguments),
                        Ok(Value::Object(_))
                    ) {
                        return Err(ToolLoopError::InvalidArguments);
                    }
                }

                let assistant_message = ProviderMessage::assistant_tool_calls(content, &calls);
                self.record_usage(usage);
                self.rounds += 1;
                self.seen_call_ids.extend(next_ids);
                self.messages.push(assistant_message.clone());
                self.pending = Some(PendingRound {
                    assistant_message: assistant_message.clone(),
                    call_ids: calls.iter().map(|call| call.id.clone()).collect(),
                });
                Ok(ConversationStep::Execute {
                    assistant_message,
                    calls,
                    usage: self.usage,
                })
            }
        }
    }

    pub fn accept_tool_results(
        &mut self,
        assistant_message: ProviderMessage,
        results: Vec<ToolResultMessage>,
    ) -> Result<(), ToolLoopError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or(ToolLoopError::UnexpectedToolResults)?;
        if serde_json::to_value(&assistant_message).ok()
            != serde_json::to_value(&pending.assistant_message).ok()
        {
            return Err(ToolLoopError::AssistantMessageMismatch);
        }
        if results.len() != pending.call_ids.len() {
            return Err(ToolLoopError::ResultCountMismatch);
        }
        if results
            .iter()
            .zip(&pending.call_ids)
            .any(|(result, expected_id)| result.call_id != *expected_id)
        {
            return Err(ToolLoopError::ResultCallIdMismatch);
        }

        self.messages.extend(
            results
                .into_iter()
                .map(|result| ProviderMessage::tool_result(result.call_id, result.content)),
        );
        self.pending = None;
        Ok(())
    }

    fn record_usage(&mut self, incoming: Option<TokenUsage>) {
        if let Some(incoming) = incoming {
            self.usage.prompt_tokens = self
                .usage
                .prompt_tokens
                .saturating_add(incoming.prompt_tokens);
            self.usage.completion_tokens = self
                .usage
                .completion_tokens
                .saturating_add(incoming.completion_tokens);
            self.usage.total_tokens = self
                .usage
                .total_tokens
                .saturating_add(incoming.total_tokens);
            let prior_reasoning = self
                .usage
                .completion_tokens_details
                .and_then(|details| details.reasoning_tokens);
            let incoming_reasoning = incoming
                .completion_tokens_details
                .and_then(|details| details.reasoning_tokens);
            if prior_reasoning.is_some() || incoming_reasoning.is_some() {
                self.usage.completion_tokens_details = Some(CompletionTokenDetails {
                    reasoning_tokens: Some(
                        prior_reasoning
                            .unwrap_or_default()
                            .saturating_add(incoming_reasoning.unwrap_or_default()),
                    ),
                });
            }
        }
    }
}
