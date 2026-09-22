use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::chat::{Message, Role};
use crate::client::TokenUsage;
use crate::context::UsageTotals;
use crate::system_context::{CompactionPolicy, ContextScope, SystemBlock};

pub type Facts = BTreeMap<String, String>;

const FACTS_SYSTEM_PROMPT: &str = "Update the key-value memory using only facts grounded in the supplied user messages. Return only a bare JSON object with string keys and string values. Use stable descriptive keys. Replace obsolete values, remove facts explicitly revoked by the user, and never turn assistant suggestions into facts. Do not invent information.";
const FACTS_BLOCK_PREFIX: &str = "Facts (JSON key-value memory):\n";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FactsState {
    facts: Facts,
    covered_message_count: usize,
    update_usage: UsageTotals,
}

impl FactsState {
    pub fn restored(facts: Facts, covered_message_count: usize, update_usage: UsageTotals) -> Self {
        Self {
            facts,
            covered_message_count,
            update_usage,
        }
    }

    pub fn facts(&self) -> &Facts {
        &self.facts
    }

    pub fn covered_message_count(&self) -> usize {
        self.covered_message_count
    }

    pub fn update_usage(&self) -> UsageTotals {
        self.update_usage
    }

    pub fn system_block(&self) -> Option<SystemBlock> {
        if self.facts.is_empty() {
            return None;
        }
        let json = serde_json::to_string(&self.facts).ok()?;
        Some(SystemBlock::new(
            "facts",
            format!("{FACTS_BLOCK_PREFIX}{json}"),
            ContextScope::Conversation,
            CompactionPolicy::Exclude,
        ))
    }

    pub fn updated(
        mut self,
        facts: Facts,
        covered_message_count: usize,
        usage: Option<TokenUsage>,
    ) -> Self {
        self.facts = facts;
        self.covered_message_count = covered_message_count;
        self.update_usage.record(usage);
        self
    }
}

pub struct FactsUpdatePlan {
    request_messages: Vec<Message>,
    covered_message_count: usize,
}

impl FactsUpdatePlan {
    pub fn request_messages(&self) -> &[Message] {
        &self.request_messages
    }

    pub fn covered_message_count(&self) -> usize {
        self.covered_message_count
    }
}

pub fn plan_facts_update(messages: &[Message], state: &FactsState) -> Option<FactsUpdatePlan> {
    let boundary = state.covered_message_count();
    if boundary >= messages.len() {
        return None;
    }
    let uncovered_users: Vec<_> = messages[boundary..]
        .iter()
        .filter(|message| message.role() == Role::User)
        .collect();
    if uncovered_users.is_empty() {
        return None;
    }

    let previous = serde_json::to_string(state.facts()).ok()?;
    let mut input = format!("Previous facts:\n{previous}\n\nNew user messages:\n");
    for (index, message) in uncovered_users.into_iter().enumerate() {
        writeln!(input, "{}. {}", index + 1, message.content()).ok()?;
    }

    Some(FactsUpdatePlan {
        request_messages: vec![
            Message::new(Role::System, FACTS_SYSTEM_PROMPT.to_owned()),
            Message::new(Role::User, input),
        ],
        covered_message_count: messages.len(),
    })
}

pub fn parse_facts_json(text: &str) -> Result<Facts, FactsError> {
    Ok(serde_json::from_str(text.trim())?)
}

#[derive(Debug, Error)]
pub enum FactsError {
    #[error("facts updater returned invalid string-to-string JSON")]
    InvalidJson(#[from] serde_json::Error),
}
