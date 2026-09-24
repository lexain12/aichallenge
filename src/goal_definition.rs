//! Parse a goal proposal from the exact line shown to the human.

use crate::workflow::MAX_WORKFLOW_TEXT_CHARS;

pub const GOAL_PROPOSAL_PREFIX: &str = "Предлагаемая цель:";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoalProposalParseError {
    Blank,
    Duplicate,
    TooLong,
    ControlCharacter,
}

pub fn parse_goal_proposal(answer: &str) -> Result<Option<String>, GoalProposalParseError> {
    let mut in_fence = false;
    let mut proposal = None;
    for raw_line in answer.split('\n') {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        let Some(text) = line.strip_prefix(GOAL_PROPOSAL_PREFIX) else {
            continue;
        };
        if proposal.is_some() {
            return Err(GoalProposalParseError::Duplicate);
        }
        let text = text.trim();
        if text.is_empty() {
            return Err(GoalProposalParseError::Blank);
        }
        if text.chars().count() > MAX_WORKFLOW_TEXT_CHARS {
            return Err(GoalProposalParseError::TooLong);
        }
        if text.chars().any(char::is_control) {
            return Err(GoalProposalParseError::ControlCharacter);
        }
        proposal = Some(text.to_owned());
    }
    Ok(proposal)
}
