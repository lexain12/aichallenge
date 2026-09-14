use deepseek_cli::chat::{ChatHistory, Role};
use deepseek_cli::client::TokenUsage;
use deepseek_cli::context::{
    ContextState, ContextSummary, UsageTotals, build_request_messages, plan_compaction,
};

fn history() -> ChatHistory {
    let mut history = ChatHistory::new("Original system".into());
    history.commit_turn("u1".into(), "a1".into());
    history.commit_turn("u2".into(), "a2".into());
    history.commit_turn("u3".into(), "a3".into());
    history
}

#[test]
fn ordinary_request_replaces_covered_prefix_with_summary() {
    let state = ContextState::with_summary(ContextSummary::new("old facts", 2));
    let request = build_request_messages(&history(), &state, true, 2, "next");
    let actual: Vec<_> = request
        .iter()
        .map(|message| (message.role(), message.content()))
        .collect();

    assert_eq!(
        actual,
        vec![
            (Role::System, "Original system"),
            (Role::System, "Summary of earlier conversation:\nold facts"),
            (Role::User, "u2"),
            (Role::Assistant, "a2"),
            (Role::User, "u3"),
            (Role::Assistant, "a3"),
            (Role::User, "next"),
        ]
    );
}

#[test]
fn disabled_or_incompatible_summary_sends_full_history() {
    let state = ContextState::with_summary(ContextSummary::new("old facts", 5));
    for (enabled, keep) in [(false, 1), (true, 4)] {
        let request = build_request_messages(&history(), &state, enabled, keep, "next");
        assert_eq!(request.len(), 8);
        assert_eq!(request[1].content(), "u1");
        assert!(
            !request
                .iter()
                .any(|message| message.content().contains("old facts"))
        );
    }
}

#[test]
fn later_compaction_uses_previous_summary_and_only_newly_eligible_messages() {
    let state = ContextState::with_summary(ContextSummary::new("old facts", 2));
    let plan = plan_compaction(&history(), &state, 2).unwrap();

    assert_eq!(plan.covered_message_count(), 4);
    assert_eq!(plan.new_message_count(), 2);
    assert!(plan.request_messages()[1].content().contains("old facts"));
    assert!(plan.request_messages()[1].content().contains("user: u2"));
    assert!(
        plan.request_messages()[1]
            .content()
            .contains("assistant: a2")
    );
    assert!(!plan.request_messages()[1].content().contains("u1"));
    assert!(!plan.request_messages()[1].content().contains("u3"));
}

#[test]
fn compaction_requires_messages_beyond_the_raw_tail_and_summary_boundary() {
    let short = ChatHistory::new("System".into());
    assert!(plan_compaction(&short, &ContextState::default(), 2).is_none());

    let state = ContextState::with_summary(ContextSummary::new("all old", 4));
    assert!(plan_compaction(&history(), &state, 2).is_none());
}

#[test]
fn usage_totals_keep_known_values_and_count_unknown_calls() {
    let mut totals = UsageTotals::default();
    totals.record(Some(TokenUsage {
        prompt_tokens: 10,
        completion_tokens: 3,
        total_tokens: 13,
        completion_tokens_details: None,
    }));
    totals.record(None);

    assert_eq!(totals.call_count(), 2);
    assert_eq!(totals.prompt_tokens(), 10);
    assert_eq!(totals.completion_tokens(), 3);
    assert_eq!(totals.total_tokens(), 13);
    assert_eq!(totals.missing_usage_count(), 1);
}
