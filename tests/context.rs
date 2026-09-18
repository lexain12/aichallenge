use deepseek_cli::chat::{ChatHistory, Role};
use deepseek_cli::client::TokenUsage;
use deepseek_cli::config::{Config, ContextConfig};
use deepseek_cli::context::{
    ContextState, ContextSummary, HistorySelection, UsageTotals, assemble_request,
    build_request_messages, plan_compaction, prepare_request,
};
use deepseek_cli::system_context::{CompactionPolicy, ContextScope, SystemBlock, SystemContext};

fn history() -> ChatHistory {
    let mut history = ChatHistory::new("Original system".into());
    history.commit_turn("u1".into(), "a1".into());
    history.commit_turn("u2".into(), "a2".into());
    history.commit_turn("u3".into(), "a3".into());
    history
}

fn context_config(strategy: &str, keep: usize) -> ContextConfig {
    let source = format!(
        "api_key = \"key\"\n[context]\nstrategy = \"{strategy}\"\nkeep_last_messages = {keep}"
    );
    Config::from_toml(&source, None).unwrap().context().clone()
}

#[test]
fn system_blocks_are_ordered_before_windowed_history() {
    let mut system = SystemContext::default();
    system.push(SystemBlock::new(
        "base",
        "Base rules",
        ContextScope::Application,
        CompactionPolicy::Exclude,
    ));
    system.push(SystemBlock::new(
        "profile",
        "Future profile",
        ContextScope::User,
        CompactionPolicy::Exclude,
    ));
    let ordinary = vec![
        deepseek_cli::chat::Message::for_request(Role::User, "u1"),
        deepseek_cli::chat::Message::for_request(Role::Assistant, "a1"),
        deepseek_cli::chat::Message::for_request(Role::User, "u2"),
    ];

    let request = assemble_request(&system, &ordinary, HistorySelection::Last(2));

    assert_eq!(
        request
            .iter()
            .map(deepseek_cli::chat::Message::content)
            .collect::<Vec<_>>(),
        ["Base rules", "Future profile", "a1", "u2"]
    );
    assert_eq!(request[0].role(), Role::System);
    assert_eq!(request[1].role(), Role::System);
}

#[test]
fn blocks_are_scope_ordered_and_compaction_is_policy_filtered() {
    let mut system = SystemContext::default();
    system.push(SystemBlock::new(
        "facts",
        "dialog facts",
        ContextScope::Conversation,
        CompactionPolicy::Exclude,
    ));
    system.push(SystemBlock::new(
        "task_memory",
        "task facts",
        ContextScope::Task,
        CompactionPolicy::Exclude,
    ));
    system.push(SystemBlock::new(
        "summary",
        "old summary",
        ContextScope::Conversation,
        CompactionPolicy::Include,
    ));
    system.push(SystemBlock::new(
        "user_memory",
        "user facts",
        ContextScope::User,
        CompactionPolicy::Exclude,
    ));

    assert_eq!(
        system
            .prompt_blocks()
            .into_iter()
            .map(SystemBlock::name)
            .collect::<Vec<_>>(),
        ["user_memory", "task_memory", "facts", "summary"]
    );
    assert_eq!(
        system
            .compaction_blocks()
            .into_iter()
            .map(SystemBlock::name)
            .collect::<Vec<_>>(),
        ["summary"]
    );
}

#[test]
fn pending_user_message_counts_toward_sliding_window() {
    let config = context_config("sliding_window", 2);
    let request = prepare_request(&history(), &ContextState::default(), &config, "next", &[]);

    assert_eq!(
        request
            .messages()
            .iter()
            .map(deepseek_cli::chat::Message::content)
            .collect::<Vec<_>>(),
        ["Original system", "a3", "next"]
    );
    assert_eq!(request.selected_message_count(), 2);
}

#[test]
fn branching_selects_complete_history_and_additional_system_blocks_stay_protected() {
    let config = context_config("branching", 1);
    let extra = [SystemBlock::new(
        "runtime",
        "Runtime rules",
        ContextScope::Task,
        CompactionPolicy::Exclude,
    )];
    let request = prepare_request(
        &history(),
        &ContextState::with_summary(ContextSummary::new("ignored", 2)),
        &config,
        "next",
        &extra,
    );

    assert_eq!(request.selected_message_count(), 7);
    assert_eq!(request.summary_boundary(), 0);
    assert_eq!(request.system_block_names(), ["base", "runtime"]);
    assert_eq!(request.messages()[0].content(), "Original system");
    assert_eq!(request.messages()[1].content(), "Runtime rules");
    assert_eq!(request.messages()[2].content(), "u1");
    assert_eq!(request.messages().last().unwrap().content(), "next");
    assert!(
        !request
            .messages()
            .iter()
            .any(|message| message.content() == "ignored")
    );
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
    let config = context_config("summary", 2);
    let prepared = prepare_request(&history(), &state, &config, "next", &[]);
    let plan = plan_compaction(&history(), &state, 2, prepared.system_context()).unwrap();

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
fn compaction_input_never_contains_excluded_blocks() {
    let state = ContextState::with_summary(ContextSummary::new("old facts", 2));
    let config = context_config("summary", 2);
    let extra = [
        SystemBlock::new(
            "user_memory",
            "private durable user fact",
            ContextScope::User,
            CompactionPolicy::Exclude,
        ),
        SystemBlock::new(
            "task_memory",
            "durable task decision",
            ContextScope::Task,
            CompactionPolicy::Exclude,
        ),
    ];
    let prepared = prepare_request(&history(), &state, &config, "next", &extra);
    let plan = plan_compaction(&history(), &state, 2, prepared.system_context()).unwrap();
    let text = plan.request_messages()[1].content();

    assert!(text.contains("old facts"));
    assert!(!text.contains("private durable user fact"));
    assert!(!text.contains("durable task decision"));
}

#[test]
fn compaction_requires_messages_beyond_the_raw_tail_and_summary_boundary() {
    let short = ChatHistory::new("System".into());
    let short_system = SystemContext::default();
    assert!(plan_compaction(&short, &ContextState::default(), 2, &short_system).is_none());

    let state = ContextState::with_summary(ContextSummary::new("all old", 4));
    let config = context_config("summary", 2);
    let prepared = prepare_request(&history(), &state, &config, "next", &[]);
    assert!(plan_compaction(&history(), &state, 2, prepared.system_context()).is_none());
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
