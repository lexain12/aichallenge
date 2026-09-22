use std::collections::BTreeMap;

use deepseek_cli::chat::{Message, Role};
use deepseek_cli::client::TokenUsage;
use deepseek_cli::context::UsageTotals;
use deepseek_cli::facts::{FactsState, parse_facts_json, plan_facts_update};

fn msg(role: Role, content: &str) -> Message {
    Message::for_request(role, content)
}

#[test]
fn persisted_stage_facts_restore_candidate_boundary_and_usage() {
    let state = FactsState::default().updated(
        BTreeMap::from([("deadline".into(), "Friday".into())]),
        2,
        None,
    );
    let encoded = serde_json::to_string(&state).unwrap();
    let restored: FactsState = serde_json::from_str(&encoded).unwrap();
    assert_eq!(restored.facts()["deadline"], "Friday");
    assert_eq!(restored.covered_message_count(), 2);
    assert_eq!(restored.update_usage().call_count(), 1);
    assert_eq!(restored.update_usage().missing_usage_count(), 1);
    let messages = [
        msg(Role::User, "old"),
        msg(Role::Assistant, "answer"),
        msg(Role::User, "new"),
    ];
    let plan = plan_facts_update(&messages, &restored).unwrap();
    assert!(plan.request_messages()[1].content().contains("1. new"));
    assert!(!plan.request_messages()[1].content().contains("1. old"));
}

#[test]
fn update_plan_contains_previous_map_and_only_uncovered_user_messages() {
    let state = FactsState::restored(
        BTreeMap::from([("goal".into(), "ship CLI".into())]),
        2,
        UsageTotals::default(),
    );
    let messages = vec![
        msg(Role::User, "old"),
        msg(Role::Assistant, "old answer"),
        msg(Role::User, "deadline Friday"),
        msg(Role::Assistant, "suggestion"),
        msg(Role::User, "cancel Friday; deadline Monday"),
    ];

    let plan = plan_facts_update(&messages, &state).unwrap();

    assert_eq!(plan.covered_message_count(), 5);
    assert!(plan.request_messages()[1].content().contains("ship CLI"));
    assert!(
        plan.request_messages()[1]
            .content()
            .contains("deadline Friday")
    );
    assert!(
        plan.request_messages()[1]
            .content()
            .contains("deadline Monday")
    );
    assert!(!plan.request_messages()[1].content().contains("suggestion"));
    assert!(!plan.request_messages()[1].content().contains("old answer"));
}

#[test]
fn parser_accepts_only_string_to_string_json_objects() {
    assert_eq!(
        parse_facts_json(r#"{"goal":"ship"}"#).unwrap()["goal"],
        "ship"
    );
    for invalid in ["", "```json\n{}\n```", "[]", r#"{"count":3}"#] {
        assert!(parse_facts_json(invalid).is_err(), "accepted {invalid:?}");
    }
}

#[test]
fn no_update_is_planned_without_uncovered_user_messages() {
    let messages = vec![msg(Role::User, "known"), msg(Role::Assistant, "answer")];
    let state = FactsState::restored(BTreeMap::new(), 1, UsageTotals::default());

    assert!(plan_facts_update(&messages, &state).is_none());
}

#[test]
fn successful_updates_replace_facts_and_accumulate_service_usage() {
    let state = FactsState::default()
        .updated(
            BTreeMap::from([("goal".into(), "ship".into())]),
            1,
            Some(TokenUsage {
                prompt_tokens: 4,
                completion_tokens: 2,
                total_tokens: 6,
                completion_tokens_details: None,
            }),
        )
        .updated(BTreeMap::new(), 3, None);

    assert!(state.facts().is_empty());
    assert_eq!(state.covered_message_count(), 3);
    assert_eq!(state.update_usage().call_count(), 2);
    assert_eq!(state.update_usage().total_tokens(), 6);
    assert_eq!(state.update_usage().missing_usage_count(), 1);
}
