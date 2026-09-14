use deepseek_cli::chat::{ChatHistory, InputAction, Role, parse_input};

#[test]
fn recognizes_commands_and_ignores_blank_input() {
    assert_eq!(parse_input("  "), InputAction::Ignore);
    assert_eq!(parse_input("/exit"), InputAction::Exit);
    assert_eq!(parse_input(" /quit "), InputAction::Exit);
    assert_eq!(parse_input("/clear"), InputAction::Clear);
    assert_eq!(parse_input(" /stats "), InputAction::Stats);
    assert_eq!(parse_input("/branch"), InputAction::Branch);
    assert_eq!(parse_input("/switch 42"), InputAction::Switch(42));
    assert_eq!(parse_input("/switch\t42"), InputAction::Switch(42));
    for input in [
        "/branch extra",
        "/switch",
        "/switch 0",
        "/switch nope",
        "/switch 1 extra",
    ] {
        assert!(matches!(parse_input(input), InputAction::InvalidCommand(_)));
    }
    assert_eq!(
        parse_input(" hello world "),
        InputAction::Send("hello world".into())
    );
    assert_eq!(
        parse_input("/unknown"),
        InputAction::Send("/unknown".into())
    );
}

#[test]
fn stages_a_user_message_without_mutating_committed_history() {
    let history = ChatHistory::new("Be concise".into());

    let request = history.request_messages("Hello");

    assert_eq!(request.len(), 2);
    assert_eq!(request[0].role(), Role::System);
    assert_eq!(request[0].content(), "Be concise");
    assert_eq!(request[1].role(), Role::User);
    assert_eq!(request[1].content(), "Hello");
    assert_eq!(history.turn_count(), 0);
}

#[test]
fn committed_turns_are_included_in_later_requests() {
    let mut history = ChatHistory::new("Be concise".into());
    history.commit_turn("Hello".into(), "Hi".into());

    let request = history.request_messages("Again");

    assert_eq!(history.turn_count(), 1);
    assert_eq!(request.len(), 4);
    assert_eq!(request[1].role(), Role::User);
    assert_eq!(request[1].content(), "Hello");
    assert_eq!(request[2].role(), Role::Assistant);
    assert_eq!(request[2].content(), "Hi");
    assert_eq!(request[3].role(), Role::User);
    assert_eq!(request[3].content(), "Again");
}

#[test]
fn clear_removes_turns_but_keeps_system_prompt() {
    let mut history = ChatHistory::new("Be concise".into());
    history.commit_turn("Hello".into(), "Hi".into());

    history.clear();
    let request = history.request_messages("Again");

    assert_eq!(history.turn_count(), 0);
    assert_eq!(request.len(), 2);
    assert_eq!(request[0].role(), Role::System);
    assert_eq!(request[0].content(), "Be concise");
    assert_eq!(request[1].role(), Role::User);
}

#[test]
fn blank_system_prompt_is_not_sent() {
    let history = ChatHistory::new("   ".into());

    let request = history.request_messages("Hello");

    assert_eq!(request.len(), 1);
    assert_eq!(request[0].role(), Role::User);
}
