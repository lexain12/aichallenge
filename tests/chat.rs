use deepseek_cli::chat::{ChatHistory, InputAction, ProfileAction, Role, parse_input};
use deepseek_cli::memory::DurableMemoryScope;

#[test]
fn parses_explicit_memory_commands_and_normalizes_values() {
    for (name, scope) in [
        ("user", DurableMemoryScope::User),
        ("task", DurableMemoryScope::Task),
    ] {
        assert_eq!(
            parse_input(&format!(
                " /remember\t{name} database  SQLite   local file "
            )),
            InputAction::Remember {
                scope,
                key: "database".into(),
                value: "SQLite local file".into()
            }
        );
        assert_eq!(
            parse_input(&format!("/forget {name} database")),
            InputAction::Forget {
                scope,
                key: "database".into()
            }
        );
        assert_eq!(
            parse_input(&format!("/memory {name}")),
            InputAction::Memory(Some(scope))
        );
    }
    assert_eq!(parse_input("/memory"), InputAction::Memory(None));
}

#[test]
fn malformed_memory_commands_are_local_usage_errors() {
    for (inputs, usage) in [
        (
            vec![
                "/remember",
                "/remember short key value",
                "/remember user",
                "/remember user key",
                "/remember task key   ",
            ],
            "usage: /remember <user|task> <key> <value>",
        ),
        (
            vec![
                "/forget",
                "/forget task",
                "/forget conversation key",
                "/forget user key extra",
            ],
            "usage: /forget <user|task> <key>",
        ),
        (
            vec!["/memory conversation", "/memory user extra"],
            "usage: /memory [user|task]",
        ),
    ] {
        for input in inputs {
            assert_eq!(
                parse_input(input),
                InputAction::InvalidCommand(usage.into()),
                "{input}"
            );
        }
    }
}

#[test]
fn parses_explicit_profile_commands_without_losing_free_form_text() {
    assert_eq!(
        parse_input("/profile"),
        InputAction::Profile(ProfileAction::Show)
    );
    assert_eq!(
        parse_input(" /profile set  Communicate briefly and directly. "),
        InputAction::Profile(ProfileAction::Set(
            "Communicate briefly and directly.".into()
        ))
    );
    assert_eq!(
        parse_input("/profile import ./profiles/Alice Profile.md"),
        InputAction::Profile(ProfileAction::Import("./profiles/Alice Profile.md".into()))
    );
    assert_eq!(
        parse_input("/profile clear"),
        InputAction::Profile(ProfileAction::Clear)
    );
}

#[test]
fn malformed_profile_commands_are_local_usage_errors() {
    for input in [
        "/profile set",
        "/profile import",
        "/profile clear extra",
        "/profile show",
        "/profile unknown",
    ] {
        assert_eq!(
            parse_input(input),
            InputAction::InvalidCommand(
                "usage: /profile [set <markdown>|import <path>|clear]".into()
            ),
            "{input}"
        );
    }
}

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

// Break caught: task status is local metadata, while command arguments must not
// be sent to the model as ordinary chat text.
#[test]
fn parses_task_status_command_exactly() {
    assert_eq!(parse_input(" /task "), InputAction::TaskStatus);
    assert_eq!(
        parse_input("/task extra"),
        InputAction::InvalidCommand("usage: /task".into())
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
