use deepseek_cli::goal_definition::parse_goal_proposal;
use deepseek_cli::workflow::MAX_WORKFLOW_TEXT_CHARS;

#[test]
fn visible_goal_is_extracted_only_from_one_standalone_line() {
    assert_eq!(
        parse_goal_proposal("Обсудим\nПредлагаемая цель: Сделать CLI\n"),
        Ok(Some("Сделать CLI".into()))
    );
    assert_eq!(parse_goal_proposal("Обсудим варианты"), Ok(None));
    assert_eq!(
        parse_goal_proposal("```\nПредлагаемая цель: Не цель\n```"),
        Ok(None)
    );
    assert_eq!(
        parse_goal_proposal("Предлагаемая цель: Точный текст  "),
        Ok(Some("Точный текст".into()))
    );
}

#[test]
fn malformed_or_ambiguous_goal_lines_are_not_proposals() {
    for answer in [
        "Предлагаемая цель: ",
        "Предлагаемая цель:\nСделать CLI",
        "Предлагаемая цель: A\nПредлагаемая цель: B",
        "Предлагаемая цель: A\rB",
        "Предлагаемая цель: A\tB",
    ] {
        assert!(parse_goal_proposal(answer).is_err(), "accepted {answer:?}");
    }
    assert!(
        parse_goal_proposal(&format!(
            "Предлагаемая цель: {}",
            "x".repeat(MAX_WORKFLOW_TEXT_CHARS + 1)
        ))
        .is_err()
    );
}
