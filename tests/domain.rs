use deepseek_cli::domain::{ConfirmationId, DialogId, JobId, RequestId, RunId, TurnId};

#[test]
fn job_id_accepts_only_canonical_uuid() {
    let canonical = "550e8400-e29b-41d4-a716-446655440000";
    assert_eq!(canonical.parse::<JobId>().unwrap().to_string(), canonical);

    for invalid in [
        "550E8400-E29B-41D4-A716-446655440000",
        "550e8400e29b41d4a716446655440000",
        "{550e8400-e29b-41d4-a716-446655440000}",
        "not-a-uuid",
    ] {
        assert!(invalid.parse::<JobId>().is_err(), "accepted {invalid}");
    }
}

#[test]
fn ids_reject_blank_or_nonpositive_values() {
    for value in [0, -1] {
        assert!(DialogId::new(value).is_err());
        assert!(TurnId::new(value).is_err());
        assert!(RunId::new(value).is_err());
    }
    assert_eq!(DialogId::new(7).unwrap().get(), 7);
    for invalid in ["", "   ", "not-a-uuid"] {
        assert!(invalid.parse::<RequestId>().is_err());
        assert!(invalid.parse::<ConfirmationId>().is_err());
    }
}
