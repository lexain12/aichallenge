use deepseek_cli::memory::{ContextError, ContextProvider, RequestScope};
use deepseek_cli::profile::{ProfileError, UserProfile, profile_markdown};
use deepseek_cli::system_context::{CompactionPolicy, ContextScope};

#[test]
fn profile_renders_one_non_compactable_user_block() {
    let profile = UserProfile::restored(
        "alice",
        "# Preferences\n\nBe concise.",
        "2026-09-18 12:00:00",
    )
    .unwrap();
    let scope = RequestScope::new("alice", "android-app").unwrap();

    let blocks = profile.blocks(&scope).unwrap();

    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].name(), "user_profile");
    assert_eq!(blocks[0].scope(), ContextScope::User);
    assert_eq!(blocks[0].compaction(), CompactionPolicy::Exclude);
    assert!(blocks[0].content().contains("soft defaults"));
    assert!(
        blocks[0]
            .content()
            .ends_with("# Preferences\n\nBe concise.")
    );
}

#[test]
fn profile_applies_across_tasks_but_not_users() {
    let profile = UserProfile::restored("alice", "Be concise.", "now").unwrap();

    assert!(
        profile
            .blocks(&RequestScope::new("alice", "task-b").unwrap())
            .is_ok()
    );
    assert_eq!(
        profile.blocks(&RequestScope::new("bob", "task-b").unwrap()),
        Err(ContextError::ProfileScopeMismatch),
    );
}

#[test]
fn profile_validation_preserves_internal_markdown() {
    assert_eq!(
        profile_markdown("  # Title\n\n- one\n- two  ").unwrap(),
        "# Title\n\n- one\n- two",
    );
    assert_eq!(profile_markdown("  \n "), Err(ProfileError::BlankContent));
}

#[test]
fn restored_profile_normalizes_user_and_exposes_stored_values() {
    let profile = UserProfile::restored(" alice ", " Be concise. ", "now").unwrap();

    assert_eq!(profile.user_id(), "alice");
    assert_eq!(profile.content_markdown(), "Be concise.");
    assert_eq!(profile.updated_at(), "now");
}
