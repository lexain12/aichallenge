use deepseek_cli::dialog::{DialogStore, StoreError};
use deepseek_cli::memory::{ContextError, ContextProvider, RequestScope};
use deepseek_cli::profile::{ProfileError, ProfileRepository, UserProfile, profile_markdown};
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

#[test]
fn profiles_replace_persist_and_stay_isolated_by_user() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    {
        let mut store = DialogStore::open(&path).unwrap();
        store
            .replace_profile(" alice ", "# Alice\n\nBe concise.")
            .unwrap();
        store
            .replace_profile("bob", "# Bob\n\nExplain decisions.")
            .unwrap();
        store
            .replace_profile("alice", "# Alice\n\nPrefer Android.")
            .unwrap();
    }

    let store = DialogStore::open(&path).unwrap();
    assert_eq!(
        store
            .load_profile("alice")
            .unwrap()
            .unwrap()
            .content_markdown(),
        "# Alice\n\nPrefer Android.",
    );
    assert_eq!(
        store
            .load_profile("bob")
            .unwrap()
            .unwrap()
            .content_markdown(),
        "# Bob\n\nExplain decisions.",
    );
    assert!(store.load_profile("carol").unwrap().is_none());
}

#[test]
fn profile_delete_reports_presence_and_uses_normalized_user_id() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    store
        .replace_profile("alice", "# Preferences\n\nBe concise.")
        .unwrap();

    assert!(store.delete_profile(" alice ").unwrap());
    assert!(!store.delete_profile("alice").unwrap());
    assert!(store.load_profile("alice").unwrap().is_none());
}

#[test]
fn repository_rejects_blank_profile_inputs_before_sql() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();

    assert!(matches!(
        store.replace_profile(" ", "valid"),
        Err(StoreError::InvalidProfile(ProfileError::BlankUserId))
    ));
    assert!(matches!(
        store.replace_profile("alice", " \n "),
        Err(StoreError::InvalidProfile(ProfileError::BlankContent))
    ));
    assert!(matches!(
        store.load_profile(" "),
        Err(StoreError::InvalidProfile(ProfileError::BlankUserId))
    ));
    assert!(matches!(
        store.delete_profile(" "),
        Err(StoreError::InvalidProfile(ProfileError::BlankUserId))
    ));
}

#[test]
fn opening_a_database_without_profile_table_migrates_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    DialogStore::open(&path).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("DROP TABLE user_profiles", [])
        .unwrap();

    let mut reopened = DialogStore::open(&path).unwrap();
    reopened
        .replace_profile("alice", "Migration works.")
        .unwrap();
    assert_eq!(
        reopened
            .load_profile("alice")
            .unwrap()
            .unwrap()
            .content_markdown(),
        "Migration works.",
    );
}
