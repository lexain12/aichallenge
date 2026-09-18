# Day 12 User Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one free-form Markdown profile per user, store it durably in SQLite, and inject it automatically into every persistent model request.

**Architecture:** A focused `profile` domain module owns validation, repository contracts, and rendering a `user_profile` system block. `DialogStore` persists one Markdown document per normalized `user_id`; `Agent` loads it before other durable context and exposes explicit local profile operations. CLI commands replace, import, show, and clear the profile without model calls.

**Tech Stack:** Rust, Tokio, rusqlite/SQLite, clap, serde, wiremock, existing `ContextProvider` and `SystemBlock` infrastructure.

**Spec:** `docs/superpowers/specs/2026-09-18-user-profile-design.md`

## Global Constraints

- SQLite is the source of truth; Markdown files are import inputs only.
- Exactly one profile exists per normalized `user_id`.
- Profile content is free-form Markdown, not a structured taxonomy.
- Writes are explicit; ordinary conversation never mutates the profile.
- Profile preferences are soft defaults overridden by the current request and task-specific context.
- The profile block is `ContextScope::User` with `CompactionPolicy::Exclude`.
- Default debug logs must not contain profile content.
- Profile commands are local, make no model calls, and do not create dialogs.
- Day 11 memory behavior and all four Day 10 context strategies remain compatible.
- Hard invariants, task state, transitions, multiple profiles, and automatic extraction are out of scope.

---

### Task 1: Profile domain and context provider

**Files:**
- Create: `src/profile.rs`
- Modify: `src/lib.rs`
- Modify: `src/memory.rs`
- Create: `tests/profile.rs`

**Interfaces:**
- Consumes: `RequestScope`, `ContextProvider`, `SystemBlock`, `ContextScope`, and `CompactionPolicy`.
- Produces: `UserProfile`, `ProfileRepository`, `ProfileError`, `profile_markdown`, and a `ContextProvider` implementation.

- [ ] **Step 1: Write failing domain tests**

Create `tests/profile.rs` with literal expectations for:

```rust
#[test]
fn profile_renders_one_non_compactable_user_block() {
    let profile = UserProfile::restored(
        "alice",
        "# Preferences\n\nBe concise.",
        "2026-09-18 12:00:00",
    ).unwrap();
    let scope = RequestScope::new("alice", "android-app").unwrap();
    let blocks = profile.blocks(&scope).unwrap();

    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].name(), "user_profile");
    assert_eq!(blocks[0].scope(), ContextScope::User);
    assert_eq!(blocks[0].compaction(), CompactionPolicy::Exclude);
    assert!(blocks[0].content().contains("soft defaults"));
    assert!(blocks[0].content().ends_with("# Preferences\n\nBe concise."));
}

#[test]
fn profile_applies_across_tasks_but_not_users() {
    let profile = UserProfile::restored("alice", "Be concise.", "now").unwrap();
    assert!(profile.blocks(&RequestScope::new("alice", "task-b").unwrap()).is_ok());
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
```

- [ ] **Step 2: Verify the tests fail for the missing module**

Run: `cargo test --test profile`

Expected: compilation fails because `deepseek_cli::profile` does not exist.

- [ ] **Step 3: Implement the profile domain**

Create `src/profile.rs` with:

```rust
pub const PROFILE_INTRODUCTION: &str = "User profile preferences. Apply them when relevant. They are soft defaults, not hard constraints. A current explicit request and task-specific context override conflicting profile preferences.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserProfile {
    user_id: String,
    content_markdown: String,
    updated_at: String,
}

impl UserProfile {
    pub fn restored(
        user_id: impl Into<String>,
        markdown: impl Into<String>,
        updated_at: impl Into<String>,
    ) -> Result<Self, ProfileError>;
    pub fn user_id(&self) -> &str;
    pub fn content_markdown(&self) -> &str;
    pub fn updated_at(&self) -> &str;
}

pub trait ProfileRepository {
    type Error;
    fn load_profile(&self, user_id: &str) -> Result<Option<UserProfile>, Self::Error>;
    fn replace_profile(&mut self, user_id: &str, markdown: &str) -> Result<(), Self::Error>;
    fn delete_profile(&mut self, user_id: &str) -> Result<bool, Self::Error>;
}
```

Use `RequestScope::new(user_id, DEFAULT_TASK_ID)` to normalize repository-facing user IDs. Add `ContextError::ProfileScopeMismatch` without changing the existing memory mismatch variant. Render the fixed preamble, one blank line, then the stored Markdown.

- [ ] **Step 4: Export the module and verify green**

Add `pub mod profile;` to `src/lib.rs`.

Run: `cargo test --test profile`

Expected: 3 tests pass.

- [ ] **Step 5: Commit the domain**

```bash
git add src/profile.rs src/lib.rs src/memory.rs tests/profile.rs
git commit -m "Implement #12: Add user profile context provider"
```

### Task 2: SQLite profile repository

**Files:**
- Modify: `src/dialog.rs`
- Modify: `tests/profile.rs`

**Interfaces:**
- Consumes: `ProfileRepository`, `UserProfile`, and `ProfileError` from Task 1.
- Produces: `DialogStore` profile persistence and backward-compatible schema creation.

- [ ] **Step 1: Add failing repository tests**

Add real temporary-SQLite tests that prove:

```rust
#[test]
fn profiles_replace_persist_and_stay_isolated_by_user() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    {
        let mut store = DialogStore::open(&path).unwrap();
        store.replace_profile(" alice ", "# Alice\n\nBe concise.").unwrap();
        store.replace_profile("bob", "# Bob\n\nExplain decisions.").unwrap();
        store.replace_profile("alice", "# Alice\n\nPrefer Android.").unwrap();
    }
    let store = DialogStore::open(&path).unwrap();
    assert_eq!(
        store.load_profile("alice").unwrap().unwrap().content_markdown(),
        "# Alice\n\nPrefer Android.",
    );
    assert_eq!(
        store.load_profile("bob").unwrap().unwrap().content_markdown(),
        "# Bob\n\nExplain decisions.",
    );
}
```

Also cover deletion true/false, blank user IDs, blank content, and opening a
Day 11 database before writing a profile.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test profile`

Expected: compilation fails because `DialogStore` does not implement `ProfileRepository`.

- [ ] **Step 3: Add the migration and repository implementation**

Create `user_profiles` inside the existing idempotent schema batch. Implement:

```rust
impl ProfileRepository for DialogStore {
    type Error = StoreError;

    fn load_profile(&self, user_id: &str) -> Result<Option<UserProfile>, Self::Error>;
    fn replace_profile(&mut self, user_id: &str, markdown: &str) -> Result<(), Self::Error>;
    fn delete_profile(&mut self, user_id: &str) -> Result<bool, Self::Error>;
}
```

Normalize before SQL. Use an immediate transaction for replace/delete. Replace
with `INSERT ... ON CONFLICT(user_id) DO UPDATE` and refresh `updated_at`.

- [ ] **Step 4: Verify persistence tests and existing dialog migrations**

Run: `cargo test --test profile --test dialog`

Expected: all profile and dialog tests pass.

- [ ] **Step 5: Commit persistence**

```bash
git add src/dialog.rs tests/profile.rs
git commit -m "Implement #12: Persist profiles by user"
```

### Task 3: Agent profile lifecycle and request injection

**Files:**
- Modify: `src/agent.rs`
- Modify: `tests/agent.rs`

**Interfaces:**
- Consumes: `ProfileRepository`, `UserProfile`, and its `ContextProvider` implementation.
- Produces: `Agent::profile`, `Agent::replace_profile`, `Agent::clear_profile`, and automatic request injection.

- [ ] **Step 1: Write failing Agent integration tests**

Add tests using real `DialogStore` and wiremock that verify:

```rust
#[tokio::test]
async fn different_users_automatically_receive_only_their_profiles() {
    // Alice profile contains a unique Android marker.
    // Bob profile contains a unique Flutter marker.
    // Send the same prompt from two scoped persistent Agents.
    // Assert each captured payload has user_profile before user_memory,
    // contains only its user's marker, and both calls succeed.
}

#[tokio::test]
async fn profile_is_reloaded_on_each_request_and_excluded_from_compaction() {
    // Replace the profile between requests.
    // Assert the next ordinary request sees the replacement.
    // Force summary compaction and assert neither profile version appears in
    // the summary request while metadata marks user_profile as Exclude.
}

#[tokio::test]
async fn profile_read_failure_preserves_input_and_prevents_other_http() {
    // Temporarily rename user_profiles after constructing the Agent.
    // Assert the user message survives, no model/facts HTTP occurs, and retry
    // works after restoring the table.
}
```

Add an in-memory test that all three profile operations return
`AgentError::ProfileRequiresStore`.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test agent profile`

Expected: compilation fails because the Agent profile API is absent.

- [ ] **Step 3: Implement Agent profile operations**

Add:

```rust
pub fn profile(&self) -> Result<Option<UserProfile>, AgentError>;
pub fn replace_profile(&mut self, markdown: &str) -> Result<(), AgentError>;
pub fn clear_profile(&mut self) -> Result<bool, AgentError>;
```

Use the active scope's `user_id`. Add `ProfileRequiresStore` to `AgentError`.

- [ ] **Step 4: Load profile before memory for every persistent request**

After persisting current input, build additional blocks in this exact order:

```rust
let mut additional_blocks = Vec::new();
if let Some(profile) = self.profile()? {
    additional_blocks.extend(profile.blocks(&self.scope)?);
}
additional_blocks.extend(self.memory_snapshot()?.blocks(&self.scope)?);
```

Retain the existing recovery branch so any context-loading error mirrors the
persisted input once and returns before sticky facts or model requests.

- [ ] **Step 5: Verify focused and regression tests**

Run: `cargo test --test agent profile`

Run: `cargo test --test agent compaction`

Expected: profile tests and existing compaction tests pass.

- [ ] **Step 6: Commit Agent integration**

```bash
git add src/agent.rs tests/agent.rs
git commit -m "Implement #12: Inject profiles into every request"
```

### Task 4: Explicit profile commands and terminal output

**Files:**
- Modify: `src/chat.rs`
- Modify: `src/main.rs`
- Modify: `src/terminal.rs`
- Modify: `tests/chat.rs`
- Modify: `tests/cli.rs`

**Interfaces:**
- Consumes: the Agent profile API from Task 3.
- Produces: `ProfileAction`, deterministic parser behavior, local import, and profile display.

- [ ] **Step 1: Write failing parser tests**

Add:

```rust
assert_eq!(parse_input("/profile"), InputAction::Profile(ProfileAction::Show));
assert_eq!(
    parse_input("/profile set Communicate briefly and directly."),
    InputAction::Profile(ProfileAction::Set(
        "Communicate briefly and directly.".into(),
    )),
);
assert_eq!(
    parse_input("/profile import ./profiles/Alice Profile.md"),
    InputAction::Profile(ProfileAction::Import(
        "./profiles/Alice Profile.md".into(),
    )),
);
assert_eq!(parse_input("/profile clear"), InputAction::Profile(ProfileAction::Clear));
```

Assert `/profile set`, `/profile import`, `/profile clear extra`, and unknown
subcommands return the exact usage:

```text
usage: /profile [set <markdown>|import <path>|clear]
```

- [ ] **Step 2: Verify parser RED**

Run: `cargo test --test chat`

Expected: compilation fails because `ProfileAction` is absent.

- [ ] **Step 3: Implement the parser**

Add:

```rust
pub enum ProfileAction {
    Show,
    Set(String),
    Import(String),
    Clear,
}
```

Parse the remainder after `set` or `import` without splitting away internal
spaces. Profile commands are recognized before treating input as `Send`.

- [ ] **Step 4: Write failing CLI tests**

Cover these observable behaviors:

- `/profile set`, `/profile`, and `/profile clear` make zero HTTP requests and
  create zero dialogs;
- `/profile import <path with spaces>` preserves multiline UTF-8 Markdown;
- a missing/non-UTF-8/blank import prints a local error, preserves the previous
  profile, and the next command still runs;
- profiles persist across processes and tasks for the same user;
- Alice and Bob issue the same prompt but captured requests contain different
  profile markers;
- default debug JSONL omits the profile markers while containing metadata for
  `user_profile`.

- [ ] **Step 5: Add terminal rendering**

Add:

```rust
pub fn write_profile<W: Write>(
    &self,
    writer: &mut W,
    user_id: &str,
    profile: Option<&UserProfile>,
) -> io::Result<()>;
```

Print `Profile · user: <id> · empty` for no row. Otherwise print
`Profile · user: <id>` followed by the complete Markdown.

- [ ] **Step 6: Execute profile actions locally**

In `src/main.rs`, map:

- `Show` to `agent.profile()` and `write_profile`;
- `Set` to `agent.replace_profile`;
- `Import` to `std::fs::read_to_string`, catching read/UTF-8 errors inside the
  loop and leaving the existing profile unchanged;
- `Clear` to `agent.clear_profile` with separate deleted/not-found messages.

Do not call `run_streaming` for any profile action.

- [ ] **Step 7: Verify commands**

Run: `cargo fmt --all`

Run: `cargo test --test chat --test cli`

Expected: parser and CLI tests pass; profile commands produce no extra API calls.

- [ ] **Step 8: Commit the CLI workflow**

```bash
git add src/chat.rs src/main.rs src/terminal.rs tests/chat.rs tests/cli.rs
git commit -m "Implement #12: Add explicit profile commands"
```

### Task 5: Documentation, video script, and release verification

**Files:**
- Modify: `README.md`
- Modify: `docs/DAYS.md`
- Create: `docs/day12-results.md`

**Interfaces:**
- Consumes: completed profile behavior and CLI text.
- Produces: reproducible Day 12 evidence and a recording-ready video script.

- [ ] **Step 1: Update user documentation**

Document:

- free-form Markdown profiles and one-profile-per-user ownership;
- `set`, `import`, show, and clear commands;
- SQLite source of truth;
- automatic request injection and precedence;
- compaction/debug privacy boundaries;
- distinction between soft profile preferences and future hard invariants.

- [ ] **Step 2: Add the reproducible result scenario**

Create `docs/day12-results.md` with exact commands that:

1. create `alice-profile.md` and `bob-profile.md` in a temporary demo directory;
2. import each under its own `--user` and a shared task;
3. run the same application-design prompt for both users;
4. inspect `user_profiles` and verify only one row per user;
5. inspect debug metadata without payload values;
6. replace Alice's profile and demonstrate the next request changes;
7. clear Bob's profile and demonstrate an empty provider;
8. record a 60–90 second video: setup, two imports, same prompt, visibly
   different responses, `/profile`, and SQLite proof.

Do not claim deterministic real-model wording. Automated evidence validates
request payload differences; the video demonstrates the behavioral effect.

- [ ] **Step 3: Run release verification**

Run: `cargo fmt --all -- --check`

Run: `cargo test`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Run: `git diff --check`

Run: `git status --short`

Run: `git log --oneline --decorate 5f4f25b1dab64fa614e0aecd6762d5e9df9f808f..HEAD`

Expected: all checks pass; only intended Day 12 commits appear.

- [ ] **Step 4: Commit documentation**

```bash
git add README.md docs/DAYS.md docs/day12-results.md
git commit -m "Docs #12: Document personalized user profiles"
```
