# Explicit Memory Layers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit conversation, task, and user memory lifetimes to the DeepSeek CLI, inject durable memory into ordinary requests, and structurally exclude it from compaction.

**Architecture:** Keep conversation history, summary, and sticky facts in the existing dialog subsystem. Add policy-bearing system blocks, a scoped key-value memory domain, SQLite-backed user/task memory, and persisted dialog scope. The agent loads the active durable layers through a context-provider boundary; the request assembler includes them, while the compaction assembler admits only blocks marked `Include`.

**Tech Stack:** Rust 2024, Tokio, Clap, rusqlite/SQLite, serde/serde_json, thiserror, wiremock, tempfile.

**Spec:** `docs/superpowers/specs/2026-09-18-memory-layers-design.md`

## Global Constraints

- SQLite remains the only durable store; do not add JSON files or another database.
- Working and long-term memory are written only by explicit `/remember task` and `/remember user` commands.
- `user_memory` and `task_memory` blocks must use `CompactionPolicy::Exclude` and must never appear in a compaction request.
- Existing Day 10 `summary`, `sliding_window`, `sticky_facts`, and `branching` behavior must remain available.
- Missing `--user` and `--task` values resolve to the exact identifier `default` for new and migrated dialogs.
- A restored dialog uses its persisted scope; explicit conflicting scope arguments fail locally before an API request.
- Task memory is addressed by both `user_id` and `task_id`; identically named tasks belonging to different users cannot share rows.
- Existing payload privacy remains unchanged: debug logs include block metadata by default and block content only when `debug.log_payloads = true`.
- No embeddings, vector search, automatic memory extraction, profile schema, task state machine, invariants, or transition rules are part of Day 11.

## File Map

- Create `src/memory.rs`: scope identifiers, durable-memory domain types, snapshots, provider interface, validation, and JSON context-block rendering.
- Create `tests/memory.rs`: domain validation, deterministic rendering, layer ordering, and policy tests.
- Modify `src/system_context.rs`: context scope, compaction policy, block metadata, deterministic ordering, and response/compaction selectors.
- Modify `src/context.rs`: policy-bearing request assembly and compaction input filtering.
- Modify `src/facts.rs`: mark sticky facts as conversation-scoped and excluded from compaction.
- Modify `src/debug_log.rs`: serialize block name, scope, and compaction policy for every request.
- Modify `src/dialog.rs`: memory tables, dialog-scope table, legacy migration, memory repository, scope-aware dialog creation/loading/listing/forking.
- Modify `src/agent.rs`: active scope, durable-memory loading and mutation, provider integration, and compaction boundary.
- Modify `src/chat.rs`: parse memory commands into typed local actions.
- Modify `src/main.rs`: scope CLI arguments, resume validation, command execution, memory display, and scope-aware dialog listing.
- Modify `src/terminal.rs`: render scope and memory reports without embedding terminal formatting in the domain layer.
- Modify `src/lib.rs`: export the memory module.
- Modify `tests/context.rs`, `tests/debug_log.rs`, `tests/dialog.rs`, `tests/agent.rs`, `tests/chat.rs`, and `tests/cli.rs`: regression and acceptance coverage.
- Modify `README.md` and `docs/DAYS.md`; create `docs/day11-results.md`: usage, architecture, commands, and reproducible video script.

---

### Task 1: Policy-bearing context blocks and executable compaction filtering

**Files:**
- Modify: `src/system_context.rs`
- Modify: `src/context.rs`
- Modify: `src/facts.rs`
- Modify: `src/debug_log.rs`
- Modify: `src/agent.rs`
- Test: `tests/context.rs`
- Test: `tests/debug_log.rs`

**Interfaces:**
- Produces: `ContextScope`, `CompactionPolicy`, `SystemBlockMetadata`, `SystemBlock::new(name, content, scope, compaction)`, `SystemContext::prompt_blocks()`, `SystemContext::compaction_blocks()`, `PreparedContext::system_context()`, and `RequestMetadata::new(strategy, system_blocks, selected_message_count, summary_boundary, facts_boundary)`.
- Consumes: existing `Message`, `ChatHistory`, `ContextState`, `ContextSummary`, and `ContextConfig` types.

- [ ] **Step 1: Write failing ordering and compaction-policy tests**

Replace the two-argument `SystemBlock::new` calls in `tests/context.rs` and add these assertions:

```rust
use deepseek_cli::system_context::{
    CompactionPolicy, ContextScope, SystemBlock, SystemContext,
};

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
```

- [ ] **Step 2: Run the focused tests and verify the API mismatch**

Run: `cargo test --test context`

Expected: compilation fails because `ContextScope`, `CompactionPolicy`, `prompt_blocks`, `compaction_blocks`, and the new `plan_compaction` parameter do not exist.

- [ ] **Step 3: Implement context metadata and selectors**

Implement the following public surface in `src/system_context.rs`:

```rust
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextScope {
    Application,
    User,
    Task,
    Conversation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionPolicy {
    Include,
    Exclude,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SystemBlockMetadata {
    pub name: String,
    pub scope: ContextScope,
    pub compaction: CompactionPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemBlock {
    name: String,
    content: String,
    scope: ContextScope,
    compaction: CompactionPolicy,
}

impl SystemBlock {
    pub fn new(
        name: impl Into<String>,
        content: impl Into<String>,
        scope: ContextScope,
        compaction: CompactionPolicy,
    ) -> Self {
        Self { name: name.into(), content: content.into(), scope, compaction }
    }

    pub fn name(&self) -> &str { &self.name }
    pub fn content(&self) -> &str { &self.content }
    pub fn scope(&self) -> ContextScope { self.scope }
    pub fn compaction(&self) -> CompactionPolicy { self.compaction }
    pub fn metadata(&self) -> SystemBlockMetadata {
        SystemBlockMetadata {
            name: self.name.clone(),
            scope: self.scope,
            compaction: self.compaction,
        }
    }
}

impl SystemContext {
    pub fn prompt_blocks(&self) -> Vec<&SystemBlock> {
        let mut blocks: Vec<_> = self.blocks.iter().collect();
        blocks.sort_by_key(|block| block.scope());
        blocks
    }

    pub fn compaction_blocks(&self) -> Vec<&SystemBlock> {
        self.prompt_blocks()
            .into_iter()
            .filter(|block| block.compaction() == CompactionPolicy::Include)
            .collect()
    }

    pub fn metadata(&self) -> Vec<SystemBlockMetadata> {
        self.prompt_blocks().into_iter().map(SystemBlock::metadata).collect()
    }
}
```

Keep Rust's stable sort behavior so conversation blocks with the same scope retain insertion order. Make `to_messages()` iterate over `prompt_blocks()`.

- [ ] **Step 4: Route response and compaction assembly through policy metadata**

In `src/context.rs`:

```rust
pub struct PreparedContext {
    messages: Vec<Message>,
    selected_message_count: usize,
    summary_boundary: usize,
    system_block_names: Vec<String>,
    system_context: SystemContext,
}

impl PreparedContext {
    pub fn system_context(&self) -> &SystemContext { &self.system_context }
    pub fn system_block_metadata(&self) -> Vec<SystemBlockMetadata> {
        self.system_context.metadata()
    }
}
```

Retain the existing `system_block_names(&self) -> &[String]` accessor and populate `system_block_names` from the ordered metadata. This keeps the Day 10 public API and its tests working while debug logging moves to structured metadata.

Construct base and summary blocks with exact policies:

```rust
SystemBlock::new(
    "base",
    history.system_prompt(),
    ContextScope::Application,
    CompactionPolicy::Exclude,
)

SystemBlock::new(
    "summary",
    format!("{SUMMARY_CONTEXT_PREFIX}{}", summary.content()),
    ContextScope::Conversation,
    CompactionPolicy::Include,
)
```

Change the compaction signature and build its input only from admitted blocks plus the eligible raw-message prefix:

```rust
pub fn plan_compaction(
    history: &ChatHistory,
    state: &ContextState,
    keep_last_messages: usize,
    system: &SystemContext,
) -> Option<CompactionPlan>
```

For every `system.compaction_blocks()` entry, append `"Context block <name>:\n<content>\n\n"` before `New messages:`. Remove the direct `Previous summary:` branch so the policy selector is the only path by which a system block reaches the compactor.

In `src/facts.rs`, construct the sticky-facts block as:

```rust
SystemBlock::new(
    "facts",
    format!("{FACTS_BLOCK_PREFIX}{json}"),
    ContextScope::Conversation,
    CompactionPolicy::Exclude,
)
```

Update `src/agent.rs` in the same step so the crate remains compilable. Pass `prepared.system_context()` into `maybe_compact`, change `maybe_compact` to accept `&SystemContext`, and pass it to `plan_compaction`. Build metadata for the existing `facts_updater` and `summary_compactor` service requests as application-scoped, excluded descriptors; append `system_context.compaction_blocks()` metadata to the compaction record. No durable memory blocks exist yet, so this is only the executable policy boundary needed by Task 4.

- [ ] **Step 5: Extend request debug metadata and its test**

Change `RequestMetadata` in `src/debug_log.rs` from `system_block_names: Vec<String>` to `system_blocks: Vec<SystemBlockMetadata>`. In `log_request`, serialize both the compatibility name list and the structured metadata:

```rust
let names: Vec<_> = context
    .system_blocks
    .iter()
    .map(|block| block.name.as_str())
    .collect();

let mut value = json!({
    "event": "request_prepared",
    "timestamp_unix_ms": timestamp_unix_ms(),
    "kind": kind,
    "strategy": context.strategy,
    "system_block_names": names,
    "system_blocks": context.system_blocks,
    "selected_message_count": context.selected_message_count,
    "summary_boundary": context.summary_boundary,
    "facts_boundary": context.facts_boundary,
    "message_count": messages.len(),
    "message_metadata": metadata,
});
```

Update `tests/debug_log.rs` to pass metadata for `base` and `facts`, then assert the safe log contains:

```rust
assert!(safe_text.contains(r#""name":"facts","scope":"conversation","compaction":"exclude""#));
```

- [ ] **Step 6: Run formatting and focused tests**

Run: `cargo fmt --all`

Run: `cargo test --test context --test debug_log`

Expected: all context and debug-log tests pass; no compaction payload contains an excluded block.

- [ ] **Step 7: Commit the context-policy boundary**

```bash
git add src/system_context.rs src/context.rs src/facts.rs src/debug_log.rs src/agent.rs tests/context.rs tests/debug_log.rs
git commit -m "Implement #11: Add context block policies"
```

---

### Task 2: Scoped memory domain and SQLite repository

**Files:**
- Create: `src/memory.rs`
- Create: `tests/memory.rs`
- Modify: `src/lib.rs`
- Modify: `src/dialog.rs`
- Test: `tests/dialog.rs`

**Interfaces:**
- Consumes: `ContextScope`, `CompactionPolicy`, and `SystemBlock` from Task 1.
- Produces: `RequestScope`, `DurableMemoryScope`, `MemoryAddress`, `MemoryEntry`, `MemorySnapshot`, `ContextProvider`, `MemoryRepository`, `DialogStore`'s repository implementation, and constants `DEFAULT_USER_ID`/`DEFAULT_TASK_ID`.

- [ ] **Step 1: Write failing memory-domain tests**

Create `tests/memory.rs`:

```rust
use deepseek_cli::memory::{
    ContextProvider, DurableMemoryScope, MemorySnapshot, RequestScope,
};
use deepseek_cli::system_context::{CompactionPolicy, ContextScope};

#[test]
fn identifiers_are_trimmed_and_empty_identifiers_are_rejected() {
    let scope = RequestScope::new(" alice ", " bot ").unwrap();
    assert_eq!(scope.user_id(), "alice");
    assert_eq!(scope.task_id(), "bot");
    assert!(RequestScope::new(" ", "bot").is_err());
    assert!(RequestScope::new("alice", " ").is_err());
}

#[test]
fn snapshot_renders_deterministic_non_compactable_blocks() {
    let scope = RequestScope::new("alice", "bot").unwrap();
    let snapshot = MemorySnapshot::new(
        scope.clone(),
        std::collections::BTreeMap::from([("language".into(), "Russian".into())]),
        std::collections::BTreeMap::from([
            ("stack".into(), "Rust".into()),
            ("database".into(), "SQLite".into()),
        ]),
    );

    let blocks = snapshot.blocks(&scope).unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].name(), "user_memory");
    assert_eq!(blocks[0].scope(), ContextScope::User);
    assert_eq!(blocks[0].compaction(), CompactionPolicy::Exclude);
    assert!(blocks[0].content().contains(r#"{"language":"Russian"}"#));
    assert_eq!(blocks[1].name(), "task_memory");
    assert!(blocks[1].content().contains(r#"{"database":"SQLite","stack":"Rust"}"#));
}

#[test]
fn empty_layers_do_not_create_empty_system_messages() {
    let scope = RequestScope::new("alice", "bot").unwrap();
    let snapshot = MemorySnapshot::new(scope.clone(), Default::default(), Default::default());
    assert!(snapshot.blocks(&scope).unwrap().is_empty());
    assert_eq!(scope.address(DurableMemoryScope::User).task_id(), None);
    assert_eq!(scope.address(DurableMemoryScope::Task).task_id(), Some("bot"));
}
```

- [ ] **Step 2: Run the memory test and verify the module is absent**

Run: `cargo test --test memory`

Expected: compilation fails because `deepseek_cli::memory` is not exported.

- [ ] **Step 3: Implement the memory domain and provider**

Create `src/memory.rs` with these exact public types and validation rules:

```rust
use std::collections::BTreeMap;
use thiserror::Error;

use crate::system_context::{
    CompactionPolicy, ContextScope, SystemBlock,
};

pub const DEFAULT_USER_ID: &str = "default";
pub const DEFAULT_TASK_ID: &str = "default";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableMemoryScope { User, Task }

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestScope {
    user_id: String,
    task_id: String,
    dialog_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemoryAddress {
    User { user_id: String },
    Task { user_id: String, task_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryEntry {
    pub key: String,
    pub value: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemorySnapshot {
    scope: RequestScope,
    user: BTreeMap<String, String>,
    task: BTreeMap<String, String>,
}

pub trait ContextProvider {
    fn blocks(&self, scope: &RequestScope) -> Result<Vec<SystemBlock>, ContextError>;
}

pub trait MemoryRepository {
    type Error;
    fn load_memory(&self, scope: &RequestScope) -> Result<MemorySnapshot, Self::Error>;
    fn upsert_memory(
        &mut self,
        address: &MemoryAddress,
        key: &str,
        value: &str,
    ) -> Result<(), Self::Error>;
    fn delete_memory(
        &mut self,
        address: &MemoryAddress,
        key: &str,
    ) -> Result<bool, Self::Error>;
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum MemoryError {
    #[error("user_id must not be blank")]
    BlankUserId,
    #[error("task_id must not be blank")]
    BlankTaskId,
    #[error("memory key must not be blank")]
    BlankKey,
    #[error("memory value must not be blank")]
    BlankValue,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContextError {
    #[error("memory snapshot does not match the active request scope")]
    ScopeMismatch,
    #[error("failed to serialize memory context: {0}")]
    Serialization(String),
}
```

Implement `RequestScope::new`, getters, `with_dialog_id`, and `address`. Trim identifiers once at construction. Implement `MemorySnapshot::new`, `user_entries`, `task_entries`, and `ContextProvider`. Serialize each non-empty `BTreeMap` with `serde_json::to_string`; use these exact block introductions:

Implement `Default for RequestScope` by calling `RequestScope::new(DEFAULT_USER_ID, DEFAULT_TASK_ID).expect("default memory scope is valid")`. Define `RequestScope::with_dialog_id(&self, dialog_id: Option<i64>) -> Self` as a cloning builder. Add `user_id`, `task_id`, and `dialog_id` getters. Add `MemoryAddress::user_id()` and `MemoryAddress::task_id()` getters; the latter returns `None` for `User` and `Some(&str)` for `Task`.

```text
Long-term user memory. Use it as background context. Active task memory is more specific, and a current explicit user request overrides conflicting memory:
Working memory for the active task. It is more specific than user memory, and a current explicit user request overrides conflicting memory:
```

Both blocks use `CompactionPolicy::Exclude`. Export the module from `src/lib.rs` with `pub mod memory;`.

- [ ] **Step 4: Write failing SQLite memory isolation tests**

Add to `tests/dialog.rs`:

```rust
use deepseek_cli::memory::{
    DurableMemoryScope, MemoryRepository, RequestScope,
};

#[test]
fn durable_memory_is_isolated_by_user_and_task() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let alice_bot = RequestScope::new("alice", "bot").unwrap();
    let alice_other = RequestScope::new("alice", "other").unwrap();
    let bob_bot = RequestScope::new("bob", "bot").unwrap();

    store.upsert_memory(
        &alice_bot.address(DurableMemoryScope::User),
        "language",
        "Russian",
    ).unwrap();
    store.upsert_memory(
        &alice_bot.address(DurableMemoryScope::Task),
        "stack",
        "Rust",
    ).unwrap();

    let first = store.load_memory(&alice_bot).unwrap();
    assert_eq!(first.user_entries()["language"], "Russian");
    assert_eq!(first.task_entries()["stack"], "Rust");
    assert_eq!(store.load_memory(&alice_other).unwrap().user_entries()["language"], "Russian");
    assert!(store.load_memory(&alice_other).unwrap().task_entries().is_empty());
    assert!(store.load_memory(&bob_bot).unwrap().user_entries().is_empty());
    assert!(store.load_memory(&bob_bot).unwrap().task_entries().is_empty());
}

#[test]
fn upsert_replaces_and_delete_reports_whether_a_key_existed() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    let address = scope.address(DurableMemoryScope::User);

    store.upsert_memory(&address, "language", "English").unwrap();
    store.upsert_memory(&address, "language", "Russian").unwrap();
    assert_eq!(store.load_memory(&scope).unwrap().user_entries()["language"], "Russian");
    assert!(store.delete_memory(&address, "language").unwrap());
    assert!(!store.delete_memory(&address, "language").unwrap());
}
```

- [ ] **Step 5: Run the store tests and verify the repository is missing**

Run: `cargo test --test dialog`

Expected: compilation fails because `DialogStore` does not implement `MemoryRepository`.

- [ ] **Step 6: Add the memory schema and repository implementation**

Add this table to `DialogStore::open` in `src/dialog.rs`:

```sql
CREATE TABLE IF NOT EXISTS memory_entries (
    scope_type TEXT NOT NULL CHECK (scope_type IN ('user', 'task')),
    user_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
    CHECK (
        (scope_type = 'user' AND task_id = '') OR
        (scope_type = 'task' AND task_id <> '')
    ),
    PRIMARY KEY (scope_type, user_id, task_id, key)
);
```

Implement `MemoryRepository for DialogStore` using:

```sql
INSERT INTO memory_entries (scope_type, user_id, task_id, key, value)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT(scope_type, user_id, task_id, key) DO UPDATE SET
    value = excluded.value,
    updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now');
```

Load the two active durable layers with:

```sql
SELECT scope_type, key, value
FROM memory_entries
WHERE user_id = ?1
  AND (
      (scope_type = 'user' AND task_id = '') OR
      (scope_type = 'task' AND task_id = ?2)
  )
ORDER BY scope_type, key;
```

Delete one addressed entry with:

```sql
DELETE FROM memory_entries
WHERE scope_type = ?1 AND user_id = ?2 AND task_id = ?3 AND key = ?4;
```

Map `MemoryAddress::User` to `scope_type = 'user', task_id = ''`; map `MemoryAddress::Task` to the non-empty task identifier. Trim and validate keys and values through helpers in `memory.rs` before opening a write transaction. Load rows ordered by `key`, split them into two `BTreeMap` values, and return `MemorySnapshot::new(scope.clone(), user, task)`.

Add `StoreError::InvalidMemory(#[from] MemoryError)` so blank keys and values are returned as typed local validation failures before SQL begins.

- [ ] **Step 7: Run formatting and memory/store tests**

Run: `cargo fmt --all`

Run: `cargo test --test memory --test dialog`

Expected: all memory-domain and dialog-store tests pass.

- [ ] **Step 8: Commit the durable memory repository**

```bash
git add src/memory.rs src/lib.rs src/dialog.rs tests/memory.rs tests/dialog.rs
git commit -m "Implement #11: Store scoped durable memory"
```

---

### Task 3: Persist the user/task scope of every dialog

**Files:**
- Modify: `src/dialog.rs`
- Modify: `tests/dialog.rs`
- Modify: existing call sites in `tests/dialog.rs`

**Interfaces:**
- Consumes: `RequestScope`, `DEFAULT_USER_ID`, and `DEFAULT_TASK_ID` from Task 2.
- Produces: `StoredDialog.scope`, `DialogSummary.scope`, `DialogStore::start_dialog_in_scope(scope, system_prompt, prompt)`, and a default-scope compatibility wrapper `start_dialog(system_prompt, prompt)`.

- [ ] **Step 1: Write failing scope persistence, migration, and branch-copy tests**

Add these cases to `tests/dialog.rs`. Keep existing `start_dialog(system_prompt, prompt)` calls as default-scope regression coverage:

```rust
#[test]
fn dialog_scope_is_persisted_listed_and_copied_to_branches() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    let id = store.start_dialog_in_scope(&scope, "System", "Question").unwrap();
    let fork = store.fork_dialog(id, 1).unwrap();

    assert_eq!(store.load(id).unwrap().scope.user_id(), "alice");
    assert_eq!(store.load(id).unwrap().scope.task_id(), "bot");
    assert_eq!(store.load(fork.new_dialog_id).unwrap().scope, store.load(id).unwrap().scope);
    assert_eq!(store.list().unwrap()[0].scope.user_id(), "alice");
}

#[test]
fn legacy_dialogs_are_migrated_to_default_scope() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    create_day8_database_with_four_messages(&path);

    let store = DialogStore::open(&path).unwrap();
    let scope = store.load(1).unwrap().scope;

    assert_eq!(scope.user_id(), DEFAULT_USER_ID);
    assert_eq!(scope.task_id(), DEFAULT_TASK_ID);
}
```

- [ ] **Step 2: Run the scope tests and verify the old signature fails**

Run: `cargo test --test dialog`

Expected: compilation fails because stored dialogs and summaries have no scope and `start_dialog_in_scope` does not exist.

- [ ] **Step 3: Add dialog scope schema and atomic creation**

Add to `DialogStore::open`:

```sql
CREATE TABLE IF NOT EXISTS dialog_scopes (
    dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
    user_id TEXT NOT NULL,
    task_id TEXT NOT NULL
);

INSERT OR IGNORE INTO dialog_scopes (dialog_id, user_id, task_id)
SELECT id, 'default', 'default' FROM dialogs;
```

Add the explicit constructor and keep the old method as a compatibility wrapper:

```rust
pub fn start_dialog_in_scope(
    &mut self,
    scope: &RequestScope,
    system_prompt: &str,
    prompt: &str,
) -> Result<i64, StoreError> {
    let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let title: String = prompt.chars().take(60).collect();
    tx.execute(
        "INSERT INTO dialogs (system_prompt, title) VALUES (?1, ?2)",
        params![system_prompt, title],
    )?;
    let id = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO dialog_scopes (dialog_id, user_id, task_id) VALUES (?1, ?2, ?3)",
        params![id, scope.user_id(), scope.task_id()],
    )?;
    tx.execute(
        "INSERT INTO messages (dialog_id, role, content) VALUES (?1, 'user', ?2)",
        params![id, prompt],
    )?;
    tx.execute(
        "UPDATE dialogs SET last_message_id = ?1 WHERE id = ?2",
        params![tx.last_insert_rowid(), id],
    )?;
    tx.commit()?;
    Ok(id)
}

pub fn start_dialog(
    &mut self,
    system_prompt: &str,
    prompt: &str,
) -> Result<i64, StoreError> {
    self.start_dialog_in_scope(&RequestScope::default(), system_prompt, prompt)
}
```

Insert `dialog_scopes` in the same immediate transaction as `dialogs` and the first user message. Extend `StoredDialog` and `DialogSummary` with `pub scope: RequestScope`. Load the scope inside the same transaction used for dialog loading.

- [ ] **Step 4: Copy scope in branch transactions and preserve it on switching**

Inside `fork_dialog`, after inserting the new dialog and before committing, execute:

```sql
INSERT INTO dialog_scopes (dialog_id, user_id, task_id)
SELECT ?1, user_id, task_id
FROM dialog_scopes
WHERE dialog_id = ?2;
```

Pass `new_dialog_id` and the source dialog ID. Treat a missing source scope as invalid stored state; migration guarantees legacy rows are populated during `open`.

- [ ] **Step 5: Run the complete dialog suite**

Run: `cargo fmt --all`

Run: `cargo test --test dialog`

Expected: all existing dialog persistence, upgrade, summary, facts, and branching tests pass with explicit default scopes; the new scope tests pass.

- [ ] **Step 6: Commit dialog scope persistence**

```bash
git add src/dialog.rs tests/dialog.rs
git commit -m "Implement #11: Bind dialogs to memory scope"
```

---

### Task 4: Load durable memory in the agent and keep it out of compaction

**Files:**
- Modify: `src/agent.rs`
- Modify: `src/context.rs`
- Test: `tests/agent.rs`

**Interfaces:**
- Consumes: the context policy API from Task 1, memory repository/provider from Task 2, and stored dialog scope from Task 3.
- Produces: `Agent::with_store_for_scope`, `Agent::scope`, `Agent::remember`, `Agent::forget`, `Agent::memory_snapshot`, and scope-preserving clear/branch behavior.

- [ ] **Step 1: Write failing ordinary-request and isolation tests**

Add to `tests/agent.rs`:

```rust
use deepseek_cli::memory::{DurableMemoryScope, RequestScope};

#[tokio::test]
async fn ordinary_request_includes_user_then_task_memory() {
    let server = MockServer::start().await;
    mount(&server, response("Answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let scope = RequestScope::new("alice", "bot").unwrap();
    let mut agent = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&directory.path().join("dialogs.sqlite3")).unwrap(),
        scope,
    ).unwrap();
    agent.remember(DurableMemoryScope::User, "language", "Russian").unwrap();
    agent.remember(DurableMemoryScope::Task, "stack", "Rust").unwrap();

    agent.run_with_prompt("What context do you have?").await.unwrap();

    let request = &server.received_requests().await.unwrap()[0];
    let body: Value = request.body_json().unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert!(messages[1]["content"].as_str().unwrap().contains("Russian"));
    assert!(messages[2]["content"].as_str().unwrap().contains("Rust"));
    assert_eq!(messages.last().unwrap()["content"], "What context do you have?");
}

#[tokio::test]
async fn restored_and_new_dialogs_observe_only_their_addressed_memory() {
    let server = MockServer::start().await;
    mount(&server, response("Answer", true)).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    let scope = RequestScope::new("alice", "bot").unwrap();
    let mut first = Agent::with_store_for_scope(
        &config(&server), DialogStore::open(&path).unwrap(), scope.clone()
    ).unwrap();
    first.remember(DurableMemoryScope::User, "language", "Russian").unwrap();
    first.remember(DurableMemoryScope::Task, "stack", "Rust").unwrap();
    first.run_with_prompt("First dialog").await.unwrap();
    let id = first.dialog_id().unwrap();
    drop(first);

    let restored = Agent::from_dialog(&config(&server), DialogStore::open(&path).unwrap(), id).unwrap();
    assert_eq!(restored.scope(), &scope.with_dialog_id(Some(id)));
    assert_eq!(restored.memory_snapshot().unwrap().task_entries()["stack"], "Rust");

    let other = Agent::with_store_for_scope(
        &config(&server),
        DialogStore::open(&path).unwrap(),
        RequestScope::new("alice", "other").unwrap(),
    ).unwrap();
    assert_eq!(other.memory_snapshot().unwrap().user_entries()["language"], "Russian");
    assert!(other.memory_snapshot().unwrap().task_entries().is_empty());
}
```

- [ ] **Step 2: Write the failing compaction-boundary test**

Add a sequence-response test using `compression_config` with a low threshold. After storing `user secret` and `task decision`, send two prompts and inspect the third request, which is the compaction request:

```rust
let requests = server.received_requests().await.unwrap();
let compaction: Value = requests[2].body_json().unwrap();
let serialized = serde_json::to_string(&compaction["messages"]).unwrap();
assert!(!serialized.contains("user secret"));
assert!(!serialized.contains("task decision"));
```

Mount exactly three responses: first answer, second answer, and summary. Assert there are exactly three requests.

- [ ] **Step 3: Run focused tests and verify the Agent API is absent**

Run: `cargo test --test agent`

Expected: compilation fails because the scope-aware constructor and memory methods do not exist.

- [ ] **Step 4: Add active scope and durable-memory operations to Agent**

Add `scope: RequestScope` to `Agent`. Keep `Agent::new` and `Agent::with_store` as default-scope compatibility constructors, and add:

```rust
pub fn with_store_for_scope(
    config: &Config,
    store: DialogStore,
    scope: RequestScope,
) -> Result<Self, ClientError>

pub fn scope(&self) -> &RequestScope

pub fn remember(
    &mut self,
    layer: DurableMemoryScope,
    key: &str,
    value: &str,
) -> Result<(), AgentError>

pub fn forget(
    &mut self,
    layer: DurableMemoryScope,
    key: &str,
) -> Result<bool, AgentError>

pub fn memory_snapshot(&self) -> Result<MemorySnapshot, AgentError>
```

Return `AgentError::MemoryRequiresStore` for mutation or inspection on an in-memory agent. Add `AgentError::Context(#[from] ContextError)` for provider failures. `remember` and `forget` delegate through `MemoryRepository` using `self.scope.address(layer)`.

`Agent::from_dialog` takes its scope from `StoredDialog.scope` and attaches `dialog_id`. `clear_history` resets conversation state and `dialog_id` but retains `user_id` and `task_id`. `switch_branch` installs the loaded branch's persisted scope.

- [ ] **Step 5: Inject memory providers before every ordinary request**

Immediately after persisting the pending user message, and before `maybe_update_facts` can make a service API call, load the snapshot and blocks:

```rust
let memory_blocks = if persistent {
    match self.memory_snapshot().and_then(|snapshot| {
        snapshot.blocks(&self.scope).map_err(AgentError::Context)
    }) {
        Ok(blocks) => blocks,
        Err(error) => {
            self.history.push(Role::User, prompt.to_owned());
            return Err(error);
        }
    }
} else {
    Vec::new()
};

let mut additional_blocks = memory_blocks;
if self.context_config.strategy() == ContextStrategy::StickyFacts {
    additional_blocks.extend(candidate_facts.system_block());
}
```

Pass `prepared.system_block_metadata()` to `RequestMetadata`. Change `maybe_compact` to accept `&SystemContext` from the prepared request and call:

```rust
plan_compaction(
    &self.history,
    &self.context_state,
    self.context_config.keep_last_messages(),
    system_context,
)
```

The compaction debug record already uses `system_context.compaction_blocks()` plus the `summary_compactor` descriptor from Task 1. Verify that adding memory blocks changes ordinary metadata but not compaction metadata.

- [ ] **Step 6: Run agent and regression tests**

Run: `cargo fmt --all`

Run: `cargo test --test agent --test context --test dialog`

Expected: memory blocks appear in ordinary requests in user-then-task order, are absent from compaction, survive restoration, and existing agent behavior passes.

- [ ] **Step 7: Commit agent memory integration**

```bash
git add src/agent.rs src/context.rs tests/agent.rs
git commit -m "Implement #11: Inject scoped memory into requests"
```

---

### Task 5: Explicit memory commands and CLI scope selection

**Files:**
- Modify: `src/chat.rs`
- Modify: `src/main.rs`
- Modify: `src/terminal.rs`
- Test: `tests/chat.rs`
- Test: `tests/cli.rs`
- Test: unit tests in `src/main.rs`

**Interfaces:**
- Consumes: `RequestScope`, `DurableMemoryScope`, Agent memory operations, `MemorySnapshot`, and `ContextStats` from earlier tasks.
- Produces: typed `InputAction::Remember`, `InputAction::Forget`, `InputAction::Memory`, `--user`, `--task`, scope mismatch validation, and terminal memory reports.

- [ ] **Step 1: Write failing command parser tests**

Add to `tests/chat.rs`:

```rust
use deepseek_cli::memory::DurableMemoryScope;

#[test]
fn parses_explicit_memory_commands() {
    assert_eq!(
        parse_input("/remember user response_language Russian"),
        InputAction::Remember {
            scope: DurableMemoryScope::User,
            key: "response_language".into(),
            value: "Russian".into(),
        }
    );
    assert_eq!(
        parse_input("/remember task database SQLite local file"),
        InputAction::Remember {
            scope: DurableMemoryScope::Task,
            key: "database".into(),
            value: "SQLite local file".into(),
        }
    );
    assert_eq!(
        parse_input("/forget task database"),
        InputAction::Forget {
            scope: DurableMemoryScope::Task,
            key: "database".into(),
        }
    );
    assert_eq!(parse_input("/memory"), InputAction::Memory(None));
    assert_eq!(
        parse_input("/memory user"),
        InputAction::Memory(Some(DurableMemoryScope::User))
    );
}

#[test]
fn malformed_memory_commands_are_local_errors() {
    for input in [
        "/remember",
        "/remember short key value",
        "/remember user key",
        "/forget task",
        "/memory conversation",
        "/memory user extra",
    ] {
        assert!(matches!(parse_input(input), InputAction::InvalidCommand(_)), "{input}");
    }
}
```

- [ ] **Step 2: Run parser tests and verify new actions are absent**

Run: `cargo test --test chat`

Expected: compilation fails because `InputAction` has no memory variants.

- [ ] **Step 3: Implement deterministic command parsing**

Add these variants to `InputAction`:

```rust
Remember { scope: DurableMemoryScope, key: String, value: String },
Forget { scope: DurableMemoryScope, key: String },
Memory(Option<DurableMemoryScope>),
```

Parse `user` and `task` through one helper:

```rust
fn parse_memory_scope(value: Option<&str>) -> Option<DurableMemoryScope> {
    match value {
        Some("user") => Some(DurableMemoryScope::User),
        Some("task") => Some(DurableMemoryScope::Task),
        _ => None,
    }
}
```

Normalize multi-word values with `parts.collect::<Vec<_>>().join(" ")`. Use these exact usage strings:

```text
usage: /remember <user|task> <key> <value>
usage: /forget <user|task> <key>
usage: /memory [user|task]
```

- [ ] **Step 4: Write failing CLI scope and local-command tests**

Add tests in `src/main.rs` for `--user alice --task bot` and rejection of blank values. Add to `tests/cli.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_commands_are_local_and_persist_across_dialogs() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(sse("Answer", 2, 1, 3))
        .expect(2)
        .mount(&server)
        .await;
    let config = write_config(&server.uri());
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let scope_args = ["--user", "alice", "--task", "bot"];

    let first = run_cli_args(
        config.path(),
        &database,
        &scope_args,
        "/remember user language Russian\n/remember task stack Rust\n/memory\n/forget task missing\nQuestion\n/exit\n",
    );
    assert!(first.status.success(), "{}", String::from_utf8_lossy(&first.stderr));
    let first_stdout = String::from_utf8(first.stdout).unwrap();
    assert!(first_stdout.contains("Long-term · language = Russian"));
    assert!(first_stdout.contains("Working · stack = Rust"));

    let second = run_cli_args(
        config.path(),
        &database,
        &scope_args,
        "/memory\nFollow up\n/exit\n",
    );
    assert!(second.status.success());
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let second_request: Value = requests[1].body_json().unwrap();
    let second_messages = serde_json::to_string(&second_request["messages"]).unwrap();
    assert!(second_messages.contains("Russian"));
    assert!(second_messages.contains("Rust"));
    assert!(!second_messages.contains("Question"));
}

#[test]
fn conflicting_scope_on_resume_fails_before_api() {
    let config = write_config("http://127.0.0.1:1");
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialogs.sqlite3");
    let mut store = DialogStore::open(&database).unwrap();
    let id = store.start_dialog_in_scope(
        &RequestScope::new("alice", "bot").unwrap(),
        "System",
        "Question",
    ).unwrap();

    let output = run_cli_args(
        config.path(),
        &database,
        &["--resume", &id.to_string(), "--user", "bob"],
        "/exit\n",
    );
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr).unwrap().contains("belongs to user 'alice'"));
}
```

- [ ] **Step 5: Add scope arguments and resume validation**

Add optional fields to `Args`:

```rust
/// Long-term memory owner for a new dialog.
#[arg(long, value_parser = parse_non_blank_id)]
user: Option<String>,
/// Working-memory task for a new dialog.
#[arg(long, value_parser = parse_non_blank_id)]
task: Option<String>,
```

Use this Clap parser for both fields:

```rust
fn parse_non_blank_id(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        Err("identifier must not be blank".to_owned())
    } else {
        Ok(value.to_owned())
    }
}
```

For a new dialog, resolve omissions to `DEFAULT_USER_ID` and `DEFAULT_TASK_ID`, then call `Agent::with_store_for_scope`. For a restored dialog, call `Agent::from_dialog`, compare only explicitly supplied values, and return:

```rust
#[error("dialog belongs to user '{stored}', not requested user '{requested}'")]
UserScopeMismatch { stored: String, requested: String },

#[error("dialog belongs to task '{stored}', not requested task '{requested}'")]
TaskScopeMismatch { stored: String, requested: String },
```

Print `Scope · user: <id> · task: <id>` once after agent construction. Extend dialog-list output to `ID | User | Task | Updated (UTC) | Messages | First message`.

- [ ] **Step 6: Execute memory actions without API calls**

Add match arms in `src/main.rs`:

```rust
InputAction::Remember { scope, key, value } => {
    agent.remember(scope, &key, &value)?;
    stdout_ui.write_block(
        &mut stdout,
        BlockStyle::System,
        &format!("Saved {} memory · {key} = {value}", scope.label()),
    )?;
}
InputAction::Forget { scope, key } => {
    let removed = agent.forget(scope, &key)?;
    let message = if removed {
        format!("Forgot {} memory · {key}", scope.label())
    } else {
        format!("No {} memory entry named {key}", scope.label())
    };
    stdout_ui.write_block(&mut stdout, BlockStyle::System, &message)?;
}
InputAction::Memory(filter) => {
    let snapshot = agent.memory_snapshot()?;
    stdout_ui.write_memory(
        &mut stdout,
        agent.scope(),
        agent.context_stats(),
        &snapshot,
        filter,
    )?;
}
```

Implement `DurableMemoryScope::label()` as `Long-term` for user and `Working` for task. Add `TerminalUi::write_memory`. With no filter it prints `Conversation · dialog: <new|#id> · strategy: <name> · messages: <count> · summary boundary: <count> · sticky facts: <count>`, followed by sorted `Working · key = value` and `Long-term · key = value` lines. With `user` or `task` filtering it prints only the selected durable section. An empty selected section prints `Long-term · empty` or `Working · empty`. Do not print entry values to debug logs unless payload logging is enabled.

- [ ] **Step 7: Run parser, CLI, and full tests**

Run: `cargo fmt --all`

Run: `cargo test --test chat --test cli`

Run: `cargo test`

Expected: memory commands cause no extra API requests; scope selection, persistence, resume mismatch, all existing CLI behavior, and the full suite pass.

- [ ] **Step 8: Commit the CLI workflow**

```bash
git add src/chat.rs src/main.rs src/terminal.rs tests/chat.rs tests/cli.rs
git commit -m "Implement #11: Add explicit memory commands"
```

---

### Task 6: Documentation, demonstration record, and release verification

**Files:**
- Modify: `README.md`
- Modify: `docs/DAYS.md`
- Create: `docs/day11-results.md`

**Interfaces:**
- Consumes: the final CLI syntax, context metadata names, and behavior implemented in Tasks 1–5.
- Produces: user-facing Day 11 instructions and a reproducible recording script.

- [ ] **Step 1: Update README for Day 11**

Change the branch banner to `Day-11` based on `Day-10`. Add a `День 11: явные слои памяти` section containing these commands:

```text
cargo run -- --user alice --task telegram-bot
/remember user response_language Russian
/remember task stack Rust
/memory
/forget task stack
```

Document the exact lifetimes:

```text
conversation = dialog ID; task = user ID + task ID; user = user ID
```

State explicitly that `user_memory` and `task_memory` appear in ordinary request system blocks and are excluded from summary compaction. Explain that profile, task-state, invariant, and transition-policy providers will reuse the same block interface without claiming those later-day features already exist.

- [ ] **Step 2: Add Day 11 navigation and recording script**

Append this row to `docs/DAYS.md`:

```markdown
| `Day-11` | Явные conversation/task/user memory layers и защита от compaction | `cargo run -- --user alice --task telegram-bot` |
```

Create `docs/day11-results.md` with these exact demonstrations:

1. Save one user entry and two task entries for `alice/telegram-bot`.
2. Run `/memory` and show conversation, working, and long-term sections.
3. Send a question and show `user_memory` then `task_memory` in the safe debug metadata.
4. Trigger summary compaction and show that compaction metadata contains no user/task blocks.
5. Start a new `alice/telegram-bot` dialog and show both durable layers without old raw messages.
6. Start `alice/another-task` and show only the user layer.
7. Start `bob/telegram-bot` and show neither Alice layer.
8. Attempt to resume Alice's dialog with `--user bob` and show the local mismatch error.

Include the commands, expected block names, and SQLite inspection queries, but do not invent real model answers or token measurements.

- [ ] **Step 3: Run release-quality verification**

Run: `cargo fmt --all -- --check`

Expected: exit code 0 and no formatting diff.

Run: `cargo test`

Expected: exit code 0; all unit and integration tests pass.

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: exit code 0 with no warnings.

Run: `git diff --check`

Expected: exit code 0 with no whitespace errors.

- [ ] **Step 4: Inspect the final diff against the acceptance criteria**

Run: `git status --short`

Expected before the documentation commit: only `README.md`, `docs/DAYS.md`, and `docs/day11-results.md` are uncommitted.

Run: `git diff -- README.md docs/DAYS.md docs/day11-results.md`

Verify the diff names all three lifetimes, explicit write commands, automatic request injection, compaction exclusion, user/task isolation, and the Day 11 branch command.

- [ ] **Step 5: Commit documentation**

```bash
git add README.md docs/DAYS.md docs/day11-results.md
git commit -m "Docs #11: Explain explicit memory layers"
```

- [ ] **Step 6: Verify the committed branch is clean**

Run: `git status --short`

Expected: no output.

Run: `git log --oneline --decorate 5726ba8988cf3d8db4368cf8ce4eeac24cb70a29..HEAD`

Expected: every Day 11 commit since the recorded merge base, including the design, plan, implementation, documentation, and review-fix commits, with `HEAD -> Day-11` on the latest commit. Do not assume a fixed commit count.

### Final-review ruling

The initial implementation steps above describe the original task sequence. Final review found that reusing the pre-turn `PreparedContext` during post-turn compaction could omit a prior summary while still skipping its covered messages when retention changed on resume. The corrected planner builds the summary block and boundary together from the post-turn history and context state, accepts additional provider blocks, applies compaction policy filtering, and exposes the admitted metadata for logging.

Repository mutations also normalize and validate directly constructed `MemoryAddress` identifiers with the same trimming/blank rules as `RequestScope`. Demo assertions must fail when their selected request record is missing. Compaction exclusion applies to direct system blocks; facts echoed in conversation may still enter summaries, and `/forget` does not erase historical conversation or summaries.

The merge-base range command supersedes both the original `-7` instruction and the earlier `-8` pre-flight ruling. Historical command transcripts remain unchanged in the SDD reports.
