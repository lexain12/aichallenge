# Day 11: Explicit Memory Layers

## Purpose

Day 11 adds an explicit memory model to the existing DeepSeek CLI. The design must distinguish conversation, task, and user lifetimes; make every durable write intentional; and exclude durable system blocks from direct conversation-compaction input. It must also provide stable extension points for the user profile, task state, invariants, and controlled transitions introduced on Days 12–15.

The implementation extends the existing SQLite persistence and `SystemContext` pipeline. It does not add a second file-based memory system or replace the context strategies implemented on Days 9–10.

## Goals

- Represent three distinct memory lifetimes:
  - short-term conversation memory;
  - working memory for one task;
  - long-term memory for one user.
- Require an explicit scope for every working- or long-term-memory write.
- Persist working and long-term memory independently from dialog history.
- Inject relevant memory into every ordinary model request as named context blocks.
- Make compaction eligibility explicit metadata and enforce it in the compaction path.
- Ensure user and task system blocks never enter a compaction request directly.
- Preserve all existing Day 10 context strategies and dialog restoration behavior.
- Make new context sources easy to add without extending one large request-building function.

## Non-goals

- Automatically infer which facts should become task or user memory.
- Define the structured user profile required by Day 12.
- Implement the task state machine required by Day 13.
- Enforce invariants or transition rules from Days 14–15.
- Add semantic search, embeddings, relevance ranking, or vector storage.
- Add a graphical interface.
- Treat provider configuration such as API keys, model, temperature, or token limits as memory.
- Erase historical conversation or summaries when a durable entry is forgotten.

## Existing Architecture

The current application already has the main primitives needed by this design:

- `ChatHistory` and the `messages` table retain the raw dialog.
- `ContextState` and `dialog_context` retain a cumulative summary.
- `FactsState` and `dialog_facts` retain dialog-scoped sticky facts.
- `SystemContext` assembles named `SystemBlock` values before selected raw messages.
- `prepare_request` accepts additional system blocks.
- `plan_compaction` reads conversation history and the previous conversation summary, not arbitrary system blocks.

Day 11 generalizes these boundaries instead of adding a parallel prompt builder.

## Chosen Approach

Three approaches were considered:

1. Add hard-coded `user_memory` and `task_memory` parameters to `prepare_request`. This is small but makes every later context source another special case.
2. Attach scope and compaction metadata to context blocks and introduce context providers. This keeps the request builder generic while remaining small enough for the current project.
3. Build a general policy engine for storage, retrieval, precedence, and prompt placement. This would be flexible but premature for the current requirements.

The implementation will use approach 2.

## Memory Model

### Request scope

Every running agent has an explicit address:

```rust
pub struct RequestScope {
    pub user_id: String,
    pub task_id: String,
    pub dialog_id: Option<i64>,
}
```

The identifiers are opaque, non-empty strings. `user_id` identifies long-term memory, `user_id + task_id` identifies working memory, and `dialog_id` identifies short-term memory. Task identifiers are user-local, so two users may both have a task named `default` without sharing memory.

For backward compatibility, an invocation without explicit identifiers uses `user_id = "default"` and `task_id = "default"`.

### Layers

| Layer | Address | Stored data in Day 11 | Lifetime |
| --- | --- | --- | --- |
| Conversation | `dialog_id` | messages, summary, sticky facts | one dialog |
| Task | `user_id + task_id` | explicit key-value facts | across dialogs of the task |
| User | `user_id` | explicit key-value facts | across all tasks and dialogs |

Conversation memory remains automatic: sending a message records it in the current dialog. Task and user memory change only through explicit memory commands.

### Entries

Day 11 uses deliberately simple key-value entries:

```rust
pub struct MemoryEntry {
    pub key: String,
    pub value: String,
    pub updated_at: String,
}

pub enum DurableMemoryScope {
    User,
    Task,
}
```

Keys and values are trimmed and must be non-empty. Repeating the same scope and key replaces its value. Day 12 may add a typed profile provider without changing this storage contract; Day 13 may add a typed task-state provider rather than forcing structured state into key-value entries.

## Context Metadata

`SystemBlock` becomes a policy-bearing context block while retaining its existing name and content behavior:

```rust
pub enum ContextScope {
    Application,
    User,
    Task,
    Conversation,
}

pub enum CompactionPolicy {
    Include,
    Exclude,
}

pub struct SystemBlock {
    name: String,
    content: String,
    scope: ContextScope,
    compaction: CompactionPolicy,
}
```

Metadata is executable policy, not documentation. `SystemContext` exposes two selections:

- ordinary-response blocks: all non-empty blocks in deterministic order;
- compaction blocks: only blocks whose policy is `Include`.

The initial policy table is:

| Block | Scope | Compaction |
| --- | --- | --- |
| base prompt | Application | Exclude |
| user memory | User | Exclude |
| task memory | Task | Exclude |
| previous conversation summary | Conversation | Include |
| sticky facts | Conversation | Exclude |

Raw conversation messages are not `SystemBlock` values. The compaction planner continues to select an eligible old prefix of raw messages independently.

The previous summary is marked `Include` because it is an input to the next cumulative summary. User memory, task memory, base instructions, and sticky-facts system blocks are excluded from the compactor's direct inputs. This is a structural block-selection guarantee: facts echoed in user or assistant messages may still be compacted and survive in conversation history or a summary after a durable entry is changed or deleted. `/forget` removes the addressed durable entry only; it does not erase historical messages or summaries.

## Context Providers

New context sources use a small common interface:

```rust
pub trait ContextProvider {
    fn blocks(&self, scope: &RequestScope) -> Result<Vec<SystemBlock>, ContextError>;
}
```

Day 11 adds providers for user and task memory. Existing base, summary, and sticky-facts construction is adapted to produce policy-bearing blocks through the same assembly boundary.

The assembler owns ordering, not individual providers. The ordinary request order is:

1. base prompt;
2. user memory;
3. task memory;
4. conversation summary;
5. sticky facts;
6. selected raw conversation messages;
7. current user message.

The order is stable for reproducible debug logs and tests. The blocks describe their scope explicitly. Day 11 does not implement a general conflict resolver; the prompt states that task memory is specific to the active task and therefore takes precedence over general user memory when both describe the same subject.

Future additions map naturally onto the same pipeline:

| Day | Provider | Scope | Compaction |
| --- | --- | --- | --- |
| 12 | user profile | User | Exclude |
| 13 | task state | Task | Exclude |
| 14 | invariants | Task | Exclude |
| 15 | transition policy | Task | Exclude |

## Persistence

SQLite remains the only durable store. Day 11 adds:

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

CREATE TABLE IF NOT EXISTS dialog_scopes (
    dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
    user_id TEXT NOT NULL,
    task_id TEXT NOT NULL
);
```

The empty `task_id` is an internal sentinel used only for user-scoped rows. This keeps the composite primary key deterministic because SQLite does not treat two `NULL` values as equal for uniqueness. The public model exposes no task identifier for user-scoped memory, and validation forbids an empty task identifier in task-scoped addresses.

On schema initialization, all legacy dialogs that lack a `dialog_scopes` row are assigned the default user and default task. New dialogs write their scope in the same transaction as the dialog and first user message.

The memory persistence API is isolated from prompt construction:

```rust
pub trait MemoryRepository {
    fn list(&self, address: &MemoryAddress) -> Result<Vec<MemoryEntry>, StoreError>;
    fn upsert(&mut self, address: &MemoryAddress, key: &str, value: &str)
        -> Result<(), StoreError>;
    fn delete(&mut self, address: &MemoryAddress, key: &str) -> Result<bool, StoreError>;
}
```

`MemoryAddress` has exactly two valid forms:

```rust
pub enum MemoryAddress {
    User { user_id: String },
    Task { user_id: String, task_id: String },
}
```

Because callers may construct the public variants directly, repository `upsert` and `delete` boundaries trim address identifiers and reject identifiers that are empty after trimming, matching `RequestScope`. For example, task address `" alice " / " bot "` writes to and deletes from canonical `alice/bot`; it cannot create a separate unreachable row.

The existing SQLite store implements this trait. Prompt code depends on the repository contract and memory snapshot, not SQL.

## CLI Scope Selection

New dialogs accept optional identifiers:

```text
cargo run -- --user alice --task telegram-bot
```

Omitted values resolve to `default`. The selected scope is shown once when the chat starts.

A restored dialog always uses its persisted scope. If explicit `--user` or `--task` values disagree with the persisted values, startup fails with a local explanatory error instead of loading memory from a different scope.

Dialog listings add user and task identifiers so that similarly named dialogs remain distinguishable.

## Memory Commands

The terminal parser adds:

```text
/remember user <key> <value...>
/remember task <key> <value...>
/forget user <key>
/forget task <key>
/memory
/memory user
/memory task
```

Examples:

```text
/remember user response_language Russian
/remember task stack Rust
/remember task database SQLite
```

These commands are local and never call the model. A successful write prints the selected layer and address. `/memory` displays three sections:

- conversation: dialog identifier, selected context strategy, message count, summary boundary, and sticky-fact count;
- task: active task entries;
- user: active user entries.

Conversation content is already visible in the chat, so `/memory` reports its state rather than duplicating every message.

Unknown scopes, blank keys, blank values, and extra arguments produce usage errors without modifying storage. Forgetting a missing key reports that nothing changed. A SQLite failure is surfaced and never followed by an in-memory success message.

## Request Flow

For an ordinary prompt:

1. Parse and validate the current request scope.
2. Persist the user message using the existing dialog transaction rules.
3. Load user and task entries for the active scope.
4. Convert each memory layer to one deterministic JSON object, sorted by key.
5. Produce `user_memory` and `task_memory` blocks with `CompactionPolicy::Exclude`.
6. Add existing base, summary, and sticky-facts blocks with their declared metadata.
7. Assemble all ordinary-response blocks and selected dialog messages.
8. Call the model and persist the completed answer as before.

Empty memory layers do not produce empty system messages.

For compaction:

1. After the completed answer is committed, select the old raw-message prefix according to the existing summary strategy.
2. Build the compatible previous-summary block and its coverage boundary from that same history snapshot; do not reuse the pre-turn ordinary request's summary selection.
3. Ask `SystemContext` for blocks allowed in compaction and include the compatible previous conversation summary when present.
4. Exclude base, user memory, task memory, and sticky facts.
5. Replace the stored conversation summary using the existing durable transaction.

Debug metadata records block names, scope, and compaction policy. Message contents remain governed by the existing `debug.log_payloads` setting.

## Isolation and Precedence

- User memory is selected by `user_id` only.
- Task memory is selected by both `user_id` and `task_id`.
- Dialog memory is selected by `dialog_id` and is bound to its persisted user/task scope.
- Starting a new dialog in the same task loses raw short-term history but retains user and task memory.
- Starting another task for the same user retains user memory but not the first task's memory.
- Selecting another user exposes neither the first user's user memory nor their task memory.

Day 11 uses this precedence for informational conflicts:

```text
current explicit request > active task memory > user memory
```

This is not an invariant system. Day 14 will introduce rules that can override a conflicting current request.

## Failure Handling

- Memory-read failure aborts the ordinary model request. Silently sending a request without expected durable memory would produce misleading behavior.
- Memory-write failure reports an error and leaves the previous value intact.
- Invalid commands never call the model or mutate storage.
- Restoring a dialog with missing scope metadata assigns only the migrated default scope; it does not guess from message content.
- A block provider returning an error prevents request assembly and names the failed provider.
- Existing sticky-facts and summary failure behavior remains unchanged.

## Testing

### Unit tests

- Parse every valid and invalid memory command.
- Validate non-empty identifiers, keys, and values.
- Verify deterministic key ordering in memory blocks.
- Verify metadata for base, user, task, summary, and sticky-facts blocks.
- Verify ordinary selection contains all applicable blocks.
- Verify compaction selection excludes user and task memory.
- Verify task information is described as more specific than user defaults.

### Store tests

- Insert, replace, list, and delete a user entry.
- Insert, replace, list, and delete a task entry.
- Isolate two tasks belonging to the same user.
- Isolate two users with identically named tasks.
- Persist and restore dialog scope.
- Migrate legacy dialogs to the default scope.
- Reject or report mismatched explicit scope on resume.

### Agent and integration tests

- An ordinary request contains user and task blocks.
- A compaction request contains neither block, even when both are populated.
- Resuming with retention increased from two to three messages preserves the previous cumulative summary when it becomes compatible after the completed turn.
- A second dialog in the same task receives both durable layers but no first-dialog raw messages.
- A second task receives only the user layer.
- A second user receives neither layer from the first user.
- `/remember`, `/forget`, and `/memory` perform no API request.
- All Day 10 strategy tests continue to pass.

## Demonstration Scenario

The Day 11 video uses one database and the following sequence:

1. Start `alice/telegram-bot`.
2. Save `response_language = Russian` in user memory.
3. Save `stack = Rust` and `database = SQLite` in task memory.
4. Show `/memory` with the three separate sections.
5. Ask the assistant to state the active language and project stack; show the `user_memory` and `task_memory` debug block names.
6. Trigger or demonstrate compaction and show that neither durable block appears in the compactor request.
7. Start a new dialog for `alice/telegram-bot`; show that durable memory remains while raw dialog history does not.
8. Start `alice/another-task`; show that only user memory remains.
9. Start `bob/telegram-bot`; show that Alice's memory is absent.

This demonstrates storage separation, explicit writes, automatic request injection, compaction exclusion, and behavioral isolation.

## Acceptance Criteria

- The three memory lifetimes are visible and independently addressable.
- Every durable memory write names `user` or `task` explicitly.
- User and task entries survive process restart.
- Relevant durable entries are automatically included in ordinary model requests.
- User and task blocks are structurally excluded from compaction and covered by tests.
- Memory from another user or task never appears in the request.
- Existing dialogs migrate to a deterministic default scope.
- All existing tests and the new Day 11 tests pass.
- README and `docs/DAYS.md` document the new branch, commands, scope selection, and video scenario.
