# Day 10 Context Strategies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add configurable summary, sliding-window, sticky-facts, and branching context strategies while keeping extensible system blocks outside conversational history reduction.

**Architecture:** `SystemContext` owns ordered system blocks independently of ordinary messages, while `context` selects raw messages according to a required `ContextStrategy`. Sticky facts use a persisted JSON key-value state updated by an isolated DeepSeek call before each ordinary response; branching atomically clones dialogs and switches only within a stored branch group.

**Tech Stack:** Rust 2024, Tokio, Reqwest, Serde/serde_json, Rusqlite, Clap, Wiremock, tempfile.

**Spec:** `docs/superpowers/specs/2026-09-14-context-strategies-design.md`

## Global Constraints

- Work on Git branch `Day-10`.
- `[context].strategy` is required; Day 9 configuration compatibility is not required.
- Supported values are exactly `summary`, `sliding_window`, `sticky_facts`, and `branching`.
- Base instructions and all future system blocks stay outside `ChatHistory`, SQLite `messages`, sliding-window selection, and summary compaction.
- The pending user input counts toward a sliding or sticky-facts window of N messages.
- Persistent dialogs keep every original message even when a strategy omits it from the API request.
- Facts are a strict JSON object with string keys and string values, grounded only in user messages.
- A facts service failure is non-fatal and does not advance its boundary; a facts persistence failure is fatal.
- `/branch` and `/switch <id>` work only in branching mode.
- Branch creation is a physical, atomic dialog copy; no DAG or copy-on-write storage.
- Do not perform real DeepSeek comparison runs until the user requests them.

---

### Task 1: Required strategy configuration

**Files:**
- Modify: `src/config.rs`
- Modify: `src/agent.rs`
- Modify: `tests/config.rs`
- Modify: `tests/agent.rs`
- Modify: `tests/client.rs`
- Modify: `tests/cli.rs`
- Modify: `deepseek.example.toml`

**Interfaces:**
- Produces: `ContextStrategy::{Summary, SlidingWindow, StickyFacts, Branching}`.
- Produces: `ContextConfig::strategy() -> ContextStrategy` and `ContextConfig::facts_max_tokens() -> u32`.
- Preserves: existing summary limit accessors for the `summary` strategy.

- [ ] **Step 1: Write failing configuration tests**

Replace Day 9 default-context assertions with explicit strategy tests and add:

```rust
use deepseek_cli::config::ContextStrategy;

#[test]
fn requires_context_strategy_and_accepts_all_four_values() {
    let missing = Config::from_toml("api_key = \"key\"", None).unwrap_err();
    assert!(missing.to_string().contains("strategy"));

    for (text, expected) in [
        ("summary", ContextStrategy::Summary),
        ("sliding_window", ContextStrategy::SlidingWindow),
        ("sticky_facts", ContextStrategy::StickyFacts),
        ("branching", ContextStrategy::Branching),
    ] {
        let source = format!("api_key = \"key\"\n[context]\nstrategy = \"{text}\"");
        assert_eq!(Config::from_toml(&source, None).unwrap().context().strategy(), expected);
    }
}

#[test]
fn rejects_removed_enabled_and_zero_facts_limit() {
    assert!(Config::from_toml(
        "api_key = \"key\"\n[context]\nstrategy = \"summary\"\nenabled = true",
        None,
    ).is_err());
    let error = Config::from_toml(
        "api_key = \"key\"\n[context]\nstrategy = \"sticky_facts\"\nfacts_max_tokens = 0",
        None,
    ).unwrap_err();
    assert!(error.to_string().contains("facts_max_tokens"));
}
```

Update every test configuration fixture to include:

```toml
[context]
strategy = "summary"
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cargo test --test config`

Expected: compilation fails because `ContextStrategy` and the new accessors do not exist.

- [ ] **Step 3: Implement the enum and strict parsing**

In `src/config.rs`, deserialize a required enum and remove `enabled`:

```rust
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ContextStrategy {
    Summary,
    SlidingWindow,
    StickyFacts,
    Branching,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawContextConfig {
    strategy: Option<ContextStrategy>,
    compact_after_prompt_tokens: Option<u64>,
    keep_last_messages: Option<usize>,
    summary_max_tokens: Option<u32>,
    facts_max_tokens: Option<u32>,
}
```

Return `ConfigError::InvalidField { field: "context.strategy", reason: "must be set" }` when absent. Add `DEFAULT_FACTS_MAX_TOKENS: u32 = 512`, validate every numeric field as greater than zero, and store all values in `ContextConfig`. Mechanically change the current agent summary checks from `enabled()` to `strategy() == ContextStrategy::Summary`; behavior remains Day 9 until later tasks. Replace the internal `ContextConfig::disabled()` used by `Agent::from_client` with `ContextConfig::full_history()`, configured as `Branching` with the numeric defaults. This preserves that constructor's full-history behavior; its lack of a store already prevents branch commands.

- [ ] **Step 4: Update the example and verify GREEN**

Add `strategy = "summary"` and `facts_max_tokens = 512` to `deepseek.example.toml`.

Run: `cargo test --test config --test client --test agent --test cli`

Expected: PASS with all fixtures using an explicit strategy.

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/agent.rs tests/config.rs tests/agent.rs tests/client.rs tests/cli.rs deepseek.example.toml
git commit -m "Implement #10: Configure context strategies"
```

### Task 2: General system context and strategy request selection

**Files:**
- Create: `src/system_context.rs`
- Modify: `src/lib.rs`
- Modify: `src/context.rs`
- Modify: `tests/context.rs`

**Interfaces:**
- Produces: `SystemBlock::new(name, content)`, `SystemBlock::name()`, and `SystemBlock::content()`.
- Produces: `SystemContext::push(block)` and `SystemContext::to_messages() -> Vec<Message>`.
- Produces: `PreparedContext { messages, selected_message_count, summary_boundary }`.
- Produces: `prepare_request(history, summary_state, config, pending_user, additional_system_blocks) -> PreparedContext`.

- [ ] **Step 1: Write failing tests for protected blocks and exact windows**

Add tests that describe the public behavior:

```rust
fn context_config(strategy: &str, keep: usize) -> ContextConfig {
    let source = format!(
        "api_key = \"key\"\n[context]\nstrategy = \"{strategy}\"\nkeep_last_messages = {keep}"
    );
    Config::from_toml(&source, None).unwrap().context().clone()
}

#[test]
fn system_blocks_are_ordered_before_windowed_history() {
    let mut system = SystemContext::default();
    system.push(SystemBlock::new("base", "Base rules"));
    system.push(SystemBlock::new("profile", "Future profile"));
    let ordinary = vec![
        Message::for_request(Role::User, "u1"),
        Message::for_request(Role::Assistant, "a1"),
        Message::for_request(Role::User, "u2"),
    ];

    let request = assemble_request(&system, &ordinary, HistorySelection::Last(2));

    assert_eq!(request.iter().map(Message::content).collect::<Vec<_>>(),
        ["Base rules", "Future profile", "a1", "u2"]);
    assert_eq!(request[0].role(), Role::System);
    assert_eq!(request[1].role(), Role::System);
}

#[test]
fn pending_user_message_counts_toward_sliding_window() {
    let config = context_config("sliding_window", 2);
    let request = prepare_request(
        &history(),
        &ContextState::default(),
        &config,
        "next",
        &[],
    );
    assert_eq!(request.messages().iter().map(Message::content).collect::<Vec<_>>(),
        ["Original system", "a3", "next"]);
    assert_eq!(request.selected_message_count(), 2);
}
```

Also retain the existing exact summary request test and add exact full-history behavior for `branching`.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test context`

Expected: compilation fails because the system-context types and strategy-aware builder do not exist.

- [ ] **Step 3: Implement the independent system-block layer**

Use owned names and contents so future callers can add blocks without extending an enum:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemBlock { name: String, content: String }

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SystemContext { blocks: Vec<SystemBlock> }
```

`push` ignores blank content. `to_messages` serializes every retained block as `Role::System` without inserting it into `ChatHistory`.

In `context.rs`, add:

```rust
pub enum HistorySelection { Full, Last(usize), After(usize) }

pub struct PreparedContext {
    messages: Vec<Message>,
    selected_message_count: usize,
    summary_boundary: usize,
    system_block_names: Vec<String>,
}
```

Build a temporary ordinary sequence from committed history plus the pending user message. Select `After(boundary)` for a compatible summary, `Last(N)` for sliding and sticky facts, and `Full` for branching. Compose the base and summary blocks, followed by the caller-supplied `additional_system_blocks`, before calling the generic `assemble_request`. Task 5 supplies the facts block; future features can use the same argument without changing selection logic.

- [ ] **Step 4: Run all context tests and verify GREEN**

Run: `cargo test --test context --test chat`

Expected: PASS; exact Day 9 summary ordering remains unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/system_context.rs src/lib.rs src/context.rs tests/context.rs
git commit -m "Implement #10: Select strategy context"
```

### Task 3: Sticky-facts state, planning, parsing, and client call

**Files:**
- Create: `src/facts.rs`
- Create: `tests/facts.rs`
- Modify: `src/lib.rs`
- Modify: `src/client.rs`
- Modify: `tests/client.rs`

**Interfaces:**
- Produces: `Facts = BTreeMap<String, String>`.
- Produces: `FactsState::restored(facts, covered_message_count, update_usage)` plus read accessors.
- Produces: `plan_facts_update(messages, state) -> Option<FactsUpdatePlan>`.
- Produces: `parse_facts_json(text) -> Result<Facts, FactsError>`.
- Produces: `DeepSeekClient::update_facts(messages, max_tokens) -> Result<SummaryResult, ClientError>`.

- [ ] **Step 1: Write failing facts unit tests**

Create `tests/facts.rs` with real messages and assertions:

```rust
fn msg(role: Role, content: &str) -> Message {
    Message::for_request(role, content)
}

#[test]
fn update_plan_contains_previous_map_and_only_uncovered_user_messages() {
    let state = FactsState::restored(
        BTreeMap::from([("goal".into(), "ship CLI".into())]),
        2,
        UsageTotals::default(),
    );
    let messages = vec![
        msg(Role::User, "old"), msg(Role::Assistant, "old answer"),
        msg(Role::User, "deadline Friday"), msg(Role::Assistant, "suggestion"),
        msg(Role::User, "cancel Friday; deadline Monday"),
    ];
    let plan = plan_facts_update(&messages, &state).unwrap();
    assert_eq!(plan.covered_message_count(), 5);
    assert!(plan.request_messages()[1].content().contains("deadline Friday"));
    assert!(plan.request_messages()[1].content().contains("deadline Monday"));
    assert!(!plan.request_messages()[1].content().contains("suggestion"));
}

#[test]
fn parser_accepts_only_string_to_string_json_objects() {
    assert_eq!(parse_facts_json(r#"{"goal":"ship"}"#).unwrap()["goal"], "ship");
    for invalid in ["", "```json\\n{}\\n```", "[]", r#"{"count":3}"#] {
        assert!(parse_facts_json(invalid).is_err(), "accepted {invalid:?}");
    }
}
```

- [ ] **Step 2: Add a failing client isolation test**

Mirror the summary test in `tests/client.rs` and assert the facts request uses temperature `0`, `max_tokens = 512`, disabled thinking, no `top_p`, no stop sequences, and returns usage.

Run: `cargo test --test facts --test client`

Expected: compilation fails because facts APIs and `update_facts` are absent.

- [ ] **Step 3: Implement facts state and strict parser**

The updater request consists of a fixed system instruction and one user message containing serialized previous facts plus numbered uncovered user messages. The system instruction must require a bare JSON object, user-grounded facts, stable descriptive keys, replacement of obsolete values, removal of explicit revocations, and no assistant suggestions.

Parse directly with `serde_json::from_str::<BTreeMap<String, String>>()`; do not strip Markdown fences or coerce values. Record service usage with the existing `UsageTotals`.

- [ ] **Step 4: Implement the isolated client call and verify GREEN**

Extract the shared deterministic service-call path used by `summarize` and expose `update_facts` with the same `SummaryResult`. Both calls use temperature `0`, disabled thinking, `top_p = None`, and an empty stop list; their caller supplies the output limit.

Run: `cargo test --test facts --test client`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/facts.rs tests/facts.rs src/lib.rs src/client.rs tests/client.rs
git commit -m "Implement #10: Extract sticky facts"
```

### Task 4: Persist and restore sticky facts

**Files:**
- Modify: `src/dialog.rs`
- Modify: `tests/dialog.rs`

**Interfaces:**
- Extends: `StoredDialog` with `facts: FactsState`.
- Produces: `DialogStore::replace_facts(id, expected_message_count, facts, usage) -> Result<FactsState, StoreError>`.

- [ ] **Step 1: Write failing persistence and rollback tests**

Add a test that starts a four-message dialog, writes facts twice (one call with known usage and one without), reopens the database, and asserts the newest map, boundary, `call_count == 2`, known token sum, and `missing_usage_count == 1`.

Add validation assertions:

```rust
assert!(matches!(
    store.replace_facts(id, 3, Facts::new(), None),
    Err(StoreError::Conflict(found)) if found == id
));
assert!(matches!(
    store.replace_facts(id, 4, Facts::new(), None),
    Ok(_)
));
```

Create a trigger that rejects an update of `facts_json`; assert the prior row and metrics remain unchanged after the error. Insert malformed JSON through a raw connection and assert `load` returns `StoreError::InvalidFacts`.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test dialog`

Expected: compilation fails because facts persistence APIs do not exist.

- [ ] **Step 3: Add schema and transactional replacement**

Create `dialog_facts` exactly as specified in the design. `replace_facts` must:

1. reject a zero or out-of-range covered boundary;
2. begin an immediate transaction;
3. verify exact message count;
4. decode prior cumulative usage;
5. record this call's optional usage;
6. serialize the map and upsert all state in one statement; and
7. commit before returning the new `FactsState`.

Loading validates the JSON type through `BTreeMap<String, String>` and verifies the boundary against the messages loaded in the same SQLite snapshot.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test dialog`

Expected: PASS, including the existing Day 7–9 migration tests.

- [ ] **Step 5: Commit**

```bash
git add src/dialog.rs tests/dialog.rs
git commit -m "Implement #10: Persist sticky facts"
```

### Task 5: Integrate sticky facts into the agent lifecycle

**Files:**
- Modify: `src/agent.rs`
- Modify: `src/context.rs`
- Modify: `tests/agent.rs`

**Interfaces:**
- Extends: `Agent` with `facts_state: FactsState`.
- Extends: `AgentEvent` with `FactsUpdateStarted`, `FactsUpdateCompleted { covered_message_count, usage }`, and `FactsUpdateFailed { error }`.
- Preserves: persistent inputs survive API failures; in-memory histories commit only complete turns.

- [ ] **Step 1: Write a failing success-path agent test**

Use a two-response Wiremock sequence: valid facts JSON followed by an ordinary answer. Assert request 1 is the isolated facts update and request 2 is exactly:

```json
[
  {"role":"system","content":"Be concise."},
  {"role":"system","content":"Facts (JSON key-value memory):\n{\"goal\":\"prepare specification\"}"},
  {"role":"user","content":"We need a specification"}
]
```

Reopen SQLite and assert the facts map, boundary, and update usage were restored.

- [ ] **Step 2: Write a failing recovery test**

Arrange four responses: invalid facts JSON, ordinary answer, valid catch-up facts JSON, ordinary answer. Assert the first ordinary request contains no facts block, while the second facts request contains both user messages and excludes assistant text. Assert the second ordinary request contains the recovered facts and the N-message window.

Add an in-memory failure case: if facts extraction succeeds but the ordinary call fails, `history()` and committed `FactsState` remain unchanged.

- [ ] **Step 3: Run and verify RED**

Run: `cargo test --test agent sticky_facts`

Expected: the tests fail because the agent never calls the facts updater.

- [ ] **Step 4: Implement staged facts update before ordinary request**

For persistent agents, save and push the user input, update and persist facts, then prepare the ordinary request. For in-memory agents, build candidate messages and candidate facts without mutating committed state; commit both only after a complete ordinary answer. On a service or parse failure, emit `FactsUpdateFailed`, retain the prior boundary, and continue. Propagate store errors immediately.

Only invoke `maybe_compact` when `strategy == Summary`. `clear_history` resets facts. `from_dialog` restores them.

- [ ] **Step 5: Verify GREEN and summary regression**

Run: `cargo test --test agent`

Expected: PASS for new facts tests and all existing summary, persistence, cancellation, and usage tests.

- [ ] **Step 6: Commit**

```bash
git add src/agent.rs src/context.rs tests/agent.rs
git commit -m "Implement #10: Use sticky facts in agent"
```

### Task 6: Atomically clone dialogs into branch groups

**Files:**
- Modify: `src/dialog.rs`
- Modify: `tests/dialog.rs`

**Interfaces:**
- Produces: `BranchInfo { dialog_id, branch_group_id, parent_dialog_id, checkpoint_message_count }`.
- Produces: `ForkResult { original_dialog_id, new_dialog_id, branch_group_id, checkpoint_message_count }`.
- Produces: `DialogStore::fork_dialog(id, expected_message_count) -> Result<ForkResult, StoreError>`.
- Produces: `DialogStore::load_branch_member(current_id, target_id) -> Result<StoredDialog, StoreError>`.
- Extends: `StoredDialog` with `branch: Option<BranchInfo>`.

- [ ] **Step 1: Write failing clone and independence tests**

Create a dialog with user/assistant messages, answer usage, summary, and facts. Fork it and assert:

```rust
assert_eq!(fork.original_dialog_id, original);
assert_ne!(fork.new_dialog_id, original);
assert_eq!(fork.checkpoint_message_count, 4);
assert_eq!(store.load(fork.new_dialog_id).unwrap().messages, store.load(original).unwrap().messages);
assert_eq!(store.load(fork.new_dialog_id).unwrap().context, store.load(original).unwrap().context);
assert_eq!(store.load(fork.new_dialog_id).unwrap().facts, store.load(original).unwrap().facts);
```

Append different turns to original and child and assert neither appears in the other. Assert their branch metadata shares one group and identifies the child's parent.

Add tests that stale `expected_message_count`, a copy trigger failure, an unrelated target, and a missing target leave the dialog count and original state unchanged.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test dialog branch`

Expected: compilation fails because branch APIs are absent.

- [ ] **Step 3: Implement schema and physical copy transaction**

Create `dialog_branches` and its group index exactly as specified. In one immediate transaction, verify the source count, insert root metadata if absent, insert the child dialog with a ` (branch)` title suffix, copy messages one-by-one while mapping old message IDs to new IDs for `message_usage`, copy optional `dialog_context` and `dialog_facts`, insert child metadata, set the child's `last_message_id`, and commit.

`load_branch_member` first requires branch metadata for the current dialog, verifies the target has the same `branch_group_id`, then loads the target. Add precise errors `NoBranchGroup(id)` and `UnrelatedBranch { current, target }`.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test dialog`

Expected: PASS with atomic rollback and independent branches proven.

- [ ] **Step 5: Commit**

```bash
git add src/dialog.rs tests/dialog.rs
git commit -m "Implement #10: Clone dialog branches"
```

### Task 7: Agent branch operations and CLI commands

**Files:**
- Modify: `src/chat.rs`
- Modify: `src/agent.rs`
- Modify: `src/main.rs`
- Modify: `tests/chat.rs`
- Modify: `tests/agent.rs`
- Modify: `tests/cli.rs`

**Interfaces:**
- Extends: `InputAction` with `Branch`, `Switch(i64)`, and `InvalidCommand(String)`.
- Produces: `Agent::branch_dialog() -> Result<ForkResult, AgentError>`.
- Produces: `Agent::switch_branch(id) -> Result<(), AgentError>`.
- Produces: `AgentError::BranchingStrategyRequired` and `AgentError::NoPersistentDialog`.

- [ ] **Step 1: Write failing parser tests**

```rust
assert_eq!(parse_input("/branch"), InputAction::Branch);
assert_eq!(parse_input("/switch 42"), InputAction::Switch(42));
for input in ["/branch extra", "/switch", "/switch 0", "/switch nope", "/switch 1 extra"] {
    assert!(matches!(parse_input(input), InputAction::InvalidCommand(_)));
}
```

Unknown slash-prefixed text continues to be sent as a normal user message.

- [ ] **Step 2: Write failing agent and CLI branch tests**

At agent level, assert branch methods reject non-branching strategies and in-memory/no-dialog agents without mutation. Under branching configuration, create a dialog, call `branch_dialog`, continue the original, switch to the child, continue it, and assert each Wiremock request contains the complete and distinct active history.

At CLI level, run this input against deterministic responses:

```text
shared question
/branch
original continuation
/switch 2
child continuation
/exit
```

Use a fresh temporary database, for which the original is ID 1 and the emitted
child is ID 2. Assert stdout reports the checkpoint and both IDs, replays child
history after switching, and neither command calls DeepSeek. Add wrong-strategy
and malformed-command cases that keep the loop alive.

- [ ] **Step 3: Run and verify RED**

Run: `cargo test --test chat --test agent --test cli branch`

Expected: parser assertions and branch operations fail because the commands are not implemented.

- [ ] **Step 4: Implement parser and agent operations**

Parse only exact reserved command shapes. `branch_dialog` and `switch_branch` first require `ContextStrategy::Branching`. Branching requires an owned `DialogStore` and active `dialog_id`. A successful switch replaces history, last usage, summary state, facts state, and dialog ID only after the store returns a fully validated target; API client, debug logger, and config stay unchanged.

- [ ] **Step 5: Integrate commands into the sequential CLI loop**

`/branch` prints `Checkpoint <count>: dialog #<original> remains active; created branch #<new>.` `/switch` prints `Switched to branch #<id>.` and replays that dialog's messages with existing role styles. Recoverable command errors use `BlockStyle::Error` and continue reading input.

- [ ] **Step 6: Verify GREEN**

Run: `cargo test --test chat --test agent --test cli`

Expected: PASS; received-request counts prove branch commands never invoke the API.

- [ ] **Step 7: Commit**

```bash
git add src/chat.rs src/agent.rs src/main.rs tests/chat.rs tests/agent.rs tests/cli.rs
git commit -m "Implement #10: Switch dialog branches"
```

### Task 8: Strategy-aware statistics, events, and debug metadata

**Files:**
- Modify: `src/context.rs`
- Modify: `src/agent.rs`
- Modify: `src/debug_log.rs`
- Modify: `src/terminal.rs`
- Modify: `src/main.rs`
- Modify: `tests/context.rs`
- Modify: `tests/debug_log.rs`
- Modify: `tests/agent.rs`
- Modify: `tests/cli.rs`

**Interfaces:**
- Extends: `ContextStats` with strategy, selected raw count, facts count/boundary/usage, and optional branch IDs.
- Extends: debug request metadata with strategy, system block names, selected count, summary boundary, and facts boundary.
- Renders: separate `Ответы`, `Summary`, and `Facts` usage lines plus known `API всего`.

- [ ] **Step 1: Write failing stats and logging tests**

Add a context assertion for sticky facts with two facts, boundary 5, and a three-message selected window. Add a terminal/CLI assertion containing:

```text
Стратегия · sticky_facts
Контекст · полная история: 8 · в запросе: 3 · facts: 2 · facts до: 5
Facts · вход: 10 · выход: 2 · всего: 12
```

For debug logging, prepare a request with blocks named `base` and `facts`; with payload logging disabled, assert the JSONL includes those names and counts but excludes fact values. With payload logging enabled, assert exact messages appear and the API key remains redacted.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test context --test debug_log --test agent --test cli`

Expected: assertions fail because stats and logs still use only Day 9 summary fields.

- [ ] **Step 3: Implement strategy-aware data and rendering**

Build `ContextStats` from the prepared request metadata, stored summary/facts states, accumulated assistant usage, and current branch metadata. Keep all counts explicit rather than overloading Day 9's `raw_message_count`. Sum known totals with saturating addition across ordinary, compaction, and facts usage. Show missing-usage counts on their own category.

Change `DebugLog::log_request` to accept a serializable metadata struct instead of a bare summary boundary. Never include `SystemBlock::content` outside the existing opt-in exact `messages` payload.

Wire facts events to short interactive stderr status messages and non-interactive failure warnings. Facts fragments are never printed as assistant output.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test context --test debug_log --test agent --test cli`

Expected: PASS with separate service accounting and no default payload leakage.

- [ ] **Step 5: Commit**

```bash
git add src/context.rs src/agent.rs src/debug_log.rs src/terminal.rs src/main.rs tests/context.rs tests/debug_log.rs tests/agent.rs tests/cli.rs
git commit -m "Implement #10: Report strategy context usage"
```

### Task 9: Day 10 documentation and final verification

**Files:**
- Modify: `README.md`
- Modify: `docs/DAYS.md`
- Create: `docs/day10-results.md`

**Interfaces:**
- Documents: configuration, system blocks, strategy behavior, commands, token accounting, and the deferred live comparison protocol.

- [ ] **Step 1: Update documentation**

Change the README banner to Day 10 and document all four strategy values with exact TOML examples. Explain that sliding/facts N includes the pending prompt, raw SQLite history is preserved, facts add one service call, facts failures recover on a later turn, and branching physically duplicates rows.

Document `/branch` and `/switch <id>` with a transcript showing the original remains active after branching. Add Day 10 to `docs/DAYS.md` and update the branch progression text.

Create `docs/day10-results.md` with the fixed 10–15-message requirements scenario, commands for four fresh database files, an objective requirements checklist, and a results table whose cells say `Pending live run` rather than inventing measurements.

- [ ] **Step 2: Verify documentation references**

Run: `rg -n 'Day 9|Day-9|enabled =|Pending live run|strategy =' README.md docs/DAYS.md docs/day10-results.md deepseek.example.toml`

Expected: remaining Day 9 references are clearly historical; no active Day 10 example uses removed `context.enabled`; all four strategy values and pending results are present.

- [ ] **Step 3: Run complete verification**

Run: `cargo fmt --check`

Expected: PASS.

Run: `cargo test`

Expected: PASS with no ignored or unexpectedly filtered tests.

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: PASS with no warnings.

Run: `git diff --check`

Expected: no output.

- [ ] **Step 4: Commit documentation**

```bash
git add README.md docs/DAYS.md docs/day10-results.md
git commit -m "Docs #10: Explain context strategies"
```

- [ ] **Step 5: Inspect final branch state**

Run: `git status --short && git log --oneline Day-9..HEAD`

Expected: clean status and one focused commit for each completed deliverable after the design and plan commits.
