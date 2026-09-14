# Day 10: Context Strategies Design

## Goal

Extend the Day 9 DeepSeek CLI with explicit context-management strategies and
select one strategy in `deepseek.toml`. Day 10 supports the existing cumulative
summary plus three new modes: a sliding window, sticky key-value facts, and
independent dialog branches. It also introduces a reusable system-context layer
whose blocks are kept separate from ordinary conversation messages and from
history reduction.

Real 10–15-message quality comparisons are intentionally deferred until the
user requests live scenario testing. This change provides deterministic tests,
token accounting, and a documented replay protocol so those runs can be made
later without changing the implementation.

## Scope and Compatibility

Work is implemented on branch `Day-10`, starting from `Day-9`.

Day 10 does not preserve configuration compatibility with Day 9. A
`[context]` section and an explicit `strategy` are required. Existing Day 9
SQLite databases remain structurally readable: old summary rows are retained,
but a strategy uses only the state that belongs to that strategy.

The four strategy values are:

- `summary`
- `sliding_window`
- `sticky_facts`
- `branching`

## Configuration

The context section accepts the complete set of strategy parameters so a user
can compare modes by changing only `strategy`:

```toml
[context]
strategy = "summary"
keep_last_messages = 10
compact_after_prompt_tokens = 6000
summary_max_tokens = 1024
facts_max_tokens = 512
```

Validation rules are:

- `strategy` is required and must be one of the four values above.
- `keep_last_messages` must be greater than zero. It is used by `summary`,
  `sliding_window`, and `sticky_facts`.
- `compact_after_prompt_tokens` and `summary_max_tokens` must be greater than
  zero. They are used only by `summary`.
- `facts_max_tokens` must be greater than zero. It is used only by
  `sticky_facts`.
- Parameters that do not apply to the selected strategy are accepted and
  ignored. This keeps strategy comparison to a one-line configuration change.
- Unknown fields remain errors.

The old `context.enabled` field is removed. To run the Day 9 behavior, select
`strategy = "summary"` explicitly.

## General System Context

System context is an independent concept, not part of any strategy. The
request builder receives an ordered `SystemContext` containing zero or more
named `SystemBlock` values. Each block becomes its own `role = "system"`
message before conversational messages.

Initial block types are:

1. the base system prompt from configuration;
2. the active cumulative summary, when the `summary` strategy has one; and
3. the current facts JSON, when the `sticky_facts` strategy has non-empty
   memory.

The abstraction must permit future blocks such as a user profile, project
rules, or runtime instructions without changing history-selection algorithms.
Blocks are composed from their authoritative state for each request. They are
never inserted into `ChatHistory` or the `messages` table.

History strategies operate only on ordinary `user` and `assistant` messages.
The summary compactor receives raw conversational messages and its own previous
summary state, not the general `SystemContext`. In particular, facts and future
system blocks can never be swallowed by summary compaction or removed by a
sliding window.

## Request Construction

All requests follow the same top-level order:

1. every non-empty `SystemContext` block in deterministic order;
2. conversational messages selected by the active strategy, in original
   order.

The pending user input counts as an ordinary conversational message when a
window is selected.

### Summary

The existing Day 9 algorithm remains unchanged. An ordinary request contains
the base system prompt, the active summary block if compatible, the raw tail,
and the pending user input. Compaction still occurs after a successful answer
whose provider-reported prompt token count reaches the configured threshold.

### Sliding Window

The API request contains the base system prompt and the newest
`keep_last_messages` ordinary messages, including the pending user input.
Older messages are excluded from the model context without a summarization
call. Full raw history remains in memory and SQLite for audit, statistics,
resumption, and later branching; “discard” means discard from the API context,
not destructive deletion of authoritative dialog data.

### Sticky Facts

The API request contains the base system prompt, a facts system block when the
map is non-empty, and the newest `keep_last_messages` ordinary messages,
including the pending user input.

Facts are a JSON object whose keys and values are strings. Keys should be
stable, descriptive identifiers such as `goal`, `constraint.deadline`,
`preference.answer_style`, or `decision.database`. Values state only
information grounded in user messages. The updater may replace obsolete
values, remove facts explicitly revoked by the user, and add newly established
facts. It must not turn unaccepted assistant suggestions into facts.

### Branching

The API request contains the base system prompt and the complete ordinary
history of the active branch. Summary and facts state that may exist in a
restored or cloned database row is ignored in this strategy.

## Facts Update Flow

In `sticky_facts`, each non-empty user input follows this sequence:

1. Persist the user message using the existing optimistic message-count guard.
2. Build a facts-update request from the previous facts JSON and every user
   message after the stored facts boundary, including the new message.
3. Call DeepSeek with temperature `0`, thinking disabled, no inherited stop
   sequences, and `facts_max_tokens` as the output limit.
4. Require a bare, valid JSON object with string keys and string values.
5. Atomically persist the replacement map, its new covered-message boundary,
   and the service-call usage.
6. Build the ordinary chat request from the newly committed facts and the
   configured window.
7. Stream and persist the ordinary answer through the existing flow.

For an in-memory agent, the same semantic ordering applies without SQLite.
Failed ordinary answers follow the existing rule: persistent input remains on
disk, while an in-memory agent commits only complete turns.

A facts API error, invalid JSON response, empty response, truncated stream, or
other service failure is non-fatal to the ordinary answer. The agent emits a
warning event, keeps the previous facts and boundary unchanged, and answers
using the previous facts plus the current raw window. On the next user turn,
the updater receives all user messages still beyond the unchanged boundary, so
the memory catches up after a transient failure.

A SQLite error while saving valid updated facts is fatal. Continuing would
make the process use memory that it failed to persist and could not restore.

## Facts Persistence

SQLite gains one current facts row per dialog:

```sql
CREATE TABLE IF NOT EXISTS dialog_facts (
    dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
    facts_json TEXT NOT NULL,
    covered_message_count INTEGER NOT NULL CHECK (covered_message_count > 0),
    update_count INTEGER NOT NULL DEFAULT 0,
    known_prompt_tokens INTEGER NOT NULL DEFAULT 0,
    known_completion_tokens INTEGER NOT NULL DEFAULT 0,
    known_total_tokens INTEGER NOT NULL DEFAULT 0,
    missing_usage_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
);
```

`covered_message_count` is an exclusive boundary in stable dialog message
order. It may include assistant rows, but only user rows after the prior
boundary are supplied as new evidence to the updater. A successful write
replaces the JSON, advances the boundary to the current full message count,
increments `update_count`, and accumulates known usage. Missing provider usage
increments `missing_usage_count` and is never treated as zero.

Loading a dialog validates that `facts_json` is an object containing only
string values and that the boundary does not exceed the stored message count.

## Branch Checkpoints and Persistence

Branching uses physical dialog copies, not shared-message references. This is
intentionally simple and transparent for the Day 10 exercise, at the cost of
duplicated rows.

SQLite gains branch metadata:

```sql
CREATE TABLE IF NOT EXISTS dialog_branches (
    dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
    branch_group_id INTEGER NOT NULL REFERENCES dialogs(id),
    parent_dialog_id INTEGER REFERENCES dialogs(id),
    checkpoint_message_count INTEGER NOT NULL CHECK (checkpoint_message_count >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
);
CREATE INDEX IF NOT EXISTS dialog_branches_by_group
    ON dialog_branches(branch_group_id, dialog_id);
```

`/branch` is available only when `strategy = "branching"` and the active
dialog has already been persisted. In one immediate transaction it:

1. verifies that the current database message count equals the agent's
   expected count;
2. registers the current dialog as the root of a branch group if it is not
   already registered;
3. creates a second dialog with the same system prompt and a visibly distinct
   title;
4. copies all current messages and their per-answer usage in stable order;
5. copies current summary and facts rows if they exist, so the result is a true
   dialog duplicate even though branching mode ignores them;
6. records the new dialog's parent, group, and exact checkpoint message count;
7. commits and returns the new dialog ID.

The original dialog remains active. Together, it and the returned copy are the
two branches from the checkpoint. Repeating `/branch` is allowed and creates
another independent child in the same group.

`/switch <id>` is available only in branching mode and only after `/branch`
has established a group. The target must belong to the active dialog's branch
group. A successful switch loads the target's original system prompt, complete
raw messages, usage, summary state, and facts state into the current agent
while retaining the process's API and strategy configuration.

Branch commands are processed only between user requests, so there is no
in-flight response to cancel. A missing dialog, unrelated dialog, stale source,
or failed transaction leaves the active agent and dialog unchanged. `/branch`
before the first persisted user message reports a recoverable command error.

## CLI and Events

Input parsing gains:

- `/branch`
- `/switch <positive-dialog-id>`

Malformed reserved commands such as `/switch`, `/switch 0`, or `/branch extra`
are command errors and are not sent to DeepSeek. Errors are printed as system
or error blocks and the input loop continues. Branching commands used under a
different strategy report that `strategy = "branching"` is required.

After `/branch`, the CLI prints the checkpoint size, active original ID, and
new branch ID. After `/switch`, it prints the newly active ID and replays that
branch's history using the existing terminal styles.

Agent events gain facts-update started, completed, and failed variants. Facts
content is not printed as an assistant response. Interactive status remains
brief, and redirected output remains stable.

## Statistics and Token Accounting

`/stats` reports the selected strategy and enough context information to
compare modes honestly:

- total persisted messages;
- ordinary messages selected for the next request at the current boundary;
- summary-covered and raw-tail counts in summary mode;
- number of facts and facts coverage boundary in sticky-facts mode;
- current dialog and branch-group information in branching mode;
- cumulative ordinary answer usage;
- cumulative summary compaction usage;
- cumulative facts-updater usage; and
- a known API total across all three categories.

Usage copied into a new branch represents the inherited cost of reaching its
checkpoint. Subsequent branch statistics diverge independently. Missing usage
is reported per category and never converted to zero.

The existing rolling footer remains the usage of only the latest ordinary
answer. Service calls do not overwrite it.

## Debug Logging

Prepared ordinary requests continue to use the existing opt-in JSONL debug
log. Records gain the active strategy, system-block names, selected raw count,
and applicable summary or facts boundaries. Exact content remains controlled
by `debug.log_payloads`.

Facts-update requests and their completion or failure are logged as separate
operations. API keys remain redacted. Branch creation and switching log IDs
and checkpoint counts but not message contents.

## Manual Comparison Protocol

The README defines a fixed 10–15-message requirements-gathering scenario and a
fresh database per strategy. A future live comparison should record:

- whether the final answer preserves all explicit requirements and decisions;
- whether repeated questions or contradictions appear;
- ordinary, summary, and facts service tokens separately;
- known total API tokens; and
- the user-visible advantages and friction of each mode.

Branching uses the same prefix up to `/branch`, then two intentionally
different continuations followed by one final synthesis request in each
branch. Sliding, summary, and sticky-facts runs replay the same linear prompt
sequence.

No subjective quality result is fabricated from mocked responses. The result
table remains marked as pending until the user requests real dialog runs.

## Test Strategy

Tests use Wiremock and temporary SQLite databases; they never call the real
DeepSeek API.

- Configuration tests cover the required strategy, all four values, removed
  `enabled`, positive limits, ignored irrelevant parameters, and unknown
  fields.
- System-context tests prove ordered blocks precede history and are unaffected
  by windowing or summary planning.
- Context tests assert exact request payloads for all four modes, including
  windows shorter than, equal to, and longer than N.
- Facts tests cover initial extraction, replacement and deletion, strict JSON
  validation, persistence, restoration, cumulative usage, missing usage,
  transient failure recovery, and fatal persistence errors.
- Branch-store tests cover atomic copying of messages and usage, context-state
  copies, lineage metadata, optimistic conflicts, rollback, and independent
  continuation.
- Agent tests prove facts update happens before ordinary chat, failures use old
  facts plus the current window, summary behavior is retained, and branching
  uses full active history.
- CLI tests cover `/branch`, `/switch`, malformed commands, wrong-strategy
  errors, replay after switching, and stable redirected output.
- Existing Day 9 tests are updated only where the intentional configuration or
  statistics contract changed.
- Final verification runs `cargo test`, `cargo fmt --check`, and
  `cargo clippy --all-targets --all-features -- -D warnings`.

## Non-Goals

- No message DAG, copy-on-write storage, or deletion of duplicate branch rows.
- No vector database, embeddings, semantic retrieval, or free-form long-term
  memory.
- No user command for manually editing facts in Day 10.
- No branching while an API response is in flight.
- No automatic multi-strategy live benchmark in this implementation phase.
