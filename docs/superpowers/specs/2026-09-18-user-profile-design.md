# Day 12 User Profile Design

## Goal

Add one durable, free-form Markdown profile per user and inject it into every
persistent request automatically. A profile describes soft preferences such as
communication style, preferred tools, and usual engineering approaches without
forcing a predefined schema.

## Product semantics

A profile is addressed only by `user_id`. It applies across every task and
dialog owned by that user.

Examples of valid profile content:

```markdown
# Preferences

Communicate briefly and directly.
For mobile applications, prefer Android, Kotlin, and Jetpack Compose.
Prefer simple architecture over speculative abstractions.
```

Profile text is a soft preference layer:

1. The current explicit user request wins over the profile.
2. Task-specific working memory wins when it conflicts with the profile.
3. The profile provides defaults when the current task is silent.
4. Hard, non-overridable rules remain outside Day 12 and belong to the future
   invariant layer.

The assistant never infers or saves a profile from ordinary conversation.
Profile replacement and deletion are explicit local operations.

## Scope and lifetime

The Day 11 layers remain unchanged:

```text
conversation = dialog ID
working      = user ID + task ID
long-term    = user ID
profile      = user ID
```

The profile is durable and user-scoped like long-term memory, but it is a
separate semantic provider rather than a special memory key. Generic long-term
memory stores facts; the profile stores free-form instructions and preferences.

There is exactly one active profile per `user_id`. Multiple named profiles,
task-specific profiles, automatic extraction, profile history, and partial
Markdown editing are not part of Day 12.

## Persistence

SQLite remains the source of truth. `DialogStore::open` creates this table for
new and existing databases:

```sql
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id TEXT PRIMARY KEY,
    content_markdown TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
);
```

Repository writes trim `user_id` and reject an empty result. They trim only
leading and trailing whitespace from the complete Markdown document, preserve
all internal formatting, and reject an empty result. Replacement is atomic.
Deletion reports whether a row existed.

The domain module exposes:

```rust
pub struct UserProfile {
    user_id: String,
    content_markdown: String,
    updated_at: String,
}

pub trait ProfileRepository {
    type Error;

    fn load_profile(&self, user_id: &str) -> Result<Option<UserProfile>, Self::Error>;
    fn replace_profile(&mut self, user_id: &str, markdown: &str)
        -> Result<(), Self::Error>;
    fn delete_profile(&mut self, user_id: &str) -> Result<bool, Self::Error>;
}
```

## Context provider

`UserProfile` implements the existing `ContextProvider`. It accepts every
`RequestScope` with the same normalized `user_id`, regardless of task or dialog.
A mismatch returns a typed context error.

The provider emits one block:

```text
name        = user_profile
scope       = user
compaction  = exclude
```

The block uses a fixed preamble followed by the stored Markdown verbatim:

```text
User profile preferences. Apply them when relevant. They are soft defaults,
not hard constraints. A current explicit request and task-specific context
override conflicting profile preferences.

<stored Markdown>
```

The block is excluded directly from summary compaction. As with Day 11 durable
memory, profile information repeated in ordinary conversation can still enter a
conversation summary.

## Request assembly

For persistent agents, profile loading occurs after the current input is saved
and before memory loading, sticky-facts updating, and the model request:

```text
persist current input
→ load user profile
→ load long-term and working memory
→ update sticky facts when enabled
→ assemble context
→ call the model
```

The deterministic prompt order is:

```text
base
→ user_profile
→ user_memory
→ task_memory
→ conversation summary/facts
→ selected ordinary messages
```

An absent profile emits no empty block. In-memory agents emit no profile and
reject profile mutation/query operations with the same store-required pattern
as durable memory.

If profile loading fails after a persistent input was saved, the Agent mirrors
that input into its in-process history exactly once and returns before memory,
facts, or model HTTP calls. The next request can recover after the store is
available again.

## Commands

Day 12 adds four explicit local commands:

```text
/profile
/profile set <markdown>
/profile import <path>
/profile clear
```

- `/profile` shows the selected user's complete stored profile or
  `Profile · empty`.
- `/profile set <markdown>` atomically replaces the profile with the rest of
  the input line. It is convenient for short preferences.
- `/profile import <path>` reads a UTF-8 Markdown file and atomically replaces
  the profile. The rest of the command line is the path, so paths may contain
  spaces. Shell expansion is not performed.
- `/profile clear` deletes the selected user's profile and reports whether one
  existed.

Malformed commands are local usage errors. Import read/UTF-8 errors are printed
as local command errors, leave the old profile unchanged, and keep the REPL
running. Profile commands do not create a dialog or call any model endpoint.

## Terminal and privacy

Profile output uses the existing system-block terminal style and identifies the
active user. The full Markdown is printed only in response to `/profile`.

With `debug.log_payloads = false`, request logs expose only block metadata and
sizes; profile content is absent. With payload logging enabled, the profile is
present in model request payloads and may contain sensitive information.

## Verification

Automated coverage must prove:

- repository replacement, persistence, user isolation, normalization, blank
  rejection, and deletion;
- the `user_profile` block has user scope, is excluded from compaction, and
  rejects a different user scope;
- the same prompt under two user IDs receives different profile blocks;
- profile changes affect the next request automatically across tasks/dialogs;
- the order is `user_profile`, `user_memory`, `task_memory`, then conversation
  blocks;
- local commands make no model calls and no dialog rows;
- import preserves multiline Markdown and failure preserves the old profile;
- default debug logs omit profile content;
- all Day 11 tests and context strategies still pass.

The Day 12 demonstration uses two profiles:

- Alice prefers concise Android/Kotlin/Compose solutions.
- Bob prefers explanatory Flutter/Dart solutions.

Both users receive the same application-design prompt. Captured request payloads
must contain only the addressed profile, and the real-model video demonstrates
the resulting personalized answers.

## Documentation deliverables

- Update `README.md` with commands, precedence, and privacy behavior.
- Update `docs/DAYS.md` with Day 12.
- Add `docs/day12-results.md` containing a reproducible two-profile scenario,
  SQLite queries, debug checks, and a concise video recording script.

## Non-goals

- Automatic profile extraction from conversation.
- Multiple named profiles per user.
- A structured taxonomy of communication or engineering fields.
- Hard invariants or refusal behavior.
- Task lifecycle state or controlled transitions.
- Profile version history or partial editing.
