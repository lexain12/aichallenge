# Day 9: Context Compression Design

## Goal

Add configurable conversation-history compression to the existing DeepSeek CLI.
The agent must keep the complete original dialog in SQLite, create and persist
separate summaries, send only the active summary plus the most recent raw
messages to DeepSeek, and expose enough metrics and diagnostics to compare a
compressed run with an uncompressed run manually.

## Scope

Day 9 extends the current single-agent interactive CLI on branch `Day-9`.
It does not add an automated answer-quality benchmark or run two agents in
parallel. The user will compare quality manually by replaying the same scenario
with compression disabled and enabled.

## Configuration

`deepseek.toml` remains the single application configuration file. It gains two
validated sections:

```toml
[context]
enabled = true
compact_after_prompt_tokens = 6000
keep_last_messages = 10
summary_max_tokens = 1024

[debug]
# log_path = "deepseek-debug.jsonl"
log_payloads = false
```

Defaults are the values above except that `debug.log_path` is absent by
default. Validation rules are:

- `compact_after_prompt_tokens`, `keep_last_messages`, and
  `summary_max_tokens` must be greater than zero.
- `debug.log_payloads = true` is allowed without a log path but has no effect.
- A relative log path is resolved from the process working directory.
- Unknown fields remain configuration errors.

When compression is disabled, the agent sends the complete original history,
does not create new summaries, and ignores stored summaries for request
construction. Stored summaries and their usage remain available to `/stats`.

## Compression Trigger

The trigger uses DeepSeek's provider-reported `prompt_tokens` from a successful
ordinary chat request. No local tokenizer or characters-per-token estimate is
introduced.

After a complete assistant answer is saved, the agent starts compaction when
all of these conditions hold:

1. compression is enabled;
2. the ordinary request returned usage;
3. `prompt_tokens >= compact_after_prompt_tokens`;
4. the full history contains messages older than the newest
   `keep_last_messages` messages; and
5. that eligible prefix extends beyond the boundary of the active summary.

This means the request that crosses the limit is completed normally. Its
summary is prepared immediately afterward and is used by the next user
request. If DeepSeek omits usage, the agent cannot truthfully evaluate the
token threshold and does not compact on that turn.

## Summary Construction

The full history is split at
`message_count - keep_last_messages`. The prefix is summarized, while the tail
remains byte-for-byte unchanged.

The first compaction request contains a dedicated summarizer system prompt and
the eligible raw prefix. A later compaction request contains the previous
summary plus only the raw messages added to the eligible prefix since that
summary. This avoids repeatedly sending all old raw messages to the summarizer.

The summarizer prompt instructs DeepSeek to preserve facts, names, decisions,
constraints, user preferences, unresolved questions, and exact technical
identifiers; to distinguish user statements from assistant suggestions; and
not to invent missing information. The summary is plain text intended as
context, not as a user-visible assistant answer.

Summary calls use the configured model and connection, force temperature to
`0`, disable thinking, stream internally without printing summary fragments,
and use `context.summary_max_tokens` as their output limit. Existing `top_p`
and `stop` settings do not apply to the summarizer because they can truncate or
distort the summary contract.

An empty, truncated, incomplete, or otherwise failed summary never becomes
active. The already saved user answer remains successful, the full history is
preserved, an interactive warning and debug event are emitted, and the agent
can retry after a later qualifying response.

## Request Construction

With no active compatible summary, an ordinary request remains:

1. original system prompt, if non-blank;
2. all raw committed messages;
3. pending user message.

With an active summary, it becomes:

1. original system prompt, if non-blank;
2. a separate system message clearly labelled as the summary of earlier
   conversation;
3. all raw messages after the summary boundary, in original order;
4. pending user message.

The complete original messages remain in memory and SQLite even though the
covered prefix is omitted from the API request.

A stored summary is compatible only when its covered-message count is no
greater than `full_message_count - keep_last_messages`. This matters if the
configuration is changed before `--resume`: an incompatible summary is not
used, so increasing `keep_last_messages` cannot silently hide messages that
should now be sent raw. The next qualifying compaction may create a replacement
summary directly from the preserved full history.

## Persistence

SQLite gains a `context_compactions` table without altering or deleting rows in
`dialogs`, `messages`, or `message_usage`:

```sql
CREATE TABLE IF NOT EXISTS context_compactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
    summary TEXT NOT NULL,
    covered_message_count INTEGER NOT NULL CHECK (covered_message_count > 0),
    usage_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
);

CREATE INDEX IF NOT EXISTS context_compactions_by_dialog
ON context_compactions(dialog_id, id);
```

Every successful compaction creates a new audit row. Old summaries remain in
the database; the newest compatible row is active. `covered_message_count` is
an exclusive prefix length in the stable message order, not a mutable database
row ID. The summary and its provider usage commit atomically. Missing usage is
stored as `NULL`, never as zero.

Restoring a dialog loads all original messages plus its compaction history.
`/clear` starts a new dialog context while leaving the previous dialog's raw
messages and summaries on disk.

## Token Accounting

Ordinary chat usage and compaction usage are separate categories because a
summary call is overhead rather than part of the user's answer. `/stats`
reports provider values accumulated for the current persisted dialog:

```text
Контекст · полная история: 24 · покрыто summary: 14 · дословно: 10
Ответы   · вход: 12000 · выход: 2000 · всего: 14000
Сжатие   · вход: 5000  · выход: 600  · всего: 5600
API всего: 19600
```

`API всего` is the sum of known ordinary and compaction totals. Each category
also reports how many completed calls lacked provider usage; unknown usage is
not treated as zero. Failed API calls that never deliver final provider usage
cannot be measured and are identified as such in the debug log.

The existing rolling footer continues to show only the latest ordinary user
request. Its `prompt_tokens` therefore visibly falls after compression. It is
not replaced with a cumulative value.

When no persisted dialog exists, the same counters are maintained in memory
for `Agent::new` so the reusable agent API has consistent behavior.

## Live Events and Debug Log

The agent layer exposes events independently of the low-level HTTP stream:

- ordinary response text;
- ordinary request usage;
- compaction started, including threshold, raw count, covered count, and kept
  count;
- compaction completed, including its usage and new boundary;
- compaction failed, including a sanitized error.

The interactive terminal shows short system-status messages for compaction
start, success, and failure. It never prints generated summary text as an
assistant response. Redirected output remains stable and does not show animated
status updates.

When `debug.log_path` is configured, the application writes append-only JSONL
events for request preparation, ordinary completion, compaction decisions,
compaction completion, and failures. By default request records contain roles,
message counts, character lengths, summary boundary, and usage but not message
contents. With `debug.log_payloads = true`, they additionally contain the exact
DeepSeek `messages` payload. Authorization headers and the API key are never
logged.

A debug-log write failure emits one warning, disables further file logging for
that process, and does not fail or interrupt the dialog.

## Commands and Manual Comparison

`/stats` prints the context and cumulative token report without calling
DeepSeek. Existing `/clear`, `/exit`, and `/quit` behavior remains unchanged.

The README documents a reproducible manual comparison:

1. choose a fixed sequence of prompts;
2. run it with `[context] enabled = false` and a fresh database;
3. run it again with compression enabled and another fresh database;
4. capture `/stats` from each run;
5. compare answer quality manually and compare ordinary input tokens, summary
   overhead, and total API tokens separately.

For a short demonstration, the documentation may use a deliberately low
threshold. Production-like runs should choose a threshold large enough that
the retained raw tail and generated summary fit comfortably below it.

## Error and Consistency Rules

- Raw user input is still persisted before the ordinary API request.
- A complete assistant answer and its usage are still persisted atomically.
- Summary persistence cannot alter the raw message count or latest-message
  concurrency guard.
- A database error while writing a successful summary is fatal, matching the
  existing rule that the application must not silently continue with state it
  failed to persist.
- An API failure while generating a summary is non-fatal because it does not
  invalidate the already persisted raw dialog.
- A stale concurrent session cannot activate a summary for a different raw
  history length; the store validates the expected message count before insert.
- API keys are redacted from all displayed and logged failures.

## Test Strategy

Tests use Wiremock and temporary SQLite databases; they do not call the real
DeepSeek service.

- Configuration tests cover defaults, overrides, zero values, unknown fields,
  and debug settings.
- Context unit tests cover prefix/tail selection, previous-summary merging,
  disabled mode, exact request order, compatibility after changing `N`, and
  absence of eligible messages.
- Agent tests prove the trigger uses provider `prompt_tokens`, the crossing
  answer completes before compaction, summary output is hidden, the next request
  contains summary plus the exact raw tail, and summary failures preserve the
  full-history path.
- Persistence tests upgrade a Day-8 database, retain every raw message, append
  multiple summary rows, restore the latest compatible row, keep usage nullable,
  and reject stale summary writes.
- Metrics tests separate ordinary and summary usage, include both in the grand
  total, and expose missing-usage counts.
- Logging tests prove payloads are opt-in and API keys never appear.
- CLI tests cover `/stats`, live compaction notices, resume, clear, and stable
  redirected output.
- Final verification runs the complete test suite, Clippy with warnings denied,
  and rustfmt check.
