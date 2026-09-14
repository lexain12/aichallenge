# Context Compression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a DeepSeek agent that keeps complete raw dialogs, replaces old request history with one cumulative summary, and reports the resulting token cost accurately.

**Architecture:** Keep raw messages authoritative in `ChatHistory` and SQLite. A focused `context` module builds ordinary and compaction requests and owns token aggregates; `Agent` orchestrates the extra DeepSeek call and atomically swaps the persisted summary only after success. Optional JSONL diagnostics observe the exact request composition without coupling logging to the HTTP client.

**Tech Stack:** Rust 2024, Tokio, Reqwest SSE, Serde/TOML/JSON, Rusqlite, Clap, Wiremock.

**Spec:** `docs/superpowers/specs/2026-09-14-context-compression-design.md`

## Global Constraints

- Keep every original user and assistant message in SQLite; never rewrite or delete raw history during compaction.
- Store at most one active summary per dialog and retain only cumulative compaction metrics in SQLite.
- Trigger compaction only from provider-reported `prompt_tokens`; missing usage is unknown, not zero.
- Preserve the newest configured number of messages byte-for-byte in ordinary requests.
- Feed the previous summary plus newly eligible raw messages into every later compaction.
- Count ordinary and compaction usage separately and include both in the known grand total.
- Never log the API key; full message payload logging is opt-in.
- A summary API failure is non-fatal, while a summary persistence failure remains fatal.
- Existing Day-8 dialog, streaming, usage-footer, and resume behavior must remain supported.

---

### Task 1: Global context and debug configuration

**Files:**
- Modify: `src/config.rs:9-225`
- Modify: `tests/config.rs`
- Modify: `deepseek.example.toml`

**Interfaces:**
- Produces: `ContextConfig`, `DebugConfig`, `Config::context() -> &ContextConfig`, and `Config::debug() -> &DebugConfig`.
- Defaults: enabled `true`, threshold `6000`, raw tail `10`, summary limit `1024`, no log path, payload logging `false`.

- [ ] **Step 1: Write failing configuration tests**

Add literal consumer-facing assertions to `tests/config.rs`:

```rust
#[test]
fn applies_context_and_debug_defaults() {
    let config = Config::from_toml("api_key = \"key\"", None).unwrap();

    assert!(config.context().enabled());
    assert_eq!(config.context().compact_after_prompt_tokens(), 6000);
    assert_eq!(config.context().keep_last_messages(), 10);
    assert_eq!(config.context().summary_max_tokens(), 1024);
    assert_eq!(config.debug().log_path(), None);
    assert!(!config.debug().log_payloads());
}

#[test]
fn reads_context_and_debug_overrides() {
    let config = Config::from_toml(
        r#"
api_key = "key"

[context]
enabled = false
compact_after_prompt_tokens = 321
keep_last_messages = 4
summary_max_tokens = 77

[debug]
log_path = "logs/context.jsonl"
log_payloads = true
"#,
        None,
    )
    .unwrap();

    assert!(!config.context().enabled());
    assert_eq!(config.context().compact_after_prompt_tokens(), 321);
    assert_eq!(config.context().keep_last_messages(), 4);
    assert_eq!(config.context().summary_max_tokens(), 77);
    assert_eq!(config.debug().log_path(), Some(Path::new("logs/context.jsonl")));
    assert!(config.debug().log_payloads());
}

#[test]
fn rejects_zero_context_limits_and_unknown_nested_fields() {
    for field in [
        "compact_after_prompt_tokens",
        "keep_last_messages",
        "summary_max_tokens",
    ] {
        let text = format!("api_key = \"key\"\n[context]\n{field} = 0");
        let error = Config::from_toml(&text, None).unwrap_err().to_string();
        assert!(error.contains(field), "unexpected error: {error}");
    }

    let error = Config::from_toml(
        "api_key = \"key\"\n[context]\nunknown = 1",
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("parse"));
}
```

The production mutations caught are: a wrong default, ignored override, acceptance of zero, and accidental acceptance of misspelled nested settings.

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
cargo test --test config context -- --nocapture
```

Expected: compilation fails because the context/debug accessors do not exist.

- [ ] **Step 3: Add validated nested configuration types**

Add raw nested structs with `#[serde(default, deny_unknown_fields)]`, then validated public types:

```rust
const DEFAULT_COMPACT_AFTER_PROMPT_TOKENS: u64 = 6000;
const DEFAULT_KEEP_LAST_MESSAGES: usize = 10;
const DEFAULT_SUMMARY_MAX_TOKENS: u32 = 1024;

#[derive(Clone, Debug)]
pub struct ContextConfig {
    enabled: bool,
    compact_after_prompt_tokens: u64,
    keep_last_messages: usize,
    summary_max_tokens: u32,
}

#[derive(Clone, Debug)]
pub struct DebugConfig {
    log_path: Option<PathBuf>,
    log_payloads: bool,
}
```

Add `context: RawContextConfig` and `debug: RawDebugConfig` to `RawConfig`, validate all three non-zero values in `Config::from_raw`, store both validated sections on `Config`, and expose the getters used by the tests. Include both sections in the redacted `Debug` implementation; neither contains credentials.

- [ ] **Step 4: Document the settings in the example config**

Append this exact runnable block to `deepseek.example.toml`:

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

- [ ] **Step 5: Run configuration tests and verify GREEN**

Run `cargo test --test config -- --nocapture` and expect every configuration test to pass.

- [ ] **Step 6: Commit**

```bash
git add src/config.rs tests/config.rs deepseek.example.toml
git commit -m "Implement #9: Add context compression configuration"
```

---

### Task 2: Pure context selection and token accounting

**Files:**
- Create: `src/context.rs`
- Create: `tests/context.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `ChatHistory`, `Message`, `Role`, `TokenUsage`, and `ContextConfig`.
- Produces: `ContextSummary`, `ContextState`, `UsageTotals`, `ContextStats`, `CompactionPlan`, `build_request_messages`, and `plan_compaction`.

- [ ] **Step 1: Write failing request-selection tests**

Create `tests/context.rs` with a helper that commits literal turns, then assert the public behavior:

```rust
use deepseek_cli::chat::{ChatHistory, Role};
use deepseek_cli::context::{
    ContextState, ContextSummary, build_request_messages, plan_compaction,
};

fn history() -> ChatHistory {
    let mut history = ChatHistory::new("Original system".into());
    history.commit_turn("u1".into(), "a1".into());
    history.commit_turn("u2".into(), "a2".into());
    history.commit_turn("u3".into(), "a3".into());
    history
}

#[test]
fn ordinary_request_replaces_covered_prefix_with_summary() {
    let state = ContextState::with_summary(ContextSummary::new("old facts", 2));
    let request = build_request_messages(&history(), &state, true, 2, "next");
    let actual: Vec<_> = request
        .iter()
        .map(|message| (message.role(), message.content()))
        .collect();

    assert_eq!(
        actual,
        vec![
            (Role::System, "Original system"),
            (Role::System, "Summary of earlier conversation:\nold facts"),
            (Role::User, "u2"),
            (Role::Assistant, "a2"),
            (Role::User, "u3"),
            (Role::Assistant, "a3"),
            (Role::User, "next"),
        ]
    );
}

#[test]
fn disabled_or_incompatible_summary_sends_full_history() {
    let state = ContextState::with_summary(ContextSummary::new("old facts", 5));
    for enabled in [false, true] {
        let keep = if enabled { 4 } else { 1 };
        let request = build_request_messages(&history(), &state, enabled, keep, "next");
        assert_eq!(request.len(), 8);
        assert_eq!(request[1].content(), "u1");
        assert!(!request.iter().any(|message| message.content().contains("old facts")));
    }
}

#[test]
fn later_compaction_uses_previous_summary_and_only_newly_eligible_messages() {
    let state = ContextState::with_summary(ContextSummary::new("old facts", 2));
    let plan = plan_compaction(&history(), &state, 2).unwrap();

    assert_eq!(plan.covered_message_count(), 4);
    assert_eq!(plan.new_message_count(), 2);
    assert!(plan.request_messages()[1].content().contains("old facts"));
    assert!(plan.request_messages()[1].content().contains("user: u2"));
    assert!(plan.request_messages()[1].content().contains("assistant: a2"));
    assert!(!plan.request_messages()[1].content().contains("u1"));
    assert!(!plan.request_messages()[1].content().contains("u3"));
}
```

These tests fail if covered raw messages leak into ordinary requests, if a changed tail size hides raw messages, or if later compactions resend the entire old prefix.

- [ ] **Step 2: Write failing aggregate tests**

Add tests that use hand-calculated totals:

```rust
use deepseek_cli::client::TokenUsage;
use deepseek_cli::context::UsageTotals;

#[test]
fn usage_totals_keep_known_values_and_count_unknown_calls() {
    let mut totals = UsageTotals::default();
    totals.record(Some(TokenUsage {
        prompt_tokens: 10,
        completion_tokens: 3,
        total_tokens: 13,
        completion_tokens_details: None,
    }));
    totals.record(None);

    assert_eq!(totals.call_count(), 2);
    assert_eq!(totals.prompt_tokens(), 10);
    assert_eq!(totals.completion_tokens(), 3);
    assert_eq!(totals.total_tokens(), 13);
    assert_eq!(totals.missing_usage_count(), 1);
}
```

- [ ] **Step 3: Run the context test and verify RED**

Run `cargo test --test context -- --nocapture` and expect compilation to fail because `deepseek_cli::context` is absent.

- [ ] **Step 4: Implement the pure context domain**

Create `src/context.rs` with these public shapes:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextSummary {
    content: String,
    covered_message_count: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UsageTotals {
    call_count: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    missing_usage_count: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContextState {
    summary: Option<ContextSummary>,
    compaction_usage: UsageTotals,
}

pub struct CompactionPlan {
    request_messages: Vec<Message>,
    covered_message_count: usize,
    new_message_count: usize,
}
```

Implement `ContextSummary::new`, getters, `ContextState::with_summary`, `ContextState::replace_summary`, `UsageTotals::record`, saturating aggregate addition, and getters. Add crate-visible restoration constructors so persistence never writes private fields directly:

```rust
impl UsageTotals {
    pub(crate) fn from_parts(
        call_count: u64,
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
        missing_usage_count: u64,
    ) -> Self;
}

impl ContextState {
    pub(crate) fn restored(
        summary: Option<ContextSummary>,
        compaction_usage: UsageTotals,
    ) -> Self;
}
```

Use this compatibility predicate everywhere:

```rust
summary.covered_message_count()
    <= history.messages().len().saturating_sub(keep_last_messages)
```

`build_request_messages` must use the compatible boundary or zero and prepend exactly one labelled summary system message. `plan_compaction` computes `target = message_count - keep_last_messages`; it returns `None` when `target == 0` or `target <= compatible_previous_boundary`.

The compaction request contains exactly two API messages: the constant summarizer system prompt and one user message. Format the user message with `Previous summary:` and `New messages:` sections, role labels, and only `messages[previous_boundary..target]`. If the stored summary is incompatible, use boundary zero and omit the previous-summary section.

Add `pub mod context;` to `src/lib.rs`.

- [ ] **Step 5: Add stats derived from authoritative state**

Add:

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContextStats {
    pub full_message_count: usize,
    pub covered_message_count: usize,
    pub raw_message_count: usize,
    pub ordinary_usage: UsageTotals,
    pub compaction_usage: UsageTotals,
}

pub fn stats(
    history: &ChatHistory,
    state: &ContextState,
    enabled: bool,
    keep_last_messages: usize,
) -> ContextStats;
```

Derive ordinary totals from assistant messages and their stored usage. Count each completed assistant message with `None` usage as one unknown ordinary call. Compute `covered_message_count` from the active compatible summary only; when compression is disabled it is zero while persisted compaction totals remain visible.

- [ ] **Step 6: Run tests and verify GREEN**

Run `cargo test --test context -- --nocapture`, then `cargo test --test chat -- --nocapture`. Expect both binaries to pass.

- [ ] **Step 7: Commit**

```bash
git add src/context.rs src/lib.rs tests/context.rs
git commit -m "Implement #9: Build compressed context requests"
```

---

### Task 3: Dedicated DeepSeek summary call

**Files:**
- Modify: `src/client.rs:15-225`
- Modify: `tests/client.rs`

**Interfaces:**
- Consumes: compaction request messages and `summary_max_tokens`.
- Produces: `SummaryResult { answer: String, usage: Option<TokenUsage> }` and `DeepSeekClient::summarize`.

- [ ] **Step 1: Write the failing HTTP-boundary test**

Configure the existing test client with `top_p` and `stop`, then add:

```rust
#[tokio::test]
async fn summary_call_uses_deterministic_isolated_options_and_returns_usage() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .and(body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": "Summarize faithfully."},
                {"role": "user", "content": "user: one\nassistant: two"}
            ],
            "temperature": 0.0,
            "max_tokens": 64,
            "stream": true,
            "stream_options": {"include_usage": true},
            "thinking": {"type": "disabled"}
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"summary\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":2,\"total_tokens\":10}}\n\ndata: [DONE]\n\n",
                ),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DeepSeekClient::new(&config_for(&server, "test-key")).unwrap();
    let messages = vec![
        Message::for_request(Role::System, "Summarize faithfully."),
        Message::for_request(Role::User, "user: one\nassistant: two"),
    ];
    let result = client.summarize(&messages, 64).await.unwrap();

    assert_eq!(result.answer(), "summary");
    assert_eq!(result.usage().unwrap().total_tokens, 10);
}
```

Expose a public request-only constructor on `Message` if integration tests cannot create the two literal messages:

```rust
pub fn for_request(role: Role, content: impl Into<String>) -> Self
```

The test fails if summary calls inherit ordinary temperature, thinking, top-p, stop, or output limit.

- [ ] **Step 2: Run the test and verify RED**

Run `cargo test --test client summary_call -- --nocapture`. Expected: compilation fails because `summarize`, `SummaryResult`, and `Message::for_request` do not exist.

- [ ] **Step 3: Refactor the client around private request options**

Keep the public ordinary methods unchanged. Extract the existing HTTP/SSE implementation into a private method accepting:

```rust
struct RequestOptions<'a> {
    temperature: f64,
    max_tokens: u32,
    thinking: Option<Thinking<'a>>,
    top_p: Option<f64>,
    stop: &'a [String],
}
```

Ordinary calls populate it from the current client fields. Summary calls use temperature `0.0`, the supplied max tokens, `thinking = disabled`, `top_p = None`, and an empty stop slice. Both paths keep `stream_options.include_usage` controlled by the global `include_usage` flag.

Implement:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SummaryResult {
    answer: String,
    usage: Option<TokenUsage>,
}

pub async fn summarize(
    &self,
    messages: &[Message],
    max_tokens: u32,
) -> Result<SummaryResult, ClientError>;
```

Capture the latest usage event without forwarding summary text. Reject blank summary text with `ClientError::EmptyAnswer`; preserve existing truncated and incomplete-stream errors.

- [ ] **Step 4: Run client tests and verify GREEN**

Run `cargo test --test client -- --nocapture`. Expect all ordinary streaming tests and the new summary request test to pass.

- [ ] **Step 5: Commit**

```bash
git add src/chat.rs src/client.rs tests/client.rs
git commit -m "Implement #9: Add deterministic summary API call"
```

---

### Task 4: Persist one current summary and cumulative compaction usage

**Files:**
- Modify: `src/dialog.rs:10-225`
- Modify: `tests/dialog.rs`

**Interfaces:**
- Consumes: `ContextSummary`, `ContextState`, `UsageTotals`, expected raw message count, and optional summary `TokenUsage`.
- Produces: `StoredDialog.context`, plus `DialogStore::replace_context(...) -> Result<ContextState, StoreError>`.

- [ ] **Step 1: Write the failing migration and replacement test**

Add a test helper that creates only the Day-8 schema and four raw messages:

```rust
fn create_day8_database_with_four_messages(path: &Path) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE dialogs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                system_prompt TEXT NOT NULL,
                title TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT '',
                last_message_id INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE message_usage (
                message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                usage_json TEXT NOT NULL
            );
            INSERT INTO dialogs (id, system_prompt, title, last_message_id)
                VALUES (1, 'System', 'u1', 4);
            INSERT INTO messages (dialog_id, role, content) VALUES
                (1, 'user', 'u1'),
                (1, 'assistant', 'a1'),
                (1, 'user', 'u2'),
                (1, 'assistant', 'a2');",
        )
        .unwrap();
}
```

Then open the store and perform two replacements:

```rust
#[test]
fn upgrades_day8_database_and_replaces_one_summary_while_accumulating_usage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dialogs.sqlite3");
    create_day8_database_with_four_messages(&path);

    let mut store = DialogStore::open(&path).unwrap();
    let first = store
        .replace_context(
            1,
            4,
            ContextSummary::new("first", 2),
            Some(TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 2,
                total_tokens: 12,
                completion_tokens_details: None,
            }),
        )
        .unwrap();
    assert_eq!(first.compaction_usage().call_count(), 1);

    let second = store
        .replace_context(1, 4, ContextSummary::new("second", 3), None)
        .unwrap();
    assert_eq!(second.summary().unwrap().content(), "second");
    assert_eq!(second.summary().unwrap().covered_message_count(), 3);
    assert_eq!(second.compaction_usage().call_count(), 2);
    assert_eq!(second.compaction_usage().total_tokens(), 12);
    assert_eq!(second.compaction_usage().missing_usage_count(), 1);

    let connection = rusqlite::Connection::open(&path).unwrap();
    let rows: i64 = connection
        .query_row("SELECT count(*) FROM dialog_context", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(store.load(1).unwrap().context, second);
    assert_eq!(store.load(1).unwrap().messages.len(), 4);
}
```

Add a second test proving `expected_message_count = 3` returns `StoreError::Conflict(1)` and leaves the previous summary and metrics unchanged.

- [ ] **Step 2: Run persistence tests and verify RED**

Run `cargo test --test dialog context -- --nocapture`. Expected: compilation fails because context persistence is absent.

- [ ] **Step 3: Add the additive schema migration and loader**

Extend `DialogStore::open` with the exact `dialog_context` schema from the spec. Extend `StoredDialog`:

```rust
pub struct StoredDialog {
    pub id: i64,
    pub system_prompt: String,
    pub messages: Vec<Message>,
    pub context: ContextState,
}
```

Load the optional context row in the same unchecked read transaction as dialog metadata and raw messages. Validate non-negative SQLite values before converting them to `usize` or `u64`; represent corrupt negative or overflowing values with a specific `StoreError::InvalidContext` variant.

- [ ] **Step 4: Implement atomic replacement**

Add:

```rust
pub fn replace_context(
    &mut self,
    id: i64,
    expected_message_count: usize,
    summary: ContextSummary,
    usage: Option<TokenUsage>,
) -> Result<ContextState, StoreError>;
```

Within one `TransactionBehavior::Immediate` transaction:

1. validate the dialog exists and raw message count equals `expected_message_count`;
2. read the existing cumulative metrics or zero values;
3. record this completed compaction, counting `None` as unknown;
4. upsert the one row with the replacement summary, boundary, updated totals, and current timestamp;
5. read back the resulting `ContextState` and commit.

Do not update `dialogs.last_message_id`, `dialogs.updated_at`, or raw messages. Use checked addition for cumulative token fields and return `StoreError::InvalidContext` rather than wrapping.

- [ ] **Step 5: Verify transaction rollback**

Create a SQLite trigger that aborts `UPDATE OF summary ON dialog_context`, attempt a second replacement, and assert that `load(1)` still returns the first summary and first totals. This catches activating an in-memory summary before its durable replacement succeeds.

- [ ] **Step 6: Run persistence tests and verify GREEN**

Run `cargo test --test dialog -- --nocapture`. Expect all Day-7/Day-8 compatibility tests and the new context tests to pass.

- [ ] **Step 7: Commit**

```bash
git add src/dialog.rs tests/dialog.rs
git commit -m "Implement #9: Persist current compressed context"
```

---

### Task 5: Opt-in redacted JSONL diagnostics

**Files:**
- Create: `src/debug_log.rs`
- Create: `tests/debug_log.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `DebugConfig`, API key, message payloads, summary boundaries, usage, and sanitized errors.
- Produces: `DebugLog::new`, `DebugLog::from_config`, `log_request`, and `log_event`; each write returns at most one user-visible warning string and never fails the dialog.

- [ ] **Step 1: Write failing payload and redaction tests**

Create `tests/debug_log.rs`:

```rust
#[test]
fn payload_content_is_opt_in_and_api_key_is_always_redacted() {
    let directory = tempfile::tempdir().unwrap();
    let safe_path = directory.path().join("safe.jsonl");
    let full_path = directory.path().join("full.jsonl");
    let messages = vec![Message::for_request(
        Role::User,
        "private text secret-key",
    )];

    let mut safe = DebugLog::new(Some(safe_path.clone()), false, "secret-key");
    assert_eq!(safe.log_request("chat", &messages, 0), None);
    let safe_text = std::fs::read_to_string(safe_path).unwrap();
    assert!(safe_text.contains("\"content_chars\":23"));
    assert!(!safe_text.contains("private text"));
    assert!(!safe_text.contains("secret-key"));

    let mut full = DebugLog::new(Some(full_path.clone()), true, "secret-key");
    assert_eq!(full.log_request("chat", &messages, 0), None);
    let full_text = std::fs::read_to_string(full_path).unwrap();
    assert!(full_text.contains("private text [REDACTED]"));
    assert!(!full_text.contains("secret-key"));
}
```

Add a test that passes a directory as `log_path`, calls logging twice, and asserts the first call returns one warning while the second returns `None`.

- [ ] **Step 2: Run the logger test and verify RED**

Run `cargo test --test debug_log -- --nocapture`. Expected: compilation fails because `debug_log` does not exist.

- [ ] **Step 3: Implement append-only JSONL logging**

Create a `DebugLog` that opens its configured path with `OpenOptions::new().create(true).append(true)`. No path means a disabled logger. Store the API key only for redaction and serialize one `serde_json::Value` per line.

Implement:

```rust
pub fn new(path: Option<PathBuf>, log_payloads: bool, api_key: &str) -> Self;

pub fn from_config(config: &DebugConfig, api_key: &str) -> Self;

pub fn log_request(
    &mut self,
    kind: &'static str,
    messages: &[Message],
    summary_boundary: usize,
) -> Option<String>;

pub fn log_event(
    &mut self,
    event: &'static str,
    details: serde_json::Value,
) -> Option<String>;
```

Request metadata always includes the event name, request kind, boundary, role, and Unicode character count for every message. Include serialized `messages` only when payload logging is enabled. Serialize to a string, replace every occurrence of the non-empty API key with `[REDACTED]`, append a newline, and flush so the file is useful during a live run.

After open, serialization, write, or flush failure, close/disable the writer and retain a boolean so only the first failure returns `debug log disabled: ...`. Never panic and never retry during the same process.

Add `pub mod debug_log;` to `src/lib.rs`.

- [ ] **Step 4: Run logger tests and verify GREEN**

Run `cargo test --test debug_log -- --nocapture` and expect both opt-in/redaction and one-warning behaviors to pass.

- [ ] **Step 5: Commit**

```bash
git add src/debug_log.rs src/lib.rs tests/debug_log.rs
git commit -m "Implement #9: Add redacted context debug log"
```

---

### Task 6: Agent orchestration and non-fatal compaction failures

**Files:**
- Modify: `src/agent.rs:1-158`
- Modify: `tests/agent.rs`

**Interfaces:**
- Consumes: configuration, pure context plan, summary client call, context persistence, and debug logger.
- Produces: `AgentEvent`, `Agent::context_stats()`, and automatic post-answer compaction.

- [ ] **Step 1: Write the failing end-to-end agent test**

Use a deterministic Wiremock responder so one endpoint can return ordinary and summary responses in order:

```rust
#[derive(Clone)]
struct SequenceResponder {
    responses: Arc<Mutex<VecDeque<ResponseTemplate>>>,
}

impl Respond for SequenceResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra request")
    }
}

async fn mount_sequence(
    server: &MockServer,
    responses: impl IntoIterator<Item = ResponseTemplate>,
) {
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(SequenceResponder {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        })
        .mount(server)
        .await;
}
```

Import `VecDeque`, `Arc`, `Mutex`, `Request`, and `Respond` in the test module. Configure threshold `3`, keep `2`, and summary limit `64`. The first ordinary call stays below the threshold; the second crosses it, causing a hidden summary call before the third ordinary request. Assert four HTTP requests and their literal message arrays:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Seen {
    CompactionStarted,
    CompactionCompleted,
    CompactionFailed,
    Other,
}

fn sse(
    answer: &str,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
) -> ResponseTemplate {
    let chunk = json!({
        "choices": [{"delta": {"content": answer}, "finish_reason": "stop"}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens
        }
    });
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
}

fn compression_config(server: &MockServer, threshold: u64, keep: usize) -> Config {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        "api_key = \"test-key\"\nbase_url = \"{}\"\nsystem_prompt = \"Be concise.\"\n[context]\nenabled = true\ncompact_after_prompt_tokens = {threshold}\nkeep_last_messages = {keep}\nsummary_max_tokens = 64\n",
        server.uri(),
    )
    .unwrap();
    Config::load(file.path(), None).unwrap()
}
```

```rust
#[tokio::test]
async fn crossing_request_is_saved_then_compacted_and_next_request_uses_summary_tail() {
    let server = MockServer::start().await;
    mount_sequence(
        &server,
        [
            sse("answer one", 2, 1, 3),
            sse("answer two", 4, 2, 6),
            sse("summary one", 7, 2, 9),
            sse("answer three", 2, 2, 4),
        ],
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("dialog.sqlite3");
    let mut agent = Agent::with_store(
        &compression_config(&server, 3, 2),
        DialogStore::open(&database).unwrap(),
    )
    .unwrap();

    agent.run_with_prompt("u1").await.unwrap();
    let mut events = Vec::new();
    agent
        .run_streaming("u2", |event| {
            let seen = match event {
                AgentEvent::CompactionStarted { .. } => Seen::CompactionStarted,
                AgentEvent::CompactionCompleted { .. } => Seen::CompactionCompleted,
                AgentEvent::CompactionFailed { .. } => Seen::CompactionFailed,
                AgentEvent::Text(_) | AgentEvent::Usage(_) | AgentEvent::DebugLogFailed { .. } => {
                    Seen::Other
                }
            };
            events.push(seen);
            Ok(())
        })
        .await
        .unwrap();
    agent.run_with_prompt("u3").await.unwrap();

    assert!(events.iter().any(|event| matches!(event, Seen::CompactionStarted)));
    assert!(events.iter().any(|event| matches!(event, Seen::CompactionCompleted)));
    assert_eq!(agent.history().messages().len(), 6);
    let stats = agent.context_stats();
    assert_eq!(stats.covered_message_count, 2);
    assert_eq!(stats.raw_message_count, 4);
    assert_eq!(stats.compaction_usage.total_tokens(), 9);

    let requests = server.received_requests().await.unwrap();
    let follow_up: Value = requests[3].body_json().unwrap();
    assert_eq!(
        follow_up["messages"],
        json!([
            {"role":"system","content":"Be concise."},
            {"role":"system","content":"Summary of earlier conversation:\nsummary one"},
            {"role":"user","content":"u2"},
            {"role":"assistant","content":"answer two"},
            {"role":"user","content":"u3"}
        ])
    );
}
```

Do not retain borrowed events in the real test; map each event immediately to an owned enum tag and numeric fields. The mutation caught is using full raw history after a successful compaction or exposing summary output as assistant text.

- [ ] **Step 2: Write the failing repeated-summary and failure tests**

Add one scenario that crosses the threshold twice and assert the second summary request contains `summary one` plus only messages between the old and new boundaries. Add another where the summary stream returns HTTP 500 and assert:

```rust
assert_eq!(agent.run_with_prompt("cross threshold").await.unwrap(), "answer");
assert_eq!(agent.context_stats().covered_message_count, 0);
assert_eq!(agent.history().messages().len(), 2);
assert!(events.iter().any(|event| matches!(event, Seen::CompactionFailed)));
```

Then mount a successful ordinary response and assert the next ordinary request still contains the complete raw history.

- [ ] **Step 3: Run focused tests and verify RED**

Run `cargo test --test agent compaction -- --nocapture`. Expected: compilation fails because agent context events and orchestration are absent.

- [ ] **Step 4: Add agent-owned context and event types**

Add fields for `ContextConfig`, `ContextState`, and `DebugLog` to `Agent`. `Agent::new` and `Agent::with_store` initialize them from `Config`; `Agent::from_dialog` loads the stored context state; the existing `from_client` constructor uses compression disabled and logging disabled to preserve its low-level testing purpose.

Define:

```rust
pub enum AgentEvent<'a> {
    Text(&'a str),
    Usage(TokenUsage),
    CompactionStarted {
        threshold: u64,
        covered_message_count: usize,
        kept_message_count: usize,
    },
    CompactionCompleted {
        covered_message_count: usize,
        usage: Option<TokenUsage>,
    },
    CompactionFailed { error: String },
    DebugLogFailed { error: String },
}
```

Change only the agent callback from low-level `StreamEvent` to `AgentEvent`; keep the client callback API unchanged. Map normal text and usage events directly.

- [ ] **Step 5: Build ordinary requests from active context**

At the beginning of `run_streaming`, call `build_request_messages` instead of `ChatHistory::request_messages`. Log the exact constructed request or redacted metadata before the HTTP call. Preserve the current rule that persistent input is saved before HTTP starts and in-memory input is committed only after a successful answer.

Expose:

```rust
pub fn context_stats(&self) -> ContextStats
```

It derives ordinary usage from raw assistant messages and uses persisted/in-memory compaction totals from `ContextState`.

- [ ] **Step 6: Compact after a saved complete answer**

After appending the assistant answer and usage to the raw history:

1. return without compaction if disabled, usage is missing, or prompt tokens are below the threshold;
2. call `plan_compaction`; return when no newer eligible prefix exists;
3. emit/log `CompactionStarted`;
4. call `client.summarize(plan.request_messages(), summary_max_tokens)`;
5. on API error, emit/log `CompactionFailed` and keep the successful ordinary answer;
6. on success, call `DialogStore::replace_context` for persistent agents before changing in-memory state, or replace in memory directly for non-persistent agents;
7. emit/log `CompactionCompleted`.

Logging failures emit `DebugLogFailed` once and do not alter control flow. A callback I/O failure remains `ClientError::Output`. A store failure propagates as `AgentError::Store`, preserving the existing fatal persistence policy.

- [ ] **Step 7: Reset context correctly**

Extend `clear_history` to reset in-memory `ContextState` along with raw history, dialog ID, and last usage. It must not delete the old dialog's SQLite context row.

- [ ] **Step 8: Run agent and regression tests and verify GREEN**

Run:

```bash
cargo test --test agent -- --nocapture
cargo test --test client --test context --test dialog -- --nocapture
```

Expect every new compaction scenario and every Day-8 behavior to pass.

- [ ] **Step 9: Commit**

```bash
git add src/agent.rs tests/agent.rs
git commit -m "Implement #9: Compact agent history after token limit"
```

---

### Task 7: `/stats`, live notices, and manual comparison documentation

**Files:**
- Modify: `src/chat.rs:50-67`
- Modify: `src/main.rs:115-180`
- Modify: `src/terminal.rs`
- Modify: `tests/chat.rs`
- Modify: `tests/cli.rs`
- Modify: `README.md`
- Modify: `docs/DAYS.md`

**Interfaces:**
- Consumes: `AgentEvent` and `ContextStats`.
- Produces: `InputAction::Stats`, `/stats`, stable formatted metrics, interactive compaction notices, and Day-9 operating instructions.

- [ ] **Step 1: Write the failing command/parser test**

Extend `tests/chat.rs`:

```rust
assert_eq!(parse_input(" /stats "), InputAction::Stats);
```

Run `cargo test --test chat recognizes_commands -- --nocapture` and verify it fails because `/stats` is currently sent to DeepSeek.

- [ ] **Step 2: Implement the local `/stats` action**

Add `Stats` to `InputAction` and map exactly `/stats` to it. Do not treat `/stats anything` as a command.

- [ ] **Step 3: Write failing deterministic formatting tests**

Add a terminal unit test using literal `ContextStats` and totals. The plain output must equal:

```text
Контекст · полная история: 24 · покрыто summary: 14 · дословно: 10
Ответы · вход: 12000 · выход: 2000 · всего: 14000
Сжатие · вход: 5000 · выход: 600 · всего: 5600
API всего · 19600
```

Add a second case where each category has one missing-usage call and assert the corresponding line ends with `· без данных API: 1`. This catches silently presenting unknown calls as zero.

- [ ] **Step 4: Implement `TerminalUi::write_context_stats`**

Format each metric line from `ContextStats` and `UsageTotals`, using checked/saturating display addition only after each category has already maintained checked internal totals. Keep this report multi-line even in a narrow terminal; unlike the rolling footer, it is explicitly requested output.

- [ ] **Step 5: Integrate agent events without corrupting streaming output**

In `main.rs`, import `AgentEvent`. The response block continues to own stdout during `run_streaming`, so render interactive compaction notices on stderr:

```rust
AgentEvent::Text(fragment) => block.write_text(fragment),
AgentEvent::Usage(_) => Ok(()),
AgentEvent::CompactionStarted { covered_message_count, kept_message_count, .. }
    if stderr_ui.is_interactive() => stderr_ui.write_status(
        &mut stderr,
        &format!("Контекст · сжимаю до {covered_message_count}, оставляю {kept_message_count} сообщений"),
    ),
AgentEvent::CompactionCompleted { covered_message_count, .. }
    if stderr_ui.is_interactive() => stderr_ui.write_status(
        &mut stderr,
        &format!("Контекст · summary обновлено до сообщения {covered_message_count}"),
    ),
AgentEvent::CompactionFailed { error } => stderr_ui.write_block(
    &mut stderr,
    BlockStyle::Error,
    &format!("context compaction failed: {error}"),
),
AgentEvent::DebugLogFailed { error } => stderr_ui.write_block(
    &mut stderr,
    BlockStyle::Error,
    &error,
),
_ => Ok(()),
```

For non-interactive stderr, suppress start/success notices but keep failures. Add `InputAction::Stats` handling that prints `agent.context_stats()` and performs no HTTP call. Keep the existing latest ordinary usage footer unchanged.

- [ ] **Step 6: Add CLI integration coverage**

Use a low-threshold temporary config and sequential Wiremock responses. Pipe prompts ending in `/stats\n/exit\n`; assert stdout contains all four report lines, only ordinary answer text appears as assistant output, and the server receives the expected number of ordinary plus summary calls. Assert `/stats` itself creates no request. Resume the same database and assert totals and the active boundary survive the process restart.

- [ ] **Step 7: Update Day-9 documentation**

Change the README branch banner to Day-9 and add:

- the configuration block and exact post-response trigger timing;
- the request shape `system + summary + raw tail + current user`;
- the fact that only one current summary is stored while complete raw history remains;
- `/stats` field definitions, including compaction overhead and missing usage;
- JSONL privacy behavior and live event behavior;
- two manual comparison commands using separate databases, for example
  `cargo run -- --db no-compression.sqlite3` and
  `cargo run -- --db compression.sqlite3` after toggling the config;
- a low-threshold demonstration example and a warning that it is intentionally
  inefficient outside a demo.

Add the Day-9 row to `docs/DAYS.md` and switch its example branch to `Day-9`.

- [ ] **Step 8: Run UI/CLI tests and verify GREEN**

Run:

```bash
cargo test --test chat --test cli -- --nocapture
cargo test terminal::tests -- --nocapture
```

Expected: parser, metrics, compaction integration, and all previous terminal behavior pass.

- [ ] **Step 9: Commit**

```bash
git add src/chat.rs src/main.rs src/terminal.rs tests/chat.rs tests/cli.rs README.md docs/DAYS.md
git commit -m "Implement #9: Show context compression metrics"
```

---

### Task 8: Full verification and implementation review

**Files:**
- Modify if required by verification: only files changed in Tasks 1-7

**Interfaces:**
- Consumes: the complete Day-9 implementation and approved design.
- Produces: fresh evidence that formatting, linting, tests, persistence compatibility, documentation, and the approved requirements agree.

- [ ] **Step 1: Run the repository Rust pre-push checks**

From the repository root, run the checked-in workflow's resolved script:

```bash
bash /Users/tiazhelkovv/.codex/plugins/cache/stonfi/push-mr-workflow/0.1.0/skills/push-mr-workflow/scripts/rust-pre-push-checks.sh
```

Expected: rustfmt check, Clippy with warnings denied, all unit/integration/doc tests, and any repository checks exit successfully.

- [ ] **Step 2: Verify the diff and requirement coverage**

Run:

```bash
git diff --check
git status --short
git diff --stat Day-8..HEAD
```

Read the approved spec line by line and confirm every configuration, trigger, request-shape, persistence, token-accounting, logging, failure, CLI, and documentation rule has a corresponding test or deliberately documented manual check.

- [ ] **Step 3: Perform a final code review**

Use `superpowers:requesting-code-review` before declaring completion. Resolve every concrete correctness or requirement issue through a new failing regression test followed by the minimal fix. Re-run the full pre-push check after any change.

- [ ] **Step 4: Commit verification fixes if any**

If review required changes, stage only those reviewed files and use:

```bash
git commit -m "Fix #9: Address context compression review"
```

Do not create an empty commit when no fixes were necessary.
