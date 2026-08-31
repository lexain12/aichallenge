# DeepSeek CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an installable Rust CLI that maintains one in-memory DeepSeek conversation and streams replies using configuration from TOML or `DEEPSEEK_API_KEY`.

**Architecture:** A thin binary composes three library modules: configuration loading and validation, deterministic in-memory chat state, and a direct Reqwest/SSE DeepSeek client. The API client accepts an output callback so streaming is independently testable without coupling HTTP behavior to terminal I/O.

**Tech Stack:** Stable Rust 2024 edition, Tokio, Reqwest with rustls, Serde/serde_json, TOML, Clap, eventsource-stream, futures-util, thiserror, Wiremock for HTTP tests.

**Spec:** `docs/superpowers/specs/2026-08-31-deepseek-cli-design.md`

## Global Constraints

- The binary name is exactly `deepseek-cli`.
- The default configuration path is exactly `deepseek.toml`.
- The default API base URL is `https://api.deepseek.com`.
- The default model is `deepseek-v4-flash`.
- Conversation history exists only for the lifetime of the process.
- `DEEPSEEK_API_KEY` overrides `api_key` from TOML.
- The API key and authorization header must never appear in program output.
- Every production behavior is preceded by a test that fails for the expected reason.

## File map

- `Cargo.toml`: package metadata, runtime dependencies, and test dependencies.
- `.gitignore`: Rust build output and the live secret-bearing configuration.
- `src/lib.rs`: public module boundary used by both the binary and integration tests.
- `src/config.rs`: TOML defaults, environment override, URL parsing, and validation.
- `src/chat.rs`: local command parsing and committed conversation state.
- `src/client.rs`: request DTOs, HTTP call, bounded errors, and SSE decoding.
- `src/main.rs`: Clap arguments, terminal prompts, streaming output, and recovery loop.
- `tests/config.rs`: externally visible configuration behavior.
- `tests/chat.rs`: command and history behavior.
- `tests/client.rs`: mock-server request and streaming behavior.
- `deepseek.example.toml`: documented safe configuration template.
- `README.md`: build, configuration, usage, and security instructions.

---

### Task 1: Crate foundation and validated configuration

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `src/lib.rs`
- Create: `src/config.rs`
- Create: `tests/config.rs`

**Interfaces:**
- Consumes: a filesystem path, an optional environment key supplied by the caller, and TOML text.
- Produces: `Config::load(path: &Path, env_api_key: Option<String>) -> Result<Config, ConfigError>` and public read-only fields `api_key`, `base_url`, `model`, `system_prompt`, `temperature`, `max_tokens`, `timeout_seconds`.

- [ ] **Step 1: Create the package manifest and ignore rules**

Create a Rust 2024 binary/library package named `deepseek-cli`. Add `anyhow`,
`clap` with `derive`, `eventsource-stream`, `futures-util`, `reqwest` with
`json`, `stream`, and `rustls-tls`, `serde` with `derive`, `serde_json`,
`thiserror`, `tokio` with `macros`, `rt-multi-thread`, and `io-util`, `toml`,
and `url`. Add `tempfile` and `wiremock` as dev dependencies. Ignore `/target`
and `/deepseek.toml`.

- [ ] **Step 2: Write failing configuration tests**

Create `tests/config.rs` with separate tests equivalent to:

```rust
#[test]
fn applies_defaults_and_reads_file_key() {
    let file = write_config("api_key = \"file-key\"");
    let config = Config::load(file.path(), None).unwrap();
    assert_eq!(config.api_key(), "file-key");
    assert_eq!(config.base_url().as_str(), "https://api.deepseek.com/");
    assert_eq!(config.model(), "deepseek-v4-flash");
    assert_eq!(config.temperature(), 1.0);
    assert_eq!(config.max_tokens(), 4096);
    assert_eq!(config.timeout_seconds(), 120);
}

#[test]
fn environment_key_overrides_file_key() {
    let file = write_config("api_key = \"file-key\"");
    let config = Config::load(file.path(), Some("env-key".into())).unwrap();
    assert_eq!(config.api_key(), "env-key");
}

#[test]
fn rejects_out_of_range_temperature_without_exposing_key() {
    let file = write_config("api_key = \"secret-key\"\ntemperature = 2.1");
    let error = Config::load(file.path(), None).unwrap_err().to_string();
    assert!(error.contains("temperature"));
    assert!(!error.contains("secret-key"));
}
```

Add focused cases for missing/blank keys, non-HTTP(S) URLs, blank model,
`max_tokens = 0`, `timeout_seconds = 0`, invalid TOML, and missing files.

- [ ] **Step 3: Run the configuration tests and verify RED**

Run: `cargo test --test config`

Expected: compilation fails because `deepseek_cli::config::Config` does not
exist yet.

- [ ] **Step 4: Implement only the configuration behavior under test**

Define a private optional `RawConfig` deserialized with Serde. Resolve every
missing value using constants. Apply the supplied environment key after TOML
deserialization, parse the base URL with `url::Url`, validate every constraint,
and expose getter methods. Define `ConfigError` variants with `thiserror`; no
variant stores formatted configuration or prints the secret.

- [ ] **Step 5: Run configuration tests and verify GREEN**

Run: `cargo test --test config`

Expected: all configuration tests pass.

- [ ] **Step 6: Refactor and commit**

Run `cargo fmt` and `cargo test --test config`, then commit:

```text
feat: add validated DeepSeek configuration
```

### Task 2: In-memory conversation state and local commands

**Files:**
- Modify: `src/lib.rs`
- Create: `src/chat.rs`
- Create: `tests/chat.rs`

**Interfaces:**
- Consumes: a configured system prompt and terminal input strings.
- Produces: serializable `Message { role: Role, content: String }`, `InputAction::{Ignore, Exit, Clear, Send(String)}`, `parse_input(&str) -> InputAction`, and `ChatHistory::{new, request_messages, commit_turn, clear, turn_count}`.

- [ ] **Step 1: Write failing local-command tests**

```rust
#[test]
fn recognizes_commands_and_ignores_blank_input() {
    assert_eq!(parse_input("  "), InputAction::Ignore);
    assert_eq!(parse_input("/exit"), InputAction::Exit);
    assert_eq!(parse_input("/quit"), InputAction::Exit);
    assert_eq!(parse_input("/clear"), InputAction::Clear);
    assert_eq!(parse_input(" hello "), InputAction::Send("hello".into()));
}
```

- [ ] **Step 2: Run the command test and verify RED**

Run: `cargo test --test chat recognizes_commands_and_ignores_blank_input`

Expected: compilation fails because the chat module does not exist.

- [ ] **Step 3: Implement minimal command parsing and verify GREEN**

Use exact-match commands after trimming whitespace; preserve internal text.
Run the same test and expect PASS.

- [ ] **Step 4: Write failing history tests**

```rust
#[test]
fn stages_a_user_message_and_commits_complete_turns() {
    let mut history = ChatHistory::new("Be concise".into());
    let request = history.request_messages("Hello");
    assert_eq!(request.iter().map(|m| m.role()).collect::<Vec<_>>(),
               [Role::System, Role::User]);
    assert_eq!(history.turn_count(), 0);

    history.commit_turn("Hello".into(), "Hi".into());
    assert_eq!(history.turn_count(), 1);
    assert_eq!(history.request_messages("Again").len(), 4);
}

#[test]
fn clear_removes_turns_but_keeps_system_prompt() {
    let mut history = ChatHistory::new("Be concise".into());
    history.commit_turn("Hello".into(), "Hi".into());
    history.clear();
    let request = history.request_messages("Again");
    assert_eq!(history.turn_count(), 0);
    assert_eq!(request[0].role(), Role::System);
}
```

- [ ] **Step 5: Run history tests and verify RED**

Run: `cargo test --test chat`

Expected: failures identify missing `ChatHistory` behavior.

- [ ] **Step 6: Implement history, verify GREEN, and commit**

Store only committed user/assistant pairs. Construct request vectors from the
optional nonblank system message, committed turns, and the staged user input.
Run `cargo fmt` and `cargo test --test chat`, then commit:

```text
feat: add in-memory chat history
```

### Task 3: Streaming DeepSeek HTTP client

**Files:**
- Modify: `src/lib.rs`
- Create: `src/client.rs`
- Create: `tests/client.rs`

**Interfaces:**
- Consumes: `Config`, a slice of `Message`, and `FnMut(&str) -> io::Result<()>`.
- Produces: `DeepSeekClient::new(&Config) -> Result<DeepSeekClient, ClientError>` and `async fn stream_chat<F>(&self, messages: &[Message], on_text: F) -> Result<String, ClientError>` where `F: FnMut(&str) -> io::Result<()>`.

- [ ] **Step 1: Write a failing successful-stream test**

Start a Wiremock server and configure two SSE JSON events followed by
`data: [DONE]`. Require `POST /chat/completions`, bearer authorization, and the
exact JSON fields from the spec. Call `stream_chat`, collect callback fragments,
and assert:

```rust
assert_eq!(fragments.concat(), "Hello world");
assert_eq!(answer, "Hello world");
```

- [ ] **Step 2: Run the stream test and verify RED**

Run: `cargo test --test client streams_content_and_sends_expected_request`

Expected: compilation fails because `DeepSeekClient` does not exist.

- [ ] **Step 3: Implement the minimal successful SSE path**

Create private serializable request structs and deserializable response structs.
Build `{base_url}/chat/completions`, set bearer auth and JSON, call
`error_for_status` only after retaining the response for safe error processing,
then parse `response.bytes_stream().eventsource()`. Ignore events without
`delta.content`, append text to a result string, invoke the callback for every
text fragment, and require `[DONE]`.

- [ ] **Step 4: Run the stream test and verify GREEN**

Run the same focused test; expect PASS.

- [ ] **Step 5: Write failing protocol and error tests**

Add separate tests proving that:

- metadata-only and `reasoning_content` chunks do not reach the callback;
- invalid JSON returns an error and does not panic;
- a closed stream without `[DONE]` returns an incomplete-stream error;
- a `401` response includes status and bounded server text;
- a `401` body that echoes the configured key contains `[REDACTED]` instead of
  the key;
- a callback I/O failure is returned immediately.

- [ ] **Step 6: Run new tests and verify RED**

Run: `cargo test --test client`

Expected: each new case fails because its explicit error behavior is missing.

- [ ] **Step 7: Implement bounded, redacted errors and protocol checks**

Read at most 4096 bytes of a non-success response stream, replace exact API key
occurrences with `[REDACTED]`, define specific `ClientError` variants, propagate
SSE/JSON/output errors, and return `IncompleteStream` unless `[DONE]` was seen.

- [ ] **Step 8: Verify GREEN, refactor, and commit**

Run `cargo fmt` and `cargo test --test client`, then commit:

```text
feat: stream DeepSeek chat completions
```

### Task 4: Interactive binary, documentation, and end-to-end verification

**Files:**
- Create: `src/main.rs`
- Create: `deepseek.example.toml`
- Create: `README.md`
- Create locally but do not commit: `deepseek.toml`

**Interfaces:**
- Consumes: `--config <path>`, terminal lines, the modules completed above, and optionally `DEEPSEEK_API_KEY`.
- Produces: interactive prompts `you> ` and `assistant> `, streamed answer text, `/clear`, `/exit`, `/quit`, and successful EOF exit.

- [ ] **Step 1: Write failing argument-parser unit tests**

Keep `Args` and `parse_args_from` in `main.rs`. Assert the default config path
is `deepseek.toml` and `--config custom.toml` overrides it.

- [ ] **Step 2: Run binary tests and verify RED**

Run: `cargo test --bin deepseek-cli`

Expected: compilation fails because the binary behavior does not exist.

- [ ] **Step 3: Implement the interactive loop**

Use `tokio::io::BufReader::new(tokio::io::stdin()).lines()`. Flush standard
output after every prompt and callback fragment. Stage messages with
`request_messages`; call `commit_turn` only after `stream_chat` succeeds. On a
request error, print a newline plus a safe error to standard error and continue.
On `/clear`, clear history and print `Conversation cleared.`

- [ ] **Step 4: Verify the binary tests and all automated checks**

Run:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo build --release
```

Expected: every command exits 0 without warnings.

- [ ] **Step 5: Add safe examples and user documentation**

Document installation with `cargo build --release`, copying
`deepseek.example.toml` to `deepseek.toml`, key precedence, every configurable
field, commands, and the warning not to commit keys. The example key must be an
obvious non-secret value.

- [ ] **Step 6: Create the ignored live configuration and verify Git safety**

Create `deepseek.toml` with the user-provided key and the agreed defaults. Run
`git check-ignore -v deepseek.toml` and `git grep` for the key; the first command
must identify `.gitignore`, and the second must produce no matches.

- [ ] **Step 7: Perform a real streaming smoke test**

Pipe a short Russian prompt followed by `/exit` into the release binary. Confirm
that at least one non-empty streamed assistant response arrives and the process
exits successfully. Do not capture or print environment variables, request
headers, or configuration contents.

- [ ] **Step 8: Commit the finished application**

Commit only tracked source, tests, examples, manifest/lockfile, and README:

```text
feat: add interactive DeepSeek CLI
```
