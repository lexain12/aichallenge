# DeepSeek CLI Design

## Goal

Build a small Rust command-line application for one interactive DeepSeek
conversation. The process retains conversation history in memory, streams each
assistant response to the terminal, and reads API settings from a TOML file.

## User experience

The installed binary is named `deepseek-cli`. Running it without arguments
loads `deepseek.toml` from the current directory. `--config <path>` selects a
different file.

After successful configuration validation, the program displays `you> `. A
non-empty line is appended to the in-memory conversation and submitted with the
entire current history. The program displays `assistant> ` and prints response
text as it arrives. When the stream completes, the full assistant message is
appended to the history and the next prompt is displayed.

The following local commands never reach the API:

- `/clear` removes all user and assistant messages while retaining the
  configured system prompt.
- `/exit` and `/quit` end the program successfully.
- End-of-file (Ctrl+D on Unix) also ends the program successfully.

Blank input is ignored. Conversation history is never persisted to disk.

## Configuration

The TOML configuration supports these fields:

```toml
api_key = "replace-me"
base_url = "https://api.deepseek.com"
model = "deepseek-v4-flash"
system_prompt = "You are a helpful assistant."
temperature = 1.0
max_tokens = 4096
timeout_seconds = 120
```

`api_key` is optional in the file when `DEEPSEEK_API_KEY` is set. The
environment variable takes precedence over the file value. All other fields
have the defaults shown above, so a configuration containing only a key is
valid.

The program rejects a missing or blank API key, an invalid HTTP(S) base URL, a
blank model, a temperature outside `0.0..=2.0`, `max_tokens = 0`, and
`timeout_seconds = 0`. Error messages identify the invalid field without ever
including the API key.

The repository contains `deepseek.example.toml`. The live `deepseek.toml` is
listed in `.gitignore` because it may contain a secret.

## Architecture

- `src/main.rs` parses `--config`, loads configuration, constructs the API
  client, and runs the terminal loop.
- `src/config.rs` deserializes TOML, applies defaults and the environment
  override, and validates all fields.
- `src/chat.rs` owns the message history and recognizes local commands.
- `src/client.rs` builds Chat Completions requests and converts the server-sent
  event response into streamed text.

The HTTP layer uses Tokio, Reqwest, Serde, and an SSE parser. It calls
`POST {base_url}/chat/completions` with a bearer token and a JSON body containing
`model`, `messages`, `temperature`, `max_tokens`, and `stream: true`.

The client emits only `choices[].delta.content`. A `data: [DONE]` event marks
successful completion. The client tolerates metadata-only chunks, including
reasoning-related fields that do not contain final answer text.

## Failure behavior

Configuration and startup failures are printed to standard error and return a
non-zero exit status. For non-success HTTP responses, the error includes the
HTTP status and a bounded response body, but never request headers or the API
key.

If transport, decoding, or SSE parsing fails during a response, the partial
assistant text remains visible for the user but is not committed to history.
The user message for that failed turn is also rolled back, leaving the previous
conversation state intact. The interactive process reports the error and
continues accepting input.

## Testing and acceptance

Automated tests cover TOML defaults and validation, environment precedence,
history mutation, command recognition, outbound request shape, authorization,
multi-event streaming, `[DONE]`, HTTP errors, and malformed stream data. HTTP
tests use a local mock server and never call DeepSeek.

Before completion, the project must pass:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo build --release
```

A final manual smoke test uses the user's local ignored configuration to send a
short prompt to the real DeepSeek API and confirms that streamed output is
received. Neither the key nor the authorization header may appear in command
output.
