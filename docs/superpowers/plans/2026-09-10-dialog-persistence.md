# Day 7: persistent dialogs

**Goal:** Keep one terminal chat, save each message in SQLite, and resume stored dialogs.

**Architecture:** `DialogStore` owns SQLite access. `Agent` optionally owns a store and dialog identity; `Agent::from_dialog` restores the stored system prompt and ordered messages. CLI uses persistence by default. API credentials stay in configuration, never in dialog metadata.

**Storage semantics:** Commit user input before HTTP starts; commit a complete nonempty assistant answer before returning success. Failed requests leave their user messages in history; partial answers are excluded. Each write is transactional. Optimistic message-count checks reject stale concurrent sessions. `/clear` detaches from the saved dialog and lazily starts another at the next message. Latest means most recent saved message, with IDs resolving ties. Empty sessions are not saved.

**Interfaces:** `DialogStore::open(path)`, `start_dialog(system_prompt, prompt)`, `append_message(id, expected_count, role, content)`, `load(id)`, `list()`, `latest_id()`. `Agent::with_store(config, store)`, `Agent::from_dialog(config, store, id)`, `Agent::dialog_id()`; existing in-memory construction remains available.

**CLI:** `--db` (default `dialogs.sqlite3` in current directory), mutually exclusive `--resume-last`, `--resume ID`, `--list-dialogs`. Listing needs no API key. Explicit resume of a missing dialog fails without creating one. Display restored messages before accepting input. Storage failure terminates the CLI; API errors permit the next input.

## Tasks

- [x] Storage: add `src/dialog.rs`, rusqlite with bundled SQLite, and `tests/dialog.rs`. Test reopen, message order, latest-by-activity, absent IDs, and stale-writer rollback using temporary on-disk databases. Run `cargo test --test dialog` before and after implementation.
- [x] Agent: extend `src/chat.rs` with read-only message access and internal history reconstruction. Extend `src/agent.rs`; test durable user input during callback, restoration of original system prompt and exact request body, failures retaining inputs, clear preserving old dialogs, and database errors preventing API requests. Run `cargo test --test agent`.
- [x] CLI: update `src/main.rs` flags, list rendering and transcript replay. Extend `tests/cli.rs` with two independent process launches against the same temporary database, listing without config, absent resume, and clear starting a new dialog. Keep existing CLI tests isolated from user databases.
- [x] Document Day 7 and durability boundaries in README and DAYS; ignore SQLite database files. Run `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`; request a code review.

Validation: 41 tests passed, including forced process termination during an API request and restoration across separate CLI processes. Clippy with warnings denied, rustfmt check, and git diff whitespace check passed. Independent review found no concrete bugs.
