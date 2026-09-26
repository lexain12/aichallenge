# Day 18 Light Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a small remote DeepSeek agent with an SSH terminal client, multiple durable dialogs, Streamable HTTP MCP tools, inspectable SQLite state, and confirmed cron scheduling that launches isolated prompt-only agent runs.

**Architecture:** The macOS binary launches the system `ssh` client and speaks strict versioned NDJSON to one `light-agent serve-stdio` process on the VM. The server owns DeepSeek, MCP, SQLite, scheduling, inspection, and export; cron starts a separate `light-agent run-job JOB_ID` process whose context contains only the cron system prompt and saved job prompt. SQLite is authoritative, while a locked synchronizer renders a marked crontab block containing only validated schedules, timezones, a fixed binary path, and opaque job IDs.

**Tech Stack:** Rust 2024, Tokio, reqwest SSE, rmcp Streamable HTTP, rusqlite/WAL, serde/JSON/TOML, clap, chrono/chrono-tz, uuid, fs2 file locking, system OpenSSH, Cronie, and the existing Python Telegram MCP service.

**Spec:** `docs/superpowers/specs/2026-09-26-day-18-light-agent-design.md`

## Global Constraints

- Produce `light-agent` and `light-agent-client`; do not expose a network listener or run an application daemon.
- The Mac client uses `ssh -T <host-alias> /opt/light-agent/bin/light-agent serve-stdio`; it stores no DeepSeek, Telegram, MCP, or SQLite credentials.
- Protocol version is `1`; inbound NDJSON lines are at most 1 MiB, message/job-prompt fields at most 256 KiB, and export chunks at most 64 KiB.
- A complete serialized provider request, including system prompt and tool schemas, is at most 512 KiB.
- Dialog context is the system prompt, every completed user/assistant message in order, and the new user message; never summarize, compact, truncate, or replay failed/interrupted turns.
- Interactive tool loops allow at most the configured number of rounds, default `8`; provider aliases remain opaque routes.
- Cron jobs default to `Europe/Moscow`; cron-launched agents receive MCP tools but never scheduler tools or source-dialog history.
- Create, update, disable, and delete schedule operations require a single-use confirmation bound to the request and SSH session and expiring after five minutes.
- The target VM must provide Cronie (or an explicitly verified equivalent) with `CRON_TZ` scheduling and `crontab -T` syntax validation; unsupported cron is a deployment error.
- No prompt, tool arguments, provider body, remote MCP error, credential, or secret may appear in crontab, ordinary client errors, `tool_runs`, logs, or exports outside its deliberately persisted message/job field.
- Default tests may not contact live DeepSeek, Telegram, SSH, or a real crontab; live tests remain ignored and require explicit environment opt-in.

## Review Focus

- SSH EOF while a write tool is in flight must interrupt the turn, mark the tool uncertain, persist no assistant answer, and never retry it; Task 10 adds the disconnect test.
- A malformed or duplicated managed-block marker in an existing crontab must fail closed without overwriting user entries; Task 7 adds the preservation test.
- A `once_at` time missed during VM downtime or a DST transition must become `missed`, never execute late or a year later, and never execute twice; Tasks 7 and 12 add clock-controlled tests.
- An export interrupted after writing chunks must leave the previous destination unchanged and remove the temporary file; Task 11 adds the atomic-export test.
- A second writer or deletion of a dialog referenced by a non-deleted job must be rejected while read-only inspection remains available; Tasks 3 and 10 add concurrency and ownership tests.

---

## Final file map

- `src/domain.rs` — validated IDs and shared lifecycle enums.
- `src/settings.rs` — strict server/client TOML parsing and secret-safe debug output.
- `src/protocol.rs` — versioned NDJSON request/event types and bounded codecs.
- `src/store/mod.rs` — SQLite opening, migrations, WAL/busy-timeout setup, and shared row decoding.
- `src/store/dialogs.rs` — dialogs, turns, messages, active-turn ownership, and recovery.
- `src/store/audit.rs` — metadata-only tool-run lifecycle.
- `src/store/jobs.rs` — cron jobs, run claims, sync state, missed jobs, and service events.
- `src/provider/mod.rs` — provider-neutral message, request, response, usage, and trait types.
- `src/provider/deepseek.rs` — DeepSeek SSE and fragmented tool-call assembly.
- `src/tools/mod.rs` — tool definitions, executor contract, composite catalog, and safe results.
- `src/tools/conversation.rs` — bounded transient tool conversation state machine.
- `src/tools/mcp.rs` — immutable Streamable HTTP MCP discovery and dispatch.
- `src/tools/scheduler.rs` — native `cron__*` tools and confirmation boundary.
- `src/agent_runner.rs` — full-history request construction, tool execution/audit, cancellation, and interactive persistence.
- `src/scheduler.rs` — schedule validation, crontab rendering/backend, locking, reconciliation, and run claiming.
- `src/inspection.rs` — logical inspection DTOs, JSON export, and restricted read-only SQL REPL.
- `src/server.rs` — one stdio session, request routing, confirmation broker, and disconnect handling.
- `src/remote_client.rs` — system-SSH child transport and protocol I/O.
- `src/terminal_client.rs` — terminal commands, rendering, confirmations, and atomic local export.
- `src/bin/light_agent.rs` — `serve-stdio`, `run-job`, `cron-sync`, and `db-shell --readonly`.
- `src/bin/light_agent_client.rs` — macOS terminal entry point.

### Task 1: Shared domain and strict settings

**Files:**
- Create: `src/domain.rs`
- Create: `src/settings.rs`
- Create: `tests/domain.rs`
- Create: `tests/settings.rs`
- Create: `light-agent.example.toml`
- Create: `light-agent-client.example.toml`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Produces: `DialogId(i64)`, `TurnId(i64)`, `RunId(i64)`, canonical UUID-backed `JobId`, `RequestId`, and `ConfirmationId`.
- Produces: `ToolOwner::{InteractiveTurn(TurnId), CronRun(RunId)}`, `TurnStatus`, `ToolRunStatus`, `JobDesiredState`, `JobSyncState`, and `CronRunStatus`.
- Produces: `ServerSettings::load(path: &Path, env_api_key: Option<String>) -> Result<ServerSettings, SettingsError>` and `ClientSettings::load(path: &Path) -> Result<ClientSettings, SettingsError>`.
- Produces: `ProviderSettings`, `McpSettings`, `McpServerSettings { name, url, bearer_token }`, and `SchedulerSettings` getters used by later tasks; bearer tokens exist only in server settings and are redacted from `Debug`.

- [ ] **Step 1: Write failing domain/settings tests**

  Add tests named `job_id_accepts_only_canonical_uuid`, `ids_reject_blank_or_nonpositive_values`, `server_settings_reject_legacy_compaction_sections`, `server_settings_default_to_exact_limits_and_moscow`, `client_settings_contain_only_ssh_transport_fields`, and `settings_debug_redacts_provider_and_mcp_secrets`. Assert defaults of `524_288`, `262_144`, eight tool rounds, five confirmation minutes, and `Europe/Moscow`; reject provider/MCP URLs containing userinfo.

- [ ] **Step 2: Run the focused tests and verify the new modules are missing**

  Run: `cargo test --locked --test domain --test settings`

  Expected: FAIL because `deepseek_cli::domain` and `deepseek_cli::settings` do not exist.

- [ ] **Step 3: Add the shared types and strict settings parser**

  Implement the interfaces above. Use `#[serde(deny_unknown_fields)]` on every raw config structure, accept `DEEPSEEK_API_KEY` only on the server, validate HTTP(S) MCP/provider URLs, canonicalize UUID strings, and exclude every secret from `Debug`.

- [ ] **Step 4: Add dependencies and example files**

  Add direct dependencies `chrono = { version = "0.4.45", features = ["serde"] }`, `chrono-tz = { version = "0.10", features = ["serde"] }`, `fs2 = "0.4"`, `tokio-util = { version = "0.7", features = ["rt"] }`, and `uuid = { version = "1", features = ["v4", "serde"] }`; add Tokio's `process` feature for the SSH and crontab child processes. The server example contains provider, MCP, scheduler, database, interactive prompt, and cron prompt settings; the client example contains only `ssh_binary`, `ssh_host`, and the fixed remote command.

- [ ] **Step 5: Run settings tests**

  Run: `cargo test --locked --test domain --test settings`

  Expected: PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add Cargo.toml Cargo.lock src/lib.rs src/domain.rs src/settings.rs tests/domain.rs tests/settings.rs light-agent.example.toml light-agent-client.example.toml
  git commit -m "feat: add light agent settings and domain types"
  ```

### Task 2: Strict bounded NDJSON protocol

**Files:**
- Create: `src/protocol.rs`
- Create: `tests/protocol.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `DialogId`, `JobId`, `RequestId`, and `ConfirmationId` from Task 1.
- Produces: `RequestEnvelope { protocol_version, request_id, request: ClientRequest }` and `ServerEnvelope { protocol_version, request_id, event: ServerEvent }`.
- Produces: `ClientRequest::{ListDialogs, CreateDialog, OpenDialog, RenameDialog, DeleteDialog, SendMessage, ConfirmAction, CancelAction, Inspect, Export}`.
- Produces: `ServerEvent::{Hello, DialogList, DialogOpened, ResponseStarted, TextDelta, ToolStarted, ToolFinished, ConfirmationRequired, TurnCompleted, TurnFailed, InspectionResult, ExportChunk, ExportCompleted, ProtocolError}`.
- Produces: protocol-owned `InspectKind`, `InspectionPayload { kind, data: serde_json::Value }`, and export events carrying sequence, base64 data, final byte count, and SHA-256; `ClientRequest::Export` deliberately has no filesystem path.
- Produces: `NdjsonReader<R>::read_request() -> Result<Option<RequestEnvelope>, ProtocolError>` and `NdjsonWriter<W>::write_event(&ServerEnvelope) -> Result<(), ProtocolError>` for Tokio async I/O.

- [ ] **Step 1: Write failing protocol tests**

  Add `round_trips_every_request_and_event`, `rejects_unknown_fields_and_types`, `rejects_version_mismatch_before_dispatch`, `rejects_line_over_one_mib_without_allocating_past_limit`, `rejects_message_and_prompt_over_256_kib`, and `export_chunks_are_at_most_64_kib`. Assert error responses contain only a bounded machine code.

- [ ] **Step 2: Run the protocol tests and verify failure**

  Run: `cargo test --locked --test protocol`

  Expected: FAIL because the protocol module does not exist.

- [ ] **Step 3: Implement protocol DTOs and bounded codecs**

  Set `PROTOCOL_VERSION = 1`, `MAX_LINE_BYTES = 1_048_576`, `MAX_CONTENT_BYTES = 262_144`, and `EXPORT_CHUNK_BYTES = 65_536`. Read with a bounded buffer rather than `AsyncBufReadExt::lines`; terminate each event with one newline and flush it.

- [ ] **Step 4: Run protocol tests**

  Run: `cargo test --locked --test protocol`

  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add src/lib.rs src/protocol.rs tests/protocol.rs
  git commit -m "feat: add bounded stdio protocol"
  ```

### Task 3: SQLite dialogs, turns, messages, and tool audit

**Files:**
- Create: `src/store/mod.rs`
- Create: `src/store/dialogs.rs`
- Create: `src/store/audit.rs`
- Create: `tests/store_dialogs.rs`
- Create: `tests/store_audit.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: ID and lifecycle types from Task 1.
- Produces: cloneable `Store::open(path: impl AsRef<Path>) -> Result<Store, StoreError>`; each operation opens a short-lived SQLite connection configured with foreign keys, WAL, and a bounded busy timeout.
- Produces: `create_dialog(&self, title: &str) -> Result<Dialog, StoreError>`, `list_dialogs(&self) -> Result<Vec<DialogSummary>, StoreError>`, `rename_dialog(&self, DialogId, &str)`, `delete_dialog(&self, DialogId)`, `begin_turn(&self, DialogId, &str) -> Result<TurnStart, StoreError>`, `completed_messages(&self, DialogId) -> Result<Vec<StoredMessage>, StoreError>`, `complete_turn(&self, TurnId, &str)`, `fail_turn`, `interrupt_turn`, and `recover_pending_turns`.
- Produces: `start_tool_run(&self, ToolRunStart) -> Result<i64, StoreError>`, `finish_tool_run(&self, id: i64, ToolRunFinish)`, `recover_pending_tool_runs`, and `list_tool_runs`; Task 3 validates interactive-turn owners, and Task 7 extends the same API to cron-run owners once `cron_runs` exists.

- [ ] **Step 1: Write failing migration and dialog tests**

  Add `migration_creates_exact_v1_schema`, `completed_history_excludes_failed_interrupted_and_pending_turns`, `begin_turn_persists_user_before_provider_work`, `complete_turn_commits_assistant_and_status_atomically`, `second_active_turn_in_same_dialog_is_busy`, `different_dialogs_can_write_concurrently`, and `readers_work_while_another_process_writes`.

- [ ] **Step 2: Write failing audit and deletion tests**

  Add `tool_run_stores_route_status_and_read_only_but_no_arguments`, `write_recovery_becomes_uncertain`, `read_only_recovery_becomes_failed`, and `dialog_delete_is_transactional`. Inspect `PRAGMA table_info(tool_runs)` and assert no argument, hash, result body, URL, or raw error column exists.

- [ ] **Step 3: Run store tests and verify failure**

  Run: `cargo test --locked --test store_dialogs --test store_audit`

  Expected: FAIL because `Store` is missing.

- [ ] **Step 4: Implement migrations and dialog/turn methods**

  Use a schema-version table; partial unique indexes enforce one pending turn per dialog and one user/assistant message per turn. Store UTC timestamps and safe error codes only. `completed_messages` orders by turn then message ID and joins only `turns.status = 'completed'`.

- [ ] **Step 5: Implement metadata-only audit methods and recovery**

  Model ownership with `owner_kind` plus `owner_id`, validate the referenced turn/run in the same transaction, and make finalization idempotent only for identical terminal metadata. Never persist call arguments or results.

- [ ] **Step 6: Run store tests**

  Run: `cargo test --locked --test store_dialogs --test store_audit`

  Expected: PASS.

- [ ] **Step 7: Commit**

  ```bash
  git add src/lib.rs src/store tests/store_dialogs.rs tests/store_audit.rs
  git commit -m "feat: persist light agent conversations and audit"
  ```

### Task 4: Provider types and DeepSeek streaming

**Files:**
- Create: `src/provider/mod.rs`
- Create: `src/provider/deepseek.rs`
- Create: `tests/deepseek.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `ProviderSettings` from Task 1.
- Produces: `ProviderMessage::{system, user, assistant, assistant_tool_calls, tool_result}`, `TokenUsage`, `ModelToolDefinition`, `ModelToolCall`, and `AssistantTurn::{FinalText, ToolCalls}`.
- Produces: `type ProviderFuture<'a> = Pin<Box<dyn Future<Output = Result<AssistantTurn, ProviderError>> + Send + 'a>>`, object-safe `Provider::stream_turn<'a>(&'a self, messages: &'a [ProviderMessage], tools: &'a [ModelToolDefinition], text_sink: &'a mut (dyn FnMut(&str) -> io::Result<()> + Send)) -> ProviderFuture<'a>`, and `DeepSeekProvider::new(&ProviderSettings) -> Result<DeepSeekProvider, ProviderError>`.
- Produces: `ProviderError::safe_code() -> &'static str` and operator-only metadata that never exposes the API key or response body to protocol callers.

- [ ] **Step 1: Port focused Day 17 provider tests as failing tests**

  Cover `streams_fragmented_text`, `assembles_fragmented_tool_calls_by_index`, `rejects_duplicate_or_malformed_call_ids`, `rejects_truncated_and_incomplete_streams`, `bounds_and_redacts_error_body`, `never_sends_tools_when_catalog_is_empty`, and `collects_provider_usage` using `wiremock`.

- [ ] **Step 2: Run provider tests and verify failure**

  Run: `cargo test --locked --test deepseek`

  Expected: FAIL because `deepseek_cli::provider` is missing.

- [ ] **Step 3: Extract the minimal provider implementation**

  Move only Chat Completions request/response types, SSE assembly, safe error handling, usage, and tool-call support from `src/client.rs`/`src/chat.rs`. Do not move summary, facts, profile, workflow, compaction, or service-call APIs.

- [ ] **Step 4: Run provider tests**

  Run: `cargo test --locked --test deepseek`

  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add src/lib.rs src/provider tests/deepseek.rs
  git commit -m "feat: extract minimal DeepSeek provider"
  ```

### Task 5: Bounded tool loop and Streamable HTTP MCP

**Files:**
- Create: `src/tools/mod.rs`
- Create: `src/tools/conversation.rs`
- Create: `src/tools/mcp.rs`
- Create: `tests/tool_conversation.rs`
- Create: `tests/mcp_registry.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: provider tool/message types from Task 4 and MCP settings from Task 1.
- Produces: `type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<ToolExecutionResult, ToolExecutionError>> + Send + 'a>>`; `ToolExecutor::{definitions, route, is_read_only, call<'a>(&'a self, &'a ModelToolCall) -> ToolFuture<'a>}`; `ToolExecutionResult`; `ToolExecutionError`; `ToolRoute`; and `CompositeToolExecutor::new(Vec<Arc<dyn ToolExecutor>>) -> Result<Self, ToolCatalogError>`.
- Produces: `ToolConversation::new(messages, definitions, max_rounds)`, `accept_assistant_turn`, and `accept_tool_results`.
- Produces: `McpRegistry::connect(&McpSettings)` and test-only `McpRegistry::from_clients`.

- [ ] **Step 1: Port the tool-conversation tests**

  Add failing tests for unknown tools, non-object arguments, duplicate IDs across rounds, exact result ordering, round limit `8`, accumulated usage, final text, and transient assistant/tool messages.

- [ ] **Step 2: Port the MCP boundary tests**

  Add failing tests for immutable discovery, alias collisions, invalid names, opaque routing, independent list/call timeouts, no automatic write retry, no redirect replay, safe allowlisted Telegram envelopes, delivery uncertainty, and rejection of non-text content.

- [ ] **Step 3: Run focused tests and verify failure**

  Run: `cargo test --locked --test tool_conversation --test mcp_registry`

  Expected: FAIL because the new tools modules do not exist.

- [ ] **Step 4: Extract the Day 17 tool loop and MCP registry**

  Adapt imports to Tasks 1 and 4. Apply each optional server bearer token through rmcp's transport configuration, never through the URL. `CompositeToolExecutor` rejects duplicate provider-facing names and delegates through stored opaque routes rather than parsing aliases.

- [ ] **Step 5: Run focused tests**

  Run: `cargo test --locked --test tool_conversation --test mcp_registry`

  Expected: PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add src/lib.rs src/tools tests/tool_conversation.rs tests/mcp_registry.rs
  git commit -m "feat: add bounded MCP tool execution"
  ```

### Task 6: Full-history agent runner

**Files:**
- Create: `src/agent_runner.rs`
- Create: `tests/agent_runner.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `Provider`, `ToolExecutor`, `ToolConversation`, `Store`, `ToolOwner`, and `CancellationToken`.
- Produces: `AgentInput { owner, system_prompt, history, prompt }`, `AgentEvent::{TextDelta, ToolStarted, ToolFinished}`, `AgentOutcome { answer, usage }`, `type AgentEventSink<'a> = dyn FnMut(AgentEvent) -> io::Result<()> + Send + 'a`, and `AgentRunner::run(&self, input: AgentInput, cancellation: CancellationToken, event_sink: &mut AgentEventSink<'_>) -> Result<AgentOutcome, AgentError>`.
- Produces: `InteractiveService::send_message(&self, dialog_id: DialogId, content: &str, cancellation: CancellationToken, event_sink: &mut AgentEventSink<'_>) -> Result<AgentOutcome, AgentError>`.

- [ ] **Step 1: Write failing full-history tests**

  Add `request_contains_system_all_completed_messages_and_new_user_only`, `failed_and_interrupted_messages_never_replay`, `tool_messages_exist_only_during_current_loop`, `every_provider_dispatch_enforces_512_kib`, `oversized_provider_answer_is_not_persisted`, and `provider_context_error_recommends_new_dialog_without_deleting_history`.

- [ ] **Step 2: Write failing persistence/audit/cancellation tests**

  Add `user_is_durable_before_provider_call`, `final_answer_and_completed_status_commit_together`, `output_failure_persists_no_assistant`, `tool_calls_run_sequentially_and_emit_events`, `cancelled_read_call_is_failed`, and `cancelled_write_call_is_uncertain_and_not_retried`.

- [ ] **Step 3: Run agent tests and verify failure**

  Run: `cargo test --locked --test agent_runner`

  Expected: FAIL because `AgentRunner` is missing.

- [ ] **Step 4: Implement request construction and the generic runner**

  Measure the complete serialized provider request including tool schemas before every dispatch, including later tool rounds. Execute calls in provider order, create/finalize audit rows around each call, pass only safe tool results back to the provider, and stop on cancellation or persistence failure.

- [ ] **Step 5: Implement interactive persistence coordination**

  `InteractiveService` starts the turn and persists the user message, loads completed history excluding that pending turn, runs the agent, and atomically commits the final assistant answer. Map every failure to a bounded safe code; interrupted work remains inspectable.

- [ ] **Step 6: Run agent tests**

  Run: `cargo test --locked --test agent_runner`

  Expected: PASS.

- [ ] **Step 7: Commit**

  ```bash
  git add src/lib.rs src/agent_runner.rs tests/agent_runner.rs
  git commit -m "feat: run full-history light agent turns"
  ```

### Task 7: Scheduler persistence and safe crontab reconciliation

**Files:**
- Create: `src/store/jobs.rs`
- Create: `src/scheduler.rs`
- Create: `tests/store_jobs.rs`
- Create: `tests/scheduler.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/audit.rs`
- Modify: `tests/store_audit.rs`
- Modify: `src/lib.rs`
- Modify: `src/settings.rs`

**Interfaces:**
- Consumes: `Store`, job/run lifecycle enums, `JobId`, `DialogId`, `chrono`, `chrono_tz`, and `SchedulerSettings`.
- Produces: `ScheduleSpec::{Cron { expression, timezone }, OnceAt { at, timezone }}` with `ScheduleSpec::parse_cron` and `ScheduleSpec::parse_once_at`.
- Produces: job CRUD methods with `desired_state` and `sync_state`, `claim_run(job_id, now) -> RunClaim`, `finish_run`, `mark_missed_once_jobs`, and timeline service-event queries.
- Produces: `type CrontabFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, SchedulerError>> + Send + 'a>>`; `CrontabBackend::{list, validate, install, preflight}` returning that future; `SystemCrontabBackend`; `CronRenderer::replace_managed_block(existing: &str, jobs: &[CronJob]) -> Result<String, SchedulerError>`; and `CronSynchronizer::sync(&self) -> CrontabFuture<'_, SyncReport>`.

- [ ] **Step 1: Write failing schedule and rendering tests**

  Cover exactly five ASCII numeric fields; lists, ranges, and steps; per-field numeric limits; rejection of names, `@` aliases, `%`, newlines, shell characters, six fields, invalid IANA zones, and overlong expressions. Assert recurring and `once_at` lines contain `CRON_TZ`, the fixed executable, and only a canonical job ID after `run-job`.

- [ ] **Step 2: Write failing state-machine tests**

  Add `create_starts_active_pending`, `successful_sync_marks_rendered_jobs_applied`, `install_failure_marks_jobs_failed_and_reports_saved_not_installed`, `disabled_or_deleted_job_never_claims_even_with_stale_line`, `overlap_is_recorded_skipped`, `once_claim_disables_before_provider_work`, `missed_once_job_is_never_run_late_or_next_year`, and `cron_tool_owner_requires_existing_run` with an injected clock.

- [ ] **Step 3: Add the managed-block preservation tests from Review Focus**

  Assert one well-formed existing block is replaced while user lines are byte-preserved. Assert missing end markers, duplicate start/end markers, or nested markers return an error and never call `install`.

- [ ] **Step 4: Run scheduler tests and verify failure**

  Run: `cargo test --locked --test store_jobs --test scheduler`

  Expected: FAIL because scheduler storage and services are missing.

- [ ] **Step 5: Implement job/run migrations and transactional methods**

  Store deleted jobs as tombstones, keep runs, allow `source_dialog_id` to become null only after dialog deletion, and reject dialog deletion while a non-deleted job references it. A partial unique index permits at most one active run per job. Extend audit-owner validation so `ToolOwner::CronRun` must reference an existing `cron_runs` row.

- [ ] **Step 6: Implement validation, rendering, locking, and reconciliation**

  Lock the configured file with `fs2`, call `preflight`, render from SQLite, validate the full candidate via `crontab -T -`, then install via `crontab -`. Cronie/version or `CRON_TZ` capability failure is fatal. Never invoke a shell.

- [ ] **Step 7: Run scheduler tests**

  Run: `cargo test --locked --test store_jobs --test scheduler`

  Expected: PASS.

- [ ] **Step 8: Commit**

  ```bash
  git add src/lib.rs src/settings.rs src/store src/scheduler.rs tests/store_audit.rs tests/store_jobs.rs tests/scheduler.rs
  git commit -m "feat: add durable cron scheduler"
  ```

### Task 8: Confirmed native scheduler tools

**Files:**
- Create: `src/tools/scheduler.rs`
- Create: `tests/scheduler_tools.rs`
- Modify: `src/tools/mod.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: scheduler/store services from Task 7 and tool contracts from Task 5.
- Produces: `ConfirmationRequest { id, request_id, preview, expires_at }`, `type ConfirmationFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ConfirmationError>> + Send + 'a>>`, `ConfirmationBroker::confirm<'a>(&'a self, request: ConfirmationRequest) -> ConfirmationFuture<'a>`, and `SchedulePreview` containing the exact normalized name, schedule, timezone, and complete prompt.
- Produces: `SchedulerToolExecutor::new(store, synchronizer, broker, source_dialog_id, request_id)` implementing `ToolExecutor` for `cron__create`, `cron__list`, `cron__update`, `cron__disable`, and `cron__delete`.

- [ ] **Step 1: Write failing schema and preview tests**

  Assert each provider definition uses a closed JSON-object schema; the server, not arguments, supplies `source_dialog_id`; previews contain the full prompt and normalized schedule; and `cron__list` is read-only.

- [ ] **Step 2: Write failing confirmation tests**

  Add `mutation_waits_for_matching_confirmation`, `rejection_changes_no_state`, `confirmation_is_single_use`, `confirmation_expires_at_five_minutes`, `cross_request_and_cross_session_ids_fail_closed`, and `altered_action_hash_fails_closed` using an injected clock/broker.

- [ ] **Step 3: Write failing sync-result tests**

  Assert successful mutations return installed state, while backend failure returns a safe `saved_not_installed` tool result and leaves the job visible with `sync_state = failed`.

- [ ] **Step 4: Run scheduler-tool tests and verify failure**

  Run: `cargo test --locked --test scheduler_tools`

  Expected: FAIL because the native executor is missing.

- [ ] **Step 5: Implement the executor and confirmation boundary**

  Parse arguments into `deny_unknown_fields` structs, produce the preview before any write, await the broker, then commit desired state and call explicit reconciliation. Do not expose scheduler definitions when constructing cron-run catalogs.

- [ ] **Step 6: Run scheduler-tool tests**

  Run: `cargo test --locked --test scheduler_tools`

  Expected: PASS.

- [ ] **Step 7: Commit**

  ```bash
  git add src/lib.rs src/tools/mod.rs src/tools/scheduler.rs tests/scheduler_tools.rs
  git commit -m "feat: add confirmed cron tools"
  ```

### Task 9: Inspection, export, and restricted DB shell

**Files:**
- Create: `src/inspection.rs`
- Create: `tests/inspection.rs`
- Create: `tests/db_shell.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: all Store query methods and protocol export DTOs.
- Produces: `InspectQuery::{Dialogs, History(Option<DialogId>), Jobs, Job(JobId), Runs(JobId), Audit, Dump}` and `InspectionService::inspect(query) -> Result<InspectionResult, InspectionError>`.
- Produces: versioned `LogicalExportV1` records and `InspectionService::write_export<W: Write>(&self, writer: &mut W) -> Result<ExportSummary, InspectionError>` with deterministic ordering and bounded memory.
- Produces: `ReadonlyDbShell::run<R: BufRead, W: Write>(input, output)`, accepting only SELECT, EXPLAIN, and safe read-only PRAGMAs after SQLite `Statement::readonly()` verification.

- [ ] **Step 1: Write failing completeness tests**

  Populate two dialogs plus completed/failed turns, active/deleted jobs, successful/missed runs, and tool rows. Assert `/dump` and export contain every persisted logical record, service events appear in history but not messages, and ordering is stable.

- [ ] **Step 2: Write failing secrecy and SQL tests**

  Assert export contains no API key, MCP URL credentials, raw arguments, provider body, or configuration secret. Reject INSERT, UPDATE, DELETE, ATTACH, writable PRAGMA, extension loading, multi-statement input, and dot/shell commands; accept parameter-free SELECT, EXPLAIN, and allowlisted read PRAGMAs against a `mode=ro` connection.

- [ ] **Step 3: Run inspection tests and verify failure**

  Run: `cargo test --locked --test inspection --test db_shell`

  Expected: FAIL because inspection is missing.

- [ ] **Step 4: Implement logical snapshots/export and restricted query output**

  Use DTOs and `serde_json::Serializer` rather than dumping the SQLite file or building the entire export in memory. Use the existing rusqlite `Statement::readonly()` API and do not enable SQLite extension loading.

- [ ] **Step 5: Run inspection tests**

  Run: `cargo test --locked --test inspection --test db_shell`

  Expected: PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add src/lib.rs src/inspection.rs tests/inspection.rs tests/db_shell.rs
  git commit -m "feat: add safe state inspection"
  ```

### Task 10: One-session stdio server

**Files:**
- Create: `src/server.rs`
- Create: `tests/server.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: Tasks 2, 3, 6, 8, and 9.
- Produces: `ServerDependencies { settings: Arc<ServerSettings>, store: Store, provider: Arc<dyn Provider>, mcp: Arc<dyn ToolExecutor>, synchronizer: Arc<CronSynchronizer>, inspection: InspectionService }`, `StdioServer::new(ServerDependencies)`, and `StdioServer::serve<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(reader, writer) -> Result<(), ServerError>`.
- Produces: session-local `SessionConfirmationBroker` that emits `ConfirmationRequired` and resolves only matching `ConfirmAction`/`CancelAction` requests.
- Produces: request handlers for dialog CRUD, send, inspect, and export; one active interactive task per session plus read-only requests.

- [ ] **Step 1: Write failing protocol-session tests**

  Add `hello_precedes_requests`, `dialog_crud_round_trip`, `send_streams_events_and_commits_answer`, `inspection_works_while_turn_is_busy`, `second_writer_gets_busy`, `confirmation_routes_only_to_originating_request`, and `export_is_sequence_numbered_base64_with_final_sha256` using duplex Tokio I/O.

- [ ] **Step 2: Add Review Focus disconnect/ownership tests**

  Add `eof_cancels_inflight_write_marks_uncertain_and_does_not_retry`, `restart_recovers_pending_turn_and_tools`, and `delete_dialog_with_live_job_is_rejected_but_inspection_still_works`.

- [ ] **Step 3: Run server tests and verify failure**

  Run: `cargo test --locked --test server`

  Expected: FAIL because `StdioServer` is missing.

- [ ] **Step 4: Implement reader, writer, and session coordinator**

  Use separate bounded input/output tasks and a `tokio::select!` coordinator so confirmation/cancel/read-only requests remain processable during a turn. Bind confirmation state to the in-memory session and request ID; drop it on EOF.

- [ ] **Step 5: Implement CRUD, inspection, and chunked export handlers**

  Emit only safe machine errors. Adapt `write_export` to a bounded 64 KiB chunking writer, sequence base64 chunks, and finish with total bytes plus SHA-256; never accept a server-side export path from the client or buffer the complete export.

- [ ] **Step 6: Run server tests**

  Run: `cargo test --locked --test server`

  Expected: PASS.

- [ ] **Step 7: Commit**

  ```bash
  git add src/lib.rs src/server.rs tests/server.rs
  git commit -m "feat: serve light agent over stdio"
  ```

### Task 11: macOS SSH terminal client

**Files:**
- Create: `src/remote_client.rs`
- Create: `src/terminal_client.rs`
- Create: `tests/remote_client.rs`
- Create: `tests/terminal_client.rs`
- Modify: `src/lib.rs`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `ClientSettings` and protocol types.
- Produces: `SshTransport::connect(&ClientSettings) -> Result<RemoteSession, ClientError>` using `tokio::process::Command` with argv, never a shell.
- Produces: `ClientAction::{Send, Exit, ListDialogs, NewDialog, OpenDialog, RenameDialog, DeleteDialog, History, Jobs, Job, Runs, Audit, Dump, Export}` and `parse_terminal_input(input: &str) -> Result<ClientAction, ClientInputError>`.
- Produces: `TerminalClient::run(session, input, output, error)`, exact `[y/N]` confirmation rendering, and atomic local export.

- [ ] **Step 1: Write failing SSH argv and command parser tests**

  Assert argv is exactly `ssh`, `-T`, configured host alias, fixed remote command; the child environment is cleared and only present values of `PATH`, `HOME`, `USER`, `LOGNAME`, `SSH_AUTH_SOCK`, `TMPDIR`, `LANG`, `LC_ALL`, `LC_CTYPE`, and `LC_MESSAGES` are restored; and commands `/dialogs`, `/new`, `/open`, `/rename`, `/delete`, `/history`, `/jobs`, `/job`, `/runs`, `/audit`, `/dump`, `/export`, and `/exit` parse strictly.

- [ ] **Step 2: Write failing rendering and confirmation tests**

  Assert text deltas stream without corrupting protocol output, control characters in IDs/names are escaped and bounded, the complete schedule/timezone/name/prompt are shown, only `y`/`Y` confirms, and EOF/default rejects.

- [ ] **Step 3: Add the atomic export test from Review Focus**

  Start with an existing destination, interrupt after one valid chunk, and assert the old file is unchanged and the `.part` file is removed. On complete hash match, assert atomic rename; on sequence/hash mismatch, reject and preserve the old file.

- [ ] **Step 4: Run client tests and verify failure**

  Run: `cargo test --locked --test remote_client --test terminal_client`

  Expected: FAIL because the client modules are missing.

- [ ] **Step 5: Implement system-SSH transport and terminal loop**

  Pipe child stdin/stdout, inherit or separately render stderr, apply the exact environment allowlist from Step 1, fail on protocol noise/version mismatch, and never retry a request after transport ambiguity. Keep active dialog state only as client presentation state; the VM remains authoritative.

- [ ] **Step 6: Implement safe atomic export**

  Create a sibling temporary file with `create_new`, stream decoded chunks while hashing, `sync_all`, verify byte count/hash, then rename. Clean up only the exact temporary file created by this request.

- [ ] **Step 7: Run client tests**

  Run: `cargo test --locked --test remote_client --test terminal_client`

  Expected: PASS.

- [ ] **Step 8: Commit**

  ```bash
  git add .gitignore src/lib.rs src/remote_client.rs src/terminal_client.rs tests/remote_client.rs tests/terminal_client.rs
  git commit -m "feat: add SSH terminal client"
  ```

### Task 12: Runtime commands and isolated cron execution

**Files:**
- Create: `src/bin/light_agent.rs`
- Create: `src/bin/light_agent_client.rs`
- Create: `tests/runtime_commands.rs`
- Create: `tests/cron_runner.rs`
- Modify: `Cargo.toml`
- Modify: `src/agent_runner.rs`
- Modify: `src/scheduler.rs`

**Interfaces:**
- Consumes: every runtime service from Tasks 1–11.
- Produces: server subcommands `serve-stdio`, `run-job JOB_ID`, `cron-sync`, and `db-shell --readonly`.
- Produces: `CronAgentService::run_job(job_id, cancellation) -> Result<CronRunOutcome, CronRunError>` with MCP-only tools and configured wall-clock timeout.
- Produces: `light-agent-client --config PATH` terminal entry point.

- [ ] **Step 1: Write failing Clap and startup tests**

  Assert exact subcommands/default paths, reject `db-shell` without `--readonly`, reject malformed job IDs before database access, keep stdout protocol-only for `serve-stdio`, and return nonzero safe failures from configuration/startup errors.

- [ ] **Step 2: Write failing isolated-run tests**

  Add `cron_request_contains_only_cron_system_prompt_and_saved_prompt`, `cron_catalog_has_mcp_but_no_cron_tools`, `result_is_stored_as_run_not_message`, `run_timeout_is_durable`, `overlap_records_skipped_without_provider_call`, and `once_at_claim_disables_before_agent_starts`.

- [ ] **Step 3: Add clock-controlled missed/DST tests**

  Verify a Moscow missed `once_at` is not late-run and a DST fallback recurring invocation permits only one active claim for the same job/scheduled minute.

- [ ] **Step 4: Run runtime tests and verify failure**

  Run: `cargo test --locked --test runtime_commands --test cron_runner`

  Expected: FAIL because binaries and `CronAgentService` are missing.

- [ ] **Step 5: Implement server/client binaries and cron service**

  `run-job` claims first, immediately makes once-at desired state disabled/pending and reconciles best-effort, runs the provider under the wall deadline, and stores the final result/error. Build the cron tool catalog from MCP only.

- [ ] **Step 6: Run runtime tests and binary smoke checks**

  Run: `cargo test --locked --test runtime_commands --test cron_runner && cargo build --locked --bins && target/debug/light-agent --help && target/debug/light-agent-client --help`

  Expected: all tests PASS; both help commands exit `0` and list only their intended interface.

- [ ] **Step 7: Commit**

  ```bash
  git add Cargo.toml src/agent_runner.rs src/scheduler.rs src/bin/light_agent.rs src/bin/light_agent_client.rs tests/runtime_commands.rs tests/cron_runner.rs
  git commit -m "feat: add light agent runtime commands"
  ```

### Task 13: Remove legacy paths, document deployment, and verify end to end

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/lib.rs`
- Modify: `README.md`
- Create: `deploy/light-agent/README.md`
- Create: `deploy/light-agent/authorized_keys.example`
- Create: `tests/day18_live.rs`
- Create: `docs/day18-results.md`
- Delete: `src/agent.rs`, `src/chat.rs`, `src/client.rs`, `src/config.rs`, `src/context.rs`, `src/debug_log.rs`, `src/dialog.rs`, `src/facts.rs`, `src/goal_definition.rs`, `src/invariants.rs`, `src/main.rs`, `src/memory.rs`, `src/profile.rs`, `src/system_context.rs`, `src/terminal.rs`, `src/tool_audit.rs`, `src/tool_calling.rs`, `src/workflow.rs`, `src/workflow_context.rs`, `src/workflow_engine.rs`, `src/workflow_model.rs`, `src/workflow_store.rs`, `src/bin/vkusvill_mcp.rs`
- Delete: `tests/agent.rs`, `tests/chat.rs`, `tests/cli.rs`, `tests/client.rs`, `tests/config.rs`, `tests/context.rs`, `tests/day17_live.rs`, `tests/debug_log.rs`, `tests/dialog.rs`, `tests/facts.rs`, `tests/goal_definition.rs`, `tests/invariants.rs`, `tests/mcp.rs`, `tests/memory.rs`, `tests/profile.rs`, `tests/tool_audit.rs`, `tests/tool_calling.rs`, `tests/workflow.rs`, `tests/workflow_context.rs`, `tests/workflow_engine.rs`, `tests/workflow_model.rs`, `tests/workflow_store.rs`
- Delete: `deepseek.example.toml`, `windows.example.toml`, `panels.example/`, `panels-day5.example/`

**Interfaces:**
- Consumes: the complete Day 18 implementation.
- Produces: a workspace whose only Rust product binaries are `light-agent` and `light-agent-client`; the Python Telegram MCP package remains unchanged and separately deployable.
- Produces: ignored opt-in live tests and exact VM/Mac deployment/acceptance instructions.

- [ ] **Step 1: Add ignored live acceptance tests before cleanup**

  Add `live_deepseek_no_tools`, `live_ssh_two_dialog_roundtrip`, `live_read_only_telegram_mcp`, `live_confirmed_cron_roundtrip`, and `live_reboot_recovery`. Require `LIGHT_AGENT_LIVE=1`; additionally require `LIGHT_AGENT_CRON_LIVE=1` before any test touches the VM crontab.

- [ ] **Step 2: Remove legacy modules, tests, examples, and dependencies**

  Set `autobins = false`, declare only the two binaries explicitly, set `default-run = "light-agent-client"`, and delete every alternate Agent/context/workflow/compaction path listed above. Remove terminal/layout and other dependencies no longer referenced. Keep historical docs and the `telegram_mcp` package.

- [ ] **Step 3: Write deployment and operator documentation**

  Document the dedicated `light-agent` account, owner-only config/secrets, `/var/lib/light-agent/state.sqlite3`, `/opt/light-agent/bin/light-agent`, Cronie preflight, fixed forced-command SSH key with forwarding/PTY disabled, Telegram loopback service, Mac client config, backup sensitivity, `/dump`, `/export`, and VM-only read-only DB shell. Make the authorized-keys example contain no real key or hostname.

- [ ] **Step 4: Run source and secrecy scans**

  Run:

  ```bash
  rg -n 'compact|summary|sticky|profile|workflow|invariant|goal_definition|memory layer' src tests Cargo.toml
  rg -n 'DEEPSEEK_API_KEY|api_key|telegram.*(token|hash)|prompt' deploy/light-agent/authorized_keys.example
  ```

  Expected: the first command finds no legacy implementation path (ordinary descriptive error text must be reviewed explicitly); the second finds no secret or prompt interpolation.

- [ ] **Step 5: Run all deterministic verification**

  Run:

  ```bash
  cargo fmt --check
  cargo clippy --locked --all-targets --all-features -- -D warnings
  cargo test --locked --all-targets --all-features
  uv sync --frozen --project telegram_mcp --group dev
  uv run --project telegram_mcp --group dev pytest telegram_mcp/tests -q
  git diff --check
  ```

  Expected: all commands exit `0`; Rust live tests are reported ignored; Python reports all Telegram MCP tests passing.

- [ ] **Step 6: Perform the real VM acceptance checklist**

  Build release binaries, install on the configured VM, run Cronie/forced-command preflight, then perform all 13 acceptance checks from the spec. Record commands, timestamps, safe outcomes, and any deliberately skipped live check in `docs/day18-results.md`; never paste secrets, prompts, or raw database contents.

- [ ] **Step 7: Re-run verification after documenting results**

  Run: the complete Step 5 command set plus `git status --short`.

  Expected: all checks still exit `0`; status lists only intended Day 18 files.

- [ ] **Step 8: Commit**

  ```bash
  git add -A
  git commit -m "feat: complete Day 18 light agent"
  ```
