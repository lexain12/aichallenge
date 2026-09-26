# Day 18 Light Agent design

## Status

Approved in conversation on 2026-09-26. This document specifies the Day 18
implementation boundary. Day 18 starts from Day 17 commit ce58940.

## Objective

Build a deliberately small agent that runs on a remote Linux VM while the
owner talks to it through a terminal client on a MacBook. The remote agent
uses DeepSeek, performs bounded tool calling, connects to Streamable HTTP MCP
servers, persists multiple dialog histories, and can create scheduled jobs
that launch independent agent processes through cron.

The design keeps the useful Day 17 provider, tool-loop, MCP, Telegram, error
sanitization, and audit work. It removes compaction and the orchestration
layers accumulated in Days 9 through 15.

## Success criteria

The system is successful when all of the following hold:

1. The Mac client connects through SSH and lists, creates, opens, renames, and
   deletes remote dialogs.
2. Dialog history survives client disconnects and VM process restarts.
3. DeepSeek answers stream to the terminal client.
4. The remote agent discovers configured MCP tools and executes a bounded
   tool conversation.
5. A natural-language scheduling request can produce a cron tool call, show
   an exact confirmation preview in the Mac client, and create the job only
   after the owner confirms it.
6. Cron invokes a new agent instance using only the saved job prompt, without
   loading the source dialog history.
7. Job results and safe tool audit records are inspectable from the Mac
   client.
8. A complete logical export shows every persisted dialog, message, job, run,
   and audit record while excluding credentials.
9. No feature silently compacts, summarizes, truncates, or drops dialog
   messages.
10. Test suites never call live DeepSeek, Telegram, or the owner's real
    crontab without explicit opt-in.

## Non-goals

Day 18 does not provide:

- a graphical Mac application;
- a public HTTP or WebSocket API;
- a continuously running agent daemon;
- multi-user authorization;
- conversation compaction or summaries;
- facts, profiles, durable semantic memory, or memory layers;
- workflow phases, controller loops, goals, invariants, or branching;
- automatic retries of writes whose delivery is unknown;
- arbitrary writable SQL access from the Mac client;
- recursive scheduling by cron-launched agents.

## System architecture

The repository remains one Rust workspace and produces two primary binaries.

### light-agent-client

The macOS terminal client owns presentation and transport only. It:

- launches the system ssh executable so OpenSSH configuration, keychain
  integration, known-host verification, ProxyJump, and Tailscale addresses
  continue to work normally;
- starts the remote command
  /opt/light-agent/bin/light-agent serve-stdio;
- exchanges versioned NDJSON messages over the child process stdin/stdout;
- renders dialogs, history, streamed assistant text, tool lifecycle events,
  cron confirmation previews, failures, and inspection output;
- writes an explicit export to a local path only when the owner asks.

The client never receives or stores the DeepSeek API key, Telegram
credentials, MCP credentials, or the remote SQLite file.

### light-agent

The Linux binary owns all trusted operations. Its command surface is:

- serve-stdio: serve one authenticated SSH client over stdin/stdout;
- run-job JOB_ID: claim and execute one scheduled job;
- cron-sync: reconcile active SQLite jobs with the managed crontab block;
- db-shell --readonly: open the local database read-only for emergency
  inspection on the VM.

One serve-stdio process exists per SSH connection. There is no listening
application socket and no application daemon. Cron-launched run-job
processes are independent of interactive connections.

### External services

DeepSeek is called only from the VM. MCP servers are configured on the VM.
The existing Telegram MCP remains a separate loopback-only service managed
by systemd. Other Streamable HTTP MCP endpoints can be configured without
changing the agent.

## SSH transport and protocol

The Mac client invokes the system ssh binary with a configured host alias and
the fixed remote serve-stdio command. The recommended production setup uses a
dedicated client key whose authorized_keys entry enforces that command and
does not grant a general shell.

stdout is reserved for one JSON object per line. Human diagnostics never
share stdout with protocol traffic. Safe operational diagnostics go to
stderr or journald.

Every client request contains:

- protocol_version;
- request_id;
- request type;
- a strictly validated payload.

Core requests are:

- list_dialogs;
- create_dialog;
- open_dialog;
- rename_dialog;
- delete_dialog;
- send_message;
- confirm_action;
- cancel_action;
- inspect;
- export.

Core server events are:

- hello;
- dialog_list;
- dialog_opened;
- response_started;
- text_delta;
- tool_started;
- tool_finished;
- confirmation_required;
- turn_completed;
- turn_failed;
- inspection_result;
- export_chunk;
- protocol_error.

Unknown fields and unknown message types are rejected. An inbound NDJSON line
is limited to 1 MiB, an individual message or cron prompt to 256 KiB, and an
export chunk to 64 KiB. The server may lower these limits through
configuration but never accepts an unbounded field. A protocol-version
mismatch fails before any state change.

Only one interactive turn may be active in a dialog. A second client trying
to write to the same dialog receives a busy response; read-only inspection
remains available.

## Dialog and context behavior

The VM stores multiple independent dialogs. The client can create, list,
open, rename, and delete them.

For a completed interactive turn, the provider receives:

1. the configured system prompt;
2. all completed user and assistant messages in the selected dialog;
3. the new user message.

There is no summary, compaction, sliding window, sticky facts, branch
selection, profile injection, or hidden memory. Transient assistant tool-call
messages and tool results exist only during the current tool loop and are not
persisted as dialog messages. Only the final assistant answer is persisted.

The request builder applies a deterministic 512 KiB maximum to the complete
serialized provider request, including the system prompt and tool schemas.
If the full dialog exceeds that boundary, or DeepSeek rejects it for context
length, the turn fails explicitly and the client recommends creating a new
dialog. The system never silently removes old messages.

The user message is persisted before the provider request. A turn record
tracks pending, completed, failed, or interrupted state. Only completed
turns enter later provider context. Failed and interrupted attempts remain
visible in inspection output without being replayed to the model.

Deleting a dialog is rejected while a non-deleted cron job references it.
After its jobs are deleted, dialog deletion removes its messages and turns in
one transaction. Historical cron job tombstones and runs retain their audit
data but lose the optional source-dialog link.

## Persistence

One SQLite database on the VM is the source of truth. WAL mode and bounded
busy timeouts allow interactive reads while cron jobs record results.

The logical schema contains:

### dialogs

- id;
- title;
- created_at;
- updated_at.

### turns

- id;
- dialog_id;
- status;
- safe_error_code;
- started_at;
- finished_at.

### messages

- id;
- dialog_id;
- turn_id;
- role limited to user or assistant;
- content;
- created_at.

### cron_jobs

- id;
- source_dialog_id, nullable only after its source dialog is deleted;
- name;
- schedule_kind limited to cron or once_at;
- schedule_value;
- timezone;
- prompt;
- desired_state limited to active, disabled, or deleted;
- sync_state limited to pending, applied, or failed;
- safe_sync_error_code;
- created_at;
- updated_at.

### cron_runs

- id;
- job_id;
- scheduled_for;
- status;
- result;
- safe_error_code;
- started_at;
- finished_at.

### tool_runs

- id;
- owner_kind limited to interactive_turn or cron_run;
- owner_id;
- call_id;
- server_name;
- tool_name;
- read_only;
- status;
- safe_error_code;
- started_at;
- finished_at.

Foreign keys, ownership checks, uniqueness constraints, and transactions
prevent cross-dialog linkage, duplicate call IDs within an owner, and partial
commits.

Tool arguments, raw MCP errors, provider error bodies, secrets, and
credentials are never stored in tool_runs.

## Inspection and export

The terminal client provides:

- /dialogs;
- /history [dialog-id];
- /jobs;
- /job JOB_ID;
- /runs JOB_ID;
- /audit;
- /dump;
- /export PATH.

/dump renders a complete human-readable logical snapshot. /export streams a
versioned JSON export from the VM and writes it to an explicitly selected
local Mac path. Both include dialogs, messages, turns, cron jobs, cron runs,
and safe tool audit rows. Neither includes configuration secrets.

The client does not expose arbitrary SQL. Emergency local inspection uses
light-agent db-shell --readonly. This is a VM-operator-only restricted query
REPL: it opens the database with SQLite mode=ro, permits SELECT, EXPLAIN, and
safe read-only PRAGMAs, and rejects writes, ATTACH, extension loading, and OS
shell escapes. It cannot be invoked through a model tool.

There are no other hidden knowledge stores in Day 18.

## Tool composition

The remote agent presents one provider tool catalog formed from:

- tools discovered through the existing immutable MCP registry;
- native scheduler tools registered under the cron namespace.

Provider aliases remain opaque routes. Tool names, arguments, call IDs,
round counts, read-only classification, timeouts, and safe error envelopes
retain the Day 17 validation rules.

Interactive sessions receive:

- cron__create;
- cron__list;
- cron__update;
- cron__disable;
- cron__delete.

Cron-launched run-job sessions receive MCP tools but never receive cron
tools. A scheduled prompt therefore cannot create, update, or delete another
schedule.

## Scheduler semantics

cron__create accepts a name, a self-contained prompt, and one of:

- a validated five-field recurring cron expression;
- a validated once_at timestamp.

The server, not the model, attaches source_dialog_id. Every schedule stores an
explicit IANA timezone. The default is Europe/Moscow.

Create, update, disable, and delete are persistent write operations. Before
execution the server emits confirmation_required with the exact normalized
schedule, timezone, name, and complete prompt. The Mac client asks the owner
to confirm. The write proceeds only when confirm_action references the
matching confirmation ID. Replayed, expired, altered, or cross-request
confirmations fail closed. A confirmation is single-use, bound to the live
SSH session and originating request, and expires after five minutes.

The prompt never becomes shell text. A managed crontab entry contains only
validated cron schedule fields, a validated timezone declaration, a fixed
absolute executable path, and the validated opaque job ID. Its command
portion is:

    /opt/light-agent/bin/light-agent run-job JOB_ID

cron-sync holds a process lock, reads active jobs from SQLite, renders the
entire managed crontab section, and installs it as one unit. SQLite remains
authoritative. Pending or failed reconciliation is visible in inspection and
is retried only by an explicit cron-sync or deployment recovery step.

Every schedule mutation first commits its desired_state with sync_state set
to pending. cron-sync changes sync_state to applied only after installing the
complete managed block; failure changes it to failed and returns a result
that explicitly says the desired change is saved but not installed. run-job
executes only jobs whose desired_state is active and whose sync_state is
applied. Disable and delete therefore fail closed even if a stale crontab
line remains. Delete is an auditable tombstone rather than immediate row
removal, so previous runs stay inspectable.

The deployment preflight verifies that the VM cron implementation honors the
timezone syntax used by the renderer. An unsupported implementation is a
deployment error; cron-sync does not silently reinterpret a job in the VM's
local timezone.

run-job atomically claims its job before calling DeepSeek. One job can have
at most one active run. An overlapping recurring invocation is recorded as
skipped. A once_at job is disabled when its single attempt is claimed, so a
crash cannot cause an automatic second attempt. A missed once_at invocation
is recorded as missed during the next cron-sync and is not executed late.

Each run has a configured wall-clock deadline. Completion, failure,
interruption, timeout, and skipped outcomes are durable.

The run receives only:

1. the configured cron system prompt;
2. the saved job prompt;
3. its allowed MCP catalog.

It never loads source-dialog messages. Its final answer is stored in
cron_runs and appears in the source dialog UI as a service event. It is not
inserted into messages and therefore does not enlarge later DeepSeek dialog
context. External notification happens only when the job prompt explicitly
asks the agent to call Telegram or another MCP tool.

## Failure and cancellation behavior

Provider, MCP, SQLite, protocol, SSH, and cron errors expose bounded machine
codes to the client. Raw bodies, URLs containing credentials, arguments, and
exception chains are operator-only data and are not sent through ordinary
client errors.

If SSH disconnects, the serve-stdio process cancels its current interactive
turn. On the next startup, any leftover pending turn becomes interrupted.
An already dispatched write may have succeeded, so pending write tool rows
become uncertain and are never retried automatically. Read-only pending
calls become failed.

The client may explicitly submit a new message after inspecting the turn and
audit status. It must not silently replay the previous request.

Output failure never synthesizes a final answer. A final assistant message
becomes completed history only after the complete answer and its turn status
commit atomically.

## Secrets and host permissions

- DeepSeek and MCP credentials exist only on the VM.
- Secret files are root-owned or owned by the dedicated light-agent account
  with restrictive modes.
- The Mac SSH key is separate from the administrative VM key.
- The client SSH key is restricted to the protocol command.
- The application account has no sudo access.
- The Telegram MCP remains loopback-only.
- The SQLite database, configuration, job prompts, and exports are treated as
  sensitive user data.
- Payload logging is disabled by default.
- log.jsonl, profiles, local configuration, databases, and build outputs stay
  outside Git.

## Day 17 reuse and removal

The implementation should extract and simplify, not add another layer around
the existing Agent.

Reuse and adapt:

- DeepSeek streaming and tool-call assembly from client.rs;
- provider message types needed by the tool loop;
- bounded ToolConversation behavior from tool_calling.rs;
- immutable MCP discovery, routing, timeouts, no-redirect behavior, and safe
  error conversion from mcp.rs;
- Telegram MCP and its tests;
- useful terminal block rendering that does not pull in old commands;
- the principle of metadata-only tool audit.

Replace with small Day 18 modules:

- configuration;
- provider messages;
- agent runner;
- SQLite store;
- NDJSON protocol;
- SSH client transport;
- scheduler;
- inspection/export;
- terminal command parsing.

Remove from the Day 18 build:

- context.rs;
- facts.rs;
- memory.rs;
- profile.rs;
- goal_definition.rs;
- invariants.rs;
- system_context.rs;
- workflow modules and workflow store;
- old dialog store;
- old monolithic agent;
- old CLI commands and workflow UI;
- compaction-specific dependencies and tests.

The old files may be deleted once their retained behavior has migrated and
the replacement tests pass. They must not remain as dead alternate paths.

## Testing strategy

All default tests are deterministic and isolated.

### Unit tests

- strict protocol parsing, version checks, IDs, and size limits;
- request and event round trips;
- context construction from completed turns only;
- no truncation or compaction;
- SQLite constraints, transactions, migrations, and concurrent reads;
- dialog CRUD and one-active-turn enforcement;
- inspection completeness and stable JSON export;
- scheduler schema validation and normalization;
- confirmation binding, expiry, replay rejection, and cancellation;
- crontab rendering without prompt or shell content;
- cron-sync idempotency and managed-block isolation;
- run claiming, overlap skipping, once_at at-most-once behavior, timeout, and
  missed execution;
- safe audit rows without arguments or payloads;
- crash recovery of pending turns, runs, and tool calls.

### Integration tests

- the Mac client against an in-process stdio server without real SSH;
- the server against mock DeepSeek SSE responses;
- MCP discovery and calls against mock Streamable HTTP clients;
- complete interactive tool loops;
- cron preview, confirmation, database commit, and fake crontab installation;
- independent run-job context containing the job prompt but no dialog history;
- disconnect and reconnect with durable history;
- full dump and export from a populated database.

### Live tests

Live DeepSeek, Telegram, SSH, VM crontab, and reboot tests are ignored by
default and require explicit environment opt-in. No default test sends a
Telegram message or edits the owner's real crontab.

## Deployment shape

The Linux release installs:

- /opt/light-agent/bin/light-agent;
- a dedicated light-agent account;
- an owner-only configuration and secret location;
- /var/lib/light-agent/state.sqlite3;
- the managed user crontab block;
- the existing loopback Telegram MCP systemd unit.

The macOS release installs light-agent-client and a small configuration that
names the SSH host alias and remote command. The Mac stores no agent database
or provider credentials.

The Day 17 secure VM deployment document remains the host-hardening base. Day
18 replaces the interactive Day 17 CLI installation steps with the two-binary
client/server layout and adds cron reconciliation and restricted SSH command
verification.

## Acceptance tests

Before Day 18 is declared complete:

1. Build both binaries from the lock file.
2. Connect from the Mac client through real SSH to the VM.
3. Create two dialogs, exchange messages in both, disconnect, reconnect, and
   verify exact separation and persistence.
4. Complete a live DeepSeek response with no tools.
5. Complete a read-only Telegram MCP call.
6. Ask the agent to create a schedule, verify the preview, reject it once,
   then confirm a second request.
7. Verify SQLite stores the prompt while crontab contains only the job ID.
8. Execute the job and prove the provider request contains the job prompt but
   no source-dialog history and no scheduler tools.
9. Verify the result appears in /runs and as a source-dialog service event.
10. Verify /dump and /export contain all logical state and no credentials.
11. Re-run cron-sync and prove that crontab entries are not duplicated.
12. Interrupt a write tool and prove that the audit reports uncertainty and
    no automatic retry occurs.
13. Reboot the VM and repeat SSH, history, MCP, cron-sync, and read-only
    inspection checks.

## Estimated effort

A production-credible first version is expected to require five to seven
focused engineering days. The scheduler, persistence boundaries, protocol,
and failure tests account for most of the work; removing compaction itself is
not the difficult part.
