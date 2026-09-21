# Day 13: Task State Machine and Stage-Isolated Context

## Goal

Add a durable, code-controlled task lifecycle to the existing DeepSeek CLI.
Each workflow task moves through `planning`, `execution`, `validation`, and
`done`; it also records the current step, expected action, structured plan,
and a compact checkpoint. Model calls may propose routing decisions, state
updates, and transitions, but only application code validates and applies
them.

The design must also isolate model context by task and stage. Full dialog
history and complete handoffs remain in SQLite for audit and replay, while an
ordinary model request receives only the active task-state projection and
messages assigned to the current stage.

## Existing Architecture and Naming

The current application already provides:

- persistent dialogs and raw messages in SQLite;
- `RequestScope { user_id, task_id, dialog_id }`;
- user-scoped and task-scoped durable memory;
- named `SystemBlock` values with scope and compaction policy;
- cumulative summary, sliding-window, sticky-facts, and branching context
  strategies;
- streamed model responses that are committed only after successful
  completion.

The existing `RequestScope.task_id` and `--task` argument identify the durable
working-memory namespace. They do not identify the new finite-state workflow
task. This distinction is required because one dialog may contain several
workflow tasks sequentially after earlier tasks reach `done`.

This document uses:

- **memory task** for the existing `RequestScope.task_id` namespace;
- **workflow task** for a new finite-state task inside a dialog;
- **stage run** for one entry into a phase. Re-entering `execution` after a
  failed validation creates another stage run rather than reusing the old one.

## Chosen Architecture

The design uses one dialog with task- and stage-tagged messages. It does not
create a new dialog for every phase or workflow task. A dialog may contain
multiple completed workflow tasks, but it may have at most one task whose
phase is not `done`.

The control flow has two model-assisted extension points:

1. An input router runs before the ordinary answer. It classifies the new user
   message as continuing the current task, replanning it, or starting a new
   task.
2. A response-processing pipeline runs after a complete ordinary answer. Its
   initial checker determines whether the current stage should remain active or
   propose a transition. Future checkers may enforce invariants or other
   policies and may use different models.

Checkers never mutate persistent state. They return typed proposals. A single
effect handler validates those proposals, asks the finite-state machine to
authorize transitions, builds a handoff when necessary, and persists the
result.

## Workflow State Model

### Phase and activity status

Phase and pause status are independent dimensions:

```rust
pub enum TaskPhase {
    Planning,
    Execution,
    Validation,
    Done,
}

pub enum TaskStatus {
    Active,
    Paused,
}
```

`done` is a terminal phase for ordinary stage transitions. Replanning a
completed task is an explicit input-routing decision, not an implicit result
of any new message.

### Current task projection

The current durable projection is conceptually:

```rust
pub struct WorkflowTaskState {
    pub id: WorkflowTaskId,
    pub dialog_id: i64,
    pub phase: TaskPhase,
    pub status: TaskStatus,
    pub goal: String,
    pub plan: TaskPlan,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub checkpoint: StageCheckpoint,
    pub current_stage_run_id: i64,
    pub incoming_handoff_id: Option<i64>,
    pub version: u64,
}
```

`version` is an optimistic-concurrency guard. A checker result produced from
version N cannot update version N+1.

The plan is structured rather than free-form:

```rust
pub struct TaskPlan {
    pub revision: u32,
    pub steps: Vec<PlanStep>,
    pub acceptance_criteria: Vec<String>,
}

pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub status: PlanStepStatus,
}

pub enum PlanStepStatus {
    Pending,
    InProgress,
    Completed,
    Blocked,
}
```

A plan change increments `revision`. A proposed plan change may add a repair
step after failed validation, but it cannot silently change the task goal,
remove an unfinished required step, or remove an acceptance criterion.
Completing a step requires checker-provided evidence accepted by the effect
handler.

## State Machines

### Stage machine

The allowed forward and repair transitions are:

```text
planning  -> execution
execution -> validation
validation -> done
validation -> execution
```

An explicit `ReplanCurrent` input decision moves the same workflow task from
`planning`, `execution`, `validation`, or `done` to a new `planning` stage run.
Even `planning -> planning` creates a new stage run so a changed goal cannot
silently reuse the previous planning context. A replan preserves the workflow
task ID, increments the plan revision, records the change request, and keeps
the previous task history immutable.

No generic `set_phase` operation is exposed. Transitions are typed events
handled by an exhaustive reducer with phase-specific guards. In particular,
`planning -> validation` and `execution -> done` are impossible.

Pause and resume do not create stage runs:

```text
any non-done phase / active --UserInterrupted--> same phase / paused
same phase / paused --ContinueCurrent--> same phase / active
same phase / paused --ReplanCurrent--> planning / active
```

### Dialog task machine

One dialog has at most one unfinished workflow task:

```text
no task + StartNewTask -> planning task
done task + StartNewTask -> new planning task in the same dialog
unfinished task + StartNewTask -> rejected
```

If a router proposes `StartNewTask` while the current task is in `planning`,
`execution`, `validation`, or paused, the ordinary model is not called. The
CLI reports that the current task must be completed or a new dialog opened.

A new dialog creates its first workflow task from its first accepted user
message. A dialog may therefore contain multiple workflow tasks over time,
but never multiple unfinished tasks.

## Input Routing

The input router runs before the user message is assigned to a task or sent to
the ordinary model. It receives the message and a compact view of the current
workflow task, not the full dialog history.

```rust
pub enum InputDecision {
    ContinueCurrent,
    StartNewTask { goal: String },
    ReplanCurrent { change_request: String },
}
```

The application validates the proposal against the dialog task machine. The
router cannot create a task or change state directly.

Safe failure behavior is:

- with an unfinished task, router failure falls back to `ContinueCurrent`;
- with no task, the first message deterministically starts a planning task;
- with a completed task, router failure leaves that task done and handles the
  message without creating or replanning a task;
- a low-confidence router decision must not automatically create a new task.

Routing must happen before the ordinary answer. Performing it afterward would
allow the first answer of a new task to be contaminated by the previous task's
context.

## Response-Processing Pipeline

### Interfaces

Checker implementations are pluggable and may use different completion
models:

```rust
pub trait CompletionModel {
    async fn complete(&self, request: ModelRequest)
        -> Result<ModelResponse, ModelError>;
}

pub trait ResponseChecker {
    async fn check(
        &self,
        context: &CheckContext,
        response: &str,
    ) -> Result<CheckResult, CheckError>;
}
```

The first implementation is `StageCompletionChecker`. Future implementations
may include invariant, safety, or quality checkers without receiving write
access to repositories.

A checker is either:

- **advisory**: failure leaves the response intact and task state unchanged;
- **blocking**: the response cannot be delivered when the checker fails or
  rejects it.

Day 13's stage checker is advisory, so ordinary response streaming remains
available. A future blocking output validator requires buffering the full
candidate response before display because already streamed content cannot be
retracted.

### Stage checker result

The stage checker returns either a patch within the current stage or a typed
transition proposal with evidence:

```rust
pub enum StageCheckDecision {
    Stay { patch: TaskStatePatch },
    ProposeTransition {
        event: TransitionEvent,
        patch: TaskStatePatch,
        evidence: Vec<String>,
    },
}
```

Saying "the work is complete" is not sufficient evidence by itself. Guards
may require a non-empty plan, completed required steps, stored artifacts, or
actual validation results. The state machine, not the checker, owns those
rules.

Checker results are parsed from a strict structured response. Invalid JSON,
unknown enum values, blank required fields, oversized payloads, and references
to unknown plan steps are rejected before effect application.

## Handoffs and State Projection

A handoff is generated only after the state machine accepts a transition. A
dedicated model call receives only the outgoing stage's task state and stage
messages. It returns a strict `HandoffPayload`, for example:

```json
{
  "summary": "The state-machine architecture is agreed.",
  "completed_step_ids": ["design-state-machine"],
  "next_step_id": "implement-storage",
  "expected_action": "Add SQLite persistence for workflow state.",
  "plan_changes": [],
  "decisions": [
    "Application code owns transitions.",
    "Response checkers only propose effects."
  ],
  "open_issues": []
}
```

The complete validated handoff is stored immutably with the transition for
audit. The effect handler also projects only the currently relevant fields
into `WorkflowTaskState`: goal, plan, current step, expected action, and
checkpoint. The state stores `incoming_handoff_id` rather than duplicating the
entire handoff.

The handoff model cannot choose `from_phase`, `to_phase`, the transition event,
or state version. Application code supplies and persists those fields.

If handoff generation or parsing fails, the ordinary answer remains saved but
the transition does not occur. The existing stage remains current and the
handoff attempt may be retried idempotently.

## Persistence

SQLite remains the source of truth. The conceptual schema adds:

### `workflow_tasks`

One row per finite-state task, including dialog ID, ordinal within the dialog,
phase, status, goal, plan JSON, current step, expected action, checkpoint JSON,
current stage-run ID, incoming handoff ID, version, and timestamps.

A partial unique index enforces at most one unfinished workflow task per
dialog. The invariant is therefore protected even if a caller bypasses the
normal router.

### `dialog_workflow_state`

One row per dialog pointing to the task currently selected for routing and
context construction. It may point to a done task until a later message starts
a new task or replans it.

### `task_stage_runs`

One row per entry into a phase. It stores the workflow task ID, phase, sequence
number, start and finish timestamps, and closed/open status. Re-entering a
phase creates a new row.

### `task_transitions`

An append-only transition ledger containing source and destination stage-run
IDs, event, source task-state version, full handoff JSON, triggering message,
and timestamp. A uniqueness constraint makes retrying the same accepted
transition idempotent.

### `message_task_stages`

Maps each ordinary user or assistant message to one workflow task and stage
run. A separate table avoids changing the established `messages` table and
allows existing databases to be upgraded safely.

### `response_processing`

Tracks checker work by assistant message: `pending`, `processing`, `completed`,
or `failed`, together with attempts, structured results, and the last error.
A completed assistant message and its pending processing row are committed in
one transaction.

Closing the outgoing stage run, inserting the transition and handoff, creating
the incoming stage run, updating the task projection, moving the dialog's
current-task pointer, and incrementing the state version occur in one SQLite
transaction. Partial transitions are impossible.

## Context Isolation

An ordinary request for a managed workflow task is assembled in this order:

1. base application prompt;
2. user profile;
3. user memory;
4. existing memory-task block;
5. current workflow-task state;
6. conversation messages belonging to the current task, current stage run,
   and current dialog;
7. the new user message.

The task-state block is workflow-task-scoped and excluded from conversation
compaction. It contains a compact rendering of phase, status, goal, structured
plan, current step, expected action, and checkpoint. `incoming_handoff_id` is
metadata for audit and is not expanded into another prompt block.

Messages from previous workflow tasks and stage runs remain in SQLite but are
not included. Previous stages enter the new context only through fields
projected from the validated handoff into the current task-state block. The
complete handoff is never duplicated into the ordinary prompt.

Existing conversation strategies apply inside the current stage run rather
than across the complete dialog. On a task or stage transition, the active
dialog summary and sticky-facts reduction boundaries are reset for the new
stage. Raw messages are not deleted. This reset is required because reusing a
dialog-wide cumulative summary would leak previous-stage context back into the
model request.

When branching mode copies a dialog, it must deep-copy the workflow-task
projection, open stage run, transition history, message mappings, and current
task pointer so the two branches evolve independently.

## End-to-End Flow

For an accepted user input:

1. On process restoration, retry pending response processing for the current
   state version before accepting another input. A repeated advisory failure
   is marked failed and leaves the last committed task state active.
2. Load the dialog's current workflow task.
3. Run or deterministically resolve input routing.
4. Validate the routing decision against the dialog task machine.
5. Apply resume, replan, or new-task initialization as required.
6. Persist the user message and its task/stage mapping together.
7. Build the stage-isolated ordinary request.
8. Stream the ordinary model response while retaining its complete buffer.
9. On complete success, persist the assistant message, usage, task/stage
   mapping, and pending processing row.
10. Run configured response checkers.
11. Pass typed results to the effect handler.
12. Apply an in-stage state patch or validate a proposed transition.
13. For an accepted transition, build and validate the handoff, then commit the
    full transition transaction.

## Pause and Resume

`Ctrl+C` is a user-interrupt event. The signal handler only requests
cancellation; it does not write SQLite directly.

When interruption occurs during input routing, an ordinary model call, a
checker call, or handoff generation:

- cancel the in-flight request;
- discard every partial model response from that request;
- do not create a response-processing job for an incomplete ordinary answer;
- preserve the last complete checkpoint and current stage run;
- persist `status = paused` for a non-done current task;
- exit cleanly.

Loading the dialog does not itself resume the task. The next accepted
`ContinueCurrent` message changes `paused` to `active` before ordinary context
assembly. `ReplanCurrent` instead creates a planning stage run and activates
the same workflow task. An invalid request to start another task leaves the
current task paused.

## Failure Semantics

- Ordinary model failure leaves the persisted user message in the current
  stage and does not advance task state.
- Input-router failure uses the safe fallbacks defined in Input Routing.
- Advisory-checker failure keeps the ordinary answer and current state.
- Invalid checker or handoff output is logged without applying effects.
- Handoff failure prevents only the transition, not storage of the ordinary
  answer.
- Illegal transitions return a local error and preserve the old state.
- Optimistic-version conflicts discard stale effects and reload state.
- Pending response-processing work is retried after restart before accepting
  later input for the same state version. A repeated advisory failure is
  recorded and may fail open for conversation while remaining closed to state
  mutation.
- Reprocessing the same assistant message cannot create a duplicate stage run
  or transition.
- Future blocking validators fail closed and require buffered delivery; Day 13
  advisory checks fail open with respect to the answer and closed with respect
  to state mutation.

## Observability

Debug events record checker name, configured model, advisory/blocking mode,
input and resulting state versions, proposed event, accepted/rejected outcome,
stage-run IDs, transition ID, processing status, and token usage. Payload text,
plan contents, checkpoints, and handoffs follow the existing
`debug.log_payloads` policy and are hidden by default.

The CLI task status view should show the active workflow-task ordinal and ID,
phase, pause status, plan revision, current step, expected action, stage-run
sequence, and whether response processing is pending or failed. It must not
present a proposed transition as committed.

## Testing

### Domain tests

- Every legal phase transition succeeds and every skipped transition fails.
- Pause and resume preserve phase and stage-run identity.
- Replan creates a new planning stage run and plan revision without changing
  workflow-task ID.
- A second unfinished task in one dialog is rejected.
- A new task is allowed after the previous task reaches done.
- Plan patches cannot remove required unfinished work or acceptance criteria.
- Stale task-state versions cannot apply effects.

### Parsing and pipeline tests

- Input routing parses all three decisions and rejects malformed output.
- Low-confidence or failed routing uses the defined safe fallback.
- Stage-checker JSON is strict and unknown events are rejected.
- Advisory failure preserves the response and leaves state unchanged.
- Checker implementations can use different `CompletionModel` instances.
- Conflicting checker proposals do not mutate state.
- Handoff parsing validates plan-step references and size limits.
- A valid handoff produces the expected compact task-state projection.

### Persistence tests

- Task creation and the first tagged user message commit atomically.
- A completed assistant message and pending processing row commit atomically.
- Accepted transition, handoff, stage-run closure/creation, task projection,
  and version increment commit atomically.
- Retrying processing is idempotent.
- Two sessions cannot create two unfinished tasks in one dialog.
- Resume restores task state, current stage run, and pending processing.
- Branch creation produces independent workflow-task state.

### Context tests

- The task-state block contains goal, plan, current step, expected action, and
  checkpoint in deterministic order.
- Messages from earlier stages and workflow tasks do not enter the request.
- Only the handoff-derived task-state projection represents the previous
  stage; the full handoff is absent from the request.
- Conversation summary and facts cannot reintroduce old-stage content.
- User profile, user memory, and memory-task context retain their established
  ordering and scope.

### Interruption tests

- `Ctrl+C` during an ordinary stream discards the partial assistant response.
- `Ctrl+C` during routing, checking, or handoff generation leaves the last
  complete checkpoint and persists pause.
- The first accepted continuation after restore resumes the same stage.
- An attempted new task while paused is rejected and does not resume or replace
  the current task.

## Scope Boundaries

Day 13 implements the task state machine, input router, generic response
checker boundary, stage-completion checker, handoff builder, persistence,
context isolation, pause/resume behavior, and diagnostics described above.

Future work may add blocking invariant checkers, transition policies beyond
the initial guards, richer model-routing configuration, human approval modes,
or background worker execution. Those features use the interfaces defined
here but are not implemented as part of this change.
