# Day 13: Task State Machine and Stage-Isolated Context

## Goal

Add a durable, code-controlled task lifecycle to the existing DeepSeek CLI.
Each workflow task moves through `planning`, `execution`, `validation`, and
`done`; it also records the current step, expected action, structured plan,
and a compact checkpoint. Real user messages and controller-generated inputs
use the same workflow-input boundary. Model calls may propose routing
decisions, state updates, and transitions, but only application code validates
and applies them.

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

1. A human-input interpreter runs before the ordinary answer. It turns the new
   user message into a typed workflow intent: continue, start a task, replan,
   or propose a stage transition.
2. A response-processing pipeline runs after a complete ordinary answer. Its
   initial continuation checker either waits for a person or emits a synthetic
   controller input that continues the current stage or proposes a transition.
   Future checkers may enforce invariants or other policies and may use
   different models.

Human and controller inputs pass through the same workflow-input handler.
Interpreters and checkers never mutate persistent state. They return typed
proposals. The handler validates those proposals, asks the finite-state
machine to authorize transitions, builds a handoff when necessary, and
persists the result.

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
same phase / paused --human Continue--> same phase / active
same phase / paused --human ProposeTransition--> accepted target / active
same phase / paused --human ReplanCurrent--> planning / active
```

### Dialog task machine

One dialog has at most one unfinished workflow task:

```text
no task + StartNewTask -> planning task
done task + StartNewTask -> new planning task in the same dialog
unfinished task + StartNewTask -> rejected
```

If the human-input interpreter proposes `StartNewTask` while the current task
is in `planning`, `execution`, `validation`, or paused, the ordinary model is
not called. The CLI reports that the current task must be completed or a new
dialog opened.

A new dialog creates its first workflow task from its first accepted user
message. A dialog may therefore contain multiple workflow tasks over time,
but never multiple unfinished tasks.

## Unified Workflow Inputs

Every state-affecting input has an explicit source and a typed intent:

```rust
pub struct WorkflowInput {
    pub source: WorkflowInputSource,
    pub intent: WorkflowIntent,
}

pub enum WorkflowInputSource {
    Human,
    Controller {
        checker: String,
        model: String,
        triggering_assistant_message_id: i64,
    },
}

pub enum WorkflowIntent {
    Continue { instruction: String },
    StartNewTask { goal: String },
    ReplanCurrent { change_request: String },
    ProposeTransition {
        event: TransitionEvent,
        evidence: Vec<String>,
    },
}
```

`WorkflowInputHandler` is the only state-affecting entry point. It validates
the intent against the dialog and stage machines, applies resume/replan/new
task initialization, invokes the handoff builder for an accepted transition,
and prepares the resulting stage context for the next ordinary model call.

### Human input interpretation

The human-input interpreter runs before the human message is assigned to a
task or sent to the ordinary model. It receives the raw message and a compact
view of the current workflow task, not the full dialog history. It may detect a
transition directly. For example, "implementation is done; start testing" may
produce `ProposeTransition { event: ExecutionCompleted, ... }` before the
ordinary answer. If the finite-state machine accepts it, the same human
message is persisted in and answered from the new validation stage.

Safe failure behavior is:

- with an unfinished task, interpreter failure falls back to `Continue` with
  the original human message as its instruction;
- with no task, the first message deterministically starts a planning task;
- with a completed task, interpreter failure leaves that task done and handles
  the message without creating or replanning a task;
- a low-confidence interpretation must not automatically create a new task or
  transition a stage.

Interpretation must happen before the ordinary answer. Performing it afterward
would let a new task or new stage receive an answer contaminated by the
previous context.

### Controller inputs

The continuation checker may emit only `Continue` or `ProposeTransition`.
It cannot create an independent task, change the user's goal, or invoke
`ReplanCurrent`. Those intents require a real human message.

A controller input is rendered to the model as a user-role instruction so the
ordinary conversation protocol remains well formed. It is persisted with
`source = controller`, hidden from the human transcript, excluded from dialog
titles and user-fact extraction, and retained in audit data. The application
must never attribute controller text to the person.

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

The first implementation is `ContinuationChecker`. Future implementations may
include invariant, safety, or quality checkers without receiving write access
to repositories.

A checker is either:

- **advisory**: failure leaves the response intact and task state unchanged;
- **blocking**: the response cannot be delivered when the checker fails or
  rejects it.

Day 13's continuation checker is advisory, so ordinary response streaming
remains available. A future blocking output validator requires buffering the
full candidate response before display because already streamed content cannot
be retracted.

### Continuation checker result

The continuation checker returns a task-state patch plus one of three control
decisions:

```rust
pub struct ContinuationCheckResult {
    pub patch: TaskStatePatch,
    pub decision: ControllerDecision,
}

pub enum ControllerDecision {
    AwaitUser,
    Continue {
        instruction: String,
        confidence: f32,
    },
    EmitTransition {
        event: TransitionEvent,
        evidence: Vec<String>,
        confidence: f32,
    },
}
```

`AwaitUser` persists the accepted patch and returns control to the terminal
prompt. `Continue` and `EmitTransition` are converted into controller-sourced
`WorkflowInput` values and passed through the same handler used for human
input. The controller never applies a patch or transition directly.

Saying "the work is complete" is not sufficient evidence by itself. Guards
may require a non-empty plan, completed required steps, stored artifacts, or
actual validation results. The state machine, not the checker, owns those
rules.

Checker results are parsed from a strict structured response. Invalid JSON,
unknown enum values, blank required fields, oversized payloads, and references
to unknown plan steps are rejected before effect application.

### Autonomous continuation loop

After an ordinary assistant response:

1. Run the continuation checker on the current task state, current stage
   context, triggering input, and complete assistant response.
2. Validate and persist its `TaskStatePatch`.
3. On `AwaitUser`, stop and display the normal prompt.
4. On `Continue`, persist a hidden controller input and call the ordinary
   model again in the same stage.
5. On `EmitTransition`, pass the synthetic transition input through the state
   machine, build the handoff, enter the accepted stage, and call the ordinary
   model with the new task-state context.
6. Repeat until the controller returns `AwaitUser`, the task reaches `done`, a
   safety limit is reached, a call fails, or the person presses `Ctrl+C`.

Continuous execution has finite, validated limits. The initial configuration
defaults are eight autonomous ordinary turns and 20,000 total API tokens after
one human input. A repeated `(task version, phase, current step,
expected action)` fingerprint, low-confidence controller result, malformed
result, or exhausted budget forces `AwaitUser`. The controller cannot start a
new task after reaching `done`; only a later human input may do that.

## Handoffs and State Projection

A handoff is generated only after the state machine accepts a transition. A
dedicated model call receives only the outgoing stage's task state, stage
messages, and the accepted workflow input that triggered the transition. It
returns a strict `HandoffPayload`, for example:

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

### `workflow_inputs`

Links every accepted user-role protocol message to its interpreted intent and
origin. Human rows point to visible user messages. Controller rows point to
hidden user-role messages and record checker name, model, triggering assistant
message, structured intent, confidence, and accepted/rejected status. Legacy
user messages without a row are treated as visible human input.

This table is the audit boundary that prevents a synthetic continuation from
being mistaken for something the person said. Context assembly includes an
accepted controller message in its stage sequence, while transcript replay,
dialog titles, user-memory extraction, and sticky user-fact extraction exclude
it.

### `response_processing`

Tracks checker work by assistant message: `pending`, `processing`, `completed`,
or `failed`, together with attempts, structured results, and the last error.
A completed assistant message and its pending processing row are committed in
one transaction. A controller decision and its hidden protocol message are
also committed together before the next ordinary model call.

Accepting a transition input, closing the outgoing stage run, inserting the
transition and handoff, creating the incoming stage run, updating the task
projection, moving the dialog's current-task pointer, and incrementing the
state version occur in one SQLite transaction. Partial transitions are
impossible.

## Context Isolation

An ordinary request for a managed workflow task is assembled in this order:

1. base application prompt;
2. user profile;
3. user memory;
4. existing memory-task block;
5. current workflow-task state;
6. visible human, hidden controller, and assistant protocol messages belonging
   to the current task, current stage run, and current dialog, in stable order;
7. the new human or controller input when it has not yet been persisted.

The task-state block is workflow-task-scoped and excluded from conversation
compaction. It contains a compact rendering of phase, status, goal, structured
plan, current step, expected action, and checkpoint. `incoming_handoff_id` is
metadata for audit and is not expanded into another prompt block.

Messages from previous workflow tasks and stage runs remain in SQLite but are
not included. Previous stages enter the new context only through fields
projected from the validated handoff into the current task-state block. The
complete handoff is never duplicated into the ordinary prompt.

Controller inputs participate in the model protocol only inside their task and
stage. They may be included in stage-local summary compaction as execution
instructions, but they are never evidence for user profile, user memory, task
memory, or sticky user facts.

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

For one real human input and every autonomous continuation it triggers:

1. On process restoration, retry pending response processing for the current
   state version before accepting another input. A repeated advisory failure
   is marked failed and leaves the last committed task state active.
2. Load the dialog's current workflow task.
3. For a human message, run or deterministically resolve human-input
   interpretation. For an autonomous turn, load the already typed controller
   intent.
4. Pass the resulting `WorkflowInput` through the shared handler and validate
   it against the dialog and stage machines.
5. Apply resume, replan, new-task initialization, or an accepted stage
   transition. Build and project a handoff before entering a new stage.
6. Persist the visible human or hidden controller user-role message, its typed
   workflow input, and its task/stage mapping together.
7. Build the stage-isolated ordinary request from the resulting state.
8. Stream the ordinary model response while retaining its complete buffer.
9. On complete success, persist the assistant message, usage, task/stage
   mapping, and pending processing row.
10. Run configured response checkers.
11. Validate and persist the checker-proposed in-stage state patch.
12. On `AwaitUser`, complete processing and return to the terminal prompt.
13. On `Continue` or `EmitTransition`, atomically persist the hidden controller
    input, enforce the autonomy limits, and return to step 4 without waiting
    for a person.

## Pause and Resume

`Ctrl+C` is a user-interrupt event. The signal handler only requests
cancellation; it does not write SQLite directly.

When interruption occurs during human-input interpretation, an ordinary model
call, a checker call, a controller turn, or handoff generation:

- cancel the in-flight request;
- discard every partial model response from that request;
- do not create a response-processing job for an incomplete ordinary answer;
- preserve the last complete checkpoint and current stage run;
- persist `status = paused` for a non-done current task;
- exit cleanly.

Loading the dialog does not itself resume the task. The next accepted
human-sourced `Continue` or `ProposeTransition` activates the task before
ordinary context assembly. `ReplanCurrent` instead creates a planning stage
run and activates the same workflow task. Controller input cannot resume a
task after process restoration without a new human message. An invalid request
to start another task leaves the current task paused.

## Failure Semantics

- Ordinary model failure leaves the persisted user message in the current
  stage and does not advance task state.
- Human-input interpreter failure uses the safe fallbacks defined in Unified
  Workflow Inputs.
- Advisory-checker failure keeps the ordinary answer and current state.
- A continuation-checker failure stops the autonomous loop and waits for a
  person; it never guesses a synthetic instruction.
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
  or transition, nor emit a second controller input.
- Reaching an autonomous turn/token limit or detecting a repeated state
  fingerprint stops the loop without changing the task phase.
- Future blocking validators fail closed and require buffered delivery; Day 13
  advisory checks fail open with respect to the answer and closed with respect
  to state mutation.

## Observability

Debug events record workflow-input source, interpreter or checker name,
configured model, advisory/blocking mode, input and resulting state versions,
proposed event, accepted/rejected outcome, autonomous turn number and budget,
stage-run IDs, transition ID, processing status, and token usage. Payload text,
plan contents, checkpoints, synthetic instructions, and handoffs follow the
existing `debug.log_payloads` policy and are hidden by default.

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

- Human-input interpretation parses all four intents and rejects malformed
  output.
- A human message may trigger an accepted stage transition before the ordinary
  answer is generated.
- Low-confidence or failed interpretation uses the defined safe fallback.
- Continuation-checker JSON is strict and unknown events are rejected.
- `AwaitUser` ends the autonomous loop without changing phase.
- `Continue` emits a hidden controller input in the same stage.
- `EmitTransition` goes through the shared workflow-input handler and FSM.
- A controller cannot create a new task or replan the current task.
- Turn/token exhaustion and repeated-state detection stop autonomous work.
- Advisory failure preserves the response and leaves state unchanged.
- Checker implementations can use different `CompletionModel` instances.
- Conflicting checker proposals do not mutate state.
- Handoff parsing validates plan-step references and size limits.
- A valid handoff produces the expected compact task-state projection.

### Persistence tests

- Task creation and the first tagged user message commit atomically.
- A completed assistant message and pending processing row commit atomically.
- A controller decision, hidden user-role message, origin metadata, and
  task/stage mapping commit atomically.
- Accepted transition, handoff, stage-run closure/creation, task projection,
  and version increment commit atomically.
- Retrying processing is idempotent.
- Retrying processing cannot emit a duplicate controller input.
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
- Hidden controller inputs remain in protocol order but are excluded from
  transcript display and user-fact extraction.
- User profile, user memory, and memory-task context retain their established
  ordering and scope.

### Interruption tests

- `Ctrl+C` during an ordinary stream discards the partial assistant response.
- `Ctrl+C` during human-input interpretation, checking, autonomous
  continuation, or handoff generation leaves the last complete checkpoint and
  persists pause.
- The first accepted continuation after restore resumes the same stage.
- An attempted new task while paused is rejected and does not resume or replace
  the current task.

## Scope Boundaries

Day 13 implements the task state machine, unified workflow-input handler,
human-input interpreter, generic response-checker boundary, continuation
checker, bounded autonomous loop, handoff builder, persistence, context
isolation, pause/resume behavior, and diagnostics described above.

Future work may add blocking invariant checkers, transition policies beyond
the initial guards, richer model-routing configuration, human approval modes,
or background worker execution. Those features use the interfaces defined
here but are not implemented as part of this change.
