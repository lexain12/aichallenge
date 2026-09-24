# Goal Definition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable `goal_definition` phase so a human can discuss, approve, and later reopen a workflow task's goal without bypassing the state machine or invariants.

**Architecture:** Reuse the existing workflow task, stage-run, input, and transition machinery. Store one active proposal as a relational projection of an assistant message; only a human `ApproveGoal` input may promote it into the task goal. Keep approval/reopen operations transactional and guarded by task version and stage ID. The response pipeline must check both the candidate answer and its extracted proposal before showing either. Existing tasks remain approved in their current phases.

**Tech Stack:** Rust, `rusqlite`, `serde`, Tokio tests, existing model/checker abstractions and JSONL diagnostics.

**Spec:** [2026-09-23-goal-definition-design.md](../specs/2026-09-23-goal-definition-design.md)

## Global Constraints

- Preserve current unrelated edits in `README.md`, `src/debug_log.rs`, `src/workflow_engine.rs`, `tests/agent.rs`, `tests/debug_log.rs`, and untracked `log.jsonl`. Inspect their diff before touching overlapping files; do not discard, stage, or commit `log.jsonl`.
- Do not push: the previous destination `git@github.com:lexain12/aichallenge.git` was not confirmed as user-owned. A local implementation does not imply authorization to publish.
- No model or controller may approve or reopen a goal. A checker decision is not a state-machine transition.
- `debug.log_payloads = false` must exclude free-form model text, proposal text, and raw rejection text; only safe identifiers/outcomes go to JSONL. Never log API keys.
- `RequestScope.task_id` is a memory namespace, not `WorkflowTaskId`; do not conflate them.
- Maintain strict stage isolation. The new stage may receive the previous approved goal and the human reopen request through explicit handoff, but not the old raw protocol.
- Apply TDD at each task: write the focused failing test, run it, implement the minimum, then rerun. Use `cargo fmt --check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings` before claiming completion.

## Review Focus

1. A conditional approval (`да, но без авторизации`) must not approve the old proposal; pin to `tests/workflow_model.rs` and `tests/workflow_engine.rs` in Task 4.
2. An old `да` after another human turn, failed model answer, or replacement proposal must not approve stale text; pin to `tests/workflow_store.rs` in Task 5.
3. Zero, duplicate, blank, multiline, or oversized `Предлагаемая цель:` lines must never produce a proposal; pin to `tests/goal_definition.rs` in Task 2.
4. Pause/resume, restart, branching, stale version, and replay must not invent an approval or bind to another branch's message; pin to `tests/workflow_store.rs` in Tasks 5 and 7.
5. Migrating an existing database must preserve old IDs, FKs, transitions, stage context, and processing jobs while accepting `goal_definition`; pin to `tests/workflow_store.rs` in Task 3.

## File and interface map

| File | Responsibility |
| --- | --- |
| `src/workflow.rs` | Phase, intent, proposal and human-only authorizations; pure state guards. |
| `src/goal_definition.rs` | Pure extraction of exactly one visible goal line. |
| `src/workflow_model.rs` | LLM intent DTO/prompt and safe fallback; no state mutation. |
| `src/workflow_store.rs` | SQLite migration, atomic proposal/approval/reopen, branch remapping. |
| `src/workflow_engine.rs` | Orchestration of interpreter, ordered invariant gates, generation, persistence. |
| `src/workflow_context.rs` | Stage-scoped model instructions and explicit reopen context. |
| `src/agent.rs`, `src/terminal.rs`, `src/debug_log.rs` | Status/debug projection and safe JSONL. |
| `tests/*.rs`, `README.md` | Regression proofs and operator-facing lifecycle. |

The domain/store boundary will use these exact new interfaces. `GoalProposal` is a loaded projection, not an independently mutable in-memory authorization. The authorization structs capture the exact source to recheck in SQLite:

```rust
pub struct GoalApprovalAuthorization {
    pub task_id: WorkflowTaskId,
    pub source_version: u64,
    pub source_stage_run_id: StageRunId,
    pub assistant_message_id: i64,
    pub goal_text: String,
}
pub struct GoalReopenAuthorization {
    pub task_id: WorkflowTaskId,
    pub source_version: u64,
    pub source_stage_run_id: StageRunId,
    pub next_plan_revision: u32,
    pub change_request: String,
}
pub fn authorize_goal_approval(
    state: &WorkflowTaskState,
    source: &WorkflowInputSource,
) -> Result<GoalApprovalAuthorization, WorkflowError>;
pub fn authorize_goal_reopen(
    state: &WorkflowTaskState,
    source: &WorkflowInputSource,
    change_request: String,
) -> Result<GoalReopenAuthorization, WorkflowError>;
```

Store methods consume `InputCommit` plus one authorization, returning `PersistedTransition`. The engine never writes `goal` directly. An invariant-rejected approval is stopped before the input is persisted and leaves the proposal available for a safe retry; a locally rejected, persisted approval counts as a non-approval turn and invalidates it. This distinction avoids both accidental approval and an unexplainable loss of an otherwise valid proposal during checker outage.

---

## Task 1: Domain transition contract

**Files:** Modify `src/workflow.rs`; test `tests/workflow.rs`.

**Interfaces:** Produces `GoalProposal`, `GoalApprovalAuthorization`, `GoalReopenAuthorization`, `authorize_goal_approval`, `authorize_goal_reopen`, and the two new `WorkflowIntent` variants for Tasks 3–7. Consumes only existing IDs and `WorkflowInputSource`.

- [ ] Add failing tests for initial phase `GoalDefinition`, no plan mutations there, no direct `GoalDefinition → Execution/Validation/Done`, human-only approval/reopen, and `Done`/`GoalDefinition` rejecting replan.

  Pin the first guard in `tests/workflow.rs` with this test (extend its existing imports):

  ```rust
  #[test]
  fn goal_definition_cannot_skip_planning() {
      let task = WorkflowTaskState::new(
          WorkflowTaskId(1), 2, 1, "Build parser".into(), StageRunId(3),
      ).unwrap();
      assert_eq!(task.phase, TaskPhase::GoalDefinition);
      assert!(task.plan.steps.is_empty());
      for event in [
          TransitionEvent::PlanningCompleted,
          TransitionEvent::ExecutionCompleted,
          TransitionEvent::ValidationPassed,
          TransitionEvent::ValidationFailed,
      ] {
          assert!(StateMachine::authorize(&task, event, &[]).is_err());
      }
  }
  ```
- [ ] Run `cargo test --test workflow goal_definition`; expected failure is the missing phase/intent/authorization contract, not an unrelated compiler error.
- [ ] Extend `TaskPhase` and `WorkflowIntent`, with explicit authorization types. Use the current proposal ID/stage/version as approval preconditions; do not let `ProposeTransition` represent approval:

  ```rust
  pub enum TaskPhase { GoalDefinition, Planning, Execution, Validation, Done }
  pub enum WorkflowIntent {
      Continue { instruction: String },
      StartNewTask { goal: String },
      ApproveGoal,
      ReopenGoal { change_request: String },
      ReplanCurrent { change_request: String },
      ProposeTransition { event: TransitionEvent, evidence: Vec<String> },
  }
  pub struct GoalProposal {
      pub text: String,
      pub assistant_message_id: i64,
      pub stage_run_id: StageRunId,
  }
  ```

- [ ] Add `goal_revision: u32` and `goal_proposal: Option<GoalProposal>` to `WorkflowTaskState`; `new` starts at `GoalDefinition`, revision zero, no proposal. Validate proposal only in the current goal-definition stage. Existing restored tasks receive a migrated approved revision and no proposal. The state also appears inside historical JSON audit results: deserialize a missing revision as 1 and a missing proposal as `None`, and omit those legacy-default values on serialization so `transition_processing_result_preserves_existing_json_shape` keeps passing.
- [ ] Make `StateMachine::validate_source` reject controller `ApproveGoal`/`ReopenGoal`; add `authorize_goal_approval` and `authorize_goal_reopen` returning typed authorizations with source task/version/stage. Approval requires an active proposal; reopen requires an active, unfinished, approved later phase. `authorize_replan` rejects `GoalDefinition` and `Done`. Reject plan append/step updates in `GoalDefinition` even via `TaskStatePatch`.
- [ ] Run `cargo test --test workflow`; update exhaustive matches in dependent modules only enough to compile this task. Do not weaken guards to make tests pass.
- [ ] Commit only `src/workflow.rs` and `tests/workflow.rs` with `git commit -m "Day 15: define goal lifecycle guards"` after the focused suite is green.

## Task 2: Visible proposal parser and prompt contract

**Files:** Add `src/goal_definition.rs`, `tests/goal_definition.rs`; modify `src/lib.rs`, `src/workflow_context.rs`.

**Interfaces:** Produces `GOAL_PROPOSAL_PREFIX` and `parse_goal_proposal(&str) -> Result<Option<String>, GoalProposalParseError>` for Task 6. Consumes `MAX_WORKFLOW_TEXT_CHARS` from `workflow.rs`.

- [ ] Add failing table tests for exactly one standalone `Предлагаемая цель: <текст>` line and the five malformed classes in Review Focus #3. Accept ordinary surrounding explanation but never parse a line inside a fenced code block as a proposal. Do not normalize the accepted text beyond trimming boundary whitespace; the stored text must equal what is shown.

  The core table in `tests/goal_definition.rs` must include these exact cases:

  ```rust
  assert_eq!(parse_goal_proposal("Обсудим\nПредлагаемая цель: Сделать CLI\n"), Ok(Some("Сделать CLI".into())));
  assert_eq!(parse_goal_proposal("Обсудим варианты"), Ok(None));
  assert!(parse_goal_proposal("Предлагаемая цель: ").is_err());
  assert!(parse_goal_proposal("Предлагаемая цель: A\nПредлагаемая цель: B").is_err());
  assert_eq!(parse_goal_proposal("```\nПредлагаемая цель: Не цель\n```"), Ok(None));
  ```
- [ ] Implement a pure parser returning `Result<Option<String>, GoalProposalParseError>`; `None` means discussion without a proposal, `Err` means an attempted but malformed proposal. Bound by `MAX_WORKFLOW_TEXT_CHARS` and reject control characters/newlines in the proposed text.

  ```rust
  pub const GOAL_PROPOSAL_PREFIX: &str = "Предлагаемая цель:";
  pub fn parse_goal_proposal(answer: &str)
      -> Result<Option<String>, GoalProposalParseError>;
  ```

- [ ] Add a `GoalDefinition`-specific system block in `prepare_workflow_request`: explain that the working goal is not approved, ask for clarification if needed, show a candidate on its own exact-prefix line, and do not plan/implement. Keep it scoped to the current stage and compatible with inherited invariants.
- [ ] Run `cargo test --test goal_definition` and `cargo test --test workflow_context`.
- [ ] Commit these four files with `git commit -m "Day 15: parse visible goal proposals"` after the focused suites are green.

## Task 3: SQLite schema migration and legacy projection

**Files:** Modify `src/workflow_store.rs`; test `tests/workflow_store.rs`.

**Interfaces:** Produces `goal_revision`/`goal_proposals` SQL projection and load/save support for the Task 5 store methods. Consumes `WorkflowTaskState::goal_revision` and `goal_proposal` from Task 1.

- [ ] Create an old-schema fixture with rows in `workflow_tasks`, `task_stage_runs`, `task_stage_context`, `task_transitions`, and pending `response_processing`; write failing migration tests that inspect IDs/FKs, phase, and pending jobs after reopening the store.

  Extend the existing legacy fixture (do not construct an impossible transition: use a valid old planning/execution pair). After `DialogStore::open`, run these exact assertions against a read connection:

  ```rust
  assert_eq!(connection.query_row("PRAGMA foreign_key_check", [], |row| row.get::<_, i64>(0)).optional().unwrap(), None);
  assert_eq!(connection.query_row("SELECT phase FROM workflow_tasks WHERE id = ?1", [legacy_task_id], |row| row.get::<_, String>(0)).unwrap(), "execution");
  assert_eq!(connection.query_row("SELECT status FROM response_processing WHERE id = ?1", [pending_id], |row| row.get::<_, String>(0)).unwrap(), "pending");
  ```
- [ ] Version the workflow schema migration, not merely `CREATE TABLE IF NOT EXISTS`. Rebuild `workflow_tasks`, `task_stage_runs`, and `task_transitions` where necessary to extend their phase/event `CHECK`s. SQLite `PRAGMA foreign_keys=OFF` must be set before opening the migration transaction; restore it after committing, run `PRAGMA foreign_key_check` before commit, and roll back on failure. Preserve columns, IDs, indexes, FKs, and `sqlite_sequence`. Follow the project's existing migration entry point, and test migration twice for idempotence.
- [ ] Add `goal_revision INTEGER NOT NULL DEFAULT 1 CHECK (goal_revision >= 0)` to tasks, backfill old tasks to revision 1, and initialize new goal-definition tasks at revision 0. Add a `goal_proposals` table keyed by task ID, with `stage_run_id` and `assistant_message_id` FKs; require one active proposal per task. Do not attempt to infer a proposal from old messages.

  ```sql
  CREATE TABLE goal_proposals (
      workflow_task_id INTEGER PRIMARY KEY REFERENCES workflow_tasks(id),
      stage_run_id INTEGER NOT NULL REFERENCES task_stage_runs(id),
      assistant_message_id INTEGER NOT NULL REFERENCES messages(id),
      text TEXT NOT NULL CHECK (length(trim(text)) > 0)
  );
  ```

- [ ] Extend row loading/saving and `WorkflowTaskState` serialization so legacy tasks load as approved with no proposal. Ensure new task creation inserts `goal_definition` into both task/stage tables and has an empty plan. Add an assertion that the legacy audit JSON round-trips byte-for-byte through the existing `transition_processing_result_preserves_existing_json_shape` test.
- [ ] Run the migration tests, then `cargo test --test workflow_store`.
- [ ] Commit only the migration/store test changes with `git commit -m "Day 15: migrate workflow goal state"` after the focused suite is green.

## Task 4: LLM interpretation and fail-closed intent routing

**Files:** Modify `src/workflow_model.rs`; test `tests/workflow_model.rs`.

**Interfaces:** Produces typed `ApproveGoal`/`ReopenGoal` interpretations for Task 6. Consumes `WorkflowTaskState::goal_proposal`; `interpret_observed` retains its existing signature and reads the proposal from `current`.

- [ ] Add failing parser/interpreter tests for `ApproveGoal`, `ReopenGoal`, ordinary yes only with an active proposal, conditional yes as `Continue`, ambiguous/low-confidence/invalid JSON/model failure as `Continue`, and no approval from `Done`.

  The typed DTO regression starts with:

  ```rust
  let approved = deepseek_cli::workflow_model::parse_human_interpretation(
      r#"{"confidence":0.99,"intent":{"type":"approve_goal"}}"#,
  ).unwrap();
  assert!(matches!(approved, HumanInterpretation::Managed { intent: WorkflowIntent::ApproveGoal, .. }));
  ```

  The interpreter test must then feed `"да, но без авторизации"` with an active proposal and a model `continue` classification, and assert that its effective intent is `Continue`. A misleading high-confidence `approve_goal` classification is an acknowledged LLM semantic risk, not something the deterministic state machine can prove false; do not claim otherwise or add a brittle regex as a fake guarantee.
- [ ] Extend `HumanIntentDto` with `ApproveGoal` and `ReopenGoal { change_request }`. Pass a compact active-proposal record (text, assistant message ID, stage ID, task version) to `HumanInputInterpreter::interpret_observed`; no proposal means the model must not return an effective approval. The interpreter may classify intent, but the domain/store recheck authorization.
- [ ] Update `INTERPRETER_PROMPT` to distinguish unconditional approval, requested changes, and explicit reopen. Keep `human_fallback` non-approving. Record why output was downgraded, using bounded codes rather than raw payload when payload logging is off.
- [ ] Run `cargo test --test workflow_model`.
- [ ] Commit only these files with `git commit -m "Day 15: interpret human goal decisions"` after the focused suite is green.

## Task 5: Atomic proposal, approval, reopen, and branch persistence

**Files:** Modify `src/workflow_store.rs`; test `tests/workflow_store.rs`.

**Interfaces:** Produces `append_goal_answer_for_processing(AnswerCommit<'_>, Option<&str>) -> Result<PersistedAnswer, StoreError>` and `commit_goal_approval(InputCommit<'_>, &GoalApprovalAuthorization) -> Result<PersistedTransition, StoreError>` / `commit_goal_reopen(InputCommit<'_>, &GoalReopenAuthorization) -> Result<PersistedTransition, StoreError>` on `WorkflowRepository`. Consumes Task 1 authorizations and Task 3 tables.

- [ ] Add failing tests for: proposal tied to saved assistant message, replacing it, invalidating it on every non-approving human turn (including a later failed response), approval exactly once, stale stage/version rejection, reopen from each unfinished later phase, reset of current plan/step/action/validation projection, pause/resume, restart, and branch remapping (Review Focus #2 and #4).

  The persistence assertion should compare all three bindings after answer commit, then force an obsolete version:

  ```rust
  let answer = store.append_goal_answer_for_processing(
      AnswerCommit {
          dialog_id: started.dialog_id,
          task_id: started.task.id,
          stage_run_id: started.stage_run_id,
          expected_version: started.task.version,
          content: "Предлагаемая цель: Сделать CLI",
          usage: None,
      },
      Some("Сделать CLI"),
  ).unwrap();
  let current = store.load_workflow(started.dialog_id).unwrap().current_task.unwrap();
  let proposal = current.goal_proposal.unwrap();
  assert_eq!(proposal.text, "Сделать CLI");
  assert_eq!(proposal.assistant_message_id, answer.message_id);
  assert_eq!(proposal.stage_run_id, started.stage_run_id);
  ```
- [ ] Keep `AnswerCommit` backward compatible for existing callers: add a repository method `append_goal_answer_for_processing(command: AnswerCommit<'_>, proposal_text: Option<&str>)`. It runs in one immediate transaction: current-task guard, answer insert, stage mapping, optional upsert of proposal with the inserted message ID, processing insert. For goal-definition answers with no proposal, remove the previous proposal in the same transaction. Reject proposal text unless phase is `GoalDefinition`; reject the old `append_answer_for_processing` method in that phase so callers cannot silently bypass proposal parsing.
- [ ] Add repository methods for `commit_goal_approval` and `commit_goal_reopen`, each consuming a human input and authorization in a single transaction. Approval verifies the still-current proposal, version, stage, and assistant message; updates goal/revision, closes proposal and stage, creates `planning` with empty plan. Reopen closes old stage, clears current plan projection, increments plan revision and version, creates a new goal-definition stage, and records the human request there. Preserve historical rows and transition audit. Reuse `workflow_inputs`/`task_transitions` with distinct `goal_approved`/`goal_reopened` events; do not shoehorn into `ReplanCurrent`.
- [ ] In `append_input`, invalidate the active proposal atomically for every accepted goal-definition human continuation. A locally rejected, persisted approval also invalidates it; an invariant-rejected approval is not persisted and leaves it intact. In all cases, a rejected approval never mutates goal/phase/plan.
- [ ] Extend `copy_workflow_branch` to copy/remap the proposal's assistant message ID using `message_id_map`, stage run ID using the stage map, and task ID using the task map. Refuse a branch whose active proposal cannot be mapped.
- [ ] Run `cargo test --test workflow_store`.
- [ ] Commit only `src/workflow_store.rs` and `tests/workflow_store.rs` with `git commit -m "Day 15: persist goal decisions atomically"` after the focused suite is green.

## Task 6: Engine orchestration and invariant gates

**Files:** Modify `src/workflow_engine.rs`, `src/workflow_model.rs`; test `tests/workflow_engine.rs`, `tests/agent.rs`.

**Interfaces:** Consumes Tasks 1–5 authorizations, parser, interpreter output, and repository methods. Produces no new public API; preserves `run_human_input` and `run_one_ordinary_turn` signatures.

- [ ] Add failing end-to-end tests: first input stays in goal definition; discussion and proposal replace; ordinary-language approval advances once; reopen from planning/execution/validation; controller cannot approve or skip; blocked/failed invariant before start, proposal, or approval leaves state unchanged; conditional yes does not approve (Review Focus #1).

  The engine acceptance test must assert the durable result, not just output text:

  ```rust
  let first = store.load_workflow(dialog_id).unwrap().current_task.unwrap();
  assert_eq!(first.phase, TaskPhase::GoalDefinition);
  assert!(first.plan.steps.is_empty());
  assert_eq!(first.goal_proposal.as_ref().map(|p| p.text.as_str()), Some("Сделать CLI"));
  // After a human "да" with a high-confidence approve_goal model result:
  let approved = store.load_workflow(dialog_id).unwrap().current_task.unwrap();
  assert_eq!(approved.phase, TaskPhase::Planning);
  assert_eq!(approved.goal, "Сделать CLI");
  assert!(approved.goal_proposal.is_none());
  ```
- [ ] In `run_human_input`, load the active proposal before interpretation; run the existing blocking invariant gate for `StartNewTask`, `ReopenGoal`, and `ApproveGoal`. For approval, check the exact saved proposal text as the candidate state change. If a checker blocks or is unavailable, do not call the state-changing store method. Keep checker order and short-circuit behavior.
- [ ] In `run_one_ordinary_turn`, buffer all goal-definition output until the candidate-response blocking check completes. Parse the candidate. If it contains a valid proposal, additionally run the invariant gate against that exact goal before emitting any bytes or persisting. On either rejection, emit the safe refusal only and leave any prior proposal invalidated by the human input. Persist answer plus proposal through `append_goal_answer_for_processing`. A malformed attempted proposal is not silently saved as an approvable answer.
- [ ] Route typed approval/reopen through the new store methods, not `commit_stage_change` handoff heuristics. Goal-definition continuation checker can continue/await but cannot request a phase change. Ensure response-processing recovery cannot autonomously create a goal proposal or approval from an old answer.
- [ ] Run `cargo test --test workflow_engine`, then `cargo test --test agent`.
- [ ] Commit only task-owned hunks after the focused suites are green; stage paths or selected hunks deliberately because these files contain prior user changes. Message: `Day 15: orchestrate goal approval and invariants`.

## Task 7: Stage context, status, diagnostics, and recovery

**Files:** Modify `src/workflow_context.rs`, `src/agent.rs`, `src/terminal.rs`, `src/debug_log.rs`, `src/workflow_store.rs`, `README.md`; test `tests/workflow_context.rs`, `tests/cli.rs`, `tests/debug_log.rs`, `tests/workflow_store.rs`.

**Interfaces:** Consumes `WorkflowTaskState::goal_revision`/`goal_proposal` and existing `WorkflowStatus`/diagnostic types. Status commands remain read-only.

- [ ] Add tests for reopen context containing only explicitly passed approved goal and reopen request, `/task` and `/debug` showing phase/working goal/proposal ID and version, no side-effect from status commands, pause/resume, restored pending processing, and JSONL safe fields with `log_payloads = false`.

  The diagnostic test must exercise real serialization and assert absence of both text values:

  ```rust
  let lines = std::fs::read_to_string(log_path).unwrap();
  assert!(lines.contains("goal_proposal"));
  assert!(!lines.contains("Сделать CLI"));
  assert!(!lines.contains("секретный текст модели"));
  ```
- [ ] Expose proposal metadata through existing workflow status/debug snapshots. Show `goal_definition` distinctly; label the goal as working rather than approved. Do not expose a proposal in a stage other than its own.
- [ ] Add diagnostic events/codes for interpreted intent, proposal parse result, invariant outcome and violated invariant IDs, authorization result, task/stage/proposal IDs, and rejection category. Gate free-form reason/model/proposal text behind `log_payloads` while keeping safe IDs visible. Integrate with existing checker logs without duplicating event emission.
- [ ] Document the lifecycle and visible approval line in `README.md`, including the honest limitation that LLM intent classification may err and the existing human-plan-approval issue is separate.
- [ ] Run `cargo test --test workflow_context --test cli --test debug_log --test workflow_store`.
- [ ] Commit only task-owned hunks after the focused suites are green; preserve earlier dirty-file edits. Message: `Day 15: surface goal lifecycle status`.

## Task 8: Whole-system verification and review

**Files:** No new production files; fix only failures introduced by this feature.

- [ ] Run `cargo fmt --check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings`; record command results, not just impressions.
- [ ] Exercise a temporary SQLite database through first message → proposal → normal-language approval → planning, plus reopen → revised proposal → approval; inspect `/task`, `/debug`, and JSONL output with payload logging disabled.
- [ ] Review the diff for accidental inclusion of current unrelated edits, secrets, `log.jsonl`, changed invariant ordering, or implicit approval paths. Compare every verification bullet in the spec to a named test. Request independent code review if the selected execution mode provides it.
- [ ] Report any unresolved risks explicitly. Do not call the feature complete if migration, crash/replay, or fail-closed invariant tests remain red.
