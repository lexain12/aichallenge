# Task State Machine and Stage-Isolated Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a durable, code-controlled workflow task lifecycle whose model-assisted routing, autonomous continuation, pause/resume, handoffs, and stage-isolated context cannot bypass finite-state-machine guards.

**Architecture:** Keep the existing dialog as the human-visible transcript, but add a workflow domain layer, an append-only SQLite audit layer, strict model adapters, and a `WorkflowEngine` used by persistent agents. All state-changing model output becomes a typed proposal; pure reducers authorize it, and repository transactions apply it with optimistic versions. Ordinary prompts are rebuilt from the current workflow task and stage run rather than from the complete dialog.

**Tech Stack:** Rust 2024, Tokio 1.53, reqwest streaming/SSE, serde/serde_json, rusqlite with bundled SQLite, clap, wiremock, tempfile.

**Spec:** `docs/superpowers/specs/2026-09-21-task-state-machine-design.md`

## Global Constraints

- The only ordinary phase graph is `planning -> execution -> validation -> done` plus `validation -> execution`; all replans are explicit `ReplanCurrent` events that create a new planning stage run.
- `TaskStatus::{Active, Paused}` is independent from phase; pause/resume never creates a stage run, and `done` cannot be paused.
- One dialog may have at most one workflow task whose phase is not `done`; enforce this in both the reducer and a SQLite partial unique index.
- Existing `RequestScope.task_id` and `--task` remain the memory-task namespace; never reuse either as `WorkflowTaskId`.
- Human and controller inputs must pass through the same `WorkflowInputHandler`; controller inputs may only continue or propose a transition.
- Model calls may propose intents, patches, handoffs, or transitions, but only application code may mutate workflow state.
- Controller text is persisted as a hidden user-role protocol message, included only in its task/stage protocol, and excluded from transcript, title, profile, memory, and sticky-facts extraction.
- The continuation checker is advisory: a failed or malformed checker result preserves the completed answer, applies no state change, and stops autonomous execution.
- Default autonomous limits are exactly 8 ordinary controller turns and 20,000 provider-reported total API tokens after one human input.
- A repeated `(task version, phase, current step, expected action)` fingerprint, missing token usage, low confidence, malformed output, API failure, `done`, or interruption stops autonomous execution.
- Handoffs are built only after a transition is locally accepted; full handoffs are immutable audit data and only their compact projection enters `WorkflowTaskState`.
- Ordinary context contains only the active workflow task and stage run; prior tasks/stages remain in SQLite and enter later stages only through handoff-projected state.
- Existing databases must migrate in place; legacy messages without workflow metadata remain visible human transcript entries.
- The signal handler only cancels work. SQLite pause persistence happens after the in-flight future has been dropped, and no partial model answer is committed.
- No background worker, blocking validator, approval UI, or transition policy beyond the specification is added in Day 13.

## Review Focus

- A whitespace-only human message or blank required model field must cause a local error/fallback and no workflow mutation; Tasks 2 and 3 pin this behavior.
- Legacy databases with unmapped messages and a completed task followed by an uninterpretable message must remain readable without leaking old history into a managed stage; Tasks 4 and 7 pin this behavior.
- A stale checker, handoff, or second process racing a state transition must lose on `expected_version` and create no duplicate task, stage, transition, or controller input; Tasks 5 and 6 pin this behavior.
- `Ctrl+C` before a first task exists and during each service-call boundary must exit cleanly, preserve the last committed checkpoint, and never manufacture a task merely to mark it paused; Task 10 pins this behavior.
- Oversized Unicode JSON at exact byte/character boundaries must be rejected before persistence without panics or partial UTF-8 truncation; Task 3 pins this behavior.

---

## File Map

- Create `src/workflow.rs`: workflow IDs, phases, plans, patches, typed inputs, transition guards, pure reducer, state fingerprints, and task-state prompt rendering.
- Create `src/workflow_model.rs`: object-safe completion-model boundary, strict JSON DTOs/parsers, interpreter, continuation checker, handoff builder, and size/confidence policy.
- Create `src/workflow_store.rs`: workflow schema migration, row decoding, atomic input/answer/checker/transition operations, stage context, pending-processing recovery, and branch copying.
- Create `src/workflow_context.rs`: current-stage protocol selection and stage-scoped summary/facts request assembly.
- Create `src/workflow_engine.rs`: shared input handler, service-call orchestration, response-processing pipeline, autonomy budget, restart recovery, and event production.
- Modify `src/client.rs`: expose deterministic non-streaming completion through the existing streaming transport with a per-call model name.
- Modify `src/config.rs`: validated `[workflow]` settings and exact defaults.
- Modify `src/dialog.rs`: expose crate-local SQLite access, return inserted message IDs, hide controller rows from normal transcript loading, and invoke workflow branch copying.
- Modify `src/agent.rs`: preserve the legacy/in-memory path, delegate managed persistent turns to `WorkflowEngine`, restore workflow state, and expose pause/status APIs.
- Modify `src/context.rs` and `src/facts.rs`: operate on a stage-local history and prevent controller messages from entering facts.
- Modify `src/chat.rs`: add `/task` parsing without changing serialized API roles.
- Modify `src/terminal.rs` and `src/main.rs`: display workflow status/events, handle multiple autonomous response blocks, and cancel on `Ctrl+C`.
- Modify `src/debug_log.rs`: add redacted workflow metadata events governed by `debug.log_payloads`.
- Modify `src/lib.rs`: export the five workflow modules.
- Modify `Cargo.toml`: enable Tokio's `signal` feature; add no new runtime crate.
- Modify `deepseek.example.toml` and `README.md`: document workflow configuration, status, pause, resume, context isolation, and controller provenance.
- Create `tests/workflow.rs`, `tests/workflow_model.rs`, `tests/workflow_store.rs`, `tests/workflow_context.rs`, and `tests/workflow_engine.rs`; extend `tests/config.rs`, `tests/client.rs`, `tests/dialog.rs`, `tests/agent.rs`, `tests/chat.rs`, and `tests/cli.rs`.

### Task 1: Workflow Configuration and Pluggable Completion Boundary

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/config.rs`
- Modify: `src/client.rs`
- Create: `src/workflow_model.rs`
- Modify: `src/lib.rs`
- Modify: `deepseek.example.toml`
- Test: `tests/config.rs`
- Test: `tests/client.rs`
- Create: `tests/workflow_model.rs`

**Interfaces:**
- Consumes: existing `DeepSeekClient`, `Message`, `TokenUsage`, and validated `Config`.
- Produces: `WorkflowConfig`; `ModelRequest`; `ModelResponse`; `ModelError`; object-safe `CompletionModel`; and `DeepSeekCompletionModel::new(client: DeepSeekClient, model: String)`.

- [ ] **Step 1: Add failing configuration tests for exact defaults and invalid limits**

```rust
#[test]
fn workflow_defaults_are_bounded_and_inherit_the_chat_model() {
    let config = Config::from_toml(
        "api_key = \"key\"\nmodel = \"chat-model\"\n[context]\nstrategy = \"summary\"",
        None,
    ).unwrap();
    let workflow = config.workflow();
    assert!(workflow.enabled());
    assert_eq!(workflow.interpreter_model(), "chat-model");
    assert_eq!(workflow.checker_model(), "chat-model");
    assert_eq!(workflow.handoff_model(), "chat-model");
    assert_eq!(workflow.interpreter_max_tokens(), 512);
    assert_eq!(workflow.checker_max_tokens(), 1024);
    assert_eq!(workflow.handoff_max_tokens(), 2048);
    assert_eq!(workflow.min_confidence(), 0.80);
    assert_eq!(workflow.max_autonomous_turns(), 8);
    assert_eq!(workflow.max_autonomous_tokens(), 20_000);
}

#[test]
fn workflow_rejects_zero_limits_and_confidence_outside_zero_to_one() {
    for source in [
        "api_key='key'\n[context]\nstrategy='summary'\n[workflow]\nchecker_max_tokens=0",
        "api_key='key'\n[context]\nstrategy='summary'\n[workflow]\nmax_autonomous_turns=0",
        "api_key='key'\n[context]\nstrategy='summary'\n[workflow]\nmin_confidence=1.1",
    ] {
        assert!(Config::from_toml(source, None).is_err());
    }
}
```

- [ ] **Step 2: Run the focused configuration tests and verify red**

Run: `cargo test --test config workflow_ -- --nocapture`

Expected: FAIL because `Config::workflow` and the `[workflow]` fields do not exist.

- [ ] **Step 3: Add `RawWorkflowConfig` and immutable validated `WorkflowConfig`**

```rust
const DEFAULT_INTERPRETER_MAX_TOKENS: u32 = 512;
const DEFAULT_CHECKER_MAX_TOKENS: u32 = 1024;
const DEFAULT_HANDOFF_MAX_TOKENS: u32 = 2048;
const DEFAULT_MIN_CONFIDENCE: f32 = 0.80;
const DEFAULT_MAX_AUTONOMOUS_TURNS: u32 = 8;
const DEFAULT_MAX_AUTONOMOUS_TOKENS: u64 = 20_000;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawWorkflowConfig {
    enabled: Option<bool>,
    interpreter_model: Option<String>,
    checker_model: Option<String>,
    handoff_model: Option<String>,
    interpreter_max_tokens: Option<u32>,
    checker_max_tokens: Option<u32>,
    handoff_max_tokens: Option<u32>,
    min_confidence: Option<f32>,
    max_autonomous_turns: Option<u32>,
    max_autonomous_tokens: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct WorkflowConfig {
    enabled: bool,
    interpreter_model: String,
    checker_model: String,
    handoff_model: String,
    interpreter_max_tokens: u32,
    checker_max_tokens: u32,
    handoff_max_tokens: u32,
    min_confidence: f32,
    max_autonomous_turns: u32,
    max_autonomous_tokens: u64,
}
```

Set `enabled` to `true` when omitted. Trim each optional model and fall back to the validated top-level model. Reject blank overrides, zero token/turn limits, and non-finite or out-of-range confidence. Add `workflow: WorkflowConfig` to `Config`, redact nothing new in `Debug`, and add all getters used by the test.

- [ ] **Step 4: Add a per-call deterministic completion method and prove model override/usage**

```rust
pub async fn complete(
    &self,
    model: &str,
    messages: &[Message],
    max_tokens: u32,
) -> Result<SummaryResult, ClientError> {
    self.deterministic_service_call(model, messages, max_tokens).await
}
```

Add `model: &str` to private `RequestOptions`, use it in `ChatRequest`, and make existing `summarize`/`update_facts` pass `&self.model`. In `tests/client.rs`, mount one SSE completion and assert the body contains `"model":"workflow-checker"`, `temperature: 0.0`, disabled thinking, no stop sequences, and that returned usage is preserved.

- [ ] **Step 5: Define the object-safe model interface and DeepSeek adapter**

```rust
use std::future::Future;
use std::pin::Pin;

pub struct ModelRequest {
    pub messages: Vec<Message>,
    pub max_tokens: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelResponse {
    pub content: String,
    pub usage: Option<TokenUsage>,
}

pub type ModelFuture<'a> = Pin<
    Box<dyn Future<Output = Result<ModelResponse, ModelError>> + Send + 'a>,
>;

pub trait CompletionModel: Send + Sync {
    fn name(&self) -> &str;
    fn complete(&self, request: ModelRequest) -> ModelFuture<'_>;
}

#[derive(Clone)]
pub struct DeepSeekCompletionModel {
    client: DeepSeekClient,
    model: String,
}
```

Implement the adapter with an owned client/model inside the boxed future:

```rust
impl CompletionModel for DeepSeekCompletionModel {
    fn name(&self) -> &str { &self.model }

    fn complete(&self, request: ModelRequest) -> ModelFuture<'_> {
        let client = self.client.clone();
        let model = self.model.clone();
        Box::pin(async move {
            let result = client.complete(&model, &request.messages, request.max_tokens).await?;
            Ok(ModelResponse {
                content: result.answer().to_owned(),
                usage: result.usage(),
            })
        })
    }
}
```

Map `ClientError` into `ModelError`, reject a blank model in the constructor, and export `workflow_model` from `src/lib.rs`. Pin the non-HTTP seam with:

```rust
#[tokio::test]
async fn completion_model_can_be_replaced_without_http() {
    let model = FakeCompletionModel::one("checker-a", "{\"decision\":\"await_user\"}", 7);
    let response = model.complete(ModelRequest {
        messages: vec![Message::new(Role::User, "inspect")],
        max_tokens: 32,
    }).await.unwrap();
    assert_eq!(model.name(), "checker-a");
    assert_eq!(response.usage.unwrap().total_tokens, 7);
}
```

- [ ] **Step 6: Document the complete `[workflow]` example**

```toml
[workflow]
enabled = true
# interpreter_model = "deepseek-v4-flash"
# checker_model = "deepseek-v4-flash"
# handoff_model = "deepseek-v4-flash"
interpreter_max_tokens = 512
checker_max_tokens = 1024
handoff_max_tokens = 2048
min_confidence = 0.80
max_autonomous_turns = 8
max_autonomous_tokens = 20000
```

- [ ] **Step 7: Run focused and regression tests**

Run: `cargo test --test config && cargo test --test client && cargo test --test workflow_model`

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml src/config.rs src/client.rs src/workflow_model.rs src/lib.rs deepseek.example.toml tests/config.rs tests/client.rs tests/workflow_model.rs
git commit -m "feat: add workflow model configuration"
```

### Task 2: Pure Workflow Domain and Guarded State Machine

**Files:**
- Create: `src/workflow.rs`
- Modify: `src/lib.rs`
- Create: `tests/workflow.rs`

**Interfaces:**
- Consumes: serde only.
- Produces: workflow ID newtypes; `TaskPhase`; `TaskStatus`; `TaskPlan`; `PlanStep`; `StageCheckpoint`; `WorkflowTaskState`; `WorkflowInput`; `WorkflowIntent`; `TaskStatePatch`; `TransitionEvent`; `StateMachine`; `TransitionAuthorization`; `ReplanAuthorization`; `StageChangeAuthorization`; `StateFingerprint`; `WorkflowError`.

- [ ] **Step 1: Write exhaustive failing tests for the transition matrix**

```rust
#[test]
fn only_declared_phase_transitions_are_authorized() {
    let cases = [
        (TaskPhase::Planning, TransitionEvent::PlanningCompleted, TaskPhase::Execution),
        (TaskPhase::Execution, TransitionEvent::ExecutionCompleted, TaskPhase::Validation),
        (TaskPhase::Validation, TransitionEvent::ValidationPassed, TaskPhase::Done),
        (TaskPhase::Validation, TransitionEvent::ValidationFailed, TaskPhase::Execution),
    ];
    for (from, event, to) in cases {
        assert_eq!(StateMachine::authorize(from, event).unwrap().to_phase, to);
    }
    for (from, event) in [
        (TaskPhase::Planning, TransitionEvent::ExecutionCompleted),
        (TaskPhase::Planning, TransitionEvent::ValidationPassed),
        (TaskPhase::Execution, TransitionEvent::ValidationPassed),
        (TaskPhase::Done, TransitionEvent::PlanningCompleted),
    ] {
        assert!(matches!(
            StateMachine::authorize(from, event),
            Err(WorkflowError::IllegalTransition { .. })
        ));
    }
}
```

Pin the other reducer guards with table-driven assertions:

```rust
#[test]
fn source_status_and_dialog_guards_are_local_and_exhaustive() {
    let active = execution_state();
    let paused = active.clone().pause().unwrap();
    assert_eq!(paused.phase, active.phase);
    assert_eq!(paused.current_stage_run_id, active.current_stage_run_id);
    assert!(StateMachine::validate_source(
        &WorkflowInputSource::Controller {
            checker: "c".into(), model: "m".into(), triggering_assistant_message_id: 9,
        },
        &WorkflowIntent::StartNewTask { goal: "other".into() },
    ).is_err());
    assert!(StateMachine::validate_new_task(Some(&active)).is_err());
    assert!(StateMachine::validate_new_task(Some(&done_state())).is_ok());
    assert!(done_state().pause().is_err());
    for text in ["", "   ", "\n\t"] {
        assert!(WorkflowIntent::human_continue(text).is_err());
    }
}
```

- [ ] **Step 2: Run the domain test and verify red**

Run: `cargo test --test workflow -- --nocapture`

Expected: FAIL because `deepseek_cli::workflow` is not exported.

- [ ] **Step 3: Define strongly typed IDs and state data**

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowTaskId(pub i64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StageRunId(pub i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhase { Planning, Execution, Validation, Done }

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus { Active, Paused }

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskPlan {
    pub revision: u32,
    pub steps: Vec<PlanStep>,
    pub acceptance_criteria: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlanStep {
    pub id: String,
    pub description: String,
    pub status: PlanStepStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepStatus { Pending, InProgress, Completed, Blocked }

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StageCheckpoint {
    pub summary: String,
    pub decisions: Vec<String>,
    pub open_issues: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTaskState {
    pub id: WorkflowTaskId,
    pub dialog_id: i64,
    pub ordinal: u32,
    pub phase: TaskPhase,
    pub status: TaskStatus,
    pub goal: String,
    pub plan: TaskPlan,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub checkpoint: StageCheckpoint,
    pub current_stage_run_id: StageRunId,
    pub current_stage_sequence: u32,
    pub incoming_handoff_id: Option<i64>,
    pub version: u64,
}
```

Use `#[serde(deny_unknown_fields)]` on all model-facing structs. Validate all persisted IDs as positive, plan step IDs as unique/nonblank, and strings by Unicode scalar count before writing them.

- [ ] **Step 4: Implement typed intents, source restrictions, and the exhaustive phase reducer**

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkflowInputSource {
    Human,
    Controller {
        checker: String,
        model: String,
        triggering_assistant_message_id: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkflowIntent {
    Continue { instruction: String },
    StartNewTask { goal: String },
    ReplanCurrent { change_request: String },
    ProposeTransition { event: TransitionEvent, evidence: Vec<String> },
}

pub fn target_phase(from: TaskPhase, event: TransitionEvent) -> Result<TaskPhase, WorkflowError> {
    match (from, event) {
        (TaskPhase::Planning, TransitionEvent::PlanningCompleted) => Ok(TaskPhase::Execution),
        (TaskPhase::Execution, TransitionEvent::ExecutionCompleted) => Ok(TaskPhase::Validation),
        (TaskPhase::Validation, TransitionEvent::ValidationPassed) => Ok(TaskPhase::Done),
        (TaskPhase::Validation, TransitionEvent::ValidationFailed) => Ok(TaskPhase::Execution),
        _ => Err(WorkflowError::IllegalTransition { from, event }),
    }
}

pub struct TransitionAuthorization {
    pub from_phase: TaskPhase,
    pub to_phase: TaskPhase,
    pub event: TransitionEvent,
    pub source_version: u64,
}

pub struct ReplanAuthorization {
    pub from_phase: TaskPhase,
    pub to_phase: TaskPhase,
    pub source_version: u64,
    pub next_plan_revision: u32,
    pub change_request: String,
}

pub enum StageChangeAuthorization {
    Transition(TransitionAuthorization),
    Replan(ReplanAuthorization),
}
```

Do not add `set_phase`. `StateMachine::authorize` must call `target_phase`, then enforce phase-specific guards: planning requires a non-empty plan and acceptance criteria; execution completion requires every step completed; validation events require non-empty evidence; validation pass requires every criterion represented in validation evidence. `StateMachine::authorize_replan` accepts only a human source, targets `Planning` from every phase including `Planning` and `Done`, preserves task ID, and returns the next plan revision plus the recorded change request.

- [ ] **Step 5: Implement monotonic plan patches with evidence**

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StepStatusUpdate {
    pub step_id: String,
    pub status: PlanStepStatus,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PlanAppend {
    pub steps: Vec<PlanStep>,
    pub acceptance_criteria: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskStatePatch {
    pub expected_version: u64,
    pub plan_append: PlanAppend,
    pub step_updates: Vec<StepStatusUpdate>,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub checkpoint: Option<StageCheckpoint>,
}
```

New tasks begin with revision `0`, no steps, and no acceptance criteria. `WorkflowTaskState::preview_patch(&self, patch, context) -> Result<Self, WorkflowError>` validates and projects all fields while retaining the source state version; repositories use that preview before a transaction. `apply_patch` finalizes the same projection and increments `version` exactly once. Planning appends steps/criteria without replacing prior entries and increments `plan.revision` once whenever `plan_append` is non-empty. Reject a stale version, unknown or duplicate step IDs, completion without evidence, changes to completed steps, removal/reordering of steps, duplicate/removal of acceptance criteria, plan additions outside `planning`, and a `current_step_id` absent from the resulting plan. The pipeline may also authorize step additions atomically with `ValidationFailed`; it passes `PatchContext::ValidationRepair` to the preview. No other phase may add steps. A non-empty accepted patch increments state `version` exactly once regardless of how many fields change.

- [ ] **Step 6: Add deterministic state rendering and fingerprints**

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StateFingerprint {
    pub version: u64,
    pub phase: TaskPhase,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
}

pub fn render_task_state(state: &WorkflowTaskState) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "workflow_task": state.ordinal,
        "phase": state.phase,
        "status": state.status,
        "goal": state.goal,
        "plan": state.plan,
        "current_step_id": state.current_step_id,
        "expected_action": state.expected_action,
        "checkpoint": state.checkpoint,
    })).expect("validated workflow state is serializable")
}
```

Test the exact rendered key order and assert that `incoming_handoff_id`, full handoff text, dialog ID, and version do not appear in the prompt block.

- [ ] **Step 7: Run the domain suite**

Run: `cargo test --test workflow`

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/workflow.rs src/lib.rs tests/workflow.rs
git commit -m "feat: add guarded workflow state machine"
```

### Task 3: Strict Interpreter, Continuation Checker, and Handoff Parsing

**Files:**
- Modify: `src/workflow_model.rs`
- Modify: `src/workflow.rs`
- Test: `tests/workflow_model.rs`

**Interfaces:**
- Consumes: `CompletionModel`, `WorkflowTaskState`, `WorkflowInput`, `TaskStatePatch`, and `TransitionEvent` from Tasks 1-2.
- Produces: `HumanInputInterpreter`; `HumanInterpretation`; `ResponseChecker`; `ContinuationChecker`; `ContinuationCheckResult`; `ControllerDecision`; `HandoffBuilder`; `HandoffPayload`; `ModelPolicyError`.

- [ ] **Step 1: Write table-driven failing parser tests**

```rust
#[test]
fn interpreter_accepts_exactly_the_four_intents() {
    let samples = [
        (r#"{"confidence":0.95,"intent":{"type":"continue","instruction":"keep implementing"}}"#, "continue"),
        (r#"{"confidence":0.95,"intent":{"type":"start_new_task","goal":"build search"}}"#, "start_new_task"),
        (r#"{"confidence":0.95,"intent":{"type":"replan_current","change_request":"support offline mode"}}"#, "replan_current"),
        (r#"{"confidence":0.95,"intent":{"type":"propose_transition","event":"execution_completed","evidence":["tests pass"]}}"#, "propose_transition"),
    ];
    for (json, expected) in samples {
        assert_eq!(parse_human_interpretation(json).unwrap().intent.kind(), expected);
    }
}

#[test]
fn strict_json_rejects_fences_unknown_fields_blank_text_and_oversize_unicode() {
    assert!(parse_human_interpretation("```json\n{}\n```").is_err());
    assert!(parse_human_interpretation(r#"{"confidence":0.9,"extra":1,"intent":{"type":"continue","instruction":"x"}}"#).is_err());
    assert!(parse_human_interpretation(r#"{"confidence":0.9,"intent":{"type":"continue","instruction":"   "}}"#).is_err());
    let oversized = "🦀".repeat(MAX_MODEL_TEXT_CHARS + 1);
    let body = serde_json::json!({"confidence": 0.9, "intent": {"type": "continue", "instruction": oversized}}).to_string();
    assert!(parse_human_interpretation(&body).is_err());
}
```

Also test unknown enum values, duplicate/unknown plan step references, more than 32 evidence items, an item above 2,048 characters, and a handoff body above 65,536 UTF-8 bytes.

- [ ] **Step 2: Run parser tests and verify red**

Run: `cargo test --test workflow_model -- --nocapture`

Expected: FAIL because the strict DTOs and parsers are absent.

- [ ] **Step 3: Define exact limits and strict DTOs**

```rust
pub const MAX_MODEL_TEXT_CHARS: usize = 8_192;
pub const MAX_MODEL_JSON_BYTES: usize = 65_536;
pub const MAX_EVIDENCE_ITEMS: usize = 32;
pub const MAX_EVIDENCE_ITEM_CHARS: usize = 2_048;
pub const MAX_HANDOFF_JSON_BYTES: usize = 65_536;
pub const MAX_PLAN_STEPS: usize = 256;
pub const MAX_ACCEPTANCE_CRITERIA: usize = 128;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanInterpretationDto {
    confidence: f32,
    intent: HumanIntentDto,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum HumanIntentDto {
    Continue { instruction: String },
    StartNewTask { goal: String },
    ReplanCurrent { change_request: String },
    ProposeTransition { event: TransitionEvent, evidence: Vec<String> },
}

pub enum ControllerDecision {
    AwaitUser,
    Continue { instruction: String, confidence: f32 },
    EmitTransition { event: TransitionEvent, evidence: Vec<String>, confidence: f32 },
}
```

Parse with one `serde_json::from_str` call and no Markdown cleanup. Check `MAX_MODEL_JSON_BYTES` before parsing, then character/item limits after parsing. Cap the resulting plan at `MAX_PLAN_STEPS` and acceptance criteria at `MAX_ACCEPTANCE_CRITERIA`. Require finite confidence in `0.0..=1.0`.

- [ ] **Step 4: Implement safe interpretation fallbacks explicitly**

```rust
pub fn human_fallback(
    raw: &str,
    current: Option<&WorkflowTaskState>,
) -> Result<HumanInterpretation, ModelPolicyError> {
    let instruction = raw.trim();
    if instruction.is_empty() {
        return Err(ModelPolicyError::BlankHumanInput);
    }
    let intent = match current {
        None => WorkflowIntent::StartNewTask { goal: instruction.to_owned() },
        Some(task) if task.phase != TaskPhase::Done => {
            WorkflowIntent::Continue { instruction: instruction.to_owned() }
        }
        Some(_) => return Ok(HumanInterpretation::unmanaged()),
    };
    Ok(HumanInterpretation::fallback(intent))
}
```

`HumanInputInterpreter::interpret` receives only the raw human text and compact current-state JSON. On API error, malformed output, or confidence below `WorkflowConfig::min_confidence`, return `human_fallback`; never turn low confidence into start/replan/transition.

- [ ] **Step 5: Define object-safe checker and continuation implementation**

```rust
pub type CheckFuture<'a> = Pin<
    Box<dyn Future<Output = Result<ContinuationCheckResult, CheckError>> + Send + 'a>,
>;

pub trait ResponseChecker: Send + Sync {
    fn name(&self) -> &str;
    fn mode(&self) -> CheckerMode;
    fn check<'a>(&'a self, context: &'a CheckContext, response: &'a str) -> CheckFuture<'a>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckerMode { Advisory, Blocking }

pub struct ContinuationCheckResult {
    pub patch: TaskStatePatch,
    pub decision: ControllerDecision,
    pub usage: Option<TokenUsage>,
}

pub struct CheckContext {
    pub task: WorkflowTaskState,
    pub stage_messages: Vec<Message>,
    pub triggering_input: WorkflowInput,
}
```

The Day 13 checker must always report `CheckerMode::Advisory`. Its prompt includes current version, phase, compact plan/checkpoint, current stage messages, triggering input, and complete assistant answer. Validate every patch using the Task 2 domain rules before returning it to the engine.

- [ ] **Step 6: Define and validate the handoff payload**

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffPayload {
    pub summary: String,
    pub completed_step_ids: Vec<String>,
    pub next_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub plan_changes: Vec<PlanStep>,
    pub decisions: Vec<String>,
    pub open_issues: Vec<String>,
}
```

`HandoffBuilder::build` receives an already authorized `StageChangeAuthorization`, outgoing state, outgoing stage messages, and triggering input. Its JSON schema deliberately contains no `from_phase`, `to_phase`, event, or version. Reject unknown/completed-step IDs, invalid next-step IDs, duplicate repair-step IDs, repair steps except on `ValidationFailed`, and payloads over `MAX_HANDOFF_JSON_BYTES`. For replan, preserve the existing goal/plan, include the human change request in the new checkpoint/open issues, set planning's expected action, and increment the plan revision supplied by `ReplanAuthorization`. Pin projection ownership with:

```rust
#[test]
fn handoff_projects_work_without_controlling_the_transition() {
    let authorization = StageChangeAuthorization::Transition(
        StateMachine::authorize(
            TaskPhase::Execution,
            TransitionEvent::ExecutionCompleted,
        ).unwrap(),
    );
    let payload = parse_handoff(&valid_handoff_json(), &execution_state(), &authorization).unwrap();
    let projected = project_handoff(&execution_state(), &authorization, &payload).unwrap();
    assert_eq!(projected.phase, TaskPhase::Validation);
    assert_eq!(projected.current_step_id.as_deref(), Some("validate"));
    assert!(!valid_handoff_json().contains("to_phase"));
}
```

- [ ] **Step 7: Run all strict-model tests**

Run: `cargo test --test workflow_model`

Expected: all tests PASS, including the exact Unicode boundary test with 8,192 characters accepted and 8,193 rejected.

- [ ] **Step 8: Commit**

```bash
git add src/workflow.rs src/workflow_model.rs tests/workflow_model.rs
git commit -m "feat: parse workflow model proposals strictly"
```

### Task 4: Workflow Schema, Migration, and Restoration

**Files:**
- Create: `src/workflow_store.rs`
- Modify: `src/dialog.rs`
- Modify: `src/lib.rs`
- Create: `tests/workflow_store.rs`
- Modify: `tests/dialog.rs`

**Interfaces:**
- Consumes: all domain state from Task 2 and existing `DialogStore`/SQLite schema.
- Produces: `WorkflowRepository`; `DialogWorkflowSnapshot`; `StageProtocolMessage`; `PendingProcessing`; schema migration invoked by `DialogStore::open`; transcript-safe `DialogStore::load`.

- [ ] **Step 1: Write migration/restoration tests against a legacy database**

```rust
#[test]
fn opening_a_legacy_database_adds_workflow_tables_without_reclassifying_messages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy.sqlite3");
    create_day8_database_with_four_messages(&path);
    let store = DialogStore::open(&path).unwrap();
    let dialog = store.load(1).unwrap();
    assert_eq!(dialog.messages.len(), 4);
    assert!(store.load_workflow(1).unwrap().current_task.is_none());
    assert!(store.load_stage_messages(StageRunId(999)).unwrap().is_empty());
}
```

Pin corrupt-row behavior with one fixture per invariant:

```rust
#[test]
fn restoration_rejects_corrupt_workflow_rows() {
    for corruption in [
        Corruption::UnknownPhase,
        Corruption::NegativeVersion,
        Corruption::NegativeOrdinal,
        Corruption::DanglingCurrentTask,
        Corruption::ClosedCurrentStage,
        Corruption::StagePhaseMismatch,
    ] {
        let fixture = CorruptWorkflowFixture::new(corruption);
        assert!(matches!(
            fixture.store.load_workflow(fixture.dialog_id),
            Err(StoreError::InvalidWorkflow(_))
        ));
    }
}
```

- [ ] **Step 2: Run migration tests and verify red**

Run: `cargo test --test workflow_store opening_a_legacy_database -- --nocapture`

Expected: FAIL because workflow tables and repository methods are absent.

- [ ] **Step 3: Add the exact schema in an idempotent migration**

```sql
CREATE TABLE IF NOT EXISTS workflow_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    phase TEXT NOT NULL CHECK (phase IN ('planning','execution','validation','done')),
    status TEXT NOT NULL CHECK (status IN ('active','paused')),
    goal TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    current_step_id TEXT,
    expected_action TEXT,
    checkpoint_json TEXT NOT NULL,
    current_stage_run_id INTEGER,
    incoming_handoff_id INTEGER,
    version INTEGER NOT NULL CHECK (version >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
    UNIQUE(dialog_id, ordinal),
    FOREIGN KEY(current_stage_run_id) REFERENCES task_stage_runs(id),
    FOREIGN KEY(incoming_handoff_id) REFERENCES task_transitions(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_unfinished_workflow_task_per_dialog
ON workflow_tasks(dialog_id) WHERE phase <> 'done';

CREATE TABLE IF NOT EXISTS dialog_workflow_state (
    dialog_id INTEGER PRIMARY KEY REFERENCES dialogs(id),
    current_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id)
);

CREATE TABLE IF NOT EXISTS task_stage_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id),
    phase TEXT NOT NULL CHECK (phase IN ('planning','execution','validation','done')),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
    finished_at TEXT,
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    UNIQUE(workflow_task_id, sequence)
);

CREATE TABLE IF NOT EXISTS task_stage_context (
    stage_run_id INTEGER PRIMARY KEY REFERENCES task_stage_runs(id),
    context_json TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now'))
);
```

Create the remaining ledger tables now so later tasks only add operations:

```sql
CREATE TABLE IF NOT EXISTS workflow_inputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dialog_id INTEGER NOT NULL REFERENCES dialogs(id),
    message_id INTEGER NOT NULL UNIQUE REFERENCES messages(id),
    source TEXT NOT NULL CHECK (source IN ('human','controller')),
    checker_name TEXT,
    model_name TEXT,
    triggering_assistant_message_id INTEGER REFERENCES messages(id),
    intent_json TEXT NOT NULL,
    confidence REAL,
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted','rejected')),
    rejection_reason TEXT,
    processing_id INTEGER UNIQUE REFERENCES response_processing(id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
    CHECK (
        (source = 'human' AND checker_name IS NULL AND model_name IS NULL
         AND processing_id IS NULL) OR
        (source = 'controller' AND checker_name IS NOT NULL AND model_name IS NOT NULL
         AND triggering_assistant_message_id IS NOT NULL AND processing_id IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS message_task_stages (
    message_id INTEGER PRIMARY KEY REFERENCES messages(id),
    workflow_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id),
    stage_run_id INTEGER NOT NULL REFERENCES task_stage_runs(id)
);

CREATE TABLE IF NOT EXISTS response_processing (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    assistant_message_id INTEGER NOT NULL REFERENCES messages(id),
    checker_name TEXT NOT NULL,
    expected_version INTEGER NOT NULL CHECK (expected_version >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending','processing','completed','failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    result_json TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')),
    UNIQUE(assistant_message_id, checker_name)
);

CREATE TABLE IF NOT EXISTS task_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_task_id INTEGER NOT NULL REFERENCES workflow_tasks(id),
    from_stage_run_id INTEGER NOT NULL REFERENCES task_stage_runs(id),
    to_stage_run_id INTEGER NOT NULL REFERENCES task_stage_runs(id),
    workflow_input_id INTEGER NOT NULL UNIQUE REFERENCES workflow_inputs(id),
    event TEXT NOT NULL CHECK (event IN (
        'planning_completed','execution_completed','validation_passed','validation_failed',
        'replan_requested'
    )),
    source_version INTEGER NOT NULL CHECK (source_version >= 0),
    handoff_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now'))
);
```

SQLite permits forward and cyclic foreign-key declarations, so execute the full batch inside one migration transaction, then verify `PRAGMA foreign_key_check` is empty. Do not temporarily disable foreign keys or rebuild existing dialog/message tables.

- [ ] **Step 4: Expose repository methods and strict row decoders**

```rust
pub trait WorkflowRepository {
    fn load_workflow(&self, dialog_id: i64) -> Result<DialogWorkflowSnapshot, StoreError>;
    fn load_stage_messages(&self, stage_run_id: StageRunId)
        -> Result<Vec<StageProtocolMessage>, StoreError>;
    fn load_pending_processing(&self, dialog_id: i64)
        -> Result<Vec<PendingProcessing>, StoreError>;
    fn close_stale_processing(&mut self, dialog_id: i64, current_version: u64)
        -> Result<usize, StoreError>;
}

#[derive(Clone, Debug)]
pub struct DialogWorkflowSnapshot {
    pub current_task: Option<WorkflowTaskState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageProtocolMessage {
    pub message_id: i64,
    pub message: Message,
    pub source: ProtocolSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolSource { Human, Controller, Assistant }
```

Make `DialogStore.connection` `pub(crate)` and implement the trait in `workflow_store.rs`. `load_pending_processing` returns `pending`, crash-left `processing`, and retryable `failed` rows with `attempts < 2`, always restricted to the dialog's current task version. `close_stale_processing` marks older incomplete rows terminal `failed` with `last_error='stale task version'` without applying their result. Decode each JSON field through serde and each integer through checked conversion. `load_stage_messages` orders by `messages.id`, and derives controller provenance only from `workflow_inputs.source = 'controller'`.

- [ ] **Step 5: Keep hidden controller rows out of normal transcript loads**

Change the message query in `DialogStore::load` to left-join `workflow_inputs` and include rows only when `workflow_inputs.source IS NULL OR workflow_inputs.source = 'human' OR messages.role = 'assistant'`. Add a regression test with one legacy user row, one human workflow row, one controller row, and two assistant rows; normal load returns four visible rows while `load_stage_messages` returns all protocol rows in order.

- [ ] **Step 6: Run persistence migration tests**

Run: `cargo test --test workflow_store && cargo test --test dialog`

Expected: all tests PASS and the existing day-7/day-8/day-10 upgrade fixtures remain valid.

- [ ] **Step 7: Commit**

```bash
git add src/workflow_store.rs src/dialog.rs src/lib.rs tests/workflow_store.rs tests/dialog.rs
git commit -m "feat: persist workflow task projections"
```

### Task 5: Atomic Workflow Inputs, Answers, Patches, and Processing Jobs

**Files:**
- Modify: `src/workflow_store.rs`
- Modify: `src/dialog.rs`
- Modify: `src/workflow.rs`
- Test: `tests/workflow_store.rs`
- Modify: `tests/agent.rs`

**Interfaces:**
- Consumes: `WorkflowInput`, validated `WorkflowTaskState`, `TaskStatePatch`, `StageProtocolMessage`, and existing message usage.
- Produces: `InputCommit`; `AnswerCommit`; `ProcessingLease`; `ControllerInputCommit`; `WorkflowRepository::{start_dialog_with_workflow_task, create_task_with_human_input, append_input, append_answer_for_processing, lease_processing, commit_await_user, commit_controller_decision, fail_processing}`.

- [ ] **Step 1: Write failing atomicity and concurrency tests**

```rust
#[test]
fn first_task_and_tagged_human_message_commit_together() {
    let mut fixture = WorkflowFixture::new();
    let committed = fixture.store.create_task_with_human_input(
        fixture.dialog_id,
        "Build a parser",
        "Build a parser",
        0,
    ).unwrap();
    let loaded = fixture.store.load_workflow(fixture.dialog_id).unwrap();
    assert_eq!(loaded.current_task.unwrap().id, committed.task.id);
    assert_eq!(
        fixture.store.load_stage_messages(committed.task.current_stage_run_id).unwrap()
            .iter().map(|row| row.message.content()).collect::<Vec<_>>(),
        ["Build a parser"]
    );
}

#[test]
fn new_dialog_scope_task_stage_and_first_message_are_one_transaction() {
    let mut store = DialogStore::open(&temp_database()).unwrap();
    let started = store.start_dialog_with_workflow_task(
        &RequestScope::new("alice", "memory-space").unwrap(),
        "System",
        "Build a parser",
    ).unwrap();
    assert_eq!(store.load(started.dialog_id).unwrap().messages.len(), 1);
    assert_eq!(store.load_workflow(started.dialog_id).unwrap()
        .current_task.unwrap().current_stage_run_id, started.stage_run_id);
}

#[test]
fn sqlite_index_rejects_two_unfinished_tasks_even_from_two_connections() {
    let fixture = SharedWorkflowFixture::new();
    let mut first = DialogStore::open(&fixture.path).unwrap();
    let mut second = DialogStore::open(&fixture.path).unwrap();
    first.create_task_with_human_input(fixture.dialog_id, "one", "one", 0).unwrap();
    assert!(matches!(
        second.create_task_with_human_input(fixture.dialog_id, "two", "two", 0),
        Err(StoreError::WorkflowConflict(_))
    ));
}
```

Use failure triggers to prove that dialog, dialog scope, message, input provenance, mapping, task creation, stage creation, and dialog pointer all roll back together. Add a stale `expected_version` test and a whitespace-only input test that leaves row counts unchanged.

- [ ] **Step 2: Run the focused tests and verify red**

Run: `cargo test --test workflow_store -- --nocapture`

Expected: FAIL because the atomic commands are not implemented.

- [ ] **Step 3: Define transaction command/result types**

```rust
pub struct InputCommit<'a> {
    pub dialog_id: i64,
    pub input: &'a WorkflowInput,
    pub protocol_text: &'a str,
    pub confidence: Option<f32>,
    pub expected_version: Option<u64>,
}

pub struct AnswerCommit<'a> {
    pub dialog_id: i64,
    pub task_id: WorkflowTaskId,
    pub stage_run_id: StageRunId,
    pub expected_version: u64,
    pub content: &'a str,
    pub usage: Option<TokenUsage>,
}

pub struct PersistedAnswer {
    pub message_id: i64,
    pub processing_id: i64,
}

pub struct ProcessingLease {
    pub processing_id: i64,
    pub assistant_message_id: i64,
    pub expected_version: u64,
    pub attempts: u32,
}
```

Every state-changing repository method begins an `IMMEDIATE` transaction, reloads the task row, compares `version`, validates the open stage run, and only then writes. Map unique/row-count races to `StoreError::WorkflowConflict(dialog_id)` rather than exposing raw SQLite constraint text.

- [ ] **Step 4: Implement new-task, continue, resume, and replan commits**

`start_dialog_with_workflow_task` must create the dialog, its `dialog_scopes` row, ordinal-1 task, planning stage, first visible message, input provenance, mapping, title, and current pointer in one transaction. `create_task_with_human_input` performs the same workflow inserts for an existing dialog after its prior task is `done` and allocates the next per-dialog ordinal. Both set the circular `current_stage_run_id` after obtaining the stage ID. `append_input` handles:

```rust
pub enum AcceptedInputEffect {
    ContinueSameStage,
    ResumeSameStage,
    RouteUnmanaged,
    Reject { reason: String },
}
```

`ReplanCurrent` is deliberately absent from this same-stage repository API; Task 6 sends it through the stage-change transaction after authorization and handoff generation. `Reject` stores the visible human input and audit result but does not map it to a stage or resume a paused task. `RouteUnmanaged` stores a visible legacy-style user message without task/stage mapping.

- [ ] **Step 5: Commit answers and pending processing atomically**

```rust
fn append_answer_for_processing(
    &mut self,
    command: AnswerCommit<'_>,
) -> Result<PersistedAnswer, StoreError>;
```

The transaction inserts the assistant message and usage, maps it to the current task/stage, inserts one `response_processing` row with status `pending`, checker name `continuation`, expected state version, and `attempts = 0`, then updates dialog activity. A trigger failure on `response_processing` must roll back the assistant message and usage. Add `UNIQUE(assistant_message_id, checker_name)`.

- [ ] **Step 6: Implement leases and idempotent checker completion**

```sql
UPDATE response_processing
SET status = 'processing', attempts = attempts + 1,
    updated_at = strftime('%Y-%m-%d %H:%M:%f','now')
WHERE id = ?1 AND status IN ('pending','processing','failed')
  AND attempts < 2 AND expected_version = ?2;
```

Normal in-process leasing accepts only `pending`/retryable `failed`; the recovery path may reclaim `processing` left by a crashed process. Concurrent recovery can duplicate a checker model call, but `expected_version`, the processing-row status guard, and unique effect keys allow only one persistent result. `lease_processing` returns `None` for a completed/terminal-failed row. `commit_await_user` validates and applies the patch, stores structured result JSON, sets status `completed`, and increments task version at most once. Repeating the method returns the prior completion without another patch application. `fail_processing` stores the sanitized error and marks `failed`; it never changes task state.

- [ ] **Step 7: Persist controller decisions and hidden inputs atomically**

```rust
pub struct ControllerInputCommit<'a> {
    pub processing_id: i64,
    pub task_id: WorkflowTaskId,
    pub stage_run_id: StageRunId,
    pub expected_version: u64,
    pub checker: &'a str,
    pub model: &'a str,
    pub triggering_assistant_message_id: i64,
    pub instruction: &'a str,
    pub intent: &'a WorkflowIntent,
    pub confidence: f32,
    pub accepted_patch: &'a TaskStatePatch,
}
```

For same-stage `Continue`, apply the patch, insert one hidden `role='user'` message, origin metadata, and stage mapping, then complete processing in the same transaction. Enforce `UNIQUE(processing_id)` on controller-origin workflow inputs. Retrying must return the existing message ID and must not insert a second instruction.

- [ ] **Step 8: Run persistence and existing agent tests**

Run: `cargo test --test workflow_store && cargo test --test agent`

Expected: all tests PASS. Existing non-workflow helpers should include `[workflow]\nenabled = false` so this task does not silently rewrite unrelated fixtures.

- [ ] **Step 9: Commit**

```bash
git add src/workflow_store.rs src/dialog.rs src/workflow.rs tests/workflow_store.rs tests/agent.rs
git commit -m "feat: commit workflow turns atomically"
```

### Task 6: Atomic Transitions, Handoffs, Idempotency, and Branch Copying

**Files:**
- Modify: `src/workflow_store.rs`
- Modify: `src/dialog.rs`
- Modify: `src/workflow.rs`
- Test: `tests/workflow_store.rs`
- Modify: `tests/dialog.rs`

**Interfaces:**
- Consumes: authorized transitions and validated `HandoffPayload` from Tasks 2-3, input/processing transactions from Task 5.
- Produces: `TransitionCommit`; `PersistedTransition`; `WorkflowRepository::commit_stage_change`; `WorkflowRepository::copy_workflow_branch`.

- [ ] **Step 1: Write failing all-or-nothing transition tests**

```rust
#[test]
fn transition_closes_old_stage_and_projects_handoff_in_one_commit() {
    let mut fixture = WorkflowFixture::execution();
    let result = fixture.store.commit_stage_change(fixture.transition_command()).unwrap();
    let current = fixture.store.load_workflow(fixture.dialog_id).unwrap().current_task.unwrap();
    assert_eq!(current.phase, TaskPhase::Validation);
    assert_eq!(current.version, fixture.source_version + 1);
    assert_ne!(current.current_stage_run_id, fixture.old_stage_run_id);
    assert_eq!(current.incoming_handoff_id, Some(result.transition_id));
    assert_eq!(current.checkpoint.summary, "Implementation complete");
    assert!(fixture.store.load_stage_messages(current.current_stage_run_id).unwrap()
        .iter().any(|row| row.message.content() == "start validation"));
}
```

Inject a trigger failure at each table touched by the transition and assert the old stage remains open, the task row/version unchanged, and no transition, handoff, input, mapping, or incoming stage exists. Add a retry test that returns the original `PersistedTransition` and a stale version test that writes nothing.

- [ ] **Step 2: Run transition tests and verify red**

Run: `cargo test --test workflow_store -- --nocapture`

Expected: FAIL because `commit_stage_change` is missing.

- [ ] **Step 3: Define the complete transition command**

```rust
pub struct TransitionCommit<'a> {
    pub dialog_id: i64,
    pub source_task: &'a WorkflowTaskState,
    pub authorization: &'a StageChangeAuthorization,
    pub triggering_input: &'a WorkflowInput,
    pub protocol_text: &'a str,
    pub confidence: Option<f32>,
    pub accepted_patch: Option<&'a TaskStatePatch>,
    pub handoff: &'a HandoffPayload,
    pub processing_id: Option<i64>,
}

pub struct PersistedTransition {
    pub transition_id: i64,
    pub input_message_id: i64,
    pub target_state: WorkflowTaskState,
}
```

Insert the triggering workflow input first inside the transaction and make `task_transitions.workflow_input_id UNIQUE`. That database-generated ID is the idempotency boundary; do not add a public API `idempotency_key` or require the user to supply one.

- [ ] **Step 4: Implement transition projection in one SQLite transaction**

Before the transaction, preview any checker patch against the source state and authorize the transition or replan against that preview; build the handoff from the preview without persisting it. In the transaction: reload/compare source version; revalidate the optional patch and stage change; insert the visible or hidden user-role message and workflow-input audit row; close the outgoing stage; insert the immutable transition with full handoff JSON and source/destination metadata; create the next stage run; map the triggering message to that incoming run; project the patch and handoff fields into one target task state; update `dialog_workflow_state`; initialize empty `task_stage_context`; complete optional response processing; commit. Replan uses event `replan_requested`, always creates a new planning run (including planning-to-planning), preserves task ID, and increments plan revision. The target task version is `source_version + 1`, not `+2`, because the patch and stage change are one accepted effect. Use this SQL guard for the state update:

```sql
UPDATE workflow_tasks
SET phase = ?1, status = 'active', plan_json = ?2,
    current_step_id = ?3, expected_action = ?4, checkpoint_json = ?5,
    current_stage_run_id = ?6, incoming_handoff_id = ?7,
    version = version + 1,
    updated_at = strftime('%Y-%m-%d %H:%M:%f','now')
WHERE id = ?8 AND version = ?9 AND current_stage_run_id = ?10;
```

Require exactly one updated row. The `done` transition still creates a terminal done stage run for audit/status, but no controller input may start ordinary work from it.

- [ ] **Step 5: Add deep workflow copying to `fork_dialog`**

```rust
fn copy_workflow_branch(
    tx: &rusqlite::Transaction<'_>,
    source_dialog_id: i64,
    target_dialog_id: i64,
    message_id_map: &BTreeMap<i64, i64>,
) -> Result<(), StoreError>;
```

Refactor existing message copying to retain an old-to-new message ID map. Copy tasks with new IDs, stage runs with new IDs, stage context, transitions/handoffs, input provenance, message mappings, response-processing records, and the current-task pointer. Rewrite every internal foreign key through explicit maps. A branch with an active task must evolve independently: pausing, patching, or transitioning it cannot change the source task/version.

- [ ] **Step 6: Test done/new-task boundaries and branch independence**

Pin task boundaries and branch independence with:

```rust
#[test]
fn only_done_dialogs_accept_a_next_human_task() {
    let mut fixture = WorkflowFixture::validation();
    assert!(fixture.start_human_task("next").is_err());
    fixture.pause().unwrap();
    assert!(fixture.start_human_task("next").is_err());
    fixture.finish_validation().unwrap();
    assert_eq!(fixture.start_human_task("next").unwrap().ordinal, 2);
    assert!(fixture.start_controller_task("forbidden").is_err());
}

#[test]
fn copied_branch_workflow_evolves_independently() {
    let mut fixture = WorkflowFixture::execution();
    let branch_id = fixture.fork().unwrap().new_dialog_id;
    fixture.store.pause_current_task(branch_id).unwrap();
    assert_eq!(fixture.current_for(branch_id).status, TaskStatus::Paused);
    assert_eq!(fixture.current().status, TaskStatus::Active);
}
```

- [ ] **Step 7: Run transition and branch suites**

Run: `cargo test --test workflow_store && cargo test --test dialog`

Expected: all tests PASS and `PRAGMA foreign_key_check` returns no rows for both original and branch databases.

- [ ] **Step 8: Commit**

```bash
git add src/workflow_store.rs src/dialog.rs src/workflow.rs tests/workflow_store.rs tests/dialog.rs
git commit -m "feat: make workflow transitions atomic"
```

### Task 7: Stage-Isolated Context, Compaction, and Facts

**Files:**
- Create: `src/workflow_context.rs`
- Modify: `src/context.rs`
- Modify: `src/facts.rs`
- Modify: `src/system_context.rs`
- Modify: `src/lib.rs`
- Create: `tests/workflow_context.rs`
- Modify: `tests/context.rs`
- Modify: `tests/facts.rs`

**Interfaces:**
- Consumes: `StageProtocolMessage`, `WorkflowTaskState`, existing `SystemBlock`, `ContextState`, `FactsState`, profile/memory blocks, and `ContextConfig`.
- Produces: `WorkflowStageContext`; `prepare_workflow_request`; `facts_candidates`; stage-local compaction input; `workflow_task` system block.

- [ ] **Step 1: Write failing isolation and ordering tests**

```rust
#[test]
fn ordinary_request_contains_only_current_stage_protocol_in_stable_order() {
    let prepared = prepare_workflow_request(WorkflowRequestInput {
        base_prompt: "BASE",
        inherited_blocks: vec![profile_block(), user_memory_block(), memory_task_block()],
        task: &validation_task(),
        stage_messages: &[
            human(41, "validate it"),
            controller(42, "run the focused tests"),
            assistant(43, "tests pass"),
        ],
        pending_input: None,
        context_config: &summary_config(),
        context_state: &ContextState::default(),
    });
    assert_eq!(prepared.system_block_names(), [
        "base", "user_profile", "user_memory", "task_memory", "workflow_task"
    ]);
    assert_eq!(
        prepared.messages().iter().filter(|m| m.role() != Role::System)
            .map(|m| (m.role(), m.content())).collect::<Vec<_>>(),
        [
            (Role::User, "validate it"),
            (Role::User, "run the focused tests"),
            (Role::Assistant, "tests pass"),
        ]
    );
}
```

Add old-task and old-stage messages containing unique leak markers and assert those markers are absent. Store a full handoff marker and assert only projected checkpoint fields appear. Create a legacy unmapped message after a done task and assert a managed stage request does not include it.

- [ ] **Step 2: Run context tests and verify red**

Run: `cargo test --test workflow_context -- --nocapture`

Expected: FAIL because workflow-specific context assembly is absent.

- [ ] **Step 3: Define stage-context input and task block**

```rust
pub struct WorkflowRequestInput<'a> {
    pub base_prompt: &'a str,
    pub inherited_blocks: Vec<SystemBlock>,
    pub task: &'a WorkflowTaskState,
    pub stage_messages: &'a [StageProtocolMessage],
    pub pending_input: Option<&'a str>,
    pub context_config: &'a ContextConfig,
    pub context_state: &'a ContextState,
}

pub struct WorkflowStageContext {
    pub prepared: PreparedContext,
    pub context_state: ContextState,
    pub facts_state: FactsState,
}

pub fn workflow_task_block(task: &WorkflowTaskState) -> SystemBlock {
    SystemBlock::new(
        "workflow_task",
        render_task_state(task),
        ContextScope::Task,
        CompactionPolicy::Exclude,
    )
}
```

`prepare_workflow_request` builds `SystemContext` in the exact order required by the spec, constructs a temporary `ChatHistory` from only current-stage protocol messages, then applies the existing summary/sliding-window/sticky-facts/branching selection within that local history.

- [ ] **Step 4: Scope summary and facts to the stage run**

Add serde derives to `ContextSummary`, `UsageTotals`, `ContextState`, and `FactsState`, then persist `ContextState` and `FactsState` as `task_stage_context.context_json` and `facts_json`:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StageReductionState {
    pub context: ContextState,
    pub facts: FactsState,
}

fn empty_stage_reductions() -> (String, String) {
    (
        serde_json::to_string(&ContextState::default()).unwrap(),
        serde_json::to_string(&FactsState::default()).unwrap(),
    )
}
```

On every new stage, initialize both to defaults. Add repository replace methods guarded by `(stage_run_id, expected_task_version, expected_stage_message_count)`. Reuse `plan_compaction` on stage-local history; never read `dialog_context` or `dialog_facts` for a managed request.

- [ ] **Step 5: Exclude controller instructions from sticky facts**

```rust
pub fn facts_candidates(messages: &[StageProtocolMessage]) -> Vec<Message> {
    messages.iter()
        .filter(|row| row.source != ProtocolSource::Controller)
        .map(|row| row.message.clone())
        .collect()
}
```

Pin summary/facts provenance separately:

```rust
#[test]
fn controller_is_summary_context_but_never_user_fact_evidence() {
    let rows = vec![
        human(1, "deadline Friday"),
        controller(2, "run cargo test"),
        assistant(3, "tests passed"),
    ];
    let summary = stage_history(&rows);
    assert!(summary.messages().iter().any(|m| m.content() == "run cargo test"));
    let facts = facts_candidates(&rows);
    assert_eq!(
        facts.iter().map(Message::content).collect::<Vec<_>>(),
        ["deadline Friday", "tests passed"]
    );
}
```

In the ordering test, assert profile/user-memory/memory-task blocks retain their existing scope/order and remain excluded from compaction.

- [ ] **Step 6: Run context and facts regressions**

Run: `cargo test --test workflow_context && cargo test --test context && cargo test --test facts`

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/workflow_context.rs src/context.rs src/facts.rs src/system_context.rs src/lib.rs tests/workflow_context.rs tests/context.rs tests/facts.rs
git commit -m "feat: isolate context by workflow stage"
```

### Task 8: Shared Workflow Input Handler and Pre-Answer Routing

**Files:**
- Create: `src/workflow_engine.rs`
- Modify: `src/agent.rs`
- Modify: `src/lib.rs`
- Create: `tests/workflow_engine.rs`
- Modify: `tests/agent.rs`

**Interfaces:**
- Consumes: interpreter/handoff services, `StateMachine`, workflow repository transactions, stage context assembler, config, profile, and memory blocks.
- Produces: `WorkflowInputHandler`; `WorkflowEngine`; `RoutingOutcome`; `WorkflowTurnEvent`; `AgentEvent::Workflow(WorkflowTurnEvent)`; `Agent::run_workflow_streaming`; dependency-injection constructor for tests.

- [ ] **Step 1: Write failing routing tests around a fake completion model**

```rust
#[tokio::test]
async fn human_transition_is_applied_before_the_ordinary_request() {
    let fixture = EngineFixture::execution()
        .interpret_as_transition(TransitionEvent::ExecutionCompleted, vec!["build green"])
        .handoff(valid_validation_handoff())
        .ordinary_answer("validation result")
        .checker_await_user();
    let result = fixture.engine.run_human_input("implementation done; test it", |_| Ok(())).await.unwrap();
    assert_eq!(result.final_state.phase, TaskPhase::Validation);
    let request = fixture.ordinary_requests().pop().unwrap();
    assert!(request.contains("\"phase\": \"validation\""));
    assert!(!request.contains("execution-only-marker"));
}
```

Pin every routing branch with a table and request counter:

```rust
#[tokio::test]
async fn routing_matrix_never_calls_ordinary_model_for_rejected_inputs() {
    for case in RoutingCase::all() {
        let fixture = EngineFixture::for_case(case);
        let outcome = fixture.run().await;
        assert_eq!(outcome.kind(), case.expected_outcome);
        assert_eq!(fixture.ordinary_call_count(), case.expected_ordinary_calls);
        assert_eq!(fixture.interpreter_call_count(), case.expected_interpreter_calls);
        assert_eq!(fixture.current().map(|s| s.status), case.expected_status);
        assert_eq!(fixture.current().map(|s| s.current_stage_run_id), case.expected_stage);
    }
}
```

`RoutingCase::all()` must contain: no-task human start; unfinished human start-new; paused human continue; paused human transition; paused human start-new; controller start-new; controller replan; human validation-pass reaching done with zero ordinary calls; and done-task interpreter failure routed unmanaged.

- [ ] **Step 2: Run engine routing tests and verify red**

Run: `cargo test --test workflow_engine -- --nocapture`

Expected: FAIL because `WorkflowEngine` and the handler do not exist.

- [ ] **Step 3: Define a single input-handling entry point**

```rust
pub enum RoutingOutcome {
    Managed {
        input_message_id: i64,
        state: WorkflowTaskState,
    },
    Unmanaged {
        input_message_id: i64,
    },
    Rejected {
        reason: String,
        state: Option<WorkflowTaskState>,
    },
}

pub struct WorkflowInputHandler<'a> {
    pub store: &'a mut DialogStore,
    pub handoff_builder: &'a HandoffBuilder,
}

impl WorkflowInputHandler<'_> {
    pub async fn handle(
        &mut self,
        dialog_id: i64,
        snapshot: DialogWorkflowSnapshot,
        input: WorkflowInput,
        protocol_text: &str,
        confidence: Option<f32>,
    ) -> Result<RoutingOutcome, WorkflowEngineError>;
}
```

The method validates source restrictions first. It delegates all phase decisions to `StateMachine`; for an authorized ordinary transition or human replan, it builds the handoff before calling `commit_stage_change`. It never exposes a repository call that accepts an arbitrary target phase.

- [ ] **Step 4: Split the existing Agent path without changing non-workflow behavior**

Rename the current body to `run_legacy_streaming`. `run_streaming` dispatches to the workflow engine only when `[workflow].enabled = true`, the agent has a store, and it has or can create a persistent dialog. In-memory `Agent::new`/`from_client` keep the current behavior. Add this injection seam:

```rust
pub struct WorkflowModels {
    pub interpreter: Arc<dyn CompletionModel>,
    pub checker: Arc<dyn CompletionModel>,
    pub handoff: Arc<dyn CompletionModel>,
}

pub fn with_workflow_models(mut self, models: WorkflowModels) -> Self {
    self.workflow_models = Some(models);
    self
}
```

Production constructors create three `DeepSeekCompletionModel` values from the configured model names. Tests use queued fake models so checker behavior is independent from HTTP streaming fixtures.

- [ ] **Step 5: Persist and route human input before ordinary generation**

For an existing task, call the interpreter with raw input and compact state. For no task, synthesize `Human/StartNewTask`. If the agent has no dialog ID, route that input through `start_dialog_with_workflow_task`; do not call the legacy `start_dialog_in_scope`, because it cannot atomically tag the first message. Pass later inputs through `WorkflowInputHandler`, then build the ordinary request only from the returned state/stage. A rejected input emits `WorkflowTurnEvent::InputRejected` and returns without calling the ordinary model. If handoff construction fails, store a human trigger as a rejected visible workflow input with no stage mapping, or mark controller processing failed without emitting a hidden input; keep the old task state and make no ordinary call. Unmanaged completed-task fallback uses base/profile/memory plus only the new input; it must not resurrect the completed stage transcript, and its persisted assistant answer has no stage mapping or processing job. A stage change whose target is `done` commits the triggering input/transition and stops without an ordinary call.

- [ ] **Step 6: Stream one managed answer and create its processing job**

Use the existing `DeepSeekClient::stream_chat_events`, buffer the full answer, and forward text/usage as events. Only after `[DONE]` and a nonblank answer, call `append_answer_for_processing`. On API/stream/output failure, retain the already-persisted input, commit no assistant row, and leave workflow state unchanged.

- [ ] **Step 7: Run routing and legacy regressions**

Run: `cargo test --test workflow_engine human_ && cargo test --test agent`

Expected: all tests PASS. Existing legacy tests execute with workflow disabled; new managed tests assert the interpreter request precedes the ordinary request.

- [ ] **Step 8: Commit**

```bash
git add src/workflow_engine.rs src/agent.rs src/lib.rs tests/workflow_engine.rs tests/agent.rs
git commit -m "feat: route inputs through workflow handler"
```

### Task 9: Advisory Response Pipeline and Bounded Autonomous Loop

**Files:**
- Modify: `src/workflow_engine.rs`
- Modify: `src/workflow_model.rs`
- Modify: `src/workflow_store.rs`
- Modify: `src/agent.rs`
- Modify: `src/main.rs`
- Test: `tests/workflow_engine.rs`
- Test: `tests/workflow_store.rs`

**Interfaces:**
- Consumes: pending processing rows, `ResponseChecker`, controller input commits, shared input handler, and workflow limits.
- Produces: `ResponsePipeline`; `AutonomyBudget`; `AutonomyStopReason`; restart processing recovery; full managed-turn loop.

- [ ] **Step 1: Write failing checker/pipeline behavior tests**

```rust
#[tokio::test]
async fn await_user_applies_one_patch_and_ends_the_loop() {
    let fixture = EngineFixture::planning()
        .ordinary_answer("plan drafted")
        .checker_result(check_result(
            patch_current_step("design", "ask for approval"),
            ControllerDecision::AwaitUser,
        ));
    let result = fixture.engine.run_human_input("draft the plan", |_| Ok(())).await.unwrap();
    assert_eq!(result.stop_reason, AutonomyStopReason::AwaitUser);
    assert_eq!(result.autonomous_turns, 0);
    assert_eq!(result.final_state.current_step_id.as_deref(), Some("design"));
    assert_eq!(fixture.ordinary_call_count(), 1);
}

#[tokio::test]
async fn continue_persists_hidden_input_then_runs_another_ordinary_turn() {
    let fixture = EngineFixture::execution()
        .ordinary_answers(["implemented part one", "implemented part two"])
        .checker_results([
            continue_result("implement part two", 0.95),
            await_user_result(),
        ]);
    let result = fixture.engine.run_human_input("continue", |_| Ok(())).await.unwrap();
    assert_eq!(result.autonomous_turns, 1);
    assert_eq!(fixture.ordinary_call_count(), 2);
    let protocol = fixture.current_stage_protocol();
    assert!(protocol.iter().any(|row| row.source == ProtocolSource::Controller));
    assert!(!fixture.visible_transcript().iter().any(|m| m.content() == "implement part two"));
}
```

Pin the remaining pipeline policies with:

```rust
#[tokio::test]
async fn unsafe_checker_outcomes_preserve_answer_and_state() {
    for outcome in [
        CheckerFixtureOutcome::MalformedJson,
        CheckerFixtureOutcome::ApiFailure,
        CheckerFixtureOutcome::LowConfidence,
        CheckerFixtureOutcome::ConflictingPatches,
    ] {
        let fixture = EngineFixture::execution().with_checker_outcome(outcome);
        let before = fixture.current();
        fixture.engine.run_human_input("continue", |_| Ok(())).await.unwrap();
        assert_eq!(fixture.current(), before);
        assert!(fixture.visible_transcript().iter().any(|m| m.content() == "ordinary answer"));
        assert_eq!(fixture.controller_input_count(), 0);
    }
}

#[tokio::test]
async fn emitted_transition_uses_handoff_model_and_shared_handler() {
    let fixture = EngineFixture::execution().checker_transition().handoff(valid_validation_handoff());
    fixture.engine.run_human_input("continue", |_| Ok(())).await.unwrap();
    assert_eq!(fixture.current().phase, TaskPhase::Validation);
    assert_eq!(fixture.model_names(), ["interpreter-a", "checker-b", "handoff-c"]);
    assert_eq!(fixture.transition_count(), 1);
}
```

- [ ] **Step 2: Run pipeline tests and verify red**

Run: `cargo test --test workflow_engine -- --nocapture`

Expected: FAIL because only one ordinary turn is implemented.

- [ ] **Step 3: Implement a pipeline that collects proposals before applying effects**

```rust
pub struct ResponsePipeline {
    checkers: Vec<Arc<dyn ResponseChecker>>,
}

pub enum PipelineOutcome {
    AwaitUser { patch: TaskStatePatch },
    Continue { patch: TaskStatePatch, instruction: String, confidence: f32 },
    Transition { patch: TaskStatePatch, event: TransitionEvent, evidence: Vec<String>, confidence: f32 },
    FailedOpen { error: String },
    Conflict { checker_names: Vec<String> },
}
```

Run every configured advisory checker against the same immutable `CheckContext` and task version. Day 13 config installs only `ContinuationChecker`, but the collector must reject multiple non-empty incompatible state patches/decisions. Do not persist any patch until collection and domain validation succeed. A blocking checker variant is representable but rejected at startup with `WorkflowEngineError::BlockingCheckerRequiresBufferedDelivery` because Day 13 streams ordinary output.

- [ ] **Step 4: Define the exact autonomy accounting policy**

```rust
pub struct AutonomyBudget {
    max_turns: u32,
    max_tokens: u64,
    turns: u32,
    tokens: u64,
    fingerprints: HashSet<StateFingerprint>,
    usage_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutonomyStopReason {
    AwaitUser,
    Done,
    TurnLimit,
    TokenLimit,
    RepeatedState,
    MissingUsage,
    LowConfidence,
    CheckerFailed,
    TransitionFailed,
    AwaitUserAfterRestart,
}
```

Start a fresh budget for each real human input. Count provider `total_tokens` from interpreter, handoff, every ordinary call, and every checker call. The first human-triggered ordinary response does not increment `turns`; each additional controller-triggered ordinary response does. If any enabled provider call lacks usage, set `usage_complete = false` and prohibit autonomous continuation. Check prospective turn/token limits and the next fingerprint before persisting a controller input.

- [ ] **Step 5: Implement the autonomous loop without recursive calls**

```rust
loop {
    let answer = self.run_one_ordinary_turn(route, &mut budget, on_event).await?;
    let processing = self.process_answer(&answer, &mut budget).await;
    match processing? {
        NextAction::Stop(reason) => return self.finish(reason, budget),
        NextAction::Controller(input) => {
            route = self.input_handler.handle_controller(input).await?;
            if route.state().phase == TaskPhase::Done {
                return self.finish(AutonomyStopReason::Done, budget);
            }
        }
    }
}
```

Use local variables rather than calling `run_human_input` recursively, so turn/token/fingerprint limits and cancellation cover the whole chain. Emit `WorkflowTurnEvent::AutonomousTurnStarted { number, phase }` before each controller-triggered ordinary call.

- [ ] **Step 6: Make answer processing idempotent**

Lease the assistant's processing row before calling checkers. For `AwaitUser`, call `commit_await_user`; for `Continue`, call `commit_controller_decision`; for a transition, build handoff then call `commit_stage_change` with `processing_id`. A lost version race converts to `AutonomyStopReason::CheckerFailed`, reloads state, and emits no new input. Re-running a completed processing row returns its stored terminal outcome and cannot duplicate a patch or controller message.

- [ ] **Step 7: Recover pending advisory work on restart without autonomous resume**

```rust
pub async fn recover_pending_processing(
    &mut self,
    dialog_id: i64,
) -> Result<Vec<RecoveredProcessing>, WorkflowEngineError>;
```

Before reading the next prompt, retry each current-version `pending` row, crash-left `processing` row, and `failed` row with `attempts < 2`. On the first failure leave it `failed` with attempts `1`; on a later restore attempt leave it terminal failed-open at attempts `2`, which excludes it from later recovery queries. On success, coerce `Continue`/`EmitTransition` to `AwaitUserAfterRestart`: a valid in-stage patch may commit, but no hidden controller input, handoff, transition, or ordinary call occurs until a new human message arrives. This satisfies both pending-work recovery and the rule that a controller cannot resume a restored process by itself.

Call `close_stale_processing` first. A job created at version N is never retried against version N+1, even if the latter was produced by a competing session. Test that its stored proposal cannot emit a transition or controller input after the version changes.

Invoke recovery from `main.rs` immediately after constructing a resumed `Agent` and before replaying the prompt. Surface failures as advisory warnings; do not convert them into controller work.

- [ ] **Step 8: Test every loop stop condition at the boundary**

Set `max_autonomous_turns = 2` and prove exactly two controller ordinary turns occur, not three. Set budget to the exact accumulated token total and prove equality is allowed while the next positive-usage call is refused. Feed the same fingerprint twice and assert no second hidden input. Feed absent usage and assert the completed answer remains but autonomy stops. Reach `done` and assert the controller cannot create the next task.

- [ ] **Step 9: Run workflow engine/store tests**

Run: `cargo test --test workflow_engine && cargo test --test workflow_store`

Expected: all tests PASS.

- [ ] **Step 10: Commit**

```bash
git add src/workflow_engine.rs src/workflow_model.rs src/workflow_store.rs src/agent.rs src/main.rs tests/workflow_engine.rs tests/workflow_store.rs
git commit -m "feat: bound autonomous workflow continuation"
```

### Task 10: Cancellation, Pause, and Human-Only Resume

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/workflow_store.rs`
- Modify: `src/workflow_engine.rs`
- Modify: `src/agent.rs`
- Modify: `src/main.rs`
- Modify: `src/terminal.rs`
- Test: `tests/workflow_store.rs`
- Test: `tests/workflow_engine.rs`
- Modify: `tests/cli.rs`

**Interfaces:**
- Consumes: one cancellable workflow future and current dialog workflow state.
- Produces: `WorkflowRepository::pause_current_task`; `Agent::pause_current_workflow`; CLI-level `tokio::select!` cancellation; human resume behavior.

- [ ] **Step 1: Write failing pause/resume repository tests**

```rust
#[test]
fn pausing_and_human_resume_preserve_the_stage_run() {
    let mut fixture = WorkflowFixture::execution();
    let stage = fixture.current().current_stage_run_id;
    fixture.store.pause_current_task(fixture.dialog_id).unwrap();
    let paused = fixture.current();
    assert_eq!(paused.status, TaskStatus::Paused);
    assert_eq!(paused.current_stage_run_id, stage);
    fixture.store.append_input(InputCommit::human_continue(
        fixture.dialog_id, paused.version, "continue",
    )).unwrap();
    let resumed = fixture.current();
    assert_eq!(resumed.status, TaskStatus::Active);
    assert_eq!(resumed.current_stage_run_id, stage);
}
```

Pin the non-happy pause cases with:

```rust
#[test]
fn pause_and_resume_source_rules_preserve_state_on_rejection() {
    let mut empty = WorkflowFixture::empty();
    assert_eq!(empty.store.pause_current_task(empty.dialog_id).unwrap(), PauseOutcome::NoTask);
    let mut done = WorkflowFixture::done();
    assert_eq!(done.store.pause_current_task(done.dialog_id).unwrap(), PauseOutcome::AlreadyDone);
    let mut paused = WorkflowFixture::execution();
    paused.pause().unwrap();
    let before = paused.current();
    assert!(paused.controller_continue("resume").is_err());
    assert!(paused.start_human_task("other").is_err());
    assert_eq!(paused.current(), before);
    assert_eq!(paused.human_replan("change plan").unwrap().status, TaskStatus::Active);
}
```

- [ ] **Step 2: Run pause tests and verify red**

Run: `cargo test --test workflow_store -- --nocapture`

Expected: FAIL because pause operations are missing.

- [ ] **Step 3: Add Tokio signal support and a post-cancellation pause operation**

Change Tokio features to:

```toml
tokio = { version = "1.53.1", features = ["io-std", "io-util", "macros", "rt-multi-thread", "signal"] }
```

Implement `pause_current_task(dialog_id)` as one `IMMEDIATE` transaction that loads the current task and updates `status='paused', version=version+1` only when `phase <> 'done' AND status='active'`. It closes no stage, creates no stage, and edits no checkpoint.

- [ ] **Step 4: Cancel the complete request lifecycle from the CLI**

Extract the existing send branch into `run_prompt`. Wrap that future rather than individual HTTP calls:

```rust
let interrupted = tokio::select! {
    biased;
    signal = tokio::signal::ctrl_c() => {
        signal?;
        true
    }
    result = run_prompt(&mut agent, &user_message, &mut stdout, &mut stderr) => {
        result?;
        false
    }
};
if interrupted {
    stdout_ui.finish_interrupted_response(&mut stdout)?;
    agent.pause_current_workflow()?;
    stdout_ui.write_block(
        &mut stdout,
        BlockStyle::System,
        "Task paused. The partial model result was discarded.",
    )?;
    return Ok(());
}
```

The `tokio::select!` branch drops the borrowed engine/HTTP future before `pause_current_workflow` writes SQLite. `finish_interrupted_response` writes a terminal reset/newline so a dropped streaming block cannot leave ANSI/live-status state active; it does not claim the partial text was committed. The signal branch itself performs no database I/O. The same wrapper covers interpreter, handoff, ordinary stream, checker, and every autonomous controller turn.

Wrap `recover_pending_processing` in the same `tokio::select!` pattern before the first prompt. `Ctrl+C` during restart recovery drops the checker future, pauses the current non-done task, and exits without emitting a controller input.

- [ ] **Step 5: Prove incomplete ordinary output and service calls do not commit**

Use a delayed wiremock SSE stream, start the CLI, send one prompt, wait until the first fragment is observed, send `SIGINT`, and inspect SQLite. Assert the input exists, no assistant message or processing row exists for the partial response, task status is paused, and checkpoint/stage ID are unchanged. For interpreter/checker/handoff tests, delay the relevant fake `CompletionModel` future behind a `Notify`, cancel the engine future, then call pause and assert no model proposal effect was persisted.

- [ ] **Step 6: Add the Unix CLI interruption test without a new crate**

```rust
#[cfg(unix)]
fn send_sigint(child: &std::process::Child) {
    let status = std::process::Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}
```

Also start the CLI with no prior dialog, interrupt while the first interpreter/ordinary call is pending, and assert no synthetic empty workflow task was created solely for pausing.

- [ ] **Step 7: Test restore does not resume until accepted human input**

Open a paused dialog with `--resume`, make the mock server fail on any request, wait for the prompt, and assert no call occurred. Then enter `continue`; assert status becomes active before the ordinary request and the same stage run/task ID is used. Repeat with a rejected new-task interpretation and assert the task remains paused.

- [ ] **Step 8: Run interruption tests**

Run: `cargo test --test workflow_store pausing && cargo test --test workflow_engine cancellation && cargo test --test cli interrupt -- --nocapture`

Expected: all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock src/workflow_store.rs src/workflow_engine.rs src/agent.rs src/main.rs src/terminal.rs tests/workflow_store.rs tests/workflow_engine.rs tests/cli.rs
git commit -m "feat: pause workflow tasks on interrupt"
```

### Task 11: Task Status, Transcript Safety, and Workflow Observability

**Files:**
- Modify: `src/chat.rs`
- Modify: `src/terminal.rs`
- Modify: `src/main.rs`
- Modify: `src/agent.rs`
- Modify: `src/debug_log.rs`
- Modify: `src/workflow_engine.rs`
- Modify: `tests/chat.rs`
- Modify: `tests/cli.rs`
- Modify: `tests/debug_log.rs`
- Modify: `tests/agent.rs`

**Interfaces:**
- Consumes: workflow task snapshots, stage/processing status, and workflow events.
- Produces: `InputAction::TaskStatus`; `WorkflowStatus`; `Agent::workflow_status`; `TerminalUi::write_workflow_status`; redacted workflow debug events.

- [ ] **Step 1: Write failing command/status and hidden-transcript tests**

```rust
#[test]
fn parses_task_status_command() {
    assert_eq!(parse_input("/task"), InputAction::TaskStatus);
    assert!(matches!(parse_input("/task extra"), InputAction::InvalidCommand(_)));
}

#[test]
fn status_distinguishes_committed_state_from_processing_proposals() {
    let status = fixture.status_with_pending_transition();
    assert_eq!(status.phase, TaskPhase::Execution);
    assert_eq!(status.processing, ProcessingStatus::Pending);
    assert_ne!(status.phase, TaskPhase::Validation);
}
```

Pin transcript/title/facts safety with:

```rust
#[tokio::test]
async fn controller_text_is_not_human_transcript_title_or_fact_input() {
    let fixture = CliWorkflowFixture::with_controller_instruction("SYNTHETIC_SECRET").await;
    let replay = fixture.resume_output().await;
    assert!(!replay.contains("you> SYNTHETIC_SECRET"));
    assert!(!fixture.dialog_title().contains("SYNTHETIC_SECRET"));
    let facts_request = fixture.facts_request_json();
    assert!(!facts_request.contains("SYNTHETIC_SECRET"));
    assert!(fixture.stage_protocol_text().contains("SYNTHETIC_SECRET"));
}
```

- [ ] **Step 2: Run status tests and verify red**

Run: `cargo test --test chat task_status && cargo test --test cli workflow_status -- --nocapture`

Expected: FAIL because `/task` and workflow display are absent.

- [ ] **Step 3: Define the status projection**

```rust
pub struct WorkflowStatus {
    pub task_id: WorkflowTaskId,
    pub ordinal: u32,
    pub phase: TaskPhase,
    pub status: TaskStatus,
    pub plan_revision: u32,
    pub current_step_id: Option<String>,
    pub expected_action: Option<String>,
    pub stage_sequence: u32,
    pub processing: Option<ProcessingStatus>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessingStatus { Pending, Processing, Completed, Failed }
```

`Agent::workflow_status` reloads the current snapshot and newest processing state. `/task` prints `No workflow task in this dialog.` when absent. After `--resume` replay, print the same status once without changing or resuming it. `TerminalUi::write_workflow_status` displays ordinal and internal ID distinctly from the memory-task label already printed by the CLI.

- [ ] **Step 4: Render multi-turn autonomous output without exposing controller text**

Add `Workflow(WorkflowTurnEvent)` to `AgentEvent` and handle these nested events in `main.rs`:

```rust
pub enum WorkflowTurnEvent {
    ResponseStarted { autonomous_turn: u32, phase: TaskPhase },
    AutonomousTurnStarted { number: u32, phase: TaskPhase },
    InputRejected { reason: String },
    ProcessingFailed { checker: String, error: String },
    Stopped { reason: AutonomyStopReason },
}
```

Finish the current assistant block before the next `ResponseStarted`, print only a neutral status such as `Controller · autonomous turn 1 · execution`, and never print the synthetic instruction. Preserve the current compaction/facts/debug warning behavior for each ordinary turn.

- [ ] **Step 5: Add metadata-only debug logging by default**

```rust
pub struct WorkflowDebugMetadata<'a> {
    pub source: &'a str,
    pub component: &'a str,
    pub model: &'a str,
    pub mode: &'a str,
    pub input_version: u64,
    pub output_version: Option<u64>,
    pub proposed_event: Option<&'a str>,
    pub accepted: bool,
    pub autonomous_turn: u32,
    pub autonomous_tokens: u64,
    pub stage_run_id: i64,
    pub transition_id: Option<i64>,
    pub processing_status: &'a str,
    pub usage: Option<TokenUsage>,
}
```

When `log_payloads=false`, log only these fields plus lengths/counts. When true, add raw interpreter/checker output, plan/checkpoint, controller instruction, and handoff under a `payload` key. Reuse API-key redaction. Tests must use a unique secret marker in every payload field and assert it is absent from default logs and present only when payload logging is enabled.

- [ ] **Step 6: Run transcript/observability regressions**

Run: `cargo test --test chat && cargo test --test debug_log && cargo test --test agent && cargo test --test cli workflow_ -- --nocapture`

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/chat.rs src/terminal.rs src/main.rs src/agent.rs src/debug_log.rs src/workflow_engine.rs tests/chat.rs tests/cli.rs tests/debug_log.rs tests/agent.rs
git commit -m "feat: expose safe workflow diagnostics"
```

### Task 12: End-to-End Scenarios, Documentation, and Final Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/DAYS.md`
- Create: `docs/day13-results.md`
- Modify: `tests/cli.rs`
- Modify: `tests/agent.rs`
- Modify: `tests/workflow_engine.rs`
- Modify: any source file required to fix a failing acceptance scenario, limited to behavior specified in the design.

**Interfaces:**
- Consumes: the complete workflow feature from Tasks 1-11.
- Produces: executable acceptance coverage, user documentation, and a clean verified branch.

- [ ] **Step 1: Add a complete legal lifecycle CLI test**

Drive one persisted dialog through these model-controlled steps with ordered wiremock/fake-model responses:

```text
human: design the parser
planning answer -> human transition PlanningCompleted
execution answer -> controller Continue
execution answer -> controller EmitTransition(ExecutionCompleted)
validation answer -> controller EmitTransition(ValidationFailed)
repair execution answer -> human transition ExecutionCompleted
validation answer -> controller EmitTransition(ValidationPassed)
```

Assert final phase `done`, every stage sequence is strictly increasing, the two execution stage runs have different IDs, all transition events are append-only and ordered, controller messages are hidden in CLI replay, and only current-stage messages were sent in each ordinary request.

- [ ] **Step 2: Add forbidden lifecycle and restart scenarios**

Test `planning -> validation`, `execution -> done`, and `done -> execution` proposals; each must fail locally with no ordinary model call and no state/version change. Then test a process stopping after an assistant answer but before checker completion: resume processes the pending advisory job once, emits no autonomous input before human contact, and a later human `continue` resumes from the stored current step without the user repeating the plan.

- [ ] **Step 3: Add an end-to-end context leak assertion**

Put unique markers in task 1 planning/execution, its full handoff, task 2 planning, a controller instruction, profile, user memory, and memory-task memory. Capture the task 2 execution request. Assert it contains base/profile/user-memory/memory-task/current task-state/current stage messages in order; it excludes task 1 raw markers/full handoff, task 2 old planning raw marker, and controller text from facts; and it includes only the checkpoint fields projected by the accepted task 2 handoff.

- [ ] **Step 4: Document operator-visible behavior and exact configuration**

Add README sections covering:

```markdown
## Workflow tasks

Persistent dialogs track one unfinished workflow task through planning,
execution, validation, and done. `/task` shows the committed state. Model
checkers may propose work, but the application validates every transition.

Press Ctrl+C during model work to cancel the current request and pause the
task. Reopen the dialog and send `continue`, a transition instruction, or a
replan request to resume; loading the dialog alone does not resume it.

Controller-generated continuation messages are stored for audit but hidden
from transcript replay and excluded from user facts and memory.
```

Explain that `--task` still selects the durable memory namespace, not the workflow task. Document all `[workflow]` keys and defaults. Add Day 13 acceptance evidence and exact test commands to `docs/day13-results.md`, then link it from `docs/DAYS.md`.

- [ ] **Step 5: Run formatting and static checks**

Run: `cargo fmt --check`

Expected: PASS. If it fails, run `cargo fmt`, inspect the diff, then rerun `cargo fmt --check`.

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: PASS with zero warnings.

- [ ] **Step 6: Run the complete test suite**

Run: `cargo test --all-targets --all-features`

Expected: every unit, integration, migration, wiremock, interruption, and end-to-end test PASS.

- [ ] **Step 7: Audit forbidden data flows directly**

Run:

```bash
rg -n "set_phase|idempotency_key" src tests
rg -n "ProtocolSource::Controller|source = 'controller'|source='controller'" src
rg -n "dialog_context|dialog_facts" src/workflow_context.rs src/workflow_engine.rs
```

Expected: the first command returns no application API that bypasses the reducer and no user-supplied transition idempotency key; the second shows explicit controller filtering/audit paths; the third shows no managed-context reads from dialog-wide summary/facts tables.

- [ ] **Step 8: Review the final diff against the specification**

Run: `git diff --check && git status --short && git log --oneline -12`

Expected: no whitespace errors; only intended source/test/docs changes are present; the task commits are visible. Manually trace one human transition and one controller transition from parser to handler to reducer to transaction, and confirm no layer can supply an arbitrary target phase.

- [ ] **Step 9: Commit documentation and acceptance coverage**

```bash
git add README.md docs/DAYS.md docs/day13-results.md tests/cli.rs tests/agent.rs tests/workflow_engine.rs src
git commit -m "test: verify workflow lifecycle end to end"
```

- [ ] **Step 10: Re-run final verification after the commit**

Run: `cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-targets --all-features && git status --short`

Expected: all commands PASS and the worktree is clean.
