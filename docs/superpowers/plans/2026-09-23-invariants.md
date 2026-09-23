# Day 14 Invariants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist project invariants and enforce them with a first, blocking LLM checker before answer delivery and human replan commits.

**Architecture:** Add a scoped SQLite repository and context provider. Extend the ordered checker pipeline with a typed invariant verdict and blocking pass, while preserving the advisory continuation pass. Use the same invariant checker on proposed human replan and task creation.

**Tech Stack:** Rust 2024, rusqlite, serde, Tokio, wiremock.

**Spec:** `docs/superpowers/specs/2026-09-23-invariants-design.md`

## Global Constraints

- Invariant rules are keyed by `(user_id, task_id, rule_id)` and never stored as dialog messages.
- With no active rules, existing streaming behavior remains unchanged.
- With rules, denied or unverified candidate text never reaches output callbacks or assistant-message persistence.
- Blocking checker runs before advisory continuation; denial prevents later checkers.
- Human replan and task creation are checked before state commit.

## Review Focus

- A conflicting replan leaves task phase, plan, stage run, and version unchanged.
- A denied generated answer leaves no assistant row or response-processing job.
- A malformed or failing checker cannot release buffered text.
- Different project scopes cannot read each other's rules.
- A rule removed during a later turn is absent from that turn's request.

---

### Task 1: Scoped rule repository and commands

**Files:** `src/invariants.rs`, `src/dialog.rs`, `src/agent.rs`, `src/chat.rs`, `src/main.rs`, `src/lib.rs`, `tests/invariants.rs`, `tests/chat.rs`.

- [ ] Write tests for scoped persistence, validation, context block, and command parsing.
- [ ] Run focused tests and observe failure from missing API.
- [ ] Add the invariant domain, SQLite table and repository methods, agent accessors, and local commands.
- [ ] Run focused tests and confirm pass.

### Task 2: LLM invariant checker and ordered blocking pipeline

**Files:** `src/invariants.rs`, `src/workflow_model.rs`, `src/workflow_engine.rs`, `tests/invariants.rs`, `tests/workflow_engine.rs`.

- [ ] Write tests for strict verdict parsing, known IDs, early denial, blocking error, and advisory checker suppression.
- [ ] Run focused tests and observe failure from missing behavior.
- [ ] Extend checker result and pipeline to support ordered blocking and advisory passes.
- [ ] Run focused tests and confirm pass.

### Task 3: Buffered delivery and precommit workflow checks

**Files:** `src/workflow_engine.rs`, `src/agent.rs`, `tests/workflow_engine.rs`, `tests/agent.rs`, `tests/cli.rs`.

- [ ] Write tests proving no emitted/saved denied answer and unchanged state after denied replan/task creation.
- [ ] Run focused tests and observe failure from current streaming or missing guard.
- [ ] Wire the invariant checker into ordinary turns and proposed human workflow intents.
- [ ] Run focused tests and confirm pass.

### Task 4: Documentation and final verification

**Files:** `README.md`, `docs/DAYS.md`, `docs/day14-results.md`, `deepseek.example.toml`.

- [ ] Document commands, failure behavior, and limits.
- [ ] Run `cargo fmt --check`, strict Clippy, and `cargo test --all-targets --all-features`.
- [ ] Review diff and record verified outcomes.
