# Day 14: Durable invariants and blocking checks

## Intent

The persistent assistant must retain project rules outside conversation history, include them in ordinary model requests, and withhold answers or workflow changes that conflict with them. A rule has a stable ID and natural-language text. Rules belong to the existing memory-task address `(user_id, task_id)`, not to a dialog or a finite-state workflow task.

## Storage and editing

SQLite owns an `invariants` table keyed by `(user_id, task_id, rule_id)`. Local `/invariant add <id> <text>`, `/invariant remove <id>`, and `/invariant list` commands edit task-scoped rules. Global `[[invariants]]` entries in the application TOML are read-only at runtime, are not copied into SQLite, and win on duplicate IDs. Ordinary messages and controller messages cannot edit rules. Every ordinary request receives a separate `invariants` system block, excluded from compaction. An empty set adds no block and preserves Day 13 behavior.

## Checks and delivery

The existing ordered response-checker pipeline gains a first, blocking `InvariantChecker`; `ContinuationChecker` remains advisory after it. The invariant checker uses an LLM and returns strict `allow` or `deny` JSON, with known rule IDs and explanations on denial. A denial stops the checker chain. An error or malformed blocking result also stops the chain. With active rules, ordinary output is buffered until the blocking check accepts it; a denied candidate is never shown or saved. The application renders a refusal from the known rule texts and validated checker reasons.

The same checker is called on a proposed human `ReplanCurrent` before the input handler commits it. It also checks initial/new task goals before task creation. For controller patches and handoffs, the full candidate response remains checked before the continuation checker runs; Day 14 does not add a second LLM evaluation of each resulting state projection. This is a deliberate first-iteration boundary, not a claim that natural-language rules are mechanically proven for every state field.

## Failure and evidence

If the blocking model is unavailable or returns invalid output, show a local verification-unavailable refusal and do not deliver or persist the candidate. Tests use a fake completion model to verify ordering, withholding, scope, storage, replan behavior, and explanation formatting. Real-world semantic accuracy of the LLM is evaluated separately with a conflict corpus; it is not a mathematical guarantee.
