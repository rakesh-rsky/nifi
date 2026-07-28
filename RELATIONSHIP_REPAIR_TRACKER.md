# Relationship Repair Improvement Tracker

## Objective

Fix relationship-selection regressions introduced during simplification while preserving the simplified architecture:

- Keep deleted repair-expansion classes deleted.
- Keep normal prompt context small.
- Keep the validator as the source of truth.
- Move deterministic relationship decisions into Java.
- Use the LLM only when Java cannot safely repair.

## Scope

This is a targeted correctness improvement, not a feature expansion.

Primary regression:

```text
Unknown source relationship 'success'
Fix: Use a discovered relationship: [failure, merged, original]
```

Example failed flow:

```text
MQTT -> batch max 1000 JSON messages -> InvokeHTTP with 5-way load balancing
log success and failure separately
```

## Phase 1: Relationship Validation Fix

| # | Action | Target | Status | Notes |
|---|--------|--------|--------|-------|
| 1 | Change | `FlowSpecificationValidator` | Done | Missing processor relationships are no longer blindly defaulted to `success` |
| 2 | Add | Relationship inference helper | Done | Infers only when there is exactly one safe non-failure output |
| 3 | Preserve | Validator source of truth | Done | Unsupported/ambiguous relationships still fail before NiFi changes |

## Phase 2: Minimal Repair Hints

| # | Action | Target | Status | Notes |
|---|--------|--------|--------|-------|
| 1 | Add | Private formatter in `CopilotController` | Done | Did not recreate `RepairHintDeriver` / `RepairContextExpander` |
| 2 | Include | Validation issue details | Done | path, component id, rejected value, suggested fix, capability type |
| 3 | Include | Relationship candidates | Done | Exact discovered relationships included through validator suggested fix |
| 4 | Bound | Repair hint size | Done | Repair-only context; normal generation prompt remains unchanged |

## Phase 3: Deterministic Auto-Repair

| # | Action | Target | Status | Notes |
|---|--------|--------|--------|-------|
| 1 | Add | Java repair pre-pass | Done | Runs before LLM repair when validation fails |
| 2 | Fix | Unsupported relationship only | Done | Safe single-relationship replacements only; no topology redesign |
| 3 | Revalidate | `flowBuilder.prepareFlow(...)` | Done | If Java repair passes, skips LLM repair |
| 4 | Fallback | Existing LLM repair | Done | Uses LLM when Java cannot safely repair |

## Phase 4: Regression Tests

| # | Test | Status | Notes |
|---|------|--------|-------|
| 1 | `MergeContent` connection should use `merged`, not `success` | Done | Covered by Java repair pre-pass |
| 2 | Single safe output can be inferred | Done | Deterministic Java behavior |
| 3 | Multi-output ambiguity remains rejected | Done | Avoid unsafe Java guesses |
| 4 | Repair prompt includes exact supported relationships | Done | Prevents weak repair context |
| 5 | Java auto-repair skips ambiguous cases | Done | Safety guard |

## Phase 5: Verification

| # | Command | Status |
|---|---------|--------|
| 1 | `mvn compile -pl . "-Denforcer.skip=true"` | Done |
| 2 | `mvn test -pl . "-Denforcer.skip=true"` | Done, 237 tests passing |
| 3 | Manual retry of MQTT -> HTTP batching prompt | Not run locally; requires live app, authenticated LLM, and target NiFi |

## Risk Controls

- Do not restore the deleted repair-expansion classes.
- Do not expand full capability context during normal generation.
- Do not silently rewrite ambiguous relationships.
- Do not deploy anything unless validation passes.
- Prefer deterministic Java correction only when the relationship is provably safe.

## Current Status

Phase 5 complete. Relationship repair improvement is ready for review.
