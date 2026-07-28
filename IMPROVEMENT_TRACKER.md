# Refactor Improvement Tracker

**Created:** 2026-07-27  
**Status:** Complete  
**Scope:** Targeted correctness, resilience, performance, and regression-test improvements

## Objective

Address the actionable findings from the refactor review without changing unrelated behavior or expanding the architecture:

- Route direct-mode self-loops outside their owning components.
- Handle malformed LLM connection entries without unhandled exceptions.
- Keep incremental routing warnings aligned with returned routes.
- Reduce repeated obstacle-processing work in direct routing.
- Cover intentional repair and parameter-context behavior with regression tests.

## Constraints

- Preserve existing public APIs and NiFi DTO contracts.
- Introduce no new mutable shared state.
- Reuse existing routing and validation abstractions.
- Do not silently repair ambiguous relationships.
- Keep optional behavior changes separate from required fixes.
- Keep each commit independently buildable and testable.

## Phase 1: DirectRouter Self-Loop Correctness

| # | Action | Target | Status | Acceptance |
|---|--------|--------|--------|------------|
| 1 | Detect self-loop edges before direct anchor calculation | `nifi-layout-engine\src\main\java\in\shrake\nifi\layout\core\router\DirectRouter.java` | Done | Non-loop direct routing remains unchanged |
| 2 | Delegate self-loop geometry to the existing orthogonal routing behavior | `DirectRouter` / `OrthogonalRouter` | Done | Self-loop paths do not cross the owning component interior |
| 3 | Add direct-mode self-loop regression coverage | `nifi-layout-engine\src\test\java\in\shrake\nifi\layout\core\routing\ObstacleAwareRoutingTest.java` | Done | Path contains exterior bends and more than two points |

**Risk:** Low. The behavior changes only for `edge.isSelfLoop()`.

## Phase 2: Malformed LLM Connection Handling

| # | Action | Target | Status | Acceptance |
|---|--------|--------|--------|------------|
| 1 | Prevent null connection values from reaching `Map.of(...)` | `nifi-copilot\src\main\java\org\apache\nifi\copilot\llm\LlmClient.java` | Done | Null entries cannot cause `NullPointerException` |
| 2 | Discard invalid null entries with bounded diagnostic logging | `LlmClient.normalizeGeneratedLayout(...)` | Done | Malformed input does not produce an internal server error |
| 3 | Preserve supported handling for valid and non-null entries | `LlmClient.normalizeGeneratedLayout(...)` | Done | Valid connections remain available for validation |
| 4 | Add null, scalar, and mixed-entry tests | `nifi-copilot\src\test\java\org\apache\nifi\copilot\llm\LlmClientTest.java` | Done | Mixed malformed input is processed deterministically |

**Risk:** Low. Only malformed generated connection entries change behavior.

## Phase 3: Incremental Routing Warning Consistency

| # | Action | Target | Status | Acceptance |
|---|--------|--------|--------|------------|
| 1 | Filter incremental warnings using retained edge IDs | `nifi-layout-engine\src\main\java\in\shrake\nifi\layout\core\service\stage\RoutingStage.java` | Done | Every warning references an edge in the returned route map |
| 2 | Apply the same invariant when restoring incremental results | `nifi-layout-engine\src\main\java\in\shrake\nifi\layout\core\LayoutEngine.java` | Done | Unaffected-edge warnings are not returned |
| 3 | Add full-mode and incremental-mode warning tests | `nifi-layout-engine\src\test\java\in\shrake\nifi\layout\core\service\stage\RoutingStageTest.java` | Done | Full mode retains all warnings; incremental mode retains only affected warnings |
| 4 | Update affected routing assertions | `ObstacleAwareRoutingTest.java` | Done | Existing incremental behavior remains covered |

**Risk:** Low. Incremental results become internally consistent; full routing is unchanged.

## Phase 4: Direct Routing Performance

| # | Action | Target | Status | Acceptance |
|---|--------|--------|--------|------------|
| 1 | Precompute component obstacle entries once per routing call | `DirectRouter.java` | Done | Graph-node lookups and base collection construction are not repeated per edge |
| 2 | Preserve source and target exclusion semantics | `DirectRouter.java` | Done | Route geometry and warnings remain unchanged |
| 3 | Compare representative large-flow routing measurements | Existing layout performance tests or benchmark harness | Deferred | No benchmark harness exists; do not introduce one for this targeted change |
| 4 | Evaluate spatial indexing only if measurements justify it | Design follow-up | Deferred | No complexity added without measured benefit |

**Note:** Precomputation reduces repeated lookups and allocations. Intersection testing can remain O(E x N) until a measured need justifies spatial indexing.

**Risk:** Low for precomputation; medium for any future spatial-index change.

## Phase 5: Missing Regression Tests

| # | Test | Target | Status | Acceptance |
|---|------|--------|--------|------------|
| 1 | Multiple unsupported relationships skip deterministic Java repair | `nifi-copilot\src\test\java\org\apache\nifi\copilot\api\CopilotControllerCapabilityTest.java` | Done | LLM repair is used and successful repaired output is returned |
| 2 | Multiple issues do not trigger an intermediate mutable-copy repair attempt | `CopilotControllerCapabilityTest.java` | Done | Existing single-issue safety boundary is protected |
| 3 | Base parameter-context name and `(2)` collision select `(3)` | `nifi-copilot\src\test\java\org\apache\nifi\copilot\builder\ParameterContextDeployerTest.java` | Done | `(3)` is created and bound without modifying existing contexts |

**Risk:** None. This phase adds test coverage only.

## Quality Gates

| Area | Required outcome |
|------|------------------|
| Architecture | No new inter-module dependency or public abstraction |
| Performance | Repeated obstacle collection construction is removed; spatial indexing remains evidence-driven |
| Concurrency | Changed classes remain stateless and use only method-local collections |
| Memory | No unbounded cache or retained per-request state; temporary routing allocations do not increase |
| Readability | Self-loop and warning-filter behavior is explicit and follows existing patterns |
| Maintainability | Shared routing behavior is reused instead of duplicated |
| Testability | Every correctness fix has focused automated regression coverage |
| Thread safety | No mutable static state or unsafe publication is introduced |
| Error handling | Malformed LLM connections cannot escape as an unhandled NPE |
| Security | Raw or unbounded LLM content is not added to logs or responses |
| NiFi compatibility | Existing scheduling fields, DTO mappings, and deployment contracts remain unchanged |

## Post-Review: Broad-Flow Repair Reliability

| # | Improvement | Status | Acceptance |
|---|-------------|--------|------------|
| 1 | Resolve wrong-package processor FQNs by unique simple name | Done | Ambiguous simple names remain rejected |
| 2 | Apply independent safe relationship repairs across multiple issues | Done | Every repaired specification is revalidated |
| 3 | Use issue-focused capability context for repair | Done | Mixed or unresolved issue sets fall back to the full capability context |
| 4 | Include exact internal property names and compatible service implementations in hints | Done | Repair does not need to guess target-specific names |
| 5 | Retry LLM repair while issue count decreases | Done | Maximum two attempts; repeated or non-improving states stop immediately |
| 6 | Add focused regression coverage for repair progress and safety | Done | No NiFi changes occur before complete validation |

## Optional Design Decisions

These items are not part of the required fixes:

| Topic | Current behavior | Decision needed | Status |
|-------|------------------|-----------------|--------|
| Parameter-context naming | Incompatible contexts cause creation of a suffixed context | Confirm this as the permanent contract for name-based consumers | Deferred |
| Processor deployment diagnostics | Deployment fails on the first processor error | Decide whether aggregated diagnostics justify attempting later deployments | Deferred |

## Recommended Commit Sequence

| # | Commit scope | Status |
|---|--------------|--------|
| 1 | `fix(layout): handle direct-router self-loops` | Ready |
| 2 | `fix(copilot): handle malformed generated connections` | Ready |
| 3 | `fix(layout): filter incremental routing warnings` | Ready |
| 4 | `perf(layout): reduce obstacle reconstruction` | Ready |
| 5 | `test: cover repair fallback and context suffix collisions` | Ready |

## Verification

| # | Command | Status |
|---|---------|--------|
| 1 | `mvn test -pl nifi-layout-engine "-Denforcer.skip=true"` | Passed |
| 2 | `mvn test -pl nifi-copilot "-Denforcer.skip=true"` | Passed |
| 3 | Run representative large-flow routing measurement | Not run |
| 4 | Confirm all quality gates and acceptance criteria | Passed |

## Current Status

All required phases are complete and verified. Formal performance benchmarking and spatial indexing remain deferred pending an established benchmark harness and measured need.
