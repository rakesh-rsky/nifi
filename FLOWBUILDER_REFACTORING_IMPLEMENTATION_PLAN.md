# FlowBuilder Refactoring Implementation Plan

# Project Summary

## Current Architecture

`FlowBuilder` is a stateless Spring component that translates a map-based flow specification into NiFi resources through `NiFiClientOperations`.

```text
CopilotController
    |
    +-- FlowBuilder.buildFlow()
    |       |
    |       +-- Layout and collision avoidance
    |       +-- ParameterContextManager
    |       +-- ControllerServiceManager
    |       +-- NiFiClientOperations
    |       +-- Snapshot-based rollback
    |
    +-- FlowBuilder.readCanvas()
            |
            +-- NiFiClientOperations
```

The supported deployment order is:

1. Resolve the target process group.
2. Capture rollback state.
3. Apply layout and collision avoidance.
4. Optionally create a child process group.
5. Create and bind a parameter context.
6. Create and enable controller services.
7. Create processors.
8. Create connections.
9. Configure auto-terminated relationships.
10. Optionally validate and start processors.
11. Restore the snapshot after a fatal failure.

## Current Implementation Status

`buildFlow()` currently performs most orchestration and resource-specific work in one method. The class also contains:

- Processor type alias resolution
- Linear layout and collision avoidance
- Canvas projection
- Processor startup ordering
- Controller-service creation and reference resolution
- Parameter-context creation and binding
- Rollback bookkeeping

The architecture review and Phases 1-12 are complete.

## Target State

`FlowBuilder` remains the public orchestrator, while private methods own individual deployment stages. The public contracts of `buildFlow()` and `readCanvas()` remain unchanged during structural refactoring.

The target `buildFlow()` should contain only:

- Input guard clauses
- Deployment-state initialization
- Ordered stage invocation
- Result construction
- Top-level failure handling and rollback delegation

Detailed extraction guidance is maintained in `FLOWBUILDER_REFACTORING_PLAN.md`.

## Working Rules

- Focus only on `FlowBuilder` and its direct dependencies.
- Keep `FlowBuilder` as the orchestrator.
- Preserve public behavior during structural extraction.
- Do not mix method extraction with intentional behavior changes.
- Complete each phase as an independently reviewable commit.
- Preserve resource creation and startup sequencing unless a phase explicitly corrects it.
- Preserve the original deployment exception when rollback also fails.
- Do not add Phase 4 canvas resources until structural and correctness work is complete.
- Tests remain deferred until explicitly requested.

## Prohibited Changes

- Do not rename public APIs.
- Do not modify `CopilotController`.
- Do not change `NiFiClientOperations`.
- Do not change public DTOs.
- Do not change tests unless required.

## Phase Completion Gate

Before marking any phase complete, verify:

- [x] No duplicate methods
- [x] No unused imports
- [x] No unreachable code
- [x] No compilation errors
- [x] No TODO placeholders
- [x] No commented-out code
- [x] Formatting is clean

## Standard Implementation Prompt

For every phase:

1. Read only the files required for the current phase.
2. Do not inspect unrelated packages.
3. Implement only the current phase.
4. Compile mentally.
5. Fix compilation errors.
6. Update this tracker.
7. Stop.
8. Do not begin the next phase.

## Status Values

| Status | Meaning |
|---|---|
| ✅ Complete | Implemented and production compilation completed |
| 🟡 In Progress | Currently being implemented |
| ❌ Not Started | Approved but not started |
| ⏸ Deferred | Intentionally postponed |

## Size Scale

| Size | Expected scope |
|---|---|
| Small | 1-3 focused methods, generally under 50 changed lines |
| Medium | One complete deployment stage, generally 40-100 changed lines |
| Large | Dependency, ownership, or contract changes, generally over 100 changed lines |

# Refactoring Roadmap

## 1. Pure Resolution Helpers

| Work Item | Extracted Method | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Resolve processor aliases and types | `resolveProcessorType(...)` | ✅ Complete | High | Small | None | `FlowBuilder.java` | Existing alias and FQN behavior preserved |
| Resolve processor default names | `resolveProcessorName(...)` | ✅ Complete | High | Small | Processor type resolution | `FlowBuilder.java` | Existing names and defaults preserved |
| Resolve component references | `resolveComponentId(...)` | ✅ Complete | High | Small | None | `FlowBuilder.java` | Existing ID fallback preserved |
| Normalize connection relationships | `resolveRelationships(...)` | ✅ Complete | High | Small | None | `FlowBuilder.java` | Empty relationships still default to `success` |
| Calculate used relationships | `findUsedRelationships(...)` | ✅ Complete | High | Small | Component and relationship resolution | `FlowBuilder.java` | Auto-termination input remains unchanged |

## 2. Deployment Preparation

| Work Item | Extracted Method | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Resolve target process group | `resolveTargetProcessGroup(...)` | ✅ Complete | High | Small | None | `FlowBuilder.java` | Existing root and explicit group behavior preserved |
| Capture rollback snapshot | `captureSnapshot(...)` | ✅ Complete | Critical | Small | Target resolution | `FlowBuilder.java` | Current rollback flag and warning behavior preserved |
| Create optional child group | `createChildProcessGroupIfPresent(...)` | ✅ Complete | High | Small | Snapshot capture | `FlowBuilder.java` | Effective group ID remains unchanged |

## 3. Dependency Resource Deployment

| Work Item | Extracted Method | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Deploy and bind parameter context | `deployParameterContext(...)` | ✅ Complete | Critical | Small | Effective group resolution | `FlowBuilder.java` | Binding remains before processor creation |
| Deploy controller services | `deployControllerServices(...)` | ✅ Complete | Critical | Small | Parameter-context stage | `FlowBuilder.java` | Existing manager behavior and ordering preserved |

## 4. Processor Creation

| Work Item | Extracted Method | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Create processors and map IDs | `createProcessors(...)` | ✅ Complete | Critical | Medium | Groups 1-3 | `FlowBuilder.java` | Type, name, position, configuration, ID mapping, and partial-failure behavior preserved |

## 5. Connections and Relationships

| Work Item | Extracted Method | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Create resolved connections | `createConnections(...)` | ✅ Complete | Critical | Medium | Processor creation | `FlowBuilder.java` | Connections remain after all processor creation attempts |
| Configure unused relationships | `configureAutoTerminatedRelationships(...)` | ✅ Complete | High | Small | Connection creation | `FlowBuilder.java` | Existing exception behavior preserved |

## 6. Processor Startup

| Work Item | Extracted Method | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Guard optional startup | `startProcessorsIfRequested(...)` | ✅ Complete | High | Small | Relationship configuration | `FlowBuilder.java` | `autoStart=false` remains a no-op |
| Validate and start processors | `startProcessors(...)` | ✅ Complete | High | Small | Startup guard | `FlowBuilder.java` | Existing declaration-order startup preserved initially |

## 7. Rollback and Orchestrator Finalization

| Work Item | Extracted Method | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Register owned resources and restore | `rollbackDeployment(...)` | ✅ Complete | Critical | Medium | Groups 2-6 | `FlowBuilder.java` | Restore conditions and suppressed exceptions preserved |
| Reduce `buildFlow()` to orchestration | Existing `buildFlow(...)` | ✅ Complete | Critical | Medium | All structural extractions | `FlowBuilder.java` | Public contract and stage ordering unchanged |
| Remove unused manager rollback methods | Remove `teardownAll(...)` and `teardown(...)` | ✅ Complete | Medium | Rollback extraction | `FlowBuilder.java` | Snapshot restoration remains the only rollback path |
| Add contextual diagnostics | Existing catches and skip paths | ✅ Complete | High | Small | Structural extraction | `FlowBuilder.java` | Control flow remains unchanged |

## 8. Rollback Correctness

| Work Item | Target Method or Area | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Fail closed when snapshot capture fails | `captureSnapshot(...)` | ✅ Complete | Critical | Small | Structural phases complete | `FlowBuilder.java` | No mutation occurs without requested rollback protection |
| Prevent child-group double deletion | `rollbackDeployment(...)` | ✅ Complete | High | Medium | Rollback extraction | `FlowBuilder.java`; direct rollback dependency only if required | Child-contained resources are not deleted twice |
| Track deployment-owned resources | Deployment state and rollback | ✅ Complete | Critical | Large | Result/failure tracking | `FlowBuilder.java`; direct rollback dependency only if required | Concurrent resources are never removed |
| Report rollback completeness | Failure handling | ✅ Complete | High | Medium | Ownership tracking | `FlowBuilder.java` | Original and cleanup failures remain distinguishable |

## 9. Controller-Service Dependencies

| Work Item | Target Method or Area | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Discover service references | `ControllerServiceManager` | ✅ Complete | High | Medium | Structural phases complete | `FlowBuilder.java` | Service-to-service references identified |
| Detect dependency cycles | `ControllerServiceManager` | ✅ Complete | High | Medium | Reference discovery | `FlowBuilder.java` | Cycles fail before service creation |
| Resolve service IDs in properties | `ControllerServiceManager` | ✅ Complete | Critical | Medium | Ordered service creation | `FlowBuilder.java` | No unresolved specification IDs are submitted |
| Enable services in dependency order | `ControllerServiceManager` | ✅ Complete | Critical | Large | Reference resolution | `FlowBuilder.java` | Dependencies are enabled first |
| Surface service failures | Deployment result and logging | ✅ Complete | Critical | Medium | Service deployment correction | `FlowBuilder.java`; direct result dependency only if required | Deployment cannot report unconditional success |

## 10. Processor and Connection Ordering

| Work Item | Target Method or Area | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Build processor dependency graph | Replace current `startupOrder(...)` behavior | ✅ Complete | High | Medium | Structural phases complete | `FlowBuilder.java` | All resolved processor connections represented |
| Detect startup cycles | Startup graph | ✅ Complete | High | Medium | Dependency graph | `FlowBuilder.java` | Cycles handled deterministically |
| Start in reverse-topological order | `startProcessors(...)` | ✅ Complete | High | Medium | Graph and cycle handling | `FlowBuilder.java` | Downstream processors start before upstream processors |
| Surface unresolved connection endpoints | `createConnections(...)` | ✅ Complete | High | Small | Connection extraction | `FlowBuilder.java` | Skips are visible in results or logs |
| Support endpoint type resolution | Connection creation | ✅ Complete | Medium | Medium | Phase 4 specification support | `FlowBuilder.java`; direct specification dependency if required | Processor, port, and funnel endpoints map correctly |

## 11. Parameter Context Correctness

| Work Item | Target Method or Area | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Distinguish created and reused contexts | `ParameterContextManager` | ✅ Complete | High | Medium | Structural phases complete | `FlowBuilder.java` | Reused contexts are never deleted |
| Reuse compatible contexts | `ParameterContextManager` | ✅ Complete | Medium | Medium | Ownership distinction | `FlowBuilder.java`; direct client contract only if required | Repeated deployment avoids duplicate-name failure |
| Validate required parameter values | Parameter-context preparation | ✅ Complete | High | Small | None | `FlowBuilder.java` | Invalid input fails before mutation |
| Preserve prior binding | Rollback state | ✅ Complete | Critical | Medium | Rollback correctness | `FlowBuilder.java`; direct rollback dependency only if required | Original binding restored after failure |

## 12. Canvas Layout and Projection

| Work Item | Target Method or Area | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Rename or replace linear layout | `applyDAGLayout(...)` | ✅ Complete | Medium | Medium | Structural phases complete | `FlowBuilder.java` | Name accurately reflects behavior or graph ranks are used |
| Read effective target canvas | `applyCollisionAvoidance(...)` | ✅ Complete | High | Small | Child-group extraction | `FlowBuilder.java` | Occupied positions come from deployment group |
| Include component bounds | `CollisionAvoider` | ✅ Complete | Medium | Medium | Phase 4 component model | `FlowBuilder.java`; direct canvas contract only if required | Relevant component types cannot overlap |
| Guarantee collision-free fallback | `CollisionAvoider.claim(...)` | ✅ Complete | Medium | Small | None | `FlowBuilder.java` | Returned position is always checked |
| Avoid mutating input specification | Layout state | ✅ Complete | Medium | Medium | Layout extraction | `FlowBuilder.java` | Caller-owned maps remain unchanged |
| Make canvas projection null-safe | `readCanvas(...)` | ✅ Complete | Medium | Small | None | `FlowBuilder.java` | Missing optional fields do not throw |
| Make specification IDs stable | `readCanvas(...)` | ✅ Complete | High | Medium | Idempotency design | `FlowBuilder.java`; direct specification dependency if required | Duplicate display names do not collide |

## 13. Idempotency and Partial Failure

| Work Item | Target Method or Area | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Introduce stable deployment identity | Deployment state | ✅ Complete | Critical | Large | Stable specification IDs | `FlowBuilder.java`; direct specification dependency if required | Repeated requests identify the same resources |
| Reuse or update matching resources | Resource creation stages | ✅ Complete | High | Large | Deployment identity | `FlowBuilder.java`; direct client contract only if required | Repeated deployment does not duplicate resources |
| Track component-level failures | Deployment result | ✅ Complete | Critical | Medium | Structural phases complete | `FlowBuilder.java`; direct result/caller dependency only if required | Partial success is explicit |
| Make retries deterministic | Deployment orchestration | ✅ Complete | High | Large | Ownership and idempotency | `FlowBuilder.java` | Retry after partial failure converges safely |

## 14. Phase 4 FlowBuilder Coverage

| Work Item | Target Resource | Status | Priority | Size | Dependency | Files to Modify | Validation |
|---|---|---|---|---|---|---|---|
| Add input/output port orchestration | Ports | ✅ Complete | High | Large | Groups 1-13 | `FlowBuilder.java`; direct specification dependency if required | Creation, ID mapping, connection use, rollback |
| Add funnel orchestration | Funnels | ✅ Complete | Medium | Medium | Endpoint type resolution | `FlowBuilder.java`; direct specification dependency if required | Creation, connection use, rollback |
| Add label orchestration | Labels | ✅ Complete | Medium | Medium | Layout corrections | `FlowBuilder.java`; direct canvas contract only if required | Creation, positioning, rollback |
| Add remote process-group orchestration | Remote process groups | ✅ Complete | High | Large | Ownership and startup sequencing | `FlowBuilder.java`; direct specification dependency if required | Creation, transmission handling, rollback |
| Define supported snippet orchestration | Snippets | ✅ Complete | Medium | Large | Ownership model | `FlowBuilder.java`; direct specification dependency if required | Explicit create/move/copy/delete semantics |

# Implementation Phases

Each phase should compile independently and be reviewable without relying on uncommitted work from a later phase.

## Phase 0: Review and Planning

- Complete the architecture and correctness review.
- Document extraction order and estimated size.
- Create the implementation tracker.

**Status:** ✅ Complete

**Exit criteria:** Review findings, detailed refactoring plan, and implementation tracker are present. No Java code is changed.

## Phase 1: Pure Helper Extraction

- Extract processor type and name resolution.
- Extract component ID and relationship resolution.
- Extract used-relationship calculation.

**Status:** ✅ Complete  
**Size:** Small  
**Dependency:** Phase 0

**Exit criteria:** Pure helper behavior matches the current inline logic.

## Phase 2: Deployment Preparation Extraction

- Extract target process-group resolution.
- Extract snapshot capture.
- Extract optional child-group creation.

**Status:** ✅ Complete  
**Size:** Small  
**Dependency:** Phase 1

**Exit criteria:** The same effective process group and snapshot state reach later stages.

## Phase 3: Dependency Resource Extraction

- Extract parameter-context deployment.
- Extract controller-service deployment.

**Status:** ✅ Complete  
**Size:** Small  
**Dependency:** Phase 2

**Exit criteria:** Parameter context and controller services retain their current creation order and behavior.

## Phase 4: Processor Creation Extraction

- Extract processor creation and ID mapping.
- Reuse the pure processor resolution helpers.

**Status:** ✅ Complete  
**Size:** Medium  
**Dependency:** Phase 3

**Exit criteria:** Created processor count, configuration, IDs, positions, and warning behavior remain unchanged.

## Phase 5: Connection and Relationship Extraction

- Extract connection creation.
- Extract auto-terminated relationship configuration.

**Status:** ✅ Complete  
**Size:** Medium  
**Dependency:** Phase 4

**Exit criteria:** Connections remain after processor creation and auto-termination remains after connections.

## Phase 6: Startup Extraction

- Extract optional startup guard.
- Extract validation and start loop.

**Status:** ✅ Complete  
**Size:** Small to Medium  
**Dependency:** Phase 5

**Exit criteria:** `autoStart=false` remains unchanged and existing startup order is preserved.

## Phase 7: Rollback Extraction and Orchestrator Finalization

- Extract rollback registration and restore.
- Reduce `buildFlow()` to orchestration.
- Remove dead manager teardown methods.
- Add contextual logging without changing control flow.

**Status:** ✅ Complete  
**Size:** Medium  
**Dependency:** Phase 6

**Exit criteria:** `buildFlow()` is a short orchestrator, snapshot restore is the only active rollback path, and public behavior remains unchanged.

## Phase 8: Rollback Correctness

- Fail closed when requested snapshot capture fails.
- Correct child-group cleanup ownership.
- Track deployment-owned resources.
- Harden concurrent rollback isolation.

**Status:** ✅ Complete  
**Size:** Large  
**Dependency:** Phase 7

**Exit criteria:** Rollback removes only resources owned by the failed deployment and restores prior bindings.

## Phase 9: Dependency and Startup Correctness

- Resolve controller-service dependencies.
- Order service enablement.
- Build processor dependency graph.
- Correct processor startup order.
- Surface unresolved dependencies.

**Status:** ✅ Complete  
**Size:** Large  
**Dependency:** Phase 8

**Exit criteria:** Dependencies are resolved before use and startup is deterministic.

## Phase 10: Parameter Context, Idempotency, and Failure Reporting

- Distinguish created and reused parameter contexts.
- Add stable resource identity.
- Reuse or update matching resources.
- Report component-level partial failures.
- Make retries deterministic.

**Status:** ✅ Complete  
**Size:** Large  
**Dependency:** Phase 9

**Exit criteria:** Repeated deployments converge without duplicates and partial outcomes are explicit.

## Phase 11: Canvas Layout and Projection

- Correct effective-group collision reads.
- Make projection null-safe.
- Guarantee checked placement.
- Introduce stable specification IDs.
- Avoid mutating input maps.
- Implement or accurately rename DAG layout.

**Status:** ✅ Complete  
**Size:** Medium to Large  
**Dependency:** Phase 7; stable IDs depend on Phase 10

**Exit criteria:** Layout is deterministic, collision-safe, and compatible with repeated deployment.

## Phase 12: Phase 4 Canvas Components

- Add ports.
- Add funnels.
- Add labels.
- Add remote process groups.
- Add explicitly supported snippet operations.
- Support non-processor connection endpoints.

**Status:** ✅ Complete  
**Size:** Large  
**Dependency:** Phases 8-11

**Exit criteria:** Every orchestrated resource has creation ordering, stable ID mapping, partial-failure reporting, and rollback ownership.

# Progress Tracker

## Planning

- [x] Complete `FlowBuilder` architecture review
- [x] Create detailed extraction plan
- [x] Define independent commit sequence
- [x] Create implementation tracker

## Structural Refactoring

- [x] Extract pure resolution helpers
- [x] Extract deployment preparation
- [x] Extract parameter-context and controller-service stages
- [x] Extract processor creation
- [x] Extract connection creation
- [x] Extract auto-termination
- [x] Extract processor startup
- [x] Extract rollback
- [x] Reduce `buildFlow()` to orchestration
- [x] Remove dead rollback methods
- [x] Improve contextual logging

## Correctness

- [x] Make requested rollback fail closed
- [x] Correct child-group rollback ownership
- [x] Isolate concurrent rollback
- [x] Resolve controller-service dependencies
- [x] Correct controller-service enablement order
- [x] Correct processor startup order
- [x] Validate parameter-context ownership and binding
- [x] Report partial failures explicitly

## Reliability and Canvas

- [x] Add stable deployment identity
- [x] Make deployment idempotent
- [x] Make retries deterministic
- [x] Correct effective-group collision reads
- [x] Guarantee collision-free placement
- [x] Make canvas projection null-safe
- [x] Stop mutating input specifications
- [x] Implement or rename DAG layout

## Phase 4 Coverage

- [x] Input/output ports
- [x] Funnels
- [x] Labels
- [x] Remote process groups
- [x] Snippets
- [x] Non-processor connection endpoints

## Phase Progress

- [x] Phase 0: Review and Planning
- [x] Phase 1: Pure Helper Extraction
- [x] Phase 2: Deployment Preparation Extraction
- [x] Phase 3: Dependency Resource Extraction
- [x] Phase 4: Processor Creation Extraction
- [x] Phase 5: Connection and Relationship Extraction
- [x] Phase 6: Startup Extraction
- [x] Phase 7: Rollback Extraction and Orchestrator Finalization
- [x] Phase 8: Rollback Correctness
- [x] Phase 9: Dependency and Startup Correctness
- [x] Phase 10: Parameter Context, Idempotency, and Failure Reporting
- [x] Phase 11: Canvas Layout and Projection
- [x] Phase 12: Phase 4 Canvas Components

# Completion Record

Update this table whenever a work item or phase is completed.

| Date | Work Item/Phase | Methods or Behavior Completed | Validation | Commit/PR | Notes |
|---|---|---|---|---|---|
| 2026-07-14 | Phase 0: Review and Planning | Architecture review, extraction plan, independent commit plan, and implementation tracker | Documentation reviewed | | No Java code changed |
| 2026-07-14 | Phase 1: Pure Helper Extraction | `resolveProcessorType`, `resolveProcessorName`, `resolveComponentId`, `resolveRelationships`, and `findUsedRelationships` | Completion gate reviewed; `nifi-copilot` production compilation successful | | Existing processor and connection behavior preserved; Phase 2 not started |
| 2026-07-14 | Phase 2: Deployment Preparation Extraction | `resolveTargetProcessGroup`, `captureSnapshot`, and `createChildProcessGroupIfPresent` | Completion gate reviewed; `nifi-copilot` production compilation successful | | Existing target, snapshot, and child-group behavior preserved; Phase 3 not started |
| 2026-07-14 | Phase 3: Dependency Resource Extraction | `deployParameterContext` and `deployControllerServices` | Completion gate reviewed; `nifi-copilot` production compilation successful | | Existing parameter-context and controller-service ordering preserved; Phase 4 not started |
| 2026-07-14 | Phase 4: Processor Creation Extraction | `createProcessors` with processor type/name resolution, configuration, creation, and ID mapping | Completion gate reviewed; `nifi-copilot` production compilation successful | | Existing per-processor partial-failure behavior preserved; Phase 5 not started |
| 2026-07-14 | Phase 5: Connection and Relationship Extraction | `createConnections` and `configureAutoTerminatedRelationships` | Completion gate reviewed; `nifi-copilot` production compilation successful | | Existing connection defaults, skips, failures, and auto-termination exception behavior preserved; Phase 6 not started |
| 2026-07-14 | Phase 6: Startup Extraction | `startProcessorsIfRequested` and `startProcessors` | Completion gate reviewed; `nifi-copilot` production compilation successful | | Existing `autoStart` guard, declaration order, validity wait, and start failure behavior preserved; Phase 7 not started |
| 2026-07-14 | Phase 7: Rollback Extraction and Orchestrator Finalization | `rollbackDeployment`, orchestrator-only `buildFlow`, dead rollback removal, and contextual diagnostics | Completion gate reviewed; `nifi-copilot` production compilation successful | | Public behavior and stage ordering preserved; structural refactoring complete; Phase 8 not started |
| 2026-07-14 | Phase 8: Rollback Correctness | Fail-closed snapshot capture, explicit deployment-owned resource tracking, ordered cleanup, binding restoration, and aggregated rollback reporting | Completion gate and focused rollback review passed; `nifi-copilot` production compilation successful | | Rollback no longer snapshot-diff deletes concurrent resources; Phase 9 not started |
| 2026-07-14 | Phase 9: Dependency and Startup Correctness | Controller-service dependency discovery, cycle rejection, reference resolution, dependency-first enablement, downstream-first processor startup, and deterministic cycle fallback | Completion gate and focused ordering review passed; `nifi-copilot` production compilation successful | | Review findings for mandatory connection ID tracking and null-safe graph counters resolved; Phase 10 not started |
| 2026-07-14 | Phase 10: Parameter Context, Idempotency, and Failure Reporting | Compatible parameter-context reuse; child-group, service, processor, and connection matching; existing-resource updates with rollback restoration; aggregated component failures | Completion gate and focused idempotency review passed; `nifi-copilot` production compilation successful | | Public APIs unchanged; retries converge without duplicating matched resources; Phase 11 not started |
| 2026-07-14 | Phase 11: Canvas Layout and Projection | Deployment-local specification copying; deterministic graph-rank layout with cycle handling; effective-group bounds-based collision avoidance; null-safe canvas projection with stable NiFi IDs | Completion gate and focused canvas review passed; `nifi-copilot` production compilation successful | | Review findings for scoped reuse exclusions, stable repeated-deployment offsets, and finite coordinates resolved; Phase 12 not started |
| 2026-07-14 | Phase 12: Phase 4 Canvas Components | Ports, funnels, labels, remote process groups, create/move/copy/delete snippets, typed local endpoints, runtime sequencing, canvas projection, and ownership-aware rollback | Completion gate and focused rollback/identity/idempotency/malformed-response/snippet-safety reviews passed; `nifi-copilot` production compilation successful | | Canonical keys: `input_ports`/`output_ports` (`id`, `name`, optional position/state), `funnels` (`id`, optional position), `labels` (`id`, `text`, optional position/style/width/height), `remote_process_groups` (`id`, `target_uri`, optional position/config/transmission), and `snippets` operation maps; copy/delete and externally owned move operations require rollback to be disabled because NiFi consumes, deletes, or obscures their prior ownership; stable NiFi IDs, unique semantic reuse, safe scalar styles, explicit state validation, and local PROCESSOR/INPUT_PORT/OUTPUT_PORT/FUNNEL connections are supported; remote-port connections are intentionally unsupported; public APIs unchanged; no later phase started |
