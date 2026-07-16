# FlowBuilder Modularization Implementation Plan

## Purpose

Modularize `FlowBuilder` into cohesive, package-private collaborators while preserving all behavior completed through FlowBuilder Phases 0-12.

The target is a public `FlowBuilder` of approximately 100-200 lines that remains the stable facade for:

- `buildFlow(...)`
- `readCanvas(...)`
- `BuildResult`

This is an organizational refactoring. It must not redesign public APIs, alter deployment behavior, or replace compensating rollback with a false transactional abstraction.

## Current State

`FlowBuilder.java` is approximately 2,350 lines. `buildFlow()` is already an orchestrator, but validation, resolution, deployment, layout, projection, runtime activation, rollback, state, and metrics concerns remain in one physical class.

### Cohesive Regions

| Current Responsibility | Representative Methods or State | Target |
|---|---|---|
| Public facade | `buildFlow(...)`, `readCanvas(...)`, `BuildResult` | `FlowBuilder` |
| Specification support | copying, map/list parsing, numeric/state conversion | `SpecificationSupport` |
| Local validation | collection shape, IDs, snippets, parameter context | `LocalPreflightValidator` |
| Target preparation | target resolution, snapshot, effective group, inventory | `DeploymentPreparationStage` |
| Layout | graph ranks, collision avoidance, bounds, placement | `CanvasLayoutEngine` |
| Projection | canvas response parsing and stable projections | `CanvasProjector` |
| Identity and endpoints | inventory registration, matching, endpoint types | `ComponentResolver` and identity strategies |
| Parameter contexts | matching, creation, binding, compatibility | `ParameterContextDeployer` |
| Controller services | dependency graph, matching, creation, enablement | `ControllerServiceDeployer` |
| Processors | alias resolution, create/update, restoration state | `ProcessorDeployer` |
| Canvas components | ports, funnels, labels, remote process groups | focused resource deployers |
| Connections | endpoint resolution, matching, create/update | `ConnectionDeployer` |
| Relationships | auto-termination | `RelationshipConfigurer` |
| Snippets | validation, selection resolution, operations | `SnippetDeployer` |
| Runtime activation | ports and remote-group states | `RuntimeStateApplier` |
| Processor startup | graph ordering, validity wait, start | `ProcessorStarter` |
| Ownership and restoration | created resources and restore actions | `OwnershipLedger` |
| Rollback | ordered compensating actions and suppressed failures | `RollbackManager` |
| Internal outcomes | stage/resource observations | `DeploymentReport` |

## Architectural Invariants

### Public Compatibility

- Until the final modularization phase is complete, do not delete, rename, replace, or reduce away the main
  public `FlowBuilder` class; it must retain `@Component`, public `BuildResult`, `buildFlow(...)`, and
  `readCanvas(...)`.
- Do not rename or change `FlowBuilder`, `buildFlow(...)`, `readCanvas(...)`, or `BuildResult`.
- Do not modify `CopilotController`.
- Do not modify `NiFiClientOperations`.
- Do not modify public DTOs.
- Do not change existing response shapes.
- Do not require callers to construct new deployment objects.
- Continue supporting both embedded and HTTP `NiFiClientOperations` implementations.

### Behavioral Compatibility

- Never mutate caller-owned specification maps or lists.
- Preserve validation order, exception behavior, logging, retries, and partial-failure behavior unless a phase explicitly documents and receives approval for a behavior change.
- Preserve fail-closed snapshot capture when rollback is requested.
- Preserve resource-specific identity, compatibility, and ambiguity rules.
- Preserve controller-service dependency ordering and cycle rejection.
- Preserve graph-aware layout, checked collision placement, stable IDs, and null-safe projection.
- Preserve downstream-first processor startup and deterministic cycle fallback.
- Preserve typed local connections and snippet rollback restrictions.
- Preserve the original deployment exception as primary and attach rollback failures as suppressed.

### Deployment Order

1. Copy and locally validate the specification.
2. Resolve the requested target process group.
3. Capture the rollback snapshot when required.
4. Resolve or create the effective process group.
5. Read effective-group inventory.
6. Apply graph layout and collision avoidance.
7. Deploy and bind the parameter context.
8. Deploy and enable controller services.
9. Ensure the NiFi type cache.
10. Register live and caller-supplied component identities.
11. Deploy processors.
12. Deploy input ports.
13. Deploy output ports.
14. Deploy funnels.
15. Deploy labels.
16. Deploy remote process groups.
17. Deploy typed connections.
18. Configure auto-terminated relationships.
19. Execute snippet operations.
20. Apply requested port and remote-group runtime states.
21. Optionally validate and start processors.
22. Construct the unchanged `BuildResult`.
23. On fatal failure, execute compensating rollback.

### Prohibited Designs

- No mutable request state in singleton Spring beans.
- No Spring request-scoped `DeploymentContext`.
- No generic context property bag.
- No deployers, stages, metrics registries, or rollback actions inside `DeploymentContext`.
- No stage-to-stage calls.
- No deployer invoking `RollbackManager`.
- No universal identity matcher that weakens resource-specific rules.
- No public contributor extension API.
- No component IDs, names, URIs, exception messages, or specification IDs in metric dimensions.
- No abstraction that claims NiFi deployment is an atomic transaction.

## Target Structure

All implementation collaborators remain package-private under `org.apache.nifi.copilot.builder` unless a documented Spring integration constraint requires otherwise.

```text
FlowBuilder
├── FlowDeploymentCoordinator
│   ├── DeploymentPreparationStage
│   ├── DependencyDeploymentStage
│   ├── ComponentDeploymentStage
│   ├── ConnectionConfigurationStage
│   └── RuntimeActivationStage
├── LocalPreflightValidator
├── LivePreflightValidator
├── ComponentResolver
│   └── resource-specific identity strategies
├── CanvasLayoutEngine
├── CanvasProjector
├── RollbackManager
├── OwnershipLedger
├── DeploymentContext
├── DeploymentState
├── DeploymentReport
└── FlowDeploymentMetricsRegistry
```

### Dependency Direction

```text
FlowBuilder
    -> coordinator / projector
        -> stages
            -> resource deployers
                -> resolver / layout / support
                    -> NiFiClientOperations

RollbackManager -> OwnershipLedger + NiFiClientOperations
Metrics -> observations only
```

Stages never call other stages. The coordinator alone controls ordering.

## State Boundaries

### DeploymentContext

An immutable, request-local object containing only:

- copied specification
- `NiFiClientOperations`
- requested target
- copied existing-ID map
- existing processor count
- auto-start flag
- rollback flag

It must not become a service locator or mutable state container.

### DeploymentTarget

Contains preparation results:

- parent process-group ID
- effective process-group ID
- previous parameter-context binding
- rollback snapshot
- effective-group inventory

### DeploymentState

Coordinator-owned typed references:

- `DeploymentTarget`
- `ComponentRegistry`
- processor deployment result
- connection deployment result
- `DeploymentReport`

Do not pass `DeploymentState` wholesale into every deployer.

### OwnershipLedger

Records only ownership and restoration information:

- owned child process group
- parameter-context ownership and previous binding
- created controller services
- created processors and processor restores
- created connections and connection restores
- canvas deletion/restoration actions
- snippet move-back actions
- port runtime restores
- remote-group transmission restores

Insert each ledger entry immediately after its mutation succeeds and before the next NiFi call.

### DeploymentReport

Records observations, timings, counts, and outcomes. It must never control rollback, identity, or sequencing.

### BuildResult

Remains unchanged:

- created processor entities
- newly created connection count

No internal state, rollback details, metrics, or warnings are added.

## Identity Boundaries

Use resource-specific identity strategies behind `ComponentResolver`.

| Resource | Preserved Identity Rule |
|---|---|
| Child process group | unique name scoped to parent |
| Parameter context | unique global name plus compatibility |
| Controller service | unique name and exact type |
| Processor | caller mapping or stable NiFi ID |
| Input/output port | exact ID, otherwise unique name within its collection |
| Funnel | exact stable NiFi ID |
| Label | exact ID, otherwise current unambiguous text behavior |
| Remote process group | exact ID, otherwise unique target URI |
| Connection | source ID/type and destination ID/type tuple |
| Snippet | dedicated snippet registry only |

`DeploymentIdentityService` should not be introduced as a single universal matcher. Shared mechanics belong in `ComponentResolver`; identity decisions remain resource-specific.

## Exact Extraction Order

1. Extract specification copying and safe collection conversion.
2. Extract NiFi response-envelope and entity access helpers.
3. Extract existing local preflight validation.
4. Introduce immutable `DeploymentContext`.
5. Extract canvas projection.
6. Extract bounds, graph ranking, collision avoidance, and placement.
7. Extract `ComponentRegistry`.
8. Extract resource-specific identity strategies.
9. Replace generic matching helpers with `ComponentResolver`.
10. Extract non-mutating live preflight checks without changing call timing.
11. Introduce typed ownership ledger entries while retaining existing rollback loops.
12. Extract `RollbackManager` with unchanged ordering.
13. Extract parameter-context deployment.
14. Extract controller-service dependency and deployment logic.
15. Extract processor deployment.
16. Extract input/output port deployment.
17. Extract funnel deployment.
18. Extract label deployment.
19. Extract remote-process-group deployment.
20. Extract connection deployment and endpoint resolution.
21. Extract relationship configuration.
22. Extract snippet deployment.
23. Extract deferred runtime-state application.
24. Extract processor startup.
25. Introduce coarse stages around extracted deployers.
26. Move exact orchestration into `FlowDeploymentCoordinator`.
27. Reduce `FlowBuilder` to facade and result conversion.
28. Evaluate contributors only after common behavior is demonstrated.
29. Add deployment metrics without affecting decisions.

# Implementation Phases

## Phase 0: Baseline and Guardrails

**Scope**

- Record the current `FlowBuilder` method and nested-type inventory.
- Record exact deployment and rollback ordering.
- Confirm the production compile command and existing test commands.
- Identify current dirty-worktree files without changing them.

**Files**

- Modify this tracker only.
- No Java changes.

**Size:** Small  
**Risk:** Low  
**Status:** ✅ Complete

**Exit criteria**

- Baseline ordering and compatibility checklist are confirmed.
- No source files are modified.

### Phase 0 Baseline Record

Captured on 2026-07-15 before modularization source changes.

| Baseline Item | Value |
|---|---|
| Repository commit | `452317dab6` |
| `FlowBuilder.java` size | 2,355 lines |
| Declared methods | 94 |
| Nested implementation types | 15 |
| Public surface | `BuildResult`, `buildFlow(...)`, `readCanvas(...)` |
| Worktree status entries | 61 pre-existing entries |
| Staged files | 51 |
| Tracked files with unstaged changes | 8 |
| `FlowBuilder.java` status | `AM` before modularization |
| Modularization tracker status | untracked planning document |
| Java files changed by Phase 0 | none |

The worktree is intentionally dirty from the completed NiFi API and FlowBuilder Phase 0-12 work. Modularization phases must not revert, overwrite, or attribute those pre-existing changes to themselves.

### Method Inventory by Cohesive Family

| Family | Current Methods |
|---|---|
| Public facade | `buildFlow`, `readCanvas` |
| Layout | `applyDAGLayout`, `processorRanks`, `addProcessorEdges`, `rankProcessors`, `applyCollisionAvoidance`, `readOccupiedBounds`, `addComponentBounds`, `claimPosition`, `positiveNumericValue` |
| Projection | `projectPorts`, `projectFunnels`, `projectLabels`, `projectRemoteProcessGroups`, `stableProjection` |
| Startup | `startupOrder`, `startProcessorsIfRequested`, `startProcessors` |
| Rollback | `rollbackDeployment`, `runRollbackStep` |
| Target preparation | `resolveTargetProcessGroup`, `captureSnapshot`, `resolveEffectiveProcessGroup`, `findUniqueChildProcessGroup` |
| Dependency wrappers | `deployParameterContext`, `deployControllerServices` |
| Canvas deployment | `deployCanvasComponents`, `deployPorts`, `deployFunnels`, `deployLabels`, `deployRemoteProcessGroups` |
| Processor deployment | `createProcessors`, `updateExistingProcessor`, `resolveProcessorType`, `resolveProcessorName` |
| Connection deployment | `createConnections`, `matchExistingConnection`, `updateExistingConnection`, `requireCreatedComponentId`, `resolveComponentId`, `resolveEndpointType`, `typeMatches`, `resolveRelationships`, `findUsedRelationships`, `configureAutoTerminatedRelationships` |
| Snippets and runtime | `executeSnippetOperations`, `resolveSnippetSelections`, `resolveSnippetId`, `resolveProcessGroupReference`, `applyRequestedRuntimeStates` |
| Specification support | `copySpecification`, `copyMap`, `copyValue`, `mapOrEmpty`, `mapOrNull`, `listOfMap`, `toStringList`, `hasDeployableWork` |
| Local validation | `validateComponentSpecIds`, `validateUniqueId`, `validateSnippetOperations`, `validateSpecificationCollections`, `normalizedOperation`, `requireNonBlank`, `optionalState`, `nullableNumber`, `optionalPositiveNumber`, `finiteRequiredNumber`, `scalarStringMap`, `optionalMapField` |
| Identity and inventory | `reusedProcessorIds`, `registerExistingProcessors`, `registerInventoryIds`, `registerProcessorTypes`, `reusedProcessorSpecIds`, `findByEntityId`, `matchByIdOrUniqueValue`, `entityName` |
| Entity and mutation support | `requireEntityId`, `entityId`, `stringOrNull`, `componentValueOrDefault`, `valueOrDefault`, `componentMap`, `numericValue`, `effectiveFlow`, `requestedPosition`, `restoreFields`, `originalUpdatedFields`, `componentState`, `transmissionState`, `contextFailure`, `throwAggregated` |

Nested manager methods remain inside `ControllerServiceManager` and `ParameterContextManager`; they are counted through the nested-type inventory rather than the declared top-level method count.

### Nested-Type Inventory

| Kind | Types |
|---|---|
| Deployment and restoration records | `EffectiveProcessGroup`, `ProcessorDeployment`, `ProcessorRestore`, `ConnectionRestore`, `RollbackAction`, `RuntimeRequest`, `RuntimeRestore`, `TransmissionRequest`, `TransmissionRestore` |
| Layout primitive | `Bounds` |
| Mutable deployment state | `ComponentRegistry`, `DeploymentResources` |
| Stateful helpers | `CollisionAvoider`, `ControllerServiceManager`, `ParameterContextManager` |

### Confirmed Deployment Sequence

The current `buildFlow(...)` sequence matches the 23-step deployment-order invariant in this document. The most sensitive boundaries are:

- snapshot capture occurs before effective-group mutation
- effective-group inventory is read before layout and deployment
- dependency resources precede processors and canvas resources
- processors precede ports, funnels, labels, and remote groups
- connections precede auto-termination and snippets
- snippets precede requested runtime states
- requested runtime states precede optional processor startup
- rollback remains inside the public facade exception boundary

### Confirmed Rollback Sequence

The current `rollbackDeployment(...)` sequence is:

1. Stop requested input/output ports.
2. Stop requested remote-process-group transmission.
3. Delete created connections in reverse order.
4. Reverse snippet actions.
5. Reverse canvas delete/restore actions.
6. Restore reused port states in reverse order.
7. Restore reused remote-group transmission states in reverse order.
8. Delete created processors in reverse order.
9. Restore updated processors in reverse order.
10. Restore updated connections in reverse order.
11. Delete created controller services in reverse order.
12. Restore or remove parameter-context binding.
13. Delete only the owned parameter context.
14. Delete only the owned child process group.
15. Attach aggregate rollback failures as suppressed on the original deployment exception.

### Confirmed Commands

Production compile:

```powershell
.\mvnw.cmd -pl 'nifi-copilot' -DskipTests compile --no-transfer-progress
```

Module tests when authorized:

```powershell
.\mvnw.cmd -pl 'nifi-copilot' test --no-transfer-progress
```

The `nifi-copilot` POM uses standard Maven test lifecycle behavior and defines no custom test command. Tests remain deferred under the current project direction; Phase 0 does not alter tests.

---

## Phase 1: Internal Support and Local Preflight

**Create**

- `SpecificationSupport.java`
- `NiFiEntitySupport.java`
- `LocalPreflightValidator.java`
- `DeploymentContext.java`

**Modify**

- `FlowBuilder.java`

**Move**

- recursive specification copying
- map/list/string/number/state conversion
- entity ID and response-envelope readers
- collection-shape validation
- global component-ID validation
- parameter-context validation
- snippet safety validation

**Behavioral intent:** Pure relocation with identical defaults, validation order, and exception behavior.  
**Size:** Medium, approximately 250-400 moved lines  
**Risk:** Medium  
**Status:** ✅ Complete

**Exit criteria**

- `FlowBuilder` delegates all current local validation.
- Caller input remains immutable.
- No duplicate conversion or validation helpers remain.

---

## Phase 2: Canvas Layout and Projection

**Create**

- `CanvasBounds.java`
- `CollisionAvoider.java`
- `CanvasLayoutEngine.java`
- `CanvasProjector.java`

**Modify**

- `FlowBuilder.java`

**Move**

- graph ranking and cycle handling
- deterministic placement
- effective-group bounds extraction
- collision search
- processor and canvas projection
- stable projection IDs and null defaults

**Behavioral intent:** Separate write-time layout from read-only projection without changing coordinates or response keys.  
**Size:** Medium, approximately 300-450 moved lines  
**Risk:** Medium  
**Status:** ✅ Complete

**Exit criteria**

- `FlowBuilder` contains no layout algorithm, collision state, or projection loops.
- `CanvasProjector` has no dependency on deployment state.

---

## Phase 3: Component Resolution and Live Preflight

**Create**

- `ComponentRegistry.java`
- `ComponentResolver.java`
- `DeploymentTarget.java`
- `LivePreflightValidator.java`
- resource-specific identity strategy classes where behavior is substantial

**Modify**

- `FlowBuilder.java`

**Move**

- inventory registration
- endpoint type resolution
- process-group matching
- parameter-context compatibility matching
- controller-service matching
- processor ID resolution
- port, funnel, label, and remote-group matching
- connection tuple matching

**Behavioral intent:** Preserve every existing identity and ambiguity rule while removing the false appearance of one universal matcher.  
**Size:** Large, approximately 400-650 moved or changed lines  
**Risk:** High  
**Status:** ✅ Complete

**Exit criteria**

- Generic matching helpers are removed.
- A parity matrix confirms identity, reuse, incompatibility, and ambiguity behavior for each resource.
- Live preflight remains non-mutating and does not change established failure timing.

---

## Phase 4: Ownership Ledger and Rollback Manager

**Create**

- `OwnershipLedger.java`
- `RollbackManager.java`
- package-private typed ownership/restoration records

**Modify**

- `FlowBuilder.java`
- extracted deployers only where ownership is registered

**Migration**

1. Mirror existing ownership lists as typed ledger entries.
2. Keep rollback loops in `FlowBuilder` temporarily.
3. Compare old and new ledger action order.
4. Move unchanged rollback execution into `RollbackManager`.
5. Remove `DeploymentResources`.

**Behavioral intent:** Exact rollback relocation only.  
**Size:** Large, approximately 350-550 changed lines  
**Risk:** Critical  
**Status:** ✅ Complete

**Completion record (2026-07-15)**

| Item | Result |
|---|---|
| `OwnershipLedger.java` created | yes — 8 typed nested records, 12 typed mutator methods, 10 unmodifiable-view accessors |
| `RollbackManager.java` created | yes — static `rollback(...)` + `runStep(...)` |
| `DeploymentResources` removed from `FlowBuilder` | yes |
| `rollbackDeployment` removed from `FlowBuilder` | yes |
| `runRollbackStep` removed from `FlowBuilder` | yes |
| Private records removed from `FlowBuilder` | yes — ProcessorRestore, ConnectionRestore, RollbackAction, RuntimeRequest, RuntimeRestore, TransmissionRequest, TransmissionRestore |
| `ControllerServiceManager.created` removed | yes — ownership registered via `ledger.addCreatedControllerServiceId(...)` |
| `ParameterContextManager` rollback fields removed | yes — fields pcId, created, boundProcessGroupId, previousBindingId removed; `deploy` made static with ledger parameter |
| All registration sites migrated | yes — every `new Xxx(...)` add call replaced with typed ledger mutator |
| Rollback order preserved | yes — identical 14-step order in `RollbackManager.rollback(...)` |
| Compile | `BUILD SUCCESS` |
| Stale reference check | no matches for DeploymentResources, rollbackDeployment, runRollbackStep |

**Required rollback order**

1. Stop requested port states.
2. Stop requested remote-group transmission.
3. Delete created connections in reverse order.
4. Reverse snippet actions.
5. Reverse canvas mutations.
6. Restore port states.
7. Restore remote-group transmission states.
8. Delete created processors.
9. Restore updated processors.
10. Restore updated connections.
11. Delete created controller services.
12. Restore parameter-context binding and delete only owned contexts.
13. Delete only the owned child process group.
14. Attach aggregate rollback failures to the original exception.

**Exit criteria**

- `DeploymentResources` and `rollbackDeployment(...)` are removed from `FlowBuilder`.
- Rollback action order and ownership are unchanged.

---

## Phase 5: Dependency and Processor Deployers

**Create**

- `ParameterContextDeployer.java`
- `ControllerServiceDeployer.java`
- `ProcessorDeployer.java`
- `RelationshipConfigurer.java`
- `ProcessorStarter.java`

**Modify**

- `FlowBuilder.java`
- `OwnershipLedger.java`

**Extraction order**

1. Parameter-context compatibility, creation, and binding.
2. Controller-service dependency graph, matching, creation, and enablement.
3. Controller-service property reference resolution.
4. Processor type/name resolution, create/update, and results.
5. Relationship auto-termination.
6. Startup graph, validity wait, and processor start.

**Behavioral intent:** Preserve dependency ordering, reuse, restoration, startup order, and public results.  
**Size:** Large, approximately 450-700 moved lines  
**Risk:** High  
**Status:** ✅ Complete

**Completion record (2026-07-15)**

| Item | Result |
|---|---|
| `ParameterContextDeployer.java` created | yes — static `deploy(...)` with null guard; registers before bind |
| `ControllerServiceDeployer.java` created | yes — request-local instance; `deployAll`, `resolveCsReferences`, `dependencyOrder`, `visitService`, `serviceDependencies` |
| `ProcessorDeployer.java` created | yes — static `deploy(...)`, private `updateExistingProcessor`, nested `Result` record; owns `throwAggregated` helper |
| `RelationshipConfigurer.java` created | yes — static `configure(...)`, package-private `resolveRelationships(...)` (called by remaining `createConnections`), private `findUsedRelationships` |
| `ProcessorStarter.java` created | yes — static `startIfRequested(...)`, private `startupOrder`, `startProcessors` with 30 s validity wait |
| `OwnershipLedger.java` modified | no — existing API sufficient |
| `ControllerServiceManager` removed from `FlowBuilder` | yes |
| `ParameterContextManager` removed from `FlowBuilder` | yes |
| `ProcessorDeployment` record removed from `FlowBuilder` | yes — replaced by `ProcessorDeployer.Result` |
| `deployParameterContext` wrapper removed | yes — inlined as direct call to `ParameterContextDeployer.deploy` |
| `deployControllerServices` wrapper removed | yes — inlined list extraction + `csDeployer.deployAll` |
| `createProcessors` removed | yes |
| `updateExistingProcessor` removed | yes |
| `configureAutoTerminatedRelationships` removed | yes |
| `resolveRelationships` removed (FlowBuilder) | yes — moved to `RelationshipConfigurer`; `createConnections` updated to call `RelationshipConfigurer.resolveRelationships` |
| `findUsedRelationships` removed | yes |
| `startupOrder` removed | yes |
| `startProcessorsIfRequested` removed | yes |
| `startProcessors` removed | yes |
| Stale imports removed | yes — `ArrayDeque`, `Deque`, `HashSet`, `LinkedHashSet`, `finiteRequiredNumber`, `numericValue`, `toStringList` |
| `FlowBuilder.java` line count | 640 (from 1067 pre-Phase-5) |
| `git diff --check` | clean — exit code 0 |
| Stale-symbol check | no matches for removed types/methods in nifi-copilot |
| Compile | `BUILD SUCCESS` |

**Exit criteria**

- No parameter-context, controller-service, processor payload, relationship, or startup logic remains in `FlowBuilder`.

---

## Phase 6A: Canvas Resource Deployers

**Create**

- `PortDeployer.java`
- `FunnelDeployer.java`
- `LabelDeployer.java`
- `RemoteProcessGroupDeployer.java`

**Modify**

- `FlowBuilder.java`
- `NiFiEntitySupport.java` (added `restoreFields` and `originalUpdatedFields`)

**Behavioral intent:** Preserve resource order, identity, update payloads, runtime restoration, aggregation, and rollback registration.  
**Size:** Large, approximately 350-500 moved lines  
**Risk:** High  
**Status:** ✅ Complete

**Completion record (2026-07-15)**

| Item | Result |
|---|---|
| `PortDeployer.java` created | yes — static `deploy(...)` with input/output flag; owns `throwAggregated`/`contextFailure` helpers |
| `FunnelDeployer.java` created | yes — static `deploy(...)`; calls `NiFiEntitySupport.restoreFields` |
| `LabelDeployer.java` created | yes — static `deploy(...)`; non-null text guard; conditional style/width/height update |
| `RemoteProcessGroupDeployer.java` created | yes — static `deploy(...)`; stop-before-update; `originalUpdatedFields`; transmission request defaults |
| `restoreFields` moved to `NiFiEntitySupport` | yes — shared by Funnel, Label, RPG deployers |
| `originalUpdatedFields` moved to `NiFiEntitySupport` | yes — used by RPG deployer |
| `deployCanvasComponents` removed from `FlowBuilder` | yes |
| `deployPorts` removed from `FlowBuilder` | yes |
| `deployFunnels` removed from `FlowBuilder` | yes |
| `deployLabels` removed from `FlowBuilder` | yes |
| `deployRemoteProcessGroups` removed from `FlowBuilder` | yes |
| `restoreFields` removed from `FlowBuilder` | yes |
| `originalUpdatedFields` removed from `FlowBuilder` | yes |
| Unused imports removed from `FlowBuilder` | yes — `componentState`, `componentValueOrDefault`, `nullableNumber`, `transmissionState`, `optionalMapField`, `optionalState`, `requireNonBlank`, `scalarStringMap`, `Set` |
| Canvas deployment order preserved | yes — input ports, output ports, funnels, labels, RPGs |
| `OwnershipLedger` unchanged | yes — existing API sufficient |
| `ComponentResolver` unchanged | yes — existing API sufficient |
| `FlowBuilder.java` line count | 370 (from 640 pre-Phase-6A) |
| `git diff --check` | clean — exit code 0 |
| Stale-symbol check | no stale methods; `restoreFields`/`originalUpdatedFields` only in NiFiEntitySupport and new deployers |
| Compile | `BUILD SUCCESS` |

**Exit criteria**

- No port, funnel, label, or remote-process-group mutation logic remains in `FlowBuilder`.

---

## Phase 6B: Connections, Snippets, and Runtime

**Create**

- `ConnectionDeployer.java`
- `SnippetDeployer.java`
- `RuntimeStateApplier.java`

**Modify**

- `FlowBuilder.java`

**Behavioral intent:** Preserve typed endpoints, relationship defaults, connection reuse/update, snippet restrictions, and deferred runtime states.  
**Size:** Large, approximately 300-450 moved lines  
**Risk:** High  
**Status:** ✅ Complete

**Completion record (2026-07-15)**

| Item | Result |
|---|---|
| `ConnectionDeployer.java` created | yes — static `deploy(...)`; empty-spec guard returns 0; `updateExistingConnection` private; owns `throwAggregated` helper |
| `SnippetDeployer.java` created | yes — static `deploy(...)`; normalized operation semantics preserved; `layoutEngine` parameter for copy x/y; owns `throwAggregated`/`contextFailure` helpers |
| `RuntimeStateApplier.java` created | yes — static `apply(...)`; port requests first, then transmission; owns `throwAggregated`/`contextFailure` helpers |
| `OwnershipLedger.java` modified | no — existing API sufficient |
| `ComponentResolver.java` modified | no — existing API sufficient |
| `createConnections` removed from `FlowBuilder` | yes |
| `updateExistingConnection` removed from `FlowBuilder` | yes |
| `executeSnippetOperations` removed from `FlowBuilder` | yes |
| `applyRequestedRuntimeStates` removed from `FlowBuilder` | yes |
| `throwAggregated` removed from `FlowBuilder` | yes |
| `contextFailure` removed from `FlowBuilder` | yes |
| Unused imports removed from `FlowBuilder` | yes — `normalizedOperation`, `requireCreatedComponentId`, `mapOrEmpty`, `ArrayList`, `HashMap`, `LinkedHashMap` |
| `FlowBuilder.java` line count | 196 (from 370 pre-Phase-6B) |
| `git diff --check` | clean — exit code 0 |
| Stale-symbol check | no stale removed methods in builder package |
| Compile | `BUILD SUCCESS` — 48 source files |

**Exit criteria**

- No resource-specific create, update, snippet, or runtime-state code remains in `FlowBuilder`.
- Remote-port connections remain explicitly unsupported under the current client contract.

---

## Phase 7: Coarse Stages and Coordinator

**Create**

- `DeploymentState.java`
- `DeploymentReport.java`
- `DeploymentPreparationStage.java`
- `DependencyDeploymentStage.java`
- `ComponentDeploymentStage.java`
- `ConnectionConfigurationStage.java`
- `RuntimeActivationStage.java`
- `FlowDeploymentCoordinator.java`

**Modify**

- `FlowBuilder.java`

**Migration**

1. Wrap existing deployer calls in coarse stages.
2. Keep the established call sequence visible.
3. Move sequencing to `FlowDeploymentCoordinator`.
4. Keep the public exception boundary in `FlowBuilder`.
5. Convert internal results to the unchanged `BuildResult`.

**Behavioral intent:** Make ordering explicit without introducing a generic transaction engine.  
**Size:** Medium, approximately 250-400 changed lines  
**Risk:** High  
**Status:** ✅ Complete

**Completion record (2026-07-15)**

| Item | Result |
|---|---|
| `DeploymentState.java` created | yes — typed fields: context, prePgId, parentSnapshot, ledger, csDeployer, target, collisionAvoider, components, created/managed processor lists, connectionsCreated; no generic property bag |
| `DeploymentReport.java` created | yes — createdProcessors list and connectionsCreated int; preserves the prior public result list semantics |
| `DeploymentPreparationStage.java` created | yes — `prepare(context, resolver)` outside-try; `prepareTarget(state, ...)` inside-try; `captureSnapshot` and `resolveEffectiveTarget` private helpers |
| `DependencyDeploymentStage.java` created | yes — static `deploy(state, resolver)`; PC then CS exact order |
| `ComponentDeploymentStage.java` created | yes — static `deploy(state, resolver, layoutEngine)`; ensureTypeCache → registry → processors → types → input ports → output ports → funnels → labels → RPGs |
| `ConnectionConfigurationStage.java` created | yes — static `deploy(state, resolver, layoutEngine)`; connections → relationship configure → snippets |
| `RuntimeActivationStage.java` created | yes — static `deploy(state)`; runtime states → startup |
| `FlowDeploymentCoordinator.java` created | yes — `prepare(context)` + `deploy(state)`; stages invoked only; no resource-specific logic |
| `FlowBuilder.java` modified | yes — 65 lines; @Component retained; BuildResult unchanged; buildFlow/readCanvas signatures unchanged; local validation inline; COORDINATOR.prepare outside try; COORDINATOR.deploy inside try; same logger text/rollback/rethrow |
| Boundary parity | yes — prepare() outside try, deploy() inside try; captureSnapshot failures propagate before catch; rollback condition uses state.parentSnapshot() |
| Stage cross-reference check | clean — no stage references another stage |
| Deployer calls in FlowBuilder | none — all resource sequencing in stages/coordinator |
| Public surface check | @Component, public record BuildResult, public buildFlow, public readCanvas all present |
| `git diff --check` | clean — exit code 0 |
| Compile | `BUILD SUCCESS` |

**Exit criteria**

- `FlowBuilder` is approximately 100-200 lines. ✅ (65 lines — all responsibilities delegated)
- `buildFlow(...)` constructs immutable request context, delegates, converts the result, and preserves the exception boundary. ✅
- `readCanvas(...)` delegates to `CanvasProjector`. ✅
- Stages do not call one another. ✅

---

## Phase 8: Optional Canvas Contributor Lifecycle

This phase is conditional. Implement it only if the extracted canvas deployers demonstrate an identical lifecycle without resource-specific exceptions.

**Potential create**

- `CanvasComponentContributor.java`
- `CanvasComponentPlan.java`

**Candidate contract**

```text
kind()
preflight(specificationView, inventory, resolver) -> immutable resource plan
deploy(plan, target, registry, ledger, client, report)
```

**Rules**

- Share sequencing only.
- Keep payload construction and identity resource-specific.
- Do not force processors, connections, parameter contexts, controller services, or snippets into the interface.
- Do not add arbitrary lifecycle hooks.

**Size:** Small to Medium  
**Risk:** Medium  
**Status:** ❌ N/A — Skipped

**Rationale:** Port, funnel, label, and RPG deployers retain materially different identity rules, update lifecycles, state handling, and rollback registration (e.g., port name-incompatibility fatal check, funnel ID-only matching, label text fallback, RPG stop-before-update and transmission-restore semantics). A shared `CanvasComponentContributor` interface would be a false abstraction over structurally distinct behavior. The evaluation is complete: no contributor classes will be created.

**Exit criteria**

- The interface is either accepted with demonstrated duplication reduction or marked `N/A` with rationale. ✅ Marked N/A.

---

## Phase 9: Deployment Metrics

**Create**

- `FlowDeploymentMetricsRegistry.java`

**Modify**

- `FlowDeploymentCoordinator.java`
- stage classes
- `RollbackManager.java`
- `FlowBuilder.java` only for construction/injection

**Implementation**

- Follow the concurrent snapshot approach used by `NiFiClientMetricsRegistry`.
- Metrics are non-throwing observations.
- Metrics never affect deployment decisions or exception handling.
- Preserve direct `new FlowBuilder()` construction if currently supported.

**Size:** Medium, approximately 200-350 lines  
**Risk:** Medium  
**Status:** ✅ Complete

**Completion record (2026-07-15)**

| Item | Result |
|---|---|
| `FlowDeploymentMetricsRegistry.java` created | yes — @Component; 8 public enums (DeploymentOutcome, StageOutcome, Stage, Resource, ComponentAction, ActionOutcome, RollbackActionCategory, RollbackOutcome); 3 public snapshot records; 5 non-throwing observation methods that swallow all metric failures; `getSnapshot()` returning unmodifiable maps; package-private `DurationStats` with LongAdder + LongAccumulator |
| `FlowBuilder.java` modified | yes — public no-arg constructor (direct construction, private registry instance); @Autowired public 1-arg constructor (Spring injection); `deploymentSucceeded` boolean + outer try/finally for non-throwing deployment observation; `COORDINATOR.prepare(context, metrics)` call; `RollbackManager.rollback(..., metrics)` call; line count 91 |
| `DeploymentState.java` modified | yes — added `final FlowDeploymentMetricsRegistry metrics` field; `metrics()` accessor |
| `DeploymentPreparationStage.java` modified | yes — `prepare(context, resolver, metrics)` passes metrics to `new DeploymentState(...)` |
| `FlowDeploymentCoordinator.java` modified | yes — `prepare(context, metrics)` signature; `deploy()` wraps each of 5 stage calls with `System.nanoTime()` + `catch (RuntimeException e)` + `metrics.observeStage(...)` |
| `DependencyDeploymentStage.java` modified | yes — extracts `state.metrics()`, passes to `ParameterContextDeployer.deploy` and `csDeployer.deployAll` |
| `ComponentDeploymentStage.java` modified | yes — extracts `state.metrics()`, passes to all 7 deployers |
| `ConnectionConfigurationStage.java` modified | yes — extracts `state.metrics()`, passes to ConnectionDeployer, RelationshipConfigurer, SnippetDeployer |
| `RuntimeActivationStage.java` modified | yes — extracts `state.metrics()`, passes to RuntimeStateApplier and ProcessorStarter |
| `ParameterContextDeployer.java` modified | yes — accepts metrics; try-catch wrapping entire operation; observes PARAMETER_CONTEXT + CREATED/REUSED + SUCCESS/FAILURE |
| `ControllerServiceDeployer.java` modified | yes — `deployAll` accepts metrics; observes CONTROLLER_SERVICE + CREATED/REUSED + SUCCESS/FAILURE in existing per-service try-catch |
| `ProcessorDeployer.java` modified | yes — `deploy` accepts metrics; `existingId` hoisted before try; observes PROCESSOR + CREATED/UPDATED + SUCCESS/FAILURE per item |
| `PortDeployer.java` modified | yes — accepts metrics; null-guarded `portAction` tracks CREATED/REUSED; observes INPUT_PORT or OUTPUT_PORT per item |
| `FunnelDeployer.java` modified | yes — accepts metrics; null-guarded `funnelAction` tracks CREATED/REUSED and switches to UPDATED only when an update executes |
| `LabelDeployer.java` modified | yes — accepts metrics; null-guarded `labelAction` tracks CREATED/UPDATED |
| `RemoteProcessGroupDeployer.java` modified | yes — accepts metrics; null-guarded `rpgAction` tracks CREATED/UPDATED |
| `ConnectionDeployer.java` modified | yes — accepts metrics; `connAction` tracks CREATED/UPDATED/REUSED/SKIPPED so duplicate specifications do not inflate update counts; null-guarded failure observation |
| `RelationshipConfigurer.java` modified | yes — accepts metrics; per-processor `catch (RuntimeException e)` wrapping; observes RELATIONSHIP + CONFIGURED + SUCCESS/FAILURE |
| `SnippetDeployer.java` modified | yes — accepts metrics; observes SNIPPET per operation with operation-specific action (create→CREATED, move→UPDATED, copy→CREATED, delete→CONFIGURED); `snippetAction()` helper for failure branch |
| `RuntimeStateApplier.java` modified | yes — accepts metrics; observes PORT_RUNTIME_STATE + CONFIGURED and REMOTE_TRANSMISSION + CONFIGURED per request |
| `ProcessorStarter.java` modified | yes — `startIfRequested` and `startProcessors` accept metrics; observes PROCESSOR + STARTED + SUCCESS/FAILURE for valid/invalid/exception cases |
| `RollbackManager.java` modified | yes — `rollback` accepts metrics; `runStep` extended with `RollbackActionCategory` parameter + per-step `observeRollbackAction`; `observeRollback(SUCCESS)` before successful return; `observeRollback(PARTIAL_FAILURE)` after failure aggregation |
| Non-throwing guarantee | yes — all 5 observation methods catch `Throwable` and call an allocation-free, non-throwing discard helper so observations cannot replace deployment or rollback failures |
| Bounded dimensions | yes — all call sites use fixed enum values only; no IDs, names, URIs, or exception data |
| Direct `new FlowBuilder()` construction | yes — public no-arg constructor uses `new FlowDeploymentMetricsRegistry()` as private accumulator |
| `@Autowired` constructor | yes — Spring uses 1-arg constructor with injected shared registry |
| Stage order preserved | yes — PREPARATION, DEPENDENCY_DEPLOYMENT, COMPONENT_DEPLOYMENT, CONNECTION_CONFIGURATION, RUNTIME_ACTIVATION unchanged |
| Exception boundary preserved | yes — outer try/finally only records metrics; inner try-catch still handles rollback/logging; RollbackManager metric failures cannot enter failures list |
| `git diff --check` | clean |
| Compile | `BUILD SUCCESS` — 57 source files |

### Metrics

| Metric | Bounded Dimensions |
|---|---|
| `nifi_copilot_flow_deployments_total` | `outcome` |
| `nifi_copilot_flow_deployment_duration_millis_total` | `outcome` |
| `nifi_copilot_flow_deployment_duration_millis_max` | `outcome` |
| `nifi_copilot_flow_stage_executions_total` | `stage`, `outcome` |
| `nifi_copilot_flow_stage_duration_millis_total` | `stage`, `outcome` |
| `nifi_copilot_flow_stage_duration_millis_max` | `stage`, `outcome` |
| `nifi_copilot_flow_components_total` | `resource`, `action`, `outcome` |
| `nifi_copilot_flow_rollback_actions_total` | `action`, `outcome` |
| `nifi_copilot_flow_rollbacks_total` | `outcome` |

Allowed values are fixed enums:

- deployment outcome: `success`, `failure`
- stage outcome: `success`, `failure`, `skipped`
- rollback outcome: `success`, `partial_failure`
- resource action: `created`, `reused`, `updated`, `configured`, `started`, `skipped`
- action outcome: `success`, `failure`
- resource and stage names: fixed implementation enums

Never use IDs, names, target URIs, processor/controller types, exception data, or concrete client class names as dimensions.

**Exit criteria**

- Deployment, stage, resource, and rollback metrics use bounded dimensions.
- Metrics cannot alter deployment outcomes.

# Review and Completion Gates

Every phase must satisfy:

- [ ] Only current-phase files were inspected and modified.
- [ ] Public APIs remain unchanged.
- [ ] `CopilotController` remains unchanged.
- [ ] `NiFiClientOperations` remains unchanged.
- [ ] Public DTOs remain unchanged.
- [ ] Caller specifications remain unmodified.
- [ ] Exact deployment ordering remains unchanged.
- [ ] Resource identity behavior remains unchanged.
- [ ] Rollback ownership and action ordering remain unchanged.
- [ ] Original exceptions remain primary.
- [ ] Dual-client behavior remains unchanged.
- [ ] No request-local mutable Spring bean was introduced.
- [ ] No God `DeploymentContext` or `DeploymentState` was introduced.
- [ ] No duplicate methods or compatibility bridges remain.
- [ ] No unused imports or unreachable code remain.
- [ ] No TODO placeholders or commented-out code remain.
- [ ] Formatting is clean.
- [ ] Production compilation succeeds.
- [ ] Existing relevant tests pass unchanged when tests are authorized.
- [ ] Focused code review finds no unresolved correctness issue.
- [ ] Tracker status and completion record are updated.
- [ ] Work stops before the next phase.

## Standard Compile Command

```powershell
.\mvnw.cmd -pl 'nifi-copilot' -DskipTests compile --no-transfer-progress
```

## Standard Phase Prompt

For every phase:

1. Read this tracker and only the source files required for the current phase.
2. Do not inspect unrelated packages.
3. Confirm the current phase is not blocked by an incomplete dependency.
4. Implement only the current phase.
5. Preserve behavior and public contracts.
6. Compile and run the completion gate.
7. Perform a focused review of the extracted responsibility.
8. Resolve review findings.
9. Update this tracker.
10. Stop.
11. Do not begin the next phase.

# Risk Register

| Risk | Control |
|---|---|
| Cyclic collaborator dependencies | Enforce one-way package dependency direction |
| God context/state object | Separate immutable request facts, typed stage state, report, registry, and ownership ledger |
| Mutable singleton request state | Keep request objects as ordinary per-call instances |
| Rollback reordering | Typed ledger categories and explicit old/new sequence comparison |
| Identity behavior drift | Resource parity matrix and one strategy per resource |
| Failure-timing drift | Move existing checks without front-loading live calls |
| Over-fragmentation | Extract cohesive method families, not one class per trivial helper |
| False contributor abstraction | Keep Phase 8 optional and canvas-only |
| Metrics cardinality | Fixed enums and prohibited dynamic dimensions |
| Metrics affecting behavior | Non-throwing observation path only |
| Dual-client differences | Depend only on `NiFiClientOperations` and shared response normalization |
| Public compatibility drift | Keep facade, result, controller, client interface, and DTOs unchanged |

# Progress Tracker

| Phase | Status | Compile | Tests | Review | Commit/PR | Notes |
|---|---|---|---|---|---|---|
| 0. Baseline and guardrails | ✅ Complete | Production compile successful | Deferred | Baseline reviewed | | No Java changes |
| 1. Support and local preflight | ✅ Complete | Production compile successful | Deferred | Behavior-preservation review complete | | Phase 1 only |
| 2. Layout and projection | ✅ Complete | Production compile successful | Deferred | Behavior-preservation review complete | | Phase 2 only; Phase 3 remains unstarted |
| 3. Resolution and live preflight | ✅ Complete | Production compile successful | Deferred | Identity and call-order parity reviewed | | Phase 3 only; Phase 4 remains unstarted |
| 4. Ownership ledger and rollback | ✅ Complete | Production compile successful | Deferred | Rollback order and ownership parity reviewed | | Phase 4 only; Phase 5 remains unstarted |
| 5. Dependencies and processors | ✅ Complete | Production compile successful | Deferred | Dependency, ownership, result, relationship, and startup parity reviewed | | Phase 5 only; Phase 6A complete |
| 6A. Canvas resource deployers | ✅ Complete | Production compile successful | Deferred | Port/funnel/label/RPG behavior, rollback registration, aggregation, and order reviewed | | Phase 6A only; Phase 6B remains unstarted |
| 6B. Connections, snippets, runtime | ✅ Complete | Production compile successful | Deferred | Connection, snippet, runtime ordering, ownership, and aggregation parity reviewed | | Phase 6B only; Phase 7 remains unstarted |
| 7. Stages and coordinator | ✅ Complete | Production compile successful | Deferred | Boundary timing, stage order, state/report semantics, and public facade reviewed | | Phase 7 only; Phase 8 remains not evaluated |
| 8. Optional contributors | ❌ N/A — Skipped | | | | | Port, funnel, label, RPG deployers retain materially different identity, update, state, and rollback lifecycles; contributor interface would be false abstraction |
| 9. Deployment metrics | ✅ Complete | Production compile successful | Deferred | Bounded dimensions, non-throwing, construction, exception boundary, and rollback order reviewed | | Phase 9 only; this is the last planned implementation phase |

# Completion Record

| Date | Phase | Files Created/Modified | Methods Migrated | Compile | Tests | Review Findings | Commit/PR | Notes |
|---|---|---|---|---|---|---|---|---|
| 2026-07-15 | Phase 0: Baseline and guardrails | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md` | None | `nifi-copilot` production compile successful | Deferred | Public surface, 94 methods, 15 nested types, deployment order, rollback order, and dirty-worktree boundary recorded | | No Java changes; Phase 1 not started |
| 2026-07-15 | Phase 1: Support and local preflight | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; created `SpecificationSupport.java`, `NiFiEntitySupport.java`, `LocalPreflightValidator.java`, `DeploymentContext.java` | Recursive specification copy; map/list/string/scalar/state conversion; entity IDs, fallbacks, envelopes, numeric readers; collection, component-ID, snippet, and parameter-context validation | `nifi-copilot` production compile successful | Deferred | Validation order/messages, mutable deep copy, insertion order, unchecked casts/defaults, null-permitting existing-ID copy, context scope, entity envelopes, finite-number handling, and public compatibility reviewed | | Phase 1 only. `FlowBuilder` remains the public `@Component` facade with public `BuildResult`, `buildFlow(...)`, and `readCanvas(...)`; do not delete, rename, replace, or reduce it away until the final modularization phase is complete. Phase 2 remains unstarted. |
| 2026-07-15 | Phase 2: Canvas layout and projection | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; created `CanvasBounds.java`, `CollisionAvoider.java`, `CanvasLayoutEngine.java`, `CanvasProjector.java` | Graph ranking, deterministic cycle handling, processor placement and offsets, reused-processor preservation, occupied bounds and dimensions, collision search and position claims, requested-position handling, canvas projection and stable IDs/defaults | `nifi-copilot` production compile successful | Deferred | Review finding for blank existing-ID mappings bypassing layout resolved; coordinates, ranking/cycles, reused-processor drift, collision padding/bounds/search, effective-group inventory timing, copied-spec-only mutation, response envelopes/nulls, output keys/order, warnings, controller-service failures, package visibility, dependency boundaries, and public facade reviewed | | Phase 2 only. `FlowBuilder` remains the public `@Component` facade with public `BuildResult`, `buildFlow(...)`, and `readCanvas(...)`; Phase 3 remains unstarted. |
| 2026-07-15 | Phase 3: Component resolution and live preflight | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; created `ComponentRegistry.java`, `ComponentResolver.java`, `DeploymentTarget.java`, `LivePreflightValidator.java` | Request-local component/snippet registry; target and child-group resolution; effective inventory target facts; inventory, existing-processor, and processor-type registration; processor aliases; resource-specific canvas matching; endpoint ID/type validation; connection tuple matching; snippet references/selections; parameter-context identity/compatibility; controller-service identity | `nifi-copilot` production compile successful using `mvnw.cmd` | Deferred | Exact messages and ambiguity rules, blank/null processor mappings, registration timing, missing connection response types, dedicated snippet identity, effective inventory envelopes, target/snapshot/API order, direct map identity, non-mutating live validation, interface-only client dependency, collaborator visibility, and public facade retention reviewed. Conflicting component types for one spec/NiFi ID and conflicting duplicate specifications for reused connections were found and resolved. | | Phase 3 only. `FlowBuilder` is 1,243 lines and remains the public `@Component` facade with public `BuildResult`, `buildFlow(...)`, and `readCanvas(...)`; Phase 4 remains unstarted. |
| 2026-07-15 | Phase 4: Ownership ledger and rollback manager | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; created `OwnershipLedger.java`, `RollbackManager.java` | Typed ownership and restoration records; processor, connection, controller-service, parameter-context, child-group, canvas, snippet, port-runtime, and remote-transmission registration; ordered compensating rollback and suppressed-failure aggregation | `nifi-copilot` production compile successful using `mvnw.cmd` | Deferred | Registration timing, request-local state, forward/reverse category order, owned-resource boundaries, parameter-binding restoration, child-group cleanup, failure suppression, interface-only client dependency, absence of snapshot restoration, and public facade retention reviewed; no correctness findings | | Phase 4 only. `DeploymentResources` and rollback execution were removed from `FlowBuilder`; `FlowBuilder` is 1,067 lines and remains the public `@Component` facade. Phase 5 remains unstarted. |
| 2026-07-15 | Phase 5: Dependency and processor deployers | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; created `ParameterContextDeployer.java`, `ControllerServiceDeployer.java`, `ProcessorDeployer.java`, `RelationshipConfigurer.java`, `ProcessorStarter.java` | Parameter-context compatibility/create/bind; controller-service dependency graph, matching, create/enable, and property references; processor create/update/results; relationship auto-termination; downstream-first startup graph and validity wait | `nifi-copilot` production compile successful using `mvnw.cmd` | Deferred | Deployment order, dependency/cycle behavior, matching and reuse, ownership registration timing, processor payloads/results and aggregation, relationship defaults, startup order/warnings/timeouts, request-local state, interface-only client dependency, and public facade retention reviewed; no correctness findings | | Phase 5 only. `FlowBuilder` is 640 lines and remains the public `@Component` facade; Phase 6A remains unstarted. |
| 2026-07-15 | Phase 6A: Canvas resource deployers | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; `NiFiEntitySupport.java`; created `PortDeployer.java`, `FunnelDeployer.java`, `LabelDeployer.java`, `RemoteProcessGroupDeployer.java` | Port input/output create/reuse/state; funnel create/update with position restore; label create/update with conditional style/width/height; RPG create/update with stop-before-update and transmission restore; rollback registration; aggregation helpers; canvas order preserved | `nifi-copilot` production compile successful using `mvnw.cmd` | Deferred | Resource-specific identity, incompatible-name fatal check, state validation, existing-state-restore timing, runtime request registration, position update semantics, restore-fields logic, original-state capture and transmission defaults, rollback order, and public facade retention reviewed; no correctness findings | | Phase 6A only. `FlowBuilder` is 370 lines; `deployCanvasComponents`, `deployPorts`, `deployFunnels`, `deployLabels`, `deployRemoteProcessGroups`, `restoreFields`, `originalUpdatedFields` removed; `OwnershipLedger` and `ComponentResolver` unchanged; Phase 6B remains unstarted. |
| 2026-07-15 | Phase 6B: Connections, snippets, and runtime | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; created `ConnectionDeployer.java`, `SnippetDeployer.java`, `RuntimeStateApplier.java` | Typed endpoint resolution; connection matching/create/update/count and duplicate conflict handling; snippet create/move/copy/delete and reverse-move ownership; deferred port and RPG runtime-state application | `nifi-copilot` production compile successful using `mvnw.cmd` | Deferred | Endpoint typing and unsupported remote ports, relationship defaults, connection reuse/restoration/ownership/count, snippet registry and rollback boundaries, runtime forward ordering, failure aggregation, stage call order, interface-only client dependency, and public facade retention reviewed; no correctness findings | | Phase 6B only. `FlowBuilder` is 196 lines and retains only orchestration, target preparation, public result mapping, and exception/rollback boundary; Phase 7 remains unstarted. |
| 2026-07-15 | Phase 8: Optional contributors | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md` | None | — | Deferred | Evaluated: port, funnel, label, RPG deployers have materially distinct identity rules (port name-incompatibility fatal, funnel ID-only, label text fallback, RPG stop-before-update), update lifecycles, and rollback registration; no shared contributor interface created | | N/A — no Java changes; false abstraction avoided |
| 2026-07-15 | Phase 9: Deployment metrics | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; created `FlowDeploymentMetricsRegistry.java`; modified `FlowBuilder.java`, `DeploymentState.java`, `FlowDeploymentCoordinator.java`, `DeploymentPreparationStage.java`, `DependencyDeploymentStage.java`, `ComponentDeploymentStage.java`, `ConnectionConfigurationStage.java`, `RuntimeActivationStage.java`, `ParameterContextDeployer.java`, `ControllerServiceDeployer.java`, `ProcessorDeployer.java`, `PortDeployer.java`, `FunnelDeployer.java`, `LabelDeployer.java`, `RemoteProcessGroupDeployer.java`, `ConnectionDeployer.java`, `RelationshipConfigurer.java`, `SnippetDeployer.java`, `RuntimeStateApplier.java`, `ProcessorStarter.java`, `RollbackManager.java` | FlowDeploymentMetricsRegistry with 8 enums, 3 snapshot records, 5 non-throwing observations, DurationStats accumulator; deployment timing in FlowBuilder finally; stage timing in coordinator; component metrics in all deployers; rollback action + overall outcome in RollbackManager; Spring-injected shared registry plus isolated direct-construction registry | `nifi-copilot` production compile successful using `mvnw.cmd` — 57 source files | Deferred | Bounded dimensions, exception/order parity, construction paths, component classification, and rollback instrumentation reviewed. Escaping metric failures, duplicate-connection update overcounting, and unchanged-funnel update overcounting were resolved and re-reviewed with no remaining correctness findings. | | This is the last planned implementation phase. `FlowBuilder` remains the public `@Component` facade at 91 lines. |
| 2026-07-15 | Phase 7: Coarse stages and coordinator | `FLOWBUILDER_MODULARIZATION_IMPLEMENTATION_PLAN.md`; `FlowBuilder.java`; created `DeploymentState.java`, `DeploymentReport.java`, `DeploymentPreparationStage.java`, `DependencyDeploymentStage.java`, `ComponentDeploymentStage.java`, `ConnectionConfigurationStage.java`, `RuntimeActivationStage.java`, `FlowDeploymentCoordinator.java` | Pre-boundary request preparation; effective-target preparation; dependency, component, connection/configuration, and runtime stages; typed state accumulation; internal report conversion; coordinator sequencing | `nifi-copilot` production compile successful using `mvnw.cmd` | Deferred | Exact pre/post exception-boundary timing, stage/deployer order, request-local mutable state, parent/effective snapshots, rollback condition, public result mutability, resource-neutral coordinator, stage independence, interface-only client dependency, and public facade retention reviewed. Result mutability and coordinator resource leakage findings were resolved and re-reviewed. | | Phase 7 only. `FlowBuilder` is 65 lines—smaller than the approximate target without removing any facade responsibility—and remains the public `@Component`; Phase 8 remains not evaluated. |

## Phase 3 Identity Parity Matrix

| Resource | Exact Lookup Key | Ambiguity Behavior | Compatibility Condition | Mutation Location |
|---|---|---|---|---|
| Child process group | Unique `name` among children of the resolved parent | More than one same-name child is fatal | Reused entity must provide a nonblank ID | Creation remains in `FlowBuilder` target preparation; not moved to Phase 5/6 |
| Processor | Caller map from spec ID to nonblank NiFi ID, or spec ID equal to a stable live inventory ID | No semantic matching or fallback; conflicting registry IDs or cross-resource component types are fatal | Blank/null caller mappings are not reuse; reused live ID must resolve consistently as a processor | Create/update is isolated in `ProcessorDeployer` |
| Input/output port | Exact stable ID, otherwise unique `name` within the separate input or output collection | Duplicate ID or duplicate same-name matches are fatal | A reused port must have the exact requested name; incompatible name is fatal because updates are unsupported | Create/reuse and runtime-request registration are isolated in `PortDeployer` |
| Funnel | Exact stable NiFi ID only | Duplicate ID matches are fatal | No positional or ordinal fallback | Create/update is isolated in `FunnelDeployer` |
| Label | Exact stable ID, otherwise unique current `label` text | Duplicate ID or duplicate same-text matches are fatal | Exact ID wins; text is the only semantic fallback | Create/update is isolated in `LabelDeployer` |
| Remote process group | Exact stable ID, otherwise unique `targetUri` | Duplicate ID or duplicate same-URI matches are fatal | Exact ID wins; target URI is the only semantic fallback | Create/update and transmission-request registration are isolated in `RemoteProcessGroupDeployer` |
| Connection | Source NiFi ID/type plus destination NiFi ID/type tuple | More than one matching tuple is fatal; duplicate specifications with conflicting relationships are fatal for both reused and created connections | Missing/blank response endpoint types remain compatible; present types compare case-insensitively | Create/update is isolated in `ConnectionDeployer` |
| Snippet | Dedicated spec-snippet registry; raw snippet IDs pass through; selections resolve only registered/known canvas IDs | Conflicting registration for one snippet spec ID is fatal | Snippets never enter normal canvas identity; unknown selection references are fatal | Create/move/copy/delete is isolated in `SnippetDeployer` |
| Parameter context | Unique global `name` | More than one same-name context is fatal | Every requested parameter must exist among non-sensitive live parameters with the exact requested value | Create/bind is isolated in `ParameterContextDeployer` |
| Controller service | Unique `name` plus exact `type` | Multiple exact matches are fatal; same name with a conflicting type is fatal | Name and type must both match exactly | Create/enable is isolated in `ControllerServiceDeployer` |

# Definition of Done

- [x] `FlowBuilder` is 65 lines, below the approximate 100-200 target while retaining the complete public facade.
- [x] `buildFlow(...)` is a concise facade around `FlowDeploymentCoordinator`.
- [x] `readCanvas(...)` delegates to `CanvasProjector`.
- [x] Implementation collaborators are package-private unless an exception is documented.
- [x] `DeploymentContext` is immutable, request-local, and narrowly scoped.
- [x] `DeploymentState` contains only typed coordinator state.
- [x] Ownership and rollback are isolated in `OwnershipLedger` and `RollbackManager`.
- [x] `DeploymentReport` remains separate from public `BuildResult`.
- [x] Resource matching uses explicit resource APIs.
- [x] Layout and projection are independent.
- [x] Coarse stages preserve exact established ordering.
- [x] Contributor abstraction is added only if Phase 8 proves it is cohesive. ✅ Evaluated as N/A — port, funnel, label, RPG deployers retain materially distinct lifecycles.
- [x] Metrics use bounded dimensions and existing concurrency conventions. ✅ Phase 9 complete.
- [ ] Public contracts, dual-client behavior, idempotency, rollback, ordering, layout, projection, and all Phase 12 resources remain unchanged.
