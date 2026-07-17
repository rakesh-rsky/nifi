# FlowBuilder Refactoring Plan

## Purpose

Refactor `FlowBuilder` so that it remains responsible only for coordinating flow deployment and canvas reads. Detailed resource creation, dependency resolution, layout, startup, failure tracking, and rollback preparation should be delegated to small private methods or the existing focused helper classes.

This plan is based on the current `FlowBuilder` review. It does not introduce code or intentionally change external behavior.

## Scope

Primary file:

- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/FlowBuilder.java`

Direct dependencies should be changed only when an extracted operation cannot remain private to `FlowBuilder` or an existing direct contract must expose an already-supported result.

## Non-Goals

- Do not redesign the Copilot controller or NiFi client architecture.
- Do not change the public `buildFlow()` or `readCanvas()` contracts during the structural refactoring.
- Do not add Phase 4 canvas components until the existing orchestration is decomposed and hardened.
- Do not combine behavior changes with method extraction.
- Do not rewrite `FlowBuilder` in one commit.

## Target Architecture

`buildFlow()` should become a short orchestration method that:

1. Validates and normalizes deployment input.
2. Resolves the deployment target.
3. Prepares rollback state.
4. Applies layout.
5. Creates resources in dependency order.
6. Configures relationships.
7. Optionally starts processors.
8. Returns the deployment result.
9. Delegates failure cleanup to one rollback method.

It should not contain resource-specific payload preparation, ID extraction, dependency traversal, relationship calculations, or rollback bookkeeping.

## Current Responsibilities in `buildFlow()`

The current method combines:

- Input validation
- Target process-group resolution
- Snapshot capture
- Layout and collision avoidance
- Optional child-process-group creation
- Parameter-context deployment
- Controller-service deployment and enablement
- Processor type resolution
- Processor configuration
- Processor creation
- ID mapping
- Connection endpoint resolution
- Connection creation
- Relationship usage calculation
- Auto-termination
- Processor validation and startup
- Result construction
- Rollback registration and execution
- Exception logging and propagation

These responsibilities should be separated incrementally.

## Extraction Order

The extraction order is intentionally bottom-up. Pure calculations and narrow resource stages should be extracted before the main orchestration structure changes.

### 1. Extract Processor Type and Name Resolution

**Proposed methods**

- `resolveProcessorType(Map<String, Object> processorSpec)`
- `resolveProcessorName(Map<String, Object> processorSpec, String resolvedType)`

**Move**

- Processor alias lookup through `PROCESSOR_REGISTRY`
- Fully qualified processor type selection
- Default processor-name calculation

**Reason**

This is pure decision logic embedded in the processor creation loop. Extracting it first reduces loop complexity without changing deployment sequencing.

**Estimated size:** Small

- 2 private methods
- Approximately 15-25 lines moved
- No expected contract changes

### 2. Extract Connection Endpoint and Relationship Resolution

**Proposed methods**

- `resolveComponentId(Object reference, Map<String, String> componentIds)`
- `resolveRelationships(Map<String, Object> connectionSpec)`
- `findUsedRelationships(String processorId, List<Map<String, Object>> connectionSpecs, Map<String, String> componentIds)`

**Move**

- Source and destination specification-ID lookup
- Existing NiFi ID fallback
- Default `success` relationship handling
- Relationship collection used by auto-termination

**Reason**

Connection creation and auto-termination currently repeat related parsing and mapping behavior. Centralizing it prevents the two stages from interpreting the same connection differently.

**Estimated size:** Small

- 3 private methods
- Approximately 20-35 lines moved or consolidated
- No expected contract changes

### 3. Extract Target Process-Group Resolution

**Proposed method**

- `resolveTargetProcessGroup(Map<String, Object> processGroupSpec, NiFiClientOperations nifi)`

**Move**

- Existing process-group ID lookup
- Root/default process-group handling
- Missing or invalid target validation

**Reason**

Target resolution is an independent precondition and should be completed before snapshot or mutation stages.

**Estimated size:** Small

- 1 private method
- Approximately 10-15 lines moved
- No expected contract changes

### 4. Extract Snapshot Capture

**Proposed method**

- `captureSnapshot(String processGroupId, boolean rollbackOnFailure, NiFiClientOperations nifi)`

**Move**

- Rollback-enabled check
- Snapshot request
- Current warning behavior when capture fails

**Reason**

This separates rollback preparation from resource deployment. The first extraction must preserve the current warning-and-continue behavior; fail-closed behavior belongs in a later correctness commit.

**Estimated size:** Small

- 1 private method
- Approximately 10-15 lines moved
- No intentional behavior change

### 5. Extract Optional Child Process-Group Creation

**Proposed method**

- `createChildProcessGroupIfPresent(Map<String, Object> processGroupSpec, String parentProcessGroupId, NiFiClientOperations nifi)`

**Move**

- Child-group presence check
- Name and position extraction
- Child-group creation
- Created child ID extraction
- Effective process-group ID selection

**Reason**

The effective deployment group is needed by every later resource stage. Returning it explicitly makes that state transition visible.

**Estimated size:** Small

- 1 private method
- Approximately 12-20 lines moved
- No expected contract changes

### 6. Extract Parameter Context and Controller Service Deployment

**Proposed methods**

- `deployParameterContext(Map<String, Object> spec, String processGroupId, ParameterContextManager manager, NiFiClientOperations nifi)`
- `deployControllerServices(Map<String, Object> spec, String processGroupId, ControllerServiceManager manager, NiFiClientOperations nifi)`

**Move**

- Optional parameter-context parsing and deployment
- Controller-service specification parsing and deployment

**Reason**

These are separate dependency stages. Parameter context binding must complete before processor creation, while controller services must be available before processor property references are resolved.

**Estimated size:** Small

- 2 private methods
- Approximately 12-20 lines moved
- Existing manager behavior retained

### 7. Extract Processor Creation

**Proposed method**

- `createProcessors(List<Map<String, Object>> processorSpecs, String processGroupId, ControllerServiceManager controllerServices, Map<String, String> componentIds, NiFiClientOperations nifi)`

**Supporting result**

- Return the existing list of created processor entities.
- Continue updating the supplied component-ID map during the behavior-preserving extraction.

**Move**

- Processor iteration
- Type and name resolution calls
- Position extraction
- Configuration extraction
- Controller-service reference resolution
- Processor creation
- Created-ID extraction
- Specification-ID to NiFi-ID mapping
- Existing per-processor warning behavior

**Reason**

This is the largest cohesive deployment stage and a primary contributor to `buildFlow()` complexity.

**Estimated size:** Medium

- 1 orchestration method plus use of methods from Step 1
- Approximately 30-45 lines moved
- No intentional behavior change

### 8. Extract Connection Creation

**Proposed method**

- `createConnections(List<Map<String, Object>> connectionSpecs, String processGroupId, Map<String, String> componentIds, NiFiClientOperations nifi)`

**Return**

- Number of successfully created connections

**Move**

- Connection iteration
- Endpoint resolution
- Unresolved-endpoint skip
- Relationship resolution
- Connection creation
- Existing per-connection warning behavior

**Reason**

Connections form a distinct stage that must execute only after processor IDs are available.

**Estimated size:** Medium

- 1 private method
- Approximately 20-30 lines moved
- No intentional behavior change

### 9. Extract Auto-Termination Configuration

**Proposed method**

- `configureAutoTerminatedRelationships(List<Map<String, Object>> createdProcessors, List<Map<String, Object>> connectionSpecs, Map<String, String> componentIds, NiFiClientOperations nifi)`

**Move**

- Created processor iteration
- Used-relationship calculation
- Calls to `autoTerminateUnusedRelationships`

**Reason**

Relationship configuration is a separate post-connection stage and should not be mixed with connection creation or processor startup.

**Estimated size:** Small

- 1 private method
- Approximately 12-20 lines moved
- Preserve current exception behavior during extraction

### 10. Extract Processor Startup

**Proposed methods**

- `startProcessorsIfRequested(boolean autoStart, List<Map<String, Object>> processorSpecs, List<Map<String, Object>> connectionSpecs, Map<String, String> componentIds, NiFiClientOperations nifi)`
- `startProcessors(List<String> startupOrder, Map<String, String> componentIds, NiFiClientOperations nifi)`

**Move**

- Auto-start condition
- Startup-order calculation call
- Processor ID lookup
- Processor-validity wait
- Start operation

**Reason**

Startup is an optional lifecycle phase and must remain separate from creation. The initial extraction should preserve the existing declaration-order behavior.

**Estimated size:** Medium

- 2 private methods
- Approximately 18-30 lines moved
- No intentional behavior change

### 11. Extract Rollback Registration and Execution

**Proposed method**

- `rollbackDeployment(Map<String, Object> snapshot, ControllerServiceManager controllerServices, ParameterContextManager parameterContexts, NiFiClientOperations nifi, Exception deploymentFailure)`

**Move**

- Created controller-service ID registration
- Created parameter-context ID registration
- Snapshot restore
- Rollback exception suppression

**Reason**

`buildFlow()` should delegate cleanup as one operation. This also establishes snapshot restoration as the only rollback mechanism before dead teardown methods are removed.

**Estimated size:** Medium

- 1 private method
- Approximately 15-25 lines moved
- Exception propagation must remain unchanged

### 12. Reduce `buildFlow()` to Orchestration

After Steps 1-11, `buildFlow()` should contain only:

- High-level guards
- Manager and deployment-state initialization
- Ordered calls to extracted stages
- Result construction
- One top-level catch that delegates rollback and rethrows

**Estimated size:** Medium

- Approximately 25-40 lines remaining in `buildFlow()`
- Approximately 100-140 lines relocated into private methods
- No public signature changes

## Independently Reviewable Commit Plan

### Commit 1: Extract Pure Resolution Helpers

**Includes**

- Processor type resolution
- Processor name resolution
- Connection endpoint resolution
- Relationship resolution
- Used-relationship calculation

**Size:** Small, approximately 35-55 changed lines

**Review focus**

- Exact preservation of defaults
- Existing-ID fallback behavior
- Relationship mapping parity

### Commit 2: Extract Deployment Preparation

**Includes**

- Target process-group resolution
- Snapshot capture
- Optional child process-group creation

**Size:** Small, approximately 35-50 changed lines

**Review focus**

- No mutation before snapshot capture
- Effective process-group ID remains unchanged
- Current snapshot failure behavior remains unchanged

### Commit 3: Extract Dependency Deployment Stages

**Includes**

- Parameter-context deployment wrapper
- Controller-service deployment wrapper

**Size:** Small, approximately 20-35 changed lines

**Review focus**

- Parameter context remains before controller services and processors
- Controller-service manager behavior remains unchanged

### Commit 4: Extract Processor Creation

**Includes**

- `createProcessors()`
- Calls to processor resolution helpers
- Existing component-ID map updates

**Size:** Medium, approximately 45-70 changed lines

**Review focus**

- Type aliases and default names
- Controller-service property reference resolution
- Position defaults
- Per-processor partial failure behavior

### Commit 5: Extract Connections and Auto-Termination

**Includes**

- `createConnections()`
- `configureAutoTerminatedRelationships()`

**Size:** Medium, approximately 45-65 changed lines

**Review focus**

- Processor creation still precedes connections
- Unresolved endpoint behavior is unchanged
- Default relationships remain unchanged
- Auto-termination remains after connection creation

### Commit 6: Extract Startup Lifecycle

**Includes**

- `startProcessorsIfRequested()`
- `startProcessors()`

**Size:** Small to Medium, approximately 30-45 changed lines

**Review focus**

- `autoStart=false` remains a no-op
- Validation wait remains before start
- Existing startup order remains unchanged

### Commit 7: Extract Rollback and Finalize Orchestrator

**Includes**

- `rollbackDeployment()`
- Final reduction of `buildFlow()`

**Size:** Medium, approximately 45-70 changed lines

**Review focus**

- Created controller-service and parameter-context IDs remain registered
- Snapshot restoration is invoked under the same conditions
- Rollback failures remain suppressed on the deployment failure
- Original exception remains the primary failure

### Commit 8: Remove Dead Rollback Methods and Improve Logs

**Includes**

- Remove `ControllerServiceManager.teardownAll()`
- Remove `ParameterContextManager.teardown()`
- Add contextual logs to existing silent failure paths

**Size:** Small, approximately 20-35 changed lines

**Review focus**

- Removed methods have no callers
- No cleanup path is lost
- Logging does not alter control flow

## Follow-Up Correctness Commits

These changes should not be mixed with the structural commits because they intentionally alter behavior.

### Commit 9: Make Rollback Preparation Fail Closed

- Abort before mutation when rollback is requested but snapshot capture fails.
- Make the deployment's rollback guarantee explicit.

**Size:** Small  
**Risk:** Medium

### Commit 10: Correct Controller-Service Dependency Handling

- Resolve controller-service-to-controller-service references.
- Detect dependency cycles.
- Create services in dependency order.
- Enable services only after dependencies are ready.
- Report failures instead of silently continuing.

**Size:** Large  
**Risk:** High

### Commit 11: Correct Processor Startup Ordering

- Build a dependency graph from connections.
- Start processors in deterministic reverse-topological order.
- Define deterministic behavior for cycles.

**Size:** Medium  
**Risk:** High

### Commit 12: Harden Partial Failure and Rollback Ownership

- Return explicit component failures.
- Avoid child-process-group double deletion.
- Track only resources owned by the deployment.
- Prevent concurrent rollback from deleting another deployment's resources.

**Size:** Large  
**Risk:** High

### Commit 13: Improve Idempotency

- Reuse or update matching resources.
- Avoid duplicate parameter contexts and controller services.
- Define stable specification identifiers and conflict behavior.

**Size:** Large  
**Risk:** High

### Commit 14: Correct Canvas Layout and Collision Handling

- Replace or rename the current linear layout.
- Read occupied positions from the effective target group.
- Include all relevant canvas component bounds.
- Remove unchecked overlap fallback.
- Avoid mutating the input specification.

**Size:** Medium to Large  
**Risk:** Medium

### Commit 15: Extend Phase 4 FlowBuilder Coverage

- Add orchestration and rollback ownership for ports, labels, funnels, remote process groups, and supported snippet operations.
- Support non-processor connection endpoint types.

**Size:** Large  
**Risk:** High

## Size Scale

| Size | Expected scope |
|---|---|
| Small | 1-3 focused methods, generally under 50 changed lines |
| Medium | One complete deployment stage, generally 40-100 changed lines |
| Large | Dependency or ownership model changes, generally over 100 changed lines or direct contract changes |

Line estimates describe review size, not final source growth. Most structural commits move existing lines rather than adding new behavior.

## Completion Criteria for the Structural Refactoring

- `buildFlow()` is a short orchestration method.
- Each deployment stage has one clear responsibility.
- Public method signatures and response shapes remain unchanged.
- Resource creation order remains:
  1. Target/child process group
  2. Parameter context and binding
  3. Controller services
  4. Processors
  5. Connections
  6. Auto-terminated relationships
  7. Optional processor startup
- Existing best-effort and fatal-failure boundaries remain unchanged until their dedicated correctness commits.
- Snapshot restoration remains the single active rollback mechanism.
- No Phase 4 resource behavior is added during structural extraction.
