# Brutally Honest Architecture Review — NiFi Copilot
**Date:** 2026-07-26  
**Reviewer:** Principal Software Engineer  
**Objective:** Class-level simplification audit  

---

## Project Overview

| Module | Packages | Classes | LOC |
|--------|----------|---------|-----|
| `nifi-copilot` | 8 | 111 | 16,745 |
| `nifi-layout-engine` | 11 | 92 | 6,580 |
| **Total** | **19** | **203** | **23,325** |

---

## Package-by-Package Review

---

### Package: `org.apache.nifi.copilot` (root)

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `NiFiCopilotApplication.java` | ~10 | **KEEP** | Spring Boot main. Nothing to change. |
| `CopilotModule.java` | 13 | **DELETE** | Custom annotation marker. Adds zero value over Spring's `@Component`. Never conditionally scanned. Pure ceremony. |

**Package verdict:** Remove `CopilotModule`. Merge remaining into whatever package holds `NiFiCopilotApplication`.

---

### Package: `org.apache.nifi.copilot.api`

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `CopilotController.java` | 818 | **REDESIGN** | God class. Handles: REST mapping, authentication dispatch, canvas reading, capability orchestration, LLM generation dispatch, repair pipeline, deletion (4 entity types with tear-down ordering), CS action application, session management, response construction. Violates SRP catastrophically. |
| `Dto.java` | 85 | **SIMPLIFY** | Fine as a DTO file, but `ChatRequest` has 8 fields including `read_canvas`, `provider`, `model` which bleed orchestration concerns into the DTO. |

**Responsibility:** REST API + orchestration + business logic (too many responsibilities)  
**Actually needed:** YES, but only the REST mapping part  
**SRP violation:** YES — extreme. Controller is the orchestrator.  
**Tight coupling:** Coupled to 11 injected dependencies. Calls into every other package.  
**Duplicates logic:** Canvas projection logic is partially duplicated from `CanvasProjector`.  
**Should be split:** YES — extract `FlowOrchestrator`, `DeletionService`. Controller becomes ~80 lines.  

---

### Package: `org.apache.nifi.copilot.auth`

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `GitHubAuthManager.java` | ~200 | **KEEP** | Clean, single-responsibility. Handles device flow + token storage. |
| `AwsAuthManager.java` | ~300 | **KEEP** | Same pattern. SSO device flow + role selection + credential refresh. |
| `InstantNow.java` | ~10 | **DELETE** | A test-seam wrapper around `Instant.now()`. Use `Clock` injection instead (standard Java pattern). |

**Responsibility:** Authentication for GitHub and AWS  
**Actually needed:** YES  
**SRP violation:** No  
**Tight coupling:** No  
**Should be merged:** No  

---

### Package: `org.apache.nifi.copilot.builder`

**39 classes.** This is the deployment pipeline.

#### Core Orchestration

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `FlowBuilder.java` | 144 | **KEEP** | Public entry point. Clean delegation. 4 constructors is excessive — reduce to 2. |
| `FlowDeploymentCoordinator.java` | 143 | **KEEP** | Sequences stages. Simple, clear. The try/catch per-stage is boilerplate but tolerable. |
| `DeploymentContext.java` | ~60 | **SIMPLIFY** | Immutable value object. 7 fields is fine but `rollbackOnFailure` and `autoStart` are always `true`/`false` from the controller. Could be a record. |
| `DeploymentState.java` | ~100 | **SIMPLIFY** | Mutable accumulator. Too many setter-style fields. Could be reduced by passing values directly between stages. |
| `DeploymentTarget.java` | 52 | **MERGE** | Simple value object with 6 fields. Merge into `DeploymentState` as nested record. |
| `DeploymentReport.java` | ~25 | **MERGE** | Trivial 2-field record. Inline into `FlowDeploymentCoordinator` or return `BuildResult` directly. |
| `DeploymentPreparationStage.java` | ~200 | **KEEP** | Resolves target PG, captures snapshot. Proper stage. |
| `DependencyPreflightStage.java` | ~80 | **KEEP** | Validates CS dependencies exist before deployment. |
| `DependencyDeploymentStage.java` | ~100 | **KEEP** | Creates CS and parameter contexts. |
| `ComponentDeploymentStage.java` | ~200 | **KEEP** | Creates processors, ports, funnels, labels, RPGs. |
| `ConnectionConfigurationStage.java` | ~150 | **KEEP** | Wires connections. |
| `LayoutDeploymentStage.java` | ~150 | **KEEP** | Invokes layout engine. |
| `RuntimeActivationStage.java` | ~100 | **KEEP** | Starts processors, enables services. |

#### Resource Deployers

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `ProcessorDeployer.java` | ~200 | **KEEP** | Creates/updates processors. |
| `ConnectionDeployer.java` | ~200 | **KEEP** | Creates connections. |
| `ControllerServiceDeployer.java` | ~300 | **KEEP** | Creates/enables controller services. Complex but necessary. |
| `ParameterContextDeployer.java` | ~150 | **KEEP** | Creates/reuses parameter contexts. |
| `PortDeployer.java` | ~80 | **KEEP** | Creates input/output ports. |
| `FunnelDeployer.java` | ~40 | **KEEP** | Creates funnels. Trivial. |
| `LabelDeployer.java` | ~40 | **KEEP** | Creates labels. Trivial. |
| `RemoteProcessGroupDeployer.java` | ~80 | **KEEP** | Creates RPGs. |
| `SnippetDeployer.java` | ~100 | **KEEP** | Handles snippet create/move/copy/delete. |
| `ProcessorStarter.java` | ~60 | **MERGE** | Starts processors. Could be inlined into `RuntimeActivationStage`. |
| `RuntimeStateApplier.java` | ~80 | **MERGE** | Applies port/RPG runtime states. Could be inlined into `RuntimeActivationStage`. |
| `RelationshipConfigurer.java` | ~60 | **MERGE** | Auto-terminates relationships. Could be inlined into `ProcessorDeployer`. |

#### Support Classes

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `ComponentRegistry.java` | ~80 | **KEEP** | Maps spec_id → nifi_id. Necessary. |
| `ComponentResolver.java` | 366 | **SIMPLIFY** | Resolves references, matches CS, validates compatibility. Some methods are only used once — inline those. |
| `CanvasProjector.java` | ~150 | **KEEP** | Reads canvas state. Clean. |
| `CanvasPositionProvider.java` | ~80 | **SIMPLIFY** | Provides provisional x/y. Could be a static utility (3 constants + 2 methods). |
| `NiFiEntitySupport.java` | ~100 | **KEEP** | Static helpers for entity parsing. |
| `SpecificationSupport.java` | ~130 | **KEEP** | Static helpers for spec parsing. |
| `OwnershipLedger.java` | ~200 | **KEEP** | Tracks created resources for rollback. Essential for correctness. |
| `RollbackManager.java` | ~250 | **KEEP** | Compensating rollback. Critical for safety. |
| `LocalPreflightValidator.java` | ~150 | **KEEP** | Validates spec structure before hitting NiFi. |
| `LivePreflightValidator.java` | ~80 | **KEEP** | Validates against live NiFi state. |
| `NiFiClientLayoutWriter.java` | ~100 | **KEEP** | Writes layout positions to NiFi. |
| `ProcessGroupFlowMapAssembler.java` | ~100 | **MERGE** | Builds flow map for layout engine. Only called from `LayoutDeploymentStage`. Inline it. |
| `LayoutMode.java` | ~20 | **KEEP** | Enum: ENGINE / DISABLED. |
| `FlowDeploymentMetricsRegistry.java` | ~200 | **SIMPLIFY** | Enterprise metrics for pre-production. 90% is unused. Keep a simple counter. |

**Responsibility:** Deployment pipeline from validated spec to NiFi canvas  
**Actually needed:** YES — this is where the real work happens  
**SRP violation:** Individual classes are fine. The package is too large (39 classes).  
**Tight coupling:** Stages are cleanly decoupled via `DeploymentState`.  
**Should be split:** No — but MERGE the 3 trivial helper classes.  

**Recommended merges:**
- `ProcessorStarter` + `RuntimeStateApplier` → into `RuntimeActivationStage`
- `RelationshipConfigurer` → into `ProcessorDeployer`
- `DeploymentTarget` → nested record in `DeploymentState`
- `DeploymentReport` → inline into coordinator
- `ProcessGroupFlowMapAssembler` → inline into `LayoutDeploymentStage`
- `CanvasPositionProvider` → static utility in `NiFiEntitySupport`

**Net class reduction: 39 → 33 (-6 classes)**

---

### Package: `org.apache.nifi.copilot.capability`

**30 classes.** This is the intelligence layer.

#### Core Pipeline

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `IntentExtractor.java` | 222 | **SIMPLIFY** | Regex-based classification into 6 endpoint types, 6 formats, 7 transforms. Functional but bloated for what it does. Could be a 60-line lookup table with `Map<Pattern, EndpointKind>`. |
| `ProcessorSeedRanker.java` | 249 | **MERGE** | Scores processors against workflow intent. Only consumer is `CapabilityPromptRenderer`. Make it a private method. |
| `PropertyRanker.java` | 125 | **MERGE** | Classifies properties into REQUIRED/SERVICE_REFERENCE/DATA_CONTRACT/OPTIONAL/RUNTIME. Only consumer is `DependencyClosureResolver`. Make it a private inner class. |
| `DependencyClosureResolver.java` | 324 | **SIMPLIFY** | Resolves transitive CS dependencies with depth-limited BFS. Over-engineered: in practice, depth is almost always 1 (a processor needs a RecordReader, that's it). Simplify to single-level. |
| `CapabilityPromptRenderer.java` | 412 | **KEEP** | Renders capability context. This is the correct abstraction. Well-structured with bounded context. |
| `CapabilityGraph.java` | 232 | **KEEP** | Immutable indexed capability graph. Clean design. Records are well-structured. |
| `CapabilityGraphBuilder.java` | ~100 | **MERGE** | Only called from `CapabilityPromptRenderer` and `CapabilityRegistry`. Could be a static factory on `CapabilityGraph`. |
| `CapabilityRegistry.java` | ~200 | **KEEP** | Caches capabilities with TTL. Sound. |
| `CapabilityRegistryManager.java` | ~60 | **MERGE** | Thin per-client cache wrapper using `IdentityHashMap`. Single-tenant in practice. Merge into `CapabilityRegistry`. |
| `CapabilityDefinitionParser.java` | ~200 | **KEEP** | Parses NiFi REST API responses into capability records. Necessary. |
| `CapabilitySnapshot.java` | 17 | **KEEP** | Clean record type. |
| `FlowSpecificationValidator.java` | 532 | **SIMPLIFY** | Validates LLM output against capabilities. Much of its complexity (relationship validation, topology checks) becomes unnecessary if Java owns topology. Property and type validation remain. Target: ~200 lines. |

#### Repair Pipeline

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `RepairHintDeriver.java` | 131 | **DELETE** | Derives repair hints from validation failures for a 2nd LLM call. If Java owns topology and type resolution, repair loop is unnecessary. |
| `RepairContextExpander.java` | 319 | **DELETE** | Builds expanded context for repair LLM call. Same reasoning. The entire repair pipeline only exists because the LLM makes mistakes that Java then tries to fix by calling the LLM again. This is a cycle that should be broken. |
| `RepairHint.java` | ~30 | **DELETE** | Record used only by above. |

#### Data Model Records

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `ProcessorCapability.java` | ~30 | **KEEP** | Record. |
| `ControllerServiceCapability.java` | ~30 | **KEEP** | Record. |
| `PropertyCapability.java` | ~30 | **KEEP** | Record. |
| `AllowableValue.java` | ~10 | **KEEP** | Record. |
| `BundleCoordinate.java` | ~10 | **KEEP** | Record. |
| `ServiceApi.java` | ~10 | **KEEP** | Record. |
| `PropertyDependency.java` | ~10 | **KEEP** | Record. |
| `WorkflowIntent.java` | 80 | **KEEP** | Record. |
| `ValidatedFlowPlan.java` | 35 | **KEEP** | Immutable wrapper. |
| `ValidationIssue.java` | ~20 | **KEEP** | Record. |
| `ValidationIssueType.java` | ~10 | **KEEP** | Enum. |
| `ValidationReport.java` | ~20 | **KEEP** | Record. |
| `FlowSpecificationValidationException.java` | ~20 | **KEEP** | Exception carrying validation report. |
| `CapabilityDiscoveryException.java` | ~20 | **DELETE** | Duplicate of same class in `service` package. |
| `UnsupportedControllerServiceException.java` | ~20 | **KEEP** | Domain exception. |
| `CapabilityMetricsRegistry.java` | ~150 | **SIMPLIFY** | 38 counters/accumulators for pre-production. Over-instrumented. Keep 5 counters max. |

**Responsibility:** Intent extraction → processor selection → prompt rendering → validation  
**Actually needed:** YES — this is the brain  
**SRP violation:** `CapabilityPromptRenderer` does too many things (intent extraction + ranking + closure + rendering)  
**Tight coupling:** `RepairContextExpander` is coupled to `DependencyClosureResolver` internals  
**Duplicates logic:** `CapabilityDiscoveryException` duplicated across packages  
**Should be split:** No  
**Should be merged:** `ProcessorSeedRanker` + `PropertyRanker` + `CapabilityGraphBuilder` + `CapabilityRegistryManager` → merge into their single consumers  

**Recommended merges/deletes:**
- `ProcessorSeedRanker` → private method in `CapabilityPromptRenderer`
- `PropertyRanker` → private inner class in `DependencyClosureResolver`
- `CapabilityGraphBuilder` → static factory on `CapabilityGraph`
- `CapabilityRegistryManager` → merge into `CapabilityRegistry`
- `RepairHintDeriver` → DELETE
- `RepairContextExpander` → DELETE
- `RepairHint` → DELETE
- `CapabilityDiscoveryException` (capability pkg) → DELETE

**Net class reduction: 30 → 22 (-8 classes)**

---

### Package: `org.apache.nifi.copilot.config`

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `CorsConfig.java` | 44 | **KEEP** | Conditional CORS. Clean. |
| `ExternalNiFiClientCondition.java` | 30 | **KEEP** | Spring Condition. Clean. |

**Responsibility:** Spring configuration  
**Actually needed:** YES  
**SRP violation:** No  

---

### Package: `org.apache.nifi.copilot.llm`

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `LlmClient.java` | 472 | **REDESIGN** | Multiple problems: (1) 127-line system prompt embedded as string literal. (2) 5 method overloads for `generateFlowSpec`. (3) 4 method overloads for `generateFlowSpecBedrock`. (4) Calls both normalizers. (5) Duplicated logic between GitHub and Bedrock paths. (6) Contains `normalizeGeneratedLayout` which is post-generation business logic. |
| `TerminalLoggerNormalizer.java` | 851 | **DELETE** | 851 lines of Tarjan's SCC algorithm, weakly-connected-component analysis, structural sibling detection to fix LLM mistakes about logger placement. This is the smoking gun. If Java places loggers, this entire class vanishes. |
| `ParallelWorkerNormalizer.java` | 127 | **DELETE** | Fixes DistributeLoad topology mistakes by the LLM. Same logic: if Java owns topology, this is dead code. |

**Responsibility:** LLM communication + post-generation correction  
**Actually needed:** LLM communication YES. Post-generation correction NO.  
**SRP violation:** YES — `LlmClient` does: prompt construction, HTTP transport (2 providers), JSON parsing, layout normalization, terminal logger correction, parallel worker correction.  
**Tight coupling:** Normalizers are tightly coupled to LLM output format assumptions.  
**Duplicates logic:** GitHub/Bedrock code paths are 80% identical (same prompt, same message construction, same post-processing).  
**Should be split:** YES — `LlmClient` should be: prompt construction + unified HTTP transport.  
**Should be deleted:** Normalizers (978 LOC combined).  

**Net class reduction: 3 → 1 (-2 classes, -978 LOC)**

---

### Package: `org.apache.nifi.copilot.service`

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `HttpNiFiClient.java` | 2,693 | **SIMPLIFY** | Implements the entire NiFi REST API. The copilot uses maybe 15 methods. The other ~50 methods (provenance, lineage, registry, version control, cluster diagnostics) are dead weight for the copilot use case. However: this may be used by tests or internal NiFi client. Keep but mark unused methods. |
| `NiFiClientOperations.java` | 1,630 | **SIMPLIFY** | Interface with ~65 methods + static helper methods. Way too many static helpers live here (`strictRevisionVersion`, `intValue`, `isRunning`, `isStopped`, etc.). Extract a `NiFiClientHelpers` or keep only the ~15 methods the copilot needs. |
| `NiFiConfigResolver.java` | 202 | **KEEP** | Loads config from properties file with env var override. Clean. |
| `NiFiRetryPolicy.java` | 156 | **KEEP** | Retry with jitter, Retry-After parsing. Clean, well-documented. |
| `NiFiAsyncRequestExecutor.java` | 200 | **KEEP** | Generic async poll loop. Well-designed. |
| `NiFiAsyncRequestState.java` | 162 | **KEEP** | Parses async response status. Clean. |
| `NiFiClientException.java` | 174 | **KEEP** | Structured exception. Clean. |
| `NiFiRevision.java` | 76 | **KEEP** | Optimistic concurrency revision. Clean. |
| `NiFiClientMetricsRegistry.java` | 230 | **SIMPLIFY** | JDK Proxy-based instrumentation with per-method metrics. Over-engineered for pre-production. A simple call counter would suffice. |
| `NiFiClientSelectionConfiguration.java` | 53 | **KEEP** | Bean selection: external vs internal. Clean. |
| `CapabilityTypeSelector.java` | 120 | **MERGE** | Deduplicates capability types by version. Only called from `CapabilityRegistry`. Merge as private method. |
| `CapabilityDiscoveryException.java` | ~20 | **KEEP** | Domain exception. The authoritative copy (delete the one in `capability` package). |

**Responsibility:** NiFi REST API client + support infrastructure  
**Actually needed:** YES — this is the NiFi communication layer  
**SRP violation:** `HttpNiFiClient` does too much (every possible NiFi operation)  
**Tight coupling:** Interface is too wide, forcing implementations to implement unused methods  
**Duplicates logic:** Static helpers in `NiFiClientOperations` duplicate logic in `NiFiEntitySupport`  

**Recommended actions:**
- `CapabilityTypeSelector` → merge into `CapabilityRegistry`
- `NiFiClientMetricsRegistry` → simplify to basic counter
- `NiFiClientOperations` → extract focused `CopilotClientApi` interface (~15 methods)

**Net class reduction: 12 → 11 (-1 class)**

---

### Package: `org.apache.nifi.copilot.store`

| Class | Lines | Verdict | Rationale |
|-------|-------|---------|-----------|
| `SessionStore.java` | 92 | **KEEP** | SQLite session persistence. Simple, focused. |

**Responsibility:** Session storage  
**Actually needed:** YES  
**SRP violation:** No  

---

## Cross-Cutting Analysis

---

### 1. Package Structure

**Current:** 8 packages, 111 classes  
**Problems:**
- `builder` has 39 classes (too many for one package, but splitting would add packages)
- `capability` mixes intelligence logic with repair pipeline and data records
- `llm` contains business logic (normalizers) that doesn't belong in a client package

**Recommended:** 5-6 packages, ~70 classes
- Delete `CopilotModule` → root package has only `NiFiCopilotApplication`
- Merge repair classes into `capability` (then delete them)
- Normalizers deleted from `llm`

---

### 2. Dependency Graph

```
api → llm, builder, capability, service, auth, store
builder → service, capability (for validation only)
capability → service (for capability discovery)
llm → (standalone, no inward deps except jackson/http)
service → (standalone)
auth → (standalone)
store → (standalone)
```

**Problem:** `api` depends on everything. This is the God Controller problem.  
**Fix:** Extract orchestration from `api` into `capability` or a new `orchestration` class.

---

### 3. Public APIs

| API | Methods | Issue |
|-----|---------|-------|
| `FlowBuilder` | 5 public methods | 2 `buildFlow` + 2 `prepareFlow` + `readCanvas` — could be 3 |
| `LlmClient` | 9 public methods | 5 overloads of `generateFlowSpec` + 4 of Bedrock — should be 2 (one per provider) |
| `NiFiClientOperations` | ~65 methods | Way too broad for the copilot |
| `CopilotController` | 12 endpoints | Fine for REST, but internal methods are chaotic |

---

### 4. Builder Pattern Usage

`FlowBuilder` is misnamed. It's not a builder pattern — it's a facade over a deployment pipeline. The name is acceptable but technically inaccurate. No actual builder pattern (`withX().withY().build()`) exists in the codebase.

**Verdict:** Naming is fine. No change needed.

---

### 5. Strategy Pattern Usage

| Strategy | Implementation | Verdict |
|----------|---------------|---------|
| `LayoutMode` (ENGINE/DISABLED) | Clean enum-based strategy selection | **KEEP** |
| `NiFiClientSelectionConfiguration` | Auto/external/internal mode selection | **KEEP** |
| `RequestLifecycle<S>` in `NiFiAsyncRequestExecutor` | Template method for async polling | **KEEP** |

Strategy pattern is used appropriately. Not over-applied.

---

### 6. Factory Usage

| Factory | Purpose | Verdict |
|---------|---------|---------|
| `CapabilityGraphBuilder` | Builds `CapabilityGraph` from `CapabilitySnapshot` | **MERGE** — make it `CapabilityGraph.from(snapshot)` |
| `NiFiClientSelectionConfiguration` | Spring `@Bean` factory for client selection | **KEEP** |

Factory usage is minimal and appropriate.

---

### 7. Prompt Generation Pipeline

```
User Message
    → IntentExtractor.extract()           [222 LOC]
    → ProcessorSeedRanker.rank()          [249 LOC]
    → DependencyClosureResolver.resolve() [324 LOC]
    → CapabilityPromptRenderer.render()   [412 LOC]
    → LlmClient.systemPrompt()            [127-line string]
    → LlmClient.buildUserMessage()        [~50 LOC]
```

**Total prompt pipeline: ~1,360 LOC**

**Problems:**
1. `IntentExtractor` uses 222 lines of regex to classify ~20 keywords. A lookup table would be 60 lines.
2. `ProcessorSeedRanker` scores processors with 249 lines of heuristics. If Java just selects processors from a lookup table (intent → processors), this becomes ~40 lines.
3. `DependencyClosureResolver` does transitive BFS with depth limiting. In practice, dependency depth is 1-2. A simple single-level resolution would be ~80 lines.
4. The system prompt at 127 lines contains 18 lines of connection rules that Java enforces anyway.

**Recommended reduction: ~1,360 LOC → ~600 LOC**

---

### 8. Validation Pipeline

```
FlowBuilder.prepareFlow()
    → LocalPreflightValidator (structural checks)
    → FlowSpecificationValidator.validateAndNormalize()
        → Resolves processor types (alias + FQN + simple name)
        → Validates properties against allowable values
        → Validates relationships against capability graph
        → Validates scheduling configuration
        → Validates controller service types
        → Normalizes types to FQN
    → Returns ValidatedFlowPlan
```

**Assessment:** Well-structured. The validator is the source of truth, which is correct.

**Problem:** If Java owns topology, the relationship validation section (~100 lines) becomes internal logic rather than LLM-output validation. Property validation and type resolution (~200 lines) remain necessary.

**Simplification target: 532 → ~250 LOC**

---

### 9. Deployment Pipeline

```
FlowDeploymentCoordinator.deploy()
    → DependencyPreflightStage    (validate CS deps)
    → DeploymentPreparationStage  (resolve target PG, capture snapshot)
    → DependencyDeploymentStage   (create CS + parameter contexts)
    → ComponentDeploymentStage    (create processors/ports/funnels/labels/RPGs)
    → ConnectionConfigurationStage (create connections)
    → LayoutDeploymentStage       (invoke layout engine)
    → RuntimeActivationStage      (start processors, enable services)
```

**Assessment:** This is the best-designed part of the system. Clean stage separation, clear ordering, proper rollback.

**Simplification opportunities:** Minimal. Merge 3 trivial helper classes. Otherwise leave alone.

---

### 10. Capability Discovery

```
CapabilityRegistry.refresh()
    → NiFiClientOperations.listProcessorTypes()
    → NiFiClientOperations.getProcessorDefinition()  (per type)
    → NiFiClientOperations.listControllerServiceTypes()
    → NiFiClientOperations.getControllerServiceDefinition()  (per type)
    → CapabilityDefinitionParser.parseProcessor() / parseControllerService()
    → CapabilityTypeSelector.selectPreferred()
    → CapabilityGraphBuilder.build()
    → Cache with TTL
```

**Assessment:** Sound design. TTL-based caching is appropriate.

**Problem:** `CapabilityTypeSelector` (120 LOC) is only called once during refresh. Inline it.

---

### 11. Capability Rendering

```
CapabilityPromptRenderer.render()
    → intentExtractor.extract()
    → processorSeedRanker.rank()
    → closureResolver.resolve()
    → BoundedContext with appendRequired/appendOptional
    → Tiered output: required → data_contract → optional → runtime
```

**Assessment:** The tiered rendering with bounded context is well-designed. The problem is not how it renders, but how much it renders (12,000 chars) and what it includes (full property specs when only type + required properties are needed).

**Simplification:** Reduce `MAX_CONTEXT_CHARS` from 12,000 to 4,000-6,000. Remove optional/runtime tiers.

---

### 12. Repair Pipeline

```
CopilotController.generateAndPrepare()
    → generateSpecification() [1st LLM call]
    → FlowBuilder.prepareFlow() [validation]
    → ON FAILURE:
        → RepairHintDeriver.derive()
        → RepairContextExpander.expand()
        → generateSpecification() [2nd LLM call with repair context]
        → FlowBuilder.prepareFlow() [re-validation]
```

**Assessment:** This is the most problematic pipeline. It exists because:
1. The LLM invents processor types → validator rejects
2. The LLM gets relationship names wrong → validator rejects
3. The LLM uses wrong property values → validator rejects

**Root cause:** The LLM is asked to make decisions that are deterministic.

**Recommendation:** DELETE the entire repair pipeline. Instead:
- Java selects processors (no hallucination possible)
- Java sets relationships (from capability graph)
- Java wires connections (topology is deterministic)
- LLM only provides property values (from user's natural language)
- Validator only checks property values (much simpler)

**LOC deleted: ~450 (RepairHintDeriver + RepairContextExpander + repair logic in controller)**

---

### 13. HttpNiFiClient

**2,693 lines.** Implements the full NiFi REST API.

**Methods actually used by the copilot (~15):**
- `getProcessGroupId`, `getProcessGroupFlow`
- `createProcessor`, `updateProcessor`, `deleteProcessor`
- `createConnection`, `deleteConnection`, `listConnections`
- `createControllerService`, `enableControllerService`, `disableControllerService`, `deleteControllerService`, `listControllerServices`
- `createParameterContext`, `updateParameterContext`, `deleteParameterContext`, `listParameterContexts`, `bindParameterContext`
- `listProcessorTypes`, `getProcessorDefinition`
- `listControllerServiceTypes`, `getControllerServiceDefinition`
- `scheduleProcessGroup`

**Methods NOT used by copilot (~50):**
- Provenance queries, lineage queries
- FlowFile queue operations (listing, drop, content)
- Registry operations (import/export flow)
- Version control (revert, change version)
- Cluster operations
- System diagnostics
- Reporting tasks
- Various get/update endpoints

**Recommendation:** Don't delete unused methods (may be used by tests or internal client), but extract a `CopilotClientApi` interface with only the ~15 needed methods. Program all copilot code against that narrow interface.

---

### 14. InternalNiFiClient

There is **no InternalNiFiClient class** in this codebase. The `NiFiClientSelectionConfiguration` references an `"internalNiFiClient"` bean by name, which would be provided by a separate NiFi extension JAR when the copilot runs embedded in NiFi.

**Assessment:** The selection mechanism is clean. External mode via `HttpNiFiClient` is the only implementation present. The `auto/internal/external` mode switching is appropriate forward-looking design.

**Verdict:** **KEEP** — this is correctly designed for future embedding.

---

### 15. Controller Service Deployment

```
ControllerServiceDeployer
    → Plans deployment: match existing CS by name+type, plan creates
    → Creates new CS with properties
    → Enables services (with ordering: dependencies first)
    → Registers in ComponentRegistry for processor property references
```

**Assessment:** Well-designed. Handles the complex enable-ordering problem correctly. The `DeploymentPlan` pattern is appropriate.

**Verdict:** **KEEP**

---

### 16. Flow Generation Pipeline (end-to-end)

```
1. CopilotController.chat()
2.   → Read canvas (optional)
3.   → Discover capabilities
4.   → Render capability context
5.   → Generate specification (LLM call)
6.   → Normalize layout (remove x/y, dedup, self-loops, terminal outputs)
7.   → TerminalLoggerNormalizer (split/consolidate loggers)
8.   → ParallelWorkerNormalizer (fix DistributeLoad)
9.   → Validate specification (FlowSpecificationValidator)
10.  → [On failure: repair pipeline → 2nd LLM call → re-validate]
11.  → Build flow (FlowDeploymentCoordinator)
12.  → Handle deletions
13.  → Apply CS actions
14.  → Construct response
```

**Problems with current pipeline:**
- Steps 6-8 (normalization): 978 LOC to fix LLM mistakes
- Step 10 (repair): 450 LOC for a 2nd LLM call
- Step 1 (controller): 818 LOC god class orchestrating everything

**Proposed simplified pipeline:**
```
1. Controller.chat()  [thin REST layer, ~80 lines]
2.   → FlowOrchestrator.generate()  [new, ~150 lines]
3.     → Read canvas
4.     → Discover capabilities  
5.     → IntentExtractor → select processors (Java lookup, not LLM)
6.     → Render minimal context (selected processors' properties only)
7.     → LLM call: "fill property values for these processors"
8.     → Validate property values (simple validation, ~100 lines)
9.     → Java builds connections (deterministic from processor order)
10.    → Java places terminal loggers
11.    → FlowDeploymentCoordinator.deploy()
12.  → Handle deletions
13.  → Construct response
```

**Steps removed:** normalization (6-8), repair (10)  
**Steps simplified:** validation (9 → property-only), prompt rendering (4 → minimal)  

---

## Summary: Classification of All 111 Classes

### DELETE (11 classes, ~2,400 LOC)

| Class | LOC | Reason |
|-------|-----|--------|
| `TerminalLoggerNormalizer` | 851 | Java should own logger placement |
| `ParallelWorkerNormalizer` | 127 | Java should own DistributeLoad wiring |
| `RepairContextExpander` | 319 | Repair pipeline unnecessary with Java-owned topology |
| `RepairHintDeriver` | 131 | Same |
| `RepairHint` | 30 | Same |
| `CopilotModule` | 13 | Unused annotation |
| `CapabilityDiscoveryException` (capability) | 20 | Duplicate |
| `InstantNow` | 10 | Use `Clock` |
| `ProcessGroupFlowMapAssembler` | 100 | Inline into `LayoutDeploymentStage` |
| `ProcessorStarter` | 60 | Inline into `RuntimeActivationStage` |
| `RuntimeStateApplier` | 80 | Inline into `RuntimeActivationStage` |

### MERGE (7 classes → absorbed into existing classes)

| Class | Into | Reason |
|-------|------|--------|
| `ProcessorSeedRanker` | `CapabilityPromptRenderer` | Single consumer |
| `PropertyRanker` | `DependencyClosureResolver` | Single consumer |
| `CapabilityGraphBuilder` | `CapabilityGraph` (static factory) | Single pattern |
| `CapabilityRegistryManager` | `CapabilityRegistry` | Thin wrapper |
| `CapabilityTypeSelector` | `CapabilityRegistry` | Single consumer |
| `DeploymentTarget` | `DeploymentState` (nested record) | Trivial value object |
| `DeploymentReport` | `FlowDeploymentCoordinator` (inline) | 2-field record |
| `RelationshipConfigurer` | `ProcessorDeployer` | Single consumer |

### SIMPLIFY (11 classes)

| Class | Current LOC | Target LOC | Change |
|-------|-------------|-----------|--------|
| `LlmClient` | 472 | ~200 | Remove overloads, unify providers, delete normalization |
| `FlowSpecificationValidator` | 532 | ~250 | Remove topology validation |
| `IntentExtractor` | 222 | ~60 | Replace regex with lookup table |
| `DependencyClosureResolver` | 324 | ~100 | Single-level resolution |
| `CapabilityMetricsRegistry` | 150 | ~40 | Keep 5 counters |
| `NiFiClientMetricsRegistry` | 230 | ~50 | Simple call counter |
| `FlowDeploymentMetricsRegistry` | 200 | ~50 | Simple timing |
| `HttpNiFiClient` | 2,693 | 2,693 | Keep but extract focused interface |
| `NiFiClientOperations` | 1,630 | 1,630 | Keep but extract focused sub-interface |
| `DeploymentContext` | 60 | 40 | Convert to record |
| `ComponentResolver` | 366 | ~250 | Inline single-use methods |

### REDESIGN (2 classes)

| Class | Reason |
|-------|--------|
| `CopilotController` | Extract orchestration into service class. Controller → thin REST layer. |
| `LlmClient` | Unify GitHub/Bedrock into single method with provider strategy. Remove all normalization. Externalize prompt construction. |

### KEEP (80 classes)

Everything else. The deployment pipeline, data records, auth, config, canvas projection, session store, NiFi client infrastructure — all clean and correctly designed.

---

## Final Metrics

| Metric | Current | After Simplification |
|--------|---------|---------------------|
| Total classes (copilot) | 111 | ~85 |
| Total LOC (copilot) | 16,745 | ~12,000 |
| `llm` package classes | 3 | 1 |
| `capability` package classes | 30 | 22 |
| `builder` package classes | 39 | 33 |
| System prompt lines | 127 | ~40 |
| Post-LLM normalization LOC | 978 | 0 |
| Repair pipeline LOC | ~450 | 0 |
| Capability context chars | 12,000 | 4,000-6,000 |

---

## The Single Biggest Problem

**The LLM is asked to make deterministic decisions, then Java spends ~1,400 LOC correcting those decisions.**

The fix is not better prompt engineering. The fix is: **stop asking the LLM questions that have deterministic answers.**

| Decision | Deterministic? | Currently | Should Be |
|----------|---------------|-----------|-----------|
| Processor selection | YES | LLM + Java ranking | Java lookup table |
| Connection topology | YES | LLM + Java normalization (978 LOC) | Java from processor order |
| Relationship names | YES | LLM + Java validation | Java from capability graph |
| Logger placement | YES | LLM + Java correction (851 LOC) | Java heuristic (20 LOC) |
| DistributeLoad wiring | YES | LLM + Java correction (127 LOC) | Java (10 LOC) |
| Controller service type | YES | LLM + Java validation | Java from API requirements |
| Property values | **NO** | LLM | LLM ✓ |
| Processor names | **NO** | LLM | LLM ✓ |
| Explanation text | **NO** | LLM | LLM ✓ |

**Only 3 decisions require the LLM. The other 6 are deterministic.**

---

*End of review.*
