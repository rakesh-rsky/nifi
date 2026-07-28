# Architecture Audit Report — NiFi Copilot
**Date:** 2026-07-26  
**Auditor Role:** Principal Software Engineer  
**Objective:** Simplification while preserving functionality  

---

## 1. Executive Summary

The NiFi Copilot system comprises **203 Java classes** across **2 modules** (~23,325 LOC) to accomplish what is fundamentally a 4-step pipeline:

1. **Understand intent** (user message → structured intent)
2. **Select capabilities** (intent → relevant NiFi processors/services)
3. **Generate spec** (LLM call with capabilities context)
4. **Deploy flow** (validated JSON → NiFi API calls)

The system is **over-abstracted** for its actual decision surface. The LLM is given too much responsibility (connection topology, logger partitioning, parallel worker strategies) that Java then aggressively corrects with **~1,600 lines of post-generation normalization** (`TerminalLoggerNormalizer` + `ParallelWorkerNormalizer` + `normalizeGeneratedLayout`). This indicates the LLM is asked to solve problems Java already knows how to solve.

**Current estimated context window per request:** ~15,000–25,000 tokens (system prompt + capability context + canvas context + history).

**Key metrics:**

| Metric | Current | Target |
|--------|---------|--------|
| Packages (copilot) | 8 | 5 |
| Classes (copilot) | 111 | ~70 |
| System prompt lines | 127 | ~40 |
| Capability context budget | 12,000 chars | 6,000 chars |
| Post-LLM normalization LOC | ~1,600 | 0 |
| LLM calls per validation failure | 2 | 1 |
| Layout engine classes | 92 | 92 (separate module, leave alone) |

---

## 2. High-Level Architecture

```
User Request
    │
    ▼
CopilotController.chat()         ← 818 lines, God controller
    │
    ├─► IntentExtractor           ← regex-based intent parsing
    ├─► ProcessorSeedRanker       ← scores processors against intent
    ├─► DependencyClosureResolver ← resolves service dependencies
    ├─► CapabilityPromptRenderer  ← renders capability context for LLM
    │
    ├─► LlmClient.generateFlowSpec()  ← GitHub Models / Bedrock
    │       │
    │       ├─► SYSTEM_PROMPT (127 lines of rules)
    │       ├─► Canvas context injection
    │       └─► Post-generation normalizers:
    │               ParallelWorkerNormalizer
    │               TerminalLoggerNormalizer (851 lines!)
    │               Self-loop removal, dedup, terminal-output removal
    │
    ├─► FlowSpecificationValidator    ← validates against capabilities
    │       (on failure) → RepairHintDeriver + RepairContextExpander
    │                    → 2nd LLM call with repair context
    │
    └─► FlowBuilder.buildFlow()
            │
            └─► FlowDeploymentCoordinator (6 stages):
                    DeploymentPreparationStage
                    DependencyPreflightStage
                    DependencyDeploymentStage
                    ComponentDeploymentStage
                    ConnectionConfigurationStage
                    LayoutDeploymentStage
                    RuntimeActivationStage
```

---

## 3. Package Responsibility Matrix

| Package | Classes | Responsibility | Concern |
|---------|---------|---------------|---------|
| `api` | 2 | REST controller + DTOs | Controller is 818 lines, doing orchestration + deletion + repair |
| `auth` | 3 | GitHub + AWS authentication | Clean, well-separated |
| `builder` | 37 | Flow deployment pipeline | Well-factored stages, but some dead abstractions |
| `capability` | 30 | Intent→Capability→Validation pipeline | Core of intelligence; complex but necessary |
| `config` | 2 | Spring configuration | Minimal |
| `llm` | 3 | LLM API calls + post-normalization | LlmClient + 2 normalizers that shouldn't exist |
| `service` | 12 | NiFi HTTP client + support | HttpNiFiClient (2,693 LOC) + interface (1,630 LOC) |
| `store` | 1 | SQLite session persistence | Minimal |

**Total: 8 packages, 111 classes**

---

## 4. Duplicate Logic

| Duplication | Location A | Location B | Impact |
|-------------|-----------|-----------|--------|
| `CapabilityDiscoveryException` | `capability` package | `service` package | Identical class in two packages |
| `intValue()` helper | `LlmClient` | `CopilotController` (delegates to `NiFiClientOperations.intValue`) | Scattered utility |
| Terminal logger detection | `LlmClient.isTerminalLogger()` | `TerminalLoggerNormalizer` (SUCCESS_RELS/FAILURE_RELS) | Logic about what's "terminal" duplicated |
| Map/list copy utilities | `SpecificationSupport` | `LlmClient.copyStringMap()` | Two implementations of deep-copy-like logic |
| Processor type resolution | `CapabilityRegistry` (alias resolution) | `FlowSpecificationValidator` (type resolution) | Both resolve FQN from short names |
| Canvas reading/projection | `CanvasProjector` | `CopilotController` (canvas enrichment logic) | Controller duplicates projection logic |

---

## 5. Over-Engineered Components

### 5.1 `TerminalLoggerNormalizer` (851 lines)

**Problem:** The LLM generates incorrect terminal logger topologies, so Java runs Tarjan's SCC algorithm, weakly-connected-component analysis, structural-sibling detection, and multi-phase split/consolidate to fix it.

**Root cause:** The system prompt asks the LLM to decide logger topology. This is a deterministic decision that Java should make.

**Recommendation:** Remove entirely. Java should place terminal loggers based on the connection graph after the LLM generates the core pipeline.

### 5.2 `ParallelWorkerNormalizer` (127 lines)

**Problem:** Corrects DistributeLoad connections post-generation because the LLM gets them wrong.

**Root cause:** Same as above. Parallel distribution topology is deterministic.

**Recommendation:** Remove. Java generates DistributeLoad connections deterministically.

### 5.3 `RepairContextExpander` (319 lines)

**Problem:** Builds an expanded capability context for a second LLM call when validation fails.

**Root cause:** The first LLM call uses incorrect types because the context was insufficient or the LLM hallucinated.

**Recommendation:** With proper Java-side processor selection, this should be unnecessary. If the validator normalizes types (it already does to some extent), the repair loop may be eliminable.

### 5.4 `FlowDeploymentMetricsRegistry` + per-stage timing

**Problem:** Enterprise-grade observability for a pre-production system.

**Recommendation:** Simplify to a single deployment duration metric until actually needed.

### 5.5 `NiFiClientOperations` interface (1,630 lines)

**Problem:** Massive interface with dozens of methods. Much of it (provenance, lineage, registry, clustering) is unused by the copilot.

**Recommendation:** Split into a focused `CopilotNiFiClient` interface with only the ~15 methods actually used.

---

## 6. Under-Engineered Components

### 6.1 `CopilotController.chat()` (god method)

**Problem:** A single 200+ line method handles: canvas reading, capability discovery, generation, repair, validation, deletion (4 types), deployment, CS actions, response construction. No separation of concerns.

**Recommendation:** Extract an orchestration service (`ChatOrchestrator` or `FlowGenerationPipeline`) that composes the stages.

### 6.2 `LlmClient` (monolithic)

**Problem:** Contains system prompt, HTTP calls to two providers, JSON extraction, post-generation normalization (calls both normalizers), and utility methods. No abstraction boundary.

**Recommendation:** Separate prompt construction from HTTP transport from response normalization.

### 6.3 Error handling throughout

**Problem:** Broad `catch (Exception e)` with string concatenation into explanation text. No structured error model.

---

## 7. Prompt Pipeline Analysis

### Current System Prompt Structure (127 lines):

```
OUTPUT SCHEMA (14 lines)         — Necessary
Field descriptions (26 lines)    — Necessary  
CANVAS LAYOUT (2 lines)          — Necessary
CONNECTIONS (18 lines)           — 90% is teaching the LLM topology rules Java can enforce
EXPLANATION RULES (2 lines)      — Minimal
PROCESSOR CONFIG (14 lines)      — Necessary
CANVAS CONTEXT (6 lines)         — Necessary
DELETIONS (4 lines)              — Necessary
```

**The 18-line CONNECTIONS section** teaches the LLM:
- Don't create self-loops → Java already removes these
- Don't connect loggers outward → Java already removes these
- Parallel workers share one logger → TerminalLoggerNormalizer fixes this
- Don't use funnels for logging → TerminalLoggerNormalizer handles this
- DistributeLoad rules → ParallelWorkerNormalizer handles this

**These ~18 lines exist because Java already enforces these rules.** The prompt is teaching the LLM something it doesn't need to know, then Java corrects it anyway.

### Capability Context Budget

- Current max: **12,000 characters** (`MAX_CONTEXT_CHARS`)
- Contains: processor types, properties (ranked), relationships, controller services, API bindings
- The `CapabilityPromptRenderer` uses a tiered approach (required → data_contract → optional → runtime)
- This is well-designed but the budget is too generous for the 3,000-token target

### Estimated Token Breakdown (current):

| Component | Chars | ~Tokens |
|-----------|-------|---------|
| System prompt | ~5,000 | ~1,500 |
| Capability context | ~12,000 | ~3,500 |
| Canvas context (5 processors) | ~500 | ~150 |
| History (last 6 messages) | ~3,000 | ~900 |
| User message | ~200 | ~60 |
| **Total input** | **~20,700** | **~6,100** |
| Output | ~3,000 | ~900 |
| **Grand total** | | **~7,000** |

With repair loop: **~14,000 tokens** per failed request.

---

## 8. Token Consumption Analysis

### Per-Request Token Budget (current estimate):

| Scenario | Input Tokens | Output Tokens | Total |
|----------|-------------|--------------|-------|
| Simple flow (empty canvas) | ~5,000 | ~800 | ~5,800 |
| Flow with canvas context | ~6,100 | ~1,000 | ~7,100 |
| Failed + repair loop | ~12,000 | ~2,000 | ~14,000 |

### Reduction Path to < 3,000 total:

1. **Remove topology rules from prompt** (-500 tokens): Java handles them
2. **Reduce capability context to 4,000 chars** (-2,300 tokens): Only include selected processors + required properties
3. **Remove history** (-900 tokens): For most requests, history adds noise not signal
4. **Slim system prompt to schema + 5 rules** (-800 tokens): Remove all connection rules
5. **Move processor selection to Java** (-1,000 tokens): Don't send all ranked processors, send only the exact ones Java selected

**Achievable target: ~2,500 input tokens** for common cases.

---

## 9. Java vs LLM Responsibility Analysis

### Current: LLM decides too much

| Decision | Current Owner | Should Be | Rationale |
|----------|--------------|-----------|-----------|
| Which processors to use | LLM (guided by capability context) | **Java** (from intent) | Deterministic mapping from intent→processors |
| Which properties to set | LLM | **LLM** | Requires understanding user's specific values |
| Connection topology | LLM (then Java corrects) | **Java** | Topological sort of processors is deterministic |
| Terminal logger placement | LLM (then Java corrects 851 LOC) | **Java** | Always: add LogAttribute after last processor |
| Parallel worker topology | LLM (then Java corrects 127 LOC) | **Java** | DistributeLoad is a known pattern |
| Relationship names | LLM (validated by Java) | **Java** | Known from capability graph |
| Controller service wiring | LLM (validated) | **Java** | API requirements are in the capability graph |
| Property values | LLM | **LLM** | Only the LLM understands user intent for values |
| Processor naming | LLM | **LLM** | Human-meaningful names |
| Explanation text | LLM | **LLM** | Natural language |

### Proposed Split:

**LLM responsibility (intent + config):**
- Parse user intent into: source, sink, transformations, property values
- Generate property values for selected processors
- Generate human-readable names and explanation

**Java responsibility (everything deterministic):**
- Select processors from intent (replace ProcessorSeedRanker → direct mapping)
- Wire connections (topological sort)
- Place terminal loggers
- Resolve controller services (API requirements → implementations)
- Set relationship names (from capability graph)
- Validate and deploy

---

## 10. Components That Should Be Deleted

| Component | Lines | Reason |
|-----------|-------|--------|
| `TerminalLoggerNormalizer` | 851 | Java should own logger placement, not fix LLM mistakes |
| `ParallelWorkerNormalizer` | 127 | Java should own DistributeLoad wiring |
| `RepairContextExpander` | 319 | With Java owning processor selection, repair loop becomes unnecessary |
| `RepairHintDeriver` | 131 | Same as above |
| `CapabilityDiscoveryException` (in `capability` pkg) | ~20 | Duplicate of same class in `service` package |
| `CopilotModule` annotation | 13 | Unused custom annotation; Spring's `@Component` suffices |
| `ProcessGroupFlowMapAssembler` | ~100 | Only used for canvas projection; inline into `CanvasProjector` |
| `FlowDeploymentMetricsRegistry` (90% of it) | ~200 | Pre-production; a simple counter suffices |
| Connection rules in system prompt | 18 lines | Java enforces these rules |
| `normalizeGeneratedLayout` in LlmClient | ~70 lines | If normalizers are deleted, this becomes trivial |

**Estimated deletable: ~1,850 LOC**

---

## 11. Components That Should Be Merged

| Merge Target | Sources | Rationale |
|-------------|---------|-----------|
| `FlowGenerationPipeline` (new) | `CopilotController.chat()` orchestration + `generateAndPrepare()` + repair logic | Extract controller's business logic into a testable service |
| `CapabilityService` | `CapabilityRegistry` + `CapabilityRegistryManager` | Manager is a thin cache wrapper; merge into Registry |
| `NiFiClient` (focused interface) | Subset of `NiFiClientOperations` | Only expose ~15 methods the copilot actually uses |
| `LlmClient` simplification | Prompt building + HTTP transport → keep together but delete normalization | Already a natural unit once normalizers are removed |
| `CapabilityPromptRenderer` | Absorb `ProcessorSeedRanker` + `PropertyRanker` as private methods | These are only used by the renderer; no need for separate classes |

**Estimated reduction: ~5 classes, ~2 packages**

---

## 12. Components That Should Be Simplified

| Component | Current State | Simplified State |
|-----------|--------------|------------------|
| System prompt | 127 lines, teaches topology rules | ~40 lines: schema + "set property values for these processors" |
| `CapabilityPromptRenderer` | Renders full processor specs with tiered properties | Renders only: processor type, required properties, and allowable values |
| `FlowSpecificationValidator` | 532 lines validating LLM topology decisions | ~200 lines validating only property values and types |
| `DependencyClosureResolver` | Complex transitive closure with depth limits | Simple single-level service resolution (most processors need 0-1 services) |
| `IntentExtractor` | 222 lines of regex for 6 endpoint types, 6 formats, 7 transforms | Could be a 60-line keyword matcher or table lookup |
| `DeploymentContext` | 7-field value object | Reduce to 4 fields once topology decisions move to Java |
| `HttpNiFiClient` | 2,693 lines implementing full NiFi API | Extract `CopilotNiFiClient` (~600 lines) with only used operations |

---

## 13. Components That Should Be Rewritten

| Component | Reason | New Design |
|-----------|--------|-----------|
| `LlmClient.generateFlowSpec*` | 5 overloads, duplicated GitHub/Bedrock logic | Single method with provider strategy; prompt built externally |
| `CopilotController.chat()` | God method orchestrating everything | Pipeline: `Intent → Selection → Generation → Validation → Deployment` |
| Connection generation | Currently: LLM generates, Java corrects | New: Java generates connections from processor adjacency list that LLM returns |

---

## 14. Risk Analysis

| Risk | Severity | Mitigation |
|------|----------|------------|
| Removing normalizers breaks edge cases | Medium | The normalizers fix LLM mistakes that won't happen if LLM doesn't generate topology |
| Reducing prompt loses LLM accuracy | High | Must validate with test suite before/after; keep integration tests for known flows |
| Merging CapabilityRegistryManager breaks multi-tenant | Low | Currently single-tenant; IdentityHashMap logic is unused complexity |
| Simplifying validator misses real errors | Medium | Validator test suite has 36 tests; maintain coverage |
| Removing repair loop reduces success rate | High | Only if Java-side processor selection is reliable; measure first-pass success before removing |
| Breaking backward compatibility | Medium | `ChatRequest`/`ChatResponse` DTOs must stay stable; internal refactoring doesn't affect API |

### Dependency Risk:

- `nifi-layout-engine` is consumed by `LayoutDeploymentStage` only. Safe to refactor independently.
- `HttpNiFiClient` is the sole `NiFiClientOperations` implementation. Extract-interface refactoring is safe.

---

## 15. Ordered Refactoring Roadmap

### Phase 1: Delete Dead Weight (Low Risk, High Impact)
**Estimated effort: 2-3 days**

1. Delete `TerminalLoggerNormalizer` (851 LOC)
2. Delete `ParallelWorkerNormalizer` (127 LOC)
3. Simplify `LlmClient.normalizeGeneratedLayout()` to only: strip x/y, deduplicate connections, remove self-loops
4. Remove the 18 connection-rule lines from `SYSTEM_PROMPT`
5. Delete duplicate `CapabilityDiscoveryException` from `capability` package
6. Delete `CopilotModule` annotation

**Prerequisite:** Add Java-side connection generation (Phase 2 step makes this work)

### Phase 2: Move Topology to Java (Medium Risk, Highest Impact)
**Estimated effort: 3-5 days**

1. Change LLM output schema: LLM returns `processors[]` with `properties` only (no `connections[]`)
2. Add `ConnectionBuilder.java` (~100 lines): generates connections from processor ordering + relationship graph
3. Add terminal logger placement in Java: append LogAttribute for last processor's success/failure
4. System prompt shrinks to ~40 lines (schema + property rules only)
5. Capability context budget drops to 4,000 chars (only need property definitions now)

**Expected result:** First-pass success rate increases (less for LLM to get wrong), tokens drop to ~2,500.

### Phase 3: Extract Orchestration (Medium Risk, Medium Impact)
**Estimated effort: 2-3 days**

1. Extract `ChatOrchestrator` from `CopilotController.chat()` (~200 lines → service)
2. Move deletion logic to `DeletionService`
3. Controller becomes thin REST layer (~100 lines)
4. Test orchestration independently of HTTP

### Phase 4: Simplify Capability Pipeline (Low Risk, Medium Impact)
**Estimated effort: 2-3 days**

1. Merge `ProcessorSeedRanker` + `PropertyRanker` into `CapabilityPromptRenderer` as private methods
2. Merge `CapabilityRegistryManager` into `CapabilityRegistry`
3. Simplify `DependencyClosureResolver` to single-level resolution
4. Delete `RepairContextExpander` + `RepairHintDeriver` (repair loop likely unnecessary after Phase 2)
5. Reduce `MAX_CONTEXT_CHARS` from 12,000 to 4,000

### Phase 5: Slim NiFi Client (Low Risk, Low-Medium Impact)
**Estimated effort: 1-2 days**

1. Extract `CopilotNiFiClient` interface with only the ~15 methods actually used
2. `HttpNiFiClient` still implements full interface for backward compat
3. All copilot code programs against the focused interface

### Phase 6: Validate and Measure (Required after each phase)

- Run full test suite (36 tests)
- Measure token usage on 10 representative requests
- Measure first-pass validation success rate
- Compare generated flows to baseline

---

## Summary Metrics After Full Refactoring

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Packages (copilot) | 8 | 5 | -37% |
| Classes (copilot) | 111 | ~70 | -37% |
| LOC (copilot) | 16,745 | ~11,000 | -34% |
| System prompt | 127 lines | ~40 lines | -69% |
| Capability context | 12,000 chars | 4,000 chars | -67% |
| Input tokens (typical) | ~6,000 | ~2,500 | -58% |
| LLM calls per request | 1-2 | 1 | -50% |
| Post-generation normalizer LOC | 1,600 | ~50 | -97% |

---

*End of report.*
