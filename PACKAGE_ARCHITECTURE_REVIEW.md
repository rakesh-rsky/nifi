# Package Architecture Review — NiFi Copilot
**Date:** 2026-07-26  
**Objective:** Reduce package count without reducing functionality  

---

## 1. Current Package Inventory

### nifi-copilot (8 packages + root, 94 classes in packages)

| # | Package | Classes | LOC (est.) |
|---|---------|---------|-----------|
| 1 | `(root)` | 2 | 25 |
| 2 | `api` | 2 | 900 |
| 3 | `auth` | 3 | 510 |
| 4 | `builder` | 39 | 5,500 |
| 5 | `capability` | 30 | 4,200 |
| 6 | `config` | 2 | 75 |
| 7 | `llm` | 3 | 1,450 |
| 8 | `service` | 12 | 5,900 |
| 9 | `store` | 1 | 92 |

### nifi-layout-engine (17 packages + root, 92 classes)

| # | Package | Classes |
|---|---------|---------|
| 1 | `(root)` | 1 |
| 2 | `core` | 1 |
| 3 | `core.algorithm` | 5 |
| 4 | `core.collision` | 5 |
| 5 | `core.exception` | 8 |
| 6 | `core.graph` | 5 |
| 7 | `core.layout` | 8 |
| 8 | `core.model` | 20 |
| 9 | `core.router` | 4 |
| 10 | `core.service` | 5 |
| 11 | `core.service.stage` | 8 |
| 12 | `core.spacing` | 7 |
| 13 | `core.util` | 3 |
| 14 | `support` | 2 |
| 15 | `support.canonical` | 7 |
| 16 | `support.writer` | 3 |
| 17 | `rest` | 2 |
| 18 | `copilot` | 1 |

**Total: 26 packages across both modules.**

---

## 2. Package Dependency Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    nifi-copilot                          │
│                                                         │
│  ┌─────────────────────────────────────────────┐        │
│  │                   api                        │        │
│  │         (CopilotController, Dto)             │        │
│  └──┬──────┬──────┬──────┬──────┬──────┬───────┘        │
│     │      │      │      │      │      │                │
│     ▼      ▼      ▼      ▼      ▼      ▼                │
│  ┌─────┐┌─────┐┌──────┐┌────┐┌───────┐┌─────┐          │
│  │auth ││ llm ││build-││capa││service││store│          │
│  │     ││     ││er    ││bili││       ││     │          │
│  │     ││     ││      ││ty  ││       ││     │          │
│  └─────┘└─────┘└──┬───┘└─┬──┘└───────┘└─────┘          │
│                    │      │       ▲                      │
│                    │      ├───────┘                      │
│                    │      │                              │
│                    └──────┼─── → service                 │
│                           └─── → service                 │
│                                                         │
│  ┌──────┐                                               │
│  │config│  (standalone — no copilot deps)               │
│  └──────┘                                               │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│              nifi-layout-engine                          │
│                                                         │
│  ┌──────────────────────────────────────────┐           │
│  │   core (algorithm, model, layout, etc.)  │           │
│  └──────────────────────┬───────────────────┘           │
│                         ▼                               │
│  ┌──────────────────────────────────────────┐           │
│  │   support (canonical, writer)            │           │
│  └──────────────────────┬───────────────────┘           │
│                         ▼                               │
│  ┌─────────┐    ┌───────────┐                           │
│  │  rest   │    │  copilot  │                           │
│  └─────────┘    └───────────┘                           │
└─────────────────────────────────────────────────────────┘

Cross-module dependency:
  nifi-copilot.builder  ──→  nifi-layout-engine.support
  nifi-copilot.builder  ──→  nifi-layout-engine.core.model
  nifi-copilot.builder  ──→  nifi-layout-engine.copilot
```

### Simplified Dependency Flow (packages only):

```
api ──→ auth         (no further deps)
api ──→ llm          (no further deps)  
api ──→ store        (no further deps)
api ──→ builder ──→ service
                 ──→ capability ──→ service
api ──→ capability ──→ service
api ──→ service      (leaf)
config               (standalone)
```

---

## 3. Circular Dependency Analysis

### Result: **NO circular dependencies exist.**

The dependency graph is strictly acyclic:

```
Topological order (leaf → root):
  1. auth         (depends on: nothing)
  2. config       (depends on: nothing)
  3. store        (depends on: nothing)
  4. llm          (depends on: nothing)
  5. service      (depends on: nothing)
  6. capability   (depends on: service)
  7. builder      (depends on: service, capability)
  8. api          (depends on: auth, llm, builder, capability, service, store)
```

**No cycles.** The architecture is properly layered. However, `api` depends on 6 of the 8 packages — a sign that the controller is doing too much.

---

## 4. Package-by-Package Review

---

### Package 1: `(root)` — 2 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | Spring Boot application entry point + `CopilotModule` annotation |
| **Should it exist?** | The root package should hold only `NiFiCopilotApplication`. |
| **Violating SRP?** | No |
| **Should merge?** | No — it's the application root |
| **Should disappear?** | No — but `CopilotModule` should be deleted (unused) |

**Verdict:** Keep root. Delete `CopilotModule`.

---

### Package 2: `api` — 2 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | REST controller + DTOs. But ACTUALLY: orchestration, deletion, repair pipeline, response construction, authentication dispatch. |
| **Should it exist?** | YES — but only as a thin REST layer |
| **Violating SRP?** | **YES — catastrophically.** `CopilotController` (818 LOC) does REST mapping + business orchestration + deletion logic + repair pipeline + response construction. This is 5 responsibilities in one class. |
| **Should merge?** | No — REST layer should remain separate |
| **Should disappear?** | No |
| **Tight coupling?** | Depends on 6 packages. Imports 18 distinct classes from other packages. |

**Over-engineered abstractions:** None (it's under-engineered — a god class).

**Recommendation:** Extract orchestration logic into a new service class (could live in `capability` or `builder`). Controller becomes ~80 lines of REST mapping.

---

### Package 3: `auth` — 3 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | GitHub device flow + token management. AWS SSO device flow + role selection + credential management. |
| **Should it exist?** | **QUESTIONABLE.** 3 classes for authentication is fine, but does this need its own package? It's only consumed by `api`. |
| **Violating SRP?** | No. Each class has one job. |
| **Should merge?** | **YES — merge into `service`** or leave as-is. It's borderline. 3 classes don't justify a package when they're consumed from exactly one place. |
| **Should disappear?** | No — the classes are needed |
| **Tight coupling?** | No — it's a leaf package with no copilot dependencies |

**Over-engineered abstractions:** `InstantNow` is a test seam for `Instant.now()`. Standard Java uses `Clock` injection.

**Recommendation:** Either merge into `service` (since auth is a service concern) OR keep as-is (it's clean). Delete `InstantNow`.

---

### Package 4: `builder` — 39 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | The entire deployment pipeline: preparation → dependency deployment → component creation → connection wiring → layout → runtime activation. Plus: rollback, canvas projection, component resolution. |
| **Should it exist?** | **YES** — deployment is a major subsystem |
| **Violating SRP?** | **At the package level: borderline.** The package handles deployment + canvas reading + metrics. At the class level: individual classes are well-factored. |
| **Should merge?** | No — 39 classes is too large to merge elsewhere |
| **Should disappear?** | No |
| **Tight coupling?** | Low. Clean stage interfaces. Only depends on `service` and `capability`. |

**Over-engineered abstractions:**
- `FlowDeploymentMetricsRegistry` — 200 LOC of per-stage timing metrics for a pre-production system
- `DeploymentTarget` — 6-field value object that could be a nested record
- `DeploymentReport` — 2-field record that could be inlined
- `ProcessGroupFlowMapAssembler` — adapts flow data for layout engine; called only from `LayoutDeploymentStage`

**Recommendation:** Keep as one package but trim 6 classes via merging/inlining (see class-level review). Net: 39 → 33.

---

### Package 5: `capability` — 30 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | THREE responsibilities: (1) Capability discovery + caching (2) Intent extraction + processor selection + prompt rendering (3) Validation + repair pipeline |
| **Should it exist?** | **YES** — but it's doing too many things |
| **Violating SRP?** | **YES.** This package combines: intelligence pipeline (intent → prompt), validation (spec → validated plan), and repair (failed spec → 2nd LLM call). These are 3 distinct phases. |
| **Should merge?** | No — it's the right size. But the repair pipeline should be deleted, not moved. |
| **Should disappear?** | No |
| **Tight coupling?** | Internal: `RepairContextExpander` → `CapabilityPromptRenderer` → `ProcessorSeedRanker` → `DependencyClosureResolver` → `PropertyRanker`. External: only depends on `service`. |

**Over-engineered abstractions:**
- `ProcessorSeedRanker` (249 LOC) — elaborate scoring heuristics (multipliers, bonuses, penalties) when a simple keyword→processor lookup table would suffice
- `DependencyClosureResolver` (324 LOC) — BFS with depth limiting for transitive service dependencies; in practice depth is almost always 1
- `RepairContextExpander` (319 LOC) — builds 16,000-char expanded context for repair call; the entire repair concept is over-engineered
- `RepairHintDeriver` (131 LOC) — derives hints from validation failures; same
- `CapabilityRegistryManager` (60 LOC) — per-client caching wrapper over `CapabilityRegistry`; there's only one client in practice
- `CapabilityMetricsRegistry` (150 LOC) — 38 counters for pre-production

**Recommendation:** Delete 4 classes (repair pipeline + duplicate exception), merge 4 classes into consumers. Net: 30 → 22.

---

### Package 6: `config` — 2 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | CORS configuration + Spring Condition for client mode detection |
| **Should it exist?** | **NO — 2 classes do not justify a package.** |
| **Violating SRP?** | No |
| **Should merge?** | **YES — merge into root package** or into `service` (since it configures client selection). |
| **Should disappear?** | **YES as a separate package** |
| **Tight coupling?** | No — standalone |

**Over-engineered abstractions:** None. Both classes are clean and minimal.

**Recommendation:** Move `CorsConfig` and `ExternalNiFiClientCondition` to the root package (alongside `NiFiCopilotApplication`). Delete the `config` package.

---

### Package 7: `llm` — 3 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | LLM API communication + post-generation normalization (terminal logger correction, parallel worker correction) |
| **Should it exist?** | **QUESTIONABLE.** After deleting normalizers, this package has 1 class. |
| **Violating SRP?** | **YES.** `LlmClient` does: prompt construction, HTTP transport (2 providers), JSON extraction, self-loop removal, terminal logger normalization, parallel worker normalization. |
| **Should merge?** | **YES — after normalizer deletion, merge `LlmClient` into `service`** (it's an API client, same as `HttpNiFiClient`) |
| **Should disappear?** | **YES as a separate package** |
| **Tight coupling?** | Internal: `LlmClient` → `TerminalLoggerNormalizer` → `ParallelWorkerNormalizer`. External: no copilot deps. |

**Over-engineered abstractions:**
- `TerminalLoggerNormalizer` (851 LOC) — Tarjan's SCC algorithm + weakly-connected-component analysis + structural sibling detection + multi-phase split/consolidate. All to fix a problem that shouldn't exist.
- `ParallelWorkerNormalizer` (127 LOC) — Same category. Fixes LLM mistakes about DistributeLoad.
- `LlmClient` has 5 overloads of `generateFlowSpec` and 4 overloads of `generateFlowSpecBedrock` — telescope pattern that should be 2 methods total.

**Recommendation:** Delete both normalizers. Move simplified `LlmClient` into `service`. Package disappears.

---

### Package 8: `service` — 12 classes

| Question | Answer |
|----------|--------|
| **Responsibility** | NiFi REST API client + retry policy + async executor + configuration + metrics + exception model |
| **Should it exist?** | **YES** — NiFi communication infrastructure |
| **Violating SRP?** | **Borderline.** The package combines HTTP client, config resolution, retry logic, async execution, metrics, and utility classes. These are all "NiFi communication infrastructure" though. |
| **Should merge?** | No — it's the right size (12 classes) |
| **Should disappear?** | No |
| **Tight coupling?** | Leaf package — nothing depends on it except `capability`, `builder`, and `api`. |

**Over-engineered abstractions:**
- `NiFiClientMetricsRegistry` (230 LOC) — JDK Proxy-based instrumentation with per-method outcome classification. Overkill for pre-production.
- `NiFiClientOperations` (1,630 LOC) — 65-method interface. Most methods unused by copilot.

**Recommendation:** Keep. Simplify metrics. Eventually extract a focused `CopilotClientApi` sub-interface.

---

### Package 9: `store` — 1 class

| Question | Answer |
|----------|--------|
| **Responsibility** | SQLite session persistence (92 LOC) |
| **Should it exist?** | **NO — 1 class does not justify a package.** |
| **Violating SRP?** | No |
| **Should merge?** | **YES — merge into `service`** (it's a persistence service) |
| **Should disappear?** | **YES as a separate package** |
| **Tight coupling?** | Consumed only by `api` |

**Recommendation:** Move `SessionStore` into `service`. Delete the `store` package.

---

## 5. Layout Engine Package Review (Summary)

The layout engine has **17 packages for 92 classes**. This is classic academic over-packaging:

| Issue | Example |
|-------|---------|
| `core.exception` — 8 exceptions in their own package | Exceptions should live beside the classes that throw them |
| `core.model` — 20 model classes in own package | Models belong with the code that uses them |
| `core.service.stage` — 8 stages in sub-package | Stages belong in `core.service` |
| `core.util` — 3 utilities in own package | Inline into their consumers |
| `copilot` — 1 class | Adapter for copilot integration |
| `rest` — 2 classes | REST DTO mapping |

**Ideal structure for layout engine: 5-6 packages**

| Package | Contains |
|---------|----------|
| `core` | Model + algorithm + layout + graph + spacing + collision + router + exceptions + utils |
| `service` | Pipeline + stages |
| `support` | Canonical model + writer + flow adapter |
| `rest` | REST DTOs |
| `copilot` | Copilot integration adapter |

**Reduction: 17 → 5 packages.** But this is a separate module and lower priority.

---

## 6. Identified Over-Engineered Abstractions

### Tier 1: Should Be Deleted Entirely

| Abstraction | Package | LOC | Problem |
|------------|---------|-----|---------|
| `TerminalLoggerNormalizer` | llm | 851 | Tarjan's algorithm to fix LLM logger mistakes. If Java owns topology, this is dead code. |
| `ParallelWorkerNormalizer` | llm | 127 | Same problem — fixes LLM DistributeLoad mistakes. |
| `RepairContextExpander` | capability | 319 | Builds 16K-char expanded context for a 2nd LLM call. The entire repair concept is over-engineered. |
| `RepairHintDeriver` | capability | 131 | Derives structured repair hints. Same. |
| Repair pipeline in controller | api | ~60 | `generateAndPrepare()` orchestrates the repair loop. |

### Tier 2: Over-Abstracted for What They Do

| Abstraction | Package | LOC | Simpler Alternative |
|------------|---------|-----|-------------------|
| `ProcessorSeedRanker` | capability | 249 | Lookup table: `intent → processor[]` (40 LOC) |
| `DependencyClosureResolver` | capability | 324 | Single-level resolution (80 LOC). Depth is almost always 1. |
| `NiFiClientMetricsRegistry` | service | 230 | Simple call counter (30 LOC) |
| `FlowDeploymentMetricsRegistry` | builder | 200 | Simple timing (30 LOC) |
| `CapabilityMetricsRegistry` | capability | 150 | 5 counters (20 LOC) |
| `CapabilityRegistryManager` | capability | 60 | Merge into `CapabilityRegistry` (single-tenant) |
| `LlmClient` 9 method overloads | llm | ~100 | 2 methods (one per provider) |

### Tier 3: Unnecessary Indirection

| Abstraction | Package | Problem |
|------------|---------|---------|
| `CopilotModule` annotation | root | Custom Spring marker with no conditional behavior. `@Component` suffices. |
| `InstantNow` | auth | Wraps `Instant.now()`. Use `Clock` injection. |
| `ProcessGroupFlowMapAssembler` | builder | Called only from `LayoutDeploymentStage`. Inline it. |
| `DeploymentReport` record | builder | 2 fields. Inline return. |
| `DeploymentTarget` record | builder | Used only by `DeploymentState`. Nest it. |

---

## 7. Proposed Package Structure

### Current: 8 packages + root = 9

### Proposed: 5 packages + root = 6

| New Package | Contents | From |
|-------------|----------|------|
| `(root)` | `NiFiCopilotApplication` + `CorsConfig` + `ExternalNiFiClientCondition` | root + config |
| `api` | `CopilotController` (thin) + `Dto` | api (slimmed) |
| `pipeline` | `FlowOrchestrator` + `LlmClient` + `IntentExtractor` + `CapabilityPromptRenderer` + `FlowSpecificationValidator` + `CapabilityRegistry` + all capability records | capability + llm (merged) |
| `builder` | All 33 deployment classes (after merges) | builder (unchanged) |
| `nifi` | `HttpNiFiClient` + `NiFiClientOperations` + `NiFiConfigResolver` + `NiFiRetryPolicy` + `NiFiAsyncRequestExecutor` + `NiFiAsyncRequestState` + `NiFiClientException` + `NiFiRevision` + `SessionStore` + auth classes | service + store + auth (merged) |

### Dependency Diagram (Proposed):

```
┌──────────┐
│   api    │ (thin REST layer, ~80 LOC)
└────┬─────┘
     │
     ▼
┌──────────┐
│ pipeline │ (orchestration + intelligence + LLM)
└────┬─────┘
     │
     ├────────────────────┐
     ▼                    ▼
┌──────────┐        ┌──────────┐
│ builder  │        │   nifi   │ (NiFi client + auth + session)
└────┬─────┘        └──────────┘
     │                    ▲
     └────────────────────┘
```

**Properties of new structure:**
- 4 dependencies (down from 9)
- No circular dependencies
- Each package has a clear single responsibility
- `api`: REST mapping
- `pipeline`: Intent → LLM → Validated plan (the "brain")
- `builder`: Validated plan → NiFi canvas (the "hands")
- `nifi`: NiFi communication infrastructure (the "transport")

---

## 8. Package Reduction Summary

### nifi-copilot

| Action | Packages Removed |
|--------|-----------------|
| Merge `config` into root | -1 |
| Merge `store` into `service` (→ `nifi`) | -1 |
| Merge `llm` into new `pipeline` (was `capability`) | -1 |
| Merge `auth` into `nifi` (was `service`) | -1 |
| Rename `capability` → `pipeline` | 0 |
| Rename `service` → `nifi` | 0 |
| **Total removed** | **-4** |

**Result: 9 → 5 packages (44% reduction)**

### nifi-layout-engine (optional, separate effort)

| Action | Packages Removed |
|--------|-----------------|
| Flatten `core.*` sub-packages into `core` | -10 |
| Merge `core.service.stage` into `core.service` | -1 |
| Merge `core.util` into `core` | -1 |
| **Total removed** | **-12** |

**Result: 17 → 5 packages (71% reduction)**

### Combined

| Module | Before | After | Reduction |
|--------|--------|-------|-----------|
| nifi-copilot | 9 | 5 | **-44%** |
| nifi-layout-engine | 17 | 5 | **-71%** |
| **Total** | **26** | **10** | **-62%** |

---

## 9. Why Each Deleted/Merged Package Should Go

### `config` → merge into root

**Why it exists:** Convention. Spring projects typically have a `config` package.  
**Why it should go:** 2 classes. Both are Spring `@Configuration` beans. They're application-level config that belongs beside `NiFiCopilotApplication`. No other package imports from `config`. Zero information gained by having it separate.

### `store` → merge into `nifi`

**Why it exists:** Separation of "persistence" from "service".  
**Why it should go:** 1 class (92 LOC). `SessionStore` is a data access object — it belongs with other infrastructure services. Its only consumer is `CopilotController`. Having a package for 1 class is organizational waste.

### `llm` → merge into `pipeline`

**Why it exists:** Separation of "LLM communication" from "capability logic".  
**Why it should go:** After deleting normalizers, it's 1 class. `LlmClient` is a step in the flow generation pipeline. It's called exclusively by the orchestration logic. Having a 1-class package adds a navigation hop for zero benefit.

### `auth` → merge into `nifi`

**Why it exists:** Separation of "authentication" from "NiFi client".  
**Why it should go:** Auth managers are client infrastructure — they produce credentials consumed by `LlmClient` and `HttpNiFiClient`. Conceptually they're part of "external communication infrastructure" alongside the NiFi client. Only `CopilotController` consumes them. 3 classes don't justify a package when their single consumer could import them from the infrastructure package.

---

## 10. Final Assessment

### The project has 26 packages for 203 classes.

**That's 7.8 classes per package on average.** With 4 packages having ≤ 3 classes, the packaging is fragmented.

**Industry guideline:** A package should have 5-20 classes representing a cohesive concept. Packages with 1-3 classes indicate over-separation.

### Packages violating the cohesion guideline:

| Package | Classes | Verdict |
|---------|---------|---------|
| `config` | 2 | ❌ Too small — merge |
| `store` | 1 | ❌ Too small — merge |
| `llm` | 3 (1 after cleanup) | ❌ Too small — merge |
| `auth` | 3 | ⚠️ Borderline — merge or keep |
| `api` | 2 | ⚠️ Borderline — but REST layer is conventionally its own package |
| `root` | 2 | ✅ Application entry point — always separate |
| `builder` | 39 | ⚠️ Borderline large — but splitting adds packages |
| `capability` | 30 | ✅ Right size |
| `service` | 12 | ✅ Right size |

### After consolidation:

| Package | Classes | Verdict |
|---------|---------|---------|
| `root` | 5 | ✅ |
| `api` | 2 | ✅ (REST convention) |
| `pipeline` | ~25 | ✅ |
| `builder` | ~33 | ✅ |
| `nifi` | ~18 | ✅ |

**Average: 16.6 classes per package. All within the 5-20 guideline.**

---

*End of review.*
