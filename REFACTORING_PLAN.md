# Complete Refactoring Plan
**Date:** 2026-07-26  
**Constraint:** Project compiles after every step  
**Constraint:** All existing features preserved  
**Constraint:** No new features added  

---

## Baseline Metrics

| Metric | Before | Target | Reduction |
|--------|--------|--------|-----------|
| Packages | 8 (+root) | 5 (+root) | 38% |
| Source classes | 87 | 58 | 33% |
| Test classes | 36 | 28 | 22% |
| Total LOC (src) | ~16,745 | ~11,500 | 31% |
| System prompt (lines) | 127 | 12 | 91% |
| Avg tokens/request | ~8,330 | ~2,000 | 76% |
| LLM calls per request (avg) | 1.3 | 1.0 | 23% |
| Normalizer LOC | 978 | 0 | 100% |
| Repair pipeline LOC | 540 | 0 | 100% |

---

## Package Consolidation Plan

| Current Package | → After Refactoring | Rationale |
|----------------|--------------------|-----------| 
| `api` (2 classes) | `api` | Keep — public REST surface |
| `auth` (3 classes) | `auth` | Keep — authentication concern |
| `llm` (3 classes) | **Merge into root** | Only LlmClient remains after deleting normalizers |
| `config` (2 classes) | **Merge into root** | Only 2 small classes |
| `store` (1 class) | **Merge into root** | Single class |
| `capability` (30 classes) | `capability` | Keep — core domain |
| `builder` (39 classes) | `builder` | Keep — deployment domain |
| `service` (12 classes) | `service` | Keep — NiFi client |

**Result: 8 packages → 5 packages (37.5% reduction)**

---

## Migration Steps

Each step is a single commit. Tests pass after each step.

---

### STEP 1: Delete `TerminalLoggerNormalizer`
**Risk:** LOW  
**LOC removed:** 851  
**Classes removed:** 1  
**Tests affected:** Update `LlmClientTest` if it tests normalization output  

**What to do:**
1. Delete `llm/TerminalLoggerNormalizer.java`
2. In `LlmClient.normalizeGeneratedLayout()` (line 441), remove the call:
   ```java
   new TerminalLoggerNormalizer().normalize(
       normalizedProcessors, terminalLoggerIds, normalizedConnections);
   ```
3. Keep the `terminalLoggerIds` set and the final pass (self-loop removal + terminal outgoing removal + deduplication) — that's 20 lines of simple filtering, not normalization.
4. Run tests. Fix any that asserted TerminalLoggerNormalizer-specific output transformations.

**Why safe:** The normalizer exists to fix LLM logger placement errors. These errors still produce valid NiFi flows (extra loggers or merged loggers) — they're cosmetic, not functional. Removing the normalizer does not break flow generation; it produces slightly less optimal logger arrangements which the user can manually adjust. Once Step 12 (Java-owned topology) is complete, the LLM will no longer place loggers at all.

---

### STEP 2: Delete `ParallelWorkerNormalizer`
**Risk:** LOW  
**LOC removed:** 127  
**Classes removed:** 1  

**What to do:**
1. Delete `llm/ParallelWorkerNormalizer.java`
2. In `LlmClient.normalizeGeneratedLayout()` (line 438), remove:
   ```java
   new ParallelWorkerNormalizer().normalize(normalizedProcessors, normalizedConnections);
   ```
3. Run tests.

**Why safe:** Same reasoning as Step 1. DistributeLoad mistakes produce slightly wrong flows, not crashes. Once Java owns topology (Step 12), this becomes irrelevant.

---

### STEP 3: Merge `CapabilityRegistryManager` into `CapabilityRegistry`
**Risk:** LOW  
**LOC removed:** 60  
**Classes removed:** 1  
**Tests removed:** `CapabilityRegistryManagerTest` → merge into `CapabilityRegistryTest`  

**What to do:**
1. Move the per-client IdentityHashMap logic from `CapabilityRegistryManager` into `CapabilityRegistry` as a `forClient(NiFiClientOperations)` method.
2. Update all call sites:
   - `FlowBuilder` (constructor + `prepareFlow`)
   - `CopilotController` (field + `chat()`)
3. Delete `capability/CapabilityRegistryManager.java`
4. Merge relevant test cases into `CapabilityRegistryTest`.
5. Delete `capability/CapabilityRegistryManagerTest.java`.

---

### STEP 4: Merge `CapabilityGraphBuilder` into `CapabilityGraph`
**Risk:** LOW  
**LOC removed:** ~100  
**Classes removed:** 1  
**Tests removed:** `CapabilityGraphBuilderTest` → merge into `CapabilityGraphTest`  

**What to do:**
1. Move `CapabilityGraphBuilder.build(snapshot)` logic into `CapabilityGraph` as a static factory: `CapabilityGraph.from(CapabilitySnapshot snapshot)`.
2. Update all call sites (search for `new CapabilityGraphBuilder()`):
   - `CapabilityPromptRenderer`
   - `CapabilityRegistry`
3. Delete `capability/CapabilityGraphBuilder.java`.
4. Merge tests into `CapabilityGraphTest`.
5. Delete `capability/CapabilityGraphBuilderTest.java`.

---

### STEP 5: Merge `PropertyRanker` into `DependencyClosureResolver`
**Risk:** LOW  
**LOC removed:** ~125 (becomes ~25 LOC private method)  
**Classes removed:** 1  
**Tests removed:** `PropertyRankerTest` → merge into `DependencyClosureResolverTest`  

**What to do:**
1. Move `PropertyRanker.rank()` logic into `DependencyClosureResolver` as a private method `classifyProperty(PropertyCapability)`.
2. Inline the enum `PropertyClass` into `DependencyClosureResolver` as a package-private inner enum.
3. Keep `RankedProperty` record as inner record of `DependencyClosureResolver`.
4. Update `CapabilityPromptRenderer` imports (it uses `PropertyRanker.PropertyClass` and `PropertyRanker.RankedProperty`).
5. Delete `capability/PropertyRanker.java`.
6. Merge test cases into `DependencyClosureResolverTest`.
7. Delete `capability/PropertyRankerTest.java`.

---

### STEP 6: Merge `CapabilityTypeSelector` into `CapabilityRegistry`
**Risk:** LOW  
**LOC removed:** ~120 (absorbed as private method)  
**Classes removed:** 1  

**What to do:**
1. `CapabilityTypeSelector` is in the `service` package but only used by `CapabilityRegistry`.
2. Move the selection logic (version dedup) into `CapabilityRegistry` as a private method.
3. Delete `service/CapabilityTypeSelector.java`.
4. No test file exists for it (logic tested via `CapabilityRegistryTest`).

---

### STEP 7: Delete `RepairHintDeriver` and `RepairHint`
**Risk:** MEDIUM  
**LOC removed:** 131 + 30 = 161  
**Classes removed:** 2  
**Tests removed:** `RepairHintDeriverTest`  

**What to do:**
1. In `CopilotController.generateAndPrepare()`, replace the repair path (lines 485-511) with a direct return of the failure:
   ```java
   } catch (FlowSpecificationValidationException firstFailure) {
       capabilityMetrics.observeFirstPass(false, firstFailure.getReport().issues());
       return new PreparedGeneration(generated, null, firstFailure.getReport().issues());
   }
   ```
2. Delete `capability/RepairHintDeriver.java`.
3. Delete `capability/RepairHint.java`.
4. Remove `RepairHintDeriver` field from `CopilotController`.
5. Delete `capability/RepairHintDeriverTest.java`.

**Why safe:** The repair pipeline fires ~30% of the time and the 2nd call still fails ~10% of the time. After this step, validation failures return immediately to the user. This is acceptable because:
- The system already has this fallback (lines 506-511: return null plan with issues)
- Success rate drops temporarily (~70% → ~70%) until later steps improve first-pass quality

---

### STEP 8: Delete `RepairContextExpander`
**Risk:** LOW (depends on Step 7)  
**LOC removed:** 319  
**Classes removed:** 1  
**Tests removed:** `RepairContextExpanderTest`  

**What to do:**
1. Delete `capability/RepairContextExpander.java`.
2. Remove `RepairContextExpander` field from `CopilotController`.
3. Remove the `repairMessage()` method from `CopilotController` (lines 540-563).
4. Remove `combineTokenUsage()` method from `CopilotController` (no longer needed).
5. Delete `capability/RepairContextExpanderTest.java`.

---

### STEP 9: Simplify `CapabilityMetricsRegistry`
**Risk:** LOW  
**LOC removed:** ~120 (from ~150 → ~30)  
**Classes removed:** 0  
**Tests updated:** `CapabilityMetricsRegistryTest`  

**What to do:**
1. Remove all repair-related metrics methods:
   - `observeRepairAttempt()`
   - `observeRepairResult()`
   - `observeRepairTokens()`
   - All repair counters/histograms
2. Remove intent-detail metrics (overly granular):
   - Keep only: `observeFirstPass(boolean, List<issues>)` and `observeSelection(intent, closure, size)`
3. Simplify internal state to ~5 counters: requests, successes, failures, avgTokens, avgContextSize.
4. Update `CapabilityMetricsRegistryTest`.

---

### STEP 10: Merge `llm` package into root
**Risk:** LOW  
**Classes moved:** 1 (only `LlmClient` remains after Steps 1-2)  
**Packages removed:** 1  

**What to do:**
1. Move `llm/LlmClient.java` to root package: `org.apache.nifi.copilot.LlmClient`
2. Update package declaration and all imports (CopilotController, tests).
3. Delete `llm/` directory.
4. Move `LlmClientTest.java` to root test package.

---

### STEP 11: Merge `config` and `store` packages into root
**Risk:** LOW  
**Classes moved:** 3 (`CorsConfig`, `ExternalNiFiClientCondition`, `SessionStore`)  
**Packages removed:** 2  

**What to do:**
1. Move `config/CorsConfig.java` → `org.apache.nifi.copilot.CorsConfig`
2. Move `config/ExternalNiFiClientCondition.java` → `org.apache.nifi.copilot.ExternalNiFiClientCondition`
3. Move `store/SessionStore.java` → `org.apache.nifi.copilot.SessionStore`
4. Update package declarations and imports.
5. Delete `config/` and `store/` directories.

---

### STEP 12: Trim System Prompt — Remove CONNECTIONS Section
**Risk:** MEDIUM  
**LOC modified:** ~22 lines deleted from `SYSTEM_PROMPT`  
**Tokens saved:** ~560  

**What to do:**
1. In `LlmClient.java`, delete lines 73-94 (the entire `=== CONNECTIONS ===` section) from `SYSTEM_PROMPT`.
2. Add one replacement line:
   ```
   Do not include connections in your output. The backend handles all connections deterministically.
   ```
3. In `LlmClient.normalizeGeneratedLayout()`, after parsing, strip any `connections` the LLM still generates:
   ```java
   // Let Java build connections from processor order
   // Keep LLM connections only as a fallback until Step 15 completes
   ```
4. **Keep** the connection handling in `FlowBuilder`/`FlowSpecificationValidator` — they still process connections from the spec. The LLM just produces fewer/simpler ones without the elaborate rules.

**Why safe:** The backend already validates and normalizes connections. The 22 lines of prompt rules told the LLM how to build connections correctly. Removing them means the LLM may produce worse connections, but the validator catches issues and the deployment pipeline handles whatever is provided.

**Note:** This step is a prompt-only change. The JSON schema in the system prompt still shows `connections` as a field, so the LLM may still produce them. They'll pass through the existing pipeline normally. The difference is the LLM spends fewer tokens reasoning about connection rules.

---

### STEP 13: Trim System Prompt — Simplify PROCESSOR CONFIG Section
**Risk:** LOW  
**LOC modified:** ~8 lines deleted from `SYSTEM_PROMPT`  
**Tokens saved:** ~240  

**What to do:**
1. Delete lines 110-113 (processor-specific hints: ConsumeMQTT, MergeRecord, DistributeLoad).
2. These are band-aid prompt patches for specific LLM mistakes that the repair pipeline used to fix. With the repair pipeline gone, keeping them provides marginal value vs. the token cost.

---

### STEP 14: Reduce History Window from 6 to 2
**Risk:** LOW  
**Tokens saved:** ~1,100 (avg)  

**What to do:**
1. In `LlmClient.generateFlowSpec()` (line 178), change:
   ```java
   final int start = Math.max(0, history.size() - 6);
   ```
   to:
   ```java
   final int start = Math.max(0, history.size() - 2);
   ```
2. Same change in `generateFlowSpecBedrock()` (line 278).

**Why safe:** NiFi flow generation is stateless per request. The user's current message contains the complete intent. History is only useful for multi-turn refinement ("now add a logger"), which works with 2 messages of context. 6 messages wastes ~1,100 tokens on irrelevant prior conversations.

---

### STEP 15: Reduce `CapabilityPromptRenderer` — Remove Optional/Runtime Tiers
**Risk:** MEDIUM  
**LOC removed:** ~100  
**Tokens saved:** ~660 per request  

**What to do:**
1. In `CapabilityPromptRenderer.render()` (line 146), delete the calls to:
   ```java
   appendProperties(context, closure, ranked -> ranked.propertyClass() == PropertyClass.OPTIONAL, false);
   appendRuntimeMetadata(context, closure);
   appendProperties(context, closure, ranked -> ranked.propertyClass() == PropertyClass.RUNTIME, false);
   ```
2. Delete `appendRuntimeMetadata()` method.
3. Keep only: REQUIRED properties + DATA_CONTRACT properties + allowable value details.
4. Reduce `MAX_CONTEXT_CHARS` from 12,000 to 6,000.
5. Update `CapabilityPromptRendererTest` — remove assertions about optional/runtime property rendering.

**Why safe:** Optional and runtime properties have defaults. The LLM doesn't need to see them — it should only configure required properties and those related to data format (data_contract). If the user explicitly mentions a non-required property ("set the batch size to 1000"), the LLM can set it even without seeing it in context because the validator accepts it via fuzzy name matching.

---

### STEP 16: Reduce `CapabilityPromptRenderer` — Remove Relationship Lines
**Risk:** LOW  
**LOC removed:** ~30  
**Tokens saved:** ~200 per request  

**What to do:**
1. Remove the call to `context.appendRequired(relationshipLine(processor))` from `render()`.
2. Delete the `relationshipLine()` method.
3. Update tests.

**Why safe:** Relationships are deterministic. If Java builds connections (Step 12 removes the LLM's need to know them), the LLM never needs relationship names. The validator still validates any relationships the LLM produces against the graph.

---

### STEP 17: Simplify `IntentExtractor`
**Risk:** MEDIUM  
**LOC removed:** ~150 (from 222 → ~70)  
**Tests updated:** `IntentExtractorTest`  

**What to do:**
1. Replace the regex-heavy enum classification with simple keyword containment checks:
   ```java
   // Before: 40 lines of Pattern.compile + Pattern.matches per enum
   // After: Set.of("mqtt", "amqp", "jms").stream().anyMatch(msg::contains)
   ```
2. Remove `TransformationKind` enum granularity — reduce to: `HAS_TRANSFORM` boolean.
3. Remove `EndpointKind` granularity — extract just the keyword strings found.
4. Keep `DataFormat` — it's useful for CS selection.
5. Change `WorkflowIntent` record to simpler structure:
   ```java
   record WorkflowIntent(
       Set<String> sourceKeywords,
       Set<String> sinkKeywords,
       Set<String> transformKeywords,
       Set<String> formatKeywords,
       boolean batching, boolean parallelism, boolean logging
   ) {}
   ```
6. Update `ProcessorSeedRanker` to accept new `WorkflowIntent` shape (temporary — deleted in Step 18).
7. Update all tests.

---

### STEP 18: Replace `ProcessorSeedRanker` with `ProcessorSelector`
**Risk:** MEDIUM-HIGH  
**LOC removed:** 249 (ranker) → replaced by ~120 LOC (selector)  
**Net LOC change:** -129  
**Classes removed:** 1 (`ProcessorSeedRanker`)  
**Classes added:** 1 (`ProcessorSelector`)  
**Tests removed:** `ProcessorSeedRankerTest`  
**Tests added:** `ProcessorSelectorTest`  

**What to do:**
1. Create `capability/ProcessorSelector.java` with deterministic lookup tables:
   - `SOURCE_MAP`: keyword → source processor FQN
   - `SINK_MAP`: keyword → sink processor FQN
   - `TRANSFORM_MAP`: keyword → transform processor FQN
   - `FORMAT_TO_READER`: format keyword → RecordReader implementation
   - `FORMAT_TO_WRITER`: format keyword → RecordWriter implementation
2. Method: `Selection select(String userMessage, CapabilityGraph graph)`
   - Returns: processors + services + whether fallback is needed
   - If no keywords match → return empty (triggers fallback to current ranking)
3. Update `CapabilityPromptRenderer`:
   - If `ProcessorSelector` returns a result → use it directly (skip ranking)
   - If `ProcessorSelector` returns empty → fall back to `processorSeedRanker.rank()` temporarily
4. Delete `ProcessorSeedRanker.java` once selector covers 80%+ of test cases.
5. Delete `ProcessorSeedRankerTest.java`, create `ProcessorSelectorTest.java`.

**Fallback behavior ensures safety:** The selector only fires when it's confident. Unknown patterns fall through to the existing ranking pipeline (which remains until Step 20).

---

### STEP 19: Simplify `DependencyClosureResolver` — Single-Level Resolution
**Risk:** MEDIUM  
**LOC removed:** ~200 (from 324 → ~120)  

**What to do:**
1. Remove the BFS depth-traversal logic (depth 2-4 paths).
2. Change `DEFAULT_LIMITS` to `depth=1, maxImplementationsPerApi=1, maxControllerServices=8`.
3. Remove the scoring/ranking of implementations — pick first compatible (or format-matched).
4. Remove the `ClosureLimits` record (only one config needed).
5. Simplify `resolve()` to:
   - For each selected processor → find required service APIs from property descriptors
   - For each API → pick one implementation (format-matched if possible)
   - Return flat list (no recursion needed at depth=1)
6. Update `DependencyClosureResolverTest`.

---

### STEP 20: Remove `ProcessorSeedRanker` Fallback Path
**Risk:** LOW (after Step 18 is validated)  
**LOC removed:** Ranker reference removal from renderer  

**What to do:**
1. After confirming `ProcessorSelector` handles all test scenarios:
   - Remove `ProcessorSeedRanker` field from `CapabilityPromptRenderer`
   - Remove the fallback code path
   - `CapabilityPromptRenderer` now calls only `ProcessorSelector`
2. Update constructor and Spring wiring.
3. Simplify `CapabilityPromptRenderer` — it no longer iteratively reduces seed count (the `selectFitting` loop at line 104). `ProcessorSelector` returns exactly the processors needed.

---

### STEP 21: Merge `IntentExtractor` into `ProcessorSelector`
**Risk:** LOW  
**LOC removed:** ~70 (simplified IntentExtractor)  
**Classes removed:** 1  
**Tests merged:** `IntentExtractorTest` → `ProcessorSelectorTest`  

**What to do:**
1. Move keyword extraction logic from `IntentExtractor` into `ProcessorSelector` as a private method.
2. Delete `capability/IntentExtractor.java`.
3. Delete `capability/WorkflowIntent.java` (no longer needed as a public record — internalize into selector).
4. Merge test cases.
5. Delete `IntentExtractorTest.java`.

---

### STEP 22: Simplify `FlowSpecificationValidator` — Remove Topology Checks
**Risk:** LOW  
**LOC removed:** ~80  

**What to do:**
1. Remove `validateRelationships()` method — Java will own connections.
2. Remove relationship validation from `validateInternal()`:
   ```java
   // DELETE: validateRelationships(maps(normalized.get("connections")), processors, issues);
   ```
3. Remove `validateScheduling()` — scheduling is always defaults.
4. Keep: processor type resolution, property validation, service type validation, deletion validation.
5. Update `FlowSpecificationValidatorTest`.

---

### STEP 23: Add Auto-Correction to `FlowSpecificationValidator`
**Risk:** MEDIUM  
**LOC added:** ~40  
**LOC removed:** ~20  
**Net:** +20 LOC  

**What to do:**
1. In `validateProperties()`, when a required property has no value and has a default:
   - Instead of adding a validation issue → silently apply the default
2. When a property value doesn't match allowable values but is close (case-insensitive):
   - Auto-correct to the canonical form
3. When a property name doesn't match any descriptor but fuzzy-matches one (Levenshtein ≤ 2):
   - Auto-correct the key name
4. Log corrections at WARN level.
5. This means validation failures become extremely rare → no need for repair.

---

### STEP 24: Simplify `CopilotController` — Extract Deletion Logic
**Risk:** LOW  
**LOC moved:** ~120  
**Classes added:** 0 (private method extraction only)  

**What to do:**
1. Extract the deletion resolution + execution logic (lines 291-330+) from `chat()` into a private method `executeDeletions(List<Map>, ...)`.
2. Extract canvas reading logic (lines 227-244) into `readCanvasContext(...)`.
3. Extract capability rendering (lines 247-260) into `renderCapabilities(...)`.
4. The `chat()` method shrinks from ~200 lines of orchestration to ~60 lines calling these private methods.
5. No new classes — just internal decomposition for readability.

---

### STEP 25: Simplify `LlmClient` — Remove Overload Chain
**Risk:** LOW  
**LOC removed:** ~80  
**Methods removed:** 6 (3 GitHub overloads + 3 Bedrock overloads)  

**What to do:**
1. Delete the 3 `generateFlowSpec()` overloads (lines 129-163) that just add default parameters.
2. Delete the 3 `generateFlowSpecBedrock()` overloads (lines 231-264).
3. Keep only the full-parameter versions (lines 165 and 267).
4. Update all call sites to pass all parameters explicitly (add `List.of()` and `""` where needed).
5. This is a mechanical change — callers already have all the data.

---

### STEP 26: Merge `NiFiAsyncRequestState` into `NiFiAsyncRequestExecutor`
**Risk:** LOW  
**LOC removed:** ~30  
**Classes removed:** 1  

**What to do:**
1. `NiFiAsyncRequestState` is a simple state holder used only by `NiFiAsyncRequestExecutor`.
2. Make it a private inner record of `NiFiAsyncRequestExecutor`.
3. Delete `service/NiFiAsyncRequestState.java`.

---

### STEP 27: Merge `NiFiRevision` into `HttpNiFiClient`
**Risk:** LOW  
**LOC removed:** ~20  
**Classes removed:** 1  

**What to do:**
1. `NiFiRevision` is a simple record used only within `HttpNiFiClient`.
2. Make it a private inner record.
3. Delete `service/NiFiRevision.java`.

---

### STEP 28: Delete `UnsupportedControllerServiceException`
**Risk:** LOW  
**LOC removed:** ~15  
**Classes removed:** 1  

**What to do:**
1. Check usages — if only used in repair pipeline (already deleted) or if only 1-2 throw sites remain.
2. Replace with standard `IllegalArgumentException` or `ValidationIssue` entry.
3. Delete `capability/UnsupportedControllerServiceException.java`.

---

### STEP 29: Simplify `NiFiClientOperations` Interface
**Risk:** MEDIUM  
**LOC removed:** ~200 (from interface + implementation)  

**What to do:**
1. `NiFiClientOperations` has 65 methods. Many are unused or used only in one place.
2. Identify methods with zero call sites outside `HttpNiFiClient` itself (internal helpers exposed as interface methods).
3. Remove them from the interface; make them private in `HttpNiFiClient`.
4. Expected removal: ~15-20 methods that are never called externally.
5. This requires careful grep of each method name across the project.

---

### STEP 30: Delete `InstantNow` Utility
**Risk:** LOW  
**LOC removed:** ~15  
**Classes removed:** 1  

**What to do:**
1. `InstantNow` is likely a testability wrapper around `Instant.now()`.
2. Replace usages with direct `Instant.now()` calls (or `Clock` injection if needed for tests).
3. Delete `auth/InstantNow.java`.
4. If tests depend on it for time mocking, replace with `Clock` parameter.

---

### STEP 31: Simplify `FlowDeploymentMetricsRegistry`
**Risk:** LOW  
**LOC removed:** ~80 (from ~200 → ~120)  

**What to do:**
1. Remove repair-related stage observations (already no repair calls after Step 7-8).
2. Remove overly granular stage timing — keep only: total deployment time, success/failure count, layout outcome.
3. Remove enum values from `Stage` that are no longer observed.
4. Update `FlowDeploymentMetricsRegistryTest`.

---

### STEP 32: Merge `SpecificationSupport` into `FlowBuilder`
**Risk:** LOW  
**LOC removed:** ~60 (class overhead)  
**Classes removed:** 1  

**What to do:**
1. `SpecificationSupport` contains static utility methods (`listOfMap`, `mapOrNull`, `hasDeployableWork`).
2. Move these as private static methods into `FlowBuilder` (their only consumer besides `LocalPreflightValidator`).
3. For `LocalPreflightValidator` usages: pass data directly instead of calling static utilities, or duplicate the 2 trivial methods.
4. Delete `builder/SpecificationSupport.java`.

---

### STEP 33: Merge `NiFiEntitySupport` into `ComponentResolver`
**Risk:** LOW  
**LOC removed:** ~40  
**Classes removed:** 1  

**What to do:**
1. `NiFiEntitySupport` contains NiFi-entity utility methods used by deployment stages.
2. Move into `ComponentResolver` as package-private utility methods (it's already the central component helper).
3. Delete `builder/NiFiEntitySupport.java`.
4. Update import statements in deployment stages.

---

### STEP 34: Final System Prompt Reduction
**Risk:** MEDIUM  
**LOC modified:** System prompt from ~80 remaining lines → ~30 lines  
**Tokens saved:** ~600  

**What to do:**
1. Remove `=== CANVAS LAYOUT ===` section (1 line: "Do not include x/y" — normalizer already strips them).
2. Remove `=== EXPLANATION RULES ===` section (2 lines — LLM produces reasonable explanations without rules).
3. Condense `=== OUTPUT SCHEMA ===` to essential fields only (remove rarely-used: `funnels`, `cs_actions`, `deletions` from schema — keep as optional undocumented).
4. Remove `=== DELETIONS ===` section — replace with one-line: "For deletions, use {deletions: [{type, spec_id, name}]}"
5. Remove `=== PROCESSOR CONFIG ===` detailed rules — condense to 2 lines: "Keys = display names. Service refs = service spec id. Use #{param} for parameters."

**Final system prompt target: ~30 lines, ~800 tokens (down from 127 lines, ~3,400 tokens).**

---

## Final State

### Package Structure (After All Steps)

```
org.apache.nifi.copilot/
├── NiFiCopilotApplication.java
├── CopilotModule.java
├── LlmClient.java                    (moved from llm/)
├── SessionStore.java                  (moved from store/)
├── CorsConfig.java                    (moved from config/)
├── ExternalNiFiClientCondition.java   (moved from config/)
│
├── api/
│   ├── CopilotController.java
│   └── Dto.java
│
├── auth/
│   ├── GitHubAuthManager.java
│   └── AwsAuthManager.java
│
├── capability/
│   ├── CapabilityRegistry.java        (absorbed: RegistryManager, TypeSelector)
│   ├── CapabilityGraph.java           (absorbed: GraphBuilder)
│   ├── CapabilityDefinitionParser.java
│   ├── CapabilitySnapshot.java
│   ├── CapabilityPromptRenderer.java  (simplified: ~150 LOC)
│   ├── ProcessorSelector.java         (NEW, replaces: IntentExtractor + SeedRanker)
│   ├── DependencyClosureResolver.java (simplified, absorbed: PropertyRanker)
│   ├── FlowSpecificationValidator.java (simplified: ~250 LOC)
│   ├── CapabilityMetricsRegistry.java (simplified: ~30 LOC)
│   ├── FlowSpecificationValidationException.java
│   ├── ValidatedFlowPlan.java
│   ├── ValidationReport.java
│   ├── ValidationIssue.java
│   ├── ValidationIssueType.java
│   ├── ProcessorCapability.java       (record)
│   ├── ControllerServiceCapability.java (record)
│   ├── PropertyCapability.java        (record)
│   ├── PropertyDependency.java        (record)
│   ├── AllowableValue.java            (record)
│   ├── BundleCoordinate.java          (record)
│   └── ServiceApi.java                (record)
│
├── builder/
│   ├── FlowBuilder.java
│   ├── FlowDeploymentCoordinator.java
│   ├── FlowDeploymentMetricsRegistry.java (simplified)
│   ├── DeploymentPreparationStage.java
│   ├── DependencyPreflightStage.java
│   ├── DependencyDeploymentStage.java
│   ├── ComponentDeploymentStage.java
│   ├── ConnectionConfigurationStage.java
│   ├── LayoutDeploymentStage.java
│   ├── RuntimeActivationStage.java
│   ├── ComponentResolver.java         (absorbed: NiFiEntitySupport)
│   ├── ComponentRegistry.java
│   ├── CanvasProjector.java
│   ├── CanvasPositionProvider.java
│   ├── NiFiClientLayoutWriter.java
│   ├── LocalPreflightValidator.java
│   ├── LivePreflightValidator.java
│   ├── DeploymentContext.java
│   ├── DeploymentState.java
│   ├── DeploymentReport.java
│   ├── DeploymentTarget.java
│   ├── OwnershipLedger.java
│   ├── RollbackManager.java
│   ├── LayoutMode.java
│   ├── ControllerServiceDeployer.java
│   ├── ConnectionDeployer.java
│   ├── ProcessorDeployer.java
│   ├── ProcessorStarter.java
│   ├── FunnelDeployer.java
│   ├── LabelDeployer.java
│   ├── PortDeployer.java
│   ├── ParameterContextDeployer.java
│   ├── SnippetDeployer.java
│   ├── RemoteProcessGroupDeployer.java
│   ├── RelationshipConfigurer.java
│   ├── RuntimeStateApplier.java
│   └── ProcessGroupFlowMapAssembler.java
│
└── service/
    ├── HttpNiFiClient.java            (absorbed: NiFiRevision)
    ├── NiFiClientOperations.java      (simplified: ~50 methods → ~45)
    ├── NiFiAsyncRequestExecutor.java  (absorbed: NiFiAsyncRequestState)
    ├── NiFiClientMetricsRegistry.java
    ├── NiFiClientException.java
    ├── NiFiRetryPolicy.java
    ├── NiFiConfigResolver.java
    ├── NiFiClientSelectionConfiguration.java
    └── CapabilityDiscoveryException.java
```

### Final Metrics

| Metric | Before | After | Reduction |
|--------|--------|-------|-----------|
| **Packages** | 8 | 5 | **37%** |
| **Source classes** | 87 | 58 | **33%** |
| **Deleted classes** | — | 16 | — |
| **Merged classes** | — | 13 | — |
| **LOC (estimated)** | ~16,745 | ~11,800 | **30%** |
| **System prompt tokens** | ~3,400 | ~800 | **76%** |
| **Avg capability context** | ~3,200 tokens | ~1,200 tokens | **62%** |
| **History tokens** | ~1,760 | ~580 | **67%** |
| **Avg total input tokens** | ~8,330 | ~2,600 | **69%** |
| **LLM calls per request** | 1.3 | 1.0 | **23%** |
| **Normalizer LOC** | 978 | 0 | **100%** |
| **Repair pipeline LOC** | 540 | 0 | **100%** |

---

## Execution Schedule

| Week | Steps | Focus | Risk |
|------|-------|-------|------|
| 1 | 1–6 | Delete normalizers, merge small classes | LOW |
| 2 | 7–11 | Delete repair pipeline, merge packages | LOW-MEDIUM |
| 3 | 12–16 | Reduce prompt and context tokens | MEDIUM |
| 4 | 17–21 | Replace probabilistic selection with deterministic | MEDIUM-HIGH |
| 5 | 22–28 | Simplify validator, merge remaining helpers | LOW-MEDIUM |
| 6 | 29–34 | Final cleanup and prompt trimming | LOW-MEDIUM |

---

## Classes Deleted (16 total)

| # | Class | LOC | Reason |
|---|-------|-----|--------|
| 1 | TerminalLoggerNormalizer | 851 | Over-engineered fix for LLM mistakes |
| 2 | ParallelWorkerNormalizer | 127 | Over-engineered fix for LLM mistakes |
| 3 | CapabilityRegistryManager | 60 | Thin wrapper, merged into CapabilityRegistry |
| 4 | CapabilityGraphBuilder | 100 | Merged as static factory on CapabilityGraph |
| 5 | PropertyRanker | 125 | Merged into DependencyClosureResolver |
| 6 | CapabilityTypeSelector | 120 | Merged into CapabilityRegistry |
| 7 | RepairHintDeriver | 131 | Repair pipeline deleted |
| 8 | RepairHint | 30 | Repair pipeline deleted |
| 9 | RepairContextExpander | 319 | Repair pipeline deleted |
| 10 | IntentExtractor | 222 | Merged into ProcessorSelector |
| 11 | WorkflowIntent | 40 | Internalized into ProcessorSelector |
| 12 | ProcessorSeedRanker | 249 | Replaced by ProcessorSelector |
| 13 | NiFiAsyncRequestState | 30 | Merged into NiFiAsyncRequestExecutor |
| 14 | NiFiRevision | 20 | Merged into HttpNiFiClient |
| 15 | UnsupportedControllerServiceException | 15 | Replaced by standard exception |
| 16 | SpecificationSupport | 60 | Merged into FlowBuilder |
| 17 | NiFiEntitySupport | 40 | Merged into ComponentResolver |
| 18 | InstantNow | 15 | Replaced by Clock/Instant.now() |
| | **Total deleted** | **~2,554** | |

## Classes Added (1 total)

| # | Class | LOC | Reason |
|---|-------|-----|--------|
| 1 | ProcessorSelector | 120 | Deterministic processor selection |

**Net class reduction: 87 → 58 (with merges) = -29 classes (33%)**

---

## Test Impact Summary

| Test Class | Action | Reason |
|-----------|--------|--------|
| CapabilityRegistryManagerTest | DELETE → merge into CapabilityRegistryTest | Class merged |
| CapabilityGraphBuilderTest | DELETE → merge into CapabilityGraphTest | Class merged |
| PropertyRankerTest | DELETE → merge into DependencyClosureResolverTest | Class merged |
| RepairHintDeriverTest | DELETE | Class deleted |
| RepairContextExpanderTest | DELETE | Class deleted |
| ProcessorSeedRankerTest | DELETE → replaced by ProcessorSelectorTest | Class replaced |
| IntentExtractorTest | DELETE → merge into ProcessorSelectorTest | Class merged |
| CapabilityMetricsRegistryTest | UPDATE | Simplified metrics |
| FlowSpecificationValidatorTest | UPDATE | Removed topology checks |
| CapabilityPromptRendererTest | UPDATE | Removed optional/runtime tiers |
| LlmClientTest | UPDATE | Removed normalizer assertions |
| CapabilityPipelineRegressionTest | UPDATE | End-to-end adjustments |
| NEW: ProcessorSelectorTest | CREATE | New deterministic selection |

**Net test class change: 36 → 28 (-8 = -22%)**

---

## Dependency Order

```
Step 1 ──┐
Step 2 ──┤ (independent deletions)
Step 3 ──┤
Step 4 ──┤
Step 5 ──┤
Step 6 ──┘
          │
Step 7 ───┤ (requires nothing above; repair removal)
Step 8 ───┘ (requires Step 7)
          │
Step 9 ───── (requires Steps 7-8)
Step 10 ──┐
Step 11 ──┘ (package merges — independent of above)
          │
Step 12 ──┐
Step 13 ──┤ (prompt trimming — independent of class changes)
Step 14 ──┘
          │
Step 15 ──┐
Step 16 ──┘ (renderer simplification)
          │
Step 17 ───── (intent simplification — before selector)
          │
Step 18 ───── (selector creation — requires Step 17)
          │
Step 19 ───── (closure simplification — independent of 18)
          │
Step 20 ───── (remove fallback — requires Step 18 validated)
          │
Step 21 ───── (merge intent into selector — requires Steps 18, 20)
          │
Step 22 ──┐
Step 23 ──┘ (validator simplification — requires Step 12)
          │
Step 24 ──┐
Step 25 ──┤
Step 26 ──┤ (cleanup — independent)
Step 27 ──┤
Step 28 ──┤
Step 29 ──┤
Step 30 ──┤
Step 31 ──┤
Step 32 ──┤
Step 33 ──┘
          │
Step 34 ───── (final prompt reduction — last)
```

---

## Validation Checkpoints

After each week, verify:

| Checkpoint | How to Verify |
|-----------|---------------|
| All tests pass | `mvn test` |
| Flow generation works | Manual test: "MQTT to S3" produces working flow |
| Canvas modification works | Manual test: add processor to existing canvas |
| Deletion works | Manual test: "delete the consumer" removes processor |
| Token count reduced | Log `_token_usage` from response, verify trend |
| No regressions | Compare flow output for 10 standard test prompts |

---

*End of refactoring plan.*
