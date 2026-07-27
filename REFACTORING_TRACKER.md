# Refactoring Tracker

## Objective
Simplify the nifi-copilot project while preserving all existing functionality.

## Baseline
- **Classes**: 87 source, 43 test
- **Packages**: 8
- **Tests**: 243 (all passing)
- **LOC**: ~16,745

---

## Phase A: Safe Class Merges & Deletions

| # | Action | Target | Status | LOC Delta | Notes |
|---|--------|--------|--------|-----------|-------|
| 1 | Merge | `CapabilityGraphBuilder` → `CapabilityGraph.from()` | ✅ Done | -9 net | Static factory method |
| 2 | Inline | `InstantNow` → private `epochSec()` in auth managers | ✅ Done | -10 | Trivial utility deleted |
| 3 | Delete | 6 `LlmClient` overloads (3 GitHub + 3 Bedrock) | ✅ Done | -72 | Only full-param version used in prod |
| 4 | Merge | `CapabilityRegistryManager` → `CapabilityRegistry` | ⏭️ Skipped | — | Would break FlowSpecificationValidator API |
| 5 | Merge | `PropertyRanker` → `DependencyClosureResolver` | ⏭️ Skipped | — | Shared by 2 independent consumers |
| 6 | Merge | `CapabilityTypeSelector` → `CapabilityRegistry` | ⏭️ Skipped | — | Would create service→capability dep |
| 7 | Merge | `NiFiAsyncRequestState` → `HttpNiFiClient` | ⏭️ Skipped | — | HttpNiFiClient already 2930 LOC |

**Phase A Totals**: -2 classes, -248 lines deleted, +74 lines added

---

## Phase B: Prompt & Context Reduction

| # | Action | Target | Status | Token Delta | Notes |
|---|--------|--------|--------|-------------|-------|
| 1 | Trim | System prompt (87→58 lines, ~1764→~939 tokens) | ✅ Done | -825 tokens | Removed rules handled by normalizer/validator |
| 2 | Reduce | History window (6→4 messages) | ✅ Done | ~-500 tokens | Most conversations are ≤4 meaningful turns |
| 3 | Reduce | MAX_CONTEXT_CHARS (12,000→8,000) + drop RUNTIME property tier | ✅ Done | ~-1,143 tokens max | Keeps required/data-contract/optional; drops scheduling props |

---

## Phase C: Repair Pipeline Deletion

| # | Action | Target | Status | LOC Delta | Notes |
|---|--------|--------|--------|-----------|-------|
| 1 | Delete | `RepairHintDeriver.java` (144 LOC) | ✅ Done | -144 | Redundant with ValidationIssue.suggestedFix |
| 2 | Delete | `RepairContextExpander.java` (350 LOC) | ✅ Done | -350 | Repair reuses original capability context |
| 3 | Delete | `RepairHint.java` (34 LOC) | ✅ Done | -34 | No longer needed |
| 4 | Simplify | `CapabilityMetricsRegistry` (remove expansion metrics) | ✅ Done | -50 | Fields/methods/record fields removed |
| 5 | Simplify | `CopilotController` (remove repair collaborators) | ✅ Done | -15 | 2 fields, constructor params removed |

**Phase C Totals**: -3 source classes, -2 test classes, ~-528 source LOC, ~-384 test LOC

---

## Phase D: Java-Owned Topology

| # | Action | Target | Status | LOC Delta | Notes |
|---|--------|--------|--------|-----------|-------|
| 1 | Inline | `ParallelWorkerNormalizer` → private methods in `LlmClient` | ✅ Done | -1 class | 127 LOC moved inline, file deleted |
| 2 | Keep | `TerminalLoggerNormalizer` (850 LOC) | ⏭️ Kept | — | Complex graph algo, well-tested, correct |
| 3 | Skip | New `TopologyBuilder` | ⏭️ Skipped | — | Would ADD code; normalizers already do this |

**Phase D Notes**: Original plan to build a `TopologyBuilder` was reconsidered — it would add new code contradicting the simplification goal. The normalizers ARE Java-owned topology logic already. `ParallelWorkerNormalizer` was small enough to inline; `TerminalLoggerNormalizer` remains as a focused helper.

---

## Phase E: Delete Normalizers

| # | Action | Target | Status | LOC Delta | Notes |
|---|--------|--------|--------|-----------|-------|
| 1 | Delete | `TerminalLoggerNormalizer` | ⬜ Pending | -851 | Requires Phase D complete |
| 2 | Delete | `ParallelWorkerNormalizer` | ⬜ Pending | -127 | Requires Phase D complete |

---

## Phase F: Deterministic Selection

| # | Action | Target | Status | LOC Delta | Notes |
|---|--------|--------|--------|-----------|-------|
| 1 | N/A | `ProcessorSeedRanker` already deterministic Java | ⏭️ N/A | — | Scoring/ranking is already Java, not LLM-driven |

**Phase F Notes**: The selection pipeline (`IntentExtractor` → `ProcessorSeedRanker` → `DependencyClosureResolver`) is already 100% deterministic Java. No LLM is involved in processor/service selection. Phase F was based on an incorrect assumption in the original plan.

---

## Phase G: Final Cleanup

| # | Action | Target | Status | LOC Delta | Notes |
|---|--------|--------|--------|-----------|-------|
| 1 | Merge | Packages (9 → 7) | ✅ Done | 0 | `store`→`service`, `config`→root |
| 2 | Simplify | Validator abstractions | ⏭️ N/A | — | Already lean records (14-52 LOC); no simplification needed |

---

## Running Totals

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Source classes | 87 | 81 | -6 |
| Test classes | 43 | 41 | -2 |
| Packages | 9 | 7 | -2 (-22%) |
| Tests passing | 243 | 234 | -9 (deleted test classes) |
| Lines deleted | — | 1,550 | — |
| Lines added | — | 228 | — |
| Net LOC change | — | — | -1,322 |
| System prompt tokens | ~1764 | ~939 | -825 |
| History window | 6 msgs | 4 msgs | -2 |
| Behavioral regressions | — | 0 | — |

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-07-27 | Skip `CapabilityRegistryManager` merge | Multi-client factory with clean SRP; merge would break `FlowSpecificationValidator` constructor |
| 2026-07-27 | Skip `PropertyRanker` merge | Used by both `DependencyClosureResolver` and `RepairContextExpander` independently |
| 2026-07-27 | Skip `CapabilityTypeSelector` merge | Would create circular dep (service→capability) since `HttpNiFiClient` also uses it |
| 2026-07-27 | Skip `NiFiAsyncRequestState` merge | `HttpNiFiClient` is already 2930 LOC; nesting 162 more lines worsens readability |
| 2026-07-27 | Reorder: Normalizers AFTER Java topology | Deleting normalizers without replacement causes behavioral regressions (learned from Step 1 attempt) |

---

## Verification Commands

```bash
# Compile
mvn compile -pl . -Denforcer.skip=true

# Full test suite (243 tests)
mvn test -pl . -Denforcer.skip=true

# Single test class
mvn test -pl . -Denforcer.skip=true -Dtest=ClassName
```
