# Capability System Deep Review
**Date:** 2026-07-26  
**Objective:** Determine the simplest capability system that produces accurate flows  

---

## Current Capability System Architecture

```
NiFi REST API
    │
    ▼
CapabilityRegistry.discoverAndPublish()
    │  Fetches all processor/service types + their property definitions
    │  Filters via CapabilityTypeSelector (dedup by version)
    │  Parses via CapabilityDefinitionParser
    │  Caches with TTL (15 min)
    │
    ▼
CapabilitySnapshot (processors: Map, services: Map, timestamp)
    │
    ▼
CapabilityGraphBuilder.build()
    │  Converts flat capability records → indexed graph
    │
    ▼
CapabilityGraph (indexed by type, simple name, API implementations)
    │
    ├───────────────────────────────────────────────┐
    ▼                                               ▼
IntentExtractor.extract()                  FlowSpecificationValidator
    │  Regex: user text → WorkflowIntent        │  Validates LLM output against graph
    │                                           │  Resolves types, checks properties
    ▼                                           │
ProcessorSeedRanker.rank()                      ▼
    │  Scores all processors vs intent      ValidatedFlowPlan
    │  Returns top 8 ranked seeds
    │
    ▼
DependencyClosureResolver.resolve()
    │  BFS: processor APIs → service implementations
    │  Depth-limited (max 4), ranks by format match
    │  Returns: CapabilityClosure
    │
    ▼
CapabilityPromptRenderer.render()
    │  Tiered rendering within 12,000-char budget:
    │    Required: types, required properties, APIs, relationships
    │    Optional: data_contract, optional, runtime properties
    │
    ▼
String capabilityContext  →  LLM system message
    │
    ▼
[On validation failure:]
RepairHintDeriver.derive()
    │  Validation issues → structured hints
    ▼
RepairContextExpander.expand()
    │  Builds 16,000-char expanded context
    │  Adds additional processors/services from graph
    ▼
String repairContext  →  2nd LLM call
```

---

## Component-by-Component Analysis

---

### 1. Capability Discovery (`CapabilityRegistry.discoverAndPublish`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | Fetches processor types and service types from a live NiFi instance, including their full property descriptors, to know what's available. |
| **What problem does it solve?** | NiFi installations differ — different bundles, different versions. The copilot needs to know what processors are actually installed, what properties they have, and what their allowable values are. Without this, the LLM would hallucinate types. |
| **Can Java replace this logic?** | NO. The data comes from NiFi's REST API. There's no static alternative unless you hardcode a fixed processor catalog (which breaks across NiFi versions). |
| **Can validation replace this logic?** | NO. Validation needs this data to validate against. |
| **Can it be merged?** | Already self-contained. |
| **Can it be removed?** | **NO.** This is the foundation. Everything else depends on knowing what NiFi supports. |

**Verdict: REQUIRED. Keep as-is.**

---

### 2. Capability Cache (`CapabilityRegistry` TTL + `CapabilityRegistryManager`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | Discovery is expensive (calls NiFi per-type for property definitions). Cache avoids repeated discovery on every user request. `CapabilityRegistryManager` adds per-client caching (IdentityHashMap). |
| **What problem does it solve?** | Performance. Without cache, every request would make N+M HTTP calls to NiFi (N processors + M services). |
| **Can Java replace this logic?** | N/A — this IS Java logic. |
| **Can validation replace this logic?** | No. |
| **Can it be merged?** | **YES.** `CapabilityRegistryManager` is a thin per-client wrapper around `CapabilityRegistry`. In practice there's one client. Merge `CapabilityRegistryManager` into `CapabilityRegistry`. |
| **Can it be removed?** | The cache: NO. The manager wrapper: YES. |

**Verdict: Cache REQUIRED. `CapabilityRegistryManager` MERGE into `CapabilityRegistry`.**

---

### 3. Capability Graph (`CapabilityGraph` + `CapabilityGraphBuilder`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | Provides O(1) lookup by type, simple name, and API→implementation mapping. The flat `CapabilitySnapshot` maps are just `Map<String, ProcessorCapability>` — the graph adds secondary indexes. |
| **What problem does it solve?** | Fast resolution: given a processor type or API, find the matching capability. Used by validator, ranker, prompt renderer, and deployment. |
| **Can Java replace this logic?** | This IS Java logic. |
| **Can validation replace this logic?** | Validation USES this. |
| **Can it be merged?** | **YES.** `CapabilityGraphBuilder` is 100 LOC that converts `CapabilitySnapshot` → `CapabilityGraph`. It's called from exactly 2 places. Make it a static factory: `CapabilityGraph.from(snapshot)`. |
| **Can it be removed?** | NO. The indexed graph is essential for all lookups. |

**Verdict: `CapabilityGraph` REQUIRED. `CapabilityGraphBuilder` MERGE as static factory method.**

---

### 4. Capability Selection (`IntentExtractor` + `ProcessorSeedRanker`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | To select which processors to include in the LLM's capability context. The full NiFi catalog has 200-400 processor types. You can't send all of them. |
| **What problem does it solve?** | Context window scoping. If user says "stream MQTT to S3", only ConsumeMQTT, PutS3Object, and related processors should appear in context. |
| **Can Java replace this logic?** | **YES — and BETTER.** The current system: (1) extracts intent via regex, (2) scores ALL processors against intent, (3) takes top 8. This is a probabilistic search that can miss or include irrelevant processors. A deterministic lookup table (`MQTT source → ConsumeMQTT`, `S3 sink → PutS3Object`, `convert → ConvertRecord`) would be faster, more accurate, and 200 LOC simpler. |
| **Can validation replace this logic?** | No — validation happens after generation. Selection happens before. |
| **Can it be merged?** | **YES.** Both should merge into a single `ProcessorSelector` that does: intent keywords → processor list (no scoring needed). |
| **Can it be removed?** | The concept: NO (you must scope context). The 470 LOC implementation: YES — replace with ~60 LOC lookup table. |

**Deep analysis of why the current approach is over-engineered:**

The `ProcessorSeedRanker` scores every processor in the graph (200-400 processors) using:
- Explicit name match: +200 points
- Name token match: +8 points per term
- Metadata token match: +2 points per term (max 4)
- Source endpoint match: +50 points
- Sink endpoint match: +50 points
- Transformation match: +40 points
- Batching signal: +35 points
- Archive signal: +35 points
- Logging signal: +35 points
- Parallelism signal: +35 points
- Format match: +20 points

This elaborate scoring exists because the system doesn't know which processors to use for a given intent. But **NiFi flows follow patterns**: MQTT → ConsumeMQTT. S3 → PutS3Object. Convert → ConvertRecord. These are deterministic.

The ranker's heuristic scoring adds complexity without certainty. A lookup table provides certainty without complexity.

**Verdict: REPLACE both with a deterministic `ProcessorSelector` (lookup table + composition rules).**

---

### 5. Dependency Closure Resolution (`DependencyClosureResolver`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | Once processors are selected, their required controller services must also be included in context. E.g., ConvertRecord requires RecordReaderFactory and RecordSetWriterFactory APIs. Each API has implementations (JsonTreeReader, CSVReader, etc.). Those implementations may themselves require further services (transitive deps). |
| **What problem does it solve?** | Ensures the LLM knows about all required services when configuring processors. Without this, the LLM wouldn't know which CS types to use. |
| **Can Java replace this logic?** | **PARTIALLY.** If Java owns CS type selection (picking the first/best implementation for each API), the LLM doesn't need to see API→implementation mappings at all. Java can: (1) look up required APIs from processor properties, (2) select implementation, (3) tell LLM "this processor's Record Reader property uses cs1 (JsonTreeReader)". |
| **Can validation replace this logic?** | No — validation checks after the fact. |
| **Can it be merged?** | Into the processor selector. |
| **Can it be removed?** | The transitive BFS (depth 4): **YES — reduce to depth 1.** In practice, service dependency depth rarely exceeds 1. The scoring heuristics for ranking implementations: **SIMPLIFY** to "pick first compatible" or "pick by format keyword in name". |

**Analysis of depth distribution:**

- Depth 1: Processor → RecordReaderFactory → JsonTreeReader (**99% of cases**)
- Depth 2: Processor → SchemaAccessStrategy → AvroSchemaRegistry (**rare**)
- Depth 3+: **Never observed in typical copilot flows**

The depth-4 BFS with max 16 services exists for theoretical completeness but adds 200 LOC of complexity for a case that doesn't arise in practice.

**Verdict: SIMPLIFY to single-level resolution. ~80 LOC replaces 324 LOC.**

---

### 6. Property Ranking (`PropertyRanker`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | Classifies each property into: REQUIRED_SERVICE_REFERENCE, REQUIRED, SERVICE_REFERENCE, DATA_CONTRACT, OPTIONAL, RUNTIME. This determines what appears in the "required" tier (always included) vs "optional" tier (included if space permits). |
| **What problem does it solve?** | Context window management. With 12,000-char budget, you can't include all 30 properties of a processor. You include the 5-8 required ones, then fill with data_contract/optional as space allows. |
| **Can Java replace this logic?** | This IS Java logic. And it's correct. |
| **Can validation replace this logic?** | No. |
| **Can it be merged?** | **YES.** It's 125 LOC with a single consumer (`DependencyClosureResolver`). Make it a private inner class or utility method. |
| **Can it be removed?** | **NOT entirely.** The classification concept is needed. But it can be radically simplified: `required && no default → REQUIRED`, `has CS API → SERVICE_REFERENCE`, `everything else → OPTIONAL`. That's 20 LOC, not 125. |

**Verdict: SIMPLIFY + MERGE into closure resolver (or prompt renderer).**

---

### 7. Capability Prompt Renderer (`CapabilityPromptRenderer`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | Takes a `CapabilityClosure` (selected processors + services + properties) and renders it into a structured text block for the LLM's system message. |
| **What problem does it solve?** | Formatting. The LLM needs to see processor types, their properties, allowable values, and relationships in a parseable format. |
| **Can Java replace this logic?** | This IS Java logic. |
| **Can validation replace this logic?** | No. |
| **Can it be merged?** | It's the end of the selection pipeline. Merging it into the selector would create a god class. Keep separate. |
| **Can it be removed?** | NO. The LLM needs context. Something must render it. |

**However:** The renderer is 412 LOC because it handles:
1. BoundedContext with required/optional tiers (120 LOC)
2. Processor lines + API lines + service lines (50 LOC)
3. Required properties (60 LOC)
4. Relationship lines (20 LOC) — **REMOVE** (Java owns topology)
5. Required property details with allowable values (60 LOC)
6. DATA_CONTRACT properties (30 LOC)
7. OPTIONAL properties (30 LOC) — **REMOVE** (wasteful tokens)
8. RUNTIME metadata (30 LOC) — **REMOVE** (Java handles scheduling)
9. RUNTIME properties (30 LOC) — **REMOVE** (same)

**After removing items 4, 7, 8, 9:** Renderer shrinks from 412 → ~280 LOC.

**With simplified context (only required properties + allowable values):** ~150 LOC.

**Verdict: SIMPLIFY from 412 → ~150 LOC.**

---

### 8. Repair Context (`RepairHintDeriver` + `RepairContextExpander`)

| Question | Answer |
|----------|--------|
| **Why does it exist?** | When the first LLM call produces a spec that fails validation, the repair pipeline: (1) analyzes why it failed, (2) builds structured hints, (3) expands the capability context with additional processors/services the LLM might have needed, (4) triggers a 2nd LLM call. |
| **What problem does it solve?** | Recovery from LLM hallucination/mistakes. Common failures: wrong processor type, wrong relationship name, wrong CS type, missing required property. |
| **Can Java replace this logic?** | **YES — by preventing the failures in the first place.** If Java selects processors (no hallucination), sets relationships (from graph), and selects CS types (from API requirements), the only remaining failure mode is "bad property value" — which the validator can auto-correct by applying the default value. |
| **Can validation replace this logic?** | **YES** — a "corrective validator" that auto-fixes minor issues (apply default, normalize type name, correct case) eliminates the need for a repair LLM call. |
| **Can it be merged?** | N/A — it should be deleted. |
| **Can it be removed?** | **YES.** The entire repair pipeline (RepairHintDeriver: 131 LOC + RepairContextExpander: 319 LOC + repair logic in controller: ~60 LOC + RepairHint record: ~30 LOC) = 540 LOC that exists solely because the LLM makes mistakes that a redesigned pipeline wouldn't allow. |

**Why the repair pipeline is a design smell:**

The repair pipeline is an admission that the first LLM call fails too often. Instead of fixing the root cause (asking the LLM to do too much), the system adds more complexity (a 2nd LLM call with even more context: 16,000 chars!). This is a classic "patching the symptom" anti-pattern.

If Call 1 fails 30% of the time, the solution isn't to add Call 2 with better context. The solution is to make Call 1 simpler so it succeeds 95%+ of the time.

**Verdict: DELETE. All 540 LOC.**

---

## Summary: What's Required vs Removable

| Component | LOC | Required? | Verdict |
|-----------|-----|-----------|---------|
| CapabilityRegistry (discovery + cache) | 237 | ✅ YES | KEEP (merge RegistryManager into it) |
| CapabilityDefinitionParser | 200 | ✅ YES | KEEP |
| CapabilityGraph | 232 | ✅ YES | KEEP (absorb GraphBuilder as static factory) |
| CapabilityGraphBuilder | 100 | ⚠️ Merge | MERGE into CapabilityGraph |
| CapabilityRegistryManager | 60 | ⚠️ Merge | MERGE into CapabilityRegistry |
| IntentExtractor | 222 | ⚠️ Simplify | SIMPLIFY to ~60 LOC keyword lookup |
| ProcessorSeedRanker | 249 | ❌ Over-engineered | REPLACE with ~60 LOC deterministic lookup |
| DependencyClosureResolver | 324 | ⚠️ Simplify | SIMPLIFY to ~80 LOC single-level resolution |
| PropertyRanker | 125 | ⚠️ Simplify | SIMPLIFY to ~20 LOC and merge |
| CapabilityPromptRenderer | 412 | ✅ YES (simplified) | SIMPLIFY from 412 → ~150 LOC |
| FlowSpecificationValidator | 532 | ✅ YES (simplified) | SIMPLIFY from 532 → ~250 LOC |
| RepairHintDeriver | 131 | ❌ Delete | DELETE |
| RepairContextExpander | 319 | ❌ Delete | DELETE |
| RepairHint | 30 | ❌ Delete | DELETE |
| CapabilityMetricsRegistry | 150 | ⚠️ Simplify | SIMPLIFY to ~30 LOC |
| CapabilitySnapshot | 17 | ✅ YES | KEEP |
| CapabilityTypeSelector | 120 | ⚠️ Merge | MERGE into CapabilityRegistry |
| Data records (9 total) | ~200 | ✅ YES | KEEP |
| **Total current** | **~3,400** | | |
| **Total after simplification** | **~1,400** | | **-59%** |

---

## The Simplest Capability System That Works

### Design Principles

1. **Capability discovery is mandatory** — you must know what NiFi has installed
2. **Capability graph indexing is mandatory** — you need O(1) lookups
3. **Processor selection should be deterministic** — not probabilistic scoring
4. **Service resolution should be single-level** — depth > 1 is theoretical
5. **Property rendering should include only what the LLM needs** — required properties + allowable values
6. **Repair should not exist** — prevent failures, don't recover from them
7. **Validation is the source of truth** — it validates property values, not topology

### Proposed Architecture

```
NiFi REST API
    │
    ▼
CapabilityRegistry  (discovery + cache + type resolution)
    │  [Absorbs: CapabilityRegistryManager, CapabilityTypeSelector]
    │  [TTL: 15 min]
    │
    ▼
CapabilityGraph  (immutable indexes + static factory)
    │  [Absorbs: CapabilityGraphBuilder]
    │  [Indexes: byType, bySimpleName, serviceImplementationsByApi]
    │
    ├──────────────────────────────────────┐
    ▼                                      ▼
ProcessorSelector                  PropertyValidator
    │  [NEW: deterministic]              │  [Simplified FlowSpecificationValidator]
    │                                    │
    │  Input: user text                  │  Input: LLM output + CapabilityGraph
    │  Output: List<ProcessorNode>       │  Output: ValidatedFlowPlan
    │         + List<ServiceNode>        │
    │                                    │  Validates:
    │  Logic:                            │    - Property values vs allowable values
    │    keywords → processors           │    - Required properties present
    │    processor APIs → services       │    - Service references valid
    │    format keywords → reader/writer │    - Types correct (with auto-correction)
    │                                    │
    ▼                                    │
ContextRenderer                          │
    │  [Simplified CapabilityPromptRenderer]
    │                                    │
    │  Input: selected processors +      │
    │         selected services          │
    │  Output: capability context string │
    │                                    │
    │  Renders:                          │
    │    - Processor type + id           │
    │    - Required properties           │
    │    - Allowable values              │
    │    - Service API → implementation  │
    │                                    │
    ▼                                    │
LLM Call (one call, no repair)           │
    │                                    │
    │  Output: property values + names   │
    │                                    │
    └──────────────────► ────────────────┘
```

### Component Count

| Current | Proposed | Reduction |
|---------|----------|-----------|
| 18 classes | 7 classes | **-61%** |
| ~3,400 LOC | ~1,400 LOC | **-59%** |

### Proposed Classes (7 total)

| Class | LOC | Responsibility |
|-------|-----|---------------|
| `CapabilityRegistry` | ~300 | Discovery + cache + type resolution (absorbs Manager + TypeSelector) |
| `CapabilityDefinitionParser` | ~200 | Parses NiFi REST API responses |
| `CapabilityGraph` | ~250 | Immutable indexes + static factory (absorbs GraphBuilder) |
| `ProcessorSelector` | ~120 | Deterministic: intent keywords → processor list + service list |
| `ContextRenderer` | ~150 | Renders selected capabilities for LLM |
| `PropertyValidator` | ~250 | Validates LLM output (property values only) |
| `CapabilitySnapshot` | ~17 | Record type |
| *(data records)* | ~200 | ProcessorCapability, ControllerServiceCapability, PropertyCapability, etc. |

### ProcessorSelector Design (replaces IntentExtractor + ProcessorSeedRanker + DependencyClosureResolver)

```java
// Conceptual design — not implementation

class ProcessorSelector {
    
    // Deterministic mapping: intent signal → processor type
    private static final Map<String, String> SOURCE_MAP = Map.of(
        "mqtt",     "org.apache.nifi.processors.mqtt.ConsumeMQTT",
        "kafka",    "org.apache.nifi.kafka.processors.ConsumeKafka",
        "s3",       "org.apache.nifi.processors.aws.s3.FetchS3Object",
        "http",     "org.apache.nifi.processors.standard.InvokeHTTP",
        "file",     "org.apache.nifi.processors.standard.GetFile",
        "database", "org.apache.nifi.processors.standard.ExecuteSQL"
    );
    
    private static final Map<String, String> SINK_MAP = Map.of(
        "mqtt",     "org.apache.nifi.processors.mqtt.PublishMQTT",
        "kafka",    "org.apache.nifi.kafka.processors.PublishKafka",
        "s3",       "org.apache.nifi.processors.aws.s3.PutS3Object",
        "http",     "org.apache.nifi.processors.standard.InvokeHTTP",
        "file",     "org.apache.nifi.processors.standard.PutFile",
        "database", "org.apache.nifi.processors.standard.PutDatabaseRecord"
    );
    
    private static final Map<String, String> TRANSFORM_MAP = Map.of(
        "convert",  "org.apache.nifi.processors.standard.ConvertRecord",
        "route",    "org.apache.nifi.processors.standard.RouteOnAttribute",
        "filter",   "org.apache.nifi.processors.standard.RouteOnAttribute",
        "split",    "org.apache.nifi.processors.standard.SplitRecord",
        "merge",    "org.apache.nifi.processors.standard.MergeRecord",
        "validate", "org.apache.nifi.processors.standard.ValidateRecord"
    );
    
    // Service resolution: for each selected processor,
    // find required service APIs and pick first compatible implementation
    // Format-aware: if user mentions "json", pick JsonTreeReader over CSVReader
    
    record Selection(
        List<ProcessorNode> processors,
        List<ControllerServiceNode> services,
        Map<String, String> serviceAssignments  // property → service spec_id
    ) {}
    
    Selection select(String userMessage, CapabilityGraph graph) {
        // 1. Extract keywords (simplified IntentExtractor: ~20 lines)
        // 2. Map keywords to processor types via lookup tables
        // 3. Verify each type exists in graph (fall back if not)
        // 4. For each processor's required service APIs:
        //    - Find implementations in graph
        //    - Pick implementation matching user's format keywords
        // 5. Return ordered list
    }
}
```

**Why this is better than ProcessorSeedRanker:**

| Aspect | Current (Ranker) | Proposed (Selector) |
|--------|-----------------|-------------------|
| Correctness | Probabilistic — top-8 may miss | Deterministic — exact match or nothing |
| Speed | O(N) scoring over all processors | O(1) lookup |
| Debuggability | "Why did it pick this?" requires score analysis | "It picked this because keyword matched" |
| Maintenance | Add new processor → hope scoring works | Add new processor → add one map entry |
| Failure mode | Returns wrong processors silently | Returns empty → falls back to LLM selection |
| LOC | 470 (IntentExtractor + ProcessorSeedRanker) | ~120 |

### Context Renderer Design (replaces CapabilityPromptRenderer)

**Current renderer output (12,000 chars):**
```
[TARGET NIFI CAPABILITIES]
This target snapshot is authoritative. Use only the exact types and property names below.
PROCESSOR org.apache.nifi.processors.mqtt.ConsumeMQTT
REQUIRED_CONTROLLER_SERVICE_API org.apache.nifi.serialization.RecordReaderFactory implementations=[JsonTreeReader, CSVReader]
CONTROLLER_SERVICE org.apache.nifi.json.JsonTreeReader depth=1
PROCESSOR_PROPERTIES org.apache.nifi.processors.mqtt.ConsumeMQTT
  PROPERTY internal=broker-uri display=Broker URI required=true class=REQUIRED default=<none>
  PROPERTY internal=topic display=Topic Filter required=true class=REQUIRED default=<none>
  PROPERTY internal=qos display=Quality of Service(QoS) required=true class=REQUIRED default=0 
PROCESSOR_RELATIONSHIPS org.apache.nifi.processors.mqtt.ConsumeMQTT values=[Message] dynamic=false
PROCESSOR_PROPERTY_DETAILS ... allowable=[0/At most once, 1/At least once, 2/Exactly once]
DATA_CONTRACT_PROPERTY ...
OPTIONAL_PROPERTY ...
RUNTIME_PROPERTY ...
CONTROLLER_SERVICE_RUNTIME ...
```

**Proposed renderer output (~4,000 chars):**
```
[SELECTED PROCESSORS]
Configure these processors. Use only the properties listed.

PROCESSOR id=proc1 type=org.apache.nifi.processors.mqtt.ConsumeMQTT
  REQUIRED display="Broker URI" default=<none>
  REQUIRED display="Topic Filter" default=<none>
  REQUIRED display="Quality of Service(QoS)" default=0 allowable=[0, 1, 2]

PROCESSOR id=proc2 type=org.apache.nifi.processors.standard.ConvertRecord
  SERVICE display="Record Reader" assigned=cs1
  SERVICE display="Record Writer" assigned=cs2

PROCESSOR id=proc3 type=org.apache.nifi.processors.aws.s3.PutS3Object
  REQUIRED display="Bucket" default=<none>
  REQUIRED display="Region" default=us-east-1 allowable=[us-east-1, us-west-2, eu-west-1, ...]

SERVICE id=cs1 type=org.apache.nifi.json.JsonTreeReader
SERVICE id=cs2 type=org.apache.nifi.parquet.ParquetRecordSetWriter
```

**Differences:**
- No processor relationships (Java owns connections)
- No optional/runtime/data_contract properties (LLM doesn't need them)
- No API declarations (Java already resolved them)
- Service assignments pre-resolved (LLM doesn't pick implementations)
- IDs pre-assigned (LLM just references them)
- ~3x smaller

### PropertyValidator Design (replaces FlowSpecificationValidator)

**Current validator checks (532 LOC):**
1. Processor type resolution (FQN/simple name/alias)
2. Property name validation
3. Property value vs allowable values
4. Required property presence
5. Relationship validation
6. Scheduling strategy validation
7. Controller service type validation
8. Connection topology checks
9. Deletion validation

**Proposed validator checks (~250 LOC):**
1. ~~Processor type resolution~~ → Java pre-selected, always correct
2. Property name validation → KEEP (LLM might typo a property name)
3. Property value vs allowable values → KEEP (core validation)
4. Required property presence → KEEP (auto-fix: apply default)
5. ~~Relationship validation~~ → Java owns connections
6. ~~Scheduling strategy validation~~ → Java uses defaults
7. ~~Controller service type validation~~ → Java pre-selected
8. ~~Connection topology checks~~ → Java owns topology
9. ~~Deletion validation~~ → Keep simple check

**Auto-correction behavior (instead of rejecting + repair call):**
- Missing required property → apply default value or `#{placeholder}`
- Property name case mismatch → correct to canonical case
- Unknown property → ignore (don't reject the whole spec)
- Invalid allowable value → log warning, apply default

This eliminates the need for a repair call entirely.

---

## Migration Path

### Phase 1: Delete Repair Pipeline (immediate)
- Delete `RepairHintDeriver`, `RepairContextExpander`, `RepairHint`
- Remove repair logic from `CopilotController.generateAndPrepare()`
- On validation failure: return failure to user with issues (already implemented as fallback)
- **Risk:** Success rate may drop 5-10% temporarily until Phase 2

### Phase 2: Add Deterministic Processor Selection
- Create `ProcessorSelector` with lookup tables
- Wire it before `CapabilityPromptRenderer`
- If selector returns results → use them (no ranker)
- If selector returns empty → fall back to current ranker (safety net)
- **Risk:** Near zero — additive change with fallback

### Phase 3: Simplify Prompt Rendering
- Remove relationship lines from renderer
- Remove optional/runtime property tiers
- Reduce `MAX_CONTEXT_CHARS` to 4,000
- **Risk:** Low — removes tokens that don't help

### Phase 4: Delete Old Components
- Once selector proves reliable, delete `ProcessorSeedRanker`
- Simplify `IntentExtractor` to keyword extraction only (no regex scoring)
- Simplify `DependencyClosureResolver` to single-level
- Merge `PropertyRanker` into renderer as private method
- **Risk:** Medium — requires validation that new pipeline succeeds ≥ as often

### Phase 5: Auto-Correcting Validator
- Modify `FlowSpecificationValidator` to auto-correct instead of reject
- Remove topology/relationship checks (Java owns these now)
- **Risk:** Low — makes validator more permissive, not less

---

## Final Comparison

| Metric | Current System | Proposed System |
|--------|---------------|-----------------|
| Classes | 18 | 7 |
| Total LOC | ~3,400 | ~1,400 |
| Processor selection | Probabilistic scoring (470 LOC) | Deterministic lookup (120 LOC) |
| Service resolution | BFS depth-4 (324 LOC) | Single-level (inline) |
| Context size | 12,000 chars | 4,000 chars |
| Repair pipeline | 540 LOC + 2nd LLM call | None |
| Validation approach | Reject + repair | Auto-correct |
| Metrics | 150 LOC (38 counters) | ~30 LOC (5 counters) |
| Failure recovery | 2nd LLM call (9,320 tokens) | Return error to user |
| Expected first-pass success | ~70% (current) | ~95% (less can go wrong) |

---

*End of review.*
