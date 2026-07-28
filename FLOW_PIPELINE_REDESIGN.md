# Flow Generation Pipeline Redesign
**Date:** 2026-07-26  
**Objective:** Make every step deterministic unless Java genuinely cannot handle it  

---

## Current Pipeline (End-to-End Trace)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 1: User Prompt Ingestion                                               │
│ CopilotController.chat() line 206                                           │
│ Input: ChatRequest { message, history, existing_processors, read_canvas }   │
│ Output: Parsed request + canvas context                                     │
│ Ownership: Java ✓                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 2: Canvas Context Reading                                              │
│ FlowBuilder.readCanvas() → CanvasProjector                                  │
│ Reads existing processors, services, groups, connections from NiFi          │
│ Ownership: Java ✓                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 3: Capability Discovery + Caching                                      │
│ CapabilityRegistryManager → CapabilityRegistry.discoverAndPublish()         │
│ CapabilityGraphBuilder.build() → CapabilityGraph                            │
│ Ownership: Java ✓                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 4: Intent Extraction                                                   │
│ IntentExtractor.extract(userMessage) → WorkflowIntent                       │
│   - EndpointKind (6 enums) via regex                                        │
│   - DataFormat (6 enums) via regex                                           │
│   - TransformationKind (7 enums) via regex                                  │
│   - Boolean flags: batching, parallelism, archive, logging                  │
│ Ownership: Java ✓ (but 222 LOC of regex is overkill)                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 5: Processor Seed Ranking                                              │
│ ProcessorSeedRanker.rank(graph, intent) → List<RankedProcessor>             │
│   - Scores ALL processors (200-400) against intent                          │
│   - Weighted heuristics (200 for explicit name, 50 for source/sink, etc.)   │
│   - Returns top 8 seeds                                                     │
│ Ownership: Java ✓ (but probabilistic — should be deterministic)             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 6: Dependency Closure Resolution                                       │
│ DependencyClosureResolver.resolve(graph, intent, seeds) → CapabilityClosure │
│   - BFS traversal: processor → required API → implementation                │
│   - Depth-limited (max 4), format-aware scoring                             │
│   - Resolves transitive controller service dependencies                     │
│ Ownership: Java ✓ (but over-engineered for practical depth of 1)            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 7: Context Rendering                                                   │
│ CapabilityPromptRenderer.render(closure, 12000) → String                    │
│   - Tiered: REQUIRED → DATA_CONTRACT → OPTIONAL → RUNTIME                  │
│   - Fits within BoundedContext (12,000 chars)                               │
│   - If overflow: iteratively reduces seed count                             │
│ Ownership: Java ✓                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 8: System Prompt Construction                                          │
│ LlmClient.systemPrompt(capabilityContext)                                   │
│   = SYSTEM_PROMPT (127 lines / ~3,400 chars) + capabilityContext            │
│ Ownership: Java constructs it, but content is for LLM                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 9: User Message Construction                                           │
│ LlmClient.buildUserMessage() → user text + [CANVAS CONTEXT]                 │
│   - Appends existing processors, connections, groups, services              │
│ Ownership: Java constructs it, LLM reads it                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 10: LLM Call #1                                                        │
│ POST to GitHub Models or AWS Bedrock                                        │
│ Input: system + history (last 6) + user message                             │
│ Output: JSON specification (processors, connections, services, etc.)         │
│ Ownership: ★ LLM ★                                                          │
│                                                                             │
│ LLM DECIDES:                                                                │
│   - Which processors to use (from capabilities)                             │
│   - Which controller services to create                                     │
│   - Processor names                                                         │
│   - Property values                                                         │
│   - Connections (topology)                                                  │
│   - Relationship names                                                      │
│   - Controller service assignments                                          │
│   - Deletions                                                               │
│   - Parameter contexts                                                      │
│   - Explanation text                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 11: Post-Generation Normalization                                      │
│ LlmClient.normalizeGeneratedLayout()                                        │
│   - Strips x/y from processors                                              │
│   - Runs ParallelWorkerNormalizer (127 LOC)                                 │
│   - Runs TerminalLoggerNormalizer (851 LOC, Tarjan's SCC)                   │
│   - Removes self-loops                                                      │
│   - Removes terminal outgoing edges                                         │
│   - Deduplicates connections                                                │
│ Ownership: Java ✓ (but 978 LOC for fixing LLM mistakes)                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 12: Local Preflight Validation                                         │
│ FlowBuilder.prepareFlow() → LocalPreflightValidator                         │
│   - validateSpecificationCollections                                        │
│   - validateComponentSpecIds                                                │
│   - validateConnectionTopology                                              │
│   - validateParameterContext                                                │
│ Ownership: Java ✓                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 13: Capability-Aware Validation                                        │
│ FlowSpecificationValidator.validateAndNormalize()                           │
│   - Resolves processor types (FQN/simple/alias)                             │
│   - Validates property names vs capability descriptors                      │
│   - Validates allowable values                                              │
│   - Validates service types                                                 │
│   - Validates relationships against capability graph                        │
│   - Normalizes type names to FQN                                            │
│ Ownership: Java ✓                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                              ┌─────┴─────┐
                              │ Valid?    │
                              └─────┬─────┘
                         YES │               │ NO
                              ▼               ▼
┌─────────────────────┐  ┌───────────────────────────────────────────────────┐
│ Step 14: Deploy     │  │ Step 13b: Repair Pipeline                         │
│ (see below)         │  │ RepairHintDeriver.derive() → structured hints     │
│                     │  │ RepairContextExpander.expand() → 16,000-char ctx  │
│                     │  │ repairMessage() → formatted failure + issues      │
│                     │  │                                                   │
│                     │  │ LLM Call #2: Same API, expanded context            │
│                     │  │   Input: repair message + expanded capabilities   │
│                     │  │   Output: Corrected JSON specification            │
│                     │  │                                                   │
│                     │  │ Re-validate → accept or fail to user              │
│                     │  │ Ownership: ★ LLM ★ (9,320 token 2nd call)        │
│                     │  └───────────────────────────────────────────────────┘
└─────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 14: Deployment                                                         │
│ FlowDeploymentCoordinator.deploy()                                          │
│   Stage 1: DeploymentPreparationStage (resolve PG, snapshot)                │
│   Stage 2: DependencyPreflightStage (check NiFi connectivity)               │
│   Stage 3: DependencyDeploymentStage (create/enable controller services)    │
│   Stage 4: ComponentDeploymentStage (create processors, funnels)            │
│   Stage 5: ConnectionConfigurationStage (create connections)                │
│   Stage 6: LayoutDeploymentStage (apply visual layout)                      │
│   Stage 7: RuntimeActivationStage (auto-start processors)                   │
│ Ownership: Java ✓                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Per-Step Determinism Analysis

| Step | Currently Owns | Can Be Deterministic? | Should Be LLM? | Verdict |
|------|---------------|----------------------|-----------------|---------|
| 1. Prompt ingestion | Java | ✅ Always | No | **JAVA** |
| 2. Canvas reading | Java | ✅ Always | No | **JAVA** |
| 3. Capability discovery | Java | ✅ Always | No | **JAVA** |
| 4. Intent extraction | Java (regex) | ✅ Yes — keyword lookup | No | **JAVA** |
| 5. Processor selection | Java (scoring) | ⚠️ Partially — common cases are deterministic, novel requests aren't | Only for novel/ambiguous | **JAVA (with LLM fallback)** |
| 6. CS resolution | Java (BFS) | ✅ Yes — API→implementation is a graph lookup | No | **JAVA** |
| 7. Context rendering | Java | ✅ Always | No | **JAVA** |
| 8. System prompt | Java builds, LLM reads | N/A | N/A | **JAVA builds, reduce content** |
| 9. User message | Java builds, LLM reads | N/A | N/A | **JAVA builds** |
| 10. LLM generation | LLM | ❌ Cannot be deterministic | **YES** | **LLM (but scoped down)** |
| 11. Post-normalization | Java (fixes LLM) | ✅ Fixable by not asking LLM to do this | No | **JAVA (or eliminate need)** |
| 12. Local preflight | Java | ✅ Always | No | **JAVA** |
| 13. Capability validation | Java | ✅ Always | No | **JAVA** |
| 13b. Repair pipeline | LLM | ⚠️ Repair is LLM, but root cause is avoidable | No — eliminate entirely | **DELETE** |
| 14. Deployment | Java | ✅ Always | No | **JAVA** |

---

## What the LLM Currently Decides (Step 10)

The LLM's output JSON contains these fields:

| Field | Can Java Do This Instead? | Analysis |
|-------|--------------------------|----------|
| `processors[].type` | **YES** — deterministic lookup from intent keywords. "consume mqtt" → ConsumeMQTT. Always the same answer. | Java should select processors. |
| `processors[].id` | **YES** — sequential IDs (proc1, proc2, ...) assigned by Java. | Trivial. |
| `processors[].name` | **PARTIALLY** — default name = processor simple name. LLM can customize ("MQTT Consumer" vs "ConsumeMQTT"). | Low value. Java can generate acceptable defaults. LLM can optionally improve. |
| `processors[].config` | **NO** — property values require understanding user intent ("broker at tcp://10.0.0.1:1883", "topic = sensors/#"). This is the LLM's core job. | **LLM must do this.** |
| `controller_services[].type` | **YES** — derived from processor API requirements + data format. "json" → JsonTreeReader. Always deterministic. | Java should select. |
| `controller_services[].id` | **YES** — sequential IDs (cs1, cs2, ...). | Trivial. |
| `controller_services[].name` | **PARTIALLY** — default = service simple name. | Low value. Java default is fine. |
| `controller_services[].properties` | **PARTIALLY** — most CS properties have correct defaults. Only custom ones (schema, connection strings) need LLM. | LLM for custom values only. |
| `connections[].from/to` | **YES** — topology follows standard patterns: source → transform → sink. Linear pipelines, fan-out patterns are all deterministic once processors are known. | Java should connect. |
| `connections[].relationships` | **YES** — relationship names are in the capability graph. "success" for most processors. Always deterministic from graph data. | Java should set. |
| `funnels[]` | **YES** — funnels are structural, added when fan-in is needed. | Java should add when needed. |
| `deletions[]` | **YES for target resolution** — but LLM must interpret "delete the kafka consumer" → which processor to delete. | LLM for intent interpretation, Java for resolution. |
| `parameter_context` | **PARTIALLY** — when to create one is intent-driven, parameter names/values come from user text. | LLM for the decision + values. |
| `cs_actions[]` | **YES** — "enable the SSL service" → match by name, set action. But interpreting which service the user means requires LLM. | LLM for interpretation. |
| `explanation` | **NO** — natural language generation is LLM's job. | **LLM must do this.** |
| `process_group` | **PARTIALLY** — whether to use one is user-driven. The name comes from user text. | LLM for decision + naming. |

---

## Classification of LLM Decisions

### ✅ Java Should Own (Deterministic)

| Decision | Why Deterministic | Current Cost (tokens in prompt) |
|----------|-------------------|-------------------------------|
| Processor type selection | Intent keywords → processor FQN is a lookup | ~2,000 (capability context showing all options) |
| Controller service type selection | Processor API requirement → implementation is graph traversal | ~800 (API/implementation listings) |
| Connection topology | Linear pipeline: [source] → [transforms...] → [sink]. Fan-out: DistributeLoad → N workers | ~560 (CONNECTION rules in system prompt) |
| Relationship names | Available relationships are in capability graph. "success", "failure" etc. are always the same per processor type | ~200 (relationship lines in context) |
| Processor IDs | Sequential assignment | 0 (already trivial) |
| Service IDs | Sequential assignment | 0 (already trivial) |
| Scheduling | Default values from capability graph | ~180 (RUNTIME metadata) |
| Layout | Already handled by Java post-LLM | 0 |

**Total tokens currently wasted on deterministic decisions: ~3,740**

### ❌ LLM Must Own (Non-Deterministic)

| Decision | Why Non-Deterministic | Minimum Context Needed |
|----------|----------------------|----------------------|
| Property value assignment | User says "broker at tcp://10.0.0.1:1883" — LLM maps free text to property | Property names + descriptions (~600 tokens) |
| Intent interpretation | "Stream sensor data to the lake" — what does "sensor data" mean? What's "the lake"? | Just the user message (~50 tokens) |
| Explanation generation | Natural language summary of what was built | List of what was created (~100 tokens) |
| Deletion target identification | "Remove the kafka consumer" from a canvas with 10 processors | Canvas context (~200 tokens) |
| Parameter context creation | User implies parameterization ("make the broker configurable") | User message (~50 tokens) |
| Custom CS property values | Schema content, connection strings, custom configurations | Property names + types (~100 tokens) |

**Total tokens needed for LLM decisions: ~1,100**

---

## Redesigned Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: DETERMINISTIC PLANNING (Java, ~5ms)                                │
│                                                                             │
│  Input: user message + canvas context                                       │
│                                                                             │
│  1.1 Request Classification                                                 │
│      Classify as: CREATE_FLOW | MODIFY_FLOW | DELETE | ENABLE_DISABLE       │
│      Java regex: "delete" → DELETE, "enable/disable" → ENABLE_DISABLE       │
│      Everything else → CREATE_FLOW or MODIFY_FLOW (if canvas non-empty)     │
│                                                                             │
│  1.2 Processor Selection (CREATE_FLOW / MODIFY_FLOW only)                   │
│      Extract keywords → lookup table → processor FQNs                       │
│      Example: "mqtt" + source position → ConsumeMQTT                        │
│               "s3" + sink position → PutS3Object                            │
│               "convert" → ConvertRecord                                     │
│      Verify each type exists in CapabilityGraph                             │
│      Fallback: if keywords match nothing → pass to LLM for selection        │
│                                                                             │
│  1.3 Controller Service Resolution                                          │
│      For each selected processor:                                           │
│        - Find required service API properties                               │
│        - Pick implementation by format keyword match                         │
│          ("json" → JsonTreeReader, "csv" → CSVReader, etc.)                 │
│        - If no format signal → pick the first implementation                │
│      Assign IDs: cs1, cs2, ...                                              │
│                                                                             │
│  1.4 Topology Construction                                                  │
│      Order: sources first, transforms middle, sinks last                    │
│      Connect linearly: proc1 → proc2 → proc3                               │
│      Relationships: use "success" (or first available from graph)           │
│      Fan-out: if DistributeLoad selected, create N parallel connections     │
│      Assign IDs: proc1, proc2, ...                                          │
│                                                                             │
│  Output: FlowSkeleton {                                                     │
│    processors: [{id, type, position_in_flow}]                               │
│    services: [{id, type, assigned_to_processor, assigned_to_property}]      │
│    connections: [{from, to, relationships}]                                  │
│    requires_llm_property_config: true/false                                 │
│  }                                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: LLM CONFIGURATION (Only when needed, ~1,500 tokens)                │
│                                                                             │
│  Called ONLY for:                                                            │
│    - Property value assignment (always)                                     │
│    - Processor selection (when Java lookup table has no match)              │
│    - Deletion target identification (DELETE requests)                       │
│    - Custom naming (if user specified names)                                │
│    - Parameter context creation                                             │
│                                                                             │
│  System Prompt (MINIMAL — ~800 tokens):                                     │
│    "You configure NiFi processor properties. Return JSON only.              │
│     For each processor, set property values based on the user's request.    │
│     Use #{param_name} for values the user wants configurable."              │
│                                                                             │
│  Context (structured, ~500 tokens for typical request):                     │
│    {                                                                         │
│      "task": "configure_properties",                                        │
│      "processors": [                                                        │
│        {                                                                     │
│          "id": "proc1",                                                     │
│          "type": "ConsumeMQTT",                                             │
│          "properties": [                                                    │
│            {"name": "Broker URI", "required": true, "default": null},       │
│            {"name": "Topic Filter", "required": true, "default": null},     │
│            {"name": "QoS", "required": true, "default": "0",               │
│             "allowable": ["0", "1", "2"]}                                   │
│          ]                                                                  │
│        }                                                                     │
│      ],                                                                     │
│      "services": [                                                          │
│        {"id": "cs1", "type": "JsonTreeReader", "properties": []}            │
│      ]                                                                       │
│    }                                                                         │
│                                                                             │
│  User Message: original user text (no canvas context — topology is done)    │
│                                                                             │
│  LLM Output (MINIMAL — ~400 tokens):                                        │
│    {                                                                         │
│      "properties": {                                                        │
│        "proc1": {"Broker URI": "tcp://broker:1883", "Topic Filter": "#"},   │
│        "proc2": {},                                                         │
│        "cs1": {}                                                            │
│      },                                                                     │
│      "names": {"proc1": "MQTT Consumer", "proc2": "S3 Writer"},            │
│      "explanation": "Streams MQTT messages to S3...",                        │
│      "parameter_context": null                                              │
│    }                                                                         │
│                                                                             │
│  Total tokens: ~800 (system) + ~500 (context) + ~200 (user) = ~1,500       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 3: ASSEMBLY (Java, ~1ms)                                              │
│                                                                             │
│  Merge LLM property values into FlowSkeleton                               │
│  Produce complete specification in current JSON schema                      │
│  Apply defaults for any properties LLM didn't set                           │
│  Assign service references (proc1.Record Reader = cs1)                      │
│                                                                             │
│  Output: Complete flow specification (same schema as today)                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 4: VALIDATION (Java, ~2ms)                                            │
│                                                                             │
│  Simplified validator (property values only):                               │
│    - Check required properties have values (auto-fix: apply default)        │
│    - Check allowable values match (auto-fix: apply default)                 │
│    - Check property names exist (auto-fix: fuzzy match + correct)           │
│    - Log warnings for corrected values                                      │
│                                                                             │
│  REMOVED from validator:                                                    │
│    - Processor type resolution (Java already selected correct types)        │
│    - Relationship validation (Java already set correct relationships)       │
│    - Connection topology checks (Java built the topology)                   │
│    - Service type validation (Java already resolved services)               │
│                                                                             │
│  Output: ValidatedFlowPlan (always succeeds — auto-corrects)                │
│                                                                             │
│  NO REPAIR PIPELINE. Validation auto-corrects or accepts.                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 5: DEPLOYMENT (Java, existing pipeline — keep as-is)                  │
│                                                                             │
│  FlowDeploymentCoordinator (unchanged):                                     │
│    1. DeploymentPreparationStage                                            │
│    2. DependencyPreflightStage                                              │
│    3. DependencyDeploymentStage                                             │
│    4. ComponentDeploymentStage                                              │
│    5. ConnectionConfigurationStage                                          │
│    6. LayoutDeploymentStage                                                 │
│    7. RuntimeActivationStage                                                │
│                                                                             │
│  No changes needed. This pipeline is well-designed.                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## What Gets Deleted

| Component | LOC | Why Deleted |
|-----------|-----|-------------|
| TerminalLoggerNormalizer | 851 | Java owns topology — no logger placement mistakes to fix |
| ParallelWorkerNormalizer | 127 | Java owns DistributeLoad connections — no mistakes to fix |
| RepairHintDeriver | 131 | No repair pipeline |
| RepairContextExpander | 319 | No repair pipeline |
| ProcessorSeedRanker | 249 | Replaced by lookup table in ProcessorSelector |
| ~50% of IntentExtractor | 110 | Simplified to keyword extraction only |
| ~60% of CapabilityPromptRenderer | 250 | Renderer becomes property-only context |
| ~50% of FlowSpecificationValidator | 266 | Topology/type checks removed |
| CONNECTIONS section (system prompt) | ~560 tokens | Java owns connections |
| Relationship rendering | ~200 tokens | Java owns relationships |
| OPTIONAL/RUNTIME properties | ~660 tokens | LLM doesn't need these |
| **Total deleted** | **~2,300 LOC** | |

---

## What Gets Added

| Component | LOC | Purpose |
|-----------|-----|---------|
| ProcessorSelector | ~120 | Keyword → processor type lookup table |
| TopologyBuilder | ~80 | Orders processors + assigns connections |
| FlowAssembler | ~60 | Merges LLM output into skeleton |
| **Total added** | **~260 LOC** | |

**Net reduction: ~2,040 LOC**

---

## Token Comparison

### Current Pipeline (typical "MQTT to S3" request)

| Component | Tokens |
|-----------|--------|
| System prompt (127 lines) | ~1,360 |
| Capability context (processors, APIs, services, all properties) | ~3,200 |
| Connection rules (22 lines) | ~560 |
| History (last 6 messages) | ~1,760 |
| User message | ~50 |
| Canvas context (if read_canvas) | ~400 |
| **Total input** | **~7,330** |
| LLM output (full spec with connections, topology, types, properties) | ~1,000 |
| **Total** | **~8,330** |

### Redesigned Pipeline (same request)

| Component | Tokens |
|-----------|--------|
| System prompt (5 lines: "configure properties, return JSON") | ~120 |
| Processor property list (only required props + allowable values) | ~400 |
| Service list (types only, no properties if defaults are fine) | ~60 |
| User message | ~50 |
| **Total input** | **~630** |
| LLM output (property values + names + explanation only) | ~300 |
| **Total** | **~930** |

**Reduction: 8,330 → 930 tokens (89%)**

---

## Edge Cases and Fallbacks

### When Java Lookup Table Has No Match

**Scenario:** User says "I want to deduplicate records using their hash"

Java finds no keyword match for "deduplicate" in lookup tables.

**Fallback:** Use current pipeline (full capability context + LLM selection) for this request only. The lookup table handles 80-90% of requests deterministically. The remaining 10-20% fall back to LLM-guided selection.

**Implementation:**
```
if (processorSelector.canSelect(userMessage)) {
    // Phase 1 deterministic path (930 tokens)
    skeleton = processorSelector.select(userMessage, graph);
} else {
    // Fallback: current rendering + let LLM pick types too (~3,000 tokens)
    context = capabilityPromptRenderer.renderFromGraph(userMessage, graph);
    // LLM selects processors AND configures properties in one call
}
```

### Canvas Modifications (MODIFY_FLOW)

**Scenario:** User says "Add a routing step between the consumer and the writer"

Java knows:
- Canvas has ConsumeMQTT (proc1) and PutS3Object (proc3)
- User wants to insert between them
- "routing" → RouteOnAttribute

Java can:
1. Select RouteOnAttribute
2. Insert it between proc1 and proc3
3. Ask LLM only for: what's the routing condition?

### Deletion Requests

**Scenario:** User says "Remove the kafka consumer"

Java cannot deterministically resolve "the kafka consumer" to a specific canvas processor when there might be multiple.

**Pipeline:**
1. Java classifies request as DELETE
2. Send to LLM with minimal prompt: "Which processors should be deleted? Return {deletions: [{spec_id, name}]}"
3. Context: canvas processor list only (~100 tokens)
4. No capability context needed at all

**Total tokens for deletion: ~300**

### Complex Multi-Step Flows

**Scenario:** "Build a flow that reads from Kafka, validates the JSON schema, routes valid records to S3 and invalid records to a dead letter queue in another Kafka topic"

Java can handle this:
1. Source: ConsumeKafka (keyword: kafka + source position)
2. Validate: ValidateRecord (keyword: validate)
3. Route: implicitly created by ValidateRecord's "valid"/"invalid" relationships
4. Sink 1: PutS3Object (keyword: s3 + "valid" branch)
5. Sink 2: PublishKafka (keyword: kafka + "invalid" branch + "dead letter")

Topology: ConsumeKafka → ValidateRecord → [valid] → PutS3Object
                                         → [invalid] → PublishKafka

The only non-deterministic parts: Kafka broker URLs, topic names, S3 bucket, schema content.

---

## LLM Prompt for Redesigned Pipeline

### New System Prompt (~120 tokens)

```
You configure Apache NiFi processor and controller service properties.
Return ONE valid JSON object — no markdown, no extra text.

Output schema:
{
  "properties": {"<component_id>": {"<Property Display Name>": "<value>"}},
  "names": {"<component_id>": "<human-friendly name>"},
  "explanation": "<1-2 sentence summary>",
  "parameter_context": {"name": "...", "parameters": {"key": "value"}} | null
}

Rules:
- Use exact property display names from the input.
- Use #{param_name} for values the user wants configurable.
- Omit properties that should keep their default.
- Supply every required property with no default.
```

### New User Message Format (~500 tokens for typical request)

```
Configure these components for: "Stream MQTT sensor data to S3 as parquet"

PROCESSORS:
  proc1 type=ConsumeMQTT
    REQUIRED "Broker URI" default=<none>
    REQUIRED "Topic Filter" default=<none>
    REQUIRED "Quality of Service(QoS)" default="0" values=["0","1","2"]
  proc2 type=ConvertRecord
    SERVICE "Record Reader" → cs1
    SERVICE "Record Writer" → cs2
  proc3 type=PutS3Object
    REQUIRED "Bucket" default=<none>
    REQUIRED "Region" default="us-east-1" values=["us-east-1","us-west-2",...]

SERVICES:
  cs1 type=JsonTreeReader (no required properties without defaults)
  cs2 type=ParquetRecordSetWriter (no required properties without defaults)
```

---

## Request Type Matrix

| Request Type | Java Handles | LLM Handles | Tokens |
|--------------|-------------|-------------|--------|
| Simple flow (MQTT → S3) | Processor selection, topology, CS resolution, relationships | Property values, names, explanation | ~930 |
| Complex flow (5+ processors) | Same as above | Same as above (more properties) | ~1,500 |
| Canvas modification (add processor) | Insert position, topology update | Property values for new processor | ~800 |
| Deletion | Nothing (maybe name matching) | Identify targets from canvas list | ~300 |
| Enable/disable CS | Nothing | Identify target service | ~250 |
| Novel/ambiguous request | Nothing (fallback to current) | Full selection + configuration | ~3,000 |
| **Weighted average** | | | **~1,100** |

---

## Comparison: Current vs Redesigned

| Metric | Current | Redesigned | Improvement |
|--------|---------|-----------|-------------|
| Avg tokens per request | ~8,330 | ~1,100 | **87% reduction** |
| LLM calls per request (avg) | 1.3 (1 + 0.3×repair) | 1.0 | **23% fewer calls** |
| Worst case tokens | ~25,000 (repair with full context) | ~3,000 (fallback path) | **88% reduction** |
| System prompt size | 127 lines / 3,400 chars | 10 lines / 400 chars | **88% smaller** |
| Post-generation normalization | 978 LOC (2 normalizers) | 0 LOC | **Eliminated** |
| Repair pipeline | 540 LOC + 2nd LLM call | 0 LOC | **Eliminated** |
| Probability of needing repair | ~30% | 0% | **Eliminated** |
| Expected first-pass success | ~70% | ~95%+ | **25% improvement** |
| LLM failure modes | Wrong type, wrong relationship, wrong CS, wrong topology | Wrong property value only | **4 modes → 1 mode** |
| Time to first token (LLM latency) | ~2-4s (large context) | ~0.5-1s (tiny context) | **~75% faster** |
| Deterministic decisions | 3 (IDs, layout, scheduling) | 9 (types, IDs, CS, topology, relationships, layout, scheduling, names, relationships) | **3× more deterministic** |

---

## Migration Path

### Phase 1: Extract Topology from LLM (Week 1)
- Create `TopologyBuilder` — Java assigns connections and relationships
- Strip CONNECTIONS section from system prompt (save 560 tokens)
- Delete `TerminalLoggerNormalizer` (851 LOC)
- Delete `ParallelWorkerNormalizer` (127 LOC)
- Java post-processes: ignore LLM connections, build its own
- **Risk: LOW** — additive, LLM output still works, Java overrides it

### Phase 2: Create ProcessorSelector (Week 1-2)
- Build lookup table with top 30-50 processor patterns
- Wire before context rendering
- If selector succeeds → use its selection, skip ranking
- If selector fails → fall back to current ranking pipeline
- **Risk: LOW** — fallback preserves behavior

### Phase 3: Move CS Selection to Java (Week 2)
- After processors are selected, resolve CS deterministically
- Format keywords → implementation selection
- Remove CS rendering from prompt (save ~800 tokens)
- **Risk: LOW** — CS selection is already nearly deterministic

### Phase 4: Split LLM Role (Week 2-3)
- Change LLM call from "design full flow" to "configure properties"
- New minimal system prompt
- New structured input (property list only)
- New minimal output schema
- **Risk: MEDIUM** — changes LLM contract, needs testing

### Phase 5: Delete Repair Pipeline (Week 3)
- Convert validator to auto-correcting mode
- Delete RepairHintDeriver, RepairContextExpander
- Remove repair logic from CopilotController
- **Risk: LOW** — if Phase 4 works, repair is unnecessary

### Phase 6: Clean Up (Week 3-4)
- Delete dead code (normalizers, ranker, old renderer paths)
- Merge IntentExtractor remnants into ProcessorSelector
- Simplify FlowSpecificationValidator
- Reduce CapabilityPromptRenderer to property-only rendering
- **Risk: LOW** — code deletion only

---

## Summary

**The fundamental insight:** The current pipeline asks the LLM to be a NiFi architect (choose components, design topology, assign relationships, configure services, AND set properties). The redesigned pipeline asks the LLM to be a NiFi property configurator (given these components and this topology, what values should the properties have?).

The first task requires understanding NiFi's component model, relationship semantics, service APIs, and topology patterns — all of which are deterministic and fully described in the capability graph.

The second task requires understanding human intent ("broker at localhost:1883") and mapping it to property values — which is genuinely non-deterministic and is what LLMs excel at.

**Move every deterministic decision into Java. Leave only human-intent-to-value mapping for the LLM.**

---

*End of pipeline redesign.*
