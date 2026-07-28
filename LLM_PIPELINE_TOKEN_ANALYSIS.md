# LLM Pipeline Token Analysis — NiFi Copilot
**Date:** 2026-07-26  
**Objective:** Reduce LLM context to < 3,000 tokens for normal flow generation  

---

## 1. Complete LLM Call Inventory

The system makes **1-2 LLM calls per user request:**

### Call 1: Primary Flow Generation

| Field | Value |
|-------|-------|
| **Purpose** | Generate NiFi flow specification from user intent |
| **Provider** | GitHub Models API (GPT-4o) or AWS Bedrock (Claude Sonnet) |
| **Temperature** | 0.2 |
| **History** | Last 6 messages |
| **Response format** | JSON mode (for GPT models) |

### Call 2: Repair Generation (conditional — on validation failure)

| Field | Value |
|-------|-------|
| **Purpose** | Fix a rejected flow specification using expanded context |
| **Provider** | Same as Call 1 |
| **Temperature** | 0.2 |
| **History** | Empty (cleared) |
| **Trigger** | `FlowSpecificationValidationException` from Call 1 |
| **Additional input** | Original request + validation issues + rejected spec + expanded capability context (16,000 chars!) |

---

## 2. Token-by-Token Breakdown — Call 1 (Primary Generation)

### System Message Components

| Section | Lines | Chars | ~Tokens | Classification | Analysis |
|---------|-------|-------|---------|---------------|----------|
| Role preamble | 2 | 120 | 35 | **Required** | Identity + output format instruction |
| OUTPUT SCHEMA | 10 | 680 | 200 | **Required** | Defines JSON structure |
| Field descriptions (process_group, parameter_context, controller_services) | 7 | 580 | 170 | **Required** | When to include each field |
| Deletions field description | 4 | 340 | 100 | **Required** | Delete semantics |
| cs_actions field description | 3 | 270 | 80 | **Required** | Enable/disable semantics |
| CANVAS LAYOUT rule | 2 | 110 | 32 | **Required** | "Don't include x/y" — one line needed |
| **CONNECTIONS rules** | **22** | **1,900** | **560** | **REDUNDANT** | Java already enforces ALL of these via TerminalLoggerNormalizer + ParallelWorkerNormalizer + normalizeGeneratedLayout |
| EXPLANATION RULES | 2 | 170 | 50 | **Optional** | Nice-to-have formatting guide |
| PROCESSOR CONFIG rules | 14 | 1,050 | 310 | **Partially Required** | Lines 101-106 required. Lines 107-113 are processor-specific hints = **WASTEFUL** |
| CANVAS CONTEXT rules | 6 | 470 | 140 | **Required** | How to handle existing components |
| DELETIONS rules | 4 | 310 | 90 | **Required** | Delete behavior |

**System prompt subtotal:** ~86 lines, ~6,000 chars, **~1,770 tokens**

### Capability Context (appended to system message)

| Section | Typical Items | Chars | ~Tokens | Classification | Analysis |
|---------|--------------|-------|---------|---------------|----------|
| HEADER | 1 | 190 | 55 | **Required** | "Use only exact types below" |
| PROCESSOR lines | 3-6 processors | 300-600 | 90-180 | **Required** | Type declarations |
| REQUIRED_CONTROLLER_SERVICE_API | 1-3 APIs | 200-600 | 60-180 | **Required** | API bindings |
| CONTROLLER_SERVICE lines | 1-4 services | 150-400 | 45-120 | **Required** | Service type declarations |
| UNRESOLVED_REQUIRED_API | 0-2 | 0-200 | 0-60 | **Optional** | Usually empty |
| **PROCESSOR_PROPERTIES (required)** | 10-25 props | 1,500-3,500 | 440-1,030 | **Required** | Display names + defaults + service refs |
| **PROCESSOR_RELATIONSHIPS** | 3-6 processors | 300-900 | 90-265 | **REDUNDANT** | Java can set relationships from the graph — LLM doesn't need them |
| **PROCESSOR_PROPERTY_DETAILS (required)** | 5-15 props | 800-2,000 | 235-590 | **Partially Redundant** | Allowable values needed. Dependencies mostly not (Java can enforce). |
| **DATA_CONTRACT properties** | 3-8 props | 400-1,200 | 120-350 | **Optional** | Nice-to-have for format configuration |
| **OPTIONAL properties** | 5-15 props | 600-2,000 | 175-590 | **WASTEFUL** | LLM rarely sets these correctly anyway |
| **RUNTIME metadata** | 3-8 entries | 300-800 | 90-235 | **WASTEFUL** | inputRequirement, scheduling, triggerSerially — Java handles all of this |
| **RUNTIME properties** | 2-6 props | 200-600 | 60-175 | **WASTEFUL** | Same reason |

**Capability context subtotal (typical 4-processor flow):** ~8,000-12,000 chars, **~2,350-3,530 tokens**

### User Message Components

| Section | Chars | ~Tokens | Classification |
|---------|-------|---------|---------------|
| [CANVAS CONTEXT] header | 20 | 6 | Required (when canvas non-empty) |
| Existing processors (5 entries) | 300 | 90 | Required (when present) |
| Existing connections | 200 | 60 | **WASTEFUL** — Java owns topology |
| Existing process groups | 100 | 30 | Optional |
| Existing controller services | 200 | 60 | Required |
| [USER REQUEST] + message | 50-300 | 15-90 | **Required** |

**User message subtotal (with canvas):** ~870 chars, **~350 tokens**

### Conversation History

| Section | Chars | ~Tokens | Classification |
|---------|-------|---------|---------------|
| Last 6 messages (3 user + 3 assistant) | ~6,000 | **~1,760** | **MOSTLY WASTEFUL** — each assistant response is a full JSON spec |

**History subtotal:** **~1,760 tokens**

---

## 3. Total Token Budget — Current State

### Scenario A: Simple flow, empty canvas, no history

| Component | Tokens |
|-----------|--------|
| System prompt | 1,770 |
| Capability context | 2,800 |
| User message | 50 |
| **Total input** | **4,620** |
| Output (JSON spec) | 800-1,200 |
| **Grand total** | **~5,600** |

### Scenario B: Flow with canvas context + history

| Component | Tokens |
|-----------|--------|
| System prompt | 1,770 |
| Capability context | 3,200 |
| Canvas context | 350 |
| History (6 messages) | 1,760 |
| User message | 60 |
| **Total input** | **7,140** |
| Output | 1,000 |
| **Grand total** | **~8,140** |

### Scenario C: Repair call (on validation failure)

| Component | Tokens |
|-----------|--------|
| System prompt | 1,770 |
| Repair capability context (16,000 chars!) | 4,700 |
| Repair message (original + issues + rejected spec) | 2,500 |
| Canvas context | 350 |
| **Total input** | **9,320** |
| Output | 1,200 |
| **Grand total** | **~10,520** |

### Scenario D: Worst case (canvas + history + repair)

| Component | Tokens |
|-----------|--------|
| Call 1 input | 7,140 |
| Call 1 output | 1,000 |
| Call 2 input | 9,320 |
| Call 2 output | 1,200 |
| **Grand total both calls** | **~18,660** |

---

## 4. Section-by-Section Classification

### System Prompt Sections

| Section | Tokens | Verdict | Reason |
|---------|--------|---------|--------|
| Role + format instruction | 35 | ✅ **Required** | Cannot remove |
| OUTPUT SCHEMA | 200 | ✅ **Required** | Defines expected JSON |
| Field explanations (PG, PC, CS) | 170 | ✅ **Required** | Tells LLM when to include each |
| Deletion field explanation | 100 | ✅ **Required** | Delete semantics |
| cs_actions explanation | 80 | ✅ **Required** | Enable/disable semantics |
| CANVAS LAYOUT (1 line) | 32 | ✅ **Required** | "No x/y" |
| **CONNECTIONS section** | **560** | ❌ **REDUNDANT** | Every rule is enforced by Java post-processing. Self-loops → removed by `normalizeGeneratedLayout`. Terminal outgoing → removed. Logger sharing → `TerminalLoggerNormalizer`. DistributeLoad → `ParallelWorkerNormalizer`. InvokeHTTP relationships → `FlowSpecificationValidator`. |
| EXPLANATION RULES | 50 | ⚠️ **Optional** | Could be 1 line: "Plain text, summarize flow" |
| PROCESSOR CONFIG (lines 101-105) | 150 | ✅ **Required** | Key semantics (display names, CS refs, params) |
| PROCESSOR CONFIG (lines 106-113) | 160 | ❌ **WASTEFUL** | Processor-specific hints (ConsumeMQTT, MergeRecord, DistributeLoad). These are per-processor knowledge that should come from capability context, not a global prompt. |
| CANVAS CONTEXT rules | 140 | ✅ **Required** | How to handle existing components |
| DELETIONS rules | 90 | ✅ **Required** | Delete behavior |

### Capability Context Sections

| Section | Tokens (typical) | Verdict | Reason |
|---------|---------|---------|--------|
| Header | 55 | ✅ **Required** | Authority declaration |
| PROCESSOR type lines | 120 | ✅ **Required** | Processor selection |
| API binding lines | 100 | ✅ **Required** | Service API declarations |
| CONTROLLER_SERVICE lines | 80 | ✅ **Required** | Service types |
| Required properties (names, defaults, CS refs) | 700 | ✅ **Required** | LLM needs to set these values |
| **PROCESSOR_RELATIONSHIPS** | **180** | ❌ **REDUNDANT** | Java knows relationships from the graph. LLM uses them for connections, but Java should own connections. |
| Required property DETAILS (allowable values) | 400 | ⚠️ **Partially Required** | Allowable values: YES. Dependencies: NO (Java enforces). |
| DATA_CONTRACT properties | 250 | ⚠️ **Optional** | Useful for format config (reader/writer types). Keep for now. |
| **OPTIONAL properties** | **380** | ❌ **WASTEFUL** | LLM rarely sets optional properties correctly. If it does, validator often rejects them. Net negative value. |
| **RUNTIME metadata** | **160** | ❌ **WASTEFUL** | inputRequirement, scheduling strategies, triggerSerially. Java handles all scheduling. LLM never uses this. |
| **RUNTIME properties** | **120** | ❌ **WASTEFUL** | Same. Scheduling period, concurrent tasks, etc. are Java-owned. |

### User Message Sections

| Section | Tokens | Verdict | Reason |
|---------|--------|---------|--------|
| [CANVAS CONTEXT] processors | 90 | ✅ **Required** | LLM must know what exists |
| [CANVAS CONTEXT] connections | 60 | ❌ **REDUNDANT** | If Java owns connections, LLM doesn't need to see existing ones |
| [CANVAS CONTEXT] process groups | 30 | ⚠️ **Optional** | Only for deletion references |
| [CANVAS CONTEXT] controller services | 60 | ✅ **Required** | For cs_actions and service reuse |
| User message | 50-90 | ✅ **Required** | The actual request |

### History

| Section | Tokens | Verdict | Reason |
|---------|--------|---------|--------|
| Last 6 messages | 1,760 | ❌ **MOSTLY WASTEFUL** | Each assistant message is a full JSON spec (600-800 tokens). User messages are ~30 tokens each. The JSON specs are noise — the validator already rejected or accepted them. History is only useful for multi-turn conversation ("now add a LogAttribute"). |

**Smarter history:** Keep only the last 2 user messages (not assistant JSON responses). ~60 tokens.

---

## 5. Token Removal Potential

| Item to Remove/Reduce | Current Tokens | Saving |
|----------------------|---------------|--------|
| CONNECTIONS section (22 lines) | 560 | **-560** |
| Processor-specific hints (lines 106-113) | 160 | **-160** |
| PROCESSOR_RELATIONSHIPS in capability context | 180 | **-180** |
| OPTIONAL properties | 380 | **-380** |
| RUNTIME metadata | 160 | **-160** |
| RUNTIME properties | 120 | **-120** |
| Canvas connections in user message | 60 | **-60** |
| History (6 msgs → 2 user-only) | 1,760 → 60 | **-1,700** |
| Explanation rules (2 lines → 0) | 50 | **-50** |
| EXPLANATION RULES embedded in prompt | 50 | **-50** |
| Property DETAILS dependencies (keep allowable values only) | ~150 | **-150** |
| **Total removable** | | **-3,570** |

---

## 6. What Can Java Do Instead?

| Currently LLM decides | Java alternative | Tokens saved |
|----------------------|------------------|------|
| **Connection topology** — LLM generates `connections[]` array | Java builds connections from processor ordering in `processors[]` array. First→second, second→third, etc. Use success relationship by default. | -560 (prompt rules) + -180 (relationships in context) |
| **Relationship names** — LLM picks from capability context | Java looks up relationships in CapabilityGraph. First non-failure relationship = primary connection. | -180 |
| **Terminal logger placement** — LLM decides where to put LogAttribute | Java appends LogAttribute after last processor(s) with failure/success routing. | -200 (prompt rules about loggers) |
| **DistributeLoad wiring** — LLM creates N connections | Java detects DistributeLoad and wires 1-through-N relationships. | -100 (prompt rules) |
| **Controller service type selection** — LLM picks type from API implementations | Java selects first compatible implementation from CapabilityGraph.serviceImplementations(). | -80 (API lines in context) |
| **Scheduling configuration** — LLM sets scheduling period, concurrent tasks | Java uses defaults. User can override via property. | -280 (runtime metadata + properties) |

---

## 7. What Can Be Cached?

| Item | Currently | Cacheable? | Cache Strategy |
|------|-----------|-----------|---------------|
| Capability context string | Rendered per-request based on user intent | **YES** | Cache by normalized intent signature (source×sink×transforms). ~20 distinct patterns cover 95% of requests. TTL = same as capability cache (15 min). |
| System prompt | Static string literal | **Already cached** | It's a `static final`. |
| Processor type resolution | Done in IntentExtractor + ProcessorSeedRanker per request | **YES** | Cache intent→processor[] mapping. The same intent always maps to the same processors. |
| Canvas context | Read from NiFi per request | **No** | Canvas changes constantly. Must be live. |
| History | Stored in session | N/A | Already persisted in SessionStore. |

---

## 8. Repair Call Analysis

### Why the repair call exists:

| Failure mode | Frequency | Root cause |
|-------------|-----------|-----------|
| Invalid processor type | High | LLM hallucinated a type not in capability context |
| Wrong relationship name | Medium | LLM guessed "success" instead of "Response" |
| Missing required property | Medium | Capability context included type but not all properties |
| Invalid CS type for API | Low | LLM picked wrong implementation |
| Invalid allowable value | Low | LLM invented a value not in the list |

### Repair call token cost:

| Component | Tokens |
|-----------|--------|
| System prompt (same) | 1,770 |
| Repair capability context (expanded to 16,000 chars!) | 4,700 |
| Repair message (original + issues + rejected spec) | 2,500 |
| Canvas context | 350 |
| **Total input** | **9,320** |

### Can the repair call be eliminated?

**YES** — if:
1. Java selects processors (no hallucination)
2. Java sets relationships (no wrong names)
3. Java selects CS types (no wrong implementations)
4. Capability context includes ALL required properties with allowable values
5. Validator auto-corrects minor issues (typos in property values) instead of rejecting

With these changes, the only possible failure is: LLM provides an invalid property value that has allowable values defined. This can be handled by Java selecting the closest match or the default, not by a 9,320-token LLM call.

---

## 9. Proposed Minimal Pipeline

### New LLM Responsibility

The LLM's job shrinks to: **"Given these processors and their properties, fill in the property values based on the user's request."**

### New System Prompt (~40 lines, ~450 tokens)

```
You are a NiFi property configurator. Return ONE JSON object, no markdown.

=== SCHEMA ===
{
  "explanation": "<1-2 sentence summary>",
  "processors": [
    {"id": "proc1", "name": "<descriptive name>", "config": {"<Property Display Name>": "<value>"}}
  ],
  "parameter_context": {"name": "...", "parameters": {"key": "value"}},
  "deletions": [{"type": "processor|process_group|controller_service|parameter_context", "spec_id": "...", "name": "..."}],
  "cs_actions": [{"name": "...", "action": "enable|disable"}]
}

=== RULES ===
- Processor types and connections are determined by the backend. You only configure properties.
- Each processor in SELECTED PROCESSORS must appear in your output with configured properties.
- Use property display names as keys.
- Reference controller services by spec id (e.g. "cs1").
- Reference parameters as #{param_name}.
- Use placeholder values (e.g. "#{my_broker_uri}") when the user didn't provide a specific value.
- Supply every listed required property.
- For deletions: match from CANVAS CONTEXT by spec_id or name.
- For cs_actions: use exact service name from CANVAS CONTEXT.
- Do not invent properties not listed in SELECTED PROCESSORS.
```

### New Capability Context (~1,200 tokens for 4-processor flow)

```
[SELECTED PROCESSORS]
These are the exact processors the backend will deploy. Configure their properties.

PROCESSOR id=proc1 type=org.apache.nifi.processors.mqtt.ConsumeMQTT
  REQUIRED_PROPERTY display="Broker URI" default=<none>
  REQUIRED_PROPERTY display="Topic Filter" default=<none>  
  REQUIRED_PROPERTY display="Quality of Service(QoS)" default=0 allowable=[0/At most once, 1/At least once, 2/Exactly once]
  OPTIONAL_PROPERTY display="Username" default=<none>
  OPTIONAL_PROPERTY display="Password" default=<none>

PROCESSOR id=proc2 type=org.apache.nifi.processors.standard.ConvertRecord
  REQUIRED_SERVICE display="Record Reader" api=RecordReaderFactory implementations=[JsonTreeReader, CSVReader, AvroReader]
  REQUIRED_SERVICE display="Record Writer" api=RecordSetWriterFactory implementations=[JsonRecordSetWriter, CSVRecordSetWriter, AvroRecordSetWriter]

PROCESSOR id=proc3 type=org.apache.nifi.processors.aws.s3.PutS3Object
  REQUIRED_PROPERTY display="Bucket" default=<none>
  REQUIRED_PROPERTY display="Region" default=us-east-1 allowable=[us-east-1/..., us-west-2/..., ...]
  REQUIRED_SERVICE display="AWS Credentials Provider" api=AWSCredentialsProviderService implementations=[AWSCredentialsProviderControllerService]

CONTROLLER_SERVICE id=cs1 type=org.apache.nifi.json.JsonTreeReader
  OPTIONAL_PROPERTY display="Starting Field Strategy" default="Root Node" allowable=[Root Node/Root Node, Nested Field/Nested Field]
```

### New User Message (~60 tokens when canvas empty, ~150 with canvas)

```
[CANVAS CONTEXT]
  spec_id=abc name=Existing Proc type=...

[USER REQUEST]
Stream MQTT messages from broker://iot to S3 bucket my-data in Parquet format
```

### New History (0-60 tokens)

Only include the last 1-2 **user** messages (no assistant JSON). The LLM doesn't need to see its previous output — that's already deployed on canvas and visible in CANVAS CONTEXT.

---

## 10. New Token Budget

### Scenario A: Simple flow, empty canvas

| Component | Tokens |
|-----------|--------|
| System prompt | 450 |
| Capability context (4 processors) | 1,200 |
| User message | 50 |
| **Total input** | **1,700** |
| Output (property config only) | 400-600 |
| **Grand total** | **~2,200** |

### Scenario B: Flow with canvas context

| Component | Tokens |
|-----------|--------|
| System prompt | 450 |
| Capability context (4 processors) | 1,200 |
| Canvas context (5 existing) | 100 |
| Previous user message | 40 |
| User message | 50 |
| **Total input** | **1,840** |
| Output | 500 |
| **Grand total** | **~2,340** |

### Scenario C: Complex flow (6 processors + 3 services)

| Component | Tokens |
|-----------|--------|
| System prompt | 450 |
| Capability context (6 processors + 3 services) | 2,000 |
| Canvas context | 100 |
| User message | 80 |
| **Total input** | **2,630** |
| Output | 700 |
| **Grand total** | **~3,330** |

### No repair call needed.

---

## 11. Comparison: Current vs Proposed

| Metric | Current | Proposed | Reduction |
|--------|---------|----------|-----------|
| System prompt tokens | 1,770 | 450 | **-75%** |
| Capability context tokens | 2,800 | 1,200 | **-57%** |
| History tokens | 1,760 | 40 | **-98%** |
| Canvas context tokens | 350 | 100 | **-71%** |
| **Total input (typical)** | **6,680** | **1,790** | **-73%** |
| Output tokens | 1,000 | 500 | **-50%** |
| **Total per-request (typical)** | **7,680** | **2,290** | **-70%** |
| Repair call probability | ~30% | 0% | **-100%** |
| Repair call tokens (when triggered) | 10,520 | 0 | **-100%** |
| **Expected average total** | **~10,800** | **~2,290** | **-79%** |

---

## 12. What Moves from Prompt to Java

| Responsibility | Tokens Removed from Prompt | Java LOC Added |
|---------------|---------------------------|---------------|
| Connection topology | 560 + 180 = 740 | ~80 (ConnectionBuilder) |
| Relationship selection | 180 | 0 (already in CapabilityGraph) |
| Terminal logger placement | 200 | ~30 (append LogAttribute) |
| DistributeLoad wiring | 100 | ~20 (detect + wire numbered rels) |
| Controller service type selection | 80 | ~10 (first compatible impl) |
| Scheduling/runtime config | 280 | 0 (use defaults) |
| Repair pipeline | 9,320 (full call) | 0 (eliminated) |
| History pruning | 1,700 | 0 (just stop sending) |
| Optional/runtime properties | 500 | 0 (just stop sending) |

**Total: ~13,100 tokens removed. ~140 LOC added to Java.**

---

## 13. Risks and Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| LLM can't infer processor order without connections | Low | Processors are already ordered in the `processors[]` array by the LLM. Java uses that order for connections. |
| Complex branching flows (RouteOnAttribute → multiple paths) | Medium | For route-based flows, have Java add a follow-up: "For processor X, list which named routes connect to which downstream processors." Or: have the LLM return a simple `"flow": ["proc1→proc2", "proc2→proc3→proc4"]` adjacency hint (~50 tokens). |
| User requests specific connection topology | Low | Parse from user message. If user says "connect failure to retry queue", Java can handle that from intent. |
| Loss of multi-turn context | Low | Canvas context already shows deployed state. Previous user messages give intent continuation. |
| LLM output quality drops without full property list | Medium | Include ALL required properties in context. Only drop OPTIONAL and RUNTIME. Required properties are what matter. |

---

## 14. Implementation Order

### Step 1: Remove wasteful prompt sections (immediate, no risk)
- Delete CONNECTIONS section from system prompt (-560 tokens)
- Delete RUNTIME metadata from capability context (-160 tokens)
- Delete RUNTIME properties from capability context (-120 tokens)
- Delete OPTIONAL properties from capability context (-380 tokens)
- Reduce history to last 2 user messages only (-1,700 tokens)
- Remove connections from canvas context (-60 tokens)

**Savings: -2,980 tokens. Zero Java changes needed.**

### Step 2: Remove relationships from capability context
- Delete PROCESSOR_RELATIONSHIPS section (-180 tokens)
- Java already has this data in CapabilityGraph

**Savings: -180 tokens. Zero Java changes needed.**  
**Requires:** Java sets relationships on connections (already done by validator).

### Step 3: Move processor selection to Java
- Java selects processors from intent (IntentExtractor already classifies intent)
- Capability context becomes "here are the exact processors, configure them"
- System prompt shrinks to new minimal version (-1,320 tokens from prompt)

**Savings: -1,320 tokens. ~60 LOC Java (processor lookup table).**

### Step 4: Move connection generation to Java
- Remove `connections[]` from LLM output schema
- Java generates connections from processor order
- Delete `ParallelWorkerNormalizer` + `TerminalLoggerNormalizer`

**Savings: Eliminates ~978 LOC of normalizers. Removes connection-related prompt rules.**

### Step 5: Eliminate repair pipeline
- With Java owning type selection, hallucination becomes impossible
- Delete `RepairHintDeriver` + `RepairContextExpander`
- Remove repair logic from controller

**Savings: Eliminates 450 LOC + entire repair call (9,320 tokens when triggered).**

---

## 15. Final State: Minimal Pipeline Architecture

```
User Request
    │
    ▼
IntentExtractor.extract()          [Java, 60 LOC]
    │
    ├─ sources: [MQTT]
    ├─ sinks: [S3]  
    ├─ transforms: [CONVERT]
    ├─ formats: [JSON, PARQUET]
    │
    ▼
ProcessorSelector.select()          [Java, 80 LOC, lookup table]
    │
    ├─ ConsumeMQTT
    ├─ ConvertRecord  
    ├─ PutS3Object
    ├─ (auto) JsonTreeReader, ParquetRecordSetWriter
    │
    ▼
MinimalContextRenderer.render()     [Java, 100 LOC]
    │
    ├─ Only: processor types + required properties + allowable values
    ├─ Budget: 4,000 chars max
    │
    ▼
LlmClient.generate()               [~2,000 tokens total]
    │
    ├─ System prompt: 450 tokens
    ├─ Context: 1,200 tokens
    ├─ User message: 50-100 tokens
    │
    ▼
LLM Response: property values + names + explanation
    │                                [~500 tokens]
    ▼
PropertyValidator.validate()        [Java, 100 LOC]
    │
    ├─ Check: all required properties present
    ├─ Check: allowable values respected
    ├─ Auto-fix: missing defaults → apply default
    │
    ▼
ConnectionBuilder.build()           [Java, 80 LOC]
    │
    ├─ Wire processors in order (success relationship)
    ├─ Add terminal LogAttribute
    ├─ Handle DistributeLoad pattern
    │
    ▼
FlowDeploymentCoordinator.deploy()  [existing, unchanged]
```

**Total input tokens: ~1,700-2,600**  
**Total output tokens: ~400-700**  
**Grand total: ~2,100-3,300 ✅ (under 3,000 for simple flows)**

---

## 16. Summary

| Current State | Problem |
|--------------|---------|
| 127-line system prompt | Teaching the LLM deterministic rules that Java enforces anyway |
| 12,000-char capability context | Sending optional/runtime properties the LLM never uses correctly |
| 6 message history | Each assistant message is 600+ tokens of JSON the LLM already generated |
| Repair call (16,000 char context) | Calling the LLM twice because it hallucinated in call 1 |
| CONNECTIONS rules (22 lines) | Every rule has a corresponding Java enforcer that runs regardless |
| RELATIONSHIPS in context | LLM uses these for connections, but Java should own connections |

| Proposed State | Benefit |
|---------------|---------|
| 40-line system prompt | Only property configuration rules |
| 4,000-char capability context | Only required properties + allowable values |
| 0-2 user messages (no assistant responses) | Just intent continuation |
| No repair call | Java can't hallucinate processor types |
| No connection rules | Java owns topology |
| No relationships in context | Java reads them from CapabilityGraph |

**Bottom line: The project currently spends ~13,000 tokens per request (with repair probability) to get the LLM to make deterministic decisions. The fix is not better prompts. The fix is fewer prompts.**

---

*End of analysis.*
