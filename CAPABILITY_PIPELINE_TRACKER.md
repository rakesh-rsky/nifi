# NiFi Copilot Capability Pipeline Tracker

## Objective

Improve first-pass flow validity and repair success by selecting, expanding, ranking, and rendering
only the installed NiFi capabilities relevant to the user's intent. Preserve the existing discovery,
validation, single-repair, and deployment architecture.

## Constraints

- Preserve `CapabilitySnapshot`, discovery caching, full validation, and fail-closed deployment.
- Preserve one bounded LLM repair attempt.
- Do not introduce workflow templates or send every discovered capability.
- Build all graph edges from discovered NiFi metadata; heuristics may rank discovered capabilities
  but must not invent component types.
- Keep the existing `MAX_CONTEXT_CHARS` limit.

## Target Pipeline

```text
Discovery/cache
    -> CapabilitySnapshot
    -> cached CapabilityGraph

User prompt
    -> IntentExtractor
    -> seed processor ranking
    -> dependency closure
    -> bounded capability rendering
    -> LLM generation
    -> full validation
    -> structured repair hints (only when invalid)
    -> repair context expansion
    -> one LLM repair
    -> full validation
    -> deploy or reject
```

## Capability Graph Contract

The graph is an LLM-independent, immutable knowledge model built from one `CapabilitySnapshot`.

```text
CapabilityGraph
  processorsByType
  servicesByType
  uniqueSimpleNames
  serviceImplementationsByApi

ProcessorNode
  type, name, description, tags/categories
  properties
  requiredControllerServiceApis
  optionalControllerServiceApis
  relationships
  schedulingConstraints

ControllerServiceNode
  type, name, description
  properties
  implementedApis
  requiredDependentApis
  optionalDependentApis

PropertyNode
  internalName, displayName, type
  required, sensitive, defaultValue
  allowableValues, dependencies
  requiredControllerServiceApi
```

No new metadata source is required. Nodes and edges adapt existing `ProcessorCapability`,
`ControllerServiceCapability`, and `PropertyCapability` data rather than duplicating discovery.

## Implementation Tracker

| Status | Task | Dependencies | Scope |
| --- | --- | --- | --- |
| [x] | CapabilityGraph model | None | Define immutable processor, service, property, API, relationship, and scheduling indexes with deterministic lookup by FQN and unique simple name. |
| [x] | CapabilityGraph builder | CapabilityGraph model | Build graph nodes and API edges from `CapabilitySnapshot`; classify required versus optional service dependencies and test chained APIs. |
| [x] | CapabilityGraph cache | CapabilityGraph builder | Cache one graph per discovered snapshot/cache generation so requests query the graph instead of rebuilding it. Preserve existing cache invalidation behavior. |
| [x] | Intent extraction | CapabilityGraph cache | Convert prompt terms into structured workflow signals such as source, format, transform, sink, batching, archive, logging, and concurrency. Keep this deterministic and independent of the generation LLM. |
| [x] | Seed processor ranking | Intent extraction | Score only installed processors against intent, name, description, tags/categories, and role. Select a small top set with deterministic tie-breaking. |
| [x] | Dependency closure resolver | Seed processor ranking | Traverse required APIs to ranked installed implementations, include chained service dependencies and essential properties, and apply a bounded depth and candidate count. |
| [x] | Property classification and ranking | CapabilityGraph builder | Classify required/API-bearing/data-contract properties as essential and scheduling/bulletin/yield-style settings as runtime metadata. Never hide required properties. |
| [x] | Bounded capability renderer | Dependency closure resolver; Property classification and ranking | Render candidates by fixed priority and evict only lower-priority details when `MAX_CONTEXT_CHARS` is reached. |
| [x] | Structured repair hints | CapabilityGraph builder | Convert validator results into typed `RepairHint` data containing issue type, affected component/path, required API, compatible installed implementations, and suggested properties. Do not make repair selection parse free-form messages. |
| [x] | Repair context expander | Bounded capability renderer; Structured repair hints | Pin original selections and merge only capabilities required by repair hints and rejected component references. Resolve API-like invented service names through graph indexes. |
| [x] | Controller repair wiring | Repair context expander | Retain the request snapshot/graph through the existing one-repair path without changing validation, deployment, or failure semantics. |
| [x] | Metrics and regression coverage | Controller repair wiring | Add selection/expansion/size/validation/repair metrics and focused graph, ranking, CSV/PostgreSQL, chained dependency, deterministic rendering, repair, and budget tests. Avoid recording prompt content or sensitive property values. |
| [x] | Module validation | Metrics and regression coverage | Run focused capability/controller tests and compile `nifi-copilot`; resolve only failures introduced by this work. |

## Ranking Rules

Processor and service ranking must be deterministic and operate only on installed capabilities.
Signals include exact format/type matches, workflow role, required API compatibility, name,
description, tags/categories, and whether a candidate was explicitly referenced by validation.

For example, CSV intent should rank `CSVReader` above generic JSON, Avro, and free-form readers.
Dependency expansion should render only the highest-ranked compatible implementations, not every
implementation of an API.

## Rendering Priority and Budget

Render in this order:

1. Selected processors.
2. Required controller-service APIs and concrete implementations.
3. Required and API-bearing properties.
4. Supported relationships.
5. Optional data-contract properties.
6. Examples or usage guidance.
7. Descriptions and runtime properties.

When the budget is exceeded, remove descriptions, examples, runtime properties, and optional
properties in that order. Never remove a selected component's required API, compatible concrete
implementation, required property, or supported relationship.

## Structured Repair Contract

`ValidationIssue` remains the validator's external result. A graph-aware adapter derives typed
repair data without changing the validator's authority:

```text
RepairHint
  issueType
  affectedComponentId
  affectedPath
  rejectedTypeOrReference
  requiredApi
  compatibleImplementationTypes
  suggestedPropertyNames
  supportedRelationships
```

Repair rendering consumes these fields directly. Human-readable reasons and fixes remain available
for the LLM and user, but they are not the source of capability expansion decisions.

## Metrics

Record aggregate, non-sensitive values for:

- Extracted intent categories.
- Seed and expanded processor counts/types.
- Expanded controller-service counts/types.
- Capability prompt size and repair expansion size.
- Validation issue types and counts.
- First-pass validity, repair attempted, and repair success.
- Repair input/output token counts when available.

## Complexity and Token Impact

- Graph build: `O(P + S + R)`, where `P` is processor metadata, `S` is service metadata, and `R`
  is property/API relationships. This occurs once per snapshot generation.
- Request selection: linear scoring over graph nodes plus bounded closure traversal.
- Rendering: linear in selected closure size, capped by the existing character budget.
- Repair: linear in validation issues plus bounded graph lookups.
- Prompt size should remain near current levels. Repair may add roughly 500-3,000 characters but
  remains under `MAX_CONTEXT_CHARS`.

## Acceptance Criteria

- CSV-to-PostgreSQL intent ranks installed file/CSV/database capabilities and concrete connection
  pools without inventing `DBCPService`.
- Selecting `ConvertRecord` discovers required reader/writer APIs and renders only top-ranked
  compatible implementations.
- Required service dependencies, essential properties, and relationships survive budget pressure.
- Runtime properties and descriptions are omitted before required capability data.
- Repair expansion is driven by typed hints, not parsing free-form validation text.
- Unknown API-like service types map to installed compatible implementations when available.
- The graph is reused across requests until the underlying capability snapshot changes.
- Metrics explain selection size, prompt size, validation failures, and repair outcomes without
  exposing user prompts, secrets, or sensitive property values.
- Invalid repaired flows remain rejected before NiFi canvas changes.
- Existing discovery, validation, deployment, and caching behavior remains backward compatible.

## Validation Commands

```powershell
.\mvnw.cmd -pl nifi-copilot -Dtest=CapabilityPromptRendererTest,CopilotControllerCapabilityTest test
.\mvnw.cmd -pl nifi-copilot -DskipTests compile
```
