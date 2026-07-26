# NiFi Dynamic Layout Improvement Plan

## Objective

Improve `nifi-layout-engine` so generated and updated NiFi flows remain readable
across linear, branching, converging, cyclic, nested, disconnected, and
high-degree topologies.

The implementation is based on Apache NiFi frontend dimensions, official flow
snapshots, and existing layout integration tests. It must preserve these
architectural boundaries:

- `nifi-layout-engine` changes geometry only. It never creates, deletes, or
  rewires processors.
- `nifi-copilot` may normalize a generated flow specification before
  deployment when an additional terminal logger is semantically required.
- Existing NiFi component configuration, relationships, IDs, revisions, and
  runtime state remain unchanged by layout.
- Core, REST, Copilot-profile, and full reactor builds must remain compatible.

## Status

| Phase | Tracker ID | Status |
| --- | --- | --- |
| 1. Topology fixtures | `add-nifi-topology-fixtures` | Complete |
| 2. Demand-aware spacing | `implement-demand-aware-spacing` | Complete |
| 3. Obstacle-aware routing | `implement-obstacle-aware-routing` | Complete |
| 4. Terminal logger partitioning | `partition-terminal-log-sinks` | Complete |
| 5. Quality validation | `validate-dynamic-layout-quality` | Complete |

## Dependency Flow

```text
Topology fixtures
├── Demand-aware spacing
│   └── Obstacle-aware routing
└── Terminal logger partitioning

Obstacle-aware routing + Terminal logger partitioning
└── Quality validation
```

---

## Phase 1: Apache NiFi Topology Fixtures

**Goal:** Establish deterministic regression fixtures before changing layout
behavior.

### 1.1 Engine topology fixtures

**Tracker:** `fixture-engine-topologies`

Create test-only `FlowTopologyFixtures` under the layout engine core tests.
Fixtures must use `ComponentDefaults`, including the NiFi processor dimensions
of `350x130`.

Required topologies:

- Linear processor pipeline
- Two-way fan-out
- Five-way load distribution
- Dense fan-in
- Sequential failure branch
- Parallel relationships between the same source and target
- Cycle, long backedge, and explicit self-loop
- Funnel fan-in
- Disconnected flows
- Input/output ports
- Remote process group
- Associated and unassociated labels
- Nested process groups

### 1.2 Copilot map fixtures

**Tracker:** `fixture-copilot-maps`

Create matching `processGroupFlow.flow` map fixtures for
`ProcessGroupFlowMapAdapter`.

Coverage:

- Entity-wrapped and plain component forms
- Shuffled input ordering
- Selected relationships
- Remote-port ownership
- Nested groups
- Stable node and edge ordering

**Depends on:** `fixture-engine-topologies`

### 1.3 Baseline invariants

**Tracker:** `fixture-baseline-invariants`

For every fixture, record:

- Expected node and edge counts
- Rank and degree shape
- Canonical component dimensions
- Relationship semantics
- Expected direct, branch, fan-in, loop, and backedge behavior
- Topology-specific spacing and bend constraints

**Depends on:** engine and Copilot fixtures.

### Phase 1 completion criteria

- Every fixture parses deterministically.
- Engine and Copilot forms produce equivalent topology shape.
- No fixture relies on copied arbitrary coordinates.
- Existing layout tests remain unchanged and pass.

---

## Phase 2: Demand-Aware Rank Spacing

**Goal:** Replace fixed branch-count spacing with bounded spacing calculated
from actual routing demand.

### 2.1 Rank-boundary demand model

**Tracker:** `spacing-rank-demand-model`

Add a value model in the core spacing package that captures:

- Maximum incoming and outgoing degree
- Number of parallel connection-label lanes
- Number of dense fan-in result buses
- Long-edge channel demand
- Label and port clearance requirements

`horizontalSpacing` and `verticalSpacing` must continue to mean clear
edge-to-edge gaps. Node height must not be counted twice.

### 2.2 Demand calculator

**Tracker:** `spacing-demand-calculator`

Calculate deterministic demand for every adjacent rank boundary from
`LayeredGraph` and `LayoutOptions`.

Constraints:

- Stable iteration order
- No graph mutation
- No unbounded expansion for extreme fan degree
- Simple one- and two-branch flows retain current compact spacing

### 2.3 Coordinate integration

**Tracker:** `spacing-coordinate-integration`

Integrate per-boundary spacing into `DefaultCoordinateAssigner`.

Preserve:

- Grid snapping
- Alignment modes
- Port placement
- Label placement
- Nested-group padding
- Incremental fixed-node behavior

### Phase 2 completion criteria

- Five-way fan-out receives enough rank clearance for all channels.
- Dense fan-in buses fit without touching processor bounds.
- Linear flows remain compact.
- No processor overlap is introduced.
- Spacing remains deterministic.

---

## Phase 3: Obstacle-Aware Deterministic Routing

**Goal:** Ensure connections do not pass through processors and use the
smallest clear deterministic route.

### 3.1 Obstacle geometry

**Tracker:** `routing-obstacle-geometry`

Add reusable geometry utilities for:

- Axis-aligned segment and bounding-box intersection
- Path intersection
- Configurable clearance
- Occupied graph bounds
- Source and destination exclusion

### 3.2 Direct-route clearance

**Tracker:** `routing-direct-clearance`

Use zero bends only when the direct source-to-target corridor is clear.
Otherwise, select the minimal clear orthogonal path.

### 3.3 Branch channel allocation

**Tracker:** `routing-channel-allocation`

Allocate deterministic channels for:

- Fan-out
- Fan-in
- Parallel source-target relationships
- Separate success and failure result buses

Connection label anchors for parallel relationships must remain at least
`240px` apart.

### 3.4 Loop and backedge routing

**Tracker:** `routing-backedge-boundary`

Route explicit self-loops and long backward/cycle edges outside occupied graph
bounds. Assign side lanes deterministically so multiple backedges do not
overlap.

### 3.5 Clear-route fallback

**Tracker:** `routing-obstacle-fallback`

If the preferred route intersects a component:

1. Evaluate a bounded set of deterministic candidate channels.
2. Choose the lowest-cost clear route.
3. Emit a layout warning if no clear route exists.

Do not silently accept a route through a processor.

### 3.6 Routing regression tests

**Tracker:** `routing-regression-tests`

Cover:

- Clear and blocked direct paths
- Two-way and five-way fan-out
- Dense success/failure fan-in
- Parallel labels
- Funnel fan-in
- Self-loops
- Multi-rank backward edges
- Disconnected flow boundaries

### Phase 3 completion criteria

- No route segment intersects a non-endpoint processor.
- Direct one-to-one connections remain zero-bend when clear.
- Parallel label lanes retain minimum clearance.
- Backedges remain outside occupied graph bounds.
- Equivalent input always produces identical routes.

---

## Phase 4: Copilot Terminal Logger Partitioning

**Goal:** Prevent distant stages from sharing a logger when doing so creates
long crossing routes, while retaining shared sinks for true parallel workers.

This phase belongs to `nifi-copilot`, not the layout engine.

### 4.1 Source classification

**Tracker:** `logger-source-classification`

Refactor `LlmClient` generated-layout normalization to classify logger
predecessors using reachability and rank:

- Parallel sibling workers
- Sequential stages
- Non-adjacent stages
- Invalid self-loops
- Terminal outgoing edges

### 4.2 Generated-spec partitioning

**Tracker:** `logger-generated-spec-partition`

Before `FlowBuilder` deployment:

- Clone only generated `LogAttribute` or `LogMessage` terminal processors when
  sequential stages require distinct sinks.
- Use stable deterministic IDs.
- Preserve processor configuration.
- Redirect only generated connections.
- Never alter already deployed topology during layout.

### 4.3 Parallel logger sharing

**Tracker:** `logger-parallel-sharing`

Preserve one shared logger per semantic outcome for true parallel siblings.
Respect explicit separate success/failure logging and existing logger reuse.
Do not create funnels solely for log aggregation.

### 4.4 Partitioning tests

**Tracker:** `logger-partition-tests`

Cover:

- `EvaluateJsonPath` and `InvokeHTTP` failures from sequential stages
- Parallel `InvokeHTTP` workers
- Separate success and failure outcomes
- Existing logger reuse
- Stable generated IDs
- Idempotent normalization
- No accidental self-loops

### Phase 4 completion criteria

- Sequential/non-adjacent stages receive stage-specific terminal loggers.
- Parallel siblings continue sharing loggers.
- Repeated normalization produces the same processor IDs and connections.
- The layout engine creates no processors.

---

## Phase 5: Dynamic Layout Quality Gates

**Goal:** Convert visual quality requirements into automated invariants.

### 5.1 Geometry assertions

**Tracker:** `quality-geometry-assertions`

Create shared test assertions for:

- Zero component overlaps
- Zero route-through-nonendpoint-node intersections
- Minimum parallel connection-label anchor separation
- Topology-specific bend limits
- Grid alignment
- Stable ordering

Do not use one global bend limit: direct, fan, dense fan-in, self-loop, and
backedge routes have different valid complexity.

### 5.2 Fixture quality suite

**Tracker:** `quality-fixture-suite`

Run every Phase 1 fixture through full layout and apply the relevant geometry
and routing invariants.

### 5.3 Property tests

**Tracker:** `quality-property-tests`

Add bounded jqwik generators for small valid graphs and verify:

- Deterministic positions and routes
- No component overlaps
- No route through processor interiors
- Useful failing seeds are retained

### 5.4 Incremental and profile validation

**Tracker:** `quality-incremental-profiles`

Verify:

- Unaffected disconnected components retain exact positions.
- Unchanged nested groups retain internal arrangement.
- Changed components and affected neighbors are re-routed.
- Core, REST, Copilot, and reactor builds remain compatible.

### Phase 5 completion criteria

- All topology fixtures satisfy their quality gates.
- Incremental layout preserves unaffected positions.
- Property tests are deterministic and repeatable.
- All supported Maven profiles pass.

---

## Validation Commands

On PowerShell, quote Maven properties containing dots.

```powershell
.\mvnw.cmd -pl nifi-layout-engine clean test
.\mvnw.cmd -pl nifi-layout-engine clean test "-Dlayout.core=true"
.\mvnw.cmd -pl nifi-layout-engine clean test "-Dlayout.rest=true"
.\mvnw.cmd -pl nifi-layout-engine clean test "-Dlayout.copilot=true"
.\mvnw.cmd -pl nifi-copilot -am test
git --no-pager diff --check
```

## Behavior-Preservation Constraints

- Do not change NiFi processor properties to improve visual layout.
- Do not duplicate non-logging processors.
- Do not use funnels as a cosmetic routing workaround.
- Do not route one processor relationship to multiple NiFi connections.
- Preserve interior-only NiFi bend-point write-back.
- Preserve direct routes when unobstructed.
- Preserve explicit self-loops and reject accidental self-loops.
- Preserve deterministic ordering and stable generated IDs.
- Do not introduce new build or testing tools.

## Execution Order

Phases 1 through 5 are complete. There are no remaining tracker items in this
plan.
