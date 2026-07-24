<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Implementation Plan: NiFi Auto Layout Engine

## Overview

This implementation plan breaks the NiFi Auto Layout Engine into incremental tasks ordered by dependency: foundational models and utilities first, then graph algorithms, layout algorithms, orchestration services, NiFi-specific adapters, and finally integration wiring. Each task builds on prior work and references specific requirements for traceability.

## Tasks

- [x] 1. Set up project structure and foundational model classes
  - [x] 1.1 Create Maven/Gradle project with Java 21, configure jqwik dependency, and set up package structure
    - Create the `com.nifi.layout` base package with sub-packages: model, graph, algorithm, layout, parser, router, spacing, collision, service, writer, util
    - Configure `pom.xml` or `build.gradle` with Java 21, jqwik 1.8+, JUnit 5, ArchUnit
    - _Requirements: 17.1_

  - [x] 1.2 Implement enums: NodeType, FlowDirection, AlignmentMode, RoutingMode, CrossingMinimizationStrategy (enum), PackingStrategy, ErrorSeverity
    - Create `NodeType` enum: PROCESSOR, PORT_INPUT, PORT_OUTPUT, FUNNEL, LABEL, REMOTE_PROCESS_GROUP, VIRTUAL
    - Create `FlowDirection` enum: TOP_TO_BOTTOM, LEFT_TO_RIGHT, BOTTOM_TO_TOP, RIGHT_TO_LEFT
    - Create `AlignmentMode` enum: CENTER, LEFT, RIGHT
    - Create `RoutingMode` enum: ORTHOGONAL, DIRECT
    - Create `PackingStrategy` enum: VERTICAL, HORIZONTAL, GRID
    - Create `ErrorSeverity` enum: ERROR, WARNING
    - _Requirements: 13.1_

  - [x] 1.3 Implement value objects: Position (record), BoundingBox (record with intersects, expand, right, bottom, center methods)
    - `Position(int x, int y)` — immutable record
    - `BoundingBox(int x, int y, int width, int height)` with `intersects(BoundingBox)`, `expand(int margin)`, `right()`, `bottom()`, `center()`
    - _Requirements: 6.1, 7.1, 7.3_

  - [x] 1.4 Implement LayoutOptions with Builder pattern and validation
    - All fields with defaults as specified in design: horizontalSpacing=80, verticalSpacing=100, flowDirection=TOP_TO_BOTTOM, gridSize=20, margins=50, padding=40, alignmentMode=CENTER, portSpacing=30, labelSpacing=20, routingMode=ORTHOGONAL, crossingStrategy=MEDIAN, packingStrategy=VERTICAL, incrementalMode=false, maxIterations=24
    - Builder validates ranges on `build()`: throw `ConfigurationValidationException` with property name, value, valid range
    - Immutable after construction
    - _Requirements: 13.1, 13.2, 13.3, 13.4_

  - [x]* 1.5 Write property test for LayoutOptions validation (Property 15)
    - **Property 15: Configuration validation**
    - **Validates: Requirements 6.9, 13.1, 13.3**

  - [x] 1.6 Implement exception hierarchy
    - `LayoutEngineException` (abstract base)
    - `GraphValidationException`, `ConfigurationValidationException`, `MaxNestingDepthException`
    - `LayoutPipelineException` with subclass `StrategyExecutionException`
    - `WriteBackException`
    - `LayoutError` value object with stageName, componentId, message, cause, severity
    - _Requirements: 20.2, 15.8_

- [x] 2. Implement core graph model and utility classes
  - [x] 2.1 Implement LayoutNode (immutable, equals/hashCode on id)
    - Fields: id, type (NodeType), boundingBox, attributes (Map<String,String>), parentGroupId
    - Ensure immutability; use defensive copies for map
    - _Requirements: 1.4, 1.9_

  - [x] 2.2 Implement LayoutEdge (immutable, equals/hashCode on id)
    - Fields: id, sourceNodeId, targetNodeId, sourcePort, targetPort, reversed (boolean), selfLoop (boolean)
    - _Requirements: 1.5_

  - [x] 2.3 Implement LayoutGraph with LinkedHashMap for determinism
    - Fields: nodes (LinkedHashMap), edges (LinkedHashMap), subgraphs (LinkedHashMap), processGroupId
    - Methods: getRoots(), getTerminals(), getOutgoingEdges(nodeId), getIncomingEdges(nodeId), getAdjacent(nodeId), getNodeCount(), getEdgeCount()
    - Use LinkedHashMap for all internal maps to ensure deterministic iteration
    - _Requirements: 1.4, 1.5, 12.2_

  - [x] 2.4 Implement LayeredGraph, CoordinateAssignment, RoutingResult, ComponentUpdate, LayoutResult
    - `LayeredGraph`: originalGraph, layers (List<List<LayoutNode>>), nodeToLayer (Map), virtualNodes (List), reversedEdges (List)
    - `CoordinateAssignment`: positions (Map<String, Position>), bounds (Map<String, BoundingBox>)
    - `RoutingResult`: edgePaths (Map<String, List<Position>>)
    - `ComponentUpdate` record: componentId, componentType, originalPosition, newPosition, newBounds
    - `LayoutResult`: updates list, totalComponentsRepositioned, connectionBendPoints, computationTimeMs, warnings
    - _Requirements: 4.4, 14.4_

  - [x] 2.5 Implement utility classes: GridSnapper, Geometry helpers, Preconditions
    - `GridSnapper.snap(Position, int gridSize)` — snaps x and y to nearest grid multiple
    - `Geometry` — distance calculations, bounding box union, etc.
    - `Preconditions` — null checks, range checks with descriptive messages
    - _Requirements: 6.2, 6.3_

  - [x]* 2.6 Write property test for grid snapping (Property 12)
    - **Property 12: Grid-aligned integer positions**
    - **Validates: Requirements 6.1, 6.2**

- [ ] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement strategy interfaces
  - [x] 4.1 Define all strategy interfaces in their respective packages
    - `LayoutAlgorithm` in layout package: `CoordinateAssignment computeLayout(LayeredGraph, LayoutOptions)`
    - `RoutingStrategy` in router package: `RoutingResult computeRoutes(LayoutGraph, CoordinateAssignment, LayoutOptions)`
    - `CollisionResolutionStrategy` in collision package: `CollisionResult resolve(List<PositionedComponent>, LayoutOptions)`
    - `SpacingStrategy` in spacing package: `SpacingValues computeSpacing(LayoutDimensions, int, LayoutOptions)`
    - `GraphBuildStrategy` in graph package: `LayoutGraph buildGraph(Object flowData, LayoutOptions)`
    - `CrossingMinimizationStrategy` (interface) in layout package: `LayeredGraph minimize(LayeredGraph, LayoutOptions)`
    - `LayerAssignmentStrategy` in layout package: `LayeredGraph assignLayers(LayoutGraph, LayoutOptions)`
    - `LayoutProgressCallback` in service package: `onStageStarted`, `onStageCompleted`, `onProgress`
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6_

  - [x] 4.2 Define PipelineStage interface and PipelineContext
    - `PipelineStage`: `getName()`, `execute(PipelineContext)`
    - `PipelineContext`: immutable with `withLayeredGraph()`, `withCoordinates()`, `withRouting()` factory methods
    - Fields: graph, options, layeredGraph, coordinates, routing, changedComponentIds, metadata
    - _Requirements: 20.1, 20.4_

- [x] 5. Implement graph construction and analysis algorithms
  - [x] 5.1 Implement GraphBuilder: constructs LayoutGraph from parsed component lists
    - Build nodes and edges from raw component data
    - Maintain LinkedHashMap ordering for determinism
    - Handle self-loop edges (source == target)
    - _Requirements: 1.4, 1.5, 12.2_

  - [x] 5.2 Implement GraphAnalyzer: detect roots, terminals, flow direction, disconnected components
    - `detectRoots(LayoutGraph)` — nodes with no incoming processor edges
    - `detectTerminals(LayoutGraph)` — nodes with no outgoing processor edges
    - `detectFlowDirection(LayoutGraph)` — analyze edge vectors, plurality wins, tie → TOP_TO_BOTTOM
    - `findConnectedComponents(LayoutGraph)` — undirected BFS/DFS to partition into subgraphs
    - _Requirements: 2.1, 2.4, 2.5, 3.1, 3.2, 10.1_

  - [x]* 5.3 Write property test for flow direction detection (Property 5)
    - **Property 5: Flow direction detection**
    - **Validates: Requirements 2.1, 2.4**

  - [x]* 5.4 Write property tests for root/terminal identification (Property 6)
    - **Property 6: Root and terminal identification**
    - **Validates: Requirements 3.1, 3.2**

  - [x]* 5.5 Write property test for connected component identification (Property 24)
    - **Property 24: Connected component identification**
    - **Validates: Requirements 10.1**

- [x] 6. Implement core graph algorithms
  - [x] 6.1 Implement TopologicalSorter using Kahn's algorithm
    - Deterministic: process nodes sorted by id when multiple have inDegree == 0
    - Throw `CycleDetectionException` if result size != node count (safety check)
    - O(V + E) time complexity
    - _Requirements: 4.1_

  - [x] 6.2 Implement TarjanCycleDetector (Tarjan's SCC)
    - Find all strongly connected components
    - For each SCC with size > 1, select minimal edge to reverse (longest span, tie-break by edge ID)
    - Mark reversed edges with `reversed = true`
    - O(V + E) time complexity
    - _Requirements: 4.2_

  - [x] 6.3 Implement ConnectedComponents (undirected BFS/DFS)
    - Partition graph into disjoint sets using undirected connectivity
    - Sort components deterministically (descending node count, lexicographic first ID)
    - _Requirements: 10.1, 10.5_

  - [x]* 6.4 Write property test for layer assignment edge constraint (Property 7)
    - **Property 7: Layer assignment edge constraint**
    - **Validates: Requirements 4.1, 4.3, 3.3, 3.4**

  - [x]* 6.5 Write property test for cycle-breaking edge restoration (Property 8)
    - **Property 8: Cycle-breaking edge restoration**
    - **Validates: Requirements 4.2**

- [ ] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement layer assignment and crossing minimization
  - [x] 8.1 Implement LongestPathLayerAssigner
    - Forward pass: assign each node to max(predecessor layers) + 1
    - Roots at layer 0, terminals at max layer
    - Compact layers to remove gaps
    - Handle empty graph (produce empty assignment)
    - _Requirements: 4.1, 4.3, 3.3, 3.4, 4.6_

  - [x] 8.2 Implement virtual node insertion in LayeredGraph
    - For each edge spanning > 1 layer, insert virtual nodes (type=VIRTUAL)
    - Each virtual node ID: `"{edgeId}_v{layer}"`
    - Replace original long edge with chain of single-layer segments
    - _Requirements: 4.4, 4.5_

  - [x]* 8.3 Write property test for virtual node correctness (Property 9)
    - **Property 9: Virtual node correctness**
    - **Validates: Requirements 4.4, 4.5**

  - [x] 8.4 Implement MedianCrossingMinimizer
    - Forward sweep (top to bottom), backward sweep (bottom to top)
    - Median sort: compute median of neighbor positions in adjacent layer
    - Stable sort for nodes with equal/undefined median values
    - Track bestOrdering and bestCrossings; terminate on 2 unchanged sweeps or maxIterations
    - _Requirements: 4.7, 5.1, 5.2, 5.4, 5.7_

  - [x] 8.5 Implement BarycenterCrossingMinimizer
    - Same structure as median but uses arithmetic mean of neighbor positions
    - _Requirements: 5.3_

  - [x] 8.6 Implement crossing count computation (merge-sort inversion counting)
    - For each adjacent layer pair, count inversions in edge orderings
    - O(E log E) per layer pair
    - _Requirements: 5.1_

  - [x]* 8.7 Write property test for crossing minimization non-degradation (Property 10)
    - **Property 10: Crossing minimization non-degradation**
    - **Validates: Requirements 5.1**

  - [x]* 8.8 Write property test for crossing minimization termination (Property 11)
    - **Property 11: Crossing minimization termination**
    - **Validates: Requirements 5.4**

- [x] 9. Implement coordinate assignment
  - [x] 9.1 Implement DefaultCoordinateAssigner
    - Compute positions based on layer index and node ordering within layer
    - Apply configured spacing (horizontal between nodes in layer, vertical between layers)
    - Apply margins (top, bottom, left, right)
    - Apply alignment mode (CENTER, LEFT, RIGHT) within each layer
    - Account for actual bounding box dimensions of each node
    - Snap all positions to grid via GridSnapper
    - Handle all four flow directions
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.8_

  - [x] 9.2 Implement DefaultSpacingStrategy
    - Compute spacing values based on layout dimensions and component count
    - Return SpacingValues with horizontal, vertical, and margin values
    - _Requirements: 6.4, 6.5_

  - [x]* 9.3 Write property test for spacing invariants (Property 13)
    - **Property 13: Spacing invariants**
    - **Validates: Requirements 6.4, 6.5, 6.8**

  - [x]* 9.4 Write property test for margin and padding constraints (Property 14)
    - **Property 14: Margin and padding constraints**
    - **Validates: Requirements 6.6, 6.7**

- [x] 10. Implement collision detection and resolution
  - [x] 10.1 Implement SweepLineCollisionDetector
    - Create start/end events sorted by x-coordinate (stable, starts before ends on tie)
    - Maintain active set (TreeSet sorted by y-coordinate)
    - Detect overlapping bounding boxes expanded by spacing margin
    - O(N log N + K) complexity
    - _Requirements: 7.1, 7.3_

  - [x] 10.2 Implement DisplacementCollisionResolver
    - Iterate up to 1000 times: detect collisions → compute minimal displacement → apply
    - Preserve relative left/right and above/below ordering
    - Track best result; return best partial result if max iterations exceeded
    - Report remaining overlap count on early termination
    - _Requirements: 7.2, 7.4, 7.5, 7.6_

  - [x]* 10.3 Write property test for collision resolution (Property 16)
    - **Property 16: Collision resolution**
    - **Validates: Requirements 7.1, 7.2, 7.4, 7.6**

  - [x]* 10.4 Write property test for collision resolution termination (Property 17)
    - **Property 17: Collision resolution termination**
    - **Validates: Requirements 7.5**

- [x] 11. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Implement connection routing
  - [x] 12.1 Implement OrthogonalRouter
    - Compute exit point from source (based on flow direction)
    - Compute entry point to target (based on flow direction)
    - Route with H/V segments only, avoiding component bounding boxes with spacing clearance
    - Handle self-loop routing: exit and re-enter same node without overlapping other paths
    - Space shared port connections by configured portSpacing
    - _Requirements: 9.2, 9.6, 9.7, 9.8_

  - [x] 12.2 Implement DirectRouter
    - Route as straight lines from source boundary to target boundary
    - Start point at source component boundary, end point at target component boundary
    - _Requirements: 9.3_

  - [x]* 12.3 Write property test for orthogonal routing correctness (Property 20)
    - **Property 20: Orthogonal routing correctness**
    - **Validates: Requirements 9.2, 9.6**

  - [x]* 12.4 Write property test for direct routing correctness (Property 21)
    - **Property 21: Direct routing correctness**
    - **Validates: Requirements 9.3**

  - [ ]* 12.5 Write property test for shared port spacing (Property 22)
    - **Property 22: Shared port spacing**
    - **Validates: Requirements 9.7**

  - [ ]* 12.6 Write property test for self-loop routing (Property 23)
    - **Property 23: Self-loop routing**
    - **Validates: Requirements 9.8**

- [x] 13. Implement label and port positioning, funnel handling
  - [x] 13.1 Implement label positioning logic in CoordinateAssigner
    - Labels associated with a component: position label bottom edge exactly labelSpacing above the component top edge
    - Labels without association: treat as independent node in layer assignment
    - _Requirements: 18.1, 18.6_

  - [x] 13.2 Implement port positioning logic
    - Input Ports at entry edge based on flow direction (top for TTB, left for LTR, bottom for BTT, right for RTL)
    - Output Ports at exit edge based on flow direction (bottom for TTB, right for LTR, top for BTT, left for RTL)
    - Distribute multiple ports evenly along edge with portSpacing, centered on midpoint
    - _Requirements: 18.2, 18.3, 18.4, 8.5, 8.6_

  - [x] 13.3 Implement funnel handling in layer assignment
    - Funnels use same topological sorting rules as processors
    - Funnels with no incoming connections treated as Root_Processor at layer 0
    - Use actual NiFi canvas dimensions for funnel sizing
    - _Requirements: 19.1, 19.2, 19.4_

  - [ ]* 13.4 Write property test for label positioning (Property 31)
    - **Property 31: Label positioning**
    - **Validates: Requirements 18.1**

  - [ ]* 13.5 Write property test for port edge positioning (Property 19)
    - **Property 19: Port edge positioning**
    - **Validates: Requirements 8.5, 8.6, 18.2, 18.3, 18.4**

- [x] 14. Implement service orchestration layer
  - [x] 14.1 Implement LayoutPipeline
    - Execute stages in defined order: Parse → Build → Detect → Cycle → Layer → Crossing → Coordinate → Collision → Route → GridAlign → WriteBack
    - Pass PipelineContext between stages (immutable, each stage returns new context)
    - Report progress via LayoutProgressCallback (onStageStarted, onStageCompleted)
    - Halt on stage failure, report stage name and cause via LayoutPipelineException
    - _Requirements: 20.1, 20.2, 20.3_

  - [x] 14.2 Implement LayoutEngine (public API facade)
    - `create()`, `create(LayoutOptions)`, `builder()` factory methods
    - `layout(LayoutGraph, LayoutOptions)` — NiFi-independent graph entry point
    - `layoutIncremental(LayoutGraph, LayoutOptions, Set<String>)` — incremental mode
    - Validate input, delegate to pipeline, measure computation time
    - Return empty LayoutResult for zero-component groups without full pipeline execution
    - _Requirements: 20.1, 20.5, 15.6, 15.7_

  - [x] 14.3 Implement strategy registration and composition in LayoutEngine.Builder
    - Builder accepts custom implementations for each strategy interface
    - Default implementations used when no custom strategy registered
    - Replacement on duplicate registration (no error)
    - Wrap custom strategy exceptions in StrategyExecutionException
    - _Requirements: 15.6, 15.7, 15.8, 15.9_

  - [x]* 14.4 Write property test for pipeline failure isolation (Property 30)
    - **Property 30: Pipeline failure isolation**
    - **Validates: Requirements 20.2**

- [x] 15. Implement disconnected subgraph layout and packing
  - [x] 15.1 Implement subgraph layout orchestration
    - Identify disconnected subgraphs via ConnectedComponents
    - Layout each independently (potentially in parallel via ForkJoinPool)
    - Merge results in deterministic order (descending node count, lexicographic ID tiebreak)
    - _Requirements: 10.1, 10.2, 10.5, 12.5_

  - [x] 15.2 Implement packing strategies (vertical, horizontal, grid)
    - VERTICAL: stack subgraphs vertically with configured spacing
    - HORIZONTAL: stack horizontally with configured spacing
    - GRID: arrange in rows, row height = tallest subgraph in row
    - Maintain subgraph spacing between adjacent bounding boxes
    - _Requirements: 10.3, 10.4_

  - [x]* 15.3 Write property test for subgraph packing (Property 25)
    - **Property 25: Subgraph packing**
    - **Validates: Requirements 10.4, 10.5**

- [x] 16. Implement nested process group layout
  - [x] 16.1 Implement recursive nested group layout
    - Layout children first (bottom-up), up to depth 10 (throw MaxNestingDepthException beyond)
    - Compute child bounding box, resize group to fit children + padding
    - Treat nested group as single node with computed dimensions in parent layout
    - Handle empty nested groups: assign minimum bounding box with padding
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.7_

  - [x]* 16.2 Write property test for process group bounding box containment (Property 18)
    - **Property 18: Process group bounding box containment**
    - **Validates: Requirements 8.2, 8.3**

  - [x]* 16.3 Write property test for recursive subgraph construction (Property 4)
    - **Property 4: Recursive subgraph construction**
    - **Validates: Requirements 1.6**

- [x] 17. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 18. Implement incremental layout mode
  - [x] 18.1 Implement incremental layout logic in LayoutEngine
    - Accept changed component IDs from caller
    - Compute affected set: changed + direct neighbors (graph-adjacent)
    - Preserve positions of unaffected components
    - Layout only movable nodes, constrained by fixed positions
    - Resolve fixed/movable collisions
    - Re-route affected connections only
    - Return unmodified positions when changed set is empty
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5_

  - [x]* 18.2 Write property test for incremental layout preservation (Property 26)
    - **Property 26: Incremental layout preservation**
    - **Validates: Requirements 11.2, 11.4**

- [x] 19. Implement NiFi parsing adapters behind an independently usable core boundary
  - [ ] 19.1 Split the build into core and adapter artifacts before adding NiFi dependencies
    - Convert the root project to a Maven aggregator while preserving the published coordinates/version and move all existing generic model, graph, algorithm, layout, router, collision, spacing, service, and utility code into `nifi-layout-core`
    - Create `nifi-layout-adapter-support` and `nifi-layout-adapter-rest`; dependencies must flow from the typed adapter to adapter-support to core only
    - Keep `LayoutEngine` in core with only `LayoutGraph` entry points; do not add `ProcessGroup`, `ProcessGroupDTO`, Copilot, Spring, `NiFiServiceFacade`, `nifi-web-api`, or UI dependencies/overloads
    - Pin the REST adapter dependency to the inspected reactor version `2.10.0-SNAPSHOT`: `org.apache.nifi:nifi-client-dto`
    - If moving sources is demonstrably too disruptive, first isolate the same boundaries as packages in the existing artifact and record the multi-module split as the immediate target; no NiFi import may escape parser packages under this fallback
    - _Requirements: 17.1, 17.2, 17.3, 17.4, 17.5_

  - [ ] 19.2 Define the NiFi-neutral adapter contract and canonical snapshot model
    - Add a generic, type-safe adapter contract such as `FlowGraphAdapter<T>` with `LayoutGraph parse(T source)`; do not use or extend the existing unsafe `GraphBuildStrategy<Object>` for typed adapters
    - Define immutable `CanonicalFlowSnapshot`, `CanonicalProcessGroup`, `CanonicalComponent`, and `CanonicalConnection` records (or equivalent) in adapter-support with no `org.apache.nifi` imports
    - Represent original IDs, parent group ID, `NodeType`, optional source position/dimensions, and a documented minimal normalized string-attribute set sufficient for deterministic write-back and adapter equivalence
    - Centralize per-`NodeType` defaults for absent positions and dimensions so both typed adapters resolve missing values identically
    - Define recursive canonical child groups with a maximum depth of 10; reject depth 11 with `MaxNestingDepthException`, while accepting an empty group as a valid snapshot
    - _Requirements: 1.4, 1.6, 1.8, 1.9, 8.7, 15.5, 17.2, 17.3_

  - [ ] 19.3 Extend graph construction for validated hierarchical graphs
    - Add a `GraphBuilder` overload accepting nodes, edges, subgraphs, and process group ID while preserving the existing overload for independent core callers
    - Validate every edge source and target before constructing `LayoutGraph`; throw `GraphValidationException` whose message identifies both the connection ID and missing component ID
    - Prevent `LayoutGraph` adjacency indexing from silently accepting dangling endpoints by requiring validated construction or enforcing the same invariant at its boundary
    - Preserve self-loops and normalize `selfLoop=true` whenever source and target IDs match
    - Keep all graph and subgraph maps insertion-ordered and immutable after construction
    - _Requirements: 1.5, 1.6, 1.7, 1.8, 12.2_

  - [ ] 19.4 Implement deterministic canonical graph assembly and edge normalization
    - Implement `CanonicalGraphAssembler` in adapter-support to convert only canonical data into `LayoutGraph`; iterate components, connections, and child groups by stable lexicographic ID because NiFi source collections are sets
    - Add each child process group as a `PROCESS_GROUP` node in its parent and recursively add its graph at `subgraphs[childId]`; retain parent IDs, bounds, normalized attributes, and valid empty child graphs
    - Encode selected relationship sets into singular `LayoutEdge.sourcePort` deterministically: escape backslash as `\\` and delimiter `|` as `\|` in each name, sort escaped names lexicographically, then join with `|`; use the destination connectable port/name when available or the empty string for `targetPort`, consistently in every adapter
    - Document that relationship encoding is reversible and that no adapter may depend on `Set` iteration order
    - _Requirements: 1.3, 1.4, 1.5, 1.6, 1.8, 1.9, 12.1, 12.2_

  - [ ] 19.6 Implement the typed REST DTO adapter
    - Implement `RestDtoAdapter implements FlowGraphAdapter<org.apache.nifi.web.api.dto.ProcessGroupDTO>` with `LayoutGraph parse(ProcessGroupDTO group)` using only `nifi-client-dto:2.10.0-SNAPSHOT`
    - Explicitly read `ProcessGroupDTO.getContents()` as `FlowSnippetDTO` and map processors, connections, input/output ports, funnels, labels, remote process groups, and nested process groups into canonical snapshots
    - Normalize null component/connection collections to empty collections, apply shared defaults for absent positions/dimensions, and define missing `contents` as an empty group only when the DTO otherwise identifies a group; reject an absent group/ID with a descriptive validation error
    - Map `ConnectableDTO` source/destination IDs and selected relationships without relying on set order; delegate hierarchy assembly and dangling endpoint checks to shared code
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9_

  - [ ] 19.7 Add the optional Copilot map-adapter boundary and document the deployment seam
    - Define a separate optional `nifi-layout-adapter-copilot` boundary (or a Copilot-owned implementation contract) that depends on adapter-support/core but does not make core or typed adapters depend on Copilot
    - Plan `ProcessGroupFlowMapAdapter` to consume the `Map<String,Object>` returned by `NiFiClientOperations.getProcessGroupFlow()`, unwrap `processGroupFlow.flow`, normalize entity/component maps into the same canonical snapshot, and never cast or convert the map payload to `ProcessGroupDTO`
    - Define the future `FlowLayoutService` orchestration seam for NiFi Copilot: obtain the map snapshot through `NiFiClientOperations`, parse to `LayoutGraph`, and invoke `LayoutEngine` after `ConnectionConfigurationStage` and before `RuntimeActivationStage`
    - Keep Task 19 parse/build-only: no position persistence, revision handling, processor activation, or replacement of temporary `CanvasLayoutEngine` creation placement; Task 20 owns write-back
    - _Requirements: 1.2, 1.3, 17.2, 17.3, 20.1_

  - [ ] 19.8 Add exhaustive canonical assembler and typed-adapter unit tests
    - Add fixtures covering every component type, actual and default positions/dimensions, normalized attributes, empty groups, self-loops, multiple selected relationships (including delimiter/backslash escaping), shuffled set ordering, nested groups through depth 10, and depth-limit failure
    - Cover REST null/empty collections, missing contents behavior, and dangling source and dangling target references
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 12.1, 12.2_

  - [ ]* 19.9 Write property test for parsing preservation and deterministic ordering (Property 1)
    - Generate canonical and typed fixtures with shuffled set iteration and assert every component/connection ID and hierarchy member is preserved in stable ID order with expected type, bounds, parent, endpoints, and normalized relationships
    - **Property 1: Parsing preserves all components and IDs**
    - **Validates: Requirements 1.1, 1.2, 1.4, 1.5, 1.6, 1.9, 12.1**

  - [ ]* 19.11 Write property test for invalid reference diagnostics (Property 3)
    - Generate missing source and target references and assert `GraphValidationException` includes the exact connection ID and missing component ID
    - **Property 3: Invalid references identify connection and missing component**
    - **Validates: Requirements 1.7**

  - [ ] 19.12 Add dependency-boundary and NiFi API compatibility validation
    - Add architecture tests proving `nifi-layout-core` and adapter-support have zero NiFi, Copilot, Spring, `NiFiServiceFacade`, `nifi-web-api`, or UI imports and that dependency direction is typed/Copilot adapter → adapter-support → core
    - Add compile/integration fixtures against `ProcessGroupDTO`/`FlowSnippetDTO` from `nifi-client-dto:2.10.0-SNAPSHOT`
    - Run non-watch Maven tests for core, adapter-support, REST adapter, and Copilot adapter packages
    - _Requirements: 17.2, 17.3, 17.4, 17.5_

  - **Definition of done**
    - The independent core remains directly usable as `LayoutGraph -> LayoutEngine.layout(...)`, the REST adapter compiles against NiFi `2.10.0-SNAPSHOT`, and all Task 19 unit/property/architecture/compatibility tests pass
  - **Non-goals**
    - No NiFi/Copilot overloads in core `LayoutEngine`; no dependency on Copilot, Spring, `NiFiServiceFacade`, `nifi-web-api`, or the NiFi UI; no write-back, optimistic revision handling, deployment, activation, or production integration in Task 19

- [x] 20. Implement NiFi writers (position write-back)
  - [ ] 20.2 Implement RestDtoWriter
    - Same write-back logic for NiFi REST DTO objects
    - Update position, dimensions, and connection bend points
    - _Requirements: 14.1, 14.2, 14.3, 14.5, 14.6_

  - [ ] 20.3 Implement LayoutResult construction in writers
    - Produce LayoutResult with: list of ComponentUpdate (original + new positions), totalComponentsRepositioned, connectionBendPoints, computationTimeMs, warnings
    - _Requirements: 14.4_

  - [ ]* 20.4 Write property test for write-back completeness (Property 28)
    - **Property 28: Write-back completeness and non-destructiveness**
    - **Validates: Requirements 14.1, 14.2, 14.3, 14.4**

  - [ ]* 20.5 Write property test for unmappable component error (Property 29)
    - **Property 29: Unmappable component error**
    - **Validates: Requirements 14.5**

- [ ] 21. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 22. Implement deterministic output guarantees and integration wiring
  - [ ] 22.1 Ensure all internal collections use deterministic ordering
    - Audit all HashMap → LinkedHashMap, all HashSet → LinkedHashSet or TreeSet
    - Verify stable sorting in all ordering operations
    - Ensure no dependency on system random, thread scheduling, or hash iteration order
    - Verify parallel subgraph results merged in deterministic order
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_

  - [ ]* 22.2 Write property test for deterministic output (Property 27)
    - **Property 27: Deterministic output**
    - **Validates: Requirements 12.1**

  - [ ] 22.3 Wire complete end-to-end pipeline with all stages
    - Connect: RestDtoAdapter or ProcessGroupFlowMapAdapter → GraphBuilder → GraphAnalyzer → TarjanCycleDetector → LongestPathLayerAssigner → CrossingMinimizer → CoordinateAssigner → CollisionResolver → ConnectionRouter → GridSnapper → Writer
    - Validate correct PipelineContext flow between stages
    - _Requirements: 20.1_

- [ ] 23. Implement ArchUnit architecture tests
  - [ ] 23.1 Write ArchUnit tests for package dependency rules
    - Verify graph, algorithm, layout, router, spacing, collision, model, util have ZERO NiFi imports
    - Verify only parser and writer import from org.apache.nifi.*
    - Verify layered dependency direction (no upward deps)
    - Verify service depends only on strategy interfaces, not concrete implementations
    - _Requirements: 17.2, 17.3, 17.4, 17.5, 17.6_

- [ ] 24. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties using jqwik (Java PBT framework)
- Unit tests validate specific examples and edge cases
- The design uses Java 21 explicitly — all implementations use Java 21 features (records, sealed classes where appropriate)
- All internal collections MUST use deterministic ordering (LinkedHashMap, TreeSet, stable sort) per Requirement 12
- NiFi-specific code is strictly confined to parser/ and writer/ packages per Requirement 17

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3", "1.6"] },
    { "id": 2, "tasks": ["1.4", "2.1", "2.2", "2.5"] },
    { "id": 3, "tasks": ["1.5", "2.3"] },
    { "id": 4, "tasks": ["2.4", "2.6"] },
    { "id": 5, "tasks": ["4.1", "4.2"] },
    { "id": 6, "tasks": ["5.1", "5.2", "6.1", "6.2", "6.3"] },
    { "id": 7, "tasks": ["5.3", "5.4", "5.5", "6.4", "6.5"] },
    { "id": 8, "tasks": ["8.1", "8.6"] },
    { "id": 9, "tasks": ["8.2", "8.4", "8.5"] },
    { "id": 10, "tasks": ["8.3", "8.7", "8.8"] },
    { "id": 11, "tasks": ["9.1", "9.2"] },
    { "id": 12, "tasks": ["9.3", "9.4", "10.1"] },
    { "id": 13, "tasks": ["10.2"] },
    { "id": 14, "tasks": ["10.3", "10.4", "12.1", "12.2"] },
    { "id": 15, "tasks": ["12.3", "12.4", "12.5", "12.6", "13.1", "13.2", "13.3"] },
    { "id": 16, "tasks": ["13.4", "13.5"] },
    { "id": 17, "tasks": ["14.1", "15.1", "15.2", "16.1"] },
    { "id": 18, "tasks": ["14.2", "14.3", "15.3", "16.2", "16.3"] },
    { "id": 19, "tasks": ["14.4", "18.1"] },
    { "id": 20, "tasks": ["18.2", "19.1"] },
    { "id": 21, "tasks": ["19.2", "19.3"] },
    { "id": 22, "tasks": ["19.4"] },
    { "id": 23, "tasks": ["19.5", "19.6", "19.7"] },
    { "id": 24, "tasks": ["19.8", "19.9", "19.10", "19.11"] },
    { "id": 25, "tasks": ["19.12"] },
    { "id": 26, "tasks": ["20.1", "20.2"] },
    { "id": 27, "tasks": ["20.3", "20.4", "20.5"] },
    { "id": 28, "tasks": ["22.1", "22.3"] },
    { "id": 29, "tasks": ["22.2", "23.1"] }
  ]
}
```
