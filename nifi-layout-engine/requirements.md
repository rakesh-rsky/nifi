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
# Requirements Document

## Introduction

The NiFi Auto Layout Engine is a production-grade Java 21 library that automatically rearranges Apache NiFi flow components on the canvas into a clean, readable layout. Over time, NiFi flows become messy: processors overlap, connections cross each other, branches are difficult to follow, process groups become disorganized, and labels and ports are misplaced. This library operates independently of the NiFi UI, accepting NiFi REST DTOs or Copilot flow maps, and outputs updated component positions. The architecture follows SOLID principles, prefers composition over inheritance, and maintains strict separation between graph construction, graph algorithms, layout algorithms, coordinate assignment, collision resolution, and NiFi adapters.

## Glossary

- **Layout_Engine**: The top-level service that orchestrates the complete layout pipeline from parsing NiFi flow data to producing updated component positions.
- **Graph_Builder**: The component responsible for constructing an internal graph representation from NiFi flow data (processors, connections, ports, funnels, labels, remote process groups).
- **Layer_Assigner**: The algorithm component that assigns each node in the graph to a hierarchical layer based on topological ordering and flow direction.
- **Crossing_Minimizer**: The algorithm component that reorders nodes within layers to minimize the number of edge crossings using median or barycenter heuristics.
- **Coordinate_Assigner**: The component that computes x,y positions for each node based on layer assignment, ordering, spacing configuration, and grid alignment.
- **Collision_Resolver**: The component that detects and resolves overlapping components by adjusting positions while preserving the overall layout structure.
- **Connection_Router**: The component that computes edge routing paths between connected components using orthogonal or direct routing strategies.
- **NiFi_Adapter**: The adapter layer that translates NiFi REST DTOs or Copilot flow maps into the internal graph representation.
- **Process_Group**: A NiFi container that holds processors, connections, ports, funnels, labels, and nested process groups.
- **Processor**: A NiFi component that performs data transformation, routing, or other operations on FlowFiles.
- **Port**: A NiFi component (Input Port or Output Port) that provides connectivity between process groups.
- **Funnel**: A NiFi component that combines data from multiple connections into a single connection.
- **Label**: A NiFi annotation component placed on the canvas for documentation purposes.
- **Remote_Process_Group**: A NiFi component that represents a connection to a remote NiFi instance.
- **Connection**: A directed link between two NiFi components representing the flow of data.
- **Layout_Options**: The configuration model containing all user-configurable parameters for the layout operation.
- **Grid**: A configurable alignment grid to which component positions are snapped after layout computation.
- **Subgraph**: A connected subset of nodes within the overall flow graph; disconnected flows produce multiple subgraphs.
- **Cycle**: A directed path in the graph that returns to its starting node, requiring special handling during layer assignment.
- **Root_Processor**: A processor with no incoming connections from other processors, representing a flow entry point.
- **Terminal_Processor**: A processor with no outgoing connections to other processors, representing a flow exit point.
- **Incremental_Layout**: A layout mode that only rearranges components in areas that have changed, preserving the positions of unchanged components.
- **Deterministic_Layout**: A property guaranteeing that identical input flows always produce identical output positions regardless of execution order or environment.

## Requirements

### Requirement 1: NiFi Flow Parsing and Graph Construction

**User Story:** As a NiFi developer, I want to parse NiFi flow data into an internal graph representation, so that layout algorithms can operate on a clean, NiFi-independent data structure.

#### Acceptance Criteria

1. WHEN a ProcessGroup is provided via NiFi REST DTOs, THE NiFi_Adapter SHALL parse all Processors, Ports, Funnels, Labels, Remote_Process_Groups, and Connections into the internal graph representation.
2. WHEN a ProcessGroup is provided as a Copilot flow map, THE NiFi_Adapter SHALL parse all supported components and Connections into the internal graph representation.
3. THE Graph_Builder SHALL represent each NiFi component as a node with position (x, y coordinates), dimensions (width and height in pixels), type (Processor, Port, Funnel, Label, or Remote_Process_Group), and identifier attributes.
4. THE Graph_Builder SHALL represent each Connection as a directed edge with source node, target node, source port, and target port attributes, including self-referencing connections where source and target are the same node.
5. WHEN a ProcessGroup contains nested ProcessGroups, THE Graph_Builder SHALL recursively construct subgraphs for each nested group up to a maximum nesting depth of 10 levels.
6. IF a component reference in a Connection does not exist in the ProcessGroup, THEN THE NiFi_Adapter SHALL throw a validation exception identifying the missing component identifier and the Connection that references it.
7. WHEN a ProcessGroup contains zero components, THE NiFi_Adapter SHALL produce a valid empty graph with no nodes and no edges.
8. THE NiFi_Adapter SHALL preserve all original component identifiers to enable position write-back after layout computation.

### Requirement 2: Flow Direction Detection

**User Story:** As a NiFi developer, I want the layout engine to detect the predominant flow direction, so that the layout reflects the logical data flow orientation.

#### Acceptance Criteria

1. WHEN a graph is constructed, THE Layout_Engine SHALL analyze connection directions by comparing source and target component positions to determine the predominant flow direction (top-to-bottom, left-to-right, bottom-to-top, or right-to-left) based on the plurality of edge vectors.
2. THE Layout_Engine SHALL use the detected flow direction as the primary axis for layer assignment.
3. WHERE a flow direction is specified in Layout_Options, THE Layout_Engine SHALL use the configured direction instead of the detected direction.
4. IF two or more flow directions have equal connection counts (tie), THEN THE Layout_Engine SHALL default to top-to-bottom as the flow direction.
5. IF the graph contains no connections, THEN THE Layout_Engine SHALL default to top-to-bottom as the flow direction.

### Requirement 3: Root and Terminal Processor Detection

**User Story:** As a NiFi developer, I want the engine to identify root and terminal processors, so that entry and exit points of the flow are positioned at the beginning and end of the layout.

#### Acceptance Criteria

1. WHEN a graph is constructed, THE Layout_Engine SHALL identify all Root_Processors (nodes with no incoming edges from other processors, excluding edges from Ports, Funnels, or Remote_Process_Groups).
2. WHEN a graph is constructed, THE Layout_Engine SHALL identify all Terminal_Processors (nodes with no outgoing edges to other processors, excluding edges to Ports, Funnels, or Remote_Process_Groups).
3. THE Layer_Assigner SHALL place Root_Processors in layer 0 (the minimum layer) of the layout.
4. THE Layer_Assigner SHALL place Terminal_Processors in the maximum assigned layer of the layout.
5. IF a processor has no incoming edges from other processors and no outgoing edges to other processors (isolated processor), THEN THE Layer_Assigner SHALL treat it as a Root_Processor and place it in layer 0.
6. IF the graph contains no Root_Processors after cycle-breaking edge reversal, THEN THE Layer_Assigner SHALL treat the processors on the broken cycle edges' source nodes as root candidates and place them in layer 0.

### Requirement 4: Layer Assignment

**User Story:** As a NiFi developer, I want processors arranged layer by layer according to their position in the data flow, so that the hierarchical structure of the flow is visually clear.

#### Acceptance Criteria

1. THE Layer_Assigner SHALL assign each node to exactly one integer layer (starting from layer 0) using topological sorting of the directed graph.
2. WHEN cycles exist in the graph, THE Layer_Assigner SHALL detect all cycles and reverse one edge per cycle to produce a directed acyclic graph suitable for topological sorting, restoring original edge directions in the output after layer assignment is complete.
3. THE Layer_Assigner SHALL assign layers such that for every edge (u, v), the layer of u is less than the layer of v (after cycle-breaking edge reversal).
4. WHEN an edge spans more than one layer, THE Layer_Assigner SHALL insert one virtual node per intermediate layer so that each resulting edge segment spans exactly one layer.
5. THE Layer_Assigner SHALL mark each inserted virtual node with a distinct type attribute so that downstream components can distinguish virtual nodes from original graph nodes.
6. IF the input graph contains no nodes, THEN THE Layer_Assigner SHALL produce an empty layer assignment with zero layers.

### Requirement 5: Edge Crossing Minimization

**User Story:** As a NiFi developer, I want edge crossings minimized, so that connection lines between processors are easy to follow visually.

#### Acceptance Criteria

1. THE Crossing_Minimizer SHALL reorder nodes within each layer such that the total number of edge crossings after reordering does not exceed the crossing count of the initial node ordering.
2. THE Crossing_Minimizer SHALL support the median heuristic for node ordering within layers.
3. THE Crossing_Minimizer SHALL support the barycenter heuristic for node ordering within layers.
4. THE Crossing_Minimizer SHALL iterate the crossing reduction sweep until the crossing count remains unchanged for 2 consecutive full sweeps or a configurable maximum iteration count (default: 24) is reached.
5. WHERE a crossing minimization strategy is specified in Layout_Options, THE Crossing_Minimizer SHALL use the configured strategy.
6. IF no crossing minimization strategy is specified in Layout_Options, THEN THE Crossing_Minimizer SHALL use the median heuristic as the default strategy.
7. THE Crossing_Minimizer SHALL perform each full sweep by processing layers alternately in forward order (first to last) and reverse order (last to first).

### Requirement 6: Coordinate Assignment and Grid Alignment

**User Story:** As a NiFi developer, I want components aligned on a configurable grid with consistent spacing, so that the layout appears organized and professional.

#### Acceptance Criteria

1. THE Coordinate_Assigner SHALL compute x,y positions for each node as integer pixel coordinates based on layer assignment, node ordering within layers, and configured spacing values.
2. THE Coordinate_Assigner SHALL snap all computed positions to the nearest point on the configured Grid.
3. WHERE a grid size is specified in Layout_Options, THE Coordinate_Assigner SHALL use the configured grid size for alignment.
4. THE Coordinate_Assigner SHALL apply configurable horizontal spacing measured as the distance between the trailing edge of one node's bounding box and the leading edge of the next node's bounding box within the same layer.
5. THE Coordinate_Assigner SHALL apply configurable vertical spacing measured as the distance between the bottom edge of one layer's tallest node bounding box and the top edge of the next layer's topmost node bounding box.
6. THE Coordinate_Assigner SHALL respect configured margins around the edges of the layout area, ensuring no component bounding box is placed closer than the margin distance to the layout area boundary.
7. THE Coordinate_Assigner SHALL respect configured padding within process group boundaries, ensuring no child component bounding box is placed closer than the padding distance to the process group's inner edge.
8. THE Coordinate_Assigner SHALL account for the actual bounding box dimensions of each node (which vary by component type) when computing positions, so that spacing values represent clear space between component edges rather than between component centers.
9. IF a configured spacing, margin, or padding value is less than zero, THEN THE Coordinate_Assigner SHALL report a validation error identifying the invalid property.

### Requirement 7: Collision Detection and Resolution

**User Story:** As a NiFi developer, I want the layout engine to prevent overlapping components, so that every processor, port, funnel, and label is clearly visible and accessible.

#### Acceptance Criteria

1. THE Collision_Resolver SHALL detect all pairs of components whose bounding boxes overlap after coordinate assignment.
2. WHEN overlapping components are detected, THE Collision_Resolver SHALL adjust positions to eliminate all overlaps while preserving the relative ordering from the layout algorithm, such that if component A was positioned to the left of or above component B before resolution, A remains to the left of or above B after resolution.
3. THE Collision_Resolver SHALL account for the actual dimensions of each component type (Processor, Port, Funnel, Label, Remote_Process_Group) as defined by their configured or default type dimensions during overlap detection.
4. THE Collision_Resolver SHALL maintain a minimum spacing of at least the configured spacing value (default: 20 pixels) between the bounding boxes of any two components whose expanded bounding boxes (original box enlarged by the spacing value on each side) would intersect.
5. IF the Collision_Resolver cannot eliminate all overlaps within 1000 displacement iterations, THEN THE Collision_Resolver SHALL terminate resolution, apply the best partial result achieved, and report the remaining overlap count.
6. WHEN overlapping components are detected, THE Collision_Resolver SHALL resolve all overlaps such that zero bounding-box intersections remain in the final output, except when terminated early per criterion 5.

### Requirement 8: Process Group Sizing and Nested Layout

**User Story:** As a NiFi developer, I want process groups automatically resized to fit their contents and nested groups laid out recursively, so that complex hierarchical flows are cleanly organized at every level.

#### Acceptance Criteria

1. WHEN a ProcessGroup contains nested ProcessGroups, THE Layout_Engine SHALL recursively layout each nested group before laying out the parent group, up to a maximum nesting depth of 10 levels.
2. THE Layout_Engine SHALL compute the bounding box of all components within a ProcessGroup after layout.
3. WHEN the computed bounding box exceeds the current ProcessGroup dimensions, THE Layout_Engine SHALL resize the ProcessGroup to fit all contained components plus configured padding on all sides.
4. THE Layout_Engine SHALL treat each nested ProcessGroup as a single node with computed dimensions (width and height from the bounding box plus padding) during parent group layout.
5. THE Layout_Engine SHALL position Input Ports at the entry edge of each ProcessGroup based on the configured flow direction (top edge for top-to-bottom, left edge for left-to-right, bottom edge for bottom-to-top, right edge for right-to-left).
6. THE Layout_Engine SHALL position Output Ports at the exit edge of each ProcessGroup based on the configured flow direction (bottom edge for top-to-bottom, right edge for left-to-right, top edge for bottom-to-top, left edge for right-to-left).
7. IF a nested ProcessGroup contains zero components, THEN THE Layout_Engine SHALL assign it a minimum bounding box with the configured padding dimensions.

### Requirement 9: Connection Routing

**User Story:** As a NiFi developer, I want connection lines routed cleanly between components, so that the data flow paths are visually clear and do not obscure other components.

#### Acceptance Criteria

1. THE Connection_Router SHALL compute routing paths for all connections after coordinate assignment.
2. THE Connection_Router SHALL support orthogonal routing mode (connections use only horizontal and vertical segments).
3. THE Connection_Router SHALL support direct routing mode (connections use straight lines between source and target).
4. WHERE a routing mode is specified in Layout_Options, THE Connection_Router SHALL use the configured routing mode.
5. IF no routing mode is specified in Layout_Options, THEN THE Connection_Router SHALL use orthogonal routing as the default mode.
6. WHEN using orthogonal routing mode, THE Connection_Router SHALL route connections to avoid passing through other component bounding boxes, maintaining a clearance of at least the configured spacing value from any component boundary.
7. WHEN multiple connections share the same source or target port, THE Connection_Router SHALL space the connection paths by at least the configured port spacing to prevent visual overlap at the port.
8. WHEN a connection is a self-loop (source and target are the same node), THE Connection_Router SHALL route the connection as a loop that exits and re-enters the node without overlapping other connections.

### Requirement 10: Disconnected Subgraph Support

**User Story:** As a NiFi developer, I want disconnected flow segments arranged in a coherent manner, so that all parts of the canvas are organized even when flows are not connected to each other.

#### Acceptance Criteria

1. WHEN a graph contains multiple disconnected subgraphs, THE Layout_Engine SHALL identify each subgraph as a separate layout unit by computing connected components using undirected connectivity.
2. THE Layout_Engine SHALL apply the full layout algorithm independently to each disconnected subgraph.
3. WHERE a packing strategy is specified in Layout_Options, THE Layout_Engine SHALL arrange the laid-out subgraphs relative to each other using the configured packing strategy (vertical stacking, horizontal stacking, or grid packing); IF no packing strategy is configured, THEN THE Layout_Engine SHALL use vertical stacking as the default.
4. THE Layout_Engine SHALL maintain the subgraph spacing value configured in Layout_Options between adjacent subgraph bounding boxes.
5. THE Layout_Engine SHALL order subgraphs for packing by descending node count, using lexicographic ordering of component identifiers as a tiebreaker, to ensure deterministic arrangement.

### Requirement 11: Incremental Layout

**User Story:** As a NiFi developer, I want the option to rearrange only changed areas of the flow, so that existing well-positioned components are not disrupted when new components are added.

#### Acceptance Criteria

1. WHERE incremental mode is enabled in Layout_Options, THE Layout_Engine SHALL accept a set of changed component identifiers from the caller indicating which components have been added, removed, or reconnected since the last layout operation.
2. WHERE incremental mode is enabled, THE Layout_Engine SHALL preserve the exact x,y positions of all components whose identifiers are not in the changed set and that are not graph-adjacent (directly connected by an edge) to a changed component.
3. WHERE incremental mode is enabled, THE Layout_Engine SHALL rearrange the changed components and their graph-adjacent neighbors (components sharing a direct edge with a changed component) to resolve collisions and maintain layer assignment consistency.
4. WHERE incremental mode is enabled, THE Layout_Engine SHALL recompute connection routing paths for all edges that have at least one endpoint in the changed set or the graph-adjacent neighbor set, while preserving routing paths for edges between unchanged components.
5. IF incremental mode is enabled and the changed set is empty, THEN THE Layout_Engine SHALL return the existing positions unmodified.

### Requirement 12: Deterministic Output

**User Story:** As a NiFi developer, I want identical input flows to always produce identical output layouts, so that the layout engine behaves predictably and results are reproducible.

#### Acceptance Criteria

1. THE Layout_Engine SHALL produce byte-identical output positions when invoked with the same input graph and the same Layout_Options, regardless of the execution environment (operating system, JVM version, or number of CPU cores).
2. THE Layout_Engine SHALL use deterministic ordering (e.g., LinkedHashMap, TreeMap, or sorted lists) for all internal collections and algorithm iterations.
3. THE Layout_Engine SHALL use stable sorting algorithms in all ordering operations, ensuring that elements with equal keys retain their insertion order.
4. THE Layout_Engine SHALL avoid dependency on system-level random number generators, thread scheduling, or hash map iteration order.
5. WHEN parallel processing of subgraphs is enabled, THE Layout_Engine SHALL merge subgraph results in a deterministic order (by the same ordering defined in Requirement 10, criterion 5) to guarantee deterministic final output.

### Requirement 13: Configuration Model

**User Story:** As a NiFi developer, I want a comprehensive configuration model, so that I can customize spacing, direction, grid, margins, routing, and behavior to match my team's preferences.

#### Acceptance Criteria

1. THE Layout_Engine SHALL accept a Layout_Options configuration object containing: horizontal spacing (integer, default 80, valid range 1–10000), vertical spacing (integer, default 100, valid range 1–10000), flow direction (enum: TOP_TO_BOTTOM, LEFT_TO_RIGHT, BOTTOM_TO_TOP, RIGHT_TO_LEFT, default TOP_TO_BOTTOM), grid size (integer, default 20, valid range 1–1000), margins top/bottom/left/right (integers, default 50 each, valid range 0–10000), padding (integer, default 40, valid range 0–10000), alignment mode (enum: CENTER, LEFT, RIGHT, default CENTER), port spacing (integer, default 30, valid range 1–1000), label spacing (integer, default 20, valid range 0–1000), routing mode (enum: ORTHOGONAL, DIRECT, default ORTHOGONAL), crossing minimization strategy (enum: MEDIAN, BARYCENTER, default MEDIAN), packing strategy (enum: VERTICAL, HORIZONTAL, GRID, default VERTICAL), incremental mode flag (boolean, default false), and maximum iterations (integer, default 24, valid range 1–1000).
2. THE Layout_Engine SHALL apply the specified default values for all Layout_Options properties when values are not explicitly configured by the caller.
3. WHEN a configuration value is provided outside the valid range specified in criterion 1, THE Layout_Engine SHALL throw a validation exception identifying the invalid property name, the provided value, and the valid range.
4. THE Layout_Options SHALL be immutable after construction using a builder pattern to ensure thread safety.

### Requirement 14: Position Write-Back

**User Story:** As a NiFi developer, I want the computed positions written back to NiFi model objects or DTOs, so that the layout results can be applied to the actual NiFi canvas.

#### Acceptance Criteria

1. WHEN layout computation is complete, THE NiFi_Adapter SHALL update the position (x, y) of each component in the original NiFi model or DTO objects, excluding virtual nodes inserted during layer assignment.
2. THE NiFi_Adapter SHALL update only position (x, y) and dimension (width, height for process groups) properties, preserving all other component properties unchanged.
3. WHEN connection routing paths have been computed, THE NiFi_Adapter SHALL update the bend-point positions for each Connection in the original NiFi model or DTO objects.
4. THE NiFi_Adapter SHALL produce a layout result object containing the list of updated components with their original positions, new positions, and the total number of components repositioned.
5. IF a component in the internal graph cannot be mapped back to the original NiFi model, THEN THE NiFi_Adapter SHALL throw an exception identifying the unmapped component by its internal identifier and type.
6. IF an error occurs during write-back of any component, THEN THE NiFi_Adapter SHALL leave previously written components in their updated state and report the error identifying the component that failed and the cause.

### Requirement 15: Extension Points

**User Story:** As a library consumer, I want well-defined extension points, so that I can plug in custom algorithms for layout, routing, spacing, collision resolution, and graph building without modifying the core library.

#### Acceptance Criteria

1. THE Layout_Engine SHALL define a strategy interface for layout algorithms that accepts a layered graph with node orderings and produces coordinate assignments for all nodes, allowing custom implementations to be registered at pipeline construction time.
2. THE Layout_Engine SHALL define a strategy interface for connection routing algorithms that accepts a graph with assigned coordinates and produces routing paths for all connections, allowing custom implementations to be registered at pipeline construction time.
3. THE Layout_Engine SHALL define a strategy interface for collision resolution that accepts a set of positioned components with bounding boxes and produces adjusted positions with no overlaps, allowing custom implementations to be registered at pipeline construction time.
4. THE Layout_Engine SHALL define a strategy interface for spacing strategies that accepts layout dimensions and component counts and produces horizontal spacing, vertical spacing, and margin values, allowing custom implementations to be registered at pipeline construction time.
5. THE Layout_Engine SHALL define a strategy interface for graph builders that accepts NiFi flow data and produces the internal graph representation, allowing custom NiFi model adapters to be registered at pipeline construction time.
6. THE Layout_Engine SHALL use composition to assemble the processing pipeline, such that each pipeline stage delegates to its corresponding strategy implementation and no stage contains hard-coded algorithm logic.
7. IF no custom strategy implementation is registered for a pipeline stage, THEN THE Layout_Engine SHALL use the built-in default implementation for that stage.
8. IF a registered custom strategy implementation throws an exception during pipeline execution, THEN THE Layout_Engine SHALL halt the pipeline and report an error identifying the strategy interface, the failed stage, and the cause of failure.
9. IF a duplicate strategy registration is attempted for the same pipeline stage, THEN THE Layout_Engine SHALL replace the previously registered implementation with the new one.

### Requirement 16: Performance and Scalability

**User Story:** As a NiFi developer working with large flows, I want the layout engine to perform efficiently, so that layouts complete in reasonable time even for flows with thousands of processors.

#### Acceptance Criteria

1. THE Layout_Engine SHALL compute a layout for a graph of 100 processors with up to 200 connections within 1 second on a reference workstation equipped with a 4-core CPU at 2.5 GHz or higher and 8 GB of available RAM.
2. THE Layout_Engine SHALL compute a layout for a graph of 500 processors with up to 1000 connections within 5 seconds on a reference workstation equipped with a 4-core CPU at 2.5 GHz or higher and 8 GB of available RAM.
3. THE Layout_Engine SHALL compute a layout for a graph of 2000 processors with up to 4000 connections within 20 seconds on a reference workstation equipped with a 4-core CPU at 2.5 GHz or higher and 8 GB of available RAM.
4. THE Layout_Engine SHALL compute a layout for a graph of 10000 processors with up to 20000 connections within 120 seconds on a reference workstation equipped with a 4-core CPU at 2.5 GHz or higher and 8 GB of available RAM.
5. WHEN a layout has been previously computed and a single processor is added or removed, THEN THE Layout_Engine SHALL complete the updated layout in no more than 50% of the time required for a full layout of the same graph size.
6. WHEN multiple disconnected subgraphs exist within the flow, THE Layout_Engine SHALL process independent subgraphs concurrently such that total layout time does not exceed 1.5 times the layout time of the largest individual subgraph.
7. WHILE computing layout for graphs of 2000 or more processors, THE Layout_Engine SHALL not cause individual pause events longer than 200 milliseconds due to memory management, and peak memory usage SHALL not exceed 4 times the memory size of the input graph representation.

### Requirement 17: Architectural Separation

**User Story:** As a library maintainer, I want strict separation between NiFi-specific code and generic graph algorithms, so that the algorithms can be reused independently and the library remains maintainable.

#### Acceptance Criteria

1. THE Layout_Engine SHALL organize code into distinct packages: graph (data structures), algorithm (generic graph algorithms), layout (layout-specific algorithms), parser (NiFi model parsing), router (connection routing), spacing (spacing strategies), collision (collision detection and resolution), model (internal domain model), service (orchestration), writer (position write-back), and util (shared utilities).
2. THE Layout_Engine SHALL ensure that packages graph, algorithm, layout, router, spacing, collision, model, and util contain no imports from NiFi-specific libraries (any class under org.apache.nifi namespace or NiFi REST DTO packages).
3. THE Layout_Engine SHALL confine all NiFi-specific dependencies (any class under org.apache.nifi namespace or NiFi REST DTO packages) to the parser and writer packages.
4. THE Layout_Engine SHALL enforce a layered dependency direction such that: parser and writer depend on model and service; service depends on graph, algorithm, layout, router, spacing, collision, and model; graph, algorithm, layout, router, spacing, collision, and util SHALL NOT depend on parser, writer, or service.
5. THE Layout_Engine SHALL use dependency injection to compose the processing pipeline, such that each strategy interface (layout algorithm, connection routing, collision resolution, spacing, and graph building) can be substituted with a custom implementation without modifying or recompiling other packages.
6. THE Layout_Engine SHALL ensure that the service package depends only on strategy interfaces defined in their respective packages, not on concrete implementations of those interfaces.

### Requirement 18: Label and Port Positioning

**User Story:** As a NiFi developer, I want labels and ports positioned appropriately relative to their associated components, so that annotations are readable and ports clearly indicate connectivity.

#### Acceptance Criteria

1. THE Coordinate_Assigner SHALL position Labels above their associated components along the primary flow axis with the configured label spacing as the gap between the label bottom edge and the component top edge.
2. THE Coordinate_Assigner SHALL position Input Ports at the entry edge of the process group based on the configured flow direction: top edge for TOP_TO_BOTTOM, left edge for LEFT_TO_RIGHT, bottom edge for BOTTOM_TO_TOP, right edge for RIGHT_TO_LEFT.
3. THE Coordinate_Assigner SHALL position Output Ports at the exit edge of the process group based on the configured flow direction: bottom edge for TOP_TO_BOTTOM, right edge for LEFT_TO_RIGHT, top edge for BOTTOM_TO_TOP, left edge for RIGHT_TO_LEFT.
4. THE Coordinate_Assigner SHALL distribute multiple ports evenly along the assigned edge with the configured port spacing between adjacent port bounding boxes, centered on the edge midpoint.
5. THE Collision_Resolver SHALL include Labels and Ports in overlap detection to prevent them from being obscured by other components.
6. IF a Label has no associated component, THEN THE Coordinate_Assigner SHALL treat it as an independent node and assign it a position through the standard layer assignment and coordinate pipeline.

### Requirement 19: Funnel Handling

**User Story:** As a NiFi developer, I want funnels positioned logically within the flow hierarchy, so that merge points in the data flow are clearly visible.

#### Acceptance Criteria

1. THE Layer_Assigner SHALL assign Funnels to layers using the same topological sorting rules applied to Processors, such that for every edge (u, Funnel), the layer of u is less than the layer of the Funnel.
2. THE Coordinate_Assigner SHALL size Funnel nodes using their actual NiFi canvas dimensions as provided in the source NiFi model or DTO.
3. WHEN a Funnel has 2 or more incoming connections, THE Connection_Router SHALL route each incoming connection path to the Funnel entry point with at least the configured port spacing between adjacent connection paths at the point of entry.
4. IF a Funnel has no incoming connections, THEN THE Layer_Assigner SHALL treat the Funnel as a Root_Processor and assign it to the first layer.

### Requirement 20: Processing Pipeline Orchestration

**User Story:** As a NiFi developer, I want a well-defined processing pipeline, so that the layout computation proceeds through all stages in the correct order and produces a complete result.

#### Acceptance Criteria

1. THE Layout_Engine SHALL execute the processing pipeline in the following order: Parse Flow, Build Graph, Detect Components, Cycle Detection, Assign Layers, Crossing Minimization, Coordinate Assignment, Collision Resolution, Connection Routing, Grid Alignment, Write Back Positions.
2. IF any stage in the pipeline fails, THEN THE Layout_Engine SHALL halt execution, discard any partial results from the failed stage, and report an error identifying the failed stage name and the cause of failure.
3. THE Layout_Engine SHALL provide progress feedback via a caller-supplied callback interface, invoking the callback with the current stage name before each pipeline stage begins execution.
4. THE Layout_Engine SHALL allow pipeline stages to be replaced with custom implementations via the strategy interfaces defined in the extension point contracts.
5. IF the input ProcessGroup contains zero components, THEN THE Layout_Engine SHALL return a valid empty layout result without executing the pipeline stages beyond Parse Flow and Build Graph.
