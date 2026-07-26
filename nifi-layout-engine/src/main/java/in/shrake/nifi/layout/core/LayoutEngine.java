/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package in.shrake.nifi.layout.core;

import in.shrake.nifi.layout.core.algorithm.ConnectedComponents;
import in.shrake.nifi.layout.core.collision.CollisionResult;
import in.shrake.nifi.layout.core.collision.PositionedComponent;
import in.shrake.nifi.layout.core.exception.MaxNestingDepthException;
import in.shrake.nifi.layout.core.graph.GraphAnalyzer;
import in.shrake.nifi.layout.core.model.*;
import in.shrake.nifi.layout.core.service.*;
import in.shrake.nifi.layout.core.service.stage.GraphAnalysisStage;
import in.shrake.nifi.layout.core.service.stage.CycleDetectionStage;
import in.shrake.nifi.layout.core.service.stage.LayerAssignmentStage;
import in.shrake.nifi.layout.core.service.stage.CrossingMinimizationStage;
import in.shrake.nifi.layout.core.service.stage.CoordinateAssignmentStage;
import in.shrake.nifi.layout.core.service.stage.CollisionResolutionStage;
import in.shrake.nifi.layout.core.service.stage.RoutingStage;
import in.shrake.nifi.layout.core.service.stage.GridAlignmentStage;
import in.shrake.nifi.layout.core.layout.LayerAssignmentStrategy;
import in.shrake.nifi.layout.core.layout.LongestPathLayerAssigner;
import in.shrake.nifi.layout.core.layout.LayoutAlgorithm;
import in.shrake.nifi.layout.core.layout.DefaultCoordinateAssigner;
import in.shrake.nifi.layout.core.layout.CrossingMinimizationStrategy;
import in.shrake.nifi.layout.core.layout.MedianCrossingMinimizer;
import in.shrake.nifi.layout.core.layout.BarycenterCrossingMinimizer;
import in.shrake.nifi.layout.core.graph.DefaultGraphAnalysisStrategy;
import in.shrake.nifi.layout.core.algorithm.DefaultCycleDetectionStrategy;
import in.shrake.nifi.layout.core.collision.CollisionResolutionStrategy;
import in.shrake.nifi.layout.core.collision.DisplacementCollisionResolver;
import in.shrake.nifi.layout.core.router.RoutingStrategy;
import in.shrake.nifi.layout.core.router.ConfiguredRouter;
import in.shrake.nifi.layout.core.spacing.SpacingStrategy;
import in.shrake.nifi.layout.core.spacing.DefaultSpacingStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LayoutEngine {

    private final LayoutPipeline pipeline;
    private final CollisionResolutionStrategy collisionStrategy;
    private final RoutingStrategy routingStrategy;

    private LayoutEngine(LayoutPipeline pipeline, CollisionResolutionStrategy collisionStrategy, RoutingStrategy routingStrategy) {
        this.pipeline = pipeline;
        this.collisionStrategy = collisionStrategy;
        this.routingStrategy = routingStrategy;
    }

    public static LayoutEngine create() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public LayoutResult layout(LayoutGraph graph, LayoutOptions options) {
        return executePipeline(graph, options, Collections.emptySet(), false, 0, null);
    }

    public LayoutResult layout(LayoutGraph graph, LayoutOptions options,
                               LayoutProgressCallback callback) {
        return executePipeline(graph, options, Collections.emptySet(), false, 0, callback);
    }

    public LayoutResult layoutIncremental(LayoutGraph graph, LayoutOptions options, Set<String> changedComponentIds) {
        return executePipeline(graph, options, changedComponentIds, true, 0, null);
    }

    private LayoutResult executePipeline(LayoutGraph graph, LayoutOptions options, Set<String> changedComponentIds,
                                         boolean incrementalMode, int depth,
                                         LayoutProgressCallback callback) {
        if (depth > 10) {
            throw new MaxNestingDepthException(depth, graph.getProcessGroupId());
        }

        if (graph == null || graph.getNodes().isEmpty()) {
            return new LayoutResult(Collections.emptyList(), 0, Collections.emptyMap(), 0, Collections.emptyList());
        }

        if (incrementalMode && (changedComponentIds == null || changedComponentIds.isEmpty())) {
            return new LayoutResult(Collections.emptyList(), 0, Collections.emptyMap(), 0, Collections.emptyList());
        }

        final Set<String> affectedSet;
        if (incrementalMode) {
            affectedSet = GraphAnalyzer.computeAffectedSet(graph, changedComponentIds);
            if (affectedSet.isEmpty()) {
                return new LayoutResult(Collections.emptyList(), 0, Collections.emptyMap(), 0, Collections.emptyList());
            }
        } else {
            affectedSet = Collections.emptySet();
        }

        List<LayoutResult> nestedResults = new ArrayList<>();
        Map<String, LayoutNode> updatedNodes = new java.util.LinkedHashMap<>(graph.getNodes());

        for (Map.Entry<String, LayoutGraph> entry : graph.getSubgraphs().entrySet()) {
            String childGroupId = entry.getKey();
            LayoutGraph childGraph = entry.getValue();

            // In incremental mode, scope changed IDs to those actually present in the child graph.
            // If none of the changed IDs exist in this child, the child is unaffected and its
            // existing bounds must be preserved without re-layout.
            final Set<String> childChangedIds;
            if (incrementalMode) {
                childChangedIds = changedComponentIds.stream()
                        .filter(id -> containsNode(childGraph, id))
                        .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
                if (childChangedIds.isEmpty()) {
                    // No changed nodes in this child group – skip re-layout entirely.
                    continue;
                }
            } else {
                childChangedIds = changedComponentIds;
            }

            LayoutResult childResult = executePipeline(childGraph, options, childChangedIds,
                    incrementalMode, depth + 1, callback);
            nestedResults.add(childResult);
            
            BoundingBox childBounds = SubgraphPacker.computeBounds(childResult);
            
            int newWidth = childBounds.width();
            int newHeight = childBounds.height();
            
            if (childResult.getUpdates().isEmpty()) {
                newWidth = Math.max(100, options.getPadding() * 3);
                newHeight = Math.max(100, options.getPadding() * 2);
            }
            
            newWidth += options.getPadding() * 2;
            newHeight += options.getPadding() * 2;
            
            LayoutNode groupNode = updatedNodes.get(childGroupId);
            if (groupNode != null && groupNode.getType() == NodeType.PROCESS_GROUP) {
                BoundingBox newBox = new BoundingBox(
                    groupNode.getBoundingBox().x(),
                    groupNode.getBoundingBox().y(),
                    newWidth,
                    newHeight
                );
                updatedNodes.put(childGroupId, groupNode.withBoundingBox(newBox));
            }
        }
        
        LayoutGraph graphForLayout = new LayoutGraph(
            updatedNodes,
            graph.getEdges(),
            graph.getSubgraphs(),
            graph.getProcessGroupId()
        );

        List<LayoutGraph> components = ConnectedComponents.partition(graphForLayout);
        boolean reportProgress = callback != null && components.size() <= 1;

        LayoutResult currentLevelResult;
        if (components.size() <= 1) {
            LayoutGraph component = components.isEmpty() ? graphForLayout : components.get(0);
            currentLevelResult = executeSinglePipeline(component, options, changedComponentIds,
                    incrementalMode, affectedSet, reportProgress ? callback : null);
        } else {
            List<SubgraphPacker.SubgraphLayout> subgraphs = components.parallelStream()
                .map(comp -> {
                    LayoutResult result = executeSinglePipeline(comp, options, changedComponentIds,
                            incrementalMode, affectedSet, null);
                    String minId = comp.getNodes().keySet().stream().min(String::compareTo).orElse("");
                    BoundingBox bounds = SubgraphPacker.computeBounds(result);
                    return new SubgraphPacker.SubgraphLayout(result, minId, comp.getNodes().size(), bounds);
                })
                .toList();
                
            SubgraphPacker packer = new SubgraphPacker();
            currentLevelResult = packer.pack(subgraphs, options);
        }

        if (incrementalMode && components.size() > 1) {
            currentLevelResult = restoreAndConstrainIncrementalResult(
                graphForLayout, options, affectedSet, currentLevelResult);
        }
        
        List<ComponentUpdate> allUpdates = new ArrayList<>(currentLevelResult.getUpdates());
        Map<String, List<Position>> allRoutes = new java.util.LinkedHashMap<>(currentLevelResult.getConnectionBendPoints());
        int totalRepositioned = currentLevelResult.getTotalComponentsRepositioned();
        long totalComputationMs = currentLevelResult.getComputationTimeMs();
        List<String> allWarnings = new ArrayList<>(currentLevelResult.getWarnings());
        
        for (LayoutResult childRes : nestedResults) {
            allUpdates.addAll(childRes.getUpdates());
            allRoutes.putAll(childRes.getConnectionBendPoints());
            totalRepositioned += childRes.getTotalComponentsRepositioned();
            totalComputationMs += childRes.getComputationTimeMs();
            allWarnings.addAll(childRes.getWarnings());
        }
        
        return new LayoutResult(allUpdates, totalRepositioned, allRoutes, totalComputationMs, allWarnings);
    }

    private static boolean containsNode(LayoutGraph graph, String nodeId) {
        if (graph.getNodes().containsKey(nodeId)) {
            return true;
        }
        return graph.getSubgraphs().values().stream()
                .anyMatch(child -> containsNode(child, nodeId));
    }

    private LayoutResult restoreAndConstrainIncrementalResult(
            LayoutGraph graph, LayoutOptions options, Set<String> affectedIds, LayoutResult packedResult) {
        Map<String, ComponentUpdate> packedUpdates = new java.util.LinkedHashMap<>();
        for (ComponentUpdate update : packedResult.getUpdates()) {
            packedUpdates.put(update.componentId(), update);
        }

        List<PositionedComponent> components = new ArrayList<>();
        Set<String> fixedIds = new java.util.LinkedHashSet<>();
        for (LayoutNode node : graph.getNodes().values()) {
            ComponentUpdate packed = packedUpdates.get(node.getId());
            Position position;
            if (affectedIds.contains(node.getId()) && packed != null) {
                position = packed.newPosition();
            } else {
                position = new Position(node.getBoundingBox().x(), node.getBoundingBox().y());
                fixedIds.add(node.getId());
            }
            BoundingBox bounds = new BoundingBox(
                position.x(), position.y(), node.getBoundingBox().width(), node.getBoundingBox().height());
            components.add(new PositionedComponent(node, position, bounds));
        }

        CollisionResult collisionResult =
            collisionStrategy.resolve(components, options, fixedIds);
        Map<String, Position> positions = new java.util.LinkedHashMap<>();
        Map<String, BoundingBox> bounds = new java.util.LinkedHashMap<>();
        List<ComponentUpdate> updates = new ArrayList<>();
        int repositioned = 0;
        for (PositionedComponent component : collisionResult.getComponents()) {
            LayoutNode node = component.node();
            positions.put(node.getId(), component.position());
            bounds.put(node.getId(), component.boundingBox());
            Position original = new Position(node.getBoundingBox().x(), node.getBoundingBox().y());
            updates.add(new ComponentUpdate(node.getId(), node.getParentGroupId(), node.getType(),
                original, component.position(), component.boundingBox()));
            if (!original.equals(component.position())) {
                repositioned++;
            }
        }

        RoutingResult allRoutes = routingStrategy.computeRoutes(
            graph, new CoordinateAssignment(positions, bounds), options);
        Map<String, List<Position>> affectedRoutes = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, List<Position>> route : allRoutes.getEdgePaths().entrySet()) {
            LayoutEdge edge = graph.getEdges().get(route.getKey());
            if (edge != null && (affectedIds.contains(edge.getSourceNodeId())
                    || affectedIds.contains(edge.getTargetNodeId()))) {
                affectedRoutes.put(route.getKey(), route.getValue());
            }
        }
        List<String> routingWarnings = allRoutes.getWarnings().stream()
            .map(Object::toString)
            .collect(Collectors.toCollection(ArrayList::new));
        return new LayoutResult(updates, repositioned, affectedRoutes,
            packedResult.getComputationTimeMs(), routingWarnings);
    }

    private LayoutResult executeSinglePipeline(LayoutGraph graph, LayoutOptions options,
                                               Set<String> changedComponentIds, boolean incrementalMode,
                                               Set<String> affectedIds,
                                               LayoutProgressCallback callback) {
        long start = System.currentTimeMillis();
        PipelineContext context = new PipelineContext(graph, options)
                                    .withIncrementalData(incrementalMode, affectedIds)
                                    .withChangedComponentIds(changedComponentIds);
                                    
        PipelineContext resultContext = pipeline.execute(context, callback);
        return buildResult(resultContext, System.currentTimeMillis() - start);
    }
    
    private LayoutResult buildResult(PipelineContext context, long durationMs) {
        CoordinateAssignment coords = context.getCoordinates();
        RoutingResult routes = context.getRouting();
        
        List<ComponentUpdate> updates = new ArrayList<>();
        int repositioned = 0;
        
        if (coords != null) {
            for (LayoutNode node : context.getGraph().getNodes().values()) {
                Position newPos = coords.getPositions().get(node.getId());
                if (newPos != null) {
                    Position originalPos = new Position(node.getBoundingBox().x(), node.getBoundingBox().y());
                    updates.add(new ComponentUpdate(
                        node.getId(), 
                        node.getParentGroupId(),
                        node.getType(), 
                        originalPos,
                        newPos, 
                        coords.getBounds().get(node.getId())
                    ));
                    if (!originalPos.equals(newPos)) {
                        repositioned++;
                    }
                }
            }
        }

        List<String> warnings = new ArrayList<>();
        if (routes != null) {
            routes.getWarnings().forEach(w -> warnings.add(w.toString()));
        }

        return new LayoutResult(
            updates, 
            repositioned, 
            routes != null ? routes.getEdgePaths() : Collections.emptyMap(), 
            durationMs, 
            warnings
        );
    }

    public static class Builder {
        private LayerAssignmentStrategy layerStrategy;
        private LayoutAlgorithm coordinateStrategy;
        private CollisionResolutionStrategy collisionStrategy;
        private RoutingStrategy routingStrategy;
        private SpacingStrategy spacingStrategy;

        public Builder withLayerAssignmentStrategy(LayerAssignmentStrategy strategy) {
            this.layerStrategy = strategy;
            return this;
        }

        public Builder withCoordinateStrategy(LayoutAlgorithm strategy) {
            this.coordinateStrategy = strategy;
            return this;
        }

        public Builder withCollisionStrategy(CollisionResolutionStrategy strategy) {
            this.collisionStrategy = strategy;
            return this;
        }

        public Builder withRoutingStrategy(RoutingStrategy strategy) {
            this.routingStrategy = strategy;
            return this;
        }
        
        public Builder withSpacingStrategy(SpacingStrategy strategy) {
            this.spacingStrategy = strategy;
            return this;
        }

        public LayoutEngine build() {
            List<PipelineStage> stages = new ArrayList<>();
            
            stages.add(new GraphAnalysisStage(new DefaultGraphAnalysisStrategy()));
            stages.add(new CycleDetectionStage(new DefaultCycleDetectionStrategy()));

            LayerAssignmentStrategy ls = layerStrategy != null ? layerStrategy : new LongestPathLayerAssigner();
            stages.add(new LayerAssignmentStage(ls));
            CrossingMinimizationStrategy median = new MedianCrossingMinimizer();
            CrossingMinimizationStrategy barycenter = new BarycenterCrossingMinimizer();
            stages.add(new CrossingMinimizationStage(median, barycenter));
            
            SpacingStrategy ss = spacingStrategy != null ? spacingStrategy : new DefaultSpacingStrategy();
            LayoutAlgorithm cs = coordinateStrategy != null ? coordinateStrategy : new DefaultCoordinateAssigner(ss);
            stages.add(new CoordinateAssignmentStage(cs));
            
            CollisionResolutionStrategy colls = collisionStrategy != null ? collisionStrategy : new DisplacementCollisionResolver();
            stages.add(new CollisionResolutionStage(colls));
            
            // Align components to the grid before routing so that route endpoint anchors
            // are automatically on the grid without a separate post-routing snap pass.
            stages.add(new GridAlignmentStage());

            RoutingStrategy rs = routingStrategy != null ? routingStrategy : new ConfiguredRouter();
            stages.add(new RoutingStage(rs));

            return new LayoutEngine(new LayoutPipeline(stages), colls, rs);
        }
    }
}
