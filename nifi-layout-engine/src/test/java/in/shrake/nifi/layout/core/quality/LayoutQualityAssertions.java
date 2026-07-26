/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.quality;

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.util.Geometry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class LayoutQualityAssertions {
    private LayoutQualityAssertions() {
    }

    public static void assertNoComponentOverlaps(
            final LayoutGraph graph,
            final LayoutResult result) {
        final Map<String, BoundingBox> bounds = boundsById(result);
        assertNoComponentOverlaps(graph, bounds);
    }

    public static void assertNoRoutesThroughNonEndpointNodes(
            final LayoutGraph graph,
            final LayoutResult result) {
        final Map<String, BoundingBox> bounds = boundsById(result);
        assertNoRoutesThroughNonEndpointNodes(graph, result, bounds);
    }

    public static void assertParallelLabelAnchorSeparation(
            final LayoutGraph graph,
            final LayoutResult result,
            final FlowDirection direction,
            final int minimumSeparation) {
        final Map<String, List<LayoutEdge>> parallelEdges = new LinkedHashMap<>();
        graph.getEdges().values().forEach(edge -> parallelEdges.computeIfAbsent(
                edge.getSourceNodeId() + "\0" + edge.getTargetNodeId(),
                ignored -> new ArrayList<>()).add(edge));

        final ToIntFunction<Position> laneCoordinate =
                direction == FlowDirection.TOP_TO_BOTTOM
                        || direction == FlowDirection.BOTTOM_TO_TOP
                        ? Position::x : Position::y;
        parallelEdges.values().stream()
                .filter(edges -> edges.size() > 1)
                .forEach(edges -> {
                    final List<Integer> anchors = edges.stream()
                            .map(LayoutEdge::getId)
                            .map(result.getConnectionBendPoints()::get)
                            .peek(path -> assertTrue(path != null && path.size() >= 5,
                                    "parallel route must expose a label lane"))
                            .map(path -> laneCoordinate.applyAsInt(path.get(2)))
                            .sorted()
                            .toList();
                    for (int index = 1; index < anchors.size(); index++) {
                        assertTrue(anchors.get(index) - anchors.get(index - 1)
                                        >= minimumSeparation,
                                "parallel label anchors are too close: " + anchors);
                    }
                });
        graph.getSubgraphs().values().forEach(child ->
                assertParallelLabelAnchorSeparation(
                        child, result, direction, minimumSeparation));
    }

    public static void assertBendLimits(
            final LayoutResult result,
            final Map<String, Integer> limitsByEdge) {
        limitsByEdge.forEach((edgeId, maximumBends) -> {
            final List<Position> path = result.getConnectionBendPoints().get(edgeId);
            assertTrue(path != null && path.size() >= 2,
                    "missing full route for " + edgeId);
            assertTrue(path.size() - 2 <= maximumBends,
                    edgeId + " has " + (path.size() - 2)
                            + " bends; maximum is " + maximumBends);
        });
    }

    public static void assertGridAligned(
            final LayoutResult result,
            final int gridSize) {
        result.getUpdates().forEach(update -> {
            assertEquals(0, Math.floorMod(update.newPosition().x(), gridSize),
                    update.componentId() + " x coordinate");
            assertEquals(0, Math.floorMod(update.newPosition().y(), gridSize),
                    update.componentId() + " y coordinate");
        });
    }

    public static void assertDeterministic(
            final LayoutResult first,
            final LayoutResult second) {
        assertEquals(first.getUpdates().stream()
                        .map(ComponentUpdate::componentId).toList(),
                second.getUpdates().stream()
                        .map(ComponentUpdate::componentId).toList(),
                "component update ordering");
        assertEquals(first.getUpdates().stream()
                        .map(ComponentUpdate::newBounds).toList(),
                second.getUpdates().stream()
                        .map(ComponentUpdate::newBounds).toList(),
                "component geometry");
        assertEquals(new ArrayList<>(first.getConnectionBendPoints().keySet()),
                new ArrayList<>(second.getConnectionBendPoints().keySet()),
                "route ordering");
        assertEquals(first.getConnectionBendPoints(),
                second.getConnectionBendPoints(), "routes");
        assertEquals(first.getWarnings(), second.getWarnings(), "warnings");
    }

    public static Map<String, BoundingBox> boundsById(
            final LayoutResult result) {
        final Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        result.getUpdates().forEach(update ->
                bounds.put(update.componentId(), update.newBounds()));
        return bounds;
    }

    private static void assertNoComponentOverlaps(
            final LayoutGraph graph,
            final Map<String, BoundingBox> bounds) {
        final List<String> nodeIds = new ArrayList<>(graph.getNodes().keySet());
        for (int first = 0; first < nodeIds.size(); first++) {
            for (int second = first + 1; second < nodeIds.size(); second++) {
                final String firstId = nodeIds.get(first);
                final String secondId = nodeIds.get(second);
                assertFalse(bounds.get(firstId).intersects(bounds.get(secondId)),
                        firstId + " overlaps " + secondId);
            }
        }
        graph.getSubgraphs().values().forEach(child ->
                assertNoComponentOverlaps(child, bounds));
    }

    private static void assertNoRoutesThroughNonEndpointNodes(
            final LayoutGraph graph,
            final LayoutResult result,
            final Map<String, BoundingBox> bounds) {
        graph.getEdges().values().forEach(edge -> {
            final List<Position> path =
                    result.getConnectionBendPoints().get(edge.getId());
            assertTrue(path != null && path.size() >= 2,
                    "missing full route for " + edge.getId());
            final List<BoundingBox> obstacles = graph.getNodes().entrySet()
                    .stream()
                    .filter(entry -> !entry.getKey().equals(edge.getSourceNodeId()))
                    .filter(entry -> !entry.getKey().equals(edge.getTargetNodeId()))
                    .map(Map.Entry::getKey)
                    .map(bounds::get)
                    .toList();
            assertFalse(Geometry.pathIntersectsAnyObstacle(path, obstacles, 0),
                    edge.getId() + " crosses a non-endpoint component");
        });
        graph.getSubgraphs().values().forEach(child ->
                assertNoRoutesThroughNonEndpointNodes(child, result, bounds));
    }
}
