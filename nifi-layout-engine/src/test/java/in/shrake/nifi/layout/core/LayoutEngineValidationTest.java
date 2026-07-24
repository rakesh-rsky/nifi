/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core;

import in.shrake.nifi.layout.core.layout.CrossingCountComputer;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LayoutEngineValidationTest {

    private static final LayoutOptions OPTIONS = LayoutOptions.defaults();

    @Test
    void representativeFlowHandlesCyclesDisconnectedComponentsNestedGroupsAndRoutes() {
        Map<String, LayoutNode> nodes = new LinkedHashMap<>();
        nodes.put("processor-a", node("processor-a", NodeType.PROCESSOR, 240, 80));
        nodes.put("processor-b", node("processor-b", NodeType.PROCESSOR, 320, 90));
        nodes.put("processor-c", node("processor-c", NodeType.PROCESSOR, 180, 110));
        nodes.put("input", node("input", NodeType.PORT_INPUT, 80, 48));
        nodes.put("funnel", node("funnel", NodeType.FUNNEL, 48, 48));
        nodes.put("output", node("output", NodeType.PORT_OUTPUT, 80, 48));
        nodes.put("label", node("label", NodeType.LABEL, 260, 45));
        nodes.put("remote", node("remote", NodeType.REMOTE_PROCESS_GROUP, 300, 125));
        nodes.put("child", node("child", NodeType.PROCESS_GROUP, 400, 300));

        Map<String, LayoutEdge> edges = new LinkedHashMap<>();
        edges.put("ab", edge("ab", "processor-a", "processor-b"));
        edges.put("bc", edge("bc", "processor-b", "processor-c"));
        edges.put("ca", edge("ca", "processor-c", "processor-a"));
        edges.put("in-funnel", edge("in-funnel", "input", "funnel"));
        edges.put("funnel-out", edge("funnel-out", "funnel", "output"));

        LayoutGraph child = new LayoutGraph(
                Map.of("child-processor", node("child-processor", NodeType.PROCESSOR, 275, 95)),
                Map.of(), Map.of(), "child");
        LayoutGraph graph = new LayoutGraph(nodes, edges, Map.of("child", child), "root");

        LayoutResult result = LayoutEngine.create().layout(graph, OPTIONS);
        Map<String, ComponentUpdate> updates = updatesById(result);

        assertEquals(nodes.size() + 1, updates.size());
        assertEquals(edges.keySet(), result.getConnectionBendPoints().keySet());
        assertNoOverlaps(nodes.keySet().stream().map(id -> updates.get(id).newBounds()).toList());

        nodes.values().stream()
                .filter(node -> node.getType() != NodeType.PROCESS_GROUP)
                .forEach(node -> {
                    BoundingBox original = node.getBoundingBox();
                    BoundingBox laidOut = updates.get(node.getId()).newBounds();
                    assertEquals(original.width(), laidOut.width(), node.getId() + " width");
                    assertEquals(original.height(), laidOut.height(), node.getId() + " height");
                });
        LayoutNode nested = child.getNode("child-processor");
        assertEquals(nested.getBoundingBox().width(), updates.get("child-processor").newBounds().width());
        assertEquals(nested.getBoundingBox().height(), updates.get("child-processor").newBounds().height());

        result.getConnectionBendPoints().forEach((edgeId, path) -> {
            LayoutEdge edge = edges.get(edgeId);
            if (!path.isEmpty()) {
                assertNotInside(path.get(0), updates.get(edge.getSourceNodeId()).newBounds());
                assertNotInside(path.get(path.size() - 1), updates.get(edge.getTargetNodeId()).newBounds());
            }
        });
    }

    @Test
    void incrementalLayoutPreservesDisconnectedPositionsAndRoutes() {
        Map<String, LayoutNode> nodes = Map.of(
                "a", positionedNode("a", 0, 0),
                "b", positionedNode("b", 0, 0),
                "c", positionedNode("c", 1000, 1000),
                "d", positionedNode("d", 1000, 1300));
        Map<String, LayoutEdge> edges = Map.of(
                "ab", edge("ab", "a", "b"),
                "cd", edge("cd", "c", "d"));

        LayoutResult result = LayoutEngine.create().layoutIncremental(
                new LayoutGraph(nodes, edges, Map.of(), "root"), OPTIONS, Set.of("a"));
        Map<String, ComponentUpdate> updates = updatesById(result);

        assertEquals(new Position(1000, 1000), updates.get("c").newPosition());
        assertEquals(new Position(1000, 1300), updates.get("d").newPosition());
        assertTrue(result.getConnectionBendPoints().containsKey("ab"));
        assertFalse(result.getConnectionBendPoints().containsKey("cd"));
    }

    @Test
    void crossingCounterReportsKnownCrossing() {
        LayoutNode a = node("a", NodeType.PROCESSOR, 240, 80);
        LayoutNode b = node("b", NodeType.PROCESSOR, 240, 80);
        LayoutNode c = node("c", NodeType.PROCESSOR, 240, 80);
        LayoutNode d = node("d", NodeType.PROCESSOR, 240, 80);

        assertEquals(1, CrossingCountComputer.countCrossings(
                List.of(List.of(a, b), List.of(c, d)),
                Map.of("a", List.of("d"), "b", List.of("c"))));
    }

    @Property(tries = 30)
    void layoutPreservesArbitraryNonSquareDimensionsAndEliminatesOverlap(
            @ForAll @IntRange(min = 40, max = 500) int firstWidth,
            @ForAll @IntRange(min = 30, max = 220) int firstHeight,
            @ForAll @IntRange(min = 40, max = 500) int secondWidth,
            @ForAll @IntRange(min = 30, max = 220) int secondHeight) {
        Map<String, LayoutNode> nodes = Map.of(
                "first", node("first", NodeType.PROCESSOR, firstWidth, firstHeight),
                "second", node("second", NodeType.LABEL, secondWidth, secondHeight));

        for (FlowDirection direction : FlowDirection.values()) {
            LayoutResult result = LayoutEngine.create().layout(
                    new LayoutGraph(nodes, Map.of(), Map.of(), "root"),
                    LayoutOptions.builder().flowDirection(direction).build());
            Map<String, ComponentUpdate> updates = updatesById(result);

            assertEquals(firstWidth, updates.get("first").newBounds().width(), direction + " first width");
            assertEquals(firstHeight, updates.get("first").newBounds().height(), direction + " first height");
            assertEquals(secondWidth, updates.get("second").newBounds().width(), direction + " second width");
            assertEquals(secondHeight, updates.get("second").newBounds().height(), direction + " second height");
            assertNoOverlaps(updates.values().stream().map(ComponentUpdate::newBounds).toList());
        }
    }

    private static LayoutNode node(String id, NodeType type, int width, int height) {
        return new LayoutNode(id, type, new BoundingBox(0, 0, width, height), Map.of(), "root");
    }

    private static LayoutNode positionedNode(String id, int x, int y) {
        return new LayoutNode(id, NodeType.PROCESSOR, new BoundingBox(x, y, 240, 80), Map.of(), "root");
    }

    private static LayoutEdge edge(String id, String source, String target) {
        return new LayoutEdge(id, source, target, null, null, false, source.equals(target));
    }

    private static Map<String, ComponentUpdate> updatesById(LayoutResult result) {
        Map<String, ComponentUpdate> updates = new LinkedHashMap<>();
        result.getUpdates().forEach(update -> updates.put(update.componentId(), update));
        return updates;
    }

    private static void assertNoOverlaps(List<BoundingBox> bounds) {
        List<String> overlaps = new ArrayList<>();
        for (int i = 0; i < bounds.size(); i++) {
            for (int j = i + 1; j < bounds.size(); j++) {
                BoundingBox a = bounds.get(i);
                BoundingBox b = bounds.get(j);
                if (a.x() < b.right() && a.right() > b.x()
                        && a.y() < b.bottom() && a.bottom() > b.y()) {
                    overlaps.add(i + "-" + j);
                }
            }
        }
        assertTrue(overlaps.isEmpty(), "overlap count=" + overlaps.size() + ": " + overlaps);
    }

    private static void assertNotInside(Position point, BoundingBox bounds) {
        boolean inside = point.x() > bounds.x() && point.x() < bounds.right()
                && point.y() > bounds.y() && point.y() < bounds.bottom();
        assertFalse(inside, point + " is inside " + bounds);
    }
}
