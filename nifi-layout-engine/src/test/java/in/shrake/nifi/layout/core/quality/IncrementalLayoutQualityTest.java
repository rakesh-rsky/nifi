/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.quality;

import in.shrake.nifi.layout.core.LayoutEngine;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IncrementalLayoutQualityTest {
    private static final LayoutOptions OPTIONS = LayoutOptions.defaults();

    @Test
    void disconnectedComponentRetainsPositionsAndUnaffectedRoute() {
        final LayoutGraph graph = graphWithPositionedComponents();
        final LayoutResult result = LayoutEngine.create()
                .layoutIncremental(graph, OPTIONS, Set.of("a"));
        final Map<String, ComponentUpdate> updates = updatesById(result);

        assertEquals(new Position(1200, 800), updates.get("c").newPosition());
        assertEquals(new Position(1200, 1100), updates.get("d").newPosition());
        assertTrue(result.getConnectionBendPoints().containsKey("a-b"));
        assertFalse(result.getConnectionBendPoints().containsKey("c-d"));
    }

    @Test
    void unchangedNestedGroupRetainsItsInternalArrangement() {
        final LayoutNode childProcessor = positionedNode(
                "child-processor", "child-group", 140, 220);
        final LayoutGraph child = new LayoutGraph(
                Map.of(childProcessor.getId(), childProcessor),
                Map.of(),
                Map.of(),
                "child-group");
        final LayoutGraph root = new LayoutGraph(
                Map.of(
                        "root-processor", positionedNode(
                                "root-processor", "root", 0, 0),
                        "child-group", new LayoutNode(
                                "child-group",
                                NodeType.PROCESS_GROUP,
                                new BoundingBox(800, 600, 600, 400),
                                Map.of(),
                                "root")),
                Map.of(),
                Map.of("child-group", child),
                "root");

        final LayoutResult result = LayoutEngine.create()
                .layoutIncremental(root, OPTIONS, Set.of("root-processor"));

        assertTrue(result.getUpdates().stream().noneMatch(update ->
                update.componentId().equals("child-processor")));
        assertEquals(new BoundingBox(140, 220, 350, 130),
                child.getNode("child-processor").getBoundingBox());
    }

    @Test
    void changedComponentAndNeighborAreRoutedWithoutMovingOtherComponent() {
        final LayoutGraph graph = graphWithPositionedComponents();
        final LayoutResult result = LayoutEngine.create()
                .layoutIncremental(graph, OPTIONS, Set.of("a"));
        final Map<String, ComponentUpdate> updates = updatesById(result);

        assertTrue(result.getConnectionBendPoints().containsKey("a-b"));
        assertTrue(result.getConnectionBendPoints().containsKey("b-e"));
        assertEquals(new Position(0, 600), updates.get("e").newPosition());
        assertEquals(new Position(1200, 800), updates.get("c").newPosition());
        assertEquals(new Position(1200, 1100), updates.get("d").newPosition());
        LayoutQualityAssertions.assertNoComponentOverlaps(graph, result);
    }

    private static LayoutGraph graphWithPositionedComponents() {
        final Map<String, LayoutNode> nodes = new LinkedHashMap<>();
        nodes.put("a", positionedNode("a", "root", 0, 0));
        nodes.put("b", positionedNode("b", "root", 0, 300));
        nodes.put("e", positionedNode("e", "root", 0, 600));
        nodes.put("c", positionedNode("c", "root", 1200, 800));
        nodes.put("d", positionedNode("d", "root", 1200, 1100));
        return new LayoutGraph(
                nodes,
                Map.of(
                        "a-b", edge("a-b", "a", "b"),
                        "b-e", edge("b-e", "b", "e"),
                        "c-d", edge("c-d", "c", "d")),
                Map.of(),
                "root");
    }

    private static LayoutNode positionedNode(
            final String id,
            final String parentGroupId,
            final int x,
            final int y) {
        return new LayoutNode(
                id,
                NodeType.PROCESSOR,
                new BoundingBox(x, y, 350, 130),
                Map.of(),
                parentGroupId);
    }

    private static LayoutEdge edge(
            final String id,
            final String source,
            final String target) {
        return new LayoutEdge(id, source, target, "success", "", false, false);
    }

    private static Map<String, ComponentUpdate> updatesById(
            final LayoutResult result) {
        final Map<String, ComponentUpdate> updates = new LinkedHashMap<>();
        result.getUpdates().forEach(update ->
                updates.put(update.componentId(), update));
        return updates;
    }
}
