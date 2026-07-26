/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.fixture;

import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.support.canonical.RelationshipCodec;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowTopologyFixturesTest {

    @Test
    void fixturesHaveExpectedRootTopologyCounts() {
        final Map<String, int[]> expected = Map.ofEntries(
                Map.entry("linear", new int[]{4, 3}),
                Map.entry("two-way-fan-out", new int[]{3, 2}),
                Map.entry("wide-distribution", new int[]{6, 5}),
                Map.entry("dense-fan-in", new int[]{5, 4}),
                Map.entry("failure-branches", new int[]{4, 4}),
                Map.entry("csv-postgres-branches", new int[]{7, 6}),
                Map.entry("parallel-relationships", new int[]{2, 3}),
                Map.entry("cycle-self-loop", new int[]{3, 4}),
                Map.entry("funnel", new int[]{4, 3}),
                Map.entry("disconnected", new int[]{4, 2}),
                Map.entry("ports-rpg-nested", new int[]{6, 3}));

        FlowTopologyFixtures.all().forEach((name, graph) -> {
            assertEquals(expected.get(name)[0], graph.getNodeCount(), name + " node count");
            assertEquals(expected.get(name)[1], graph.getEdgeCount(), name + " edge count");
        });
    }

    @Test
    void fixturesUseCanonicalNiFiDimensions() {
        FlowTopologyFixtures.all().values().stream()
                .flatMap(graph -> graph.getNodes().values().stream())
                .filter(node -> node.getType() == NodeType.PROCESSOR)
                .forEach(node -> {
                    assertEquals(350, node.getBoundingBox().width(), node.getId() + " width");
                    assertEquals(130, node.getBoundingBox().height(), node.getId() + " height");
                });
    }

    @Test
    void fixturesCaptureRequiredDegreeAndRelationshipShapes() {
        final LayoutGraph distribution = FlowTopologyFixtures.wideDistribution();
        assertEquals(5, distribution.getOutgoingEdges("distributor").size());

        final LayoutGraph fanIn = FlowTopologyFixtures.denseFanIn();
        assertEquals(4, fanIn.getIncomingEdges("result-log").size());

        final LayoutGraph failure = FlowTopologyFixtures.failureBranches();
        assertEquals(
                java.util.List.of("failure", "unmatched"),
                RelationshipCodec.decode(failure.getEdge("extract-failure").getSourcePort()));

        final LayoutGraph parallel = FlowTopologyFixtures.parallelRelationships();
        assertEquals(3, parallel.getOutgoingEdges("merge").size());
        assertTrue(parallel.getEdges().values().stream()
                .allMatch(edge -> "invoke".equals(edge.getTargetNodeId())));

        final LayoutGraph cycle = FlowTopologyFixtures.cycleWithSelfLoop();
        final LayoutEdge selfLoop = cycle.getEdge("a-a");
        assertTrue(selfLoop.isSelfLoop());

        final LayoutGraph funnel = FlowTopologyFixtures.funnelFlow();
        assertEquals(NodeType.FUNNEL, funnel.getNode("funnel").getType());
        assertEquals(2, funnel.getIncomingEdges("funnel").size());
        assertEquals(1, funnel.getOutgoingEdges("funnel").size());

        final LayoutGraph disconnected = FlowTopologyFixtures.disconnectedSubgraphs();
        assertEquals(2, disconnected.getRoots().size());
        assertEquals(2, disconnected.getTerminals().size());
    }

    @Test
    void nestedFixtureIncludesPortsRpgLabelAndChildGraph() {
        final LayoutGraph graph = FlowTopologyFixtures.portsRpgsNestedGroup();

        assertEquals(NodeType.PORT_INPUT, graph.getNode("input").getType());
        assertEquals(NodeType.PORT_OUTPUT, graph.getNode("output").getType());
        assertEquals(NodeType.REMOTE_PROCESS_GROUP, graph.getNode("remote").getType());
        assertEquals(NodeType.LABEL, graph.getNode("label").getType());
        assertEquals(NodeType.PROCESS_GROUP, graph.getNode("child-group").getType());

        final LayoutGraph child = graph.getSubgraphs().get("child-group");
        assertEquals(3, child.getNodeCount());
        assertEquals(2, child.getEdgeCount());
    }
}
