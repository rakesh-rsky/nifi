/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.copilot.fixture;

import in.shrake.nifi.layout.copilot.ProcessGroupFlowMapAdapter;
import in.shrake.nifi.layout.core.fixture.FlowTopologyFixtures;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopilotAdapterRoundTripTest {
    private final ProcessGroupFlowMapAdapter adapter = new ProcessGroupFlowMapAdapter();

    @Test
    void mapFixturesMatchCanonicalEngineTopology() {
        final Map<String, LayoutGraph> expectedFixtures = FlowTopologyFixtures.all();
        final Map<String, Map<String, Object>> mapFixtures = CopilotFlowMapFixtures.all();

        assertEquals(expectedFixtures.keySet(), mapFixtures.keySet());
        expectedFixtures.forEach((name, expected) ->
                assertEquivalent(expected, adapter.parse(mapFixtures.get(name)), name));
    }

    @Test
    void mapFixturesExerciseWrappedPlainAndShuffledRepresentations() {
        boolean wrapped = false;
        boolean plain = false;
        for (Map<String, Object> fixture : CopilotFlowMapFixtures.all().values()) {
            final Map<String, Object> processGroupFlow = map(fixture.get("processGroupFlow"));
            final Map<String, Object> flow = map(processGroupFlow.get("flow"));
            for (Object processor : list(flow.get("processors"))) {
                final Map<String, Object> value = map(processor);
                wrapped |= value.containsKey("component");
                plain |= !value.containsKey("component");
            }
        }

        assertTrue(wrapped, "fixtures must include NiFi entity-wrapped components");
        assertTrue(plain, "fixtures must include plain component maps");

        final LayoutGraph distribution = adapter.parse(CopilotFlowMapFixtures.wideDistribution());
        assertEquals(
                java.util.List.of("distributor", "worker-1", "worker-2",
                        "worker-3", "worker-4", "worker-5"),
                distribution.getNodes().keySet().stream().toList());
        assertEquals(
                java.util.List.of("route-1", "route-2", "route-3", "route-4", "route-5"),
                distribution.getEdges().keySet().stream().toList());
    }

    private static void assertEquivalent(
            final LayoutGraph expected,
            final LayoutGraph actual,
            final String path) {
        assertEquals(expected.getProcessGroupId(), actual.getProcessGroupId(), path + " group");
        assertEquals(expected.getNodes().keySet(), actual.getNodes().keySet(), path + " nodes");
        assertEquals(expected.getEdges().keySet(), actual.getEdges().keySet(), path + " edges");
        assertEquals(expected.getSubgraphs().keySet(), actual.getSubgraphs().keySet(),
                path + " subgraphs");

        expected.getNodes().forEach((id, expectedNode) ->
                assertNode(expectedNode, actual.getNode(id), path + "/" + id));
        expected.getEdges().forEach((id, expectedEdge) ->
                assertEdge(expectedEdge, actual.getEdge(id), path + "/" + id));
        expected.getSubgraphs().forEach((id, expectedChild) ->
                assertEquivalent(expectedChild, actual.getSubgraphs().get(id), path + "/" + id));
    }

    private static void assertNode(
            final LayoutNode expected,
            final LayoutNode actual,
            final String path) {
        assertEquals(expected.getType(), actual.getType(), path + " type");
        assertEquals(expected.getBoundingBox(), actual.getBoundingBox(), path + " bounds");
        assertEquals(expected.getAttributes(), actual.getAttributes(), path + " attributes");
        assertEquals(expected.getParentGroupId(), actual.getParentGroupId(), path + " parent");
    }

    private static void assertEdge(
            final LayoutEdge expected,
            final LayoutEdge actual,
            final String path) {
        assertEquals(expected.getSourceNodeId(), actual.getSourceNodeId(), path + " source");
        assertEquals(expected.getTargetNodeId(), actual.getTargetNodeId(), path + " target");
        assertEquals(expected.getSourcePort(), actual.getSourcePort(), path + " relationships");
        assertEquals(expected.getTargetPort(), actual.getTargetPort(), path + " target port");
        assertEquals(expected.isSelfLoop(), actual.isSelfLoop(), path + " self-loop");
        assertFalse(actual.isReversed(), path + " must not be pre-reversed");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(final Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    private static java.util.List<?> list(final Object value) {
        return value instanceof java.util.List<?> list ? list : java.util.List.of();
    }
}
