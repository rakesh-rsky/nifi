/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.quality;

import in.shrake.nifi.layout.core.LayoutEngine;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.NodeType;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

class LayoutPropertyTest {
    private static final LayoutOptions OPTIONS = LayoutOptions.defaults();

    @Property(tries = 50)
    void boundedGraphsRemainDeterministicClearAndNonOverlapping(
            @ForAll("smallValidGraphs") final LayoutGraph graph) {
        final LayoutEngine engine = LayoutEngine.create();
        final LayoutResult first = engine.layout(graph, OPTIONS);
        final LayoutResult second = engine.layout(graph, OPTIONS);

        LayoutQualityAssertions.assertNoComponentOverlaps(graph, first);
        LayoutQualityAssertions.assertNoRoutesThroughNonEndpointNodes(graph, first);
        LayoutQualityAssertions.assertGridAligned(first, OPTIONS.getGridSize());
        LayoutQualityAssertions.assertDeterministic(first, second);
    }

    @Provide
    Arbitrary<LayoutGraph> smallValidGraphs() {
        return Combinators.combine(
                Arbitraries.integers().between(1, 7),
                Arbitraries.longs())
                .as(this::graphFromSeed);
    }

    private LayoutGraph graphFromSeed(final int nodeCount, final long seed) {
        final Map<String, LayoutNode> nodes = new LinkedHashMap<>();
        for (int index = 0; index < nodeCount; index++) {
            final String id = "node-" + index;
            nodes.put(id, new LayoutNode(
                    id,
                    NodeType.PROCESSOR,
                    new BoundingBox(0, 0, 350, 130),
                    Map.of("name", id),
                    "root"));
        }

        final Random random = new Random(seed);
        final Map<String, LayoutEdge> edges = new LinkedHashMap<>();
        for (int source = 0; source < nodeCount; source++) {
            for (int target = source + 1; target < nodeCount; target++) {
                if (random.nextInt(4) == 0) {
                    final String id = "edge-" + source + "-" + target;
                    edges.put(id, new LayoutEdge(
                            id,
                            "node-" + source,
                            "node-" + target,
                            "success",
                            "",
                            false,
                            false));
                }
            }
        }
        return new LayoutGraph(nodes, edges, Map.of(), "root");
    }
}
