/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.quality;

import in.shrake.nifi.layout.core.LayoutEngine;
import in.shrake.nifi.layout.core.fixture.FlowTopologyFixtures;
import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

class FixtureLayoutQualityTest {
    private static final LayoutOptions OPTIONS = LayoutOptions.defaults();
    private static final Map<String, Map<String, Integer>> MAXIMUM_BENDS = Map.ofEntries(
            Map.entry("linear", Map.of("a-b", 0, "b-c", 0, "c-d", 0)),
            Map.entry("two-way-fan-out", Map.of(
                    "source-failure", 2, "source-success", 2)),
            Map.entry("wide-distribution", Map.of(
                    "route-1", 4, "route-2", 4, "route-3", 2,
                    "route-4", 4, "route-5", 4)),
            Map.entry("dense-fan-in", Map.of(
                    "worker-1-result", 3, "worker-2-result", 3,
                    "worker-3-result", 3, "worker-4-result", 3)),
            Map.entry("failure-branches", Map.of(
                    "build-success", 0, "extract-failure", 4,
                    "extract-matched", 2, "invoke-failure", 2)),
            Map.entry("csv-postgres-branches", Map.of(
                    "get-convert", 0, "convert-validate", 0,
                    "validate-database", 2, "validate-failure", 4,
                    "database-archive", 4, "database-failure", 2)),
            Map.entry("parallel-relationships", Map.of(
                    "failure", 4, "merged", 4, "original", 4)),
            Map.entry("cycle-self-loop", Map.of(
                    "a-a", 4, "a-b", 2, "b-c", 0, "c-a", 4)),
            Map.entry("funnel", Map.of(
                    "a-funnel", 2, "b-funnel", 2, "funnel-sink", 0)),
            Map.entry("disconnected", Map.of("a-b", 0, "c-d", 0)),
            Map.entry("ports-rpg-nested", Map.of(
                    "input-processor", 0, "processor-output", 4,
                    "processor-remote", 2, "child-input-processor", 0,
                    "child-processor-output", 0)));

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtures")
    void fixtureSatisfiesGeometryAndRoutingGates(
            final String name,
            final LayoutGraph graph) {
        final LayoutEngine engine = LayoutEngine.create();
        final LayoutResult first = engine.layout(graph, OPTIONS);
        final LayoutResult second = engine.layout(graph, OPTIONS);

        LayoutQualityAssertions.assertNoComponentOverlaps(graph, first);
        LayoutQualityAssertions.assertNoRoutesThroughNonEndpointNodes(graph, first);
        LayoutQualityAssertions.assertParallelLabelAnchorSeparation(
                graph, first, FlowDirection.TOP_TO_BOTTOM, 240);
        LayoutQualityAssertions.assertGridAligned(first, OPTIONS.getGridSize());
        LayoutQualityAssertions.assertBendLimits(first, MAXIMUM_BENDS.get(name));
        LayoutQualityAssertions.assertDeterministic(first, second);
    }

    static Stream<Object[]> fixtures() {
        return FlowTopologyFixtures.all().entrySet().stream()
                .map(entry -> new Object[]{entry.getKey(), entry.getValue()});
    }
}
