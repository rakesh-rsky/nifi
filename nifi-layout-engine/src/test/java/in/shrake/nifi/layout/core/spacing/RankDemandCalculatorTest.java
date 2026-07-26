/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.spacing;

import in.shrake.nifi.layout.core.fixture.FlowTopologyFixtures;
import in.shrake.nifi.layout.core.layout.LongestPathLayerAssigner;
import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RankDemandCalculatorTest {
    private final RankDemandCalculator calculator = new RankDemandCalculator();
    private final LongestPathLayerAssigner layerAssigner = new LongestPathLayerAssigner();

    @Test
    void detectsWideFanOutDemand() {
        RankDemand demand = demands(FlowTopologyFixtures.wideDistribution()).get(0);

        assertEquals(5, demand.maxOutgoingDegree());
        assertEquals(1, demand.denseBusCount());
    }

    @Test
    void detectsDenseFanInDemand() {
        RankDemand demand = demands(FlowTopologyFixtures.denseFanIn()).get(0);

        assertEquals(4, demand.maxIncomingDegree());
        assertEquals(1, demand.denseBusCount());
    }

    @Test
    void detectsParallelRelationshipDemand() {
        RankDemand demand = demands(FlowTopologyFixtures.parallelRelationships()).get(0);

        assertEquals(3, demand.maxParallelEdges());
    }

    @Test
    void leavesLinearBoundariesAtUnitDemand() {
        for (RankDemand demand : demands(FlowTopologyFixtures.linearPipeline())) {
            assertEquals(1, demand.maxOutgoingDegree());
            assertEquals(1, demand.maxIncomingDegree());
            assertEquals(1, demand.maxParallelEdges());
            assertEquals(0, demand.denseBusCount());
            assertEquals(0, demand.longEdgeCount());
        }
    }

    private List<RankDemand> demands(final LayoutGraph graph) {
        final LayeredGraph layered = layerAssigner.assignLayers(
                graph, LayoutOptions.defaults());
        return calculator.calculate(layered);
    }
}
