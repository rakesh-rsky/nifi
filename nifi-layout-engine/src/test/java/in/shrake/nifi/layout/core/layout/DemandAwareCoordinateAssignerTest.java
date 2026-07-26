/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.layout;

import in.shrake.nifi.layout.core.fixture.FlowTopologyFixtures;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.spacing.DefaultSpacingStrategy;
import in.shrake.nifi.layout.core.spacing.SpacingStrategy;
import in.shrake.nifi.layout.core.spacing.SpacingValues;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemandAwareCoordinateAssignerTest {
    private final LongestPathLayerAssigner layerAssigner = new LongestPathLayerAssigner();
    private final DefaultCoordinateAssigner coordinateAssigner =
            new DefaultCoordinateAssigner(new DefaultSpacingStrategy());

    @Test
    void expandsFiveWayDistributionBoundary() {
        CoordinateAssignment assignment = assign(
                FlowTopologyFixtures.wideDistribution(), LayoutOptions.defaults());

        assertTrue(verticalClearance(assignment, "distributor", "worker-1") >= 220);
    }

    @Test
    void preservesSimpleLinearBoundarySpacing() {
        CoordinateAssignment assignment = assign(
                FlowTopologyFixtures.linearPipeline(), LayoutOptions.defaults());

        assertEquals(110, verticalClearance(assignment, "a", "b"));
    }

    @Test
    void appliesDemandAlongEveryFlowAxis() {
        for (FlowDirection direction : FlowDirection.values()) {
            LayoutOptions options = LayoutOptions.builder()
                    .flowDirection(direction)
                    .build();
            CoordinateAssignment assignment = assign(
                    FlowTopologyFixtures.wideDistribution(), options);

            assertTrue(flowAxisClearance(
                    assignment, "distributor", "worker-1", direction) >= 220,
                    direction::name);
        }
    }

    @Test
    void retainsDemandSpacingForCustomSpacingStrategies() {
        SpacingStrategy customStrategy = (dimensions, count, options) ->
                new SpacingValues(
                        options.getHorizontalSpacing(),
                        options.getVerticalSpacing(),
                        options.getMarginTop(),
                        options.getMarginBottom(),
                        options.getMarginLeft(),
                        options.getMarginRight(),
                        options.getPadding());
        DefaultCoordinateAssigner customAssigner =
                new DefaultCoordinateAssigner(customStrategy);
        LayoutOptions options = LayoutOptions.defaults();
        LayeredGraph layered = layerAssigner.assignLayers(
                FlowTopologyFixtures.wideDistribution(), options);
        CoordinateAssignment assignment = customAssigner.computeLayout(layered, options);

        assertTrue(verticalClearance(assignment, "distributor", "worker-1") >= 220);
    }

    @Test
    void gridSnappingNeverReducesRankClearance() {
        LayoutOptions options = LayoutOptions.builder().gridSize(1000).build();
        CoordinateAssignment assignment = assign(
                FlowTopologyFixtures.linearPipeline(), options);

        assertTrue(verticalClearance(assignment, "a", "b")
                >= options.getVerticalSpacing());
    }

    private CoordinateAssignment assign(
            final LayoutGraph graph,
            final LayoutOptions options) {
        LayeredGraph layered = layerAssigner.assignLayers(graph, options);
        return coordinateAssigner.computeLayout(layered, options);
    }

    private int verticalClearance(
            final CoordinateAssignment assignment,
            final String sourceId,
            final String targetId) {
        BoundingBox source = assignment.getBounds().get(sourceId);
        BoundingBox target = assignment.getBounds().get(targetId);
        return target.y() - source.y() - source.height();
    }

    private int flowAxisClearance(
            final CoordinateAssignment assignment,
            final String sourceId,
            final String targetId,
            final FlowDirection direction) {
        BoundingBox source = assignment.getBounds().get(sourceId);
        BoundingBox target = assignment.getBounds().get(targetId);
        return switch (direction) {
            case TOP_TO_BOTTOM -> target.y() - source.y() - source.height();
            case BOTTOM_TO_TOP -> source.y() - target.y() - target.height();
            case LEFT_TO_RIGHT -> target.x() - source.x() - source.width();
            case RIGHT_TO_LEFT -> source.x() - target.x() - target.width();
        };
    }
}
