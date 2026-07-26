/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.spacing;

import in.shrake.nifi.layout.core.model.LayoutOptions;

final class RankSpacingCalculator {
    private static final int MAX_DYNAMIC_LANES = 8;
    private static final int MAX_LONG_EDGE_CHANNELS = 4;
    private static final int MIN_BUS_CHANNEL_SPACING = 50;

    private RankSpacingCalculator() {
    }

    static int calculate(
            final RankDemand demand,
            final SpacingValues spacing,
            final LayoutOptions options) {
        final int laneSpacing = options.getPortSpacing();
        final int maximumDegree = Math.max(
                demand.maxOutgoingDegree(), demand.maxIncomingDegree());
        final int fanOutClearance = maximumDegree > 2
                ? (boundedLanes(maximumDegree) - 1) * laneSpacing : 0;
        final int parallelLabelClearance = demand.maxParallelEdges() > 1
                ? (boundedLanes(demand.maxParallelEdges()) - 1) * laneSpacing : 0;
        final int denseBusClearance = demand.denseBusCount() > 1
                ? (boundedLanes(demand.denseBusCount()) - 1)
                        * Math.max(MIN_BUS_CHANNEL_SPACING, laneSpacing) : 0;
        final int longEdgeClearance = Math.min(
                demand.longEdgeCount(), MAX_LONG_EDGE_CHANNELS) * laneSpacing;
        final int componentClearance = Math.max(
                demand.labelPresent() ? options.getLabelSpacing() : 0,
                demand.portPresent() ? options.getPortSpacing() : 0);

        final int dynamicClearance = Math.max(
                Math.max(fanOutClearance, parallelLabelClearance),
                Math.max(Math.max(denseBusClearance, longEdgeClearance), componentClearance));
        return spacing.verticalSpacing() + dynamicClearance;
    }

    private static int boundedLanes(final int lanes) {
        return Math.min(lanes, MAX_DYNAMIC_LANES);
    }
}
