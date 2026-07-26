/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.spacing;

import in.shrake.nifi.layout.core.model.LayoutOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultSpacingStrategyTest {
    private final DefaultSpacingStrategy strategy = new DefaultSpacingStrategy();
    private final LayoutOptions options = LayoutOptions.defaults();
    private final SpacingValues spacing = strategy.computeSpacing(
            new LayoutDimensions(0, 0), 0, options);

    @Test
    void preservesBaseSpacingForSimpleBoundaries() {
        RankDemand demand = new RankDemand(0, 1, 1, 1, 0, 0, false, false);

        assertEquals(options.getVerticalSpacing(),
                strategy.computeRankSpacing(demand, spacing, options));
    }

    @Test
    void providesClearanceForWideDistribution() {
        RankDemand demand = new RankDemand(0, 5, 1, 1, 1, 0, false, false);

        assertEquals(220, strategy.computeRankSpacing(demand, spacing, options));
    }

    @Test
    void capsPathologicalDegreeExpansion() {
        RankDemand demand = new RankDemand(0, 100, 1, 1, 1, 0, false, false);

        assertEquals(310, strategy.computeRankSpacing(demand, spacing, options));
    }
}
