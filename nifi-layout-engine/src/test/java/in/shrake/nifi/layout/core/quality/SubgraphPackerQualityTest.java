/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.quality;

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.service.SubgraphPacker;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubgraphPackerQualityTest {

    @Test
    void gridAlignmentCannotConsumeRequestedPackingGap() {
        final LayoutResult first = subgraphResult("a");
        final LayoutResult second = subgraphResult("b");
        final BoundingBox bounds = SubgraphPacker.computeBounds(first);
        final LayoutOptions options = LayoutOptions.builder()
                .verticalSpacing(9)
                .gridSize(20)
                .build();

        final LayoutResult packed = new SubgraphPacker().pack(
                List.of(
                        new SubgraphPacker.SubgraphLayout(
                                first, "a", 1, bounds),
                        new SubgraphPacker.SubgraphLayout(
                                second, "b", 1, bounds)),
                options);
        final Map<String, BoundingBox> packedBounds =
                LayoutQualityAssertions.boundsById(packed);
        final BoundingBox firstBounds = packedBounds.get("a");
        final BoundingBox secondBounds = packedBounds.get("b");

        assertFalse(firstBounds.intersects(secondBounds));
        assertTrue(secondBounds.y() - firstBounds.bottom()
                >= options.getVerticalSpacing());
        LayoutQualityAssertions.assertGridAligned(
                packed, options.getGridSize());
    }

    private static LayoutResult subgraphResult(final String id) {
        final Position position = new Position(40, 40);
        final BoundingBox bounds = new BoundingBox(40, 40, 350, 130);
        return new LayoutResult(
                List.of(new ComponentUpdate(
                        id,
                        "root",
                        NodeType.PROCESSOR,
                        position,
                        position,
                        bounds)),
                1,
                Map.of(),
                0,
                List.of());
    }
}
