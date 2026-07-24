/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.copilot.builder;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link CanvasPositionProvider} retains only provisional creation
 * coordinates and performs no DAG ranking or collision avoidance.
 */
class CanvasPositionProviderTest {

    private static final double CANVAS_START_X = 100;
    private static final double CANVAS_START_Y = 200;
    private static final double DELTA = 0.0;

    private final CanvasPositionProvider provider = new CanvasPositionProvider();

    // -------------------------------------------------------------------------
    // provisionalPosition — explicit coordinates
    // -------------------------------------------------------------------------

    @Test
    void provisionalPositionUsesExplicitX() {
        final Map<String, Object> spec = Map.of("x", 500, "y", 300);
        final double[] pos = provider.provisionalPosition(spec);
        assertEquals(500.0, pos[0], DELTA, "x should match spec");
        assertEquals(300.0, pos[1], DELTA, "y should match spec");
    }

    @Test
    void provisionalPositionFallsBackToDefaultWhenXAbsent() {
        final Map<String, Object> spec = Map.of("y", 350);
        final double[] pos = provider.provisionalPosition(spec);
        assertEquals(CANVAS_START_X, pos[0], DELTA, "x should default to canvas start");
        assertEquals(350.0, pos[1], DELTA, "y should match spec");
    }

    @Test
    void provisionalPositionFallsBackToDefaultWhenYAbsent() {
        final Map<String, Object> spec = Map.of("x", 250);
        final double[] pos = provider.provisionalPosition(spec);
        assertEquals(250.0, pos[0], DELTA, "x should match spec");
        assertEquals(CANVAS_START_Y, pos[1], DELTA, "y should default to canvas start");
    }

    @Test
    void provisionalPositionReturnsCanvasStartForEmptySpec() {
        final double[] pos = provider.provisionalPosition(Map.of());
        assertArrayEquals(new double[]{CANVAS_START_X, CANVAS_START_Y}, pos, DELTA);
    }

    // -------------------------------------------------------------------------
    // No DAG effect — positions are not modified by connection topology
    // -------------------------------------------------------------------------

    @Test
    void provisionalPositionIsIndependentOfConnections() {
        // Two specs with the same explicit position: without collision avoidance
        // or DAG ranking both should return exactly what they declare.
        final double[] first = provider.provisionalPosition(Map.of("x", 100, "y", 200));
        final double[] second = provider.provisionalPosition(Map.of("x", 100, "y", 200));
        assertArrayEquals(first, second, DELTA,
                "positions must not be modified by DAG or collision logic");
    }

    // -------------------------------------------------------------------------
    // defaultX / defaultY / xOrDefault / yOrDefault
    // -------------------------------------------------------------------------

    @Test
    void defaultsMatchCanvasStart() {
        assertEquals(CANVAS_START_X, provider.defaultX(), DELTA);
        assertEquals(CANVAS_START_Y, provider.defaultY(), DELTA);
    }

    @Test
    void xOrDefaultReturnsValueWhenPresent() {
        assertEquals(42.0, provider.xOrDefault(42), DELTA);
    }

    @Test
    void xOrDefaultReturnsCanvasStartForNull() {
        assertEquals(CANVAS_START_X, provider.xOrDefault(null), DELTA);
    }

    @Test
    void yOrDefaultReturnsValueWhenPresent() {
        assertEquals(77.0, provider.yOrDefault(77.0), DELTA);
    }

    @Test
    void yOrDefaultReturnsCanvasStartForNull() {
        assertEquals(CANVAS_START_Y, provider.yOrDefault(null), DELTA);
    }

    // -------------------------------------------------------------------------
    // requestedPosition — update path
    // -------------------------------------------------------------------------

    @Test
    void requestedPositionReturnsEmptyMapWhenNoCoordinatesInSpec() {
        assertTrue(provider.requestedPosition(Map.of(), Map.of()).isEmpty());
    }

    @Test
    void requestedPositionUsesSpecXAndOriginalY() {
        final Map<String, Object> original = new LinkedHashMap<>();
        original.put("position", Map.of("x", 10, "y", 20));

        final Map<String, Object> result = provider.requestedPosition(Map.of("x", 99), original);

        assertFalse(result.isEmpty());
        @SuppressWarnings("unchecked")
        final Map<String, Object> position = (Map<String, Object>) result.get("position");
        assertEquals(99.0, ((Number) position.get("x")).doubleValue(), DELTA);
        assertEquals(20.0, ((Number) position.get("y")).doubleValue(), DELTA);
    }

    @Test
    void requestedPositionUsesBothSpecValues() {
        final Map<String, Object> original = new LinkedHashMap<>();
        original.put("position", Map.of("x", 5, "y", 10));

        final Map<String, Object> result =
                provider.requestedPosition(Map.of("x", 300, "y", 400), original);

        @SuppressWarnings("unchecked")
        final Map<String, Object> position = (Map<String, Object>) result.get("position");
        assertEquals(300.0, ((Number) position.get("x")).doubleValue(), DELTA);
        assertEquals(400.0, ((Number) position.get("y")).doubleValue(), DELTA);
    }
}
