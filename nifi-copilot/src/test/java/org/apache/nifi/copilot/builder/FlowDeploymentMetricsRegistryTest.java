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

import in.shrake.nifi.layout.core.model.LayoutResult;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FlowDeploymentMetricsRegistry}: direct observation and
 * coordinator-driven exactly-once verification.
 */
class FlowDeploymentMetricsRegistryTest {

    private static final String PG_ID = "pg-metrics";
    private FlowDeploymentMetricsRegistry metrics;

    @BeforeEach
    void setUp() {
        metrics = new FlowDeploymentMetricsRegistry();
    }

    // -------------------------------------------------------------------------
    // Direct observation tests
    // -------------------------------------------------------------------------

    @Test
    void engineSuccessRecordsMovedAndRoutedCounts() {
        observe("ENGINE", FlowDeploymentMetricsRegistry.LayoutOutcome.SUCCESS, 5, 3);
        FlowDeploymentMetricsRegistry.LayoutMetrics lm = layoutMetrics("ENGINE|SUCCESS");
        assertEquals(5L, lm.movedComponentCount());
        assertEquals(3L, lm.routedConnectionCount());
        assertEquals(1L, lm.count());
    }

    @Test
    void disabledModeRecordsSkipped() {
        observe("DISABLED", FlowDeploymentMetricsRegistry.LayoutOutcome.SKIPPED, 0, 0);
        assertNotNull(layoutMetrics("DISABLED|SKIPPED"));
        assertEquals(0L, layoutMetrics("DISABLED|SKIPPED").movedComponentCount());
    }

    @Test
    void engineFailureRecordsZeroCounts() {
        observe("ENGINE", FlowDeploymentMetricsRegistry.LayoutOutcome.FAILURE, 0, 0);
        FlowDeploymentMetricsRegistry.LayoutMetrics lm = layoutMetrics("ENGINE|FAILURE");
        assertEquals(1L, lm.count());
        assertEquals(0L, lm.movedComponentCount());
    }

    @Test
    void multipleObservationsAccumulate() {
        long now = System.nanoTime();
        metrics.observeLayout("ENGINE", FlowDeploymentMetricsRegistry.LayoutOutcome.SUCCESS,
                now - 1_000_000L, 3, 2);
        metrics.observeLayout("ENGINE", FlowDeploymentMetricsRegistry.LayoutOutcome.SUCCESS,
                now - 2_000_000L, 4, 1);
        FlowDeploymentMetricsRegistry.LayoutMetrics lm = layoutMetrics("ENGINE|SUCCESS");
        assertEquals(2L, lm.count());
        assertEquals(7L, lm.movedComponentCount());
        assertEquals(3L, lm.routedConnectionCount());
    }

    @Test
    void successAndFailureTrackedIndependently() {
        observe("ENGINE", FlowDeploymentMetricsRegistry.LayoutOutcome.SUCCESS, 5, 2);
        observe("ENGINE", FlowDeploymentMetricsRegistry.LayoutOutcome.FAILURE, 0, 0);
        assertEquals(1L, layoutMetrics("ENGINE|SUCCESS").count());
        assertEquals(1L, layoutMetrics("ENGINE|FAILURE").count());
    }

    // -------------------------------------------------------------------------
    // Coordinator-driven: success with moved/routed
    // -------------------------------------------------------------------------

    @Test
    void coordinatorSuccessRecordsExactlyOneLayoutMetric() {
        LayoutDeploymentStage.LayoutExecutor executor =
                mock(LayoutDeploymentStage.LayoutExecutor.class);
        when(executor.layout(anyMap(), any(), any()))
                .thenReturn(new LayoutResult(List.of(), 4, Map.of("c1", List.of()), 0, List.of()));

        runCoordinator(LayoutMode.ENGINE, executor);

        FlowDeploymentMetricsRegistry.LayoutMetrics lm = layoutMetrics("ENGINE|SUCCESS");
        assertNotNull(lm);
        assertEquals(1L, lm.count());
        assertEquals(4L, lm.movedComponentCount());
        assertEquals(1L, lm.routedConnectionCount());
    }

    // -------------------------------------------------------------------------
    // Coordinator-driven: disabled skip
    // -------------------------------------------------------------------------

    @Test
    void coordinatorDisabledModeRecordsSkippedMetric() {
        runCoordinator(LayoutMode.DISABLED, null);

        assertNotNull(layoutMetrics("DISABLED|SKIPPED"));
        assertEquals(1L, layoutMetrics("DISABLED|SKIPPED").count());
    }

    // -------------------------------------------------------------------------
    // Coordinator-driven: failure
    // -------------------------------------------------------------------------

    @Test
    void coordinatorLayoutFailureRecordsFailureMetric() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupId(anyString())).thenReturn(PG_ID);
        doNothing().when(nifi).ensureTypeCache();
        when(nifi.getProcessGroupFlow(PG_ID))
                .thenReturn(emptyFlowResponse())
                .thenThrow(new RuntimeException("boom"));

        FlowDeploymentCoordinator coordinator =
                new FlowDeploymentCoordinator(LayoutMode.ENGINE);
        DeploymentState state = coordinator.prepare(
                new DeploymentContext(Map.of(), nifi, PG_ID, null, 0, false, false), metrics);

        assertThrows(RuntimeException.class, () -> coordinator.deploy(state));

        assertNotNull(layoutMetrics("ENGINE|FAILURE"));
        assertEquals(1L, layoutMetrics("ENGINE|FAILURE").count());
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void observe(String mode, FlowDeploymentMetricsRegistry.LayoutOutcome outcome,
                         int moved, int routed) {
        metrics.observeLayout(mode, outcome, System.nanoTime() - 1_000_000L, moved, routed);
    }

    private FlowDeploymentMetricsRegistry.LayoutMetrics layoutMetrics(String key) {
        return metrics.getSnapshot().layouts().get(key);
    }

    private void runCoordinator(LayoutMode mode, LayoutDeploymentStage.LayoutExecutor executor) {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupId(anyString())).thenReturn(PG_ID);
        doNothing().when(nifi).ensureTypeCache();
        when(nifi.getProcessGroupFlow(PG_ID)).thenReturn(emptyFlowResponse());

        FlowDeploymentCoordinator coordinator = executor != null
                ? new FlowDeploymentCoordinator(mode, new ComponentResolver(),
                        new CanvasPositionProvider(), new LivePreflightValidator(), executor)
                : new FlowDeploymentCoordinator(mode);
        DeploymentState state = coordinator.prepare(
                new DeploymentContext(Map.of(), nifi, PG_ID, null, 0, false, false), metrics);
        coordinator.deploy(state);
    }

    private static Map<String, Object> emptyFlowResponse() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of());
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("connections", List.of());
        flow.put("processGroups", List.of());
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", PG_ID);
        pgFlow.put("flow", flow);
        return Map.of("processGroupFlow", pgFlow);
    }
}
