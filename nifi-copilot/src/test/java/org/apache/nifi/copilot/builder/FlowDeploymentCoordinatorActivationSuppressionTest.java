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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Acceptance tests for {@link FlowDeploymentCoordinator} stage ordering, activation
 * suppression on layout failure, and {@link FlowBuilder}-driven rollback.
 */
class FlowDeploymentCoordinatorActivationSuppressionTest {

    private static final String PG_ID = "pg-act-test";

    /**
     * Proves connection-configuration → layout → runtime-activation ordering by
     * running the real coordinator pipeline with two processors, one connection,
     * and a mock {@link LayoutDeploymentStage.LayoutExecutor}.  Answer callbacks
     * record the actual NiFi call sequence.
     */
    @Test
    void connectionConfigBeforeLayoutBeforeActivation() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupId(anyString())).thenReturn(PG_ID);
        doNothing().when(nifi).ensureTypeCache();
        when(nifi.getProcessGroupFlow(PG_ID)).thenReturn(emptyFlowResponse());
        when(nifi.createProcessor(eq(PG_ID), anyString(), anyString(),
                anyDouble(), anyDouble(), any()))
                .thenReturn(Map.of("id", "proc-1"))
                .thenReturn(Map.of("id", "proc-2"));
        when(nifi.waitForProcessorValid(anyString(), anyInt())).thenReturn(true);

        List<String> callOrder = new ArrayList<>();
        when(nifi.createConnection(eq(PG_ID), anyString(), anyString(),
                anyString(), anyString(), anyList()))
                .thenAnswer(inv -> {
                    callOrder.add("CONNECTION_CONFIG");
                    return Map.of("id", "conn-1");
                });
        doAnswer(inv -> { callOrder.add("ACTIVATION"); return null; })
                .when(nifi).startProcessor(anyString());

        LayoutDeploymentStage.LayoutExecutor executor =
                mock(LayoutDeploymentStage.LayoutExecutor.class);
        when(executor.layout(anyMap(), any(), any())).thenAnswer(inv -> {
            callOrder.add("LAYOUT");
            return emptyResult();
        });

        FlowDeploymentCoordinator coordinator = new FlowDeploymentCoordinator(
                LayoutMode.ENGINE, new ComponentResolver(), new CanvasPositionProvider(),
                new LivePreflightValidator(), executor);
        FlowDeploymentMetricsRegistry metrics = new FlowDeploymentMetricsRegistry();

        DeploymentContext context = new DeploymentContext(
                twoProcessorSpec(), nifi, PG_ID, null, 0, true, false);
        DeploymentState state = coordinator.prepare(context, metrics);
        coordinator.deploy(state);

        int connIdx = callOrder.indexOf("CONNECTION_CONFIG");
        int layoutIdx = callOrder.indexOf("LAYOUT");
        int activIdx = callOrder.indexOf("ACTIVATION");
        assertTrue(connIdx >= 0, "connection configuration must run");
        assertTrue(layoutIdx >= 0, "layout must run");
        assertTrue(activIdx >= 0, "runtime activation must run");
        assertTrue(connIdx < layoutIdx,
                "connection config must precede layout; order=" + callOrder);
        assertTrue(layoutIdx < activIdx,
                "layout must precede activation; order=" + callOrder);
    }

    @Test
    void layoutFailureSuppressesActivation() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupFlow(PG_ID))
                .thenReturn(emptyFlowResponse())
                .thenThrow(new RuntimeException("layout fetch failed"));
        when(nifi.getProcessGroupId(anyString())).thenReturn(PG_ID);
        doNothing().when(nifi).ensureTypeCache();

        FlowDeploymentCoordinator coordinator =
                new FlowDeploymentCoordinator(LayoutMode.ENGINE);
        FlowDeploymentMetricsRegistry metrics = new FlowDeploymentMetricsRegistry();

        DeploymentState state = coordinator.prepare(
                new DeploymentContext(Map.of(), nifi, PG_ID, null, 0, true, false), metrics);
        assertThrows(RuntimeException.class, () -> coordinator.deploy(state));

        verify(nifi, never()).startProcessor(anyString());
        assertEquals(1, metrics.getSnapshot().layouts().get("ENGINE|FAILURE").count());
    }

    /**
     * Exercises the real {@link FlowBuilder#buildFlow} rollback path: a processor
     * is created, the layout stage fails, and FlowBuilder invokes
     * {@link RollbackManager} which deletes the created processor.
     */
    @Test
    void layoutFailureTriggersFlowBuilderRollback() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupId(anyString())).thenReturn(PG_ID);
        doNothing().when(nifi).ensureTypeCache();
        when(nifi.snapshotProcessGroup(anyString())).thenReturn(Map.of("pg_id", PG_ID));
        when(nifi.getProcessGroupFlow(PG_ID))
                .thenReturn(emptyFlowResponse())   // prepareTarget inventory
                .thenThrow(new RuntimeException("layout fetch failed")); // layout fresh flow
        when(nifi.createProcessor(eq(PG_ID), anyString(), anyString(),
                anyDouble(), anyDouble(), any()))
                .thenReturn(Map.of("id", "proc-to-rollback"));

        LayoutDeploymentStage.LayoutExecutor executor =
                mock(LayoutDeploymentStage.LayoutExecutor.class);
        FlowDeploymentMetricsRegistry metrics = new FlowDeploymentMetricsRegistry();
        FlowDeploymentCoordinator coordinator = new FlowDeploymentCoordinator(
                LayoutMode.ENGINE, new ComponentResolver(), new CanvasPositionProvider(),
                new LivePreflightValidator(), executor);
        FlowBuilder builder = new FlowBuilder(metrics, coordinator);

        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("processors", List.of(Map.of(
                "id", "p1",
                "type", "org.apache.nifi.processors.standard.GenerateFlowFile")));

        assertThrows(RuntimeException.class, () ->
                builder.buildFlow(spec, PG_ID, nifi, null, 0, false, true));

        verify(nifi).deleteProcessor("proc-to-rollback");
        verify(nifi, never()).startProcessor(anyString());
        assertEquals(1L, metrics.getSnapshot().rollbacks().getOrDefault("SUCCESS", 0L));
    }

    @Test
    void disabledModeSkipsLayoutAndActivationProceeds() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupFlow(PG_ID)).thenReturn(emptyFlowResponse());
        when(nifi.getProcessGroupId(anyString())).thenReturn(PG_ID);
        doNothing().when(nifi).ensureTypeCache();

        FlowDeploymentCoordinator coordinator =
                new FlowDeploymentCoordinator(LayoutMode.DISABLED);
        FlowDeploymentMetricsRegistry metrics = new FlowDeploymentMetricsRegistry();

        DeploymentState state = coordinator.prepare(
                new DeploymentContext(Map.of(), nifi, PG_ID, null, 0, false, false), metrics);
        assertDoesNotThrow(() -> coordinator.deploy(state));

        assertEquals(1, metrics.getSnapshot().layouts().get("DISABLED|SKIPPED").count());
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static Map<String, Object> twoProcessorSpec() {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("processors", List.of(
                Map.of("id", "p1",
                        "type", "org.apache.nifi.processors.standard.GenerateFlowFile"),
                Map.of("id", "p2",
                        "type", "org.apache.nifi.processors.standard.LogAttribute")));
        spec.put("connections", List.of(Map.of("from", "p1", "to", "p2")));
        return spec;
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
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processGroupFlow", pgFlow);
        return response;
    }

    private static LayoutResult emptyResult() {
        return new LayoutResult(List.of(), 0, Map.of(), 0, List.of());
    }
}
