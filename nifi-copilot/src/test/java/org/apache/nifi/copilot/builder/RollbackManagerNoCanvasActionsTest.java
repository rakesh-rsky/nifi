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
 * Verifies rollback without canvas actions both through direct
 * {@link RollbackManager} calls and through the actual {@link FlowBuilder}
 * post-deployment failure path.
 */
class RollbackManagerNoCanvasActionsTest {

    private static final String PG_ID = "pg-no-canvas-rollback";

    private NiFiClientOperations nifi;
    private FlowDeploymentMetricsRegistry metrics;

    @BeforeEach
    void setUp() {
        nifi = mock(NiFiClientOperations.class);
        metrics = new FlowDeploymentMetricsRegistry();
    }

    // -------------------------------------------------------------------------
    // Direct RollbackManager tests without canvas actions
    // -------------------------------------------------------------------------

    @Test
    void createdProcessorIsDeletedOnRollback() {
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);
        ledger.addCreatedProcessorId("proc-1");

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failure"), metrics);

        verify(nifi).deleteProcessor("proc-1");
    }

    @Test
    void createdConnectionIsDeletedOnRollback() {
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);
        ledger.addCreatedConnectionId("conn-1");

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failure"), metrics);

        verify(nifi).deleteConnection("conn-1");
    }

    @Test
    void rollbackWithNoCanvasActionsNeverCallsUpdateMethods() {
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);
        ledger.addCreatedProcessorId("proc-1");
        ledger.addCreatedConnectionId("conn-1");

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failure"), metrics);

        verify(nifi, never()).updateProcessor(anyString(), any());
        verify(nifi, never()).updateConnection(anyString(), any());
        verify(nifi, never()).updateInputPort(anyString(), any());
        verify(nifi, never()).updateOutputPort(anyString(), any());
        verify(nifi, never()).updateFunnel(anyString(), any());
        verify(nifi, never()).updateLabel(anyString(), any());
        verify(nifi, never()).updateRemoteProcessGroup(anyString(), any());
    }

    @Test
    void rollbackOutcomeIsSuccess() {
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);
        ledger.addCreatedProcessorId("proc-1");

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failure"), metrics);

        assertEquals(1L, metrics.getSnapshot().rollbacks().getOrDefault("SUCCESS", 0L));
    }

    @Test
    void emptyLedgerRollbackHasNoInteractionsWithNifi() {
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failure"), metrics);

        verifyNoInteractions(nifi);
    }

    @Test
    void multipleCreatedResourcesAreDeleted() {
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);
        ledger.addCreatedConnectionId("conn-a");
        ledger.addCreatedConnectionId("conn-b");
        ledger.addCreatedProcessorId("proc-x");
        ledger.addCreatedProcessorId("proc-y");

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failure"), metrics);

        verify(nifi).deleteConnection("conn-a");
        verify(nifi).deleteConnection("conn-b");
        verify(nifi).deleteProcessor("proc-x");
        verify(nifi).deleteProcessor("proc-y");
    }

    @Test
    void updatedProcessorIsRestoredFromRestore() {
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);
        ledger.addProcessorRestore("proc-updated", Map.of("name", "original-name"));

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failure"), metrics);

        verify(nifi).updateProcessor(eq("proc-updated"),
                argThat(u -> "original-name".equals(u.get("name"))));
    }

    // -------------------------------------------------------------------------
    // FlowBuilder-driven: disabled layout post-deployment rollback
    // -------------------------------------------------------------------------

    /**
     * Exercises the actual {@link FlowBuilder#buildFlow} path with layout disabled
     * mode and rollback enabled.  The first processor is created successfully;
     * the second fails during component deployment.  FlowBuilder catches the
     * exception and invokes rollback, which deletes the first processor without
     * any canvas-action metadata.
     */
    @Test
    void flowBuilderRollbackDeletesCreatedProcessor() {
        when(nifi.getProcessGroupId(anyString())).thenReturn(PG_ID);
        doNothing().when(nifi).ensureTypeCache();
        when(nifi.snapshotProcessGroup(anyString())).thenReturn(Map.of("pg_id", PG_ID));
        when(nifi.getProcessGroupFlow(PG_ID)).thenReturn(emptyFlowResponse());
        when(nifi.createProcessor(eq(PG_ID),
                eq("org.apache.nifi.processors.standard.GenerateFlowFile"),
                anyString(), anyDouble(), anyDouble(), any()))
                .thenReturn(Map.of("id", "proc-1"));
        when(nifi.createProcessor(eq(PG_ID),
                eq("org.apache.nifi.processors.standard.LogAttribute"),
                anyString(), anyDouble(), anyDouble(), any()))
                .thenThrow(new RuntimeException("creation failed"));
        stubProcessorCapabilities(
                nifi,
                "org.apache.nifi.processors.standard.GenerateFlowFile",
                "org.apache.nifi.processors.standard.LogAttribute");

        FlowDeploymentMetricsRegistry builderMetrics = new FlowDeploymentMetricsRegistry();
        FlowDeploymentCoordinator coordinator =
                new FlowDeploymentCoordinator(LayoutMode.DISABLED);
        FlowBuilder builder = new FlowBuilder(builderMetrics, coordinator);

        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("processors", List.of(
                Map.of("id", "p1",
                        "type", "org.apache.nifi.processors.standard.GenerateFlowFile"),
                Map.of("id", "p2",
                        "type", "org.apache.nifi.processors.standard.LogAttribute")));

        assertThrows(Exception.class, () ->
                builder.buildFlow(spec, PG_ID, nifi, null, 0, false, true));

        verify(nifi).deleteProcessor("proc-1");
        verify(nifi, never()).updateProcessor(eq("proc-1"),
                argThat(u -> u.containsKey("position")));
        assertEquals(1L, builderMetrics.getSnapshot().rollbacks().getOrDefault("SUCCESS", 0L));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

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

    private static void stubProcessorCapabilities(
            final NiFiClientOperations nifi, final String... processorTypes) {
        when(nifi.listProcessorTypes()).thenReturn(java.util.Arrays.stream(processorTypes)
                .map(type -> Map.<String, Object>of(
                        "type", type,
                        "bundle", Map.of("group", "g", "artifact", "a", "version", "1")))
                .toList());
        when(nifi.listControllerServiceTypes()).thenReturn(List.of());
        for (String type : processorTypes) {
            when(nifi.getProcessorDefinition("g", "a", "1", type)).thenReturn(Map.of(
                    "type", type,
                    "propertyDescriptors", Map.of(),
                    "supportedRelationships", List.of(Map.of("name", "success")),
                    "supportedSchedulingStrategies", List.of("TIMER_DRIVEN"),
                    "supportsDynamicProperties", false,
                    "supportsDynamicRelationships", false,
                    "triggerSerially", false));
        }
    }
}
