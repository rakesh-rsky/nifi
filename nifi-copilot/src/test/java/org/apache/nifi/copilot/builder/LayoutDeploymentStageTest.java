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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LayoutDeploymentStageTest {

    private NiFiClientOperations nifi;
    private static final String PG_ID = "pg-1";

    @BeforeEach
    void setUp() {
        nifi = mock(NiFiClientOperations.class);
    }

    @Test
    void disabledModeReturnsSkippedWithoutNifiCall() {
        DeploymentState state = buildState(emptyInventory());
        LayoutDeploymentStage.LayoutObservation obs = LayoutDeploymentStage.deploy(state, LayoutMode.DISABLED);
        assertTrue(obs.skipped());
        verify(nifi, never()).getProcessGroupFlow(anyString());
    }

    @Test
    void engineModeWithEmptyInventoryFetchesFreshFlow() {
        when(nifi.getProcessGroupFlow(PG_ID)).thenReturn(minimalFlowResponse(PG_ID));
        DeploymentState state = buildState(emptyInventory());
        LayoutDeploymentStage.LayoutExecutor executor = mock(LayoutDeploymentStage.LayoutExecutor.class);
        when(executor.layout(anyMap(), same(nifi), same(state.ledger()))).thenReturn(emptyResult());

        LayoutDeploymentStage.LayoutObservation obs =
                LayoutDeploymentStage.deploy(state, LayoutMode.ENGINE, executor);

        assertFalse(obs.skipped());
        verify(nifi, atLeastOnce()).getProcessGroupFlow(PG_ID);
        verify(executor).layout(anyMap(), same(nifi), same(state.ledger()));
        verify(executor, never()).layoutIncremental(anyMap(), any(), any(), anySet());
    }

    @Test
    void engineModeWithExistingFlowAndChangedIdsRunsIncrementalLayout() {
        when(nifi.getProcessGroupFlow(PG_ID)).thenReturn(minimalFlowWithProcessor(PG_ID, "proc-existing"));
        DeploymentState state = buildState(nonEmptyInventory("proc-existing"));
        state.ledger().addChangedCanvasId("proc-existing");
        LayoutDeploymentStage.LayoutExecutor executor = mock(LayoutDeploymentStage.LayoutExecutor.class);
        when(executor.layoutIncremental(anyMap(), same(nifi), same(state.ledger()), anySet()))
                .thenReturn(emptyResult());

        LayoutDeploymentStage.LayoutObservation obs =
                LayoutDeploymentStage.deploy(state, LayoutMode.ENGINE, executor);

        assertFalse(obs.skipped());
        verify(executor).layoutIncremental(
                anyMap(), same(nifi), same(state.ledger()), eq(Set.of("proc-existing")));
        verify(executor, never()).layout(anyMap(), any(), any());
    }

    @Test
    void engineModeWithStructuralConnectionChangesRunsFullLayout() {
        when(nifi.getProcessGroupFlow(PG_ID)).thenReturn(minimalFlowWithProcessor(PG_ID, "proc-existing"));
        DeploymentState state = buildState(nonEmptyInventory("proc-existing"));
        state.ledger().addChangedCanvasId("proc-existing");
        state.ledger().addCreatedConnectionId("connection-1");
        state.ledger().addCreatedConnectionId("connection-2");
        state.ledger().addCreatedConnectionId("connection-3");
        LayoutDeploymentStage.LayoutExecutor executor = mock(LayoutDeploymentStage.LayoutExecutor.class);
        when(executor.layout(anyMap(), same(nifi), same(state.ledger()))).thenReturn(emptyResult());

        LayoutDeploymentStage.LayoutObservation obs =
                LayoutDeploymentStage.deploy(state, LayoutMode.ENGINE, executor);

        assertFalse(obs.skipped());
        verify(executor).layout(anyMap(), same(nifi), same(state.ledger()));
        verify(executor, never()).layoutIncremental(anyMap(), any(), any(), anySet());
    }

    @Test
    void engineModeWithExistingFlowAndNoChangedIdsIsSkipped() {
        DeploymentState state = buildState(nonEmptyInventory("proc-existing"));

        LayoutDeploymentStage.LayoutObservation obs = LayoutDeploymentStage.deploy(state, LayoutMode.ENGINE);

        assertTrue(obs.skipped());
        verify(nifi, never()).getProcessGroupFlow(anyString());
    }

    @Test
    void engineModeNifiFailurePropagatesException() {
        when(nifi.getProcessGroupFlow(PG_ID))
                .thenThrow(new RuntimeException("NiFi fetch failed"));
        DeploymentState state = buildState(emptyInventory());

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> LayoutDeploymentStage.deploy(state, LayoutMode.ENGINE));
        assertTrue(ex.getMessage().contains("NiFi fetch failed")
                || (ex.getCause() != null && ex.getCause().getMessage().contains("NiFi fetch failed")));
    }

    /**
     * Proves that the recursive refreshed snapshot goes through the real
     * assembler → parser → layout-selection → writer path when no
     * {@link LayoutDeploymentStage.LayoutExecutor} override is supplied.
     *
     * <p>A two-level hierarchy (root → child-pg) is assembled; the real
     * {@code DEFAULT_EXECUTOR} runs.  Assertions verify both levels were fetched,
     * the layout produced observable component writes (updateProcessor calls),
     * and the observation reports a non-zero moved count.
     */
    @Test
    void engineModeRealExecutorParsesRecursiveHierarchicalSnapshot() {
        final String childPgId = "child-pg";

        when(nifi.getProcessGroupFlow(PG_ID))
                .thenReturn(hierarchicalFlowResponse(PG_ID, "p1", childPgId));
        when(nifi.getProcessGroupFlow(childPgId))
                .thenReturn(simpleFlowResponse(childPgId, "p2"));

        DeploymentState state = buildState(emptyInventory());

        LayoutDeploymentStage.LayoutObservation obs = LayoutDeploymentStage.deploy(state, LayoutMode.ENGINE);

        verify(nifi, atLeastOnce()).getProcessGroupFlow(PG_ID);
        verify(nifi, times(1)).getProcessGroupFlow(childPgId);
        assertFalse(obs.skipped(),
                "Real layout execution must not be skipped for a new-flow scenario");
        assertTrue(obs.movedCount() > 0,
                "Real assembler + parser + layout must reposition at least one component");
        verify(nifi, atLeastOnce()).updateProcessor(anyString(), argThat(u -> u.containsKey("position")));
    }

    private static Map<String, Object> hierarchicalFlowResponse(
            final String pgId, final String processorId, final String childPgId) {
        Map<String, Object> flow = emptyFlow();
        flow.put("processors", List.of(processorEntity(processorId)));
        // Child group entity: top-level "id" is enough for ProcessGroupFlowMapAssembler.extractId()
        Map<String, Object> childEntity = new LinkedHashMap<>();
        childEntity.put("id", childPgId);
        Map<String, Object> childComp = new LinkedHashMap<>();
        childComp.put("id", childPgId);
        childComp.put("name", childPgId + "-name");
        childComp.put("position", Map.of("x", 0.0, "y", 0.0));
        childEntity.put("component", childComp);
        childEntity.put("revision", Map.of("version", 1));
        flow.put("processGroups", new java.util.ArrayList<>(List.of(childEntity)));
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", pgId);
        pgFlow.put("flow", flow);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processGroupFlow", pgFlow);
        return response;
    }

    private static Map<String, Object> simpleFlowResponse(
            final String pgId, final String processorId) {
        Map<String, Object> flow = emptyFlow();
        flow.put("processors", List.of(processorEntity(processorId)));
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", pgId);
        pgFlow.put("flow", flow);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processGroupFlow", pgFlow);
        return response;
    }

    private DeploymentState buildState(final Map<String, Object> inventoryResponse) {
        DeploymentContext context = new DeploymentContext(
                Map.of(), nifi, PG_ID, null, 0, false, false);
        OwnershipLedger ledger = new OwnershipLedger(PG_ID);
        FlowDeploymentMetricsRegistry metrics = new FlowDeploymentMetricsRegistry();
        DeploymentState state = new DeploymentState(context, PG_ID, null, ledger,
                new ControllerServiceDeployer(), metrics);
        DeploymentTarget target = new DeploymentTarget(
                PG_ID, PG_ID, null, null, inventoryResponse, NiFiEntitySupport.effectiveFlow(inventoryResponse));
        state.setTarget(target);
        return state;
    }

    private static Map<String, Object> emptyInventory() {
        Map<String, Object> flow = emptyFlow();
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", PG_ID);
        pgFlow.put("flow", flow);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processGroupFlow", pgFlow);
        return response;
    }

    private static Map<String, Object> nonEmptyInventory(final String processorId) {
        Map<String, Object> flow = emptyFlow();
        flow.put("processors", List.of(processorEntity(processorId)));
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", PG_ID);
        pgFlow.put("flow", flow);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processGroupFlow", pgFlow);
        return response;
    }

    private static Map<String, Object> minimalFlowResponse(final String pgId) {
        Map<String, Object> flow = emptyFlow();
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", pgId);
        pgFlow.put("flow", flow);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processGroupFlow", pgFlow);
        return response;
    }

    private static Map<String, Object> minimalFlowWithProcessor(final String pgId, final String procId) {
        Map<String, Object> flow = emptyFlow();
        flow.put("processors", List.of(processorEntity(procId)));
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", pgId);
        pgFlow.put("flow", flow);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("processGroupFlow", pgFlow);
        return response;
    }

    private static Map<String, Object> emptyFlow() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of());
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("connections", List.of());
        flow.put("processGroups", List.of());
        return flow;
    }

    private static Map<String, Object> processorEntity(final String processorId) {
        Map<String, Object> component = new LinkedHashMap<>();
        component.put("id", processorId);
        component.put("name", processorId + "-name");
        component.put("type", "org.apache.nifi.processors.Test");
        component.put("position", Map.of("x", 100.0, "y", 100.0));
        component.put("dimensions", Map.of("width", 352.0, "height", 128.0));
        Map<String, Object> entity = new LinkedHashMap<>();
        entity.put("id", processorId);
        entity.put("revision", Map.of("version", 1));
        entity.put("component", component);
        return entity;
    }

    private static LayoutResult emptyResult() {
        return new LayoutResult(List.of(), 0, Map.of(), 0, List.of());
    }
}
