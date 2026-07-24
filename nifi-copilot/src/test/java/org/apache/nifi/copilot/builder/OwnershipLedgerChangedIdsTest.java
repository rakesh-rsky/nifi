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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies that real deployers record all created/updated layoutable component IDs
 * in {@link OwnershipLedger#changedCanvasIds()}.
 */
class OwnershipLedgerChangedIdsTest {

    private static final String PG_ID = "pg-ledger-test";

    private NiFiClientOperations nifi;
    private OwnershipLedger ledger;
    private FlowDeploymentMetricsRegistry metrics;

    @BeforeEach
    void setUp() {
        nifi = mock(NiFiClientOperations.class);
        ledger = new OwnershipLedger(PG_ID);
        metrics = new FlowDeploymentMetricsRegistry();
    }

    // -------------------------------------------------------------------------
    // Ledger API basics
    // -------------------------------------------------------------------------

    @Test
    void emptyByDefault() {
        assertTrue(ledger.changedCanvasIds().isEmpty());
    }

    @Test
    void nullIdIsIgnored() {
        ledger.addChangedCanvasId(null);
        assertTrue(ledger.changedCanvasIds().isEmpty());
    }

    @Test
    void blankIdIsIgnored() {
        ledger.addChangedCanvasId("   ");
        assertTrue(ledger.changedCanvasIds().isEmpty());
    }

    @Test
    void duplicateIdsAreDeduped() {
        ledger.addChangedCanvasId("x");
        ledger.addChangedCanvasId("x");
        assertEquals(1, ledger.changedCanvasIds().size());
    }

    @Test
    void returnedSetIsUnmodifiable() {
        assertThrows(UnsupportedOperationException.class,
                () -> ledger.changedCanvasIds().add("should-fail"));
    }

    // -------------------------------------------------------------------------
    // Deployer-driven: ProcessorDeployer
    // -------------------------------------------------------------------------

    @Test
    void processorDeployerRecordsCreatedProcessorId() {
        when(nifi.createProcessor(eq(PG_ID), anyString(), anyString(),
                anyDouble(), anyDouble(), any()))
                .thenReturn(Map.of("id", "proc-nifi-1"));

        ProcessorDeployer.deploy(
                List.of(Map.of("id", "p1",
                        "type", "org.apache.nifi.processors.standard.GenerateFlowFile")),
                PG_ID, new ControllerServiceDeployer(), new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("proc-nifi-1"));
        assertTrue(ledger.createdProcessorIds().contains("proc-nifi-1"));
    }

    @Test
    void processorDeployerRecordsUpdatedProcessorId() {
        ComponentRegistry components = new ComponentRegistry();
        components.register("p1", "proc-existing", "PROCESSOR");
        when(nifi.getProcessor("proc-existing")).thenReturn(
                entity("proc-existing", Map.of("name", "Old Name",
                        "position", Map.of("x", 10, "y", 20))));

        ProcessorDeployer.deploy(
                List.of(Map.of("id", "p1", "type", "generate", "name", "New Name")),
                PG_ID, new ControllerServiceDeployer(), new CanvasPositionProvider(),
                components, ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("proc-existing"));
    }

    // -------------------------------------------------------------------------
    // Deployer-driven: PortDeployer (input port)
    // -------------------------------------------------------------------------

    @Test
    void portDeployerRecordsCreatedPortId() {
        when(nifi.createInputPort(eq(PG_ID), eq("MyPort"), anyDouble(), anyDouble()))
                .thenReturn(Map.of("id", "port-nifi-1"));

        Map<String, Object> flow = emptyFlow();
        PortDeployer.deploy(
                List.of(Map.of("id", "ip1", "name", "MyPort")),
                PG_ID, flow, "inputPorts", "INPUT_PORT", true,
                new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("port-nifi-1"));
    }

    @Test
    void portDeployerRecordsCreatedOutputPortId() {
        when(nifi.createOutputPort(eq(PG_ID), eq("Output"), anyDouble(), anyDouble()))
                .thenReturn(Map.of("id", "output-nifi-1"));

        PortDeployer.deploy(
                List.of(Map.of("id", "op1", "name", "Output")),
                PG_ID, emptyFlow(), "outputPorts", "OUTPUT_PORT", false,
                new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("output-nifi-1"));
    }

    // -------------------------------------------------------------------------
    // Deployer-driven: FunnelDeployer
    // -------------------------------------------------------------------------

    @Test
    void funnelDeployerRecordsCreatedFunnelId() {
        when(nifi.createFunnel(eq(PG_ID), anyDouble(), anyDouble()))
                .thenReturn(Map.of("id", "funnel-nifi-1"));

        FunnelDeployer.deploy(
                List.of(Map.of("id", "f1")),
                PG_ID, emptyFlow(),
                new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("funnel-nifi-1"));
    }

    @Test
    void funnelDeployerRecordsUpdatedFunnelId() {
        Map<String, Object> flow = emptyFlow();
        flow.put("funnels", List.of(entity("f1",
                Map.of("id", "f1", "position", Map.of("x", 10, "y", 20)))));

        FunnelDeployer.deploy(
                List.of(Map.of("id", "f1", "x", 30, "y", 40)),
                PG_ID, flow, new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("f1"));
    }

    // -------------------------------------------------------------------------
    // Deployer-driven: LabelDeployer
    // -------------------------------------------------------------------------

    @Test
    void labelDeployerRecordsCreatedLabelId() {
        when(nifi.createLabel(eq(PG_ID), eq("My Label"), anyDouble(), anyDouble(),
                any(), any(), any()))
                .thenReturn(Map.of("id", "label-nifi-1"));

        LabelDeployer.deploy(
                List.of(Map.of("id", "lbl1", "text", "My Label")),
                PG_ID, emptyFlow(),
                new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("label-nifi-1"));
    }

    @Test
    void labelDeployerRecordsUpdatedLabelId() {
        Map<String, Object> flow = emptyFlow();
        flow.put("labels", List.of(entity("label-existing",
                Map.of("id", "label-existing", "label", "My Label",
                        "position", Map.of("x", 10, "y", 20)))));

        LabelDeployer.deploy(
                List.of(Map.of("id", "label-existing", "text", "My Label", "x", 30, "y", 40)),
                PG_ID, flow, new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("label-existing"));
    }

    // -------------------------------------------------------------------------
    // Deployer-driven: RemoteProcessGroupDeployer
    // -------------------------------------------------------------------------

    @Test
    void rpgDeployerRecordsCreatedRpgId() {
        when(nifi.createRemoteProcessGroup(eq(PG_ID), eq("http://remote:8080/nifi"),
                anyDouble(), anyDouble(), any()))
                .thenReturn(Map.of("id", "rpg-nifi-1"));

        RemoteProcessGroupDeployer.deploy(
                List.of(Map.of("id", "rpg1", "target_uri", "http://remote:8080/nifi")),
                PG_ID, emptyFlow(),
                new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("rpg-nifi-1"));
    }

    @Test
    void rpgDeployerRecordsUpdatedRpgId() {
        Map<String, Object> flow = emptyFlow();
        flow.put("remoteProcessGroups", List.of(entity("rpg-existing",
                Map.of("id", "rpg-existing", "targetUri", "http://remote:8080/nifi",
                        "position", Map.of("x", 10, "y", 20)))));

        RemoteProcessGroupDeployer.deploy(
                List.of(Map.of("id", "rpg-existing",
                        "target_uri", "http://remote:8080/nifi", "x", 30, "y", 40)),
                PG_ID, flow, new CanvasPositionProvider(),
                new ComponentRegistry(), ledger, new ComponentResolver(), nifi, metrics);

        assertTrue(ledger.changedCanvasIds().contains("rpg-existing"));
    }

    @Test
    void preparationRecordsCreatedProcessGroupId() {
        when(nifi.listChildProcessGroups(PG_ID)).thenReturn(List.of());
        when(nifi.createProcessGroup(eq(PG_ID), eq("Child"), anyDouble(), anyDouble()))
                .thenReturn(Map.of("id", "child-nifi"));
        when(nifi.getProcessGroupFlow("child-nifi")).thenReturn(
                Map.of("processGroupFlow", Map.of("id", "child-nifi", "flow", emptyFlow())));
        DeploymentContext context = new DeploymentContext(
                Map.of("process_group", Map.of("name", "Child")),
                nifi, PG_ID, null, 0, false, false);
        DeploymentState state = new DeploymentState(
                context, PG_ID, null, ledger, new ControllerServiceDeployer(), metrics);

        DeploymentPreparationStage.prepareTarget(
                state, new ComponentResolver(), new LivePreflightValidator());

        assertTrue(ledger.changedCanvasIds().contains("child-nifi"));
    }

    // -------------------------------------------------------------------------
    // Deployer-driven: ConnectionDeployer (connection + both endpoints)
    // -------------------------------------------------------------------------

    @Test
    void connectionDeployerRecordsConnectionAndEndpoints() {
        ComponentRegistry components = new ComponentRegistry();
        components.register("src-spec", "src-nifi", "PROCESSOR");
        components.register("dst-spec", "dst-nifi", "PROCESSOR");

        when(nifi.createConnection(PG_ID, "src-nifi", "PROCESSOR",
                "dst-nifi", "PROCESSOR", List.of("success")))
                .thenReturn(Map.of("id", "conn-nifi-1"));

        ConnectionDeployer.deploy(
                List.of(Map.of("from", "src-spec", "to", "dst-spec")),
                PG_ID, components, ledger, new ComponentResolver(), nifi,
                new ArrayList<>(), metrics);

        Set<String> changed = ledger.changedCanvasIds();
        assertTrue(changed.contains("conn-nifi-1"), "connection ID");
        assertTrue(changed.contains("src-nifi"), "source endpoint");
        assertTrue(changed.contains("dst-nifi"), "destination endpoint");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    private static Map<String, Object> entity(
            final String id, final Map<String, Object> component) {
        return Map.of("id", id, "component", component, "revision", Map.of("version", 1));
    }
}
