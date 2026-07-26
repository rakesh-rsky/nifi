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

import in.shrake.nifi.layout.core.exception.WriteBackException;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.support.writer.LayoutWriteRequest;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NiFiClientLayoutWriter}.
 *
 * <p>Tests cover: processor position update, port position update, funnel/label/
 * remote-process-group updates, connection bend persistence, rollback registration
 * via {@link OwnershipLedger}, no-op when position unchanged, and unknown NodeType.
 */
class NiFiClientLayoutWriterTest {

    private NiFiClientOperations nifi;
    private OwnershipLedger ledger;
    private NiFiClientLayoutWriter writer;

    @BeforeEach
    void setUp() {
        nifi = mock(NiFiClientOperations.class);
        ledger = mock(OwnershipLedger.class);
        writer = new NiFiClientLayoutWriter(nifi, ledger);

        // Default stub for getConnection used in bend write
        when(nifi.getConnection(anyString())).thenReturn(Map.of("component", Map.of("bends", List.of())));
    }

    // -------------------------------------------------------------------------
    // Processor position update
    // -------------------------------------------------------------------------

    @Test
    void processorPositionIsUpdatedWhenChanged() {
        LayoutGraph graph = singleNodeGraph("proc1", NodeType.PROCESSOR, 0, 0);
        LayoutWriteRequest request = writeRequest(graph, Map.of("proc1", new Position(100, 200)));

        writer.write(Map.of(), request);

        verify(nifi).updateProcessor(eq("proc1"), argThat(u -> positionMatches(u, 100, 200)));
    }

    @Test
    void processorRollbackRegisteredBeforeUpdate() {
        LayoutGraph graph = singleNodeGraph("proc1", NodeType.PROCESSOR, 50, 60);
        LayoutWriteRequest request = writeRequest(graph, Map.of("proc1", new Position(100, 200)));

        writer.write(Map.of(), request);

        // Rollback must be registered before the actual update
        var inOrder = inOrder(ledger, nifi);
        inOrder.verify(ledger).addCanvasAction(contains("proc1"), any());
        inOrder.verify(nifi).updateProcessor(eq("proc1"), any());
    }

    // -------------------------------------------------------------------------
    // No-op when position unchanged
    // -------------------------------------------------------------------------

    @Test
    void noUpdateWhenPositionUnchanged() {
        LayoutGraph graph = singleNodeGraph("proc1", NodeType.PROCESSOR, 100, 200);
        LayoutWriteRequest request = writeRequest(graph, Map.of("proc1", new Position(100, 200)));

        writer.write(Map.of(), request);

        verify(nifi, never()).updateProcessor(anyString(), any());
        verify(ledger, never()).addCanvasAction(anyString(), any());
    }

    // -------------------------------------------------------------------------
    // All supported NodeTypes dispatch to correct methods
    // -------------------------------------------------------------------------

    @Test
    void inputPortDispatchesToUpdateInputPort() {
        LayoutGraph graph = singleNodeGraph("port1", NodeType.PORT_INPUT, 0, 0);
        writer.write(Map.of(), writeRequest(graph, Map.of("port1", new Position(10, 20))));
        verify(nifi).updateInputPort(eq("port1"), argThat(u -> positionMatches(u, 10, 20)));
    }

    @Test
    void outputPortDispatchesToUpdateOutputPort() {
        LayoutGraph graph = singleNodeGraph("port2", NodeType.PORT_OUTPUT, 0, 0);
        writer.write(Map.of(), writeRequest(graph, Map.of("port2", new Position(10, 20))));
        verify(nifi).updateOutputPort(eq("port2"), argThat(u -> positionMatches(u, 10, 20)));
    }

    @Test
    void funnelDispatchesToUpdateFunnel() {
        LayoutGraph graph = singleNodeGraph("funnel1", NodeType.FUNNEL, 0, 0);
        writer.write(Map.of(), writeRequest(graph, Map.of("funnel1", new Position(10, 20))));
        verify(nifi).updateFunnel(eq("funnel1"), argThat(u -> positionMatches(u, 10, 20)));
    }

    @Test
    void labelDispatchesToUpdateLabel() {
        LayoutGraph graph = singleNodeGraph("label1", NodeType.LABEL, 0, 0);
        writer.write(Map.of(), writeRequest(graph, Map.of("label1", new Position(10, 20))));
        verify(nifi).updateLabel(eq("label1"), argThat(u -> positionMatches(u, 10, 20)));
    }

    @Test
    void remoteProcessGroupDispatchesToUpdateRemoteProcessGroup() {
        LayoutGraph graph = singleNodeGraph("rpg1", NodeType.REMOTE_PROCESS_GROUP, 0, 0);
        writer.write(Map.of(), writeRequest(graph, Map.of("rpg1", new Position(10, 20))));
        verify(nifi).updateRemoteProcessGroup(eq("rpg1"), argThat(u -> positionMatches(u, 10, 20)));
    }

    @Test
    void processGroupDispatchesToUpdateProcessGroup() {
        LayoutGraph graph = singleNodeGraph("pg1", NodeType.PROCESS_GROUP, 0, 0);
        writer.write(Map.of(), writeRequest(graph, Map.of("pg1", new Position(10, 20))));
        verify(nifi).updateProcessGroup(eq("pg1"), argThat(u -> positionMatches(u, 10, 20)));
    }

    @Test
    void virtualNodeIsSkippedSilently() {
        LayoutGraph graph = singleNodeGraph("v1", NodeType.VIRTUAL, 0, 0);
        LayoutWriteRequest request = writeRequest(graph, Map.of());
        assertDoesNotThrow(() -> writer.write(Map.of(), request));
        // No component update methods should be called for VIRTUAL nodes
        verify(nifi, never()).updateProcessor(anyString(), any());
        verify(nifi, never()).updateProcessGroup(anyString(), any());
        verify(nifi, never()).updateInputPort(anyString(), any());
        verify(nifi, never()).updateOutputPort(anyString(), any());
        verify(nifi, never()).updateFunnel(anyString(), any());
        verify(nifi, never()).updateLabel(anyString(), any());
        verify(nifi, never()).updateRemoteProcessGroup(anyString(), any());
    }

    // -------------------------------------------------------------------------
    // Connection bends
    // -------------------------------------------------------------------------

    @Test
    void connectionBendsArePersistedViaUpdateConnection() {
        LayoutGraph graph = emptyGraph();
        Map<String, List<Position>> bends = Map.of(
                "conn1", List.of(new Position(50, 100), new Position(150, 100))
        );
        LayoutWriteRequest request = writeRequestWithBends(graph, Map.of(), bends);

        writer.write(Map.of(), request);

        verify(nifi).getConnection("conn1");
        verify(nifi).updateConnection(eq("conn1"), argThat(u -> u.containsKey("bends")));
    }

    @Test
    void connectionEndpointAnchorsAreNotPersistedAsBends() {
        LayoutGraph graph = emptyGraph();
        Map<String, List<Position>> path = Map.of(
                "conn1", List.of(
                        new Position(50, 100),
                        new Position(100, 150),
                        new Position(150, 200)));
        LayoutWriteRequest request = writeRequestWithBends(graph, Map.of(), path);

        writer.write(Map.of(), request);

        verify(nifi).updateConnection(eq("conn1"), argThat(update ->
                update.get("bends").equals(List.of(Map.of(
                        "x", 100.0, "y", 150.0)))));
    }

    @Test
    void connectionBendRollbackRegisteredBeforeUpdate() {
        LayoutGraph graph = emptyGraph();
        Map<String, List<Position>> bends = Map.of(
                "conn1", List.of(new Position(50, 100))
        );
        LayoutWriteRequest request = writeRequestWithBends(graph, Map.of(), bends);

        writer.write(Map.of(), request);

        var inOrder = inOrder(nifi, ledger);
        inOrder.verify(nifi).getConnection("conn1");
        inOrder.verify(ledger).addCanvasAction(contains("conn1"), any());
        inOrder.verify(nifi).updateConnection(eq("conn1"), any());
    }

    @Test
    void originalBendsCapturedFromCurrentEntity() {
        List<Map<String, Object>> originalBends = List.of(
                Map.of("x", 10.0, "y", 20.0),
                Map.of("x", 30.0, "y", 40.0)
        );
        when(nifi.getConnection("conn1")).thenReturn(
                Map.of("component", Map.of("bends", originalBends))
        );

        LayoutGraph graph = emptyGraph();
        Map<String, List<Position>> newBends = Map.of("conn1", List.of(new Position(99, 99)));
        LayoutWriteRequest request = writeRequestWithBends(graph, Map.of(), newBends);

        // Capture the rollback action runnable to inspect it later
        final Runnable[] capturedAction = new Runnable[1];
        doAnswer(inv -> {
            capturedAction[0] = inv.getArgument(1);
            return null;
        }).when(ledger).addCanvasAction(anyString(), any());

        writer.write(Map.of(), request);

        // Execute the captured rollback action
        capturedAction[0].run();
        // The rollback should call updateConnection with the original bends
        verify(nifi).updateConnection(eq("conn1"), argThat(u -> {
            Object bends = u.get("bends");
            return bends != null && bends.equals(originalBends);
        }));
    }

    // -------------------------------------------------------------------------
    // Partial failure: rollback actions still registered for completed mutations
    // -------------------------------------------------------------------------

    @Test
    void rollbackActionsRegisteredBeforeFailingMutation() {
        LayoutGraph graph = twoNodeGraph(
                "proc1", NodeType.PROCESSOR, 0, 0,
                "proc2", NodeType.PROCESSOR, 0, 0
        );
        // proc1 succeeds; proc2 throws
        when(nifi.updateProcessor(eq("proc1"), any())).thenReturn(Map.of());
        when(nifi.updateProcessor(eq("proc2"), any())).thenThrow(new RuntimeException("NiFi error"));

        LayoutWriteRequest request = writeRequest(graph, Map.of(
                "proc1", new Position(100, 100),
                "proc2", new Position(200, 200)
        ));

        assertThrows(RuntimeException.class, () -> writer.write(Map.of(), request));

        // Rollback for proc1 must have been registered before proc1's mutation
        verify(ledger, atLeastOnce()).addCanvasAction(contains("proc1"), any());
    }

    @Test
    void rollbackManagerRestoresCompletedProcessorMutation() {
        OwnershipLedger realLedger = new OwnershipLedger("target-pg");
        NiFiClientLayoutWriter realWriter = new NiFiClientLayoutWriter(nifi, realLedger);
        FlowDeploymentMetricsRegistry metrics = new FlowDeploymentMetricsRegistry();

        LayoutGraph graph = twoNodeGraph(
                "proc1", NodeType.PROCESSOR, 0, 0,
                "proc2", NodeType.PROCESSOR, 0, 0
        );

        when(nifi.updateProcessor(eq("proc1"), any())).thenReturn(Map.of());
        when(nifi.updateProcessor(eq("proc2"), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> updates = invocation.getArgument(1);
            @SuppressWarnings("unchecked")
            Map<String, Object> position = (Map<String, Object>) updates.get("position");
            double x = ((Number) position.get("x")).doubleValue();
            double y = ((Number) position.get("y")).doubleValue();
            if (x == 200.0 && y == 200.0) {
                throw new RuntimeException("NiFi error");
            }
            return Map.of();
        });

        LayoutWriteRequest request = writeRequest(graph, Map.of(
                "proc1", new Position(100, 100),
                "proc2", new Position(200, 200)
        ));

        RuntimeException failure = assertThrows(RuntimeException.class, () -> realWriter.write(Map.of(), request));

        RollbackManager.rollback(realLedger, nifi, failure, metrics);

        verify(nifi, atLeastOnce()).updateProcessor(eq("proc1"), argThat(u -> positionMatches(u, 0, 0)));
    }

    @Test
    void newlyRoutedCreatedConnectionCanvasActionExecutesBeforeDeletion() {
        OwnershipLedger realLedger = new OwnershipLedger("target-pg");
        NiFiClientLayoutWriter realWriter = new NiFiClientLayoutWriter(nifi, realLedger);
        FlowDeploymentMetricsRegistry metrics = new FlowDeploymentMetricsRegistry();

        // Simulate ConnectionDeployer: new connection created
        realLedger.addCreatedConnectionId("conn-new");

        // Use real NiFiClientLayoutWriter to register bend-restore canvas action
        when(nifi.getConnection("conn-new")).thenReturn(
                Map.of("component", Map.of("bends", List.of(Map.of("x", 1.0, "y", 2.0)))));
        LayoutGraph graph = emptyGraph();
        Map<String, List<Position>> bends = Map.of(
                "conn-new", List.of(new Position(50, 100)));
        LayoutWriteRequest request = writeRequestWithBends(graph, Map.of(), bends);
        realWriter.write(Map.of(), request);
        realLedger.addCanvasDeletionAction("delete endpoint",
                () -> nifi.deleteFunnel("endpoint-new"));

        // Reset invocations so we only track rollback calls
        clearInvocations(nifi);

        // Capture execution order during rollback
        List<String> callOrder = new ArrayList<>();
        doAnswer(inv -> { callOrder.add("canvas-restore"); return null; })
                .when(nifi).updateConnection(eq("conn-new"), any());
        doAnswer(inv -> { callOrder.add("delete-connection"); return null; })
                .when(nifi).deleteConnection("conn-new");
        doAnswer(inv -> { callOrder.add("delete-endpoint"); return null; })
                .when(nifi).deleteFunnel("endpoint-new");

        RollbackManager.rollback(realLedger, nifi, new RuntimeException("post-layout failure"), metrics);

        int restoreIdx = callOrder.indexOf("canvas-restore");
        int deleteIdx  = callOrder.indexOf("delete-connection");
        int endpointIdx = callOrder.indexOf("delete-endpoint");
        assertTrue(restoreIdx >= 0,  "canvas-restore action must be executed");
        assertTrue(deleteIdx  >= 0,  "delete-connection action must be executed");
        assertTrue(endpointIdx >= 0, "endpoint deletion action must be executed");
        assertTrue(restoreIdx < deleteIdx,
                "canvas-restore must precede delete-connection; got order: " + callOrder);
        assertTrue(deleteIdx < endpointIdx,
                "connection deletion must precede endpoint deletion; got order: " + callOrder);
    }

    // -------------------------------------------------------------------------
    // Null checks
    // -------------------------------------------------------------------------

    @Test
    void nullNifiThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new NiFiClientLayoutWriter(null, ledger));
    }

    @Test
    void nullLedgerThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new NiFiClientLayoutWriter(nifi, null));
    }

    @Test
    void nullRequestThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> writer.write(Map.of(), null));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static LayoutGraph singleNodeGraph(String id, NodeType type, int x, int y) {
        int w = switch (type) {
            case PROCESSOR -> 240;
            case PORT_INPUT, PORT_OUTPUT -> 80;
            case FUNNEL -> 48;
            case LABEL -> 150;
            case PROCESS_GROUP -> 400;
            case REMOTE_PROCESS_GROUP -> 240;
            default -> 50;
        };
        int h = switch (type) {
            case PROCESSOR -> 80;
            case PORT_INPUT, PORT_OUTPUT -> 48;
            case FUNNEL -> 48;
            case LABEL -> 50;
            case PROCESS_GROUP -> 300;
            case REMOTE_PROCESS_GROUP -> 120;
            default -> 50;
        };
        LayoutNode node = new LayoutNode(id, type, new BoundingBox(x, y, w, h), Map.of(), "parent-pg");
        return new LayoutGraph(Map.of(id, node), Map.of(), Map.of(), "pg");
    }

    private static LayoutGraph twoNodeGraph(String id1, NodeType type1, int x1, int y1,
                                             String id2, NodeType type2, int x2, int y2) {
        LayoutNode n1 = new LayoutNode(id1, type1, new BoundingBox(x1, y1, 240, 80), Map.of(), "parent-pg");
        LayoutNode n2 = new LayoutNode(id2, type2, new BoundingBox(x2, y2, 240, 80), Map.of(), "parent-pg");
        Map<String, LayoutNode> nodes = new LinkedHashMap<>();
        nodes.put(id1, n1);
        nodes.put(id2, n2);
        return new LayoutGraph(nodes, Map.of(), Map.of(), "pg");
    }

    private static LayoutGraph emptyGraph() {
        return new LayoutGraph(Map.of(), Map.of(), Map.of(), "pg");
    }

    private static LayoutWriteRequest writeRequest(LayoutGraph graph, Map<String, Position> newPositions) {
        return writeRequestWithBends(graph, newPositions, Map.of());
    }

    private static LayoutWriteRequest writeRequestWithBends(LayoutGraph graph,
                                                              Map<String, Position> newPositions,
                                                              Map<String, List<Position>> bends) {
        Map<String, Position> positions = new LinkedHashMap<>();
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        // Include existing node positions
        for (LayoutNode node : graph.getNodes().values()) {
            BoundingBox bb = node.getBoundingBox();
            positions.put(node.getId(), new Position(bb.x(), bb.y()));
            bounds.put(node.getId(), bb);
        }
        // Override with new positions
        positions.putAll(newPositions);
        for (Map.Entry<String, Position> e : newPositions.entrySet()) {
            LayoutNode node = graph.getNodes().get(e.getKey());
            if (node != null) {
                bounds.put(e.getKey(), new BoundingBox(e.getValue().x(), e.getValue().y(),
                        node.getBoundingBox().width(), node.getBoundingBox().height()));
            }
        }
        CoordinateAssignment coords = new CoordinateAssignment(positions, bounds);
        RoutingResult routing = new RoutingResult(bends);
        return new LayoutWriteRequest(graph, coords, routing, 0L, List.of());
    }

    @SuppressWarnings("unchecked")
    private static boolean positionMatches(Map<String, Object> updates, double expectedX, double expectedY) {
        Object pos = updates.get("position");
        if (!(pos instanceof Map<?, ?> posMap)) return false;
        Object x = ((Map<String, Object>) posMap).get("x");
        Object y = ((Map<String, Object>) posMap).get("y");
        return x instanceof Number nx && y instanceof Number ny
                && Math.abs(nx.doubleValue() - expectedX) < 0.001
                && Math.abs(ny.doubleValue() - expectedY) < 0.001;
    }
}
