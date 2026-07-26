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
package in.shrake.nifi.layout.copilot;

import in.shrake.nifi.layout.core.LayoutEngine;
import in.shrake.nifi.layout.core.exception.GraphValidationException;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ProcessGroupFlowMapAdapter}.
 *
 * <p>Tests cover: empty groups, all supported component types, shuffled ordering,
 * malformed endpoints (missing IDs on connections), and recursive child groups
 * via injected "flow" payloads.
 */
class ProcessGroupFlowMapAdapterTest {

    private final ProcessGroupFlowMapAdapter adapter = new ProcessGroupFlowMapAdapter();

    // -------------------------------------------------------------------------
    // Empty / minimal groups
    // -------------------------------------------------------------------------

    @Test
    void emptyGroupWithNoComponents() {
        Map<String, Object> input = flowMap("pg1", null, Map.of(
                "processors", List.of(),
                "inputPorts", List.of(),
                "outputPorts", List.of(),
                "funnels", List.of(),
                "labels", List.of(),
                "remoteProcessGroups", List.of(),
                "connections", List.of(),
                "processGroups", List.of()
        ));
        LayoutGraph graph = adapter.parse(input);
        assertNotNull(graph);
        assertTrue(graph.getNodes().isEmpty(), "empty group should produce no nodes");
        assertTrue(graph.getEdges().isEmpty(), "empty group should produce no edges");
    }

    @Test
    void nullSourceThrowsGraphValidationException() {
        assertThrows(GraphValidationException.class, () -> adapter.parse(null));
    }

    @Test
    void missingGroupIdThrowsGraphValidationException() {
        Map<String, Object> input = new LinkedHashMap<>();
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("flow", Map.of("processors", List.of()));
        // Deliberately omit "id"
        input.put("processGroupFlow", pgFlow);
        assertThrows(GraphValidationException.class, () -> adapter.parse(input));
    }

    @Test
    void missingFlowThrowsGraphValidationException() {
        Map<String, Object> input = new LinkedHashMap<>();
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", "pg1");
        // Deliberately omit "flow"
        input.put("processGroupFlow", pgFlow);
        assertThrows(GraphValidationException.class, () -> adapter.parse(input));
    }

    // -------------------------------------------------------------------------
    // All supported component types
    // -------------------------------------------------------------------------

    @Test
    void allSupportedComponentTypesAreIncluded() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of(component("proc1", "org.apache.nifi.processors.standard.GetFile", "GetFile", 0, 0, 240, 80)));
        flow.put("inputPorts", List.of(component("in1", null, "Input", 100, 0, 80, 48)));
        flow.put("outputPorts", List.of(component("out1", null, "Output", 200, 0, 80, 48)));
        flow.put("funnels", List.of(component("funnel1", null, null, 50, 50, 48, 48)));
        flow.put("labels", List.of(labelComponent("label1", "My Label", 300, 0, 150, 50)));
        flow.put("remoteProcessGroups", List.of(component("rpg1", null, "Remote", 400, 0, 240, 120)));
        flow.put("connections", List.of());
        flow.put("processGroups", List.of());

        Map<String, Object> input = flowMap("pg1", null, flow);
        LayoutGraph graph = adapter.parse(input);

        assertNodeType(graph, "proc1", NodeType.PROCESSOR);
        assertNodeType(graph, "in1", NodeType.PORT_INPUT);
        assertNodeType(graph, "out1", NodeType.PORT_OUTPUT);
        assertNodeType(graph, "funnel1", NodeType.FUNNEL);
        assertNodeType(graph, "label1", NodeType.LABEL);
        assertNodeType(graph, "rpg1", NodeType.REMOTE_PROCESS_GROUP);
    }

    @Test
    void shuffledComponentOrderingProducesSameNodes() {
        List<Object> processors = List.of(
                component("proc3", "Type", "C", 2, 0, 240, 80),
                component("proc1", "Type", "A", 0, 0, 240, 80),
                component("proc2", "Type", "B", 1, 0, 240, 80)
        );
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", processors);
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("connections", List.of());
        flow.put("processGroups", List.of());

        LayoutGraph graph = adapter.parse(flowMap("pg1", null, flow));
        assertTrue(graph.getNodes().containsKey("proc1"), "proc1 must be present");
        assertTrue(graph.getNodes().containsKey("proc2"), "proc2 must be present");
        assertTrue(graph.getNodes().containsKey("proc3"), "proc3 must be present");
    }

    // -------------------------------------------------------------------------
    // Malformed endpoints
    // -------------------------------------------------------------------------

    @Test
    void connectionWithMissingSourceIdThrowsException() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of(component("proc1", "Type", "Name", 0, 0, 240, 80)));
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("processGroups", List.of());

        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("id", "conn1");
        Map<String, Object> source = new LinkedHashMap<>();
        // Deliberately omit "id" in source
        Map<String, Object> destination = new LinkedHashMap<>();
        destination.put("id", "proc1");
        connection.put("source", source);
        connection.put("destination", destination);
        connection.put("selectedRelationships", List.of("success"));
        flow.put("connections", List.of(connection));

        assertThrows(GraphValidationException.class, () -> adapter.parse(flowMap("pg1", null, flow)));
    }

    @Test
    void connectionWithMissingDestinationIdThrowsException() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of(component("proc1", "Type", "Name", 0, 0, 240, 80)));
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("processGroups", List.of());

        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("id", "conn1");
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("id", "proc1");
        Map<String, Object> destination = new LinkedHashMap<>();
        // Deliberately omit "id" in destination
        connection.put("source", source);
        connection.put("destination", destination);
        connection.put("selectedRelationships", List.of("success"));
        flow.put("connections", List.of(connection));

        assertThrows(GraphValidationException.class, () -> adapter.parse(flowMap("pg1", null, flow)));
    }

    // -------------------------------------------------------------------------
    // Connections
    // -------------------------------------------------------------------------

    @Test
    void connectionEdgesAreRepresented() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of(
                component("src", "Type", "Source", 0, 0, 240, 80),
                component("dst", "Type", "Dest", 400, 0, 240, 80)
        ));
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("connections", List.of(connection("conn1", "src", "dst", List.of("success"))));
        flow.put("processGroups", List.of());

        LayoutGraph graph = adapter.parse(flowMap("pg1", null, flow));
        assertFalse(graph.getEdges().isEmpty(), "connection should produce at least one edge");
    }

    @Test
    void missingDimensionsUseRenderedNiFiGeometryForFanOutLayout() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of(
                componentWithoutDimensions("http"),
                componentWithoutDimensions("success"),
                componentWithoutDimensions("failure")
        ));
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("connections", List.of(
                connection("success-route", "http", "success", List.of("response")),
                connection("failure-route", "http", "failure", List.of("failure"))
        ));
        flow.put("processGroups", List.of());

        LayoutGraph graph = adapter.parse(flowMap("pg1", null, flow));
        assertEquals(350, graph.getNode("http").getBoundingBox().width());
        assertEquals(130, graph.getNode("http").getBoundingBox().height());

        LayoutResult result = LayoutEngine.create().layout(graph, LayoutOptions.defaults());
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        result.getUpdates().forEach(update -> bounds.put(update.componentId(), update.newBounds()));

        BoundingBox success = bounds.get("success");
        BoundingBox failure = bounds.get("failure");
        BoundingBox left = success.x() < failure.x() ? success : failure;
        BoundingBox right = success.x() < failure.x() ? failure : success;
        assertTrue(right.x() - left.right() >= LayoutOptions.defaults().getHorizontalSpacing());

        BoundingBox source = bounds.get("http");
        assertTrue(success.y() - source.bottom() >= LayoutOptions.defaults().getVerticalSpacing());
        assertTrue(failure.y() - source.bottom() >= LayoutOptions.defaults().getVerticalSpacing());
        assertEquals(4, result.getConnectionBendPoints().get("success-route").size());
        assertEquals(4, result.getConnectionBendPoints().get("failure-route").size());
    }

    @Test
    void fanOutConnectionsUseSeparateNiFiLabelLanes() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of(
                componentWithoutDimensions("merge"),
                componentWithoutDimensions("invoke")
        ));
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("connections", List.of(
                connection("merged", "merge", "invoke", List.of("merged")),
                connection("original", "merge", "invoke", List.of("original")),
                connection("failure", "merge", "invoke", List.of("failure"))
        ));
        flow.put("processGroups", List.of());

        LayoutResult result = LayoutEngine.create().layout(
                adapter.parse(flowMap("pg1", null, flow)), LayoutOptions.defaults());
        List<Position> labelAnchors = result.getConnectionBendPoints().values().stream()
                .map(path -> path.get(2))
                .sorted(java.util.Comparator.comparingInt(Position::x))
                .toList();

        assertEquals(3, labelAnchors.size());
        assertTrue(labelAnchors.get(1).x() - labelAnchors.get(0).x() >= 240);
        assertTrue(labelAnchors.get(2).x() - labelAnchors.get(1).x() >= 240);
    }

    @Test
    void fiveWayDistributionUsesExpandedRankSpacing() {
        Map<String, Object> flow = new LinkedHashMap<>();
        List<Map<String, Object>> processors = new ArrayList<>();
        processors.add(componentWithoutDimensions("distributor"));
        for (int i = 1; i <= 5; i++) {
            processors.add(componentWithoutDimensions("worker-" + i));
        }
        flow.put("processors", processors);
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        List<Map<String, Object>> connections = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            connections.add(connection("route-" + i, "distributor", "worker-" + i,
                    List.of(String.valueOf(i))));
        }
        flow.put("connections", connections);
        flow.put("processGroups", List.of());

        LayoutResult result = LayoutEngine.create().layout(
                adapter.parse(flowMap("pg1", null, flow)), LayoutOptions.defaults());
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        result.getUpdates().forEach(update -> bounds.put(update.componentId(), update.newBounds()));
        int workerTop = processors.stream()
                .skip(1)
                .map(processor -> bounds.get(processor.get("id")).y())
                .min(Integer::compareTo)
                .orElseThrow();

        assertTrue(workerTop - bounds.get("distributor").bottom() >= 200);
    }

    @Test
    void denseSuccessAndFailureFanInUsesSeparateSharedChannels() {
        Map<String, Object> flow = new LinkedHashMap<>();
        List<Map<String, Object>> processors = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            processors.add(componentWithoutDimensions("worker-" + i));
        }
        processors.add(componentWithoutDimensions("success-log"));
        processors.add(componentWithoutDimensions("failure-log"));
        flow.put("processors", processors);
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        List<Map<String, Object>> connections = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            connections.add(connection("worker-" + i + "-success",
                    "worker-" + i, "success-log", List.of("response")));
            connections.add(connection("worker-" + i + "-failure",
                    "worker-" + i, "failure-log", List.of("failure", "no retry")));
        }
        flow.put("connections", connections);
        flow.put("processGroups", List.of());

        LayoutResult result = LayoutEngine.create().layout(
                adapter.parse(flowMap("pg1", null, flow)), LayoutOptions.defaults());

        for (int i = 1; i <= 5; i++) {
            List<Position> successPath =
                    result.getConnectionBendPoints().get("worker-" + i + "-success");
            List<Position> failurePath =
                    result.getConnectionBendPoints().get("worker-" + i + "-failure");
            assertEquals(5, successPath.size());
            assertEquals(5, failurePath.size());
            assertTrue(Math.abs(successPath.get(2).y() - failurePath.get(2).y()) >= 50);
        }
    }

    // -------------------------------------------------------------------------
    // Recursive child groups (simulating assembled payload)
    // -------------------------------------------------------------------------

    @Test
    void childGroupWithInjectedFlowIsIncluded() {
        // Simulates what ProcessGroupFlowMapAssembler does: injects a "flow" key
        // into child group entities so the adapter sees nested components.
        Map<String, Object> childFlow = new LinkedHashMap<>();
        childFlow.put("processors", List.of(component("child-proc", "Type", "Name", 0, 0, 240, 80)));
        childFlow.put("inputPorts", List.of());
        childFlow.put("outputPorts", List.of());
        childFlow.put("funnels", List.of());
        childFlow.put("labels", List.of());
        childFlow.put("remoteProcessGroups", List.of());
        childFlow.put("connections", List.of());
        childFlow.put("processGroups", List.of());

        Map<String, Object> childEntity = new LinkedHashMap<>();
        childEntity.put("id", "child-pg");
        childEntity.put("component", Map.of(
                "id", "child-pg",
                "name", "Child",
                "position", Map.of("x", 100.0, "y", 200.0),
                "dimensions", Map.of("width", 400.0, "height", 300.0)));
        childEntity.put("flow", childFlow);

        Map<String, Object> parentFlow = new LinkedHashMap<>();
        parentFlow.put("processors", List.of());
        parentFlow.put("inputPorts", List.of());
        parentFlow.put("outputPorts", List.of());
        parentFlow.put("funnels", List.of());
        parentFlow.put("labels", List.of());
        parentFlow.put("remoteProcessGroups", List.of());
        parentFlow.put("connections", List.of());
        parentFlow.put("processGroups", List.of(childEntity));

        LayoutGraph graph = adapter.parse(flowMap("root-pg", null, parentFlow));

        // The root graph should have one subgraph for child-pg
        assertFalse(graph.getSubgraphs().isEmpty(), "child group should produce a subgraph");
        LayoutGraph childGraph = graph.getSubgraphs().get("child-pg");
        assertNotNull(childGraph, "subgraph for child-pg must exist");
        assertTrue(childGraph.getNodes().containsKey("child-proc"), "child processor must be in child subgraph");
        LayoutNode childGroupNode = graph.getNodes().get("child-pg");
        assertNotNull(childGroupNode, "child process group node must exist in parent graph");
        assertEquals(100, childGroupNode.getBoundingBox().x(), "child process group X must come from entity.position");
        assertEquals(200, childGroupNode.getBoundingBox().y(), "child process group Y must come from entity.position");
    }

    @Test
    void childGroupWithEmptyFlowProducesEmptySubgraph() {
        // Without assembler injection, child flow is empty — group is created but with no components.
        Map<String, Object> childEntity = new LinkedHashMap<>();
        childEntity.put("id", "child-pg");
        childEntity.put("component", Map.of("id", "child-pg", "name", "Child"));
        // No "flow" key

        Map<String, Object> parentFlow = new LinkedHashMap<>();
        parentFlow.put("processors", List.of());
        parentFlow.put("inputPorts", List.of());
        parentFlow.put("outputPorts", List.of());
        parentFlow.put("funnels", List.of());
        parentFlow.put("labels", List.of());
        parentFlow.put("remoteProcessGroups", List.of());
        parentFlow.put("connections", List.of());
        parentFlow.put("processGroups", List.of(childEntity));

        LayoutGraph graph = adapter.parse(flowMap("root-pg", null, parentFlow));

        // A subgraph is created even for an empty child
        LayoutGraph childGraph = graph.getSubgraphs().get("child-pg");
        assertNotNull(childGraph, "subgraph for child-pg must exist even if empty");
        assertTrue(childGraph.getNodes().isEmpty(), "empty child flow should produce no nodes");
    }

    // -------------------------------------------------------------------------
    // Entity wrapping (NiFi entity vs flat map)
    // -------------------------------------------------------------------------

    @Test
    void wrappedEntityComponentMergedWithTopLevel() {
        Map<String, Object> componentMap = new LinkedHashMap<>();
        componentMap.put("id", "proc1");
        componentMap.put("name", "Processor One");
        componentMap.put("type", "org.apache.nifi.Test");
        componentMap.put("position", Map.of("x", 10.0, "y", 20.0));
        componentMap.put("dimensions", Map.of("width", 240.0, "height", 80.0));

        Map<String, Object> entity = new LinkedHashMap<>();
        entity.put("id", "proc1");
        entity.put("revision", Map.of("version", 1));
        entity.put("component", componentMap);

        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("processors", List.of(entity));
        flow.put("inputPorts", List.of());
        flow.put("outputPorts", List.of());
        flow.put("funnels", List.of());
        flow.put("labels", List.of());
        flow.put("remoteProcessGroups", List.of());
        flow.put("connections", List.of());
        flow.put("processGroups", List.of());

        LayoutGraph graph = adapter.parse(flowMap("pg1", null, flow));
        assertNodeType(graph, "proc1", NodeType.PROCESSOR);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static Map<String, Object> flowMap(String id, String parentId, Map<String, Object> flow) {
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", id);
        if (parentId != null) pgFlow.put("parentGroupId", parentId);
        pgFlow.put("flow", flow);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("processGroupFlow", pgFlow);
        return root;
    }

    private static Map<String, Object> component(String id, String type, String name, double x, double y, double w, double h) {
        Map<String, Object> comp = new LinkedHashMap<>();
        comp.put("id", id);
        if (type != null) comp.put("type", type);
        if (name != null) comp.put("name", name);
        comp.put("position", Map.of("x", x, "y", y));
        comp.put("dimensions", Map.of("width", w, "height", h));
        return comp;
    }

    private static Map<String, Object> componentWithoutDimensions(String id) {
        return Map.of(
                "id", id,
                "type", "org.apache.nifi.Test",
                "name", id,
                "position", Map.of("x", 0.0, "y", 0.0));
    }

    private static Map<String, Object> labelComponent(String id, String text, double x, double y, double w, double h) {
        Map<String, Object> comp = new LinkedHashMap<>();
        comp.put("id", id);
        comp.put("label", text);
        comp.put("position", Map.of("x", x, "y", y));
        comp.put("width", w);
        comp.put("height", h);
        return comp;
    }

    private static Map<String, Object> connection(String id, String srcId, String dstId, List<String> rels) {
        Map<String, Object> conn = new LinkedHashMap<>();
        conn.put("id", id);
        conn.put("source", Map.of("id", srcId));
        conn.put("destination", Map.of("id", dstId));
        conn.put("selectedRelationships", new ArrayList<>(rels));
        return conn;
    }

    private static void assertNodeType(LayoutGraph graph, String nodeId, NodeType expected) {
        LayoutNode node = graph.getNodes().get(nodeId);
        assertNotNull(node, "Node '" + nodeId + "' must be present");
        assertEquals(expected, node.getType(), "Node '" + nodeId + "' type mismatch");
    }
}
