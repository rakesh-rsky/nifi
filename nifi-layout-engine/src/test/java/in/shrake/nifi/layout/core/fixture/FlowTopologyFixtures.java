/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.fixture;

import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.support.canonical.ComponentDefaults;
import in.shrake.nifi.layout.support.canonical.RelationshipCodec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FlowTopologyFixtures {
    private static final String ROOT = "root";
    private static final String PROCESSOR_TYPE = "org.apache.nifi.TestProcessor";

    private FlowTopologyFixtures() {
    }

    public static Map<String, LayoutGraph> all() {
        final Map<String, LayoutGraph> fixtures = new LinkedHashMap<>();
        fixtures.put("linear", linearPipeline());
        fixtures.put("two-way-fan-out", twoWayFanOut());
        fixtures.put("wide-distribution", wideDistribution());
        fixtures.put("dense-fan-in", denseFanIn());
        fixtures.put("failure-branches", failureBranches());
        fixtures.put("csv-postgres-branches", csvPostgresBranches());
        fixtures.put("parallel-relationships", parallelRelationships());
        fixtures.put("cycle-self-loop", cycleWithSelfLoop());
        fixtures.put("funnel", funnelFlow());
        fixtures.put("disconnected", disconnectedSubgraphs());
        fixtures.put("ports-rpg-nested", portsRpgsNestedGroup());
        return Collections.unmodifiableMap(fixtures);
    }

    public static LayoutGraph linearPipeline() {
        return graph(
                nodes(processor("a"), processor("b"), processor("c"), processor("d")),
                edges(
                        edge("a-b", "a", "b", "success"),
                        edge("b-c", "b", "c", "success"),
                        edge("c-d", "c", "d", "success")));
    }

    public static LayoutGraph twoWayFanOut() {
        return graph(
                nodes(processor("source"), processor("success"), processor("failure")),
                edges(
                        edge("source-success", "source", "success", "response"),
                        edge("source-failure", "source", "failure", "failure")));
    }

    public static LayoutGraph wideDistribution() {
        final Map<String, LayoutNode> nodes = nodes(
                processor("distributor"),
                processor("worker-1"),
                processor("worker-2"),
                processor("worker-3"),
                processor("worker-4"),
                processor("worker-5"));
        final Map<String, LayoutEdge> edges = new LinkedHashMap<>();
        for (int index = 1; index <= 5; index++) {
            put(edges, edge("route-" + index, "distributor", "worker-" + index,
                    String.valueOf(index)));
        }
        return graph(nodes, edges);
    }

    public static LayoutGraph denseFanIn() {
        return graph(
                nodes(
                        processor("worker-1"),
                        processor("worker-2"),
                        processor("worker-3"),
                        processor("worker-4"),
                        processor("result-log")),
                edges(
                        edge("worker-1-result", "worker-1", "result-log", "response"),
                        edge("worker-2-result", "worker-2", "result-log", "response"),
                        edge("worker-3-result", "worker-3", "result-log", "response"),
                        edge("worker-4-result", "worker-4", "result-log", "response")));
    }

    public static LayoutGraph failureBranches() {
        return graph(
                nodes(
                        processor("extract"),
                        processor("build-url"),
                        processor("invoke"),
                        processor("failure-log")),
                edges(
                        edge("extract-matched", "extract", "build-url", "matched"),
                        edge("build-success", "build-url", "invoke", "success"),
                        edge("extract-failure", "extract", "failure-log",
                                List.of("unmatched", "failure")),
                        edge("invoke-failure", "invoke", "failure-log",
                                List.of("no retry", "failure"))));
    }

    public static LayoutGraph csvPostgresBranches() {
        return graph(
                nodes(
                        processor("get-file"),
                        processor("convert-record"),
                        processor("validate-record"),
                        processor("put-database-record"),
                        processor("archive"),
                        processor("validate-failure"),
                        processor("database-failure")),
                edges(
                        edge("get-convert", "get-file", "convert-record", "success"),
                        edge("convert-validate", "convert-record", "validate-record", "success"),
                        edge("validate-database", "validate-record", "put-database-record", "valid"),
                        edge("validate-failure", "validate-record", "validate-failure", "invalid"),
                        edge("database-archive", "put-database-record", "archive", "success"),
                        edge("database-failure", "put-database-record", "database-failure", "failure")));
    }

    public static LayoutGraph parallelRelationships() {
        return graph(
                nodes(processor("merge"), processor("invoke")),
                edges(
                        edge("merged", "merge", "invoke", "merged"),
                        edge("original", "merge", "invoke", "original"),
                        edge("failure", "merge", "invoke", "failure")));
    }

    public static LayoutGraph cycleWithSelfLoop() {
        return graph(
                nodes(processor("a"), processor("b"), processor("c")),
                edges(
                        edge("a-b", "a", "b", "success"),
                        edge("b-c", "b", "c", "success"),
                        edge("c-a", "c", "a", "retry"),
                        edge("a-a", "a", "a", "retry")));
    }

    public static LayoutGraph funnelFlow() {
        return graph(
                nodes(
                        processor("source-a"),
                        processor("source-b"),
                        node("funnel", NodeType.FUNNEL, Map.of()),
                        processor("sink")),
                edges(
                        edge("a-funnel", "source-a", "funnel", "success"),
                        edge("b-funnel", "source-b", "funnel", "success"),
                        edge("funnel-sink", "funnel", "sink", "")));
    }

    public static LayoutGraph disconnectedSubgraphs() {
        return graph(
                nodes(processor("a"), processor("b"), processor("c"), processor("d")),
                edges(
                        edge("a-b", "a", "b", "success"),
                        edge("c-d", "c", "d", "success")));
    }

    public static LayoutGraph portsRpgsNestedGroup() {
        final LayoutGraph child = new LayoutGraph(
                nodes(
                        node("child-input", NodeType.PORT_INPUT,
                                Map.of("name", "Child Input"), "child-group"),
                        processor("child-processor", "child-group"),
                        node("child-output", NodeType.PORT_OUTPUT,
                                Map.of("name", "Child Output"), "child-group")),
                edges(
                        edge("child-input-processor", "child-input", "child-processor", ""),
                        edge("child-processor-output", "child-processor", "child-output", "success")),
                Map.of(),
                "child-group");

        final Map<String, LayoutNode> rootNodes = nodes(
                node("input", NodeType.PORT_INPUT, Map.of("name", "Input")),
                processor("processor"),
                node("output", NodeType.PORT_OUTPUT, Map.of("name", "Output")),
                node("remote", NodeType.REMOTE_PROCESS_GROUP, Map.of("name", "Remote")),
                node("label", NodeType.LABEL, Map.of("text", "Flow Boundary")),
                node("child-group", NodeType.PROCESS_GROUP, Map.of()));
        return new LayoutGraph(
                rootNodes,
                edges(
                        edge("input-processor", "input", "processor", ""),
                        edge("processor-output", "processor", "output", "success"),
                        edge("processor-remote", "processor", "remote", "success")),
                Map.of("child-group", child),
                ROOT);
    }

    private static LayoutGraph graph(
            final Map<String, LayoutNode> nodes,
            final Map<String, LayoutEdge> edges) {
        return new LayoutGraph(nodes, edges, Map.of(), ROOT);
    }

    private static LayoutNode processor(final String id) {
        return processor(id, ROOT);
    }

    private static LayoutNode processor(final String id, final String parentGroupId) {
        return node(id, NodeType.PROCESSOR,
                Map.of("name", id, "type", PROCESSOR_TYPE), parentGroupId);
    }

    private static LayoutNode node(
            final String id,
            final NodeType type,
            final Map<String, String> attributes) {
        return node(id, type, attributes, ROOT);
    }

    private static LayoutNode node(
            final String id,
            final NodeType type,
            final Map<String, String> attributes,
            final String parentGroupId) {
        return new LayoutNode(id, type, ComponentDefaults.bounds(type), attributes, parentGroupId);
    }

    private static LayoutEdge edge(
            final String id,
            final String source,
            final String target,
            final String relationship) {
        return edge(id, source, target,
                relationship.isEmpty() ? List.of() : List.of(relationship));
    }

    private static LayoutEdge edge(
            final String id,
            final String source,
            final String target,
            final List<String> relationships) {
        return new LayoutEdge(id, source, target, RelationshipCodec.encode(relationships),
                "", false, source.equals(target));
    }

    private static Map<String, LayoutNode> nodes(final LayoutNode... values) {
        final Map<String, LayoutNode> nodes = new LinkedHashMap<>();
        for (LayoutNode value : values) {
            nodes.put(value.getId(), value);
        }
        return nodes;
    }

    private static Map<String, LayoutEdge> edges(final LayoutEdge... values) {
        final Map<String, LayoutEdge> edges = new LinkedHashMap<>();
        for (LayoutEdge value : values) {
            put(edges, value);
        }
        return edges;
    }

    private static void put(
            final Map<String, LayoutEdge> edges,
            final LayoutEdge value) {
        edges.put(value.getId(), value);
    }
}
