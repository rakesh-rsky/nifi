/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.copilot.fixture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CopilotFlowMapFixtures {
    private static final String PROCESSOR_TYPE = "org.apache.nifi.TestProcessor";

    private CopilotFlowMapFixtures() {
    }

    public static Map<String, Map<String, Object>> all() {
        final Map<String, Map<String, Object>> fixtures = new LinkedHashMap<>();
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

    public static Map<String, Object> linearPipeline() {
        return processorFlow(
                List.of(processor("c", true), processor("a", false),
                        processor("d", false), processor("b", true)),
                List.of(
                        connection("c-d", "c", "d", List.of("success")),
                        connection("a-b", "a", "b", List.of("success")),
                        connection("b-c", "b", "c", List.of("success"))));
    }

    public static Map<String, Object> twoWayFanOut() {
        return processorFlow(
                List.of(processor("failure", true), processor("source", false),
                        processor("success", false)),
                List.of(
                        connection("source-failure", "source", "failure", List.of("failure")),
                        connection("source-success", "source", "success", List.of("response"))));
    }

    public static Map<String, Object> wideDistribution() {
        return processorFlow(
                List.of(
                        processor("worker-3", true),
                        processor("worker-1", false),
                        processor("distributor", true),
                        processor("worker-5", false),
                        processor("worker-2", true),
                        processor("worker-4", false)),
                List.of(
                        connection("route-5", "distributor", "worker-5", List.of("5")),
                        connection("route-2", "distributor", "worker-2", List.of("2")),
                        connection("route-4", "distributor", "worker-4", List.of("4")),
                        connection("route-1", "distributor", "worker-1", List.of("1")),
                        connection("route-3", "distributor", "worker-3", List.of("3"))));
    }

    public static Map<String, Object> denseFanIn() {
        return processorFlow(
                List.of(
                        processor("worker-4", false),
                        processor("result-log", true),
                        processor("worker-2", true),
                        processor("worker-1", false),
                        processor("worker-3", false)),
                List.of(
                        connection("worker-4-result", "worker-4", "result-log", List.of("response")),
                        connection("worker-2-result", "worker-2", "result-log", List.of("response")),
                        connection("worker-1-result", "worker-1", "result-log", List.of("response")),
                        connection("worker-3-result", "worker-3", "result-log", List.of("response"))));
    }

    public static Map<String, Object> failureBranches() {
        return processorFlow(
                List.of(
                        processor("invoke", true),
                        processor("extract", false),
                        processor("failure-log", true),
                        processor("build-url", false)),
                List.of(
                        connection("invoke-failure", "invoke", "failure-log",
                                List.of("no retry", "failure")),
                        connection("extract-failure", "extract", "failure-log",
                                List.of("unmatched", "failure")),
                        connection("build-success", "build-url", "invoke", List.of("success")),
                        connection("extract-matched", "extract", "build-url", List.of("matched"))));
    }

    public static Map<String, Object> csvPostgresBranches() {
        return processorFlow(
                List.of(
                        processor("get-file", true),
                        processor("convert-record", false),
                        processor("validate-record", true),
                        processor("put-database-record", false),
                        processor("archive", true),
                        processor("validate-failure", false),
                        processor("database-failure", true)),
                List.of(
                        connection("get-convert", "get-file", "convert-record",
                               List.of("success")),
                        connection("convert-validate", "convert-record", "validate-record",
                               List.of("success")),
                        connection("validate-database", "validate-record", "put-database-record",
                               List.of("valid")),
                        connection("validate-failure", "validate-record", "validate-failure",
                               List.of("invalid")),
                        connection("database-archive", "put-database-record", "archive",
                               List.of("success")),
                        connection("database-failure", "put-database-record", "database-failure",
                               List.of("failure"))));
    }

    public static Map<String, Object> parallelRelationships() {
        return processorFlow(
                List.of(processor("invoke", true), processor("merge", false)),
                List.of(
                        connection("original", "merge", "invoke", List.of("original")),
                        connection("failure", "merge", "invoke", List.of("failure")),
                        connection("merged", "merge", "invoke", List.of("merged"))));
    }

    public static Map<String, Object> cycleWithSelfLoop() {
        return processorFlow(
                List.of(processor("b", true), processor("a", false), processor("c", true)),
                List.of(
                        connection("c-a", "c", "a", List.of("retry")),
                        connection("a-a", "a", "a", List.of("retry")),
                        connection("b-c", "b", "c", List.of("success")),
                        connection("a-b", "a", "b", List.of("success"))));
    }

    public static Map<String, Object> funnelFlow() {
        final Map<String, Object> flow = emptyFlow();
        flow.put("processors", List.of(
                processor("source-b", true),
                processor("sink", false),
                processor("source-a", false)));
        flow.put("funnels", List.of(component("funnel", null, null, false)));
        flow.put("connections", List.of(
                connection("funnel-sink", "funnel", "sink", List.of()),
                connection("b-funnel", "source-b", "funnel", List.of("success")),
                connection("a-funnel", "source-a", "funnel", List.of("success"))));
        return flowMap("root", flow);
    }

    public static Map<String, Object> disconnectedSubgraphs() {
        return processorFlow(
                List.of(processor("d", true), processor("b", false),
                        processor("a", true), processor("c", false)),
                List.of(
                        connection("c-d", "c", "d", List.of("success")),
                        connection("a-b", "a", "b", List.of("success"))));
    }

    public static Map<String, Object> portsRpgsNestedGroup() {
        final Map<String, Object> childFlow = emptyFlow();
        childFlow.put("processors", List.of(processor("child-processor", true)));
        childFlow.put("inputPorts", List.of(component(
                "child-input", null, "Child Input", false)));
        childFlow.put("outputPorts", List.of(component(
                "child-output", null, "Child Output", true)));
        childFlow.put("connections", List.of(
                connection("child-processor-output", "child-processor", "child-output",
                        List.of("success")),
                connection("child-input-processor", "child-input", "child-processor", List.of())));

        final Map<String, Object> child = new LinkedHashMap<>();
        child.put("id", "child-group");
        child.put("component", Map.of(
                "id", "child-group",
                "position", Map.of("x", 0, "y", 0),
                "dimensions", Map.of("width", 384, "height", 176)));
        child.put("flow", childFlow);

        final Map<String, Object> flow = emptyFlow();
        flow.put("processors", List.of(processor("processor", true)));
        flow.put("inputPorts", List.of(component("input", null, "Input", false)));
        flow.put("outputPorts", List.of(component("output", null, "Output", true)));
        flow.put("labels", List.of(label("label", "Flow Boundary", true)));
        flow.put("remoteProcessGroups", List.of(component(
                "remote", null, "Remote", false)));
        flow.put("processGroups", List.of(child));
        flow.put("connections", List.of(
                connection("processor-remote", "processor", "remote", List.of("success")),
                connection("input-processor", "input", "processor", List.of()),
                connection("processor-output", "processor", "output", List.of("success"))));
        return flowMap("root", flow);
    }

    private static Map<String, Object> processorFlow(
            final List<Map<String, Object>> processors,
            final List<Map<String, Object>> connections) {
        final Map<String, Object> flow = emptyFlow();
        flow.put("processors", processors);
        flow.put("connections", connections);
        return flowMap("root", flow);
    }

    private static Map<String, Object> emptyFlow() {
        final Map<String, Object> flow = new LinkedHashMap<>();
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

    private static Map<String, Object> processor(final String id, final boolean wrapped) {
        return component(id, PROCESSOR_TYPE, id, wrapped);
    }

    private static Map<String, Object> component(
            final String id,
            final String type,
            final String name,
            final boolean wrapped) {
        final Map<String, Object> component = new LinkedHashMap<>();
        component.put("id", id);
        if (type != null) {
            component.put("type", type);
        }
        if (name != null) {
            component.put("name", name);
        }
        component.put("position", Map.of("x", 0, "y", 0));
        if (!wrapped) {
            return component;
        }
        final Map<String, Object> entity = new LinkedHashMap<>();
        entity.put("id", id);
        entity.put("revision", Map.of("version", 0));
        entity.put("component", component);
        return entity;
    }

    private static Map<String, Object> label(
            final String id,
            final String text,
            final boolean wrapped) {
        final Map<String, Object> component = new LinkedHashMap<>();
        component.put("id", id);
        component.put("label", text);
        component.put("position", Map.of("x", 0, "y", 0));
        if (!wrapped) {
            return component;
        }
        return Map.of("id", id, "component", component);
    }

    private static Map<String, Object> connection(
            final String id,
            final String source,
            final String destination,
            final List<String> relationships) {
        final Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("id", id);
        connection.put("source", Map.of("id", source));
        connection.put("destination", Map.of("id", destination));
        connection.put("selectedRelationships", new ArrayList<>(relationships));
        return connection;
    }

    private static Map<String, Object> flowMap(
            final String id,
            final Map<String, Object> flow) {
        final Map<String, Object> processGroupFlow = new LinkedHashMap<>();
        processGroupFlow.put("id", id);
        processGroupFlow.put("flow", flow);
        return Map.of("processGroupFlow", processGroupFlow);
    }
}
