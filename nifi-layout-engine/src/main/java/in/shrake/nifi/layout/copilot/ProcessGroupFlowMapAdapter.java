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

import in.shrake.nifi.layout.support.FlowGraphAdapter;
import in.shrake.nifi.layout.core.exception.GraphValidationException;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.support.canonical.*;

import java.util.*;

/**
 * Parses the map returned by NiFiClientOperations.getProcessGroupFlow(). It deliberately
 * has no compile dependency on Copilot or NiFi DTOs and never casts payloads to ProcessGroupDTO.
 */
public final class ProcessGroupFlowMapAdapter implements FlowGraphAdapter<Map<String, Object>> {
    private final CanonicalGraphAssembler assembler = new CanonicalGraphAssembler();

    @Override
    public LayoutGraph parse(Map<String, Object> source) {
        Map<String, Object> processGroupFlow = map(source == null ? null : source.get("processGroupFlow"));
        if (processGroupFlow.isEmpty() && source != null && source.containsKey("flow")) processGroupFlow = source;
        Map<String, Object> flow = map(processGroupFlow.get("flow"));
        if (flow.isEmpty()) throw new GraphValidationException("Map payload must contain processGroupFlow.flow");
        String id = first(string(processGroupFlow.get("id")), string(flow.get("id")), string(source == null ? null : source.get("id")));
        if (blank(id)) throw new GraphValidationException("Map process group id must not be absent");
        return assembler.assemble(new CanonicalFlowSnapshot(
                toGroup(id, string(processGroupFlow.get("parentGroupId")), processGroupFlow, flow)));
    }

    private CanonicalProcessGroup toGroup(String id, String parentId,
                                          Map<String, Object> groupData,
                                          Map<String, Object> flow) {
        List<CanonicalComponent> components = new ArrayList<>();
        addComponents(components, flow.get("processors"), NodeType.PROCESSOR, id);
        addComponents(components, flow.get("inputPorts"), NodeType.PORT_INPUT, id);
        addComponents(components, flow.get("outputPorts"), NodeType.PORT_OUTPUT, id);
        addComponents(components, flow.get("funnels"), NodeType.FUNNEL, id);
        addComponents(components, flow.get("labels"), NodeType.LABEL, id);
        addComponents(components, flow.get("remoteProcessGroups"), NodeType.REMOTE_PROCESS_GROUP, id);

        Map<String, String> remotePortOwners = remotePortOwners(flow.get("remoteProcessGroups"));
        List<CanonicalConnection> connections = values(flow.get("connections")).stream().map(this::entity).map(connection -> {
            String connectionId = required(connection, "id", "connection");
            Map<String, Object> source = componentMap(connection.get("source"));
            Map<String, Object> destination = componentMap(connection.get("destination"));
            String sourceId = remotePortOwners.getOrDefault(required(source, "id", "connection " + connectionId + " source"), string(source.get("id")));
            String targetRaw = required(destination, "id", "connection " + connectionId + " destination");
            String targetId = remotePortOwners.getOrDefault(targetRaw, targetRaw);
            return new CanonicalConnection(connectionId, id, sourceId, targetId,
                    strings(connection.get("selectedRelationships")), first(string(destination.get("name")), ""));
        }).toList();

        List<CanonicalProcessGroup> children = values(flow.get("processGroups")).stream().map(this::entity).map(group -> {
            String childId = required(group, "id", "child process group");
            Map<String, Object> childFlow = map(group.get("flow"));
            if (childFlow.isEmpty()) childFlow = map(group.get("contents"));
            return toGroup(childId, id, group, childFlow);
        }).toList();
        return new CanonicalProcessGroup(id, parentId,
                ComponentDefaults.bounds(NodeType.PROCESS_GROUP,
                        firstNumber(number(groupData, "position", "x"), number(flow, "position", "x")),
                        firstNumber(number(groupData, "position", "y"), number(flow, "position", "y")),
                        firstNumber(number(groupData, "dimensions", "width"), number(flow, "dimensions", "width")),
                        firstNumber(number(groupData, "dimensions", "height"), number(flow, "dimensions", "height"))),
                Map.of(), components, connections, children);
    }

    private void addComponents(List<CanonicalComponent> output, Object raw, NodeType type, String parentId) {
        for (Object value : values(raw)) {
            Map<String, Object> component = entity(value);
            String componentId = required(component, "id", type.name().toLowerCase(Locale.ROOT));
            Number width = type == NodeType.LABEL ? firstNumber(component.get("width"), number(component, "dimensions", "width")) : number(component, "dimensions", "width");
            Number height = type == NodeType.LABEL ? firstNumber(component.get("height"), number(component, "dimensions", "height")) : number(component, "dimensions", "height");
            Map<String, String> attributes = new LinkedHashMap<>();
            put(attributes, "name", string(component.get("name")));
            put(attributes, "type", string(component.get("type")));
            put(attributes, "text", string(component.get("label")));
            output.add(new CanonicalComponent(componentId, first(string(component.get("parentGroupId")), parentId), type,
                    ComponentDefaults.bounds(type, number(component, "position", "x"), number(component, "position", "y"), width, height), attributes));
        }
    }

    private Map<String, String> remotePortOwners(Object groups) {
        Map<String, String> owners = new LinkedHashMap<>();
        for (Object raw : values(groups)) {
            Map<String, Object> group = entity(raw);
            String groupId = string(group.get("id"));
            Map<String, Object> contents = map(group.get("contents"));
            for (Object port : values(contents.get("inputPorts"))) owners.put(required(entity(port), "id", "remote input port"), groupId);
            for (Object port : values(contents.get("outputPorts"))) owners.put(required(entity(port), "id", "remote output port"), groupId);
        }
        return owners;
    }

    /** Unwraps both NiFi entity maps ({@code component}) and already-normalized component maps. */
    private Map<String, Object> entity(Object value) {
        Map<String, Object> entity = map(value);
        Map<String, Object> component = map(entity.get("component"));
        if (component.isEmpty()) return entity;
        Map<String, Object> merged = new LinkedHashMap<>(entity);
        merged.remove("component");
        merged.putAll(component);
        return merged;
    }

    private Map<String, Object> componentMap(Object value) { return entity(value); }
    private static List<Object> values(Object value) { return value instanceof Collection<?> c ? List.copyOf(c) : List.of(); }
    private static List<String> strings(Object value) { return values(value).stream().map(ProcessGroupFlowMapAdapter::string).filter(Objects::nonNull).toList(); }
    @SuppressWarnings("unchecked") private static Map<String, Object> map(Object value) { return value instanceof Map<?, ?> m ? new LinkedHashMap<>((Map<String, Object>) m) : Map.of(); }
    private static Number number(Map<String, Object> source, String object, String field) { return firstNumber(map(source.get(object)).get(field), null); }
    private static Number firstNumber(Number primary, Number fallback) { return primary != null ? primary : fallback; }
    private static Number firstNumber(Object value, Number fallback) { return value instanceof Number n ? n : fallback; }
    private static String string(Object value) { return value == null ? null : String.valueOf(value); }
    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private static String first(String... values) { for (String value : values) if (!blank(value)) return value; return null; }
    private static String required(Map<String, Object> source, String field, String description) { String value = string(source.get(field)); if (blank(value)) throw new GraphValidationException(description + " id must not be absent"); return value; }
    private static void put(Map<String, String> target, String key, String value) { if (!blank(value)) target.put(key, value); }
}
