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

package in.shrake.nifi.layout.rest;

import in.shrake.nifi.layout.support.FlowGraphAdapter;
import in.shrake.nifi.layout.core.exception.GraphValidationException;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.support.canonical.*;
import org.apache.nifi.web.api.dto.*;

import java.util.*;

/** Typed adapter for the public NiFi client DTO artifact. */
public final class RestDtoAdapter implements FlowGraphAdapter<ProcessGroupDTO> {
    private final CanonicalGraphAssembler assembler = new CanonicalGraphAssembler();

    @Override
    public LayoutGraph parse(ProcessGroupDTO group) {
        return assembler.assemble(new CanonicalFlowSnapshot(toGroup(group, null)));
    }

    private CanonicalProcessGroup toGroup(ProcessGroupDTO group, String fallbackParentId) {
        if (group == null || blank(group.getId())) {
            throw new GraphValidationException("REST process group and id must not be absent");
        }
        String parentId = first(group.getParentGroupId(), fallbackParentId);
        FlowSnippetDTO contents = group.getContents();
        PositionDTO position = group.getPosition();
        BoundingBox bounds = ComponentDefaults.bounds(NodeType.PROCESS_GROUP, x(position), y(position), null, null);
        if (contents == null) {
            return new CanonicalProcessGroup(group.getId(), parentId, bounds, attributes("name", group.getName()),
                    List.of(), List.of(), List.of());
        }

        List<CanonicalComponent> components = new ArrayList<>();
        collection(contents.getProcessors()).forEach(v -> components.add(component(v, NodeType.PROCESSOR,
                attributes("name", v.getName(), "type", v.getType()), null, null, group.getId())));
        collection(contents.getInputPorts()).forEach(v -> components.add(component(v, NodeType.PORT_INPUT,
                attributes("name", v.getName(), "type", v.getType()), null, null, group.getId())));
        collection(contents.getOutputPorts()).forEach(v -> components.add(component(v, NodeType.PORT_OUTPUT,
                attributes("name", v.getName(), "type", v.getType()), null, null, group.getId())));
        collection(contents.getFunnels()).forEach(v -> components.add(component(v, NodeType.FUNNEL, Map.of(), null, null, group.getId())));
        collection(contents.getLabels()).forEach(v -> components.add(component(v, NodeType.LABEL,
                attributes("text", v.getLabel()), v.getWidth(), v.getHeight(), group.getId())));
        collection(contents.getRemoteProcessGroups()).forEach(v -> components.add(component(v, NodeType.REMOTE_PROCESS_GROUP,
                attributes("name", v.getName()), null, null, group.getId())));

        Map<String, String> remotePorts = remotePortOwners(contents.getRemoteProcessGroups());
        List<CanonicalConnection> connections = collection(contents.getConnections()).stream().map(connection -> {
            ConnectableDTO source = require(connection.getSource(), connection.getId(), "source");
            ConnectableDTO target = require(connection.getDestination(), connection.getId(), "destination");
            String sourceId = remotePorts.getOrDefault(source.getId(), source.getId());
            String targetId = remotePorts.getOrDefault(target.getId(), target.getId());
            String targetPort = first(target.getName(), "");
            return new CanonicalConnection(requireId(connection.getId(), "connection"), group.getId(), sourceId,
                    targetId, collection(connection.getSelectedRelationships()), targetPort);
        }).toList();

        List<CanonicalProcessGroup> children = collection(contents.getProcessGroups()).stream()
                .map(child -> toGroup(child, group.getId())).toList();
        return new CanonicalProcessGroup(group.getId(), parentId, bounds, attributes("name", group.getName()),
                components, connections, children);
    }

    private CanonicalComponent component(ComponentDTO dto, NodeType type, Map<String, String> attributes,
                                         Number width, Number height, String fallbackParentId) {
        PositionDTO p = dto.getPosition();
        return new CanonicalComponent(requireId(dto.getId(), "component"), first(dto.getParentGroupId(), fallbackParentId),
                type, ComponentDefaults.bounds(type, x(p), y(p), width, height), attributes);
    }

    private static Map<String, String> remotePortOwners(Collection<RemoteProcessGroupDTO> groups) {
        Map<String, String> owners = new LinkedHashMap<>();
        collection(groups).forEach(group -> {
            RemoteProcessGroupContentsDTO contents = group.getContents();
            if (contents == null) return;
            collection(contents.getInputPorts()).forEach(port -> owners.put(port.getId(), group.getId()));
            collection(contents.getOutputPorts()).forEach(port -> owners.put(port.getId(), group.getId()));
        });
        return owners;
    }

    private static ConnectableDTO require(ConnectableDTO value, String connectionId, String endpoint) {
        if (value == null || blank(value.getId())) {
            throw new GraphValidationException("Connection '" + connectionId + "' has absent " + endpoint + " endpoint id");
        }
        return value;
    }

    private static String requireId(String id, String description) {
        if (blank(id)) throw new GraphValidationException(description + " id must not be absent");
        return id;
    }

    private static Double x(PositionDTO p) { return p == null ? null : p.getX(); }
    private static Double y(PositionDTO p) { return p == null ? null : p.getY(); }
    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private static String first(String value, String fallback) { return blank(value) ? fallback : value; }

    private static <T> List<T> collection(Collection<T> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    private static Map<String, String> attributes(String... pairs) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) if (!blank(pairs[i + 1])) result.put(pairs[i], pairs[i + 1]);
        return result;
    }
}
