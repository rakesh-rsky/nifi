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


import in.shrake.nifi.layout.core.exception.WriteBackException;

import in.shrake.nifi.layout.core.model.*;
import in.shrake.nifi.layout.support.writer.*;
import org.apache.nifi.web.api.dto.*;

import java.util.*;

/** Applies core layout output to mutable NiFi REST DTOs. */
public final class RestDtoWriter implements FlowLayoutWriter<ProcessGroupDTO> {
    private final LayoutResultFactory results = new LayoutResultFactory();

    @Override
    public LayoutResult write(ProcessGroupDTO target, LayoutWriteRequest request) {
        Objects.requireNonNull(target, "target must not be null");
        Map<String, ComponentDTO> components = new LinkedHashMap<>();
        Map<String, ConnectionDTO> connections = new LinkedHashMap<>();
        index(target, components, connections);
        LayoutResult result = results.create(request);
        for (ComponentUpdate update : result.getUpdates()) {
            ComponentDTO component = components.get(update.componentId());
            if (component == null) throw unmappable(update);
            try {
                component.setPosition(new PositionDTO((double) update.newPosition().x(),
                        (double) update.newPosition().y()));
            } catch (RuntimeException error) {
                throw failure(update, error);
            }
        }
        writeRoutes(request, connections);
        return result;
    }

    private void writeRoutes(LayoutWriteRequest request, Map<String, ConnectionDTO> connections) {
        if (request.routing() == null) return;
        for (Map.Entry<String, List<Position>> route : request.routing().getEdgePaths().entrySet()) {
            ConnectionDTO connection = connections.get(route.getKey());
            if (connection == null) {
                throw new WriteBackException(route.getKey(), "CONNECTION", "original connection cannot be mapped");
            }
            try {
                List<Position> path = route.getValue();
                // Paths are full source-to-target routes; NiFi bends are interior points only.
                List<Position> bends = path.size() <= 2 ? List.of() : path.subList(1, path.size() - 1);
                connection.setBends(bends.stream()
                        .map(p -> new PositionDTO((double) p.x(), (double) p.y())).toList());
            } catch (RuntimeException error) {
                throw new WriteBackException(route.getKey(), "CONNECTION", error.getMessage(), error);
            }
        }
    }

    private void index(ProcessGroupDTO group, Map<String, ComponentDTO> components,
                       Map<String, ConnectionDTO> connections) {
        components.put(group.getId(), group);
        FlowSnippetDTO flow = group.getContents();
        if (flow == null) return;
        add(components, flow.getProcessors()); add(components, flow.getInputPorts());
        add(components, flow.getOutputPorts()); add(components, flow.getFunnels());
        add(components, flow.getLabels()); add(components, flow.getRemoteProcessGroups());
        collection(flow.getConnections()).forEach(c -> connections.put(c.getId(), c));
        collection(flow.getProcessGroups()).forEach(child -> index(child, components, connections));
    }

    private static void add(Map<String, ComponentDTO> target, Collection<? extends ComponentDTO> values) {
        collection(values).forEach(value -> target.put(value.getId(), value));
    }
    private static WriteBackException unmappable(ComponentUpdate update) {
        return new WriteBackException(update.componentId(), update.componentType().name(),
                "original component cannot be mapped");
    }
    private static WriteBackException failure(ComponentUpdate update, RuntimeException error) {
        return new WriteBackException(update.componentId(), update.componentType().name(),
                error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage(), error);
    }
    private static <T> List<T> collection(Collection<T> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
