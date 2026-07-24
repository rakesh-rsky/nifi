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

package in.shrake.nifi.layout.support.canonical;

import in.shrake.nifi.layout.core.exception.MaxNestingDepthException;
import in.shrake.nifi.layout.core.graph.GraphBuilder;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.NodeType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts NiFi-neutral canonical snapshots into validated, deterministic core graphs. */
public final class CanonicalGraphAssembler {
    private static final int MAX_DEPTH = 10;

    public LayoutGraph assemble(CanonicalFlowSnapshot snapshot) {
        return assembleGroup(snapshot.rootGroup(), 0);
    }

    private LayoutGraph assembleGroup(CanonicalProcessGroup group, int depth) {
        if (depth > MAX_DEPTH) throw new MaxNestingDepthException(depth, group.id());
        List<LayoutNode> nodes = new ArrayList<>();
        group.components().stream().sorted(Comparator.comparing(CanonicalComponent::id))
                .map(this::toNode).forEach(nodes::add);

        Map<String, LayoutGraph> subgraphs = new LinkedHashMap<>();
        group.childGroups().stream().sorted(Comparator.comparing(CanonicalProcessGroup::id)).forEach(child -> {
            nodes.add(new LayoutNode(child.id(), NodeType.PROCESS_GROUP, child.bounds(), child.attributes(), group.id()));
            subgraphs.put(child.id(), assembleGroup(child, depth + 1));
        });
        nodes.sort(Comparator.comparing(LayoutNode::getId));

        List<LayoutEdge> edges = group.connections().stream()
                .sorted(Comparator.comparing(CanonicalConnection::id)).map(this::toEdge).toList();
        return new GraphBuilder().build(nodes, edges, subgraphs, group.id());
    }

    private LayoutNode toNode(CanonicalComponent component) {
        return new LayoutNode(component.id(), component.type(), component.bounds(),
                component.attributes(), component.parentGroupId());
    }

    private LayoutEdge toEdge(CanonicalConnection connection) {
        return new LayoutEdge(connection.id(), connection.sourceId(), connection.targetId(),
                RelationshipCodec.encode(connection.relationships()), connection.targetPort(), false,
                connection.sourceId().equals(connection.targetId()));
    }
}
