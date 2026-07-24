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

package in.shrake.nifi.layout.core.graph;

import in.shrake.nifi.layout.core.exception.GraphValidationException;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public final class GraphBuilder {

    public LayoutGraph build(Collection<LayoutNode> nodes, Collection<LayoutEdge> edges, String processGroupId) {
        return build(nodes, edges, Map.of(), processGroupId);
    }

    public LayoutGraph build(Collection<LayoutNode> nodes, Collection<LayoutEdge> edges,
                             Map<String, LayoutGraph> subgraphs, String processGroupId) {
        Map<String, LayoutNode> nodeMap = new LinkedHashMap<>();
        for (LayoutNode node : nodes) {
            nodeMap.put(node.getId(), node);
        }

        Map<String, LayoutEdge> edgeMap = new LinkedHashMap<>();
        for (LayoutEdge edge : edges) {
            validateEndpoint(nodeMap, edge, edge.getSourceNodeId());
            validateEndpoint(nodeMap, edge, edge.getTargetNodeId());
            boolean selfLoop = edge.getSourceNodeId().equals(edge.getTargetNodeId());
            edgeMap.put(edge.getId(), selfLoop == edge.isSelfLoop() ? edge : new LayoutEdge(
                    edge.getId(), edge.getSourceNodeId(), edge.getTargetNodeId(),
                    edge.getSourcePort(), edge.getTargetPort(), edge.isReversed(), selfLoop));
        }

        return new LayoutGraph(nodeMap, edgeMap, new LinkedHashMap<>(subgraphs), processGroupId);
    }

    private static void validateEndpoint(Map<String, LayoutNode> nodes, LayoutEdge edge, String endpointId) {
        if (!nodes.containsKey(endpointId)) {
            throw new GraphValidationException(endpointId, edge.getId());
        }
    }
}
