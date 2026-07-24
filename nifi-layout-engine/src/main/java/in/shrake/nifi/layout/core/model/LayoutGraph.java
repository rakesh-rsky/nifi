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

package in.shrake.nifi.layout.core.model;

import in.shrake.nifi.layout.core.exception.GraphValidationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class LayoutGraph {
    private final Map<String, LayoutNode> nodes;
    private final Map<String, LayoutEdge> edges;
    private final Map<String, LayoutGraph> subgraphs;
    private final String processGroupId;

    private final Map<String, List<LayoutEdge>> outgoingEdges;
    private final Map<String, List<LayoutEdge>> incomingEdges;

    public LayoutGraph(Map<String, LayoutNode> nodes, Map<String, LayoutEdge> edges, Map<String, LayoutGraph> subgraphs, String processGroupId) {
        this.nodes = sortedCopy(nodes);
        this.edges = sortedCopy(edges);
        this.subgraphs = sortedCopy(subgraphs);
        this.processGroupId = processGroupId;

        Map<String, List<LayoutEdge>> out = new LinkedHashMap<>();
        Map<String, List<LayoutEdge>> in = new LinkedHashMap<>();
        for (String nodeId : this.nodes.keySet()) {
            out.put(nodeId, new ArrayList<>());
            in.put(nodeId, new ArrayList<>());
        }
        for (LayoutEdge edge : this.edges.values()) {
            if (!out.containsKey(edge.getSourceNodeId())) {
                throw new GraphValidationException(edge.getSourceNodeId(), edge.getId());
            }
            if (!in.containsKey(edge.getTargetNodeId())) {
                throw new GraphValidationException(edge.getTargetNodeId(), edge.getId());
            }
            out.get(edge.getSourceNodeId()).add(edge);
            in.get(edge.getTargetNodeId()).add(edge);
        }
        Map<String, List<LayoutEdge>> immutableOut = new LinkedHashMap<>();
        Map<String, List<LayoutEdge>> immutableIn = new LinkedHashMap<>();
        out.forEach((id, value) -> immutableOut.put(id, List.copyOf(value)));
        in.forEach((id, value) -> immutableIn.put(id, List.copyOf(value)));
        this.outgoingEdges = Collections.unmodifiableMap(immutableOut);
        this.incomingEdges = Collections.unmodifiableMap(immutableIn);
    }

    public Map<String, LayoutNode> getNodes() { return nodes; }
    public Map<String, LayoutEdge> getEdges() { return edges; }
    public Map<String, LayoutGraph> getSubgraphs() { return subgraphs; }
    public String getProcessGroupId() { return processGroupId; }

    public LayoutNode getNode(String id) { return nodes.get(id); }
    public LayoutEdge getEdge(String id) { return edges.get(id); }

    public int getNodeCount() { return nodes.size(); }
    public int getEdgeCount() { return edges.size(); }

    public List<LayoutEdge> getOutgoingEdges(String nodeId) {
        return outgoingEdges.getOrDefault(nodeId, Collections.emptyList());
    }

    public List<LayoutEdge> getIncomingEdges(String nodeId) {
        return incomingEdges.getOrDefault(nodeId, Collections.emptyList());
    }

    public List<LayoutNode> getAdjacent(String nodeId) {
        List<String> adjacentIds = new ArrayList<>();
        for (LayoutEdge edge : getOutgoingEdges(nodeId)) {
            if (!adjacentIds.contains(edge.getTargetNodeId())) {
                adjacentIds.add(edge.getTargetNodeId());
            }
        }
        for (LayoutEdge edge : getIncomingEdges(nodeId)) {
            if (!adjacentIds.contains(edge.getSourceNodeId())) {
                adjacentIds.add(edge.getSourceNodeId());
            }
        }
        adjacentIds.sort(String::compareTo);
        List<LayoutNode> adjacent = new ArrayList<>();
        for (String id : adjacentIds) {
            LayoutNode node = nodes.get(id);
            if (node != null) {
                adjacent.add(node);
            }
        }
        return adjacent;
    }

    private static <T> Map<String, T> sortedCopy(Map<String, T> source) {
        LinkedHashMap<String, T> sorted = new LinkedHashMap<>();
        source.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(sorted);
    }

    // Basic logic to satisfy getRoots and getTerminals as simple getters
    // A more precise definition might be provided by GraphAnalyzer later
    public List<LayoutNode> getRoots() {
        List<LayoutNode> roots = new ArrayList<>();
        for (LayoutNode node : nodes.values()) {
            if (getIncomingEdges(node.getId()).isEmpty()) {
                roots.add(node);
            }
        }
        return roots;
    }

    public List<LayoutNode> getTerminals() {
        List<LayoutNode> terminals = new ArrayList<>();
        for (LayoutNode node : nodes.values()) {
            if (getOutgoingEdges(node.getId()).isEmpty()) {
                terminals.add(node);
            }
        }
        return terminals;
    }
}
