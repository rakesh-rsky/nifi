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

package in.shrake.nifi.layout.core.algorithm;

import in.shrake.nifi.layout.core.graph.GraphBuilder;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class ConnectedComponents {
    private ConnectedComponents() {}

    public static List<LayoutGraph> partition(LayoutGraph graph) {
        List<LayoutGraph> components = new ArrayList<>();
        Set<String> visited = new LinkedHashSet<>();
        
        List<LayoutNode> sortedNodes = new ArrayList<>(graph.getNodes().values());
        sortedNodes.sort(Comparator.comparing(LayoutNode::getId));
        
        for (LayoutNode node : sortedNodes) {
            if (!visited.contains(node.getId())) {
                Set<String> componentNodeIds = new LinkedHashSet<>();
                dfsUndirected(graph, node.getId(), visited, componentNodeIds);
                
                List<LayoutNode> cNodes = new ArrayList<>();
                for (String id : componentNodeIds) {
                    cNodes.add(graph.getNode(id));
                }
                
                List<LayoutEdge> cEdges = new ArrayList<>();
                for (LayoutEdge edge : graph.getEdges().values()) {
                    if (componentNodeIds.contains(edge.getSourceNodeId()) && componentNodeIds.contains(edge.getTargetNodeId())) {
                        cEdges.add(edge);
                    }
                }
                
                components.add(new GraphBuilder().build(cNodes, cEdges, graph.getProcessGroupId()));
            }
        }
        
        components.sort((g1, g2) -> {
            int cmp = Integer.compare(g2.getNodes().size(), g1.getNodes().size());
            if (cmp != 0) return cmp;
            
            String minId1 = g1.getNodes().keySet().stream().min(String::compareTo).orElse("");
            String minId2 = g2.getNodes().keySet().stream().min(String::compareTo).orElse("");
            return minId1.compareTo(minId2);
        });
        
        return components;
    }

    private static void dfsUndirected(LayoutGraph graph, String nodeId, Set<String> visited, Set<String> componentNodes) {
        visited.add(nodeId);
        componentNodes.add(nodeId);
        
        List<LayoutNode> adj = new ArrayList<>(graph.getAdjacent(nodeId));
        adj.sort(Comparator.comparing(LayoutNode::getId));
        
        for (LayoutNode neighbor : adj) {
            if (!visited.contains(neighbor.getId())) {
                dfsUndirected(graph, neighbor.getId(), visited, componentNodes);
            }
        }
    }
}
