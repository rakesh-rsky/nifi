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

import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class GraphAnalyzer {
    
    private GraphAnalyzer() {}
    
    public static List<LayoutNode> detectRoots(LayoutGraph graph) {
        List<LayoutNode> roots = new ArrayList<>();
        for (LayoutNode node : graph.getNodes().values()) {
            if (node.getType() != NodeType.PROCESSOR) continue;
            
            boolean hasIncomingProcessor = false;
            for (LayoutEdge edge : graph.getIncomingEdges(node.getId())) {
                LayoutNode source = graph.getNode(edge.getSourceNodeId());
                if (source != null && source.getType() == NodeType.PROCESSOR) {
                    hasIncomingProcessor = true;
                    break;
                }
            }
            if (!hasIncomingProcessor) {
                roots.add(node);
            }
        }
        return roots;
    }
    
    public static List<LayoutNode> detectTerminals(LayoutGraph graph) {
        List<LayoutNode> terminals = new ArrayList<>();
        for (LayoutNode node : graph.getNodes().values()) {
            if (node.getType() != NodeType.PROCESSOR) continue;
            
            boolean hasOutgoingProcessor = false;
            for (LayoutEdge edge : graph.getOutgoingEdges(node.getId())) {
                LayoutNode target = graph.getNode(edge.getTargetNodeId());
                if (target != null && target.getType() == NodeType.PROCESSOR) {
                    hasOutgoingProcessor = true;
                    break;
                }
            }
            if (!hasOutgoingProcessor) {
                terminals.add(node);
            }
        }
        return terminals;
    }
    
    public static FlowDirection detectFlowDirection(LayoutGraph graph) {
        int right = 0;
        int left = 0;
        int down = 0;
        int up = 0;
        
        for (LayoutEdge edge : graph.getEdges().values()) {
            LayoutNode source = graph.getNode(edge.getSourceNodeId());
            LayoutNode target = graph.getNode(edge.getTargetNodeId());
            
            if (source == null || target == null) continue;
            
            Position pos1 = source.getBoundingBox().center();
            Position pos2 = target.getBoundingBox().center();
            
            int dx = pos2.x() - pos1.x();
            int dy = pos2.y() - pos1.y();
            
            if (Math.abs(dx) > Math.abs(dy)) {
                if (dx > 0) right++;
                else if (dx < 0) left++;
            } else {
                if (dy > 0) down++;
                else if (dy < 0) up++;
            }
        }
        
        int max = Math.max(Math.max(right, left), Math.max(down, up));
        if (max == down && down > 0) return FlowDirection.TOP_TO_BOTTOM;
        if (max == right && right > 0) return FlowDirection.LEFT_TO_RIGHT;
        if (max == up && up > 0) return FlowDirection.BOTTOM_TO_TOP;
        if (max == left && left > 0) return FlowDirection.RIGHT_TO_LEFT;
        
        return FlowDirection.TOP_TO_BOTTOM;
    }
    
    public static List<LayoutGraph> findConnectedComponents(LayoutGraph graph) {
        List<LayoutGraph> components = new ArrayList<>();
        Set<String> visited = new LinkedHashSet<>();
        
        for (LayoutNode node : graph.getNodes().values()) {
            if (!visited.contains(node.getId())) {
                Set<String> componentNodes = new LinkedHashSet<>();
                dfs(graph, node.getId(), visited, componentNodes);
                
                List<LayoutNode> cNodes = new ArrayList<>();
                for (String id : componentNodes) {
                    cNodes.add(graph.getNode(id));
                }
                
                List<LayoutEdge> cEdges = new ArrayList<>();
                for (LayoutEdge edge : graph.getEdges().values()) {
                    if (componentNodes.contains(edge.getSourceNodeId()) && componentNodes.contains(edge.getTargetNodeId())) {
                        cEdges.add(edge);
                    }
                }
                
                components.add(new GraphBuilder().build(cNodes, cEdges, graph.getProcessGroupId()));
            }
        }
        return components;
    }
    
    private static void dfs(LayoutGraph graph, String nodeId, Set<String> visited, Set<String> componentNodes) {
        visited.add(nodeId);
        componentNodes.add(nodeId);
        
        for (LayoutNode adj : graph.getAdjacent(nodeId)) {
            if (!visited.contains(adj.getId())) {
                dfs(graph, adj.getId(), visited, componentNodes);
            }
        }
    }
    
    public static Set<String> computeAffectedSet(LayoutGraph graph, Set<String> changedIds) {
        Set<String> affected = new LinkedHashSet<>(changedIds);
        for (String id : changedIds) {
            LayoutNode node = graph.getNode(id);
            if (node != null) {
                for (LayoutNode adj : graph.getAdjacent(id)) {
                    affected.add(adj.getId());
                }
            }
        }
        return affected;
    }
}
