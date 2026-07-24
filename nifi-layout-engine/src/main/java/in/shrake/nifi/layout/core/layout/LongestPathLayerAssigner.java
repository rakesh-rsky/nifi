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

package in.shrake.nifi.layout.core.layout;

import in.shrake.nifi.layout.core.algorithm.TopologicalSorter;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.NodeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LongestPathLayerAssigner implements LayerAssignmentStrategy {

    @Override
    public LayeredGraph assignLayers(LayoutGraph graph, LayoutOptions options) {
        if (graph.getNodes().isEmpty()) {
            return new LayeredGraph(graph, Collections.emptyList(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
        }

        List<LayoutNode> sortedNodes = TopologicalSorter.sort(graph);
        Map<String, Integer> nodeToLayer = new LinkedHashMap<>();

        int maxLayer = 0;
        for (LayoutNode node : sortedNodes) {
            int layer = 0;
            for (LayoutEdge edge : graph.getIncomingEdges(node.getId())) {
                if (edge.isSelfLoop() || edge.isReversed()) continue;
                Integer sourceLayer = nodeToLayer.get(edge.getSourceNodeId());
                if (sourceLayer != null) {
                    layer = Math.max(layer, sourceLayer + 1);
                }
            }
            nodeToLayer.put(node.getId(), layer);
            maxLayer = Math.max(maxLayer, layer);
        }

        for (LayoutNode node : sortedNodes) {
            boolean hasOutgoing = false;
            for (LayoutEdge edge : graph.getOutgoingEdges(node.getId())) {
                if (!edge.isSelfLoop() && !edge.isReversed()) {
                    hasOutgoing = true;
                    break;
                }
            }
            if (!hasOutgoing) {
                nodeToLayer.put(node.getId(), maxLayer);
            }
        }

        List<LayoutNode> virtualNodes = new ArrayList<>();
        Map<String, LayoutNode> finalNodes = new LinkedHashMap<>(graph.getNodes());
        
        for (LayoutEdge edge : graph.getEdges().values()) {
            if (edge.isSelfLoop() || edge.isReversed()) continue;
            
            int sourceLayer = nodeToLayer.get(edge.getSourceNodeId());
            int targetLayer = nodeToLayer.get(edge.getTargetNodeId());
            
            if (targetLayer - sourceLayer > 1) {
                for (int l = sourceLayer + 1; l < targetLayer; l++) {
                    String vNodeId = edge.getId() + "_v" + l;
                    LayoutNode vNode = new LayoutNode(vNodeId, NodeType.VIRTUAL, new BoundingBox(0, 0, 1, 1), Collections.emptyMap(), graph.getProcessGroupId());
                    virtualNodes.add(vNode);
                    nodeToLayer.put(vNodeId, l);
                }
            }
        }
        
        Map<Integer, List<LayoutNode>> layerMap = new TreeMap<>();
        for (Map.Entry<String, Integer> entry : nodeToLayer.entrySet()) {
            LayoutNode n = finalNodes.get(entry.getKey());
            if (n == null) {
                n = virtualNodes.stream().filter(v -> v.getId().equals(entry.getKey())).findFirst().orElse(null);
            }
            if (n != null) {
                layerMap.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(n);
            }
        }
        
        List<List<LayoutNode>> layers = new ArrayList<>();
        Map<String, Integer> compactedNodeToLayer = new LinkedHashMap<>();
        int currentLayerIndex = 0;
        for (List<LayoutNode> layerNodes : layerMap.values()) {
            if (layerNodes.isEmpty()) continue;
            layers.add(layerNodes);
            for (LayoutNode n : layerNodes) {
                compactedNodeToLayer.put(n.getId(), currentLayerIndex);
            }
            currentLayerIndex++;
        }
        
        List<LayoutEdge> reversedEdges = new ArrayList<>();
        for (LayoutEdge edge : graph.getEdges().values()) {
            if (edge.isReversed()) {
                reversedEdges.add(edge);
            }
        }
        
        return new LayeredGraph(graph, layers, compactedNodeToLayer, virtualNodes, reversedEdges);
    }
}
