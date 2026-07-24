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

import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MedianCrossingMinimizer implements CrossingMinimizationStrategy {

    @Override
    public LayeredGraph minimize(LayeredGraph layeredGraph, LayoutOptions options) {
        if (layeredGraph.getLayers().isEmpty()) {
            return layeredGraph;
        }
        
        Map<String, List<String>> upNeighbors = new LinkedHashMap<>();
        Map<String, List<String>> downNeighbors = new LinkedHashMap<>();
        
        LayoutGraph originalGraph = layeredGraph.getOriginalGraph();
        for (LayoutEdge edge : originalGraph.getEdges().values()) {
            if (edge.isSelfLoop() || edge.isReversed()) continue;
            
            String source = edge.getSourceNodeId();
            String target = edge.getTargetNodeId();
            
            int sourceLayer = layeredGraph.getNodeToLayer().get(source);
            int targetLayer = layeredGraph.getNodeToLayer().get(target);
            
            if (targetLayer - sourceLayer > 1) {
                String current = source;
                for (int l = sourceLayer + 1; l < targetLayer; l++) {
                    String vNode = edge.getId() + "_v" + l;
                    upNeighbors.computeIfAbsent(vNode, k -> new ArrayList<>()).add(current);
                    downNeighbors.computeIfAbsent(current, k -> new ArrayList<>()).add(vNode);
                    current = vNode;
                }
                upNeighbors.computeIfAbsent(target, k -> new ArrayList<>()).add(current);
                downNeighbors.computeIfAbsent(current, k -> new ArrayList<>()).add(target);
            } else if (targetLayer - sourceLayer == 1) {
                upNeighbors.computeIfAbsent(target, k -> new ArrayList<>()).add(source);
                downNeighbors.computeIfAbsent(source, k -> new ArrayList<>()).add(target);
            }
        }
        
        List<List<LayoutNode>> currentLayers = new ArrayList<>();
        for (List<LayoutNode> layer : layeredGraph.getLayers()) {
            currentLayers.add(new ArrayList<>(layer));
        }
        
        int bestCrossings = countCrossings(currentLayers, downNeighbors);
        List<List<LayoutNode>> bestLayers = copyLayers(currentLayers);
        
        int maxIterations = options.getMaxIterations();
        int unchangedSweeps = 0;
        
        for (int i = 0; i < maxIterations; i++) {
            for (int l = 1; l < currentLayers.size(); l++) {
                sweepLayer(currentLayers.get(l), currentLayers.get(l-1), upNeighbors);
            }
            
            for (int l = currentLayers.size() - 2; l >= 0; l--) {
                sweepLayer(currentLayers.get(l), currentLayers.get(l+1), downNeighbors);
            }
            
            int crossings = countCrossings(currentLayers, downNeighbors);
            if (crossings < bestCrossings) {
                bestCrossings = crossings;
                bestLayers = copyLayers(currentLayers);
                unchangedSweeps = 0;
            } else {
                unchangedSweeps++;
                if (unchangedSweeps >= 2) {
                    break;
                }
            }
        }
        
        return new LayeredGraph(layeredGraph.getOriginalGraph(), bestLayers, layeredGraph.getNodeToLayer(), layeredGraph.getVirtualNodes(), layeredGraph.getReversedEdges());
    }
    
    private void sweepLayer(List<LayoutNode> targetLayer, List<LayoutNode> sourceLayer, Map<String, List<String>> neighborMap) {
        Map<String, Integer> positionMap = new LinkedHashMap<>();
        for (int i = 0; i < sourceLayer.size(); i++) {
            positionMap.put(sourceLayer.get(i).getId(), i);
        }
        
        Map<String, Double> medians = new LinkedHashMap<>();
        for (LayoutNode node : targetLayer) {
            List<String> neighbors = neighborMap.getOrDefault(node.getId(), new ArrayList<>());
            List<Integer> positions = new ArrayList<>();
            for (String n : neighbors) {
                Integer pos = positionMap.get(n);
                if (pos != null) {
                    positions.add(pos);
                }
            }
            
            if (positions.isEmpty()) {
                medians.put(node.getId(), -1.0);
            } else {
                positions.sort(Integer::compareTo);
                int size = positions.size();
                if (size % 2 == 1) {
                    medians.put(node.getId(), (double) positions.get(size / 2));
                } else {
                    double m1 = positions.get(size / 2 - 1);
                    double m2 = positions.get(size / 2);
                    medians.put(node.getId(), (m1 + m2) / 2.0);
                }
            }
        }
        
        targetLayer.sort((n1, n2) -> {
            double m1 = medians.get(n1.getId());
            double m2 = medians.get(n2.getId());
            if (m1 == -1.0 && m2 == -1.0) return 0;
            if (m1 == -1.0) return 0; 
            if (Double.compare(m1, m2) == 0) return 0;
            return Double.compare(m1, m2);
        });
    }
    
    private int countCrossings(List<List<LayoutNode>> layers, Map<String, List<String>> downNeighbors) {
        return CrossingCountComputer.countCrossings(layers, downNeighbors);
    }
    
    private List<List<LayoutNode>> copyLayers(List<List<LayoutNode>> original) {
        List<List<LayoutNode>> copy = new ArrayList<>();
        for (List<LayoutNode> layer : original) {
            copy.add(new ArrayList<>(layer));
        }
        return copy;
    }
}
