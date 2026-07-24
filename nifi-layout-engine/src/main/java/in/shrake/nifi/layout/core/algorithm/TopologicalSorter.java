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

import in.shrake.nifi.layout.core.exception.CycleDetectionException;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public final class TopologicalSorter {
    private TopologicalSorter() {}

    public static List<LayoutNode> sort(LayoutGraph graph) {
        Map<String, Integer> inDegree = new LinkedHashMap<>();
        for (LayoutNode node : graph.getNodes().values()) {
            inDegree.put(node.getId(), 0);
        }
        
        for (LayoutEdge edge : graph.getEdges().values()) {
            if (edge.isSelfLoop() || edge.isReversed()) continue;
            inDegree.put(edge.getTargetNodeId(), inDegree.getOrDefault(edge.getTargetNodeId(), 0) + 1);
        }
        
        PriorityQueue<LayoutNode> zeroInDegree = new PriorityQueue<>(Comparator.comparing(LayoutNode::getId));
        for (LayoutNode node : graph.getNodes().values()) {
            if (inDegree.get(node.getId()) == 0) {
                zeroInDegree.add(node);
            }
        }
        
        List<LayoutNode> result = new ArrayList<>();
        while (!zeroInDegree.isEmpty()) {
            LayoutNode node = zeroInDegree.poll();
            result.add(node);
            
            for (LayoutEdge edge : graph.getOutgoingEdges(node.getId())) {
                if (edge.isSelfLoop() || edge.isReversed()) continue;
                
                String targetId = edge.getTargetNodeId();
                int currentInDegree = inDegree.get(targetId) - 1;
                inDegree.put(targetId, currentInDegree);
                
                if (currentInDegree == 0) {
                    zeroInDegree.add(graph.getNode(targetId));
                }
            }
        }
        
        if (result.size() != graph.getNodes().size()) {
            throw new CycleDetectionException("Cycle detected: Result size " + result.size() + " != Node count " + graph.getNodes().size());
        }
        
        return result;
    }
}
