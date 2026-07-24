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

import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public final class TarjanCycleDetector {
    private TarjanCycleDetector() {}

    private static class TarjanState {
        int index = 0;
        Map<String, Integer> indices = new LinkedHashMap<>();
        Map<String, Integer> lowLinks = new LinkedHashMap<>();
        Stack<String> stack = new Stack<>();
        Map<String, Boolean> onStack = new LinkedHashMap<>();
        List<List<String>> sccs = new ArrayList<>();
    }

    public static List<LayoutEdge> detectAndBreakCycles(LayoutGraph graph) {
        List<LayoutEdge> allReversed = new ArrayList<>();
        boolean cycleFound = true;
        
        while (cycleFound) {
            cycleFound = false;
            TarjanState state = new TarjanState();
            
            List<LayoutNode> nodes = new ArrayList<>(graph.getNodes().values());
            nodes.sort(Comparator.comparing(LayoutNode::getId));
            
            for (LayoutNode node : nodes) {
                if (!state.indices.containsKey(node.getId())) {
                    strongConnect(node.getId(), graph, state, allReversed);
                }
            }
            
            for (List<String> scc : state.sccs) {
                if (scc.size() > 1) {
                    cycleFound = true;
                    List<LayoutEdge> internalEdges = new ArrayList<>();
                    for (String src : scc) {
                        for (LayoutEdge edge : graph.getOutgoingEdges(src)) {
                            if (scc.contains(edge.getTargetNodeId()) && !allReversed.contains(edge) && !edge.isSelfLoop() && !edge.isReversed()) {
                                internalEdges.add(edge);
                            }
                        }
                    }
                    
                    internalEdges.sort((e1, e2) -> {
                        int span1 = Math.abs(state.indices.get(e1.getSourceNodeId()) - state.indices.get(e1.getTargetNodeId()));
                        int span2 = Math.abs(state.indices.get(e2.getSourceNodeId()) - state.indices.get(e2.getTargetNodeId()));
                        if (span1 != span2) {
                            return Integer.compare(span2, span1); 
                        }
                        return e1.getId().compareTo(e2.getId()); 
                    });
                    
                    if (!internalEdges.isEmpty()) {
                        allReversed.add(internalEdges.get(0));
                    }
                }
            }
        }
        
        return allReversed;
    }
    
    private static void strongConnect(String v, LayoutGraph graph, TarjanState state, List<LayoutEdge> allReversed) {
        state.indices.put(v, state.index);
        state.lowLinks.put(v, state.index);
        state.index++;
        state.stack.push(v);
        state.onStack.put(v, true);
        
        List<LayoutEdge> edges = new ArrayList<>(graph.getOutgoingEdges(v));
        edges.sort(Comparator.comparing(LayoutEdge::getId));
        
        for (LayoutEdge edge : edges) {
            if (edge.isSelfLoop() || edge.isReversed() || allReversed.contains(edge)) continue;
            
            String w = edge.getTargetNodeId();
            if (!state.indices.containsKey(w)) {
                strongConnect(w, graph, state, allReversed);
                state.lowLinks.put(v, Math.min(state.lowLinks.get(v), state.lowLinks.get(w)));
            } else if (state.onStack.getOrDefault(w, false)) {
                state.lowLinks.put(v, Math.min(state.lowLinks.get(v), state.indices.get(w)));
            }
        }
        
        if (state.lowLinks.get(v).equals(state.indices.get(v))) {
            List<String> scc = new ArrayList<>();
            String w;
            do {
                w = state.stack.pop();
                state.onStack.put(w, false);
                scc.add(w);
            } while (!v.equals(w));
            state.sccs.add(scc);
        }
    }
}
