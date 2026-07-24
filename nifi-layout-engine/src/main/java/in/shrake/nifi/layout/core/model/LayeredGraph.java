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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class LayeredGraph {
    private final LayoutGraph originalGraph;
    private final List<List<LayoutNode>> layers;
    private final Map<String, Integer> nodeToLayer;
    private final List<LayoutNode> virtualNodes;
    private final List<LayoutEdge> reversedEdges;

    public LayeredGraph(LayoutGraph originalGraph, List<List<LayoutNode>> layers, Map<String, Integer> nodeToLayer, List<LayoutNode> virtualNodes, List<LayoutEdge> reversedEdges) {
        this.originalGraph = originalGraph;
        List<List<LayoutNode>> layerCopies = new ArrayList<>();
        for (List<LayoutNode> layer : layers) {
            layerCopies.add(List.copyOf(layer));
        }
        this.layers = Collections.unmodifiableList(layerCopies);
        this.nodeToLayer = Collections.unmodifiableMap(new LinkedHashMap<>(nodeToLayer));
        this.virtualNodes = List.copyOf(virtualNodes);
        this.reversedEdges = List.copyOf(reversedEdges);
    }

    public LayoutGraph getOriginalGraph() { return originalGraph; }
    public List<List<LayoutNode>> getLayers() { return layers; }
    public Map<String, Integer> getNodeToLayer() { return nodeToLayer; }
    public List<LayoutNode> getVirtualNodes() { return virtualNodes; }
    public List<LayoutEdge> getReversedEdges() { return reversedEdges; }
}
