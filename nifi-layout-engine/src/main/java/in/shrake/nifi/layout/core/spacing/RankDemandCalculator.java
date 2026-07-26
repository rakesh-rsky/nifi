/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.spacing;

import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.NodeType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RankDemandCalculator {

    public List<RankDemand> calculate(final LayeredGraph graph) {
        final List<RankDemand> demands = new ArrayList<>();
        for (int boundary = 0; boundary < graph.getLayers().size() - 1; boundary++) {
            demands.add(calculateBoundary(graph, boundary));
        }
        return List.copyOf(demands);
    }

    private RankDemand calculateBoundary(final LayeredGraph graph, final int boundary) {
        final Map<String, Integer> outgoing = new LinkedHashMap<>();
        final Map<String, Integer> incoming = new LinkedHashMap<>();
        final Map<String, Integer> parallel = new LinkedHashMap<>();
        int longEdgeCount = 0;

        for (LayoutEdge edge : graph.getOriginalGraph().getEdges().values()) {
            if (edge.isSelfLoop() || edge.isReversed()) {
                continue;
            }
            final Integer sourceLayer = graph.getNodeToLayer().get(edge.getSourceNodeId());
            final Integer targetLayer = graph.getNodeToLayer().get(edge.getTargetNodeId());
            if (sourceLayer == null || targetLayer == null || targetLayer <= sourceLayer) {
                continue;
            }
            if (sourceLayer == boundary) {
                outgoing.merge(edge.getSourceNodeId(), 1, Integer::sum);
            }
            if (targetLayer == boundary + 1) {
                incoming.merge(edge.getTargetNodeId(), 1, Integer::sum);
            }
            if (sourceLayer <= boundary && targetLayer > boundary) {
                parallel.merge(edge.getSourceNodeId() + '\0' + edge.getTargetNodeId(),
                        1, Integer::sum);
                if (targetLayer - sourceLayer > 1) {
                    longEdgeCount++;
                }
            }
        }

        final int denseBusCount = (int) outgoing.values().stream().filter(value -> value >= 3).count()
                + (int) incoming.values().stream().filter(value -> value >= 3).count();
        final List<LayoutNode> destinationLayer = graph.getLayers().get(boundary + 1);
        final boolean labelPresent = destinationLayer.stream()
                .anyMatch(node -> node.getType() == NodeType.LABEL);
        final boolean portPresent = destinationLayer.stream()
                .anyMatch(node -> node.getType() == NodeType.PORT_INPUT
                        || node.getType() == NodeType.PORT_OUTPUT);

        return new RankDemand(
                boundary,
                maximum(outgoing),
                maximum(incoming),
                maximum(parallel),
                denseBusCount,
                longEdgeCount,
                labelPresent,
                portPresent);
    }

    private int maximum(final Map<String, Integer> counts) {
        return counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    }
}
