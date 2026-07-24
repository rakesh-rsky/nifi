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

package in.shrake.nifi.layout.core.service.stage;

import in.shrake.nifi.layout.core.algorithm.CycleDetectionStrategy;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.service.PipelineContext;
import in.shrake.nifi.layout.core.service.PipelineStage;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Materializes deterministic cycle-breaking selections in the graph passed to layering. */
public final class CycleDetectionStage implements PipelineStage {
    private final CycleDetectionStrategy cycleDetection;

    public CycleDetectionStage(CycleDetectionStrategy cycleDetection) {
        this.cycleDetection = Objects.requireNonNull(cycleDetection);
    }

    @Override
    public String getName() {
        return "Cycle Detection";
    }

    @Override
    public PipelineContext execute(PipelineContext context) {
        LayoutGraph graph = context.getGraph();
        Set<String> reversedIds = new LinkedHashSet<>();
        for (LayoutEdge edge : cycleDetection.detectAndBreakCycles(graph)) {
            reversedIds.add(edge.getId());
        }
        if (reversedIds.isEmpty()) {
            return context.withMetadata("reversedEdgeIds", Set.of());
        }

        Map<String, LayoutEdge> edges = new LinkedHashMap<>();
        for (LayoutEdge edge : graph.getEdges().values()) {
            edges.put(edge.getId(), new LayoutEdge(edge.getId(), edge.getSourceNodeId(),
                    edge.getTargetNodeId(), edge.getSourcePort(), edge.getTargetPort(),
                    edge.isReversed() || reversedIds.contains(edge.getId()), edge.isSelfLoop()));
        }
        LayoutGraph acyclicGraph = new LayoutGraph(graph.getNodes(), edges,
                graph.getSubgraphs(), graph.getProcessGroupId());
        return context.withGraph(acyclicGraph).withMetadata("reversedEdgeIds", reversedIds);
    }
}
