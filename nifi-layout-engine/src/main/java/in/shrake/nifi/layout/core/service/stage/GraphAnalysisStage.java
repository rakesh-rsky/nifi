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

import in.shrake.nifi.layout.core.graph.GraphAnalysisStrategy;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.service.PipelineContext;
import in.shrake.nifi.layout.core.service.PipelineStage;

import java.util.Objects;

/** Computes deterministic graph facts consumed by diagnostics and downstream stages. */
public final class GraphAnalysisStage implements PipelineStage {
    private final GraphAnalysisStrategy graphAnalysis;

    public GraphAnalysisStage(GraphAnalysisStrategy graphAnalysis) {
        this.graphAnalysis = Objects.requireNonNull(graphAnalysis);
    }

    @Override
    public String getName() {
        return "Detect Components";
    }

    @Override
    public PipelineContext execute(PipelineContext context) {
        LayoutGraph graph = context.getGraph();
        return context
                .withMetadata("roots", graphAnalysis.detectRoots(graph))
                .withMetadata("terminals", graphAnalysis.detectTerminals(graph))
                .withMetadata("flowDirection", graphAnalysis.detectFlowDirection(graph))
                .withMetadata("connectedComponents", graphAnalysis.findConnectedComponents(graph));
    }
}
