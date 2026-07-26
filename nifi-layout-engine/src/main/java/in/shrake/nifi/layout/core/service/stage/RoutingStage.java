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

import in.shrake.nifi.layout.core.service.PipelineStage;
import in.shrake.nifi.layout.core.service.PipelineContext;
import in.shrake.nifi.layout.core.router.RoutingStrategy;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.LayoutEdge;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;

public class RoutingStage implements PipelineStage {
    private final RoutingStrategy strategy;
    
    public RoutingStage(RoutingStrategy strategy) {
        this.strategy = strategy;
    }
    
    @Override
    public String getName() {
        return "Connection Routing";
    }
    
    @Override
    public PipelineContext execute(PipelineContext context) {
        RoutingResult fullRouting = strategy.computeRoutes(context.getGraph(), context.getCoordinates(), context.getOptions());
        
        if (context.isIncrementalMode() && context.getAffectedComponentIds() != null && !context.getAffectedComponentIds().isEmpty()) {
            Map<String, List<Position>> filtered = new LinkedHashMap<>();

            for (Map.Entry<String, List<Position>> entry : fullRouting.getEdgePaths().entrySet()) {
                LayoutEdge edge = context.getGraph().getEdges().get(entry.getKey());
                if (edge != null) {
                    if (context.getAffectedComponentIds().contains(edge.getSourceNodeId()) ||
                        context.getAffectedComponentIds().contains(edge.getTargetNodeId())) {
                        filtered.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            return context.withRouting(new RoutingResult(
                    filtered, fullRouting.getWarnings()));
        }
        
        return context.withRouting(fullRouting);
    }
}
