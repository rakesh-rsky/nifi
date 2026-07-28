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

import in.shrake.nifi.layout.core.fixture.FlowTopologyFixtures;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.core.model.RoutingWarning;
import in.shrake.nifi.layout.core.router.RoutingStrategy;
import in.shrake.nifi.layout.core.service.PipelineContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingStageTest {

    @Test
    void incrementalModeFiltersWarningsToAffectedEdges() {
        LayoutGraph graph = FlowTopologyFixtures.disconnectedSubgraphs();
        PipelineContext context = new PipelineContext(graph, LayoutOptions.defaults())
                .withIncrementalData(true, Set.of("a"));

        RoutingResult result = new RoutingStage(warningRoutingStrategy())
                .execute(context)
                .getRouting();

        assertEquals(Set.of("a-b"), result.getEdgePaths().keySet());
        assertEquals(
                List.of("a-b"),
                result.getWarnings().stream().map(RoutingWarning::edgeId).toList());
        assertTrue(result.getWarnings().stream()
                .allMatch(warning -> result.getEdgePaths().containsKey(warning.edgeId())));
    }

    @Test
    void fullModeRetainsAllWarnings() {
        LayoutGraph graph = FlowTopologyFixtures.disconnectedSubgraphs();
        PipelineContext context = new PipelineContext(graph, LayoutOptions.defaults());

        RoutingResult result = new RoutingStage(warningRoutingStrategy())
                .execute(context)
                .getRouting();

        assertEquals(Set.of("a-b", "c-d"), result.getEdgePaths().keySet());
        assertEquals(
                List.of("a-b", "c-d"),
                result.getWarnings().stream().map(RoutingWarning::edgeId).toList());
    }

    private static RoutingStrategy warningRoutingStrategy() {
        return (graph, coordinates, options) -> {
            Map<String, List<Position>> routes = new LinkedHashMap<>();
            List<RoutingWarning> warnings = new ArrayList<>();
            graph.getEdges().values().forEach(edge -> {
                routes.put(edge.getId(), List.of(
                        new Position(0, 0),
                        new Position(100, 100)));
                warnings.add(new RoutingWarning(edge.getId(), "blocked"));
            });
            return new RoutingResult(routes, warnings);
        };
    }
}
