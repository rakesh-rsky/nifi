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

package in.shrake.nifi.layout.core.router;

import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.RoutingResult;

import java.util.Objects;

/** Selects the concrete router from immutable layout options for each invocation. */
public final class ConfiguredRouter implements RoutingStrategy {
    private final RoutingStrategy orthogonal;
    private final RoutingStrategy direct;

    public ConfiguredRouter() {
        this(new OrthogonalRouter(), new DirectRouter());
    }

    public ConfiguredRouter(RoutingStrategy orthogonal, RoutingStrategy direct) {
        this.orthogonal = Objects.requireNonNull(orthogonal);
        this.direct = Objects.requireNonNull(direct);
    }

    @Override
    public RoutingResult computeRoutes(LayoutGraph graph, CoordinateAssignment coordinates,
                                       LayoutOptions options) {
        return switch (options.getRoutingMode()) {
            case DIRECT -> direct.computeRoutes(graph, coordinates, options);
            case ORTHOGONAL -> orthogonal.computeRoutes(graph, coordinates, options);
        };
    }
}
