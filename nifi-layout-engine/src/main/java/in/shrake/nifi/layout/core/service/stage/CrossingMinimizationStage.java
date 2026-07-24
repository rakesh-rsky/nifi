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

import in.shrake.nifi.layout.core.layout.CrossingMinimizationStrategy;
import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.service.PipelineContext;
import in.shrake.nifi.layout.core.service.PipelineStage;

import java.util.Objects;

/** Applies the configured crossing minimizer after layer assignment. */
public final class CrossingMinimizationStage implements PipelineStage {
    private final CrossingMinimizationStrategy median;
    private final CrossingMinimizationStrategy barycenter;

    public CrossingMinimizationStage(CrossingMinimizationStrategy median,
                                     CrossingMinimizationStrategy barycenter) {
        this.median = Objects.requireNonNull(median);
        this.barycenter = Objects.requireNonNull(barycenter);
    }

    @Override
    public String getName() {
        return "Crossing Minimization";
    }

    @Override
    public PipelineContext execute(PipelineContext context) {
        CrossingMinimizationStrategy strategy = switch (context.getOptions().getCrossingStrategy()) {
            case BARYCENTER -> barycenter;
            case MEDIAN -> median;
        };
        LayeredGraph minimized = strategy.minimize(context.getLayeredGraph(), context.getOptions());
        return context.withLayeredGraph(minimized);
    }
}
