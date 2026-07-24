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

package in.shrake.nifi.layout.support;

import in.shrake.nifi.layout.core.LayoutEngine;
import in.shrake.nifi.layout.support.writer.FlowLayoutWriter;
import in.shrake.nifi.layout.support.writer.LayoutWriteRequest;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.core.service.LayoutProgressCallback;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * NiFi-neutral adapter-to-core-to-writer orchestration boundary.
 *
 * <p>{@link #layout} always performs a full layout regardless of
 * {@link LayoutOptions#isIncrementalMode()}.  Use {@link #layoutIncremental}
 * to perform an incremental layout for a known set of changed component IDs.
 */
public final class FlowLayoutService<T> {
    private final FlowGraphAdapter<T> adapter;
    private final FlowLayoutWriter<T> writer;
    private final LayoutEngine engine;

    public FlowLayoutService(FlowGraphAdapter<T> adapter, FlowLayoutWriter<T> writer) {
        this(adapter, writer, LayoutEngine.create());
    }

    public FlowLayoutService(FlowGraphAdapter<T> adapter, FlowLayoutWriter<T> writer,
                             LayoutEngine engine) {
        this.adapter = Objects.requireNonNull(adapter);
        this.writer = Objects.requireNonNull(writer);
        this.engine = Objects.requireNonNull(engine);
    }

    public LayoutResult layout(T flow, LayoutOptions options) {
        return layout(flow, options, null);
    }

    public LayoutResult layout(T flow, LayoutOptions options, LayoutProgressCallback callback) {
        LayoutGraph graph = adapter.parse(flow);
        LayoutResult coreResult = engine.layout(graph, options, callback);
        return writeResult(flow, graph, coreResult);
    }

    /**
     * Runs incremental layout for the supplied set of changed component IDs and
     * writes the result back to the flow representation.  This method always
     * performs incremental layout regardless of {@link LayoutOptions#isIncrementalMode()}.
     */
    public LayoutResult layoutIncremental(T flow, LayoutOptions options, Set<String> changedComponentIds) {
        LayoutGraph graph = adapter.parse(flow);
        LayoutResult coreResult = engine.layoutIncremental(graph, options, changedComponentIds);
        return writeResult(flow, graph, coreResult);
    }

    private LayoutResult writeResult(T flow, LayoutGraph graph, LayoutResult coreResult) {
        Map<String, Position> positions = new LinkedHashMap<>();
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        for (ComponentUpdate update : coreResult.getUpdates()) {
            positions.put(update.componentId(), update.newPosition());
            bounds.put(update.componentId(), update.newBounds());
        }
        LayoutWriteRequest request = new LayoutWriteRequest(graph,
                new CoordinateAssignment(positions, bounds),
                new RoutingResult(coreResult.getConnectionBendPoints()),
                coreResult.getComputationTimeMs(), coreResult.getWarnings());
        return writer.write(flow, request);
    }
}
