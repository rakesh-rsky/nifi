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

package in.shrake.nifi.layout.core.service;

import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.RoutingResult;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class PipelineContext {
    private final LayoutGraph graph;
    private final LayoutOptions options;
    private final LayeredGraph layeredGraph;
    private final CoordinateAssignment coordinates;
    private final RoutingResult routing;
    private final Set<String> changedComponentIds;
    private final boolean incrementalMode;
    private final Set<String> affectedComponentIds;
    private final Map<String, Object> metadata;

    public PipelineContext(LayoutGraph graph, LayoutOptions options) {
        this(graph, options, null, null, null, Collections.emptySet(), false, Collections.emptySet(), Collections.emptyMap());
    }

    public PipelineContext(LayoutOptions options) {
        this(null, options, null, null, null, Collections.emptySet(), false, Collections.emptySet(), Collections.emptyMap());
    }

    private PipelineContext(
            LayoutGraph graph,
            LayoutOptions options,
            LayeredGraph layeredGraph,
            CoordinateAssignment coordinates,
            RoutingResult routing,
            Set<String> changedComponentIds,
            boolean incrementalMode,
            Set<String> affectedComponentIds,
            Map<String, Object> metadata
    ) {
        this.graph = graph;
        this.options = options;
        this.layeredGraph = layeredGraph;
        this.coordinates = coordinates;
        this.routing = routing;
        this.incrementalMode = incrementalMode;
        
        this.changedComponentIds = changedComponentIds == null 
            ? Collections.emptySet() 
            : Collections.unmodifiableSet(new LinkedHashSet<>(changedComponentIds));
            
        this.affectedComponentIds = affectedComponentIds == null
            ? Collections.emptySet()
            : Collections.unmodifiableSet(new LinkedHashSet<>(affectedComponentIds));
            
        this.metadata = metadata == null 
            ? Collections.emptyMap() 
            : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    public LayoutGraph getGraph() { return graph; }
    public LayoutOptions getOptions() { return options; }
    public LayeredGraph getLayeredGraph() { return layeredGraph; }
    public CoordinateAssignment getCoordinates() { return coordinates; }
    public RoutingResult getRouting() { return routing; }
    public Set<String> getChangedComponentIds() { return changedComponentIds; }
    public boolean isIncrementalMode() { return incrementalMode; }
    public Set<String> getAffectedComponentIds() { return affectedComponentIds; }
    public Map<String, Object> getMetadata() { return metadata; }

    public PipelineContext withGraph(LayoutGraph g) {
        return new PipelineContext(g, options, layeredGraph, coordinates, routing, changedComponentIds, incrementalMode, affectedComponentIds, metadata);
    }

    public PipelineContext withLayeredGraph(LayeredGraph lg) {
        return new PipelineContext(graph, options, lg, coordinates, routing, changedComponentIds, incrementalMode, affectedComponentIds, metadata);
    }

    public PipelineContext withCoordinates(CoordinateAssignment ca) {
        return new PipelineContext(graph, options, layeredGraph, ca, routing, changedComponentIds, incrementalMode, affectedComponentIds, metadata);
    }

    public PipelineContext withRouting(RoutingResult rr) {
        return new PipelineContext(graph, options, layeredGraph, coordinates, rr, changedComponentIds, incrementalMode, affectedComponentIds, metadata);
    }
    
    public PipelineContext withChangedComponentIds(Set<String> changedIds) {
        return new PipelineContext(graph, options, layeredGraph, coordinates, routing, changedIds, incrementalMode, affectedComponentIds, metadata);
    }
    
    public PipelineContext withIncrementalData(boolean isIncremental, Set<String> affectedIds) {
        return new PipelineContext(graph, options, layeredGraph, coordinates, routing, changedComponentIds, isIncremental, affectedIds, metadata);
    }
    
    public PipelineContext withMetadata(String key, Object value) {
        Map<String, Object> newMetadata = new LinkedHashMap<>(metadata);
        newMetadata.put(key, value);
        return new PipelineContext(graph, options, layeredGraph, coordinates, routing, changedComponentIds, incrementalMode, affectedComponentIds, newMetadata);
    }
}
