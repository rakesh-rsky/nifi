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

package in.shrake.nifi.layout.support.writer;

import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.RoutingResult;

import java.util.List;
import java.util.Objects;

/** NiFi-neutral input shared by typed, map-based, and client-backed writers. */
public record LayoutWriteRequest(
        LayoutGraph graph,
        CoordinateAssignment coordinates,
        RoutingResult routing,
        long computationTimeMs,
        List<String> warnings) {

    public LayoutWriteRequest {
        Objects.requireNonNull(graph, "graph must not be null");
        Objects.requireNonNull(coordinates, "coordinates must not be null");
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
        if (computationTimeMs < 0) {
            throw new IllegalArgumentException("computationTimeMs must not be negative");
        }
    }
}
