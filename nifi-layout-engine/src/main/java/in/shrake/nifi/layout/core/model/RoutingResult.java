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

package in.shrake.nifi.layout.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Routing result holding the full endpoint-to-endpoint path for each edge.
 *
 * <p>Every path starts with the source-component exit anchor and ends with the
 * target-component entry anchor.  Interior points between the first and last
 * are bend candidates.  Writers that persist NiFi connection bends must exclude
 * the first and last points from each path so that only interior bend points
 * are stored.
 *
 * <p>Warnings are edge-scoped messages emitted when no fully clear route was
 * found.  The single-argument constructor preserves backward compatibility.
 */
public final class RoutingResult {
    private final Map<String, List<Position>> edgePaths;
    private final List<RoutingWarning> warnings;

    /** Backward-compatible constructor: no warnings. */
    public RoutingResult(Map<String, List<Position>> edgePaths) {
        this(edgePaths, List.of());
    }

    public RoutingResult(Map<String, List<Position>> edgePaths, List<RoutingWarning> warnings) {
        LinkedHashMap<String, List<Position>> copy = new LinkedHashMap<>();
        edgePaths.forEach((edgeId, path) -> copy.put(edgeId, List.copyOf(path)));
        this.edgePaths = Collections.unmodifiableMap(copy);
        this.warnings = List.copyOf(warnings);
    }

    public Map<String, List<Position>> getEdgePaths() { return edgePaths; }
    public List<RoutingWarning> getWarnings() { return warnings; }
}
