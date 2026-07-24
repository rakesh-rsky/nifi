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

public final class LayoutResult {
    private final List<ComponentUpdate> updates;
    private final int totalComponentsRepositioned;
    private final Map<String, List<Position>> connectionBendPoints;
    private final long computationTimeMs;
    private final List<String> warnings;

    public LayoutResult(List<ComponentUpdate> updates, int totalComponentsRepositioned, Map<String, List<Position>> connectionBendPoints, long computationTimeMs, List<String> warnings) {
        this.updates = List.copyOf(updates);
        this.totalComponentsRepositioned = totalComponentsRepositioned;
        LinkedHashMap<String, List<Position>> bends = new LinkedHashMap<>();
        connectionBendPoints.forEach((edgeId, path) -> bends.put(edgeId, List.copyOf(path)));
        this.connectionBendPoints = Collections.unmodifiableMap(bends);
        this.computationTimeMs = computationTimeMs;
        this.warnings = List.copyOf(warnings);
    }

    public List<ComponentUpdate> getUpdates() { return updates; }
    public int getTotalComponentsRepositioned() { return totalComponentsRepositioned; }
    public Map<String, List<Position>> getConnectionBendPoints() { return connectionBendPoints; }
    public long getComputationTimeMs() { return computationTimeMs; }
    public List<String> getWarnings() { return warnings; }
}
