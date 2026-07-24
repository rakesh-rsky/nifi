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

import in.shrake.nifi.layout.core.exception.WriteBackException;
import in.shrake.nifi.layout.core.model.*;
import in.shrake.nifi.layout.core.model.*;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builds deterministic public results without depending on a concrete NiFi representation. */
public final class LayoutResultFactory {
    public LayoutResult create(LayoutWriteRequest request) {
        List<ComponentUpdate> updates = new ArrayList<>();
        collect(request.graph(), request.coordinates(), updates);
        int repositioned = (int) updates.stream()
                .filter(update -> !update.originalPosition().equals(update.newPosition())).count();
        Map<String, List<Position>> bends = request.routing() == null
                ? Map.of() : new LinkedHashMap<>(request.routing().getEdgePaths());
        return new LayoutResult(updates, repositioned, bends,
                request.computationTimeMs(), request.warnings());
    }

    private void collect(LayoutGraph graph, CoordinateAssignment coordinates,
                         List<ComponentUpdate> updates) {
        for (LayoutNode node : graph.getNodes().values()) {
            if (node.getType() == NodeType.VIRTUAL) continue;
            Position position = coordinates.getPositions().get(node.getId());
            if (position == null) {
                throw new WriteBackException(node.getId(), node.getType().name(),
                        "no computed position is available");
            }
            BoundingBox oldBounds = node.getBoundingBox();
            BoundingBox newBounds = coordinates.getBounds().get(node.getId());
            if (newBounds == null) {
                newBounds = new BoundingBox(position.x(), position.y(),
                        oldBounds.width(), oldBounds.height());
            }
            updates.add(new ComponentUpdate(node.getId(), node.getParentGroupId(),
                    node.getType(), new Position(oldBounds.x(), oldBounds.y()), position, newBounds));
        }
        for (LayoutGraph child : graph.getSubgraphs().values()) {
            collect(child, coordinates, updates);
        }
    }
}
