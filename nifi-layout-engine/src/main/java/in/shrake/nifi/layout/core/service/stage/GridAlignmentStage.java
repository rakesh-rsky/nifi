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

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.core.service.PipelineContext;
import in.shrake.nifi.layout.core.service.PipelineStage;
import in.shrake.nifi.layout.core.util.GridSnapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Snaps component positions to the configured grid.  This stage runs after
 * collision resolution and before connection routing so that route endpoint
 * anchors are derived from already-aligned component boundaries.
 *
 * <p>When routing results are present (e.g. in a custom pipeline that places
 * this stage after routing) only <em>interior</em> bend points are snapped;
 * the first and last points of each path are left untouched to avoid
 * detaching anchors from their component boundaries.  Consecutive duplicate
 * points introduced by snapping are removed to preserve orthogonality.
 */
public final class GridAlignmentStage implements PipelineStage {
    @Override
    public String getName() {
        return "Grid Alignment";
    }

    @Override
    public PipelineContext execute(PipelineContext context) {
        int gridSize = context.getOptions().getGridSize();
        Map<String, Position> positions = new LinkedHashMap<>();
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        context.getCoordinates().getPositions().forEach((id, position) -> {
            Position snapped = GridSnapper.snap(position, gridSize);
            BoundingBox current = context.getCoordinates().getBounds().get(id);
            positions.put(id, snapped);
            bounds.put(id, new BoundingBox(snapped.x(), snapped.y(), current.width(), current.height()));
        });

        PipelineContext result = context.withCoordinates(new CoordinateAssignment(positions, bounds));
        if (context.getRouting() == null) {
            return result;
        }

        // Snap only interior bend points; endpoints stay anchored to component boundaries.
        Map<String, List<Position>> routes = new LinkedHashMap<>();
        context.getRouting().getEdgePaths().forEach((edgeId, path) -> {
            if (path.size() <= 2) {
                routes.put(edgeId, path);
                return;
            }
            List<Position> snapped = new ArrayList<>(path.size());
            snapped.add(path.get(0));
            for (int i = 1; i < path.size() - 1; i++) {
                snapped.add(GridSnapper.snap(path.get(i), gridSize));
            }
            snapped.add(path.get(path.size() - 1));
            routes.put(edgeId, deduplicateConsecutive(snapped));
        });
        return result.withRouting(new RoutingResult(routes));
    }

    private static List<Position> deduplicateConsecutive(List<Position> path) {
        List<Position> deduped = new ArrayList<>(path.size());
        for (Position p : path) {
            if (deduped.isEmpty() || !p.equals(deduped.get(deduped.size() - 1))) {
                deduped.add(p);
            }
        }
        return deduped;
    }
}
