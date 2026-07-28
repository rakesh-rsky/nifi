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

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.core.model.RoutingWarning;
import in.shrake.nifi.layout.core.util.Geometry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Direct-mode router.  Emits a two-point path [sourceAnchor, targetAnchor] for
 * every edge so that {@link in.shrake.nifi.layout.rest.RestDtoWriter} persists
 * zero interior bend points (full-path semantics: strip first and last).
 */
public class DirectRouter implements RoutingStrategy {
    private static final int OBSTACLE_CLEARANCE = 10;

    @Override
    public RoutingResult computeRoutes(LayoutGraph graph, CoordinateAssignment coordinates,
                                       LayoutOptions options) {
        Map<String, List<Position>> routes = new LinkedHashMap<>();
        List<RoutingWarning> warnings = new ArrayList<>();
        RoutingResult orthogonalRoutes = null;
        Map<String, BoundingBox> componentBounds = new LinkedHashMap<>();
        for (String nodeId : graph.getNodes().keySet()) {
            BoundingBox bounds = coordinates.getBounds().get(nodeId);
            if (bounds != null) {
                componentBounds.put(nodeId, bounds);
            }
        }

        for (LayoutEdge edge : graph.getEdges().values()) {
            BoundingBox sBb = coordinates.getBounds().get(edge.getSourceNodeId());
            BoundingBox tBb = coordinates.getBounds().get(edge.getTargetNodeId());

            if (sBb == null || tBb == null) {
                continue;
            }
            if (edge.isSelfLoop()) {
                if (orthogonalRoutes == null) {
                    orthogonalRoutes = new OrthogonalRouter()
                            .computeRoutes(graph, coordinates, options);
                }
                routes.put(edge.getId(), orthogonalRoutes.getEdgePaths().get(edge.getId()));
                orthogonalRoutes.getWarnings().stream()
                        .filter(warning -> warning.edgeId().equals(edge.getId()))
                        .forEach(warnings::add);
                continue;
            }
            // Anchor on the face that geometrically faces the other component so that
            // backward edges (source visually past target on the primary axis) exit and
            // enter on the correct side without passing through the endpoint interior.
            Position src = geometricExitAnchor(sBb, tBb);
            Position dst = geometricEntryAnchor(sBb, tBb);
            List<Position> directPath = List.of(src, dst);
            if (intersectsObstacle(src, dst, componentBounds, edge)) {
                if (orthogonalRoutes == null) {
                    orthogonalRoutes = new OrthogonalRouter()
                            .computeRoutes(graph, coordinates, options);
                }
                routes.put(edge.getId(), orthogonalRoutes.getEdgePaths().get(edge.getId()));
                orthogonalRoutes.getWarnings().stream()
                        .filter(warning -> warning.edgeId().equals(edge.getId()))
                        .forEach(warnings::add);
            } else {
                routes.put(edge.getId(), directPath);
            }
        }

        return new RoutingResult(routes, warnings);
    }

    private static boolean intersectsObstacle(
            Position source,
            Position target,
            Map<String, BoundingBox> componentBounds,
            LayoutEdge edge) {
        for (Map.Entry<String, BoundingBox> entry : componentBounds.entrySet()) {
            String componentId = entry.getKey();
            if (!componentId.equals(edge.getSourceNodeId())
                    && !componentId.equals(edge.getTargetNodeId())
                    && Geometry.segmentIntersectsBounds(
                            source, target, entry.getValue(), OBSTACLE_CLEARANCE)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Exit anchor on the face of {@code src} that geometrically faces {@code dst}.
     * Uses the primary spatial offset (max of |Δx|, |Δy|) to decide which axis to
     * favour, then picks the near or far face on that axis.
     */
    private static Position geometricExitAnchor(BoundingBox src, BoundingBox dst) {
        int dx = dst.center().x() - src.center().x();
        int dy = dst.center().y() - src.center().y();
        if (Math.abs(dx) >= Math.abs(dy)) {
            // Horizontal primary: exit left or right
            return dx >= 0
                    ? new Position(src.right(), src.center().y())
                    : new Position(src.x(), src.center().y());
        } else {
            // Vertical primary: exit top or bottom
            return dy >= 0
                    ? new Position(src.center().x(), src.bottom())
                    : new Position(src.center().x(), src.y());
        }
    }

    /**
     * Entry anchor on the face of {@code dst} that geometrically faces {@code src}.
     */
    private static Position geometricEntryAnchor(BoundingBox src, BoundingBox dst) {
        int dx = dst.center().x() - src.center().x();
        int dy = dst.center().y() - src.center().y();
        if (Math.abs(dx) >= Math.abs(dy)) {
            return dx >= 0
                    ? new Position(dst.x(), dst.center().y())
                    : new Position(dst.right(), dst.center().y());
        } else {
            return dy >= 0
                    ? new Position(dst.center().x(), dst.y())
                    : new Position(dst.center().x(), dst.bottom());
        }
    }

}
