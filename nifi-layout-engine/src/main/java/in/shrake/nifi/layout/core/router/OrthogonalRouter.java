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
import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.core.model.RoutingWarning;
import in.shrake.nifi.layout.core.util.Geometry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Orthogonal router that emits <em>full</em> endpoint-to-endpoint paths.
 *
 * <p>Every path begins with the source-component exit anchor and ends with the
 * target-component entry anchor.  Interior points are orthogonal bend waypoints.
 * {@link in.shrake.nifi.layout.rest.RestDtoWriter} strips the first and last
 * points so that only interior bends are persisted to NiFi.
 *
 * <h3>Routing tiers</h3>
 * <ol>
 *   <li>Self-loops – exterior lane around the owning component.
 *   <li>Backedges (reversed edges or visually backward) – exterior lane outside
 *       the full occupied graph boundary, with one lane per backedge to prevent
 *       overlaps.
 *   <li>Dense fan-in – shared horizontal bus for multi-source-to-same-target
 *       topologies.
 *   <li>Clear 1-to-1 corridor – zero interior bends when the bounding-box
 *       corridor between source exit and target entry is obstacle-free.
 *   <li>Minimal clear path – single bend when fan-out or fan-in degree > 1 and
 *       the corridor is obstacle-free.
 *   <li>Parallel/fan-out/fan-in lanes – separated label-anchor lanes.
 *   <li>Obstacle-aware single-edge fallback – candidate midpoints around
 *       obstacle boundaries; emits a {@link RoutingWarning} if no clear
 *       candidate is found.
 * </ol>
 */
public class OrthogonalRouter implements RoutingStrategy {
    private static final int CONNECTION_LABEL_WIDTH = 240;
    private static final int CONNECTION_LABEL_CLEARANCE = 20;
    private static final int OBSTACLE_CLEARANCE = 10;

    private enum RouteKind {
        SELF_LOOP,
        BACKEDGE,
        DENSE_FAN_IN,
        SIMPLE,
        SEPARATED,
        OBSTACLE_AWARE
    }

    private record GridState(int xIndex, int yIndex, int direction) {
    }

    private record GridQueueEntry(GridState state, long cost) {
    }

    @Override
    public RoutingResult computeRoutes(LayoutGraph graph, CoordinateAssignment coordinates,
                                       LayoutOptions options) {
        Map<String, List<Position>> routes = new LinkedHashMap<>();
        FlowDirection flowDir = options.getFlowDirection();
        List<RoutingWarning> warnings = new ArrayList<>();

        // Pre-compute edge adjacency lists (sorted by graph's deterministic edge order).
        Map<String, List<LayoutEdge>> sourceEdges = new LinkedHashMap<>();
        Map<String, List<LayoutEdge>> targetEdges = new LinkedHashMap<>();
        for (LayoutEdge edge : graph.getEdges().values()) {
            sourceEdges.computeIfAbsent(edge.getSourceNodeId(), k -> new ArrayList<>()).add(edge);
            targetEdges.computeIfAbsent(edge.getTargetNodeId(), k -> new ArrayList<>()).add(edge);
        }

        // Fix 3 – pre-compute all anchors so that visual direction (forward vs backward)
        // can be determined BEFORE building lane indices.  Edges that are flagged
        // isReversed OR whose anchor coordinates are visually backward are ALL
        // dispatched as backedges and must receive distinct deterministic lanes.
        Map<String, Position> preStart = new LinkedHashMap<>();
        Map<String, Position> preEnd   = new LinkedHashMap<>();
        java.util.Set<String> backedgeIds = new java.util.LinkedHashSet<>();

        for (LayoutEdge e : graph.getEdges().values()) {
            BoundingBox sBb0 = coordinates.getBounds().get(e.getSourceNodeId());
            BoundingBox tBb0 = coordinates.getBounds().get(e.getTargetNodeId());
            if (sBb0 == null || tBb0 == null) continue;
            List<LayoutEdge> sl = sourceEdges.get(e.getSourceNodeId());
            List<LayoutEdge> tl = targetEdges.get(e.getTargetNodeId());
            Position s = shouldUseLateralBranchExit(
                    e, sBb0, tBb0, flowDir, sourceEdges, graph)
                    ? getLateralExitPoint(sBb0, tBb0, flowDir)
                    : getExitPoint(
                            sBb0, flowDir, sl.indexOf(e), sl.size(), options.getPortSpacing());
            Position t = getEntryPoint(tBb0, flowDir, tl.indexOf(e), tl.size(), options.getPortSpacing());
            preStart.put(e.getId(), s);
            preEnd.put(e.getId(), t);
            if (!e.isSelfLoop() && (e.isReversed() || !isForwardEdge(s, t, flowDir))) {
                backedgeIds.add(e.getId());
            }
        }

        // Self-loops and backedges share the same exterior channel space.
        Map<String, Integer> exteriorLanes = new LinkedHashMap<>();
        int exteriorLaneIndex = 0;
        for (LayoutEdge e : graph.getEdges().values()) {
            if (e.isSelfLoop() || backedgeIds.contains(e.getId())) {
                exteriorLanes.put(e.getId(), exteriorLaneIndex++);
            }
        }

        // Occupied bounding box of all graph nodes – used for backedge exterior lanes.
        BoundingBox graphBounds = computeGraphBounds(coordinates);

        for (LayoutEdge edge : graph.getEdges().values()) {
            BoundingBox sBb = coordinates.getBounds().get(edge.getSourceNodeId());
            BoundingBox tBb = coordinates.getBounds().get(edge.getTargetNodeId());
            if (sBb == null || tBb == null) continue;

            Position start = preStart.get(edge.getId());
            Position end   = preEnd.get(edge.getId());
            if (start == null || end == null) continue;

            List<LayoutEdge> sList = sourceEdges.get(edge.getSourceNodeId());
            int sIndex = sList.indexOf(edge);
            List<LayoutEdge> tList = targetEdges.get(edge.getTargetNodeId());
            int tIndex = tList.indexOf(edge);
            List<LayoutEdge> parallelEdges = sList.stream()
                    .filter(c -> c.getTargetNodeId().equals(edge.getTargetNodeId()))
                    .toList();

            boolean isForward = isForwardEdge(start, end, flowDir);

            // Build interior bend points.
            List<Position> interior = new ArrayList<>();
            RouteKind routeKind;
            int routeLaneIndex = 0;

            if (edge.isSelfLoop()) {
                routeKind = RouteKind.SELF_LOOP;
                routeLaneIndex = exteriorLanes.getOrDefault(edge.getId(), 0);
                buildSelfLoopInterior(
                        interior, start, end, graphBounds, flowDir, options, routeLaneIndex);

            } else if (backedgeIds.contains(edge.getId())) {
                routeKind = RouteKind.BACKEDGE;
                routeLaneIndex = exteriorLanes.getOrDefault(edge.getId(), 0);
                buildBackedgeInterior(
                        interior, start, end, flowDir, options, graphBounds, routeLaneIndex);

            } else if (parallelEdges.size() == 1 && tList.size() >= 3) {
                routeKind = RouteKind.DENSE_FAN_IN;
                List<String> denseTargets = denseTargetsAtRank(
                        targetEdges, coordinates, edge.getTargetNodeId(), flowDir);
                routeLaneIndex = Math.max(0, denseTargets.indexOf(edge.getTargetNodeId()));
                buildDenseFanInPath(interior, start, end, flowDir,
                        routeLaneIndex, denseTargets.size());

            } else if (parallelEdges.size() == 1
                    && isCorridorClear(graph, coordinates, edge, start, end)) {
                routeKind = RouteKind.SIMPLE;
                buildMinimalClearPath(interior, start, end, flowDir,
                        sList.size(), tList.size());

            } else {
                boolean parallel = parallelEdges.size() > 1;
                boolean fanOut = !parallel && sList.size() > 1;
                int laneIndex = parallel ? parallelEdges.indexOf(edge)
                        : fanOut ? sIndex : tIndex;
                int laneCount = parallel ? parallelEdges.size()
                        : fanOut ? sList.size() : tList.size();
                BoundingBox laneOwner = fanOut ? sBb : tBb;
                routeLaneIndex = laneIndex;

                if (!parallel && laneCount == 1) {
                    routeKind = RouteKind.OBSTACLE_AWARE;
                    // Single forward edge whose corridor is blocked: obstacle-aware routing.
                    List<BoundingBox> obstacles = collectObstacles(graph, coordinates, edge);
                    buildObstacleAwareSingleInterior(interior, start, end, flowDir, options,
                            obstacles, warnings, edge.getId());
                } else {
                    routeKind = RouteKind.SEPARATED;
                    buildSeparatedLanedInterior(interior, start, end, flowDir,
                            options.getVerticalSpacing(), options.getHorizontalSpacing(),
                            sBb, tBb, laneIndex, laneCount, laneOwner);
                }
            }

            if (routeKind != RouteKind.OBSTACLE_AWARE) {
                List<BoundingBox> obstacles = collectObstacles(graph, coordinates, edge);
                if (!obstacles.isEmpty()) {
                    List<Position> candidateFull = fullPath(start, interior, end);
                    if (Geometry.pathIntersectsAnyObstacle(
                            candidateFull, obstacles, OBSTACLE_CLEARANCE)) {
                        if (routeKind == RouteKind.SELF_LOOP
                                || routeKind == RouteKind.BACKEDGE
                                || routeKind == RouteKind.DENSE_FAN_IN
                                || routeKind == RouteKind.SEPARATED) {
                            int lanePitch = CONNECTION_LABEL_WIDTH
                                    + CONNECTION_LABEL_CLEARANCE;
                            replaceWithExteriorRoute(
                                    interior, start, end, flowDir, options, graphBounds,
                                    routeLaneIndex, lanePitch, obstacles, warnings, edge.getId());
                        } else {
                            interior.clear();
                            buildObstacleAwareSingleInterior(
                                    interior, start, end, flowDir, options,
                                    obstacles, warnings, edge.getId());
                        }
                    }
                }
            }

            // Wrap: full path = [src_anchor, ...interior..., dst_anchor].
            List<Position> path = new ArrayList<>(interior.size() + 2);
            path.add(start);
            path.addAll(interior);
            path.add(end);
            routes.put(edge.getId(), path);
        }

        return new RoutingResult(routes, warnings);
    }

    private void replaceWithExteriorRoute(
            List<Position> interior,
            Position start,
            Position end,
            FlowDirection flowDirection,
            LayoutOptions options,
            BoundingBox graphBounds,
            int laneIndex,
            int lanePitch,
            List<BoundingBox> obstacles,
            List<RoutingWarning> warnings,
            String edgeId) {
        List<Position> preferredFallback = null;
        for (boolean positiveSide : List.of(true, false)) {
            List<Position> candidate = buildExteriorInterior(
                    start, end, flowDirection, options, graphBounds,
                    laneIndex, lanePitch, positiveSide);
            if (preferredFallback == null) {
                preferredFallback = candidate;
            }
            if (!Geometry.pathIntersectsAnyObstacle(
                    fullPath(start, candidate, end), obstacles, OBSTACLE_CLEARANCE)) {
                interior.clear();
                interior.addAll(candidate);
                return;
            }
        }

        warnings.add(new RoutingWarning(edgeId,
                "no clear exterior route found; route may cross a component"));
        interior.clear();
        interior.addAll(preferredFallback);
    }

    private List<Position> buildExteriorInterior(
            Position start,
            Position end,
            FlowDirection flowDirection,
            LayoutOptions options,
            BoundingBox graphBounds,
            int laneIndex,
            int lanePitch,
            boolean positiveSide) {
        int stub = Math.max(10, Math.min(
                options.getHorizontalSpacing(), options.getVerticalSpacing()) / 2);
        int laneOffset = Math.addExact(
                stub, Math.multiplyExact(laneIndex, lanePitch));
        List<Position> path = new ArrayList<>(4);
        if (flowDirection == FlowDirection.TOP_TO_BOTTOM
                || flowDirection == FlowDirection.BOTTOM_TO_TOP) {
            int direction = flowDirection == FlowDirection.TOP_TO_BOTTOM ? 1 : -1;
            int sideX = positiveSide
                    ? Math.addExact(graphBounds.right(), laneOffset)
                    : Math.subtractExact(graphBounds.x(), laneOffset);
            int sourceY = start.y() + direction * stub;
            int targetY = end.y() - direction * stub;
            path.add(new Position(start.x(), sourceY));
            path.add(new Position(sideX, sourceY));
            path.add(new Position(sideX, targetY));
            path.add(new Position(end.x(), targetY));
        } else {
            int direction = flowDirection == FlowDirection.LEFT_TO_RIGHT ? 1 : -1;
            int sideY = positiveSide
                    ? Math.addExact(graphBounds.bottom(), laneOffset)
                    : Math.subtractExact(graphBounds.y(), laneOffset);
            int sourceX = start.x() + direction * stub;
            int targetX = end.x() - direction * stub;
            path.add(new Position(sourceX, start.y()));
            path.add(new Position(sourceX, sideY));
            path.add(new Position(targetX, sideY));
            path.add(new Position(targetX, end.y()));
        }
        return path;
    }

    private List<Position> fullPath(
            Position start,
            List<Position> interior,
            Position end) {
        List<Position> path = new ArrayList<>(interior.size() + 2);
        path.add(start);
        path.addAll(interior);
        path.add(end);
        return path;
    }

    // -------------------------------------------------------------------------
    // Corridor / obstacle helpers
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when the axis-aligned bounding-box corridor between
     * {@code start} and {@code end} (expanded by {@link #OBSTACLE_CLEARANCE})
     * contains no obstacle nodes.
     */
    private boolean isCorridorClear(LayoutGraph graph, CoordinateAssignment coordinates,
                                     LayoutEdge edge, Position start, Position end) {
        int minX = Math.min(start.x(), end.x()) - OBSTACLE_CLEARANCE;
        int maxX = Math.max(start.x(), end.x()) + OBSTACLE_CLEARANCE;
        int minY = Math.min(start.y(), end.y()) - OBSTACLE_CLEARANCE;
        int maxY = Math.max(start.y(), end.y()) + OBSTACLE_CLEARANCE;
        BoundingBox corridor = new BoundingBox(minX, minY, maxX - minX, maxY - minY);

        for (Map.Entry<String, BoundingBox> entry : coordinates.getBounds().entrySet()) {
            String nodeId = entry.getKey();
            if (nodeId.equals(edge.getSourceNodeId()) || nodeId.equals(edge.getTargetNodeId())
                    || graph.getNode(nodeId) == null) {
                continue;
            }
            if (entry.getValue().intersects(corridor)) {
                return false;
            }
        }
        return true;
    }

    private List<BoundingBox> collectObstacles(LayoutGraph graph, CoordinateAssignment coords,
                                               LayoutEdge edge) {
        List<BoundingBox> obstacles = new ArrayList<>();
        for (Map.Entry<String, BoundingBox> entry : coords.getBounds().entrySet()) {
            String id = entry.getKey();
            if (!id.equals(edge.getSourceNodeId()) && !id.equals(edge.getTargetNodeId())
                    && graph.getNode(id) != null) {
                obstacles.add(entry.getValue());
            }
        }
        return obstacles;
    }

    // -------------------------------------------------------------------------
    // Direction helpers
    // -------------------------------------------------------------------------

    private static boolean isForwardEdge(Position start, Position end, FlowDirection dir) {
        return switch (dir) {
            case TOP_TO_BOTTOM -> start.y() <= end.y();
            case BOTTOM_TO_TOP -> start.y() >= end.y();
            case LEFT_TO_RIGHT -> start.x() <= end.x();
            case RIGHT_TO_LEFT -> start.x() >= end.x();
        };
    }

    // -------------------------------------------------------------------------
    // Lane pre-computation
    // -------------------------------------------------------------------------

    private static BoundingBox computeGraphBounds(CoordinateAssignment coords) {
        BoundingBox bounds = null;
        for (BoundingBox bb : coords.getBounds().values()) {
            bounds = Geometry.union(bounds, bb);
        }
        return bounds != null ? bounds : new BoundingBox(0, 0, 0, 0);
    }

    // -------------------------------------------------------------------------
    // Interior-bend builders (all add ONLY interior points to `path`)
    // -------------------------------------------------------------------------

    /** Self-loop routed outside the occupied graph bounds on an exterior lane. */
    private void buildSelfLoopInterior(List<Position> path, Position start, Position end,
                                       BoundingBox graphBounds, FlowDirection flowDir,
                                       LayoutOptions options, int laneIndex) {
        int base = Math.max(10, Math.min(options.getHorizontalSpacing(),
                options.getVerticalSpacing()) / 2);
        int space = base + laneIndex
                * (CONNECTION_LABEL_WIDTH + CONNECTION_LABEL_CLEARANCE);

        switch (flowDir) {
            case TOP_TO_BOTTOM -> {
                path.add(new Position(start.x(), start.y() + base));
                path.add(new Position(graphBounds.right() + space, start.y() + base));
                path.add(new Position(graphBounds.right() + space, end.y() - base));
                path.add(new Position(end.x(), end.y() - base));
            }
            case BOTTOM_TO_TOP -> {
                path.add(new Position(start.x(), start.y() - base));
                path.add(new Position(graphBounds.right() + space, start.y() - base));
                path.add(new Position(graphBounds.right() + space, end.y() + base));
                path.add(new Position(end.x(), end.y() + base));
            }
            case LEFT_TO_RIGHT -> {
                path.add(new Position(start.x() + base, start.y()));
                path.add(new Position(start.x() + base, graphBounds.bottom() + space));
                path.add(new Position(end.x() - base, graphBounds.bottom() + space));
                path.add(new Position(end.x() - base, end.y()));
            }
            case RIGHT_TO_LEFT -> {
                path.add(new Position(start.x() - base, start.y()));
                path.add(new Position(start.x() - base, graphBounds.bottom() + space));
                path.add(new Position(end.x() + base, graphBounds.bottom() + space));
                path.add(new Position(end.x() + base, end.y()));
            }
        }
    }

    /**
     * Backedge routed outside the full occupied graph boundary.
     * Each lane gets an incremental offset so multiple backedges do not overlap.
     */
    private void buildBackedgeInterior(List<Position> path, Position start, Position end,
                                       FlowDirection flowDir, LayoutOptions options,
                                       BoundingBox graphBounds, int laneIndex) {
        int base = Math.max(10, Math.min(options.getHorizontalSpacing(),
                options.getVerticalSpacing()) / 2);
        int stub = base;
        int laneOffset = base + laneIndex
                * (CONNECTION_LABEL_WIDTH + CONNECTION_LABEL_CLEARANCE);

        switch (flowDir) {
            case TOP_TO_BOTTOM -> {
                int sideX = graphBounds.right() + laneOffset;
                path.add(new Position(start.x(), start.y() + stub));
                path.add(new Position(sideX, start.y() + stub));
                path.add(new Position(sideX, end.y() - stub));
                path.add(new Position(end.x(), end.y() - stub));
            }
            case BOTTOM_TO_TOP -> {
                int sideX = graphBounds.right() + laneOffset;
                path.add(new Position(start.x(), start.y() - stub));
                path.add(new Position(sideX, start.y() - stub));
                path.add(new Position(sideX, end.y() + stub));
                path.add(new Position(end.x(), end.y() + stub));
            }
            case LEFT_TO_RIGHT -> {
                int sideY = graphBounds.bottom() + laneOffset;
                path.add(new Position(start.x() + stub, start.y()));
                path.add(new Position(start.x() + stub, sideY));
                path.add(new Position(end.x() - stub, sideY));
                path.add(new Position(end.x() - stub, end.y()));
            }
            case RIGHT_TO_LEFT -> {
                int sideY = graphBounds.bottom() + laneOffset;
                path.add(new Position(start.x() - stub, start.y()));
                path.add(new Position(start.x() - stub, sideY));
                path.add(new Position(end.x() + stub, sideY));
                path.add(new Position(end.x() + stub, end.y()));
            }
        }
    }

    /** Shared bus for dense fan-in topologies (multiple sources → one dense target). */
    private void buildDenseFanInPath(List<Position> path, Position start, Position end,
                                     FlowDirection flowDir, int laneIndex, int laneCount) {
        if (laneIndex < 0 || laneCount < 1) {
            return;
        }
        if (flowDir == FlowDirection.TOP_TO_BOTTOM || flowDir == FlowDirection.BOTTOM_TO_TOP) {
            int direction = flowDir == FlowDirection.TOP_TO_BOTTOM ? 1 : -1;
            int gap = Math.abs(end.y() - start.y());
            int busOffset = (laneIndex + 1) * gap / (laneCount + 1);
            int stub = Math.max(10, Math.min(20, busOffset / 2));
            int busY = start.y() + direction * busOffset;
            path.add(new Position(start.x(), start.y() + direction * stub));
            path.add(new Position(start.x(), busY));
            path.add(new Position(end.x(), busY));
        } else {
            int direction = flowDir == FlowDirection.LEFT_TO_RIGHT ? 1 : -1;
            int gap = Math.abs(end.x() - start.x());
            int busOffset = (laneIndex + 1) * gap / (laneCount + 1);
            int stub = Math.max(10, Math.min(20, busOffset / 2));
            int busX = start.x() + direction * busOffset;
            path.add(new Position(start.x() + direction * stub, start.y()));
            path.add(new Position(busX, start.y()));
            path.add(new Position(busX, end.y()));
        }
    }

    /**
     * Minimal bend path for clear corridors.
     * Zero bends for 1-to-1; one bend for fan-out or fan-in degree > 1.
     */
    private void buildMinimalClearPath(List<Position> path, Position start, Position end,
                                       FlowDirection flowDir, int sourceEdgeCount,
                                       int targetEdgeCount) {
        if (sourceEdgeCount == 1 && targetEdgeCount == 1) {
            return; // zero bends
        }
        boolean vertical = flowDir == FlowDirection.TOP_TO_BOTTOM
                || flowDir == FlowDirection.BOTTOM_TO_TOP;
        int midpoint = vertical
                ? (start.y() + end.y()) / 2
                : (start.x() + end.x()) / 2;
        path.addAll(twoPointInterior(start, end, flowDir, midpoint));
    }

    /**
     * Obstacle-aware routing for a single forward edge whose corridor is
     * blocked.  Tries candidate midpoints derived from obstacle boundaries.
     * If all candidates are blocked, uses the default midpoint and emits a
     * {@link RoutingWarning}.
     */
    private void buildObstacleAwareSingleInterior(List<Position> path, Position start,
                                                   Position end, FlowDirection flowDir,
                                                   LayoutOptions options,
                                                   List<BoundingBox> obstacles,
                                                   List<RoutingWarning> warnings,
                                                   String edgeId) {
        boolean vertical = flowDir == FlowDirection.TOP_TO_BOTTOM
                || flowDir == FlowDirection.BOTTOM_TO_TOP;
        int startPrimary = vertical ? start.y() : start.x();
        int endPrimary = vertical ? end.y() : end.x();
        int lo = Math.min(startPrimary, endPrimary);
        int hi = Math.max(startPrimary, endPrimary);

        // Candidate midpoints: obstacle boundaries ± clearance ± 1 (strictly outside).
        TreeSet<Integer> candidateSet = new TreeSet<>();
        for (BoundingBox obs : obstacles) {
            int obsLo = vertical ? obs.y() : obs.x();
            int obsHi = vertical ? obs.bottom() : obs.right();
            int above = obsLo - OBSTACLE_CLEARANCE - 1;
            int below = obsHi + OBSTACLE_CLEARANCE + 1;
            if (above > lo && above < hi) candidateSet.add(above);
            if (below > lo && below < hi) candidateSet.add(below);
        }
        // Always include the default midpoint so it's tried if no obstacle candidate works.
        int defaultMid = (startPrimary + endPrimary) / 2;
        candidateSet.add(defaultMid);

        List<List<Position>> candidates = new ArrayList<>();
        for (int mid : candidateSet) {
            candidates.add(twoPointInterior(start, end, flowDir, mid));
        }

        TreeSet<Integer> secondaryChannels = new TreeSet<>();
        for (BoundingBox obstacle : obstacles) {
            if (vertical) {
                secondaryChannels.add(obstacle.x() - OBSTACLE_CLEARANCE - 1);
                secondaryChannels.add(obstacle.right() + OBSTACLE_CLEARANCE + 1);
            } else {
                secondaryChannels.add(obstacle.y() - OBSTACLE_CLEARANCE - 1);
                secondaryChannels.add(obstacle.bottom() + OBSTACLE_CLEARANCE + 1);
            }
        }
        for (int channel : secondaryChannels) {
            candidates.add(secondaryChannelInterior(
                    start, end, flowDir, channel));
        }

        List<Position> bestInterior = null;
        int bestLength = Integer.MAX_VALUE;
        for (List<Position> interior : candidates) {
            List<Position> full = fullPath(start, interior, end);
            if (!Geometry.pathIntersectsAnyObstacle(full, obstacles, OBSTACLE_CLEARANCE)) {
                int length = Geometry.pathLength(full);
                if (length < bestLength) {
                    bestLength = length;
                    bestInterior = interior;
                }
            }
        }
        if (bestInterior != null) {
            path.addAll(bestInterior);
            return;
        }

        List<Position> gridRoute = findClearGridRoute(start, end, obstacles);
        if (!gridRoute.isEmpty()) {
            path.addAll(gridRoute.subList(1, gridRoute.size() - 1));
            return;
        }

        // No clear route – use default and warn.
        warnings.add(new RoutingWarning(edgeId,
                "no clear orthogonal route found; route may cross a component"));
        path.addAll(twoPointInterior(start, end, flowDir, defaultMid));
    }

    private List<Position> findClearGridRoute(
            Position start,
            Position end,
            List<BoundingBox> obstacles) {
        TreeSet<Integer> candidateX = new TreeSet<>();
        TreeSet<Integer> candidateY = new TreeSet<>();
        candidateX.add(start.x());
        candidateX.add(end.x());
        candidateY.add(start.y());
        candidateY.add(end.y());
        for (BoundingBox obstacle : obstacles) {
            candidateX.add(obstacle.x() - OBSTACLE_CLEARANCE - 1);
            candidateX.add(obstacle.right() + OBSTACLE_CLEARANCE + 1);
            candidateY.add(obstacle.y() - OBSTACLE_CLEARANCE - 1);
            candidateY.add(obstacle.bottom() + OBSTACLE_CLEARANCE + 1);
        }

        List<Integer> xCoordinates = boundedCoordinates(
                candidateX, start.x(), end.x());
        List<Integer> yCoordinates = boundedCoordinates(
                candidateY, start.y(), end.y());
        int startX = xCoordinates.indexOf(start.x());
        int startY = yCoordinates.indexOf(start.y());
        int endX = xCoordinates.indexOf(end.x());
        int endY = yCoordinates.indexOf(end.y());

        GridState initial = new GridState(startX, startY, 0);
        Map<GridState, Long> distances = new HashMap<>();
        Map<GridState, GridState> previous = new HashMap<>();
        PriorityQueue<GridQueueEntry> queue = new PriorityQueue<>(
                Comparator.comparingLong(GridQueueEntry::cost)
                        .thenComparingInt(entry -> entry.state().xIndex())
                        .thenComparingInt(entry -> entry.state().yIndex())
                        .thenComparingInt(entry -> entry.state().direction()));
        distances.put(initial, 0L);
        queue.add(new GridQueueEntry(initial, 0L));

        GridState destination = null;
        while (!queue.isEmpty()) {
            GridQueueEntry currentEntry = queue.remove();
            GridState current = currentEntry.state();
            if (currentEntry.cost() != distances.getOrDefault(current, Long.MAX_VALUE)) {
                continue;
            }
            if (current.xIndex() == endX && current.yIndex() == endY) {
                destination = current;
                break;
            }

            int[][] neighbors = {
                    {current.xIndex() - 1, current.yIndex(), 1},
                    {current.xIndex() + 1, current.yIndex(), 1},
                    {current.xIndex(), current.yIndex() - 1, 2},
                    {current.xIndex(), current.yIndex() + 1, 2}
            };
            for (int[] neighbor : neighbors) {
                if (neighbor[0] < 0 || neighbor[0] >= xCoordinates.size()
                        || neighbor[1] < 0 || neighbor[1] >= yCoordinates.size()) {
                    continue;
                }
                Position from = gridPosition(current, xCoordinates, yCoordinates);
                Position to = new Position(
                        xCoordinates.get(neighbor[0]), yCoordinates.get(neighbor[1]));
                if (Geometry.pathIntersectsAnyObstacle(
                        List.of(from, to), obstacles, OBSTACLE_CLEARANCE)) {
                    continue;
                }
                GridState next = new GridState(
                        neighbor[0], neighbor[1], neighbor[2]);
                long bendPenalty = current.direction() == 0
                        || current.direction() == next.direction() ? 0 : 1;
                long nextCost = currentEntry.cost()
                        + (long) Geometry.pathLength(List.of(from, to)) * 1_000
                        + bendPenalty;
                if (nextCost < distances.getOrDefault(next, Long.MAX_VALUE)) {
                    distances.put(next, nextCost);
                    previous.put(next, current);
                    queue.add(new GridQueueEntry(next, nextCost));
                }
            }
        }
        if (destination == null) {
            return List.of();
        }

        List<Position> route = new ArrayList<>();
        for (GridState state = destination; state != null; state = previous.get(state)) {
            route.add(gridPosition(state, xCoordinates, yCoordinates));
        }
        Collections.reverse(route);
        return removeCollinearPoints(route);
    }

    private List<Integer> boundedCoordinates(
            TreeSet<Integer> candidates,
            int start,
            int end) {
        final int maximumCoordinates = 32;
        if (candidates.size() <= maximumCoordinates) {
            return List.copyOf(candidates);
        }
        TreeSet<Integer> selected = new TreeSet<>();
        selected.add(candidates.first());
        selected.add(candidates.last());
        selected.add(start);
        selected.add(end);
        long midpoint = ((long) start + end) / 2;
        candidates.stream()
                .filter(value -> !selected.contains(value))
                .sorted(Comparator
                        .comparingLong((Integer value) -> Math.min(
                                Math.min(Math.abs((long) value - start),
                                        Math.abs((long) value - end)),
                                Math.abs((long) value - midpoint)))
                        .thenComparingInt(Integer::intValue))
                .limit(maximumCoordinates - selected.size())
                .forEach(selected::add);
        return List.copyOf(selected);
    }

    private Position gridPosition(
            GridState state,
            List<Integer> xCoordinates,
            List<Integer> yCoordinates) {
        return new Position(
                xCoordinates.get(state.xIndex()),
                yCoordinates.get(state.yIndex()));
    }

    private List<Position> removeCollinearPoints(List<Position> path) {
        if (path.size() < 3) {
            return path;
        }
        List<Position> simplified = new ArrayList<>();
        simplified.add(path.get(0));
        for (int index = 1; index < path.size() - 1; index++) {
            Position previous = simplified.get(simplified.size() - 1);
            Position current = path.get(index);
            Position next = path.get(index + 1);
            boolean collinear = previous.x() == current.x() && current.x() == next.x()
                    || previous.y() == current.y() && current.y() == next.y();
            if (!collinear) {
                simplified.add(current);
            }
        }
        simplified.add(path.get(path.size() - 1));
        return simplified;
    }

    private static List<Position> secondaryChannelInterior(
            Position start,
            Position end,
            FlowDirection flowDirection,
            int channel) {
        if (flowDirection == FlowDirection.TOP_TO_BOTTOM
                || flowDirection == FlowDirection.BOTTOM_TO_TOP) {
            return List.of(
                    new Position(channel, start.y()),
                    new Position(channel, end.y()));
        }
        return List.of(
                new Position(start.x(), channel),
                new Position(end.x(), channel));
    }

    /** Two interior bend points for an L-shaped orthogonal path at the given midpoint. */
    private static List<Position> twoPointInterior(Position start, Position end,
                                                    FlowDirection flowDir, int mid) {
        if (flowDir == FlowDirection.TOP_TO_BOTTOM || flowDir == FlowDirection.BOTTOM_TO_TOP) {
            return List.of(new Position(start.x(), mid), new Position(end.x(), mid));
        } else {
            return List.of(new Position(mid, start.y()), new Position(mid, end.y()));
        }
    }

    /**
     * Separated lane routing for parallel connections, fan-out, and fan-in.
     * Equivalent to the previous {@code buildOrthogonalPath} forward branch.
     */
    private void buildSeparatedLanedInterior(List<Position> path, Position start, Position end,
                                              FlowDirection flowDir, int vSpacing, int hSpacing,
                                              BoundingBox sourceBb, BoundingBox targetBb,
                                              int laneIndex, int laneCount,
                                              BoundingBox laneOwner) {
        if (laneCount > 1) {
            buildSeparatedForwardPath(path, start, end, flowDir, vSpacing, hSpacing,
                    laneIndex, laneCount, laneOwner);
        } else {
            // Single edge, forward, but via general orthogonal path (2 bends at midpoint).
            if (flowDir == FlowDirection.TOP_TO_BOTTOM
                    || flowDir == FlowDirection.BOTTOM_TO_TOP) {
                int midY = (start.y() + end.y()) / 2;
                path.add(new Position(start.x(), midY));
                path.add(new Position(end.x(), midY));
            } else {
                int midX = (start.x() + end.x()) / 2;
                path.add(new Position(midX, start.y()));
                path.add(new Position(midX, end.y()));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Dense fan-in support
    // -------------------------------------------------------------------------

    private List<String> denseTargetsAtRank(
            Map<String, List<LayoutEdge>> targetEdges,
            CoordinateAssignment coordinates,
            String targetId,
            FlowDirection flowDir) {
        BoundingBox targetBounds = coordinates.getBounds().get(targetId);
        boolean vertical = flowDir == FlowDirection.TOP_TO_BOTTOM
                || flowDir == FlowDirection.BOTTOM_TO_TOP;
        return targetEdges.entrySet().stream()
                .filter(entry -> entry.getValue().size() >= 3)
                .map(Map.Entry::getKey)
                .filter(id -> {
                    BoundingBox b = coordinates.getBounds().get(id);
                    return b != null && (vertical
                            ? b.y() == targetBounds.y()
                            : b.x() == targetBounds.x());
                })
                .sorted((l, r) -> {
                    BoundingBox lb = coordinates.getBounds().get(l);
                    BoundingBox rb = coordinates.getBounds().get(r);
                    return vertical
                            ? Integer.compare(lb.x(), rb.x())
                            : Integer.compare(lb.y(), rb.y());
                })
                .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------
    // Separated-lane fan-out / parallel routing
    // -------------------------------------------------------------------------

    private void buildSeparatedForwardPath(List<Position> path, Position start, Position end,
                                           FlowDirection flowDir, int vSpacing, int hSpacing,
                                           int laneIndex, int laneCount,
                                           BoundingBox laneOwner) {
        double centeredIndex = laneIndex - (laneCount - 1) / 2.0;
        if (flowDir == FlowDirection.TOP_TO_BOTTOM
                || flowDir == FlowDirection.BOTTOM_TO_TOP) {
            int direction = flowDir == FlowDirection.TOP_TO_BOTTOM ? 1 : -1;
            int stub = Math.max(10, Math.min(vSpacing / 3,
                    Math.abs(end.y() - start.y()) / 3));
            int laneX = laneOwner.center().x()
                    + (int) Math.round(centeredIndex
                            * (CONNECTION_LABEL_WIDTH + CONNECTION_LABEL_CLEARANCE));
            int sourceY = start.y() + direction * stub;
            int targetY = end.y() - direction * stub;
            path.add(new Position(start.x(), sourceY));
            path.add(new Position(laneX, sourceY));
            path.add(new Position(laneX, targetY));
            path.add(new Position(end.x(), targetY));
        } else {
            int direction = flowDir == FlowDirection.LEFT_TO_RIGHT ? 1 : -1;
            int stub = Math.max(10, Math.min(hSpacing / 3,
                    Math.abs(end.x() - start.x()) / 3));
            int laneY = laneOwner.center().y()
                    + (int) Math.round(centeredIndex
                            * (CONNECTION_LABEL_WIDTH + CONNECTION_LABEL_CLEARANCE));
            int sourceX = start.x() + direction * stub;
            int targetX = end.x() - direction * stub;
            path.add(new Position(sourceX, start.y()));
            path.add(new Position(sourceX, laneY));
            path.add(new Position(targetX, laneY));
            path.add(new Position(targetX, end.y()));
        }
    }

    // -------------------------------------------------------------------------
    // Anchor computation
    // -------------------------------------------------------------------------

    private Position getExitPoint(BoundingBox bb, FlowDirection flowDir, int index, int total,
                                   int portSpacing) {
        int offset = clampedOffset(index, total, portSpacing, bb, flowDir, true);
        return switch (flowDir) {
            case TOP_TO_BOTTOM -> new Position(bb.center().x() + offset, bb.bottom());
            case BOTTOM_TO_TOP -> new Position(bb.center().x() + offset, bb.y());
            case LEFT_TO_RIGHT -> new Position(bb.right(), bb.center().y() + offset);
            case RIGHT_TO_LEFT -> new Position(bb.x(), bb.center().y() + offset);
        };
    }

    private Position getEntryPoint(BoundingBox bb, FlowDirection flowDir, int index, int total,
                                    int portSpacing) {
        int offset = clampedOffset(index, total, portSpacing, bb, flowDir, false);
        return switch (flowDir) {
            case TOP_TO_BOTTOM -> new Position(bb.center().x() + offset, bb.y());
            case BOTTOM_TO_TOP -> new Position(bb.center().x() + offset, bb.bottom());
            case LEFT_TO_RIGHT -> new Position(bb.x(), bb.center().y() + offset);
            case RIGHT_TO_LEFT -> new Position(bb.right(), bb.center().y() + offset);
        };
    }

    private boolean shouldUseLateralBranchExit(
            LayoutEdge edge,
            BoundingBox source,
            BoundingBox target,
            FlowDirection flowDirection,
            Map<String, List<LayoutEdge>> sourceEdges,
            LayoutGraph graph) {
        List<LayoutEdge> outgoing =
                sourceEdges.getOrDefault(edge.getSourceNodeId(), List.of());
        if (outgoing.size() < 2
                || outgoing.stream().map(LayoutEdge::getSourcePort).distinct().count() < 2
                || !graph.getOutgoingEdges(edge.getTargetNodeId()).isEmpty()) {
            return false;
        }

        int dx = target.center().x() - source.center().x();
        int dy = target.center().y() - source.center().y();
        return switch (flowDirection) {
            case TOP_TO_BOTTOM, BOTTOM_TO_TOP -> Math.abs(dx) > source.width() / 2;
            case LEFT_TO_RIGHT, RIGHT_TO_LEFT -> Math.abs(dy) > source.height() / 2;
        };
    }

    private Position getLateralExitPoint(
            BoundingBox source,
            BoundingBox target,
            FlowDirection flowDirection) {
        return switch (flowDirection) {
            case TOP_TO_BOTTOM, BOTTOM_TO_TOP -> target.center().x() >= source.center().x()
                    ? new Position(source.right(), source.center().y())
                    : new Position(source.x(), source.center().y());
            case LEFT_TO_RIGHT, RIGHT_TO_LEFT -> target.center().y() >= source.center().y()
                    ? new Position(source.center().x(), source.bottom())
                    : new Position(source.center().x(), source.y());
        };
    }

    /**
     * Computes the port offset and clamps it so it stays within the component
     * boundary, preventing fan-out/fan-in anchors from escaping the component
     * for very high degrees.
     */
    private static int clampedOffset(int index, int total, int portSpacing,
                                      BoundingBox bb, FlowDirection flowDir, boolean isExit) {
        int raw = (int) ((index - (total - 1) / 2.0) * portSpacing);
        boolean lateral = flowDir == FlowDirection.TOP_TO_BOTTOM
                || flowDir == FlowDirection.BOTTOM_TO_TOP;
        int halfDim = lateral ? bb.width() / 2 : bb.height() / 2;
        int margin = 4; // minimum inset from component edge
        return Math.max(-(halfDim - margin), Math.min(halfDim - margin, raw));
    }
}