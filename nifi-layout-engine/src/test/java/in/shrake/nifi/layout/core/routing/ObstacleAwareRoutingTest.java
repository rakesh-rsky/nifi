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
package in.shrake.nifi.layout.core.routing;

import in.shrake.nifi.layout.core.LayoutEngine;
import in.shrake.nifi.layout.core.fixture.FlowTopologyFixtures;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayoutEdge;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.RoutingResult;
import in.shrake.nifi.layout.core.model.RoutingWarning;
import in.shrake.nifi.layout.core.router.DirectRouter;
import in.shrake.nifi.layout.core.router.OrthogonalRouter;
import in.shrake.nifi.layout.core.router.RoutingStrategy;
import in.shrake.nifi.layout.core.service.SubgraphPacker;
import in.shrake.nifi.layout.core.util.Geometry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 regression tests for obstacle-aware deterministic routing.
 *
 * <p>Each test covers one requirement from the routing tiers:
 * self-loops, backedges, dense fan-in, clear corridors, obstacle avoidance,
 * fan-out lanes, parallel labels, fallback warnings, REST bend extraction,
 * and cross-direction coverage.
 */
class ObstacleAwareRoutingTest {

    private static final LayoutOptions DEFAULTS = LayoutOptions.defaults();

    // -------------------------------------------------------------------------
    // 1. Every routed path has at least 2 points (full-path semantics)
    // -------------------------------------------------------------------------

    @Test
    void everyRoutedPathHasAtLeastTwoPoints() {
        LayoutGraph graph = FlowTopologyFixtures.linearPipeline();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        result.getConnectionBendPoints().forEach((id, path) ->
            assertTrue(path.size() >= 2,
                    "path for " + id + " must have ≥ 2 points (src + dst anchors)"));
    }

    @Test
    void terminalBranchesExitTowardTheirLateralTargets() {
        LayoutGraph graph = FlowTopologyFixtures.csvPostgresBranches();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);
        Map<String, BoundingBox> bounds = boundsMap(result);

        assertLateralSourceAnchor(
                result, bounds, "validate-failure", "validate-record", "validate-failure");
        assertLateralSourceAnchor(
                result, bounds, "database-archive", "put-database-record", "archive");

        List<Position> databaseFailure =
                result.getConnectionBendPoints().get("database-failure");
        BoundingBox database = bounds.get("put-database-record");
        assertEquals(database.bottom(), databaseFailure.getFirst().y(),
                "vertically aligned failure branch should retain the primary-flow exit");
    }

    private static void assertLateralSourceAnchor(
            LayoutResult result,
            Map<String, BoundingBox> bounds,
            String edgeId,
            String sourceId,
            String targetId) {
        List<Position> path = result.getConnectionBendPoints().get(edgeId);
        BoundingBox source = bounds.get(sourceId);
        BoundingBox target = bounds.get(targetId);
        Position anchor = path.getFirst();

        int expectedX = target.center().x() >= source.center().x()
                ? source.right() : source.x();
        assertEquals(expectedX, anchor.x(), edgeId + " should exit the source-facing side");
        assertEquals(source.center().y(), anchor.y(),
                edgeId + " should use the source side-center anchor");
    }

    // -------------------------------------------------------------------------
    // 2. DirectRouter emits two-point path → REST persists zero bends
    // -------------------------------------------------------------------------

    @Test
    void directRouterEmitsTwoPointPathAndRestPersistsZeroBends() {
        LayoutNode source = node("source", 0, 0, 100, 80);
        LayoutNode target = node("target", 0, 300, 100, 80);
        LayoutGraph graph = graph(
                map("source", source, "target", target),
                map("edge", fwdEdge("edge", "source", "target")));
        CoordinateAssignment coords = coordsFromGraph(graph);

        DirectRouter router = new DirectRouter();
        RoutingResult result = router.computeRoutes(graph, coords, DEFAULTS);

        result.getEdgePaths().forEach((id, path) -> {
            assertEquals(2, path.size(),
                    "DirectRouter must emit exactly [src, dst] for " + id);
            // Simulate REST writer: strip first and last anchor.
            List<Position> bends = path.size() <= 2 ? List.of()
                    : path.subList(1, path.size() - 1);
            assertTrue(bends.isEmpty(),
                    "REST bends must be empty for direct route " + id);
        });
        assertTrue(result.getWarnings().isEmpty(),
                "DirectRouter must not emit warnings");
    }

    // -------------------------------------------------------------------------
    // 3. Blocked corridor → obstacle-aware routing finds clear alternative
    // -------------------------------------------------------------------------

    @Test
    void blockedDirectRouteAvoidsObstacleWithNoWarning() {
        // source (0,0,100,80), target (300,400,100,80)
        // obstacle (200,150,100,80) – blocks the default midpoint corridor
        LayoutNode source = node("source", 0, 0, 100, 80);
        LayoutNode target = node("target", 300, 400, 100, 80);
        LayoutNode obstacle = node("obstacle", 200, 150, 100, 80);
        LayoutEdge edge = fwdEdge("e1", "source", "target");

        LayoutGraph graph = graph(
                map("source", source, "target", target, "obstacle", obstacle),
                map("e1", edge));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.TOP_TO_BOTTOM).build();

        OrthogonalRouter router = new OrthogonalRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);

        List<Position> path = result.getEdgePaths().get("e1");
        assertNotNull(path);
        assertTrue(path.size() >= 2, "path must have at least src and dst");

        // Interior segments (skipping first/last) must not cross the obstacle.
        assertNoInteriorIntersection(path, obstacle.getBoundingBox());
        assertTrue(result.getWarnings().isEmpty(),
                "no warning when a clear alternative exists; warnings: " + result.getWarnings());
    }

    // -------------------------------------------------------------------------
    // 4. Fully blocked corridor → fallback route + warning emitted
    // -------------------------------------------------------------------------

    @Test
    void blockedRouteWithNoAlternativeEmitsWarning() {
        // obstacle covers entire vertical corridor between source and target
        LayoutNode source = node("source", 0, 0, 100, 80);
        LayoutNode target = node("target", 300, 400, 100, 80);
        LayoutNode obstacle = node("obstacle", 0, 80, 400, 330);
        LayoutEdge edge = fwdEdge("e1", "source", "target");

        LayoutGraph graph = graph(
                map("source", source, "target", target, "obstacle", obstacle),
                map("e1", edge));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.TOP_TO_BOTTOM).build();

        OrthogonalRouter router = new OrthogonalRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);

        List<Position> path = result.getEdgePaths().get("e1");
        assertNotNull(path);
        assertTrue(path.size() >= 2);
        assertFalse(result.getWarnings().isEmpty(),
                "a warning must be emitted when no clear route exists");
        assertTrue(result.getWarnings().stream().anyMatch(w -> w.edgeId().equals("e1")),
                "warning must reference edge 'e1'");
    }

    // -------------------------------------------------------------------------
    // 5. Self-loop interior bends are outside the component bounds
    // -------------------------------------------------------------------------

    @Test
    void selfLoopBendsAreOutsideComponentBounds() {
        LayoutGraph graph = FlowTopologyFixtures.cycleWithSelfLoop();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        Map<String, BoundingBox> bounds = boundsMap(result);
        List<Position> path = result.getConnectionBendPoints().get("a-a");
        assertNotNull(path, "self-loop path must be present");
        assertTrue(path.size() >= 2);

        BoundingBox compBounds = bounds.get("a");
        assertNotNull(compBounds, "component 'a' must have layout bounds");

        // Interior bend points (indices 1..size-2) must not be strictly inside the component.
        for (int i = 1; i < path.size() - 1; i++) {
            assertNotStrictlyInside(path.get(i), compBounds,
                    "self-loop interior bend[" + i + "]");
        }
    }

    // -------------------------------------------------------------------------
    // 6. Backedge routes outside the occupied graph boundary
    // -------------------------------------------------------------------------

    @Test
    void backedgeIsRoutedOutsideGraphBounds() {
        LayoutGraph graph = FlowTopologyFixtures.cycleWithSelfLoop();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        Map<String, BoundingBox> allBounds = boundsMap(result);
        BoundingBox graphBounds = allBounds.values().stream()
                .reduce(null, Geometry::union);
        assertNotNull(graphBounds);

        // c-a is the topologically backward edge in the cycle.
        List<Position> path = result.getConnectionBendPoints().get("c-a");
        assertNotNull(path, "backedge c-a must be routed");
        assertTrue(path.size() >= 6,
                "backedge full path must have src + ≥4 bends + dst, got " + path.size());

        // At least one interior bend must be outside the occupied graph bounding box.
        boolean anyOutside = false;
        for (int i = 1; i < path.size() - 1; i++) {
            Position p = path.get(i);
            if (p.x() > graphBounds.right() || p.x() < graphBounds.x()
                    || p.y() > graphBounds.bottom() || p.y() < graphBounds.y()) {
                anyOutside = true;
                break;
            }
        }
        assertTrue(anyOutside,
                "at least one interior bend must be outside the occupied graph bounds");
        List<Position> selfLoopPath = result.getConnectionBendPoints().get("a-a");
        assertTrue(Math.abs(path.get(2).x() - selfLoopPath.get(2).x()) >= 240,
                "self-loops and backedges must use distinct exterior label lanes");
    }

    // -------------------------------------------------------------------------
    // 7. Two reversed backedges use distinct external lane positions
    // -------------------------------------------------------------------------

    @Test
    void multipleBackedgesUseDifferentLanes() {
        LayoutNode a = node("a", 0, 0, 100, 80);
        LayoutNode b = node("b", 0, 200, 100, 80);
        LayoutNode c = node("c", 0, 400, 100, 80);
        LayoutEdge ba = revEdge("b-a", "b", "a");
        LayoutEdge ca = revEdge("c-a", "c", "a");

        LayoutGraph graph = graph(
                map("a", a, "b", b, "c", c),
                map("b-a", ba, "c-a", ca));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.TOP_TO_BOTTOM).build();

        OrthogonalRouter router = new OrthogonalRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);

        List<Position> pathBA = result.getEdgePaths().get("b-a");
        List<Position> pathCA = result.getEdgePaths().get("c-a");
        assertNotNull(pathBA);
        assertNotNull(pathCA);

        // Interior index 1 (full index 2) carries the sideX column for the exterior lane.
        int sideBA = pathBA.get(2).x();
        int sideCA = pathCA.get(2).x();
        assertNotEquals(sideBA, sideCA,
                "two backedges must use different external lane positions; both got " + sideBA);
        assertTrue(Math.abs(sideBA - sideCA) >= 240,
                "parallel backedge label lanes must be at least 240px apart");
    }

    // -------------------------------------------------------------------------
    // 8. Fan-out lanes maintain ≥240 px label gap
    // -------------------------------------------------------------------------

    @Test
    void fanOutLanesMaintainMinimumLabelGap() {
        LayoutGraph graph = FlowTopologyFixtures.wideDistribution();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        // Only collect paths that use the separated-lane topology (size ≥ 6).
        // Path = [src, stub, laneX-corner, laneX-corner2, stub2, dst]; laneX is at index 2.
        List<Integer> laneXs = new ArrayList<>();
        for (List<Position> path : result.getConnectionBendPoints().values()) {
            if (path.size() >= 6) {
                laneXs.add(path.get(2).x());
            }
        }
        laneXs.sort(Integer::compareTo);

        // If any lanes were assigned, adjacent ones must be ≥240 px apart.
        for (int i = 1; i < laneXs.size(); i++) {
            int gap = laneXs.get(i) - laneXs.get(i - 1);
            assertTrue(gap >= 240,
                    "adjacent label lanes must be ≥240 px apart, got " + gap
                            + " between sorted lane " + (i - 1) + " and " + i);
        }
    }

    // -------------------------------------------------------------------------
    // 9. Dense fan-in: all paths are routed
    // -------------------------------------------------------------------------

    @Test
    void denseFanInAllPathsAreRouted() {
        LayoutGraph graph = FlowTopologyFixtures.denseFanIn();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        assertEquals(4, result.getConnectionBendPoints().size(),
                "all four dense fan-in connections must be routed");
        result.getConnectionBendPoints().forEach((id, path) ->
                assertTrue(path.size() >= 2,
                        "dense fan-in path " + id + " must have ≥2 points"));
    }

    // -------------------------------------------------------------------------
    // 10. Parallel labels maintain ≥240 px gap
    // -------------------------------------------------------------------------

    @Test
    void parallelLabelLanesMaintainMinimumGap() {
        LayoutGraph graph = FlowTopologyFixtures.parallelRelationships();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        // Parallel edges produce 6-point paths; label anchor x is at index 2.
        List<Integer> laneXs = result.getConnectionBendPoints().values().stream()
                .filter(path -> path.size() >= 6)
                .map(path -> path.get(2).x())
                .sorted()
                .toList();

        assertEquals(3, laneXs.size(),
                "three parallel edges must each produce a separated-lane path");
        assertTrue(laneXs.get(1) - laneXs.get(0) >= 240,
                "gap between lanes 0 and 1 must be ≥240px, got "
                        + (laneXs.get(1) - laneXs.get(0)));
        assertTrue(laneXs.get(2) - laneXs.get(1) >= 240,
                "gap between lanes 1 and 2 must be ≥240px, got "
                        + (laneXs.get(2) - laneXs.get(1)));
    }

    // -------------------------------------------------------------------------
    // 11. Funnel flow: all three connections are routed
    // -------------------------------------------------------------------------

    @Test
    void funnelFlowAllConnectionsAreRouted() {
        LayoutGraph graph = FlowTopologyFixtures.funnelFlow();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        assertEquals(3, result.getConnectionBendPoints().size(),
                "all three funnel connections must be routed");
        result.getConnectionBendPoints().forEach((id, path) ->
                assertTrue(path.size() >= 2,
                        "funnel path " + id + " must have ≥2 points"));
    }

    // -------------------------------------------------------------------------
    // 12. Disconnected subgraphs: both edges are independently routed
    // -------------------------------------------------------------------------

    @Test
    void disconnectedSubgraphsAreIndependentlyRouted() {
        LayoutGraph graph = FlowTopologyFixtures.disconnectedSubgraphs();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        assertTrue(result.getConnectionBendPoints().containsKey("a-b"),
                "edge a-b must be routed");
        assertTrue(result.getConnectionBendPoints().containsKey("c-d"),
                "edge c-d must be routed");
    }

    // -------------------------------------------------------------------------
    // 13. All four flow directions produce valid paths
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(FlowDirection.class)
    void allFlowDirectionsProduceValidPaths(FlowDirection dir) {
        LayoutGraph graph = FlowTopologyFixtures.twoWayFanOut();
        LayoutResult result = LayoutEngine.create().layout(graph,
                LayoutOptions.builder().flowDirection(dir).build());

        result.getConnectionBendPoints().forEach((edgeId, path) ->
                assertTrue(path.size() >= 2,
                        dir + " path for " + edgeId + " must have ≥2 points"));
    }

    // -------------------------------------------------------------------------
    // 14. Routing is deterministic: two runs produce identical results
    // -------------------------------------------------------------------------

    @Test
    void routingIsDeterministic() {
        LayoutGraph graph = FlowTopologyFixtures.failureBranches();
        LayoutResult first = LayoutEngine.create().layout(graph, DEFAULTS);
        LayoutResult second = LayoutEngine.create().layout(graph, DEFAULTS);

        first.getConnectionBendPoints().forEach((id, path1) -> {
            List<Position> path2 = second.getConnectionBendPoints().get(id);
            assertEquals(path1, path2,
                    "path for edge " + id + " must be identical on repeat invocation");
        });
    }

    // -------------------------------------------------------------------------
    // 15. Full path: first and last points are NOT strictly inside their components
    // -------------------------------------------------------------------------

    @Test
    void fullPathEndpointsAreNotInsideComponents() {
        LayoutGraph graph = FlowTopologyFixtures.linearPipeline();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);
        Map<String, BoundingBox> bounds = boundsMap(result);

        graph.getEdges().forEach((edgeId, edge) -> {
            List<Position> path = result.getConnectionBendPoints().get(edgeId);
            if (path == null || path.size() < 2) return;
            BoundingBox srcBounds = bounds.get(edge.getSourceNodeId());
            BoundingBox dstBounds = bounds.get(edge.getTargetNodeId());
            assertNotStrictlyInside(path.get(0), srcBounds,
                    edgeId + " src anchor must not be inside source component");
            assertNotStrictlyInside(path.get(path.size() - 1), dstBounds,
                    edgeId + " dst anchor must not be inside target component");
        });
    }

    // -------------------------------------------------------------------------
    // 16. REST writer strips endpoints → only interior bends persisted
    // -------------------------------------------------------------------------

    @Test
    void restInteriorBendsExcludeEndpointAnchors() {
        LayoutGraph graph = FlowTopologyFixtures.parallelRelationships();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        result.getConnectionBendPoints().forEach((id, fullPath) -> {
            List<Position> bends = fullPath.size() <= 2 ? List.of()
                    : fullPath.subList(1, fullPath.size() - 1);
            assertEquals(fullPath.size() - 2, bends.size(),
                    "REST bends for " + id + " must equal full-path size - 2");
        });
    }

    // -------------------------------------------------------------------------
    // 17. Geometry.segmentIntersectsBounds strict-boundary semantics
    // -------------------------------------------------------------------------

    @Test
    void geometrySegmentEndingExactlyOnBoundaryIsNotFlagged() {
        BoundingBox box = new BoundingBox(100, 100, 100, 100);
        Position a = new Position(50, 150);
        Position b = new Position(100, 150); // endpoint on box.x exactly
        assertFalse(Geometry.segmentIntersectsBounds(a, b, box, 0),
                "segment whose endpoint exactly touches box edge must not be flagged");
    }

    @Test
    void geometrySegmentPassingThroughBoxIsFlagged() {
        BoundingBox box = new BoundingBox(100, 100, 100, 100);
        Position a = new Position(50, 150);
        Position b = new Position(250, 150); // passes through the box
        assertTrue(Geometry.segmentIntersectsBounds(a, b, box, 0),
                "segment through box must be flagged");
    }

    @Test
    void geometryPathLengthIsManhattanDistance() {
        List<Position> path = List.of(
                new Position(0, 0),
                new Position(100, 0),
                new Position(100, 50));
        assertEquals(150, Geometry.pathLength(path));
    }

    @Test
    void geometryOccupiedBoundsExcludesSpecifiedIds() {
        Map<String, BoundingBox> allBounds = new LinkedHashMap<>();
        allBounds.put("a", new BoundingBox(0, 0, 100, 100));
        allBounds.put("b", new BoundingBox(200, 200, 100, 100));
        BoundingBox result = Geometry.occupiedBounds(allBounds, java.util.Set.of("b"));
        assertNotNull(result);
        assertEquals(new BoundingBox(0, 0, 100, 100), result);
    }

    @Test
    void geometryUnionOfTwoBoxesIsCorrect() {
        BoundingBox a = new BoundingBox(0, 0, 100, 100);
        BoundingBox b = new BoundingBox(150, 50, 100, 100);
        BoundingBox union = Geometry.union(a, b);
        assertEquals(0, union.x());
        assertEquals(0, union.y());
        assertEquals(250, union.right());
        assertEquals(150, union.bottom());
    }

    // -------------------------------------------------------------------------
    // 18. Fix 4 regression: interior-vertex obstacle-boundary touch is detected
    // -------------------------------------------------------------------------

    @Test
    void pathInteriorVertexOnObstacleBoundaryIsDetected() {
        // Path [(0,150),(100,150),(0,50)]: interior vertex (100,150) lies exactly
        // on the left boundary of box(100,100,100,100).
        // pathIntersectsAnyObstacle must flag this – it is NOT a path-endpoint touch.
        BoundingBox box = new BoundingBox(100, 100, 100, 100);
        List<Position> path = List.of(
                new Position(0, 150),
                new Position(100, 150),   // interior vertex on left boundary
                new Position(0, 50));
        assertTrue(Geometry.pathIntersectsAnyObstacle(path, List.of(box), 0),
                "path whose interior vertex lies exactly on obstacle boundary must be detected");
    }

    @Test
    void twoPointPathEndpointTouchRemainsExempt() {
        // A 2-point path [p0, p1] whose END is exactly on the box boundary must NOT be
        // flagged – p1 is the full-path last point (path endpoint) and stays exempt.
        BoundingBox box = new BoundingBox(100, 100, 100, 100);
        List<Position> path = List.of(
                new Position(0, 150),
                new Position(100, 150));   // endpoint on box.x exactly
        assertFalse(Geometry.pathIntersectsAnyObstacle(path, List.of(box), 0),
                "two-point path whose END touches obstacle boundary must remain exempt");
    }

    // -------------------------------------------------------------------------
    // 19. Fix 1: fan-out route through obstacle is rerouted or warned
    // -------------------------------------------------------------------------

    @Test
    void fanOutRouteThroughObstacleIsReroutedOrWarned() {
        // Fan-out: source → targetA, source → targetB.
        // obstacle is placed so the preferred fan-out laneX=180 path goes through it.
        // After Fix 1 the router must either reroute the path or emit a warning.
        LayoutNode source   = node("source",    0,   0, 100,  80);
        LayoutNode targetA  = node("targetA",   0, 400, 100,  80);
        LayoutNode targetB  = node("targetB",  400, 400, 100,  80);
        LayoutNode obstacle = node("obstacle", 160,  80, 100, 300);  // blocks laneX≈180

        LayoutEdge e1 = fwdEdge("src-ta", "source", "targetA");
        LayoutEdge e2 = fwdEdge("src-tb", "source", "targetB");

        LayoutGraph graph = graph(
                map("source", source, "targetA", targetA, "targetB", targetB,
                        "obstacle", obstacle),
                map("src-ta", e1, "src-tb", e2));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.TOP_TO_BOTTOM).build();

        OrthogonalRouter router = new OrthogonalRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);

        for (String edgeId : List.of("src-ta", "src-tb")) {
            List<Position> path = result.getEdgePaths().get(edgeId);
            assertNotNull(path, edgeId + " must be routed");
            assertTrue(path.size() >= 2, edgeId + " must have ≥2 points");

            // The final path must not silently cross the obstacle without a warning.
            boolean intersects = Geometry.pathIntersectsAnyObstacle(
                    path, List.of(obstacle.getBoundingBox()), 0);
            boolean warned = result.getWarnings().stream()
                    .anyMatch(w -> w.edgeId().equals(edgeId));
            assertTrue(!intersects || warned,
                    edgeId + ": path intersects obstacle but no warning was emitted "
                            + "(Fix 1 regression – preferred laned path was not validated)");
        }
    }

    // -------------------------------------------------------------------------
    // 20. Fix 2: horizontal-flow parallel lanes maintain ≥240 px gap
    // -------------------------------------------------------------------------

    @Test
    void horizontalParallelLanesMaintainMinimumGap() {
        LayoutGraph graph = FlowTopologyFixtures.parallelRelationships();

        for (FlowDirection dir : new FlowDirection[]{
                FlowDirection.LEFT_TO_RIGHT, FlowDirection.RIGHT_TO_LEFT}) {
            LayoutResult result = LayoutEngine.create().layout(graph,
                    LayoutOptions.builder().flowDirection(dir).build());

            // For horizontal flow the label anchor Y is at full-path index 2.
            List<Integer> laneYs = result.getConnectionBendPoints().values().stream()
                    .filter(p -> p.size() >= 6)
                    .map(p -> p.get(2).y())
                    .sorted()
                    .toList();

            for (int i = 1; i < laneYs.size(); i++) {
                int gap = Math.abs(laneYs.get(i) - laneYs.get(i - 1));
                assertTrue(gap >= 240,
                        dir + ": adjacent horizontal label lanes must be ≥240 px apart, got " + gap);
            }
        }
    }

    // -------------------------------------------------------------------------
    // 21. Fix 3: non-reversed visually-backward multi-edge → distinct lanes
    // -------------------------------------------------------------------------

    @Test
    void visuallyBackwardNonReversedMultiEdgesGetDistinctLanes() {
        // Two forward (isReversed=false) edges whose source is BELOW both targets
        // in TOP_TO_BOTTOM flow → both are visually backward.
        // After Fix 3 they must receive distinct exterior lane x-positions.
        LayoutNode source  = node("source",   0, 400, 100,  80);
        LayoutNode targetA = node("targetA",  0,   0, 100,  80);
        LayoutNode targetB = node("targetB", 200,  0, 100,  80);
        LayoutEdge e1 = fwdEdge("s-ta", "source", "targetA");
        LayoutEdge e2 = fwdEdge("s-tb", "source", "targetB");

        LayoutGraph graph = graph(
                map("source", source, "targetA", targetA, "targetB", targetB),
                map("s-ta", e1, "s-tb", e2));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.TOP_TO_BOTTOM).build();

        OrthogonalRouter router = new OrthogonalRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);

        List<Position> pathTA = result.getEdgePaths().get("s-ta");
        List<Position> pathTB = result.getEdgePaths().get("s-tb");
        assertNotNull(pathTA, "s-ta must be routed");
        assertNotNull(pathTB, "s-tb must be routed");
        assertTrue(pathTA.size() >= 4, "backedge s-ta must have ≥4 points");
        assertTrue(pathTB.size() >= 4, "backedge s-tb must have ≥4 points");

        // The exterior sideX column is at full-path index 2 for TOP_TO_BOTTOM backedges.
        int sideTA = pathTA.get(2).x();
        int sideTB = pathTB.get(2).x();
        assertNotEquals(sideTA, sideTB,
                "visually-backward non-reversed edges must use distinct exterior lane x-positions; "
                        + "both got x=" + sideTA + " (Fix 3 regression)");
    }

    // -------------------------------------------------------------------------
    // 22. Fix 5: DirectRouter backward edge exits/enters on facing boundary
    // -------------------------------------------------------------------------

    @Test
    void directRouterBackwardLtrEdgeUsesGeometricAnchors() {
        // LEFT_TO_RIGHT flow: source is to the RIGHT of target → backward edge.
        // Exit must be from source.left; entry must be at target.right.
        LayoutNode source = node("source", 300, 100, 100, 80);
        LayoutNode target = node("target", 100, 100, 100, 80);
        LayoutEdge edge   = fwdEdge("e1", "source", "target");

        LayoutGraph graph = graph(map("source", source, "target", target), map("e1", edge));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.LEFT_TO_RIGHT).build();

        DirectRouter router = new DirectRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);
        List<Position> path = result.getEdgePaths().get("e1");

        assertNotNull(path);
        assertEquals(2, path.size(), "DirectRouter must emit exactly [src, dst]");
        assertEquals(source.getBoundingBox().x(), path.get(0).x(),
                "backward LTR edge must exit from source.left (x=" + source.getBoundingBox().x() + ")");
        assertEquals(target.getBoundingBox().right(), path.get(1).x(),
                "backward LTR edge must enter at target.right (x=" + target.getBoundingBox().right() + ")");
    }

    @Test
    void directRouterForwardLtrEdgeUsesConfiguredAnchors() {
        // LEFT_TO_RIGHT flow: source is to the LEFT of target → forward edge.
        // Exit must be from source.right; entry at target.left.
        LayoutNode source = node("source", 100, 100, 100, 80);
        LayoutNode target = node("target", 300, 100, 100, 80);
        LayoutEdge edge   = fwdEdge("e1", "source", "target");

        LayoutGraph graph = graph(map("source", source, "target", target), map("e1", edge));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.LEFT_TO_RIGHT).build();

        DirectRouter router = new DirectRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);
        List<Position> path = result.getEdgePaths().get("e1");

        assertNotNull(path);
        assertEquals(2, path.size());
        assertEquals(source.getBoundingBox().right(), path.get(0).x(),
                "forward LTR edge must exit from source.right");
        assertEquals(target.getBoundingBox().x(), path.get(1).x(),
                "forward LTR edge must enter at target.left");
    }

    @Test
    void directRouterBackwardTtbEdgeUsesGeometricAnchors() {
        // TOP_TO_BOTTOM flow: source is BELOW target (higher y) → backward edge.
        // Exit must be from source.top (source.y); entry at target.bottom.
        LayoutNode source = node("source", 100, 400, 100, 80);
        LayoutNode target = node("target", 100, 100, 100, 80);
        LayoutEdge edge   = fwdEdge("e1", "source", "target");

        LayoutGraph graph = graph(map("source", source, "target", target), map("e1", edge));
        CoordinateAssignment coords = coordsFrom(graph);
        LayoutOptions opts = LayoutOptions.builder()
                .flowDirection(FlowDirection.TOP_TO_BOTTOM).build();

        DirectRouter router = new DirectRouter();
        RoutingResult result = router.computeRoutes(graph, coords, opts);
        List<Position> path = result.getEdgePaths().get("e1");

        assertNotNull(path);
        assertEquals(2, path.size());
        assertEquals(source.getBoundingBox().y(), path.get(0).y(),
                "backward TTB edge must exit from source.top (y=" + source.getBoundingBox().y() + ")");
        assertEquals(target.getBoundingBox().bottom(), path.get(1).y(),
                "backward TTB edge must enter at target.bottom (y=" + target.getBoundingBox().bottom() + ")");
    }

    // -------------------------------------------------------------------------
    // 23. Fix 6: SubgraphPacker propagates routing warnings from each subgraph
    // -------------------------------------------------------------------------

    @Test
    void subgraphPackerPropagatesWarnings() {
        // Build two minimal LayoutResults: one carries a warning, the other does not.
        // SubgraphPacker.pack must aggregate warnings from both.
        Map<String, List<Position>> emptyRoutes = Map.of();
        List<ComponentUpdate> emptyUpdates = List.of();

        LayoutResult withWarning = new LayoutResult(
                emptyUpdates, 0, emptyRoutes, 0,
                List.of("edge blocked-edge: no clear route"));
        LayoutResult clean = new LayoutResult(
                emptyUpdates, 0, emptyRoutes, 0, List.of());

        BoundingBox dummyBounds = new BoundingBox(0, 0, 100, 100);
        SubgraphPacker packer = new SubgraphPacker();
        LayoutResult packed = packer.pack(
                List.of(
                        new SubgraphPacker.SubgraphLayout(withWarning, "a", 1, dummyBounds),
                        new SubgraphPacker.SubgraphLayout(clean,       "z", 1, dummyBounds)),
                LayoutOptions.defaults());

        assertFalse(packed.getWarnings().isEmpty(),
                "SubgraphPacker.pack must propagate warnings from packed subgraphs");
        assertTrue(packed.getWarnings().stream().anyMatch(w -> w.contains("blocked-edge")),
                "warning text from subgraph must survive into packed result");
    }

    @Test
    void blockedParallelRoutesPreserveLabelLaneSeparation() {
        LayoutNode source = node("source", 0, 0, 100, 80);
        LayoutNode target = node("target", 0, 500, 100, 80);
        LayoutNode obstacle = node("obstacle", -100, 100, 300, 300);
        LayoutGraph graph = graph(
                map("source", source, "target", target, "obstacle", obstacle),
                map(
                        "edge-a", fwdEdge("edge-a", "source", "target"),
                        "edge-b", fwdEdge("edge-b", "source", "target"),
                        "edge-c", fwdEdge("edge-c", "source", "target")));

        RoutingResult result = new OrthogonalRouter().computeRoutes(
                graph, coordsFrom(graph), DEFAULTS);
        List<Integer> laneXs = result.getEdgePaths().values().stream()
                .map(path -> path.get(2).x())
                .sorted()
                .toList();

        assertEquals(3, laneXs.size());
        assertTrue(laneXs.get(1) - laneXs.get(0) >= 240);
        assertTrue(laneXs.get(2) - laneXs.get(1) >= 240);
    }

    @Test
    void blockedBackedgeUsesClearOppositeExteriorBoundary() {
        LayoutNode source = node("source", 0, 400, 100, 80);
        LayoutNode target = node("target", 0, 0, 100, 80);
        LayoutNode obstacle = node("obstacle", 120, 490, 100, 100);
        LayoutGraph graph = graph(
                map("source", source, "target", target, "obstacle", obstacle),
                map("backedge", revEdge("backedge", "source", "target")));

        RoutingResult result = new OrthogonalRouter().computeRoutes(
                graph, coordsFrom(graph), DEFAULTS);
        List<Position> path = result.getEdgePaths().get("backedge");
        BoundingBox occupied = Geometry.occupiedBounds(
                coordsFrom(graph).getBounds(), java.util.Set.of());

        assertTrue(path.get(2).x() < occupied.x()
                || path.get(2).x() > occupied.right());
        assertFalse(Geometry.pathIntersectsAnyObstacle(
                path, List.of(obstacle.getBoundingBox()), 10));
        assertTrue(result.getWarnings().isEmpty());
    }

    @Test
    void clearFanRoutesRemainOrthogonal() {
        LayoutGraph graph = FlowTopologyFixtures.twoWayFanOut();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);

        result.getConnectionBendPoints().forEach((edgeId, path) -> {
            for (int index = 0; index < path.size() - 1; index++) {
                Position first = path.get(index);
                Position second = path.get(index + 1);
                assertTrue(first.x() == second.x() || first.y() == second.y(),
                        edgeId + " contains a diagonal segment");
            }
        });
    }

    @Test
    void canonicalDenseFanInUsesSharedBus() {
        LayoutGraph graph = FlowTopologyFixtures.denseFanIn();
        LayoutResult result = LayoutEngine.create().layout(graph, DEFAULTS);
        List<Integer> busCoordinates = result.getConnectionBendPoints().values().stream()
                .map(path -> path.get(2).y())
                .distinct()
                .toList();

        assertEquals(1, busCoordinates.size());
        result.getConnectionBendPoints().values()
                .forEach(path -> assertEquals(5, path.size()));
    }

    @Test
    void incrementalReroutingReplacesAffectedWarnings() {
        LayoutGraph graph = FlowTopologyFixtures.disconnectedSubgraphs();
        RoutingStrategy warningRouter = (currentGraph, coordinates, options) -> {
            Map<String, List<Position>> routes = new LinkedHashMap<>();
            List<RoutingWarning> warnings = new ArrayList<>();
            currentGraph.getEdges().values().forEach(edge -> {
                BoundingBox source = coordinates.getBounds().get(edge.getSourceNodeId());
                BoundingBox target = coordinates.getBounds().get(edge.getTargetNodeId());
                routes.put(edge.getId(), List.of(source.center(), target.center()));
                warnings.add(new RoutingWarning(edge.getId(), "blocked"));
            });
            return new RoutingResult(routes, warnings);
        };

        LayoutResult result = LayoutEngine.builder()
                .withRoutingStrategy(warningRouter)
                .build()
                .layoutIncremental(graph, DEFAULTS, java.util.Set.of("a"));

        assertEquals(1, result.getWarnings().stream()
                .filter(warning -> warning.startsWith("edge a-b:"))
                .count());
        assertEquals(1, result.getWarnings().stream()
                .filter(warning -> warning.startsWith("edge c-d:"))
                .count());
    }

    @Test
    void blockedOneToOneRouteUsesSecondaryAxisChannel() {
        LayoutNode source = node("source", 0, 0, 100, 80);
        LayoutNode target = node("target", 0, 600, 100, 80);
        LayoutNode obstacle = node("obstacle", 20, 150, 80, 380);
        LayoutGraph graph = graph(
                map("source", source, "target", target, "obstacle", obstacle),
                map("edge", fwdEdge("edge", "source", "target")));

        RoutingResult result = new OrthogonalRouter().computeRoutes(
                graph, coordsFrom(graph), DEFAULTS);
        List<Position> path = result.getEdgePaths().get("edge");

        assertFalse(Geometry.pathIntersectsAnyObstacle(
                path, List.of(obstacle.getBoundingBox()), 10));
        assertTrue(result.getWarnings().isEmpty());
        assertTrue(path.stream().anyMatch(point ->
                point.x() < obstacle.getBoundingBox().x()
                        || point.x() > obstacle.getBoundingBox().right()));
    }

    @Test
    void directModeFallsBackWhenProcessorBlocksSegment() {
        LayoutNode source = node("source", 0, 0, 100, 80);
        LayoutNode target = node("target", 0, 600, 100, 80);
        LayoutNode obstacle = node("obstacle", 20, 200, 80, 100);
        LayoutGraph graph = graph(
                map("source", source, "target", target, "obstacle", obstacle),
                map("edge", fwdEdge("edge", "source", "target")));
        LayoutOptions options = LayoutOptions.builder()
                .routingMode(in.shrake.nifi.layout.core.model.RoutingMode.DIRECT)
                .build();

        RoutingResult result = new DirectRouter().computeRoutes(
                graph, coordsFrom(graph), options);
        List<Position> path = result.getEdgePaths().get("edge");

        assertTrue(path.size() > 2);
        assertFalse(Geometry.pathIntersectsAnyObstacle(
                path, List.of(obstacle.getBoundingBox()), 10));
    }

    @Test
    void obstacleSearchFindsMultiAxisDogleg() {
        LayoutNode source = node("source", 0, 0, 100, 80);
        LayoutNode target = node("target", 0, 600, 100, 80);
        LayoutNode upperLeft = node("upper-left", -300, 0, 280, 80);
        LayoutNode upperRight = node("upper-right", 120, 0, 280, 80);
        LayoutNode center = node("center", -10, 150, 120, 380);
        LayoutGraph graph = graph(
                map(
                        "source", source,
                        "target", target,
                        "upper-left", upperLeft,
                        "upper-right", upperRight,
                        "center", center),
                map("edge", fwdEdge("edge", "source", "target")));
        List<BoundingBox> obstacles = List.of(
                upperLeft.getBoundingBox(),
                upperRight.getBoundingBox(),
                center.getBoundingBox());

        RoutingResult result = new OrthogonalRouter().computeRoutes(
                graph, coordsFrom(graph), DEFAULTS);
        List<Position> path = result.getEdgePaths().get("edge");

        assertFalse(Geometry.pathIntersectsAnyObstacle(path, obstacles, 10));
        assertTrue(result.getWarnings().isEmpty());
        assertTrue(path.size() >= 5);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static LayoutNode node(String id, int x, int y, int w, int h) {
        return new LayoutNode(id, NodeType.PROCESSOR,
                new BoundingBox(x, y, w, h), Map.of(), "root");
    }

    private static LayoutEdge fwdEdge(String id, String src, String tgt) {
        return new LayoutEdge(id, src, tgt, "success", "", false, src.equals(tgt));
    }

    private static LayoutEdge revEdge(String id, String src, String tgt) {
        return new LayoutEdge(id, src, tgt, "retry", "", true, false);
    }

    private static LayoutGraph graph(Map<String, LayoutNode> nodes,
                                      Map<String, LayoutEdge> edges) {
        return new LayoutGraph(nodes, edges, Map.of(), "root");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> map(Object... pairs) {
        Map<K, V> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((K) pairs[i], (V) pairs[i + 1]);
        }
        return m;
    }

    private static CoordinateAssignment coordsFrom(LayoutGraph graph) {
        Map<String, Position> positions = new LinkedHashMap<>();
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        graph.getNodes().forEach((id, n) -> {
            positions.put(id, new Position(n.getBoundingBox().x(), n.getBoundingBox().y()));
            bounds.put(id, n.getBoundingBox());
        });
        return new CoordinateAssignment(positions, bounds);
    }

    private static CoordinateAssignment coordsFromGraph(LayoutGraph graph) {
        return coordsFrom(graph);
    }

    private static Map<String, BoundingBox> boundsMap(LayoutResult result) {
        Map<String, BoundingBox> map = new LinkedHashMap<>();
        result.getUpdates().forEach(u -> map.put(u.componentId(), u.newBounds()));
        return map;
    }

    /** Assert that no segment in path[1..size-2] intersects the given obstacle. */
    private static void assertNoInteriorIntersection(List<Position> path,
                                                      BoundingBox obstacle) {
        for (int i = 1; i < path.size() - 2; i++) {
            assertFalse(Geometry.segmentIntersectsBounds(path.get(i), path.get(i + 1),
                    obstacle, 0),
                    "interior segment " + i + "→" + (i + 1)
                            + " (" + path.get(i) + "→" + path.get(i + 1)
                            + ") must not cross obstacle " + obstacle);
        }
    }

    private static void assertNotStrictlyInside(Position p, BoundingBox bounds,
                                                 String label) {
        if (bounds == null) return;
        boolean inside = p.x() > bounds.x() && p.x() < bounds.right()
                && p.y() > bounds.y() && p.y() < bounds.bottom();
        assertFalse(inside, label + ": " + p + " must not be strictly inside " + bounds);
    }
}
