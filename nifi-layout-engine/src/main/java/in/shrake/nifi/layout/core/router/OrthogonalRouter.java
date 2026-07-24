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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OrthogonalRouter implements RoutingStrategy {
    private static final int CONNECTION_LABEL_WIDTH = 240;
    private static final int CONNECTION_LABEL_CLEARANCE = 20;
    private static final int HORIZONTAL_FLOW_LANE_SPACING = 60;

    @Override
    public RoutingResult computeRoutes(LayoutGraph graph, CoordinateAssignment coordinates, LayoutOptions options) {
        Map<String, List<Position>> routes = new LinkedHashMap<>();
        FlowDirection flowDir = options.getFlowDirection();

        Map<String, List<LayoutEdge>> sourceEdges = new LinkedHashMap<>();
        Map<String, List<LayoutEdge>> targetEdges = new LinkedHashMap<>();
        
        for (LayoutEdge edge : graph.getEdges().values()) {
            sourceEdges.computeIfAbsent(edge.getSourceNodeId(), k -> new ArrayList<>()).add(edge);
            targetEdges.computeIfAbsent(edge.getTargetNodeId(), k -> new ArrayList<>()).add(edge);
        }

        for (LayoutEdge edge : graph.getEdges().values()) {
            BoundingBox sBb = coordinates.getBounds().get(edge.getSourceNodeId());
            BoundingBox tBb = coordinates.getBounds().get(edge.getTargetNodeId());
            
            if (sBb == null || tBb == null) continue;

            List<LayoutEdge> sList = sourceEdges.get(edge.getSourceNodeId());
            int sIndex = sList.indexOf(edge);
            
            List<LayoutEdge> tList = targetEdges.get(edge.getTargetNodeId());
            int tIndex = tList.indexOf(edge);
            List<LayoutEdge> parallelEdges = sList.stream()
                    .filter(candidate -> candidate.getTargetNodeId().equals(edge.getTargetNodeId()))
                    .toList();
            
            Position start = getExitPoint(sBb, flowDir, sIndex, sList.size(), options.getPortSpacing());
            Position end = getEntryPoint(tBb, flowDir, tIndex, tList.size(), options.getPortSpacing());

            List<Position> path = new ArrayList<>();

            if (edge.getSourceNodeId().equals(edge.getTargetNodeId())) {
                buildSelfLoopPath(path, start, end, sBb, flowDir, options);
            } else if (parallelEdges.size() == 1 && sList.size() > 1 && tList.size() >= 3) {
                List<String> denseTargets = denseTargetsAtRank(
                        targetEdges, sourceEdges, coordinates, edge.getTargetNodeId(), flowDir);
                buildDenseFanInPath(path, start, end, flowDir,
                        denseTargets.indexOf(edge.getTargetNodeId()), denseTargets.size());
            } else if (parallelEdges.size() == 1
                    && !hasBlockingComponent(graph, coordinates, edge, sBb.center(), tBb.center())) {
                buildMinimalClearPath(path, start, end, flowDir, sList.size(), tList.size());
            } else {
                boolean parallel = parallelEdges.size() > 1;
                boolean fanOut = !parallel && sList.size() > 1;
                int laneIndex = parallel ? parallelEdges.indexOf(edge) : fanOut ? sIndex : tIndex;
                int laneCount = parallel ? parallelEdges.size() : fanOut ? sList.size() : tList.size();
                buildOrthogonalPath(path, start, end, flowDir, options.getVerticalSpacing(),
                        options.getHorizontalSpacing(), sBb, tBb, laneIndex, laneCount,
                        fanOut ? sBb : tBb);
            }
            
            routes.put(edge.getId(), path);
        }

        return new RoutingResult(routes);
    }

    private List<String> denseTargetsAtRank(
            Map<String, List<LayoutEdge>> targetEdges,
            Map<String, List<LayoutEdge>> sourceEdges,
            CoordinateAssignment coordinates,
            String targetId,
            FlowDirection flowDir) {
        BoundingBox targetBounds = coordinates.getBounds().get(targetId);
        boolean vertical = flowDir == FlowDirection.TOP_TO_BOTTOM || flowDir == FlowDirection.BOTTOM_TO_TOP;
        return targetEdges.entrySet().stream()
                .filter(entry -> entry.getValue().size() >= 3)
                .filter(entry -> entry.getValue().stream()
                        .anyMatch(edge -> sourceEdges.getOrDefault(edge.getSourceNodeId(), List.of()).size() > 1))
                .map(Map.Entry::getKey)
                .filter(id -> {
                    BoundingBox bounds = coordinates.getBounds().get(id);
                    return bounds != null && (vertical
                            ? bounds.y() == targetBounds.y()
                            : bounds.x() == targetBounds.x());
                })
                .sorted((left, right) -> {
                    BoundingBox leftBounds = coordinates.getBounds().get(left);
                    BoundingBox rightBounds = coordinates.getBounds().get(right);
                    return vertical
                            ? Integer.compare(leftBounds.x(), rightBounds.x())
                            : Integer.compare(leftBounds.y(), rightBounds.y());
                })
                .toList();
    }

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

    private void buildMinimalClearPath(List<Position> path, Position start, Position end,
                                       FlowDirection flowDir, int sourceEdgeCount, int targetEdgeCount) {
        if (sourceEdgeCount == 1 && targetEdgeCount == 1) {
            return;
        }
        if (flowDir == FlowDirection.TOP_TO_BOTTOM || flowDir == FlowDirection.BOTTOM_TO_TOP) {
            int bendX = sourceEdgeCount > 1 ? end.x() : start.x();
            path.add(new Position(bendX, (start.y() + end.y()) / 2));
        } else {
            int bendY = sourceEdgeCount > 1 ? end.y() : start.y();
            path.add(new Position((start.x() + end.x()) / 2, bendY));
        }
    }

    private boolean hasBlockingComponent(LayoutGraph graph, CoordinateAssignment coordinates,
                                         LayoutEdge edge, Position start, Position end) {
        for (Map.Entry<String, BoundingBox> entry : coordinates.getBounds().entrySet()) {
            String nodeId = entry.getKey();
            if (nodeId.equals(edge.getSourceNodeId()) || nodeId.equals(edge.getTargetNodeId())
                    || graph.getNode(nodeId) == null) {
                continue;
            }
            if (intersects(start, end, entry.getValue(), 10)) {
                return true;
            }
        }
        return false;
    }

    private boolean intersects(Position start, Position end, BoundingBox bounds, int clearance) {
        double minX = bounds.x() - clearance;
        double maxX = bounds.right() + clearance;
        double minY = bounds.y() - clearance;
        double maxY = bounds.bottom() + clearance;
        double dx = end.x() - start.x();
        double dy = end.y() - start.y();
        double[] range = {0.0, 1.0};
        return clip(-dx, start.x() - minX, range)
                && clip(dx, maxX - start.x(), range)
                && clip(-dy, start.y() - minY, range)
                && clip(dy, maxY - start.y(), range);
    }

    private boolean clip(double direction, double distance, double[] range) {
        if (direction == 0) {
            return distance >= 0;
        }
        double ratio = distance / direction;
        if (direction < 0) {
            if (ratio > range[1]) return false;
            range[0] = Math.max(range[0], ratio);
        } else {
            if (ratio < range[0]) return false;
            range[1] = Math.min(range[1], ratio);
        }
        return true;
    }

    private Position getExitPoint(BoundingBox bb, FlowDirection flowDir, int index, int total, int portSpacing) {
        int offset = (int) ((index - (total - 1) / 2.0) * portSpacing);
        return switch (flowDir) {
            case TOP_TO_BOTTOM -> new Position(bb.center().x() + offset, bb.bottom());
            case BOTTOM_TO_TOP -> new Position(bb.center().x() + offset, bb.y());
            case LEFT_TO_RIGHT -> new Position(bb.right(), bb.center().y() + offset);
            case RIGHT_TO_LEFT -> new Position(bb.x(), bb.center().y() + offset);
        };
    }

    private Position getEntryPoint(BoundingBox bb, FlowDirection flowDir, int index, int total, int portSpacing) {
        int offset = (int) ((index - (total - 1) / 2.0) * portSpacing);
        return switch (flowDir) {
            case TOP_TO_BOTTOM -> new Position(bb.center().x() + offset, bb.y());
            case BOTTOM_TO_TOP -> new Position(bb.center().x() + offset, bb.bottom());
            case LEFT_TO_RIGHT -> new Position(bb.x(), bb.center().y() + offset);
            case RIGHT_TO_LEFT -> new Position(bb.right(), bb.center().y() + offset);
        };
    }

    private void buildOrthogonalPath(List<Position> path, Position start, Position end,
                                     FlowDirection flowDir, int vSpacing, int hSpacing,
                                     BoundingBox sourceBb, BoundingBox targetBb,
                                     int laneIndex, int laneCount, BoundingBox laneOwner) {
        boolean isVertical = (flowDir == FlowDirection.TOP_TO_BOTTOM || flowDir == FlowDirection.BOTTOM_TO_TOP);
        
        boolean isForward = false;
        switch (flowDir) {
            case TOP_TO_BOTTOM: isForward = start.y() <= end.y(); break;
            case BOTTOM_TO_TOP: isForward = start.y() >= end.y(); break;
            case LEFT_TO_RIGHT: isForward = start.x() <= end.x(); break;
            case RIGHT_TO_LEFT: isForward = start.x() >= end.x(); break;
        }

        if (isForward) {
            if (laneCount > 1) {
                buildSeparatedForwardPath(path, start, end, flowDir, vSpacing, hSpacing,
                        laneIndex, laneCount, laneOwner);
            } else if (isVertical) {
                int midY = (start.y() + end.y()) / 2;
                path.add(new Position(start.x(), midY));
                path.add(new Position(end.x(), midY));
            } else {
                int midX = (start.x() + end.x()) / 2;
                path.add(new Position(midX, start.y()));
                path.add(new Position(midX, end.y()));
            }
        } else {
            int space = Math.min(hSpacing, vSpacing) / 2;
            space = Math.max(space, 10);
            
            int sideX = Math.max(sourceBb.right(), targetBb.right()) + space;
            int sideY = Math.max(sourceBb.bottom(), targetBb.bottom()) + space;
            
            switch (flowDir) {
                case TOP_TO_BOTTOM:
                    path.add(new Position(start.x(), start.y() + space));
                    path.add(new Position(sideX, start.y() + space));
                    path.add(new Position(sideX, end.y() - space));
                    path.add(new Position(end.x(), end.y() - space));
                    break;
                case BOTTOM_TO_TOP:
                    path.add(new Position(start.x(), start.y() - space));
                    path.add(new Position(sideX, start.y() - space));
                    path.add(new Position(sideX, end.y() + space));
                    path.add(new Position(end.x(), end.y() + space));
                    break;
                case LEFT_TO_RIGHT:
                    path.add(new Position(start.x() + space, start.y()));
                    path.add(new Position(start.x() + space, sideY));
                    path.add(new Position(end.x() - space, sideY));
                    path.add(new Position(end.x() - space, end.y()));
                    break;
                case RIGHT_TO_LEFT:
                    path.add(new Position(start.x() - space, start.y()));
                    path.add(new Position(start.x() - space, sideY));
                    path.add(new Position(end.x() + space, sideY));
                    path.add(new Position(end.x() + space, end.y()));
                    break;
            }
        }
    }

    private void buildSeparatedForwardPath(List<Position> path, Position start, Position end,
                                           FlowDirection flowDir, int vSpacing, int hSpacing,
                                           int laneIndex, int laneCount, BoundingBox laneOwner) {
        double centeredIndex = laneIndex - (laneCount - 1) / 2.0;
        if (flowDir == FlowDirection.TOP_TO_BOTTOM || flowDir == FlowDirection.BOTTOM_TO_TOP) {
            int direction = flowDir == FlowDirection.TOP_TO_BOTTOM ? 1 : -1;
            int stub = Math.max(10, Math.min(vSpacing / 3, Math.abs(end.y() - start.y()) / 3));
            int laneX = laneOwner.center().x()
                    + (int) Math.round(centeredIndex * (CONNECTION_LABEL_WIDTH + CONNECTION_LABEL_CLEARANCE));
            int sourceY = start.y() + direction * stub;
            int targetY = end.y() - direction * stub;
            path.add(new Position(start.x(), sourceY));
            path.add(new Position(laneX, sourceY));
            path.add(new Position(laneX, targetY));
            path.add(new Position(end.x(), targetY));
        } else {
            int direction = flowDir == FlowDirection.LEFT_TO_RIGHT ? 1 : -1;
            int stub = Math.max(10, Math.min(hSpacing / 3, Math.abs(end.x() - start.x()) / 3));
            int laneY = laneOwner.center().y()
                    + (int) Math.round(centeredIndex * HORIZONTAL_FLOW_LANE_SPACING);
            int sourceX = start.x() + direction * stub;
            int targetX = end.x() - direction * stub;
            path.add(new Position(sourceX, start.y()));
            path.add(new Position(sourceX, laneY));
            path.add(new Position(targetX, laneY));
            path.add(new Position(targetX, end.y()));
        }
    }

    private void buildSelfLoopPath(List<Position> path, Position start, Position end, BoundingBox bb, FlowDirection flowDir, LayoutOptions options) {
        int space = Math.min(options.getHorizontalSpacing(), options.getVerticalSpacing()) / 2;
        space = Math.max(space, 10);
        
        switch (flowDir) {
            case TOP_TO_BOTTOM:
                path.add(new Position(start.x(), start.y() + space));
                path.add(new Position(bb.right() + space, start.y() + space));
                path.add(new Position(bb.right() + space, end.y() - space));
                path.add(new Position(end.x(), end.y() - space));
                break;
            case BOTTOM_TO_TOP:
                path.add(new Position(start.x(), start.y() - space));
                path.add(new Position(bb.right() + space, start.y() - space));
                path.add(new Position(bb.right() + space, end.y() + space));
                path.add(new Position(end.x(), end.y() + space));
                break;
            case LEFT_TO_RIGHT:
                path.add(new Position(start.x() + space, start.y()));
                path.add(new Position(start.x() + space, bb.bottom() + space));
                path.add(new Position(end.x() - space, bb.bottom() + space));
                path.add(new Position(end.x() - space, end.y()));
                break;
            case RIGHT_TO_LEFT:
                path.add(new Position(start.x() - space, start.y()));
                path.add(new Position(start.x() - space, bb.bottom() + space));
                path.add(new Position(end.x() + space, bb.bottom() + space));
                path.add(new Position(end.x() + space, end.y()));
                break;
        }
    }
}
