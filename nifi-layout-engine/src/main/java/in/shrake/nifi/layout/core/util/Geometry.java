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

package in.shrake.nifi.layout.core.util;

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.Position;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Stateless geometry utilities shared across routing and layout stages.
 *
 * <p>All segment/bounds tests use axis-aligned arithmetic only.
 * Clearance is added to each obstacle bounding box before intersection tests,
 * providing the configurable per-segment minimum gap.
 */
public final class Geometry {
    private Geometry() {}

    // -------------------------------------------------------------------------
    // Bounding-box helpers
    // -------------------------------------------------------------------------

    public static BoundingBox union(BoundingBox b1, BoundingBox b2) {
        if (b1 == null) return b2;
        if (b2 == null) return b1;

        int minX = Math.min(b1.x(), b2.x());
        int minY = Math.min(b1.y(), b2.y());
        int maxX = Math.max(b1.right(), b2.right());
        int maxY = Math.max(b1.bottom(), b2.bottom());

        return new BoundingBox(minX, minY, maxX - minX, maxY - minY);
    }

    /**
     * Returns the bounding box that covers all entries in {@code allBounds}
     * whose key is NOT in {@code excludeIds}.  Returns {@code null} when every
     * entry is excluded.
     */
    public static BoundingBox occupiedBounds(Map<String, BoundingBox> allBounds,
                                             Set<String> excludeIds) {
        BoundingBox result = null;
        for (Map.Entry<String, BoundingBox> entry : allBounds.entrySet()) {
            if (!excludeIds.contains(entry.getKey())) {
                result = union(result, entry.getValue());
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Segment / path intersection
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when the line segment from {@code a} to {@code b}
     * intersects the bounding box {@code box} expanded uniformly by
     * {@code clearance} on all sides.
     *
     * <p>Uses Liang–Barsky parametric clipping so it handles both axis-aligned
     * and diagonal segments.  A segment that merely <em>touches</em> the
     * expanded boundary (t == 0 or t == 1 exactly) is <strong>not</strong>
     * considered an intersection, preserving the invariant that anchor points
     * placed on a component boundary are not flagged as crossing the component.
     */
    public static boolean segmentIntersectsBounds(Position a, Position b, BoundingBox box,
                                                  int clearance) {
        double minX = box.x() - clearance;
        double maxX = box.right() + clearance;
        double minY = box.y() - clearance;
        double maxY = box.bottom() + clearance;
        double dx = b.x() - a.x();
        double dy = b.y() - a.y();
        double[] t = {0.0, 1.0};
        return clip(-dx, a.x() - minX, t)
                && clip(dx, maxX - a.x(), t)
                && clip(-dy, a.y() - minY, t)
                && clip(dy, maxY - a.y(), t)
                && t[0] < t[1]; // strict: touching only at endpoint is not a crossing
    }

    private static boolean clip(double p, double q, double[] t) {
        if (p == 0) {
            return q >= 0;
        }
        double r = q / p;
        if (p < 0) {
            if (r > t[1]) return false;
            if (r > t[0]) t[0] = r;
        } else {
            if (r < t[0]) return false;
            if (r < t[1]) t[1] = r;
        }
        return true;
    }

    /**
     * Returns {@code true} when any segment of {@code path} (consecutive pairs
     * of points) intersects any obstacle in {@code obstacles} with the given
     * {@code clearance}.
     *
     * <p>Touches at the full path's <em>first</em> point ({@code path.get(0)}) and
     * <em>last</em> point ({@code path.get(path.size()-1)}) are exempt – those are
     * component-boundary anchors placed by the caller.  Touches at <em>interior</em>
     * path vertices (bend points that are simultaneously the end of one segment and
     * the start of the next) are <strong>not</strong> exempt and are treated as real
     * intersections.
     */
    public static boolean pathIntersectsAnyObstacle(List<Position> path,
                                                     Collection<BoundingBox> obstacles,
                                                     int clearance) {
        int last = path.size() - 1;
        for (int i = 0; i < last; i++) {
            Position pa = path.get(i);
            Position pb = path.get(i + 1);
            boolean exemptStart = (i == 0);      // pa is the full-path start anchor
            boolean exemptEnd   = (i == last - 1); // pb is the full-path end anchor
            for (BoundingBox obs : obstacles) {
                if (segmentIntersectsExempting(pa, pb, obs, clearance, exemptStart, exemptEnd)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Segment–bounds intersection with per-endpoint exemption control.
     *
     * <p>Uses the same Liang–Barsky parametric clipping as
     * {@link #segmentIntersectsBounds}, but the endpoint-touch exemption is
     * governed by {@code exemptStart} and {@code exemptEnd} rather than
     * being unconditional:
     * <ul>
     *   <li>{@code exemptStart=true} – a touch that occurs only at {@code t=0}
     *       (i.e., the segment's start lies exactly on the expanded boundary) is
     *       <em>not</em> counted as an intersection.</li>
     *   <li>{@code exemptEnd=true} – same rule for {@code t=1}.</li>
     * </ul>
     * When both flags are {@code true} the behaviour is identical to
     * {@link #segmentIntersectsBounds} (strict {@code t[0] < t[1]}).
     */
    private static boolean segmentIntersectsExempting(Position a, Position b, BoundingBox box,
                                                       int clearance,
                                                       boolean exemptStart, boolean exemptEnd) {
        double minX = box.x()      - clearance;
        double maxX = box.right()  + clearance;
        double minY = box.y()      - clearance;
        double maxY = box.bottom() + clearance;
        double dx = b.x() - a.x();
        double dy = b.y() - a.y();
        double[] t = {0.0, 1.0};
        if (!clip(-dx, a.x() - minX, t)) return false;
        if (!clip(dx,  maxX - a.x(), t)) return false;
        if (!clip(-dy, a.y() - minY, t)) return false;
        if (!clip(dy,  maxY - a.y(), t)) return false;

        if (exemptStart && exemptEnd) {
            // Both path endpoints are exempt – only a strict interior crossing counts.
            return t[0] < t[1];
        } else if (exemptStart) {
            // Start exempt: the intersection must extend past t=0.
            return t[1] > 0.0;
        } else if (exemptEnd) {
            // End exempt: the intersection must start before t=1.
            return t[0] < 1.0;
        } else {
            // Neither endpoint is exempt: any overlap with [0,1] counts.
            return t[0] <= t[1];
        }
    }

    // -------------------------------------------------------------------------
    // Path metrics
    // -------------------------------------------------------------------------

    /**
     * Returns the Manhattan (L1) length of a multi-segment path: the sum of
     * |Δx| + |Δy| for every consecutive pair of points.
     */
    public static int pathLength(List<Position> path) {
        int length = 0;
        for (int i = 0; i < path.size() - 1; i++) {
            length += Math.abs(path.get(i + 1).x() - path.get(i).x())
                    + Math.abs(path.get(i + 1).y() - path.get(i).y());
        }
        return length;
    }
}
