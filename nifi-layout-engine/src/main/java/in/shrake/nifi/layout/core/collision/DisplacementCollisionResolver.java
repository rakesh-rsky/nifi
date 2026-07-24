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

package in.shrake.nifi.layout.core.collision;

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.Position;

import java.util.ArrayList;
import java.util.List;

public class DisplacementCollisionResolver implements CollisionResolutionStrategy {

    private final SweepLineCollisionDetector detector;

    public DisplacementCollisionResolver() {
        this.detector = new SweepLineCollisionDetector();
    }
    
    public DisplacementCollisionResolver(SweepLineCollisionDetector detector) {
        this.detector = detector;
    }

    @Override
    public CollisionResult resolve(List<PositionedComponent> components, LayoutOptions options) {
        return resolve(components, options, java.util.Collections.emptySet());
    }

    @Override
    public CollisionResult resolve(List<PositionedComponent> components, LayoutOptions options, java.util.Set<String> fixedComponentIds) {
        int maxIterations = options.getMaxIterations() > 0 ? options.getMaxIterations() : 1000;
        int margin = options.getPadding();
        
        List<PositionedComponent> current = new ArrayList<>(components);
        List<PositionedComponent> best = new ArrayList<>(current);
        int bestOverlaps = Integer.MAX_VALUE;
        
        for (int i = 0; i < maxIterations; i++) {
            List<SweepLineCollisionDetector.CollisionPair> collisions = detector.detectCollisions(current, margin);
            
            if (collisions.isEmpty()) {
                return CollisionResult.success(current);
            }
            
            if (collisions.size() < bestOverlaps) {
                bestOverlaps = collisions.size();
                best = new ArrayList<>(current);
            }
            
            int[] dx = new int[current.size()];
            int[] dy = new int[current.size()];
            
            for (SweepLineCollisionDetector.CollisionPair pair : collisions) {
                PositionedComponent c1 = current.get(pair.index1());
                PositionedComponent c2 = current.get(pair.index2());
                
                boolean fixed1 = fixedComponentIds.contains(c1.node().getId());
                boolean fixed2 = fixedComponentIds.contains(c2.node().getId());
                
                if (fixed1 && fixed2) {
                    continue; // both fixed, can't resolve
                }
                
                BoundingBox b1 = c1.boundingBox().expand(margin);
                BoundingBox b2 = c2.boundingBox().expand(margin);
                
                int overlapX = Math.min(b1.right(), b2.right()) - Math.max(b1.x(), b2.x());
                int overlapY = Math.min(b1.bottom(), b2.bottom()) - Math.max(b1.y(), b2.y());
                
                if (overlapX > 0 && overlapY > 0) {
                    if (overlapX < overlapY) {
                        if (fixed1) {
                            int totalShift = overlapX + 1;
                            if (b1.center().x() <= b2.center().x()) dx[pair.index2()] += totalShift;
                            else dx[pair.index2()] -= totalShift;
                        } else if (fixed2) {
                            int totalShift = overlapX + 1;
                            if (b1.center().x() <= b2.center().x()) dx[pair.index1()] -= totalShift;
                            else dx[pair.index1()] += totalShift;
                        } else {
                            int shift = (overlapX / 2) + 1;
                            if (b1.center().x() <= b2.center().x()) {
                                dx[pair.index1()] -= shift;
                                dx[pair.index2()] += shift;
                            } else {
                                dx[pair.index1()] += shift;
                                dx[pair.index2()] -= shift;
                            }
                        }
                    } else {
                        if (fixed1) {
                            int totalShift = overlapY + 1;
                            if (b1.center().y() <= b2.center().y()) dy[pair.index2()] += totalShift;
                            else dy[pair.index2()] -= totalShift;
                        } else if (fixed2) {
                            int totalShift = overlapY + 1;
                            if (b1.center().y() <= b2.center().y()) dy[pair.index1()] -= totalShift;
                            else dy[pair.index1()] += totalShift;
                        } else {
                            int shift = (overlapY / 2) + 1;
                            if (b1.center().y() <= b2.center().y()) {
                                dy[pair.index1()] -= shift;
                                dy[pair.index2()] += shift;
                            } else {
                                dy[pair.index1()] += shift;
                                dy[pair.index2()] -= shift;
                            }
                        }
                    }
                }
            }
            
            List<PositionedComponent> next = new ArrayList<>(current.size());
            for (int j = 0; j < current.size(); j++) {
                PositionedComponent c = current.get(j);
                if (dx[j] == 0 && dy[j] == 0) {
                    next.add(c);
                } else {
                    Position newPos = new Position(c.position().x() + dx[j], c.position().y() + dy[j]);
                    BoundingBox newBB = new BoundingBox(newPos.x(), newPos.y(), c.boundingBox().width(), c.boundingBox().height());
                    next.add(new PositionedComponent(c.node(), newPos, newBB));
                }
            }
            current = next;
        }
        
        return CollisionResult.partial(best, bestOverlaps);
    }
}
