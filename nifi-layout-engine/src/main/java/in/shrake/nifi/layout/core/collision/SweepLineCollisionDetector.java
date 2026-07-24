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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class SweepLineCollisionDetector {
    
    public record CollisionPair(int index1, int index2) {}
    
    public List<CollisionPair> detectCollisions(List<PositionedComponent> components, int margin) {
        if (components.isEmpty()) return List.of();
        
        List<Event> events = new ArrayList<>();
        int maxHeight = 0;
        
        for (int i = 0; i < components.size(); i++) {
            BoundingBox bb = components.get(i).boundingBox().expand(margin);
            maxHeight = Math.max(maxHeight, bb.height());
            events.add(new Event(bb.x(), true, i, bb));
            events.add(new Event(bb.right(), false, i, bb));
        }
        
        events.sort((e1, e2) -> {
            if (e1.x != e2.x) {
                return Integer.compare(e1.x, e2.x);
            }
            if (e1.isStart && !e2.isStart) return -1;
            if (!e1.isStart && e2.isStart) return 1;
            return Integer.compare(e1.index, e2.index);
        });
        
        TreeSet<ActiveItem> activeSet = new TreeSet<>((a1, a2) -> {
            if (a1.bb.y() != a2.bb.y()) {
                return Integer.compare(a1.bb.y(), a2.bb.y());
            }
            return Integer.compare(a1.index, a2.index);
        });
        
        List<CollisionPair> collisions = new ArrayList<>();
        ActiveItem[] activeItems = new ActiveItem[components.size()];
        
        for (Event event : events) {
            if (event.isStart) {
                ActiveItem dummyStart = new ActiveItem(-1, new BoundingBox(0, event.bb.y() - maxHeight, 0, 0));
                for (ActiveItem active : activeSet.tailSet(dummyStart)) {
                    if (active.bb.y() >= event.bb.bottom()) {
                        break;
                    }
                    if (active.bb.bottom() > event.bb.y()) {
                        collisions.add(new CollisionPair(event.index, active.index));
                    }
                }
                
                ActiveItem item = new ActiveItem(event.index, event.bb);
                activeSet.add(item);
                activeItems[event.index] = item;
            } else {
                ActiveItem item = activeItems[event.index];
                if (item != null) {
                    activeSet.remove(item);
                    activeItems[event.index] = null;
                }
            }
        }
        
        return collisions;
    }
    
    private record Event(int x, boolean isStart, int index, BoundingBox bb) {}
    private record ActiveItem(int index, BoundingBox bb) {}
}
