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

import in.shrake.nifi.layout.core.service.PipelineStage;
import in.shrake.nifi.layout.core.service.PipelineContext;
import in.shrake.nifi.layout.core.layout.LayoutAlgorithm;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.LayoutNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CoordinateAssignmentStage implements PipelineStage {
    private final LayoutAlgorithm strategy;
    
    public CoordinateAssignmentStage(LayoutAlgorithm strategy) {
        this.strategy = strategy;
    }
    
    @Override
    public String getName() {
        return "Coordinate Assignment";
    }
    
    @Override
    public PipelineContext execute(PipelineContext context) {
        CoordinateAssignment fullAssignment = strategy.computeLayout(context.getLayeredGraph(), context.getOptions());
        
        if (!context.isIncrementalMode()) {
            return context.withCoordinates(fullAssignment);
        }
        
        Set<String> affected = context.getAffectedComponentIds();
        if (affected == null || affected.isEmpty()) {
            return context.withCoordinates(fullAssignment);
        }
        
        Map<String, Position> positions = new LinkedHashMap<>(fullAssignment.getPositions());
        Map<String, BoundingBox> bounds = new LinkedHashMap<>(fullAssignment.getBounds());
        
        long newSumX = 0, newSumY = 0;
        int count = 0;
        for (String id : affected) {
            Position p = fullAssignment.getPositions().get(id);
            if (p != null) {
                newSumX += p.x();
                newSumY += p.y();
                count++;
            }
        }
        
        long oldSumX = 0, oldSumY = 0;
        int oldCount = 0;
        for (String id : affected) {
            LayoutNode node = context.getGraph().getNode(id);
            if (node != null) {
                oldSumX += node.getBoundingBox().x();
                oldSumY += node.getBoundingBox().y();
                oldCount++;
            }
        }
        
        int dx = 0, dy = 0;
        if (count > 0 && oldCount > 0) {
            int newCenterX = (int) (newSumX / count);
            int newCenterY = (int) (newSumY / count);
            int oldCenterX = (int) (oldSumX / oldCount);
            int oldCenterY = (int) (oldSumY / oldCount);
            
            dx = oldCenterX - newCenterX;
            dy = oldCenterY - newCenterY;
        }
        
        for (LayoutNode node : context.getGraph().getNodes().values()) {
            String id = node.getId();
            if (affected.contains(id)) {
                Position p = fullAssignment.getPositions().get(id);
                if (p != null) {
                    Position shiftedPos = new Position(p.x() + dx, p.y() + dy);
                    positions.put(id, shiftedPos);
                    BoundingBox ob = fullAssignment.getBounds().get(id);
                    bounds.put(id, new BoundingBox(shiftedPos.x(), shiftedPos.y(), ob.width(), ob.height()));
                }
            } else {
                Position origPos = new Position(node.getBoundingBox().x(), node.getBoundingBox().y());
                positions.put(id, origPos);
                bounds.put(id, node.getBoundingBox());
            }
        }
        
        CoordinateAssignment finalAssignment = new CoordinateAssignment(positions, bounds);
        return context.withCoordinates(finalAssignment);
    }
}
