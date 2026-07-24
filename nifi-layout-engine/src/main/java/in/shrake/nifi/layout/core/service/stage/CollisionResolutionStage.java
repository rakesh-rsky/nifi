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
import in.shrake.nifi.layout.core.collision.CollisionResolutionStrategy;
import in.shrake.nifi.layout.core.collision.PositionedComponent;
import in.shrake.nifi.layout.core.collision.CollisionResult;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.BoundingBox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CollisionResolutionStage implements PipelineStage {
    private final CollisionResolutionStrategy strategy;
    
    public CollisionResolutionStage(CollisionResolutionStrategy strategy) {
        this.strategy = strategy;
    }
    
    @Override
    public String getName() {
        return "Collision Resolution";
    }
    
    @Override
    public PipelineContext execute(PipelineContext context) {
        CoordinateAssignment currentCoordinates = context.getCoordinates();
        List<PositionedComponent> components = new ArrayList<>();
        
        for (LayoutNode node : context.getGraph().getNodes().values()) {
            Position pos = currentCoordinates.getPositions().get(node.getId());
            BoundingBox bb = currentCoordinates.getBounds().get(node.getId());
            if (pos != null && bb != null) {
                components.add(new PositionedComponent(node, pos, bb));
            }
        }
        
        java.util.Set<String> fixedComponentIds = java.util.Collections.emptySet();
        if (context.isIncrementalMode() && context.getAffectedComponentIds() != null) {
            fixedComponentIds = new java.util.LinkedHashSet<>();
            for (LayoutNode node : context.getGraph().getNodes().values()) {
                if (!context.getAffectedComponentIds().contains(node.getId())) {
                    fixedComponentIds.add(node.getId());
                }
            }
        }
        
        CollisionResult result = strategy.resolve(components, context.getOptions(), fixedComponentIds);
        
        Map<String, Position> positions = new LinkedHashMap<>();
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        
        for (PositionedComponent pc : result.getComponents()) {
            positions.put(pc.node().getId(), pc.position());
            bounds.put(pc.node().getId(), pc.boundingBox());
        }
        
        // Ensure virtual nodes from layered graph are preserved, as collision resolution might ignore them
        for (LayoutNode vNode : context.getLayeredGraph().getVirtualNodes()) {
            Position vPos = currentCoordinates.getPositions().get(vNode.getId());
            BoundingBox vBb = currentCoordinates.getBounds().get(vNode.getId());
            if (vPos != null && vBb != null && !positions.containsKey(vNode.getId())) {
                positions.put(vNode.getId(), vPos);
                bounds.put(vNode.getId(), vBb);
            }
        }
        
        return context.withCoordinates(new CoordinateAssignment(positions, bounds));
    }
}
