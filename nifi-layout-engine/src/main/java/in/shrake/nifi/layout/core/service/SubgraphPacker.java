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

package in.shrake.nifi.layout.core.service;

import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.PackingStrategy;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.model.BoundingBox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SubgraphPacker {

    public record SubgraphLayout(LayoutResult result, String firstNodeId, int nodeCount, BoundingBox bounds) {}

    public LayoutResult pack(List<SubgraphLayout> subgraphs, LayoutOptions options) {
        if (subgraphs.isEmpty()) {
            return new LayoutResult(Collections.emptyList(), 0, Collections.emptyMap(), 0, Collections.emptyList());
        }
        if (subgraphs.size() == 1) {
            return subgraphs.get(0).result();
        }

        List<SubgraphLayout> sorted = new ArrayList<>(subgraphs);
        sorted.sort(Comparator.<SubgraphLayout>comparingInt(SubgraphLayout::nodeCount).reversed()
                .thenComparing(SubgraphLayout::firstNodeId));

        int currentX = options.getMarginLeft();
        int currentY = options.getMarginTop();
        int rowBottom = currentY;
        int gridColumns = (int) Math.ceil(Math.sqrt(sorted.size()));
        
        PackingStrategy strategy = options.getPackingStrategy() != null ? options.getPackingStrategy() : PackingStrategy.VERTICAL;

        List<ComponentUpdate> allUpdates = new ArrayList<>();
        Map<String, List<Position>> allRoutes = new LinkedHashMap<>();
        List<String> allWarnings = new ArrayList<>();
        long totalComputeTime = 0;
        int totalRepositioned = 0;

        for (int i = 0; i < sorted.size(); i++) {
            SubgraphLayout sg = sorted.get(i);
            BoundingBox bounds = sg.bounds();
            
            int offsetX = snapForward(
                    currentX - bounds.x(), options.getGridSize());
            int offsetY = snapForward(
                    currentY - bounds.y(), options.getGridSize());
            int placedLeft = bounds.x() + offsetX;
            int placedTop = bounds.y() + offsetY;
            
            LayoutResult res = sg.result();
            totalComputeTime += res.getComputationTimeMs();
            totalRepositioned += res.getTotalComponentsRepositioned();
            allWarnings.addAll(res.getWarnings());
            
            for (ComponentUpdate update : res.getUpdates()) {
                Position newPos = new Position(update.newPosition().x() + offsetX, update.newPosition().y() + offsetY);
                BoundingBox newBounds = new BoundingBox(newPos.x(), newPos.y(), update.newBounds().width(), update.newBounds().height());
                allUpdates.add(new ComponentUpdate(update.componentId(), update.parentGroupId(), update.componentType(), update.originalPosition(), newPos, newBounds));
            }
            
            for (Map.Entry<String, List<Position>> entry : res.getConnectionBendPoints().entrySet()) {
                List<Position> newBends = new ArrayList<>();
                for (Position p : entry.getValue()) {
                    newBends.add(new Position(p.x() + offsetX, p.y() + offsetY));
                }
                allRoutes.put(entry.getKey(), newBends);
            }
            
            if (strategy == PackingStrategy.VERTICAL) {
                currentY = placedTop + bounds.height()
                        + options.getVerticalSpacing();
            } else if (strategy == PackingStrategy.HORIZONTAL) {
                currentX = placedLeft + bounds.width()
                        + options.getHorizontalSpacing();
            } else if (strategy == PackingStrategy.GRID) {
                rowBottom = Math.max(rowBottom, placedTop + bounds.height());
                if ((i + 1) % gridColumns == 0) {
                    currentX = options.getMarginLeft();
                    currentY = rowBottom + options.getVerticalSpacing();
                    rowBottom = currentY;
                } else {
                    currentX = placedLeft + bounds.width()
                            + options.getHorizontalSpacing();
                }
            }
        }
        
        return new LayoutResult(allUpdates, totalRepositioned, allRoutes, totalComputeTime, allWarnings);
    }

    private static int snapForward(int value, int gridSize) {
        int remainder = Math.floorMod(value, gridSize);
        return remainder == 0 ? value : value + gridSize - remainder;
    }
    
    public static BoundingBox computeBounds(LayoutResult result) {
        if (result.getUpdates().isEmpty()) return new BoundingBox(0, 0, 0, 0);
        int minX = Integer.MAX_VALUE, minY = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE, maxY = Integer.MIN_VALUE;
        for (ComponentUpdate u : result.getUpdates()) {
            minX = Math.min(minX, u.newBounds().x());
            minY = Math.min(minY, u.newBounds().y());
            maxX = Math.max(maxX, u.newBounds().x() + u.newBounds().width());
            maxY = Math.max(maxY, u.newBounds().y() + u.newBounds().height());
        }
        
        for (List<Position> bends : result.getConnectionBendPoints().values()) {
            for (Position p : bends) {
                minX = Math.min(minX, p.x());
                minY = Math.min(minY, p.y());
                maxX = Math.max(maxX, p.x());
                maxY = Math.max(maxY, p.y());
            }
        }
        
        return new BoundingBox(minX, minY, maxX - minX, maxY - minY);
    }
}
