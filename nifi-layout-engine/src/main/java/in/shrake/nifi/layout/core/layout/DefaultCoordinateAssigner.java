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

package in.shrake.nifi.layout.core.layout;

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.CoordinateAssignment;
import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayeredGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.core.spacing.LayoutDimensions;
import in.shrake.nifi.layout.core.spacing.SpacingStrategy;
import in.shrake.nifi.layout.core.spacing.SpacingValues;
import in.shrake.nifi.layout.core.util.GridSnapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DefaultCoordinateAssigner implements LayoutAlgorithm {

    private final SpacingStrategy spacingStrategy;

    public DefaultCoordinateAssigner(SpacingStrategy spacingStrategy) {
        this.spacingStrategy = java.util.Objects.requireNonNull(spacingStrategy);
    }

    @Override
    public CoordinateAssignment computeLayout(LayeredGraph layeredGraph, LayoutOptions options) {
        int nodeCount = layeredGraph.getOriginalGraph().getNodes().size() + layeredGraph.getVirtualNodes().size();
        SpacingValues spacing = spacingStrategy.computeSpacing(new LayoutDimensions(0, 0), nodeCount, options);
        
        Map<String, Position> positions = new LinkedHashMap<>();
        Map<String, BoundingBox> bounds = new LinkedHashMap<>();
        
        // For horizontal flow directions the computation space is transposed relative to
        // the rendered canvas. Within-layer items are arranged along the computation X axis
        // (which becomes Y after rotation) and between-layer items along computation Y (which
        // becomes X after rotation).  Using the physical height as the within-layer extent and
        // the physical width as the between-layer extent means the rotation step correctly
        // restores physical dimensions without any additional swapping.
        FlowDirection computeFlowDir = options.getFlowDirection() != null
                ? options.getFlowDirection() : FlowDirection.TOP_TO_BOTTOM;
        boolean isHorizontal = computeFlowDir == FlowDirection.LEFT_TO_RIGHT
                || computeFlowDir == FlowDirection.RIGHT_TO_LEFT;

        int currentY = spacing.marginTop();
        
        for (int layerIndex = 0; layerIndex < layeredGraph.getLayers().size(); layerIndex++) {
            List<LayoutNode> layer = layeredGraph.getLayers().get(layerIndex);
            int maxBottomInLayer = currentY;
            int currentX = spacing.marginLeft();
            
            for (LayoutNode node : layer) {
                BoundingBox bb = node.getBoundingBox();
                // In horizontal layouts swap the roles of width and height so that the
                // rotation step (which does newW = bb.height(), newH = bb.width()) yields
                // physical dimensions for collision and routing geometry.
                int w = isHorizontal ? bb.height() : bb.width();
                int h = isHorizontal ? bb.width() : bb.height();
                
                Position pos = GridSnapper.snap(new Position(currentX, currentY), options.getGridSize());
                positions.put(node.getId(), pos);
                bounds.put(node.getId(), new BoundingBox(pos.x(), pos.y(), w, h));
                
                currentX = pos.x() + w + spacing.horizontalSpacing();
                maxBottomInLayer = Math.max(maxBottomInLayer, pos.y() + h);
            }
            
            currentY = maxBottomInLayer
                    + adaptiveLayerSpacing(layeredGraph, layerIndex, spacing.verticalSpacing());
        }
        
        if (options.getAlignmentMode() != null) {
            int maxLayerWidth = 0;
            Map<Integer, Integer> layerWidths = new LinkedHashMap<>();
            
            for (int l = 0; l < layeredGraph.getLayers().size(); l++) {
                List<LayoutNode> layer = layeredGraph.getLayers().get(l);
                if (layer.isEmpty()) {
                    layerWidths.put(l, 0);
                    continue;
                }
                LayoutNode lastNode = layer.get(layer.size() - 1);
                BoundingBox lastBounds = bounds.get(lastNode.getId());
                int layerW = lastBounds.x() + lastBounds.width() - spacing.marginLeft();
                layerWidths.put(l, layerW);
                maxLayerWidth = Math.max(maxLayerWidth, layerW);
            }
            
            for (int l = 0; l < layeredGraph.getLayers().size(); l++) {
                List<LayoutNode> layer = layeredGraph.getLayers().get(l);
                int layerW = layerWidths.get(l);
                int shiftX = 0;
                
                switch (options.getAlignmentMode()) {
                    case CENTER:
                        shiftX = (maxLayerWidth - layerW) / 2;
                        break;
                    case RIGHT:
                        shiftX = maxLayerWidth - layerW;
                        break;
                    case LEFT:
                    default:
                        shiftX = 0;
                        break;
                }
                
                if (shiftX > 0) {
                    for (LayoutNode node : layer) {
                        Position pos = positions.get(node.getId());
                        BoundingBox bb = bounds.get(node.getId());
                        Position newPos = GridSnapper.snap(new Position(pos.x() + shiftX, pos.y()), options.getGridSize());
                        positions.put(node.getId(), newPos);
                        bounds.put(node.getId(), new BoundingBox(newPos.x(), newPos.y(), bb.width(), bb.height()));
                    }
                }
            }
        }
        
        if (options.getFlowDirection() != null) {
            int maxX = 0;
            int maxY = 0;
            for (BoundingBox bb : bounds.values()) {
                maxX = Math.max(maxX, bb.x() + bb.width());
                maxY = Math.max(maxY, bb.y() + bb.height());
            }
            
            for (Map.Entry<String, Position> entry : positions.entrySet()) {
                String id = entry.getKey();
                Position pos = entry.getValue();
                BoundingBox bb = bounds.get(id);
                
                int newX = pos.x();
                int newY = pos.y();
                int newW = bb.width();
                int newH = bb.height();
                
                switch (options.getFlowDirection()) {
                    case LEFT_TO_RIGHT:
                        newX = pos.y();
                        newY = pos.x();
                        newW = bb.height();
                        newH = bb.width();
                        break;
                    case BOTTOM_TO_TOP:
                        newY = maxY - pos.y() - newH;
                        break;
                    case RIGHT_TO_LEFT:
                        newX = maxY - pos.y() - newH;
                        newY = pos.x();
                        newW = bb.height();
                        newH = bb.width();
                        break;
                    case TOP_TO_BOTTOM:
                    default:
                        break;
                }
                
                Position snappedPos = GridSnapper.snap(new Position(newX, newY), options.getGridSize());
                positions.put(id, snappedPos);
                bounds.put(id, new BoundingBox(snappedPos.x(), snappedPos.y(), newW, newH));
            }
        }
        
        // 13.1 Labels
        for (LayoutNode node : layeredGraph.getOriginalGraph().getNodes().values()) {
            if (node.getType() == NodeType.LABEL) {
                String associatedId = node.getAttributes().get("associatedComponentId");
                if (associatedId != null && positions.containsKey(associatedId)) {
                    Position compPos = positions.get(associatedId);
                    
                    int lx = compPos.x();
                    int ly = compPos.y() - node.getBoundingBox().height() - options.getLabelSpacing();
                    
                    Position newPos = GridSnapper.snap(new Position(lx, ly), options.getGridSize());
                    positions.put(node.getId(), newPos);
                    bounds.put(node.getId(), new BoundingBox(newPos.x(), newPos.y(), node.getBoundingBox().width(), node.getBoundingBox().height()));
                }
            }
        }
        
        // 13.2 Ports
        List<LayoutNode> inputPorts = new ArrayList<>();
        List<LayoutNode> outputPorts = new ArrayList<>();
        
        for (LayoutNode node : layeredGraph.getOriginalGraph().getNodes().values()) {
            if (node.getType() == NodeType.PORT_INPUT) inputPorts.add(node);
            else if (node.getType() == NodeType.PORT_OUTPUT) outputPorts.add(node);
        }
        
        int minX = Integer.MAX_VALUE, minY = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE, maxY = Integer.MIN_VALUE;
        boolean hasNodes = false;
        
        for (Map.Entry<String, BoundingBox> entry : bounds.entrySet()) {
            LayoutNode node = layeredGraph.getOriginalGraph().getNode(entry.getKey());
            if (node != null && node.getType() != NodeType.PORT_INPUT && node.getType() != NodeType.PORT_OUTPUT) {
                BoundingBox bb = entry.getValue();
                minX = Math.min(minX, bb.x());
                minY = Math.min(minY, bb.y());
                maxX = Math.max(maxX, bb.x() + bb.width());
                maxY = Math.max(maxY, bb.y() + bb.height());
                hasNodes = true;
            }
        }
        
        if (!hasNodes) {
            minX = 0; minY = 0; maxX = 100; maxY = 100;
        }
        
        int midX = (minX + maxX) / 2;
        int midY = (minY + maxY) / 2;
        
        FlowDirection flowDir = options.getFlowDirection() != null ? options.getFlowDirection() : FlowDirection.TOP_TO_BOTTOM;
        
        if (!inputPorts.isEmpty()) {
            inputPorts.sort(Comparator.comparing(LayoutNode::getId));
            repositionPorts(inputPorts, true, flowDir, options.getPortSpacing(), minX, minY, maxX, maxY, midX, midY, positions, bounds, options.getGridSize());
        }
        
        if (!outputPorts.isEmpty()) {
            outputPorts.sort(Comparator.comparing(LayoutNode::getId));
            repositionPorts(outputPorts, false, flowDir, options.getPortSpacing(), minX, minY, maxX, maxY, midX, midY, positions, bounds, options.getGridSize());
        }
        
        return new CoordinateAssignment(positions, bounds);
    }

    private int adaptiveLayerSpacing(LayeredGraph graph, int layerIndex, int baseSpacing) {
        if (layerIndex >= graph.getLayers().size() - 1) {
            return baseSpacing;
        }
        Map<String, Integer> outgoing = new LinkedHashMap<>();
        Map<String, Integer> incoming = new LinkedHashMap<>();
        for (var edge : graph.getOriginalGraph().getEdges().values()) {
            Integer sourceLayer = graph.getNodeToLayer().get(edge.getSourceNodeId());
            Integer targetLayer = graph.getNodeToLayer().get(edge.getTargetNodeId());
            if (sourceLayer == null || targetLayer == null) {
                continue;
            }
            if (sourceLayer == layerIndex && targetLayer > layerIndex) {
                outgoing.merge(edge.getSourceNodeId(), 1, Integer::sum);
            }
            if (targetLayer == layerIndex + 1 && sourceLayer < targetLayer) {
                incoming.merge(edge.getTargetNodeId(), 1, Integer::sum);
            }
        }
        int branchCount = Math.max(
                outgoing.values().stream().mapToInt(Integer::intValue).max().orElse(0),
                incoming.values().stream().mapToInt(Integer::intValue).max().orElse(0));
        return branchCount > 2 ? baseSpacing + (branchCount - 1) * 25 : baseSpacing;
    }
    
    private void repositionPorts(List<LayoutNode> ports, boolean isInput, FlowDirection flowDir, int portSpacing, 
                                 int minX, int minY, int maxX, int maxY, int midX, int midY, 
                                 Map<String, Position> positions, Map<String, BoundingBox> bounds, int gridSize) {
        
        int total = ports.size();
        for (int i = 0; i < total; i++) {
            LayoutNode port = ports.get(i);
            BoundingBox orig = port.getBoundingBox();
            
            double offsetFactor = i - (total - 1) / 2.0;
            int offset = (int) (offsetFactor * (orig.width() + portSpacing)); 
            
            int nx = orig.x(), ny = orig.y();
            
            if (isInput) {
                switch (flowDir) {
                    case TOP_TO_BOTTOM:
                        nx = midX + offset - orig.width() / 2;
                        ny = minY - orig.height() - portSpacing;
                        break;
                    case BOTTOM_TO_TOP:
                        nx = midX + offset - orig.width() / 2;
                        ny = maxY + portSpacing;
                        break;
                    case LEFT_TO_RIGHT:
                        nx = minX - orig.width() - portSpacing;
                        ny = midY + offset - orig.height() / 2;
                        break;
                    case RIGHT_TO_LEFT:
                        nx = maxX + portSpacing;
                        ny = midY + offset - orig.height() / 2;
                        break;
                }
            } else {
                switch (flowDir) {
                    case TOP_TO_BOTTOM:
                        nx = midX + offset - orig.width() / 2;
                        ny = maxY + portSpacing;
                        break;
                    case BOTTOM_TO_TOP:
                        nx = midX + offset - orig.width() / 2;
                        ny = minY - orig.height() - portSpacing;
                        break;
                    case LEFT_TO_RIGHT:
                        nx = maxX + portSpacing;
                        ny = midY + offset - orig.height() / 2;
                        break;
                    case RIGHT_TO_LEFT:
                        nx = minX - orig.width() - portSpacing;
                        ny = midY + offset - orig.height() / 2;
                        break;
                }
            }
            
            Position newPos = GridSnapper.snap(new Position(nx, ny), gridSize);
            positions.put(port.getId(), newPos);
            bounds.put(port.getId(), new BoundingBox(newPos.x(), newPos.y(), orig.width(), orig.height()));
        }
    }
}
