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

package in.shrake.nifi.layout.core.model;

import in.shrake.nifi.layout.core.exception.ConfigurationValidationException;

public final class LayoutOptions {
    private final int horizontalSpacing;
    private final int verticalSpacing;
    private final FlowDirection flowDirection;
    private final int gridSize;
    private final int marginTop;
    private final int marginBottom;
    private final int marginLeft;
    private final int marginRight;
    private final int padding;
    private final AlignmentMode alignmentMode;
    private final int portSpacing;
    private final int labelSpacing;
    private final RoutingMode routingMode;
    private final CrossingMinimizationStrategy crossingStrategy;
    private final PackingStrategy packingStrategy;
    private final boolean incrementalMode;
    private final int maxIterations;

    private LayoutOptions(Builder builder) {
        this.horizontalSpacing = builder.horizontalSpacing;
        this.verticalSpacing = builder.verticalSpacing;
        this.flowDirection = builder.flowDirection;
        this.gridSize = builder.gridSize;
        this.marginTop = builder.marginTop;
        this.marginBottom = builder.marginBottom;
        this.marginLeft = builder.marginLeft;
        this.marginRight = builder.marginRight;
        this.padding = builder.padding;
        this.alignmentMode = builder.alignmentMode;
        this.portSpacing = builder.portSpacing;
        this.labelSpacing = builder.labelSpacing;
        this.routingMode = builder.routingMode;
        this.crossingStrategy = builder.crossingStrategy;
        this.packingStrategy = builder.packingStrategy;
        this.incrementalMode = builder.incrementalMode;
        this.maxIterations = builder.maxIterations;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static LayoutOptions defaults() {
        return builder().build();
    }

    public int getHorizontalSpacing() { return horizontalSpacing; }
    public int getVerticalSpacing() { return verticalSpacing; }
    public FlowDirection getFlowDirection() { return flowDirection; }
    public int getGridSize() { return gridSize; }
    public int getMarginTop() { return marginTop; }
    public int getMarginBottom() { return marginBottom; }
    public int getMarginLeft() { return marginLeft; }
    public int getMarginRight() { return marginRight; }
    public int getPadding() { return padding; }
    public AlignmentMode getAlignmentMode() { return alignmentMode; }
    public int getPortSpacing() { return portSpacing; }
    public int getLabelSpacing() { return labelSpacing; }
    public RoutingMode getRoutingMode() { return routingMode; }
    public CrossingMinimizationStrategy getCrossingStrategy() { return crossingStrategy; }
    public PackingStrategy getPackingStrategy() { return packingStrategy; }
    public boolean isIncrementalMode() { return incrementalMode; }
    public int getMaxIterations() { return maxIterations; }

    public static final class Builder {
        private int horizontalSpacing = 80;
        private int verticalSpacing = 100;
        private FlowDirection flowDirection = FlowDirection.TOP_TO_BOTTOM;
        private int gridSize = 20;
        private int marginTop = 50;
        private int marginBottom = 50;
        private int marginLeft = 50;
        private int marginRight = 50;
        private int padding = 40;
        private AlignmentMode alignmentMode = AlignmentMode.CENTER;
        private int portSpacing = 30;
        private int labelSpacing = 20;
        private RoutingMode routingMode = RoutingMode.ORTHOGONAL;
        private CrossingMinimizationStrategy crossingStrategy = CrossingMinimizationStrategy.MEDIAN;
        private PackingStrategy packingStrategy = PackingStrategy.VERTICAL;
        private boolean incrementalMode = false;
        private int maxIterations = 24;

        private Builder() {}

        public Builder horizontalSpacing(int value) {
            this.horizontalSpacing = value;
            return this;
        }

        public Builder verticalSpacing(int value) {
            this.verticalSpacing = value;
            return this;
        }

        public Builder flowDirection(FlowDirection direction) {
            if (direction != null) {
                this.flowDirection = direction;
            }
            return this;
        }

        public Builder gridSize(int value) {
            this.gridSize = value;
            return this;
        }

        public Builder margins(int top, int bottom, int left, int right) {
            this.marginTop = top;
            this.marginBottom = bottom;
            this.marginLeft = left;
            this.marginRight = right;
            return this;
        }

        public Builder padding(int value) {
            this.padding = value;
            return this;
        }

        public Builder alignmentMode(AlignmentMode mode) {
            if (mode != null) {
                this.alignmentMode = mode;
            }
            return this;
        }

        public Builder portSpacing(int value) {
            this.portSpacing = value;
            return this;
        }

        public Builder labelSpacing(int value) {
            this.labelSpacing = value;
            return this;
        }

        public Builder routingMode(RoutingMode mode) {
            if (mode != null) {
                this.routingMode = mode;
            }
            return this;
        }

        public Builder crossingStrategy(CrossingMinimizationStrategy strategy) {
            if (strategy != null) {
                this.crossingStrategy = strategy;
            }
            return this;
        }

        public Builder packingStrategy(PackingStrategy strategy) {
            if (strategy != null) {
                this.packingStrategy = strategy;
            }
            return this;
        }

        public Builder incrementalMode(boolean enabled) {
            this.incrementalMode = enabled;
            return this;
        }

        public Builder maxIterations(int value) {
            this.maxIterations = value;
            return this;
        }

        public LayoutOptions build() {
            validate();
            return new LayoutOptions(this);
        }

        private void validate() {
            if (horizontalSpacing < 1 || horizontalSpacing > 10000) {
                throw new ConfigurationValidationException("horizontalSpacing", horizontalSpacing, "1-10000");
            }
            if (verticalSpacing < 1 || verticalSpacing > 10000) {
                throw new ConfigurationValidationException("verticalSpacing", verticalSpacing, "1-10000");
            }
            if (gridSize < 1 || gridSize > 1000) {
                throw new ConfigurationValidationException("gridSize", gridSize, "1-1000");
            }
            if (marginTop < 0 || marginTop > 10000) {
                throw new ConfigurationValidationException("marginTop", marginTop, "0-10000");
            }
            if (marginBottom < 0 || marginBottom > 10000) {
                throw new ConfigurationValidationException("marginBottom", marginBottom, "0-10000");
            }
            if (marginLeft < 0 || marginLeft > 10000) {
                throw new ConfigurationValidationException("marginLeft", marginLeft, "0-10000");
            }
            if (marginRight < 0 || marginRight > 10000) {
                throw new ConfigurationValidationException("marginRight", marginRight, "0-10000");
            }
            if (padding < 0 || padding > 10000) {
                throw new ConfigurationValidationException("padding", padding, "0-10000");
            }
            if (portSpacing < 1 || portSpacing > 1000) {
                throw new ConfigurationValidationException("portSpacing", portSpacing, "1-1000");
            }
            if (labelSpacing < 0 || labelSpacing > 1000) {
                throw new ConfigurationValidationException("labelSpacing", labelSpacing, "0-1000");
            }
            if (maxIterations < 1 || maxIterations > 1000) {
                throw new ConfigurationValidationException("maxIterations", maxIterations, "1-1000");
            }
        }
    }
}
