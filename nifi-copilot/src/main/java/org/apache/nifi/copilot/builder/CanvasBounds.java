package org.apache.nifi.copilot.builder;

record CanvasBounds(double x, double y, double width, double height) {
    private static final int PADDING_X = 24;
    private static final int PADDING_Y = 24;

    boolean overlaps(final CanvasBounds other) {
        return x < other.x + other.width + PADDING_X
                && x + width + PADDING_X > other.x
                && y < other.y + other.height + PADDING_Y
                && y + height + PADDING_Y > other.y;
    }
}
