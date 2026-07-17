package org.apache.nifi.copilot.builder;

import java.util.ArrayList;
import java.util.List;

final class CollisionAvoider {
    private static final int SEARCH_COLUMNS = 100;
    private static final int SEARCH_ROWS = 100;

    private final List<CanvasBounds> occupied;

    CollisionAvoider(final List<CanvasBounds> occupied) {
        this.occupied = new ArrayList<>(occupied);
    }

    private boolean isFree(final double x, final double y, final double width, final double height) {
        final CanvasBounds candidate = new CanvasBounds(x, y, width, height);
        for (CanvasBounds bounds : occupied) {
            if (candidate.overlaps(bounds)) {
                return false;
            }
        }
        return true;
    }

    double[] claim(final double x, final double y, final double width, final double height) {
        for (int row = 0; row < SEARCH_ROWS; row++) {
            for (int col = 0; col < SEARCH_COLUMNS; col++) {
                final double candidateX = x + col * CanvasLayoutEngine.PROCESSOR_SPACING_X;
                final double candidateY = y + row * CanvasLayoutEngine.PROCESSOR_SPACING_Y;
                if (isFree(candidateX, candidateY, width, height)) {
                    final double[] position = new double[]{candidateX, candidateY};
                    occupied.add(new CanvasBounds(candidateX, candidateY, width, height));
                    return position;
                }
            }
        }
        throw new IllegalStateException("No collision-free component position found after "
                + (SEARCH_COLUMNS * SEARCH_ROWS) + " deterministic attempts");
    }
}
