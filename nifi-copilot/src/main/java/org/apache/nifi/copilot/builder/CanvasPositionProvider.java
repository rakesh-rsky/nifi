package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.finiteRequiredNumber;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.numericValue;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Supplies provisional creation coordinates for new canvas components.
 * Explicit spec values are used as-is; absent values fall back to the canvas
 * start position. DAG ranking and collision avoidance are handled by the
 * dedicated {@link LayoutDeploymentStage} after all connections are created.
 */
final class CanvasPositionProvider {
    private static final int CANVAS_START_X = 100;
    private static final int CANVAS_START_Y = 200;

    /**
     * Returns {@code [x, y]} from the specification, falling back to the canvas
     * start position for each absent coordinate.
     */
    double[] provisionalPosition(final Map<String, Object> specification) {
        return new double[]{
            xOrDefault(specification.get("x")),
            yOrDefault(specification.get("y"))
        };
    }

    double xOrDefault(final Object value) {
        return numericValue(value, CANVAS_START_X);
    }

    double yOrDefault(final Object value) {
        return numericValue(value, CANVAS_START_Y);
    }

    double defaultX() {
        return CANVAS_START_X;
    }

    double defaultY() {
        return CANVAS_START_Y;
    }

    /**
     * Returns a position update map when the specification contains at least one
     * of {@code x} or {@code y}; returns an empty map otherwise.
     * Used by update paths where the original component's existing position is
     * preserved for any coordinate not present in the specification.
     */
    Map<String, Object> requestedPosition(
            final Map<String, Object> specification,
            final Map<String, Object> originalComponent) {
        if (!specification.containsKey("x") && !specification.containsKey("y")) {
            return new LinkedHashMap<>();
        }
        final Map<String, Object> original = mapOrEmpty(originalComponent.get("position"));
        final Map<String, Object> position = new LinkedHashMap<>();
        position.put("x", specification.containsKey("x")
                ? finiteRequiredNumber(specification.get("x"), "x")
                : numericValue(original.get("x"), 0));
        position.put("y", specification.containsKey("y")
                ? finiteRequiredNumber(specification.get("y"), "y")
                : numericValue(original.get("y"), 0));
        final Map<String, Object> updates = new LinkedHashMap<>();
        updates.put("position", position);
        return updates;
    }
}
