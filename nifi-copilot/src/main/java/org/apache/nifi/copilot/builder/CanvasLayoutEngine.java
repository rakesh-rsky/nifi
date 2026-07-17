package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.componentMap;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.componentValueOrDefault;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.effectiveFlow;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.entityId;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.finiteRequiredNumber;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.numericValue;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.optionalPositiveNumber;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;
import static org.apache.nifi.copilot.builder.SpecificationSupport.stringOrNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

final class CanvasLayoutEngine {
    private static final int CANVAS_START_X = 100;
    private static final int CANVAS_START_Y = 200;
    static final int PROCESSOR_SPACING_X = 400;
    static final int PROCESSOR_SPACING_Y = 250;
    private static final double PROCESSOR_WIDTH = 352;
    private static final double PROCESSOR_HEIGHT = 128;
    private static final double PORT_WIDTH = 240;
    private static final double PORT_HEIGHT = 80;
    private static final double FUNNEL_WIDTH = 48;
    private static final double FUNNEL_HEIGHT = 48;
    private static final double LABEL_WIDTH = 200;
    private static final double LABEL_HEIGHT = 80;
    private static final double REMOTE_PROCESS_GROUP_WIDTH = 384;
    private static final double REMOTE_PROCESS_GROUP_HEIGHT = 176;

    CollisionAvoider prepare(
            final DeploymentContext context,
            final Map<String, Object> flowData) {
        final Map<String, Object> specification = context.specification();
        final List<Map<String, Object>> processors = listOfMap(specification.get("processors"));
        final Set<String> reusedProcessorIds = reusedProcessorIds(processors, context.existingIds());
        final Set<String> reusedProcessorSpecIds = reusedProcessorSpecIds(
                processors, context.existingIds(), effectiveFlow(flowData));
        applyDAGLayout(specification, Math.max(0, context.existingCount() - reusedProcessorIds.size()),
                reusedProcessorSpecIds);
        final CollisionAvoider collisionAvoider = new CollisionAvoider(readOccupiedBounds(flowData));
        applyCollisionAvoidance(specification, reusedProcessorSpecIds, collisionAvoider);
        return collisionAvoider;
    }

    private void applyDAGLayout(
            final Map<String, Object> specification,
            final int existingCount,
            final Set<String> reusedProcessorSpecIds) {
        final List<Map<String, Object>> processors = listOfMap(specification.get("processors"));
        final Map<String, Integer> ranks = processorRanks(
                processors, listOfMap(specification.get("connections")));
        final Map<Integer, Integer> processorCountsByRank = processorCountsByRank(
                processors, ranks, reusedProcessorSpecIds);
        final int largestRank = processorCountsByRank.values().stream()
                .mapToInt(Integer::intValue)
                .max()
                .orElse(1);
        final Map<Integer, Integer> rowsByRank = new HashMap<>();
        final int xOffset = existingCount * PROCESSOR_SPACING_X;
        processors.sort((left, right) -> String.valueOf(left.get("id"))
                .compareTo(String.valueOf(right.get("id"))));
        for (Map<String, Object> processor : processors) {
            if (reusedProcessorSpecIds.contains(String.valueOf(processor.get("id")))) {
                continue;
            }
            final int rank = ranks.getOrDefault(String.valueOf(processor.get("id")), 0);
            final int row = rowsByRank.getOrDefault(rank, 0);
            rowsByRank.put(rank, row + 1);
            if (processor.get("x") == null) {
                processor.put("x", CANVAS_START_X + xOffset + rank * PROCESSOR_SPACING_X);
            }
            if (processor.get("y") == null) {
                final int rankSize = processorCountsByRank.getOrDefault(rank, 1);
                final double centeredOffset = (largestRank - rankSize)
                        * PROCESSOR_SPACING_Y / 2.0;
                processor.put("y", CANVAS_START_Y + centeredOffset + row * PROCESSOR_SPACING_Y);
            }
        }
    }

    private static Map<Integer, Integer> processorCountsByRank(
            final List<Map<String, Object>> processors,
            final Map<String, Integer> ranks,
            final Set<String> reusedProcessorSpecIds) {
        final Map<Integer, Integer> counts = new HashMap<>();
        for (Map<String, Object> processor : processors) {
            final String id = String.valueOf(processor.get("id"));
            if (!reusedProcessorSpecIds.contains(id)) {
                counts.merge(ranks.getOrDefault(id, 0), 1, Integer::sum);
            }
        }
        return counts;
    }

    private static Map<String, Integer> processorRanks(
            final List<Map<String, Object>> processors,
            final List<Map<String, Object>> connections) {
        final Map<String, Set<String>> outgoing = new LinkedHashMap<>();
        final Map<String, Integer> indegrees = new HashMap<>();
        final Map<String, Integer> ranks = new HashMap<>();
        for (Map<String, Object> processor : processors) {
            final String id = String.valueOf(processor.get("id"));
            outgoing.put(id, new TreeSet<>());
            indegrees.put(id, 0);
            ranks.put(id, 0);
        }
        addProcessorEdges(connections, outgoing, indegrees);
        rankProcessors(outgoing, indegrees, ranks);
        return ranks;
    }

    private static void addProcessorEdges(
            final List<Map<String, Object>> connections,
            final Map<String, Set<String>> outgoing,
            final Map<String, Integer> indegrees) {
        for (Map<String, Object> connection : connections) {
            final String source = stringOrNull(connection.get("from"));
            final String destination = stringOrNull(connection.get("to"));
            if (source != null && destination != null && outgoing.containsKey(source)
                    && outgoing.containsKey(destination) && outgoing.get(source).add(destination)) {
                indegrees.put(destination, indegrees.get(destination) + 1);
            }
        }
    }

    private static void rankProcessors(
            final Map<String, Set<String>> outgoing,
            final Map<String, Integer> indegrees,
            final Map<String, Integer> ranks) {
        final PriorityQueue<String> ready = new PriorityQueue<>();
        indegrees.forEach((id, degree) -> {
            if (degree == 0) {
                ready.add(id);
            }
        });
        final Set<String> placed = new HashSet<>();
        while (placed.size() < outgoing.size()) {
            if (ready.isEmpty()) {
                ready.add(outgoing.keySet().stream()
                        .filter(id -> !placed.contains(id)).sorted().findFirst().orElseThrow());
            }
            final String source = ready.remove();
            if (!placed.add(source)) {
                continue;
            }
            for (String destination : outgoing.get(source)) {
                if (placed.contains(destination)) {
                    continue;
                }
                ranks.put(destination, Math.max(ranks.get(destination), ranks.get(source) + 1));
                final int remaining = indegrees.get(destination) - 1;
                indegrees.put(destination, remaining);
                if (remaining == 0) {
                    ready.add(destination);
                }
            }
        }
    }

    private void applyCollisionAvoidance(
            final Map<String, Object> specification,
            final Set<String> reusedProcessorIds,
            final CollisionAvoider collisionAvoider) {
        final List<Map<String, Object>> processors = listOfMap(specification.get("processors"));
        processors.sort((left, right) -> String.valueOf(left.get("id"))
                .compareTo(String.valueOf(right.get("id"))));
        for (Map<String, Object> processor : processors) {
            final String existingId = processor.get("id") == null
                    ? null : String.valueOf(processor.get("id"));
            if (existingId != null && reusedProcessorIds.contains(existingId)) {
                continue;
            }
            final double[] claimed = collisionAvoider.claim(
                    xOrDefault(processor.get("x")),
                    yOrDefault(processor.get("y")),
                    PROCESSOR_WIDTH,
                    PROCESSOR_HEIGHT);
            processor.put("x", claimed[0]);
            processor.put("y", claimed[1]);
        }
    }

    double[] claimPortPosition(
            final Map<String, Object> specification,
            final CollisionAvoider collisionAvoider) {
        return claimPosition(specification, collisionAvoider, PORT_WIDTH, PORT_HEIGHT);
    }

    double[] claimFunnelPosition(
            final Map<String, Object> specification,
            final CollisionAvoider collisionAvoider) {
        return claimPosition(specification, collisionAvoider, FUNNEL_WIDTH, FUNNEL_HEIGHT);
    }

    double[] claimLabelPosition(
            final Map<String, Object> specification,
            final CollisionAvoider collisionAvoider) {
        final double width = optionalPositiveNumber(
                specification.get("width"), LABEL_WIDTH, "label width");
        final double height = optionalPositiveNumber(
                specification.get("height"), LABEL_HEIGHT, "label height");
        return claimPosition(specification, collisionAvoider, width, height);
    }

    double[] claimRemoteProcessGroupPosition(
            final Map<String, Object> specification,
            final CollisionAvoider collisionAvoider) {
        return claimPosition(
                specification, collisionAvoider,
                REMOTE_PROCESS_GROUP_WIDTH, REMOTE_PROCESS_GROUP_HEIGHT);
    }

    private double[] claimPosition(
            final Map<String, Object> specification,
            final CollisionAvoider collisionAvoider,
            final double width,
            final double height) {
        return collisionAvoider.claim(
                xOrDefault(specification.get("x")),
                yOrDefault(specification.get("y")),
                width,
                height);
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

    private static List<CanvasBounds> readOccupiedBounds(final Map<String, Object> flowData) {
        final Map<String, Object> flow = effectiveFlow(flowData);
        final List<CanvasBounds> occupied = new ArrayList<>();
        addComponentBounds(occupied, flow, "processors", PROCESSOR_WIDTH, PROCESSOR_HEIGHT);
        addComponentBounds(occupied, flow, "processGroups",
                REMOTE_PROCESS_GROUP_WIDTH, REMOTE_PROCESS_GROUP_HEIGHT);
        addComponentBounds(occupied, flow, "remoteProcessGroups",
                REMOTE_PROCESS_GROUP_WIDTH, REMOTE_PROCESS_GROUP_HEIGHT);
        addComponentBounds(occupied, flow, "inputPorts", PORT_WIDTH, PORT_HEIGHT);
        addComponentBounds(occupied, flow, "outputPorts", PORT_WIDTH, PORT_HEIGHT);
        addComponentBounds(occupied, flow, "funnels", FUNNEL_WIDTH, FUNNEL_HEIGHT);
        addComponentBounds(occupied, flow, "labels", LABEL_WIDTH, LABEL_HEIGHT);
        return occupied;
    }

    private static void addComponentBounds(
            final List<CanvasBounds> occupied,
            final Map<String, Object> flow,
            final String collection,
            final double defaultWidth,
            final double defaultHeight) {
        for (Map<String, Object> entity : listOfMap(flow.get(collection))) {
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final Map<String, Object> position = componentMap(entity, component, "position");
            final Map<String, Object> dimensions = componentMap(entity, component, "dimensions");
            final Object width = "labels".equals(collection)
                    ? componentValueOrDefault(component, entity, "width", defaultWidth)
                    : dimensions.get("width");
            final Object height = "labels".equals(collection)
                    ? componentValueOrDefault(component, entity, "height", defaultHeight)
                    : dimensions.get("height");
            occupied.add(new CanvasBounds(
                    numericValue(position.get("x"), 0),
                    numericValue(position.get("y"), 0),
                    positiveNumericValue(width, defaultWidth),
                    positiveNumericValue(height, defaultHeight)));
        }
    }

    private static double positiveNumericValue(final Object value, final double defaultValue) {
        final double number = numericValue(value, defaultValue);
        return number > 0 ? number : defaultValue;
    }

    private static Set<String> reusedProcessorIds(
            final List<Map<String, Object>> processorSpecifications,
            final Map<String, String> existingIdMap) {
        if (existingIdMap == null || existingIdMap.isEmpty()) {
            return Set.of();
        }
        final Set<String> reusedIds = new HashSet<>();
        for (Map<String, Object> processorSpecification : processorSpecifications) {
            final String nifiId = existingIdMap.get(
                    String.valueOf(processorSpecification.get("id")));
            if (nifiId != null && !nifiId.isBlank()) {
                reusedIds.add(nifiId);
            }
        }
        return reusedIds;
    }

    private static Set<String> reusedProcessorSpecIds(
            final List<Map<String, Object>> processorSpecifications,
            final Map<String, String> existingIdMap,
            final Map<String, Object> flow) {
        final Set<String> inventoryIds = new HashSet<>();
        listOfMap(flow.get("processors")).forEach(entity -> inventoryIds.add(entityId(entity)));
        final Set<String> reused = new HashSet<>();
        for (Map<String, Object> processor : processorSpecifications) {
            final String specId = String.valueOf(processor.get("id"));
            final String mappedId = existingIdMap == null ? null : existingIdMap.get(specId);
            if ((mappedId != null && !mappedId.isBlank()) || inventoryIds.contains(specId)) {
                reused.add(specId);
            }
        }
        return reused;
    }
}
