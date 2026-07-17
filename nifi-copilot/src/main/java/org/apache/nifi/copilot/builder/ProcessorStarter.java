package org.apache.nifi.copilot.builder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines processor startup order and starts processors when auto-start is enabled.
 * Stateless.
 */
final class ProcessorStarter {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorStarter.class);

    private ProcessorStarter() {
    }

    /**
     * Starts processors in downstream-first topological order when {@code autoStart} is
     * {@code true}.  Processors involved in a dependency cycle are appended in declaration
     * order with a warning.
     */
    static void startIfRequested(
            final boolean autoStart,
            final List<Map<String, Object>> processorSpecs,
            final List<Map<String, Object>> connectionSpecs,
            final ComponentRegistry components,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        if (!autoStart) {
            return;
        }
        final List<String> order = startupOrder(processorSpecs, connectionSpecs);
        startProcessors(order, components, nifi, metrics);
    }

    private static List<String> startupOrder(
            final List<Map<String, Object>> processors,
            final List<Map<String, Object>> connections) {
        final Set<String> processorIds = new LinkedHashSet<>();
        for (Map<String, Object> processor : processors) {
            processorIds.add(String.valueOf(processor.get("id")));
        }

        final Map<String, Set<String>> downstreamByProcessor = new LinkedHashMap<>();
        final Map<String, Set<String>> upstreamByProcessor = new LinkedHashMap<>();
        for (String processorId : processorIds) {
            downstreamByProcessor.put(processorId, new LinkedHashSet<>());
            upstreamByProcessor.put(processorId, new LinkedHashSet<>());
        }

        for (Map<String, Object> connection : connections) {
            final String source = String.valueOf(connection.get("from"));
            final String destination = String.valueOf(connection.get("to"));
            if (processorIds.contains(source) && processorIds.contains(destination)
                    && downstreamByProcessor.get(source).add(destination)) {
                upstreamByProcessor.get(destination).add(source);
            }
        }

        final Map<String, Integer> remainingDownstreamCounts = new LinkedHashMap<>();
        final Deque<String> ready = new ArrayDeque<>();
        for (String processorId : processorIds) {
            final int downstreamCount = downstreamByProcessor.get(processorId).size();
            remainingDownstreamCounts.put(processorId, downstreamCount);
            if (downstreamCount == 0) {
                ready.addLast(processorId);
            }
        }

        final List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            final String processorId = ready.removeFirst();
            order.add(processorId);
            for (String upstreamId : upstreamByProcessor.get(processorId)) {
                final Integer downstreamCount = remainingDownstreamCounts.get(upstreamId);
                if (downstreamCount == null) {
                    continue;
                }
                final int remaining = downstreamCount - 1;
                remainingDownstreamCounts.put(upstreamId, remaining);
                if (remaining == 0) {
                    ready.addLast(upstreamId);
                }
            }
        }

        if (order.size() < processorIds.size()) {
            final List<String> cycleMembers = new ArrayList<>();
            for (String processorId : processorIds) {
                if (!order.contains(processorId)) {
                    cycleMembers.add(processorId);
                }
            }
            logger.warn("Processor dependency cycle detected; using declaration order for: {}", cycleMembers);
            order.addAll(cycleMembers);
        }
        return order;
    }

    private static void startProcessors(
            final List<String> startupOrder,
            final ComponentRegistry components,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        for (String specId : startupOrder) {
            final String processorId = components.id(specId);
            if (processorId == null) {
                continue;
            }
            if (nifi.waitForProcessorValid(processorId, 30)) {
                try {
                    nifi.startProcessor(processorId);
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PROCESSOR,
                            FlowDeploymentMetricsRegistry.ComponentAction.STARTED,
                            FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
                } catch (Exception e) {
                    logger.warn("Failed to start processor {}: {}", processorId, e.getMessage());
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PROCESSOR,
                            FlowDeploymentMetricsRegistry.ComponentAction.STARTED,
                            FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                }
            } else {
                logger.warn("Processor {} did not become valid before startup timeout", processorId);
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PROCESSOR,
                        FlowDeploymentMetricsRegistry.ComponentAction.STARTED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
            }
        }
    }
}
