package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.toStringList;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Configures auto-terminated relationships for managed processors after connections
 * have been deployed.  Stateless.
 */
final class RelationshipConfigurer {

    private RelationshipConfigurer() {
    }

    /**
     * For each managed processor, collects the relationships that are consumed by
     * outgoing connections, then delegates to NiFi to auto-terminate the remainder.
     */
    static void configure(
            final List<Map<String, Object>> managedProcessors,
            final List<Map<String, Object>> connectionSpecs,
            final ComponentRegistry components,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        for (Map<String, Object> managedProcessor : managedProcessors) {
            final String processorId = String.valueOf(managedProcessor.get("id"));
            try {
                final Set<String> usedRelationships =
                        findUsedRelationships(processorId, connectionSpecs, components, resolver);
                nifi.autoTerminateUnusedRelationships(processorId, usedRelationships);
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.RELATIONSHIP,
                        FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (RuntimeException e) {
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.RELATIONSHIP,
                        FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                throw e;
            }
        }
    }

    /**
     * Returns the explicit relationships for a connection, or {@code ["success"]} when
     * the source is a PROCESSOR with no explicit list, or an empty list otherwise.
     * Called by connection deployment code as well.
     */
    static List<String> resolveRelationships(
            final Map<String, Object> connectionSpec,
            final String sourceType) {
        final List<String> relationships = toStringList(connectionSpec.get("relationships"));
        if (!relationships.isEmpty()) {
            return relationships;
        }
        return "PROCESSOR".equals(sourceType) ? List.of("success") : List.of();
    }

    private static Set<String> findUsedRelationships(
            final String processorId,
            final List<Map<String, Object>> connectionSpecs,
            final ComponentRegistry components,
            final ComponentResolver resolver) {
        final Set<String> usedRelationships = new HashSet<>();
        for (Map<String, Object> connectionSpec : connectionSpecs) {
            if (processorId.equals(resolver.resolveEndpointId(connectionSpec.get("from"), components))) {
                usedRelationships.addAll(resolveRelationships(connectionSpec, "PROCESSOR"));
            }
        }
        return usedRelationships;
    }
}
