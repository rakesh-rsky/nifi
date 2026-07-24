package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireCreatedComponentId;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;
import static org.apache.nifi.copilot.builder.SpecificationSupport.toStringList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Creates or updates connections and returns the count of newly created connections.
 * Stateless; all ownership state is recorded in the caller-supplied {@link OwnershipLedger}.
 */
final class ConnectionDeployer {

    private ConnectionDeployer() {
    }

    /**
     * Iterates {@code connectionSpecs}, matching each to an existing connection by
     * source/destination ID and type or creating a new one.  Empty specs return 0
     * without any NiFi calls.
     *
     * <p>Ownership registration timing:
     * <ul>
     *   <li>Restore entry added before {@code updateConnection} call for reused connections.</li>
     *   <li>Created-ID entry added immediately after ID extraction for new connections.</li>
     * </ul>
     *
     * @return count of newly created connections
     */
    static int deploy(
            final List<Map<String, Object>> connectionSpecs,
            final String processGroupId,
            final ComponentRegistry components,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final List<Map<String, Object>> existingConnections,
            final FlowDeploymentMetricsRegistry metrics) {
        if (connectionSpecs.isEmpty()) {
            return 0;
        }
        final List<Exception> failures = new ArrayList<>();
        final Map<String, List<String>> requestedRelationshipsByConnectionId = new HashMap<>();
        int connectionsCreated = 0;
        for (Map<String, Object> connectionSpec : connectionSpecs) {
            FlowDeploymentMetricsRegistry.ComponentAction connAction = null;
            try {
                final String sourceReference = String.valueOf(connectionSpec.get("from"));
                final String destinationReference = String.valueOf(connectionSpec.get("to"));
                final String source = resolver.resolveEndpointId(sourceReference, components);
                final String destination = resolver.resolveEndpointId(destinationReference, components);
                if (source == null || destination == null) {
                    throw new IllegalStateException("Connection endpoint could not be resolved: from="
                            + connectionSpec.get("from") + ", to=" + connectionSpec.get("to"));
                }
                if (source.equals(destination)
                        && !Boolean.TRUE.equals(connectionSpec.get("allow_self_loop"))) {
                    throw new IllegalArgumentException("Resolved connection endpoints refer to the same component; "
                            + "set allow_self_loop=true only for an intentional feedback loop");
                }
                final String sourceType = resolver.resolveEndpointType(
                        sourceReference, connectionSpec.get("from_type"), components);
                final String destinationType = resolver.resolveEndpointType(
                        destinationReference, connectionSpec.get("to_type"), components);
                final List<String> relationships =
                        RelationshipConfigurer.resolveRelationships(connectionSpec, sourceType);
                final Map<String, Object> existing = resolver.matchConnection(
                        existingConnections, source, sourceType, destination, destinationType);
                if (existing != null) {
                    final String existingId = requireEntityId(existing, "connection");
                    final List<String> previousRelationships =
                            requestedRelationshipsByConnectionId.putIfAbsent(existingId, relationships);
                    if (previousRelationships != null && !previousRelationships.equals(relationships)) {
                        connAction = FlowDeploymentMetricsRegistry.ComponentAction.SKIPPED;
                        throw new IllegalArgumentException(
                                "Duplicate connection specifications have conflicting relationships");
                    }
                    if (previousRelationships == null) {
                        connAction = FlowDeploymentMetricsRegistry.ComponentAction.UPDATED;
                        updateExistingConnection(existing, relationships, ledger, nifi);
                        ledger.addChangedCanvasId(existingId);
                        ledger.addChangedCanvasId(source);
                        ledger.addChangedCanvasId(destination);
                    } else {
                        connAction = FlowDeploymentMetricsRegistry.ComponentAction.REUSED;
                    }
                } else {
                    final Map<String, Object> reassigned = releaseRelationships(
                            existingConnections, source, sourceType, destination, destinationType,
                            relationships, ledger, nifi);
                    if (reassigned != null) {
                        connAction = FlowDeploymentMetricsRegistry.ComponentAction.UPDATED;
                        final String connectionId = requireEntityId(reassigned, "connection");
                        requestedRelationshipsByConnectionId.put(connectionId, relationships);
                        ledger.addChangedCanvasId(connectionId);
                        ledger.addChangedCanvasId(source);
                        ledger.addChangedCanvasId(destination);
                    } else {
                        connAction = FlowDeploymentMetricsRegistry.ComponentAction.CREATED;
                        final Map<String, Object> result = nifi.createConnection(
                                processGroupId,
                                source,
                                sourceType,
                                destination,
                                destinationType,
                                relationships);
                        final String connectionId = requireCreatedComponentId(result, "connection");
                        ledger.addCreatedConnectionId(connectionId);
                        ledger.addChangedCanvasId(connectionId);
                        ledger.addChangedCanvasId(source);
                        ledger.addChangedCanvasId(destination);
                        requestedRelationshipsByConnectionId.put(connectionId, relationships);
                        final Map<String, Object> createdComponent = new LinkedHashMap<>();
                        createdComponent.put("id", connectionId);
                        createdComponent.put("source", Map.of("id", source, "type", sourceType));
                        createdComponent.put("destination", Map.of("id", destination, "type", destinationType));
                        createdComponent.put("selectedRelationships", relationships);
                        existingConnections.add(Map.of("id", connectionId, "component", createdComponent));
                        connectionsCreated++;
                    }
                }
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.CONNECTION,
                        connAction, FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                if (connAction != null) {
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.CONNECTION,
                            connAction, FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                }
                failures.add(new IllegalStateException("Connection from " + connectionSpec.get("from")
                        + " to " + connectionSpec.get("to") + " failed: " + e.getMessage(), e));
            }
        }
        if (!failures.isEmpty()) {
            throwAggregated("Connection deployment", failures);
        }
        return connectionsCreated;
    }

    private static Map<String, Object> releaseRelationships(
            final List<Map<String, Object>> existingConnections,
            final String source,
            final String sourceType,
            final String destination,
            final String destinationType,
            final List<String> requestedRelationships,
            final OwnershipLedger ledger,
            final NiFiClientOperations nifi) {
        for (int index = 0; index < existingConnections.size(); index++) {
            final Map<String, Object> entity = existingConnections.get(index);
            final Map<String, Object> component = new LinkedHashMap<>(mapOrEmpty(entity.get("component")));
            final Map<String, Object> existingSource = mapOrEmpty(component.get("source"));
            final Map<String, Object> existingDestination = mapOrEmpty(component.get("destination"));
            if (!source.equals(String.valueOf(existingSource.get("id")))
                    || !sourceType.equalsIgnoreCase(String.valueOf(existingSource.get("type")))
                    || destination.equals(String.valueOf(existingDestination.get("id")))) {
                continue;
            }
            final List<String> currentRelationships = toStringList(component.get("selectedRelationships"));
            final List<String> overlapping = currentRelationships.stream()
                    .filter(requestedRelationships::contains)
                    .toList();
            if (overlapping.isEmpty()) {
                continue;
            }

            final String connectionId = requireEntityId(entity, "connection");
            ledger.addConnectionRestore(connectionId, new LinkedHashMap<>(component));
            ledger.addChangedCanvasId(connectionId);
            ledger.addChangedCanvasId(String.valueOf(existingDestination.get("id")));
            final List<String> remaining = currentRelationships.stream()
                    .filter(relationship -> !requestedRelationships.contains(relationship))
                    .toList();
            final Map<String, Object> updates = new LinkedHashMap<>();
            if (remaining.isEmpty()) {
                final Map<String, Object> newDestination =
                        Map.of("id", destination, "type", destinationType);
                updates.put("destination", newDestination);
                updates.put("selectedRelationships", requestedRelationships);
                nifi.updateConnection(connectionId, updates);
                component.put("destination", newDestination);
                component.put("selectedRelationships", requestedRelationships);
                final Map<String, Object> updatedEntity = updatedEntity(entity, component);
                existingConnections.set(index, updatedEntity);
                return updatedEntity;
            }

            updates.put("selectedRelationships", remaining);
            nifi.updateConnection(connectionId, updates);
            component.put("selectedRelationships", remaining);
            existingConnections.set(index, updatedEntity(entity, component));
        }
        return null;
    }

    private static Map<String, Object> updatedEntity(
            final Map<String, Object> entity,
            final Map<String, Object> component) {
        final Map<String, Object> updated = new LinkedHashMap<>(entity);
        updated.put("component", component);
        return updated;
    }

    private static void updateExistingConnection(
            final Map<String, Object> existing,
            final List<String> relationships,
            final OwnershipLedger ledger,
            final NiFiClientOperations nifi) {
        final String connectionId = requireEntityId(existing, "connection");
        final Map<String, Object> originalComponent = new LinkedHashMap<>(mapOrEmpty(existing.get("component")));
        ledger.addConnectionRestore(connectionId, originalComponent);
        nifi.updateConnection(connectionId, Map.of("selectedRelationships", relationships));
    }

    private static void throwAggregated(final String stage, final List<Exception> failures) {
        final IllegalStateException aggregate = new IllegalStateException(
                stage + " failed with " + failures.size() + " error(s)");
        failures.forEach(aggregate::addSuppressed);
        throw aggregate;
    }
}
