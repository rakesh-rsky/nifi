package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Deploys and enables controller services in dependency order.
 * Request-local: holds the spec-ID to NiFi-ID mapping used to resolve
 * controller-service references inside processor and service property maps.
 */
final class ControllerServiceDeployer {

    private final Map<String, String> specToNifi = new HashMap<>();

    /**
     * Deploys all {@code services} in dependency order, reusing any existing service
     * that matches by name and type, and enables each service that is not already enabled.
     */
    void deployAll(
            final List<Map<String, Object>> services,
            final String pgId,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Map<String, Object>> existing = nifi.listControllerServices(pgId);
        for (Map<String, Object> controllerService : dependencyOrder(services)) {
            final String specId = String.valueOf(controllerService.get("id"));
            final String serviceName = String.valueOf(controllerService.get("name"));
            final String serviceType = String.valueOf(controllerService.get("type"));
            final Map<String, Object> reuse =
                    resolver.matchControllerService(existing, serviceName, serviceType);
            try {
                final String nifiId;
                final boolean alreadyEnabled;
                if (reuse != null) {
                    nifiId = String.valueOf(reuse.get("id"));
                    alreadyEnabled = "ENABLED".equals(
                            String.valueOf(reuse.getOrDefault("state", "DISABLED")));
                    specToNifi.put(specId, nifiId);
                } else {
                    final Map<String, Object> result = nifi.createControllerService(
                            pgId,
                            serviceType,
                            serviceName,
                            resolveCsReferences(mapOrEmpty(controllerService.get("properties"))));
                    final Object resultId = result.get("id");
                    if (resultId == null || String.valueOf(resultId).isBlank()) {
                        throw new IllegalStateException("Controller service creation returned no ID");
                    }
                    nifiId = String.valueOf(resultId);
                    specToNifi.put(specId, nifiId);
                    ledger.addCreatedControllerServiceId(nifiId);
                    alreadyEnabled = false;
                }
                if (!alreadyEnabled) {
                    nifi.enableControllerService(nifiId);
                }
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.CONTROLLER_SERVICE,
                        reuse != null ? FlowDeploymentMetricsRegistry.ComponentAction.REUSED
                                      : FlowDeploymentMetricsRegistry.ComponentAction.CREATED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.CONTROLLER_SERVICE,
                        reuse != null ? FlowDeploymentMetricsRegistry.ComponentAction.REUSED
                                      : FlowDeploymentMetricsRegistry.ComponentAction.CREATED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                throw new IllegalStateException(
                        "Failed creating or enabling controller service " + serviceName, e);
            }
        }
    }

    /**
     * Replaces spec-level controller-service ID references in {@code properties} with
     * their resolved NiFi IDs.  Returns {@code properties} unchanged when no mapping exists.
     */
    Map<String, Object> resolveCsReferences(final Map<String, Object> properties) {
        if (specToNifi.isEmpty()) {
            return properties;
        }
        final Map<String, Object> out = new HashMap<>();
        for (var e : properties.entrySet()) {
            out.put(e.getKey(),
                    specToNifi.getOrDefault(String.valueOf(e.getValue()), String.valueOf(e.getValue())));
        }
        return out;
    }

    private static List<Map<String, Object>> dependencyOrder(
            final List<Map<String, Object>> services) {
        final Map<String, Map<String, Object>> servicesById = new LinkedHashMap<>();
        for (Map<String, Object> service : services) {
            final String serviceId = String.valueOf(service.get("id"));
            if (servicesById.putIfAbsent(serviceId, service) != null) {
                throw new IllegalArgumentException("Duplicate controller service ID: " + serviceId);
            }
        }
        final Map<String, Integer> visitStates = new HashMap<>();
        final List<String> visitPath = new ArrayList<>();
        final List<Map<String, Object>> ordered = new ArrayList<>();
        for (String serviceId : servicesById.keySet()) {
            visitService(serviceId, servicesById, visitStates, visitPath, ordered);
        }
        return ordered;
    }

    private static void visitService(
            final String serviceId,
            final Map<String, Map<String, Object>> servicesById,
            final Map<String, Integer> visitStates,
            final List<String> visitPath,
            final List<Map<String, Object>> ordered) {
        final int state = visitStates.getOrDefault(serviceId, 0);
        if (state == 2) {
            return;
        }
        if (state == 1) {
            final int cycleStart = visitPath.indexOf(serviceId);
            final List<String> cycle = new ArrayList<>(
                    visitPath.subList(cycleStart < 0 ? 0 : cycleStart, visitPath.size()));
            cycle.add(serviceId);
            throw new IllegalArgumentException(
                    "Controller service dependency cycle: " + String.join(" -> ", cycle));
        }
        visitStates.put(serviceId, 1);
        visitPath.add(serviceId);
        final Map<String, Object> service = servicesById.get(serviceId);
        for (String dependencyId : serviceDependencies(service, servicesById.keySet())) {
            visitService(dependencyId, servicesById, visitStates, visitPath, ordered);
        }
        visitPath.remove(visitPath.size() - 1);
        visitStates.put(serviceId, 2);
        ordered.add(service);
    }

    private static Set<String> serviceDependencies(
            final Map<String, Object> service,
            final Set<String> serviceIds) {
        final Set<String> dependencies = new LinkedHashSet<>();
        for (Object propertyValue : mapOrEmpty(service.get("properties")).values()) {
            final String candidateId = String.valueOf(propertyValue);
            if (serviceIds.contains(candidateId)) {
                dependencies.add(candidateId);
            }
        }
        return dependencies;
    }
}
