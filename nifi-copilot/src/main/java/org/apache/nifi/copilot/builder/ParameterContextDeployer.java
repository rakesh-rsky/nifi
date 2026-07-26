package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Deploys and binds a parameter context for a process group.
 * Stateless; all state lives in the caller-supplied {@link OwnershipLedger}.
 */
final class ParameterContextDeployer {

    private ParameterContextDeployer() {
    }

    /**
     * Creates or reuses a parameter context and binds it to the given process group.
     * Does nothing when {@code parameterContextSpec} is {@code null}.
     * Registers ownership in {@code ledger} before binding so that a bind failure
     * triggers rollback of a freshly created context.
     */
    static void deploy(
            final Map<String, Object> parameterContextSpec,
            final String processGroupId,
            final String previousBindingId,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        if (parameterContextSpec == null) {
            return;
        }
        final String requestedName = String.valueOf(parameterContextSpec.get("name"));
        final Map<String, String> params = new LinkedHashMap<>();
        for (var e : mapOrEmpty(parameterContextSpec.get("parameters")).entrySet()) {
            params.put(e.getKey(), String.valueOf(e.getValue()));
        }
        String deploymentName = requestedName;
        Map<String, Object> existing = resolver.findUniqueParameterContext(deploymentName, nifi);
        int suffix = 2;
        while (existing != null && !resolver.isParameterContextCompatible(existing, params)) {
            deploymentName = requestedName + " (" + suffix++ + ")";
            existing = resolver.findUniqueParameterContext(deploymentName, nifi);
        }
        FlowDeploymentMetricsRegistry.ComponentAction componentAction =
                FlowDeploymentMetricsRegistry.ComponentAction.CREATED;
        try {
            final String contextId;
            if (existing != null) {
                contextId = requireEntityId(existing, "parameter context");
                componentAction = FlowDeploymentMetricsRegistry.ComponentAction.REUSED;
            } else {
                final Map<String, Object> res = nifi.createParameterContext(
                        deploymentName,
                        params,
                        String.valueOf(parameterContextSpec.getOrDefault("description", "")));
                contextId = requireEntityId(res, "created parameter context");
            }
            ledger.registerParameterContext(contextId, existing == null, processGroupId, previousBindingId);
            nifi.bindParameterContextToProcessGroup(processGroupId, contextId);
            metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PARAMETER_CONTEXT,
                    componentAction, FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
        } catch (RuntimeException e) {
            metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PARAMETER_CONTEXT,
                    componentAction, FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
            throw e;
        }
    }
}
