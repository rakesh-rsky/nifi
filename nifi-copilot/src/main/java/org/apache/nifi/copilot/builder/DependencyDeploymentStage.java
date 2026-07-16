package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrNull;

import java.util.List;
import java.util.Map;

/**
 * Deploys the parameter context and controller services in the established dependency order.
 * Parameter context is deployed and bound first, then controller services in dependency order.
 */
final class DependencyDeploymentStage {

    private DependencyDeploymentStage() {
    }

    static void deploy(final DeploymentState state, final ComponentResolver resolver) {
        final DeploymentContext context = state.context();
        final Map<String, Object> spec = context.specification();
        final String pgId = state.target().effectiveProcessGroupId();
        final FlowDeploymentMetricsRegistry metrics = state.metrics();

        final Map<String, Object> parameterContextSpec = mapOrNull(spec.get("parameter_context"));
        ParameterContextDeployer.deploy(
                parameterContextSpec, pgId,
                state.target().previousParameterContextBindingId(),
                state.ledger(), resolver, context.nifi(), metrics);

        final List<Map<String, Object>> csSpecs = listOfMap(spec.get("controller_services"));
        if (!csSpecs.isEmpty()) {
            state.csDeployer().deployAll(csSpecs, pgId, state.ledger(), resolver, context.nifi(), metrics);
        }
    }
}
