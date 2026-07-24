package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;

import java.util.List;
import java.util.Map;

/**
 * Validates reusable controller services before any deployment mutation.
 */
final class DependencyPreflightStage {

    private DependencyPreflightStage() {
    }

    static void validate(final DeploymentState state, final ComponentResolver resolver) {
        final List<Map<String, Object>> controllerServices =
                listOfMap(state.context().specification().get("controller_services"));
        if (controllerServices.isEmpty()) {
            state.setControllerServicePlan(ControllerServiceDeployer.emptyPlan());
            return;
        }

        final String existingTargetId =
                DeploymentPreparationStage.findExistingEffectiveProcessGroupId(state, resolver);
        final List<Map<String, Object>> existing = existingTargetId == null
                ? List.of()
                : state.context().nifi().listControllerServices(existingTargetId);
        state.setControllerServicePlan(
                state.csDeployer().preflightAll(controllerServices, existing, resolver));
    }
}
