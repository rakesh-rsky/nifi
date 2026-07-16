package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;

import java.util.Map;

/**
 * Applies deferred port and remote-process-group runtime state requests, then
 * optionally starts processors in downstream-first topological order.
 */
final class RuntimeActivationStage {

    private RuntimeActivationStage() {
    }

    static void deploy(final DeploymentState state) {
        final DeploymentContext context = state.context();
        final Map<String, Object> spec = context.specification();
        final FlowDeploymentMetricsRegistry metrics = state.metrics();

        RuntimeStateApplier.apply(state.ledger(), context.nifi(), metrics);
        ProcessorStarter.startIfRequested(
                context.autoStart(),
                listOfMap(spec.get("processors")),
                listOfMap(spec.get("connections")),
                state.components(),
                context.nifi(),
                metrics);
    }
}
