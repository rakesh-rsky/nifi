package org.apache.nifi.copilot.builder;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Applies deferred port and remote-process-group runtime state requests recorded
 * in the {@link OwnershipLedger}.  Port requests are applied first in forward
 * registration order, then remote-process-group transmission requests in forward order.
 * Stateless.
 */
final class RuntimeStateApplier {

    private RuntimeStateApplier() {
    }

    static void apply(final OwnershipLedger ledger, final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Exception> failures = new ArrayList<>();
        for (var request : ledger.runtimeRequests()) {
            try {
                if (request.input()) {
                    nifi.setInputPortRunStatus(request.id(), request.state());
                } else {
                    nifi.setOutputPortRunStatus(request.id(), request.state());
                }
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PORT_RUNTIME_STATE,
                        FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PORT_RUNTIME_STATE,
                        FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                failures.add(contextFailure("Port state " + request.id() + " -> " + request.state(), e));
            }
        }
        for (var request : ledger.remoteTransmissionRequests()) {
            try {
                nifi.setRemoteProcessGroupTransmission(request.id(), request.state());
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.REMOTE_TRANSMISSION,
                        FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.REMOTE_TRANSMISSION,
                        FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                failures.add(contextFailure(
                        "Remote transmission " + request.id() + " -> " + request.state(), e));
            }
        }
        if (!failures.isEmpty()) {
            throwAggregated("Runtime state deployment", failures);
        }
    }

    private static void throwAggregated(final String stage, final List<Exception> failures) {
        final IllegalStateException aggregate = new IllegalStateException(
                stage + " failed with " + failures.size() + " error(s)");
        failures.forEach(aggregate::addSuppressed);
        throw aggregate;
    }

    private static IllegalStateException contextFailure(final String context, final Exception failure) {
        return new IllegalStateException(context + " failed: " + failure.getMessage(), failure);
    }
}
