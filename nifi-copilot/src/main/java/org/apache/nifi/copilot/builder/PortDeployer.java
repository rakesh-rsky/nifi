package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.componentState;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.componentValueOrDefault;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;
import static org.apache.nifi.copilot.builder.SpecificationSupport.optionalState;
import static org.apache.nifi.copilot.builder.SpecificationSupport.requireNonBlank;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.copilot.service.NiFiClientOperations;

final class PortDeployer {
    private PortDeployer() {
    }

    static void deploy(
            final List<Map<String, Object>> specs,
            final String processGroupId,
            final Map<String, Object> flow,
            final String collection,
            final String connectableType,
            final boolean input,
            final CanvasLayoutEngine canvasLayoutEngine,
            final CollisionAvoider collisionAvoider,
            final ComponentRegistry components,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Map<String, Object>> existing = listOfMap(flow.get(collection));
        final List<Exception> failures = new ArrayList<>();
        final FlowDeploymentMetricsRegistry.Resource resource =
                input ? FlowDeploymentMetricsRegistry.Resource.INPUT_PORT
                       : FlowDeploymentMetricsRegistry.Resource.OUTPUT_PORT;
        for (Map<String, Object> portSpec : specs) {
            final String specId = String.valueOf(portSpec.get("id"));
            FlowDeploymentMetricsRegistry.ComponentAction portAction = null;
            try {
                final String name = requireNonBlank(portSpec.get("name"), connectableType + " name");
                final String state = optionalState(portSpec.get("state"),
                        Set.of("RUNNING", "STOPPED", "DISABLED"), connectableType + " state");
                final Map<String, Object> match =
                        resolver.matchPort(existing, specId, name, connectableType);
                portAction = match == null
                        ? FlowDeploymentMetricsRegistry.ComponentAction.CREATED
                        : FlowDeploymentMetricsRegistry.ComponentAction.REUSED;
                final String nifiId;
                if (match == null) {
                    final double[] position =
                            canvasLayoutEngine.claimPortPosition(portSpec, collisionAvoider);
                    final Map<String, Object> result = input
                            ? nifi.createInputPort(processGroupId, name, position[0], position[1])
                            : nifi.createOutputPort(processGroupId, name, position[0], position[1]);
                    nifiId = requireEntityId(result, "created " + connectableType);
                    ledger.addCanvasAction("delete " + connectableType + " " + nifiId, () -> {
                        if (input) {
                            nifi.deleteInputPort(nifiId);
                        } else {
                            nifi.deleteOutputPort(nifiId);
                        }
                    });
                } else {
                    nifiId = requireEntityId(match, connectableType);
                    final Map<String, Object> component = mapOrEmpty(match.get("component"));
                    final String existingName = String.valueOf(
                            componentValueOrDefault(component, match, "name", ""));
                    if (!name.equals(existingName)) {
                        throw new IllegalStateException(connectableType + " " + nifiId
                                + " has incompatible name '" + existingName + "'; port updates are unsupported");
                    }
                    if (state != null) {
                        final String original = componentState(match, "STOPPED");
                        ledger.addRuntimeRestore(input, nifiId, original);
                    }
                }
                components.register(specId, nifiId, connectableType);
                if (state != null) {
                    ledger.addRuntimeRequest(input, nifiId, state);
                }
                metrics.observeComponent(resource, portAction, FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                if (portAction != null) {
                    metrics.observeComponent(resource, portAction, FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                }
                failures.add(contextFailure(connectableType + " '" + specId + "'", e));
            }
        }
        if (!failures.isEmpty()) {
            throwAggregated(connectableType + " deployment", failures);
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
