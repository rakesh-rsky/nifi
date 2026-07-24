package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.originalUpdatedFields;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.restoreFields;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.transmissionState;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;
import static org.apache.nifi.copilot.builder.SpecificationSupport.optionalMapField;
import static org.apache.nifi.copilot.builder.SpecificationSupport.optionalState;
import static org.apache.nifi.copilot.builder.SpecificationSupport.requireNonBlank;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.copilot.service.NiFiClientOperations;

final class RemoteProcessGroupDeployer {
    private RemoteProcessGroupDeployer() {
    }

    static void deploy(
            final List<Map<String, Object>> specs,
            final String processGroupId,
            final Map<String, Object> flow,
            final CanvasPositionProvider positionProvider,
            final ComponentRegistry components,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Map<String, Object>> existing = listOfMap(flow.get("remoteProcessGroups"));
        final List<Exception> failures = new ArrayList<>();
        for (Map<String, Object> rpgSpec : specs) {
            final String specId = String.valueOf(rpgSpec.get("id"));
            FlowDeploymentMetricsRegistry.ComponentAction rpgAction = null;
            try {
                final String targetUri = requireNonBlank(rpgSpec.get("target_uri"), "remote target_uri");
                final String state = optionalState(rpgSpec.get("transmission_state"),
                        Set.of("TRANSMITTING", "STOPPED"), "remote transmission_state");
                final Map<String, Object> match =
                        resolver.matchRemoteProcessGroup(existing, specId, targetUri);
                rpgAction = match == null
                        ? FlowDeploymentMetricsRegistry.ComponentAction.CREATED
                        : FlowDeploymentMetricsRegistry.ComponentAction.UPDATED;
                final String nifiId;
                if (match == null) {
                    final double[] position =
                            positionProvider.provisionalPosition(rpgSpec);
                    final Map<String, Object> result = nifi.createRemoteProcessGroup(processGroupId, targetUri,
                            position[0], position[1], optionalMapField(rpgSpec, "config"));
                    nifiId = requireEntityId(result, "created remote process group");
                    ledger.addChangedCanvasId(nifiId);
                    ledger.addCanvasDeletionAction("delete remote process group " + nifiId,
                            () -> nifi.deleteRemoteProcessGroup(nifiId));
                    if (state != null) {
                        ledger.addRemoteTransmissionRequest(nifiId, state);
                    }
                } else {
                    nifiId = requireEntityId(match, "remote process group");
                    final Map<String, Object> component = new LinkedHashMap<>(mapOrEmpty(match.get("component")));
                    final String originalState = transmissionState(match);
                    final Map<String, Object> updates = new LinkedHashMap<>(optionalMapField(rpgSpec, "config"));
                    updates.put("targetUri", targetUri);
                    updates.putAll(positionProvider.requestedPosition(rpgSpec, component));
                    final Map<String, Object> originalUpdates = originalUpdatedFields(component, updates);
                    ledger.addRemoteTransmissionRestore(nifiId, originalState);
                    if ("TRANSMITTING".equals(originalState)) {
                        nifi.setRemoteProcessGroupTransmission(nifiId, "STOPPED");
                    }
                    ledger.addCanvasAction("restore remote process group " + nifiId,
                            () -> nifi.updateRemoteProcessGroup(nifiId, originalUpdates));
                    nifi.updateRemoteProcessGroup(nifiId, updates);
                    ledger.addChangedCanvasId(nifiId);
                    ledger.addRemoteTransmissionRequest(nifiId, state == null ? originalState : state);
                }
                components.register(specId, nifiId, "REMOTE_PROCESS_GROUP");
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.REMOTE_PROCESS_GROUP,
                        rpgAction, FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                if (rpgAction != null) {
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.REMOTE_PROCESS_GROUP,
                            rpgAction, FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                }
                failures.add(contextFailure("REMOTE_PROCESS_GROUP '" + specId + "'", e));
            }
        }
        if (!failures.isEmpty()) {
            throwAggregated("Remote process group deployment", failures);
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
