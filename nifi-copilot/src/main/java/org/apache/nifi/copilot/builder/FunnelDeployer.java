package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.restoreFields;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

final class FunnelDeployer {
    private FunnelDeployer() {
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
        final List<Map<String, Object>> existing = listOfMap(flow.get("funnels"));
        final List<Exception> failures = new ArrayList<>();
        for (Map<String, Object> funnelSpec : specs) {
            final String specId = String.valueOf(funnelSpec.get("id"));
            FlowDeploymentMetricsRegistry.ComponentAction funnelAction = null;
            try {
                final Map<String, Object> match = resolver.matchFunnel(existing, specId);
                funnelAction = match == null
                        ? FlowDeploymentMetricsRegistry.ComponentAction.CREATED
                        : FlowDeploymentMetricsRegistry.ComponentAction.REUSED;
                final String nifiId;
                if (match == null) {
                    final double[] position = positionProvider.provisionalPosition(funnelSpec);
                    final Map<String, Object> result = nifi.createFunnel(processGroupId, position[0], position[1]);
                    nifiId = requireEntityId(result, "created funnel");
                    ledger.addChangedCanvasId(nifiId);
                    ledger.addCanvasDeletionAction("delete funnel " + nifiId, () -> nifi.deleteFunnel(nifiId));
                } else {
                    nifiId = requireEntityId(match, "funnel");
                    final Map<String, Object> component = new LinkedHashMap<>(mapOrEmpty(match.get("component")));
                    final Map<String, Object> updates =
                            positionProvider.requestedPosition(funnelSpec, component);
                    if (!updates.isEmpty()) {
                        funnelAction = FlowDeploymentMetricsRegistry.ComponentAction.UPDATED;
                        ledger.addCanvasAction("restore funnel " + nifiId,
                                () -> nifi.updateFunnel(nifiId, restoreFields(component, "position")));
                        nifi.updateFunnel(nifiId, updates);
                        ledger.addChangedCanvasId(nifiId);
                    }
                }
                components.register(specId, nifiId, "FUNNEL");
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.FUNNEL,
                        funnelAction, FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                if (funnelAction != null) {
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.FUNNEL,
                            funnelAction, FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                }
                failures.add(contextFailure("FUNNEL '" + specId + "'", e));
            }
        }
        if (!failures.isEmpty()) {
            throwAggregated("Funnel deployment", failures);
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
