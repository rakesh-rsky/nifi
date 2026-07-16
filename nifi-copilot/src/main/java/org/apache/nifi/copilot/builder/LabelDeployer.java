package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.nullableNumber;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.restoreFields;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;
import static org.apache.nifi.copilot.builder.SpecificationSupport.scalarStringMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

final class LabelDeployer {
    private LabelDeployer() {
    }

    static void deploy(
            final List<Map<String, Object>> specs,
            final String processGroupId,
            final Map<String, Object> flow,
            final CanvasLayoutEngine canvasLayoutEngine,
            final CollisionAvoider collisionAvoider,
            final ComponentRegistry components,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Map<String, Object>> existing = listOfMap(flow.get("labels"));
        final List<Exception> failures = new ArrayList<>();
        for (Map<String, Object> labelSpec : specs) {
            final String specId = String.valueOf(labelSpec.get("id"));
            FlowDeploymentMetricsRegistry.ComponentAction labelAction = null;
            try {
                if (!labelSpec.containsKey("text") || labelSpec.get("text") == null) {
                    throw new IllegalArgumentException("Label text must not be null");
                }
                final String text = String.valueOf(labelSpec.get("text"));
                final Map<String, String> style = scalarStringMap(labelSpec.get("style"), "label style");
                final Map<String, Object> match = resolver.matchLabel(existing, specId, text);
                labelAction = match == null
                        ? FlowDeploymentMetricsRegistry.ComponentAction.CREATED
                        : FlowDeploymentMetricsRegistry.ComponentAction.UPDATED;
                final String nifiId;
                if (match == null) {
                    final double[] position =
                            canvasLayoutEngine.claimLabelPosition(labelSpec, collisionAvoider);
                    final Map<String, Object> result = nifi.createLabel(processGroupId, text,
                            position[0], position[1], style,
                            nullableNumber(labelSpec.get("width"), "label width"),
                            nullableNumber(labelSpec.get("height"), "label height"));
                    nifiId = requireEntityId(result, "created label");
                    ledger.addCanvasAction("delete label " + nifiId, () -> nifi.deleteLabel(nifiId));
                } else {
                    nifiId = requireEntityId(match, "label");
                    final Map<String, Object> component = new LinkedHashMap<>(mapOrEmpty(match.get("component")));
                    final Map<String, Object> updates =
                            canvasLayoutEngine.requestedPosition(labelSpec, component);
                    updates.put("label", text);
                    if (labelSpec.containsKey("style")) {
                        updates.put("style", style);
                    }
                    if (labelSpec.containsKey("width")) {
                        updates.put("width", nullableNumber(labelSpec.get("width"), "label width"));
                    }
                    if (labelSpec.containsKey("height")) {
                        updates.put("height", nullableNumber(labelSpec.get("height"), "label height"));
                    }
                    ledger.addCanvasAction("restore label " + nifiId,
                            () -> nifi.updateLabel(nifiId,
                                    restoreFields(component, "label", "position", "style", "width", "height")));
                    nifi.updateLabel(nifiId, updates);
                }
                components.register(specId, nifiId, "LABEL");
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.LABEL,
                        labelAction, FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                if (labelAction != null) {
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.LABEL,
                            labelAction, FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                }
                failures.add(contextFailure("LABEL '" + specId + "'", e));
            }
        }
        if (!failures.isEmpty()) {
            throwAggregated("Label deployment", failures);
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
