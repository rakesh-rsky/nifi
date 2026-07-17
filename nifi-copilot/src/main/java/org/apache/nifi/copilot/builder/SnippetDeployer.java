package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.LocalPreflightValidator.normalizedOperation;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Executes snippet operations (create, move, copy, delete) in specification order.
 * Stateless; snippet IDs are registered in the caller-supplied {@link ComponentRegistry}
 * and rollback actions are recorded in the caller-supplied {@link OwnershipLedger}.
 */
final class SnippetDeployer {

    private SnippetDeployer() {
    }

    /**
     * Iterates {@code operations} in order, executing each normalized operation.
     *
     * <p>Operation semantics:
     * <ul>
     *   <li>{@code create} — resolves selections, creates the snippet, requires an ID,
     *       and registers it in the dedicated snippet registry only.</li>
     *   <li>{@code move} — resolves snippet and destination, mutates, then registers
     *       a reverse move rollback action with exact description and timing.</li>
     *   <li>{@code copy} — resolves snippet and destination; uses layout x/y defaults;
     *       no rollback action registered under existing preflight restrictions.</li>
     *   <li>{@code delete} — resolves and deletes the snippet.</li>
     * </ul>
     */
    static void deploy(
            final List<Map<String, Object>> operations,
            final String processGroupId,
            final ComponentRegistry components,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final CanvasLayoutEngine layoutEngine,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Exception> failures = new ArrayList<>();
        for (int index = 0; index < operations.size(); index++) {
            final Map<String, Object> operation = operations.get(index);
            final String name = normalizedOperation(operation);
            try {
                if ("create".equals(name)) {
                    final String specId = String.valueOf(operation.get("id"));
                    final Map<String, Object> selections = resolver.resolveSnippetSelections(
                            mapOrEmpty(operation.get("selections")), components);
                    final String snippetId = requireEntityId(
                            nifi.createSnippet(processGroupId, selections), "created snippet");
                    components.registerSnippet(specId, snippetId);
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.SNIPPET,
                            FlowDeploymentMetricsRegistry.ComponentAction.CREATED,
                            FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
                } else if ("move".equals(name)) {
                    final String snippetId = resolver.resolveSnippetId(operation.get("snippet_id"), components);
                    final String destination = resolver.resolveProcessGroupReference(
                            operation.get("destination_process_group_id"), components);
                    nifi.moveSnippet(snippetId, destination);
                    ledger.addSnippetAction("move snippet " + snippetId + " back to " + processGroupId,
                            () -> nifi.moveSnippet(snippetId, processGroupId));
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.SNIPPET,
                            FlowDeploymentMetricsRegistry.ComponentAction.UPDATED,
                            FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
                } else if ("copy".equals(name)) {
                    final String snippetId = resolver.resolveSnippetId(operation.get("snippet_id"), components);
                    final String destination = resolver.resolveProcessGroupReference(
                            operation.get("destination_process_group_id"), components);
                    nifi.copySnippet(snippetId, destination,
                            layoutEngine.xOrDefault(operation.get("x")),
                            layoutEngine.yOrDefault(operation.get("y")));
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.SNIPPET,
                            FlowDeploymentMetricsRegistry.ComponentAction.CREATED,
                            FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
                } else {
                    final String snippetId = resolver.resolveSnippetId(operation.get("snippet_id"), components);
                    nifi.deleteSnippet(snippetId);
                    metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.SNIPPET,
                            FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED,
                            FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
                }
            } catch (Exception e) {
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.SNIPPET,
                        snippetAction(name), FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                failures.add(contextFailure("Snippet operation " + index + " (" + name + ")", e));
            }
        }
        if (!failures.isEmpty()) {
            throwAggregated("Snippet deployment", failures);
        }
    }

    private static FlowDeploymentMetricsRegistry.ComponentAction snippetAction(final String operationName) {
        return switch (operationName) {
            case "create", "copy" -> FlowDeploymentMetricsRegistry.ComponentAction.CREATED;
            case "move" -> FlowDeploymentMetricsRegistry.ComponentAction.UPDATED;
            default -> FlowDeploymentMetricsRegistry.ComponentAction.CONFIGURED;
        };
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
