package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;

import java.util.List;
import java.util.Map;

/**
 * Deploys connections, configures auto-terminated relationships, and executes
 * snippet operations in the established order.
 */
final class ConnectionConfigurationStage {

    private ConnectionConfigurationStage() {
    }

    static void deploy(
            final DeploymentState state,
            final ComponentResolver resolver,
            final CanvasPositionProvider positionProvider) {
        final DeploymentContext context = state.context();
        final Map<String, Object> spec = context.specification();
        final String pgId = state.target().effectiveProcessGroupId();
        final Map<String, Object> flow = state.target().effectiveFlow();
        final ComponentRegistry components = state.components();
        final FlowDeploymentMetricsRegistry metrics = state.metrics();

        final List<Map<String, Object>> connections = listOfMap(spec.get("connections"));
        final int connectionsCreated = ConnectionDeployer.deploy(
                connections, pgId, components, state.ledger(),
                resolver, context.nifi(), listOfMap(flow.get("connections")), metrics);

        RelationshipConfigurer.configure(
                state.managedProcessors(), connections, components, resolver, context.nifi(), metrics);

        SnippetDeployer.deploy(listOfMap(spec.get("snippets")), pgId,
                components, state.ledger(), resolver, positionProvider, context.nifi(), metrics);

        state.setConnectionsCreated(connectionsCreated);
    }
}
