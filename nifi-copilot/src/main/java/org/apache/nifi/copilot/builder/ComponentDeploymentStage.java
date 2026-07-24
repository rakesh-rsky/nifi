package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;

import java.util.Map;

/**
 * Ensures the NiFi type cache, registers live and caller-supplied component identity,
 * deploys processors, then deploys the five canvas resource types in the established
 * order: input ports, output ports, funnels, labels, remote process groups.
 */
final class ComponentDeploymentStage {

    private ComponentDeploymentStage() {
    }

    static void deploy(
            final DeploymentState state,
            final ComponentResolver resolver,
            final CanvasPositionProvider positionProvider) {
        final DeploymentContext context = state.context();
        final Map<String, Object> spec = context.specification();
        final String pgId = state.target().effectiveProcessGroupId();
        final Map<String, Object> flow = state.target().effectiveFlow();
        final FlowDeploymentMetricsRegistry metrics = state.metrics();

        context.nifi().ensureTypeCache();

        final ComponentRegistry components = new ComponentRegistry();
        resolver.registerInventory(flow, components);
        resolver.registerExistingProcessors(
                listOfMap(spec.get("processors")), context.existingIds(), components);

        final ProcessorDeployer.Result processorResult = ProcessorDeployer.deploy(
                listOfMap(spec.get("processors")), pgId, state.csDeployer(), positionProvider,
                components, state.ledger(), resolver, context.nifi(), metrics);
        resolver.registerProcessorTypes(listOfMap(spec.get("processors")), components);

        PortDeployer.deploy(listOfMap(spec.get("input_ports")), pgId, flow,
                "inputPorts", "INPUT_PORT", true,
                positionProvider, components, state.ledger(), resolver, context.nifi(), metrics);
        PortDeployer.deploy(listOfMap(spec.get("output_ports")), pgId, flow,
                "outputPorts", "OUTPUT_PORT", false,
                positionProvider, components, state.ledger(), resolver, context.nifi(), metrics);
        FunnelDeployer.deploy(listOfMap(spec.get("funnels")), pgId, flow,
                positionProvider, components, state.ledger(), resolver, context.nifi(), metrics);
        LabelDeployer.deploy(listOfMap(spec.get("labels")), pgId, flow,
                positionProvider, components, state.ledger(), resolver, context.nifi(), metrics);
        RemoteProcessGroupDeployer.deploy(listOfMap(spec.get("remote_process_groups")), pgId, flow,
                positionProvider, components, state.ledger(), resolver, context.nifi(), metrics);

        state.setComponents(components);
        state.setProcessorResults(processorResult.created(), processorResult.managed());
    }
}
