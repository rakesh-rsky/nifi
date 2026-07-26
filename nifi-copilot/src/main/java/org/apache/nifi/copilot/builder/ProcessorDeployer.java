package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.numericValue;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Creates or updates processors and returns the deployment result.
 * Stateless; all ownership state is recorded in the caller-supplied {@link OwnershipLedger}.
 */
final class ProcessorDeployer {

    private ProcessorDeployer() {
    }

    /** Typed result of a processor deployment pass. */
    record Result(List<Map<String, Object>> created, List<Map<String, Object>> managed) {
    }

    /**
     * Iterates {@code processorSpecs}, creating new processors or updating existing ones.
     * Controller-service property references are resolved via {@code csDeployer}.
     * Default coordinates are obtained from {@code positionProvider} for processors without
     * an existing NiFi ID.
     *
     * <p>Ownership registration timing:
     * <ul>
     *   <li>Restore entry added before {@code updateProcessor} call for existing processors.</li>
     *   <li>Created-ID entry added after ID extraction for new processors.</li>
     * </ul>
     */
    static Result deploy(
            final List<Map<String, Object>> processorSpecs,
            final String processGroupId,
            final ControllerServiceDeployer csDeployer,
            final CanvasPositionProvider positionProvider,
            final ComponentRegistry components,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Map<String, Object>> createdProcessors = new ArrayList<>();
        final List<Map<String, Object>> managedProcessors = new ArrayList<>();
        for (Map<String, Object> processorSpec : processorSpecs) {
            final String specId = String.valueOf(processorSpec.get("id"));
            final String existingId = components.id(specId);
            try {
                final String processorType = resolver.resolveProcessorType(processorSpec);
                final String processorName = resolver.resolveProcessorName(processorSpec, processorType);
                final Map<String, Object> configuration = csDeployer.resolveCsReferences(
                        mapOrEmpty(processorSpec.get("config")));
                final Map<String, Object> scheduling = mapOrEmpty(processorSpec.get("scheduling"));
                final Double x = processorSpec.containsKey("x")
                        ? Double.valueOf(NiFiClientOperations.doubleValue(processorSpec.get("x")))
                        : existingId == null ? Double.valueOf(positionProvider.defaultX()) : null;
                final Double y = processorSpec.containsKey("y")
                        ? Double.valueOf(NiFiClientOperations.doubleValue(processorSpec.get("y")))
                        : existingId == null ? Double.valueOf(positionProvider.defaultY()) : null;
                if (existingId != null && !existingId.isBlank()) {
                    updateExistingProcessor(
                            existingId, processorName, x, y, configuration, scheduling, ledger, nifi);
                    final Map<String, Object> managedEntry = new LinkedHashMap<>();
                    managedEntry.put("spec_id", specId);
                    managedEntry.put("id", existingId);
                    managedEntry.put("name", processorName);
                    managedEntry.put("type", processorType);
                    managedProcessors.add(managedEntry);
                    components.recordProcessorId(specId, existingId);
                    ledger.addChangedCanvasId(existingId);
                } else {
                    final Map<String, Object> result = nifi.createProcessor(
                            processGroupId,
                            processorType,
                            processorName,
                            x,
                            y,
                            configuration.isEmpty() ? null : configuration);
                    final Map<String, Object> createdEntry = new LinkedHashMap<>();
                    createdEntry.put("spec_id", specId);
                    createdEntry.putAll(result);
                    final String processorId = requireEntityId(result, "created processor");
                    createdEntry.put("id", processorId);
                    createdProcessors.add(createdEntry);
                    managedProcessors.add(createdEntry);
                    components.recordProcessorId(specId, processorId);
                    ledger.addChangedCanvasId(processorId);
                    ledger.addCreatedProcessorId(processorId);
                    if (!scheduling.isEmpty()) {
                        nifi.updateProcessor(processorId, Map.of("config", scheduling));
                    }
                }
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PROCESSOR,
                        (existingId != null && !existingId.isBlank())
                                ? FlowDeploymentMetricsRegistry.ComponentAction.UPDATED
                                : FlowDeploymentMetricsRegistry.ComponentAction.CREATED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
            } catch (Exception e) {
                metrics.observeComponent(FlowDeploymentMetricsRegistry.Resource.PROCESSOR,
                        (existingId != null && !existingId.isBlank())
                                ? FlowDeploymentMetricsRegistry.ComponentAction.UPDATED
                                : FlowDeploymentMetricsRegistry.ComponentAction.CREATED,
                        FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
                throw new IllegalStateException(
                        "Processor '" + specId + "' deployment failed: " + e.getMessage(), e);
            }
        }
        return new Result(createdProcessors, managedProcessors);
    }

    private static void updateExistingProcessor(
            final String processorId,
            final String name,
            final Double x,
            final Double y,
            final Map<String, Object> configuration,
            final Map<String, Object> scheduling,
            final OwnershipLedger ledger,
            final NiFiClientOperations nifi) {
        final Map<String, Object> entity = nifi.getProcessor(processorId);
        final Map<String, Object> component = mapOrEmpty(entity.get("component"));
        final Map<String, Object> original = new LinkedHashMap<>();
        if (component.get("name") != null) {
            original.put("name", component.get("name"));
        }
        if (component.get("position") != null) {
            original.put("position", component.get("position"));
        }
        if (component.get("config") != null) {
            original.put("config", component.get("config"));
        }
        if (!original.isEmpty()) {
            ledger.addProcessorRestore(processorId, original);
        }
        final Map<String, Object> updates = new LinkedHashMap<>();
        updates.put("name", name);
        if (x != null || y != null) {
            final Map<String, Object> currentPosition = mapOrEmpty(component.get("position"));
            updates.put("position", Map.of(
                    "x", x == null ? numericValue(currentPosition.get("x"), 0) : x,
                    "y", y == null ? numericValue(currentPosition.get("y"), 0) : y));
        }
        if (!configuration.isEmpty() || !scheduling.isEmpty()) {
            final Map<String, Object> configUpdates = new LinkedHashMap<>(scheduling);
            if (!configuration.isEmpty()) {
                configUpdates.put("properties", configuration);
            }
            updates.put("config", configUpdates);
        }
        nifi.updateProcessor(processorId, updates);
    }
}
