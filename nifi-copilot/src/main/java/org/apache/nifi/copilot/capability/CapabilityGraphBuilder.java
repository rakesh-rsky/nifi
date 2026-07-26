package org.apache.nifi.copilot.capability;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.SchedulingConstraints;

public final class CapabilityGraphBuilder {
    public CapabilityGraph build(final CapabilitySnapshot snapshot) {
        Objects.requireNonNull(snapshot, "Capability snapshot is required");
        return new CapabilityGraph(
                snapshot.processors().values().stream().map(this::processorNode).toList(),
                snapshot.controllerServices().values().stream().map(this::controllerServiceNode).toList());
    }

    private ProcessorNode processorNode(final ProcessorCapability capability) {
        final Map<String, PropertyNode> properties = properties(capability.properties());
        return new ProcessorNode(
                capability.type(),
                capability.bundle(),
                properties,
                capability.supportsDynamicProperties(),
                serviceApis(properties, true),
                serviceApis(properties, false),
                capability.relationships(),
                capability.supportsDynamicRelationships(),
                new SchedulingConstraints(
                        capability.inputRequirement(),
                        capability.supportedSchedulingStrategies(),
                        capability.triggerSerially()));
    }

    private ControllerServiceNode controllerServiceNode(final ControllerServiceCapability capability) {
        final Map<String, PropertyNode> properties = properties(capability.properties());
        return new ControllerServiceNode(
                capability.type(),
                capability.bundle(),
                properties,
                capability.supportsDynamicProperties(),
                capability.serviceApis(),
                serviceApis(properties, true),
                serviceApis(properties, false));
    }

    private Map<String, PropertyNode> properties(final Map<String, PropertyCapability> capabilities) {
        final Map<String, PropertyNode> properties = new LinkedHashMap<>();
        capabilities.values().stream()
                .sorted(java.util.Comparator.comparing(PropertyCapability::name))
                .map(this::propertyNode)
                .forEach(property -> properties.put(property.name(), property));
        return properties;
    }

    private PropertyNode propertyNode(final PropertyCapability capability) {
        return new PropertyNode(
                capability.name(),
                capability.displayName(),
                capability.required(),
                capability.defaultValue(),
                capability.dynamic(),
                capability.sensitive(),
                capability.allowableValues(),
                capability.dependencies(),
                capability.requiredServiceApi());
    }

    private Set<ServiceApi> serviceApis(
            final Map<String, PropertyNode> properties,
            final boolean required) {
        final Set<ServiceApi> apis = new LinkedHashSet<>();
        properties.values().stream()
                .filter(property -> property.required() == required)
                .map(PropertyNode::requiredControllerServiceApi)
                .filter(Objects::nonNull)
                .forEach(apis::add);
        return apis;
    }
}
