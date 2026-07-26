package org.apache.nifi.copilot.capability;

import java.util.Map;
import java.util.Set;

public record ProcessorCapability(
        String type,
        BundleCoordinate bundle,
        Map<String, PropertyCapability> properties,
        boolean supportsDynamicProperties,
        Set<String> relationships,
        boolean supportsDynamicRelationships,
        String inputRequirement,
        Set<String> supportedSchedulingStrategies,
        boolean triggerSerially) {
    public ProcessorCapability {
        if (type == null || type.isBlank() || bundle == null) {
            throw new IllegalArgumentException("Processor type and bundle are required");
        }
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        relationships = relationships == null ? Set.of() : Set.copyOf(relationships);
        supportedSchedulingStrategies = supportedSchedulingStrategies == null
                ? Set.of() : Set.copyOf(supportedSchedulingStrategies);
    }
}
