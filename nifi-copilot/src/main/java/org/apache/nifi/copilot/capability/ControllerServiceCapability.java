package org.apache.nifi.copilot.capability;

import java.util.Map;
import java.util.Set;

public record ControllerServiceCapability(
        String type,
        BundleCoordinate bundle,
        Map<String, PropertyCapability> properties,
        boolean supportsDynamicProperties,
        Set<ServiceApi> serviceApis) {
    public ControllerServiceCapability {
        if (type == null || type.isBlank() || bundle == null) {
            throw new IllegalArgumentException("Controller service type and bundle are required");
        }
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        serviceApis = serviceApis == null ? Set.of() : Set.copyOf(serviceApis);
    }
}
