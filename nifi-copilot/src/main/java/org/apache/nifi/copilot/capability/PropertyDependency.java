package org.apache.nifi.copilot.capability;

import java.util.Set;

public record PropertyDependency(
        String propertyName,
        String propertyDisplayName,
        Set<String> dependentValues) {
    public PropertyDependency {
        if (propertyName == null || propertyName.isBlank()) {
            throw new IllegalArgumentException("Dependency property name is required");
        }
        propertyDisplayName = propertyDisplayName == null || propertyDisplayName.isBlank()
                ? propertyName : propertyDisplayName;
        dependentValues = dependentValues == null ? Set.of() : Set.copyOf(dependentValues);
    }
}
