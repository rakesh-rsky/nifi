package org.apache.nifi.copilot.capability;

import java.util.List;

public record PropertyCapability(
        String name,
        String displayName,
        boolean required,
        String defaultValue,
        boolean dynamic,
        boolean sensitive,
        List<AllowableValue> allowableValues,
        List<PropertyDependency> dependencies,
        ServiceApi requiredServiceApi) {
    public PropertyCapability {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Property internal name is required");
        }
        displayName = displayName == null || displayName.isBlank() ? name : displayName;
        allowableValues = allowableValues == null ? List.of() : List.copyOf(allowableValues);
        dependencies = dependencies == null ? List.of() : List.copyOf(dependencies);
    }
}
