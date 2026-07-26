package org.apache.nifi.copilot.capability;

public record ServiceApi(String type, BundleCoordinate bundle) {
    public ServiceApi {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Service API type is required");
        }
    }
}
