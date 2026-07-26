package org.apache.nifi.copilot.capability;

import java.time.Instant;
import java.util.Map;

public record CapabilitySnapshot(
        Map<String, ProcessorCapability> processors,
        Map<String, ControllerServiceCapability> controllerServices,
        Instant discoveredAt) {
    public CapabilitySnapshot {
        processors = Map.copyOf(processors);
        controllerServices = Map.copyOf(controllerServices);
        if (discoveredAt == null) {
            throw new IllegalArgumentException("Discovery timestamp is required");
        }
    }
}
