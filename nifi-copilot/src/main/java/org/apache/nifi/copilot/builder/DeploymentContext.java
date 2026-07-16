package org.apache.nifi.copilot.builder;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

final class DeploymentContext {
    private final Map<String, Object> specification;
    private final NiFiClientOperations nifi;
    private final String requestedTarget;
    private final Map<String, String> existingIds;
    private final int existingCount;
    private final boolean autoStart;
    private final boolean rollbackOnFailure;

    DeploymentContext(
            final Map<String, Object> specification,
            final NiFiClientOperations nifi,
            final String requestedTarget,
            final Map<String, String> existingIds,
            final int existingCount,
            final boolean autoStart,
            final boolean rollbackOnFailure) {
        this.specification = SpecificationSupport.copySpecification(specification);
        this.nifi = nifi;
        this.requestedTarget = requestedTarget;
        this.existingIds = existingIds == null
                ? null : Collections.unmodifiableMap(new LinkedHashMap<>(existingIds));
        this.existingCount = existingCount;
        this.autoStart = autoStart;
        this.rollbackOnFailure = rollbackOnFailure;
    }

    Map<String, Object> specification() {
        return specification;
    }

    NiFiClientOperations nifi() {
        return nifi;
    }

    String requestedTarget() {
        return requestedTarget;
    }

    Map<String, String> existingIds() {
        return existingIds;
    }

    int existingCount() {
        return existingCount;
    }

    boolean autoStart() {
        return autoStart;
    }

    boolean rollbackOnFailure() {
        return rollbackOnFailure;
    }
}
