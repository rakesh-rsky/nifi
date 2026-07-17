package org.apache.nifi.copilot.builder;

import java.util.Map;

final class DeploymentTarget {
    private final String parentProcessGroupId;
    private final String effectiveProcessGroupId;
    private final String previousParameterContextBindingId;
    private final Map<String, Object> rollbackSnapshot;
    private final Map<String, Object> inventoryResponse;
    private final Map<String, Object> effectiveFlow;

    DeploymentTarget(
            final String parentProcessGroupId,
            final String effectiveProcessGroupId,
            final String previousParameterContextBindingId,
            final Map<String, Object> rollbackSnapshot,
            final Map<String, Object> inventoryResponse,
            final Map<String, Object> effectiveFlow) {
        this.parentProcessGroupId = parentProcessGroupId;
        this.effectiveProcessGroupId = effectiveProcessGroupId;
        this.previousParameterContextBindingId = previousParameterContextBindingId;
        this.rollbackSnapshot = rollbackSnapshot;
        this.inventoryResponse = inventoryResponse;
        this.effectiveFlow = effectiveFlow;
    }

    String parentProcessGroupId() {
        return parentProcessGroupId;
    }

    String effectiveProcessGroupId() {
        return effectiveProcessGroupId;
    }

    String previousParameterContextBindingId() {
        return previousParameterContextBindingId;
    }

    Map<String, Object> rollbackSnapshot() {
        return rollbackSnapshot;
    }

    Map<String, Object> inventoryResponse() {
        return inventoryResponse;
    }

    Map<String, Object> effectiveFlow() {
        return effectiveFlow;
    }
}
