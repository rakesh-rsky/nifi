package org.apache.nifi.copilot.builder;

import java.util.List;
import java.util.Map;

/**
 * Internal record of deployment outcomes used by {@link FlowDeploymentCoordinator}
 * to produce the public {@link FlowBuilder.BuildResult}.
 * Separate from {@code BuildResult} to keep internal state out of the public API.
 */
final class DeploymentReport {

    private final List<Map<String, Object>> createdProcessors;
    private final int connectionsCreated;

    DeploymentReport(
            final List<Map<String, Object>> createdProcessors,
            final int connectionsCreated) {
        this.createdProcessors = createdProcessors;
        this.connectionsCreated = connectionsCreated;
    }

    List<Map<String, Object>> createdProcessors() {
        return createdProcessors;
    }

    int connectionsCreated() {
        return connectionsCreated;
    }
}
