package org.apache.nifi.copilot.builder;

import java.util.List;
import java.util.Map;

/**
 * Per-request mutable coordinator state accumulating typed results as stages execute.
 * Owned exclusively by {@link FlowDeploymentCoordinator}; specific fields are passed
 * into resource deployers rather than this object as a whole.
 */
final class DeploymentState {

    private final DeploymentContext context;
    private final String prePgId;
    private final Map<String, Object> parentSnapshot;
    private final OwnershipLedger ledger;
    private final ControllerServiceDeployer csDeployer;
    private final FlowDeploymentMetricsRegistry metrics;

    // Set by DeploymentPreparationStage.prepareTarget
    private DeploymentTarget target;

    // Set by DependencyPreflightStage
    private ControllerServiceDeployer.DeploymentPlan controllerServicePlan;

    // Set by ComponentDeploymentStage
    private ComponentRegistry components;
    private List<Map<String, Object>> createdProcessors;
    private List<Map<String, Object>> managedProcessors;

    // Set by ConnectionConfigurationStage
    private int connectionsCreated;

    DeploymentState(
            final DeploymentContext context,
            final String prePgId,
            final Map<String, Object> parentSnapshot,
            final OwnershipLedger ledger,
            final ControllerServiceDeployer csDeployer,
            final FlowDeploymentMetricsRegistry metrics) {
        this.context = context;
        this.prePgId = prePgId;
        this.parentSnapshot = parentSnapshot;
        this.ledger = ledger;
        this.csDeployer = csDeployer;
        this.metrics = metrics;
    }

    DeploymentContext context() {
        return context;
    }

    String prePgId() {
        return prePgId;
    }

    Map<String, Object> parentSnapshot() {
        return parentSnapshot;
    }

    OwnershipLedger ledger() {
        return ledger;
    }

    ControllerServiceDeployer csDeployer() {
        return csDeployer;
    }

    FlowDeploymentMetricsRegistry metrics() {
        return metrics;
    }

    DeploymentTarget target() {
        return target;
    }

    void setTarget(final DeploymentTarget target) {
        this.target = target;
    }

    ControllerServiceDeployer.DeploymentPlan controllerServicePlan() {
        return controllerServicePlan;
    }

    void setControllerServicePlan(final ControllerServiceDeployer.DeploymentPlan controllerServicePlan) {
        this.controllerServicePlan = controllerServicePlan;
    }

    ComponentRegistry components() {
        return components;
    }

    void setComponents(final ComponentRegistry components) {
        this.components = components;
    }

    List<Map<String, Object>> managedProcessors() {
        return managedProcessors;
    }

    void setProcessorResults(
            final List<Map<String, Object>> createdProcessors,
            final List<Map<String, Object>> managedProcessors) {
        this.createdProcessors = createdProcessors;
        this.managedProcessors = managedProcessors;
    }

    int connectionsCreated() {
        return connectionsCreated;
    }

    void setConnectionsCreated(final int connectionsCreated) {
        this.connectionsCreated = connectionsCreated;
    }

    DeploymentReport report() {
        return new DeploymentReport(createdProcessors, connectionsCreated);
    }
}
