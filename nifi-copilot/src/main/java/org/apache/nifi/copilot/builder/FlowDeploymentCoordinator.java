package org.apache.nifi.copilot.builder;

/**
 * Sequences the deployment stages in the established order.
 * All resource-specific logic remains in the individual stage classes.
 * Stages are never called from one another; the coordinator is the sole sequencer.
 *
 * <p>The coordinator exposes two entry points that must be called from different
 * sides of the exception boundary in {@link FlowBuilder}:
 * <ul>
 *   <li>{@link #prepare} — outside the try/catch</li>
 *   <li>{@link #deploy} — inside the try/catch</li>
 * </ul>
 */
final class FlowDeploymentCoordinator {

    private static final ComponentResolver COMPONENT_RESOLVER = new ComponentResolver();
    private static final CanvasLayoutEngine CANVAS_LAYOUT_ENGINE = new CanvasLayoutEngine();
    private static final LivePreflightValidator LIVE_PREFLIGHT_VALIDATOR = new LivePreflightValidator();

    /**
     * Resolves the parent process group, optionally captures the parent snapshot,
     * and creates per-build helpers. Must be called outside the deployment try/catch.
     */
    DeploymentState prepare(final DeploymentContext context, final FlowDeploymentMetricsRegistry metrics) {
        return DeploymentPreparationStage.prepare(context, COMPONENT_RESOLVER, metrics);
    }

    /**
     * Executes all deployment stages in the established order and returns the report.
     * Each stage is timed and observed in the metrics registry. Stages are never
     * called from one another; the coordinator is the sole sequencer.
     * Must be called inside the deployment try/catch.
     */
    DeploymentReport deploy(final DeploymentState state) {
        final FlowDeploymentMetricsRegistry metrics = state.metrics();

        long stageStart = System.nanoTime();
        try {
            DeploymentPreparationStage.prepareTarget(
                    state, COMPONENT_RESOLVER, LIVE_PREFLIGHT_VALIDATOR, CANVAS_LAYOUT_ENGINE);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.PREPARATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.PREPARATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        try {
            DependencyDeploymentStage.deploy(state, COMPONENT_RESOLVER);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.DEPENDENCY_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.DEPENDENCY_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        try {
            ComponentDeploymentStage.deploy(state, COMPONENT_RESOLVER, CANVAS_LAYOUT_ENGINE);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.COMPONENT_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.COMPONENT_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        try {
            ConnectionConfigurationStage.deploy(state, COMPONENT_RESOLVER, CANVAS_LAYOUT_ENGINE);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.CONNECTION_CONFIGURATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.CONNECTION_CONFIGURATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        try {
            RuntimeActivationStage.deploy(state);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.RUNTIME_ACTIVATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.RUNTIME_ACTIVATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        return state.report();
    }
}
