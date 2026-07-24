package org.apache.nifi.copilot.builder;

/**
 * Sequences the deployment stages in the established order.
 * All resource-specific logic remains in the individual stage classes.
 * Stages are never called from one another; the coordinator is the sole sequencer.
 *
 * <p>The coordinator is instance-configured: the {@link LayoutMode} and helper objects
 * are supplied at construction time and shared across calls.
 *
 * <p>The coordinator exposes two entry points that must be called from different
 * sides of the exception boundary in {@link FlowBuilder}:
 * <ul>
 *   <li>{@link #prepare} — outside the try/catch</li>
 *   <li>{@link #deploy} — inside the try/catch</li>
 * </ul>
 */
final class FlowDeploymentCoordinator {

    private final ComponentResolver componentResolver;
    private final CanvasPositionProvider canvasPositionProvider;
    private final LivePreflightValidator livePreflightValidator;
    private final LayoutMode layoutMode;
    private final LayoutDeploymentStage.LayoutExecutor layoutExecutor;

    FlowDeploymentCoordinator(final LayoutMode layoutMode) {
        this(layoutMode, new ComponentResolver(), new CanvasPositionProvider(), new LivePreflightValidator(), null);
    }

    /** Package-private constructor for targeted testing with injected helpers. */
    FlowDeploymentCoordinator(
            final LayoutMode layoutMode,
            final ComponentResolver componentResolver,
            final CanvasPositionProvider canvasPositionProvider,
            final LivePreflightValidator livePreflightValidator) {
        this(layoutMode, componentResolver, canvasPositionProvider, livePreflightValidator, null);
    }

    /** Package-private constructor for testing with injected layout executor. */
    FlowDeploymentCoordinator(
            final LayoutMode layoutMode,
            final ComponentResolver componentResolver,
            final CanvasPositionProvider canvasPositionProvider,
            final LivePreflightValidator livePreflightValidator,
            final LayoutDeploymentStage.LayoutExecutor layoutExecutor) {
        this.layoutMode = layoutMode;
        this.componentResolver = componentResolver;
        this.canvasPositionProvider = canvasPositionProvider;
        this.livePreflightValidator = livePreflightValidator;
        this.layoutExecutor = layoutExecutor;
    }

    /**
     * Resolves the parent process group, optionally captures the parent snapshot,
     * and creates per-build helpers. Must be called outside the deployment try/catch.
     */
    DeploymentState prepare(final DeploymentContext context, final FlowDeploymentMetricsRegistry metrics) {
        return DeploymentPreparationStage.prepare(context, componentResolver, metrics);
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
            DependencyPreflightStage.validate(state, componentResolver);
            DeploymentPreparationStage.prepareTarget(
                    state, componentResolver, livePreflightValidator);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.PREPARATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.PREPARATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        try {
            DependencyDeploymentStage.deploy(state, componentResolver);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.DEPENDENCY_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.DEPENDENCY_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        try {
            ComponentDeploymentStage.deploy(state, componentResolver, canvasPositionProvider);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.COMPONENT_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.COMPONENT_DEPLOYMENT,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        try {
            ConnectionConfigurationStage.deploy(state, componentResolver, canvasPositionProvider);
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.CONNECTION_CONFIGURATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS, stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.CONNECTION_CONFIGURATION,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            throw e;
        }

        stageStart = System.nanoTime();
        LayoutDeploymentStage.LayoutObservation layoutObs = null;
        try {
            layoutObs = layoutExecutor != null
                    ? LayoutDeploymentStage.deploy(state, layoutMode, layoutExecutor)
                    : LayoutDeploymentStage.deploy(state, layoutMode);
            metrics.observeStage(
                    FlowDeploymentMetricsRegistry.Stage.LAYOUT,
                    layoutObs.skipped()
                            ? FlowDeploymentMetricsRegistry.StageOutcome.SKIPPED
                            : FlowDeploymentMetricsRegistry.StageOutcome.SUCCESS,
                    stageStart);
        } catch (RuntimeException e) {
            metrics.observeStage(FlowDeploymentMetricsRegistry.Stage.LAYOUT,
                    FlowDeploymentMetricsRegistry.StageOutcome.FAILURE, stageStart);
            metrics.observeLayout(layoutMode.name(),
                    FlowDeploymentMetricsRegistry.LayoutOutcome.FAILURE, stageStart, 0, 0);
            throw e;
        }
        metrics.observeLayout(
                layoutMode.name(),
                layoutObs.skipped()
                        ? FlowDeploymentMetricsRegistry.LayoutOutcome.SKIPPED
                        : FlowDeploymentMetricsRegistry.LayoutOutcome.SUCCESS,
                stageStart,
                layoutObs.movedCount(),
                layoutObs.routedCount());

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
