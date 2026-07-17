package org.apache.nifi.copilot.builder;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import org.springframework.stereotype.Component;

/**
 * Production-safe, in-memory metrics registry for {@link FlowBuilder} deployments.
 *
 * <h2>Cardinality constraints</h2>
 * <p>All dimensions are fixed enums. No component IDs, names, URIs, processor or
 * controller-service types, exception messages, or user-supplied data are ever used
 * as key material.
 *
 * <h2>Thread safety</h2>
 * <p>All accumulators are {@link LongAdder} and {@link LongAccumulator}, which are
 * safe for concurrent use without additional synchronization.
 *
 * <h2>Non-throwing guarantee</h2>
 * <p>Every observation method catches and discards all internal metric failures;
 * metric failures can never propagate into deployment code or change exceptions.
 *
 * <h2>Metrics tracked</h2>
 * <ul>
 *   <li>Deployment count, total and max duration by {@link DeploymentOutcome}</li>
 *   <li>Stage count, total and max duration by {@link Stage} and {@link StageOutcome}</li>
 *   <li>Component count by {@link Resource}, {@link ComponentAction}, and {@link ActionOutcome}</li>
 *   <li>Rollback action count by {@link RollbackActionCategory} and {@link ActionOutcome}</li>
 *   <li>Overall rollback count by {@link RollbackOutcome}</li>
 * </ul>
 */
@Component
public class FlowDeploymentMetricsRegistry {

    // =========================================================================
    // Fixed dimension enums
    // =========================================================================

    public enum DeploymentOutcome { SUCCESS, FAILURE }

    public enum StageOutcome { SUCCESS, FAILURE, SKIPPED }

    public enum Stage {
        PREPARATION,
        DEPENDENCY_DEPLOYMENT,
        COMPONENT_DEPLOYMENT,
        CONNECTION_CONFIGURATION,
        RUNTIME_ACTIVATION
    }

    public enum Resource {
        PARAMETER_CONTEXT,
        CONTROLLER_SERVICE,
        PROCESSOR,
        INPUT_PORT,
        OUTPUT_PORT,
        FUNNEL,
        LABEL,
        REMOTE_PROCESS_GROUP,
        CONNECTION,
        SNIPPET,
        RELATIONSHIP,
        PORT_RUNTIME_STATE,
        REMOTE_TRANSMISSION
    }

    public enum ComponentAction { CREATED, REUSED, UPDATED, CONFIGURED, STARTED, SKIPPED }

    public enum ActionOutcome { SUCCESS, FAILURE }

    public enum RollbackActionCategory {
        STOP_PORT,
        STOP_REMOTE_TRANSMISSION,
        DELETE_CONNECTION,
        REVERSE_SNIPPET,
        REVERSE_CANVAS,
        RESTORE_PORT_STATE,
        RESTORE_REMOTE_TRANSMISSION,
        DELETE_PROCESSOR,
        RESTORE_PROCESSOR,
        RESTORE_CONNECTION,
        DELETE_CONTROLLER_SERVICE,
        RESTORE_PARAMETER_CONTEXT,
        DELETE_CHILD_PROCESS_GROUP
    }

    public enum RollbackOutcome { SUCCESS, PARTIAL_FAILURE }

    // =========================================================================
    // Snapshot types (immutable point-in-time records)
    // =========================================================================

    /**
     * Immutable count and duration snapshot for a single deployment outcome.
     */
    public record DeploymentMetrics(long count, long totalDurationMillis, long maxDurationMillis) {}

    /**
     * Immutable count and duration snapshot for a single stage and outcome combination.
     */
    public record StageMetrics(long count, long totalDurationMillis, long maxDurationMillis) {}

    /**
     * Comprehensive immutable point-in-time snapshot of all deployment metrics.
     * Map keys are enum names joined with {@code "|"}, e.g. {@code "PREPARATION|SUCCESS"}.
     * Only combinations that have been observed appear in the maps.
     */
    public record Snapshot(
            Map<String, DeploymentMetrics> deployments,
            Map<String, StageMetrics> stages,
            Map<String, Long> components,
            Map<String, Long> rollbackActions,
            Map<String, Long> rollbacks) {}

    // =========================================================================
    // Mutable accumulators
    // =========================================================================

    // key: DeploymentOutcome.name()
    private final ConcurrentHashMap<String, DurationStats> deploymentStats = new ConcurrentHashMap<>();
    // key: Stage.name() + "|" + StageOutcome.name()
    private final ConcurrentHashMap<String, DurationStats> stageStats = new ConcurrentHashMap<>();
    // key: Resource.name() + "|" + ComponentAction.name() + "|" + ActionOutcome.name()
    private final ConcurrentHashMap<String, LongAdder> componentCounts = new ConcurrentHashMap<>();
    // key: RollbackActionCategory.name() + "|" + ActionOutcome.name()
    private final ConcurrentHashMap<String, LongAdder> rollbackActionCounts = new ConcurrentHashMap<>();
    // key: RollbackOutcome.name()
    private final ConcurrentHashMap<String, LongAdder> rollbackCounts = new ConcurrentHashMap<>();

    // =========================================================================
    // Observation methods (all non-throwing)
    // =========================================================================

    /**
     * Records a completed deployment with the given outcome.
     *
     * @param outcome   SUCCESS or FAILURE
     * @param startNanos {@link System#nanoTime()} captured at the start of the deployment
     */
    public void observeDeployment(final DeploymentOutcome outcome, final long startNanos) {
        try {
            final long durationNanos = System.nanoTime() - startNanos;
            deploymentStats.computeIfAbsent(outcome.name(), k -> new DurationStats()).record(durationNanos);
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    /**
     * Records the completion of a coarse deployment stage.
     *
     * @param stage     the stage that completed
     * @param outcome   SUCCESS, FAILURE, or SKIPPED
     * @param startNanos {@link System#nanoTime()} captured at the start of the stage
     */
    public void observeStage(final Stage stage, final StageOutcome outcome, final long startNanos) {
        try {
            final long durationNanos = System.nanoTime() - startNanos;
            final String key = stage.name() + "|" + outcome.name();
            stageStats.computeIfAbsent(key, k -> new DurationStats()).record(durationNanos);
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    /**
     * Records a single component resource action outcome.
     *
     * @param resource the fixed resource type
     * @param action   the action taken (CREATED, REUSED, UPDATED, etc.)
     * @param outcome  SUCCESS or FAILURE
     */
    public void observeComponent(final Resource resource, final ComponentAction action, final ActionOutcome outcome) {
        try {
            final String key = resource.name() + "|" + action.name() + "|" + outcome.name();
            componentCounts.computeIfAbsent(key, k -> new LongAdder()).increment();
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    /**
     * Records the outcome of a single rollback action step.
     *
     * @param action  the fixed rollback action category
     * @param outcome SUCCESS or FAILURE
     */
    public void observeRollbackAction(final RollbackActionCategory action, final ActionOutcome outcome) {
        try {
            final String key = action.name() + "|" + outcome.name();
            rollbackActionCounts.computeIfAbsent(key, k -> new LongAdder()).increment();
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    /**
     * Records the overall outcome of a compensating rollback execution.
     *
     * @param outcome SUCCESS if all steps succeeded, PARTIAL_FAILURE if any step failed
     */
    public void observeRollback(final RollbackOutcome outcome) {
        try {
            rollbackCounts.computeIfAbsent(outcome.name(), k -> new LongAdder()).increment();
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    private static void ignoreMetricFailure(final Throwable ignored) {
        // Observation must never alter deployment or rollback behavior, including during VM pressure.
    }

    // =========================================================================
    // Snapshot accessor
    // =========================================================================

    /**
     * Returns an immutable point-in-time snapshot of all observed metrics.
     * Only dimension combinations that have been observed appear in the maps.
     */
    public Snapshot getSnapshot() {
        final Map<String, DeploymentMetrics> deployments = new LinkedHashMap<>();
        for (final Map.Entry<String, DurationStats> e : deploymentStats.entrySet()) {
            deployments.put(e.getKey(), toDeploymentMetrics(e.getValue()));
        }
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        for (final Map.Entry<String, DurationStats> e : stageStats.entrySet()) {
            stages.put(e.getKey(), toStageMetrics(e.getValue()));
        }
        final Map<String, Long> components = new LinkedHashMap<>();
        for (final Map.Entry<String, LongAdder> e : componentCounts.entrySet()) {
            components.put(e.getKey(), e.getValue().sum());
        }
        final Map<String, Long> rollbackActions = new LinkedHashMap<>();
        for (final Map.Entry<String, LongAdder> e : rollbackActionCounts.entrySet()) {
            rollbackActions.put(e.getKey(), e.getValue().sum());
        }
        final Map<String, Long> rollbacks = new LinkedHashMap<>();
        for (final Map.Entry<String, LongAdder> e : rollbackCounts.entrySet()) {
            rollbacks.put(e.getKey(), e.getValue().sum());
        }
        return new Snapshot(
                Collections.unmodifiableMap(deployments),
                Collections.unmodifiableMap(stages),
                Collections.unmodifiableMap(components),
                Collections.unmodifiableMap(rollbackActions),
                Collections.unmodifiableMap(rollbacks));
    }

    // =========================================================================
    // Internal accumulator (package-private for testability)
    // =========================================================================

    static final class DurationStats {
        final LongAdder count = new LongAdder();
        final LongAdder totalDurationMillis = new LongAdder();
        final LongAccumulator maxDurationMillis = new LongAccumulator(Long::max, 0L);

        void record(final long durationNanos) {
            count.increment();
            final long durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos);
            totalDurationMillis.add(durationMs);
            maxDurationMillis.accumulate(durationMs);
        }
    }

    private static DeploymentMetrics toDeploymentMetrics(final DurationStats stats) {
        return new DeploymentMetrics(
                stats.count.sum(),
                stats.totalDurationMillis.sum(),
                stats.maxDurationMillis.get());
    }

    private static StageMetrics toStageMetrics(final DurationStats stats) {
        return new StageMetrics(
                stats.count.sum(),
                stats.totalDurationMillis.sum(),
                stats.maxDurationMillis.get());
    }
}
