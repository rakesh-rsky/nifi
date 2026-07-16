package org.apache.nifi.copilot.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.stereotype.Component;

/**
 * Production-safe, in-memory, registry-backed metrics store for
 * {@link NiFiClientOperations} calls.
 *
 * <h2>Cardinality constraints</h2>
 * <p>Keys are bounded to the set of method names declared in
 * {@link NiFiClientOperations}. Outcome labels are one of five fixed strings:
 * {@code success}, {@code client_error}, {@code server_error},
 * {@code transport_error}, {@code failure}. No endpoint paths, component IDs,
 * request arguments, response bodies, usernames, or exception messages are ever
 * used as key material.
 *
 * <h2>Per-operation metrics</h2>
 * <ul>
 *   <li>Call count (total invocations)</li>
 *   <li>Per-outcome counts (success / client_error / server_error /
 *       transport_error / failure)</li>
 *   <li>Total duration in milliseconds (sum over all calls)</li>
 *   <li>Maximum observed duration in milliseconds</li>
 * </ul>
 *
 * <h2>Instrumentation proxy</h2>
 * <p>{@link #instrument(NiFiClientOperations)} wraps a {@link NiFiClientOperations}
 * target in a {@link Proxy} that records every top-level public operation for
 * both external (HTTP) and embedded clients. Object-class methods
 * ({@code equals}, {@code hashCode}, {@code toString}) are forwarded without
 * recording. {@link InvocationTargetException} is always unwrapped so callers
 * receive the original exception type.
 */
@Component
public class NiFiClientMetricsRegistry {
    static final String OUTCOME_SUCCESS = "success";
    static final String OUTCOME_CLIENT_ERROR = "client_error";
    static final String OUTCOME_SERVER_ERROR = "server_error";
    static final String OUTCOME_TRANSPORT_ERROR = "transport_error";
    static final String OUTCOME_FAILURE = "failure";

    private final ConcurrentHashMap<String, OperationStats> statsMap = new ConcurrentHashMap<>();

    // =========================================================================
    // Snapshot record
    // =========================================================================

    /**
     * Immutable point-in-time snapshot of metrics for a single operation.
     */
    public record OperationSnapshot(
            String operationName,
            long callCount,
            long successCount,
            long clientErrorCount,
            long serverErrorCount,
            long transportErrorCount,
            long otherFailureCount,
            long totalDurationMillis,
            long maxDurationMillis) {

        /**
         * Returns the sum of all non-success outcome counts.
         */
        public long failureCount() {
            return clientErrorCount + serverErrorCount + transportErrorCount + otherFailureCount;
        }
    }

    // =========================================================================
    // Public accessors
    // =========================================================================

    /**
     * Returns an immutable snapshot of metrics for all observed operations.
     * Operations that have not been called do not appear in the map.
     *
     * @return unmodifiable map from operation name to its current snapshot
     */
    public Map<String, OperationSnapshot> getSnapshots() {
        final Map<String, OperationSnapshot> result = new LinkedHashMap<>();
        for (final Map.Entry<String, OperationStats> entry : statsMap.entrySet()) {
            result.put(entry.getKey(), toSnapshot(entry.getKey(), entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Returns an immutable snapshot for the named operation, or {@code null} if
     * the operation has never been called.
     *
     * @param operationName the exact method name as declared in {@link NiFiClientOperations}
     * @return snapshot, or {@code null}
     */
    public OperationSnapshot getSnapshot(final String operationName) {
        final OperationStats stats = statsMap.get(operationName);
        return stats == null ? null : toSnapshot(operationName, stats);
    }

    // =========================================================================
    // Instrumentation proxy
    // =========================================================================

    /**
     * Wraps {@code target} in a {@link Proxy} that records call count, outcome,
     * and duration for every method declared in {@link NiFiClientOperations}.
     *
     * <ul>
     *   <li>Return values from {@code target} are passed through unchanged.</li>
     *   <li>{@link InvocationTargetException} is always unwrapped; callers see
     *       the original exception type.</li>
     *   <li>Methods declared on {@link Object} are forwarded without recording.</li>
     *   <li>Outcome classification follows HTTP status: 4xx → {@code client_error},
     *       5xx → {@code server_error}, 0 with {@code isRetryable} → {@code transport_error},
     *       any other exception → {@code failure}.</li>
     * </ul>
     *
     * @param target the client to wrap; must implement {@link NiFiClientOperations}
     * @return a proxy that records metrics and delegates to {@code target}
     */
    public NiFiClientOperations instrument(final NiFiClientOperations target) {
        return (NiFiClientOperations) Proxy.newProxyInstance(
                target.getClass().getClassLoader(),
                new Class[]{NiFiClientOperations.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(target, args);
                    }
                    return recordedInvoke(target, method, args);
                }
        );
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    private Object recordedInvoke(final NiFiClientOperations target,
                                   final Method method,
                                   final Object[] args) throws Throwable {
        final String operationName = method.getName();
        final OperationStats stats = statsMap.computeIfAbsent(operationName, k -> new OperationStats());
        final long startNanos = System.nanoTime();
        try {
            final Object result = method.invoke(target, args);
            final long durationNanos = System.nanoTime() - startNanos;
            stats.record(OUTCOME_SUCCESS, durationNanos);
            return result;
        } catch (InvocationTargetException ite) {
            final long durationNanos = System.nanoTime() - startNanos;
            final Throwable cause = ite.getCause() != null ? ite.getCause() : ite;
            stats.record(classifyOutcome(cause), durationNanos);
            throw cause;
        } catch (Throwable t) {
            final long durationNanos = System.nanoTime() - startNanos;
            stats.record(OUTCOME_FAILURE, durationNanos);
            throw t;
        }
    }

    private static String classifyOutcome(final Throwable t) {
        if (t instanceof NiFiClientException nce) {
            final int status = nce.getHttpStatus();
            if (status >= 500) {
                return OUTCOME_SERVER_ERROR;
            }
            if (status >= 400) {
                return OUTCOME_CLIENT_ERROR;
            }
            if (status == 0 && nce.isRetryable()) {
                return OUTCOME_TRANSPORT_ERROR;
            }
        }
        return OUTCOME_FAILURE;
    }

    private static OperationSnapshot toSnapshot(final String name, final OperationStats stats) {
        return new OperationSnapshot(
                name,
                stats.callCount.sum(),
                stats.successCount.sum(),
                stats.clientErrorCount.sum(),
                stats.serverErrorCount.sum(),
                stats.transportErrorCount.sum(),
                stats.otherFailureCount.sum(),
                stats.totalDurationMillis.sum(),
                stats.maxDurationMillis.get()
        );
    }

    // =========================================================================
    // Mutable per-operation accumulators (package-private for testability)
    // =========================================================================

    static final class OperationStats {
        final LongAdder callCount = new LongAdder();
        final LongAdder successCount = new LongAdder();
        final LongAdder clientErrorCount = new LongAdder();
        final LongAdder serverErrorCount = new LongAdder();
        final LongAdder transportErrorCount = new LongAdder();
        final LongAdder otherFailureCount = new LongAdder();
        final LongAdder totalDurationMillis = new LongAdder();
        final LongAccumulator maxDurationMillis = new LongAccumulator(Long::max, 0L);

        void record(final String outcome, final long durationNanos) {
            callCount.increment();
            final long durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos);
            totalDurationMillis.add(durationMs);
            maxDurationMillis.accumulate(durationMs);
            switch (outcome) {
                case OUTCOME_SUCCESS -> successCount.increment();
                case OUTCOME_CLIENT_ERROR -> clientErrorCount.increment();
                case OUTCOME_SERVER_ERROR -> serverErrorCount.increment();
                case OUTCOME_TRANSPORT_ERROR -> transportErrorCount.increment();
                default -> otherFailureCount.increment();
            }
        }
    }
}
