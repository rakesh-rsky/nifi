package org.apache.nifi.copilot.service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Transport-neutral executor for NiFi asynchronous request lifecycles.
 *
 * <p>NiFi models several operations (e.g. process-group run-status changes,
 * parameter-context updates) as async requests: a client submits an operation,
 * polls until the request reaches a terminal state, then deletes the request
 * resource. This executor encapsulates that pattern independent of whether the
 * underlying transport is an HTTP call or an in-process facade.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li><b>Submit</b> – calls {@link RequestLifecycle#submit()}, obtaining an
 *       opaque request identifier. If submit fails the executor propagates the
 *       exception immediately without any cleanup.</li>
 *   <li><b>Poll</b> – repeatedly calls {@link RequestLifecycle#poll(String, Duration)} at
 *       the configured interval until the status is complete, failure, or the
 *       timeout elapses.</li>
 *   <li><b>Cleanup</b> – calls {@link RequestLifecycle#cleanup(String)}
 *       unconditionally once a request identifier has been obtained, regardless
 *       of success or failure.</li>
 * </ol>
 *
 * <h2>Failure semantics</h2>
 * <ul>
 *   <li>If the operation fails (failure terminal state, timeout, or interrupted
 *       poll) <em>and</em> cleanup also throws, the cleanup exception is
 *       {@linkplain Throwable#addSuppressed suppressed} onto the primary failure.</li>
 *   <li>If the operation succeeds but cleanup throws, the cleanup exception is
 *       surfaced to the caller.</li>
 * </ul>
 */
@Component
public class NiFiAsyncRequestExecutor {
    private static final Logger logger = LoggerFactory.getLogger(NiFiAsyncRequestExecutor.class);

    private final long pollIntervalMillis;
    private final long timeoutMillis;

    /**
     * Constructs an executor using configuration-driven poll interval and timeout.
     *
     * @param config configuration resolver supplying async timing parameters
     */
    public NiFiAsyncRequestExecutor(final NiFiConfigResolver config) {
        this.pollIntervalMillis = config.resolveAsyncPollIntervalMillis();
        this.timeoutMillis = config.resolveAsyncTimeoutSeconds() * 1000L;
    }

    /**
     * Lifecycle contract for a NiFi asynchronous request.
     *
     * <p>Implementations must be transport-neutral: both HTTP and in-process
     * implementations can fulfill this interface.
     *
     * @param <S> the status type returned by {@link #poll(String, Duration)}; its values
     *            drive the terminal-state checks
     */
    public interface RequestLifecycle<S> {

        /**
         * Submits the asynchronous operation and returns an opaque request identifier
         * used for subsequent polling and cleanup.
         *
         * @return a non-null request identifier
         * @throws NiFiClientException on submission failure
         */
        String submit();

        /**
         * Polls the current status of the identified request.
         *
         * @param requestId the identifier returned by {@link #submit()}
         * @param remainingTime maximum time remaining for the complete lifecycle;
         *        implementations should constrain transport timeouts accordingly
         * @return the current status object; never {@code null}
         * @throws NiFiClientException on poll failure
         */
        S poll(String requestId, Duration remainingTime);

        /**
         * Returns {@code true} when the status represents a successful terminal state.
         *
         * @param status the value returned by the most recent {@link #poll(String, Duration)}
         * @return {@code true} if the operation completed successfully
         */
        boolean isComplete(S status);

        /**
         * Returns {@code true} when the status represents a failure terminal state.
         *
         * @param status the value returned by the most recent {@link #poll(String, Duration)}
         * @return {@code true} if the operation has failed
         */
        boolean isFailure(S status);

        /**
         * Releases the async request resource. Called unconditionally after a
         * request identifier has been obtained, regardless of the outcome.
         *
         * @param requestId the identifier returned by {@link #submit()}
         * @throws NiFiClientException on cleanup failure
         */
        void cleanup(String requestId);
    }

    /**
     * Executes an asynchronous NiFi operation through its full lifecycle.
     *
     * @param <S>       the status type
     * @param lifecycle the lifecycle implementation to drive
     * @return the last polled status when the operation completes successfully
     * @throws NiFiClientException if the operation fails, times out, or the
     *         cleanup step surfaces an error after an otherwise-successful run
     */
    public <S> S execute(final RequestLifecycle<S> lifecycle) {
        final String requestId = lifecycle.submit();
        if (requestId == null || requestId.isBlank()) {
            throw new NiFiClientException("ASYNC_SUBMIT", "", 0, null, false,
                    "Async NiFi request submission returned no request identifier");
        }
        S lastStatus = null;
        RuntimeException primaryFailure = null;

        try {
            final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            final long startNanos = System.nanoTime();
            while (true) {
                long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
                if (remainingNanos <= 0) {
                    primaryFailure = timeoutException(requestId);
                    break;
                }

                lastStatus = lifecycle.poll(requestId, Duration.ofNanos(remainingNanos));
                remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
                if (remainingNanos <= 0) {
                    primaryFailure = timeoutException(requestId);
                    break;
                }
                if (lifecycle.isComplete(lastStatus)) {
                    logger.debug("Async request {} completed", requestId);
                    break;
                }
                if (lifecycle.isFailure(lastStatus)) {
                    primaryFailure = new NiFiClientException("ASYNC", requestId, 0, null, false,
                            "Async NiFi request reached failure state");
                    break;
                }
                final long remainingMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                try {
                    Thread.sleep(Math.min(pollIntervalMillis, remainingMillis));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    primaryFailure = new NiFiClientException("ASYNC", requestId, 0, null, false,
                            "Async NiFi request poll interrupted", ie);
                    break;
                }
            }
        } catch (RuntimeException e) {
            primaryFailure = e instanceof NiFiClientException
                    ? e
                    : new NiFiClientException("ASYNC_POLL", requestId, 0, null, false,
                            "Async NiFi request polling failed", e);
        }

        // Unconditional cleanup
        try {
            lifecycle.cleanup(requestId);
        } catch (Exception cleanupEx) {
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(cleanupEx);
                throw primaryFailure;
            }
            logger.debug("Async request {} cleanup failed after successful completion", requestId);
            if (cleanupEx instanceof NiFiClientException nce) {
                throw nce;
            }
            throw new NiFiClientException("ASYNC_CLEANUP", requestId, 0, null, false,
                    "Async NiFi request cleanup failed", cleanupEx);
        }

        if (primaryFailure != null) {
            throw primaryFailure;
        }
        return lastStatus;
    }

    private NiFiClientException timeoutException(final String requestId) {
        return new NiFiClientException("ASYNC", requestId, 0, null, false,
                "Async NiFi request timed out after " + (timeoutMillis / 1000L) + "s");
    }
}
