package org.apache.nifi.copilot.service;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Shared retry policy for NiFi client operations.
 *
 * <p>Covers two orthogonal retry concerns:
 * <ol>
 *   <li><b>Transient GET retries</b> – HTTP 429, 502, 503, 504 and transport
 *       {@link java.io.IOException} / {@link java.net.http.HttpTimeoutException}.
 *       Bounded exponential backoff with ±25 % jitter; honours the
 *       {@code Retry-After} response header (delta-seconds or RFC 1123 date).</li>
 *   <li><b>Revision-conflict retries</b> – applied by callers after re-reading
 *       the latest entity revision. The same {@link #maxAttempts()} cap governs
 *       both concerns.</li>
 * </ol>
 *
 * <p>This class never logs request bodies, response bodies, credentials, or tokens.
 */
@Component
public class NiFiRetryPolicy {
    private static final Logger logger = LoggerFactory.getLogger(NiFiRetryPolicy.class);

    /** RFC 1123 HTTP-date formatter used for {@code Retry-After} date parsing. */
    private static final DateTimeFormatter HTTP_DATE_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME;

    private final int maxAttempts;
    private final long baseDelayMillis;
    private final long maxDelayMillis;

    /**
     * Constructs a retry policy from resolved configuration values.
     *
     * @param config configuration resolver supplying retry parameters
     */
    public NiFiRetryPolicy(final NiFiConfigResolver config) {
        this.maxAttempts = config.resolveRetryMaxAttempts();
        this.baseDelayMillis = config.resolveRetryBaseDelayMillis();
        this.maxDelayMillis = config.resolveRetryMaxDelayMillis();
    }

    /**
     * Returns the maximum number of attempts (including the first try) for both
     * transient GET retries and revision-conflict retries.
     *
     * @return maximum attempt count, in the range 1–10
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Returns {@code true} when the given exception represents a transient error
     * for a GET request that should be retried.
     *
     * <p>Transient conditions: HTTP 429, 502, 503, 504 and transport failures
     * (status {@code 0}, {@link NiFiClientException#isRetryable()} {@code true}).
     * 401 is intentionally excluded because token refresh is handled separately.
     *
     * @param e the exception to classify
     * @return {@code true} if the request should be retried
     */
    public boolean isTransientGetError(final NiFiClientException e) {
        final int status = e.getHttpStatus();
        return status == 429 || status == 502 || status == 503 || status == 504
                || (status == 0 && e.isRetryable());
    }

    /**
     * Computes the delay in milliseconds before the next attempt.
     *
     * <p>If {@code retryAfterMillis} is non-negative it is used directly (capped at
     * {@code maxDelayMillis}). Otherwise, bounded exponential backoff with ±25 % jitter
     * is applied: {@code baseDelayMillis * 2^(attempt-1)}, capped at
     * {@code maxDelayMillis}, multiplied by a random factor in [0.75, 1.25).
     *
     * @param attempt 1-based attempt number that just failed
     * @param retryAfterMillis server-requested delay from a {@code Retry-After} header,
     *        or {@code -1} when not present
     * @return sleep duration in milliseconds (≥ 0)
     */
    public long computeDelayMillis(final int attempt, final long retryAfterMillis) {
        if (retryAfterMillis >= 0) {
            return Math.min(retryAfterMillis, maxDelayMillis);
        }
        final int shift = Math.min(attempt - 1, 30);
        final long exponential = baseDelayMillis * (1L << shift);
        final long capped = Math.min(exponential, maxDelayMillis);
        final double jitter = 0.75 + 0.5 * ThreadLocalRandom.current().nextDouble();
        return Math.max(0L, Math.min((long) (capped * jitter), maxDelayMillis));
    }

    /**
     * Parses a {@code Retry-After} header value into milliseconds.
     *
     * <p>Accepts delta-seconds (non-negative integer string) or an RFC 1123
     * HTTP-date string. Returns {@code -1} when the value is absent, blank,
     * or cannot be parsed.
     *
     * @param retryAfterHeader the raw header value, or {@code null}
     * @return delay in milliseconds (≥ 0) or {@code -1} when not parseable
     */
    public long parseRetryAfterMillis(final String retryAfterHeader) {
        if (retryAfterHeader == null || retryAfterHeader.isBlank()) {
            return -1L;
        }
        final String trimmed = retryAfterHeader.trim();
        try {
            final long seconds = Long.parseLong(trimmed);
            if (seconds >= 0) {
                try {
                    return Math.multiplyExact(seconds, 1000L);
                } catch (ArithmeticException e) {
                    return Long.MAX_VALUE;
                }
            }
        } catch (NumberFormatException ignored) {
        }
        try {
            final ZonedDateTime retryAt = ZonedDateTime.parse(trimmed, HTTP_DATE_FORMATTER);
            final long delayMillis = retryAt.toInstant().toEpochMilli() - System.currentTimeMillis();
            return Math.max(0L, delayMillis);
        } catch (DateTimeParseException ignored) {
        }
        return -1L;
    }

    /**
     * Sleeps for the given duration in milliseconds. An interruption aborts the
     * retry sequence and preserves the thread interrupt flag.
     *
     * @param millis duration to sleep; non-positive values are ignored
     * @throws NiFiClientException when the sleep is interrupted
     */
    public void sleep(final long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Retry sleep interrupted");
            throw new NiFiClientException("RETRY", "", 0, null, false,
                    "NiFi retry wait was interrupted", e);
        }
    }
}
