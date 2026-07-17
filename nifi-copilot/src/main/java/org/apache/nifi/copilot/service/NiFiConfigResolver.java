package org.apache.nifi.copilot.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class NiFiConfigResolver {
    private static final Logger logger = LoggerFactory.getLogger(NiFiConfigResolver.class);
    private static final String CONFIG_RESOURCE = "nifi.properties";
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 60;
    private static final int DEFAULT_RETRY_MAX_ATTEMPTS = 3;
    private static final long DEFAULT_RETRY_BASE_DELAY_MILLIS = 250L;
    private static final long DEFAULT_RETRY_MAX_DELAY_MILLIS = 5000L;
    private static final long DEFAULT_ASYNC_POLL_INTERVAL_MILLIS = 1000L;
    private static final long DEFAULT_ASYNC_TIMEOUT_SECONDS = 120L;
    private static final long MAX_RETRY_BASE_DELAY_MILLIS = 60_000L;
    private static final long MAX_RETRY_DELAY_MILLIS = 300_000L;
    private static final long MAX_ASYNC_POLL_INTERVAL_MILLIS = 60_000L;
    private static final long MAX_ASYNC_TIMEOUT_SECONDS = 86_400L;
    private final Properties properties = loadProperties();

    public String resolveBaseUrl() {
        final String explicit = System.getenv("NIFI_BASE_URL");
        if (explicit != null && !explicit.isBlank()) {
            return stripTrailingSlash(explicit);
        }
        return stripTrailingSlash(properties.getProperty("nifi.base.url", "https://localhost:8443").trim());
    }

    public String resolveUsername() {
        final String explicit = System.getenv("NIFI_USERNAME");
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return properties.getProperty("nifi.username", "");
    }

    public String resolvePassword() {
        final String explicit = System.getenv("NIFI_PASSWORD");
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return properties.getProperty("nifi.password", "");
    }

    public boolean verifySsl() {
        final String raw = System.getenv().getOrDefault("NIFI_VERIFY_SSL",
                properties.getProperty("nifi.verify.ssl", "true")).toLowerCase();
        return !(raw.equals("false") || raw.equals("0") || raw.equals("no"));
    }

    /**
     * Returns the TCP connect timeout in seconds. Reads NIFI_CONNECT_TIMEOUT_SECONDS env var
     * or nifi.connect.timeout.seconds property. Defaults to 30. Must be positive.
     */
    public int resolveConnectTimeoutSeconds() {
        return resolvePositiveInt("NIFI_CONNECT_TIMEOUT_SECONDS", "nifi.connect.timeout.seconds", DEFAULT_CONNECT_TIMEOUT_SECONDS);
    }

    /**
     * Returns the HTTP request timeout in seconds. Reads NIFI_REQUEST_TIMEOUT_SECONDS env var
     * or nifi.request.timeout.seconds property. Defaults to 60. Must be positive.
     */
    public int resolveRequestTimeoutSeconds() {
        return resolvePositiveInt("NIFI_REQUEST_TIMEOUT_SECONDS", "nifi.request.timeout.seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS);
    }

    /**
     * Returns the maximum number of retry attempts for transient GET errors and revision
     * conflicts. Reads NIFI_RETRY_MAX_ATTEMPTS env var or nifi.retry.max.attempts property.
     * Defaults to 3. Must be in the range 1–10.
     */
    public int resolveRetryMaxAttempts() {
        final String raw = System.getenv().getOrDefault("NIFI_RETRY_MAX_ATTEMPTS",
                properties.getProperty("nifi.retry.max.attempts", "")).trim();
        if (raw.isBlank()) {
            return DEFAULT_RETRY_MAX_ATTEMPTS;
        }
        try {
            final int v = Integer.parseInt(raw);
            if (v < 1 || v > 10) {
                logger.warn("Configuration value for nifi.retry.max.attempts must be 1-10, got {}; using default {}", v, DEFAULT_RETRY_MAX_ATTEMPTS);
                return DEFAULT_RETRY_MAX_ATTEMPTS;
            }
            return v;
        } catch (NumberFormatException e) {
            logger.warn("Configuration value for nifi.retry.max.attempts is not a valid integer: '{}'; using default {}", raw, DEFAULT_RETRY_MAX_ATTEMPTS);
            return DEFAULT_RETRY_MAX_ATTEMPTS;
        }
    }

    /**
     * Returns the base exponential-backoff delay in milliseconds. Reads
     * NIFI_RETRY_BASE_DELAY_MILLIS env var or nifi.retry.base.delay.millis property.
     * Defaults to 250. Must be positive.
     */
    public long resolveRetryBaseDelayMillis() {
        return resolveBoundedPositiveLong("NIFI_RETRY_BASE_DELAY_MILLIS", "nifi.retry.base.delay.millis",
                DEFAULT_RETRY_BASE_DELAY_MILLIS, MAX_RETRY_BASE_DELAY_MILLIS);
    }

    /**
     * Returns the maximum backoff delay in milliseconds. Reads NIFI_RETRY_MAX_DELAY_MILLIS
     * env var or nifi.retry.max.delay.millis property. Defaults to 5000. Must be positive and
     * not less than the base delay.
     */
    public long resolveRetryMaxDelayMillis() {
        final long maxDelay = resolveBoundedPositiveLong("NIFI_RETRY_MAX_DELAY_MILLIS", "nifi.retry.max.delay.millis",
                DEFAULT_RETRY_MAX_DELAY_MILLIS, MAX_RETRY_DELAY_MILLIS);
        final long baseDelay = resolveRetryBaseDelayMillis();
        if (maxDelay < baseDelay) {
            logger.warn("nifi.retry.max.delay.millis ({}) is less than nifi.retry.base.delay.millis ({}); using base delay as maximum", maxDelay, baseDelay);
            return baseDelay;
        }
        return maxDelay;
    }

    /**
     * Returns the async request poll interval in milliseconds. Reads
     * NIFI_ASYNC_POLL_INTERVAL_MILLIS env var or nifi.async.poll.interval.millis property.
     * Defaults to 1000. Must be positive.
     */
    public long resolveAsyncPollIntervalMillis() {
        return resolveBoundedPositiveLong("NIFI_ASYNC_POLL_INTERVAL_MILLIS", "nifi.async.poll.interval.millis",
                DEFAULT_ASYNC_POLL_INTERVAL_MILLIS, MAX_ASYNC_POLL_INTERVAL_MILLIS);
    }

    /**
     * Returns the async request timeout in seconds. Reads NIFI_ASYNC_TIMEOUT_SECONDS env var
     * or nifi.async.timeout.seconds property. Defaults to 120. Must be positive.
     */
    public long resolveAsyncTimeoutSeconds() {
        return resolveBoundedPositiveLong("NIFI_ASYNC_TIMEOUT_SECONDS", "nifi.async.timeout.seconds",
                DEFAULT_ASYNC_TIMEOUT_SECONDS, MAX_ASYNC_TIMEOUT_SECONDS);
    }

    private int resolvePositiveInt(final String envVar, final String property, final int defaultValue) {
        final String raw = System.getenv().getOrDefault(envVar, properties.getProperty(property, "")).trim();
        if (raw.isBlank()) {
            return defaultValue;
        }
        try {
            final int v = Integer.parseInt(raw);
            if (v <= 0) {
                logger.warn("Configuration value for {} must be positive, got {}; using default {}", property, v, defaultValue);
                return defaultValue;
            }
            return v;
        } catch (NumberFormatException e) {
            logger.warn("Configuration value for {} is not a valid integer: '{}'; using default {}", property, raw, defaultValue);
            return defaultValue;
        }
    }

    private long resolveBoundedPositiveLong(final String envVar, final String property, final long defaultValue, final long maximumValue) {
        final String raw = System.getenv().getOrDefault(envVar, properties.getProperty(property, "")).trim();
        if (raw.isBlank()) {
            return defaultValue;
        }
        try {
            final long v = Long.parseLong(raw);
            if (v <= 0 || v > maximumValue) {
                logger.warn("Configuration value for {} must be between 1 and {}, got {}; using default {}",
                        property, maximumValue, v, defaultValue);
                return defaultValue;
            }
            return v;
        } catch (NumberFormatException e) {
            logger.warn("Configuration value for {} is not a valid long integer: '{}'; using default {}", property, raw, defaultValue);
            return defaultValue;
        }
    }

    private String stripTrailingSlash(final String value) {
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private Properties loadProperties() {
        final Properties resolved = new Properties();
        try {
            final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try (InputStream inputStream = classLoader.getResourceAsStream(CONFIG_RESOURCE)) {
                if (inputStream != null) {
                    resolved.load(inputStream);
                } else {
                    logger.warn("Classpath resource {} not found; using defaults.", CONFIG_RESOURCE);
                }
            }
        } catch (IOException e) {
            logger.warn("Failed loading classpath resource {}; using defaults.", CONFIG_RESOURCE, e);
        }
        return resolved;
    }
}
