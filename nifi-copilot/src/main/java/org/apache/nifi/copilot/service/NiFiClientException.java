package org.apache.nifi.copilot.service;

/**
 * Runtime exception representing a failed NiFi client operation.
 * <p>
 * This exception captures structured request and response metadata for callers
 * that need to inspect the failed HTTP method, path, status code, response
 * body, retryability, and optional retry delay without leaking potentially
 * sensitive response bodies through the exception message.
 */
public class NiFiClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String method;
    private final String path;
    private final int httpStatus;
    private final String responseBody;
    private final boolean retryable;
    private final long retryAfterMillis;
    private final String sanitizedMessage;

    /**
     * Creates an exception for a failed NiFi request.
     *
     * @param method HTTP method used for the request
     * @param path request path associated with the failure
     * @param httpStatus HTTP status code, or {@code 0} when unavailable
     * @param responseBody raw response body, which may be {@code null}
     * @param retryable whether the failure is considered retryable
     * @param message base exception message
     */
    public NiFiClientException(final String method, final String path, final int httpStatus,
            final String responseBody, final boolean retryable, final String message) {
        this(method, path, httpStatus, responseBody, retryable, message, null, -1L);
    }

    /**
     * Creates an exception for a failed NiFi request with a cause.
     *
     * @param method HTTP method used for the request
     * @param path request path associated with the failure
     * @param httpStatus HTTP status code, or {@code 0} when unavailable
     * @param responseBody raw response body, which may be {@code null}
     * @param retryable whether the failure is considered retryable
     * @param message base exception message
     * @param cause underlying cause
     */
    public NiFiClientException(final String method, final String path, final int httpStatus,
            final String responseBody, final boolean retryable, final String message, final Throwable cause) {
        this(method, path, httpStatus, responseBody, retryable, message, cause, -1L);
    }

    /**
     * Creates an exception for a failed NiFi request, carrying an optional
     * Retry-After delay parsed from the server response header.
     *
     * @param method HTTP method used for the request
     * @param path request path associated with the failure
     * @param httpStatus HTTP status code, or {@code 0} when unavailable
     * @param responseBody raw response body, which may be {@code null}
     * @param retryable whether the failure is considered retryable
     * @param message base exception message
     * @param retryAfterMillis server-requested retry delay in milliseconds,
     *        or {@code -1} when no Retry-After header was present
     */
    public NiFiClientException(final String method, final String path, final int httpStatus,
            final String responseBody, final boolean retryable, final String message,
            final long retryAfterMillis) {
        this(method, path, httpStatus, responseBody, retryable, message, null, retryAfterMillis);
    }

    /**
     * Master constructor. All other constructors delegate here.
     *
     * @param method HTTP method used for the request
     * @param path request path associated with the failure
     * @param httpStatus HTTP status code, or {@code 0} when unavailable
     * @param responseBody raw response body, which may be {@code null}
     * @param retryable whether the failure is considered retryable
     * @param message base exception message
     * @param cause underlying cause, or {@code null}
     * @param retryAfterMillis server-requested retry delay in milliseconds,
     *        or {@code -1} when no Retry-After header was present
     */
    public NiFiClientException(final String method, final String path, final int httpStatus,
            final String responseBody, final boolean retryable, final String message,
            final Throwable cause, final long retryAfterMillis) {
        super(message, cause);
        this.method = method == null ? "" : method;
        this.path = path == null ? "" : path;
        this.httpStatus = httpStatus;
        this.responseBody = responseBody;
        this.retryable = retryable;
        this.retryAfterMillis = retryAfterMillis;
        this.sanitizedMessage = message + " [method=" + this.method + ", path=" + this.path + ", httpStatus=" + this.httpStatus + "]";
    }

    /**
     * Creates an exception for authentication or connection failures that occur
     * before request metadata is fully known.
     *
     * @param message base exception message
     * @param cause underlying cause
     */
    public NiFiClientException(final String message, final Throwable cause) {
        this("", "", 0, null, false, message, cause, -1L);
    }

    /**
     * Returns the HTTP method associated with the failure.
     *
     * @return the HTTP method, or an empty string when unavailable
     */
    public String getMethod() {
        return method;
    }

    /**
     * Returns the request path associated with the failure.
     *
     * @return the request path, or an empty string when unavailable
     */
    public String getPath() {
        return path;
    }

    /**
     * Returns the HTTP status code associated with the failure.
     *
     * @return the HTTP status code, or {@code 0} when unavailable
     */
    public int getHttpStatus() {
        return httpStatus;
    }

    /**
     * Returns the raw response body associated with the failure.
     *
     * @return the raw response body, or {@code null} when unavailable
     */
    public String getResponseBody() {
        return responseBody;
    }

    /**
     * Indicates whether the failure is retryable.
     *
     * @return {@code true} when retryable; otherwise {@code false}
     */
    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Returns the server-requested retry delay derived from a {@code Retry-After}
     * response header, or {@code -1} when no such header was present.
     *
     * @return retry delay in milliseconds, or {@code -1} when unavailable
     */
    public long getRetryAfterMillis() {
        return retryAfterMillis;
    }

    /**
     * Returns a sanitized exception message that excludes the response body.
     *
     * @return the sanitized exception message
     */
    @Override
    public String getMessage() {
        return sanitizedMessage;
    }
}
