package org.apache.nifi.copilot.service;

import java.util.Map;

/**
 * Immutable snapshot of the status of a NiFi asynchronous request, parsed from
 * a nested container within a poll or submit response map.
 *
 * <p>Container key, ID field, completion field, and optional failure-reason field
 * are all configurable because different NiFi async resources use different keys:
 * <ul>
 *   <li>Parameter-context update: {@code request / requestId / complete / failureReason}</li>
 *   <li>FlowFile listing:         {@code listingRequest / id / finished / failureReason}</li>
 *   <li>Queue drop:               {@code dropRequest / id / finished / failureReason}</li>
 *   <li>Provenance query:         {@code provenance / id / finished} (no failure-reason)</li>
 *   <li>Lineage query:            {@code lineage / id / finished} (no failure-reason)</li>
 *   <li>Version update/revert:    {@code request / requestId / complete / failureReason}</li>
 * </ul>
 *
 * <p>Missing or malformed fields always fail explicitly:
 * <ul>
 *   <li>The container must be present and be a {@link Map}.</li>
 *   <li>The completion field, when present in the map, must be a {@link Boolean}.</li>
 *   <li>The failure-reason field, when present in the map, must be a {@link String}.</li>
 * </ul>
 */
public final class NiFiAsyncRequestState {

    private final String requestId;
    private final Boolean complete;
    private final String failureReason;

    private NiFiAsyncRequestState(final String requestId, final Boolean complete, final String failureReason) {
        this.requestId = requestId;
        this.complete = complete;
        this.failureReason = failureReason;
    }

    /**
     * Parses an async request state from a response map.
     *
     * @param response           the full response map returned by a submit or poll call
     * @param containerKey       key within {@code response} holding the request object
     *                           (e.g. {@code "request"}, {@code "listingRequest"},
     *                           {@code "provenance"})
     * @param idField            field within the container holding the request ID
     *                           (e.g. {@code "requestId"}, {@code "id"})
     * @param completionField    field indicating completion; value must be {@link Boolean}
     *                           when present (e.g. {@code "complete"}, {@code "finished"})
     * @param failureReasonField field carrying the failure description; value must be
     *                           {@link String} when present; pass {@code null} for async
     *                           operations that have no failure-reason field (provenance,
     *                           lineage), in which case {@link #isTerminalFailure()} always
     *                           returns {@code false}
     * @return parsed state; never {@code null}
     * @throws IllegalStateException if the container is missing or not a map, or if the
     *                               completion field is not a Boolean, or if the
     *                               failure-reason field is not a String
     */
    @SuppressWarnings("unchecked")
    public static NiFiAsyncRequestState parse(
            final Map<String, Object> response,
            final String containerKey,
            final String idField,
            final String completionField,
            final String failureReasonField) {

        final Object containerObj = response.get(containerKey);
        if (!(containerObj instanceof Map<?, ?>)) {
            throw new IllegalStateException(
                    "Async response is missing required container '" + containerKey + "'");
        }
        final Map<String, Object> container = (Map<String, Object>) containerObj;

        final Object rawId = container.get(idField);
        if (rawId != null && !(rawId instanceof String)) {
            throw new IllegalStateException(
                    "Async request ID field '" + idField + "' must be String, got: "
                            + rawId.getClass().getSimpleName());
        }
        final String requestId = rawId == null ? null : ((String) rawId).trim();

        final Boolean complete;
        if (!container.containsKey(completionField)) {
            complete = null;
        } else {
            final Object rawComplete = container.get(completionField);
            if (rawComplete == null) {
                complete = null;
            } else if (rawComplete instanceof Boolean b) {
                complete = b;
            } else {
                throw new IllegalStateException(
                        "Async completion field '" + completionField + "' must be Boolean, got: "
                                + rawComplete.getClass().getSimpleName());
            }
        }

        final String failureReason;
        if (failureReasonField == null) {
            failureReason = null;
        } else if (!container.containsKey(failureReasonField)) {
            failureReason = null;
        } else {
            final Object rawReason = container.get(failureReasonField);
            if (rawReason == null) {
                failureReason = null;
            } else if (rawReason instanceof String s) {
                failureReason = s;
            } else {
                throw new IllegalStateException(
                        "Async failure-reason field '" + failureReasonField + "' must be String, got: "
                                + rawReason.getClass().getSimpleName());
            }
        }

        return new NiFiAsyncRequestState(requestId, complete, failureReason);
    }

    /**
     * Returns the request ID.
     *
     * @return non-blank request ID; never {@code null}
     * @throws IllegalStateException if the request ID is null or blank
     */
    public String getRequiredRequestId() {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalStateException(
                    "Async request ID is required but was missing or blank in the server response");
        }
        return requestId;
    }

    /**
     * Returns {@code true} when the completion field is {@link Boolean#TRUE} and the
     * failure-reason field is absent or blank.
     *
     * @return {@code true} for the terminal success state
     */
    public boolean isTerminalSuccess() {
        requireCompletionState();
        return Boolean.TRUE.equals(complete) && (failureReason == null || failureReason.isBlank());
    }

    /**
     * Returns {@code true} when the failure-reason field is present and non-blank.
     * Always returns {@code false} when no {@code failureReasonField} was configured.
     *
     * @return {@code true} for the terminal failure state
     */
    public boolean isTerminalFailure() {
        requireCompletionState();
        return Boolean.TRUE.equals(complete) && failureReason != null && !failureReason.isBlank();
    }

    private void requireCompletionState() {
        if (complete == null) {
            throw new IllegalStateException("Async response is missing the required completion state");
        }
    }
}
