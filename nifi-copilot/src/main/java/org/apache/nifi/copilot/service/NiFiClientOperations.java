package org.apache.nifi.copilot.service;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public interface NiFiClientOperations {
    String getProcessGroupId(String pgId);

    Map<String, Object> createProcessGroup(String parentPgId, String name, double x, double y);

    Map<String, Object> getProcessGroupFlow(String pgId);

    void ensureTypeCache();

    Map<String, Object> createProcessor(String pgId, String processorType, String name, Double x, Double y, Map<String, Object> properties);

    void startProcessor(String procId);

    void deleteProcessor(String procId);

    boolean waitForProcessorValid(String procId, int timeoutSec);

    void autoTerminateUnusedRelationships(String procId, Set<String> usedRelationships);

    Map<String, Object> createConnection(String pgId, String sourceId, String sourceType, String destId, String destType, List<String> relationships);

    void deleteConnection(String connId);

    List<Map<String, Object>> listControllerServices(String pgId);

    Map<String, Object> createControllerService(String pgId, String serviceType, String name, Map<String, Object> properties);

    void enableControllerService(String csId);

    void disableControllerService(String csId);

    void deleteControllerService(String csId);

    /**
     * Creates a parameter context with the given name, parameters, and description.
     * Maps to {@code POST /parameter-contexts} with revision 0.
     *
     * @param name        the parameter context name; must not be blank
     * @param parameters  map of parameter name to value
     * @param description optional description; null treated as empty string
     * @return map containing {@code id} and {@code name} of the created parameter context
     */
    Map<String, Object> createParameterContext(String name, Map<String, String> parameters, String description);

    /**
     * Binds the parameter context with the given ID to the process group with the given ID.
     * Maps to {@code PUT /process-groups/{pgId}} with a fresh revision on each attempt.
     *
     * @param pgId the process group ID; must not be blank
     * @param pcId the parameter context ID to bind; must not be blank
     */
    void bindParameterContextToProcessGroup(String pgId, String pcId);

    /**
     * Deletes the parameter context with the given ID.
     * Maps to {@code DELETE /parameter-contexts/{id}} with a fresh revision on each attempt.
     *
     * @param pcId the parameter context ID; must not be blank
     */
    void deleteParameterContext(String pcId);

    /**
     * Explicitly removes the parameter context binding from the process group with the given ID.
     * Equivalent to a PUT on the process group with {@code parameterContext: null}, clearing
     * any previously bound parameter context. A fresh revision is obtained on every retry attempt.
     * <p>This is used during rollback to restore a process group's original (unbound) state before
     * the newly created parameter context is deleted, preventing failures caused by running
     * components that still reference the context being removed.
     *
     * @param pgId the process group ID; must not be blank
     * @throws IllegalArgumentException if pgId is blank
     */
    void unbindParameterContextFromProcessGroup(String pgId);

    /**
     * Lists all parameter contexts visible to the current user.
     * Maps to {@code GET /flow/parameter-contexts}; returns the complete entity maps.
     *
     * @return list of complete {@code ParameterContextEntity} maps
     */
    List<Map<String, Object>> listParameterContexts();

    /**
     * Updates the parameter context with the given ID using a synchronous PUT.
     * A fresh entity revision is obtained on every attempt; the caller-provided map is copied
     * and any {@code id} and {@code revision} keys are removed to prevent overriding the
     * component identity or optimistic-lock revision.
     * Maps to {@code PUT /parameter-contexts/{id}}.
     *
     * @param id      the parameter context ID; must not be blank
     * @param updates the fields to update on the component; must not be null or empty
     * @return the updated {@code ParameterContextEntity} as a map
     * @throws IllegalArgumentException if id is blank or updates is null/empty
     */
    Map<String, Object> updateParameterContext(String id, Map<String, Object> updates);

    /**
     * Submits an asynchronous parameter context update request.
     * A fresh revision is obtained before every submission; the caller-provided updates map is
     * sanitized of {@code id} and {@code revision} keys. Maps to
     * {@code POST /parameter-contexts/{id}/update-requests}.
     * The returned map wraps a {@code ParameterContextUpdateRequestEntity} with a nested
     * {@code request} object whose {@code requestId} is used for subsequent poll and cleanup calls.
     *
     * @param id      the parameter context ID; must not be blank
     * @param updates the update fields (parameters, name, description, etc.); must not be null or empty
     * @return the initial {@code ParameterContextUpdateRequestEntity} map
     * @throws IllegalArgumentException if id is blank or updates is null/empty
     */
    Map<String, Object> submitParameterContextUpdateRequest(String id, Map<String, Object> updates);

    /**
     * Polls the status of an outstanding asynchronous parameter context update request.
     * Maps to {@code GET /parameter-contexts/{id}/update-requests/{requestId}}.
     *
     * @param id        the parameter context ID; must not be blank
     * @param requestId the update request ID returned by {@link #submitParameterContextUpdateRequest}; must not be blank
     * @return the current {@code ParameterContextUpdateRequestEntity} map with nested {@code request} object
     * @throws IllegalArgumentException if id or requestId is blank
     */
    Map<String, Object> getParameterContextUpdateRequest(String id, String requestId);

    /**
     * Deletes a completed or cancelled asynchronous parameter context update request.
     * Maps to {@code DELETE /parameter-contexts/{id}/update-requests/{requestId}}.
     *
     * @param id        the parameter context ID; must not be blank
     * @param requestId the update request ID; must not be blank
     * @return the terminal {@code ParameterContextUpdateRequestEntity} map
     * @throws IllegalArgumentException if id or requestId is blank
     */
    Map<String, Object> deleteParameterContextUpdateRequest(String id, String requestId);

    /**
     * Performs a live parameter context update by driving the full asynchronous request
     * lifecycle via {@link NiFiAsyncRequestExecutor}: submit → poll until complete or timeout →
     * cleanup unconditionally.
     * <p>Success is determined when the nested {@code request.complete} is {@code true} and
     * {@code request.failureReason} is blank. Failure is detected when {@code failureReason}
     * is non-blank. Returns the terminal poll status map.
     *
     * @param id      the parameter context ID; must not be blank
     * @param updates the update fields; must not be null or empty
     * @return the terminal {@code ParameterContextUpdateRequestEntity} map from the last successful poll
     * @throws IllegalArgumentException if id is blank or updates is null/empty
     * @throws NiFiClientException      if the update fails, times out, or is interrupted
     */
    Map<String, Object> updateParameterContextLive(String id, Map<String, Object> updates);

    /**
     * Retrieves the process group entity with the given ID.
     *
     * @param processGroupId the ID of the process group; must not be blank
     * @return the complete process group entity as a map
     * @throws IllegalArgumentException if processGroupId is blank
     */
    Map<String, Object> getProcessGroup(String processGroupId);

    /**
     * Updates the process group with the given ID, applying the specified update fields.
     * The caller-provided map is copied; {@code id} and {@code revision} keys within the update
     * map are ignored to prevent overriding the component identity or optimistic-lock revision.
     *
     * @param processGroupId the ID of the process group to update; must not be blank
     * @param updates        the fields to update on the component; must not be null or empty
     * @return the updated process group entity as a map
     * @throws IllegalArgumentException if processGroupId is blank or updates is null/empty
     */
    Map<String, Object> updateProcessGroup(String processGroupId, Map<String, Object> updates);

    /**
     * Deletes the process group with the given ID.
     *
     * @param processGroupId the ID of the process group to delete; must not be blank
     * @throws IllegalArgumentException if processGroupId is blank
     */
    void deleteProcessGroup(String processGroupId);

    /**
     * Lists all processors that are direct members of the process group with the given ID.
     * Returns the complete processor entity maps as returned by NiFi.
     *
     * @param processGroupId the ID of the process group; must not be blank
     * @return list of complete processor entity maps
     * @throws IllegalArgumentException if processGroupId is blank
     */
    List<Map<String, Object>> listProcessors(String processGroupId);

    /**
     * Lists all connections in the process group with the given ID.
     * Returns the complete connection entity maps as returned by NiFi.
     *
     * @param processGroupId the ID of the process group; must not be blank
     * @return list of complete connection entity maps
     * @throws IllegalArgumentException if processGroupId is blank
     */
    List<Map<String, Object>> listConnections(String processGroupId);

    /**
     * Lists the direct child process groups of the process group with the given ID.
     * Returns the complete child process group entity maps as returned by NiFi.
     *
     * @param processGroupId the ID of the parent process group; must not be blank
     * @return list of complete child process group entity maps
     * @throws IllegalArgumentException if processGroupId is blank
     */
    List<Map<String, Object>> listChildProcessGroups(String processGroupId);

    /**
     * Schedules all applicable components in the process group with the given ID to the
     * specified state. Accepted states are {@code RUNNING}, {@code STOPPED}, {@code ENABLED},
     * and {@code DISABLED} (case-insensitive). NiFi gathers current component revisions
     * server-side; no explicit revision tracking is required by the caller.
     *
     * @param processGroupId the ID of the process group; must not be blank
     * @param state          the target schedule state; must be RUNNING, STOPPED, ENABLED, or DISABLED
     * @return the schedule-components result entity as a map
     * @throws IllegalArgumentException if processGroupId is blank or state is not a recognized value
     */
    Map<String, Object> scheduleProcessGroup(String processGroupId, String state);

    /**
     * Captures the current state of the process group for rollback purposes.
     * Records pre-existing processor IDs, connection IDs, child process group IDs, and
     * the target process group's current parameter-context binding ID. The returned
     * mutable snapshot also contains empty lists for deployment-created controller
     * service and parameter context IDs so the deployment coordinator can register
     * exactly the global resources that rollback owns.
     *
     * @param pgId the process group ID; must not be blank
     * @return snapshot map containing {@code pg_id}, {@code processors}, {@code connections},
     *         {@code processGroups}, {@code createdControllerServiceIds},
     *         {@code createdParameterContextIds}, and {@code originalPcBindingId} (nullable)
     */
    Map<String, Object> snapshotProcessGroup(String pgId);

    /**
     * Restores the process group to the state captured by {@link #snapshotProcessGroup}.
     * Deletes newly created processors, connections, and child process groups first; then
     * registered deployment-created controller services (disabled before deletion); then, if
     * {@code originalPcBindingId} is present in the snapshot, restores the target process
     * group's original parameter-context binding (rebinds the original context, or explicitly
     * unbinds when the original was {@code null}); finally deletes only the registered
     * deployment-created parameter contexts. Pre-existing and concurrently created
     * global resources are never deleted.
     * <p>Cleanup failures are collected and rethrown as a single {@link NiFiClientException}
     * with all individual failures attached as suppressed exceptions.
     *
     * @param snapshot the snapshot map returned by {@link #snapshotProcessGroup}
     * @throws NiFiClientException if one or more resources could not be deleted during restore
     */
    void restoreFromSnapshot(Map<String, Object> snapshot);

    List<double[]> getOccupiedPositions(String pgId);

    static int intValue(final Object v) {
        if (v == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(v));
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Extracts the revision version for update/delete operations, throwing if missing or invalid.
     * Unlike {@link #intValue(Object)}, this method never silently defaults to 0 for non-creation operations.
     *
     * @param v the raw version value (from a revision map)
     * @return the non-negative revision version
     * @throws IllegalStateException if the value is null, unparseable, or negative
     */
    static long strictRevisionVersion(final Object v) {
        if (v == null) {
            throw new IllegalStateException("Revision version is required for update/delete operations but was null");
        }
        final String raw = String.valueOf(v).trim();
        if (raw.isBlank()) {
            throw new IllegalStateException("Revision version is required for update/delete operations but was blank");
        }
        final long version;
        try {
            version = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Revision version is not a valid integer: '" + raw + "'", e);
        }
        if (version < 0) {
            throw new IllegalStateException("Revision version must be non-negative for update/delete operations, got: " + version);
        }
        return version;
    }

    static double doubleValue(final Object v) {
        if (v == null) {
            return 0D;
        }
        try {
            return Double.parseDouble(String.valueOf(v));
        } catch (Exception e) {
            return 0D;
        }
    }

    /**
     * Retrieves diagnostics for the processor with the given ID.
     * Maps to {@code GET /processors/{id}/diagnostics}.
     *
     * @param processorId the ID of the processor; must not be blank
     * @return the complete {@code ProcessorDiagnosticsEntity} as a map
     * @throws IllegalArgumentException if processorId is blank
     */
    Map<String, Object> getProcessorDiagnostics(String processorId);

    /**
     * Retrieves the component state for the processor with the given ID.
     * Maps to {@code GET /processors/{id}/state}.
     *
     * @param processorId the ID of the processor; must not be blank
     * @return the {@code ComponentStateEntity} as a map
     * @throws IllegalArgumentException if processorId is blank
     */
    Map<String, Object> getProcessorState(String processorId);

    /**
     * Clears the component state for the processor with the given ID.
     * Maps to {@code POST /processors/{id}/state/clear-requests}.
     * Returns the {@code ComponentStateEntity} reflecting the post-clear state.
     *
     * @param processorId the ID of the processor; must not be blank
     * @return the {@code ComponentStateEntity} as a map after clearing
     * @throws IllegalArgumentException if processorId is blank
     */
    Map<String, Object> clearProcessorState(String processorId);

    /**
     * Terminates active threads for the processor with the given ID.
     * Maps to {@code DELETE /processors/{id}/threads}.
     *
     * @param processorId the ID of the processor; must not be blank
     * @return the {@code ProcessorEntity} as a map
     * @throws IllegalArgumentException if processorId is blank
     */
    Map<String, Object> terminateProcessorThreads(String processorId);

    /**
     * Retrieves the processor entity with the given ID.
     * Maps to {@code GET /processors/{id}}.
     *
     * @param processorId the ID of the processor; must not be blank
     * @return the complete {@code ProcessorEntity} as a map
     * @throws IllegalArgumentException if processorId is blank
     */
    Map<String, Object> getProcessor(String processorId);

    /**
     * Updates the processor with the given ID using a whitelist of officially mutable fields:
     * {@code name}, {@code position}, {@code style}, {@code config}, and {@code bundle}.
     * The caller-provided map must not be null and must contain at least one of these fields.
     * The {@code id} and {@code revision} keys are ignored; the component id is always forced
     * to {@code processorId}. Run state must be managed separately via
     * {@link #startProcessor}; {@code state} and {@code physicalState} in the update map
     * are excluded. A fresh entity revision is obtained on every retry attempt inside
     * {@code executeWithRevisionRetry}. Maps to {@code PUT /processors/{id}}.
     *
     * @param processorId the ID of the processor to update; must not be blank
     * @param updates     the mutable fields to apply; must not be null and must contain at least
     *                    one of: name, position, style, config, bundle
     * @return the updated {@code ProcessorEntity} as a map
     * @throws IllegalArgumentException if processorId is blank, updates is null,
     *                                  or updates contains no supported mutable fields
     */
    Map<String, Object> updateProcessor(String processorId, Map<String, Object> updates);

    /**
     * Validates that {@code state} is a recognized process-group schedule state:
     * {@code RUNNING}, {@code STOPPED}, {@code ENABLED}, or {@code DISABLED}.
     * Comparison is case-insensitive.
     *
     * @param state the state value to validate
     * @throws IllegalArgumentException if state is blank or not one of the recognized values
     */
    static String normalizeScheduleState(final String state) {
        if (state == null || state.isBlank()) {
            throw new IllegalArgumentException("Schedule state must not be blank");
        }
        final String normalized = state.toUpperCase(Locale.ROOT);
        if (!"RUNNING".equals(normalized) && !"STOPPED".equals(normalized)
                && !"ENABLED".equals(normalized) && !"DISABLED".equals(normalized)) {
            throw new IllegalArgumentException(
                    "Schedule state must be one of RUNNING, STOPPED, ENABLED, DISABLED; got: " + state);
        }
        return normalized;
    }

    /**
     * Normalizes a remote process group transmission state.
     * Accepted values are {@code TRANSMITTING} and {@code STOPPED} (case-insensitive).
     *
     * @param state the raw state string
     * @return the normalized (uppercase) state
     * @throws IllegalArgumentException if state is blank or not recognized
     */
    static String normalizeRemoteTransmissionState(final String state) {
        if (state == null || state.isBlank()) {
            throw new IllegalArgumentException("Transmission state must not be blank");
        }
        final String normalized = state.toUpperCase(Locale.ROOT);
        if (!"TRANSMITTING".equals(normalized) && !"STOPPED".equals(normalized)) {
            throw new IllegalArgumentException(
                    "Transmission state must be TRANSMITTING or STOPPED; got: " + state);
        }
        return normalized;
    }

    /**
     * Normalizes a port run status.
     * Accepted values are {@code RUNNING}, {@code STOPPED}, and {@code DISABLED} (case-insensitive).
     * {@code ENABLED} is intentionally not accepted; use the process-group schedule state for that.
     *
     * @param state the raw state string
     * @return the normalized (uppercase) state
     * @throws IllegalArgumentException if state is blank or not one of the accepted values
     */
    static String normalizePortRunStatus(final String state) {
        if (state == null || state.isBlank()) {
            throw new IllegalArgumentException("Port run status must not be blank");
        }
        final String normalized = state.toUpperCase(Locale.ROOT);
        if (!"RUNNING".equals(normalized) && !"STOPPED".equals(normalized) && !"DISABLED".equals(normalized)) {
            throw new IllegalArgumentException(
                    "Port run status must be one of RUNNING, STOPPED, DISABLED; got: " + state);
        }
        return normalized;
    }

    /**
     * Normalizes a diagnostic level string to upper-case.
     * Null or blank input is normalized to {@code BASIC}.
     * Only {@code BASIC} and {@code VERBOSE} are accepted (case-insensitive) for non-blank input.
     *
     * @param level the raw diagnostic level string; null or blank defaults to {@code BASIC}
     * @return the normalized (uppercase) diagnostic level
     * @throws IllegalArgumentException if level is non-blank but not {@code BASIC} or {@code VERBOSE}
     */
    static String normalizeDiagnosticLevel(final String level) {
        if (level == null || level.isBlank()) {
            return "BASIC";
        }
        final String normalized = level.toUpperCase(Locale.ROOT);
        if (!"BASIC".equals(normalized) && !"VERBOSE".equals(normalized)) {
            throw new IllegalArgumentException(
                    "diagnosticLevel must be BASIC or VERBOSE; got: " + level);
        }
        return normalized;
    }

    /**
     * Normalizes an access policy action string to lower-case.
     * Only {@code read} and {@code write} are accepted (case-insensitive).
     *
     * @param action the raw action string
     * @return the normalized (lower-case) action
     * @throws IllegalArgumentException if action is blank or not {@code read} or {@code write}
     */
    static String normalizeAccessPolicyAction(final String action) {
        if (action == null || action.isBlank()) {
            throw new IllegalArgumentException("Access policy action must not be blank");
        }
        final String normalized = action.toLowerCase(Locale.ROOT);
        if (!"read".equals(normalized) && !"write".equals(normalized)) {
            throw new IllegalArgumentException(
                    "Access policy action must be read or write; got: " + action);
        }
        return normalized;
    }

    /**
     * Normalizes a cluster node status string to upper-case.
     * Accepted values are {@code CONNECTING}, {@code DISCONNECTING}, and {@code OFFLOADING}
     * (case-insensitive), honouring all three transitional states that
     * {@code StandardNiFiServiceFacade.updateNode} explicitly accepts.
     *
     * @param status the raw status string
     * @return the normalized (uppercase) status
     * @throws IllegalArgumentException if status is blank or not one of the accepted values
     */
    static String normalizeClusterNodeStatus(final String status) {
        if (status == null || status.isBlank()) {
            throw new IllegalArgumentException("Cluster node status must not be blank");
        }
        final String normalized = status.toUpperCase(Locale.ROOT);
        if (!"CONNECTING".equals(normalized) && !"DISCONNECTING".equals(normalized)
                && !"OFFLOADING".equals(normalized)) {
            throw new IllegalArgumentException(
                    "Cluster node status must be one of CONNECTING, DISCONNECTING, OFFLOADING; got: " + status);
        }
        return normalized;
    }

    /**
     * Retrieves the connection with the given ID.
     * Maps to {@code GET /connections/{id}}.
     *
     * @param connectionId the connection ID; must not be blank
     * @return the complete connection entity as a map
     * @throws IllegalArgumentException if connectionId is blank
     */
    Map<String, Object> getConnection(String connectionId);

    /**
     * Updates the connection with the given ID, applying the specified update fields.
     * The current entity revision is fetched fresh on every retry attempt to prevent replaying
     * stale revisions. The caller-provided map is copied; {@code id} and {@code revision} keys
     * within the update map are ignored to prevent overriding the component identity or
     * optimistic-lock revision.
     *
     * @param connectionId the ID of the connection to update; must not be blank
     * @param updates      the fields to update on the component; must not be null or empty
     * @return the updated connection entity as a map
     * @throws IllegalArgumentException if connectionId is blank or updates is null/empty
     */
    Map<String, Object> updateConnection(String connectionId, Map<String, Object> updates);

    /**
     * Retrieves aggregate statistics for the connection with the given ID.
     * Maps to {@code GET /flow/connections/{id}/statistics} using default aggregate semantics
     * (nodewise=false, no clusterNodeId).
     *
     * @param connectionId the ID of the connection; must not be blank
     * @return the complete {@code ConnectionStatisticsEntity} as a map
     * @throws IllegalArgumentException if connectionId is blank
     */
    Map<String, Object> getConnectionStatistics(String connectionId);

    /**
     * Updates the controller service with the given ID, applying the specified update fields.
     * The current entity revision is fetched fresh on every retry attempt to prevent replaying
     * stale revisions. The caller-provided map is copied; {@code id} and {@code revision} keys
     * are ignored to prevent overriding the component identity or optimistic-lock revision.
     *
     * @param controllerServiceId the ID of the controller service to update; must not be blank
     * @param updates             the fields to update on the component; must not be null or empty
     * @return the updated controller service entity as a map
     * @throws IllegalArgumentException if controllerServiceId is blank or updates is null/empty
     */
    Map<String, Object> updateControllerService(String controllerServiceId, Map<String, Object> updates);

    /**
     * Retrieves all referencing components for the controller service with the given ID.
     * Maps to {@code GET /controller-services/{id}/references}.
     *
     * @param controllerServiceId the ID of the controller service; must not be blank
     * @return the complete {@code ControllerServiceReferencingComponentsEntity} as a map
     * @throws IllegalArgumentException if controllerServiceId is blank
     */
    Map<String, Object> getControllerServiceReferences(String controllerServiceId);

    /**
     * Updates the referencing components of the controller service with the given ID to the
     * specified state. Accepted states are {@code RUNNING}, {@code STOPPED}, {@code ENABLED},
     * and {@code DISABLED} (case-insensitive). On each attempt the current references and their
     * revisions are freshly read; stale revision state is never replayed.
     * <ul>
     *   <li>For {@code RUNNING}/{@code STOPPED}: revisions are collected for {@code Processor}
     *       and {@code ReportingTask} reference types.</li>
     *   <li>For {@code ENABLED}/{@code DISABLED}: revisions are collected for
     *       {@code ControllerService} reference types.</li>
     * </ul>
     * An empty referencing-component set is a valid no-op.
     *
     * @param controllerServiceId the ID of the controller service; must not be blank
     * @param state               the target state; must be RUNNING, STOPPED, ENABLED, or DISABLED
     * @return the updated {@code ControllerServiceReferencingComponentsEntity} as a map
     * @throws IllegalArgumentException if controllerServiceId is blank or state is unrecognized
     */
    Map<String, Object> updateControllerServiceReferences(String controllerServiceId, String state);

    /**
     * Retrieves the overall controller status.
     * Maps to {@code GET /flow/status}.
     *
     * @return the {@code ControllerStatusEntity} as a map
     */
    Map<String, Object> getFlowStatus();

    /**
     * Retrieves information about the currently authenticated user.
     * Maps to {@code GET /flow/current-user}.
     *
     * @return the {@code CurrentUserEntity} as a map
     */
    Map<String, Object> getCurrentUser();

    /**
     * Retrieves the bulletin board, filtered by the supplied non-null criteria.
     * Maps to {@code GET /flow/bulletin-board}. Only non-null parameters are appended to the
     * query string, URL-encoded where applicable.
     *
     * @param after      include only bulletins with an ID after this value; must be &gt;= 0 when
     *                   supplied; null means no filter
     * @param sourceName regex pattern matching bulletin source names; null means no filter
     * @param message    regex pattern matching bulletin messages; null means no filter
     * @param sourceId   regex pattern matching bulletin source IDs; null means no filter
     * @param groupId    regex pattern matching bulletin group IDs; null means no filter
     * @param limit      maximum number of bulletins to return; must be &gt;= 1 when supplied;
     *                   null means no limit
     * @return the {@code BulletinBoardEntity} as a map
     * @throws IllegalArgumentException if {@code after} &lt; 0 or {@code limit} &lt; 1 when supplied
     */
    Map<String, Object> getBulletinBoard(Long after, String sourceName, String message,
            String sourceId, String groupId, Integer limit);

    /**
     * Searches the NiFi canvas using the given query string and active process group context.
     * Maps to {@code GET /flow/search-results?q=...&a=...}, with values URL-encoded.
     * NiFi permits empty query strings; null arguments are normalized to empty strings rather
     * than rejected.
     *
     * @param query         the search query string; null is normalized to empty
     * @param activeGroupId the currently active process group ID for scoped searches;
     *                      null is normalized to empty
     * @return the {@code SearchResultsEntity} as a map
     */
    Map<String, Object> searchFlow(String query, String activeGroupId);

    /**
     * Retrieves build and version metadata about this NiFi instance.
     * Maps to {@code GET /flow/about}. Build metadata is sourced from the framework NAR bundle
     * when available. The URI and content-viewer URL fields are HTTP-request-derived and will be
     * absent in the embedded client.
     *
     * @return the {@code AboutEntity} as a map
     */
    Map<String, Object> getAboutInfo();

    /**
     * Recursively collects revision versions for all referencing components applicable to the given
     * normalized schedule state. Reference types are filtered as follows:
     * <ul>
     *   <li>{@code RUNNING}/{@code STOPPED}: collects {@code Processor} and
     *       {@code ReportingTask} reference types.</li>
     *   <li>{@code ENABLED}/{@code DISABLED}: collects {@code ControllerService} reference
     *       types.</li>
     * </ul>
     * Each entity map must carry an {@code "id"} string, a {@code "revision"} sub-map with a
     * {@code "version"} key, and a {@code "component"} sub-map with a {@code "referenceType"} key.
     * {@code ControllerService} components may also carry a {@code "referencingComponents"} nested
     * list that is traversed recursively. Components with {@code "referenceCycle": true} are
     * skipped to break reference cycles. Uses {@link #strictRevisionVersion(Object)} to validate
     * every collected version.
     *
     * @param referenceEntities list of referencing component entity maps as returned by
     *        {@code GET /controller-services/{id}/references} or equivalent facade method
     * @param normalizedState   the normalized target state (RUNNING, STOPPED, ENABLED, or DISABLED)
     * @return a map from component ID to revision version; never null, may be empty
     * @throws IllegalStateException if any applicable component is missing a valid revision version
     */
    static Map<String, Long> collectReferenceRevisions(
            final List<Map<String, Object>> referenceEntities,
            final String normalizedState) {
        final Map<String, Long> result = new HashMap<>();
        collectReferenceRevisionsInto(referenceEntities, normalizedState, result);
        return result;
    }

    private static void collectReferenceRevisionsInto(
            final List<?> entities,
            final String normalizedState,
            final Map<String, Long> target) {
        if (entities == null) {
            return;
        }
        final boolean forScheduled = "RUNNING".equals(normalizedState) || "STOPPED".equals(normalizedState);
        for (final Object obj : entities) {
            if (!(obj instanceof Map<?, ?> rawEntity)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            final Map<String, Object> entity = (Map<String, Object>) rawEntity;
            final Object componentObj = entity.get("component");
            if (!(componentObj instanceof Map<?, ?> rawComponent)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            final Map<String, Object> component = (Map<String, Object>) rawComponent;
            final String referenceType = String.valueOf(component.getOrDefault("referenceType", ""));
            final boolean isControllerService = "ControllerService".equals(referenceType);

            // Skip already-seen services to break reference cycles
            if (isControllerService && Boolean.TRUE.equals(component.get("referenceCycle"))) {
                continue;
            }

            if (forScheduled) {
                if ("Processor".equals(referenceType) || "ReportingTask".equals(referenceType)) {
                    final String id = String.valueOf(entity.get("id"));
                    final Object revObj = entity.get("revision");
                    if (!(revObj instanceof Map<?, ?> rawRev)) {
                        throw new IllegalStateException(
                                "Revision is missing for referencing component: " + id);
                    }
                    target.put(id, strictRevisionVersion(((Map<?, ?>) rawRev).get("version")));
                }
            } else if (isControllerService) {
                final String id = String.valueOf(entity.get("id"));
                final Object revObj = entity.get("revision");
                if (!(revObj instanceof Map<?, ?> rawRev)) {
                    throw new IllegalStateException(
                            "Revision is missing for referencing component: " + id);
                }
                target.put(id, strictRevisionVersion(((Map<?, ?>) rawRev).get("version")));
            }

            // Recurse into nested references carried by controller service components
            if (isControllerService) {
                final Object nestedObj = component.get("referencingComponents");
                if (nestedObj instanceof List<?> nestedList) {
                    collectReferenceRevisionsInto(nestedList, normalizedState, target);
                }
            }
        }
    }

    /**
     * Creates a remote process group in the process group with the given ID.
     * Maps to {@code POST /process-groups/{id}/remote-process-groups} with revision 0.
     * The optional {@code configuration} map may supply additional component fields;
     * {@code id}, {@code revision}, and {@code parentGroupId} keys are stripped and
     * the explicit {@code targetUri} and position are always forced from arguments.
     *
     * @param processGroupId the parent process group ID; must not be blank
     * @param targetUri      the target URI of the remote NiFi instance; must not be blank
     * @param x              X coordinate on the canvas
     * @param y              Y coordinate on the canvas
     * @param configuration  optional extra component fields; null is treated as empty
     * @return the complete remote process group entity as a map
     * @throws IllegalArgumentException if processGroupId or targetUri is blank
     */
    Map<String, Object> createRemoteProcessGroup(String processGroupId, String targetUri, double x, double y,
            Map<String, Object> configuration);

    /**
     * Retrieves the remote process group with the given ID.
     * Maps to {@code GET /remote-process-groups/{id}}.
     *
     * @param id the remote process group ID; must not be blank
     * @return the complete remote process group entity as a map
     * @throws IllegalArgumentException if id is blank
     */
    Map<String, Object> getRemoteProcessGroup(String id);

    /**
     * Updates the remote process group with the given ID, applying the specified update fields.
     * The current entity revision is fetched fresh on every retry attempt. The caller-provided
     * map is copied; {@code id} and {@code revision} keys are ignored to prevent overriding the
     * component identity or optimistic-lock revision.
     *
     * @param id      the remote process group ID; must not be blank
     * @param updates the fields to update; must not be null or empty
     * @return the updated remote process group entity as a map
     * @throws IllegalArgumentException if id is blank or updates is null/empty
     */
    Map<String, Object> updateRemoteProcessGroup(String id, Map<String, Object> updates);

    /**
     * Sets the transmission state of the remote process group with the given ID.
     * Maps to {@code PUT /remote-process-groups/{id}/run-status}.
     * Accepted states are {@code TRANSMITTING} and {@code STOPPED} (case-insensitive).
     *
     * @param id    the remote process group ID; must not be blank
     * @param state the target transmission state; must be TRANSMITTING or STOPPED
     * @return the updated remote process group entity as a map
     * @throws IllegalArgumentException if id is blank or state is not recognized
     */
    Map<String, Object> setRemoteProcessGroupTransmission(String id, String state);

    /**
     * Deletes the remote process group with the given ID.
     * Stops transmission first (failure is propagated, not swallowed).
     * Maps to {@code DELETE /remote-process-groups/{id}}.
     *
     * @param id the remote process group ID; must not be blank
     * @throws IllegalArgumentException if id is blank
     */
    void deleteRemoteProcessGroup(String id);

    /**
     * Creates an input port in the process group with the given ID.
     * Maps to {@code POST /process-groups/{id}/input-ports} with revision 0.
     *
     * @param processGroupId the parent process group ID; must not be blank
     * @param name           the port name; must not be blank
     * @param x              X coordinate on the canvas
     * @param y              Y coordinate on the canvas
     * @return the complete input port entity as a map
     * @throws IllegalArgumentException if processGroupId or name is blank
     */
    Map<String, Object> createInputPort(String processGroupId, String name, double x, double y);

    /**
     * Retrieves the input port with the given ID.
     * Maps to {@code GET /input-ports/{id}}.
     *
     * @param portId the input port ID; must not be blank
     * @return the complete input port entity as a map
     * @throws IllegalArgumentException if portId is blank
     */
    Map<String, Object> getInputPort(String portId);

    /**
     * Sets the run status of the input port with the given ID.
     * Maps to {@code PUT /input-ports/{id}/run-status}.
     * Accepted states are {@code RUNNING}, {@code STOPPED}, and {@code DISABLED} (case-insensitive).
     *
     * @param portId the input port ID; must not be blank
     * @param state  the target run state; must be RUNNING, STOPPED, or DISABLED
     * @return the updated input port entity as a map
     * @throws IllegalArgumentException if portId is blank or state is not recognized
     */
    Map<String, Object> setInputPortRunStatus(String portId, String state);

    /**
     * Deletes the input port with the given ID.
     * Maps to {@code DELETE /input-ports/{id}}.
     *
     * @param portId the input port ID; must not be blank
     * @throws IllegalArgumentException if portId is blank
     */
    void deleteInputPort(String portId);

    /**
     * Creates an output port in the process group with the given ID.
     * Maps to {@code POST /process-groups/{id}/output-ports} with revision 0.
     *
     * @param processGroupId the parent process group ID; must not be blank
     * @param name           the port name; must not be blank
     * @param x              X coordinate on the canvas
     * @param y              Y coordinate on the canvas
     * @return the complete output port entity as a map
     * @throws IllegalArgumentException if processGroupId or name is blank
     */
    Map<String, Object> createOutputPort(String processGroupId, String name, double x, double y);

    /**
     * Retrieves the output port with the given ID.
     * Maps to {@code GET /output-ports/{id}}.
     *
     * @param portId the output port ID; must not be blank
     * @return the complete output port entity as a map
     * @throws IllegalArgumentException if portId is blank
     */
    Map<String, Object> getOutputPort(String portId);

    /**
     * Sets the run status of the output port with the given ID.
     * Maps to {@code PUT /output-ports/{id}/run-status}.
     * Accepted states are {@code RUNNING}, {@code STOPPED}, and {@code DISABLED} (case-insensitive).
     *
     * @param portId the output port ID; must not be blank
     * @param state  the target run state; must be RUNNING, STOPPED, or DISABLED
     * @return the updated output port entity as a map
     * @throws IllegalArgumentException if portId is blank or state is not recognized
     */
    Map<String, Object> setOutputPortRunStatus(String portId, String state);

    /**
     * Updates the input port with the given ID, applying the specified update fields.
     * The current entity revision is fetched fresh on every retry attempt.
     * The caller-provided map is copied; {@code id} and {@code revision} keys are ignored
     * to prevent overriding the component identity or optimistic-lock revision.
     * Maps to {@code PUT /input-ports/{id}}.
     *
     * @param portId  the input port ID; must not be blank
     * @param updates the fields to update on the component; must not be null or empty
     * @return the updated input port entity as a map
     * @throws IllegalArgumentException if portId is blank or updates is null/empty
     */
    Map<String, Object> updateInputPort(String portId, Map<String, Object> updates);

    /**
     * Deletes the output port with the given ID.
     * Maps to {@code DELETE /output-ports/{id}}.
     *
     * @param portId the output port ID; must not be blank
     * @throws IllegalArgumentException if portId is blank
     */
    void deleteOutputPort(String portId);

    /**
     * Updates the output port with the given ID, applying the specified update fields.
     * The current entity revision is fetched fresh on every retry attempt.
     * The caller-provided map is copied; {@code id} and {@code revision} keys are ignored
     * to prevent overriding the component identity or optimistic-lock revision.
     * Maps to {@code PUT /output-ports/{id}}.
     *
     * @param portId  the output port ID; must not be blank
     * @param updates the fields to update on the component; must not be null or empty
     * @return the updated output port entity as a map
     * @throws IllegalArgumentException if portId is blank or updates is null/empty
     */
    Map<String, Object> updateOutputPort(String portId, Map<String, Object> updates);

    /**
     * Creates a label in the process group with the given ID.
     * Maps to {@code POST /process-groups/{id}/labels} with revision 0.
     * If {@code style} is null it is treated as an empty map.
     * {@code width} and {@code height} are optional; null values are omitted from the DTO.
     *
     * @param processGroupId the parent process group ID; must not be blank
     * @param text           the label text; must not be null
     * @param x              X coordinate on the canvas
     * @param y              Y coordinate on the canvas
     * @param style          optional style map (e.g. font-size, background-color); null = empty
     * @param width          optional label width in pixels; null means no explicit width
     * @param height         optional label height in pixels; null means no explicit height
     * @return the complete label entity as a map
     * @throws IllegalArgumentException if processGroupId is blank or text is null
     */
    Map<String, Object> createLabel(String processGroupId, String text, double x, double y,
            Map<String, String> style, Double width, Double height);

    /**
     * Retrieves the label with the given ID.
     * Maps to {@code GET /labels/{id}}.
     *
     * @param labelId the label ID; must not be blank
     * @return the complete label entity as a map
     * @throws IllegalArgumentException if labelId is blank
     */
    Map<String, Object> getLabel(String labelId);

    /**
     * Updates the label with the given ID, applying the specified update fields.
     * The current entity revision is fetched fresh on every retry attempt. The caller-provided
     * map is copied; {@code id} and {@code revision} keys are ignored.
     *
     * @param labelId the label ID; must not be blank
     * @param updates the fields to update; must not be null or empty
     * @return the updated label entity as a map
     * @throws IllegalArgumentException if labelId is blank or updates is null/empty
     */
    Map<String, Object> updateLabel(String labelId, Map<String, Object> updates);

    /**
     * Deletes the label with the given ID.
     * Maps to {@code DELETE /labels/{id}}.
     *
     * @param labelId the label ID; must not be blank
     * @throws IllegalArgumentException if labelId is blank
     */
    void deleteLabel(String labelId);

    /**
     * Creates a funnel in the process group with the given ID.
     * Maps to {@code POST /process-groups/{id}/funnels} with revision 0.
     *
     * @param processGroupId the parent process group ID; must not be blank
     * @param x              X coordinate on the canvas
     * @param y              Y coordinate on the canvas
     * @return the complete funnel entity as a map
     * @throws IllegalArgumentException if processGroupId is blank
     */
    Map<String, Object> createFunnel(String processGroupId, double x, double y);

    /**
     * Retrieves the funnel with the given ID.
     * Maps to {@code GET /funnels/{id}}.
     *
     * @param funnelId the funnel ID; must not be blank
     * @return the complete funnel entity as a map
     * @throws IllegalArgumentException if funnelId is blank
     */
    Map<String, Object> getFunnel(String funnelId);

    /**
     * Updates the funnel with the given ID, applying the specified update fields.
     * The current entity revision is fetched fresh on every retry attempt. The caller-provided
     * map is copied; {@code id} and {@code revision} keys are ignored.
     *
     * @param funnelId the funnel ID; must not be blank
     * @param updates  the fields to update; must not be null or empty
     * @return the updated funnel entity as a map
     * @throws IllegalArgumentException if funnelId is blank or updates is null/empty
     */
    Map<String, Object> updateFunnel(String funnelId, Map<String, Object> updates);

    /**
     * Deletes the funnel with the given ID.
     * Maps to {@code DELETE /funnels/{id}}.
     * Calls verifyDeleteFunnel before deletion (internal) or just fetches revision (HTTP).
     *
     * @param funnelId the funnel ID; must not be blank
     * @throws IllegalArgumentException if funnelId is blank
     */
    void deleteFunnel(String funnelId);

    /**
     * Creates a snippet for the specified components within the parent process group.
     * Maps to {@code POST /snippets}. The {@code componentSelections} map must contain
     * at least one entry; allowed keys are: {@code processGroups}, {@code remoteProcessGroups},
     * {@code processors}, {@code inputPorts}, {@code outputPorts}, {@code connections},
     * {@code labels}, {@code funnels}. The parent group ID is always forced from the explicit
     * argument; {@code id}, {@code uri}, and {@code parentGroupId} keys in the selections are
     * stripped.
     *
     * @param parentProcessGroupId the ID of the process group containing the components; must not be blank
     * @param componentSelections  a map of component-type key to component-id map; must not be null or empty
     * @return the created snippet entity as a map
     * @throws IllegalArgumentException if parentProcessGroupId is blank or componentSelections is null/empty
     */
    Map<String, Object> createSnippet(String parentProcessGroupId, Map<String, Object> componentSelections);

    /**
     * Moves the snippet with the given ID to the destination process group.
     * Maps to {@code PUT /snippets/{id}}; the server gathers component revisions.
     *
     * @param snippetId                 the snippet ID; must not be blank
     * @param destinationProcessGroupId the destination process group ID; must not be blank
     * @return the updated snippet entity as a map
     * @throws IllegalArgumentException if snippetId or destinationProcessGroupId is blank
     */
    Map<String, Object> moveSnippet(String snippetId, String destinationProcessGroupId);

    /**
     * Copies the snippet into the destination process group at the given origin.
     * Maps to {@code POST /process-groups/{destinationId}/snippet-instance}.
     *
     * @param snippetId                 the snippet ID to copy; must not be blank
     * @param destinationProcessGroupId the destination process group ID; must not be blank
     * @param originX                   X coordinate for the copied components on the canvas
     * @param originY                   Y coordinate for the copied components on the canvas
     * @return the complete FlowEntity map for the copied flow fragment
     * @throws IllegalArgumentException if snippetId or destinationProcessGroupId is blank
     */
    Map<String, Object> copySnippet(String snippetId, String destinationProcessGroupId, double originX, double originY);

    /**
     * Deletes the snippet with the given ID and all its contained components.
     * Maps to {@code DELETE /snippets/{id}}; the server gathers component revisions.
     *
     * @param snippetId the snippet ID; must not be blank
     * @return the deleted snippet entity as a map
     * @throws IllegalArgumentException if snippetId is blank
     */
    Map<String, Object> deleteSnippet(String snippetId);

    // =========================================================================
    // FlowFile queue operations
    // =========================================================================

    /**
     * Submits a request to list the FlowFiles in the queue of the specified connection.
     * Maps to {@code POST /flowfile-queues/{connectionId}/listing-requests}.
     * The returned map wraps a {@code ListingRequestEntity} whose nested
     * {@code listingRequest.id} is the request ID for subsequent poll and cleanup calls.
     *
     * @param connectionId the connection ID; must not be blank
     * @return the initial {@code ListingRequestEntity} map
     * @throws IllegalArgumentException if connectionId is blank
     */
    Map<String, Object> submitFlowFileListingRequest(String connectionId);

    /**
     * Polls the status of an outstanding FlowFile listing request.
     * Maps to {@code GET /flowfile-queues/{connectionId}/listing-requests/{requestId}}.
     *
     * @param connectionId the connection ID; must not be blank
     * @param requestId    the listing request ID from {@link #submitFlowFileListingRequest}; must not be blank
     * @return the current {@code ListingRequestEntity} map
     * @throws IllegalArgumentException if connectionId or requestId is blank
     */
    Map<String, Object> getFlowFileListingRequest(String connectionId, String requestId);

    /**
     * Deletes a completed FlowFile listing request.
     * Maps to {@code DELETE /flowfile-queues/{connectionId}/listing-requests/{requestId}}.
     *
     * @param connectionId the connection ID; must not be blank
     * @param requestId    the listing request ID; must not be blank
     * @return the terminal {@code ListingRequestEntity} map
     * @throws IllegalArgumentException if connectionId or requestId is blank
     */
    Map<String, Object> deleteFlowFileListingRequest(String connectionId, String requestId);

    /**
     * Lists the FlowFiles in the specified connection's queue by driving the full asynchronous
     * listing lifecycle: submit → poll until {@code listingRequest.finished} is {@code true}
     * and {@code failureReason} is blank → cleanup unconditionally.
     * Returns the terminal poll status map containing the full {@code ListingRequestEntity}.
     *
     * @param connectionId the connection ID; must not be blank
     * @return the terminal {@code ListingRequestEntity} map from the last poll
     * @throws IllegalArgumentException if connectionId is blank
     * @throws NiFiClientException      if the listing fails, times out, or is interrupted
     */
    Map<String, Object> listFlowFiles(String connectionId);

    /**
     * Retrieves the details of a specific FlowFile from the connection's queue.
     * Maps to {@code GET /flowfile-queues/{connectionId}/flowfiles/{flowFileUuid}}.
     * Returns the complete {@code FlowFileEntity} map.
     *
     * @param connectionId  the connection ID; must not be blank
     * @param flowFileUuid  the FlowFile UUID; must not be blank
     * @return the complete {@code FlowFileEntity} map
     * @throws IllegalArgumentException if connectionId or flowFileUuid is blank
     */
    Map<String, Object> getFlowFileDetails(String connectionId, String flowFileUuid);

    /**
     * Downloads the content of a specific FlowFile as a raw byte stream.
     * Maps to {@code GET /flowfile-queues/{connectionId}/flowfiles/{flowFileUuid}/content}.
     * <p>The caller is responsible for closing the returned {@link java.io.InputStream}.
     * No Range support is provided; the full content is streamed.
     *
     * @param connectionId  the connection ID; must not be blank
     * @param flowFileUuid  the FlowFile UUID; must not be blank
     * @return an open {@link java.io.InputStream} over the FlowFile's content; never {@code null}
     * @throws IllegalArgumentException if connectionId or flowFileUuid is blank
     * @throws NiFiClientException      on transport or HTTP error
     */
    java.io.InputStream downloadFlowFileContent(String connectionId, String flowFileUuid);

    /**
     * Submits a request to drop all FlowFiles from the specified connection's queue.
     * Maps to {@code POST /flowfile-queues/{connectionId}/drop-requests}.
     * <p>The {@code confirmed} parameter must be {@code true}; passing {@code false} is an
     * explicit destructive guard that causes an {@link IllegalArgumentException} to be thrown
     * before any network or facade call is made.
     * The returned map wraps a {@code DropRequestEntity} whose nested
     * {@code dropRequest.id} is the request ID for subsequent poll and cleanup calls.
     *
     * @param connectionId the connection ID; must not be blank
     * @param confirmed    must be {@code true} to proceed; {@code false} throws {@link IllegalArgumentException}
     * @return the initial {@code DropRequestEntity} map
     * @throws IllegalArgumentException if connectionId is blank or confirmed is {@code false}
     */
    Map<String, Object> submitQueueDropRequest(String connectionId, boolean confirmed);

    /**
     * Polls the status of an outstanding queue drop request.
     * Maps to {@code GET /flowfile-queues/{connectionId}/drop-requests/{requestId}}.
     *
     * @param connectionId the connection ID; must not be blank
     * @param requestId    the drop request ID from {@link #submitQueueDropRequest}; must not be blank
     * @return the current {@code DropRequestEntity} map
     * @throws IllegalArgumentException if connectionId or requestId is blank
     */
    Map<String, Object> getQueueDropRequest(String connectionId, String requestId);

    /**
     * Deletes a completed queue drop request.
     * Maps to {@code DELETE /flowfile-queues/{connectionId}/drop-requests/{requestId}}.
     *
     * @param connectionId the connection ID; must not be blank
     * @param requestId    the drop request ID; must not be blank
     * @return the terminal {@code DropRequestEntity} map
     * @throws IllegalArgumentException if connectionId or requestId is blank
     */
    Map<String, Object> deleteQueueDropRequest(String connectionId, String requestId);

    /**
     * Drops all FlowFiles from the specified connection's queue by driving the full asynchronous
     * drop lifecycle: submit → poll until {@code dropRequest.finished} is {@code true}
     * and {@code failureReason} is blank → cleanup unconditionally.
     * <p>The {@code confirmed} parameter must be {@code true}; passing {@code false} throws
     * {@link IllegalArgumentException} before any operation is initiated.
     *
     * @param connectionId the connection ID; must not be blank
     * @param confirmed    must be {@code true} to proceed
     * @return the terminal {@code DropRequestEntity} map from the last poll
     * @throws IllegalArgumentException if connectionId is blank or confirmed is {@code false}
     * @throws NiFiClientException      if the drop fails, times out, or is interrupted
     */
    Map<String, Object> dropFlowFileQueue(String connectionId, boolean confirmed);

    // =========================================================================
    // Provenance operations
    // =========================================================================

    /**
     * Submits a provenance query to NiFi.
     * Maps to {@code POST /provenance}. The HTTP request body is
     * {@code {"provenance":{"request": request}}}; the internal facade invokes the
     * authorised {@code ProvenanceResource} endpoint directly.
     * <p>An empty map is accepted and queries all provenance events (server defaults apply).
     *
     * @param request the contents of a {@code ProvenanceRequestDTO}; may be empty but must not be null.
     *        Supported fields: {@code searchTerms}, {@code startDate}, {@code endDate},
     *        {@code minimumFileSize}, {@code maximumFileSize}, {@code maxResults},
     *        {@code clusterNodeId}, {@code summarize}, {@code incrementalResults}.
     * @return the initial {@code ProvenanceEntity} map
     * @throws IllegalArgumentException if request is null
     */
    Map<String, Object> submitProvenanceQuery(Map<String, Object> request);

    /**
     * Polls the current status of a previously submitted provenance query.
     * Maps to {@code GET /provenance/{id}}.
     * The GET helper provides transient-error retry.
     *
     * @param queryId          the provenance query ID returned by {@link #submitProvenanceQuery}; must not be blank
     * @param clusterNodeId    the cluster node ID to poll; null or blank means all nodes
     * @param summarize        whether to summarize the provenance events returned
     * @param incrementalResults whether to return results before the query completes
     * @return the current {@code ProvenanceEntity} map
     * @throws IllegalArgumentException if queryId is blank
     */
    Map<String, Object> getProvenanceQuery(String queryId, String clusterNodeId, boolean summarize,
            boolean incrementalResults);

    /**
     * Deletes a completed or cancelled provenance query.
     * Maps to {@code DELETE /provenance/{id}}.
     *
     * @param queryId       the provenance query ID; must not be blank
     * @param clusterNodeId the cluster node ID where the query lives; null or blank means all nodes
     * @return the terminal {@code ProvenanceEntity} map
     * @throws IllegalArgumentException if queryId is blank
     */
    Map<String, Object> deleteProvenanceQuery(String queryId, String clusterNodeId);

    /**
     * Executes a provenance query through the full asynchronous lifecycle via
     * {@link NiFiAsyncRequestExecutor}: submit → poll until {@code provenance.finished} is
     * {@code true} → cleanup unconditionally.
     * <p>The following optional fields are extracted from {@code request} to control routing and
     * display:
     * <ul>
     *   <li>{@code clusterNodeId} – nonblank string routes to that cluster node (optional)</li>
     *   <li>{@code summarize} – boolean, defaults to {@code false}</li>
     *   <li>{@code incrementalResults} – boolean, defaults to {@code true}</li>
     * </ul>
     * {@code results.errors} in the poll response is treated as result data, not a lifecycle
     * failure (cluster queries return partial node errors there); {@code isFailure} always
     * returns {@code false}.
     *
     * @param request the {@code ProvenanceRequestDTO} contents; may be empty but must not be null
     * @return the terminal {@code ProvenanceEntity} map from the last poll
     * @throws IllegalArgumentException if request is null
     * @throws NiFiClientException      if the query times out or is interrupted
     */
    Map<String, Object> queryProvenance(Map<String, Object> request);

    /**
     * Submits a lineage query to NiFi.
     * Maps to {@code POST /provenance/lineage}. The HTTP request body is
     * {@code {"lineage":{"request": request}}}; the internal facade invokes the authorised
     * {@code ProvenanceResource} endpoint directly.
     * <p>Official {@code LineageRequestDTO} fields: {@code lineageRequestType} (PARENTS, CHILDREN,
     * FLOWFILE), {@code eventId}, {@code uuid}, {@code clusterNodeId}. The server validates
     * exact field combinations.
     *
     * @param request the contents of a {@code LineageRequestDTO}; must not be null or empty
     * @return the initial {@code LineageEntity} map
     * @throws IllegalArgumentException if request is null or empty
     */
    Map<String, Object> submitLineageQuery(Map<String, Object> request);

    /**
     * Polls the current status of a previously submitted lineage query.
     * Maps to {@code GET /provenance/lineage/{id}}.
     * The GET helper provides transient-error retry.
     *
     * @param lineageId     the lineage query ID returned by {@link #submitLineageQuery}; must not be blank
     * @param clusterNodeId the cluster node ID; required for clustered lineage, null or blank for standalone
     * @return the current {@code LineageEntity} map
     * @throws IllegalArgumentException if lineageId is blank
     */
    Map<String, Object> getLineageQuery(String lineageId, String clusterNodeId);

    /**
     * Deletes a completed or cancelled lineage query.
     * Maps to {@code DELETE /provenance/lineage/{id}}.
     *
     * @param lineageId     the lineage query ID; must not be blank
     * @param clusterNodeId the cluster node ID; required for clustered lineage, null or blank for standalone
     * @return the terminal {@code LineageEntity} map
     * @throws IllegalArgumentException if lineageId is blank
     */
    Map<String, Object> deleteLineageQuery(String lineageId, String clusterNodeId);

    /**
     * Executes a lineage query through the full asynchronous lifecycle via
     * {@link NiFiAsyncRequestExecutor}: submit → poll until {@code lineage.finished} is
     * {@code true} → cleanup unconditionally.
     * <p>The optional {@code clusterNodeId} field in the request map is extracted and used for
     * all poll and cleanup calls. Official clustered lineage requires it; standalone permits null.
     * {@code isFailure} always returns {@code false} for lineage queries.
     *
     * @param request the {@code LineageRequestDTO} contents; must not be null or empty
     * @return the terminal {@code LineageEntity} map from the last poll
     * @throws IllegalArgumentException if request is null or empty
     * @throws NiFiClientException      if the query times out or is interrupted
     */
    Map<String, Object> queryLineage(Map<String, Object> request);

    // =========================================================================
    // Registry, versioning, and process-group import/export operations
    // =========================================================================

    /**
     * Lists all configured NiFi Registry clients.
     * Maps to {@code GET /controller/registry-clients} and returns the nested
     * {@code registries} collection as entity maps.
     *
     * @return list of flow registry client entity maps
     */
    List<Map<String, Object>> listRegistryClients();

    /**
     * Creates a new NiFi Registry client.
     * Maps to {@code POST /controller/registry-clients} with revision version {@code 0}.
     * The provided component map is copied and sanitized so caller-supplied {@code id}
     * and {@code revision} fields are ignored.
     *
     * @param component the registry client component fields; must not be null or empty
     * @return the created {@code FlowRegistryClientEntity} as a map
     * @throws IllegalArgumentException if component is null or empty
     */
    Map<String, Object> createRegistryClient(Map<String, Object> component);

    /**
     * Retrieves a specific NiFi Registry client.
     * Maps to {@code GET /controller/registry-clients/{id}}.
     *
     * @param id the registry client ID; must not be blank
     * @return the {@code FlowRegistryClientEntity} as a map
     * @throws IllegalArgumentException if id is blank
     */
    Map<String, Object> getRegistryClient(String id);

    /**
     * Updates a NiFi Registry client using optimistic revision control.
     * The latest entity revision is re-read for each retry attempt. The caller-provided
     * update map is copied and sanitized so {@code revision} is ignored and {@code id}
     * is forced to the requested resource ID.
     *
     * @param id      the registry client ID; must not be blank
     * @param updates the update fields; must not be null or empty
     * @return the updated {@code FlowRegistryClientEntity} as a map
     * @throws IllegalArgumentException if id is blank or updates is null/empty
     */
    Map<String, Object> updateRegistryClient(String id, Map<String, Object> updates);

    /**
     * Deletes a NiFi Registry client using optimistic revision control.
     * The current revision is fetched immediately before deletion.
     *
     * @param id the registry client ID; must not be blank
     * @return the deleted {@code FlowRegistryClientEntity} response as a map
     * @throws IllegalArgumentException if id is blank
     */
    Map<String, Object> deleteRegistryClient(String id);

    /**
     * Lists buckets available from the specified registry client and optional branch.
     * Maps to {@code GET /flow/registries/{registryId}/buckets} and returns the nested
     * {@code buckets} collection.
     *
     * @param registryId the registry client ID; must not be blank
     * @param branch     optional branch name; null or blank uses the registry default branch
     * @return list of bucket entity maps
     * @throws IllegalArgumentException if registryId is blank
     */
    List<Map<String, Object>> listRegistryBuckets(String registryId, String branch);

    /**
     * Lists flows available from the specified registry bucket and optional branch.
     * Maps to {@code GET /flow/registries/{registryId}/buckets/{bucketId}/flows} and
     * returns the nested {@code versionedFlows} collection.
     *
     * @param registryId the registry client ID; must not be blank
     * @param bucketId   the bucket ID; must not be blank
     * @param branch     optional branch name; null or blank uses the registry default branch
     * @return list of versioned flow entity maps
     * @throws IllegalArgumentException if registryId or bucketId is blank
     */
    List<Map<String, Object>> listRegistryFlows(String registryId, String bucketId, String branch);

    /**
     * Retrieves version control information for a process group.
     * Maps to {@code GET /versions/process-groups/{id}}.
     *
     * @param processGroupId the process group ID; must not be blank
     * @return the {@code VersionControlInformationEntity} as a map
     * @throws IllegalArgumentException if processGroupId is blank
     */
    Map<String, Object> getVersionInformation(String processGroupId);

    /**
     * Acquires an active version-control request slot for a process group.
     * Maps to {@code POST /versions/active-requests} and returns the plain-text request ID.
     * This operation is not retried as a revision-bearing mutation.
     *
     * @param processGroupId the process group ID; must not be blank
     * @return the created active request ID
     * @throws IllegalArgumentException if processGroupId is blank
     */
    String acquireVersionRequest(String processGroupId);

    /**
     * Updates an existing active version-control request with selected flow coordinates and
     * component mapping information. The current process-group revision is re-read before
     * each attempt and the supplied version-control information is copied with
     * {@code groupId} forced to {@code processGroupId}.
     *
     * @param requestId                 the active request ID; must not be blank
     * @param processGroupId            the target process group ID; must not be blank
     * @param versionControlInformation the version-control information fields; must not be null
     * @param componentMapping          the versioned-component-to-instance mapping; must not be null
     * @return the updated {@code VersionControlInformationEntity} as a map
     * @throws IllegalArgumentException if requestId/processGroupId is blank or either map is null
     */
    Map<String, Object> updateVersionRequestMapping(String requestId, String processGroupId,
            Map<String, Object> versionControlInformation, Map<String, Object> componentMapping);

    /**
     * Releases an active version-control request.
     * Maps to {@code DELETE /versions/active-requests/{id}}.
     * Successful empty responses are returned as {@link Map#of()}.
     *
     * @param requestId the active request ID; must not be blank
     * @return the delete response as a map, or an empty map for empty success bodies
     * @throws IllegalArgumentException if requestId is blank
     */
    Map<String, Object> releaseVersionRequest(String requestId);

    /**
     * Starts version control for a process group or commits local changes to the registry.
     * The current process-group revision is re-read for each retry attempt and the supplied
     * {@code versionedFlow} map is copied before submission.
     *
     * @param processGroupId the process group ID; must not be blank
     * @param versionedFlow  the versioned-flow coordinates/details; must not be null or empty
     * @return the resulting {@code VersionControlInformationEntity} as a map
     * @throws IllegalArgumentException if processGroupId is blank or versionedFlow is null/empty
     */
    Map<String, Object> startVersionControl(String processGroupId, Map<String, Object> versionedFlow);

    /**
     * Submits an asynchronous version-update request for a versioned process group.
     * Maps to {@code POST /versions/update-requests/process-groups/{id}} and retries on
     * revision conflicts after re-reading the current process-group revision.
     *
     * @param processGroupId            the process group ID; must not be blank
     * @param versionControlInformation the version-control information fields; must not be null
     * @return the initial {@code VersionedFlowUpdateRequestEntity} as a map
     * @throws IllegalArgumentException if processGroupId is blank or versionControlInformation is null
     */
    Map<String, Object> submitVersionUpdate(String processGroupId, Map<String, Object> versionControlInformation);

    /**
     * Retrieves the current status of an asynchronous version-update request.
     * Maps to {@code GET /versions/update-requests/{id}}.
     *
     * @param requestId the update request ID; must not be blank
     * @return the current {@code VersionedFlowUpdateRequestEntity} as a map
     * @throws IllegalArgumentException if requestId is blank
     */
    Map<String, Object> getVersionUpdateRequest(String requestId);

    /**
     * Deletes an asynchronous version-update request.
     * Maps to {@code DELETE /versions/update-requests/{id}}.
     *
     * @param requestId the update request ID; must not be blank
     * @return the delete response as a map
     * @throws IllegalArgumentException if requestId is blank
     */
    Map<String, Object> deleteVersionUpdateRequest(String requestId);

    /**
     * Executes the full asynchronous lifecycle for updating a versioned process group via
     * {@link NiFiAsyncRequestExecutor}: submit → poll until the nested {@code request.complete}
     * is {@code true} with blank {@code request.failureReason} → cleanup unconditionally.
     *
     * @param processGroupId            the process group ID; must not be blank
     * @param versionControlInformation the version-control information fields; must not be null
     * @return the terminal {@code VersionedFlowUpdateRequestEntity} map from the last poll
     * @throws IllegalArgumentException if processGroupId is blank or versionControlInformation is null
     * @throws NiFiClientException      if the update fails, times out, or is interrupted
     */
    Map<String, Object> updateVersionedProcessGroup(String processGroupId,
            Map<String, Object> versionControlInformation);

    /**
     * Submits an asynchronous revert request for a versioned process group.
     * Maps to {@code POST /versions/revert-requests/process-groups/{id}} and retries on
     * revision conflicts after re-reading the current process-group revision.
     *
     * @param processGroupId            the process group ID; must not be blank
     * @param versionControlInformation the version-control information fields; must not be null
     * @return the initial {@code VersionedFlowUpdateRequestEntity} as a map
     * @throws IllegalArgumentException if processGroupId is blank or versionControlInformation is null
     */
    Map<String, Object> submitVersionRevert(String processGroupId, Map<String, Object> versionControlInformation);

    /**
     * Retrieves the current status of an asynchronous version-revert request.
     * Maps to {@code GET /versions/revert-requests/{id}}.
     *
     * @param requestId the revert request ID; must not be blank
     * @return the current {@code VersionedFlowUpdateRequestEntity} as a map
     * @throws IllegalArgumentException if requestId is blank
     */
    Map<String, Object> getVersionRevertRequest(String requestId);

    /**
     * Deletes an asynchronous version-revert request.
     * Maps to {@code DELETE /versions/revert-requests/{id}}.
     *
     * @param requestId the revert request ID; must not be blank
     * @return the delete response as a map
     * @throws IllegalArgumentException if requestId is blank
     */
    Map<String, Object> deleteVersionRevertRequest(String requestId);

    /**
     * Executes the full asynchronous lifecycle for reverting a versioned process group via
     * {@link NiFiAsyncRequestExecutor}: submit → poll until the nested {@code request.complete}
     * is {@code true} with blank {@code request.failureReason} → cleanup unconditionally.
     *
     * @param processGroupId            the process group ID; must not be blank
     * @param versionControlInformation the version-control information fields; must not be null
     * @return the terminal {@code VersionedFlowUpdateRequestEntity} map from the last poll
     * @throws IllegalArgumentException if processGroupId is blank or versionControlInformation is null
     * @throws NiFiClientException      if the revert fails, times out, or is interrupted
     */
    Map<String, Object> revertVersionedProcessGroup(String processGroupId,
            Map<String, Object> versionControlInformation);

    /**
     * Exports a process group as a JSON flow snapshot.
     * Maps to {@code GET /process-groups/{id}/download} with query flags controlling whether
     * referenced controller services and component state are included.
     *
     * @param processGroupId            the process group ID; must not be blank
     * @param includeReferencedServices whether referenced controller services outside the group are included
     * @param includeComponentState     whether component state is included
     * @return the exported {@code RegisteredFlowSnapshot} payload as a map
     * @throws IllegalArgumentException if processGroupId is blank
     */
    Map<String, Object> exportProcessGroup(String processGroupId, boolean includeReferencedServices,
            boolean includeComponentState);

    /**
     * Imports a process-group JSON snapshot into the given parent process group.
     * Maps to {@code POST /process-groups/{parentProcessGroupId}/process-groups/import} with
     * revision version {@code 0}, caller-supplied name, position, and flow snapshot.
     *
     * @param parentProcessGroupId the parent process group ID; must not be blank
     * @param groupName            the imported process group name; must not be blank
     * @param positionX            the target X coordinate
     * @param positionY            the target Y coordinate
     * @param flowSnapshot         the flow snapshot payload; must not be null or empty
     * @return the created {@code ProcessGroupEntity} response as a map
     * @throws IllegalArgumentException if parentProcessGroupId/groupName is blank or flowSnapshot is null/empty
     */
    Map<String, Object> importProcessGroup(String parentProcessGroupId, String groupName,
            double positionX, double positionY, Map<String, Object> flowSnapshot);

    // =========================================================================
    // System diagnostics, cluster, counters, and security operations
    // =========================================================================

    /**
     * Retrieves system diagnostics, optionally broken down per node.
     * Maps to {@code GET /system-diagnostics} with optional {@code nodewise},
     * {@code diagnosticLevel} (BASIC/VERBOSE, default BASIC), and {@code clusterNodeId} parameters.
     * Null/blank {@code diagnosticLevel} is normalized to {@code BASIC}.
     * Null/blank {@code clusterNodeId} is treated as absent.
     * {@code nodewise=true} and a non-blank {@code clusterNodeId} are mutually exclusive.
     *
     * @param nodewise        if {@code true}, includes per-node breakdowns
     * @param diagnosticLevel the diagnostic verbosity level; null or blank defaults to {@code BASIC};
     *                        only {@code BASIC} and {@code VERBOSE} are accepted
     * @param clusterNodeId   optional cluster node ID to target; null or blank means all nodes
     * @return the {@code SystemDiagnosticsEntity} as a map
     * @throws IllegalArgumentException if {@code diagnosticLevel} is non-blank and not recognized,
     *                                  or if both {@code nodewise} is {@code true} and {@code clusterNodeId}
     *                                  is non-blank
     */
    Map<String, Object> getSystemDiagnostics(boolean nodewise, String diagnosticLevel, String clusterNodeId);

    /**
     * Retrieves JMX metrics, optionally filtered by a bean name pattern.
     * Maps to {@code GET /system-diagnostics/jmx-metrics} with optional {@code beanNameFilter}.
     * If {@code beanNameFilter} is non-null and non-blank it is URL-encoded before appending.
     * This is a non-guaranteed NiFi endpoint and may not be available in every distribution.
     *
     * @param beanNameFilter optional regex pattern to filter MBean names; null or blank means no filter
     * @return the {@code JmxMetricsResultsEntity} as a map
     */
    Map<String, Object> getJmxMetrics(String beanNameFilter);

    /**
     * Retrieves a summary of the NiFi cluster.
     * Maps to {@code GET /flow/cluster/summary}.
     *
     * @return the cluster summary as a map
     */
    Map<String, Object> getClusterSummary();

    /**
     * Retrieves the full NiFi cluster node listing.
     * Maps to {@code GET /controller/cluster}.
     *
     * @return the {@code ClusterEntity} as a map
     */
    Map<String, Object> getClusterNodes();

    /**
     * Retrieves a specific node in the NiFi cluster.
     * Maps to {@code GET /controller/cluster/nodes/{id}}.
     *
     * @param nodeId the cluster node ID; must not be blank
     * @return the {@code NodeEntity} as a map
     * @throws IllegalArgumentException if nodeId is blank
     */
    Map<String, Object> getClusterNode(String nodeId);

    /**
     * Lists the NiFi counters, optionally broken down per node.
     * Maps to {@code GET /counters} with optional {@code nodewise} and {@code clusterNodeId}.
     * This is a non-guaranteed NiFi endpoint and may not be available in every distribution.
     * Null/blank {@code clusterNodeId} is treated as absent.
     * {@code nodewise=true} and a non-blank {@code clusterNodeId} are mutually exclusive.
     *
     * @param nodewise      if {@code true}, includes per-node breakdowns
     * @param clusterNodeId optional cluster node ID to target; null or blank means all nodes
     * @return the {@code CountersEntity} as a map
     * @throws IllegalArgumentException if both {@code nodewise} is {@code true} and
     *                                  {@code clusterNodeId} is non-blank
     */
    Map<String, Object> listCounters(boolean nodewise, String clusterNodeId);

    /**
     * Updates the status of the specified cluster node.
     * Maps to {@code PUT /controller/cluster/nodes/{id}}.
     * Accepted status values are {@code CONNECTING}, {@code DISCONNECTING}, and {@code OFFLOADING}
     * (case-insensitive), as these are the three transitional states that
     * {@code StandardNiFiServiceFacade.updateNode} explicitly honours.
     * Server/resource Write authorization remains authoritative; no preflight access-policy check
     * is performed here as such a check would be non-atomic.
     * <p>The {@code confirmed} parameter must be {@code true}; passing {@code false} throws
     * {@link IllegalArgumentException} before any network or facade call is made.
     *
     * @param nodeId    the cluster node ID; must not be blank
     * @param status    the target node status; must be CONNECTING, DISCONNECTING, or OFFLOADING
     * @param confirmed must be {@code true} to proceed; {@code false} throws {@link IllegalArgumentException}
     * @return the updated {@code NodeEntity} as a map
     * @throws IllegalArgumentException if nodeId is blank, status is not recognized, or confirmed is {@code false}
     */
    Map<String, Object> updateClusterNode(String nodeId, String status, boolean confirmed);

    /**
     * Removes the specified node from the NiFi cluster.
     * Maps to {@code DELETE /controller/cluster/nodes/{id}}.
     * A preflight {@code GET} is performed to confirm that the node's current status is
     * {@code DISCONNECTED} or {@code OFFLOADED} before the deletion is issued, matching
     * the states that {@code StandardNiFiServiceFacade.deleteNode} accepts. The preflight
     * is not atomic; the server's own authorization and state checks remain authoritative.
     * <p>The {@code confirmed} parameter must be {@code true}; passing {@code false} throws
     * {@link IllegalArgumentException} before any network or facade call is made.
     *
     * @param nodeId    the cluster node ID; must not be blank
     * @param confirmed must be {@code true} to proceed; {@code false} throws {@link IllegalArgumentException}
     * @return the deleted {@code NodeEntity} as a map
     * @throws IllegalArgumentException if nodeId is blank or confirmed is {@code false}
     * @throws IllegalStateException    if the node's current status is not DISCONNECTED or OFFLOADED
     */
    Map<String, Object> removeClusterNode(String nodeId, boolean confirmed);

    /**
     * Resets the specified counter value to zero.
     * Maps to {@code PUT /counters/{id}} with no request body.
     * This is a non-guaranteed NiFi endpoint and may not be available in every distribution;
     * no preflight access-policy check is performed as such a check would be non-atomic.
     * Server/resource Write authorization remains authoritative.
     * <p>The {@code confirmed} parameter must be {@code true}; passing {@code false} throws
     * {@link IllegalArgumentException} before any network or facade call is made.
     *
     * @param counterId the counter ID; must not be blank
     * @param confirmed must be {@code true} to proceed; {@code false} throws {@link IllegalArgumentException}
     * @return the updated {@code CounterEntity} as a map
     * @throws IllegalArgumentException if counterId is blank or confirmed is {@code false}
     */
    Map<String, Object> resetCounter(String counterId, boolean confirmed);

    /**
     * Resets all counter values to zero.
     * Maps to {@code PUT /counters} with no request body.
     * This is a non-guaranteed NiFi endpoint and may not be available in every distribution;
     * no preflight access-policy check is performed as such a check would be non-atomic.
     * Server/resource Write authorization remains authoritative.
     * <p>The {@code confirmed} parameter must be {@code true}; passing {@code false} throws
     * {@link IllegalArgumentException} before any network or facade call is made.
     *
     * @param confirmed must be {@code true} to proceed; {@code false} throws {@link IllegalArgumentException}
     * @return the updated {@code CountersEntity} as a map
     * @throws IllegalArgumentException if confirmed is {@code false}
     */
    Map<String, Object> resetAllCounters(boolean confirmed);

    /**
     * Logs out the current user and clears any locally held session token.
     * Maps to {@code DELETE /access/logout} in the HTTP client; the embedded client
     * is an intentional no-op because no HTTP JWT session exists.
     * The local token is cleared only after a successful server-side logout.
     */
    void logout();

    /**
     * Retrieves the authentication configuration for this NiFi instance.
     * Maps to {@code GET /authentication/configuration}.
     *
     * @return the {@code AuthenticationConfigurationEntity} as a map
     */
    Map<String, Object> getAuthenticationConfiguration();

    /**
     * Lists all authorizable resources registered with this NiFi instance.
     * Maps to {@code GET /resources} and unwraps the nested {@code resources} collection.
     *
     * @return list of {@code ResourceDTO} maps
     */
    List<Map<String, Object>> listAuthorizableResources();

    /**
     * Retrieves the access policy for the given action and resource.
     * Maps to {@code GET /policies/{action}/{resource}}.
     * {@code action} must be {@code read} or {@code write} (case-insensitive, normalized to lower-case).
     * {@code resource} must not be blank; all leading slashes are stripped before constructing the path.
     *
     * @param action   the policy action; must be {@code read} or {@code write} (case-insensitive)
     * @param resource the policy resource path; must not be blank; leading slashes are stripped
     * @return the {@code AccessPolicyEntity} as a map
     * @throws IllegalArgumentException if {@code action} is not recognized or {@code resource} is blank
     */
    Map<String, Object> getAccessPolicy(String action, String resource);

    /**
     * Lists all users visible to the current user.
     * Maps to {@code GET /tenants/users} and unwraps the nested {@code users} collection.
     *
     * @return list of {@code UserEntity} maps
     */
    List<Map<String, Object>> listUsers();

    /**
     * Lists all user groups visible to the current user.
     * Maps to {@code GET /tenants/user-groups} and unwraps the nested {@code userGroups} collection.
     *
     * @return list of {@code UserGroupEntity} maps
     */
    List<Map<String, Object>> listUserGroups();
}
