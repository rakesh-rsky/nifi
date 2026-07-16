package org.apache.nifi.copilot.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Per-build record of owned resources and restore actions needed for compensating rollback.
 * Registered immediately after each successful NiFi mutation.
 */
final class OwnershipLedger {

    record ProcessorRestore(String processorId, Map<String, Object> originalUpdates) {}

    record ConnectionRestore(String connectionId, Map<String, Object> originalComponent) {}

    record RollbackAction(String description, Runnable action) {}

    record RuntimeRequest(boolean input, String id, String state) {}

    record RuntimeRestore(boolean input, String id, String state) {}

    record TransmissionRequest(String id, String state) {}

    record TransmissionRestore(String id, String state) {}

    record ParameterContextOwnership(
            String pcId,
            boolean created,
            String boundProcessGroupId,
            String previousBindingId) {}

    private final String targetProcessGroupId;
    private String childProcessGroupId;
    private ParameterContextOwnership parameterContextOwnership;

    private final List<String> createdProcessorIds = new ArrayList<>();
    private final List<String> createdConnectionIds = new ArrayList<>();
    private final List<String> createdControllerServiceIds = new ArrayList<>();
    private final List<ProcessorRestore> updatedProcessors = new ArrayList<>();
    private final List<ConnectionRestore> updatedConnections = new ArrayList<>();
    private final List<RollbackAction> snippetActions = new ArrayList<>();
    private final List<RollbackAction> canvasActions = new ArrayList<>();
    private final List<RuntimeRequest> runtimeRequests = new ArrayList<>();
    private final List<RuntimeRestore> runtimeRestores = new ArrayList<>();
    private final List<TransmissionRequest> remoteTransmissionRequests = new ArrayList<>();
    private final List<TransmissionRestore> remoteTransmissionRestores = new ArrayList<>();

    OwnershipLedger(final String targetProcessGroupId) {
        this.targetProcessGroupId = targetProcessGroupId;
    }

    String targetProcessGroupId() {
        return targetProcessGroupId;
    }

    void setChildProcessGroupId(final String childProcessGroupId) {
        this.childProcessGroupId = childProcessGroupId;
    }

    String childProcessGroupId() {
        return childProcessGroupId;
    }

    void addCreatedProcessorId(final String processorId) {
        createdProcessorIds.add(processorId);
    }

    void addCreatedConnectionId(final String connectionId) {
        createdConnectionIds.add(connectionId);
    }

    void addCreatedControllerServiceId(final String serviceId) {
        createdControllerServiceIds.add(serviceId);
    }

    void addProcessorRestore(final String processorId, final Map<String, Object> originalUpdates) {
        updatedProcessors.add(new ProcessorRestore(processorId, originalUpdates));
    }

    void addConnectionRestore(final String connectionId, final Map<String, Object> originalComponent) {
        updatedConnections.add(new ConnectionRestore(connectionId, originalComponent));
    }

    void addSnippetAction(final String description, final Runnable action) {
        snippetActions.add(new RollbackAction(description, action));
    }

    void addCanvasAction(final String description, final Runnable action) {
        canvasActions.add(new RollbackAction(description, action));
    }

    void addRuntimeRequest(final boolean input, final String id, final String state) {
        runtimeRequests.add(new RuntimeRequest(input, id, state));
    }

    void addRuntimeRestore(final boolean input, final String id, final String state) {
        runtimeRestores.add(new RuntimeRestore(input, id, state));
    }

    void addRemoteTransmissionRequest(final String id, final String state) {
        remoteTransmissionRequests.add(new TransmissionRequest(id, state));
    }

    void addRemoteTransmissionRestore(final String id, final String state) {
        remoteTransmissionRestores.add(new TransmissionRestore(id, state));
    }

    void registerParameterContext(
            final String pcId,
            final boolean created,
            final String boundProcessGroupId,
            final String previousBindingId) {
        this.parameterContextOwnership = new ParameterContextOwnership(pcId, created, boundProcessGroupId, previousBindingId);
    }

    ParameterContextOwnership parameterContextOwnership() {
        return parameterContextOwnership;
    }

    List<RuntimeRequest> runtimeRequests() {
        return Collections.unmodifiableList(runtimeRequests);
    }

    List<TransmissionRequest> remoteTransmissionRequests() {
        return Collections.unmodifiableList(remoteTransmissionRequests);
    }

    List<String> createdProcessorIds() {
        return Collections.unmodifiableList(createdProcessorIds);
    }

    List<String> createdConnectionIds() {
        return Collections.unmodifiableList(createdConnectionIds);
    }

    List<String> createdControllerServiceIds() {
        return Collections.unmodifiableList(createdControllerServiceIds);
    }

    List<ProcessorRestore> updatedProcessors() {
        return Collections.unmodifiableList(updatedProcessors);
    }

    List<ConnectionRestore> updatedConnections() {
        return Collections.unmodifiableList(updatedConnections);
    }

    List<RollbackAction> snippetActions() {
        return Collections.unmodifiableList(snippetActions);
    }

    List<RollbackAction> canvasActions() {
        return Collections.unmodifiableList(canvasActions);
    }

    List<RuntimeRestore> runtimeRestores() {
        return Collections.unmodifiableList(runtimeRestores);
    }

    List<TransmissionRestore> remoteTransmissionRestores() {
        return Collections.unmodifiableList(remoteTransmissionRestores);
    }
}
