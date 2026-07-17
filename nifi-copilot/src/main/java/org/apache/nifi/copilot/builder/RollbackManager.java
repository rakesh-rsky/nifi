package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes compensating rollback for a failed deployment in the exact order required:
 *  1. stop requested ports (forward)
 *  2. stop requested RPG transmission (forward)
 *  3. delete created connections (reverse)
 *  4. reverse snippet actions (reverse)
 *  5. reverse canvas mutations (reverse)
 *  6. restore port states (reverse)
 *  7. restore RPG transmission states (reverse)
 *  8. delete created processors (reverse)
 *  9. restore updated processors (reverse)
 * 10. restore updated connections (reverse)
 * 11. delete created controller services (reverse)
 * 12. restore or remove parameter-context binding; delete only owned context
 * 13. delete only the owned child process group
 * 14. attach aggregate failures as suppressed on the original deployment exception
 */
final class RollbackManager {

    private static final Logger logger = LoggerFactory.getLogger(RollbackManager.class);

    private RollbackManager() {}

    static void rollback(
            final OwnershipLedger ledger,
            final NiFiClientOperations nifi,
            final Exception deploymentFailure,
            final FlowDeploymentMetricsRegistry metrics) {
        final List<Exception> failures = new ArrayList<>();

        for (var request : ledger.runtimeRequests()) {
            runStep("stop port " + request.id(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.STOP_PORT,
                    failures, metrics, () -> {
                        if (request.input()) {
                            nifi.setInputPortRunStatus(request.id(), "STOPPED");
                        } else {
                            nifi.setOutputPortRunStatus(request.id(), "STOPPED");
                        }
                    });
        }
        for (var request : ledger.remoteTransmissionRequests()) {
            runStep("stop remote transmission " + request.id(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.STOP_REMOTE_TRANSMISSION,
                    failures, metrics,
                    () -> nifi.setRemoteProcessGroupTransmission(request.id(), "STOPPED"));
        }
        final List<String> connectionIds = ledger.createdConnectionIds();
        for (int i = connectionIds.size() - 1; i >= 0; i--) {
            final String connectionId = connectionIds.get(i);
            runStep("delete connection " + connectionId,
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.DELETE_CONNECTION,
                    failures, metrics,
                    () -> nifi.deleteConnection(connectionId));
        }
        final List<OwnershipLedger.RollbackAction> snippetActions = ledger.snippetActions();
        for (int i = snippetActions.size() - 1; i >= 0; i--) {
            final var action = snippetActions.get(i);
            runStep(action.description(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.REVERSE_SNIPPET,
                    failures, metrics, action.action());
        }
        final List<OwnershipLedger.RollbackAction> canvasActions = ledger.canvasActions();
        for (int i = canvasActions.size() - 1; i >= 0; i--) {
            final var action = canvasActions.get(i);
            runStep(action.description(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.REVERSE_CANVAS,
                    failures, metrics, action.action());
        }
        final List<OwnershipLedger.RuntimeRestore> runtimeRestores = ledger.runtimeRestores();
        for (int i = runtimeRestores.size() - 1; i >= 0; i--) {
            final var restore = runtimeRestores.get(i);
            runStep("restore port state " + restore.id(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_PORT_STATE,
                    failures, metrics, () -> {
                        if (restore.input()) {
                            nifi.setInputPortRunStatus(restore.id(), restore.state());
                        } else {
                            nifi.setOutputPortRunStatus(restore.id(), restore.state());
                        }
                    });
        }
        final List<OwnershipLedger.TransmissionRestore> transmissionRestores = ledger.remoteTransmissionRestores();
        for (int i = transmissionRestores.size() - 1; i >= 0; i--) {
            final var restore = transmissionRestores.get(i);
            runStep("restore remote transmission " + restore.id(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_REMOTE_TRANSMISSION,
                    failures, metrics,
                    () -> nifi.setRemoteProcessGroupTransmission(restore.id(), restore.state()));
        }
        final List<String> processorIds = ledger.createdProcessorIds();
        for (int i = processorIds.size() - 1; i >= 0; i--) {
            final String processorId = processorIds.get(i);
            runStep("delete processor " + processorId,
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.DELETE_PROCESSOR,
                    failures, metrics,
                    () -> nifi.deleteProcessor(processorId));
        }
        final List<OwnershipLedger.ProcessorRestore> updatedProcessors = ledger.updatedProcessors();
        for (int i = updatedProcessors.size() - 1; i >= 0; i--) {
            final var restore = updatedProcessors.get(i);
            runStep("restore processor " + restore.processorId(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_PROCESSOR,
                    failures, metrics,
                    () -> nifi.updateProcessor(restore.processorId(), restore.originalUpdates()));
        }
        final List<OwnershipLedger.ConnectionRestore> updatedConnections = ledger.updatedConnections();
        for (int i = updatedConnections.size() - 1; i >= 0; i--) {
            final var restore = updatedConnections.get(i);
            runStep("restore connection " + restore.connectionId(),
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_CONNECTION,
                    failures, metrics, () -> {
                        final Object originalRelationships =
                                mapOrEmpty(restore.originalComponent()).get("selectedRelationships");
                        final Map<String, Object> restoreUpdates = new LinkedHashMap<>();
                        restoreUpdates.put("selectedRelationships",
                                originalRelationships == null ? List.of() : originalRelationships);
                        nifi.updateConnection(restore.connectionId(), restoreUpdates);
                    });
        }
        final List<String> controllerServiceIds = ledger.createdControllerServiceIds();
        for (int i = controllerServiceIds.size() - 1; i >= 0; i--) {
            final String serviceId = controllerServiceIds.get(i);
            runStep("delete controller service " + serviceId,
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.DELETE_CONTROLLER_SERVICE,
                    failures, metrics,
                    () -> nifi.deleteControllerService(serviceId));
        }

        final OwnershipLedger.ParameterContextOwnership pc = ledger.parameterContextOwnership();
        if (pc != null && pc.pcId() != null) {
            final String boundProcessGroupId = pc.boundProcessGroupId();
            final boolean ownedChild = boundProcessGroupId != null
                    && boundProcessGroupId.equals(ledger.childProcessGroupId());
            if (boundProcessGroupId != null) {
                if (ownedChild) {
                    runStep("unbind parameter context from owned child process group " + boundProcessGroupId,
                            FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_PARAMETER_CONTEXT,
                            failures, metrics,
                            () -> nifi.unbindParameterContextFromProcessGroup(boundProcessGroupId));
                } else if (pc.previousBindingId() == null) {
                    runStep("restore empty parameter context binding on " + boundProcessGroupId,
                            FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_PARAMETER_CONTEXT,
                            failures, metrics,
                            () -> nifi.unbindParameterContextFromProcessGroup(boundProcessGroupId));
                } else {
                    final String previousBindingId = pc.previousBindingId();
                    runStep("restore parameter context binding " + previousBindingId,
                            FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_PARAMETER_CONTEXT,
                            failures, metrics,
                            () -> nifi.bindParameterContextToProcessGroup(boundProcessGroupId, previousBindingId));
                }
            }
            if (pc.created()) {
                final String pcId = pc.pcId();
                runStep("delete parameter context " + pcId,
                        FlowDeploymentMetricsRegistry.RollbackActionCategory.RESTORE_PARAMETER_CONTEXT,
                        failures, metrics,
                        () -> nifi.deleteParameterContext(pcId));
            }
        }
        final String childPgId = ledger.childProcessGroupId();
        if (childPgId != null) {
            runStep("delete child process group " + childPgId,
                    FlowDeploymentMetricsRegistry.RollbackActionCategory.DELETE_CHILD_PROCESS_GROUP,
                    failures, metrics,
                    () -> nifi.deleteProcessGroup(childPgId));
        }

        if (failures.isEmpty()) {
            logger.info("Rollback completed for process group {}", ledger.targetProcessGroupId());
            metrics.observeRollback(FlowDeploymentMetricsRegistry.RollbackOutcome.SUCCESS);
            return;
        }

        final IllegalStateException rollbackFailure = new IllegalStateException(
                "Rollback of process group " + ledger.targetProcessGroupId()
                        + " completed with " + failures.size() + " error(s)");
        failures.forEach(rollbackFailure::addSuppressed);
        deploymentFailure.addSuppressed(rollbackFailure);
        logger.error("Rollback also failed: {}", rollbackFailure.getMessage());
        metrics.observeRollback(FlowDeploymentMetricsRegistry.RollbackOutcome.PARTIAL_FAILURE);
    }

    private static void runStep(
            final String description,
            final FlowDeploymentMetricsRegistry.RollbackActionCategory category,
            final List<Exception> failures,
            final FlowDeploymentMetricsRegistry metrics,
            final Runnable rollbackStep) {
        try {
            rollbackStep.run();
            metrics.observeRollbackAction(category, FlowDeploymentMetricsRegistry.ActionOutcome.SUCCESS);
        } catch (Exception e) {
            logger.warn("Rollback: failed to {}: {}", description, e.getMessage());
            failures.add(e);
            metrics.observeRollbackAction(category, FlowDeploymentMetricsRegistry.ActionOutcome.FAILURE);
        }
    }
}
