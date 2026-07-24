package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.effectiveFlow;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.requireEntityId;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrNull;
import static org.apache.nifi.copilot.builder.SpecificationSupport.stringOrNull;

import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;

/**
 * Preparation stage with two distinct call points that preserve the established
 * exception boundary.
 *
 * <p>{@link #prepare} is invoked <em>outside</em> the deployment try/catch in
 * {@link FlowBuilder} — it resolves the parent process group, captures the parent
 * snapshot when rollback is requested, and creates per-build helpers.
 *
 * <p>{@link #prepareTarget} is the first operation <em>inside</em> the try/catch —
 * it resolves or creates the effective child process group, captures the child
 * snapshot when rollback is requested, and validates the target.
 */
final class DeploymentPreparationStage {

    private DeploymentPreparationStage() {
    }

    /**
     * Resolves the parent process group, optionally captures the parent snapshot,
     * and creates per-build state. Must be called outside the deployment try/catch.
     */
    static DeploymentState prepare(
            final DeploymentContext context,
            final ComponentResolver resolver,
            final FlowDeploymentMetricsRegistry metrics) {
        final String prePgId = resolver.resolveTargetProcessGroup(
                context.requestedTarget(), context.nifi());
        final Map<String, Object> parentSnapshot =
                captureSnapshot(prePgId, context.rollbackOnFailure(), context.nifi());
        final ControllerServiceDeployer csDeployer = new ControllerServiceDeployer();
        final OwnershipLedger ledger = new OwnershipLedger(prePgId);
        return new DeploymentState(context, prePgId, parentSnapshot, ledger, csDeployer, metrics);
    }

    /**
     * Resolves or creates the effective process group, optionally captures its
     * snapshot, and validates the target. Must be called inside the deployment
     * try/catch.
     */
    static void prepareTarget(
            final DeploymentState state,
            final ComponentResolver resolver,
            final LivePreflightValidator liveValidator) {
        final DeploymentContext context = state.context();
        final Map<String, Object> processGroupSpec =
                mapOrNull(context.specification().get("process_group"));
        final DeploymentTarget target = resolveEffectiveTarget(
                processGroupSpec, state.prePgId(), state.parentSnapshot(),
                context.rollbackOnFailure(), state.ledger(), resolver, context.nifi());
        liveValidator.validatePreparedTarget(target);
        state.setTarget(target);
    }

    static String findExistingEffectiveProcessGroupId(
            final DeploymentState state,
            final ComponentResolver resolver) {
        final Map<String, Object> processGroupSpec =
                mapOrNull(state.context().specification().get("process_group"));
        if (processGroupSpec == null) {
            return state.prePgId();
        }
        final String name = requireProcessGroupName(processGroupSpec);
        final Map<String, Object> existingChild = resolver.findUniqueChildProcessGroup(
                state.prePgId(), name, state.context().nifi());
        return existingChild == null ? null : requireEntityId(existingChild, "child process group");
    }

    private static DeploymentTarget resolveEffectiveTarget(
            final Map<String, Object> processGroupSpec,
            final String parentProcessGroupId,
            final Map<String, Object> parentSnapshot,
            final boolean rollbackOnFailure,
            final OwnershipLedger ledger,
            final ComponentResolver resolver,
            final NiFiClientOperations nifi) {
        final String effectiveProcessGroupId;
        final String previousBinding;
        final Map<String, Object> effectiveSnapshot;
        if (processGroupSpec == null) {
            effectiveProcessGroupId = parentProcessGroupId;
            effectiveSnapshot = parentSnapshot;
            previousBinding = parentSnapshot == null
                    ? null : stringOrNull(parentSnapshot.get("originalPcBindingId"));
        } else {
            final String name = requireProcessGroupName(processGroupSpec);
            final Map<String, Object> existingChild =
                    resolver.findUniqueChildProcessGroup(parentProcessGroupId, name, nifi);
            if (existingChild != null) {
                effectiveProcessGroupId = requireEntityId(existingChild, "child process group");
                effectiveSnapshot = captureSnapshot(effectiveProcessGroupId, rollbackOnFailure, nifi);
                previousBinding = effectiveSnapshot == null
                        ? null : stringOrNull(effectiveSnapshot.get("originalPcBindingId"));
            } else {
                final Map<String, Object> createdProcessGroup = nifi.createProcessGroup(
                        parentProcessGroupId,
                        name,
                        NiFiClientOperations.doubleValue(processGroupSpec.getOrDefault("x", 400)),
                        NiFiClientOperations.doubleValue(processGroupSpec.getOrDefault("y", 300)));
                effectiveProcessGroupId = requireEntityId(createdProcessGroup, "created process group");
                ledger.setChildProcessGroupId(effectiveProcessGroupId);
                ledger.addChangedCanvasId(effectiveProcessGroupId);
                effectiveSnapshot = null;
                previousBinding = null;
            }
        }
        final Map<String, Object> inventoryResponse = nifi.getProcessGroupFlow(effectiveProcessGroupId);
        return new DeploymentTarget(
                parentProcessGroupId,
                effectiveProcessGroupId,
                previousBinding,
                effectiveSnapshot,
                inventoryResponse,
                effectiveFlow(inventoryResponse));
    }

    private static String requireProcessGroupName(final Map<String, Object> processGroupSpec) {
        final Object nameValue = processGroupSpec.get("name");
        if (nameValue == null || String.valueOf(nameValue).isBlank()) {
            throw new IllegalArgumentException("Process group name must not be blank");
        }
        return String.valueOf(nameValue);
    }

    private static Map<String, Object> captureSnapshot(
            final String processGroupId,
            final boolean rollbackOnFailure,
            final NiFiClientOperations nifi) {
        if (!rollbackOnFailure) {
            return null;
        }
        try {
            return nifi.snapshotProcessGroup(processGroupId);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Could not capture pre-deploy snapshot for process group " + processGroupId, e);
        }
    }
}
