/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.effectiveFlow;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;

import in.shrake.nifi.layout.copilot.ProcessGroupFlowMapAdapter;
import in.shrake.nifi.layout.core.model.LayoutOptions;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.support.FlowLayoutService;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Post-deployment layout stage that runs after connections are created and before runtime
 * activation.
 *
 * <p>In {@link LayoutMode#ENGINE} mode the stage fetches a fresh recursive flow snapshot,
 * chooses a full layout for new flows and topology-heavy connection changes, or an
 * incremental layout for smaller updates, and persists the result via
 * {@link NiFiClientLayoutWriter}. A failure is propagated as a {@link RuntimeException},
 * which prevents the {@link RuntimeActivationStage} from running.
 *
 * <p>In {@link LayoutMode#DISABLED} mode the stage is a no-op: provisional coordinates
 * assigned by {@link CanvasPositionProvider} are retained as-is.
 */
final class LayoutDeploymentStage {
    private static final Logger logger = LoggerFactory.getLogger(LayoutDeploymentStage.class);
    private static final int FULL_LAYOUT_CONNECTION_CHANGE_THRESHOLD = 3;

    private static final LayoutExecutor DEFAULT_EXECUTOR = new LayoutExecutor() {
        @Override
        public LayoutResult layout(
                final Map<String, Object> flow,
                final NiFiClientOperations nifi,
                final OwnershipLedger ledger) {
            return service(nifi, ledger).layout(flow, LayoutOptions.defaults());
        }

        @Override
        public LayoutResult layoutIncremental(
                final Map<String, Object> flow,
                final NiFiClientOperations nifi,
                final OwnershipLedger ledger,
                final Set<String> changedIds) {
            return service(nifi, ledger).layoutIncremental(flow, LayoutOptions.defaults(), changedIds);
        }

        private FlowLayoutService<Map<String, Object>> service(
                final NiFiClientOperations nifi,
                final OwnershipLedger ledger) {
            return new FlowLayoutService<>(
                    new ProcessGroupFlowMapAdapter(),
                    new NiFiClientLayoutWriter(nifi, ledger));
        }
    };

    private LayoutDeploymentStage() {
    }

    interface LayoutExecutor {
        LayoutResult layout(
                Map<String, Object> flow,
                NiFiClientOperations nifi,
                OwnershipLedger ledger);

        LayoutResult layoutIncremental(
                Map<String, Object> flow,
                NiFiClientOperations nifi,
                OwnershipLedger ledger,
                Set<String> changedIds);
    }

    /**
     * Immutable observation produced by a single layout stage invocation.
     *
     * @param movedCount number of components repositioned; 0 for SKIPPED/FAILURE
     * @param routedCount number of connections re-routed; 0 for SKIPPED/FAILURE
     * @param skipped {@code true} when the stage was disabled
     */
    record LayoutObservation(int movedCount, int routedCount, boolean skipped) {
        static final LayoutObservation SKIPPED = new LayoutObservation(0, 0, true);
    }

    /**
     * Executes the layout stage for the given mode.
     *
     * @param state the deployment state; must not be null
     * @param mode the layout mode; must not be null
     * @return observation with counts; never null
     * @throws RuntimeException if ENGINE layout or write-back fails
     */
    static LayoutObservation deploy(final DeploymentState state, final LayoutMode mode) {
        return deploy(state, mode, DEFAULT_EXECUTOR);
    }

    static LayoutObservation deploy(
            final DeploymentState state,
            final LayoutMode mode,
            final LayoutExecutor executor) {
        if (mode == LayoutMode.DISABLED) {
            logger.info("Canvas layout skipped: mode={}", mode);
            return LayoutObservation.SKIPPED;
        }

        final String pgId = state.target().effectiveProcessGroupId();
        final NiFiClientOperations nifi = state.context().nifi();
        final OwnershipLedger ledger = state.ledger();
        final boolean isNewFlow = hasNoCanvasComponents(state.target().inventoryResponse());
        final int connectionChangeCount = connectionChangeCount(ledger);
        final boolean requiresFullLayout = isNewFlow
                || connectionChangeCount >= FULL_LAYOUT_CONNECTION_CHANGE_THRESHOLD;

        if (!isNewFlow) {
            final Set<String> changedIds = ledger.changedCanvasIds();
            if (changedIds.isEmpty()) {
                logger.info("Canvas layout skipped: mode=ENGINE, no changed canvas components");
                return LayoutObservation.SKIPPED;
            }
        }

        logger.info("Canvas layout running: mode=ENGINE, strategy={}, changedIds={}, connectionChanges={}",
                requiresFullLayout ? "FULL" : "INCREMENTAL",
                ledger.changedCanvasIds().size(), connectionChangeCount);
        final Map<String, Object> freshFlowMap = ProcessGroupFlowMapAssembler.assemble(pgId, nifi);
        final LayoutResult result = requiresFullLayout
                ? executor.layout(freshFlowMap, nifi, ledger)
                : executor.layoutIncremental(freshFlowMap, nifi, ledger, ledger.changedCanvasIds());

        logger.info("Canvas layout completed: strategy={}, moved={}, routed={}",
                requiresFullLayout ? "FULL" : "INCREMENTAL",
                result.getTotalComponentsRepositioned(), result.getConnectionBendPoints().size());
        return new LayoutObservation(
                result.getTotalComponentsRepositioned(),
                result.getConnectionBendPoints().size(),
                false);
    }

    private static int connectionChangeCount(final OwnershipLedger ledger) {
        final Set<String> connectionIds = new LinkedHashSet<>(ledger.createdConnectionIds());
        ledger.updatedConnections().stream()
                .map(OwnershipLedger.ConnectionRestore::connectionId)
                .forEach(connectionIds::add);
        return connectionIds.size();
    }

    /**
     * Returns {@code true} when the process-group flow response contains no canvas
     * components (processors, ports, funnels, labels, remote process groups, or child
     * process groups). Used to choose between full and incremental layout.
     */
    private static boolean hasNoCanvasComponents(final Map<String, Object> inventoryResponse) {
        final Map<String, Object> flow = effectiveFlow(inventoryResponse);
        return listOfMap(flow.get("processors")).isEmpty()
                && listOfMap(flow.get("inputPorts")).isEmpty()
                && listOfMap(flow.get("outputPorts")).isEmpty()
                && listOfMap(flow.get("funnels")).isEmpty()
                && listOfMap(flow.get("labels")).isEmpty()
                && listOfMap(flow.get("remoteProcessGroups")).isEmpty()
                && listOfMap(flow.get("processGroups")).isEmpty();
    }
}
