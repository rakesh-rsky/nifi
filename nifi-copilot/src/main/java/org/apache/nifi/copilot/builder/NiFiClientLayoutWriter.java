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

import in.shrake.nifi.layout.core.exception.WriteBackException;
import in.shrake.nifi.layout.core.model.ComponentUpdate;
import in.shrake.nifi.layout.core.model.LayoutResult;
import in.shrake.nifi.layout.core.model.NodeType;
import in.shrake.nifi.layout.core.model.Position;
import in.shrake.nifi.layout.support.writer.FlowLayoutWriter;
import in.shrake.nifi.layout.support.writer.LayoutResultFactory;
import in.shrake.nifi.layout.support.writer.LayoutWriteRequest;
import org.apache.nifi.copilot.service.NiFiClientOperations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * NiFi-client-backed {@link FlowLayoutWriter} that persists computed positions and
 * connection bends to NiFi via {@link NiFiClientOperations}.
 *
 * <p>Before each remote canvas mutation the original position or bend state is
 * captured from the live entity and a restoration {@link Runnable} is registered in
 * the supplied {@link OwnershipLedger} as a canvas action.  If a partial write
 * fails, {@link RollbackManager} reverses every completed mutation by executing the
 * registered canvas actions in reverse order.
 *
 * <p>Unknown or unmappable component IDs raise {@link WriteBackException}; partial
 * output is never silently skipped.
 */
public final class NiFiClientLayoutWriter implements FlowLayoutWriter<Map<String, Object>> {

    private final NiFiClientOperations nifi;
    private final OwnershipLedger ledger;
    private final LayoutResultFactory factory = new LayoutResultFactory();

    /**
     * Creates a new writer that dispatches mutations through {@code nifi} and
     * registers restoration actions in {@code ledger}.
     *
     * @param nifi   the NiFi client; must not be null
     * @param ledger the per-deployment ledger for rollback registration; must not be null
     */
    public NiFiClientLayoutWriter(final NiFiClientOperations nifi, final OwnershipLedger ledger) {
        this.nifi = Objects.requireNonNull(nifi, "nifi must not be null");
        this.ledger = Objects.requireNonNull(ledger, "ledger must not be null");
    }

    /**
     * Applies computed positions and connection bends to the live NiFi canvas.
     *
     * <p>The {@code target} parameter is accepted for interface compatibility with
     * other {@link FlowLayoutWriter} implementations and is not mutated; all
     * component IDs are resolved through the write request graph.
     *
     * @param target  the raw flow snapshot map (unused, accepted for interface compatibility)
     * @param request the layout result to write; must not be null
     * @return the structured layout result produced from {@code request}
     * @throws WriteBackException if an unrecognised {@link NodeType} is encountered
     */
    @Override
    public LayoutResult write(final Map<String, Object> target, final LayoutWriteRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        final LayoutResult result = factory.create(request);
        writeComponentPositions(result);
        writeConnectionBends(result);
        return result;
    }

    private void writeComponentPositions(final LayoutResult result) {
        for (final ComponentUpdate update : result.getUpdates()) {
            if (update.originalPosition().equals(update.newPosition())) {
                continue;
            }
            applyComponentUpdate(update);
        }
    }

    private void applyComponentUpdate(final ComponentUpdate update) {
        final String id = update.componentId();
        final NodeType type = update.componentType();
        final Map<String, Object> newPosMap = positionMap(update.newPosition());
        final Map<String, Object> origPosMap = positionMap(update.originalPosition());

        switch (type) {
            case PROCESSOR -> {
                ledger.addCanvasAction("restore processor position " + id,
                        () -> nifi.updateProcessor(id, Map.of("position", origPosMap)));
                nifi.updateProcessor(id, Map.of("position", newPosMap));
            }
            case PROCESS_GROUP -> {
                ledger.addCanvasAction("restore process group position " + id,
                        () -> nifi.updateProcessGroup(id, Map.of("position", origPosMap)));
                nifi.updateProcessGroup(id, Map.of("position", newPosMap));
            }
            case PORT_INPUT -> {
                ledger.addCanvasAction("restore input port position " + id,
                        () -> nifi.updateInputPort(id, Map.of("position", origPosMap)));
                nifi.updateInputPort(id, Map.of("position", newPosMap));
            }
            case PORT_OUTPUT -> {
                ledger.addCanvasAction("restore output port position " + id,
                        () -> nifi.updateOutputPort(id, Map.of("position", origPosMap)));
                nifi.updateOutputPort(id, Map.of("position", newPosMap));
            }
            case FUNNEL -> {
                ledger.addCanvasAction("restore funnel position " + id,
                        () -> nifi.updateFunnel(id, Map.of("position", origPosMap)));
                nifi.updateFunnel(id, Map.of("position", newPosMap));
            }
            case LABEL -> {
                ledger.addCanvasAction("restore label position " + id,
                        () -> nifi.updateLabel(id, Map.of("position", origPosMap)));
                nifi.updateLabel(id, Map.of("position", newPosMap));
            }
            case REMOTE_PROCESS_GROUP -> {
                ledger.addCanvasAction("restore remote process group position " + id,
                        () -> nifi.updateRemoteProcessGroup(id, Map.of("position", origPosMap)));
                nifi.updateRemoteProcessGroup(id, Map.of("position", newPosMap));
            }
            case VIRTUAL -> {
                // VIRTUAL nodes are structural placeholders with no remote counterpart; skip.
            }
            default -> throw new WriteBackException(id, type.name(),
                    "unsupported NodeType for canvas write-back: " + type);
        }
    }

    private void writeConnectionBends(final LayoutResult result) {
        for (final Map.Entry<String, List<Position>> entry : result.getConnectionBendPoints().entrySet()) {
            final String connectionId = entry.getKey();
            final List<Position> newBends = entry.getValue();

            // Capture original bends from the live entity before mutating.
            final Map<String, Object> currentEntity = nifi.getConnection(connectionId);
            final List<Map<String, Object>> originalBends = extractBends(currentEntity);

            ledger.addCanvasAction("restore connection bends " + connectionId,
                    () -> nifi.updateConnection(connectionId, Map.of("bends", originalBends)));
            nifi.updateConnection(connectionId, Map.of("bends", convertBends(newBends)));
        }
    }

    private static Map<String, Object> positionMap(final Position position) {
        final Map<String, Object> pos = new LinkedHashMap<>();
        pos.put("x", (double) position.x());
        pos.put("y", (double) position.y());
        return pos;
    }

    private static List<Map<String, Object>> convertBends(final List<Position> positions) {
        final List<Map<String, Object>> bends = new ArrayList<>(positions.size());
        for (final Position p : positions) {
            final Map<String, Object> bend = new LinkedHashMap<>();
            bend.put("x", (double) p.x());
            bend.put("y", (double) p.y());
            bends.add(bend);
        }
        return bends;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> extractBends(final Map<String, Object> entity) {
        final Object comp = entity.get("component");
        if (comp instanceof Map<?, ?> compMap) {
            final Object bends = ((Map<String, Object>) compMap).get("bends");
            if (bends instanceof List<?> bendList) {
                final List<Map<String, Object>> result = new ArrayList<>(bendList.size());
                for (final Object raw : bendList) {
                    if (raw instanceof Map<?, ?> bendMap) {
                        result.add((Map<String, Object>) bendMap);
                    }
                }
                return result;
            }
        }
        return List.of();
    }
}
