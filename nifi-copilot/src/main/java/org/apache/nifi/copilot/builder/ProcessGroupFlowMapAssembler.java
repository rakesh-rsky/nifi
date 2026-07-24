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

import org.apache.nifi.copilot.service.NiFiClientOperations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Assembles a fully-populated, recursively-complete process-group flow map
 * from {@link NiFiClientOperations#getProcessGroupFlow(String)} calls.
 *
 * <p>NiFi's {@code GET /flow/process-groups/{id}} response includes child process
 * group entities in the {@code flow.processGroups} list but typically omits the
 * child's own flow contents.  This assembler detects that and recursively fetches
 * each child's flow, injecting the contents into the child entity so the resulting
 * map is fully nested and compatible with
 * {@link in.shrake.nifi.layout.copilot.ProcessGroupFlowMapAdapter}.
 *
 * <p>Cycle detection uses a per-call visited set that tracks the current ancestor
 * path (DFS path-tracking, not global mark).  Depth is capped at
 * {@value #MAX_DEPTH} to prevent run-away recursion against pathological or
 * corrupted NiFi instances.
 */
public final class ProcessGroupFlowMapAssembler {

    /** Maximum nesting depth before aborting recursive assembly. Matches the layout parser limit. */
    public static final int MAX_DEPTH = 10;

    private ProcessGroupFlowMapAssembler() { }

    /**
     * Builds and returns a recursively-complete flow map for the process group
     * with the given ID.  The returned map is compatible with
     * {@code ProcessGroupFlowMapAdapter.parse()}.
     *
     * @param rootId the root process group ID; must not be blank
     * @param nifi   the NiFi client used for fetching child flows
     * @return assembled flow map with all nested child flows populated
     * @throws IllegalArgumentException if rootId is blank
     * @throws IllegalStateException    if a cycle or depth violation is detected
     */
    public static Map<String, Object> assemble(final String rootId, final NiFiClientOperations nifi) {
        if (rootId == null || rootId.isBlank()) {
            throw new IllegalArgumentException("rootId must not be blank");
        }
        return assembleRecursive(rootId, nifi, new LinkedHashSet<>(), 0);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> assembleRecursive(
            final String pgId,
            final NiFiClientOperations nifi,
            final Set<String> visited,
            final int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalStateException(
                    "Maximum process group nesting depth (" + MAX_DEPTH
                    + ") exceeded at group: " + pgId);
        }
        if (!visited.add(pgId)) {
            throw new IllegalStateException(
                    "Cycle detected in process group hierarchy: group '" + pgId
                    + "' already appears in the current ancestor path: " + visited);
        }
        try {
            final Map<String, Object> response = nifi.getProcessGroupFlow(pgId);
            return injectChildFlows(response, nifi, visited, depth);
        } finally {
            visited.remove(pgId);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> injectChildFlows(
            final Map<String, Object> response,
            final NiFiClientOperations nifi,
            final Set<String> visited,
            final int depth) {
        final Object pgFlowRaw = response.get("processGroupFlow");
        if (!(pgFlowRaw instanceof Map<?, ?> pgFlowMap)) {
            return response;
        }
        final Map<String, Object> pgFlow = (Map<String, Object>) pgFlowMap;

        final Object flowRaw = pgFlow.get("flow");
        if (!(flowRaw instanceof Map<?, ?> flowMap)) {
            return response;
        }
        final Map<String, Object> flow = (Map<String, Object>) flowMap;

        final Object childGroupsRaw = flow.get("processGroups");
        if (!(childGroupsRaw instanceof List<?> childGroupsList) || childGroupsList.isEmpty()) {
            return response;
        }

        final List<Map<String, Object>> enrichedChildren = new ArrayList<>(childGroupsList.size());
        for (final Object rawChild : childGroupsList) {
            if (!(rawChild instanceof Map<?, ?> rawChildMap)) {
                continue;
            }
            final Map<String, Object> childEntity = (Map<String, Object>) rawChildMap;
            final String childId = extractId(childEntity);
            if (childId == null || childId.isBlank()) {
                enrichedChildren.add(childEntity);
                continue;
            }

            // Always recursively assemble the child to ensure all descendant flows are
            // injected, even when the parent response already includes a partial "flow" key.
            final Map<String, Object> childResponse = assembleRecursive(childId, nifi, visited, depth + 1);
            final Map<String, Object> childFlow = extractFlow(childResponse);

            final Map<String, Object> enriched = new LinkedHashMap<>(childEntity);
            enriched.put("flow", childFlow);
            enrichedChildren.add(enriched);
        }

        // Rebuild flow with enriched processGroups
        final Map<String, Object> enrichedFlow = new LinkedHashMap<>(flow);
        enrichedFlow.put("processGroups", enrichedChildren);

        final Map<String, Object> enrichedPgFlow = new LinkedHashMap<>(pgFlow);
        enrichedPgFlow.put("flow", enrichedFlow);

        final Map<String, Object> result = new LinkedHashMap<>(response);
        result.put("processGroupFlow", enrichedPgFlow);
        return result;
    }

    /**
     * Extracts the component ID from a child entity, handling both the plain
     * {@code {"id": "...", ...}} form and the NiFi-wrapped
     * {@code {"id": "...", "component": {"id": "...", ...}}} form.
     */
    @SuppressWarnings("unchecked")
    private static String extractId(final Map<String, Object> entity) {
        final Object id = entity.get("id");
        if (id != null && !String.valueOf(id).isBlank()) {
            return String.valueOf(id);
        }
        final Object comp = entity.get("component");
        if (comp instanceof Map<?, ?> compMap) {
            final Object compId = ((Map<String, Object>) compMap).get("id");
            if (compId != null && !String.valueOf(compId).isBlank()) {
                return String.valueOf(compId);
            }
        }
        return null;
    }

    /** Extracts {@code processGroupFlow.flow} from a {@code getProcessGroupFlow()} response. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractFlow(final Map<String, Object> response) {
        final Object pgFlowRaw = response.get("processGroupFlow");
        if (pgFlowRaw instanceof Map<?, ?> pgFlow) {
            final Object flowRaw = ((Map<String, Object>) pgFlow).get("flow");
            if (flowRaw instanceof Map<?, ?> flow) {
                return (Map<String, Object>) flow;
            }
        }
        return Map.of();
    }
}
