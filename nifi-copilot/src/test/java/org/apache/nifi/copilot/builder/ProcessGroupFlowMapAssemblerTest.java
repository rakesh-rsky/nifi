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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ProcessGroupFlowMapAssembler}.
 *
 * <p>Tests cover: empty root group, single-level child, recursive two-level nesting,
 * cycle detection, and depth-guard enforcement.
 */
class ProcessGroupFlowMapAssemblerTest {

    // -------------------------------------------------------------------------
    // Argument validation
    // -------------------------------------------------------------------------

    @Test
    void nullRootIdThrowsIllegalArgumentException() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        assertThrows(IllegalArgumentException.class, () -> ProcessGroupFlowMapAssembler.assemble(null, nifi));
    }

    @Test
    void blankRootIdThrowsIllegalArgumentException() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        assertThrows(IllegalArgumentException.class, () -> ProcessGroupFlowMapAssembler.assemble("   ", nifi));
    }

    // -------------------------------------------------------------------------
    // Empty root group
    // -------------------------------------------------------------------------

    @Test
    void emptyRootGroupReturnsRootResponseUnchanged() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        Map<String, Object> rootResponse = pgFlowResponse("root", null, emptyFlow());
        when(nifi.getProcessGroupFlow("root")).thenReturn(rootResponse);

        Map<String, Object> result = ProcessGroupFlowMapAssembler.assemble("root", nifi);

        assertNotNull(result);
        assertEquals("root", extractId(result));
        verify(nifi, times(1)).getProcessGroupFlow("root");
        verifyNoMoreInteractions(nifi);
    }

    // -------------------------------------------------------------------------
    // Single-level child groups
    // -------------------------------------------------------------------------

    @Test
    void childWithEmptyFlowGetsFlowInjected() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);

        // Root response with one child that has no "flow" key
        Map<String, Object> childEntity = childGroupEntity("child1", null);
        Map<String, Object> rootFlow = flowWithChildren(List.of(childEntity));
        when(nifi.getProcessGroupFlow("root")).thenReturn(pgFlowResponse("root", null, rootFlow));

        // Child's own response with a processor
        Map<String, Object> childOwnFlow = flowWithProcessors(List.of(processorEntity("proc1")));
        when(nifi.getProcessGroupFlow("child1")).thenReturn(pgFlowResponse("child1", "root", childOwnFlow));

        Map<String, Object> result = ProcessGroupFlowMapAssembler.assemble("root", nifi);

        // The child entity in the result should have "flow" injected
        List<?> children = extractChildren(result);
        assertEquals(1, children.size());
        Map<?, ?> enrichedChild = (Map<?, ?>) children.get(0);
        assertNotNull(enrichedChild.get("flow"), "child entity must have flow injected");
    }

    @Test
    void multipleChildrenAllGetFlowsInjected() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);

        Map<String, Object> rootFlow = flowWithChildren(List.of(
                childGroupEntity("child1", null),
                childGroupEntity("child2", null),
                childGroupEntity("child3", null)
        ));
        when(nifi.getProcessGroupFlow("root")).thenReturn(pgFlowResponse("root", null, rootFlow));
        when(nifi.getProcessGroupFlow("child1")).thenReturn(pgFlowResponse("child1", "root", emptyFlow()));
        when(nifi.getProcessGroupFlow("child2")).thenReturn(pgFlowResponse("child2", "root", emptyFlow()));
        when(nifi.getProcessGroupFlow("child3")).thenReturn(pgFlowResponse("child3", "root", emptyFlow()));

        Map<String, Object> result = ProcessGroupFlowMapAssembler.assemble("root", nifi);

        List<?> children = extractChildren(result);
        assertEquals(3, children.size());
        for (Object raw : children) {
            assertNotNull(((Map<?, ?>) raw).get("flow"), "every child must have flow injected");
        }
        verify(nifi, times(1)).getProcessGroupFlow("root");
        verify(nifi, times(1)).getProcessGroupFlow("child1");
        verify(nifi, times(1)).getProcessGroupFlow("child2");
        verify(nifi, times(1)).getProcessGroupFlow("child3");
    }

    // -------------------------------------------------------------------------
    // Recursive two-level nesting
    // -------------------------------------------------------------------------

    @Test
    void grandchildFlowIsInjectedRecursively() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);

        // root → child → grandchild
        Map<String, Object> rootFlow = flowWithChildren(List.of(childGroupEntity("child", null)));
        when(nifi.getProcessGroupFlow("root")).thenReturn(pgFlowResponse("root", null, rootFlow));

        Map<String, Object> childFlow = flowWithChildren(List.of(childGroupEntity("grandchild", null)));
        when(nifi.getProcessGroupFlow("child")).thenReturn(pgFlowResponse("child", "root", childFlow));

        Map<String, Object> grandchildFlow = flowWithProcessors(List.of(processorEntity("proc-gc")));
        when(nifi.getProcessGroupFlow("grandchild")).thenReturn(pgFlowResponse("grandchild", "child", grandchildFlow));

        Map<String, Object> result = ProcessGroupFlowMapAssembler.assemble("root", nifi);

        // root level has child
        List<?> rootChildren = extractChildren(result);
        assertEquals(1, rootChildren.size());

        // child has grandchild injected
        Map<?, ?> enrichedChild = (Map<?, ?>) rootChildren.get(0);
        Map<?, ?> injectedChildFlow = (Map<?, ?>) enrichedChild.get("flow");
        assertNotNull(injectedChildFlow, "child must have flow");
        List<?> grandchildren = (List<?>) injectedChildFlow.get("processGroups");
        assertNotNull(grandchildren, "child flow must contain processGroups");
        assertFalse(grandchildren.isEmpty(), "grandchild must be present in child flow");

        Map<?, ?> enrichedGrandchild = (Map<?, ?>) grandchildren.get(0);
        assertNotNull(enrichedGrandchild.get("flow"), "grandchild must have flow injected");

        verify(nifi, times(1)).getProcessGroupFlow("root");
        verify(nifi, times(1)).getProcessGroupFlow("child");
        verify(nifi, times(1)).getProcessGroupFlow("grandchild");
    }

    // -------------------------------------------------------------------------
    // Cycle detection
    // -------------------------------------------------------------------------

    @Test
    void selfReferencingChildDetectedAsCycle() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);

        // root response lists root itself as a child (degenerate cycle)
        Map<String, Object> rootFlow = flowWithChildren(List.of(childGroupEntity("root", null)));
        when(nifi.getProcessGroupFlow("root")).thenReturn(pgFlowResponse("root", null, rootFlow));

        assertThrows(IllegalStateException.class,
                () -> ProcessGroupFlowMapAssembler.assemble("root", nifi),
                "self-referencing child must be detected as a cycle");
    }

    @Test
    void circularChildReferenceDetectedAsCycle() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);

        // root → child → root (cycle)
        Map<String, Object> rootFlow = flowWithChildren(List.of(childGroupEntity("child", null)));
        when(nifi.getProcessGroupFlow("root")).thenReturn(pgFlowResponse("root", null, rootFlow));

        Map<String, Object> childFlow = flowWithChildren(List.of(childGroupEntity("root", null)));
        when(nifi.getProcessGroupFlow("child")).thenReturn(pgFlowResponse("child", "root", childFlow));

        assertThrows(IllegalStateException.class,
                () -> ProcessGroupFlowMapAssembler.assemble("root", nifi),
                "circular child reference must be detected as a cycle");
    }

    // -------------------------------------------------------------------------
    // Depth guard
    // -------------------------------------------------------------------------

    @Test
    void depthGuardTriggersAtMaxDepthPlusOne() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        // Build a linear chain of depth MAX_DEPTH + 1
        int limit = ProcessGroupFlowMapAssembler.MAX_DEPTH + 1;
        for (int i = 0; i < limit; i++) {
            String pgId = "pg" + i;
            String childId = "pg" + (i + 1);
            Map<String, Object> flow = flowWithChildren(List.of(childGroupEntity(childId, null)));
            when(nifi.getProcessGroupFlow(pgId)).thenReturn(pgFlowResponse(pgId, null, flow));
        }
        // The leaf at depth limit+1 returns an empty flow
        when(nifi.getProcessGroupFlow("pg" + limit)).thenReturn(pgFlowResponse("pg" + limit, null, emptyFlow()));

        assertThrows(IllegalStateException.class,
                () -> ProcessGroupFlowMapAssembler.assemble("pg0", nifi),
                "depth guard must trigger at depth > MAX_DEPTH");
    }

    // -------------------------------------------------------------------------
    // Child entity with pre-existing "flow" key is still recursed
    // -------------------------------------------------------------------------

    @Test
    void childWithExistingFlowStillRecursesForGrandchildren() {
        NiFiClientOperations nifi = mock(NiFiClientOperations.class);

        // Simulate parent already having a partial child flow, with a grandchild that needs enrichment
        Map<String, Object> existingChildFlow = flowWithChildren(List.of(childGroupEntity("grandchild", null)));

        Map<String, Object> childEntity = new LinkedHashMap<>(childGroupEntity("child", null));
        childEntity.put("flow", existingChildFlow);

        Map<String, Object> rootFlow = flowWithChildren(List.of(childEntity));
        when(nifi.getProcessGroupFlow("root")).thenReturn(pgFlowResponse("root", null, rootFlow));

        // Child's own fresh response (the assembler always re-fetches)
        Map<String, Object> childOwnFlow = flowWithChildren(List.of(childGroupEntity("grandchild", null)));
        when(nifi.getProcessGroupFlow("child")).thenReturn(pgFlowResponse("child", "root", childOwnFlow));

        Map<String, Object> grandchildFlow = emptyFlow();
        when(nifi.getProcessGroupFlow("grandchild")).thenReturn(pgFlowResponse("grandchild", "child", grandchildFlow));

        // Should not throw; grandchild flow must be injected
        assertDoesNotThrow(() -> ProcessGroupFlowMapAssembler.assemble("root", nifi));
        verify(nifi, times(1)).getProcessGroupFlow("grandchild");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static Map<String, Object> pgFlowResponse(String id, String parentId, Map<String, Object> flow) {
        Map<String, Object> pgFlow = new LinkedHashMap<>();
        pgFlow.put("id", id);
        if (parentId != null) pgFlow.put("parentGroupId", parentId);
        pgFlow.put("flow", flow);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("processGroupFlow", pgFlow);
        return root;
    }

    private static Map<String, Object> emptyFlow() {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("processors", List.of());
        f.put("inputPorts", List.of());
        f.put("outputPorts", List.of());
        f.put("funnels", List.of());
        f.put("labels", List.of());
        f.put("remoteProcessGroups", List.of());
        f.put("connections", List.of());
        f.put("processGroups", List.of());
        return f;
    }

    private static Map<String, Object> flowWithChildren(List<Object> children) {
        Map<String, Object> f = emptyFlow();
        f.put("processGroups", new ArrayList<>(children));
        return f;
    }

    private static Map<String, Object> flowWithProcessors(List<Object> processors) {
        Map<String, Object> f = emptyFlow();
        f.put("processors", new ArrayList<>(processors));
        return f;
    }

    private static Map<String, Object> childGroupEntity(String id, String parentId) {
        Map<String, Object> comp = new LinkedHashMap<>();
        comp.put("id", id);
        comp.put("name", id + "-name");
        if (parentId != null) comp.put("parentGroupId", parentId);
        comp.put("position", Map.of("x", 0.0, "y", 0.0));
        Map<String, Object> entity = new LinkedHashMap<>();
        entity.put("id", id);
        entity.put("revision", Map.of("version", 1));
        entity.put("component", comp);
        return entity;
    }

    private static Map<String, Object> processorEntity(String id) {
        Map<String, Object> comp = new LinkedHashMap<>();
        comp.put("id", id);
        comp.put("name", id + "-name");
        comp.put("type", "org.apache.nifi.processors.Test");
        comp.put("position", Map.of("x", 0.0, "y", 0.0));
        Map<String, Object> entity = new LinkedHashMap<>();
        entity.put("id", id);
        entity.put("revision", Map.of("version", 1));
        entity.put("component", comp);
        return entity;
    }

    @SuppressWarnings("unchecked")
    private static String extractId(Map<String, Object> result) {
        Map<String, Object> pgFlow = (Map<String, Object>) result.get("processGroupFlow");
        return pgFlow != null ? String.valueOf(pgFlow.get("id")) : null;
    }

    @SuppressWarnings("unchecked")
    private static List<?> extractChildren(Map<String, Object> result) {
        Map<String, Object> pgFlow = (Map<String, Object>) result.get("processGroupFlow");
        if (pgFlow == null) return List.of();
        Map<String, Object> flow = (Map<String, Object>) pgFlow.get("flow");
        if (flow == null) return List.of();
        Object children = flow.get("processGroups");
        return children instanceof List<?> l ? l : List.of();
    }
}
