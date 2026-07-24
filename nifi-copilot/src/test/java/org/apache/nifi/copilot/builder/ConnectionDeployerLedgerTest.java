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
import org.mockito.InOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Verifies that {@link ConnectionDeployer#deploy} records created and updated connection IDs
 * and both connection endpoints in the {@link OwnershipLedger}, driven by the real deployer
 * rather than by manually adding IDs.
 */
class ConnectionDeployerLedgerTest {

    private static final String PG_ID = "pg-deployer-test";

    private NiFiClientOperations nifi;
    private OwnershipLedger ledger;
    private ComponentRegistry registry;
    private ComponentResolver resolver;
    private FlowDeploymentMetricsRegistry metrics;

    @BeforeEach
    void setUp() {
        nifi = mock(NiFiClientOperations.class);
        ledger = new OwnershipLedger(PG_ID);
        registry = new ComponentRegistry();
        resolver = new ComponentResolver();
        metrics = new FlowDeploymentMetricsRegistry();

        registry.register("source-spec", "source-nifi", "PROCESSOR");
        registry.register("dest-spec",   "dest-nifi",   "PROCESSOR");
    }

    // -------------------------------------------------------------------------
    // Newly created connection
    // -------------------------------------------------------------------------

    @Test
    void createdConnectionIdAppearsInLedger() {
        when(nifi.createConnection(PG_ID, "source-nifi", "PROCESSOR",
                "dest-nifi", "PROCESSOR", List.of("success")))
                .thenReturn(Map.of("id", "conn-nifi"));

        ConnectionDeployer.deploy(
                List.of(Map.of("from", "source-spec", "to", "dest-spec")),
                PG_ID, registry, ledger, resolver, nifi, new ArrayList<>(), metrics);

        assertTrue(ledger.createdConnectionIds().contains("conn-nifi"),
                "Created connection ID must appear in createdConnectionIds");
    }

    @Test
    void createdConnectionAddsConnectionAndBothEndpointsToChangedIds() {
        when(nifi.createConnection(PG_ID, "source-nifi", "PROCESSOR",
                "dest-nifi", "PROCESSOR", List.of("success")))
                .thenReturn(Map.of("id", "conn-nifi"));

        ConnectionDeployer.deploy(
                List.of(Map.of("from", "source-spec", "to", "dest-spec")),
                PG_ID, registry, ledger, resolver, nifi, new ArrayList<>(), metrics);

        Set<String> changed = ledger.changedCanvasIds();
        assertTrue(changed.contains("conn-nifi"),   "connection ID must be in changedCanvasIds");
        assertTrue(changed.contains("source-nifi"), "source endpoint must be in changedCanvasIds");
        assertTrue(changed.contains("dest-nifi"),   "destination endpoint must be in changedCanvasIds");
    }

    @Test
    void rejectsDifferentReferencesThatResolveToSameProcessor() {
        registry.register("source-alias", "source-nifi", "PROCESSOR");

        assertThrows(IllegalStateException.class, () -> ConnectionDeployer.deploy(
                List.of(Map.of("from", "source-spec", "to", "source-alias")),
                PG_ID, registry, ledger, resolver, nifi, new ArrayList<>(), metrics));
        verify(nifi, never()).createConnection(anyString(), anyString(), anyString(),
                anyString(), anyString(), anyList());
    }

    @Test
    void releasesOverlappingRelationshipBeforeCreatingSeparateConnection() {
        registry.register("old-log", "old-log-nifi", "PROCESSOR");
        registry.register("new-log", "new-log-nifi", "PROCESSOR");
        List<Map<String, Object>> existing = existingConnections(
                "existing-conn", "source-nifi", "old-log-nifi", List.of("success", "failure"));
        when(nifi.createConnection(PG_ID, "source-nifi", "PROCESSOR",
                "new-log-nifi", "PROCESSOR", List.of("success")))
                .thenReturn(Map.of("id", "new-conn"));

        ConnectionDeployer.deploy(
                List.of(Map.of("from", "source-spec", "to", "new-log",
                        "relationships", List.of("success"))),
                PG_ID, registry, ledger, resolver, nifi, existing, metrics);

        InOrder inOrder = inOrder(nifi);
        inOrder.verify(nifi).updateConnection("existing-conn",
                Map.of("selectedRelationships", List.of("failure")));
        inOrder.verify(nifi).createConnection(PG_ID, "source-nifi", "PROCESSOR",
                "new-log-nifi", "PROCESSOR", List.of("success"));
    }

    @Test
    void repointsExistingConnectionWhenAllRelationshipsMove() {
        registry.register("old-log", "old-log-nifi", "PROCESSOR");
        registry.register("new-log", "new-log-nifi", "PROCESSOR");
        List<Map<String, Object>> existing = existingConnections(
                "existing-conn", "source-nifi", "old-log-nifi", List.of("success", "failure"));

        int created = ConnectionDeployer.deploy(
                List.of(Map.of("from", "source-spec", "to", "new-log",
                        "relationships", List.of("success", "failure"))),
                PG_ID, registry, ledger, resolver, nifi, existing, metrics);

        assertEquals(0, created);
        verify(nifi).updateConnection("existing-conn", Map.of(
                "destination", Map.of("id", "new-log-nifi", "type", "PROCESSOR"),
                "selectedRelationships", List.of("success", "failure")));
        verify(nifi, never()).createConnection(anyString(), anyString(), anyString(),
                anyString(), anyString(), anyList());
    }

    // -------------------------------------------------------------------------
    // Updated (reused) connection
    // -------------------------------------------------------------------------

    @Test
    void updatedConnectionIsNotInCreatedList() {
        List<Map<String, Object>> existing = existingConnections("existing-conn",
                "source-nifi", "dest-nifi");

        ConnectionDeployer.deploy(
                List.of(Map.of("from", "source-spec", "to", "dest-spec")),
                PG_ID, registry, ledger, resolver, nifi, existing, metrics);

        assertFalse(ledger.createdConnectionIds().contains("existing-conn"),
                "Updated (reused) connection must NOT appear in createdConnectionIds");
    }

    @Test
    void updatedConnectionAddsConnectionAndBothEndpointsToChangedIds() {
        List<Map<String, Object>> existing = existingConnections("existing-conn",
                "source-nifi", "dest-nifi");

        ConnectionDeployer.deploy(
                List.of(Map.of("from", "source-spec", "to", "dest-spec")),
                PG_ID, registry, ledger, resolver, nifi, existing, metrics);

        Set<String> changed = ledger.changedCanvasIds();
        assertTrue(changed.contains("existing-conn"), "connection ID must be in changedCanvasIds");
        assertTrue(changed.contains("source-nifi"),   "source endpoint must be in changedCanvasIds");
        assertTrue(changed.contains("dest-nifi"),     "destination endpoint must be in changedCanvasIds");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static List<Map<String, Object>> existingConnections(
            final String connId, final String sourceNifiId, final String destNifiId) {
        return existingConnections(connId, sourceNifiId, destNifiId, List.of("success"));
    }

    private static List<Map<String, Object>> existingConnections(
            final String connId, final String sourceNifiId, final String destNifiId,
            final List<String> relationships) {
        Map<String, Object> comp = new java.util.LinkedHashMap<>();
        comp.put("id", connId);
        comp.put("source", Map.of("id", sourceNifiId, "type", "PROCESSOR"));
        comp.put("destination", Map.of("id", destNifiId, "type", "PROCESSOR"));
        comp.put("selectedRelationships", relationships);
        Map<String, Object> entity = new java.util.LinkedHashMap<>();
        entity.put("id", connId);
        entity.put("component", comp);
        entity.put("revision", Map.of("version", 1));
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(entity);
        return list;
    }
}
