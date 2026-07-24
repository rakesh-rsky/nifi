/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.nifi.copilot.builder;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ControllerServiceDeployerTest {

    private static final String SECRET = "do-not-disclose";

    @Test
    void reusesServiceWhenRequestedPropertiesMatch() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.listControllerServices("pg")).thenReturn(List.of(existingService(Map.of("url", "https://example"))));

        new ControllerServiceDeployer().deployAll(
                List.of(serviceSpec(Map.of("url", "https://example"))),
                "pg", new OwnershipLedger("pg"), new ComponentResolver(), nifi,
                new FlowDeploymentMetricsRegistry());

        verify(nifi).enableControllerService("cs-1");
        verify(nifi, never()).createControllerService(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyMap());
    }

    @Test
    void rejectsPropertyMismatchWithoutDisclosingValues() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.listControllerServices("pg")).thenReturn(
                List.of(existingService(Map.of("password", "existing-secret", "url", "old-url"))));

        final IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
                new ControllerServiceDeployer().deployAll(
                        List.of(serviceSpec(Map.of("password", SECRET, "url", "new-url"))),
                        "pg", new OwnershipLedger("pg"), new ComponentResolver(), nifi,
                        new FlowDeploymentMetricsRegistry()));

        assertTrue(failure.getMessage().contains("password"));
        assertTrue(failure.getMessage().contains("url"));
        assertTrue(failure.getMessage().contains("<provided>"));
        assertFalse(failure.getMessage().contains(SECRET));
        assertFalse(failure.getMessage().contains("existing-secret"));
        assertFalse(failure.getMessage().contains("new-url"));
        verify(nifi, never()).enableControllerService("cs-1");
    }

    @Test
    void laterMismatchPreflightCausesNoEarlierOrDependencyMutation() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupId("pg")).thenReturn("pg");
        when(nifi.listControllerServices("pg")).thenReturn(List.of(
                existingService("cs-1", "First Service", Map.of("url", "same")),
                existingService("cs-2", "Second Service", Map.of("url", "old"))));

        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("parameter_context",
                Map.of("name", "Parameters", "parameters", Map.of("key", "value")));
        specification.put("controller_services", List.of(
                serviceSpec("spec-1", "First Service", Map.of("url", "same")),
                serviceSpec("spec-2", "Second Service", Map.of("url", "new"))));

        final FlowDeploymentCoordinator coordinator =
                new FlowDeploymentCoordinator(LayoutMode.DISABLED);
        final DeploymentState state = coordinator.prepare(
                new DeploymentContext(specification, nifi, "pg", null, 0, false, false),
                new FlowDeploymentMetricsRegistry());

        assertThrows(IllegalStateException.class, () -> coordinator.deploy(state));

        verify(nifi, never()).createProcessGroup(anyString(), anyString(), anyDouble(), anyDouble());
        verify(nifi, never()).createParameterContext(anyString(), anyMap(), any());
        verify(nifi, never()).bindParameterContextToProcessGroup(anyString(), anyString());
        verify(nifi, never()).createControllerService(anyString(), anyString(), anyString(), anyMap());
        verify(nifi, never()).enableControllerService(anyString());
        verify(nifi, never()).getProcessGroupFlow(anyString());
    }

    @Test
    void preservesNullSensitivePropertyWhenReferenceMapIsPopulated() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.listControllerServices("pg")).thenReturn(List.of(
                Map.of("id", "cs-existing", "name", "Shared Service", "type", "example.Service",
                        "state", "ENABLED", "properties", Map.of())));
        when(nifi.createControllerService(eq("pg"), eq("example.Service"), eq("Dependent Service"), anyMap()))
                .thenReturn(Map.of("id", "cs-created"));

        final Map<String, Object> dependentProperties = new LinkedHashMap<>();
        dependentProperties.put("service", "spec-shared");
        dependentProperties.put("password", null);
        new ControllerServiceDeployer().deployAll(
                List.of(
                        serviceSpec("spec-shared", "Shared Service", Map.of()),
                        serviceSpec("spec-dependent", "Dependent Service", dependentProperties)),
                "pg", new OwnershipLedger("pg"), new ComponentResolver(), nifi,
                new FlowDeploymentMetricsRegistry());

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> propertiesCaptor =
                ArgumentCaptor.forClass(Map.class);
        verify(nifi).createControllerService(
                eq("pg"), eq("example.Service"), eq("Dependent Service"), propertiesCaptor.capture());
        assertEquals("cs-existing", propertiesCaptor.getValue().get("service"));
        assertTrue(propertiesCaptor.getValue().containsKey("password"));
        assertNull(propertiesCaptor.getValue().get("password"));
    }

    private static Map<String, Object> serviceSpec(final Map<String, Object> properties) {
        return serviceSpec("spec-cs", "Shared Service", properties);
    }

    private static Map<String, Object> serviceSpec(
            final String id, final String name, final Map<String, Object> properties) {
        return Map.of("id", id, "name", name, "type", "example.Service", "properties", properties);
    }

    private static Map<String, Object> existingService(final Map<String, Object> properties) {
        return existingService("cs-1", "Shared Service", properties);
    }

    private static Map<String, Object> existingService(
            final String id, final String name, final Map<String, Object> properties) {
        return Map.of("id", id, "name", name, "type", "example.Service",
                "state", "DISABLED", "properties", properties);
    }
}
