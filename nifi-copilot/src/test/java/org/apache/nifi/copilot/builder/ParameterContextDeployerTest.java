/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.nifi.copilot.builder;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.Test;

class ParameterContextDeployerTest {

    @Test
    void reusesContextWhenSensitiveParameterValueIsMasked() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.listParameterContexts()).thenReturn(List.of(parameterContext(
                "pc-1", "Parameters",
                parameter("url", "jdbc:postgresql://localhost/db", false),
                parameter("password", null, true))));

        ParameterContextDeployer.deploy(
                parameterContextSpec("Parameters", Map.of(
                        "url", "jdbc:postgresql://localhost/db",
                        "password", "new-secret")),
                "pg", null, new OwnershipLedger("pg"), new ComponentResolver(), nifi,
                new FlowDeploymentMetricsRegistry());

        verify(nifi, never()).createParameterContext(anyString(), anyMap(), anyString());
        verify(nifi).bindParameterContextToProcessGroup("pg", "pc-1");
    }

    @Test
    void createsUniquelyNamedContextWhenSharedContextIsIncompatible() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.listParameterContexts()).thenReturn(List.of(parameterContext(
                "pc-1", "Parameters", parameter("url", "old-url", false))));
        when(nifi.createParameterContext(eq("Parameters (2)"), anyMap(), eq("")))
                .thenReturn(Map.of("id", "pc-2"));

        ParameterContextDeployer.deploy(
                parameterContextSpec("Parameters", Map.of("url", "new-url")),
                "pg", null, new OwnershipLedger("pg"), new ComponentResolver(), nifi,
                new FlowDeploymentMetricsRegistry());

        verify(nifi).createParameterContext(
                "Parameters (2)", Map.of("url", "new-url"), "");
        verify(nifi).bindParameterContextToProcessGroup("pg", "pc-2");
    }

    private static Map<String, Object> parameterContextSpec(
            final String name, final Map<String, Object> parameters) {
        return Map.of("name", name, "parameters", parameters);
    }

    @SafeVarargs
    private static Map<String, Object> parameterContext(
            final String id, final String name, final Map<String, Object>... parameters) {
        return Map.of("id", id, "component", Map.of(
                "name", name,
                "parameters", List.of(parameters)));
    }

    private static Map<String, Object> parameter(
            final String name, final String value, final boolean sensitive) {
        final Map<String, Object> details = new java.util.LinkedHashMap<>();
        details.put("name", name);
        details.put("value", value);
        details.put("sensitive", sensitive);
        return Map.of("parameter", details);
    }
}
