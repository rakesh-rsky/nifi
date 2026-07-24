/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.nifi.copilot.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HttpNiFiClientPhase5Test {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void listControllerServicesPreservesComponentProperties() throws Exception {
        startServer(exchange -> respond(exchange, 200, """
                {"controllerServices":[{"id":"cs-1","component":{
                  "name":"Service","type":"example.Service","state":"ENABLED",
                  "parentGroupId":"pg","properties":{"url":"https://example","password":null}
                }}]}"""));

        final List<Map<String, Object>> services = newClient().listControllerServices("pg");

        assertEquals(Map.of("url", "https://example"), withoutNullValues(propertiesOf(services.get(0))));
        assertTrue(propertiesOf(services.get(0)).containsKey("password"));
    }

    @Test
    void failedCacheInitializationPublishesNothingAndCanBeRetried() throws Exception {
        final AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            final int request = requests.incrementAndGet();
            respond(exchange, 200, request == 1
                    ? """
                      {"processorTypes":[
                        {"type":"example.Partial","bundle":{"group":"g","artifact":"a","version":"1"}},
                        "invalid"
                      ]}"""
                    : """
                      {"processorTypes":[
                        {"type":"example.Retry","bundle":{"group":"g","artifact":"a","version":"1"}}
                      ]}""");
        });
        final HttpNiFiClient client = newClient();

        assertThrows(IllegalStateException.class, client::ensureTypeCache);
        client.ensureTypeCache();
        client.ensureTypeCache();

        assertEquals(2, requests.get());
        assertThrows(IllegalArgumentException.class, () ->
                client.createProcessor("pg", "example.Partial", "partial", 0D, 0D, Map.of()));
    }

    @Test
    void concurrentFirstUseInitializesCacheOnce() throws Exception {
        final AtomicInteger requests = new AtomicInteger();
        final CountDownLatch requestStarted = new CountDownLatch(1);
        final CountDownLatch releaseResponse = new CountDownLatch(1);
        startServer(exchange -> {
            requests.incrementAndGet();
            requestStarted.countDown();
            try {
                releaseResponse.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted", e);
            }
            respond(exchange, 200, """
                    {"processorTypes":[
                      {"type":"example.Concurrent","bundle":{"group":"g","artifact":"a","version":"1"}}
                    ]}""");
        });
        final HttpNiFiClient client = newClient();
        final ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    client.ensureTypeCache();
                    return null;
                }));
            }
            start.countDown();
            assertTrue(requestStarted.await(5, TimeUnit.SECONDS));
            releaseResponse.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            releaseResponse.countDown();
            executor.shutdownNow();
        }

        assertEquals(1, requests.get());
    }

    @Test
    void connectionBendUpdateRereadsRevisionAfterConflict() throws Exception {
        final AtomicInteger gets = new AtomicInteger();
        final AtomicInteger puts = new AtomicInteger();
        final List<String> requestBodies = new CopyOnWriteArrayList<>();
        startServer(exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                final int version = gets.incrementAndGet();
                respond(exchange, 200, """
                        {"revision":{"version":%d},"component":{"id":"conn-1","bends":[]}}
                        """.formatted(version));
                return;
            }
            requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            if (puts.incrementAndGet() == 1) {
                respond(exchange, 409, "Invalid Revision");
            } else {
                respond(exchange, 200, """
                        {"revision":{"version":3},"component":{"id":"conn-1"}}
                        """);
            }
        });
        final NiFiRetryPolicy retryPolicy = mock(NiFiRetryPolicy.class);
        when(retryPolicy.maxAttempts()).thenReturn(2);
        when(retryPolicy.computeDelayMillis(anyInt(), anyLong())).thenReturn(0L);
        final HttpNiFiClient client = newClient(retryPolicy);

        client.updateConnection("conn-1", Map.of(
                "bends", List.of(Map.of("x", 50, "y", 100))));

        assertEquals(2, gets.get());
        assertEquals(2, puts.get());
        assertTrue(requestBodies.get(0).contains("\"version\":1"));
        assertTrue(requestBodies.get(1).contains("\"version\":2"));
        assertTrue(requestBodies.get(1).contains("\"bends\""));
    }

    private void startServer(final ExchangeHandler apiHandler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/nifi-api/access/token", exchange -> respond(exchange, 200, "token"));
        server.createContext("/nifi-api/", apiHandler::handle);
        server.start();
    }

    private HttpNiFiClient newClient() {
        final NiFiRetryPolicy retryPolicy = mock(NiFiRetryPolicy.class);
        when(retryPolicy.maxAttempts()).thenReturn(1);
        return newClient(retryPolicy);
    }

    private HttpNiFiClient newClient(final NiFiRetryPolicy retryPolicy) {
        final NiFiConfigResolver config = mock(NiFiConfigResolver.class);
        when(config.resolveBaseUrl()).thenReturn("http://localhost:" + server.getAddress().getPort());
        when(config.resolveConnectTimeoutSeconds()).thenReturn(5);
        when(config.resolveRequestTimeoutSeconds()).thenReturn(5);
        when(config.verifySsl()).thenReturn(true);
        when(config.resolveUsername()).thenReturn("user");
        when(config.resolvePassword()).thenReturn("password");
        return new HttpNiFiClient(config, retryPolicy, mock(NiFiAsyncRequestExecutor.class));
    }

    private static Map<String, Object> propertiesOf(final Map<String, Object> service) {
        final Object properties = service.get("properties");
        if (!(properties instanceof Map<?, ?> rawProperties)) {
            throw new AssertionError("Expected controller-service properties");
        }
        final Map<String, Object> result = new java.util.LinkedHashMap<>();
        rawProperties.forEach((key, value) -> result.put(String.valueOf(key), value));
        return result;
    }

    private static Map<String, Object> withoutNullValues(final Map<String, Object> source) {
        return source.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static void respond(final HttpExchange exchange, final int status, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
