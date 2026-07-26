package org.apache.nifi.copilot.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HttpNiFiClientCapabilityTest {
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void discoversTypesAndEncodesEveryDefinitionPathSegment() throws Exception {
        final AtomicReference<String> rawPath = new AtomicReference<>();
        startServer(exchange -> {
            rawPath.set(exchange.getRequestURI().getRawPath());
            if (rawPath.get().endsWith("/processor-types")) {
                respond(exchange, 200, """
                        {"processorTypes":[{"type":"example.Type","bundle":{
                          "group":"group name","artifact":"artifact/name","version":"1+2"}}]}""");
            } else {
                respond(exchange, 200, """
                        {"type":"example.Type","propertyDescriptors":{}}""");
            }
        });
        final HttpNiFiClient client = newClient();

        assertEquals(1, client.listProcessorTypes().size());
        client.getProcessorDefinition("group name", "artifact/name", "1+2", "example.Type");

        assertTrue(rawPath.get().endsWith(
                "/flow/processor-definition/group%20name/artifact%2Fname/1%2B2/example.Type"));
    }

    @Test
    void acceptsDefinitionWithoutDescriptorsForProcessorWithoutProperties() throws Exception {
        startServer(exchange -> respond(exchange, 200, "{\"type\":\"example.Type\"}"));
        final HttpNiFiClient client = newClient();

        final Map<String, Object> definition =
                client.getProcessorDefinition("g", "a", "1", "example.Type");

        assertEquals(Map.of(), definition.get("propertyDescriptors"));
    }

    @Test
    void rejectsMissingTypeArrayAndEmptyDefinition() throws Exception {
        startServer(exchange -> respond(exchange, 200,
                exchange.getRequestURI().getPath().endsWith("processor-types")
                        ? "{}" : "{}"));
        final HttpNiFiClient client = newClient();

        assertThrows(CapabilityDiscoveryException.class, client::listProcessorTypes);
        assertThrows(CapabilityDiscoveryException.class,
                () -> client.getProcessorDefinition("g", "a", "1", "example.Type"));
    }

    private void startServer(final Handler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/nifi-api/access/token", exchange -> respond(exchange, 200, "token"));
        server.createContext("/nifi-api/", handler::handle);
        server.start();
    }

    private HttpNiFiClient newClient() {
        final NiFiConfigResolver config = mock(NiFiConfigResolver.class);
        when(config.resolveBaseUrl()).thenReturn("http://localhost:" + server.getAddress().getPort());
        when(config.resolveConnectTimeoutSeconds()).thenReturn(5);
        when(config.resolveRequestTimeoutSeconds()).thenReturn(5);
        when(config.verifySsl()).thenReturn(true);
        when(config.resolveUsername()).thenReturn("user");
        when(config.resolvePassword()).thenReturn("password");
        final NiFiRetryPolicy retry = mock(NiFiRetryPolicy.class);
        when(retry.maxAttempts()).thenReturn(1);
        return new HttpNiFiClient(config, retry, mock(NiFiAsyncRequestExecutor.class));
    }

    private static void respond(final HttpExchange exchange, final int status, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
