package org.apache.nifi.copilot.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("httpNiFiClient")
public class HttpNiFiClient implements NiFiClientOperations {
    private static final Logger logger = LoggerFactory.getLogger(HttpNiFiClient.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final NiFiConfigResolver config;
    private final NiFiRetryPolicy retryPolicy;
    private final NiFiAsyncRequestExecutor asyncExecutor;
    private final HttpClient client;
    private final String baseUrl;
    private final Duration requestTimeout;
    private volatile String token;
    private final Map<String, Map<String, Object>> typeBundleCache = new HashMap<>();

    public HttpNiFiClient(final NiFiConfigResolver config, final NiFiRetryPolicy retryPolicy,
            final NiFiAsyncRequestExecutor asyncExecutor) {
        this.config = config;
        this.retryPolicy = retryPolicy;
        this.asyncExecutor = asyncExecutor;
        this.baseUrl = config.resolveBaseUrl();
        this.requestTimeout = Duration.ofSeconds(config.resolveRequestTimeoutSeconds());
        this.client = buildClient(config.verifySsl(), config.resolveConnectTimeoutSeconds());
    }

    private HttpClient buildClient(final boolean verifySsl, final long connectTimeoutSeconds) {
        try {
            final Duration connectTimeout = Duration.ofSeconds(connectTimeoutSeconds);
            if (verifySsl) {
                return HttpClient.newBuilder().connectTimeout(connectTimeout).build();
            }
            final TrustManager[] trustAll = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }

                        @Override
                        public void checkClientTrusted(final X509Certificate[] xcs, final String string) {
                        }

                        @Override
                        public void checkServerTrusted(final X509Certificate[] xcs, final String string) {
                        }
                    }
            };
            final SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAll, new SecureRandom());
            return HttpClient.newBuilder().sslContext(sc).connectTimeout(connectTimeout).build();
        } catch (Exception e) {
            throw new NiFiClientException("HTTP_CLIENT", baseUrl, 0, null, false,
                    "Failed to configure the NiFi HTTP client", e);
        }
    }

    private synchronized void ensureToken() {
        if (token != null && !token.isBlank()) {
            return;
        }

        final String username = config.resolveUsername();
        final String password = config.resolvePassword();
        if (username.isBlank() || password.isBlank()) {
            throw new NiFiClientException("POST", "/nifi-api/access/token", 0, null, false,
                    "NiFi authentication requires a username and password");
        }

        try {
            final String body = "username=" + URLEncoder.encode(username, StandardCharsets.UTF_8)
                    + "&password=" + URLEncoder.encode(password, StandardCharsets.UTF_8);
            final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/nifi-api/access/token"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .timeout(requestTimeout)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new NiFiClientException("POST", "/nifi-api/access/token", response.statusCode(), response.body(), false,
                        "NiFi authentication failed: HTTP " + response.statusCode());
            }
            token = response.body() == null ? "" : response.body().trim();
            if (token.isBlank()) {
                throw new NiFiClientException("POST", "/nifi-api/access/token", response.statusCode(), null, false,
                        "NiFi authentication returned an empty token");
            }
        } catch (NiFiClientException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NiFiClientException("POST", "/nifi-api/access/token", 0, null, false,
                    "NiFi authentication was interrupted", e);
        } catch (java.io.IOException e) {
            throw new NiFiClientException("POST", "/nifi-api/access/token", 0, null, true,
                    "NiFi authentication request failed", e);
        }
    }

    private HttpRequest.Builder buildRequestBuilder(final String path) {
        ensureToken();
        return HttpRequest.newBuilder(URI.create(baseUrl + "/nifi-api" + path))
                .header("Authorization", "Bearer " + token)
                .timeout(requestTimeout);
    }

    /**
     * Executes a GET request with transient-error retry (HTTP 429/502/503/504,
     * transport IOException/timeout) and a single concurrency-safe 401 token
     * refresh. The 401 refresh does not consume a transient-attempt slot.
     *
     * @param path API path (e.g. {@code /processors/abc})
     * @return parsed response body, or an empty map for empty 2xx responses
     * @throws NiFiClientException on non-retryable failure or exhausted attempts
     */
    private Map<String, Object> get(final String path) {
        final int maxAttempts = retryPolicy.maxAttempts();
        boolean tokenRefreshed = false;

        for (int attempt = 1; ; attempt++) {
            final HttpRequest request = buildRequestBuilder(path).GET().build();
            try {
                return sendOnce("GET", path, request);
            } catch (NiFiClientException e) {
                // One-time concurrency-safe 401 token refresh (separate from transient count)
                if (e.getHttpStatus() == 401 && !tokenRefreshed) {
                    final String usedToken = request.headers().firstValue("Authorization").orElse("");
                    synchronized (this) {
                        if (usedToken.equals("Bearer " + token)) {
                            token = null;
                        }
                    }

                    tokenRefreshed = true;
                    attempt--; // 401 refresh does not count as a transient attempt
                    continue;
                }
                if (!retryPolicy.isTransientGetError(e) || attempt >= maxAttempts) {
                    throw e;
                }
                final long delay = retryPolicy.computeDelayMillis(attempt, e.getRetryAfterMillis());
                logger.debug("GET {} transient error status={}, attempt {}/{}, delay={}ms",
                        path, e.getHttpStatus(), attempt, maxAttempts, delay);
                retryPolicy.sleep(delay);
            }
        }
    }

    private Map<String, Object> getUnauthenticated(final String path) {
        final int maxAttempts = retryPolicy.maxAttempts();
        for (int attempt = 1; ; attempt++) {
            final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/nifi-api" + path))
                    .timeout(requestTimeout)
                    .GET()
                    .build();
            try {
                return sendOnce("GET", path, request);
            } catch (NiFiClientException e) {
                if (!retryPolicy.isTransientGetError(e) || attempt >= maxAttempts) {
                    throw e;
                }
                final long delay = retryPolicy.computeDelayMillis(attempt, e.getRetryAfterMillis());
                logger.debug("GET {} transient error status={}, attempt {}/{}, delay={}ms",
                        path, e.getHttpStatus(), attempt, maxAttempts, delay);
                retryPolicy.sleep(delay);
            }
        }
    }

    private Map<String, Object> post(final String path, final Object body) {
        final String json = serialize(body);
        return executeWithRetry("POST", path, () -> buildRequestBuilder(path)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build());
    }

    private String postForText(final String path, final Object body) {
        final String json = serialize(body);
        return executeWithRetryForText("POST", path, () -> buildRequestBuilder(path)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build());
    }

    private Map<String, Object> put(final String path, final Object body) {
        final String json = serialize(body);
        return executeWithRetry("PUT", path, () -> buildRequestBuilder(path)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(json))
                .build());
    }

    private Map<String, Object> delete(final String path, final String query) {
        final String fullPath = path + (query == null || query.isBlank() ? "" : "?" + query);
        return executeWithRetry("DELETE", fullPath, () -> buildRequestBuilder(fullPath).DELETE().build());
    }

    /**
     * Issues an authenticated PUT request with no request body and no Content-Type header.
     * Retries once after clearing the token on HTTP 401. Never retries on other errors.
     *
     * @param path API path for the PUT request
     * @return parsed response body, or an empty map for empty 2xx responses
     * @throws NiFiClientException on transport or HTTP error
     */
    private Map<String, Object> putWithoutBody(final String path) {
        return executeWithRetry("PUT", path, () -> buildRequestBuilder(path)
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build());
    }

    /**
     * Executes a non-GET request built by the supplier, retrying once after
     * clearing the token on HTTP 401. Never retries on other errors.
     *
     * @param method HTTP method for logging/exceptions
     * @param path full API path for logging/exceptions
     * @param supplier builds the HttpRequest (called up to twice on 401 retry)
     * @return parsed response body, or empty map for empty 2xx
     * @throws NiFiClientException on transport or HTTP error
     */
    private Map<String, Object> executeWithRetry(final String method, final String path,
            final Supplier<HttpRequest> supplier) {
        return executeWith401Retry(method, path, supplier,
                request -> sendOnce(method, path, request));
    }

    private String executeWithRetryForText(final String method, final String path,
            final Supplier<HttpRequest> supplier) {
        return executeWith401Retry(method, path, supplier,
                request -> sendOnceForText(method, path, request));
    }

    /**
     * Shared 401-retry wrapper. Calls {@code sender} with the initial request.
     * On a 401, clears the cached token (only if it matches the Authorization used
     * in the initial request) then retries exactly once with a fresh request from
     * {@code supplier}.
     */
    private <T> T executeWith401Retry(final String method, final String path,
            final Supplier<HttpRequest> supplier,
            final Function<HttpRequest, T> sender) {
        final HttpRequest initialRequest = supplier.get();
        try {
            return sender.apply(initialRequest);
        } catch (NiFiClientException e) {
            if (e.getHttpStatus() == 401) {
                final String failedAuthorization = initialRequest.headers()
                        .firstValue("Authorization")
                        .orElse("");
                synchronized (this) {
                    if (failedAuthorization.equals("Bearer " + token)) {
                        token = null;
                    }
                }
                return sender.apply(supplier.get());
            }
            throw e;
        }
    }

    /**
     * Sends a single HTTP request and returns the parsed response body.
     * Logs request timing at DEBUG level. Parses {@code Retry-After} header
     * and attaches the parsed delay to any thrown {@link NiFiClientException}.
     *
     * @param method HTTP method name (for logging/exceptions)
     * @param path API path used for context
     * @param request request to send
     * @return parsed response body as a map, or an empty map for empty success bodies
     * @throws NiFiClientException on transport error or non-2xx HTTP status
     */
    private Map<String, Object> sendOnce(final String method, final String path, final HttpRequest request) {
        final long startNanos = System.nanoTime();
        try {
            final String body = sendRaw(method, path, request, startNanos);
            if (body == null || body.isBlank()) {
                return Map.of();
            }
            return mapper.readValue(body, new TypeReference<>() {
            });
        } catch (NiFiClientException e) {
            throw e;
        } catch (java.io.IOException e) {
            throw new NiFiClientException(method, path, 0, null, false,
                    "NiFi response could not be parsed as JSON", e);
        } catch (RuntimeException e) {
            throw new NiFiClientException(method, path, 0, null, false, "NiFi response could not be processed", e);
        }
    }

    private String sendOnceForText(final String method, final String path, final HttpRequest request) {
        final long startNanos = System.nanoTime();
        try {
            final String body = sendRaw(method, path, request, startNanos);
            return body == null ? "" : body;
        } catch (NiFiClientException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new NiFiClientException(method, path, 0, null, false, "NiFi response could not be processed", e);
        }
    }

    /**
     * Sends an HTTP request, logs timing, validates the status code, and returns
     * the raw response body string. Callers are responsible for parsing the body.
     *
     * @param method     HTTP method name (for logging/exceptions)
     * @param path       API path used for context
     * @param request    prepared request to send
     * @param startNanos {@link System#nanoTime()} snapshot taken before this call
     * @return raw response body, possibly {@code null} if the server sent no body
     * @throws NiFiClientException on non-2xx status, transport error, or interruption
     */
    private String sendRaw(final String method, final String path,
            final HttpRequest request, final long startNanos) {
        try {
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            final int status = response.statusCode();
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            logger.debug("NiFi {} {} -> {} in {}ms", method, path, status, elapsedMs);
            if (status < 200 || status >= 300) {
                final String body = response.body();
                final String retryAfterHeader = response.headers().firstValue("Retry-After").orElse(null);
                final long retryAfterMillis = retryPolicy.parseRetryAfterMillis(retryAfterHeader);
                throw new NiFiClientException(method, path, status, body, false,
                        "NiFi " + method + " " + path + " failed: HTTP " + status, retryAfterMillis);
            }
            return response.body();
        } catch (NiFiClientException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NiFiClientException(method, path, 0, null, false, "NiFi request interrupted", e);
        } catch (java.io.IOException e) {
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            logger.debug("NiFi {} {} -> IOException in {}ms", method, path, elapsedMs);
            throw new NiFiClientException(method, path, 0, null, true, "NiFi request failed", e);
        }
    }

    private String serialize(final Object body) {
        try {
            return mapper.writeValueAsString(body);
        } catch (Exception e) {
            throw new NiFiClientException("Failed to serialize request body", e);
        }
    }

    /**
     * Executes a revision-bearing mutation operation, re-reading the entity and
     * retrying on HTTP 409 (revision conflict) up to {@link NiFiRetryPolicy#maxAttempts()}
     * times. The operation lambda must re-read the latest entity revision on each
     * call; stale bodies are never replayed.
     *
     * @param description human-readable description used in debug logs
     * @param operation lambda that performs a fresh GET + mutation; must never
     *        capture stale revision state from outside
     * @throws NiFiClientException on non-409 failure or after all attempts exhausted
     */
    private void executeWithRevisionRetry(final String description, final Runnable operation) {
        final int maxAttempts = retryPolicy.maxAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                operation.run();
                return;
            } catch (NiFiClientException e) {
                if (!isRevisionConflict(e) || attempt >= maxAttempts) {
                    throw e;
                }
                logger.debug("Revision conflict on {}, attempt {}/{}", description, attempt, maxAttempts);
                retryPolicy.sleep(retryPolicy.computeDelayMillis(attempt, -1L));
            }
        }
    }

    private boolean isRevisionConflict(final NiFiClientException exception) {
        final int status = exception.getHttpStatus();
        if (status != 400 && status != 409) {
            return false;
        }
        final String responseBody = exception.getResponseBody();
        return responseBody != null
                && (responseBody.contains("Invalid Revision")
                || responseBody.contains("most up-to-date revision"));
    }

    @Override
    public String getProcessGroupId(final String pgId) {
        if (pgId != null && !"root".equals(pgId) && !pgId.isBlank()) {
            try {
                getProcessGroup(pgId);
                return pgId;
            } catch (Exception ignored) {
            }
        }
        return String.valueOf(get("/process-groups/root").get("id"));
    }

    @Override
    public Map<String, Object> createProcessGroup(final String parentPgId, final String name, final double x, final double y) {
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", Map.of("name", name, "position", Map.of("x", x, "y", y))
        );
        final Map<String, Object> data = post("/process-groups/" + parentPgId + "/process-groups", body);
        return Map.of("id", data.get("id"), "name", ((Map<?, ?>) data.get("component")).get("name"));
    }

    @Override
    public Map<String, Object> getProcessGroupFlow(final String pgId) {
        return get("/flow/process-groups/" + pgId);
    }

    @Override
    public void ensureTypeCache() {
        if (!typeBundleCache.isEmpty()) {
            return;
        }
        final Map<String, Object> data = get("/flow/processor-types");
        final List<?> types = (List<?>) data.getOrDefault("processorTypes", List.of());
        for (Object obj : types) {
            final Map<String, Object> type = (Map<String, Object>) obj;
            final String fqn = String.valueOf(type.getOrDefault("type", ""));
            final Map<String, Object> bundle = (Map<String, Object>) type.get("bundle");
            if (!fqn.isBlank() && bundle != null) {
                typeBundleCache.putIfAbsent(fqn, bundle);
            }
        }
    }

    private Map.Entry<String, Map<String, Object>> resolveBundle(final String processorType) {
        ensureTypeCache();
        if (typeBundleCache.containsKey(processorType)) {
            return Map.entry(processorType, typeBundleCache.get(processorType));
        }
        final String simple = processorType.substring(processorType.lastIndexOf('.') + 1).toLowerCase();
        for (Map.Entry<String, Map<String, Object>> entry : typeBundleCache.entrySet()) {
            final String keySimple = entry.getKey().substring(entry.getKey().lastIndexOf('.') + 1).toLowerCase();
            if (keySimple.equals(simple)) {
                return Map.entry(entry.getKey(), entry.getValue());
            }
        }
        return null;
    }

    @Override
    public Map<String, Object> createProcessor(
            final String pgId,
            final String processorType,
            final String name,
            final Double x,
            final Double y,
            final Map<String, Object> properties
    ) {
        final Map.Entry<String, Map<String, Object>> resolved = resolveBundle(processorType);
        if (resolved == null) {
            throw new IllegalArgumentException("Processor type '" + processorType + "' is not available in this NiFi instance.");
        }
        final Map<String, Object> component = new HashMap<>();
        component.put("type", resolved.getKey());
        component.put("bundle", resolved.getValue());
        component.put("name", name);
        component.put("position", Map.of("x", x, "y", y));
        if (properties != null && !properties.isEmpty()) {
            component.put("config", Map.of("properties", properties));
        }
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", component
        );
        final Map<String, Object> data = post("/process-groups/" + pgId + "/processors", body);
        final Map<String, Object> comp = (Map<String, Object>) data.get("component");
        return Map.of("id", data.get("id"), "name", comp.get("name"), "type", comp.get("type"));
    }

    @Override
    public void startProcessor(final String procId) {
        setProcessorState(procId, "RUNNING");
    }

    private void stopProcessor(final String procId) {
        setProcessorState(procId, "STOPPED");
    }

    private void setProcessorState(final String procId, final String state) {
        executeWithRevisionRetry("processor " + procId + " run-status", () -> {
            final Map<String, Object> entity = get("/processors/" + procId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "state", state
            );
            put("/processors/" + procId + "/run-status", body);
        });
    }

    @Override
    public void deleteProcessor(final String procId) {
        try {
            stopProcessor(procId);
        } catch (RuntimeException e) {
            logger.warn("Could not stop processor {} before deletion: {}", procId, e.getMessage());
        }
        executeWithRevisionRetry("delete processor " + procId, () -> {
            final Map<String, Object> entity = get("/processors/" + procId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/processors/" + procId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public boolean waitForProcessorValid(final String procId, final int timeoutSec) {
        final long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            final Map<String, Object> entity = get("/processors/" + procId);
            final Map<String, Object> component = (Map<String, Object>) entity.getOrDefault("component", Map.of());
            final List<String> errors = (List<String>) component.getOrDefault("validationErrors", List.of());
            if (errors.isEmpty()) {
                return true;
            }
            sleep(1500L);
        }
        return false;
    }

    @Override
    public void autoTerminateUnusedRelationships(final String procId, final Set<String> usedRelationships) {
        try {
            executeWithRevisionRetry("auto-terminate relationships " + procId, () -> {
                final Map<String, Object> entity = get("/processors/" + procId);
                final Map<String, Object> component = (Map<String, Object>) entity.getOrDefault("component", Map.of());
                final NiFiRevision revision = NiFiRevision.fromEntity(entity);
                final List<Map<String, Object>> relationships = (List<Map<String, Object>>) component.getOrDefault("relationships", List.of());
                final List<String> terminate = new ArrayList<>();
                for (Map<String, Object> relationship : relationships) {
                    final String name = String.valueOf(relationship.getOrDefault("name", ""));
                    if (!usedRelationships.contains(name)) {
                        terminate.add(name);
                    }
                }
                if (terminate.isEmpty()) {
                    return;
                }
                final Map<String, Object> body = Map.of(
                        "revision", revision.toMap(),
                        "component", Map.of("id", procId, "config", Map.of("autoTerminatedRelationships", terminate))
                );
                put("/processors/" + procId, body);
            });
        } catch (Exception e) {
            logger.warn("Could not auto-terminate relationships on {}: {}", procId, e.getMessage());
        }
    }

    @Override
    public Map<String, Object> createConnection(
            final String pgId,
            final String sourceId,
            final String sourceType,
            final String destId,
            final String destType,
            final List<String> relationships
    ) {
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", Map.of(
                        "source", Map.of("id", sourceId, "groupId", pgId, "type", sourceType),
                        "destination", Map.of("id", destId, "groupId", pgId, "type", destType),
                        "selectedRelationships", relationships
                )
        );
        final Map<String, Object> data = post("/process-groups/" + pgId + "/connections", body);
        return Map.of("id", data.get("id"));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void deleteConnection(final String connId) {
        try {
            final Map<String, Object> entity = get("/connections/" + connId);
            final Map<String, Object> component =
                    (Map<String, Object>) entity.getOrDefault("component", entity);
            stopConnectionEndpoint(component, "source");
            stopConnectionEndpoint(component, "destination");
        } catch (RuntimeException e) {
            logger.warn("Could not stop endpoints for connection {} before deletion: {}",
                    connId, e.getMessage());
        }
        executeWithRevisionRetry("delete connection " + connId, () -> {
            final Map<String, Object> entity = get("/connections/" + connId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/connections/" + connId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @SuppressWarnings("unchecked")
    private void stopConnectionEndpoint(final Map<String, Object> component, final String endpointKey) {
        final Map<String, Object> endpoint =
                (Map<String, Object>) component.getOrDefault(endpointKey, Map.of());
        final String id = String.valueOf(endpoint.getOrDefault("id", ""));
        final String type = String.valueOf(endpoint.getOrDefault("type", ""));
        if (!id.isBlank() && "PROCESSOR".equalsIgnoreCase(type)) {
            try {
                stopProcessor(id);
            } catch (RuntimeException ignored) {
            }
        }
    }

    @Override
    public List<Map<String, Object>> listControllerServices(final String pgId) {
        final Map<String, Object> data = get("/flow/process-groups/" + pgId + "/controller-services");
        final List<?> services = (List<?>) data.getOrDefault("controllerServices", List.of());
        final List<Map<String, Object>> out = new ArrayList<>();
        for (Object raw : services) {
            final Map<String, Object> service = (Map<String, Object>) raw;
            final Map<String, Object> component = (Map<String, Object>) service.get("component");
            final Map<String, Object> projected = new HashMap<>();
            projected.put("id", service.get("id"));
            projected.put("name", component.get("name"));
            projected.put("type", component.get("type"));
            projected.put("state", component.getOrDefault("state", "DISABLED"));
            projected.put("parentGroupId", component.get("parentGroupId"));
            out.add(projected);
        }
        return out;
    }

    @Override
    public Map<String, Object> createControllerService(
            final String pgId,
            final String serviceType,
            final String name,
            final Map<String, Object> properties
    ) {
        final Map<String, Object> component = new HashMap<>();
        component.put("type", serviceType);
        component.put("name", name);
        if (properties != null && !properties.isEmpty()) {
            component.put("properties", properties);
        }
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", component
        );
        final Map<String, Object> data = post("/process-groups/" + pgId + "/controller-services", body);
        final Map<String, Object> created = (Map<String, Object>) data.get("component");
        return Map.of("id", data.get("id"), "name", created.get("name"), "state", created.getOrDefault("state", "DISABLED"));
    }

    @Override
    public void enableControllerService(final String csId) {
        setControllerServiceState(csId, "ENABLED");
    }

    @Override
    public void disableControllerService(final String csId) {
        setControllerServiceState(csId, "DISABLED");
    }

    private void setControllerServiceState(final String csId, final String state) {
        executeWithRevisionRetry("controller service " + csId + " run-status", () -> {
            final Map<String, Object> entity = get("/controller-services/" + csId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "state", state
            );
            put("/controller-services/" + csId + "/run-status", body);
        });
        waitForControllerServiceState(csId, state, 60);
    }

    private boolean waitForControllerServiceState(final String csId, final String targetState, final int timeoutSec) {
        final long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            final Map<String, Object> component = (Map<String, Object>) get("/controller-services/" + csId).getOrDefault("component", Map.of());
            final String state = String.valueOf(component.getOrDefault("state", ""));
            if (targetState.equals(state)) {
                return true;
            }
            sleep(2000L);
        }
        return false;
    }

    @Override
    public void deleteControllerService(final String csId) {
        try {
            disableControllerService(csId);
        } catch (RuntimeException e) {
            logger.warn("Could not disable controller service {} before deletion: {}", csId, e.getMessage());
        }
        executeWithRevisionRetry("delete controller service " + csId, () -> {
            final Map<String, Object> entity = get("/controller-services/" + csId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/controller-services/" + csId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public Map<String, Object> createParameterContext(final String name, final Map<String, String> parameters, final String description) {
        final List<Map<String, Object>> paramList = new ArrayList<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            paramList.add(Map.of("parameter", Map.of("name", entry.getKey(), "value", entry.getValue(), "sensitive", false, "description", "")));
        }
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", Map.of("name", name, "description", description == null ? "" : description, "parameters", paramList)
        );
        final Map<String, Object> data = post("/parameter-contexts", body);
        final Map<String, Object> comp = (Map<String, Object>) data.get("component");
        return Map.of("id", data.get("id"), "name", comp.get("name"));
    }

    @Override
    public void bindParameterContextToProcessGroup(final String pgId, final String pcId) {
        executeWithRevisionRetry("bind parameter context to process group " + pgId, () -> {
            final Map<String, Object> entity = get("/process-groups/" + pgId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", Map.of("id", pgId, "parameterContext", Map.of("id", pcId))
            );
            put("/process-groups/" + pgId, body);
        });
    }

    @Override
    public void unbindParameterContextFromProcessGroup(final String pgId) {
        if (pgId == null || pgId.isBlank()) {
            throw new IllegalArgumentException("pgId must not be blank");
        }
        executeWithRevisionRetry("unbind parameter context from process group " + pgId, () -> {
            final Map<String, Object> entity = get("/process-groups/" + pgId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>();
            component.put("id", pgId);
            component.put("parameterContext", null);
            final Map<String, Object> body = new HashMap<>();
            body.put("revision", revision.toMap());
            body.put("component", component);
            put("/process-groups/" + pgId, body);
        });
    }

    @Override
    public void deleteParameterContext(final String pcId) {
        executeWithRevisionRetry("delete parameter context " + pcId, () -> {
            final Map<String, Object> entity = get("/parameter-contexts/" + pcId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/parameter-contexts/" + pcId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public Map<String, Object> snapshotProcessGroup(final String pgId) {
        final Map<String, Object> flowData = getProcessGroupFlow(pgId);
        final Map<String, Object> processGroupFlow = castMap(flowData.getOrDefault("processGroupFlow", Map.of()));
        final Map<String, Object> flow = castMap(processGroupFlow.getOrDefault("flow", Map.of()));

        String originalPcBindingId = null;
        final Map<String, Object> pgEntity = get("/process-groups/" + pgId);
        final Map<String, Object> comp = castMap(pgEntity.getOrDefault("component", Map.of()));
        final Object pcRef = comp.get("parameterContext");
        if (pcRef instanceof Map<?, ?>) {
            final Object pcId = ((Map<?, ?>) pcRef).get("id");
            if (pcId != null && !String.valueOf(pcId).isBlank()) {
                originalPcBindingId = String.valueOf(pcId);
            }
        }

        final Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("pg_id", pgId);
        snapshot.put("processors", flow.getOrDefault("processors", List.of()));
        snapshot.put("connections", flow.getOrDefault("connections", List.of()));
        snapshot.put("processGroups", flow.getOrDefault("processGroups", List.of()));
        snapshot.put("createdControllerServiceIds", new ArrayList<String>());
        snapshot.put("createdParameterContextIds", new ArrayList<String>());
        snapshot.put("originalPcBindingId", originalPcBindingId);
        return snapshot;
    }

    @Override
    public void restoreFromSnapshot(final Map<String, Object> snapshot) {
        final String pgId = String.valueOf(snapshot.get("pg_id"));
        final Set<String> snapProcIds = extractIds(toMapList(snapshot.getOrDefault("processors", List.of())));
        final Set<String> snapConnIds = extractIds(toMapList(snapshot.getOrDefault("connections", List.of())));
        final Set<String> snapChildPgIds = extractIds(toMapList(snapshot.getOrDefault("processGroups", List.of())));
        final List<String> createdCsIds = (List<String>) snapshot.getOrDefault("createdControllerServiceIds", List.of());
        final List<String> createdPcIds = (List<String>) snapshot.getOrDefault("createdParameterContextIds", List.of());

        final Map<String, Object> currentFlow = getProcessGroupFlow(pgId);
        final Map<String, Object> current = castMap(castMap(currentFlow.getOrDefault("processGroupFlow", Map.of()))
                .getOrDefault("flow", Map.of()));

        final List<Exception> failures = new ArrayList<>();

        // Delete newly created connections first (must precede processor deletion)
        for (Map<String, Object> conn : toMapList(current.getOrDefault("connections", List.of()))) {
            final String id = String.valueOf(conn.get("id"));
            if (!snapConnIds.contains(id)) {
                try {
                    deleteConnection(id);
                } catch (Exception e) {
                    logger.warn("Rollback: failed to delete connection {}: {}", id, e.getMessage());
                    failures.add(e);
                }
            }
        }

        // Delete newly created processors
        for (Map<String, Object> proc : toMapList(current.getOrDefault("processors", List.of()))) {
            final String id = String.valueOf(proc.get("id"));
            if (!snapProcIds.contains(id)) {
                try {
                    deleteProcessor(id);
                } catch (Exception e) {
                    logger.warn("Rollback: failed to delete processor {}: {}", id, e.getMessage());
                    failures.add(e);
                }
            }
        }

        // Delete newly created child process groups
        for (Map<String, Object> child : toMapList(current.getOrDefault("processGroups", List.of()))) {
            final String id = String.valueOf(child.get("id"));
            if (!snapChildPgIds.contains(id)) {
                try {
                    executeWithRevisionRetry("delete child process group " + id, () -> {
                        final Map<String, Object> entity = get("/process-groups/" + id);
                        final NiFiRevision revision = NiFiRevision.fromEntity(entity);
                        delete("/process-groups/" + id, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
                    });
                } catch (Exception e) {
                    logger.warn("Rollback: failed to delete child process group {}: {}", id, e.getMessage());
                    failures.add(e);
                }
            }
        }

        // Delete newly created controller services
        for (String id : createdCsIds) {
            try {
                deleteControllerService(id);
            } catch (Exception e) {
                logger.warn("Rollback: failed to delete controller service {}: {}", id, e.getMessage());
                failures.add(e);
            }
        }

        // Restore the target process group's original parameter context binding before deleting contexts
        if (snapshot.containsKey("originalPcBindingId")) {
            final String originalPcBindingId = (String) snapshot.get("originalPcBindingId");
            try {
                if (originalPcBindingId != null) {
                    bindParameterContextToProcessGroup(pgId, originalPcBindingId);
                } else {
                    unbindParameterContextFromProcessGroup(pgId);
                }
            } catch (Exception e) {
                logger.warn("Rollback: failed to restore parameter context binding for process group {}: {}", pgId, e.getMessage());
                failures.add(e);
            }
        }

        // Delete newly created parameter contexts
        for (String id : createdPcIds) {
            try {
                deleteParameterContext(id);
            } catch (Exception e) {
                logger.warn("Rollback: failed to delete parameter context {}: {}", id, e.getMessage());
                failures.add(e);
            }
        }

        if (!failures.isEmpty()) {
            final NiFiClientException ex = new NiFiClientException("ROLLBACK", pgId, 0, null, false,
                    "Rollback of process group " + pgId + " completed with " + failures.size() + " error(s)");
            failures.forEach(ex::addSuppressed);
            throw ex;
        }
    }

    @Override
    public List<double[]> getOccupiedPositions(final String pgId) {
        final Map<String, Object> flowData = getProcessGroupFlow(pgId);
        final Map<String, Object> flow = (Map<String, Object>) ((Map<String, Object>) flowData.getOrDefault("processGroupFlow", Map.of()))
                .getOrDefault("flow", Map.of());
        final List<double[]> positions = new ArrayList<>();
        for (Map<String, Object> p : (List<Map<String, Object>>) flow.getOrDefault("processors", List.of())) {
            final Map<String, Object> pos = (Map<String, Object>) p.getOrDefault("position", Map.of());
            positions.add(new double[]{doubleValue(pos.get("x")), doubleValue(pos.get("y"))});
        }
        for (Map<String, Object> pg : (List<Map<String, Object>>) flow.getOrDefault("processGroups", List.of())) {
            final Map<String, Object> pos = (Map<String, Object>) pg.getOrDefault("position", Map.of());
            positions.add(new double[]{doubleValue(pos.get("x")), doubleValue(pos.get("y"))});
        }
        return positions;
    }

    private Set<String> extractIds(final List<Map<String, Object>> entities) {
        return entities.stream().map(entity -> String.valueOf(entity.get("id"))).collect(Collectors.toSet());
    }

    @Override
    public Map<String, Object> getProcessGroup(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        return get("/process-groups/" + processGroupId);
    }

    @Override
    public Map<String, Object> updateProcessGroup(final String processGroupId, final Map<String, Object> updates) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update process group " + processGroupId, () -> {
            final Map<String, Object> entity = getProcessGroup(processGroupId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", processGroupId);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/process-groups/" + processGroupId, body));
        });
        return result.get();
    }

    @Override
    public void deleteProcessGroup(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        executeWithRevisionRetry("delete process group " + processGroupId, () -> {
            final Map<String, Object> entity = getProcessGroup(processGroupId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/process-groups/" + processGroupId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public List<Map<String, Object>> listProcessors(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final Map<String, Object> data = get("/process-groups/" + processGroupId + "/processors");
        return toMapList(data.getOrDefault("processors", List.of()));
    }

    @Override
    public List<Map<String, Object>> listConnections(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final Map<String, Object> data = get("/process-groups/" + processGroupId + "/connections");
        return toMapList(data.getOrDefault("connections", List.of()));
    }

    @Override
    public List<Map<String, Object>> listChildProcessGroups(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final Map<String, Object> data = get("/process-groups/" + processGroupId + "/process-groups");
        return toMapList(data.getOrDefault("processGroups", List.of()));
    }

    @Override
    public Map<String, Object> scheduleProcessGroup(final String processGroupId, final String state) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final String normalizedState = NiFiClientOperations.normalizeScheduleState(state);
        final Map<String, Object> body = Map.of("id", processGroupId, "state", normalizedState);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("schedule process group " + processGroupId, () ->
                result.set(put("/flow/process-groups/" + processGroupId, body)));
        return result.get();
    }

    @Override
    public Map<String, Object> getProcessorDiagnostics(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        return get("/processors/" + processorId + "/diagnostics");
    }

    @Override
    public Map<String, Object> getProcessorState(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        return get("/processors/" + processorId + "/state");
    }

    @Override
    public Map<String, Object> clearProcessorState(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        return post("/processors/" + processorId + "/state/clear-requests", Map.of());
    }

    @Override
    public Map<String, Object> terminateProcessorThreads(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        return delete("/processors/" + processorId + "/threads", null);
    }

    private static final Set<String> PROCESSOR_MUTABLE_FIELDS = Set.of("name", "position", "style", "config", "bundle");

    @Override
    public Map<String, Object> getProcessor(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        return get("/processors/" + processorId);
    }

    @Override
    public Map<String, Object> updateProcessor(final String processorId, final Map<String, Object> updates) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        if (updates == null) {
            throw new IllegalArgumentException("updates must not be null");
        }
        final Map<String, Object> component = buildProcessorComponent(processorId, updates);
        if (component.size() <= 1) {
            throw new IllegalArgumentException("updates must contain at least one of: name, position, style, config, bundle");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update processor " + processorId, () -> {
            final Map<String, Object> entity = get("/processors/" + processorId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/processors/" + processorId, body));
        });
        return result.get();
    }

    private static Map<String, Object> buildProcessorComponent(final String processorId, final Map<String, Object> updates) {
        final Map<String, Object> component = new HashMap<>();
        component.put("id", processorId);
        for (final String field : PROCESSOR_MUTABLE_FIELDS) {
            if (updates.containsKey(field)) {
                component.put(field, updates.get(field));
            }
        }
        return component;
    }

    @Override
    public Map<String, Object> updateConnection(final String connectionId, final Map<String, Object> updates) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update connection " + connectionId, () -> {
            final Map<String, Object> entity = get("/connections/" + connectionId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", connectionId);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/connections/" + connectionId, body));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getConnectionStatistics(final String connectionId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return get("/flow/connections/" + connectionId + "/statistics");
    }

    @Override
    public Map<String, Object> updateControllerService(final String controllerServiceId, final Map<String, Object> updates) {
        if (controllerServiceId == null || controllerServiceId.isBlank()) {
            throw new IllegalArgumentException("controllerServiceId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update controller service " + controllerServiceId, () -> {
            final Map<String, Object> entity = get("/controller-services/" + controllerServiceId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", controllerServiceId);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/controller-services/" + controllerServiceId, body));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getControllerServiceReferences(final String controllerServiceId) {
        if (controllerServiceId == null || controllerServiceId.isBlank()) {
            throw new IllegalArgumentException("controllerServiceId must not be blank");
        }
        return get("/controller-services/" + controllerServiceId + "/references");
    }

    @Override
    public Map<String, Object> updateControllerServiceReferences(final String controllerServiceId, final String state) {
        if (controllerServiceId == null || controllerServiceId.isBlank()) {
            throw new IllegalArgumentException("controllerServiceId must not be blank");
        }
        final String normalizedState = NiFiClientOperations.normalizeScheduleState(state);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update controller service references " + controllerServiceId, () -> {
            final Map<String, Object> refsEntity = get("/controller-services/" + controllerServiceId + "/references");
            final List<Map<String, Object>> references =
                    toMapList(refsEntity.getOrDefault("controllerServiceReferencingComponents", List.of()));
            final Map<String, Long> revisionVersions =
                    NiFiClientOperations.collectReferenceRevisions(references, normalizedState);
            final Map<String, Object> referencingRevisions = new HashMap<>();
            for (Map.Entry<String, Long> entry : revisionVersions.entrySet()) {
                referencingRevisions.put(entry.getKey(),
                        Map.of("version", entry.getValue(), "clientId", UUID.randomUUID().toString()));
            }
            final Map<String, Object> body = new HashMap<>();
            body.put("id", controllerServiceId);
            body.put("state", normalizedState);
            body.put("referencingComponentRevisions", referencingRevisions);
            result.set(put("/controller-services/" + controllerServiceId + "/references", body));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getFlowStatus() {
        return get("/flow/status");
    }

    @Override
    public Map<String, Object> getCurrentUser() {
        return get("/flow/current-user");
    }

    @Override
    public Map<String, Object> getBulletinBoard(final Long after, final String sourceName, final String message,
            final String sourceId, final String groupId, final Integer limit) {
        if (after != null && after < 0) {
            throw new IllegalArgumentException("after must be >= 0 when supplied");
        }
        if (limit != null && limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1 when supplied");
        }
        final StringBuilder query = new StringBuilder();
        if (after != null) {
            appendQueryParam(query, "after", String.valueOf(after));
        }
        if (sourceName != null) {
            appendQueryParam(query, "sourceName", URLEncoder.encode(sourceName, StandardCharsets.UTF_8));
        }
        if (message != null) {
            appendQueryParam(query, "message", URLEncoder.encode(message, StandardCharsets.UTF_8));
        }
        if (sourceId != null) {
            appendQueryParam(query, "sourceId", URLEncoder.encode(sourceId, StandardCharsets.UTF_8));
        }
        if (groupId != null) {
            appendQueryParam(query, "groupId", URLEncoder.encode(groupId, StandardCharsets.UTF_8));
        }
        if (limit != null) {
            appendQueryParam(query, "limit", String.valueOf(limit));
        }
        final String path = "/flow/bulletin-board" + (query.length() > 0 ? "?" + query : "");
        return get(path);
    }

    @Override
    public Map<String, Object> searchFlow(final String query, final String activeGroupId) {
        final String q = query == null ? "" : query;
        final String a = activeGroupId == null ? "" : activeGroupId;
        final String path = "/flow/search-results?q=" + URLEncoder.encode(q, StandardCharsets.UTF_8)
                + "&a=" + URLEncoder.encode(a, StandardCharsets.UTF_8);
        return get(path);
    }

    @Override
    public Map<String, Object> getAboutInfo() {
        return get("/flow/about");
    }

    @Override
    public Map<String, Object> createRemoteProcessGroup(final String processGroupId, final String targetUri,
            final double x, final double y, final Map<String, Object> configuration) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (targetUri == null || targetUri.isBlank()) {
            throw new IllegalArgumentException("targetUri must not be blank");
        }
        final Map<String, Object> component = new HashMap<>(configuration == null ? Map.of() : configuration);
        component.remove("id");
        component.remove("revision");
        component.remove("parentGroupId");
        component.remove("targetUris");
        component.put("targetUri", targetUri);
        component.put("position", Map.of("x", x, "y", y));
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", component
        );
        return post("/process-groups/" + processGroupId + "/remote-process-groups", body);
    }

    @Override
    public Map<String, Object> getRemoteProcessGroup(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        return get("/remote-process-groups/" + id);
    }

    @Override
    public Map<String, Object> updateRemoteProcessGroup(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update remote process group " + id, () -> {
            final Map<String, Object> entity = get("/remote-process-groups/" + id);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", id);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/remote-process-groups/" + id, body));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> setRemoteProcessGroupTransmission(final String id, final String state) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        final String normalized = NiFiClientOperations.normalizeRemoteTransmissionState(state);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("set remote process group transmission " + id, () -> {
            final Map<String, Object> entity = get("/remote-process-groups/" + id);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "state", normalized
            );
            result.set(put("/remote-process-groups/" + id + "/run-status", body));
        });
        return result.get();
    }

    @Override
    public void deleteRemoteProcessGroup(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        setRemoteProcessGroupTransmission(id, "STOPPED");
        executeWithRevisionRetry("delete remote process group " + id, () -> {
            final Map<String, Object> entity = get("/remote-process-groups/" + id);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/remote-process-groups/" + id, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public Map<String, Object> createInputPort(final String processGroupId, final String name, final double x, final double y) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", Map.of("name", name, "position", Map.of("x", x, "y", y))
        );
        return post("/process-groups/" + processGroupId + "/input-ports", body);
    }

    @Override
    public Map<String, Object> getInputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        return get("/input-ports/" + portId);
    }

    @Override
    public Map<String, Object> setInputPortRunStatus(final String portId, final String state) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        final String normalized = NiFiClientOperations.normalizePortRunStatus(state);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("set input port run status " + portId, () -> {
            final Map<String, Object> entity = get("/input-ports/" + portId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "state", normalized
            );
            result.set(put("/input-ports/" + portId + "/run-status", body));
        });
        return result.get();
    }

    @Override
    public void deleteInputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        executeWithRevisionRetry("delete input port " + portId, () -> {
            final Map<String, Object> entity = get("/input-ports/" + portId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/input-ports/" + portId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public Map<String, Object> createOutputPort(final String processGroupId, final String name, final double x, final double y) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", Map.of("name", name, "position", Map.of("x", x, "y", y))
        );
        return post("/process-groups/" + processGroupId + "/output-ports", body);
    }

    @Override
    public Map<String, Object> getOutputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        return get("/output-ports/" + portId);
    }

    @Override
    public Map<String, Object> setOutputPortRunStatus(final String portId, final String state) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        final String normalized = NiFiClientOperations.normalizePortRunStatus(state);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("set output port run status " + portId, () -> {
            final Map<String, Object> entity = get("/output-ports/" + portId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "state", normalized
            );
            result.set(put("/output-ports/" + portId + "/run-status", body));
        });
        return result.get();
    }

    @Override
    public void deleteOutputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        executeWithRevisionRetry("delete output port " + portId, () -> {
            final Map<String, Object> entity = get("/output-ports/" + portId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/output-ports/" + portId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public Map<String, Object> createLabel(final String processGroupId, final String text, final double x, final double y,
            final Map<String, String> style, final Double width, final Double height) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (text == null) {
            throw new IllegalArgumentException("text must not be null");
        }
        final Map<String, Object> component = new HashMap<>();
        component.put("label", text);
        component.put("position", Map.of("x", x, "y", y));
        component.put("style", style == null ? Map.of() : style);
        if (width != null) {
            component.put("width", width);
        }
        if (height != null) {
            component.put("height", height);
        }
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", component
        );
        return post("/process-groups/" + processGroupId + "/labels", body);
    }

    @Override
    public Map<String, Object> getLabel(final String labelId) {
        if (labelId == null || labelId.isBlank()) {
            throw new IllegalArgumentException("labelId must not be blank");
        }
        return get("/labels/" + labelId);
    }

    @Override
    public Map<String, Object> updateLabel(final String labelId, final Map<String, Object> updates) {
        if (labelId == null || labelId.isBlank()) {
            throw new IllegalArgumentException("labelId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update label " + labelId, () -> {
            final Map<String, Object> entity = get("/labels/" + labelId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", labelId);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/labels/" + labelId, body));
        });
        return result.get();
    }

    @Override
    public void deleteLabel(final String labelId) {
        if (labelId == null || labelId.isBlank()) {
            throw new IllegalArgumentException("labelId must not be blank");
        }
        executeWithRevisionRetry("delete label " + labelId, () -> {
            final Map<String, Object> entity = get("/labels/" + labelId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/labels/" + labelId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public Map<String, Object> createFunnel(final String processGroupId, final double x, final double y) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final Map<String, Object> body = Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", Map.of("position", Map.of("x", x, "y", y))
        );
        return post("/process-groups/" + processGroupId + "/funnels", body);
    }

    @Override
    public Map<String, Object> getFunnel(final String funnelId) {
        if (funnelId == null || funnelId.isBlank()) {
            throw new IllegalArgumentException("funnelId must not be blank");
        }
        return get("/funnels/" + funnelId);
    }

    @Override
    public Map<String, Object> updateFunnel(final String funnelId, final Map<String, Object> updates) {
        if (funnelId == null || funnelId.isBlank()) {
            throw new IllegalArgumentException("funnelId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update funnel " + funnelId, () -> {
            final Map<String, Object> entity = get("/funnels/" + funnelId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", funnelId);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/funnels/" + funnelId, body));
        });
        return result.get();
    }

    @Override
    public void deleteFunnel(final String funnelId) {
        if (funnelId == null || funnelId.isBlank()) {
            throw new IllegalArgumentException("funnelId must not be blank");
        }
        executeWithRevisionRetry("delete funnel " + funnelId, () -> {
            final Map<String, Object> entity = get("/funnels/" + funnelId);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            delete("/funnels/" + funnelId, "version=" + revision.version() + "&clientId=" + UUID.randomUUID());
        });
    }

    @Override
    public Map<String, Object> createSnippet(final String parentProcessGroupId, final Map<String, Object> componentSelections) {
        if (parentProcessGroupId == null || parentProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("parentProcessGroupId must not be blank");
        }
        if (componentSelections == null || componentSelections.isEmpty()) {
            throw new IllegalArgumentException("componentSelections must not be null or empty");
        }
        final Set<String> allowedKeys = Set.of("processGroups", "remoteProcessGroups", "processors",
                "inputPorts", "outputPorts", "connections", "labels", "funnels");
        final Map<String, Object> snippet = new HashMap<>();
        snippet.put("parentGroupId", parentProcessGroupId);
        for (Map.Entry<String, Object> entry : componentSelections.entrySet()) {
            if (allowedKeys.contains(entry.getKey())) {
                snippet.put(entry.getKey(), entry.getValue());
            }
        }
        if (snippet.size() == 1) {
            throw new IllegalArgumentException(
                    "componentSelections must contain at least one allowed component type key");
        }
        snippet.remove("id");
        snippet.remove("uri");
        snippet.remove("parentGroupId");
        snippet.put("parentGroupId", parentProcessGroupId);
        final Map<String, Object> body = Map.of("snippet", snippet);
        return post("/snippets", body);
    }

    @Override
    public Map<String, Object> moveSnippet(final String snippetId, final String destinationProcessGroupId) {
        if (snippetId == null || snippetId.isBlank()) {
            throw new IllegalArgumentException("snippetId must not be blank");
        }
        if (destinationProcessGroupId == null || destinationProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("destinationProcessGroupId must not be blank");
        }
        final Map<String, Object> snippet = Map.of("id", snippetId, "parentGroupId", destinationProcessGroupId);
        return put("/snippets/" + snippetId, Map.of("snippet", snippet));
    }

    @Override
    public Map<String, Object> copySnippet(final String snippetId, final String destinationProcessGroupId,
            final double originX, final double originY) {
        if (snippetId == null || snippetId.isBlank()) {
            throw new IllegalArgumentException("snippetId must not be blank");
        }
        if (destinationProcessGroupId == null || destinationProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("destinationProcessGroupId must not be blank");
        }
        final Map<String, Object> body = Map.of(
                "snippetId", snippetId,
                "originX", originX,
                "originY", originY
        );
        return post("/process-groups/" + destinationProcessGroupId + "/snippet-instance", body);
    }

    @Override
    public Map<String, Object> deleteSnippet(final String snippetId) {
        if (snippetId == null || snippetId.isBlank()) {
            throw new IllegalArgumentException("snippetId must not be blank");
        }
        return delete("/snippets/" + snippetId, null);
    }

    // =========================================================================
    // Parameter context management
    // =========================================================================

    @Override
    public List<Map<String, Object>> listParameterContexts() {
        final Map<String, Object> data = get("/flow/parameter-contexts");
        return toMapList(data.getOrDefault("parameterContexts", List.of()));
    }

    @Override
    public Map<String, Object> updateParameterContext(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update parameter context " + id, () -> {
            final Map<String, Object> entity = get("/parameter-contexts/" + id);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", id);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(put("/parameter-contexts/" + id, body));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> submitParameterContextUpdateRequest(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("submit parameter context update " + id, () -> {
            final Map<String, Object> entity = get("/parameter-contexts/" + id);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> component = new HashMap<>(updates);
            component.remove("id");
            component.remove("revision");
            component.put("id", id);
            final Map<String, Object> body = Map.of(
                    "revision", revision.toMap(),
                    "component", component
            );
            result.set(post("/parameter-contexts/" + id + "/update-requests", body));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getParameterContextUpdateRequest(final String id, final String requestId) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return get("/parameter-contexts/" + id + "/update-requests/" + requestId);
    }

    @Override
    public Map<String, Object> deleteParameterContextUpdateRequest(final String id, final String requestId) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return delete("/parameter-contexts/" + id + "/update-requests/" + requestId, null);
    }

    @Override
    public Map<String, Object> updateParameterContextLive(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitParameterContextUpdateRequest(id, updates);
                return NiFiAsyncRequestState.parse(response, "request", "requestId", "complete", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getParameterContextUpdateRequest(id, reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteParameterContextUpdateRequest(id, reqId);
            }
        });
    }

    // =========================================================================
    // FlowFile queue operations
    // =========================================================================

    @Override
    public Map<String, Object> submitFlowFileListingRequest(final String connectionId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return post("/flowfile-queues/" + connectionId + "/listing-requests", Map.of());
    }

    @Override
    public Map<String, Object> getFlowFileListingRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return get("/flowfile-queues/" + connectionId + "/listing-requests/" + requestId);
    }

    @Override
    public Map<String, Object> deleteFlowFileListingRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return delete("/flowfile-queues/" + connectionId + "/listing-requests/" + requestId, null);
    }

    @Override
    public Map<String, Object> listFlowFiles(final String connectionId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitFlowFileListingRequest(connectionId);
                return NiFiAsyncRequestState.parse(response, "listingRequest", "id", "finished", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getFlowFileListingRequest(connectionId, reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "listingRequest", "id", "finished", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "listingRequest", "id", "finished", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteFlowFileListingRequest(connectionId, reqId);
            }
        });
    }

    @Override
    public Map<String, Object> getFlowFileDetails(final String connectionId, final String flowFileUuid) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (flowFileUuid == null || flowFileUuid.isBlank()) {
            throw new IllegalArgumentException("flowFileUuid must not be blank");
        }
        return get("/flowfile-queues/" + connectionId + "/flowfiles/" + flowFileUuid);
    }

    @Override
    public InputStream downloadFlowFileContent(final String connectionId, final String flowFileUuid) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (flowFileUuid == null || flowFileUuid.isBlank()) {
            throw new IllegalArgumentException("flowFileUuid must not be blank");
        }
        return getInputStream("/flowfile-queues/" + connectionId + "/flowfiles/" + flowFileUuid + "/content");
    }

    @Override
    public Map<String, Object> submitQueueDropRequest(final String connectionId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive queue drop operation");
        }
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return post("/flowfile-queues/" + connectionId + "/drop-requests", Map.of());
    }

    @Override
    public Map<String, Object> getQueueDropRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return get("/flowfile-queues/" + connectionId + "/drop-requests/" + requestId);
    }

    @Override
    public Map<String, Object> deleteQueueDropRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return delete("/flowfile-queues/" + connectionId + "/drop-requests/" + requestId, null);
    }

    @Override
    public Map<String, Object> dropFlowFileQueue(final String connectionId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive queue drop operation");
        }
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitQueueDropRequest(connectionId, true);
                return NiFiAsyncRequestState.parse(response, "dropRequest", "id", "finished", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getQueueDropRequest(connectionId, reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "dropRequest", "id", "finished", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "dropRequest", "id", "finished", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteQueueDropRequest(connectionId, reqId);
            }
        });
    }

    /**
     * Issues an authenticated GET request and returns the raw response body as an {@link InputStream}.
     * Performs one token refresh on HTTP 401. Non-2xx responses cause the stream to be closed and a
     * {@link NiFiClientException} to be thrown. The caller owns the returned stream and is responsible
     * for closing it.
     */
    private InputStream getInputStream(final String path) {
        ensureToken();
        final HttpRequest initialRequest = buildRequestBuilder(path).GET().build();
        boolean tokenRefreshed = false;
        HttpRequest request = initialRequest;
        while (true) {
            try {
                final HttpResponse<InputStream> response =
                        client.send(request, HttpResponse.BodyHandlers.ofInputStream());
                final int status = response.statusCode();
                if (status == 401 && !tokenRefreshed) {
                    final String usedToken = request.headers().firstValue("Authorization").orElse("");
                    try {
                        response.body().close();
                    } catch (Exception ignored) {
                    }
                    synchronized (this) {
                        if (usedToken.equals("Bearer " + token)) {
                            token = null;
                        }
                    }
                    tokenRefreshed = true;
                    request = buildRequestBuilder(path).GET().build();
                    continue;
                }
                if (status < 200 || status >= 300) {
                    try {
                        response.body().close();
                    } catch (Exception ignored) {
                    }
                    throw new NiFiClientException("GET", path, status, null, false,
                            "NiFi GET " + path + " failed: HTTP " + status);
                }
                return response.body();
            } catch (NiFiClientException e) {
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NiFiClientException("GET", path, 0, null, false,
                        "NiFi streaming GET interrupted", e);
            } catch (java.io.IOException e) {
                throw new NiFiClientException("GET", path, 0, null, true,
                        "NiFi streaming GET failed", e);
            }
        }
    }

    private static void appendQueryParam(final StringBuilder sb, final String name, final String value) {
        if (sb.length() > 0) {
            sb.append('&');
        }
        sb.append(name).append('=').append(value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(final Object o) {
        if (o instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        return new HashMap<>();
    }

    private static boolean isBlankOrNull(final Object v) {
        if (v == null) {
            return true;
        }
        return String.valueOf(v).isBlank();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> toMapList(final Object listObj) {
        if (!(listObj instanceof List<?> list)) {
            return List.of();
        }
        final List<Map<String, Object>> out = new ArrayList<>();
        for (final Object item : list) {
            if (item instanceof Map<?, ?> m) {
                out.add((Map<String, Object>) m);
            }
        }
        return out;
    }

    private static double doubleValue(final Object v) {
        return NiFiClientOperations.doubleValue(v);
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    // =========================================================================
    // Provenance operations
    // =========================================================================

    @Override
    public Map<String, Object> submitProvenanceQuery(final Map<String, Object> request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        return post("/provenance", Map.of("provenance", Map.of("request", normalizedRequest)));
    }

    @Override
    public Map<String, Object> getProvenanceQuery(final String queryId, final String clusterNodeId,
            final boolean summarize, final boolean incrementalResults) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        final StringBuilder query = new StringBuilder();
        query.append("summarize=").append(summarize);
        query.append("&incrementalResults=").append(incrementalResults);
        if (clusterNodeId != null && !clusterNodeId.isBlank()) {
            query.append("&clusterNodeId=").append(URLEncoder.encode(clusterNodeId, StandardCharsets.UTF_8));
        }
        return get("/provenance/" + queryId + "?" + query);
    }

    @Override
    public Map<String, Object> deleteProvenanceQuery(final String queryId, final String clusterNodeId) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        final String queryString = (clusterNodeId != null && !clusterNodeId.isBlank())
                ? "clusterNodeId=" + URLEncoder.encode(clusterNodeId, StandardCharsets.UTF_8)
                : null;
        return delete("/provenance/" + queryId, queryString);
    }

    @Override
    public Map<String, Object> queryProvenance(final Map<String, Object> request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        final String clusterNodeId = extractNonblankString(normalizedRequest, "clusterNodeId");
        final boolean summarize = extractBoolean(normalizedRequest, "summarize", false);
        final boolean incrementalResults = extractBoolean(normalizedRequest, "incrementalResults", true);
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitProvenanceQuery(normalizedRequest);
                return NiFiAsyncRequestState.parse(response, "provenance", "id", "finished", null)
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String queryId, final java.time.Duration remainingTime) {
                return getProvenanceQuery(queryId, clusterNodeId, summarize, incrementalResults);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "provenance", "id", "finished", null)
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "provenance", "id", "finished", null)
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String queryId) {
                deleteProvenanceQuery(queryId, clusterNodeId);
            }
        });
    }

    @Override
    public Map<String, Object> submitLineageQuery(final Map<String, Object> request) {
        if (request == null || request.isEmpty()) {
            throw new IllegalArgumentException("request must not be null or empty");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        return post("/provenance/lineage", Map.of("lineage", Map.of("request", normalizedRequest)));
    }

    @Override
    public Map<String, Object> getLineageQuery(final String lineageId, final String clusterNodeId) {
        if (lineageId == null || lineageId.isBlank()) {
            throw new IllegalArgumentException("lineageId must not be blank");
        }
        if (clusterNodeId != null && !clusterNodeId.isBlank()) {
            return get("/provenance/lineage/" + lineageId + "?clusterNodeId="
                    + URLEncoder.encode(clusterNodeId, StandardCharsets.UTF_8));
        }
        return get("/provenance/lineage/" + lineageId);
    }

    @Override
    public Map<String, Object> deleteLineageQuery(final String lineageId, final String clusterNodeId) {
        if (lineageId == null || lineageId.isBlank()) {
            throw new IllegalArgumentException("lineageId must not be blank");
        }
        final String queryString = (clusterNodeId != null && !clusterNodeId.isBlank())
                ? "clusterNodeId=" + URLEncoder.encode(clusterNodeId, StandardCharsets.UTF_8)
                : null;
        return delete("/provenance/lineage/" + lineageId, queryString);
    }

    @Override
    public Map<String, Object> queryLineage(final Map<String, Object> request) {
        if (request == null || request.isEmpty()) {
            throw new IllegalArgumentException("request must not be null or empty");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        final String clusterNodeId = extractNonblankString(normalizedRequest, "clusterNodeId");
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitLineageQuery(normalizedRequest);
                return NiFiAsyncRequestState.parse(response, "lineage", "id", "finished", null)
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String lineageId, final java.time.Duration remainingTime) {
                return getLineageQuery(lineageId, clusterNodeId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "lineage", "id", "finished", null)
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "lineage", "id", "finished", null)
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String lineageId) {
                deleteLineageQuery(lineageId, clusterNodeId);
            }
        });
    }

    // =========================================================================
    // Registry, versioning, and process-group import/export operations
    // =========================================================================

    @Override
    public List<Map<String, Object>> listRegistryClients() {
        return toMapList(get("/controller/registry-clients").get("registries"));
    }

    @Override
    public Map<String, Object> createRegistryClient(final Map<String, Object> component) {
        if (component == null || component.isEmpty()) {
            throw new IllegalArgumentException("component must not be null or empty");
        }
        final Map<String, Object> sanitizedComponent = new HashMap<>(component);
        sanitizedComponent.remove("id");
        sanitizedComponent.remove("revision");
        return post("/controller/registry-clients", Map.of(
                "revision", NiFiRevision.zero().toMap(),
                "component", sanitizedComponent
        ));
    }

    @Override
    public Map<String, Object> getRegistryClient(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        return get("/controller/registry-clients/" + id);
    }

    @Override
    public Map<String, Object> updateRegistryClient(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update registry client " + id, () -> {
            final Map<String, Object> entity = get("/controller/registry-clients/" + id);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            final Map<String, Object> sanitizedUpdates = new HashMap<>(updates);
            sanitizedUpdates.remove("id");
            sanitizedUpdates.remove("revision");
            sanitizedUpdates.put("id", id);
            result.set(put("/controller/registry-clients/" + id, Map.of(
                    "revision", revision.toMap(),
                    "component", sanitizedUpdates
            )));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> deleteRegistryClient(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("delete registry client " + id, () -> {
            final Map<String, Object> entity = get("/controller/registry-clients/" + id);
            final NiFiRevision revision = NiFiRevision.fromEntity(entity);
            result.set(delete("/controller/registry-clients/" + id,
                    "version=" + revision.version() + "&clientId=" + UUID.randomUUID()));
        });
        return result.get();
    }

    @Override
    public List<Map<String, Object>> listRegistryBuckets(final String registryId, final String branch) {
        if (registryId == null || registryId.isBlank()) {
            throw new IllegalArgumentException("registryId must not be blank");
        }
        String path = "/flow/registries/" + registryId + "/buckets";
        if (branch != null && !branch.isBlank()) {
            path += "?branch=" + URLEncoder.encode(branch, StandardCharsets.UTF_8);
        }
        return toMapList(get(path).get("buckets"));
    }

    @Override
    public List<Map<String, Object>> listRegistryFlows(final String registryId, final String bucketId,
            final String branch) {
        if (registryId == null || registryId.isBlank()) {
            throw new IllegalArgumentException("registryId must not be blank");
        }
        if (bucketId == null || bucketId.isBlank()) {
            throw new IllegalArgumentException("bucketId must not be blank");
        }
        String path = "/flow/registries/" + registryId + "/buckets/" + bucketId + "/flows";
        if (branch != null && !branch.isBlank()) {
            path += "?branch=" + URLEncoder.encode(branch, StandardCharsets.UTF_8);
        }
        return toMapList(get(path).get("versionedFlows"));
    }

    @Override
    public Map<String, Object> getVersionInformation(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        return get("/versions/process-groups/" + processGroupId);
    }

    @Override
    public String acquireVersionRequest(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        return postForText("/versions/active-requests", Map.of("processGroupId", processGroupId));
    }

    @Override
    public Map<String, Object> updateVersionRequestMapping(final String requestId, final String processGroupId,
            final Map<String, Object> versionControlInformation, final Map<String, Object> componentMapping) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        if (componentMapping == null) {
            throw new IllegalArgumentException("componentMapping must not be null");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update version request mapping " + requestId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final NiFiRevision revision =
                    NiFiRevision.fromRevisionMap(castMap(versionInfo.get("processGroupRevision")));
            final Map<String, Object> sanitizedVci = new HashMap<>(versionControlInformation);
            sanitizedVci.put("groupId", processGroupId);
            result.set(put("/versions/active-requests/" + requestId, Map.of(
                    "processGroupRevision", revision.toMap(),
                    "versionControlInformation", sanitizedVci,
                    "versionControlComponentMapping", componentMapping,
                    "disconnectedNodeAcknowledged", false
            )));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> releaseVersionRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return delete("/versions/active-requests/" + requestId, "disconnectedNodeAcknowledged=false");
    }

    @Override
    public Map<String, Object> startVersionControl(final String processGroupId, final Map<String, Object> versionedFlow) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionedFlow == null || versionedFlow.isEmpty()) {
            throw new IllegalArgumentException("versionedFlow must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("start version control " + processGroupId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final NiFiRevision revision =
                    NiFiRevision.fromRevisionMap(castMap(versionInfo.get("processGroupRevision")));
            result.set(post("/versions/process-groups/" + processGroupId, Map.of(
                    "processGroupRevision", revision.toMap(),
                    "versionedFlow", new HashMap<>(versionedFlow),
                    "disconnectedNodeAcknowledged", false
            )));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> submitVersionUpdate(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("submit version update " + processGroupId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final NiFiRevision revision =
                    NiFiRevision.fromRevisionMap(castMap(versionInfo.get("processGroupRevision")));
            final Map<String, Object> sanitizedVci = new HashMap<>(versionControlInformation);
            sanitizedVci.put("groupId", processGroupId);
            result.set(post("/versions/update-requests/process-groups/" + processGroupId, Map.of(
                    "processGroupRevision", revision.toMap(),
                    "versionControlInformation", sanitizedVci,
                    "disconnectedNodeAcknowledged", false
            )));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getVersionUpdateRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return get("/versions/update-requests/" + requestId);
    }

    @Override
    public Map<String, Object> deleteVersionUpdateRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return delete("/versions/update-requests/" + requestId, "disconnectedNodeAcknowledged=false");
    }

    @Override
    public Map<String, Object> updateVersionedProcessGroup(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitVersionUpdate(processGroupId, versionControlInformation);
                return NiFiAsyncRequestState.parse(response, "request", "requestId", "complete", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getVersionUpdateRequest(reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteVersionUpdateRequest(reqId);
            }
        });
    }

    @Override
    public Map<String, Object> submitVersionRevert(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("submit version revert " + processGroupId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final NiFiRevision revision =
                    NiFiRevision.fromRevisionMap(castMap(versionInfo.get("processGroupRevision")));
            final Map<String, Object> sanitizedVci = new HashMap<>(versionControlInformation);
            sanitizedVci.put("groupId", processGroupId);
            result.set(post("/versions/revert-requests/process-groups/" + processGroupId, Map.of(
                    "processGroupRevision", revision.toMap(),
                    "versionControlInformation", sanitizedVci,
                    "disconnectedNodeAcknowledged", false
            )));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getVersionRevertRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return get("/versions/revert-requests/" + requestId);
    }

    @Override
    public Map<String, Object> deleteVersionRevertRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return delete("/versions/revert-requests/" + requestId, "disconnectedNodeAcknowledged=false");
    }

    @Override
    public Map<String, Object> revertVersionedProcessGroup(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitVersionRevert(processGroupId, versionControlInformation);
                return NiFiAsyncRequestState.parse(response, "request", "requestId", "complete", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getVersionRevertRequest(reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteVersionRevertRequest(reqId);
            }
        });
    }

    @Override
    public Map<String, Object> exportProcessGroup(final String processGroupId,
            final boolean includeReferencedServices, final boolean includeComponentState) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        return get("/process-groups/" + processGroupId + "/download?includeReferencedServices="
                + includeReferencedServices + "&includeComponentState=" + includeComponentState);
    }

    @Override
    public Map<String, Object> importProcessGroup(final String parentProcessGroupId, final String groupName,
            final double positionX, final double positionY, final Map<String, Object> flowSnapshot) {
        if (parentProcessGroupId == null || parentProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("parentProcessGroupId must not be blank");
        }
        if (groupName == null || groupName.isBlank()) {
            throw new IllegalArgumentException("groupName must not be blank");
        }
        if (flowSnapshot == null || flowSnapshot.isEmpty()) {
            throw new IllegalArgumentException("flowSnapshot must not be null or empty");
        }
        return post("/process-groups/" + parentProcessGroupId + "/process-groups/import", Map.of(
                "groupId", parentProcessGroupId,
                "groupName", groupName,
                "disconnectedNodeAcknowledged", false,
                "flowSnapshot", flowSnapshot,
                "positionDTO", Map.of("x", positionX, "y", positionY),
                "revisionDTO", Map.of("version", 0, "clientId", UUID.randomUUID().toString())
        ));
    }

    // =========================================================================
    // System diagnostics, cluster, counters, and security operations
    // =========================================================================

    @Override
    public Map<String, Object> getSystemDiagnostics(final boolean nodewise, final String diagnosticLevel,
            final String clusterNodeId) {
        final String normalizedLevel = NiFiClientOperations.normalizeDiagnosticLevel(diagnosticLevel);
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank())
                ? clusterNodeId.trim() : null;
        if (nodewise && effectiveNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node");
        }
        final StringBuilder query = new StringBuilder();
        appendQueryParam(query, "nodewise", String.valueOf(nodewise));
        appendQueryParam(query, "diagnosticLevel", normalizedLevel);
        if (effectiveNodeId != null) {
            appendQueryParam(query, "clusterNodeId", URLEncoder.encode(effectiveNodeId, StandardCharsets.UTF_8));
        }
        return get("/system-diagnostics?" + query);
    }

    @Override
    public Map<String, Object> getJmxMetrics(final String beanNameFilter) {
        if (beanNameFilter != null && !beanNameFilter.isBlank()) {
            return get("/system-diagnostics/jmx-metrics?beanNameFilter="
                    + URLEncoder.encode(beanNameFilter, StandardCharsets.UTF_8));
        }
        return get("/system-diagnostics/jmx-metrics");
    }

    @Override
    public Map<String, Object> getClusterSummary() {
        return get("/flow/cluster/summary");
    }

    @Override
    public Map<String, Object> getClusterNodes() {
        return get("/controller/cluster");
    }

    @Override
    public Map<String, Object> getClusterNode(final String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        return get("/controller/cluster/nodes/" + nodeId);
    }

    @Override
    public Map<String, Object> listCounters(final boolean nodewise, final String clusterNodeId) {
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank())
                ? clusterNodeId.trim() : null;
        if (nodewise && effectiveNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node");
        }
        final StringBuilder query = new StringBuilder();
        appendQueryParam(query, "nodewise", String.valueOf(nodewise));
        if (effectiveNodeId != null) {
            appendQueryParam(query, "clusterNodeId", URLEncoder.encode(effectiveNodeId, StandardCharsets.UTF_8));
        }
        return get("/counters?" + query);
    }

    @Override
    public Map<String, Object> updateClusterNode(final String nodeId, final String status, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive cluster node status update");
        }
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        final String id = nodeId.trim();
        final String normalized = NiFiClientOperations.normalizeClusterNodeStatus(status);
        logger.info("Updating cluster node {} to status {}", id, normalized);
        final Map<String, Object> body = Map.of("node", Map.of("nodeId", id, "status", normalized));
        return put("/controller/cluster/nodes/" + id, body);
    }

    @Override
    public Map<String, Object> removeClusterNode(final String nodeId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive cluster node removal");
        }
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        final String id = nodeId.trim();
        final Map<String, Object> entity = getClusterNode(id);
        final Object nodeObj = entity.get("node");
        if (!(nodeObj instanceof Map<?, ?>)) {
            throw new IllegalStateException("Cluster node response for " + id + " did not contain a 'node' map");
        }
        final Object statusObj = ((Map<?, ?>) nodeObj).get("status");
        if (!(statusObj instanceof String)) {
            throw new IllegalStateException("Cluster node 'node.status' is missing or not a string for node: " + id);
        }
        final String currentStatus = ((String) statusObj).trim().toUpperCase(Locale.ROOT);
        if (!"DISCONNECTED".equals(currentStatus) && !"OFFLOADED".equals(currentStatus)) {
            throw new IllegalStateException("Cluster node " + id + " cannot be removed: current status is "
                    + currentStatus + " (must be DISCONNECTED or OFFLOADED)");
        }
        logger.info("Removing cluster node {}", id);
        return delete("/controller/cluster/nodes/" + id, null);
    }

    @Override
    public Map<String, Object> resetCounter(final String counterId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive counter reset");
        }
        if (counterId == null || counterId.isBlank()) {
            throw new IllegalArgumentException("counterId must not be blank");
        }
        final String id = counterId.trim();
        logger.info("Resetting counter {}", id);
        return putWithoutBody("/counters/" + id);
    }

    @Override
    public Map<String, Object> resetAllCounters(final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive counter reset");
        }
        logger.info("Resetting all counters");
        return putWithoutBody("/counters");
    }

    @Override
    public void logout() {
        final HttpRequest request = buildRequestBuilder("/access/logout").DELETE().build();
        final String usedToken = request.headers().firstValue("Authorization").orElse("");
        sendOnce("DELETE", "/access/logout", request);
        synchronized (this) {
            if (usedToken.equals("Bearer " + token)) {
                token = null;
            }
        }
    }

    @Override
    public Map<String, Object> getAuthenticationConfiguration() {
        return getUnauthenticated("/authentication/configuration");
    }

    @Override
    public List<Map<String, Object>> listAuthorizableResources() {
        return toMapList(get("/resources").get("resources"));
    }

    @Override
    public Map<String, Object> getAccessPolicy(final String action, final String resource) {
        final String normalizedAction = NiFiClientOperations.normalizeAccessPolicyAction(action);
        if (resource == null || resource.isBlank()) {
            throw new IllegalArgumentException("resource must not be blank");
        }
        final String strippedResource = resource.replaceAll("^/+", "");
        return get("/policies/" + normalizedAction + "/" + strippedResource);
    }

    @Override
    public List<Map<String, Object>> listUsers() {
        return toMapList(get("/tenants/users").get("users"));
    }

    @Override
    public List<Map<String, Object>> listUserGroups() {
        return toMapList(get("/tenants/user-groups").get("userGroups"));
    }

    private static String extractNonblankString(final Map<String, Object> map, final String key) {
        final Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String stringValue)) {
            throw new IllegalArgumentException(key + " must be a string");
        }
        final String str = stringValue.trim();
        return str.isBlank() ? null : str;
    }

    private static Map<String, Object> normalizeClusterNodeId(final Map<String, Object> request) {
        final Map<String, Object> normalized = new HashMap<>(request);
        final String clusterNodeId = extractNonblankString(normalized, "clusterNodeId");
        if (clusterNodeId == null) {
            normalized.remove("clusterNodeId");
        } else {
            normalized.put("clusterNodeId", clusterNodeId);
        }
        return normalized;
    }

    private static boolean extractBoolean(final Map<String, Object> map, final String key,
            final boolean defaultValue) {
        final Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        final String str = String.valueOf(value).trim();
        if ("true".equalsIgnoreCase(str)) {
            return true;
        }
        if ("false".equalsIgnoreCase(str)) {
            return false;
        }
        throw new IllegalArgumentException("Cannot convert '" + key + "' to boolean: " + value);
    }
}
