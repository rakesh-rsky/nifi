package org.apache.nifi.copilot.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
    private final HttpClient client;
    private final String baseUrl;
    private volatile String token;
    private final Map<String, Map<String, Object>> typeBundleCache = new HashMap<>();

    public HttpNiFiClient(final NiFiConfigResolver config) {
        this.config = config;
        this.baseUrl = config.resolveBaseUrl();
        this.client = buildClient(config.verifySsl());
    }

    private HttpClient buildClient(final boolean verifySsl) {
        try {
            if (verifySsl) {
                return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
            }
            final TrustManager[] trustAll = new TrustManager[]{
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        public void checkClientTrusted(final X509Certificate[] xcs, final String string) {}
                        public void checkServerTrusted(final X509Certificate[] xcs, final String string) {}
                    }
            };
            final SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAll, new SecureRandom());
            return HttpClient.newBuilder().sslContext(sc).connectTimeout(Duration.ofSeconds(30)).build();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private synchronized void ensureToken() {
        if (token != null && !token.isBlank()) {
            return;
        }
        try {
            final String username = config.resolveUsername();
            final String password = config.resolvePassword();
            if (username.isBlank() || password.isBlank()) {
                throw new RuntimeException("NiFi external mode requires NIFI_USERNAME and NIFI_PASSWORD.");
            }
            final String body = "username=" + URLEncoder.encode(username, StandardCharsets.UTF_8)
                    + "&password=" + URLEncoder.encode(password, StandardCharsets.UTF_8);
            final HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/nifi-api/access/token"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new RuntimeException("NiFi auth failed: HTTP " + response.statusCode());
            }
            token = response.body().trim();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private HttpRequest.Builder request(final String path) {
        ensureToken();
        return HttpRequest.newBuilder(URI.create(baseUrl + "/nifi-api" + path))
                .header("Authorization", "Bearer " + token);
    }

    private Map<String, Object> get(final String path) {
        try {
            final HttpResponse<String> response = client.send(request(path).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new RuntimeException("NiFi GET " + path + " failed: HTTP " + response.statusCode());
            }
            return mapper.readValue(response.body(), new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Map<String, Object> post(final String path, final Object body) {
        return withBody("POST", path, body);
    }

    private Map<String, Object> put(final String path, final Object body) {
        return withBody("PUT", path, body);
    }

    private Map<String, Object> delete(final String path, final String query) {
        try {
            final HttpRequest request = request(path + (query == null || query.isBlank() ? "" : "?" + query)).DELETE().build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new RuntimeException("NiFi DELETE " + path + " failed: HTTP " + response.statusCode());
            }
            if (response.body() == null || response.body().isBlank()) {
                return Map.of();
            }
            return mapper.readValue(response.body(), new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Map<String, Object> withBody(final String method, final String path, final Object body) {
        try {
            final HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body));
            final HttpRequest.Builder builder = request(path).header("Content-Type", "application/json");
            if ("POST".equals(method)) {
                builder.POST(publisher);
            } else {
                builder.PUT(publisher);
            }
            final HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                logger.error("NiFi {} {} failed {}: {}", method, path, response.statusCode(), response.body());
                throw new RuntimeException("NiFi " + method + " " + path + " failed: HTTP " + response.statusCode());
            }
            return mapper.readValue(response.body(), new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public String getProcessGroupId(final String pgId) {
        if (pgId != null && !"root".equals(pgId) && !pgId.isBlank()) {
            try {
                get("/process-groups/" + pgId);
                return pgId;
            } catch (Exception ignored) {
            }
        }
        return String.valueOf(get("/process-groups/root").get("id"));
    }

    @Override
    public Map<String, Object> createProcessGroup(final String parentPgId, final String name, final double x, final double y) {
        final Map<String, Object> body = Map.of(
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", 0),
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
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", 0),
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
        final Map<String, Object> entity = get("/processors/" + procId);
        final Map<String, Object> rev = (Map<String, Object>) entity.get("revision");
        final Map<String, Object> body = Map.of(
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", intValue(rev.get("version"))),
                "component", Map.of("id", procId, "state", state)
        );
        put("/processors/" + procId, body);
    }

    @Override
    public void deleteProcessor(final String procId) {
        try {
            stopProcessor(procId);
        } catch (Exception ignored) {
        }
        final Map<String, Object> entity = get("/processors/" + procId);
        final Map<String, Object> rev = (Map<String, Object>) entity.get("revision");
        delete("/processors/" + procId, "version=" + intValue(rev.get("version")) + "&clientId=" + UUID.randomUUID());
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
            final Map<String, Object> entity = get("/processors/" + procId);
            final Map<String, Object> component = (Map<String, Object>) entity.getOrDefault("component", Map.of());
            final Map<String, Object> rev = (Map<String, Object>) entity.getOrDefault("revision", Map.of());
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
                    "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", intValue(rev.get("version"))),
                    "component", Map.of("id", procId, "config", Map.of("autoTerminatedRelationships", terminate))
            );
            put("/processors/" + procId, body);
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
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", 0),
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
    public void deleteConnection(final String connId) {
        final Map<String, Object> entity = get("/connections/" + connId);
        final Map<String, Object> rev = (Map<String, Object>) entity.getOrDefault("revision", Map.of());
        delete("/connections/" + connId, "version=" + intValue(rev.get("version")) + "&clientId=" + UUID.randomUUID());
    }

    @Override
    public List<Map<String, Object>> listControllerServices(final String pgId) {
        final Map<String, Object> data = get("/flow/process-groups/" + pgId + "/controller-services");
        final List<?> services = (List<?>) data.getOrDefault("controllerServices", List.of());
        final List<Map<String, Object>> out = new ArrayList<>();
        for (Object raw : services) {
            final Map<String, Object> service = (Map<String, Object>) raw;
            final Map<String, Object> component = (Map<String, Object>) service.get("component");
            out.add(Map.of(
                    "id", service.get("id"),
                    "name", component.get("name"),
                    "type", component.get("type"),
                    "state", component.getOrDefault("state", "DISABLED")
            ));
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
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", 0),
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
        final Map<String, Object> entity = get("/controller-services/" + csId);
        final Map<String, Object> rev = (Map<String, Object>) entity.get("revision");
        final Map<String, Object> body = Map.of(
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", intValue(rev.get("version"))),
                "state", state
        );
        put("/controller-services/" + csId + "/run-status", body);
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
        } catch (Exception ignored) {
        }
        final Map<String, Object> entity = get("/controller-services/" + csId);
        final Map<String, Object> rev = (Map<String, Object>) entity.get("revision");
        delete("/controller-services/" + csId, "version=" + intValue(rev.get("version")) + "&clientId=" + UUID.randomUUID());
    }

    @Override
    public Map<String, Object> createParameterContext(final String name, final Map<String, String> parameters, final String description) {
        final List<Map<String, Object>> paramList = new ArrayList<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            paramList.add(Map.of("parameter", Map.of("name", entry.getKey(), "value", entry.getValue(), "sensitive", false, "description", "")));
        }
        final Map<String, Object> body = Map.of(
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", 0),
                "component", Map.of("name", name, "description", description == null ? "" : description, "parameters", paramList)
        );
        final Map<String, Object> data = post("/parameter-contexts", body);
        final Map<String, Object> comp = (Map<String, Object>) data.get("component");
        return Map.of("id", data.get("id"), "name", comp.get("name"));
    }

    @Override
    public void bindParameterContextToProcessGroup(final String pgId, final String pcId) {
        final Map<String, Object> entity = get("/process-groups/" + pgId);
        final Map<String, Object> rev = (Map<String, Object>) entity.get("revision");
        final Map<String, Object> body = Map.of(
                "revision", Map.of("clientId", UUID.randomUUID().toString(), "version", intValue(rev.get("version"))),
                "component", Map.of("id", pgId, "parameterContext", Map.of("id", pcId))
        );
        put("/process-groups/" + pgId, body);
    }

    @Override
    public void deleteParameterContext(final String pcId) {
        final Map<String, Object> entity = get("/parameter-contexts/" + pcId);
        final Map<String, Object> rev = (Map<String, Object>) entity.get("revision");
        delete("/parameter-contexts/" + pcId, "version=" + intValue(rev.get("version")) + "&clientId=" + UUID.randomUUID());
    }

    @Override
    public Map<String, Object> snapshotProcessGroup(final String pgId) {
        final Map<String, Object> flowData = getProcessGroupFlow(pgId);
        final Map<String, Object> processGroupFlow = (Map<String, Object>) flowData.getOrDefault("processGroupFlow", Map.of());
        final Map<String, Object> flow = (Map<String, Object>) processGroupFlow.getOrDefault("flow", Map.of());
        return Map.of(
                "pg_id", pgId,
                "processors", flow.getOrDefault("processors", List.of()),
                "connections", flow.getOrDefault("connections", List.of()),
                "processGroups", flow.getOrDefault("processGroups", List.of())
        );
    }

    @Override
    public void restoreFromSnapshot(final Map<String, Object> snapshot) {
        final String pgId = String.valueOf(snapshot.get("pg_id"));
        final Set<String> snapProcIds = extractIds((List<Map<String, Object>>) snapshot.getOrDefault("processors", List.of()));
        final Set<String> snapConnIds = extractIds((List<Map<String, Object>>) snapshot.getOrDefault("connections", List.of()));
        final Set<String> snapChildPgIds = extractIds((List<Map<String, Object>>) snapshot.getOrDefault("processGroups", List.of()));

        final Map<String, Object> currentFlow = getProcessGroupFlow(pgId);
        final Map<String, Object> current = (Map<String, Object>) ((Map<String, Object>) currentFlow.getOrDefault("processGroupFlow", Map.of()))
                .getOrDefault("flow", Map.of());

        for (Map<String, Object> proc : (List<Map<String, Object>>) current.getOrDefault("processors", List.of())) {
            final String id = String.valueOf(proc.get("id"));
            if (!snapProcIds.contains(id)) {
                try { deleteProcessor(id); } catch (Exception ignored) {}
            }
        }
        for (Map<String, Object> conn : (List<Map<String, Object>>) current.getOrDefault("connections", List.of())) {
            final String id = String.valueOf(conn.get("id"));
            if (!snapConnIds.contains(id)) {
                try { deleteConnection(id); } catch (Exception ignored) {}
            }
        }
        for (Map<String, Object> child : (List<Map<String, Object>>) current.getOrDefault("processGroups", List.of())) {
            final String id = String.valueOf(child.get("id"));
            if (!snapChildPgIds.contains(id)) {
                try {
                    final int rev = intValue(((Map<String, Object>) child.getOrDefault("revision", Map.of())).get("version"));
                    delete("/process-groups/" + id, "version=" + rev + "&clientId=" + UUID.randomUUID());
                } catch (Exception ignored) {}
            }
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

    private static int intValue(final Object v) {
        return NiFiClientOperations.intValue(v);
    }

    private static double doubleValue(final Object v) {
        return NiFiClientOperations.doubleValue(v);
    }

    private static void sleep(final long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}
