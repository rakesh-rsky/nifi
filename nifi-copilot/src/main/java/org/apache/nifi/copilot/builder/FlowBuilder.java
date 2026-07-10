package org.apache.nifi.copilot.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class FlowBuilder {
    private static final Logger logger = LoggerFactory.getLogger(FlowBuilder.class);
    private static final int CANVAS_START_X = 100;
    private static final int CANVAS_START_Y = 200;
    private static final int PROCESSOR_SPACING_X = 400;
    private static final int PROCESSOR_SPACING_Y = 250;
    private static final int COLLISION_MARGIN_X = 360;
    private static final int COLLISION_MARGIN_Y = 140;

    private static final Map<String, String> PROCESSOR_REGISTRY = Map.ofEntries(
            Map.entry("kafka", "org.apache.nifi.kafka.processors.ConsumeKafka"),
            Map.entry("kafka-consume", "org.apache.nifi.kafka.processors.ConsumeKafka"),
            Map.entry("kafka-publish", "org.apache.nifi.kafka.processors.PublishKafka"),
            Map.entry("mqtt", "org.apache.nifi.processors.mqtt.ConsumeMQTT"),
            Map.entry("mqtt-consume", "org.apache.nifi.processors.mqtt.ConsumeMQTT"),
            Map.entry("mqtt-publish", "org.apache.nifi.processors.mqtt.PublishMQTT"),
            Map.entry("getfile", "org.apache.nifi.processors.standard.GetFile"),
            Map.entry("putfile", "org.apache.nifi.processors.standard.PutFile"),
            Map.entry("listfile", "org.apache.nifi.processors.standard.ListFile"),
            Map.entry("fetchfile", "org.apache.nifi.processors.standard.FetchFile"),
            Map.entry("s3", "org.apache.nifi.processors.aws.s3.PutS3Object"),
            Map.entry("s3-put", "org.apache.nifi.processors.aws.s3.PutS3Object"),
            Map.entry("s3-fetch", "org.apache.nifi.processors.aws.s3.FetchS3Object"),
            Map.entry("s3-list", "org.apache.nifi.processors.aws.s3.ListS3"),
            Map.entry("http", "org.apache.nifi.processors.standard.InvokeHTTP"),
            Map.entry("invokehttp", "org.apache.nifi.processors.standard.InvokeHTTP"),
            Map.entry("json", "org.apache.nifi.processors.standard.EvaluateJsonPath"),
            Map.entry("xpath", "org.apache.nifi.processors.standard.EvaluateXPath"),
            Map.entry("jolt", "org.apache.nifi.processors.standard.JoltTransformJSON"),
            Map.entry("convert", "org.apache.nifi.processors.standard.ConvertRecord"),
            Map.entry("replace", "org.apache.nifi.processors.standard.ReplaceText"),
            Map.entry("split", "org.apache.nifi.processors.standard.SplitText"),
            Map.entry("merge", "org.apache.nifi.processors.standard.MergeContent"),
            Map.entry("route", "org.apache.nifi.processors.standard.RouteOnAttribute"),
            Map.entry("updateattr", "org.apache.nifi.processors.standard.UpdateAttribute"),
            Map.entry("sql", "org.apache.nifi.processors.standard.ExecuteSQL"),
            Map.entry("putdb", "org.apache.nifi.processors.standard.PutDatabaseRecord"),
            Map.entry("log", "org.apache.nifi.processors.standard.LogAttribute"),
            Map.entry("logattribute", "org.apache.nifi.processors.standard.LogAttribute"),
            Map.entry("generate", "org.apache.nifi.processors.standard.GenerateFlowFile"),
            Map.entry("azureblob", "org.apache.nifi.processors.azure.storage.PutAzureBlobStorage_v12")
    );

    public record BuildResult(List<Map<String, Object>> createdProcessors, int connectionsCreated) {
    }

    public BuildResult buildFlow(
            final Map<String, Object> spec,
            final String processGroupId,
            final NiFiClientOperations nifi,
            final Map<String, String> existingIdMap,
            final int existingCount,
            final boolean autoStart,
            final boolean rollbackOnFailure) {
        final List<Map<String, Object>> processorsSpec = listOfMap(spec.get("processors"));
        if (processorsSpec.isEmpty()) {
            return new BuildResult(List.of(), 0);
        }

        String prePgId = nifi.getProcessGroupId(processGroupId);
        Map<String, Object> snapshot = null;
        if (rollbackOnFailure) {
            try {
                snapshot = nifi.snapshotProcessGroup(prePgId);
            } catch (Exception e) {
                logger.warn("Could not capture pre-deploy snapshot: {}", e.getMessage());
            }
        }

        final ControllerServiceManager csManager = new ControllerServiceManager();
        final ParameterContextManager pcManager = new ParameterContextManager();
        final List<Map<String, Object>> created = new ArrayList<>();
        int connectionsCreated = 0;

        try {
            applyDAGLayout(spec, existingCount);
            applyCollisionAvoidance(spec, prePgId, nifi);

            String pgId = prePgId;
            final Map<String, Object> processGroupSpec = mapOrNull(spec.get("process_group"));
            if (processGroupSpec != null) {
                final Map<String, Object> createdPg = nifi.createProcessGroup(
                        pgId,
                        String.valueOf(processGroupSpec.get("name")),
                        NiFiClientOperations.doubleValue(processGroupSpec.getOrDefault("x", 400)),
                        NiFiClientOperations.doubleValue(processGroupSpec.getOrDefault("y", 300)));
                pgId = String.valueOf(createdPg.get("id"));
            }

            final Map<String, Object> pcSpec = mapOrNull(spec.get("parameter_context"));
            if (pcSpec != null) {
                pcManager.deploy(pcSpec, pgId, nifi);
            }

            final List<Map<String, Object>> csSpec = listOfMap(spec.get("controller_services"));
            if (!csSpec.isEmpty()) {
                csManager.deployAll(csSpec, pgId, nifi);
            }

            nifi.ensureTypeCache();
            final Map<String, String> allIdMap = new HashMap<>(existingIdMap == null ? Map.of() : existingIdMap);

            for (Map<String, Object> proc : processorsSpec) {
                final String specId = String.valueOf(proc.get("id"));
                String ptype = String.valueOf(proc.get("type"));
                ptype = PROCESSOR_REGISTRY.getOrDefault(ptype.toLowerCase(), ptype);
                final String procName = proc.containsKey("name") && proc.get("name") != null
                        ? String.valueOf(proc.get("name"))
                        : ptype.substring(ptype.lastIndexOf('.') + 1);
                final Double x = proc.containsKey("x") ? NiFiClientOperations.doubleValue(proc.get("x")) : (double) CANVAS_START_X;
                final Double y = proc.containsKey("y") ? NiFiClientOperations.doubleValue(proc.get("y")) : (double) CANVAS_START_Y;
                final Map<String, Object> config = csManager.resolveCsReferences(mapOrEmpty(proc.get("config")));
                try {
                    final Map<String, Object> result = nifi.createProcessor(pgId, ptype, procName, x, y, config.isEmpty() ? null : config);
                    final Map<String, Object> createdEntry = new LinkedHashMap<>();
                    createdEntry.put("spec_id", specId);
                    createdEntry.putAll(result);
                    created.add(createdEntry);
                    allIdMap.put(specId, String.valueOf(result.get("id")));
                } catch (Exception e) {
                    logger.warn("Failed creating processor {}: {}", procName, e.getMessage());
                }
            }

            final List<Map<String, Object>> connections = listOfMap(spec.get("connections"));
            for (Map<String, Object> conn : connections) {
                final String source = allIdMap.get(String.valueOf(conn.get("from")));
                final String dest = allIdMap.get(String.valueOf(conn.get("to")));
                if (source == null || dest == null) {
                    continue;
                }
                final List<String> rels = toStringList(conn.get("relationships"));
                try {
                    nifi.createConnection(pgId, source, "PROCESSOR", dest, "PROCESSOR", rels.isEmpty() ? List.of("success") : rels);
                    connectionsCreated++;
                } catch (Exception e) {
                    logger.warn("Failed creating connection: {}", e.getMessage());
                }
            }

            for (Map<String, Object> p : created) {
                final String procNifiId = String.valueOf(p.get("id"));
                final Set<String> usedRels = new HashSet<>();
                for (Map<String, Object> conn : connections) {
                    final String fromSpec = String.valueOf(conn.get("from"));
                    if (procNifiId.equals(allIdMap.get(fromSpec))) {
                        usedRels.addAll(toStringList(conn.get("relationships")));
                    }
                }
                nifi.autoTerminateUnusedRelationships(procNifiId, usedRels);
            }

            if (autoStart) {
                final List<String> order = startupOrder(processorsSpec, connections);
                for (String specId : order) {
                    final String procId = allIdMap.get(specId);
                    if (procId == null) {
                        continue;
                    }
                    if (nifi.waitForProcessorValid(procId, 30)) {
                        try {
                            nifi.startProcessor(procId);
                        } catch (Exception e) {
                            logger.warn("Failed to start processor {}: {}", procId, e.getMessage());
                        }
                    }
                }
            }

            return new BuildResult(created, connectionsCreated);
        } catch (Exception e) {
            if (rollbackOnFailure && snapshot != null) {
                try {
                    nifi.restoreFromSnapshot(snapshot);
                    csManager.teardownAll(nifi);
                    pcManager.teardown(nifi);
                } catch (Exception rb) {
                    logger.error("Rollback also failed: {}", rb.getMessage());
                }
            }
            throw e;
        }
    }

    private void applyDAGLayout(final Map<String, Object> spec, final int existingCount) {
        final List<Map<String, Object>> processors = listOfMap(spec.get("processors"));
        final int xOffset = existingCount * PROCESSOR_SPACING_X;
        for (int i = 0; i < processors.size(); i++) {
            Map<String, Object> p = processors.get(i);
            if (!p.containsKey("x") || p.get("x") == null) {
                p.put("x", CANVAS_START_X + xOffset + i * PROCESSOR_SPACING_X);
            }
            if (!p.containsKey("y") || p.get("y") == null) {
                p.put("y", CANVAS_START_Y);
            }
        }
    }

    private void applyCollisionAvoidance(final Map<String, Object> spec, final String pgId, final NiFiClientOperations nifi) {
        try {
            final List<double[]> occupied = nifi.getOccupiedPositions(pgId);
            final CollisionAvoider avoider = new CollisionAvoider(occupied);
            for (Map<String, Object> p : listOfMap(spec.get("processors"))) {
                final double[] claimed = avoider.claim(
                        NiFiClientOperations.doubleValue(p.get("x")),
                        NiFiClientOperations.doubleValue(p.get("y")));
                p.put("x", claimed[0]);
                p.put("y", claimed[1]);
            }
        } catch (Exception e) {
            logger.warn("Collision avoidance unavailable: {}", e.getMessage());
        }
    }

    public Map<String, Object> readCanvas(final NiFiClientOperations nifi, final String processGroupId) {
        final String pgId = nifi.getProcessGroupId(processGroupId);
        final Map<String, Object> flowData = nifi.getProcessGroupFlow(pgId);
        final Map<String, Object> flow = mapOrEmpty(mapOrEmpty(flowData.get("processGroupFlow")).get("flow"));
        final List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> entity : listOfMap(flow.get("processors"))) {
            final Map<String, Object> comp = mapOrEmpty(entity.get("component"));
            final Map<String, Object> pos = mapOrEmpty(entity.get("position"));
            final String name = String.valueOf(comp.getOrDefault("name", ""));
            out.add(Map.of(
                    "nifi_id", entity.get("id"),
                    "spec_id", name.toLowerCase().replace(" ", "_").replace("-", "_"),
                    "name", name,
                    "type", comp.getOrDefault("type", ""),
                    "x", pos.getOrDefault("x", 0),
                    "y", pos.getOrDefault("y", 0),
                    "state", comp.getOrDefault("state", "STOPPED")));
        }
        final List<Map<String, Object>> css = new ArrayList<>();
        try {
            for (Map<String, Object> cs : nifi.listControllerServices(pgId)) {
                css.add(Map.of(
                        "nifi_id", cs.get("id"),
                        "name", cs.get("name"),
                        "type", cs.get("type"),
                        "state", cs.getOrDefault("state", "DISABLED")));
            }
        } catch (Exception e) {
            logger.warn("Could not read controller services: {}", e.getMessage());
        }
        return Map.of("process_group_id", pgId, "processors", out, "controller_services", css);
    }

    private List<String> startupOrder(final List<Map<String, Object>> processors, final List<Map<String, Object>> connections) {
        final List<String> ids = new ArrayList<>();
        for (Map<String, Object> p : processors) {
            ids.add(String.valueOf(p.get("id")));
        }
        return ids;
    }

    private static Map<String, Object> mapOrEmpty(final Object in) {
        if (in instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return new HashMap<>();
    }

    private static Map<String, Object> mapOrNull(final Object in) {
        if (in instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return null;
    }

    private static List<Map<String, Object>> listOfMap(final Object in) {
        if (!(in instanceof List<?> list)) {
            return new ArrayList<>();
        }
        final List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : list) {
            if (o instanceof Map<?, ?> map) {
                out.add((Map<String, Object>) map);
            }
        }
        return out;
    }

    private static List<String> toStringList(final Object in) {
        if (!(in instanceof List<?> list)) {
            return new ArrayList<>();
        }
        final List<String> out = new ArrayList<>();
        for (Object o : list) {
            out.add(String.valueOf(o));
        }
        return out;
    }

    private static final class CollisionAvoider {
        private final List<double[]> occupied;

        private CollisionAvoider(final List<double[]> occupied) {
            this.occupied = new ArrayList<>(occupied);
        }

        private boolean isFree(final double x, final double y) {
            for (double[] p : occupied) {
                if (Math.abs(p[0] - x) < COLLISION_MARGIN_X && Math.abs(p[1] - y) < COLLISION_MARGIN_Y) {
                    return false;
                }
            }
            return true;
        }

        private double[] claim(final double x, final double y) {
            if (isFree(x, y)) {
                final double[] pos = new double[]{x, y};
                occupied.add(pos);
                return pos;
            }
            for (int row = 0; row < 20; row++) {
                for (int col = 0; col < 20; col++) {
                    final double cx = x + col * PROCESSOR_SPACING_X;
                    final double cy = y + row * PROCESSOR_SPACING_Y;
                    if (isFree(cx, cy)) {
                        final double[] pos = new double[]{cx, cy};
                        occupied.add(pos);
                        return pos;
                    }
                }
            }
            final double[] fallback = new double[]{x + 20 * PROCESSOR_SPACING_X, y};
            occupied.add(fallback);
            return fallback;
        }
    }

    private static final class ControllerServiceManager {
        private final Map<String, String> specToNifi = new HashMap<>();
        private final List<String> created = new ArrayList<>();

        private void deployAll(final List<Map<String, Object>> services, final String pgId, final NiFiClientOperations nifi) {
            for (Map<String, Object> cs : services) {
                try {
                    final Map<String, Object> r = nifi.createControllerService(
                            pgId,
                            String.valueOf(cs.get("type")),
                            String.valueOf(cs.get("name")),
                            mapOrEmpty(cs.get("properties")));
                    final String nifiId = String.valueOf(r.get("id"));
                    specToNifi.put(String.valueOf(cs.get("id")), nifiId);
                    created.add(nifiId);
                    nifi.enableControllerService(nifiId);
                } catch (Exception ignored) {
                }
            }
        }

        private Map<String, Object> resolveCsReferences(final Map<String, Object> properties) {
            if (specToNifi.isEmpty()) {
                return properties;
            }
            final Map<String, Object> out = new HashMap<>();
            for (var e : properties.entrySet()) {
                out.put(e.getKey(), specToNifi.getOrDefault(String.valueOf(e.getValue()), String.valueOf(e.getValue())));
            }
            return out;
        }

        private void teardownAll(final NiFiClientOperations nifi) {
            for (int i = created.size() - 1; i >= 0; i--) {
                try {
                    nifi.deleteControllerService(created.get(i));
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static final class ParameterContextManager {
        private String pcId;

        private void deploy(final Map<String, Object> pcSpec, final String pgId, final NiFiClientOperations nifi) {
            final Map<String, String> params = new HashMap<>();
            for (var e : mapOrEmpty(pcSpec.get("parameters")).entrySet()) {
                params.put(e.getKey(), String.valueOf(e.getValue()));
            }
            final Map<String, Object> res = nifi.createParameterContext(
                    String.valueOf(pcSpec.get("name")),
                    params,
                    String.valueOf(pcSpec.getOrDefault("description", "")));
            pcId = String.valueOf(res.get("id"));
            nifi.bindParameterContextToProcessGroup(pgId, pcId);
        }

        private void teardown(final NiFiClientOperations nifi) {
            if (pcId != null && !pcId.isBlank()) {
                try {
                    nifi.deleteParameterContext(pcId);
                } catch (Exception ignored) {
                }
            }
        }
    }
}
