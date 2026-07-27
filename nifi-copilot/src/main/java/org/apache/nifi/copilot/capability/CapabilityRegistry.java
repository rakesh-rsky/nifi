package org.apache.nifi.copilot.capability;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.nifi.copilot.service.CapabilityDiscoveryException;
import org.apache.nifi.copilot.service.CapabilityTypeSelector;
import org.apache.nifi.copilot.service.NiFiClientOperations;

public final class CapabilityRegistry {
    public static final Map<String, String> LEGACY_PROCESSOR_ALIASES = Map.ofEntries(
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
            Map.entry("azureblob", "org.apache.nifi.processors.azure.storage.PutAzureBlobStorage_v12"));

    private final NiFiClientOperations client;
    private final Duration ttl;
    private final Clock clock;
    private final CapabilityDefinitionParser parser;
    private volatile PublishedCapabilities published;

    public CapabilityRegistry(final NiFiClientOperations client, final Duration ttl) {
        this(client, ttl, Clock.systemUTC(), new CapabilityDefinitionParser());
    }

    CapabilityRegistry(
            final NiFiClientOperations client,
            final Duration ttl,
            final Clock clock,
            final CapabilityDefinitionParser parser) {
        if (client == null || ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("Client and positive capability cache TTL are required");
        }
        if (clock == null || parser == null) {
            throw new IllegalArgumentException("Capability cache collaborators are required");
        }
        this.client = client;
        this.ttl = ttl;
        this.clock = clock;
        this.parser = parser;
    }



    public CapabilitySnapshot snapshot() {
        return capabilities().snapshot();
    }

    public CapabilityGraph graph() {
        return capabilities().graph();
    }

    public CapabilitySet capabilitySet() {
        final PublishedCapabilities current = capabilities();
        return new CapabilitySet(current.snapshot(), current.graph());
    }

    public synchronized CapabilitySnapshot refresh() {
        return discoverAndPublish(true).snapshot();
    }

    public Optional<ProcessorCapability> resolveProcessor(final String requestedType) {
        return resolve(requestedType, snapshot().processors(), LEGACY_PROCESSOR_ALIASES);
    }

    public Optional<ControllerServiceCapability> resolveControllerService(final String requestedType) {
        return resolve(requestedType, snapshot().controllerServices(), Map.of());
    }

    public ControllerServiceCapability requireControllerService(final String requestedType) {
        return resolveControllerService(requestedType).orElseThrow(() ->
                new UnsupportedControllerServiceException(
                        "Controller service type is unavailable or ambiguous: " + requestedType));
    }

    private PublishedCapabilities capabilities() {
        final PublishedCapabilities current = published;
        if (isCurrent(current)) {
            return current;
        }
        synchronized (this) {
            final PublishedCapabilities rechecked = published;
            if (isCurrent(rechecked)) {
                return rechecked;
            }
            return discoverAndPublish(rechecked != null);
        }
    }

    private boolean isCurrent(final PublishedCapabilities capabilities) {
        return capabilities != null
                && clock.instant().isBefore(capabilities.snapshot().discoveredAt().plus(ttl));
    }

    private PublishedCapabilities discoverAndPublish(final boolean refreshClient) {
        try {
            if (refreshClient) {
                client.refreshCapabilityCaches();
            }
            final List<Map<String, Object>> processorTypes = client.listProcessorTypes();
            final List<Map<String, Object>> serviceTypes = client.listControllerServiceTypes();
            if (processorTypes == null || serviceTypes == null) {
                throw new CapabilityDiscoveryException("NiFi capability type discovery returned null");
            }
            final Map<String, ProcessorCapability> processors = new LinkedHashMap<>();
            for (Map<String, Object> type : CapabilityTypeSelector.preferredTypes(processorTypes)) {
                final Coordinates coordinates = coordinates(type);
                final Map<String, Object> definition = client.getProcessorDefinition(
                        coordinates.bundle.group(), coordinates.bundle.artifact(),
                        coordinates.bundle.version(), coordinates.type);
                final ProcessorCapability capability = parser.parseProcessor(type, definition);
                processors.put(capability.type(), capability);
            }
            final Map<String, ControllerServiceCapability> services = new LinkedHashMap<>();
            for (Map<String, Object> type : CapabilityTypeSelector.preferredTypes(serviceTypes)) {
                final Coordinates coordinates = coordinates(type);
                final Map<String, Object> definition = client.getControllerServiceDefinition(
                        coordinates.bundle.group(), coordinates.bundle.artifact(),
                        coordinates.bundle.version(), coordinates.type);
                final ControllerServiceCapability capability = parser.parseControllerService(type, definition);
                services.put(capability.type(), capability);
            }
            final CapabilitySnapshot complete = new CapabilitySnapshot(processors, services, clock.instant());
            final PublishedCapabilities discovered =
                    new PublishedCapabilities(complete, CapabilityGraph.from(complete));
            published = discovered;
            return discovered;
        } catch (CapabilityDiscoveryException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new CapabilityDiscoveryException("Failed to discover NiFi capabilities; no new snapshot was published", e);
        }
    }

    private Coordinates coordinates(final Map<String, Object> type) {
        if (type == null) {
            throw new CapabilityDiscoveryException("NiFi capability type entry is null");
        }
        final Object typeName = type.get("type");
        final Object rawBundle = type.get("bundle");
        if (!(typeName instanceof String name) || name.isBlank() || !(rawBundle instanceof Map<?, ?> raw)) {
            throw new CapabilityDiscoveryException("NiFi capability type entry is incomplete");
        }
        final Object group = raw.get("group");
        final Object artifact = raw.get("artifact");
        final Object version = raw.get("version");
        if (!(group instanceof String g) || !(artifact instanceof String a) || !(version instanceof String v)) {
            throw new CapabilityDiscoveryException("NiFi capability bundle coordinates are incomplete for " + name);
        }
        return new Coordinates(name, new BundleCoordinate(g, a, v));
    }

    private <T> Optional<T> resolve(
            final String requestedType, final Map<String, T> capabilities, final Map<String, String> aliases) {
        if (requestedType == null || requestedType.isBlank()) {
            return Optional.empty();
        }
        final T exact = capabilities.get(requestedType);
        if (exact != null) {
            return Optional.of(exact);
        }
        final String normalized = requestedType.toLowerCase(Locale.ROOT);
        final String alias = aliases.get(normalized);
        if (alias != null && capabilities.containsKey(alias)) {
            return Optional.of(capabilities.get(alias));
        }
        T match = null;
        for (Map.Entry<String, T> entry : capabilities.entrySet()) {
            final String fqn = entry.getKey();
            final String simple = fqn.substring(fqn.lastIndexOf('.') + 1);
            if (simple.equalsIgnoreCase(requestedType)) {
                if (match != null) {
                    return Optional.empty();
                }
                match = entry.getValue();
            }
        }
        return Optional.ofNullable(match);
    }

    private record Coordinates(String type, BundleCoordinate bundle) {
    }

    private record PublishedCapabilities(CapabilitySnapshot snapshot, CapabilityGraph graph) {
    }

    public record CapabilitySet(CapabilitySnapshot snapshot, CapabilityGraph graph) {
        public CapabilitySet {
            if (snapshot == null || graph == null) {
                throw new IllegalArgumentException("Capability snapshot and graph are required");
            }
        }
    }
}
