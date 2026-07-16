package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.entityId;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.entityName;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrNull;
import static org.apache.nifi.copilot.builder.SpecificationSupport.requireNonBlank;
import static org.apache.nifi.copilot.builder.SpecificationSupport.stringOrNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.nifi.copilot.service.NiFiClientOperations;

final class ComponentResolver {
    private static final Set<String> LOCAL_ENDPOINT_TYPES =
            Set.of("PROCESSOR", "INPUT_PORT", "OUTPUT_PORT", "FUNNEL");
    private static final Map<String, String> PROCESSOR_TYPES = Map.ofEntries(
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

    String resolveTargetProcessGroup(final String processGroupId, final NiFiClientOperations nifi) {
        try {
            final String resolvedProcessGroupId = nifi.getProcessGroupId(processGroupId);
            if (resolvedProcessGroupId == null || resolvedProcessGroupId.isBlank()) {
                throw new IllegalStateException("Could not resolve process group ID for: " + processGroupId);
            }
            return resolvedProcessGroupId;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve process group '" + processGroupId + "': " + e.getMessage(), e);
        }
    }

    Map<String, Object> findUniqueChildProcessGroup(
            final String parentProcessGroupId,
            final String name,
            final NiFiClientOperations nifi) {
        final List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> child : nifi.listChildProcessGroups(parentProcessGroupId)) {
            if (name.equals(entityName(child))) {
                matches.add(child);
            }
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple child process groups named '" + name
                    + "' under parent " + parentProcessGroupId + "; cannot resolve a unique target");
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    void registerInventory(final Map<String, Object> flow, final ComponentRegistry components) {
        final Map<String, String> collectionTypes = Map.of(
                "processGroups", "PROCESS_GROUP",
                "remoteProcessGroups", "REMOTE_PROCESS_GROUP",
                "processors", "PROCESSOR",
                "inputPorts", "INPUT_PORT",
                "outputPorts", "OUTPUT_PORT",
                "connections", "CONNECTION",
                "labels", "LABEL",
                "funnels", "FUNNEL");
        for (var collection : collectionTypes.entrySet()) {
            for (Map<String, Object> entity : listOfMap(flow.get(collection.getKey()))) {
                final String id = entityId(entity);
                if (id != null) {
                    components.register(id, id, collection.getValue());
                }
            }
        }
    }

    void registerExistingProcessors(
            final List<Map<String, Object>> processorSpecs,
            final Map<String, String> existingIdMap,
            final ComponentRegistry components) {
        if (existingIdMap == null) {
            return;
        }
        for (Map<String, Object> processor : processorSpecs) {
            final String specId = String.valueOf(processor.get("id"));
            final String nifiId = existingIdMap.get(specId);
            if (nifiId != null && !nifiId.isBlank()) {
                components.register(specId, nifiId, "PROCESSOR");
            }
        }
    }

    void registerProcessorTypes(
            final List<Map<String, Object>> processorSpecs,
            final ComponentRegistry components) {
        for (Map<String, Object> processor : processorSpecs) {
            final String specId = String.valueOf(processor.get("id"));
            if (components.contains(specId)) {
                components.register(specId, components.id(specId), "PROCESSOR");
            }
        }
    }

    String resolveProcessorType(final Map<String, Object> processorSpec) {
        final String type = String.valueOf(processorSpec.get("type"));
        return PROCESSOR_TYPES.getOrDefault(type.toLowerCase(), type);
    }

    String resolveProcessorName(final Map<String, Object> processorSpec, final String resolvedType) {
        return processorSpec.containsKey("name") && processorSpec.get("name") != null
                ? String.valueOf(processorSpec.get("name"))
                : resolvedType.substring(resolvedType.lastIndexOf('.') + 1);
    }

    Map<String, Object> matchPort(
            final List<Map<String, Object>> entities,
            final String id,
            final String name,
            final String resourceType) {
        return matchExactIdOrUniqueField(entities, id, "name", name, resourceType);
    }

    Map<String, Object> matchFunnel(final List<Map<String, Object>> entities, final String id) {
        return findUniqueEntityById(entities, id, "FUNNEL");
    }

    Map<String, Object> matchLabel(
            final List<Map<String, Object>> entities,
            final String id,
            final String text) {
        return matchExactIdOrUniqueField(entities, id, "label", text, "LABEL");
    }

    Map<String, Object> matchRemoteProcessGroup(
            final List<Map<String, Object>> entities,
            final String id,
            final String targetUri) {
        return matchExactIdOrUniqueField(
                entities, id, "targetUri", targetUri, "REMOTE_PROCESS_GROUP");
    }

    String resolveEndpointId(final Object reference, final ComponentRegistry components) {
        return components.id(String.valueOf(reference));
    }

    String resolveEndpointType(
            final String reference,
            final Object explicitValue,
            final ComponentRegistry components) {
        final String inferred = components.type(reference);
        if (inferred == null) {
            throw new IllegalArgumentException("Connection endpoint type is unknown for reference: " + reference);
        }
        if (!LOCAL_ENDPOINT_TYPES.contains(inferred)) {
            throw new IllegalArgumentException("Unsupported local connection endpoint type: " + inferred);
        }
        if (explicitValue != null) {
            final String explicit = String.valueOf(explicitValue).trim().toUpperCase(Locale.ROOT);
            if (!LOCAL_ENDPOINT_TYPES.contains(explicit)) {
                throw new IllegalArgumentException("Unsupported explicit connection endpoint type: " + explicit);
            }
            if (!explicit.equals(inferred)) {
                throw new IllegalArgumentException("Explicit endpoint type " + explicit
                        + " disagrees with inferred type " + inferred + " for " + reference);
            }
        }
        return inferred;
    }

    Map<String, Object> matchConnection(
            final List<Map<String, Object>> existingConnections,
            final String source,
            final String sourceType,
            final String destination,
            final String destinationType) {
        final List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> connection : existingConnections) {
            final Map<String, Object> component = mapOrEmpty(connection.get("component"));
            final Map<String, Object> existingSource = mapOrEmpty(component.get("source"));
            final Map<String, Object> existingDestination = mapOrEmpty(component.get("destination"));
            final String connectionSource = String.valueOf(existingSource.get("id"));
            final String connectionDestination = String.valueOf(existingDestination.get("id"));
            final String existingSourceType = stringOrNull(existingSource.get("type"));
            final String existingDestinationType = stringOrNull(existingDestination.get("type"));
            if (source.equals(connectionSource) && destination.equals(connectionDestination)
                    && typeMatches(sourceType, existingSourceType)
                    && typeMatches(destinationType, existingDestinationType)) {
                matches.add(connection);
            }
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple existing connections from " + source + " to " + destination
                    + "; cannot resolve a unique connection to reuse");
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    Map<String, Object> resolveSnippetSelections(
            final Map<String, Object> requested,
            final ComponentRegistry components) {
        final Map<String, Object> resolved = new LinkedHashMap<>();
        for (var entry : requested.entrySet()) {
            final Map<String, Object> ids = mapOrNull(entry.getValue());
            if (ids == null || ids.isEmpty()) {
                throw new IllegalArgumentException(
                        "Snippet selection '" + entry.getKey() + "' must be a nonempty map");
            }
            final Map<String, Object> mapped = new LinkedHashMap<>();
            for (var selected : ids.entrySet()) {
                final String reference = selected.getKey();
                final String nifiId = components.idOrReference(reference);
                if (!components.knowsNifiId(nifiId)) {
                    throw new IllegalArgumentException("Unknown snippet component reference: " + reference);
                }
                mapped.put(nifiId, selected.getValue());
            }
            resolved.put(entry.getKey(), mapped);
        }
        return resolved;
    }

    String resolveSnippetId(final Object reference, final ComponentRegistry components) {
        final String value = requireNonBlank(reference, "snippet_id");
        return components.snippetIdOrReference(value);
    }

    String resolveProcessGroupReference(final Object reference, final ComponentRegistry components) {
        final String value = requireNonBlank(reference, "destination_process_group_id");
        final String resolved = components.idOrReference(value);
        final String type = components.type(value);
        if (type != null && !"PROCESS_GROUP".equals(type)) {
            throw new IllegalArgumentException("Destination reference '" + value + "' is not a process group");
        }
        return resolved;
    }

    Map<String, Object> findUniqueParameterContext(
            final String name,
            final NiFiClientOperations nifi) {
        final List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> context : nifi.listParameterContexts()) {
            if (name.equals(entityName(context))) {
                matches.add(context);
            }
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple parameter contexts named '" + name
                    + "'; cannot resolve a unique context");
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    boolean isParameterContextCompatible(
            final Map<String, Object> context,
            final Map<String, String> requested) {
        final Map<String, String> nonSensitive = new HashMap<>();
        final Map<String, Object> component = mapOrEmpty(context.get("component"));
        for (Map<String, Object> parameterEntry : listOfMap(component.get("parameters"))) {
            final Map<String, Object> parameter = mapOrEmpty(parameterEntry.get("parameter"));
            final Object parameterName = parameter.get("name");
            if (parameterName == null || Boolean.parseBoolean(String.valueOf(parameter.get("sensitive")))) {
                continue;
            }
            final Object value = parameter.get("value");
            nonSensitive.put(String.valueOf(parameterName), value == null ? null : String.valueOf(value));
        }
        for (var entry : requested.entrySet()) {
            if (!nonSensitive.containsKey(entry.getKey())
                    || !Objects.equals(nonSensitive.get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    Map<String, Object> matchControllerService(
            final List<Map<String, Object>> existing,
            final String name,
            final String type) {
        final List<Map<String, Object>> sameName = new ArrayList<>();
        final List<Map<String, Object>> sameNameAndType = new ArrayList<>();
        for (Map<String, Object> service : existing) {
            if (name.equals(String.valueOf(service.get("name")))) {
                sameName.add(service);
                if (type.equals(String.valueOf(service.get("type")))) {
                    sameNameAndType.add(service);
                }
            }
        }
        if (sameNameAndType.size() > 1) {
            throw new IllegalStateException("Multiple controller services named '" + name
                    + "' of type '" + type + "'; cannot resolve a unique service to reuse");
        }
        if (sameNameAndType.size() == 1) {
            return sameNameAndType.get(0);
        }
        if (!sameName.isEmpty()) {
            throw new IllegalStateException("Controller service named '" + name
                    + "' already exists with a conflicting type; expected type '" + type + "'");
        }
        return null;
    }

    private Map<String, Object> matchExactIdOrUniqueField(
            final List<Map<String, Object>> entities,
            final String id,
            final String field,
            final String value,
            final String resourceType) {
        final Map<String, Object> idMatch = findUniqueEntityById(entities, id, resourceType);
        if (idMatch != null) {
            return idMatch;
        }
        final List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> entity : entities) {
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final Object candidate = component.containsKey(field) ? component.get(field) : entity.get(field);
            if (Objects.equals(value, candidate == null ? null : String.valueOf(candidate))) {
                matches.add(entity);
            }
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple " + resourceType + " entities match " + field
                    + " '" + value + "'");
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    private Map<String, Object> findUniqueEntityById(
            final List<Map<String, Object>> entities,
            final String id,
            final String resourceType) {
        final List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> entity : entities) {
            if (id.equals(entityId(entity))) {
                matches.add(entity);
            }
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple " + resourceType + " entities have ID " + id);
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    private boolean typeMatches(final String requested, final String existing) {
        return existing == null || existing.isBlank() || requested.equalsIgnoreCase(existing);
    }
}
