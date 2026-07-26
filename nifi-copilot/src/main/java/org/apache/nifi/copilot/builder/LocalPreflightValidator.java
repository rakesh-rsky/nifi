package org.apache.nifi.copilot.builder;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class LocalPreflightValidator {
    private LocalPreflightValidator() {
    }

    static void validateComponentSpecIds(final Map<String, Object> spec) {
        final Set<String> seen = new HashSet<>();
        for (String collection : List.of("processors", "input_ports", "output_ports",
                "funnels", "labels", "remote_process_groups")) {
            for (Map<String, Object> componentSpec : SpecificationSupport.listOfMap(spec.get(collection))) {
                validateUniqueId(componentSpec.get("id"), collection, seen);
            }
        }
        for (Map<String, Object> snippet : SpecificationSupport.listOfMap(spec.get("snippets"))) {
            if ("create".equals(normalizedOperation(snippet))) {
                validateUniqueId(snippet.get("id"), "snippets", seen);
            }
        }
    }

    static void validateConnectionTopology(final List<Map<String, Object>> connections) {
        for (int index = 0; index < connections.size(); index++) {
            final Map<String, Object> connection = connections.get(index);
            final String source = SpecificationSupport.requireNonBlank(
                    connection.get("from"), "connection from");
            final String destination = SpecificationSupport.requireNonBlank(
                    connection.get("to"), "connection to");
            if (source.equals(destination) && !Boolean.TRUE.equals(connection.get("allow_self_loop"))) {
                throw new IllegalArgumentException("Connection at index " + index + " links '" + source
                        + "' to itself; omit unused relationships so NiFi can auto-terminate them, "
                        + "or set allow_self_loop=true for an intentional feedback loop");
            }
        }
    }

    private static void validateUniqueId(
            final Object idValue,
            final String collection,
            final Set<String> seen) {
        if (idValue == null || String.valueOf(idValue).isBlank()) {
            throw new IllegalArgumentException(collection + " spec id must not be blank");
        }
        final String specId = String.valueOf(idValue);
        if (!seen.add(specId)) {
            throw new IllegalArgumentException("Duplicate component spec id across collections: " + specId);
        }
    }

    static void validateSnippetOperations(
            final List<Map<String, Object>> operations,
            final boolean rollbackOnFailure) {
        final Set<String> allowedSelections = Set.of("processGroups", "remoteProcessGroups", "processors",
                "inputPorts", "outputPorts", "connections", "labels", "funnels");
        final Set<String> createdSnippetIds = new HashSet<>();
        for (int index = 0; index < operations.size(); index++) {
            final Map<String, Object> operation = operations.get(index);
            final String name = normalizedOperation(operation);
            if (!Set.of("create", "move", "copy", "delete").contains(name)) {
                throw new IllegalArgumentException("Unsupported snippet operation at index " + index + ": " + name);
            }
            if ("create".equals(name)) {
                final String snippetId =
                        SpecificationSupport.requireNonBlank(operation.get("id"), "snippet create id");
                createdSnippetIds.add(snippetId);
                final Map<String, Object> selections =
                        SpecificationSupport.mapOrNull(operation.get("selections"));
                if (selections == null || selections.isEmpty()) {
                    throw new IllegalArgumentException("Snippet create selections must be a nonempty map");
                }
                for (String key : selections.keySet()) {
                    if (!allowedSelections.contains(key)) {
                        throw new IllegalArgumentException("Unsupported snippet selection key: " + key);
                    }
                    if (SpecificationSupport.mapOrNull(selections.get(key)) == null
                            || SpecificationSupport.mapOrEmpty(selections.get(key)).isEmpty()) {
                        throw new IllegalArgumentException(
                                "Snippet selection '" + key + "' must be a nonempty map");
                    }
                }
            } else {
                SpecificationSupport.requireNonBlank(operation.get("snippet_id"), "snippet_id");
                if ("move".equals(name) || "copy".equals(name)) {
                    SpecificationSupport.requireNonBlank(operation.get("destination_process_group_id"),
                            "destination_process_group_id");
                }
                if (rollbackOnFailure && "move".equals(name)
                        && !createdSnippetIds.contains(String.valueOf(operation.get("snippet_id")))) {
                    throw new IllegalArgumentException(
                            "Moving an externally owned snippet cannot be rolled back without its original parent");
                }
                if (rollbackOnFailure && ("copy".equals(name) || "delete".equals(name))) {
                    throw new IllegalArgumentException(
                            "Snippet " + name
                                    + " consumes or deletes its source and cannot be used with rollbackOnFailure=true");
                }
            }
        }
    }

    static String normalizedOperation(final Map<String, Object> operation) {
        final Object value = operation.get("operation");
        return value == null ? "" : String.valueOf(value).trim().toLowerCase(Locale.ROOT);
    }

    static void validateSpecificationCollections(final Map<String, Object> spec) {
        for (String key : List.of("process_group", "parameter_context")) {
            final Object value = spec.get(key);
            if (value != null && !(value instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("'" + key + "' must be a map");
            }
        }
        for (String key : List.of("controller_services", "processors", "input_ports", "output_ports",
                "funnels", "labels", "remote_process_groups", "connections", "snippets",
                "deletions", "cs_actions")) {
            final Object value = spec.get(key);
            if (value == null) {
                continue;
            }
            if (!(value instanceof List<?> list)) {
                throw new IllegalArgumentException("'" + key + "' must be a list");
            }
            for (Object item : list) {
                if (!(item instanceof Map<?, ?>)) {
                    throw new IllegalArgumentException("'" + key + "' entries must be maps");
                }
            }
        }
    }

    static void validateParameterContext(final Map<String, Object> pcSpec) {
        final Object nameValue = pcSpec.get("name");
        if (nameValue == null || String.valueOf(nameValue).isBlank()) {
            throw new IllegalArgumentException("Parameter context name must not be blank");
        }
        if (pcSpec.get("parameters") != null && !(pcSpec.get("parameters") instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Parameter context parameters must be a map");
        }
        for (var entry : SpecificationSupport.mapOrEmpty(pcSpec.get("parameters")).entrySet()) {
            if (entry.getKey() == null || String.valueOf(entry.getKey()).isBlank()) {
                throw new IllegalArgumentException("Parameter names must not be blank");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(
                        "Parameter '" + entry.getKey() + "' must not have a null value");
            }
        }
    }
}
