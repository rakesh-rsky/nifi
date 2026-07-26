/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.nifi.copilot.llm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Expands a generated DistributeLoad connection that routes multiple numbered
 * relationships to one worker into one distinct worker per relationship.
 */
final class ParallelWorkerNormalizer {

    void normalize(
            final List<Map<String, Object>> processors,
            final List<Map<String, Object>> connections) {
        final Map<String, Map<String, Object>> processorsById = new HashMap<>();
        final Set<String> usedIds = new HashSet<>();
        for (Map<String, Object> processor : processors) {
            final String id = String.valueOf(processor.getOrDefault("id", ""));
            if (!id.isBlank()) {
                processorsById.put(id, processor);
                usedIds.add(id);
            }
        }

        final List<Map<String, Object>> originalConnections = List.copyOf(connections);
        final List<Map<String, Object>> replacements = new ArrayList<>();
        final Set<Map<String, Object>> replaced = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());

        for (Map<String, Object> connection : originalConnections) {
            final String distributorId = String.valueOf(connection.getOrDefault("from", ""));
            final Map<String, Object> distributor = processorsById.get(distributorId);
            if (!isDistributeLoad(distributor)) {
                continue;
            }
            final List<String> relationships = numberedRelationships(connection.get("relationships"));
            if (relationships.size() < 2) {
                continue;
            }
            final String workerId = String.valueOf(connection.getOrDefault("to", ""));
            final Map<String, Object> worker = processorsById.get(workerId);
            if (worker == null || isDistributeLoad(worker)) {
                continue;
            }

            replaced.add(connection);
            for (int i = 0; i < relationships.size(); i++) {
                final String relationship = relationships.get(i);
                final String targetId;
                if (i == 0) {
                    targetId = workerId;
                } else {
                    targetId = uniqueCloneId(workerId, relationship, usedIds);
                    final Map<String, Object> clone = new LinkedHashMap<>(worker);
                    clone.put("id", targetId);
                    final String name = String.valueOf(worker.getOrDefault("name", workerId));
                    clone.put("name", name + " " + relationship);
                    processors.add(clone);
                    processorsById.put(targetId, clone);
                    for (Map<String, Object> outgoing : originalConnections) {
                        if (workerId.equals(String.valueOf(outgoing.get("from")))) {
                            final Map<String, Object> clonedConnection = new LinkedHashMap<>(outgoing);
                            clonedConnection.put("from", targetId);
                            replacements.add(clonedConnection);
                        }
                    }
                }
                final Map<String, Object> distributedConnection = new LinkedHashMap<>(connection);
                distributedConnection.put("to", targetId);
                distributedConnection.put("relationships", List.of(relationship));
                replacements.add(distributedConnection);
            }
        }

        if (!replaced.isEmpty()) {
            connections.removeIf(replaced::contains);
            connections.addAll(replacements);
        }
    }

    private static boolean isDistributeLoad(final Map<String, Object> processor) {
        return processor != null
                && String.valueOf(processor.getOrDefault("type", ""))
                        .toLowerCase(java.util.Locale.ROOT)
                        .endsWith("distributeload");
    }

    private static List<String> numberedRelationships(final Object value) {
        if (!(value instanceof List<?> relationships)) {
            return List.of();
        }
        final List<String> numbered = new ArrayList<>();
        for (Object relationship : relationships) {
            final String name = String.valueOf(relationship);
            if (!name.matches("\\d+")) {
                return List.of();
            }
            numbered.add(name);
        }
        return numbered;
    }

    private static String uniqueCloneId(
            final String workerId,
            final String relationship,
            final Set<String> usedIds) {
        final String base = workerId + "-parallel-" + relationship;
        String candidate = base;
        int suffix = 2;
        while (!usedIds.add(candidate)) {
            candidate = base + "-" + suffix++;
        }
        return candidate;
    }
}
