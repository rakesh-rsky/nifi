package org.apache.nifi.copilot.builder;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

final class ComponentRegistry {
    private final Map<String, String> ids = new LinkedHashMap<>();
    private final Map<String, String> types = new LinkedHashMap<>();
    private final Map<String, String> nifiIdTypes = new LinkedHashMap<>();
    private final Map<String, String> snippetIds = new LinkedHashMap<>();
    private final Set<String> knownNifiIds = new HashSet<>();

    void register(final String specId, final String nifiId, final String type) {
        final String previousId = ids.putIfAbsent(specId, nifiId);
        if (previousId != null && !previousId.equals(nifiId)) {
            throw new IllegalStateException("Specification ID '" + specId
                    + "' resolved to conflicting NiFi IDs");
        }
        final String previousType = types.putIfAbsent(specId, type);
        if (previousType != null && !previousType.equals(type)) {
            throw new IllegalStateException("Specification ID '" + specId
                    + "' resolved to conflicting component types");
        }
        final String previousNifiIdType = nifiIdTypes.putIfAbsent(nifiId, type);
        if (previousNifiIdType != null && !previousNifiIdType.equals(type)) {
            throw new IllegalStateException("NiFi ID '" + nifiId
                    + "' resolved to conflicting component types");
        }
        knownNifiIds.add(nifiId);
    }

    void registerSnippet(final String specId, final String snippetId) {
        final String previous = snippetIds.putIfAbsent(specId, snippetId);
        if (previous != null && !previous.equals(snippetId)) {
            throw new IllegalStateException("Snippet spec ID '" + specId
                    + "' resolved to conflicting NiFi IDs");
        }
    }

    void recordProcessorId(final String specId, final String nifiId) {
        ids.put(specId, nifiId);
    }

    String id(final String reference) {
        return ids.get(reference);
    }

    String idOrReference(final String reference) {
        return ids.getOrDefault(reference, reference);
    }

    String type(final String reference) {
        return types.get(reference);
    }

    String snippetIdOrReference(final String reference) {
        return snippetIds.getOrDefault(reference, reference);
    }

    boolean contains(final String reference) {
        return ids.containsKey(reference);
    }

    boolean knowsNifiId(final String id) {
        return knownNifiIds.contains(id);
    }
}
