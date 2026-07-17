package org.apache.nifi.copilot.service;

import java.util.Map;
import java.util.UUID;

/**
 * Immutable value type representing a NiFi component revision used for
 * optimistic-concurrency control.
 *
 * <p>Parsing uses {@link NiFiClientOperations#strictRevisionVersion(Object)} so
 * missing or invalid versions always fail explicitly rather than silently
 * defaulting to zero.
 *
 * <p>The {@code clientId} in incoming entity revision maps is validated when
 * present (must not be blank) but is never re-used in outgoing request bodies:
 * {@link #toMap()} always generates a fresh random client ID.
 */
public record NiFiRevision(long version) {

    /**
     * Parses the {@code "revision"} nested map from the given entity map.
     *
     * @param entity the entity map returned by a NiFi GET call
     * @return the parsed revision
     * @throws IllegalStateException if {@code "revision"} is missing or is not a map
     */
    public static NiFiRevision fromEntity(final Map<String, Object> entity) {
        final Object revObj = entity.get("revision");
        if (!(revObj instanceof Map<?, ?> rawRev)) {
            throw new IllegalStateException(
                    "Entity is missing required 'revision' map for update/delete operations");
        }
        @SuppressWarnings("unchecked")
        final Map<String, Object> revisionMap = (Map<String, Object>) rawRev;
        return fromRevisionMap(revisionMap);
    }

    /**
     * Parses a revision map directly.
     *
     * <p>Validates {@code clientId} when present (must not be blank).
     * Uses {@link NiFiClientOperations#strictRevisionVersion(Object)} for the
     * {@code "version"} field so missing or non-numeric values throw explicitly.
     *
     * @param revisionMap the raw revision map (e.g. from {@code entity.get("revision")})
     * @return the parsed revision
     * @throws IllegalStateException if {@code version} is missing, invalid, or {@code clientId} is blank
     */
    public static NiFiRevision fromRevisionMap(final Map<String, Object> revisionMap) {
        final Object rawClientId = revisionMap.get("clientId");
        if (rawClientId != null && (!(rawClientId instanceof String clientId) || clientId.isBlank())) {
            throw new IllegalStateException("Revision 'clientId' must be a non-blank string when present");
        }
        return new NiFiRevision(NiFiClientOperations.strictRevisionVersion(revisionMap.get("version")));
    }

    /**
     * Creates a zero revision for new-component creation requests.
     *
     * @return a revision with version {@code 0}
     */
    public static NiFiRevision zero() {
        return new NiFiRevision(0L);
    }

    /**
     * Returns a revision map with the stored version and a fresh random client ID,
     * ready for use in NiFi API request bodies.
     *
     * @return immutable map with {@code "clientId"} (fresh UUID) and {@code "version"}
     */
    public Map<String, Object> toMap() {
        return Map.of("clientId", UUID.randomUUID().toString(), "version", version);
    }
}
