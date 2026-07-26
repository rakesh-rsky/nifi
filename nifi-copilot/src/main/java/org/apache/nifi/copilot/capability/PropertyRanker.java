package org.apache.nifi.copilot.capability;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.springframework.stereotype.Component;

@Component
public final class PropertyRanker {
    private static final Set<String> RUNTIME_TERMS = Set.of(
            "bulletin level",
            "concurrent tasks",
            "execution node",
            "penalty duration",
            "run duration",
            "scheduling period",
            "scheduling strategy",
            "yield duration");
    private static final Set<String> DATA_CONTRACT_TERMS = Set.of(
            "character set",
            "charset",
            "content type",
            "date format",
            "decimal format",
            "delimiter",
            "encoding",
            "escape character",
            "field separator",
            "header",
            "line separator",
            "mime type",
            "null string",
            "quote character",
            "record separator",
            "schema",
            "time format",
            "timestamp format");

    public List<RankedProperty> rank(final Map<String, PropertyNode> properties) {
        if (properties == null || properties.isEmpty()) {
            return List.of();
        }
        return properties.values().stream()
                .map(this::classify)
                .sorted(Comparator
                        .comparingInt(RankedProperty::priority)
                        .thenComparing(ranked -> ranked.property().name()))
                .toList();
    }

    public RankedProperty classify(final PropertyNode property) {
        if (property == null) {
            throw new IllegalArgumentException("Property is required");
        }
        final PropertyClass propertyClass;
        if (property.required() && property.requiredControllerServiceApi() != null) {
            propertyClass = PropertyClass.REQUIRED_SERVICE_REFERENCE;
        } else if (property.required()) {
            propertyClass = PropertyClass.REQUIRED;
        } else if (property.requiredControllerServiceApi() != null) {
            propertyClass = PropertyClass.SERVICE_REFERENCE;
        } else {
            final String searchableName = normalizedName(property);
            if (containsTerm(searchableName, RUNTIME_TERMS)) {
                propertyClass = PropertyClass.RUNTIME;
            } else if (containsTerm(searchableName, DATA_CONTRACT_TERMS)) {
                propertyClass = PropertyClass.DATA_CONTRACT;
            } else {
                propertyClass = PropertyClass.OPTIONAL;
            }
        }
        return new RankedProperty(property, propertyClass);
    }

    private boolean containsTerm(final String value, final Set<String> terms) {
        return terms.stream().anyMatch(term -> containsPhrase(value, term));
    }

    private boolean containsPhrase(final String value, final String term) {
        return (" " + value + " ").contains(" " + term + " ");
    }

    private String normalizedName(final PropertyNode property) {
        return normalize(property.name() + " " + property.displayName());
    }

    private String normalize(final String value) {
        return value
                .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", " ")
                .trim();
    }

    public record RankedProperty(
            PropertyNode property,
            PropertyClass propertyClass) {
        public RankedProperty {
            if (property == null || propertyClass == null) {
                throw new IllegalArgumentException("Property and classification are required");
            }
        }

        public int priority() {
            return propertyClass.priority();
        }

        public boolean essential() {
            return propertyClass.essential();
        }
    }

    public enum PropertyClass {
        REQUIRED_SERVICE_REFERENCE(0, true),
        REQUIRED(1, true),
        SERVICE_REFERENCE(2, true),
        DATA_CONTRACT(3, true),
        OPTIONAL(4, false),
        RUNTIME(5, false);

        private final int priority;
        private final boolean essential;

        PropertyClass(final int priority, final boolean essential) {
            this.priority = priority;
            this.essential = essential;
        }

        public int priority() {
            return priority;
        }

        public boolean essential() {
            return essential;
        }
    }
}
