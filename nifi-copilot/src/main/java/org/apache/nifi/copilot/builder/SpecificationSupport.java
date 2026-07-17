package org.apache.nifi.copilot.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class SpecificationSupport {
    private SpecificationSupport() {
    }

    static Map<String, Object> mapOrEmpty(final Object in) {
        if (in instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return new HashMap<>();
    }

    static Map<String, Object> copySpecification(final Map<String, Object> specification) {
        Objects.requireNonNull(specification, "specification must not be null");
        return copyMap(specification);
    }

    private static Map<String, Object> copyMap(final Map<?, ?> source) {
        final Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(String.valueOf(key), copyValue(value)));
        return copy;
    }

    private static Object copyValue(final Object value) {
        if (value instanceof Map<?, ?> map) {
            return copyMap(map);
        }
        if (value instanceof List<?> list) {
            final List<Object> copy = new ArrayList<>(list.size());
            list.forEach(item -> copy.add(copyValue(item)));
            return copy;
        }
        return value;
    }

    static Map<String, Object> mapOrNull(final Object in) {
        if (in instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return null;
    }

    static List<Map<String, Object>> listOfMap(final Object in) {
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

    static List<String> toStringList(final Object in) {
        if (!(in instanceof List<?> list)) {
            return new ArrayList<>();
        }
        final List<String> out = new ArrayList<>();
        for (Object o : list) {
            out.add(String.valueOf(o));
        }
        return out;
    }

    static boolean hasDeployableWork(final Map<String, Object> spec) {
        if (mapOrNull(spec.get("process_group")) != null || mapOrNull(spec.get("parameter_context")) != null) {
            return true;
        }
        for (String key : List.of("controller_services", "processors", "input_ports", "output_ports",
                "funnels", "labels", "remote_process_groups", "connections", "snippets")) {
            if (!listOfMap(spec.get(key)).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    static Map<String, String> scalarStringMap(final Object value, final String description) {
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException(description + " must be a map");
        }
        final Map<String, String> converted = new LinkedHashMap<>();
        for (var entry : map.entrySet()) {
            final Object styleValue = entry.getValue();
            if (!(styleValue instanceof String || styleValue instanceof Number
                    || styleValue instanceof Boolean || styleValue instanceof Character
                    || styleValue instanceof Enum<?>)) {
                throw new IllegalArgumentException(description + " value for '" + entry.getKey()
                        + "' must be scalar");
            }
            converted.put(String.valueOf(entry.getKey()), String.valueOf(styleValue));
        }
        return converted;
    }

    static Map<String, Object> optionalMapField(
            final Map<String, Object> specification,
            final String field) {
        final Object value = specification.get(field);
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("'" + field + "' must be a map");
        }
        return mapOrEmpty(value);
    }

    static String requireNonBlank(final Object value, final String description) {
        if (value == null || String.valueOf(value).isBlank()) {
            throw new IllegalArgumentException(description + " must not be blank");
        }
        return String.valueOf(value);
    }

    static String optionalState(
            final Object value,
            final Set<String> allowed,
            final String description) {
        if (value == null) {
            return null;
        }
        final String state = String.valueOf(value).trim().toUpperCase(Locale.ROOT);
        if (!allowed.contains(state)) {
            throw new IllegalArgumentException(description + " must be one of " + allowed);
        }
        return state;
    }

    static String stringOrNull(final Object value) {
        return value == null ? null : String.valueOf(value);
    }
}
