package org.apache.nifi.copilot.builder;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class NiFiEntitySupport {
    private NiFiEntitySupport() {
    }

    static String requireCreatedComponentId(
            final Map<String, Object> result,
            final String resourceType) {
        if (result != null) {
            Object id = result.get("id");
            if (id == null) {
                id = SpecificationSupport.mapOrEmpty(result.get("component")).get("id");
            }
            if (id != null && !String.valueOf(id).isBlank()) {
                return String.valueOf(id);
            }
        }
        throw new IllegalStateException("Created " + resourceType + " response did not contain an ID");
    }

    static String entityName(final Map<String, Object> entity) {
        final Object componentName = SpecificationSupport.mapOrEmpty(entity.get("component")).get("name");
        if (componentName != null) {
            return String.valueOf(componentName);
        }
        final Object topLevelName = entity.get("name");
        return topLevelName == null ? null : String.valueOf(topLevelName);
    }

    static String componentState(final Map<String, Object> entity, final String defaultState) {
        final Map<String, Object> component = SpecificationSupport.mapOrEmpty(entity.get("component"));
        return String.valueOf(componentValueOrDefault(component, entity, "state", defaultState))
                .toUpperCase(Locale.ROOT);
    }

    static String transmissionState(final Map<String, Object> entity) {
        final Map<String, Object> component = SpecificationSupport.mapOrEmpty(entity.get("component"));
        final Object transmission = componentValueOrDefault(component, entity, "transmissionStatus", null);
        if (transmission != null) {
            return String.valueOf(transmission).toUpperCase(Locale.ROOT);
        }
        final Object transmitting = componentValueOrDefault(component, entity, "transmitting", false);
        return Boolean.parseBoolean(String.valueOf(transmitting)) ? "TRANSMITTING" : "STOPPED";
    }

    static Double nullableNumber(final Object value, final String description) {
        return value == null ? null : finiteRequiredNumber(value, description);
    }

    static double optionalPositiveNumber(
            final Object value,
            final double defaultValue,
            final String description) {
        if (value == null) {
            return defaultValue;
        }
        final double number = finiteRequiredNumber(value, description);
        if (number <= 0) {
            throw new IllegalArgumentException(description + " must be positive");
        }
        return number;
    }

    static double finiteRequiredNumber(final Object value, final String description) {
        final double number;
        try {
            number = value instanceof Number numeric
                    ? numeric.doubleValue() : Double.parseDouble(String.valueOf(value));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(description + " must be numeric", e);
        }
        if (!Double.isFinite(number)) {
            throw new IllegalArgumentException(description + " must be finite");
        }
        return number;
    }

    static String requireEntityId(final Map<String, Object> entity, final String description) {
        if (entity != null) {
            Object id = entity.get("id");
            if (id == null) {
                id = SpecificationSupport.mapOrEmpty(entity.get("component")).get("id");
            }
            if (id != null && !String.valueOf(id).isBlank()) {
                return String.valueOf(id);
            }
        }
        throw new IllegalStateException(description + " response did not contain an ID");
    }

    static String entityId(final Map<String, Object> entity) {
        final Object entityId = entity.get("id");
        final Object componentId = SpecificationSupport.mapOrEmpty(entity.get("component")).get("id");
        final Object id = entityId == null ? componentId : entityId;
        return id == null || String.valueOf(id).isBlank() ? null : String.valueOf(id);
    }

    static Object componentValueOrDefault(
            final Map<String, Object> primary,
            final Map<String, Object> secondary,
            final String key,
            final Object defaultValue) {
        final Object primaryValue = primary.get(key);
        if (primaryValue != null) {
            return primaryValue;
        }
        final Object secondaryValue = secondary.get(key);
        return secondaryValue == null ? defaultValue : secondaryValue;
    }

    static Object valueOrDefault(
            final Map<String, Object> values,
            final String key,
            final Object defaultValue) {
        final Object value = values.get(key);
        return value == null ? defaultValue : value;
    }

    static Map<String, Object> componentMap(
            final Map<String, Object> entity,
            final Map<String, Object> component,
            final String key) {
        final Map<String, Object> topLevel = SpecificationSupport.mapOrNull(entity.get(key));
        return topLevel == null || topLevel.isEmpty()
                ? SpecificationSupport.mapOrEmpty(component.get(key)) : topLevel;
    }

    static double numericValue(final Object value, final double defaultValue) {
        final double parsed;
        if (value instanceof Number number) {
            parsed = number.doubleValue();
        } else if (value != null) {
            try {
                parsed = Double.parseDouble(String.valueOf(value));
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
        return Double.isFinite(parsed) ? parsed : defaultValue;
    }

    static Map<String, Object> restoreFields(
            final Map<String, Object> component,
            final String... fields) {
        final Map<String, Object> restore = new LinkedHashMap<>();
        for (String field : fields) {
            if (component.containsKey(field) && component.get(field) != null) {
                restore.put(field, component.get(field));
            }
        }
        if (restore.isEmpty()) {
            throw new IllegalStateException("Original component fields required for rollback are unavailable");
        }
        return restore;
    }

    static Map<String, Object> originalUpdatedFields(
            final Map<String, Object> component,
            final Map<String, Object> updates) {
        final Map<String, Object> original = new LinkedHashMap<>();
        for (String field : updates.keySet()) {
            original.put(field, component.get(field));
        }
        return original;
    }

    static Map<String, Object> effectiveFlow(final Map<String, Object> response) {
        final Map<String, Object> safe = SpecificationSupport.mapOrEmpty(response);
        final Map<String, Object> processGroupFlow =
                SpecificationSupport.mapOrEmpty(safe.get("processGroupFlow"));
        final Map<String, Object> nested = SpecificationSupport.mapOrEmpty(processGroupFlow.get("flow"));
        if (!nested.isEmpty()) {
            return nested;
        }
        final Map<String, Object> direct = SpecificationSupport.mapOrEmpty(safe.get("flow"));
        return direct.isEmpty() ? safe : direct;
    }
}
