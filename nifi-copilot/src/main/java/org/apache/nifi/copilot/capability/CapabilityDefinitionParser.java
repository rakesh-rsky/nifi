package org.apache.nifi.copilot.capability;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class CapabilityDefinitionParser {
    public ProcessorCapability parseProcessor(
            final Map<String, Object> documentedType, final Map<String, Object> definition) {
        final String type = requiredString(documentedType, "type", "processor type");
        final BundleCoordinate bundle = bundle(documentedType.get("bundle"), "processor " + type);
        verifyDefinitionType(definition, type);
        return new ProcessorCapability(
                type,
                bundle,
                properties(definition),
                bool(definition, "supportsDynamicProperties"),
                names(definition.get("supportedRelationships"), "name", "supportedRelationships"),
                bool(definition, "supportsDynamicRelationships"),
                optionalString(definition.get("inputRequirement")),
                strings(definition.get("supportedSchedulingStrategies"), "supportedSchedulingStrategies"),
                bool(definition, "triggerSerially"));
    }

    public ControllerServiceCapability parseControllerService(
            final Map<String, Object> documentedType, final Map<String, Object> definition) {
        final String type = requiredString(documentedType, "type", "controller service type");
        final BundleCoordinate bundle = bundle(documentedType.get("bundle"), "controller service " + type);
        verifyDefinitionType(definition, type);
        final Set<ServiceApi> apis = new LinkedHashSet<>();
        apis.addAll(serviceApis(documentedType.get("controllerServiceApis"), "controllerServiceApis"));
        apis.addAll(serviceApis(definition.get("providedApiImplementations"), "providedApiImplementations"));
        return new ControllerServiceCapability(
                type, bundle, properties(definition), bool(definition, "supportsDynamicProperties"), apis);
    }

    private Map<String, PropertyCapability> properties(final Map<String, Object> definition) {
        final Object raw = definition.get("propertyDescriptors");
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Map<?, ?> descriptors)) {
            throw invalid("definition propertyDescriptors must be an object");
        }
        final Map<String, PropertyCapability> parsed = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : descriptors.entrySet()) {
            if (!(entry.getKey() instanceof String mapName) || !(entry.getValue() instanceof Map<?, ?> rawDescriptor)) {
                throw invalid("propertyDescriptors contains an invalid entry");
            }
            final Map<String, Object> descriptor = stringMap(rawDescriptor, "property descriptor " + mapName);
            final String name = optionalString(descriptor.get("name")) == null
                    ? mapName : optionalString(descriptor.get("name"));
            if (!mapName.equals(name)) {
                throw invalid("property descriptor key '" + mapName + "' does not match internal name '" + name + "'");
            }
            final List<AllowableValue> allowableValues = new ArrayList<>();
            final Object rawValues = descriptor.get("allowableValues");
            if (rawValues != null) {
                if (!(rawValues instanceof List<?> values)) {
                    throw invalid("allowableValues for property '" + name + "' must be an array");
                }
                for (Object rawValue : values) {
                    if (!(rawValue instanceof Map<?, ?> valueMap)) {
                        throw invalid("allowableValues for property '" + name + "' contains an invalid entry");
                    }
                    final Map<String, Object> value = stringMap(valueMap, "allowable value");
                    allowableValues.add(new AllowableValue(
                            requiredString(value, "value", "allowable value"),
                            optionalString(value.get("displayName"))));
                }
            }
            final Object requiredApi = descriptor.get("typeProvidedByValue");
            parsed.put(name, new PropertyCapability(
                    name,
                    optionalString(descriptor.get("displayName")),
                    bool(descriptor, "required"),
                    optionalString(descriptor.get("defaultValue")),
                    bool(descriptor, "dynamic"),
                    bool(descriptor, "sensitive"),
                    allowableValues,
                    dependencies(descriptor.get("dependencies")),
                    requiredApi == null ? null : serviceApi(requiredApi, "typeProvidedByValue")));
        }
        return parsed;
    }

    private List<PropertyDependency> dependencies(final Object raw) {
        if (raw == null) {
            return List.of();
        }
        if (!(raw instanceof List<?> values)) {
            throw invalid("dependencies must be an array");
        }
        final List<PropertyDependency> dependencies = new ArrayList<>();
        for (Object value : values) {
            if (!(value instanceof Map<?, ?> rawDependency)) {
                throw invalid("dependencies contains an invalid entry");
            }
            final Map<String, Object> dependency = stringMap(rawDependency, "property dependency");
            dependencies.add(new PropertyDependency(
                    requiredString(dependency, "propertyName", "property dependency"),
                    optionalString(dependency.get("propertyDisplayName")),
                    strings(dependency.get("dependentValues"), "dependentValues")));
        }
        return dependencies;
    }

    private Set<ServiceApi> serviceApis(final Object raw, final String field) {
        if (raw == null) {
            return Set.of();
        }
        if (!(raw instanceof List<?> list)) {
            throw invalid(field + " must be an array");
        }
        final Set<ServiceApi> result = new LinkedHashSet<>();
        for (Object value : list) {
            result.add(serviceApi(value, field));
        }
        return result;
    }

    private ServiceApi serviceApi(final Object raw, final String field) {
        if (!(raw instanceof Map<?, ?> map)) {
            throw invalid(field + " contains an invalid service API");
        }
        final Map<String, Object> api = stringMap(map, field);
        final String type = requiredString(api, "type", field);
        final Map<String, Object> coordinates;
        if (api.get("bundle") instanceof Map<?, ?> nestedBundle) {
            coordinates = stringMap(nestedBundle, field + " bundle");
        } else {
            coordinates = api;
        }
        final String group = optionalString(coordinates.get("group"));
        final String artifact = optionalString(coordinates.get("artifact"));
        final String version = optionalString(coordinates.get("version"));
        final BundleCoordinate coordinate = group == null && artifact == null && version == null
                ? null : new BundleCoordinate(group, artifact, version);
        return new ServiceApi(type, coordinate);
    }

    private BundleCoordinate bundle(final Object raw, final String context) {
        if (!(raw instanceof Map<?, ?> map)) {
            throw invalid(context + " bundle must be an object");
        }
        final Map<String, Object> value = stringMap(map, context + " bundle");
        return new BundleCoordinate(
                requiredString(value, "group", context + " bundle"),
                requiredString(value, "artifact", context + " bundle"),
                requiredString(value, "version", context + " bundle"));
    }

    private void verifyDefinitionType(final Map<String, Object> definition, final String expected) {
        if (definition == null || definition.isEmpty()) {
            throw invalid("definition for '" + expected + "' is empty");
        }
        final String actual = optionalString(definition.get("type"));
        if (actual != null && !expected.equals(actual)) {
            throw invalid("definition type '" + actual + "' does not match listed type '" + expected + "'");
        }
    }

    private Set<String> names(final Object raw, final String key, final String field) {
        if (raw == null) {
            return Set.of();
        }
        if (!(raw instanceof List<?> list)) {
            throw invalid(field + " must be an array");
        }
        final Set<String> result = new LinkedHashSet<>();
        for (Object value : list) {
            if (!(value instanceof Map<?, ?> map)) {
                throw invalid(field + " contains an invalid entry");
            }
            result.add(requiredString(stringMap(map, field), key, field));
        }
        return result;
    }

    private Set<String> strings(final Object raw, final String field) {
        if (raw == null) {
            return Set.of();
        }
        if (!(raw instanceof List<?> list)) {
            throw invalid(field + " must be an array");
        }
        final Set<String> result = new LinkedHashSet<>();
        for (Object value : list) {
            final String string = optionalString(value);
            if (string == null) {
                throw invalid(field + " contains an invalid value");
            }
            result.add(string);
        }
        return result;
    }

    private boolean bool(final Map<String, Object> map, final String key) {
        final Object value = map.get(key);
        if (value == null) {
            return false;
        }
        if (!(value instanceof Boolean bool)) {
            throw invalid(key + " must be a boolean");
        }
        return bool;
    }

    private String requiredString(final Map<String, Object> map, final String key, final String context) {
        final String value = optionalString(map.get(key));
        if (value == null) {
            throw invalid(context + " requires non-blank '" + key + "'");
        }
        return value;
    }

    private String optionalString(final Object value) {
        return value instanceof String string && !string.isBlank() ? string : null;
    }

    private Map<String, Object> stringMap(final Map<?, ?> raw, final String context) {
        final Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw invalid(context + " contains a non-string field name");
            }
            result.put(key, entry.getValue());
        }
        return result;
    }

    private IllegalArgumentException invalid(final String message) {
        return new IllegalArgumentException("Invalid NiFi capability response: " + message);
    }
}
