package org.apache.nifi.copilot.capability;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Immutable, deterministic indexes over discovered NiFi capability metadata.
 */
public final class CapabilityGraph {
    private static final Comparator<ServiceApi> API_ORDER = Comparator
            .comparing(ServiceApi::type)
            .thenComparing(api -> api.bundle() == null ? "" : api.bundle().group())
            .thenComparing(api -> api.bundle() == null ? "" : api.bundle().artifact())
            .thenComparing(api -> api.bundle() == null ? "" : api.bundle().version());

    private final Map<String, ProcessorNode> processorsByType;
    private final Map<String, ControllerServiceNode> servicesByType;
    private final Map<String, ProcessorNode> processorsByUniqueSimpleName;
    private final Map<String, ControllerServiceNode> servicesByUniqueSimpleName;
    private final Map<String, List<ControllerServiceNode>> serviceImplementationsByApi;

    public CapabilityGraph(
            final Collection<ProcessorNode> processors,
            final Collection<ControllerServiceNode> controllerServices) {
        processorsByType = indexByType(processors, ProcessorNode::type, "processor");
        servicesByType = indexByType(controllerServices, ControllerServiceNode::type, "controller service");
        processorsByUniqueSimpleName = indexUniqueSimpleNames(processorsByType);
        servicesByUniqueSimpleName = indexUniqueSimpleNames(servicesByType);
        serviceImplementationsByApi = indexServiceImplementations(servicesByType.values());
    }

    public Map<String, ProcessorNode> processorsByType() {
        return processorsByType;
    }

    public Map<String, ControllerServiceNode> servicesByType() {
        return servicesByType;
    }

    public Map<String, ProcessorNode> processorsByUniqueSimpleName() {
        return processorsByUniqueSimpleName;
    }

    public Map<String, ControllerServiceNode> servicesByUniqueSimpleName() {
        return servicesByUniqueSimpleName;
    }

    public Map<String, List<ControllerServiceNode>> serviceImplementationsByApi() {
        return serviceImplementationsByApi;
    }

    public Optional<ProcessorNode> resolveProcessor(final String typeOrSimpleName) {
        return resolve(typeOrSimpleName, processorsByType, processorsByUniqueSimpleName);
    }

    public Optional<ControllerServiceNode> resolveControllerService(final String typeOrSimpleName) {
        return resolve(typeOrSimpleName, servicesByType, servicesByUniqueSimpleName);
    }

    public List<ControllerServiceNode> serviceImplementations(final ServiceApi requiredApi) {
        if (requiredApi == null) {
            return List.of();
        }
        return serviceImplementationsByApi.getOrDefault(requiredApi.type(), List.of()).stream()
                .filter(service -> service.implementedApis().stream()
                        .anyMatch(provided -> compatible(requiredApi, provided)))
                .toList();
    }

    private static boolean compatible(final ServiceApi required, final ServiceApi provided) {
        return required.type().equals(provided.type())
                && (required.bundle() == null || provided.bundle() == null
                || required.bundle().equals(provided.bundle()));
    }

    private static <T> Optional<T> resolve(
            final String requested,
            final Map<String, T> byType,
            final Map<String, T> byUniqueSimpleName) {
        if (requested == null || requested.isBlank()) {
            return Optional.empty();
        }
        final T exact = byType.get(requested);
        if (exact != null) {
            return Optional.of(exact);
        }
        return Optional.ofNullable(byUniqueSimpleName.get(
                simpleName(requested).toLowerCase(Locale.ROOT)));
    }

    private static <T> Map<String, T> indexByType(
            final Collection<T> capabilities,
            final Function<T, String> type,
            final String capabilityKind) {
        if (capabilities == null || capabilities.isEmpty()) {
            return Map.of();
        }
        final List<T> ordered = capabilities.stream()
                .sorted(Comparator.comparing(type))
                .toList();
        final Map<String, T> indexed = new LinkedHashMap<>();
        for (T capability : ordered) {
            final String capabilityType = type.apply(capability);
            if (indexed.putIfAbsent(capabilityType, capability) != null) {
                throw new IllegalArgumentException(
                        "Duplicate " + capabilityKind + " type: " + capabilityType);
            }
        }
        return Collections.unmodifiableMap(indexed);
    }

    private static <T> Map<String, T> indexUniqueSimpleNames(final Map<String, T> byType) {
        final Map<String, List<T>> candidates = new LinkedHashMap<>();
        byType.forEach((type, capability) -> candidates
                .computeIfAbsent(simpleName(type).toLowerCase(Locale.ROOT), ignored -> new ArrayList<>())
                .add(capability));
        final Map<String, T> unique = new LinkedHashMap<>();
        candidates.forEach((simpleName, matches) -> {
            if (matches.size() == 1) {
                unique.put(simpleName, matches.getFirst());
            }
        });
        return Collections.unmodifiableMap(unique);
    }

    private static Map<String, List<ControllerServiceNode>> indexServiceImplementations(
            final Collection<ControllerServiceNode> services) {
        final Map<String, List<ControllerServiceNode>> implementations = new LinkedHashMap<>();
        services.forEach(service -> service.implementedApis().forEach(api -> implementations
                .computeIfAbsent(api.type(), ignored -> new ArrayList<>())
                .add(service)));
        final Map<String, List<ControllerServiceNode>> immutable = new LinkedHashMap<>();
        implementations.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> immutable.put(
                        entry.getKey(),
                        entry.getValue().stream()
                                .distinct()
                                .sorted(Comparator.comparing(ControllerServiceNode::type))
                                .toList()));
        return Collections.unmodifiableMap(immutable);
    }

    private static String simpleName(final String type) {
        return type.substring(type.lastIndexOf('.') + 1);
    }

    private static <T> Map<String, T> immutableSortedMap(final Map<String, T> values) {
        if (values == null || values.isEmpty()) {
            return Map.of();
        }
        final Map<String, T> sorted = new LinkedHashMap<>();
        values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(sorted);
    }

    private static <T> Set<T> immutableSortedSet(
            final Collection<T> values,
            final Comparator<? super T> comparator) {
        if (values == null || values.isEmpty()) {
            return Set.of();
        }
        final Set<T> sorted = new LinkedHashSet<>();
        values.stream().sorted(comparator).forEach(sorted::add);
        return Collections.unmodifiableSet(sorted);
    }


    /**
     * Creates a {@code CapabilityGraph} from a discovered capability snapshot.
     */
    public static CapabilityGraph from(final CapabilitySnapshot snapshot) {
        Objects.requireNonNull(snapshot, "Capability snapshot is required");
        return new CapabilityGraph(
                snapshot.processors().values().stream().map(CapabilityGraph::toProcessorNode).toList(),
                snapshot.controllerServices().values().stream().map(CapabilityGraph::toControllerServiceNode).toList());
    }

    private static ProcessorNode toProcessorNode(final ProcessorCapability capability) {
        final Map<String, PropertyNode> properties = toProperties(capability.properties());
        return new ProcessorNode(
                capability.type(),
                capability.bundle(),
                properties,
                capability.supportsDynamicProperties(),
                extractServiceApis(properties, true),
                extractServiceApis(properties, false),
                capability.relationships(),
                capability.supportsDynamicRelationships(),
                new SchedulingConstraints(
                        capability.inputRequirement(),
                        capability.supportedSchedulingStrategies(),
                        capability.triggerSerially()));
    }

    private static ControllerServiceNode toControllerServiceNode(final ControllerServiceCapability capability) {
        final Map<String, PropertyNode> properties = toProperties(capability.properties());
        return new ControllerServiceNode(
                capability.type(),
                capability.bundle(),
                properties,
                capability.supportsDynamicProperties(),
                capability.serviceApis(),
                extractServiceApis(properties, true),
                extractServiceApis(properties, false));
    }

    private static Map<String, PropertyNode> toProperties(final Map<String, PropertyCapability> capabilities) {
        final Map<String, PropertyNode> properties = new LinkedHashMap<>();
        capabilities.values().stream()
                .sorted(Comparator.comparing(PropertyCapability::name))
                .map(CapabilityGraph::toPropertyNode)
                .forEach(property -> properties.put(property.name(), property));
        return properties;
    }

    private static PropertyNode toPropertyNode(final PropertyCapability capability) {
        return new PropertyNode(
                capability.name(),
                capability.displayName(),
                capability.required(),
                capability.defaultValue(),
                capability.dynamic(),
                capability.sensitive(),
                capability.allowableValues(),
                capability.dependencies(),
                capability.requiredServiceApi());
    }

    private static Set<ServiceApi> extractServiceApis(
            final Map<String, PropertyNode> properties,
            final boolean required) {
        final Set<ServiceApi> apis = new LinkedHashSet<>();
        properties.values().stream()
                .filter(property -> property.required() == required)
                .map(PropertyNode::requiredControllerServiceApi)
                .filter(Objects::nonNull)
                .forEach(apis::add);
        return apis;
    }
    public record ProcessorNode(
            String type,
            BundleCoordinate bundle,
            Map<String, PropertyNode> properties,
            boolean supportsDynamicProperties,
            Set<ServiceApi> requiredControllerServiceApis,
            Set<ServiceApi> optionalControllerServiceApis,
            Set<String> relationships,
            boolean supportsDynamicRelationships,
            SchedulingConstraints schedulingConstraints) {
        public ProcessorNode {
            requireTypeAndBundle(type, bundle, "Processor");
            properties = immutableSortedMap(properties);
            requiredControllerServiceApis =
                    immutableSortedSet(requiredControllerServiceApis, API_ORDER);
            optionalControllerServiceApis =
                    immutableSortedSet(optionalControllerServiceApis, API_ORDER);
            relationships = immutableSortedSet(relationships, Comparator.naturalOrder());
            schedulingConstraints = schedulingConstraints == null
                    ? new SchedulingConstraints(null, Set.of(), false)
                    : schedulingConstraints;
        }
    }

    public record ControllerServiceNode(
            String type,
            BundleCoordinate bundle,
            Map<String, PropertyNode> properties,
            boolean supportsDynamicProperties,
            Set<ServiceApi> implementedApis,
            Set<ServiceApi> requiredDependentApis,
            Set<ServiceApi> optionalDependentApis) {
        public ControllerServiceNode {
            requireTypeAndBundle(type, bundle, "Controller service");
            properties = immutableSortedMap(properties);
            implementedApis = immutableSortedSet(implementedApis, API_ORDER);
            requiredDependentApis = immutableSortedSet(requiredDependentApis, API_ORDER);
            optionalDependentApis = immutableSortedSet(optionalDependentApis, API_ORDER);
        }
    }

    public record PropertyNode(
            String name,
            String displayName,
            boolean required,
            String defaultValue,
            boolean dynamic,
            boolean sensitive,
            List<AllowableValue> allowableValues,
            List<PropertyDependency> dependencies,
            ServiceApi requiredControllerServiceApi) {
        public PropertyNode {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Property internal name is required");
            }
            displayName = displayName == null || displayName.isBlank() ? name : displayName;
            allowableValues = allowableValues == null ? List.of() : List.copyOf(allowableValues);
            dependencies = dependencies == null ? List.of() : List.copyOf(dependencies);
        }
    }

    public record SchedulingConstraints(
            String inputRequirement,
            Set<String> supportedStrategies,
            boolean triggerSerially) {
        public SchedulingConstraints {
            supportedStrategies = immutableSortedSet(supportedStrategies, Comparator.naturalOrder());
        }
    }

    private static void requireTypeAndBundle(
            final String type,
            final BundleCoordinate bundle,
            final String capabilityKind) {
        if (type == null || type.isBlank() || bundle == null) {
            throw new IllegalArgumentException(capabilityKind + " type and bundle are required");
        }
    }
}
