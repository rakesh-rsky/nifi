package org.apache.nifi.copilot.capability;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public final class FlowSpecificationValidator {
    private static final Pattern PARAMETER = Pattern.compile(".*#\\{[^}]+}.*", Pattern.DOTALL);
    private final CapabilityRegistry registry;

    public FlowSpecificationValidator(final CapabilityRegistry registry) {
        this.registry = registry;
    }

    public ValidationReport validate(final Map<String, Object> specification) {
        return validateInternal(specification).report();
    }

    public ValidatedFlowPlan validateAndNormalize(final Map<String, Object> specification) {
        final ValidationResult result = validateInternal(specification);
        result.report().throwIfInvalid();
        return new ValidatedFlowPlan(result.specification());
    }

    private ValidationResult validateInternal(final Map<String, Object> specification) {
        final List<ValidationIssue> issues = new ArrayList<>();
        final Map<String, Object> normalized = mutableMap(specification);
        validateActions(normalized, issues);
        final Map<String, ServiceBinding> services = validateServices(
                maps(normalized.get("controller_services")), issues);
        final Map<String, ProcessorCapability> processors = new LinkedHashMap<>();
        for (Map<String, Object> processor : maps(normalized.get("processors"))) {
            final String id = text(processor.get("id"), "<unknown>");
            final String requestedType = text(processor.get("type"), "");
            final Optional<ProcessorCapability> resolved = registry.resolveProcessor(requestedType);
            if (resolved.isEmpty()) {
                issue(issues, id, "type", "Unknown or ambiguous processor type '" + requestedType + "'",
                        "Use an exact discovered processor FQN or an unambiguous simple name",
                        ValidationIssueType.UNKNOWN_PROCESSOR_TYPE, "", requestedType, null);
                continue;
            }
            final ProcessorCapability capability = resolved.get();
            processor.put("type", capability.type());
            processors.put(id, capability);
            final String suppliedPropertyField = processor.containsKey("properties") ? "properties" : "config";
            processor.put("config", validateProperties(
                    id, capability.type(), "config", map(processor.get(suppliedPropertyField)),
                    capability.properties(),
                    capability.supportsDynamicProperties(), services, issues));
            processor.remove("properties");
            validateScheduling(processor, capability, id, issues);
        }
        validateRelationships(maps(normalized.get("connections")), processors, issues);
        return new ValidationResult(new ValidationReport(issues), normalized);
    }

    private void validateActions(
            final Map<String, Object> specification, final List<ValidationIssue> issues) {
        final List<Map<String, Object>> deletions = maps(specification.get("deletions"));
        final Set<String> deletionTypes = Set.of(
                "processor", "process_group", "controller_service", "parameter_context");
        for (int index = 0; index < deletions.size(); index++) {
            final Map<String, Object> deletion = deletions.get(index);
            final String type = text(deletion.get("type"), "").toLowerCase(java.util.Locale.ROOT)
                    .replace(' ', '_');
            if (!deletionTypes.contains(type)) {
                issue(issues, "", "deletions[" + index + "].type",
                        "Unsupported deletion type '" + type + "'",
                        "Use processor, process_group, controller_service, or parameter_context",
                        ValidationIssueType.INVALID_DELETION);
            } else {
                deletion.put("type", type);
            }
            if (text(deletion.get("spec_id"), "").isBlank()
                    && text(deletion.get("name"), "").isBlank()) {
                issue(issues, "", "deletions[" + index + "]",
                        "Deletion target is missing", "Provide a spec_id or name",
                        ValidationIssueType.INVALID_DELETION);
            }
        }
        final List<Map<String, Object>> actions = maps(specification.get("cs_actions"));
        for (int index = 0; index < actions.size(); index++) {
            final Map<String, Object> action = actions.get(index);
            final String name = text(action.get("name"), "");
            final String operation = text(action.get("action"), "").toLowerCase(java.util.Locale.ROOT);
            if (name.isBlank()) {
                issue(issues, "", "cs_actions[" + index + "].name",
                        "Controller service name is missing", "Use an exact canvas service name",
                        ValidationIssueType.INVALID_CONTROLLER_SERVICE_ACTION);
            }
            if (!Set.of("enable", "disable").contains(operation)) {
                issue(issues, name, "cs_actions[" + index + "].action",
                        "Unsupported controller service action '" + operation + "'",
                        "Use enable or disable",
                        ValidationIssueType.INVALID_CONTROLLER_SERVICE_ACTION);
            } else {
                action.put("action", operation);
            }
        }
    }

    public void validateOrThrow(final Map<String, Object> specification) {
        validate(specification).throwIfInvalid();
    }

    private Map<String, ServiceBinding> validateServices(
            final List<Map<String, Object>> specs, final List<ValidationIssue> issues) {
        final Map<String, ServiceBinding> servicesById = new LinkedHashMap<>();
        final Map<String, List<ServiceBinding>> servicesByName = new LinkedHashMap<>();
        final List<ServiceSpecification> resolvedServices = new ArrayList<>();
        final Map<String, Integer> nameCounts = new LinkedHashMap<>();
        for (Map<String, Object> service : specs) {
            final String id = text(service.get("id"), "<unknown>");
            final String name = text(service.get("name"), id);
            nameCounts.merge(name, 1, Integer::sum);
        }
        for (Map<String, Object> service : specs) {
            final String id = text(service.get("id"), "<unknown>");
            final String name = text(service.get("name"), id);
            if (nameCounts.get(name) > 1) {
                issue(issues, id, "name", "Duplicate controller service name",
                        "Use a unique controller service name",
                        ValidationIssueType.DUPLICATE_CONTROLLER_SERVICE_NAME);
            }
        }
        for (Map<String, Object> service : specs) {
            final String id = text(service.get("id"), "<unknown>");
            final String name = text(service.get("name"), id);
            final String requestedType = text(service.get("type"), "");
            final Optional<ControllerServiceCapability> resolved = registry.resolveControllerService(requestedType);
            if (resolved.isEmpty()) {
                issue(issues, id, "type", "Unknown or ambiguous controller service type '" + requestedType + "'",
                        "Use an exact discovered controller service FQN or an unambiguous simple name",
                        ValidationIssueType.UNKNOWN_CONTROLLER_SERVICE_TYPE,
                        "",
                        requestedType,
                        null);
                continue;
            }
            final ControllerServiceCapability capability = resolved.get();
            service.put("type", capability.type());
            final ServiceBinding binding = new ServiceBinding(id, name, capability);
            servicesById.put(id, binding);
            servicesByName.computeIfAbsent(name, ignored -> new ArrayList<>()).add(binding);
            resolvedServices.add(new ServiceSpecification(service, binding));
        }
        final Map<String, ServiceBinding> services = new LinkedHashMap<>(servicesById);
        for (Map.Entry<String, List<ServiceBinding>> entry : servicesByName.entrySet()) {
            if (nameCounts.get(entry.getKey()) == 1) {
                services.putIfAbsent(entry.getKey(), entry.getValue().getFirst());
            }
        }
        for (ServiceSpecification resolved : resolvedServices) {
            final Map<String, Object> service = resolved.specification();
            final ServiceBinding binding = resolved.binding();
            service.put("properties", validateProperties(
                    binding.id(), binding.capability().type(), "properties",
                    map(service.get("properties")),
                    binding.capability().properties(), binding.capability().supportsDynamicProperties(),
                    services, issues));
        }
        return services;
    }

    private Map<String, Object> validateProperties(
            final String componentId,
            final String capabilityType,
            final String basePath,
            final Map<String, Object> supplied,
            final Map<String, PropertyCapability> descriptors,
            final boolean dynamic,
            final Map<String, ServiceBinding> services,
            final List<ValidationIssue> issues) {
        final Map<String, PropertyCapability> accepted = new LinkedHashMap<>();
        descriptors.values().forEach(descriptor -> {
            accepted.put(descriptor.name(), descriptor);
            accepted.putIfAbsent(descriptor.displayName(), descriptor);
        });
        final Set<String> present = new java.util.HashSet<>();
        final Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : supplied.entrySet()) {
            final PropertyCapability descriptor = accepted.get(entry.getKey());
            if (descriptor == null) {
                if (!dynamic) {
                    issue(issues, componentId, basePath + "." + entry.getKey(), "Unsupported property",
                            "Remove the property or use an exact discovered internal/display name",
                            ValidationIssueType.UNSUPPORTED_PROPERTY,
                            capabilityType,
                            entry.getKey(),
                            null);
                }
                if (dynamic) {
                    normalized.put(entry.getKey(), entry.getValue());
                }
                continue;
            }
            final Object rawValue = entry.getValue();
            final String path = basePath + "." + descriptor.name();
            Object normalizedValue = rawValue;
            if (rawValue == null || String.valueOf(rawValue).isBlank()) {
                normalized.put(descriptor.name(), normalizedValue);
                continue;
            }
            present.add(descriptor.name());
            final String value = String.valueOf(rawValue);
            if (!descriptor.allowableValues().isEmpty() && !PARAMETER.matcher(value).matches()) {
                final Optional<AllowableValue> allowable = descriptor.allowableValues().stream()
                        .filter(allowed -> value.equals(allowed.value()) || value.equals(allowed.displayName()))
                        .findFirst();
                if (allowable.isEmpty()) {
                    issue(issues, componentId, path, "Value is not one of the discovered allowable values",
                            "Use an allowable internal/display value or a #{parameter} reference",
                            ValidationIssueType.INVALID_ALLOWABLE_VALUE,
                            capabilityType,
                            value,
                            null);
                } else {
                    normalizedValue = allowable.get().value();
                }
            }
            if (descriptor.requiredServiceApi() != null) {
                ServiceBinding service = services.get(value);
                if (service == null) {
                    final ServiceBinding compatible =
                            uniqueCompatibleService(services, descriptor.requiredServiceApi());
                    if (compatible != null) {
                        service = compatible;
                    }
                } else if (!implementsApi(service.capability(), descriptor.requiredServiceApi())) {
                    final ServiceBinding compatible =
                            uniqueCompatibleService(services, descriptor.requiredServiceApi());
                    if (compatible != null) {
                        service = compatible;
                    }
                }
                if (service == null) {
                    issue(issues, componentId, path, "Controller service reference does not resolve",
                            "Reference a controller_services id or unique name",
                            ValidationIssueType.UNRESOLVED_CONTROLLER_SERVICE_REFERENCE,
                            capabilityType,
                            value,
                            descriptor.requiredServiceApi());
                } else if (!implementsApi(service.capability(), descriptor.requiredServiceApi())) {
                    issue(issues, componentId, path, "Controller service does not implement required API "
                            + descriptor.requiredServiceApi().type(),
                            "Reference a service implementing the discovered required API",
                            ValidationIssueType.INCOMPATIBLE_CONTROLLER_SERVICE,
                            capabilityType,
                            value,
                            descriptor.requiredServiceApi());
                } else {
                    normalizedValue = service.id();
                }
            }
            normalized.put(descriptor.name(), normalizedValue);
        }
        for (PropertyCapability descriptor : descriptors.values()) {
            if (descriptor.required() && descriptor.defaultValue() == null
                    && dependenciesActive(descriptor, normalized, descriptors)
                    && !present.contains(descriptor.name())) {
                issue(issues, componentId, basePath + "." + descriptor.name(), "Missing required property",
                        "Set '" + descriptor.displayName() + "' using its internal or display name",
                        ValidationIssueType.MISSING_REQUIRED_PROPERTY,
                        capabilityType,
                        "",
                        descriptor.requiredServiceApi());
            }
        }
        return normalized;
    }

    private boolean dependenciesActive(
            final PropertyCapability descriptor,
            final Map<String, Object> normalized,
            final Map<String, PropertyCapability> descriptors) {
        for (PropertyDependency dependency : descriptor.dependencies()) {
            final PropertyCapability dependencyDescriptor = descriptors.get(dependency.propertyName());
            final Object configured = normalized.get(dependency.propertyName());
            final String effective = configured == null && dependencyDescriptor != null
                    ? dependencyDescriptor.defaultValue() : text(configured, "");
            if (dependency.dependentValues().isEmpty()) {
                if (effective == null || effective.isBlank()) {
                    return false;
                }
            } else if (effective == null || !dependency.dependentValues().contains(effective)) {
                return false;
            }
        }
        return true;
    }

    private void validateScheduling(
            final Map<String, Object> processor,
            final ProcessorCapability capability,
            final String id,
            final List<ValidationIssue> issues) {
        final Map<String, Object> scheduling = map(processor.get("scheduling"));
        final Object strategyValue = first(processor, scheduling, "scheduling_strategy", "schedulingStrategy");
        final Map<String, Object> normalized = new LinkedHashMap<>();
        if (strategyValue != null) {
            final String strategy = String.valueOf(strategyValue);
            normalized.put("schedulingStrategy", strategy);
            if (!capability.supportedSchedulingStrategies().isEmpty()
                    && !capability.supportedSchedulingStrategies().contains(strategy)) {
                issue(issues, id, "scheduling.strategy", "Unsupported scheduling strategy '" + strategy + "'",
                        "Use one of " + capability.supportedSchedulingStrategies().stream().sorted().toList(),
                        ValidationIssueType.UNSUPPORTED_SCHEDULING_STRATEGY,
                        capability.type(),
                        strategy,
                        null);
            }
        }
        final Object tasksValue = first(processor, scheduling, "concurrent_tasks", "concurrentTasks");
        if (tasksValue != null) {
            final Integer tasks = integer(tasksValue);
            if (tasks == null || tasks <= 0) {
                issue(issues, id, "scheduling.concurrentTasks", "Concurrent tasks must be a positive integer",
                        "Set concurrent tasks to 1 or greater",
                        ValidationIssueType.INVALID_CONCURRENT_TASKS,
                        capability.type(),
                        String.valueOf(tasksValue),
                        null);
            } else if (capability.triggerSerially() && tasks > 1) {
                issue(issues, id, "scheduling.concurrentTasks", "Serial processor cannot use more than one concurrent task",
                        "Set concurrent tasks to 1",
                        ValidationIssueType.INVALID_CONCURRENT_TASKS,
                        capability.type(),
                        String.valueOf(tasksValue),
                        null);
            } else {
                normalized.put("concurrentlySchedulableTaskCount", tasks);
            }
        }
        copySchedulingValue(processor, scheduling, normalized, "scheduling_period", "schedulingPeriod");
        copySchedulingValue(processor, scheduling, normalized, "execution_node", "executionNode");
        copySchedulingValue(processor, scheduling, normalized, "penalty_duration", "penaltyDuration");
        copySchedulingValue(processor, scheduling, normalized, "yield_duration", "yieldDuration");
        copySchedulingValue(processor, scheduling, normalized, "bulletin_level", "bulletinLevel");
        copySchedulingValue(processor, scheduling, normalized, "run_duration_millis", "runDurationMillis");
        if (!normalized.isEmpty()) {
            processor.put("scheduling", normalized);
        } else {
            processor.remove("scheduling");
        }
        processor.keySet().removeAll(Set.of(
                "scheduling_strategy", "schedulingStrategy", "concurrent_tasks", "concurrentTasks",
                "scheduling_period", "schedulingPeriod", "execution_node", "executionNode",
                "penalty_duration", "penaltyDuration", "yield_duration", "yieldDuration",
                "bulletin_level", "bulletinLevel", "run_duration_millis", "runDurationMillis"));
    }

    private void copySchedulingValue(
            final Map<String, Object> processor,
            final Map<String, Object> scheduling,
            final Map<String, Object> normalized,
            final String snake,
            final String camel) {
        final Object value = first(processor, scheduling, snake, camel);
        if (value != null) {
            normalized.put(camel, value);
        }
    }

    private void validateRelationships(
            final List<Map<String, Object>> connections,
            final Map<String, ProcessorCapability> processors,
            final List<ValidationIssue> issues) {
        for (int index = 0; index < connections.size(); index++) {
            final Map<String, Object> connection = connections.get(index);
            final String source = text(connection.get("from"), "");
            final ProcessorCapability capability = processors.get(source);
            final Object rawRelationships = connection.get("relationships");
            if (connection.containsKey("relationships") && !(rawRelationships instanceof List<?>)) {
                issue(issues, source, "connections[" + index + "].relationships",
                        "Relationships must be a list", "Use a list of discovered relationship names",
                        ValidationIssueType.INVALID_RELATIONSHIPS,
                        capability == null ? "" : capability.type(),
                        String.valueOf(rawRelationships),
                        null);
                continue;
            }
            if (capability == null) {
                continue;
            }
            final List<?> supplied = rawRelationships instanceof List<?> list ? list : List.of();
            final List<?> relationships = supplied.isEmpty() ? List.of("success") : supplied;
            connection.put("relationships", new ArrayList<>(relationships));
            final List<String> normalizedRelationships = new ArrayList<>();
            for (Object raw : relationships) {
                final String relationship = String.valueOf(raw);
                final String resolved = resolveRelationship(capability, relationship);
                normalizedRelationships.add(resolved == null ? relationship : resolved);
                if (resolved == null) {
                    issue(issues, source, "connections[" + index + "].relationships",
                            "Unknown source relationship '" + relationship + "'",
                            "Use a discovered relationship: " + capability.relationships().stream().sorted().toList(),
                            ValidationIssueType.UNSUPPORTED_RELATIONSHIP,
                            capability.type(),
                            relationship,
                            null);
                }
            }
            connection.put("relationships", normalizedRelationships);
        }
    }

    private String resolveRelationship(final ProcessorCapability capability, final String requested) {
        if (capability.supportsDynamicRelationships() || capability.relationships().contains(requested)) {
            return requested;
        }
        final List<String> matches = capability.relationships().stream()
                .filter(relationship -> relationship.equalsIgnoreCase(requested))
                .toList();
        return matches.size() == 1 ? matches.getFirst() : null;
    }

    private boolean implementsApi(
            final ControllerServiceCapability service, final ServiceApi required) {
        return service.serviceApis().stream().anyMatch(provided ->
                provided.type().equals(required.type())
                        && (required.bundle() == null || provided.bundle() == null
                        || required.bundle().equals(provided.bundle())));
    }

    private ServiceBinding uniqueCompatibleService(
            final Map<String, ServiceBinding> services,
            final ServiceApi required) {
        final List<ServiceBinding> compatible = services.values().stream()
                .filter(service -> implementsApi(service.capability(), required))
                .collect(java.util.stream.Collectors.toMap(
                        ServiceBinding::id,
                        service -> service,
                        (first, ignored) -> first,
                        LinkedHashMap::new))
                .values().stream()
                .toList();
        return compatible.size() == 1 ? compatible.getFirst() : null;
    }

    private Object first(
            final Map<String, Object> processor,
            final Map<String, Object> scheduling,
            final String snake,
            final String camel) {
        if (scheduling.containsKey(snake)) {
            return scheduling.get(snake);
        }
        if (scheduling.containsKey(camel)) {
            return scheduling.get(camel);
        }
        if (processor.containsKey(snake)) {
            return processor.get(snake);
        }
        return processor.get(camel);
    }

    private Integer integer(final Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.valueOf(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void issue(
            final List<ValidationIssue> issues,
            final String component,
            final String path,
            final String reason,
            final String fix) {
        issue(issues, component, path, reason, fix, ValidationIssueType.OTHER);
    }

    private void issue(
            final List<ValidationIssue> issues,
            final String component,
            final String path,
            final String reason,
            final String fix,
            final ValidationIssueType issueType) {
        issue(issues, component, path, reason, fix, issueType, "", "", null);
    }

    private void issue(
            final List<ValidationIssue> issues,
            final String component,
            final String path,
            final String reason,
            final String fix,
            final ValidationIssueType issueType,
            final String capabilityType,
            final String rejectedValue,
            final ServiceApi requiredApi) {
        issues.add(new ValidationIssue(
                component,
                path,
                reason,
                fix,
                issueType,
                capabilityType,
                rejectedValue,
                requiredApi));
    }

    private List<Map<String, Object>> maps(final Object raw) {
        if (!(raw instanceof List<?> list)) {
            return List.of();
        }
        final List<Map<String, Object>> result = new ArrayList<>();
        for (Object value : list) {
            result.add(map(value));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> map(final Object raw) {
        return raw instanceof Map<?, ?> value ? (Map<String, Object>) value : Map.of();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mutableMap(final Object raw) {
        if (!(raw instanceof Map<?, ?> source)) {
            return new LinkedHashMap<>();
        }
        final Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(String.valueOf(key), mutableCopy(value)));
        return copy;
    }

    private Object mutableCopy(final Object value) {
        if (value instanceof Map<?, ?>) {
            return mutableMap(value);
        }
        if (value instanceof List<?> list) {
            final List<Object> copy = new ArrayList<>(list.size());
            list.forEach(element -> copy.add(mutableCopy(element)));
            return copy;
        }
        return value;
    }

    private String text(final Object value, final String fallback) {
        return value == null || String.valueOf(value).isBlank() ? fallback : String.valueOf(value);
    }

    private record ServiceBinding(String id, String name, ControllerServiceCapability capability) {
    }

    private record ServiceSpecification(Map<String, Object> specification, ServiceBinding binding) {
    }

    private record ValidationResult(ValidationReport report, Map<String, Object> specification) {
    }
}
