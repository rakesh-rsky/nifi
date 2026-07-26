package org.apache.nifi.copilot.capability;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.ApiSelection;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.CapabilityClosure;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.ControllerServiceSelection;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.ProcessorSelection;
import org.apache.nifi.copilot.capability.ProcessorSeedRanker.RankedProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public final class RepairContextExpander {
    private static final int MAX_REPAIR_CONTEXT_CHARS = 16_000;
    private static final int MAX_ADDITIONAL_PROCESSORS = 8;
    private static final int MAX_ADDITIONAL_SERVICES = 8;
    private static final int MAX_IMPLEMENTATIONS_PER_API = 1;
    private static final int MAX_DEPENDENCY_DEPTH = 4;
    private static final int MAX_REPAIR_HINTS = 24;
    private static final int MAX_HINT_VALUE_CHARS = 160;
    private static final String HINT_HEADER = "\n[STRUCTURED REPAIR HINTS]\n";

    private final CapabilityPromptRenderer renderer;
    private final PropertyRanker propertyRanker;
    private final CapabilityMetricsRegistry metrics;

    public RepairContextExpander() {
        this(
                new CapabilityPromptRenderer(),
                new PropertyRanker(),
                new CapabilityMetricsRegistry());
    }

    @Autowired
    public RepairContextExpander(
            final CapabilityPromptRenderer renderer,
            final PropertyRanker propertyRanker,
            final CapabilityMetricsRegistry metrics) {
        if (renderer == null || propertyRanker == null || metrics == null) {
            throw new IllegalArgumentException("Repair context collaborators are required");
        }
        this.renderer = renderer;
        this.propertyRanker = propertyRanker;
        this.metrics = metrics;
    }

    public String expand(
            final String userIntent,
            final CapabilityGraph graph,
            final List<RepairHint> hints,
            final Map<String, Object> rejectedSpecification) {
        return expand(
                userIntent,
                graph,
                hints,
                rejectedSpecification,
                MAX_REPAIR_CONTEXT_CHARS);
    }

    String expand(
            final String userIntent,
            final CapabilityGraph graph,
            final List<RepairHint> hints,
            final Map<String, Object> rejectedSpecification,
            final int maxContextChars) {
        if (graph == null || hints == null || rejectedSpecification == null) {
            throw new IllegalArgumentException(
                    "Capability graph, repair hints, and rejected specification are required");
        }

        final List<RepairHint> boundedHints = hints.stream()
                .limit(MAX_REPAIR_HINTS)
                .toList();
        final String hintContext = renderHints(boundedHints, hints.size() - boundedHints.size());
        final int capabilityBudget = maxContextChars - hintContext.length();
        if (capabilityBudget <= 0) {
            throw new IllegalStateException(
                    "Structured repair hints exceed the capability context character limit");
        }
        final int expansionReserve = Math.min(12_000, capabilityBudget / 3);
        final CapabilityClosure original = renderer.select(
                userIntent, graph, capabilityBudget - expansionReserve);
        final ExpandedClosure expanded = expandClosure(
                original, graph, boundedHints, rejectedSpecification);
        final String rendered =
                renderer.render(expanded.closure(), capabilityBudget) + hintContext;
        metrics.observeRepairExpansion(original, expanded.closure(), rendered.length());
        return rendered;
    }

    private ExpandedClosure expandClosure(
            final CapabilityClosure original,
            final CapabilityGraph graph,
            final List<RepairHint> hints,
            final Map<String, Object> rejectedSpecification) {
        final Map<String, ProcessorSelection> processors = new LinkedHashMap<>();
        original.processors().forEach(selection ->
                processors.put(selection.processor().type(), selection));
        final Map<String, ControllerServiceSelection> services = new LinkedHashMap<>();
        original.controllerServices().forEach(selection ->
                services.put(selection.service().type(), selection));
        final Map<String, MutableApiSelection> apis = new LinkedHashMap<>();
        original.requiredApiSelections().forEach(selection ->
                apis.put(selection.api().type(), new MutableApiSelection(selection)));

        rejectedTypes(rejectedSpecification, "processors").stream()
                .map(graph::resolveProcessor)
                .flatMap(java.util.Optional::stream)
                .sorted(Comparator.comparing(ProcessorNode::type))
                .filter(processor -> !processors.containsKey(processor.type()))
                .limit(MAX_ADDITIONAL_PROCESSORS)
                .forEach(processor -> processors.put(
                        processor.type(), processorSelection(processor)));

        rejectedTypes(rejectedSpecification, "controller_services").stream()
                .map(graph::resolveControllerService)
                .flatMap(java.util.Optional::stream)
                .sorted(Comparator.comparing(ControllerServiceNode::type))
                .filter(service -> !services.containsKey(service.type()))
                .limit(MAX_ADDITIONAL_SERVICES)
                .forEach(service -> services.put(
                        service.type(), serviceSelection(service, 1, Set.of())));

        final Map<String, LinkedHashSet<String>> preferredTypesByApi = new LinkedHashMap<>();
        final Deque<ApiRequest> pending = new ArrayDeque<>();
        hints.stream()
                .filter(hint -> hint.requiredApi() != null)
                .forEach(hint -> {
                    pending.addLast(new ApiRequest(hint.requiredApi(), 1));
                    preferredTypesByApi
                            .computeIfAbsent(
                                    hint.requiredApi().type(),
                                    ignored -> new LinkedHashSet<>())
                            .addAll(hint.compatibleImplementationTypes());
                });
        processors.values().stream()
                .filter(selection -> !originalProcessor(original, selection.processor().type()))
                .flatMap(selection -> selection.requiredApis().stream())
                .forEach(api -> pending.addLast(new ApiRequest(api, 1)));
        services.values().stream()
                .filter(selection -> !originalService(original, selection.service().type()))
                .flatMap(selection -> selection.service().requiredDependentApis().stream())
                .forEach(api -> pending.addLast(new ApiRequest(api, 1)));

        final Set<String> processedApiTypes = new LinkedHashSet<>();
        while (!pending.isEmpty()) {
            final ApiRequest request = pending.removeFirst();
            if (request.depth() > MAX_DEPENDENCY_DEPTH
                    || !processedApiTypes.add(request.api().type())) {
                continue;
            }
            final MutableApiSelection apiSelection = apis.computeIfAbsent(
                    request.api().type(),
                    ignored -> new MutableApiSelection(request.api()));
            final List<ControllerServiceNode> candidates = candidates(
                    graph,
                    request.api(),
                    preferredTypesByApi.getOrDefault(
                            request.api().type(), new LinkedHashSet<>()));
            for (ControllerServiceNode candidate : candidates) {
                if (apiSelection.implementationTypes.size()
                        >= MAX_IMPLEMENTATIONS_PER_API) {
                    break;
                }
                if (!services.containsKey(candidate.type())
                        && additionalServiceCount(original, services)
                        >= MAX_ADDITIONAL_SERVICES) {
                    break;
                }
                apiSelection.add(candidate);
                services.computeIfAbsent(
                        candidate.type(),
                        ignored -> serviceSelection(
                                candidate, request.depth(), Set.of(request.api())));
                if (request.depth() < MAX_DEPENDENCY_DEPTH) {
                    candidate.requiredDependentApis().forEach(api ->
                            pending.addLast(new ApiRequest(api, request.depth() + 1)));
                }
            }
        }

        return new ExpandedClosure(new CapabilityClosure(
                processors.values().stream().toList(),
                apis.values().stream()
                        .map(MutableApiSelection::toSelection)
                        .sorted(Comparator.comparing(selection -> selection.api().type()))
                        .toList(),
                services.values().stream()
                        .sorted(Comparator.comparing(selection -> selection.service().type()))
                        .toList(),
                original.unresolvedRequiredApis(),
                original.truncatedRequiredApis()));
    }

    private List<ControllerServiceNode> candidates(
            final CapabilityGraph graph,
            final ServiceApi api,
            final Set<String> preferredTypes) {
        final Map<String, ControllerServiceNode> candidates = new LinkedHashMap<>();
        preferredTypes.stream()
                .map(graph::resolveControllerService)
                .flatMap(java.util.Optional::stream)
                .filter(service -> graph.serviceImplementations(api).stream()
                        .anyMatch(compatible -> compatible.type().equals(service.type())))
                .forEach(service -> candidates.put(service.type(), service));
        graph.serviceImplementations(api).forEach(service ->
                candidates.putIfAbsent(service.type(), service));
        return List.copyOf(candidates.values());
    }

    private ProcessorSelection processorSelection(final ProcessorNode processor) {
        return new ProcessorSelection(
                new RankedProcessor(processor, 1, Set.of("repair-reference")),
                propertyRanker.rank(processor.properties()),
                processor.requiredControllerServiceApis());
    }

    private ControllerServiceSelection serviceSelection(
            final ControllerServiceNode service,
            final int depth,
            final Set<ServiceApi> requiredByApis) {
        return new ControllerServiceSelection(
                service,
                0,
                Set.of("repair-reference"),
                depth,
                requiredByApis,
                propertyRanker.rank(service.properties()));
    }

    private boolean originalProcessor(
            final CapabilityClosure original,
            final String type) {
        return original.processors().stream()
                .anyMatch(selection -> selection.processor().type().equals(type));
    }

    private boolean originalService(
            final CapabilityClosure original,
            final String type) {
        return original.controllerServices().stream()
                .anyMatch(selection -> selection.service().type().equals(type));
    }

    private int additionalServiceCount(
            final CapabilityClosure original,
            final Map<String, ControllerServiceSelection> services) {
        return services.size() - original.controllerServices().size();
    }

    private List<String> rejectedTypes(
            final Map<String, Object> specification,
            final String field) {
        final Object raw = specification.get(field);
        if (!(raw instanceof Collection<?> components)) {
            return List.of();
        }
        final List<String> types = new ArrayList<>();
        for (Object component : components) {
            if (component instanceof Map<?, ?> values) {
                final Object type = values.get("type");
                if (type != null && !String.valueOf(type).isBlank()) {
                    types.add(String.valueOf(type));
                }
            }
        }
        return types.stream().distinct().sorted().toList();
    }

    private String renderHints(
            final List<RepairHint> hints,
            final int omittedHintCount) {
        final StringBuilder rendered = new StringBuilder(HINT_HEADER);
        hints.forEach(hint -> rendered
                .append("REPAIR_HINT issue=").append(hint.issueType())
                .append(" component=").append(value(hint.affectedComponentId()))
                .append(" path=").append(value(hint.affectedPath()))
                .append(" rejected=").append(value(hint.rejectedTypeOrReference()))
                .append(" requiredApi=")
                .append(hint.requiredApi() == null ? "<none>" : hint.requiredApi().type())
                .append(" compatible=")
                .append(hint.compatibleImplementationTypes().stream()
                        .limit(MAX_IMPLEMENTATIONS_PER_API)
                        .toList())
                .append(" properties=")
                .append(hint.suggestedPropertyNames().stream().limit(8).toList())
                .append(" relationships=")
                .append(hint.supportedRelationships().stream().limit(12).toList())
                .append('\n'));
        if (omittedHintCount > 0) {
            rendered.append("OMITTED_REPAIR_HINTS count=")
                    .append(omittedHintCount)
                    .append('\n');
        }
        return rendered.toString();
    }

    private String value(final String value) {
        if (value == null || value.isBlank()) {
            return "<none>";
        }
        final String normalized = value.replaceAll("\\s+", " ").trim();
        return normalized.length() <= MAX_HINT_VALUE_CHARS
                ? normalized
                : normalized.substring(0, MAX_HINT_VALUE_CHARS);
    }

    private record ApiRequest(ServiceApi api, int depth) {
    }

    private record ExpandedClosure(CapabilityClosure closure) {
    }

    private static final class MutableApiSelection {
        private final ServiceApi api;
        private final Map<String, ControllerServiceNode> implementations =
                new LinkedHashMap<>();
        private final Set<String> implementationTypes = new LinkedHashSet<>();

        private MutableApiSelection(final ServiceApi api) {
            this.api = api;
        }

        private MutableApiSelection(final ApiSelection selection) {
            this(selection.api());
            selection.implementations().forEach(this::add);
        }

        private void add(final ControllerServiceNode service) {
            implementations.putIfAbsent(service.type(), service);
            implementationTypes.add(service.type());
        }

        private ApiSelection toSelection() {
            return new ApiSelection(api, List.copyOf(implementations.values()));
        }
    }
}
