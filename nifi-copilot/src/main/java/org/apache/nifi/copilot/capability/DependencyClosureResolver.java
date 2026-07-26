package org.apache.nifi.copilot.capability;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.ProcessorSeedRanker.RankedProcessor;
import org.apache.nifi.copilot.capability.PropertyRanker.RankedProperty;
import org.apache.nifi.copilot.capability.WorkflowIntent.DataFormat;
import org.springframework.stereotype.Component;

@Component
public final class DependencyClosureResolver {
    static final Limits DEFAULT_LIMITS = new Limits(4, 1, 16);

    private final PropertyRanker propertyRanker;

    public DependencyClosureResolver(final PropertyRanker propertyRanker) {
        this.propertyRanker = propertyRanker;
    }

    public CapabilityClosure resolve(
            final CapabilityGraph graph,
            final WorkflowIntent intent,
            final List<RankedProcessor> processorSeeds) {
        return resolve(graph, intent, processorSeeds, DEFAULT_LIMITS);
    }

    CapabilityClosure resolve(
            final CapabilityGraph graph,
            final WorkflowIntent intent,
            final List<RankedProcessor> processorSeeds,
            final Limits limits) {
        if (graph == null || intent == null || processorSeeds == null || limits == null) {
            throw new IllegalArgumentException(
                    "Capability graph, workflow intent, processor seeds, and limits are required");
        }

        final List<ProcessorSelection> processors = processorSeeds.stream()
                .map(seed -> new ProcessorSelection(
                        seed,
                        propertyRanker.rank(seed.processor().properties()),
                        seed.processor().requiredControllerServiceApis()))
                .toList();
        final Deque<ApiRequest> pending = new ArrayDeque<>();
        processors.forEach(processor -> processor.requiredApis()
                .forEach(api -> pending.addLast(new ApiRequest(api, 1))));

        final Set<ServiceApi> processedApis = new LinkedHashSet<>();
        final Map<ServiceApi, ApiSelection> apiSelections = new LinkedHashMap<>();
        final Map<String, MutableServiceSelection> selectedServices = new LinkedHashMap<>();
        final Set<ServiceApi> unresolvedApis = new LinkedHashSet<>();
        final Set<ServiceApi> truncatedApis = new LinkedHashSet<>();

        while (!pending.isEmpty()) {
            final ApiRequest request = pending.removeFirst();
            if (!processedApis.add(request.api())) {
                continue;
            }
            if (request.depth() > limits.maxDepth()) {
                truncatedApis.add(request.api());
                continue;
            }

            final List<RankedService> installed = rankImplementations(
                    graph.serviceImplementations(request.api()), request.api(), intent);
            if (installed.isEmpty()) {
                unresolvedApis.add(request.api());
                continue;
            }

            final List<ControllerServiceNode> implementations = new ArrayList<>();
            for (RankedService candidate : installed) {
                if (implementations.size() >= limits.maxImplementationsPerApi()) {
                    truncatedApis.add(request.api());
                    break;
                }
                final MutableServiceSelection existing = selectedServices.get(candidate.service().type());
                if (existing == null && selectedServices.size() >= limits.maxControllerServices()) {
                    truncatedApis.add(request.api());
                    break;
                }

                implementations.add(candidate.service());
                final MutableServiceSelection selection = selectedServices.computeIfAbsent(
                        candidate.service().type(),
                        ignored -> new MutableServiceSelection(candidate, request.depth()));
                selection.requiredByApis.add(request.api());
                selection.depth = Math.min(selection.depth, request.depth());

                for (ServiceApi dependency : candidate.service().requiredDependentApis()) {
                    if (request.depth() < limits.maxDepth()) {
                        pending.addLast(new ApiRequest(dependency, request.depth() + 1));
                    } else {
                        truncatedApis.add(dependency);
                    }
                }
            }
            if (!implementations.isEmpty()) {
                apiSelections.put(request.api(), new ApiSelection(request.api(), implementations));
            }
        }

        final List<ControllerServiceSelection> services = selectedServices.values().stream()
                .map(MutableServiceSelection::toSelection)
                .sorted(Comparator.comparing(selection -> selection.service().type()))
                .toList();
        return new CapabilityClosure(
                processors,
                apiSelections.values().stream()
                        .sorted(Comparator.comparing(selection -> selection.api().type()))
                        .toList(),
                services,
                unresolvedApis,
                truncatedApis);
    }

    private List<RankedService> rankImplementations(
            final List<ControllerServiceNode> implementations,
            final ServiceApi requiredApi,
            final WorkflowIntent intent) {
        return implementations.stream()
                .map(service -> score(service, requiredApi, intent))
                .sorted(Comparator
                        .comparingInt(RankedService::score)
                        .reversed()
                        .thenComparing(candidate -> candidate.service().type()))
                .toList();
    }

    private RankedService score(
            final ControllerServiceNode service,
            final ServiceApi requiredApi,
            final WorkflowIntent intent) {
        final String simpleName = simpleName(service.type()).toLowerCase(Locale.ROOT);
        final Set<String> nameTokens = words(simpleName(service.type()));
        final Set<String> signals = new TreeSet<>();
        int score = 0;

        for (DataFormat format : intent.formats()) {
            final String formatToken = format.name().toLowerCase(Locale.ROOT);
            if (nameTokens.contains(formatToken)) {
                score += 100;
                signals.add("format:" + format.name());
                if (isPreferredFormat(formatToken, requiredApi, intent.normalizedText())) {
                    score += 50;
                    signals.add("format-role:" + format.name());
                }
            }
        }
        for (String term : intent.terms()) {
            if (term.length() >= 3 && nameTokens.contains(term)) {
                score += 10;
                signals.add("name:" + term);
            }
        }
        if (intent.normalizedText().contains(simpleName)) {
            score += 200;
            signals.add("explicit-name");
        }
        return new RankedService(service, score, signals);
    }

    private boolean isPreferredFormat(
            final String format,
            final ServiceApi requiredApi,
            final String normalizedText) {
        final String apiType = requiredApi.type().toLowerCase(Locale.ROOT);
        final int position = normalizedText.indexOf(format);
        if (position < 0) {
            return false;
        }
        if (apiType.contains("reader")) {
            return earliestFormatPosition(normalizedText) == position;
        }
        if (apiType.contains("writer")) {
            return latestFormatPosition(normalizedText) == position;
        }
        return false;
    }

    private int earliestFormatPosition(final String normalizedText) {
        int earliest = Integer.MAX_VALUE;
        for (DataFormat format : DataFormat.values()) {
            final int position = normalizedText.indexOf(format.name().toLowerCase(Locale.ROOT));
            if (position >= 0) {
                earliest = Math.min(earliest, position);
            }
        }
        return earliest;
    }

    private int latestFormatPosition(final String normalizedText) {
        int latest = -1;
        for (DataFormat format : DataFormat.values()) {
            latest = Math.max(
                    latest,
                    normalizedText.lastIndexOf(format.name().toLowerCase(Locale.ROOT)));
        }
        return latest;
    }

    private static String simpleName(final String type) {
        return type.substring(type.lastIndexOf('.') + 1);
    }

    private static Set<String> words(final String value) {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        final Set<String> words = new LinkedHashSet<>();
        final String spaced = value
                .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2");
        for (String word : spaced.toLowerCase(Locale.ROOT).split("[^a-z0-9]+")) {
            if (!word.isBlank()) {
                words.add(word);
            }
        }
        return words;
    }

    record Limits(
            int maxDepth,
            int maxImplementationsPerApi,
            int maxControllerServices) {
        Limits {
            if (maxDepth <= 0 || maxImplementationsPerApi <= 0 || maxControllerServices <= 0) {
                throw new IllegalArgumentException("Dependency closure limits must be positive");
            }
        }
    }

    public record CapabilityClosure(
            List<ProcessorSelection> processors,
            List<ApiSelection> requiredApiSelections,
            List<ControllerServiceSelection> controllerServices,
            Set<ServiceApi> unresolvedRequiredApis,
            Set<ServiceApi> truncatedRequiredApis) {
        public CapabilityClosure {
            processors = processors == null ? List.of() : List.copyOf(processors);
            requiredApiSelections =
                    requiredApiSelections == null ? List.of() : List.copyOf(requiredApiSelections);
            controllerServices =
                    controllerServices == null ? List.of() : List.copyOf(controllerServices);
            unresolvedRequiredApis = immutableApis(unresolvedRequiredApis);
            truncatedRequiredApis = immutableApis(truncatedRequiredApis);
        }
    }

    public record ProcessorSelection(
            RankedProcessor rankedProcessor,
            List<RankedProperty> properties,
            Set<ServiceApi> requiredApis) {
        public ProcessorSelection {
            if (rankedProcessor == null) {
                throw new IllegalArgumentException("Ranked processor is required");
            }
            properties = properties == null ? List.of() : List.copyOf(properties);
            requiredApis = immutableApis(requiredApis);
        }

        public ProcessorNode processor() {
            return rankedProcessor.processor();
        }

        public List<PropertyNode> essentialProperties() {
            return properties.stream()
                    .filter(RankedProperty::essential)
                    .map(RankedProperty::property)
                    .toList();
        }
    }

    public record ApiSelection(
            ServiceApi api,
            List<ControllerServiceNode> implementations) {
        public ApiSelection {
            if (api == null) {
                throw new IllegalArgumentException("Required API is required");
            }
            implementations = implementations == null ? List.of() : List.copyOf(implementations);
        }
    }

    public record ControllerServiceSelection(
            ControllerServiceNode service,
            int score,
            Set<String> matchedSignals,
            int depth,
            Set<ServiceApi> requiredByApis,
            List<RankedProperty> properties) {
        public ControllerServiceSelection {
            if (service == null || depth <= 0) {
                throw new IllegalArgumentException(
                        "Controller service and positive dependency depth are required");
            }
            matchedSignals = matchedSignals == null
                    ? Set.of()
                    : Collections.unmodifiableSet(new TreeSet<>(matchedSignals));
            requiredByApis = immutableApis(requiredByApis);
            properties = properties == null ? List.of() : List.copyOf(properties);
        }

        public List<PropertyNode> essentialProperties() {
            return properties.stream()
                    .filter(RankedProperty::essential)
                    .map(RankedProperty::property)
                    .toList();
        }
    }

    private record ApiRequest(ServiceApi api, int depth) {
    }

    private record RankedService(
            ControllerServiceNode service,
            int score,
            Set<String> matchedSignals) {
    }

    private final class MutableServiceSelection {
        private final RankedService rankedService;
        private final Set<ServiceApi> requiredByApis = new LinkedHashSet<>();
        private int depth;

        private MutableServiceSelection(final RankedService rankedService, final int depth) {
            this.rankedService = rankedService;
            this.depth = depth;
        }

        private ControllerServiceSelection toSelection() {
            return new ControllerServiceSelection(
                    rankedService.service(),
                    rankedService.score(),
                    rankedService.matchedSignals(),
                    depth,
                    requiredByApis,
                    propertyRanker.rank(rankedService.service().properties()));
        }
    }

    private static Set<ServiceApi> immutableApis(final Set<ServiceApi> apis) {
        if (apis == null || apis.isEmpty()) {
            return Set.of();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<>(apis));
    }
}
