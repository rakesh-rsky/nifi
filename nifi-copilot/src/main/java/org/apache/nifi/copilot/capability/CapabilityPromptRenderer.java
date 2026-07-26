package org.apache.nifi.copilot.capability;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.ApiSelection;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.CapabilityClosure;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.ControllerServiceSelection;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.ProcessorSelection;
import org.apache.nifi.copilot.capability.PropertyRanker.PropertyClass;
import org.apache.nifi.copilot.capability.PropertyRanker.RankedProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CapabilityPromptRenderer {
    static final int MAX_CONTEXT_CHARS = 12_000;
    private static final int MAX_ALLOWABLE_VALUES = 12;
    private static final String HEADER = """
            [TARGET NIFI CAPABILITIES]
            This target snapshot is authoritative. Use only the exact types and property names below.
            Omit a component or property rather than inventing one.
            """;

    private final IntentExtractor intentExtractor;
    private final ProcessorSeedRanker processorSeedRanker;
    private final DependencyClosureResolver closureResolver;
    private final CapabilityGraphBuilder graphBuilder;
    private final CapabilityMetricsRegistry metrics;

    public CapabilityPromptRenderer() {
        this(
                new IntentExtractor(),
                new ProcessorSeedRanker(),
                new DependencyClosureResolver(new PropertyRanker()),
                new CapabilityGraphBuilder(),
                new CapabilityMetricsRegistry());
    }

    @Autowired
    public CapabilityPromptRenderer(
            final IntentExtractor intentExtractor,
            final ProcessorSeedRanker processorSeedRanker,
            final DependencyClosureResolver closureResolver,
            final CapabilityMetricsRegistry metrics) {
        this(
                intentExtractor,
                processorSeedRanker,
                closureResolver,
                new CapabilityGraphBuilder(),
                metrics);
    }

    CapabilityPromptRenderer(
            final IntentExtractor intentExtractor,
            final ProcessorSeedRanker processorSeedRanker,
            final DependencyClosureResolver closureResolver,
            final CapabilityGraphBuilder graphBuilder,
            final CapabilityMetricsRegistry metrics) {
        if (intentExtractor == null
                || processorSeedRanker == null
                || closureResolver == null
                || graphBuilder == null
                || metrics == null) {
            throw new IllegalArgumentException("Capability renderer collaborators are required");
        }
        this.intentExtractor = intentExtractor;
        this.processorSeedRanker = processorSeedRanker;
        this.closureResolver = closureResolver;
        this.graphBuilder = graphBuilder;
        this.metrics = metrics;
    }

    public String render(final String userIntent, final CapabilitySnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("Capability snapshot is required");
        }
        return render(userIntent, graphBuilder.build(snapshot), MAX_CONTEXT_CHARS);
    }

    public String renderFromGraph(final String userIntent, final CapabilityGraph graph) {
        return render(userIntent, graph, MAX_CONTEXT_CHARS);
    }

    String render(
            final String userIntent,
            final CapabilityGraph graph,
            final int maxContextChars) {
        if (graph == null) {
            throw new IllegalArgumentException("Capability graph is required");
        }
        if (maxContextChars < HEADER.length()) {
            throw new IllegalArgumentException("Capability context limit is smaller than the required header");
        }

        final WorkflowIntent intent = intentExtractor.extract(userIntent);
        final CapabilityClosure closure = selectFitting(intent, graph, maxContextChars);
        final String rendered = render(closure, maxContextChars);
        metrics.observeSelection(intent, closure, rendered.length());
        return rendered;
    }

    private CapabilityClosure selectFitting(
            final WorkflowIntent intent,
            final CapabilityGraph graph,
            final int maxContextChars) {
        final List<ProcessorSeedRanker.RankedProcessor> ranked =
                processorSeedRanker.rank(graph, intent);
        for (int seedCount = ranked.size(); seedCount >= 0; seedCount--) {
            final CapabilityClosure closure = closureResolver.resolve(
                    graph, intent, ranked.subList(0, seedCount));
            try {
                render(closure, maxContextChars);
                return closure;
            } catch (RequiredContextOverflowException e) {
                if (seedCount == 0) {
                    throw e;
                }
            }
        }
        throw new IllegalStateException("Unable to select capability context");
    }

    CapabilityClosure select(
            final String userIntent,
            final CapabilityGraph graph) {
        if (graph == null) {
            throw new IllegalArgumentException("Capability graph is required");
        }
        final WorkflowIntent intent = intentExtractor.extract(userIntent);
        return selectFitting(intent, graph, MAX_CONTEXT_CHARS);
    }

    CapabilityClosure select(
            final String userIntent,
            final CapabilityGraph graph,
            final int maxContextChars) {
        if (graph == null) {
            throw new IllegalArgumentException("Capability graph is required");
        }
        return selectFitting(
                intentExtractor.extract(userIntent), graph, maxContextChars);
    }

    String render(
            final CapabilityClosure closure,
            final int maxContextChars) {
        if (closure == null) {
            throw new IllegalArgumentException("Capability closure is required");
        }
        final BoundedContext context = new BoundedContext(maxContextChars);
        context.appendRequired(HEADER);

        closure.processors().forEach(processor ->
                context.appendRequired(processorLine(processor)));
        closure.requiredApiSelections().forEach(api ->
                context.appendRequired(apiLine(api)));
        closure.controllerServices().forEach(service ->
                context.appendRequired(serviceLine(service)));
        closure.unresolvedRequiredApis().forEach(api ->
                context.appendRequired("UNRESOLVED_REQUIRED_API " + formatApi(api) + '\n'));

        appendRequiredProperties(context, closure);

        closure.processors().forEach(processor ->
                context.appendRequired(relationshipLine(processor)));

        appendRequiredPropertyDetails(context, closure);
        appendProperties(
                context,
                closure,
                ranked -> ranked.propertyClass() == PropertyClass.DATA_CONTRACT,
                false);
        appendProperties(
                context,
                closure,
                ranked -> ranked.propertyClass() == PropertyClass.OPTIONAL,
                false);
        appendRuntimeMetadata(context, closure);
        appendProperties(
                context,
                closure,
                ranked -> ranked.propertyClass() == PropertyClass.RUNTIME,
                false);

        return context.toString();
    }

    private void appendProperties(
            final BoundedContext context,
            final CapabilityClosure closure,
            final Predicate<RankedProperty> selection,
            final boolean required) {
        closure.processors().forEach(processor -> processor.properties().stream()
                .filter(selection)
                .forEach(property -> appendProperty(
                        context, "PROCESSOR_PROPERTY", processor.processor().type(), property, required)));
        closure.controllerServices().forEach(service -> service.properties().stream()
                .filter(selection)
                .forEach(property -> appendProperty(
                        context, "CONTROLLER_SERVICE_PROPERTY", service.service().type(), property, required)));
    }

    private void appendRequiredProperties(
            final BoundedContext context,
            final CapabilityClosure closure) {
        closure.processors().forEach(processor -> appendRequiredProperties(
                context,
                "PROCESSOR_PROPERTIES",
                processor.processor().type(),
                processor.properties()));
        closure.controllerServices().forEach(service -> appendRequiredProperties(
                context,
                "CONTROLLER_SERVICE_PROPERTIES",
                service.service().type(),
                service.properties()));
    }

    private void appendRequiredProperties(
            final BoundedContext context,
            final String kind,
            final String componentType,
            final List<RankedProperty> properties) {
        final List<RankedProperty> required = properties.stream()
                .filter(ranked -> ranked.propertyClass() == PropertyClass.REQUIRED_SERVICE_REFERENCE
                        || ranked.propertyClass() == PropertyClass.REQUIRED
                        || ranked.propertyClass() == PropertyClass.SERVICE_REFERENCE)
                .toList();
        if (required.isEmpty()) {
            return;
        }
        context.appendRequired(kind + " " + componentType + '\n');
        required.forEach(property -> {
            final StringBuilder line = propertyLine("  PROPERTY", "", property);
            line.append('\n');
            context.appendRequired(line.toString());
        });
    }

    private void appendProperty(
            final BoundedContext context,
            final String kind,
            final String componentType,
            final RankedProperty rankedProperty,
            final boolean required) {
        final PropertyNode property = rankedProperty.property();
        final StringBuilder line = propertyLine(
                kind, componentType, rankedProperty);
        if (!required) {
            appendPropertyDetails(line, property);
        }
        line.append('\n');
        if (required) {
            context.appendRequired(line.toString());
        } else {
            context.appendOptional(line.toString());
        }
    }

    private StringBuilder propertyLine(
            final String kind,
            final String componentType,
            final RankedProperty rankedProperty) {
        final PropertyNode property = rankedProperty.property();
        final StringBuilder line = new StringBuilder()
                .append(kind);
        if (!componentType.isBlank()) {
            line.append(' ').append(componentType);
        }
        line
                .append(" internal=").append(property.name())
                .append(" display=").append(property.displayName())
                .append(" required=").append(property.required())
                .append(" class=").append(rankedProperty.propertyClass())
                .append(" default=").append(defaultValue(property));
        if (property.requiredControllerServiceApi() != null) {
            line.append(" controllerServiceApi=")
                    .append(formatApi(property.requiredControllerServiceApi()));
        }
        return line;
    }

    private void appendRequiredPropertyDetails(
            final BoundedContext context,
            final CapabilityClosure closure) {
        final Predicate<RankedProperty> required = ranked ->
                ranked.propertyClass() == PropertyClass.REQUIRED_SERVICE_REFERENCE
                        || ranked.propertyClass() == PropertyClass.REQUIRED
                        || ranked.propertyClass() == PropertyClass.SERVICE_REFERENCE;
        closure.processors().forEach(processor -> processor.properties().stream()
                .filter(required)
                .forEach(property -> appendPropertyDetails(
                        context,
                        "PROCESSOR_PROPERTY_DETAILS",
                        processor.processor().type(),
                        property)));
        closure.controllerServices().forEach(service -> service.properties().stream()
                .filter(required)
                .forEach(property -> appendPropertyDetails(
                        context,
                        "CONTROLLER_SERVICE_PROPERTY_DETAILS",
                        service.service().type(),
                        property)));
    }

    private void appendPropertyDetails(
            final BoundedContext context,
            final String kind,
            final String componentType,
            final RankedProperty rankedProperty) {
        final PropertyNode property = rankedProperty.property();
        if (property.dependencies().isEmpty() && property.allowableValues().isEmpty()) {
            return;
        }
        final StringBuilder line = new StringBuilder()
                .append(kind).append(' ').append(componentType)
                .append(" internal=").append(property.name());
        appendPropertyDetails(line, property);
        line.append('\n');
        context.appendOptional(line.toString());
    }

    private void appendPropertyDetails(
            final StringBuilder line,
            final PropertyNode property) {
        if (!property.dependencies().isEmpty()) {
            line.append(" dependsOn=").append(property.dependencies().stream()
                    .limit(MAX_ALLOWABLE_VALUES)
                    .map(dependency -> dependency.propertyName()
                            + (dependency.dependentValues().isEmpty()
                                    ? "=<configured>"
                                    : "=" + dependency.dependentValues().stream()
                                            .sorted()
                                            .limit(MAX_ALLOWABLE_VALUES)
                                            .toList()))
                    .sorted()
                    .toList());
        }
        if (!property.allowableValues().isEmpty()) {
            final List<String> allowableValues = property.allowableValues().stream()
                    .map(value -> value.value() + "/" + value.displayName())
                    .sorted()
                    .limit(MAX_ALLOWABLE_VALUES)
                    .toList();
            line.append(" allowable=").append(allowableValues);
            if (property.allowableValues().size() > allowableValues.size()) {
                line.append(" (+")
                        .append(property.allowableValues().size() - allowableValues.size())
                        .append(" more)");
            }
        }
    }

    private String processorLine(final ProcessorSelection processor) {
        return "PROCESSOR " + processor.processor().type() + '\n';
    }

    private String apiLine(final ApiSelection selection) {
        return "REQUIRED_CONTROLLER_SERVICE_API " + formatApi(selection.api())
                + " implementations=" + selection.implementations().stream()
                        .map(CapabilityGraph.ControllerServiceNode::type)
                        .toList()
                + '\n';
    }

    private String serviceLine(final ControllerServiceSelection selection) {
        return "CONTROLLER_SERVICE " + selection.service().type()
                + " depth=" + selection.depth()
                + '\n';
    }

    private String relationshipLine(final ProcessorSelection selection) {
        return "PROCESSOR_RELATIONSHIPS " + selection.processor().type()
                + " values=" + sorted(selection.processor().relationships())
                + " dynamic=" + selection.processor().supportsDynamicRelationships()
                + '\n';
    }

    private void appendRuntimeMetadata(
            final BoundedContext context,
            final CapabilityClosure closure) {
        closure.processors().forEach(selection -> context.appendOptional(
                "PROCESSOR_RUNTIME " + selection.processor().type()
                        + " inputRequirement=" + selection.processor()
                                .schedulingConstraints().inputRequirement()
                        + " scheduling=" + sorted(selection.processor()
                                .schedulingConstraints().supportedStrategies())
                        + " triggerSerially=" + selection.processor()
                                .schedulingConstraints().triggerSerially()
                        + " dynamicProperties=" + selection.processor().supportsDynamicProperties()
                        + '\n'));
        closure.controllerServices().forEach(selection -> context.appendOptional(
                "CONTROLLER_SERVICE_RUNTIME " + selection.service().type()
                        + " dynamicProperties=" + selection.service().supportsDynamicProperties()
                        + '\n'));
    }

    private String defaultValue(final PropertyNode property) {
        if (property.sensitive()) {
            return "<sensitive>";
        }
        return property.defaultValue() == null ? "<none>" : property.defaultValue();
    }

    private String formatApi(final ServiceApi api) {
        if (api.bundle() == null) {
            return api.type();
        }
        return api.type() + "@"
                + api.bundle().group() + ":"
                + api.bundle().artifact() + ":"
                + api.bundle().version();
    }

    private List<String> sorted(final Set<String> values) {
        return values.stream().sorted().toList();
    }

    private static final class BoundedContext {
        private final int limit;
        private final StringBuilder value = new StringBuilder();

        private BoundedContext(final int limit) {
            this.limit = limit;
        }

        private void appendRequired(final String text) {
            if (value.length() + text.length() > limit) {
                throw new RequiredContextOverflowException(
                        "Required capability context exceeds the configured character limit");
            }
            value.append(text);
        }

        private void appendOptional(final String text) {
            if (value.length() + text.length() <= limit) {
                value.append(text);
            }
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    private static final class RequiredContextOverflowException extends IllegalStateException {
        private RequiredContextOverflowException(final String message) {
            super(message);
        }
    }
}
