package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.SchedulingConstraints;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.CapabilityClosure;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.Limits;
import org.apache.nifi.copilot.capability.ProcessorSeedRanker.RankedProcessor;
import org.junit.jupiter.api.Test;

class DependencyClosureResolverTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("group", "artifact", "1");
    private static final ServiceApi READER_API = api("RecordReaderFactory");
    private static final ServiceApi WRITER_API = api("RecordSetWriterFactory");
    private static final ServiceApi SCHEMA_API = api("SchemaRegistry");

    private final IntentExtractor intentExtractor = new IntentExtractor();
    private final ProcessorSeedRanker seedRanker = new ProcessorSeedRanker();
    private final DependencyClosureResolver resolver = new DependencyClosureResolver(new PropertyRanker());

    @Test
    void expandsRequiredApisAndRanksFormatCompatibleImplementations() {
        final ProcessorNode convertRecord = processor(
                "ConvertRecord",
                Map.of(
                        "reader", property("reader", true, READER_API),
                        "writer", property("writer", true, WRITER_API),
                        "optional", property("optional", false, null)),
                Set.of(READER_API, WRITER_API));
        final CapabilityGraph graph = new CapabilityGraph(
                List.of(convertRecord),
                List.of(
                        service("CSVReader", Set.of(READER_API), Set.of(), Map.of()),
                        service("JsonTreeReader", Set.of(READER_API), Set.of(), Map.of()),
                        service("AvroReader", Set.of(READER_API), Set.of(), Map.of()),
                        service("JsonRecordSetWriter", Set.of(WRITER_API), Set.of(), Map.of()),
                        service("AvroRecordSetWriter", Set.of(WRITER_API), Set.of(), Map.of())));
        final WorkflowIntent intent = intentExtractor.extract("Convert CSV records to JSON");

        final CapabilityClosure closure = resolver.resolve(
                graph, intent, seedRanker.rank(graph, intent));

        assertEquals(
                List.of("example.CSVReader"),
                implementationTypes(closure, READER_API));
        assertEquals(
                List.of("example.JsonRecordSetWriter"),
                implementationTypes(closure, WRITER_API));
        assertEquals(
                List.of("reader", "writer"),
                closure.processors().getFirst().essentialProperties().stream()
                        .map(PropertyNode::name)
                        .toList());
        assertTrue(closure.unresolvedRequiredApis().isEmpty());
    }

    @Test
    void followsRequiredServiceDependenciesAndIncludesEssentialServiceProperties() {
        final ProcessorNode processor = processor(
                "ConvertRecord",
                Map.of("reader", property("reader", true, READER_API)),
                Set.of(READER_API));
        final ControllerServiceNode reader = service(
                "CSVReader",
                Set.of(READER_API),
                Set.of(SCHEMA_API),
                Map.of(
                        "registry", property("registry", true, SCHEMA_API),
                        "optional", property("optional", false, null)));
        final CapabilityGraph graph = new CapabilityGraph(
                List.of(processor),
                List.of(reader, service(
                        "AvroSchemaRegistry", Set.of(SCHEMA_API), Set.of(), Map.of())));
        final WorkflowIntent intent = intentExtractor.extract("Convert CSV records");

        final CapabilityClosure closure = resolver.resolve(
                graph, intent, seedRanker.rank(graph, intent));

        assertEquals(List.of("example.CSVReader"), implementationTypes(closure, READER_API));
        assertEquals(List.of("example.AvroSchemaRegistry"), implementationTypes(closure, SCHEMA_API));
        assertEquals(
                2,
                closure.controllerServices().stream()
                        .filter(selection -> selection.service().type().equals(
                                "example.AvroSchemaRegistry"))
                        .findFirst()
                        .orElseThrow()
                        .depth());
        assertEquals(
                List.of("registry"),
                closure.controllerServices().stream()
                        .filter(selection -> selection.service().type().equals("example.CSVReader"))
                        .findFirst()
                        .orElseThrow()
                        .essentialProperties().stream()
                        .map(PropertyNode::name)
                        .toList());
    }

    @Test
    void boundsCandidatesAndDepthWhileReportingTruncation() {
        final ProcessorNode processor = processor(
                "ConvertRecord",
                Map.of("reader", property("reader", true, READER_API)),
                Set.of(READER_API));
        final CapabilityGraph graph = new CapabilityGraph(
                List.of(processor),
                List.of(
                        service("CSVReader", Set.of(READER_API), Set.of(SCHEMA_API), Map.of()),
                        service("JsonTreeReader", Set.of(READER_API), Set.of(), Map.of()),
                        service("AvroReader", Set.of(READER_API), Set.of(), Map.of()),
                        service("SchemaRegistry", Set.of(SCHEMA_API), Set.of(), Map.of())));
        final WorkflowIntent intent = intentExtractor.extract("Convert CSV records");

        final CapabilityClosure closure = resolver.resolve(
                graph,
                intent,
                seedRanker.rank(graph, intent),
                new Limits(1, 2, 2));

        assertEquals(2, implementationTypes(closure, READER_API).size());
        assertTrue(closure.truncatedRequiredApis().contains(READER_API));
        assertTrue(closure.truncatedRequiredApis().contains(SCHEMA_API));
        assertEquals(2, closure.controllerServices().size());
    }

    @Test
    void reportsRequiredApisWithoutInstalledImplementations() {
        final ProcessorNode processor = processor(
                "ConvertRecord",
                Map.of("reader", property("reader", true, READER_API)),
                Set.of(READER_API));
        final CapabilityGraph graph = new CapabilityGraph(List.of(processor), List.of());
        final WorkflowIntent intent = intentExtractor.extract("Convert records");

        final CapabilityClosure closure = resolver.resolve(
                graph, intent, seedRanker.rank(graph, intent));

        assertEquals(Set.of(READER_API), closure.unresolvedRequiredApis());
        assertTrue(closure.controllerServices().isEmpty());
    }

    private List<String> implementationTypes(
            final CapabilityClosure closure,
            final ServiceApi api) {
        return closure.requiredApiSelections().stream()
                .filter(selection -> selection.api().equals(api))
                .findFirst()
                .orElseThrow()
                .implementations().stream()
                .map(ControllerServiceNode::type)
                .toList();
    }

    private ProcessorNode processor(
            final String simpleName,
            final Map<String, PropertyNode> properties,
            final Set<ServiceApi> requiredApis) {
        return new ProcessorNode(
                "example." + simpleName,
                BUNDLE,
                properties,
                false,
                requiredApis,
                Set.of(),
                Set.of("success"),
                false,
                new SchedulingConstraints(null, Set.of("TIMER_DRIVEN"), false));
    }

    private ControllerServiceNode service(
            final String simpleName,
            final Set<ServiceApi> implementedApis,
            final Set<ServiceApi> requiredApis,
            final Map<String, PropertyNode> properties) {
        return new ControllerServiceNode(
                "example." + simpleName,
                BUNDLE,
                properties,
                false,
                implementedApis,
                requiredApis,
                Set.of());
    }

    private PropertyNode property(
            final String name,
            final boolean required,
            final ServiceApi serviceApi) {
        return new PropertyNode(
                name,
                name,
                required,
                null,
                false,
                false,
                List.of(),
                List.of(),
                serviceApi);
    }

    private static ServiceApi api(final String simpleName) {
        return new ServiceApi("example." + simpleName, null);
    }
}
