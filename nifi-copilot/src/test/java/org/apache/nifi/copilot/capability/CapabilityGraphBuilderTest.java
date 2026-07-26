package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.junit.jupiter.api.Test;

class CapabilityGraphBuilderTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("group", "artifact", "1.0");
    private static final ServiceApi READER_API =
            new ServiceApi("org.apache.nifi.serialization.RecordReaderFactory", null);
    private static final ServiceApi SCHEMA_API =
            new ServiceApi("org.apache.nifi.schemaregistry.services.SchemaRegistry", null);

    private final CapabilityGraphBuilder builder = new CapabilityGraphBuilder();

    @Test
    void adaptsProcessorMetadataAndClassifiesServiceProperties() {
        final PropertyCapability reader = property("reader", true, READER_API);
        final PropertyCapability optionalSchema = property("schema", false, SCHEMA_API);
        final ProcessorCapability processor = new ProcessorCapability(
                "org.apache.nifi.processors.standard.ConvertRecord",
                BUNDLE,
                Map.of(reader.name(), reader, optionalSchema.name(), optionalSchema),
                true,
                Set.of("success", "failure"),
                false,
                "INPUT_REQUIRED",
                Set.of("TIMER_DRIVEN"),
                true);

        final ProcessorNode node = builder.build(snapshot(Map.of(processor.type(), processor), Map.of()))
                .resolveProcessor("ConvertRecord")
                .orElseThrow();

        assertEquals(Set.of(READER_API), node.requiredControllerServiceApis());
        assertEquals(Set.of(SCHEMA_API), node.optionalControllerServiceApis());
        assertEquals(Set.of("failure", "success"), node.relationships());
        assertEquals("INPUT_REQUIRED", node.schedulingConstraints().inputRequirement());
        assertEquals(Set.of("TIMER_DRIVEN"), node.schedulingConstraints().supportedStrategies());
        assertEquals(true, node.schedulingConstraints().triggerSerially());
        assertEquals(true, node.supportsDynamicProperties());
        final PropertyNode readerNode = node.properties().get("reader");
        assertEquals("reader display", readerNode.displayName());
        assertSame(READER_API, readerNode.requiredControllerServiceApi());
    }

    @Test
    void buildsChainedControllerServiceApiEdges() {
        final ProcessorCapability processor = new ProcessorCapability(
                "example.RecordProcessor",
                BUNDLE,
                Map.of("reader", property("reader", true, READER_API)),
                false,
                Set.of(),
                false,
                null,
                Set.of(),
                false);
        final ControllerServiceCapability reader = new ControllerServiceCapability(
                "example.CSVReader",
                BUNDLE,
                Map.of("registry", property("registry", true, SCHEMA_API)),
                false,
                Set.of(READER_API));
        final ControllerServiceCapability registry = new ControllerServiceCapability(
                "example.AvroSchemaRegistry",
                BUNDLE,
                Map.of(),
                false,
                Set.of(SCHEMA_API));

        final CapabilityGraph graph = builder.build(snapshot(
                Map.of(processor.type(), processor),
                Map.of(reader.type(), reader, registry.type(), registry)));

        final ProcessorNode processorNode = graph.resolveProcessor(processor.type()).orElseThrow();
        final ControllerServiceNode readerNode = graph.serviceImplementations(
                processorNode.requiredControllerServiceApis().iterator().next()).getFirst();
        assertEquals(Set.of(SCHEMA_API), readerNode.requiredDependentApis());
        assertEquals(
                List.of("example.AvroSchemaRegistry"),
                graph.serviceImplementations(readerNode.requiredDependentApis().iterator().next())
                        .stream()
                        .map(ControllerServiceNode::type)
                        .toList());
    }

    @Test
    void classifiesOptionalControllerServiceDependencies() {
        final ControllerServiceCapability service = new ControllerServiceCapability(
                "example.Reader",
                BUNDLE,
                Map.of("registry", property("registry", false, SCHEMA_API)),
                false,
                Set.of(READER_API));

        final ControllerServiceNode node = builder.build(snapshot(
                        Map.of(), Map.of(service.type(), service)))
                .resolveControllerService(service.type())
                .orElseThrow();

        assertEquals(Set.of(SCHEMA_API), node.optionalDependentApis());
        assertFalse(node.requiredDependentApis().contains(SCHEMA_API));
    }

    private CapabilitySnapshot snapshot(
            final Map<String, ProcessorCapability> processors,
            final Map<String, ControllerServiceCapability> services) {
        return new CapabilitySnapshot(processors, services, Instant.parse("2026-01-01T00:00:00Z"));
    }

    private PropertyCapability property(
            final String name,
            final boolean required,
            final ServiceApi serviceApi) {
        return new PropertyCapability(
                name,
                name + " display",
                required,
                null,
                false,
                false,
                List.of(),
                List.of(),
                serviceApi);
    }
}
