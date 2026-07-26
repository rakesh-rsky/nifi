package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.SchedulingConstraints;
import org.junit.jupiter.api.Test;

class CapabilityGraphTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("group", "artifact", "1.0");

    @Test
    void resolvesFqnAndCaseInsensitiveUniqueSimpleName() {
        final ProcessorNode processor = processor("org.apache.nifi.processors.ConvertRecord");
        final CapabilityGraph graph = new CapabilityGraph(List.of(processor), List.of());

        assertSame(processor, graph.resolveProcessor(processor.type()).orElseThrow());
        assertSame(processor, graph.resolveProcessor("convertrecord").orElseThrow());
    }

    @Test
    void excludesAmbiguousSimpleNames() {
        final CapabilityGraph graph = new CapabilityGraph(
                List.of(processor("one.shared.Processor"), processor("two.shared.Processor")),
                List.of());

        assertFalse(graph.resolveProcessor("Processor").isPresent());
        assertFalse(graph.processorsByUniqueSimpleName().containsKey("processor"));
    }

    @Test
    void indexesCompatibleServiceImplementationsDeterministically() {
        final ServiceApi api = new ServiceApi("org.apache.nifi.serialization.RecordReaderFactory", null);
        final ControllerServiceNode second = service("example.JsonTreeReader", Set.of(api));
        final ControllerServiceNode first = service("example.CSVReader", Set.of(api));
        final CapabilityGraph graph = new CapabilityGraph(List.of(), List.of(second, first));

        assertEquals(
                List.of(first, second),
                graph.serviceImplementations(api));
        assertSame(first, graph.resolveControllerService("csvreader").orElseThrow());
    }

    @Test
    void exposesImmutableNodeAndIndexCollections() {
        final PropertyNode property = new PropertyNode(
                "reader", "Record Reader", true, null, false, false,
                List.of(), List.of(), null);
        final ProcessorNode processor = new ProcessorNode(
                "example.ConvertRecord",
                BUNDLE,
                Map.of(property.name(), property),
                false,
                Set.of(),
                Set.of(),
                Set.of("success"),
                false,
                new SchedulingConstraints("INPUT_REQUIRED", Set.of("TIMER_DRIVEN"), false));
        final CapabilityGraph graph = new CapabilityGraph(List.of(processor), List.of());

        assertThrows(UnsupportedOperationException.class,
                () -> graph.processorsByType().put("other", processor));
        assertThrows(UnsupportedOperationException.class,
                () -> processor.properties().put("other", property));
        assertThrows(UnsupportedOperationException.class,
                () -> processor.relationships().add("failure"));
    }

    @Test
    void rejectsDuplicateFullyQualifiedTypes() {
        final ProcessorNode processor = processor("example.Processor");

        assertThrows(IllegalArgumentException.class,
                () -> new CapabilityGraph(List.of(processor, processor), List.of()));
    }

    private ProcessorNode processor(final String type) {
        return new ProcessorNode(
                type,
                BUNDLE,
                Map.of(),
                false,
                Set.of(),
                Set.of(),
                Set.of(),
                false,
                new SchedulingConstraints(null, Set.of(), false));
    }

    private ControllerServiceNode service(final String type, final Set<ServiceApi> implementedApis) {
        return new ControllerServiceNode(
                type,
                BUNDLE,
                Map.of(),
                false,
                implementedApis,
                Set.of(),
                Set.of());
    }
}
