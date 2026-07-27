package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CapabilityPromptRendererTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("g", "a", "1");

    @Test
    void rendersBoundedDeterministicContextFromInstalledCapabilitiesOnly() {
        final Map<String, ProcessorCapability> processors = new LinkedHashMap<>();
        processors.put("org.example.Unrelated", processor("org.example.Unrelated", Map.of()));
        processors.put("org.apache.nifi.processors.standard.InvokeHTTP", processor(
                "org.apache.nifi.processors.standard.InvokeHTTP",
                Map.of("url", property("url", "Remote URL", true, null, null))));
        processors.put("org.apache.nifi.processors.mqtt.ConsumeMQTT", processor(
                "org.apache.nifi.processors.mqtt.ConsumeMQTT",
                Map.of(
                        "broker-uri", property("broker-uri", "Broker URI", true, null, null),
                        "topic", property("topic", "Topic", true, null, null))));
        final CapabilitySnapshot snapshot = new CapabilitySnapshot(
                processors, Map.of(), Instant.parse("2026-01-01T00:00:00Z"));
        final CapabilityPromptRenderer renderer = new CapabilityPromptRenderer();

        final String first = renderer.render(
                "Read MQTT JSON and send to HTTP with five parallel workers", snapshot);
        final String second = renderer.render(
                "Read MQTT JSON and send to HTTP with five parallel workers", snapshot);

        assertEquals(first, second);
        assertTrue(first.length() <= CapabilityPromptRenderer.MAX_CONTEXT_CHARS);
        assertTrue(first.contains("PROCESSOR org.apache.nifi.processors.mqtt.ConsumeMQTT"));
        assertTrue(first.contains(
                "internal=broker-uri display=Broker URI required=true class=REQUIRED"));
        assertTrue(first.contains("PROCESSOR org.apache.nifi.processors.standard.InvokeHTTP"));
        assertFalse(first.contains("PROCESSOR org.example.Unrelated"));
        assertFalse(first.contains("DistributeLoad"));
    }

    @Test
    void rendersRequiredApiConcreteImplementationsAndChainedDependencies() {
        final ServiceApi readerApi = new ServiceApi("org.example.ReaderApi", BUNDLE);
        final ServiceApi schemaApi = new ServiceApi("org.example.SchemaApi", BUNDLE);
        final ProcessorCapability processor = processor("org.example.ReadRecords", Map.of(
                "reader", property("reader", "Record Reader", true, null, readerApi)));
        final ControllerServiceCapability reader = new ControllerServiceCapability(
                "org.example.JsonReader",
                BUNDLE,
                Map.of("registry", property(
                        "registry", "Schema Registry", true, null, schemaApi)),
                false,
                Set.of(readerApi));
        final ControllerServiceCapability registry = new ControllerServiceCapability(
                "org.example.SchemaRegistry", BUNDLE, Map.of(), false, Set.of(schemaApi));
        final CapabilitySnapshot snapshot = new CapabilitySnapshot(
                Map.of(processor.type(), processor),
                Map.of(reader.type(), reader, registry.type(), registry),
                Instant.now());

        final String rendered = new CapabilityPromptRenderer().render("read JSON records", snapshot);

        assertTrue(rendered.contains(
                "REQUIRED_CONTROLLER_SERVICE_API org.example.ReaderApi@g:a:1 "
                        + "implementations=[org.example.JsonReader]"));
        assertTrue(rendered.contains("CONTROLLER_SERVICE org.example.JsonReader"));
        assertTrue(rendered.contains("CONTROLLER_SERVICE org.example.SchemaRegistry"));
        assertTrue(rendered.contains("controllerServiceApi=org.example.ReaderApi@g:a:1"));
        assertTrue(rendered.contains("controllerServiceApi=org.example.SchemaApi@g:a:1"));
    }

    @Test
    void ranksFormatCompatibleReaderAndWriterImplementations() {
        final ServiceApi readerApi = new ServiceApi("org.example.ReaderApi", BUNDLE);
        final ServiceApi writerApi = new ServiceApi("org.example.WriterApi", BUNDLE);
        final ProcessorCapability processor = processor("org.example.ConvertRecord", Map.of(
                "reader", property("reader", "Record Reader", true, null, readerApi),
                "writer", property("writer", "Record Writer", true, null, writerApi)));
        final Map<String, ControllerServiceCapability> services = Map.of(
                "org.example.CSVReader", service("org.example.CSVReader", readerApi),
                "org.example.JsonTreeReader", service("org.example.JsonTreeReader", readerApi),
                "org.example.AvroReader", service("org.example.AvroReader", readerApi),
                "org.example.JsonRecordSetWriter",
                        service("org.example.JsonRecordSetWriter", writerApi),
                "org.example.AvroRecordSetWriter",
                        service("org.example.AvroRecordSetWriter", writerApi));

        final String rendered = new CapabilityPromptRenderer().render(
                "Convert CSV records to JSON",
                new CapabilitySnapshot(
                        Map.of(processor.type(), processor), services, Instant.now()));

        assertTrue(rendered.contains(
                "implementations=[org.example.CSVReader]"));
        assertTrue(rendered.contains(
                "implementations=[org.example.JsonRecordSetWriter]"));
    }

    @Test
    void evictsRuntimeAndOptionalPropertiesBeforeRequiredCapabilityData() {
        final Map<String, PropertyCapability> properties = new LinkedHashMap<>();
        properties.put("required-url", property(
                "required-url", "Required URL", true, null, null));
        properties.put("schema", property(
                "schema", "Schema Access Strategy", false, null, null));
        for (int index = 0; index < 20; index++) {
            properties.put(
                    "optional-" + index,
                    property("optional-" + index, "Optional " + index, false, null, null));
        }
        properties.put("yield-duration", property(
                "yield-duration", "Yield Duration", false, null, null));
        final CapabilitySnapshot snapshot = new CapabilitySnapshot(
                Map.of(
                        "org.example.InvokeHTTP",
                        processor("org.example.InvokeHTTP", properties)),
                Map.of(),
                Instant.now());
        final CapabilityPromptRenderer renderer = new CapabilityPromptRenderer();
        final CapabilityGraph graph = CapabilityGraph.from(snapshot);

        final String rendered = renderer.render("send to HTTP", graph, 750);

        assertTrue(rendered.length() <= 750);
        assertTrue(rendered.contains("PROCESSOR org.example.InvokeHTTP"));
        assertTrue(rendered.contains("internal=required-url"));
        assertTrue(rendered.contains("PROCESSOR_RELATIONSHIPS org.example.InvokeHTTP"));
        assertFalse(rendered.contains("internal=yield-duration"));
        assertFalse(rendered.contains("internal=optional-19"));
    }

    @Test
    void evictsVerboseRequiredPropertyDetailsWithoutDroppingRequiredProperties() {
        final Map<String, PropertyCapability> properties = new LinkedHashMap<>();
        final List<AllowableValue> allowableValues = java.util.stream.IntStream.range(0, 12)
                .mapToObj(index -> new AllowableValue(
                        "internal-" + index + "-" + "x".repeat(120),
                        "Display " + index + " " + "y".repeat(120)))
                .toList();
        for (int index = 0; index < 15; index++) {
            properties.put(
                    "required-" + index,
                    new PropertyCapability(
                            "required-" + index,
                            "Required " + index,
                            true,
                            null,
                            false,
                            false,
                            allowableValues,
                            List.of(),
                            null));
        }
        final CapabilityGraph graph = CapabilityGraph.from(new CapabilitySnapshot(
                Map.of(
                        "org.example.LogMessage",
                        processor("org.example.LogMessage", properties)),
                Map.of(),
                Instant.now()));

        final String rendered =
                new CapabilityPromptRenderer().render("Log failures", graph, 4_000);

        assertTrue(rendered.length() <= 4_000);
        assertTrue(rendered.contains("internal=required-0"));
        assertTrue(rendered.contains("internal=required-14"));
        assertFalse(rendered.contains("allowable="));
    }

    @Test
    void reducesLowestRankedSeedsWhenRequiredFactsExceedBudget() {
        final Map<String, ProcessorCapability> processors = new LinkedHashMap<>();
        for (int processorIndex = 0; processorIndex < 10; processorIndex++) {
            final Map<String, PropertyCapability> properties = new LinkedHashMap<>();
            for (int propertyIndex = 0; propertyIndex < 30; propertyIndex++) {
                final String name = "required-" + propertyIndex;
                properties.put(name, property(name, "Required " + propertyIndex, true, null, null));
            }
            final String type = "org.example.LogProcessor" + processorIndex;
            processors.put(type, processor(type, properties));
        }
        final CapabilityGraph graph = CapabilityGraph.from(new CapabilitySnapshot(
                processors, Map.of(), Instant.now()));

        final String rendered =
                new CapabilityPromptRenderer().render("Log failures", graph, 5_000);
        final int selectedProcessors =
                rendered.split("PROCESSOR org\\.example\\.LogProcessor", -1).length - 1;

        assertTrue(rendered.length() <= 5_000);
        assertTrue(selectedProcessors > 0);
        assertTrue(selectedProcessors < 10);
    }

    private ProcessorCapability processor(
            final String type,
            final Map<String, PropertyCapability> properties) {
        return new ProcessorCapability(
                type,
                BUNDLE,
                properties,
                false,
                Set.of("success"),
                false,
                "INPUT_ALLOWED",
                Set.of("TIMER_DRIVEN"),
                false);
    }

    private ControllerServiceCapability service(
            final String type,
            final ServiceApi api) {
        return new ControllerServiceCapability(
                type, BUNDLE, Map.of(), false, Set.of(api));
    }

    private PropertyCapability property(
            final String name,
            final String displayName,
            final boolean required,
            final String defaultValue,
            final ServiceApi api) {
        return new PropertyCapability(
                name,
                displayName,
                required,
                defaultValue,
                false,
                false,
                List.of(),
                List.of(),
                api);
    }
}
