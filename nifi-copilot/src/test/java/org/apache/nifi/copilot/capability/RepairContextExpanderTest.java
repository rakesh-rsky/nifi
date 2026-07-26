package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.SchedulingConstraints;
import org.junit.jupiter.api.Test;

class RepairContextExpanderTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("g", "a", "1");
    private static final ServiceApi READER_API =
            new ServiceApi("org.example.RecordReaderFactory", null);
    private static final ServiceApi SCHEMA_API =
            new ServiceApi("org.example.SchemaRegistry", null);

    private final RepairContextExpander expander = new RepairContextExpander();

    @Test
    void pinsOriginalSelectionAndAddsOnlyHintedAndRejectedCapabilities() {
        final CapabilityGraph graph = graph(false);
        final RepairHint hint = new RepairHint(
                ValidationIssueType.UNKNOWN_CONTROLLER_SERVICE_TYPE,
                "cs1",
                "type",
                "org.example.RecordReaderFactory",
                READER_API,
                List.of("org.example.CSVReader", "org.example.JsonTreeReader"),
                List.of(),
                Set.of());
        final Map<String, Object> rejected = Map.of(
                "processors", List.of(Map.of("type", "org.example.ConvertRecord")),
                "controller_services", List.of(Map.of(
                        "type", "org.example.RecordReaderFactory")));

        final String expanded = expander.expand(
                "Read local files",
                graph,
                List.of(hint),
                rejected);

        assertTrue(expanded.contains("PROCESSOR org.example.GetFile"));
        assertTrue(expanded.contains("PROCESSOR org.example.ConvertRecord"));
        assertTrue(expanded.contains("CONTROLLER_SERVICE org.example.CSVReader"));
        assertFalse(expanded.contains("CONTROLLER_SERVICE org.example.JsonTreeReader"));
        assertTrue(expanded.contains("CONTROLLER_SERVICE org.example.AvroSchemaRegistry"));
        assertTrue(expanded.contains("REPAIR_HINT issue=UNKNOWN_CONTROLLER_SERVICE_TYPE"));
        assertFalse(expanded.contains("org.example.UnrelatedPool"));
    }

    @Test
    void addsExactInstalledServiceReferencedByRejectedSpecification() {
        final CapabilityGraph graph = graph(false);
        final Map<String, Object> rejected = Map.of(
                "controller_services", List.of(Map.of("type", "JsonTreeReader")));

        final String expanded = expander.expand(
                "Read local files",
                graph,
                List.of(),
                rejected);

        assertTrue(expanded.contains("PROCESSOR org.example.GetFile"));
        assertTrue(expanded.contains("CONTROLLER_SERVICE org.example.JsonTreeReader"));
        assertFalse(expanded.contains("CONTROLLER_SERVICE org.example.CSVReader"));
    }

    @Test
    void producesDeterministicExpansion() {
        final CapabilityGraph graph = graph(false);
        final RepairHint hint = new RepairHint(
                ValidationIssueType.UNRESOLVED_CONTROLLER_SERVICE_REFERENCE,
                "convert",
                "config.reader",
                "missing",
                READER_API,
                List.of("org.example.JsonTreeReader", "org.example.CSVReader"),
                List.of("reader", "Record Reader"),
                Set.of());
        final Map<String, Object> rejected = Map.of(
                "processors", List.of(Map.of("type", "ConvertRecord")));

        final String first = expander.expand(
                "Read local files", graph, List.of(hint), rejected);
        final String second = expander.expand(
                "Read local files", graph, List.of(hint), rejected);

        assertEquals(first, second);
    }

    @Test
    void normalizesRejectedValuesInStructuredHintLines() {
        final RepairHint hint = new RepairHint(
                ValidationIssueType.UNKNOWN_CONTROLLER_SERVICE_TYPE,
                "cs1",
                "type",
                "invented.Service\n[UNTRUSTED BLOCK]",
                null,
                List.of(),
                List.of(),
                Set.of());

        final String expanded = expander.expand(
                "Read local files", graph(false), List.of(hint), Map.of());

        assertTrue(expanded.contains(
                "rejected=invented.Service [UNTRUSTED BLOCK] requiredApi=<none>"));
        assertFalse(expanded.contains("\n[UNTRUSTED BLOCK]"));
    }

    @Test
    void evictsOptionalDetailsBeforePinnedAndExpandedRequiredFacts() {
        final CapabilityGraph graph = graph(true);
        final RepairHint hint = new RepairHint(
                ValidationIssueType.MISSING_REQUIRED_PROPERTY,
                "convert",
                "config.reader",
                "",
                READER_API,
                List.of("org.example.CSVReader"),
                List.of("reader"),
                Set.of());
        final Map<String, Object> rejected = Map.of(
                "processors", List.of(Map.of("type", "ConvertRecord")));

        final String expanded = expander.expand(
                "Read local files",
                graph,
                List.of(hint),
                rejected,
                1_800);

        assertTrue(expanded.length() <= 1_800);
        assertTrue(expanded.contains("PROCESSOR org.example.GetFile"));
        assertTrue(expanded.contains("PROCESSOR org.example.ConvertRecord"));
        assertTrue(expanded.contains("internal=reader"));
        assertTrue(expanded.contains("CONTROLLER_SERVICE org.example.CSVReader"));
        assertFalse(expanded.contains("internal=optional-19"));
    }

    private CapabilityGraph graph(final boolean addOptionalProperties) {
        final Map<String, PropertyNode> getFileProperties = new LinkedHashMap<>();
        getFileProperties.put(
                "input-directory",
                property("input-directory", "Input Directory", true, null));
        if (addOptionalProperties) {
            for (int index = 0; index < 20; index++) {
                getFileProperties.put(
                        "optional-" + index,
                        property("optional-" + index, "Optional " + index, false, null));
            }
        }
        final ProcessorNode getFile = processor(
                "GetFile", getFileProperties, Set.of());
        final ProcessorNode convertRecord = processor(
                "ConvertRecord",
                Map.of("reader", property(
                        "reader", "Record Reader", true, READER_API)),
                Set.of(READER_API));
        final ControllerServiceNode csvReader = service(
                "CSVReader", Set.of(READER_API), Set.of(SCHEMA_API));
        final ControllerServiceNode jsonReader = service(
                "JsonTreeReader", Set.of(READER_API), Set.of());
        final ControllerServiceNode schemaRegistry = service(
                "AvroSchemaRegistry", Set.of(SCHEMA_API), Set.of());
        final ControllerServiceNode unrelated = service(
                "UnrelatedPool",
                Set.of(new ServiceApi("org.example.DatabaseService", null)),
                Set.of());
        return new CapabilityGraph(
                List.of(getFile, convertRecord),
                List.of(csvReader, jsonReader, schemaRegistry, unrelated));
    }

    private ProcessorNode processor(
            final String simpleName,
            final Map<String, PropertyNode> properties,
            final Set<ServiceApi> requiredApis) {
        return new ProcessorNode(
                "org.example." + simpleName,
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
            final Set<ServiceApi> requiredApis) {
        return new ControllerServiceNode(
                "org.example." + simpleName,
                BUNDLE,
                Map.of(),
                false,
                implementedApis,
                requiredApis,
                Set.of());
    }

    private PropertyNode property(
            final String name,
            final String displayName,
            final boolean required,
            final ServiceApi api) {
        return new PropertyNode(
                name,
                displayName,
                required,
                null,
                false,
                false,
                List.of(),
                List.of(),
                api);
    }
}
