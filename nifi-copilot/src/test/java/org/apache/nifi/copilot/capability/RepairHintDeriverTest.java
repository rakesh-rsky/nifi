package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.SchedulingConstraints;
import org.junit.jupiter.api.Test;

class RepairHintDeriverTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("g", "a", "1");
    private static final ServiceApi READER_API =
            new ServiceApi("org.example.RecordReaderFactory", null);

    private final RepairHintDeriver deriver = new RepairHintDeriver();

    @Test
    void derivesCompatibleImplementationsFromTypedRequiredApi() {
        final CapabilityGraph graph = graph();
        final ValidationIssue issue = issue(
                ValidationIssueType.INCOMPATIBLE_CONTROLLER_SERVICE,
                "org.example.ConvertRecord",
                "config.reader",
                "wrong-service",
                READER_API);

        final RepairHint hint = deriver.derive(issue, graph);

        assertEquals(READER_API, hint.requiredApi());
        assertEquals(
                List.of("org.example.CSVReader", "org.example.JsonTreeReader"),
                hint.compatibleImplementationTypes());
        assertEquals(List.of("Record Reader", "reader"), hint.suggestedPropertyNames());
    }

    @Test
    void derivesPropertiesAndRelationshipsFromCapabilityTypeAndPath() {
        final CapabilityGraph graph = graph();
        final RepairHint missing = deriver.derive(
                issue(
                        ValidationIssueType.MISSING_REQUIRED_PROPERTY,
                        "org.example.ConvertRecord",
                        "config.reader",
                        "",
                        READER_API),
                graph);
        final RepairHint relationship = deriver.derive(
                issue(
                        ValidationIssueType.UNSUPPORTED_RELATIONSHIP,
                        "org.example.ConvertRecord",
                        "connections[0].relationships",
                        "success",
                        null),
                graph);

        assertEquals(List.of("Record Reader", "reader"), missing.suggestedPropertyNames());
        assertEquals(Set.of("Failure", "Original"), relationship.supportedRelationships());
    }

    @Test
    void resolvesUnknownApiLikeServiceTypesWithoutParsingReasonText() {
        final ValidationIssue issue = new ValidationIssue(
                "cs1",
                "type",
                "unrelated human text",
                "unrelated fix",
                ValidationIssueType.UNKNOWN_CONTROLLER_SERVICE_TYPE,
                "",
                "org.example.RecordReaderFactory",
                null);

        final RepairHint hint = deriver.derive(issue, graph());

        assertEquals(READER_API, hint.requiredApi());
        assertEquals(
                List.of("org.example.CSVReader", "org.example.JsonTreeReader"),
                hint.compatibleImplementationTypes());
    }

    @Test
    void doesNotInferExpansionFromFreeFormMessages() {
        final ValidationIssue issue = new ValidationIssue(
                "cs1",
                "type",
                "RecordReaderFactory missing; use CSVReader",
                "inject compatible services");

        final RepairHint hint = deriver.derive(issue, graph());

        assertEquals(ValidationIssueType.OTHER, hint.issueType());
        assertNull(hint.requiredApi());
        assertTrue(hint.compatibleImplementationTypes().isEmpty());
    }

    private ValidationIssue issue(
            final ValidationIssueType type,
            final String capabilityType,
            final String path,
            final String rejected,
            final ServiceApi requiredApi) {
        return new ValidationIssue(
                "component",
                path,
                "ignored",
                "ignored",
                type,
                capabilityType,
                rejected,
                requiredApi);
    }

    private CapabilityGraph graph() {
        final PropertyNode reader = new PropertyNode(
                "reader",
                "Record Reader",
                true,
                null,
                false,
                false,
                List.of(),
                List.of(),
                READER_API);
        final ProcessorNode processor = new ProcessorNode(
                "org.example.ConvertRecord",
                BUNDLE,
                Map.of(reader.name(), reader),
                false,
                Set.of(READER_API),
                Set.of(),
                Set.of("Original", "Failure"),
                false,
                new SchedulingConstraints(null, Set.of(), false));
        return new CapabilityGraph(
                List.of(processor),
                List.of(
                        service("org.example.CSVReader"),
                        service("org.example.JsonTreeReader")));
    }

    private ControllerServiceNode service(final String type) {
        return new ControllerServiceNode(
                type,
                BUNDLE,
                Map.of(),
                false,
                Set.of(READER_API),
                Set.of(),
                Set.of());
    }
}
