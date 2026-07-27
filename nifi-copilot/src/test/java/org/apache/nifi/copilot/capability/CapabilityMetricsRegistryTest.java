package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.SchedulingConstraints;
import org.junit.jupiter.api.Test;

class CapabilityMetricsRegistryTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("g", "a", "1");
    private static final ServiceApi READER_API =
            new ServiceApi("org.example.RecordReaderFactory", null);

    @Test
    void recordsSelectionRepairValidationAndTokenAggregatesWithoutPromptContent() {
        final CapabilityMetricsRegistry metrics = new CapabilityMetricsRegistry();
        final PropertyRanker propertyRanker = new PropertyRanker();
        final CapabilityPromptRenderer renderer = new CapabilityPromptRenderer(
                new IntentExtractor(),
                new ProcessorSeedRanker(),
                new DependencyClosureResolver(propertyRanker),
                metrics);
        final CapabilityGraph graph = graph();
        final String prompt = "Read local files secret-value-123";

        final String context = renderer.renderFromGraph(prompt, graph);
        final ValidationIssue issue = new ValidationIssue(
                "convert",
                "config.reader",
                "missing",
                "set reader",
                ValidationIssueType.MISSING_REQUIRED_PROPERTY,
                "org.example.ConvertRecord",
                "",
                READER_API);
        metrics.observeFirstPass(false, List.of(issue));
        metrics.observeRepairAttempt();
        metrics.observeRepairTokens(17, 9);
        metrics.observeRepairResult(true, List.of());

        final CapabilityMetricsRegistry.Snapshot snapshot = metrics.snapshot();

        assertEquals(1, snapshot.selectionCount());
        assertEquals(1, snapshot.seedProcessorCount());
        assertEquals((long) context.length(), snapshot.promptCharacters());
        assertEquals(1L, snapshot.intentCategories().get("SOURCE_LOCAL_FILE"));
        assertEquals(1L, snapshot.seedProcessorTypes().get("org.example.GetFile"));
        assertEquals(1L, snapshot.validationIssueTypes().get("MISSING_REQUIRED_PROPERTY"));
        assertEquals(1, snapshot.firstPassInvalid());
        assertEquals(1, snapshot.repairAttempts());
        assertEquals(1, snapshot.repairSuccesses());
        assertEquals(17, snapshot.repairInputTokens());
        assertEquals(9, snapshot.repairOutputTokens());
        assertFalse(snapshot.toString().contains("secret-value-123"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> snapshot.intentCategories().put("PROMPT", 1L));
    }

    @Test
    void recordsSuccessfulFirstPassSeparately() {
        final CapabilityMetricsRegistry metrics = new CapabilityMetricsRegistry();

        metrics.observeFirstPass(true, List.of());

        assertEquals(1, metrics.snapshot().firstPassValid());
        assertEquals(0, metrics.snapshot().repairAttempts());
        assertTrue(metrics.snapshot().validationIssueTypes().isEmpty());
    }

    private CapabilityGraph graph() {
        final ProcessorNode getFile = processor(
                "GetFile",
                Map.of("directory", property("directory", "Input Directory", true, null)),
                Set.of());
        final ProcessorNode convertRecord = processor(
                "ConvertRecord",
                Map.of("reader", property("reader", "Record Reader", true, READER_API)),
                Set.of(READER_API));
        final ControllerServiceNode csvReader = new ControllerServiceNode(
                "org.example.CSVReader",
                BUNDLE,
                Map.of(),
                false,
                Set.of(READER_API),
                Set.of(),
                Set.of());
        return new CapabilityGraph(List.of(getFile, convertRecord), List.of(csvReader));
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
