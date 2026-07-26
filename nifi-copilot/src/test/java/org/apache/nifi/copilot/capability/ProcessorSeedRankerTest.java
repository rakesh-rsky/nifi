package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.SchedulingConstraints;
import org.junit.jupiter.api.Test;

class ProcessorSeedRankerTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("group", "artifact", "1");
    private final IntentExtractor intentExtractor = new IntentExtractor();
    private final ProcessorSeedRanker ranker = new ProcessorSeedRanker();

    @Test
    void ranksCompleteCsvDatabaseWorkflowSignalsWithoutSelectingUnrelatedProcessors() {
        final CapabilityGraph graph = graph(
                "ListFile",
                "FetchFile",
                "ConvertRecord",
                "ValidateRecord",
                "PutDatabaseRecord",
                "MoveFile",
                "LogMessage",
                "ConsumeMQTT",
                "InvokeHTTP");
        final WorkflowIntent intent = intentExtractor.extract(
                "Read CSV files from a local directory, convert them to JSON, validate each record, "
                        + "and write the data to PostgreSQL. Archive processed files and log failures separately.");

        final List<String> rankedTypes = ranker.rank(graph, intent).stream()
                .map(ranked -> simpleName(ranked.processor()))
                .toList();

        assertTrue(rankedTypes.containsAll(List.of(
                "ListFile",
                "FetchFile",
                "ConvertRecord",
                "ValidateRecord",
                "PutDatabaseRecord",
                "MoveFile",
                "LogMessage")));
        assertTrue(!rankedTypes.contains("ConsumeMQTT"));
        assertTrue(!rankedTypes.contains("InvokeHTTP"));
    }

    @Test
    void usesEndpointRoleToPreferDatabaseSourceProcessors() {
        final CapabilityGraph graph = graph(
                "PutDatabaseRecord",
                "QueryDatabaseTable",
                "ExecuteSQL",
                "ConvertRecord");
        final WorkflowIntent intent = intentExtractor.extract("Read records from PostgreSQL");

        final List<ProcessorSeedRanker.RankedProcessor> ranked = ranker.rank(graph, intent);

        assertTrue(score(ranked, "QueryDatabaseTable") > score(ranked, "PutDatabaseRecord"));
        assertTrue(score(ranked, "ExecuteSQL") > score(ranked, "PutDatabaseRecord"));
    }

    @Test
    void ranksMqttHttpBatchLoggingAndParallelismProcessorsFromInstalledGraph() {
        final CapabilityGraph graph = graph(
                "ConsumeMQTT",
                "MergeRecord",
                "DistributeLoad",
                "InvokeHTTP",
                "LogAttribute",
                "PutDatabaseRecord");
        final WorkflowIntent intent = intentExtractor.extract(
                "Read MQTT JSON, batch 1000 messages, send to HTTP with 5 parallel calls, "
                        + "and log success and failure.");

        final List<String> rankedTypes = ranker.rank(graph, intent).stream()
                .map(ranked -> simpleName(ranked.processor()))
                .toList();

        assertTrue(rankedTypes.containsAll(List.of(
                "ConsumeMQTT", "MergeRecord", "DistributeLoad", "InvokeHTTP", "LogAttribute")));
        assertTrue(!rankedTypes.contains("PutDatabaseRecord"));
    }

    @Test
    void appliesLimitAndDeterministicTypeTieBreaking() {
        final CapabilityGraph graph = graph(
                "LogZulu",
                "LogAlpha",
                "LogMike");
        final WorkflowIntent intent = intentExtractor.extract("Log failures");

        final List<String> first = ranker.rank(graph, intent, 2).stream()
                .map(ranked -> ranked.processor().type())
                .toList();
        final List<String> second = ranker.rank(graph, intent, 2).stream()
                .map(ranked -> ranked.processor().type())
                .toList();

        assertEquals(List.of("example.LogAlpha", "example.LogMike"), first);
        assertEquals(first, second);
    }

    @Test
    void returnsNoSeedsForBlankIntent() {
        assertTrue(ranker.rank(graph("ConsumeMQTT"), intentExtractor.extract(null)).isEmpty());
    }

    @Test
    void doesNotTreatEmbeddedTextAsAWorkflowSignal() {
        final List<String> rankedTypes = ranker.rank(
                        graph("MoveFile", "RemoveRecordField"),
                        intentExtractor.extract("Archive processed data"))
                .stream()
                .map(ranked -> simpleName(ranked.processor()))
                .toList();

        assertEquals(List.of("MoveFile"), rankedTypes);
    }

    private int score(
            final List<ProcessorSeedRanker.RankedProcessor> ranked,
            final String simpleName) {
        return ranked.stream()
                .filter(candidate -> simpleName(candidate.processor()).equals(simpleName))
                .findFirst()
                .map(ProcessorSeedRanker.RankedProcessor::score)
                .orElse(0);
    }

    private CapabilityGraph graph(final String... simpleNames) {
        final List<ProcessorNode> processors = new ArrayList<>();
        for (String simpleName : simpleNames) {
            processors.add(new ProcessorNode(
                    "example." + simpleName,
                    BUNDLE,
                    Map.of(),
                    false,
                    Set.of(),
                    Set.of(),
                    Set.of("success"),
                    false,
                    new SchedulingConstraints(null, Set.of("TIMER_DRIVEN"), false)));
        }
        return new CapabilityGraph(processors, List.of());
    }

    private String simpleName(final ProcessorNode processor) {
        return processor.type().substring(processor.type().lastIndexOf('.') + 1);
    }
}
