package org.apache.nifi.copilot.llm;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LlmClientTest {

    @Test
    void capabilityContextIsAdditiveSystemGuidanceForBothProviderPaths() {
        final LlmClient client = new LlmClient();

        final String prompt = client.systemPrompt("[TARGET NIFI CAPABILITIES]\nPROCESSOR exact.Type");

        assertTrue(prompt.contains("PROCESSOR exact.Type"));
        assertTrue(prompt.contains("Never invent processor types"));
        assertEquals(client.systemPrompt(""), client.systemPrompt(null));
    }

    // -------------------------------------------------------------------------
    // Canvas-context message builder
    // -------------------------------------------------------------------------

    @Test
    void includesExistingConnectionsInCanvasContext() {
        final String message = new LlmClient().buildUserMessage(
                "Count processors and connections",
                List.of(Map.of("spec_id", "processor-1", "name", "Generate", "type", "GenerateFlowFile")),
                List.of(),
                List.of(),
                List.of(Map.of(
                        "from", "processor-1",
                        "to", "processor-2",
                        "relationships", List.of("success"))));

        assertTrue(message.contains("Connections on canvas:"));
        assertTrue(message.contains("from=processor-1 to=processor-2 relationships=[success]"));
    }

    // -------------------------------------------------------------------------
    // Updated: success and failure loggers must NOT be merged into one
    // -------------------------------------------------------------------------

    /**
     * Three parallel InvokeHTTP workers each send both a success (response) and a failure
     * relationship to two separate generated loggers.  The loggers serve different semantic
     * outcomes and must remain distinct.  The unintended self-loop on success-logger is removed;
     * the explicit allow_self_loop on retry is preserved.
     */
    @Test
    void retainsSeparateOutcomeLoggersForParallelWorkers() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "id", "success-logger",
                        "type", "org.apache.nifi.processors.standard.LogAttribute")),
                new LinkedHashMap<>(Map.of(
                        "id", "failure-logger",
                        "type", "org.apache.nifi.processors.standard.LogMessage"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                // unintended self-loop — must be removed
                new LinkedHashMap<>(Map.of("from", "success-logger", "to", "success-logger",
                        "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "worker-1", "to", "success-logger",
                        "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "worker-1", "to", "failure-logger",
                        "relationships", List.of("failure", "no retry", "retry"))),
                new LinkedHashMap<>(Map.of("from", "worker-2", "to", "success-logger",
                        "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "worker-2", "to", "failure-logger",
                        "relationships", List.of("failure", "no retry", "retry"))),
                new LinkedHashMap<>(Map.of("from", "worker-3", "to", "success-logger",
                        "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "worker-3", "to", "failure-logger",
                        "relationships", List.of("failure", "no retry", "retry"))),
                // explicit self-loop — must be kept
                new LinkedHashMap<>(Map.of("from", "retry", "to", "retry",
                        "relationships", List.of("retry"), "allow_self_loop", true)),
                new LinkedHashMap<>(Map.of("from", "source", "to", "target",
                        "relationships", List.of("success")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        // Both loggers remain — they serve different outcomes
        assertEquals(2, processors.size());
        // 9 input connections – 1 removed self-loop = 8
        assertEquals(8, connections.size());
        // The invalid self-loop on success-logger must be gone
        assertFalse(connections.stream().anyMatch(c ->
                "success-logger".equals(c.get("from")) && "success-logger".equals(c.get("to"))));
        // The explicit self-loop on retry must still be present
        assertTrue(connections.stream().anyMatch(c ->
                "retry".equals(c.get("from")) && "retry".equals(c.get("to"))));
    }

    // -------------------------------------------------------------------------
    // Existing: single-worker separate success / failure branches preserved
    // -------------------------------------------------------------------------

    @Test
    void preservesSeparateLoggersForSingleWorkerBranches() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "success-log", "type", "logattribute")),
                new LinkedHashMap<>(Map.of("id", "failure-log", "type", "logmessage"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "http", "to", "success-log",
                        "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "http", "to", "failure-log",
                        "relationships", List.of("failure", "no retry")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);

        assertEquals(2, ((List<?>) normalized.get("processors")).size());
        assertEquals(2, ((List<?>) normalized.get("connections")).size());
    }

    @Test
    void expandsCombinedLoadRelationshipsIntoDistinctParallelWorkers() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "id", "distribute",
                        "name", "Distribute Load",
                        "type", "org.apache.nifi.processors.standard.DistributeLoad")),
                new LinkedHashMap<>(Map.of(
                        "id", "invoke",
                        "name", "Invoke HTTP API",
                        "type", "org.apache.nifi.processors.standard.InvokeHTTP",
                        "config", Map.of("HTTP URL", "http://localhost:8080/measurement/devices"))),
                new LinkedHashMap<>(Map.of(
                        "id", "success-log",
                        "name", "Log Success",
                        "type", "org.apache.nifi.processors.standard.LogMessage",
                        "preserve_separate_terminal", true)),
                new LinkedHashMap<>(Map.of(
                        "id", "failure-log",
                        "name", "Log Failure",
                        "type", "org.apache.nifi.processors.standard.LogMessage",
                        "preserve_separate_terminal", true)))));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "from", "distribute",
                        "to", "invoke",
                        "relationships", List.of("1", "2", "3", "4", "5"))),
                new LinkedHashMap<>(Map.of(
                        "from", "invoke",
                        "to", "success-log",
                        "relationships", List.of("Response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "invoke",
                        "to", "failure-log",
                        "relationships", List.of("Failure", "No Retry", "Retry"))))));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final Set<String> workerIds = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).endsWith("InvokeHTTP"))
                .map(p -> String.valueOf(p.get("id")))
                .collect(Collectors.toSet());
        assertEquals(5, workerIds.size());
        assertEquals(5, connections.stream()
                .filter(c -> "distribute".equals(c.get("from")))
                .filter(c -> workerIds.contains(String.valueOf(c.get("to"))))
                .count());
        assertTrue(connections.stream()
                .filter(c -> "distribute".equals(c.get("from")))
                .allMatch(c -> ((List<?>) c.get("relationships")).size() == 1));
        assertEquals(5, connections.stream()
                .filter(c -> workerIds.contains(String.valueOf(c.get("from"))))
                .filter(c -> "success-log".equals(c.get("to")))
                .count());
        assertEquals(5, connections.stream()
                .filter(c -> workerIds.contains(String.valueOf(c.get("from"))))
                .filter(c -> "failure-log".equals(c.get("to")))
                .count());
    }

    // -------------------------------------------------------------------------
    // Sequential EvaluateJsonPath → InvokeHTTP sharing a failure logger → split
    // -------------------------------------------------------------------------

    @Test
    void splitsLoggerSharedBySequentialPipelineStages() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "extract", "name", "Extract", "type", "EvaluateJsonPath")),
                new LinkedHashMap<>(Map.of("id", "update", "name", "Build URL", "type", "UpdateAttribute")),
                new LinkedHashMap<>(Map.of("id", "invoke", "name", "Invoke API", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "failure-log", "name", "Log Failure", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "extract", "to", "update",
                        "relationships", List.of("matched"))),
                new LinkedHashMap<>(Map.of("from", "update", "to", "invoke",
                        "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "extract", "to", "failure-log",
                        "relationships", List.of("failure", "unmatched"))),
                new LinkedHashMap<>(Map.of("from", "invoke", "to", "failure-log",
                        "relationships", List.of("failure", "no retry")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        final long failureDestinations = connections.stream()
                .filter(c -> List.of("extract", "invoke").contains(c.get("from")))
                .filter(c -> String.valueOf(c.get("to")).startsWith("failure-log"))
                .map(c -> c.get("to"))
                .distinct()
                .count();

        assertEquals(2, loggerCount);
        assertEquals(2, failureDestinations);
    }

    // -------------------------------------------------------------------------
    // Mixed topology: earlier stage + 3 parallel workers → failure logger split
    // -------------------------------------------------------------------------

    /**
     * An early-stage processor feeds directly into the failure logger AND routes through a
     * pipe into 3 parallel workers that also feed the failure logger.  Because early-stage is
     * sequential with (ancestor of) the workers, the logger must be split: the workers share one
     * clone and the early stage gets the other.
     */
    @Test
    void mixedTopologyEarlierStagePlusParallelWorkersGetSeparateLoggers() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "early", "name", "Early Stage", "type", "EvaluateJsonPath")),
                new LinkedHashMap<>(Map.of("id", "pipe", "name", "Pipe", "type", "UpdateAttribute")),
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w3", "name", "Worker 3", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "flog", "name", "Log Failures", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "early", "to", "pipe", "relationships", List.of("matched"))),
                new LinkedHashMap<>(Map.of("from", "pipe", "to", "w1", "relationships", List.of("1"))),
                new LinkedHashMap<>(Map.of("from", "pipe", "to", "w2", "relationships", List.of("2"))),
                new LinkedHashMap<>(Map.of("from", "pipe", "to", "w3", "relationships", List.of("3"))),
                new LinkedHashMap<>(Map.of("from", "early", "to", "flog", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "w1", "to", "flog", "relationships", List.of("failure", "no retry"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "flog", "relationships", List.of("failure", "no retry"))),
                new LinkedHashMap<>(Map.of("from", "w3", "to", "flog", "relationships", List.of("failure", "no retry")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        assertEquals(2, loggerCount, "early stage and workers must receive separate loggers");

        // All 3 workers must converge on a single (shared) logger clone
        final Set<String> workerDestinations = connections.stream()
                .filter(c -> Set.of("w1", "w2", "w3").contains(c.get("from")))
                .map(c -> String.valueOf(c.get("to")))
                .collect(Collectors.toSet());
        assertEquals(1, workerDestinations.size(), "parallel workers must share one logger");

        // The early stage must use a different logger than the workers
        final String earlyDest = connections.stream()
                .filter(c -> "early".equals(c.get("from")) && String.valueOf(c.get("to")).startsWith("flog"))
                .map(c -> String.valueOf(c.get("to")))
                .findFirst()
                .orElse(null);
        assertNotEquals(earlyDest, workerDestinations.iterator().next(),
                "early-stage logger must differ from worker logger");
    }

    // -------------------------------------------------------------------------
    // Parallel workers keep their shared logger (no split)
    // -------------------------------------------------------------------------

    @Test
    void parallelWorkersRetainSharedLogger() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "dist", "name", "Distributor", "type", "DistributeLoad")),
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w3", "name", "Worker 3", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "flog", "name", "Log Failure", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w1", "relationships", List.of("1"))),
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w2", "relationships", List.of("2"))),
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w3", "relationships", List.of("3"))),
                new LinkedHashMap<>(Map.of("from", "w1", "to", "flog", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "flog", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "w3", "to", "flog", "relationships", List.of("failure")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        assertEquals(1, loggerCount, "parallel workers must share one logger — no split");

        final Set<String> workerDests = connections.stream()
                .filter(c -> Set.of("w1", "w2", "w3").contains(c.get("from")))
                .map(c -> String.valueOf(c.get("to")))
                .collect(Collectors.toSet());
        assertEquals(1, workerDests.size());
        assertEquals("flog", workerDests.iterator().next());
    }

    // -------------------------------------------------------------------------
    // Model generated one-logger-per-worker for same outcome → consolidate
    // -------------------------------------------------------------------------

    @Test
    void consolidatesOneLoggerPerWorkerWithinSuccessOutcome() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "dist", "type", "DistributeLoad")),
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w3", "name", "Worker 3", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-s1", "name", "Log Success 1", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-s2", "name", "Log Success 2", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-s3", "name", "Log Success 3", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w1", "relationships", List.of("1"))),
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w2", "relationships", List.of("2"))),
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w3", "relationships", List.of("3"))),
                new LinkedHashMap<>(Map.of("from", "w1", "to", "log-s1", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "log-s2", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w3", "to", "log-s3", "relationships", List.of("response")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        assertEquals(1, loggerCount, "one-per-worker success loggers must be consolidated");

        final Map<String, Object> sharedLogger = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .findFirst()
                .orElseThrow();
        // Fix 4: the kept logger retains its original name; "Log API Results" is never written
        assertEquals("Log Success 1", sharedLogger.get("name"));

        // All 3 workers must connect to the same single logger
        final Set<String> dests = connections.stream()
                .filter(c -> Set.of("w1", "w2", "w3").contains(c.get("from")))
                .map(c -> String.valueOf(c.get("to")))
                .collect(Collectors.toSet());
        assertEquals(1, dests.size());
        assertEquals(6, connections.size());
    }

    @Test
    void consolidatesSeparatelyForSuccessAndFailureOutcomes() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "dist", "type", "DistributeLoad")),
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-s1", "name", "Log Success 1", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-s2", "name", "Log Success 2", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-f1", "name", "Log Failure 1", "type", "LogMessage")),
                new LinkedHashMap<>(Map.of("id", "log-f2", "name", "Log Failure 2", "type", "LogMessage"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w1", "relationships", List.of("1"))),
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w2", "relationships", List.of("2"))),
                new LinkedHashMap<>(Map.of("from", "w1", "to", "log-s1", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "log-s2", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w1", "to", "log-f1", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "log-f2", "relationships", List.of("failure")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        // distributor + 2 workers + 1 success logger + 1 failure logger = 5
        assertEquals(5, processors.size());
        assertEquals(6, connections.size());

        // Success and failure destinations must remain distinct
        final Set<String> successDests = connections.stream()
                .filter(c -> ((List<?>) c.get("relationships")).contains("response"))
                .map(c -> String.valueOf(c.get("to")))
                .collect(Collectors.toSet());
        final Set<String> failureDests = connections.stream()
                .filter(c -> ((List<?>) c.get("relationships")).contains("failure"))
                .map(c -> String.valueOf(c.get("to")))
                .collect(Collectors.toSet());
        assertEquals(1, successDests.size(), "success loggers consolidated to one");
        assertEquals(1, failureDests.size(), "failure loggers consolidated to one");
        assertNotEquals(successDests.iterator().next(), failureDests.iterator().next(),
                "success and failure sinks must not be the same logger");
    }

    // -------------------------------------------------------------------------
    // Explicit preserve_separate_terminal: no consolidation, no split
    // -------------------------------------------------------------------------

    @Test
    void explicitPreserveSeparateTerminalPreventsConsolidation() {
        // Two parallel workers, each with their own success logger, both flagged to stay separate
        final Map<String, Object> specification = new LinkedHashMap<>();
        final Map<String, Object> logA = new LinkedHashMap<>();
        logA.put("id", "log-a");
        logA.put("type", "LogAttribute");
        logA.put("preserve_separate_terminal", true);
        final Map<String, Object> logB = new LinkedHashMap<>();
        logB.put("id", "log-b");
        logB.put("type", "LogAttribute");
        logB.put("preserve_separate_terminal", true);
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                logA, logB
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "w1", "to", "log-a", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "log-b", "relationships", List.of("response")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        // preserve_separate_terminal must prevent consolidation
        assertEquals(2, loggerCount);
    }

    // -------------------------------------------------------------------------
    // Canvas logger absent from generated processors remains untouched
    // -------------------------------------------------------------------------

    @Test
    void canvasLoggerAbsentFromGeneratedProcessorsRemainsUntouched() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "proc-1", "name", "Fetch", "type", "InvokeHTTP"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "proc-1", "to", "canvas-logger",
                        "relationships", List.of("failure")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        assertEquals(1, processors.size());
        assertEquals(1, connections.size());
        assertEquals("canvas-logger", connections.get(0).get("to"));
    }

    // -------------------------------------------------------------------------
    // Stable source-derived IDs
    // -------------------------------------------------------------------------

    @Test
    void stableSourceDerivedIdsAreConsistentAcrossRuns() {
        // Two independent normalizations of the same input must produce identical clone IDs
        final Map<String, Object> normalized1 =
                new LlmClient().normalizeGeneratedLayout(buildSequentialSpec());
        final Map<String, Object> normalized2 =
                new LlmClient().normalizeGeneratedLayout(buildSequentialSpec());

        final List<String> ids1 = cast(normalized1.get("processors")).stream()
                .map(p -> String.valueOf(p.get("id")))
                .sorted()
                .collect(Collectors.toList());
        final List<String> ids2 = cast(normalized2.get("processors")).stream()
                .map(p -> String.valueOf(p.get("id")))
                .sorted()
                .collect(Collectors.toList());

        assertEquals(ids1, ids2, "clone IDs must be identical across independent normalizations");

        // The clone ID must be derived from the source processor ID (not positional)
        assertTrue(ids1.stream().anyMatch(id -> id.contains("invoke")),
                "clone ID must embed the source processor ID");
    }

    // -------------------------------------------------------------------------
    // Idempotent repeated normalization
    // -------------------------------------------------------------------------

    @Test
    void repeatedNormalizationIsIdempotent() {
        final Map<String, Object> firstResult =
                new LlmClient().normalizeGeneratedLayout(buildSequentialSpec());

        // Re-run normalization on a fresh copy of the already-normalized spec
        final Map<String, Object> secondInput = new LinkedHashMap<>();
        secondInput.put("processors", new ArrayList<>(cast(firstResult.get("processors"))
                .stream()
                .map(p -> new LinkedHashMap<>(p))
                .collect(Collectors.toList())));
        secondInput.put("connections", new ArrayList<>(cast(firstResult.get("connections"))
                .stream()
                .map(c -> new LinkedHashMap<>(c))
                .collect(Collectors.toList())));

        final Map<String, Object> secondResult =
                new LlmClient().normalizeGeneratedLayout(secondInput);

        final List<String> ids1 = cast(firstResult.get("processors")).stream()
                .map(p -> String.valueOf(p.get("id"))).sorted().collect(Collectors.toList());
        final List<String> ids2 = cast(secondResult.get("processors")).stream()
                .map(p -> String.valueOf(p.get("id"))).sorted().collect(Collectors.toList());

        final List<String> conns1 = cast(firstResult.get("connections")).stream()
                .map(c -> c.get("from") + "->" + c.get("to")).sorted().collect(Collectors.toList());
        final List<String> conns2 = cast(secondResult.get("connections")).stream()
                .map(c -> c.get("from") + "->" + c.get("to")).sorted().collect(Collectors.toList());

        assertEquals(ids1, ids2, "processor IDs must be stable across repeated normalizations");
        assertEquals(conns1, conns2, "connections must be stable across repeated normalizations");
    }

    // -------------------------------------------------------------------------
    // Nested processor configuration is preserved in clones
    // -------------------------------------------------------------------------

    @Test
    void preservesNestedProcessorConfigInClones() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        final Map<String, Object> loggerConfig = new LinkedHashMap<>();
        loggerConfig.put("Log Prefix", "FAILURE: ");
        loggerConfig.put("Log Level", "WARN");
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "extract", "name", "Extract", "type", "EvaluateJsonPath")),
                new LinkedHashMap<>(Map.of("id", "update", "name", "Build URL", "type", "UpdateAttribute")),
                new LinkedHashMap<>(Map.of("id", "invoke", "name", "Invoke API", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "flog", "name", "Log Failure", "type", "LogAttribute",
                        "config", loggerConfig))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "extract", "to", "update", "relationships", List.of("matched"))),
                new LinkedHashMap<>(Map.of("from", "update", "to", "invoke", "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "extract", "to", "flog", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "invoke", "to", "flog", "relationships", List.of("failure", "no retry")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));

        // The clone (for invoke) must carry the same config as the original
        final List<Map<String, Object>> loggers = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .collect(Collectors.toList());
        assertEquals(2, loggers.size());
        for (final Map<String, Object> logger : loggers) {
            final Map<?, ?> config = (Map<?, ?>) logger.get("config");
            assertEquals("FAILURE: ", config.get("Log Prefix"),
                    "config must be preserved in all logger clones");
            assertEquals("WARN", config.get("Log Level"),
                    "config must be preserved in all logger clones");
        }
    }

    // -------------------------------------------------------------------------
    // Invalid self-loops and terminal outgoing edges are excluded
    // -------------------------------------------------------------------------

    @Test
    void invalidSelfLoopsAndTerminalOutgoingEdgesAreExcluded() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "proc-1", "name", "Step", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "slog", "name", "Log Success", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "proc-1", "to", "slog", "relationships", List.of("response"))),
                // unintended self-loop on the logger
                new LinkedHashMap<>(Map.of("from", "slog", "to", "slog", "relationships", List.of("success"))),
                // terminal outgoing edge (no allow_terminal_output flag)
                new LinkedHashMap<>(Map.of("from", "slog", "to", "proc-1", "relationships", List.of("success")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        // Only the valid proc-1 → slog connection must survive
        assertEquals(1, connections.size());
        assertEquals("proc-1", connections.get(0).get("from"));
        assertEquals("slog", connections.get(0).get("to"));
    }

    // -------------------------------------------------------------------------
    // No accidental self-loops or funnels introduced by normalization
    // -------------------------------------------------------------------------

    @Test
    void normalizationDoesNotIntroduceSelfLoopsOrFunnels() {
        final Map<String, Object> normalized =
                new LlmClient().normalizeGeneratedLayout(buildSequentialSpec());
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        // No processor with type "funnel" or similar must appear
        assertFalse(processors.stream()
                .anyMatch(p -> String.valueOf(p.get("type")).toLowerCase().contains("funnel")),
                "normalization must not create funnel processors");

        // No connection with from == to and no explicit allow_self_loop
        assertFalse(connections.stream()
                .anyMatch(c -> c.get("from").equals(c.get("to"))
                        && !Boolean.TRUE.equals(c.get("allow_self_loop"))),
                "normalization must not create unintended self-loops");
    }

    // -------------------------------------------------------------------------
    // Fix 1: Disconnected equal-rank sources must NOT be parallel siblings
    // -------------------------------------------------------------------------

    /**
     * Reproducer: a→log, b→log with no generated path between a and b.
     * a and b are in different weakly-connected components of the clean graph, so they must
     * NOT be treated as parallel siblings — the logger must be split into two.
     */
    @Test
    void disconnectedSourcesAreNotParallelSiblings() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "a", "name", "Source A", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "b", "name", "Source B", "type", "PutFile")),
                new LinkedHashMap<>(Map.of("id", "log", "name", "Shared Log", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "a", "to", "log", "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "b", "to", "log", "relationships", List.of("success")))
                // NOTE: no connection between a and b — they are in different WCC components
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        assertEquals(2, loggerCount,
                "disconnected equal-rank sources must not share a logger — each needs its own");

        final List<Map<String, Object>> connections = cast(normalized.get("connections"));
        final Set<String> loggerTargets = connections.stream()
                .filter(c -> "a".equals(c.get("from")) || "b".equals(c.get("from")))
                .map(c -> String.valueOf(c.get("to")))
                .collect(Collectors.toSet());
        assertEquals(2, loggerTargets.size(), "a and b must route to distinct loggers");
    }

    // -------------------------------------------------------------------------
    // Fix 2: Cycle-aware rank computation
    // -------------------------------------------------------------------------

    /**
     * Reproducer: c1↔c2 cycle, c1→short, c1→mid→deep, short→log, deep→log.
     * Without SCC condensation both short and deep get rank 0 and appear parallel.
     * After condensation: short=rank 1, deep=rank 2 — different depths, so the logger is split.
     */
    @Test
    void cycleAwareRankSplitsShortAndDeepBranches() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "c1", "name", "Cycle 1", "type", "RouteOnAttribute")),
                new LinkedHashMap<>(Map.of("id", "c2", "name", "Cycle 2", "type", "UpdateAttribute")),
                new LinkedHashMap<>(Map.of("id", "short", "name", "Short Branch", "type", "PutFile")),
                new LinkedHashMap<>(Map.of("id", "mid",   "name", "Middle",       "type", "UpdateAttribute")),
                new LinkedHashMap<>(Map.of("id", "deep",  "name", "Deep Branch",  "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log",   "name", "End Log",      "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "c1",    "to", "c2",    "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "c2",    "to", "c1",    "relationships", List.of("retry"))),
                new LinkedHashMap<>(Map.of("from", "c1",    "to", "short", "relationships", List.of("matched"))),
                new LinkedHashMap<>(Map.of("from", "c1",    "to", "mid",   "relationships", List.of("unmatched"))),
                new LinkedHashMap<>(Map.of("from", "mid",   "to", "deep",  "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "short", "to", "log",   "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "deep",  "to", "log",   "relationships", List.of("success")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        assertEquals(2, loggerCount, "short and deep are at different depths after SCC condensation — must split");

        final String shortDest = connections.stream()
                .filter(c -> "short".equals(c.get("from")))
                .map(c -> String.valueOf(c.get("to")))
                .findFirst().orElse(null);
        final String deepDest = connections.stream()
                .filter(c -> "deep".equals(c.get("from")))
                .map(c -> String.valueOf(c.get("to")))
                .findFirst().orElse(null);
        assertNotEquals(shortDest, deepDest, "short and deep must route to separate loggers");
    }

    // -------------------------------------------------------------------------
    // Fix 3: Outcome detection ignores invalid self-loops on loggers
    // -------------------------------------------------------------------------

    /**
     * Reproducer: one logger has an invalid failure self-loop; the other does not.
     * Both receive only "response" from their respective workers.
     * After Fix 3, the self-loop is ignored during outcome detection, both are classified
     * SUCCESS, and they are consolidated into a single logger.
     */
    @Test
    void selfLoopOnLoggerIgnoredForOutcomeDetection() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "dist", "type", "DistributeLoad")),
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-a", "name", "Log A", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-b", "name", "Log B", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w1", "relationships", List.of("1"))),
                new LinkedHashMap<>(Map.of("from", "dist", "to", "w2", "relationships", List.of("2"))),
                new LinkedHashMap<>(Map.of("from", "w1",    "to", "log-a", "relationships", List.of("response"))),
                // invalid failure self-loop on log-a — must not change outcome to MIXED
                new LinkedHashMap<>(Map.of("from", "log-a", "to", "log-a", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "w2",    "to", "log-b", "relationships", List.of("response")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));

        final long loggerCount = processors.stream()
                .filter(p -> String.valueOf(p.get("type")).toLowerCase().endsWith("logattribute"))
                .count();
        assertEquals(1, loggerCount,
                "the self-loop must be ignored for outcome detection; both loggers are SUCCESS and must consolidate");
    }

    // -------------------------------------------------------------------------
    // Fix 4: Only semantically equivalent loggers are consolidated
    // -------------------------------------------------------------------------

    /** LogAttribute and LogMessage have different types and must never be consolidated. */
    @Test
    void differentProcessorTypesAreNotConsolidated() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-a", "name", "Log A", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-b", "name", "Log B", "type", "LogMessage"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "w1", "to", "log-a", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "log-b", "relationships", List.of("response")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));

        final long logAttributeCount = processors.stream()
                .filter(p -> normalizedType(p).equals("logattribute")).count();
        final long logMessageCount = processors.stream()
                .filter(p -> normalizedType(p).equals("logmessage")).count();
        assertEquals(1, logAttributeCount, "LogAttribute logger must remain");
        assertEquals(1, logMessageCount,   "LogMessage logger must remain");
    }

    /** Loggers with different behavior configs must not be consolidated even if same type/outcome. */
    @Test
    void differentConfigsAreNotConsolidated() {
        final Map<String, Object> logAConfig = new LinkedHashMap<>();
        logAConfig.put("Log Level", "INFO");
        final Map<String, Object> logBConfig = new LinkedHashMap<>();
        logBConfig.put("Log Level", "WARN");

        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "w1", "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-a", "name", "Log A", "type", "LogAttribute", "config", logAConfig)),
                new LinkedHashMap<>(Map.of("id", "log-b", "name", "Log B", "type", "LogAttribute", "config", logBConfig))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "w1", "to", "log-a", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w2", "to", "log-b", "relationships", List.of("response")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));

        final long loggerCount = processors.stream()
                .filter(p -> normalizedType(p).equals("logattribute")).count();
        assertEquals(2, loggerCount, "loggers with different configs (INFO vs WARN) must not be consolidated");
    }

    // -------------------------------------------------------------------------
    // Fix 5: Split logger names are independent across groups
    // -------------------------------------------------------------------------

    /**
     * Reproducer: three sequential sources A→B→C all connecting to the same logger.
     * The logger is split into 3.  Each clone's name must be derived from the original base
     * name, never from a previously renamed version (no name accumulation).
     */
    @Test
    void splitNamesAreIndependentAcrossGroups() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "src-a", "name", "Generator",   "type", "GenerateFlowFile")),
                new LinkedHashMap<>(Map.of("id", "src-b", "name", "Transformer", "type", "UpdateAttribute")),
                new LinkedHashMap<>(Map.of("id", "src-c", "name", "Invoker",     "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log",   "name", "Log Results", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "src-a", "to", "src-b", "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "src-b", "to", "src-c", "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "src-a", "to", "log",   "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "src-b", "to", "log",   "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of("from", "src-c", "to", "log",   "relationships", List.of("failure")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));

        final List<String> loggerNames = processors.stream()
                .filter(p -> normalizedType(p).equals("logattribute"))
                .map(p -> String.valueOf(p.get("name")))
                .collect(Collectors.toList());

        assertEquals(3, loggerNames.size(), "each sequential source must receive its own logger");

        // Every name must be exactly "<base> - <sourceName>" — no accumulated prefixes like
        // "Log Results - Generator - Transformer"
        for (final String name : loggerNames) {
            final long dashCount = name.chars().filter(c -> c == '-').count();
            // Each name has exactly one " - " separator (base + " - " + sourceName)
            assertTrue(name.startsWith("Log Results - "),
                    "name must start with original base: " + name);
            // There must be no second " - " that would indicate an accumulated prefix
            assertFalse(name.substring("Log Results - ".length()).contains(" - "),
                    "name must not contain an accumulated prior-group prefix: " + name);
        }
    }

    // -------------------------------------------------------------------------
    // Fix 6: External/canvas incoming connection protects logger from removal
    // -------------------------------------------------------------------------

    /**
     * Reproducer: two parallel workers generate equivalent loggers, but log-b is also targeted
     * by a canvas (external) processor.  log-b must NOT be removed; both loggers must survive.
     */
    @Test
    void externalIncomingConnectionProtectsLoggerFromConsolidation() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "w1",    "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2",    "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-a", "name", "Log A",    "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-b", "name", "Log B",    "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "w1",          "to", "log-a", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w2",          "to", "log-b", "relationships", List.of("response"))),
                // canvas-node is NOT in the generated processor list — this is an external reference
                new LinkedHashMap<>(Map.of("from", "canvas-node", "to", "log-b", "relationships", List.of("success")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final long loggerCount = processors.stream()
                .filter(p -> normalizedType(p).equals("logattribute")).count();
        assertEquals(2, loggerCount,
                "log-b has an external incoming connection and must not be removed during consolidation");

        // The external connection to log-b must still be present unchanged
        assertTrue(connections.stream().anyMatch(c ->
                "canvas-node".equals(c.get("from")) && "log-b".equals(c.get("to"))),
                "external canvas-node→log-b connection must remain intact");
    }

    // -------------------------------------------------------------------------
    // Fix 7: Outgoing connections from a removed terminal logger are cleaned up
    // -------------------------------------------------------------------------

    /**
     * Reproducer: log-b is consolidated into log-a and removed.  log-b also has an invalid
     * outgoing connection log-b→tail.  After normalization no connection from log-b must remain
     * (dangling source reference would be created without Fix 7).
     */
    @Test
    void outgoingConnectionFromRemovedLoggerIsCleanedUp() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "dist",  "type", "DistributeLoad")),
                new LinkedHashMap<>(Map.of("id", "w1",   "name", "Worker 1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2",   "name", "Worker 2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-a","name", "Log A",    "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-b","name", "Log B",    "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "tail", "name", "Tail",     "type", "PutFile"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "dist",  "to", "w1",    "relationships", List.of("1"))),
                new LinkedHashMap<>(Map.of("from", "dist",  "to", "w2",    "relationships", List.of("2"))),
                new LinkedHashMap<>(Map.of("from", "w1",    "to", "log-a", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of("from", "w2",    "to", "log-b", "relationships", List.of("response"))),
                // invalid outgoing from a terminal logger — would dangle if log-b is removed
                new LinkedHashMap<>(Map.of("from", "log-b", "to", "tail",  "relationships", List.of("success")))
        )));

        final Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        final long loggerCount = processors.stream()
                .filter(p -> normalizedType(p).equals("logattribute")).count();
        assertEquals(1, loggerCount, "log-b must be consolidated into log-a");

        // No connection with source log-b must survive
        assertFalse(connections.stream().anyMatch(c -> "log-b".equals(c.get("from"))),
                "all connections from the removed logger log-b must be cleaned up");
    }

    // -------------------------------------------------------------------------
    // Fix 8: Stable IDs are connection-order independent
    // -------------------------------------------------------------------------

    /**
     * Reversing the connection list must produce the exact same processor IDs and connection
     * topology as the original order.
     */
    @Test
    void stableIdsAreConnectionOrderIndependent() {
        final Map<String, Object> spec1 = buildSequentialSpec();
        final Map<String, Object> spec2 = buildSequentialSpec();
        // Reverse the connections list in spec2 to exercise order-independence
        final List<Map<String, Object>> conns2 = cast(spec2.get("connections"));
        Collections.reverse(conns2);

        final Map<String, Object> normalized1 = new LlmClient().normalizeGeneratedLayout(spec1);
        final Map<String, Object> normalized2 = new LlmClient().normalizeGeneratedLayout(spec2);

        final List<String> ids1 = cast(normalized1.get("processors")).stream()
                .map(p -> String.valueOf(p.get("id"))).sorted().collect(Collectors.toList());
        final List<String> ids2 = cast(normalized2.get("processors")).stream()
                .map(p -> String.valueOf(p.get("id"))).sorted().collect(Collectors.toList());

        assertEquals(ids1, ids2, "clone IDs must be identical regardless of connection-list order");

        final List<String> edges1 = cast(normalized1.get("connections")).stream()
                .map(c -> c.get("from") + "->" + c.get("to")).sorted().collect(Collectors.toList());
        final List<String> edges2 = cast(normalized2.get("connections")).stream()
                .map(c -> c.get("from") + "->" + c.get("to")).sorted().collect(Collectors.toList());

        assertEquals(edges1, edges2, "connection topology must be identical regardless of input order");
    }

    @Test
    void disconnectedSplitRemainsStableAcrossRepeatedNormalization() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "a", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "b", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of(
                        "id", "log", "name", "Log Failure", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "log", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of(
                        "from", "b", "to", "log", "relationships", List.of("failure")))
        )));

        final LlmClient client = new LlmClient();
        client.normalizeGeneratedLayout(specification);
        client.normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(specification.get("processors"));
        final List<Map<String, Object>> connections = cast(specification.get("connections"));

        assertEquals(2, processors.stream()
                .filter(processor -> normalizedType(processor).equals("logattribute"))
                .count());
        assertEquals(2, connections.stream()
                .map(connection -> String.valueOf(connection.get("to")))
                .distinct()
                .count());
    }

    @Test
    void disconnectedPerSourceLoggersAreNotConsolidated() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "a", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "b", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-a", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-b", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "log-a", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "b", "to", "log-b", "relationships", List.of("response")))
        )));

        final Map<String, Object> normalized =
                new LlmClient().normalizeGeneratedLayout(specification);

        assertEquals(2, cast(normalized.get("processors")).stream()
                .filter(processor -> normalizedType(processor).equals("logattribute"))
                .count());
    }

    @Test
    void allowedTerminalOutputPreventsLoggerRemoval() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "root", "type", "DistributeLoad")),
                new LinkedHashMap<>(Map.of("id", "w1", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "w2", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log-a", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-b", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "tail", "type", "PutFile"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "from", "root", "to", "w1", "relationships", List.of("1"))),
                new LinkedHashMap<>(Map.of(
                        "from", "root", "to", "w2", "relationships", List.of("2"))),
                new LinkedHashMap<>(Map.of(
                        "from", "w1", "to", "log-a", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "w2", "to", "log-b", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "log-b", "to", "tail", "relationships", List.of("success"),
                        "allow_terminal_output", true))
        )));

        final Map<String, Object> normalized =
                new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        assertEquals(2, processors.stream()
                .filter(processor -> normalizedType(processor).equals("logattribute"))
                .count());
        assertTrue(connections.stream().anyMatch(connection ->
                "log-b".equals(connection.get("from"))
                        && "tail".equals(connection.get("to"))
                        && Boolean.TRUE.equals(connection.get("allow_terminal_output"))));
    }

    @Test
    void commonDescendantDoesNotMakeIndependentRootsSiblings() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "a", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "b", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "join", "type", "MergeContent")),
                new LinkedHashMap<>(Map.of("id", "log", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "join", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "b", "to", "join", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "log", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of(
                        "from", "b", "to", "log", "relationships", List.of("failure")))
        )));

        final Map<String, Object> normalized =
                new LlmClient().normalizeGeneratedLayout(specification);

        assertEquals(2, cast(normalized.get("processors")).stream()
                .filter(processor -> normalizedType(processor).equals("logattribute"))
                .count());
    }

    @Test
    void commonDescendantDoesNotConsolidateIndependentRootLoggers() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "a", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "b", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "join", "type", "MergeContent")),
                new LinkedHashMap<>(Map.of("id", "log-a", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "log-b", "type", "LogAttribute"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "join", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "b", "to", "join", "relationships", List.of("response"))),
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "log-a", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of(
                        "from", "b", "to", "log-b", "relationships", List.of("failure")))
        )));

        final LlmClient client = new LlmClient();
        client.normalizeGeneratedLayout(specification);
        client.normalizeGeneratedLayout(specification);

        assertEquals(2, cast(specification.get("processors")).stream()
                .filter(processor -> normalizedType(processor).equals("logattribute"))
                .count());
    }

    @Test
    void loggerWithAllowedOutputIsNotPartitioned() {
        final Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "a", "type", "EvaluateJsonPath")),
                new LinkedHashMap<>(Map.of("id", "b", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "log", "type", "LogAttribute")),
                new LinkedHashMap<>(Map.of("id", "tail", "type", "PutFile"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "b", "relationships", List.of("matched"))),
                new LinkedHashMap<>(Map.of(
                        "from", "a", "to", "log", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of(
                        "from", "b", "to", "log", "relationships", List.of("failure"))),
                new LinkedHashMap<>(Map.of(
                        "from", "log", "to", "tail", "relationships", List.of("success"),
                        "allow_terminal_output", true))
        )));

        final Map<String, Object> normalized =
                new LlmClient().normalizeGeneratedLayout(specification);
        final List<Map<String, Object>> processors = cast(normalized.get("processors"));
        final List<Map<String, Object>> connections = cast(normalized.get("connections"));

        assertEquals(1, processors.stream()
                .filter(processor -> normalizedType(processor).equals("logattribute"))
                .count());
        assertTrue(connections.stream().anyMatch(connection ->
                "log".equals(connection.get("from"))
                        && "tail".equals(connection.get("to"))));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private <T> List<Map<String, T>> cast(final Object value) {
        return (List<Map<String, T>>) value;
    }
    /** Returns the short, lower-cased processor type (strips package prefix). */
    private String normalizedType(final Map<String, Object> proc) {
        final String type = String.valueOf(proc.getOrDefault("type", "")).toLowerCase();
        final int dot = type.lastIndexOf('.');
        return dot >= 0 ? type.substring(dot + 1) : type;
    }

    /** Builds the sequential EvaluateJsonPath → UpdateAttribute → InvokeHTTP failure spec. */
    private Map<String, Object> buildSequentialSpec() {
        final Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("id", "extract", "name", "Extract", "type", "EvaluateJsonPath")),
                new LinkedHashMap<>(Map.of("id", "update", "name", "Build URL", "type", "UpdateAttribute")),
                new LinkedHashMap<>(Map.of("id", "invoke", "name", "Invoke API", "type", "InvokeHTTP")),
                new LinkedHashMap<>(Map.of("id", "failure-log", "name", "Log Failure", "type", "LogAttribute"))
        )));
        spec.put("connections", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of("from", "extract", "to", "update", "relationships", List.of("matched"))),
                new LinkedHashMap<>(Map.of("from", "update", "to", "invoke", "relationships", List.of("success"))),
                new LinkedHashMap<>(Map.of("from", "extract", "to", "failure-log", "relationships", List.of("failure", "unmatched"))),
                new LinkedHashMap<>(Map.of("from", "invoke", "to", "failure-log", "relationships", List.of("failure", "no retry")))
        )));
        return spec;
    }
}
