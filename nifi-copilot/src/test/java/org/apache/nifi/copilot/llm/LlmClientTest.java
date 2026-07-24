package org.apache.nifi.copilot.llm;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LlmClientTest {

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

    @Test
    void consolidatesParallelResultLoggersAndMergesWorkerRelationships() {
        Map<String, Object> specification = new LinkedHashMap<>();
        specification.put("processors", new ArrayList<>(List.of(
                new LinkedHashMap<>(Map.of(
                        "id", "success-logger",
                        "type", "org.apache.nifi.processors.standard.LogAttribute")),
                new LinkedHashMap<>(Map.of(
                        "id", "failure-logger",
                        "type", "org.apache.nifi.processors.standard.LogMessage"))
        )));
        specification.put("connections", new ArrayList<>(List.of(
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
                new LinkedHashMap<>(Map.of("from", "retry", "to", "retry",
                        "relationships", List.of("retry"), "allow_self_loop", true)),
                new LinkedHashMap<>(Map.of("from", "source", "to", "target",
                        "relationships", List.of("success")))
        )));

        Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);
        List<?> processors = (List<?>) normalized.get("processors");
        List<?> connections = (List<?>) normalized.get("connections");

        assertEquals(1, processors.size());
        assertEquals("Log API Results", ((Map<?, ?>) processors.getFirst()).get("name"));
        assertEquals(5, connections.size());
        assertEquals(List.of("response", "failure", "no retry", "retry"),
                ((Map<?, ?>) connections.get(0)).get("relationships"));
        assertEquals("success-logger", ((Map<?, ?>) connections.get(1)).get("to"));
    }

    @Test
    void preservesSeparateLoggersForSingleWorkerBranches() {
        Map<String, Object> specification = new LinkedHashMap<>();
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

        Map<String, Object> normalized = new LlmClient().normalizeGeneratedLayout(specification);

        assertEquals(2, ((List<?>) normalized.get("processors")).size());
        assertEquals(2, ((List<?>) normalized.get("connections")).size());
    }

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
        final List<Map<String, Object>> processors =
                (List<Map<String, Object>>) normalized.get("processors");
        final List<Map<String, Object>> connections =
                (List<Map<String, Object>>) normalized.get("connections");
        final long loggerCount = processors.stream()
                .filter(processor -> String.valueOf(processor.get("type"))
                        .toLowerCase().endsWith("logattribute"))
                .count();
        final long failureDestinations = connections.stream()
                .filter(connection -> List.of("extract", "invoke").contains(connection.get("from")))
                .filter(connection -> String.valueOf(connection.get("to")).startsWith("failure-log"))
                .map(connection -> connection.get("to"))
                .distinct()
                .count();

        assertEquals(2, loggerCount);
        assertEquals(2, failureDestinations);
    }
}
