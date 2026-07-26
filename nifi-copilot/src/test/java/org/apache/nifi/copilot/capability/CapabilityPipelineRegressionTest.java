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

class CapabilityPipelineRegressionTest {
    private static final BundleCoordinate BUNDLE = new BundleCoordinate("g", "a", "1");
    private static final ServiceApi READER_API =
            new ServiceApi("org.apache.nifi.serialization.RecordReaderFactory", null);
    private static final ServiceApi WRITER_API =
            new ServiceApi("org.apache.nifi.serialization.RecordSetWriterFactory", null);
    private static final ServiceApi DBCP_API =
            new ServiceApi("org.apache.nifi.dbcp.DBCPService", null);

    @Test
    void rendersCompleteCsvPostgresqlCapabilityClosureWithoutInventedServices() {
        final Map<String, ProcessorCapability> processors = new LinkedHashMap<>();
        processors.put("org.example.ListFile", processor("org.example.ListFile", Map.of()));
        processors.put("org.example.FetchFile", processor("org.example.FetchFile", Map.of()));
        processors.put("org.example.ConvertRecord", processor(
                "org.example.ConvertRecord",
                Map.of(
                        "reader", property("reader", "Record Reader", true, READER_API),
                        "writer", property("writer", "Record Writer", true, WRITER_API))));
        processors.put("org.example.ValidateRecord", processor(
                "org.example.ValidateRecord",
                Map.of("reader", property(
                        "reader", "Record Reader", true, READER_API))));
        processors.put("org.example.PutDatabaseRecord", processor(
                "org.example.PutDatabaseRecord",
                Map.of(
                        "reader", property("reader", "Record Reader", true, READER_API),
                        "pool", property(
                                "pool", "Database Connection Pooling Service", true, DBCP_API))));
        processors.put("org.example.MoveFile", processor("org.example.MoveFile", Map.of()));
        processors.put("org.example.LogMessage", processor("org.example.LogMessage", Map.of()));

        final Map<String, ControllerServiceCapability> services = Map.of(
                "org.apache.nifi.csv.CSVReader",
                service("org.apache.nifi.csv.CSVReader", READER_API),
                "org.apache.nifi.json.JsonRecordSetWriter",
                service("org.apache.nifi.json.JsonRecordSetWriter", WRITER_API),
                "org.apache.nifi.dbcp.DBCPConnectionPool",
                service("org.apache.nifi.dbcp.DBCPConnectionPool", DBCP_API),
                "org.example.UnrelatedService",
                service(
                        "org.example.UnrelatedService",
                        new ServiceApi("org.example.UnrelatedApi", null)));
        final CapabilitySnapshot snapshot = new CapabilitySnapshot(
                processors, services, Instant.parse("2026-01-01T00:00:00Z"));
        final CapabilityPromptRenderer renderer = new CapabilityPromptRenderer();
        final String prompt = "Read CSV files from a local directory, convert them to JSON, "
                + "validate each record, and write the data to PostgreSQL. "
                + "Archive processed files and log failures separately.";

        final String first = renderer.render(prompt, snapshot);
        final String second = renderer.render(prompt, snapshot);

        assertEquals(first, second);
        assertTrue(first.length() <= CapabilityPromptRenderer.MAX_CONTEXT_CHARS);
        for (String processor : List.of(
                "ListFile",
                "FetchFile",
                "ConvertRecord",
                "ValidateRecord",
                "PutDatabaseRecord",
                "MoveFile",
                "LogMessage")) {
            assertTrue(first.contains("PROCESSOR org.example." + processor), processor);
        }
        assertTrue(first.contains("CONTROLLER_SERVICE org.apache.nifi.csv.CSVReader"));
        assertTrue(first.contains(
                "CONTROLLER_SERVICE org.apache.nifi.json.JsonRecordSetWriter"));
        assertTrue(first.contains(
                "CONTROLLER_SERVICE org.apache.nifi.dbcp.DBCPConnectionPool"));
        assertFalse(first.contains(
                "CONTROLLER_SERVICE org.apache.nifi.dbcp.DBCPService"));
        assertFalse(first.contains("org.example.UnrelatedService"));
    }

    private ProcessorCapability processor(
            final String type,
            final Map<String, PropertyCapability> properties) {
        return new ProcessorCapability(
                type,
                BUNDLE,
                properties,
                false,
                Set.of("success", "failure"),
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
            final ServiceApi api) {
        return new PropertyCapability(
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
