package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.nifi.copilot.capability.WorkflowIntent.DataFormat;
import org.apache.nifi.copilot.capability.WorkflowIntent.EndpointKind;
import org.apache.nifi.copilot.capability.WorkflowIntent.LogOutcome;
import org.apache.nifi.copilot.capability.WorkflowIntent.TransformationKind;
import org.junit.jupiter.api.Test;

class IntentExtractorTest {
    private final IntentExtractor extractor = new IntentExtractor();

    @Test
    void extractsMqttHttpBatchingLoggingAndParallelism() {
        final WorkflowIntent intent = extractor.extract(
                "Read from MQTT and send the JSON to http://localhost:8080/measurement/devices. "
                        + "Maximum 1000 messages can be sent as an array in one API call, "
                        + "log success and failure separately, and use 5 parallel API calls.");

        assertEquals(Set.of(EndpointKind.MQTT), intent.sources());
        assertEquals(Set.of(EndpointKind.HTTP), intent.sinks());
        assertEquals(Set.of(DataFormat.JSON), intent.formats());
        assertEquals(Set.of(TransformationKind.MERGE), intent.transformations());
        assertTrue(intent.batching());
        assertEquals(1000, intent.maximumBatchSize().orElseThrow());
        assertEquals(Set.of(LogOutcome.SUCCESS, LogOutcome.FAILURE), intent.logging());
        assertEquals(5, intent.parallelism().orElseThrow());
    }

    @Test
    void extractsCsvFileToPostgresqlWorkflow() {
        final WorkflowIntent intent = extractor.extract(
                "Read CSV files from a local directory, convert them to JSON, validate each record, "
                        + "and write the data to PostgreSQL. Archive processed files and log failures separately.");

        assertEquals(Set.of(EndpointKind.LOCAL_FILE), intent.sources());
        assertEquals(Set.of(EndpointKind.DATABASE), intent.sinks());
        assertEquals(Set.of(DataFormat.CSV, DataFormat.JSON), intent.formats());
        assertEquals(
                Set.of(TransformationKind.CONVERT, TransformationKind.VALIDATE),
                intent.transformations());
        assertTrue(intent.archive());
        assertEquals(Set.of(LogOutcome.FAILURE), intent.logging());
        assertFalse(intent.batching());
        assertTrue(intent.maximumBatchSize().isEmpty());
        assertTrue(intent.parallelism().isEmpty());
    }

    @Test
    void extractsDirectionalUseOfTheSameEndpointKind() {
        final WorkflowIntent intent = extractor.extract(
                "Query records from PostgreSQL, filter them, then insert the result into MySQL.");

        assertEquals(Set.of(EndpointKind.DATABASE), intent.sources());
        assertEquals(Set.of(EndpointKind.DATABASE), intent.sinks());
        assertEquals(Set.of(TransformationKind.FILTER), intent.transformations());
    }

    @Test
    void returnsEmptyImmutableIntentForBlankPrompt() {
        final WorkflowIntent intent = extractor.extract(null);

        assertTrue(intent.terms().isEmpty());
        assertTrue(intent.sources().isEmpty());
        assertTrue(intent.sinks().isEmpty());
        assertTrue(intent.formats().isEmpty());
        assertTrue(intent.transformations().isEmpty());
        assertTrue(intent.logging().isEmpty());
        assertFalse(intent.batching());
        assertFalse(intent.archive());
    }
}
