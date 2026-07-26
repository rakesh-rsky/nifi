package org.apache.nifi.copilot.builder;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.Test;

class ProcessorDeployerSchedulingTest {
    @Test
    void appliesSchedulingAfterCreate() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.createProcessor(anyString(), anyString(), anyString(), anyDouble(), anyDouble(), any()))
                .thenReturn(Map.of("id", "created"));

        deploy(List.of(processor("p1", Map.of(
                "schedulingStrategy", "TIMER_DRIVEN",
                "schedulingPeriod", "5 sec",
                "concurrentlySchedulableTaskCount", 2))), nifi, new ComponentRegistry());

        verify(nifi).updateProcessor("created", Map.of("config", Map.of(
                "schedulingStrategy", "TIMER_DRIVEN",
                "schedulingPeriod", "5 sec",
                "concurrentlySchedulableTaskCount", 2)));
    }

    @Test
    void combinesPropertiesAndSchedulingOnUpdate() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        final ComponentRegistry components = new ComponentRegistry();
        components.register("p1", "existing", "PROCESSOR");
        when(nifi.getProcessor("existing")).thenReturn(Map.of("component", Map.of("name", "old")));
        final Map<String, Object> spec = processor("p1", Map.of("schedulingStrategy", "CRON_DRIVEN"));
        spec.put("config", Map.of("url", "https://example.test"));

        deploy(List.of(spec), nifi, components);

        verify(nifi).updateProcessor(eq("existing"), org.mockito.ArgumentMatchers.argThat(update -> {
            final Map<?, ?> config = (Map<?, ?>) update.get("config");
            return "CRON_DRIVEN".equals(config.get("schedulingStrategy"))
                    && Map.of("url", "https://example.test").equals(config.get("properties"));
        }));
    }

    @Test
    void stopsOnFirstUnexpectedProcessorMutationError() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.createProcessor(anyString(), anyString(), anyString(), anyDouble(), anyDouble(), any()))
                .thenThrow(new RuntimeException("failed"));

        assertThrows(IllegalStateException.class, () -> deploy(
                List.of(processor("p1", Map.of()), processor("p2", Map.of())),
                nifi, new ComponentRegistry()));

        verify(nifi, never()).updateProcessor(anyString(), any());
        verify(nifi).createProcessor(anyString(), anyString(), anyString(), anyDouble(), anyDouble(), any());
    }

    private void deploy(
            final List<Map<String, Object>> processors,
            final NiFiClientOperations nifi,
            final ComponentRegistry components) {
        ProcessorDeployer.deploy(
                processors, "root", new ControllerServiceDeployer(), new CanvasPositionProvider(),
                components, new OwnershipLedger("root"), new ComponentResolver(), nifi,
                new FlowDeploymentMetricsRegistry());
    }

    private Map<String, Object> processor(final String id, final Map<String, Object> scheduling) {
        final Map<String, Object> processor = new java.util.LinkedHashMap<>();
        processor.put("id", id);
        processor.put("type", "org.example.Processor");
        processor.put("scheduling", scheduling);
        return processor;
    }
}
