package org.apache.nifi.copilot.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.nifi.copilot.capability.FlowSpecificationValidationException;
import org.apache.nifi.copilot.capability.ValidatedFlowPlan;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlowBuilderCapabilityTest {
    private NiFiClientOperations nifi;
    private FlowBuilder builder;

    @BeforeEach
    void setUp() {
        nifi = mock(NiFiClientOperations.class);
        when(nifi.listProcessorTypes()).thenReturn(List.of(type("org.example.RequiredProcessor")));
        when(nifi.listControllerServiceTypes()).thenReturn(List.of());
        when(nifi.getProcessorDefinition("g", "a", "1", "org.example.RequiredProcessor"))
                .thenReturn(Map.of(
                        "type", "org.example.RequiredProcessor",
                        "propertyDescriptors", Map.of("required-value", Map.of(
                                "name", "required-value", "displayName", "Required Value",
                                "required", true, "dynamic", false, "sensitive", false)),
                        "supportedRelationships", List.of(Map.of("name", "success")),
                        "supportedSchedulingStrategies", List.of("TIMER_DRIVEN"),
                        "supportsDynamicProperties", false,
                        "supportsDynamicRelationships", false,
                        "triggerSerially", false));
        builder = new FlowBuilder();
    }

    @Test
    void prepareIsReadOnlyAndNormalizesThePlan() {
        final ValidatedFlowPlan plan = builder.prepareFlow(Map.of(
                "processors", List.of(Map.of(
                        "id", "p1", "type", "RequiredProcessor",
                        "config", Map.of("Required Value", "configured"))),
                "connections", List.of()), nifi);

        final Map<String, Object> processor = maps(plan.specification().get("processors")).getFirst();
        assertEquals("org.example.RequiredProcessor", processor.get("type"));
        assertEquals(Map.of("required-value", "configured"), processor.get("config"));
        verifyNoMutations();
    }

    @Test
    void missingRequiredPropertyFailsBeforeAnyMutation() {
        assertThrows(FlowSpecificationValidationException.class, () -> builder.prepareFlow(Map.of(
                "processors", List.of(Map.of("id", "p1", "type", "RequiredProcessor")),
                "connections", List.of()), nifi));

        verifyNoMutations();
    }

    @Test
    void unsupportedControllerServiceFailsBeforeAnyMutation() {
        assertThrows(FlowSpecificationValidationException.class, () -> builder.prepareFlow(Map.of(
                "controller_services", List.of(Map.of(
                        "id", "cs1", "name", "Missing", "type", "org.example.Missing",
                        "properties", Map.of())),
                "processors", List.of(),
                "connections", List.of()), nifi));

        verifyNoMutations();
    }

    private void verifyNoMutations() {
        verify(nifi, never()).createProcessor(
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        verify(nifi, never()).updateProcessor(
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap());
        verify(nifi, never()).createControllerService(
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap());
        verify(nifi, never()).deleteProcessor(org.mockito.ArgumentMatchers.anyString());
        verify(nifi, never()).enableControllerService(org.mockito.ArgumentMatchers.anyString());
        verify(nifi, never()).scheduleProcessGroup(
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString());
    }

    private Map<String, Object> type(final String name) {
        return Map.of("type", name, "bundle", Map.of("group", "g", "artifact", "a", "version", "1"));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> maps(final Object value) {
        return (List<Map<String, Object>>) value;
    }
}
