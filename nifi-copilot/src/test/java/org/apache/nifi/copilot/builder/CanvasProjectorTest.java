package org.apache.nifi.copilot.builder;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CanvasProjectorTest {

    @Test
    void projectsConnectionsWithEndpointsAndRelationships() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        when(nifi.getProcessGroupId("root")).thenReturn("root");
        when(nifi.getProcessGroupFlow("root")).thenReturn(Map.of(
                "processGroupFlow", Map.of("flow", Map.of(
                        "processors", List.of(),
                        "connections", List.of(Map.of(
                                "id", "connection-1",
                                "component", Map.of(
                                        "source", Map.of("id", "processor-1"),
                                        "destination", Map.of("id", "processor-2"),
                                        "selectedRelationships", List.of("success"))))))));
        when(nifi.listControllerServices("root")).thenReturn(List.of());

        final Map<String, Object> canvas = new CanvasProjector().readCanvas(nifi, "root");
        final List<?> connections = (List<?>) canvas.get("connections");
        final Map<?, ?> connection = (Map<?, ?>) connections.getFirst();

        assertEquals(1, connections.size());
        assertEquals("processor-1", connection.get("from"));
        assertEquals("processor-2", connection.get("to"));
        assertEquals(List.of("success"), connection.get("relationships"));
    }
}
