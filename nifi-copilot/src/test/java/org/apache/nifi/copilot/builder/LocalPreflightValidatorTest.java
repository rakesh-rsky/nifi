package org.apache.nifi.copilot.builder;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalPreflightValidatorTest {

    @Test
    void rejectsImplicitSelfLoopBeforeDeployment() {
        List<Map<String, Object>> connections = List.of(Map.of(
                "from", "logger",
                "to", "logger",
                "relationships", List.of("success")));

        assertThrows(IllegalArgumentException.class,
                () -> LocalPreflightValidator.validateConnectionTopology(connections));
    }

    @Test
    void permitsExplicitSelfLoop() {
        List<Map<String, Object>> connections = List.of(Map.of(
                "from", "retry",
                "to", "retry",
                "relationships", List.of("retry"),
                "allow_self_loop", true));

        assertDoesNotThrow(() -> LocalPreflightValidator.validateConnectionTopology(connections));
    }
}
