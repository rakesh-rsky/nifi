package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.service.CapabilityDiscoveryException;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.Test;

class CapabilityRegistryTest {
    @Test
    void resolvesFqnSimpleNameAndLegacyAliasFromOneSnapshot() {
        final NiFiClientOperations client = clientWithProcessor("org.apache.nifi.processors.mqtt.ConsumeMQTT");
        final CapabilityRegistry registry = new CapabilityRegistry(client, Duration.ofMinutes(5));

        final ProcessorCapability exact = registry.resolveProcessor(
                "org.apache.nifi.processors.mqtt.ConsumeMQTT").orElseThrow();
        assertSame(exact, registry.resolveProcessor("ConsumeMQTT").orElseThrow());
        assertSame(exact, registry.resolveProcessor("mqtt").orElseThrow());
        verify(client, times(1)).listProcessorTypes();
    }

    @Test
    void failedRefreshDoesNotPublishPartialSnapshot() {
        final NiFiClientOperations client = clientWithProcessor("example.First");
        final CapabilityRegistry registry = new CapabilityRegistry(client, Duration.ofMinutes(5));
        final CapabilitySnapshot initial = registry.snapshot();
        final CapabilityGraph initialGraph = registry.graph();
        when(client.listProcessorTypes()).thenReturn(List.of(type("example.Second")));
        when(client.getProcessorDefinition("g", "a", "1", "example.Second"))
                .thenThrow(new IllegalStateException("definition unavailable"));

        assertThrows(CapabilityDiscoveryException.class, registry::refresh);
        assertSame(initial, registry.snapshot());
        assertSame(initialGraph, registry.graph());
        assertEquals(Set.of("example.First"), registry.snapshot().processors().keySet());
        verify(client).refreshCapabilityCaches();
    }

    @Test
    void explicitRefreshAtomicallyReplacesSnapshot() {
        final NiFiClientOperations client = clientWithProcessor("example.First");
        final CapabilityRegistry registry = new CapabilityRegistry(client, Duration.ofMinutes(5));
        registry.snapshot();
        when(client.listProcessorTypes()).thenReturn(List.of(type("example.Second")));
        when(client.getProcessorDefinition("g", "a", "1", "example.Second"))
                .thenReturn(Map.of("type", "example.Second", "propertyDescriptors", Map.of()));

        final CapabilitySnapshot refreshed = registry.refresh();

        assertEquals(Set.of("example.Second"), refreshed.processors().keySet());
        verify(client).refreshCapabilityCaches();
    }

    @Test
    void reusesGraphForCurrentSnapshotAndReplacesItOnRefresh() {
        final NiFiClientOperations client = clientWithProcessor("example.First");
        final CapabilityRegistry registry = new CapabilityRegistry(client, Duration.ofMinutes(5));

        final CapabilityGraph initial = registry.graph();
        assertSame(initial, registry.graph());
        assertEquals(Set.of("example.First"), initial.processorsByType().keySet());

        when(client.listProcessorTypes()).thenReturn(List.of(type("example.Second")));
        when(client.getProcessorDefinition("g", "a", "1", "example.Second"))
                .thenReturn(Map.of("type", "example.Second", "propertyDescriptors", Map.of()));
        registry.refresh();

        final CapabilityGraph refreshed = registry.graph();
        assertNotSame(initial, refreshed);
        assertEquals(Set.of("example.Second"), refreshed.processorsByType().keySet());
        verify(client, times(2)).listProcessorTypes();
    }

    @Test
    void returnsSnapshotAndGraphFromOnePublishedGeneration() {
        final CapabilityRegistry registry = new CapabilityRegistry(
                clientWithProcessor("example.First"), Duration.ofMinutes(5));

        final CapabilityRegistry.CapabilitySet capabilitySet = registry.capabilitySet();

        assertSame(capabilitySet.snapshot(), registry.snapshot());
        assertSame(capabilitySet.graph(), registry.graph());
        assertEquals(
                capabilitySet.snapshot().processors().keySet(),
                capabilitySet.graph().processorsByType().keySet());
    }

    @Test
    void ttlRediscoveryReplacesGraphGeneration() {
        final NiFiClientOperations client = clientWithProcessor("example.Processor");
        final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        final CapabilityRegistry registry = new CapabilityRegistry(
                client, Duration.ofMinutes(5), clock, new CapabilityDefinitionParser());
        final CapabilityGraph initial = registry.graph();

        clock.advance(Duration.ofMinutes(6));

        assertNotSame(initial, registry.graph());
        verify(client, times(2)).listProcessorTypes();
    }

    @Test
    void requireControllerServiceThrowsDedicatedExceptionWhenUnsupported() {
        final CapabilityRegistry registry =
                new CapabilityRegistry(clientWithProcessor("example.Processor"), Duration.ofMinutes(5));

        assertThrows(UnsupportedControllerServiceException.class,
                () -> registry.requireControllerService("example.MissingService"));
    }

    @Test
    void selectsNewestBundleWhenTypeExistsInMultipleVersions() {
        final NiFiClientOperations client = mock(NiFiClientOperations.class);
        when(client.listProcessorTypes()).thenReturn(List.of(
                type("example.Processor", "1.9.0"),
                type("example.Processor", "1.10.0")));
        when(client.listControllerServiceTypes()).thenReturn(List.of());
        when(client.getProcessorDefinition("g", "a", "1.10.0", "example.Processor"))
                .thenReturn(Map.of(
                        "type", "example.Processor",
                        "propertyDescriptors", Map.of()));

        final ProcessorCapability capability =
                new CapabilityRegistry(client, Duration.ofMinutes(5))
                        .resolveProcessor("example.Processor")
                        .orElseThrow();

        assertEquals("1.10.0", capability.bundle().version());
        verify(client, never()).getProcessorDefinition(
                "g", "a", "1.9.0", "example.Processor");
    }

    @Test
    void stableReleaseOutranksSnapshotBundle() {
        final NiFiClientOperations client = mock(NiFiClientOperations.class);
        when(client.listProcessorTypes()).thenReturn(List.of(
                type("example.Processor", "2.0.0-SNAPSHOT"),
                type("example.Processor", "2.0.0")));
        when(client.listControllerServiceTypes()).thenReturn(List.of());
        when(client.getProcessorDefinition("g", "a", "2.0.0", "example.Processor"))
                .thenReturn(Map.of(
                        "type", "example.Processor",
                        "propertyDescriptors", Map.of()));

        final ProcessorCapability capability =
                new CapabilityRegistry(client, Duration.ofMinutes(5))
                        .resolveProcessor("example.Processor")
                        .orElseThrow();

        assertEquals("2.0.0", capability.bundle().version());
        verify(client, never()).getProcessorDefinition(
                "g", "a", "2.0.0-SNAPSHOT", "example.Processor");
    }

    @Test
    void ttlRediscoveryInvalidatesClientBundleCache() {
        final NiFiClientOperations client = clientWithProcessor("example.Processor");
        final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        final CapabilityRegistry registry = new CapabilityRegistry(
                client, Duration.ofMinutes(5), clock, new CapabilityDefinitionParser());
        registry.snapshot();

        clock.advance(Duration.ofMinutes(6));
        registry.snapshot();

        verify(client).refreshCapabilityCaches();
        verify(client, times(2)).listProcessorTypes();
    }

    private NiFiClientOperations clientWithProcessor(final String name) {
        final NiFiClientOperations client = mock(NiFiClientOperations.class);
        when(client.listProcessorTypes()).thenReturn(List.of(type(name)));
        when(client.listControllerServiceTypes()).thenReturn(List.of());
        when(client.getProcessorDefinition("g", "a", "1", name))
                .thenReturn(Map.of("type", name, "propertyDescriptors", Map.of()));
        return client;
    }

    private Map<String, Object> type(final String name) {
        return type(name, "1");
    }

    private Map<String, Object> type(
            final String name,
            final String version) {
        return Map.of("type", name, "bundle",
                Map.of("group", "g", "artifact", "a", "version", version));
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(final Instant instant) {
            this.instant = instant;
        }

        void advance(final Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
