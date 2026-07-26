package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.Test;

class CapabilityRegistryManagerTest {
    @Test
    void cachesByClientIdentityAndNeverCrossesTargets() {
        final NiFiClientOperations first = emptyClient();
        final NiFiClientOperations second = emptyClient();
        final CapabilityRegistryManager manager = new CapabilityRegistryManager(Duration.ofMinutes(1));

        assertSame(manager.registry(first), manager.registry(first));
        assertNotSame(manager.registry(first), manager.registry(second));
        manager.snapshot(first);
        manager.snapshot(first);
        assertSame(manager.graph(first), manager.graph(first));
        assertSame(manager.capabilitySet(first).snapshot(), manager.snapshot(first));
        assertSame(manager.capabilitySet(first).graph(), manager.graph(first));
        manager.snapshot(second);

        verify(first).listProcessorTypes();
        verify(second).listProcessorTypes();
    }

    @Test
    void refreshIsTargetSpecific() {
        final NiFiClientOperations first = emptyClient();
        final NiFiClientOperations second = emptyClient();
        final CapabilityRegistryManager manager = new CapabilityRegistryManager(Duration.ofMinutes(1));

        manager.refresh(first);

        verify(first).refreshCapabilityCaches();
        verify(first).listProcessorTypes();
        verify(second, org.mockito.Mockito.never()).refreshCapabilityCaches();
    }

    private NiFiClientOperations emptyClient() {
        final NiFiClientOperations client = mock(NiFiClientOperations.class);
        when(client.listProcessorTypes()).thenReturn(List.of());
        when(client.listControllerServiceTypes()).thenReturn(List.of());
        return client;
    }
}
