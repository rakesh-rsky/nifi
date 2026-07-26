package org.apache.nifi.copilot.capability;

import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CapabilityRegistryManager {
    public static final Duration DEFAULT_TTL = Duration.ofMinutes(15);

    private final Duration ttl;
    private final Map<NiFiClientOperations, CapabilityRegistry> registries = new IdentityHashMap<>();

    public CapabilityRegistryManager() {
        this(DEFAULT_TTL);
    }

    @Autowired
    public CapabilityRegistryManager(
            @Value("${nifi.copilot.capability.cache-ttl:PT15M}") final Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("Capability cache TTL must be positive");
        }
        this.ttl = ttl;
    }

    public CapabilitySnapshot snapshot(final NiFiClientOperations client) {
        return registry(client).snapshot();
    }

    public CapabilityGraph graph(final NiFiClientOperations client) {
        return registry(client).graph();
    }

    public CapabilityRegistry.CapabilitySet capabilitySet(final NiFiClientOperations client) {
        return registry(client).capabilitySet();
    }

    public CapabilitySnapshot refresh(final NiFiClientOperations client) {
        return registry(client).refresh();
    }

    public synchronized CapabilityRegistry registry(final NiFiClientOperations client) {
        if (client == null) {
            throw new IllegalArgumentException("NiFi client is required");
        }
        return registries.computeIfAbsent(client, key -> new CapabilityRegistry(key, ttl));
    }
}
