package org.apache.nifi.copilot.service;

public class CapabilityDiscoveryException extends IllegalStateException {
    public CapabilityDiscoveryException(final String message) {
        super(message);
    }

    public CapabilityDiscoveryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
