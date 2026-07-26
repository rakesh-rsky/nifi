package org.apache.nifi.copilot.capability;

public class UnsupportedControllerServiceException extends RuntimeException {
    public UnsupportedControllerServiceException(final String message) {
        super(message);
    }
}
