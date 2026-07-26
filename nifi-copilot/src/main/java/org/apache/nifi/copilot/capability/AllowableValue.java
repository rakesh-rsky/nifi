package org.apache.nifi.copilot.capability;

public record AllowableValue(String value, String displayName) {
    public AllowableValue {
        if (value == null) {
            throw new IllegalArgumentException("Allowable value is required");
        }
    }
}
