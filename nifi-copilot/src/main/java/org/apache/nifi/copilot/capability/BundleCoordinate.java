package org.apache.nifi.copilot.capability;

public record BundleCoordinate(String group, String artifact, String version) {
    public BundleCoordinate {
        if (blank(group) || blank(artifact) || blank(version)) {
            throw new IllegalArgumentException("Bundle group, artifact, and version are required");
        }
    }

    private static boolean blank(final String value) {
        return value == null || value.isBlank();
    }
}
