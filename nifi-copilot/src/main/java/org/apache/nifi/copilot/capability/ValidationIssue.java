package org.apache.nifi.copilot.capability;

public record ValidationIssue(
        String componentId,
        String path,
        String reason,
        String suggestedFix,
        ValidationIssueType issueType,
        String capabilityType,
        String rejectedValue,
        ServiceApi requiredApi)
        implements Comparable<ValidationIssue> {
    public ValidationIssue(
            final String componentId,
            final String path,
            final String reason,
            final String suggestedFix) {
        this(
                componentId,
                path,
                reason,
                suggestedFix,
                ValidationIssueType.OTHER,
                "",
                "",
                null);
    }

    public ValidationIssue {
        componentId = componentId == null ? "" : componentId;
        path = path == null ? "" : path;
        reason = reason == null ? "" : reason;
        suggestedFix = suggestedFix == null ? "" : suggestedFix;
        issueType = issueType == null ? ValidationIssueType.OTHER : issueType;
        capabilityType = capabilityType == null ? "" : capabilityType;
        rejectedValue = rejectedValue == null ? "" : rejectedValue;
    }

    @Override
    public int compareTo(final ValidationIssue other) {
        int comparison = componentId.compareTo(other.componentId);
        if (comparison == 0) {
            comparison = path.compareTo(other.path);
        }
        if (comparison == 0) {
            comparison = reason.compareTo(other.reason);
        }
        return comparison;
    }
}
