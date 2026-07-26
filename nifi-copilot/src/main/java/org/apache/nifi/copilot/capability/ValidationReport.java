package org.apache.nifi.copilot.capability;

import java.util.List;

public record ValidationReport(List<ValidationIssue> issues) {
    public ValidationReport {
        issues = issues == null ? List.of() : issues.stream().sorted().toList();
    }

    public boolean valid() {
        return issues.isEmpty();
    }

    public void throwIfInvalid() {
        if (!valid()) {
            throw new FlowSpecificationValidationException(this);
        }
    }
}
