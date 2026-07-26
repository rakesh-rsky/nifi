package org.apache.nifi.copilot.capability;

public class FlowSpecificationValidationException extends RuntimeException {
    private final ValidationReport report;

    public FlowSpecificationValidationException(final ValidationReport report) {
        super("Flow specification capability validation failed with " + report.issues().size() + " issue(s)");
        this.report = report;
    }

    public ValidationReport getReport() {
        return report;
    }
}
