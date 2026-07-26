package org.apache.nifi.copilot.capability;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import org.apache.nifi.copilot.capability.DependencyClosureResolver.CapabilityClosure;
import org.springframework.stereotype.Component;

@Component
public final class CapabilityMetricsRegistry {
    private final LongAdder selectionCount = new LongAdder();
    private final LongAdder seedProcessorCount = new LongAdder();
    private final LongAdder selectedServiceCount = new LongAdder();
    private final LongAdder promptCharacters = new LongAdder();
    private final LongAccumulator maxPromptCharacters = new LongAccumulator(Long::max, 0);
    private final LongAdder firstPassValid = new LongAdder();
    private final LongAdder firstPassInvalid = new LongAdder();
    private final LongAdder repairAttempts = new LongAdder();
    private final LongAdder repairSuccesses = new LongAdder();
    private final LongAdder repairFailures = new LongAdder();
    private final LongAdder repairAddedProcessors = new LongAdder();
    private final LongAdder repairAddedServices = new LongAdder();
    private final LongAdder repairContextCharacters = new LongAdder();
    private final LongAccumulator maxRepairContextCharacters = new LongAccumulator(Long::max, 0);
    private final LongAdder repairInputTokens = new LongAdder();
    private final LongAdder repairOutputTokens = new LongAdder();

    private final ConcurrentHashMap<String, LongAdder> intentCategories = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> seedProcessorTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> selectedServiceTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> repairProcessorTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> repairServiceTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> validationIssueTypes = new ConcurrentHashMap<>();

    public void observeSelection(
            final WorkflowIntent intent,
            final CapabilityClosure closure,
            final int contextCharacters) {
        try {
            selectionCount.increment();
            seedProcessorCount.add(closure.processors().size());
            selectedServiceCount.add(closure.controllerServices().size());
            promptCharacters.add(contextCharacters);
            maxPromptCharacters.accumulate(contextCharacters);
            intent.sources().forEach(value -> increment(intentCategories, "SOURCE_" + value.name()));
            intent.formats().forEach(value -> increment(intentCategories, "FORMAT_" + value.name()));
            intent.transformations().forEach(value ->
                    increment(intentCategories, "TRANSFORM_" + value.name()));
            intent.sinks().forEach(value -> increment(intentCategories, "SINK_" + value.name()));
            if (intent.batching()) {
                increment(intentCategories, "BATCHING");
            }
            if (intent.archive()) {
                increment(intentCategories, "ARCHIVE");
            }
            if (!intent.logging().isEmpty()) {
                increment(intentCategories, "LOGGING");
            }
            if (intent.parallelism().isPresent()) {
                increment(intentCategories, "PARALLELISM");
            }
            closure.processors().forEach(selection ->
                    increment(seedProcessorTypes, selection.processor().type()));
            closure.controllerServices().forEach(selection ->
                    increment(selectedServiceTypes, selection.service().type()));
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    public void observeRepairExpansion(
            final CapabilityClosure original,
            final CapabilityClosure expanded,
            final int contextCharacters) {
        try {
            repairContextCharacters.add(contextCharacters);
            maxRepairContextCharacters.accumulate(contextCharacters);
            expanded.processors().stream()
                    .filter(selection -> original.processors().stream().noneMatch(originalSelection ->
                            originalSelection.processor().type().equals(selection.processor().type())))
                    .forEach(selection -> {
                        repairAddedProcessors.increment();
                        increment(repairProcessorTypes, selection.processor().type());
                    });
            expanded.controllerServices().stream()
                    .filter(selection -> original.controllerServices().stream().noneMatch(originalSelection ->
                            originalSelection.service().type().equals(selection.service().type())))
                    .forEach(selection -> {
                        repairAddedServices.increment();
                        increment(repairServiceTypes, selection.service().type());
                    });
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    public void observeFirstPass(
            final boolean valid,
            final List<ValidationIssue> issues) {
        try {
            if (valid) {
                firstPassValid.increment();
            } else {
                firstPassInvalid.increment();
                observeIssues(issues);
            }
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    public void observeRepairAttempt() {
        try {
            repairAttempts.increment();
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    public void observeRepairTokens(final long inputTokens, final long outputTokens) {
        try {
            repairInputTokens.add(Math.max(0, inputTokens));
            repairOutputTokens.add(Math.max(0, outputTokens));
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    public void observeRepairResult(
            final boolean valid,
            final List<ValidationIssue> issues) {
        try {
            if (valid) {
                repairSuccesses.increment();
            } else {
                repairFailures.increment();
                observeIssues(issues);
            }
        } catch (Throwable ignored) {
            ignoreMetricFailure(ignored);
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(
                selectionCount.sum(),
                seedProcessorCount.sum(),
                selectedServiceCount.sum(),
                promptCharacters.sum(),
                maxPromptCharacters.get(),
                firstPassValid.sum(),
                firstPassInvalid.sum(),
                repairAttempts.sum(),
                repairSuccesses.sum(),
                repairFailures.sum(),
                repairAddedProcessors.sum(),
                repairAddedServices.sum(),
                repairContextCharacters.sum(),
                maxRepairContextCharacters.get(),
                repairInputTokens.sum(),
                repairOutputTokens.sum(),
                snapshot(intentCategories),
                snapshot(seedProcessorTypes),
                snapshot(selectedServiceTypes),
                snapshot(repairProcessorTypes),
                snapshot(repairServiceTypes),
                snapshot(validationIssueTypes));
    }

    private void observeIssues(final List<ValidationIssue> issues) {
        if (issues != null) {
            issues.forEach(issue -> increment(validationIssueTypes, issue.issueType().name()));
        }
    }

    private void increment(
            final ConcurrentHashMap<String, LongAdder> metrics,
            final String key) {
        metrics.computeIfAbsent(key, ignored -> new LongAdder()).increment();
    }

    private Map<String, Long> snapshot(final ConcurrentHashMap<String, LongAdder> source) {
        final Map<String, Long> values = new LinkedHashMap<>();
        source.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> values.put(entry.getKey(), entry.getValue().sum()));
        return Collections.unmodifiableMap(values);
    }

    private static void ignoreMetricFailure(final Throwable ignored) {
        // Metrics must never alter generation, validation, or deployment behavior.
    }

    public record Snapshot(
            long selectionCount,
            long seedProcessorCount,
            long selectedServiceCount,
            long promptCharacters,
            long maxPromptCharacters,
            long firstPassValid,
            long firstPassInvalid,
            long repairAttempts,
            long repairSuccesses,
            long repairFailures,
            long repairAddedProcessors,
            long repairAddedServices,
            long repairContextCharacters,
            long maxRepairContextCharacters,
            long repairInputTokens,
            long repairOutputTokens,
            Map<String, Long> intentCategories,
            Map<String, Long> seedProcessorTypes,
            Map<String, Long> selectedServiceTypes,
            Map<String, Long> repairProcessorTypes,
            Map<String, Long> repairServiceTypes,
            Map<String, Long> validationIssueTypes) {
    }
}
