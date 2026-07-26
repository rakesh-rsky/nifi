package org.apache.nifi.copilot.capability;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.copilot.capability.WorkflowIntent.DataFormat;
import org.apache.nifi.copilot.capability.WorkflowIntent.EndpointKind;
import org.apache.nifi.copilot.capability.WorkflowIntent.LogOutcome;
import org.apache.nifi.copilot.capability.WorkflowIntent.TransformationKind;
import org.springframework.stereotype.Component;

@Component
public class IntentExtractor {
    private static final String SOURCE_DIRECTION =
            "(?:read|consume|receive|listen|fetch|ingest|input|source|from)";
    private static final String SINK_DIRECTION =
            "(?:send|write|publish|post|put|store|insert|output|sink|into|to)";
    private static final Pattern DIRECTION = Pattern.compile(
            "\\b(read|consume|receive|listen|fetch|ingest|input|source|from|"
                    + "send|write|publish|post|put|store|insert|output|sink|into|to)\\b");
    private static final Pattern CLAUSE_BOUNDARY = Pattern.compile(
            "\\b(?:archive|convert|enrich|filter|log|merge|route|split|then|validate)\\w*\\b");
    private static final Pattern MAXIMUM_BATCH_SIZE = Pattern.compile(
            "\\b(?:maximum|max|up to)\\s+(?:of\\s+)?(\\d+)\\s+"
                    + "(?:messages|records|items|rows)\\b");
    private static final Pattern PER_REQUEST_BATCH_SIZE = Pattern.compile(
            "\\b(\\d+)\\s+(?:messages|records|items|rows)\\s+"
                    + "(?:per|in\\s+(?:one|an|each))\\s+(?:batch|array|request|api\\s+call)\\b");
    private static final Pattern NUMBER_BEFORE_PARALLELISM = Pattern.compile(
            "\\b(\\d+|one|two|three|four|five|six|seven|eight|nine|ten)\\s+"
                    + "(?:\\w+\\s+){0,2}(?:parallel|concurrent|workers?)\\b");
    private static final Pattern PARALLELISM_BEFORE_NUMBER = Pattern.compile(
            "\\b(?:parallel|concurrent)\\s+(?:calls?\\s+|requests?\\s+|workers?\\s+)?"
                    + "(\\d+|one|two|three|four|five|six|seven|eight|nine|ten)\\b");
    private static final Map<String, Integer> NUMBER_WORDS = Map.ofEntries(
            Map.entry("one", 1),
            Map.entry("two", 2),
            Map.entry("three", 3),
            Map.entry("four", 4),
            Map.entry("five", 5),
            Map.entry("six", 6),
            Map.entry("seven", 7),
            Map.entry("eight", 8),
            Map.entry("nine", 9),
            Map.entry("ten", 10));

    public WorkflowIntent extract(final String prompt) {
        final String normalized = normalize(prompt);
        final Set<String> terms = normalized.isEmpty()
                ? Set.of()
                : new LinkedHashSet<>(Arrays.asList(normalized.split(" ")));
        final Set<EndpointKind> sources = EnumSet.noneOf(EndpointKind.class);
        final Set<EndpointKind> sinks = EnumSet.noneOf(EndpointKind.class);
        classifyEndpoint(normalized, EndpointKind.MQTT, sources, sinks, "mqtt");
        classifyEndpoint(normalized, EndpointKind.KAFKA, sources, sinks, "kafka");
        classifyEndpoint(normalized, EndpointKind.HTTP, sources, sinks,
                "http", "https", "api", "endpoint", "rest");
        classifyEndpoint(normalized, EndpointKind.DATABASE, sources, sinks,
                "database", "jdbc", "sql", "postgres", "postgresql", "mysql", "oracle");
        classifyEndpoint(normalized, EndpointKind.LOCAL_FILE, sources, sinks,
                "file", "files", "directory", "folder");
        classifyEndpoint(normalized, EndpointKind.OBJECT_STORAGE, sources, sinks,
                "s3", "object storage", "blob", "bucket");

        final Set<DataFormat> formats = EnumSet.noneOf(DataFormat.class);
        addWhenPresent(normalized, formats, DataFormat.CSV, "csv");
        addWhenPresent(normalized, formats, DataFormat.JSON, "json");
        addWhenPresent(normalized, formats, DataFormat.AVRO, "avro");
        addWhenPresent(normalized, formats, DataFormat.XML, "xml");
        addWhenPresent(normalized, formats, DataFormat.PARQUET, "parquet");
        addWhenPresent(normalized, formats, DataFormat.TEXT, "text", "txt");

        final Set<TransformationKind> transformations = EnumSet.noneOf(TransformationKind.class);
        addWhenPresent(normalized, transformations, TransformationKind.CONVERT,
                "convert", "converts", "converted", "transform", "transforms");
        addWhenPresent(normalized, transformations, TransformationKind.VALIDATE,
                "validate", "validates", "validation");
        addWhenPresent(normalized, transformations, TransformationKind.FILTER, "filter", "filters");
        addWhenPresent(normalized, transformations, TransformationKind.ROUTE, "route", "routes");
        addWhenPresent(normalized, transformations, TransformationKind.SPLIT, "split", "splits");
        addWhenPresent(normalized, transformations, TransformationKind.MERGE,
                "merge", "merges", "batch", "batching", "array");
        addWhenPresent(normalized, transformations, TransformationKind.ENRICH,
                "enrich", "enriches", "lookup");

        final boolean batching = containsAny(normalized, "batch", "batching", "array", "group");
        final OptionalInt maximumBatchSize = firstPositiveInteger(
                normalized, MAXIMUM_BATCH_SIZE, PER_REQUEST_BATCH_SIZE);
        final boolean archive = containsAny(normalized, "archive", "archives", "archived");
        final Set<LogOutcome> logging = logging(normalized);
        final OptionalInt parallelism = firstNumber(
                normalized, NUMBER_BEFORE_PARALLELISM, PARALLELISM_BEFORE_NUMBER);

        return new WorkflowIntent(
                normalized,
                terms,
                sources,
                formats,
                transformations,
                sinks,
                batching,
                maximumBatchSize,
                archive,
                logging,
                parallelism);
    }

    private void classifyEndpoint(
            final String text,
            final EndpointKind endpoint,
            final Set<EndpointKind> sources,
            final Set<EndpointKind> sinks,
            final String... aliases) {
        if (!containsAny(text, aliases)) {
            return;
        }
        for (String alias : aliases) {
            final Matcher endpointMatcher =
                    Pattern.compile("\\b" + Pattern.quote(alias) + "\\b").matcher(text);
            while (endpointMatcher.find()) {
                final String direction = nearestDirection(text, endpointMatcher.start());
                if (direction != null && Pattern.matches(SOURCE_DIRECTION, direction)) {
                    sources.add(endpoint);
                } else if (direction != null && Pattern.matches(SINK_DIRECTION, direction)) {
                    sinks.add(endpoint);
                }
            }
        }
    }

    private String nearestDirection(final String text, final int endpointStart) {
        final Matcher matcher = DIRECTION.matcher(text.substring(0, endpointStart));
        String direction = null;
        int directionEnd = -1;
        while (matcher.find()) {
            direction = matcher.group(1);
            directionEnd = matcher.end();
        }
        if (direction == null) {
            return null;
        }
        final String between = text.substring(directionEnd, endpointStart).trim();
        return (between.isEmpty() || between.split(" ").length <= 5)
                && !CLAUSE_BOUNDARY.matcher(between).find()
                ? direction
                : null;
    }

    private <E extends Enum<E>> void addWhenPresent(
            final String text,
            final Set<E> values,
            final E value,
            final String... aliases) {
        if (containsAny(text, aliases)) {
            values.add(value);
        }
    }

    private Set<LogOutcome> logging(final String text) {
        if (!containsAny(text, "log", "logs", "logging")) {
            return Set.of();
        }
        final Set<LogOutcome> outcomes = EnumSet.noneOf(LogOutcome.class);
        if (containsAny(text, "success", "successful", "succeeded")) {
            outcomes.add(LogOutcome.SUCCESS);
        }
        if (containsAny(text, "failure", "failures", "failed", "error", "errors")) {
            outcomes.add(LogOutcome.FAILURE);
        }
        if (outcomes.isEmpty()) {
            outcomes.add(LogOutcome.ALL);
        }
        return outcomes;
    }

    private OptionalInt firstPositiveInteger(
            final String text,
            final Pattern... patterns) {
        for (Pattern pattern : patterns) {
            final Matcher matcher = pattern.matcher(text);
            if (matcher.find()) {
                final int value = Integer.parseInt(matcher.group(1));
                if (value > 0) {
                    return OptionalInt.of(value);
                }
            }
        }
        return OptionalInt.empty();
    }

    private OptionalInt firstNumber(final String text, final Pattern... patterns) {
        for (Pattern pattern : patterns) {
            final Matcher matcher = pattern.matcher(text);
            if (matcher.find()) {
                final String value = matcher.group(1);
                final Integer numberWord = NUMBER_WORDS.get(value);
                final int parsed = numberWord == null ? parseInteger(value) : numberWord;
                if (parsed > 0) {
                    return OptionalInt.of(parsed);
                }
            }
        }
        return OptionalInt.empty();
    }

    private int parseInteger(final String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private boolean containsAny(final String text, final String... aliases) {
        for (String alias : aliases) {
            if (Pattern.compile("\\b" + Pattern.quote(alias) + "\\b").matcher(text).find()) {
                return true;
            }
        }
        return false;
    }

    private String normalize(final String prompt) {
        if (prompt == null || prompt.isBlank()) {
            return "";
        }
        return prompt.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", " ")
                .trim()
                .replaceAll("\\s+", " ");
    }
}
