package org.apache.nifi.copilot.capability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.WorkflowIntent.DataFormat;
import org.apache.nifi.copilot.capability.WorkflowIntent.EndpointKind;
import org.apache.nifi.copilot.capability.WorkflowIntent.TransformationKind;
import org.springframework.stereotype.Component;

@Component
public final class ProcessorSeedRanker {
    static final int DEFAULT_MAX_SEEDS = 8;

    public List<RankedProcessor> rank(
            final CapabilityGraph graph,
            final WorkflowIntent intent) {
        return rank(graph, intent, DEFAULT_MAX_SEEDS);
    }

    List<RankedProcessor> rank(
            final CapabilityGraph graph,
            final WorkflowIntent intent,
            final int limit) {
        if (graph == null || intent == null) {
            throw new IllegalArgumentException("Capability graph and workflow intent are required");
        }
        if (limit <= 0 || intent.normalizedText().isBlank()) {
            return List.of();
        }
        return graph.processorsByType().values().stream()
                .map(processor -> score(processor, intent))
                .filter(candidate -> candidate.score() > 0)
                .sorted(Comparator
                        .comparingInt(ScoredProcessor::score)
                        .reversed()
                        .thenComparing(candidate -> candidate.processor().type()))
                .limit(limit)
                .map(candidate -> new RankedProcessor(
                        candidate.processor(), candidate.score(), candidate.matchedSignals()))
                .toList();
    }

    private ScoredProcessor score(
            final ProcessorNode processor,
            final WorkflowIntent intent) {
        final ProcessorProfile profile = ProcessorProfile.from(processor);
        final Score score = new Score();

        if (intent.normalizedText().contains(profile.simpleName())
                || intent.normalizedText().contains(profile.spacedName())) {
            score.add(200, "explicit-name");
        }
        scoreTerms(profile, intent, score);
        scoreEndpoints(profile, intent, score);
        scoreTransformations(profile, intent, score);
        scoreOperationalSignals(profile, intent, score);
        scoreFormats(profile, intent, score);

        return new ScoredProcessor(processor, score.value, score.signals);
    }

    private void scoreTerms(
            final ProcessorProfile profile,
            final WorkflowIntent intent,
            final Score score) {
        int metadataMatches = 0;
        for (String term : intent.terms()) {
            if (term.length() < 3) {
                continue;
            }
            if (profile.nameTokens().contains(term)) {
                score.add(8, "name:" + term);
            } else if (metadataMatches < 4 && profile.metadataTokens().contains(term)) {
                score.add(2, "metadata:" + term);
                metadataMatches++;
            }
        }
    }

    private void scoreEndpoints(
            final ProcessorProfile profile,
            final WorkflowIntent intent,
            final Score score) {
        for (EndpointKind endpoint : EndpointKind.values()) {
            if (!profile.matches(endpoint)) {
                continue;
            }
            if (intent.sources().contains(endpoint) && profile.sourceRole()) {
                score.add(50, "source:" + endpoint.name());
            }
            if (intent.sinks().contains(endpoint) && profile.sinkRole()) {
                score.add(50, "sink:" + endpoint.name());
            }
            if ((intent.sources().contains(endpoint) || intent.sinks().contains(endpoint))
                    && !profile.sourceRole() && !profile.sinkRole()) {
                score.add(20, "endpoint:" + endpoint.name());
            }
        }
    }

    private void scoreTransformations(
            final ProcessorProfile profile,
            final WorkflowIntent intent,
            final Score score) {
        for (TransformationKind transformation : intent.transformations()) {
            if (profile.matches(transformation)) {
                score.add(40, "transform:" + transformation.name());
            }
        }
    }

    private void scoreOperationalSignals(
            final ProcessorProfile profile,
            final WorkflowIntent intent,
            final Score score) {
        if (intent.batching() && profile.containsAny("merge", "batch", "bin")) {
            score.add(35, "batching");
        }
        if (intent.archive() && profile.containsAny("archive", "move")) {
            score.add(35, "archive");
        }
        if (!intent.logging().isEmpty() && profile.containsAny("log")) {
            score.add(35, "logging");
        }
        if (intent.parallelism().isPresent()
                && profile.containsAny("balance", "distribute", "load", "partition")) {
            score.add(35, "parallelism");
        }
    }

    private void scoreFormats(
            final ProcessorProfile profile,
            final WorkflowIntent intent,
            final Score score) {
        for (DataFormat format : intent.formats()) {
            final String token = format.name().toLowerCase(Locale.ROOT);
            if (profile.nameTokens().contains(token)) {
                score.add(20, "format:" + format.name());
            }
        }
    }

    public record RankedProcessor(
            ProcessorNode processor,
            int score,
            Set<String> matchedSignals) {
        public RankedProcessor {
            if (processor == null || score <= 0) {
                throw new IllegalArgumentException("Ranked processor and positive score are required");
            }
            matchedSignals = matchedSignals == null || matchedSignals.isEmpty()
                    ? Set.of()
                    : Collections.unmodifiableSet(new TreeSet<>(matchedSignals));
        }
    }

    private record ScoredProcessor(
            ProcessorNode processor,
            int score,
            Set<String> matchedSignals) {
    }

    private static final class Score {
        private int value;
        private final Set<String> signals = new LinkedHashSet<>();

        private void add(final int points, final String signal) {
            if (signals.add(signal)) {
                value += points;
            }
        }
    }

    private record ProcessorProfile(
            String simpleName,
            String spacedName,
            Set<String> nameTokens,
            Set<String> metadataTokens,
            boolean sourceRole,
            boolean sinkRole) {
        private static final Set<String> SOURCE_ACTIONS = Set.of(
                "capture", "consume", "execute", "fetch", "get", "ingest", "list",
                "listen", "query", "read", "receive", "tail");
        private static final Set<String> SINK_ACTIONS = Set.of(
                "invoke", "insert", "post", "publish", "put", "send", "store", "write");

        private static ProcessorProfile from(final ProcessorNode processor) {
            final String simpleName = simpleName(processor.type()).toLowerCase(Locale.ROOT);
            final List<String> splitName = splitCamelCase(simpleName(processor.type()));
            final Set<String> nameTokens = Set.copyOf(splitName);
            final Set<String> metadataTokens = new LinkedHashSet<>(nameTokens);
            processor.properties().values().forEach(property -> {
                metadataTokens.addAll(words(property.name()));
                metadataTokens.addAll(words(property.displayName()));
            });
            return new ProcessorProfile(
                    simpleName,
                    String.join(" ", splitName),
                    nameTokens,
                    Set.copyOf(metadataTokens),
                    containsAny(nameTokens, SOURCE_ACTIONS),
                    containsAny(nameTokens, SINK_ACTIONS));
        }

        private boolean matches(final EndpointKind endpoint) {
            return switch (endpoint) {
                case DATABASE -> containsAny("database", "db", "jdbc", "sql");
                case HTTP -> containsAny("http", "https", "rest");
                case KAFKA -> containsAny("kafka");
                case LOCAL_FILE -> containsAny("file", "directory", "folder");
                case MQTT -> containsAny("mqtt");
                case OBJECT_STORAGE -> containsAny("blob", "bucket", "object", "s3");
            };
        }

        private boolean matches(final TransformationKind transformation) {
            return switch (transformation) {
                case CONVERT -> containsAny("convert", "transform");
                case ENRICH -> containsAny("enrich", "lookup");
                case FILTER -> containsAny("filter");
                case MERGE -> containsAny("batch", "merge");
                case ROUTE -> containsAny("route");
                case SPLIT -> containsAny("split");
                case VALIDATE -> containsAny("validate");
            };
        }

        private boolean containsAny(final String... candidates) {
            for (String candidate : candidates) {
                if (nameTokens.contains(candidate)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean containsAny(
                final Set<String> values,
                final Set<String> candidates) {
            return values.stream().anyMatch(candidates::contains);
        }

        private static String simpleName(final String type) {
            return type.substring(type.lastIndexOf('.') + 1);
        }

        private static List<String> splitCamelCase(final String value) {
            final String spaced = value
                    .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
                    .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2");
            return words(spaced);
        }

        private static List<String> words(final String value) {
            if (value == null || value.isBlank()) {
                return List.of();
            }
            final List<String> words = new ArrayList<>();
            for (String word : value.toLowerCase(Locale.ROOT).split("[^a-z0-9]+")) {
                if (!word.isBlank()) {
                    words.add(word);
                }
            }
            return words;
        }
    }
}
