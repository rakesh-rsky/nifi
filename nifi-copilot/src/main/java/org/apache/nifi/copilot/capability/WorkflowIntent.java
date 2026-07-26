package org.apache.nifi.copilot.capability;

import java.util.Collections;
import java.util.EnumSet;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;

public record WorkflowIntent(
        String normalizedText,
        Set<String> terms,
        Set<EndpointKind> sources,
        Set<DataFormat> formats,
        Set<TransformationKind> transformations,
        Set<EndpointKind> sinks,
        boolean batching,
        OptionalInt maximumBatchSize,
        boolean archive,
        Set<LogOutcome> logging,
        OptionalInt parallelism) {
    public WorkflowIntent {
        normalizedText = normalizedText == null ? "" : normalizedText;
        terms = immutableTerms(terms);
        sources = immutableEnums(sources);
        formats = immutableEnums(formats);
        transformations = immutableEnums(transformations);
        sinks = immutableEnums(sinks);
        maximumBatchSize = maximumBatchSize == null ? OptionalInt.empty() : maximumBatchSize;
        logging = immutableEnums(logging);
        parallelism = parallelism == null ? OptionalInt.empty() : parallelism;
    }

    private static Set<String> immutableTerms(final Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Set.of();
        }
        return Collections.unmodifiableSet(new TreeSet<>(values));
    }

    private static <E extends Enum<E>> Set<E> immutableEnums(final Set<E> values) {
        if (values == null || values.isEmpty()) {
            return Set.of();
        }
        return Collections.unmodifiableSet(EnumSet.copyOf(values));
    }

    public enum EndpointKind {
        DATABASE,
        HTTP,
        KAFKA,
        LOCAL_FILE,
        MQTT,
        OBJECT_STORAGE
    }

    public enum DataFormat {
        AVRO,
        CSV,
        JSON,
        PARQUET,
        TEXT,
        XML
    }

    public enum TransformationKind {
        CONVERT,
        ENRICH,
        FILTER,
        MERGE,
        ROUTE,
        SPLIT,
        VALIDATE
    }

    public enum LogOutcome {
        ALL,
        FAILURE,
        SUCCESS
    }
}
