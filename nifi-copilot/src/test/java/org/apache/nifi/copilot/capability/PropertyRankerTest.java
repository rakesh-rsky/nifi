package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.apache.nifi.copilot.capability.PropertyRanker.PropertyClass;
import org.apache.nifi.copilot.capability.PropertyRanker.RankedProperty;
import org.junit.jupiter.api.Test;

class PropertyRankerTest {
    private static final ServiceApi READER_API =
            new ServiceApi("example.RecordReaderFactory", null);

    private final PropertyRanker ranker = new PropertyRanker();

    @Test
    void ranksRequiredAndServicePropertiesBeforeOptionalMetadata() {
        final List<RankedProperty> ranked = ranker.rank(Map.of(
                "optional", property("optional", "Optional setting", false, null),
                "schema", property("schema", "Schema Access Strategy", false, null),
                "reader", property("reader", "Record Reader", true, READER_API),
                "service", property("service", "Lookup Service", false, READER_API),
                "directory", property("directory", "Input Directory", true, null),
                "yield", property("yield", "Yield Duration", false, null)));

        assertEquals(
                List.of(
                        PropertyClass.REQUIRED_SERVICE_REFERENCE,
                        PropertyClass.REQUIRED,
                        PropertyClass.SERVICE_REFERENCE,
                        PropertyClass.DATA_CONTRACT,
                        PropertyClass.OPTIONAL,
                        PropertyClass.RUNTIME),
                ranked.stream().map(RankedProperty::propertyClass).toList());
        assertEquals(
                List.of("reader", "directory", "service", "schema", "optional", "yield"),
                ranked.stream().map(item -> item.property().name()).toList());
    }

    @Test
    void neverClassifiesRequiredRuntimeNamedPropertyAsRuntime() {
        final RankedProperty ranked =
                ranker.classify(property("yield", "Yield Duration", true, null));

        assertEquals(PropertyClass.REQUIRED, ranked.propertyClass());
        assertTrue(ranked.essential());
    }

    @Test
    void recognizesDataContractNamesWithoutMatchingEmbeddedWords() {
        assertEquals(
                PropertyClass.DATA_CONTRACT,
                ranker.classify(property(
                                "csv-delimiter", "Value Separator", false, null))
                        .propertyClass());
        assertEquals(
                PropertyClass.OPTIONAL,
                ranker.classify(property(
                                "record-limit", "Maximum Records", false, null))
                        .propertyClass());
    }

    @Test
    void recognizesRuntimeMetadataAndLeavesGenericPropertiesOptional() {
        assertEquals(
                PropertyClass.RUNTIME,
                ranker.classify(property(
                                "bulletinLevel", "Bulletin Level", false, null))
                        .propertyClass());
        assertEquals(
                PropertyClass.RUNTIME,
                ranker.classify(property(
                                "penalty-duration", "Penalty Duration", false, null))
                        .propertyClass());
        assertFalse(ranker.classify(property(
                        "timeout", "Connection Timeout", false, null))
                .essential());
    }

    @Test
    void returnsDeterministicNameOrderWithinAClass() {
        final List<String> names = ranker.rank(Map.of(
                        "z", property("z", "Zulu", false, null),
                        "a", property("a", "Alpha", false, null)))
                .stream()
                .map(item -> item.property().name())
                .toList();

        assertEquals(List.of("a", "z"), names);
    }

    private PropertyNode property(
            final String name,
            final String displayName,
            final boolean required,
            final ServiceApi serviceApi) {
        return new PropertyNode(
                name,
                displayName,
                required,
                null,
                false,
                false,
                List.of(),
                List.of(),
                serviceApi);
    }
}
