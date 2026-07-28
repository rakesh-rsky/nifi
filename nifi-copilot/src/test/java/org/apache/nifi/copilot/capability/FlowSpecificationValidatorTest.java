package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlowSpecificationValidatorTest {
    private FlowSpecificationValidator validator;

    @BeforeEach
    void setUp() {
        final NiFiClientOperations client = mock(NiFiClientOperations.class);
        when(client.listProcessorTypes()).thenReturn(List.of(
                type("org.apache.nifi.processors.mqtt.ConsumeMQTT"),
                type("org.apache.nifi.processors.standard.MergeRecord"),
                type("org.apache.nifi.processors.standard.InvokeHTTP"),
                type("example.AmbiguousProcessor"),
                type("example.DynamicProcessor")));
        when(client.listControllerServiceTypes()).thenReturn(List.of(
                serviceType("example.MqttConnection", "example.MqttApi"),
                serviceType("example.RecordReader", "example.RecordReaderApi"),
                serviceType("example.RecordWriter", "example.RecordWriterApi"),
                serviceType("example.ReaderWithSchemaReference", "example.RecordReaderApi"),
                serviceType("example.SchemaReferenceReader", "example.SchemaReferenceReaderApi"),
                serviceType("example.Unrelated", "example.OtherApi")));
        when(client.getProcessorDefinition("g", "a", "1", "org.apache.nifi.processors.mqtt.ConsumeMQTT"))
                .thenReturn(processorDefinition("org.apache.nifi.processors.mqtt.ConsumeMQTT", false,
                        List.of(property("broker-uri", "Broker URI", true, null),
                                property("topic-filter", "Topic Filter", true, null),
                                property("last-will-message", "Last Will Message", false, null),
                                dependentProperty("last-will-topic", "Last Will Topic",
                                        "last-will-message", List.of()),
                                serviceProperty("mqtt-service", "MQTT Connection Service", "example.MqttApi")),
                        List.of("Success"), List.of("TIMER_DRIVEN")));
        when(client.getProcessorDefinition("g", "a", "1", "org.apache.nifi.processors.standard.MergeRecord"))
                .thenReturn(processorDefinition("org.apache.nifi.processors.standard.MergeRecord", true,
                        List.of(serviceProperty("record-reader", "Record Reader", "example.RecordReaderApi"),
                                serviceProperty("record-writer", "Record Writer", "example.RecordWriterApi")),
                        List.of("merged", "failure"), List.of("TIMER_DRIVEN")));
        when(client.getProcessorDefinition("g", "a", "1", "org.apache.nifi.processors.standard.InvokeHTTP"))
                .thenReturn(processorDefinition("org.apache.nifi.processors.standard.InvokeHTTP", false,
                        List.of(property("remote-url", "Remote URL", true, null),
                                allowable("method", "HTTP Method", "GET", "POST")),
                        List.of("response", "failure"), List.of("TIMER_DRIVEN", "CRON_DRIVEN")));
        when(client.getProcessorDefinition("g", "a", "1", "example.AmbiguousProcessor"))
                .thenReturn(processorDefinition("example.AmbiguousProcessor", false,
                        List.of(), List.of("success", "response", "failure"), List.of("TIMER_DRIVEN")));
        final Map<String, Object> dynamicDefinition = new java.util.LinkedHashMap<>(
                processorDefinition("example.DynamicProcessor", false, List.of(),
                        List.of("success"), List.of("TIMER_DRIVEN")));
        dynamicDefinition.put("supportsDynamicProperties", true);
        when(client.getProcessorDefinition("g", "a", "1", "example.DynamicProcessor"))
                .thenReturn(dynamicDefinition);
        for (Map<String, Object> service : List.of(
                serviceType("example.MqttConnection", "example.MqttApi"),
                serviceType("example.RecordReader", "example.RecordReaderApi"),
                serviceType("example.RecordWriter", "example.RecordWriterApi"),
                serviceType("example.ReaderWithSchemaReference", "example.RecordReaderApi"),
                serviceType("example.SchemaReferenceReader", "example.SchemaReferenceReaderApi"),
                serviceType("example.Unrelated", "example.OtherApi"))) {
            final String type = String.valueOf(service.get("type"));
            final List<Map<String, Object>> properties;
            if (type.equals("example.MqttConnection")) {
                properties = List.of(
                        allowableWithDisplay("mode", "Mode", "internal-mode", "Friendly Mode"));
            } else if (type.equals("example.ReaderWithSchemaReference")) {
                properties = List.of(serviceProperty(
                        "schema-reference-reader",
                        "Schema Reference Reader",
                        "example.SchemaReferenceReaderApi"));
            } else {
                properties = List.of();
            }
            final Map<String, Object> descriptors = new java.util.LinkedHashMap<>();
            properties.forEach(property -> descriptors.put(String.valueOf(property.get("name")), property));
            when(client.getControllerServiceDefinition("g", "a", "1", type))
                    .thenReturn(Map.of("type", type, "propertyDescriptors", descriptors,
                            "supportsDynamicProperties", false));
        }
        validator = new FlowSpecificationValidator(new CapabilityRegistry(client, Duration.ofMinutes(5)));
    }

    @Test
    void validatesMqttFromDescriptorsWithoutMqttRules() {
        final Map<String, Object> valid = spec(
                List.of(service("mqtt-cs", "example.MqttConnection")),
                List.of(processor("consumer", "mqtt", Map.of(
                        "Broker URI", "tcp://broker:1883",
                        "Topic Filter", "events/#",
                        "MQTT Connection Service", "mqtt-cs"))),
                List.of(connection("consumer", "success")));
        assertTrue(validator.validate(valid).valid());

        final ValidationReport invalid = validator.validate(spec(
                List.of(service("bad", "example.Unrelated")),
                List.of(processor("consumer", "mqtt", Map.of(
                        "Broker URI", "tcp://broker",
                        "MQTT Connection Service", "bad"))),
                List.of()));
        assertReasons(invalid, "Missing required property", "does not implement required API");
    }

    @Test
    void enforcesRequiredPropertyOnlyWhenDependenciesAreActive() {
        final Map<String, Object> base = Map.of(
                "Broker URI", "tcp://broker",
                "Topic Filter", "events/#",
                "MQTT Connection Service", "mqtt-cs");
        assertTrue(validator.validate(spec(
                List.of(service("mqtt-cs", "example.MqttConnection")),
                List.of(processor("consumer", "mqtt", base)), List.of())).valid());

        final Map<String, Object> withLastWill = new java.util.LinkedHashMap<>(base);
        withLastWill.put("Last Will Message", "offline");
        assertReasons(validator.validate(spec(
                List.of(service("mqtt-cs", "example.MqttConnection")),
                List.of(processor("consumer", "mqtt", withLastWill)), List.of())),
                "Missing required property");
    }

    @Test
    void catchesRecordServicesInvokeHttpRelationshipsAndScheduling() {
        final Map<String, Object> merge = processor("merge", "MergeRecord", Map.of());
        merge.put("concurrent_tasks", 2);
        merge.put("scheduling_strategy", "EVENT_DRIVEN");
        final Map<String, Object> invoke = processor("http", "InvokeHTTP",
                Map.of("Remote URL", "#{url}", "HTTP Method", "PATCH"));
        final ValidationReport report = validator.validate(spec(
                List.of(),
                List.of(merge, invoke),
                List.of(connection("merge", "unknown"))));

        assertReasons(report,
                "Missing required property",
                "Unknown source relationship",
                "Unsupported scheduling strategy",
                "Serial processor cannot",
                "not one of the discovered allowable values");
    }

    @Test
    void rejectsUnknownServiceAndProcessorTypes() {
        final ValidationReport report = validator.validate(spec(
                List.of(service("missing-service", "not.Installed")),
                List.of(processor("missing-processor", "not.Installed", Map.of())),
                List.of()));
        assertReasons(report, "Unknown or ambiguous controller service", "Unknown or ambiguous processor");
        assertTrue(report.issues().stream().anyMatch(issue ->
                issue.issueType() == ValidationIssueType.UNKNOWN_CONTROLLER_SERVICE_TYPE
                        && issue.rejectedValue().equals("not.Installed")));
        assertTrue(report.issues().stream().anyMatch(issue ->
                issue.issueType() == ValidationIssueType.UNKNOWN_PROCESSOR_TYPE
                        && issue.rejectedValue().equals("not.Installed")));
    }

    @Test
    void returnsTypedMetadataForGraphAwareRepair() {
        final ValidationReport report = validator.validate(spec(
                List.of(),
                List.of(processor("merge", "MergeRecord", Map.of())),
                List.of(connection("merge", "unknown"))));

        final ValidationIssue reader = report.issues().stream()
                .filter(issue -> issue.path().equals("config.record-reader"))
                .findFirst()
                .orElseThrow();
        final ValidationIssue relationship = report.issues().stream()
                .filter(issue -> issue.issueType() == ValidationIssueType.UNSUPPORTED_RELATIONSHIP)
                .findFirst()
                .orElseThrow();

        assertEquals(ValidationIssueType.MISSING_REQUIRED_PROPERTY, reader.issueType());
        assertEquals("org.apache.nifi.processors.standard.MergeRecord", reader.capabilityType());
        assertEquals("example.RecordReaderApi", reader.requiredApi().type());
        assertTrue(reader.suggestedFix().contains("Record Reader"));
        assertTrue(reader.suggestedFix().contains("record-reader"));
        assertEquals("org.apache.nifi.processors.standard.MergeRecord", relationship.capabilityType());
        assertEquals("unknown", relationship.rejectedValue());
    }

    @Test
    void repairsIncompatibleServiceReferenceWhenOneCompatibleServiceIsDeclared() {
        final Map<String, Object> reader = service(
                "reader", "example.ReaderWithSchemaReference",
                Map.of("Schema Reference Reader", "wrong"));
        final Map<String, Object> compatible =
                service("schema-reader", "example.SchemaReferenceReader");
        final Map<String, Object> wrong = service("wrong", "example.Unrelated");

        final ValidatedFlowPlan plan = validator.validateAndNormalize(spec(
                List.of(reader, compatible, wrong), List.of(), List.of()));
        final Map<String, Object> normalizedReader =
                maps(plan.specification().get("controller_services")).getFirst();

        assertEquals(
                "schema-reader",
                map(normalizedReader.get("properties")).get("schema-reference-reader"));
    }

    @Test
    void normalizesAllAcceptedAliasesWithoutMutatingInputAndReturnsDeeplyImmutablePlan() {
        final Map<String, Object> service = new java.util.LinkedHashMap<>();
        service.put("id", "service-id");
        service.put("name", "friendly-service");
        service.put("type", "MqttConnection");
        service.put("properties", new java.util.LinkedHashMap<>(Map.of("Mode", "Friendly Mode")));
        final Map<String, Object> processor = processor("consumer", "mqtt", new java.util.LinkedHashMap<>(Map.of(
                "Broker URI", "tcp://broker",
                "Topic Filter", "events/#",
                "MQTT Connection Service", "friendly-service")));
        final Map<String, Object> connection = new java.util.LinkedHashMap<>(
                Map.of("from", "consumer", "to", "destination", "relationships", new ArrayList<>()));
        final Map<String, Object> original = new java.util.LinkedHashMap<>(
                spec(List.of(service), List.of(processor), List.of(connection)));

        final ValidatedFlowPlan plan = validator.validateAndNormalize(original);
        final Map<String, Object> normalizedService = maps(plan.specification().get("controller_services")).getFirst();
        final Map<String, Object> normalizedProcessor = maps(plan.specification().get("processors")).getFirst();
        final Map<String, Object> normalizedConnection = maps(plan.specification().get("connections")).getFirst();

        assertEquals("example.MqttConnection", normalizedService.get("type"));
        assertEquals(Map.of("mode", "internal-mode"), normalizedService.get("properties"));
        assertEquals("org.apache.nifi.processors.mqtt.ConsumeMQTT", normalizedProcessor.get("type"));
        assertEquals("service-id", map(normalizedProcessor.get("config")).get("mqtt-service"));
        assertEquals("tcp://broker", map(normalizedProcessor.get("config")).get("broker-uri"));
        assertEquals(List.of("Success"), normalizedConnection.get("relationships"));
        assertEquals("MqttConnection", service.get("type"));
        assertTrue(map(service.get("properties")).containsKey("Mode"));
        assertEquals("mqtt", processor.get("type"));
        assertTrue(map(processor.get("config")).containsKey("MQTT Connection Service"));
        assertTrue(((List<?>) connection.get("relationships")).isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> plan.specification().put("new", "value"));
        assertThrows(UnsupportedOperationException.class,
                () -> map(normalizedProcessor.get("config")).put("new", "value"));
        assertThrows(UnsupportedOperationException.class,
                () -> ((List<Object>) normalizedConnection.get("relationships")).add("failure"));
    }

    @Test
    void preservesDynamicPropertiesUnchanged() {
        final Map<String, Object> processor = processor(
                "dynamic", "DynamicProcessor", new java.util.LinkedHashMap<>(Map.of("Custom Name", "Custom Value")));

        final ValidatedFlowPlan plan = validator.validateAndNormalize(
                spec(List.of(), List.of(processor), List.of()));

        assertEquals(Map.of("Custom Name", "Custom Value"),
                maps(plan.specification().get("processors")).getFirst().get("config"));
    }

    @Test
    void infersOnlyUnambiguousSafeRelationship() {
        final Map<String, Object> accepted = new java.util.LinkedHashMap<>(
                Map.of("from", "consumer", "to", "destination"));
        final ValidatedFlowPlan plan = validator.validateAndNormalize(spec(
                List.of(),
                List.of(processor("consumer", "DynamicProcessor", Map.of())),
                List.of(accepted)));
        assertEquals(List.of("success"),
                maps(plan.specification().get("connections")).getFirst().get("relationships"));

        final Map<String, Object> mergeConnection = new java.util.LinkedHashMap<>(
                Map.of("from", "merge", "to", "destination"));
        final ValidatedFlowPlan mergePlan = validator.validateAndNormalize(spec(
                List.of(service("reader", "example.RecordReader"), service("writer", "example.RecordWriter")),
                List.of(processor("merge", "MergeRecord",
                        Map.of("Record Reader", "reader", "Record Writer", "writer"))),
                List.of(mergeConnection)));
        assertEquals(List.of("merged"),
                maps(mergePlan.specification().get("connections")).getFirst().get("relationships"));

        final ValidationReport rejected = validator.validate(spec(
                List.of(), List.of(processor("ambiguous", "example.AmbiguousProcessor", Map.of())),
                List.of(new java.util.LinkedHashMap<>(Map.of("from", "ambiguous", "to", "destination")))));
        assertReasons(rejected, "Source relationship is required");
    }

    @Test
    void rejectsMalformedRelationshipsAndDuplicateServiceNames() {
        final Map<String, Object> malformed = new java.util.LinkedHashMap<>(
                Map.of("from", "consumer", "to", "destination", "relationships", "success"));
        final ValidationReport malformedReport = validator.validate(spec(
                List.of(),
                List.of(processor("consumer", "mqtt", Map.of(
                        "Broker URI", "tcp://broker", "Topic Filter", "events/#"))),
                List.of(malformed)));
        assertReasons(malformedReport, "Relationships must be a list");

        final Map<String, Object> first = new java.util.LinkedHashMap<>(service("first", "example.MqttConnection"));
        final Map<String, Object> second = new java.util.LinkedHashMap<>(service("second", "example.MqttConnection"));
        first.put("name", "duplicate");
        second.put("name", "duplicate");
        assertReasons(validator.validate(spec(List.of(first, second), List.of(), List.of())),
                "Duplicate controller service name");
    }

    @Test
    void validatesAndNormalizesDeletionAndControllerServiceActions() {
        final Map<String, Object> invalid = new java.util.LinkedHashMap<>(
                spec(List.of(), List.of(), List.of()));
        invalid.put("deletions", List.of(Map.of("type", "anything")));
        invalid.put("cs_actions", List.of(Map.of("name", "", "action", "restart")));

        assertReasons(validator.validate(invalid),
                "Unsupported deletion type", "Deletion target is missing",
                "Controller service name is missing", "Unsupported controller service action");

        final Map<String, Object> valid = new java.util.LinkedHashMap<>(
                spec(List.of(), List.of(), List.of()));
        valid.put("deletions", List.of(Map.of("type", "process group", "name", "group")));
        valid.put("cs_actions", List.of(Map.of("name", "service", "action", "ENABLE")));
        final Map<String, Object> normalized = validator.validateAndNormalize(valid).specification();
        assertEquals("process_group", maps(normalized.get("deletions")).getFirst().get("type"));
        assertEquals("enable", maps(normalized.get("cs_actions")).getFirst().get("action"));
    }

    private void assertReasons(final ValidationReport report, final String... fragments) {
        assertFalse(report.valid());
        for (String fragment : fragments) {
            assertTrue(report.issues().stream().anyMatch(issue -> issue.reason().contains(fragment)),
                    () -> "Missing reason fragment " + fragment + " in " + report.issues());
        }
    }

    private Map<String, Object> type(final String type) {
        return Map.of("type", type, "bundle", bundle());
    }

    private Map<String, Object> serviceType(final String type, final String api) {
        return Map.of("type", type, "bundle", bundle(),
                "controllerServiceApis", List.of(Map.of("type", api)));
    }

    private Map<String, Object> bundle() {
        return Map.of("group", "g", "artifact", "a", "version", "1");
    }

    private Map<String, Object> processorDefinition(
            final String type,
            final boolean serial,
            final List<Map<String, Object>> properties,
            final List<String> relationships,
            final List<String> strategies) {
        final Map<String, Object> descriptors = new java.util.LinkedHashMap<>();
        properties.forEach(property -> descriptors.put(String.valueOf(property.get("name")), property));
        return Map.of(
                "type", type,
                "propertyDescriptors", descriptors,
                "supportedRelationships", relationships.stream().map(name -> Map.of("name", name)).toList(),
                "supportedSchedulingStrategies", strategies,
                "supportsDynamicProperties", false,
                "supportsDynamicRelationships", false,
                "triggerSerially", serial);
    }

    private Map<String, Object> property(
            final String name, final String displayName, final boolean required, final String defaultValue) {
        final Map<String, Object> value = new java.util.LinkedHashMap<>();
        value.put("name", name);
        value.put("displayName", displayName);
        value.put("required", required);
        value.put("dynamic", false);
        value.put("sensitive", false);
        if (defaultValue != null) {
            value.put("defaultValue", defaultValue);
        }
        return value;
    }

    private Map<String, Object> serviceProperty(final String name, final String displayName, final String api) {
        final Map<String, Object> value = property(name, displayName, true, null);
        value.put("typeProvidedByValue", Map.of("type", api));
        return value;
    }

    private Map<String, Object> dependentProperty(
            final String name,
            final String displayName,
            final String dependencyName,
            final List<String> dependentValues) {
        final Map<String, Object> value = property(name, displayName, true, null);
        final Map<String, Object> dependency = new java.util.LinkedHashMap<>();
        dependency.put("propertyName", dependencyName);
        dependency.put("dependentValues", dependentValues);
        value.put("dependencies", List.of(dependency));
        return value;
    }

    private Map<String, Object> allowable(
            final String name, final String displayName, final String... values) {
        final Map<String, Object> value = property(name, displayName, false, null);
        value.put("allowableValues", java.util.Arrays.stream(values)
                .map(allowed -> Map.<String, Object>of("value", allowed)).toList());
        return value;
    }

    private Map<String, Object> allowableWithDisplay(
            final String name, final String displayName, final String value, final String valueDisplayName) {
        final Map<String, Object> property = property(name, displayName, false, null);
        property.put("allowableValues", List.of(Map.of("value", value, "displayName", valueDisplayName)));
        return property;
    }

    private Map<String, Object> service(final String id, final String type) {
        return Map.of("id", id, "name", id, "type", type, "properties", Map.of());
    }

    private Map<String, Object> service(
            final String id, final String type, final Map<String, Object> properties) {
        return Map.of("id", id, "name", id, "type", type, "properties", properties);
    }

    private Map<String, Object> processor(
            final String id, final String type, final Map<String, Object> properties) {
        final Map<String, Object> value = new java.util.LinkedHashMap<>();
        value.put("id", id);
        value.put("type", type);
        value.put("config", properties);
        return value;
    }

    private Map<String, Object> connection(final String source, final String relationship) {
        return Map.of("from", source, "to", "destination", "relationships", List.of(relationship));
    }

    private Map<String, Object> spec(
            final List<Map<String, Object>> services,
            final List<Map<String, Object>> processors,
            final List<Map<String, Object>> connections) {
        return Map.of("controller_services", services, "processors", processors, "connections", connections);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> map(final Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> maps(final Object value) {
        return (List<Map<String, Object>>) value;
    }
}
