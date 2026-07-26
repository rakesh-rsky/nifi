package org.apache.nifi.copilot.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CapabilityDefinitionParserTest {
    private final CapabilityDefinitionParser parser = new CapabilityDefinitionParser();

    @Test
    void preservesInternalAndDisplayNamesAndControllerServiceApi() {
        final ProcessorCapability capability = parser.parseProcessor(
                type("example.Consume"),
                Map.of(
                        "type", "example.Consume",
                        "propertyDescriptors", Map.of("broker-uri", Map.of(
                                "name", "broker-uri",
                                "displayName", "Broker URI",
                                "required", true,
                                "sensitive", false,
                                "dynamic", false,
                                "allowableValues", List.of(Map.of("value", "tcp", "displayName", "TCP")),
                                "typeProvidedByValue", Map.of("type", "example.ConnectionApi"))),
                        "supportedRelationships", List.of(Map.of("name", "success")),
                        "supportedSchedulingStrategies", List.of("TIMER_DRIVEN"),
                        "supportsDynamicProperties", false,
                        "supportsDynamicRelationships", false,
                        "triggerSerially", true));

        final PropertyCapability property = capability.properties().get("broker-uri");
        assertEquals("Broker URI", property.displayName());
        assertEquals("example.ConnectionApi", property.requiredServiceApi().type());
        assertEquals("tcp", property.allowableValues().getFirst().value());
        assertEquals(Set.of("success"), capability.relationships());
    }

    @Test
    void acceptsProcessorDefinitionWithoutDescriptors() {
        final ProcessorCapability capability =
                parser.parseProcessor(type("example.NoProperties"), Map.of("type", "example.NoProperties"));

        assertEquals(Map.of(), capability.properties());
    }

    @Test
    void preservesPropertyDependencies() {
        final ProcessorCapability capability = parser.parseProcessor(
                type("example.Dependent"),
                Map.of("type", "example.Dependent", "propertyDescriptors", Map.of(
                        "mode", Map.of("name", "mode", "defaultValue", "off"),
                        "conditional", Map.of(
                                "name", "conditional",
                                "required", true,
                                "dependencies", List.of(Map.of(
                                        "propertyName", "mode",
                                        "propertyDisplayName", "Mode",
                                        "dependentValues", List.of("on")))))));

        assertEquals(Set.of("on"),
                capability.properties().get("conditional").dependencies().getFirst().dependentValues());
    }

    @Test
    void rejectsDescriptorKeyThatDiffersFromInternalName() {
        assertThrows(IllegalArgumentException.class, () -> parser.parseProcessor(
                type("example.Bad"),
                Map.of("type", "example.Bad", "propertyDescriptors",
                        Map.of("wrong", Map.of("name", "actual")))));
    }

    @Test
    void readsDocumentedControllerServiceApiNestedBundle() {
        final Map<String, Object> documented = new java.util.LinkedHashMap<>(type("example.Service"));
        documented.put("controllerServiceApis", List.of(Map.of(
                "type", "example.Api",
                "bundle", Map.of("group", "api-g", "artifact", "api-a", "version", "2"))));

        final ControllerServiceCapability capability = parser.parseControllerService(
                documented,
                Map.of("type", "example.Service", "propertyDescriptors", Map.of()));

        assertEquals(new BundleCoordinate("api-g", "api-a", "2"),
                capability.serviceApis().iterator().next().bundle());
    }

    private Map<String, Object> type(final String type) {
        return Map.of("type", type, "bundle",
                Map.of("group", "g", "artifact", "a", "version", "1"));
    }
}
