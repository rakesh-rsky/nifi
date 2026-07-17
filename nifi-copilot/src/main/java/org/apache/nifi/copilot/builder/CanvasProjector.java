package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.NiFiEntitySupport.componentMap;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.componentValueOrDefault;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.effectiveFlow;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.entityId;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.transmissionState;
import static org.apache.nifi.copilot.builder.NiFiEntitySupport.valueOrDefault;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrEmpty;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CanvasProjector {
    private static final Logger logger =
            LoggerFactory.getLogger("org.apache.nifi.copilot.builder.FlowBuilder");

    Map<String, Object> readCanvas(
            final NiFiClientOperations nifi,
            final String processGroupId) {
        final String pgId = nifi.getProcessGroupId(processGroupId);
        final Map<String, Object> flowData = nifi.getProcessGroupFlow(pgId);
        final Map<String, Object> flow = effectiveFlow(flowData);
        final List<Map<String, Object>> processors = new ArrayList<>();
        for (Map<String, Object> entity : listOfMap(flow.get("processors"))) {
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final String id = entityId(entity);
            if (id == null) {
                logger.warn("Omitting canvas processor without a NiFi component ID");
                continue;
            }
            final Map<String, Object> position = componentMap(entity, component, "position");
            final Map<String, Object> projected = new LinkedHashMap<>();
            projected.put("nifi_id", id);
            projected.put("spec_id", id);
            projected.put("name", componentValueOrDefault(component, entity, "name", ""));
            projected.put("type", componentValueOrDefault(component, entity, "type", ""));
            projected.put("x", valueOrDefault(position, "x", 0));
            projected.put("y", valueOrDefault(position, "y", 0));
            projected.put("state", componentValueOrDefault(component, entity, "state", "STOPPED"));
            processors.add(projected);
        }
        final List<Map<String, Object>> controllerServices = new ArrayList<>();
        try {
            for (Map<String, Object> service : nifi.listControllerServices(pgId)) {
                final Map<String, Object> component = mapOrEmpty(service.get("component"));
                final String id = entityId(service);
                final Map<String, Object> projected = new LinkedHashMap<>();
                projected.put("nifi_id", id);
                if (id != null) {
                    projected.put("spec_id", id);
                }
                projected.put("name", componentValueOrDefault(component, service, "name", ""));
                projected.put("type", componentValueOrDefault(component, service, "type", ""));
                projected.put("state", componentValueOrDefault(component, service, "state", "DISABLED"));
                controllerServices.add(projected);
            }
        } catch (Exception e) {
            logger.warn("Could not read controller services: {}", e.getMessage());
        }
        final Map<String, Object> canvas = new LinkedHashMap<>();
        canvas.put("process_group_id", pgId);
        canvas.put("processors", processors);
        canvas.put("process_groups", projectProcessGroups(flow));
        canvas.put("controller_services", controllerServices);
        canvas.put("input_ports", projectPorts(flow, "inputPorts"));
        canvas.put("output_ports", projectPorts(flow, "outputPorts"));
        canvas.put("funnels", projectFunnels(flow));
        canvas.put("labels", projectLabels(flow));
        canvas.put("remote_process_groups", projectRemoteProcessGroups(flow));
        return canvas;
    }

    private static List<Map<String, Object>> projectProcessGroups(final Map<String, Object> flow) {
        final List<Map<String, Object>> projected = new ArrayList<>();
        for (Map<String, Object> entity : listOfMap(flow.get("processGroups"))) {
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final String id = entityId(entity);
            if (id == null) {
                continue;
            }
            final Map<String, Object> position = componentMap(entity, component, "position");
            final Map<String, Object> item = stableProjection(id);
            item.put("name", componentValueOrDefault(component, entity, "name", ""));
            item.put("x", valueOrDefault(position, "x", 0));
            item.put("y", valueOrDefault(position, "y", 0));
            projected.add(item);
        }
        return projected;
    }

    private static List<Map<String, Object>> projectPorts(
            final Map<String, Object> flow,
            final String collection) {
        final List<Map<String, Object>> projected = new ArrayList<>();
        for (Map<String, Object> entity : listOfMap(flow.get(collection))) {
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final String id = entityId(entity);
            if (id == null) {
                continue;
            }
            final Map<String, Object> position = componentMap(entity, component, "position");
            final Map<String, Object> item = stableProjection(id);
            item.put("name", componentValueOrDefault(component, entity, "name", ""));
            item.put("x", valueOrDefault(position, "x", 0));
            item.put("y", valueOrDefault(position, "y", 0));
            item.put("state", componentValueOrDefault(component, entity, "state", "STOPPED"));
            projected.add(item);
        }
        return projected;
    }

    private static List<Map<String, Object>> projectFunnels(final Map<String, Object> flow) {
        final List<Map<String, Object>> projected = new ArrayList<>();
        for (Map<String, Object> entity : listOfMap(flow.get("funnels"))) {
            final String id = entityId(entity);
            if (id == null) {
                continue;
            }
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final Map<String, Object> position = componentMap(entity, component, "position");
            final Map<String, Object> item = stableProjection(id);
            item.put("x", valueOrDefault(position, "x", 0));
            item.put("y", valueOrDefault(position, "y", 0));
            projected.add(item);
        }
        return projected;
    }

    private static List<Map<String, Object>> projectLabels(final Map<String, Object> flow) {
        final List<Map<String, Object>> projected = new ArrayList<>();
        for (Map<String, Object> entity : listOfMap(flow.get("labels"))) {
            final String id = entityId(entity);
            if (id == null) {
                continue;
            }
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final Map<String, Object> position = componentMap(entity, component, "position");
            final Map<String, Object> item = stableProjection(id);
            item.put("text", componentValueOrDefault(component, entity, "label", ""));
            item.put("x", valueOrDefault(position, "x", 0));
            item.put("y", valueOrDefault(position, "y", 0));
            item.put("style", valueOrDefault(component, "style", Map.of()));
            item.put("width", componentValueOrDefault(component, entity, "width", 0));
            item.put("height", componentValueOrDefault(component, entity, "height", 0));
            projected.add(item);
        }
        return projected;
    }

    private static List<Map<String, Object>> projectRemoteProcessGroups(
            final Map<String, Object> flow) {
        final List<Map<String, Object>> projected = new ArrayList<>();
        for (Map<String, Object> entity : listOfMap(flow.get("remoteProcessGroups"))) {
            final String id = entityId(entity);
            if (id == null) {
                continue;
            }
            final Map<String, Object> component = mapOrEmpty(entity.get("component"));
            final Map<String, Object> position = componentMap(entity, component, "position");
            final Map<String, Object> item = stableProjection(id);
            item.put("target_uri", componentValueOrDefault(component, entity, "targetUri", ""));
            item.put("x", valueOrDefault(position, "x", 0));
            item.put("y", valueOrDefault(position, "y", 0));
            item.put("transmission_state", transmissionState(entity));
            projected.add(item);
        }
        return projected;
    }

    private static Map<String, Object> stableProjection(final String id) {
        final Map<String, Object> projected = new LinkedHashMap<>();
        projected.put("nifi_id", id);
        projected.put("spec_id", id);
        return projected;
    }
}
