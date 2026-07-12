/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("internalNiFiClient")
public class InternalNiFiClient implements NiFiClientOperations {
    private static final Logger logger = LoggerFactory.getLogger(InternalNiFiClient.class);

    private final NiFiServiceFacade serviceFacade;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, DocumentedTypeDTO> typeCache = new HashMap<>();

    public InternalNiFiClient(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Override
    public String getProcessGroupId(final String pgId) {
        if (pgId != null && !"root".equals(pgId) && !pgId.isBlank()) {
            try {
                serviceFacade.getProcessGroup(pgId);
                return pgId;
            } catch (Exception ignored) {
            }
        }
        return serviceFacade.getProcessGroup("root").getId();
    }

    @Override
    public Map<String, Object> createProcessGroup(final String parentPgId, final String name, final double x, final double y) {
        final ProcessGroupDTO processGroup = new ProcessGroupDTO();
        final String newPgId = UUID.randomUUID().toString();
        processGroup.setId(newPgId);
        processGroup.setName(name);
        processGroup.setPosition(position(x, y));
        final ProcessGroupEntity created = serviceFacade.createProcessGroup(revision(0, newPgId), parentPgId, processGroup);
        final Map<String, Object> pgResult = new HashMap<>();
        pgResult.put("id", created.getId());
        pgResult.put("name", created.getComponent() != null ? created.getComponent().getName() : null);
        return pgResult;
    }

    @Override
    public Map<String, Object> getProcessGroupFlow(final String pgId) {
        return objectMapper.convertValue(serviceFacade.getProcessGroupFlow(pgId, false), new TypeReference<>() {
        });
    }

    @Override
    public void ensureTypeCache() {
        if (!typeCache.isEmpty()) {
            return;
        }
        final Set<DocumentedTypeDTO> types = serviceFacade.getProcessorTypes(null, null, null);
        if (types == null) {
            return;
        }
        for (DocumentedTypeDTO documentedType : types) {
            if (documentedType.getType() != null && documentedType.getBundle() != null) {
                typeCache.putIfAbsent(documentedType.getType(), documentedType);
            }
        }
    }

    @Override
    public Map<String, Object> createProcessor(
            final String pgId,
            final String processorType,
            final String name,
            final Double x,
            final Double y,
            final Map<String, Object> properties
    ) {
        final DocumentedTypeDTO resolved = resolveType(processorType);
        if (resolved == null) {
            throw new IllegalArgumentException("Processor type '" + processorType + "' is not available in this NiFi instance.");
        }

        final ProcessorDTO processor = new ProcessorDTO();
        final String newProcId = UUID.randomUUID().toString();
        processor.setId(newProcId);
        processor.setType(resolved.getType());
        processor.setBundle(resolved.getBundle());
        processor.setName(name);
        processor.setPosition(position(x, y));

        if (properties != null && !properties.isEmpty()) {
            final ProcessorConfigDTO config = new ProcessorConfigDTO();
            final Map<String, String> stringProps = new HashMap<>();
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                stringProps.put(property.getKey(), String.valueOf(property.getValue()));
            }
            config.setProperties(stringProps);
            processor.setConfig(config);
        }

        final ProcessorEntity created = serviceFacade.createProcessor(revision(0, newProcId), pgId, processor);
        final Map<String, Object> procResult = new HashMap<>();
        procResult.put("id", created.getId());
        procResult.put("name", created.getComponent() != null ? created.getComponent().getName() : null);
        procResult.put("type", created.getComponent() != null ? created.getComponent().getType() : null);
        return procResult;
    }

    @Override
    public void startProcessor(final String procId) {
        final ProcessorEntity entity = serviceFacade.getProcessor(procId);
        final ProcessorDTO processor = entity.getComponent();
        processor.setId(procId);
        processor.setState("RUNNING");
        serviceFacade.updateProcessor(revision(version(entity), procId), processor);
    }

    private void stopProcessor(final String procId) {
        final ProcessorEntity entity = serviceFacade.getProcessor(procId);
        final ProcessorDTO processor = entity.getComponent();
        processor.setId(procId);
        processor.setState("STOPPED");
        serviceFacade.updateProcessor(revision(version(entity), procId), processor);
    }

    @Override
    public void deleteProcessor(final String procId) {
        try {
            stopProcessor(procId);
        } catch (Exception ignored) {
        }
        final ProcessorEntity entity = serviceFacade.getProcessor(procId);
        serviceFacade.deleteProcessor(revision(version(entity), procId), procId);
    }

    @Override
    public boolean waitForProcessorValid(final String procId, final int timeoutSec) {
        final long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            final ProcessorEntity entity = serviceFacade.getProcessor(procId);
            final List<String> errors = entity.getComponent().getValidationErrors() == null
                    ? List.of()
                    : new ArrayList<>(entity.getComponent().getValidationErrors());
            if (errors.isEmpty()) {
                return true;
            }
            sleep(1500L);
        }
        return false;
    }

    @Override
    public void autoTerminateUnusedRelationships(final String procId, final Set<String> usedRelationships) {
        try {
            final ProcessorEntity entity = serviceFacade.getProcessor(procId);
            final ProcessorDTO processor = entity.getComponent();
            final Set<String> terminate = processor.getRelationships() == null
                    ? Set.of()
                    : processor.getRelationships().stream()
                    .map(relationship -> relationship.getName())
                    .filter(name -> !usedRelationships.contains(name))
                    .collect(Collectors.toSet());

            if (terminate.isEmpty()) {
                return;
            }

            final ProcessorConfigDTO config = processor.getConfig() == null ? new ProcessorConfigDTO() : processor.getConfig();
            config.setAutoTerminatedRelationships(terminate);
            processor.setConfig(config);
            processor.setId(procId);
            serviceFacade.updateProcessor(revision(version(entity), procId), processor);
        } catch (Exception e) {
            logger.warn("Could not auto-terminate relationships on {}: {}", procId, e.getMessage());
        }
    }

    @Override
    public Map<String, Object> createConnection(
            final String pgId,
            final String sourceId,
            final String sourceType,
            final String destId,
            final String destType,
            final List<String> relationships
    ) {
        final ConnectionDTO connection = new ConnectionDTO();
        final String newConnId = UUID.randomUUID().toString();
        connection.setId(newConnId);
        connection.setSource(connectable(sourceId, pgId, sourceType));
        connection.setDestination(connectable(destId, pgId, destType));
        connection.setSelectedRelationships(new HashSet<>(relationships));
        final ConnectionEntity created = serviceFacade.createConnection(revision(0, newConnId), pgId, connection);
        final Map<String, Object> connResult = new HashMap<>();
        connResult.put("id", created.getId());
        return connResult;
    }

    @Override
    public void deleteConnection(final String connId) {
        final ConnectionEntity entity = serviceFacade.getConnection(connId);
        serviceFacade.deleteConnection(revision(version(entity), connId), connId);
    }

    @Override
    public List<Map<String, Object>> listControllerServices(final String pgId) {
        final List<Map<String, Object>> out = new ArrayList<>();
        for (ControllerServiceEntity service : serviceFacade.getControllerServices(pgId, true, false, false)) {
            final ControllerServiceDTO component = service.getComponent();
            out.add(Map.of(
                    "id", service.getId(),
                    "name", component.getName(),
                    "type", component.getType(),
                    "state", component.getState() == null ? "DISABLED" : component.getState()
            ));
        }
        return out;
    }

    @Override
    public Map<String, Object> createControllerService(
            final String pgId,
            final String serviceType,
            final String name,
            final Map<String, Object> properties
    ) {
        final ControllerServiceDTO service = new ControllerServiceDTO();
        final String newCsId = UUID.randomUUID().toString();
        service.setId(newCsId);
        service.setType(serviceType);
        service.setName(name);
        if (properties != null && !properties.isEmpty()) {
            final Map<String, String> stringProps = new HashMap<>();
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                stringProps.put(property.getKey(), String.valueOf(property.getValue()));
            }
            service.setProperties(stringProps);
        }

        final ControllerServiceEntity created = serviceFacade.createControllerService(revision(0, newCsId), pgId, service);
        final Map<String, Object> csResult = new HashMap<>();
        csResult.put("id", created.getId());
        csResult.put("name", created.getComponent() != null ? created.getComponent().getName() : null);
        csResult.put("state", created.getComponent() != null ? created.getComponent().getState() : null);
        return csResult;
    }

    @Override
    public void enableControllerService(final String csId) {
        setControllerServiceState(csId, "ENABLED");
    }

    @Override
    public void disableControllerService(final String csId) {
        setControllerServiceState(csId, "DISABLED");
    }

    private void setControllerServiceState(final String csId, final String state) {
        final ControllerServiceEntity entity = serviceFacade.getControllerService(csId, false);
        final ControllerServiceDTO service = entity.getComponent();
        service.setId(csId);
        service.setState(state);
        serviceFacade.updateControllerService(revision(version(entity), csId), service);
        waitForControllerServiceState(csId, state, 60);
    }

    private boolean waitForControllerServiceState(final String csId, final String targetState, final int timeoutSec) {
        final long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            final ControllerServiceEntity entity = serviceFacade.getControllerService(csId, false);
            final String state = entity.getComponent().getState();
            if (targetState.equals(state)) {
                return true;
            }
            sleep(2000L);
        }
        return false;
    }

    @Override
    public void deleteControllerService(final String csId) {
        try {
            disableControllerService(csId);
        } catch (Exception ignored) {
        }
        final ControllerServiceEntity entity = serviceFacade.getControllerService(csId, false);
        serviceFacade.deleteControllerService(revision(version(entity), csId), csId);
    }

    @Override
    public Map<String, Object> createParameterContext(final String name, final Map<String, String> parameters, final String description) {
        final ParameterContextDTO context = new ParameterContextDTO();
        final String newPcId = UUID.randomUUID().toString();
        context.setId(newPcId);
        context.setName(name);
        context.setDescription(description == null ? "" : description);

        final Set<ParameterEntity> contextParameters = new HashSet<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            final ParameterDTO parameter = new ParameterDTO();
            parameter.setName(entry.getKey());
            parameter.setValue(entry.getValue());
            parameter.setSensitive(false);
            parameter.setDescription("");

            final ParameterEntity parameterEntity = new ParameterEntity();
            parameterEntity.setParameter(parameter);
            contextParameters.add(parameterEntity);
        }
        context.setParameters(contextParameters);

        final ParameterContextEntity created = serviceFacade.createParameterContext(revision(0, newPcId), context);
        final Map<String, Object> pcResult = new HashMap<>();
        pcResult.put("id", created.getId());
        pcResult.put("name", created.getComponent() != null ? created.getComponent().getName() : null);
        return pcResult;
    }

    @Override
    public void bindParameterContextToProcessGroup(final String pgId, final String pcId) {
        final ProcessGroupEntity entity = serviceFacade.getProcessGroup(pgId);
        final ProcessGroupDTO processGroup = entity.getComponent();
        processGroup.setId(pgId);

        final ParameterContextReferenceDTO reference = new ParameterContextReferenceDTO();
        reference.setId(pcId);
        final ParameterContextReferenceEntity referenceEntity = new ParameterContextReferenceEntity();
        referenceEntity.setId(pcId);
        referenceEntity.setComponent(reference);
        processGroup.setParameterContext(referenceEntity);

        serviceFacade.updateProcessGroup(revision(version(entity), pgId), processGroup);
    }

    @Override
    public void deleteParameterContext(final String pcId) {
        final ParameterContextEntity entity = serviceFacade.getParameterContext(pcId, false, NiFiUserUtils.getNiFiUser());
        serviceFacade.deleteParameterContext(revision(version(entity), pcId), pcId);
    }

    @Override
    public Map<String, Object> snapshotProcessGroup(final String pgId) {
        final Map<String, Object> flowData = getProcessGroupFlow(pgId);
        final Map<String, Object> processGroupFlow = mapOrEmpty(flowData.get("processGroupFlow"));
        final Map<String, Object> flow = mapOrEmpty(processGroupFlow.get("flow"));
        return Map.of(
                "pg_id", pgId,
                "processors", flow.getOrDefault("processors", List.of()),
                "connections", flow.getOrDefault("connections", List.of()),
                "processGroups", flow.getOrDefault("processGroups", List.of())
        );
    }

    @Override
    public void restoreFromSnapshot(final Map<String, Object> snapshot) {
        final String pgId = String.valueOf(snapshot.get("pg_id"));
        final Set<String> snapProcIds = extractIds(listOfMap(snapshot.get("processors")));
        final Set<String> snapConnIds = extractIds(listOfMap(snapshot.get("connections")));
        final Set<String> snapChildPgIds = extractIds(listOfMap(snapshot.get("processGroups")));

        final Map<String, Object> currentFlow = getProcessGroupFlow(pgId);
        final Map<String, Object> current = mapOrEmpty(mapOrEmpty(currentFlow.get("processGroupFlow")).get("flow"));

        for (Map<String, Object> processor : listOfMap(current.get("processors"))) {
            final String id = String.valueOf(processor.get("id"));
            if (!snapProcIds.contains(id)) {
                try {
                    deleteProcessor(id);
                } catch (Exception ignored) {
                }
            }
        }
        for (Map<String, Object> connection : listOfMap(current.get("connections"))) {
            final String id = String.valueOf(connection.get("id"));
            if (!snapConnIds.contains(id)) {
                try {
                    deleteConnection(id);
                } catch (Exception ignored) {
                }
            }
        }
        for (Map<String, Object> child : listOfMap(current.get("processGroups"))) {
            final String id = String.valueOf(child.get("id"));
            if (!snapChildPgIds.contains(id)) {
                try {
                    final ProcessGroupEntity processGroup = serviceFacade.getProcessGroup(id);
                    serviceFacade.deleteProcessGroup(revision(version(processGroup), id), id);
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Override
    public List<double[]> getOccupiedPositions(final String pgId) {
        final Map<String, Object> flowData = getProcessGroupFlow(pgId);
        final Map<String, Object> flow = mapOrEmpty(mapOrEmpty(flowData.get("processGroupFlow")).get("flow"));
        final List<double[]> positions = new ArrayList<>();
        for (Map<String, Object> processor : listOfMap(flow.get("processors"))) {
            final Map<String, Object> pos = mapOrEmpty(processor.get("position"));
            positions.add(new double[]{NiFiClientOperations.doubleValue(pos.get("x")), NiFiClientOperations.doubleValue(pos.get("y"))});
        }
        for (Map<String, Object> processGroup : listOfMap(flow.get("processGroups"))) {
            final Map<String, Object> pos = mapOrEmpty(processGroup.get("position"));
            positions.add(new double[]{NiFiClientOperations.doubleValue(pos.get("x")), NiFiClientOperations.doubleValue(pos.get("y"))});
        }
        return positions;
    }

    private static Revision revision(final long version, final String componentId) {
        return new Revision(version, UUID.randomUUID().toString(), componentId);
    }

    private static long version(final org.apache.nifi.web.api.entity.ComponentEntity entity) {
        if (entity.getRevision() == null || entity.getRevision().getVersion() == null) {
            return 0L;
        }
        return entity.getRevision().getVersion();
    }

    private static PositionDTO position(final Double x, final Double y) {
        final PositionDTO position = new PositionDTO();
        position.setX(x == null ? 0D : x);
        position.setY(y == null ? 0D : y);
        return position;
    }

    private static ConnectableDTO connectable(final String id, final String groupId, final String type) {
        final ConnectableDTO connectable = new ConnectableDTO();
        connectable.setId(id);
        connectable.setGroupId(groupId);
        connectable.setType(type);
        return connectable;
    }

    private DocumentedTypeDTO resolveType(final String processorType) {
        ensureTypeCache();
        if (typeCache.containsKey(processorType)) {
            return typeCache.get(processorType);
        }
        final String simple = processorType.substring(processorType.lastIndexOf('.') + 1).toLowerCase();
        for (Map.Entry<String, DocumentedTypeDTO> entry : typeCache.entrySet()) {
            final String keySimple = entry.getKey().substring(entry.getKey().lastIndexOf('.') + 1).toLowerCase();
            if (keySimple.equals(simple)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static Set<String> extractIds(final List<Map<String, Object>> entities) {
        return entities.stream().map(entity -> String.valueOf(entity.get("id"))).collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapOrEmpty(final Object in) {
        if (in instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> listOfMap(final Object in) {
        if (!(in instanceof List<?> list)) {
            return List.of();
        }
        final List<Map<String, Object>> output = new ArrayList<>();
        for (Object value : list) {
            if (value instanceof Map<?, ?> map) {
                output.add((Map<String, Object>) map);
            }
        }
        return output;
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
