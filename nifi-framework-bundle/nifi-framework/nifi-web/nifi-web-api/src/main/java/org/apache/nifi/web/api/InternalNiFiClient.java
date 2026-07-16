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
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.copilot.service.NiFiAsyncRequestExecutor;
import org.apache.nifi.copilot.service.NiFiAsyncRequestState;
import org.apache.nifi.copilot.service.NiFiClientException;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.apache.nifi.copilot.service.NiFiRetryPolicy;
import org.apache.nifi.diagnostics.DiagnosticLevel;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.remote.util.ClusterUrlParser;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AboutDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.entity.AboutEntity;
import org.apache.nifi.web.api.entity.BulletinBoardEntity;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.ControllerStatusEntity;
import org.apache.nifi.web.api.entity.CreateActiveRequestEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LineageEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupUploadEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.SearchResultsEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.entity.StartVersionControlRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlComponentMappingEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.ObjectMapperResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("internalNiFiClient")
public class InternalNiFiClient implements NiFiClientOperations {
    private static final Logger logger = LoggerFactory.getLogger(InternalNiFiClient.class);
    private static final Set<String> PROCESSOR_MUTABLE_FIELDS = Set.of("name", "position", "style", "config", "bundle");

    private final NiFiServiceFacade serviceFacade;
    private final NiFiRetryPolicy retryPolicy;
    private final NiFiAsyncRequestExecutor asyncExecutor;
    private final ParameterContextResource parameterContextResource;
    private final ProvenanceResource provenanceResource;
    private final ControllerResource controllerResource;
    private final FlowResource flowResource;
    private final VersionsResource versionsResource;
    private final ProcessGroupResource processGroupResource;
    private final SystemDiagnosticsResource systemDiagnosticsResource;
    private final CountersResource countersResource;
    private final AuthenticationResource authenticationResource;
    private final ResourceResource resourceResource;
    private final AccessPolicyResource accessPolicyResource;
    private final TenantsResource tenantsResource;
    private final ObjectMapper objectMapper = new ObjectMapperResolver().getContext(Object.class);
    private final Map<String, DocumentedTypeDTO> typeCache = new HashMap<>();

    public InternalNiFiClient(final NiFiServiceFacade serviceFacade, final NiFiRetryPolicy retryPolicy,
            final NiFiAsyncRequestExecutor asyncExecutor, final ParameterContextResource parameterContextResource,
            final ProvenanceResource provenanceResource, final ControllerResource controllerResource,
            final FlowResource flowResource, final VersionsResource versionsResource,
            final ProcessGroupResource processGroupResource,
            final SystemDiagnosticsResource systemDiagnosticsResource,
            final CountersResource countersResource,
            final AuthenticationResource authenticationResource,
            final ResourceResource resourceResource,
            final AccessPolicyResource accessPolicyResource,
            final TenantsResource tenantsResource) {
        this.serviceFacade = serviceFacade;
        this.retryPolicy = retryPolicy;
        this.asyncExecutor = asyncExecutor;
        this.parameterContextResource = parameterContextResource;
        this.provenanceResource = provenanceResource;
        this.controllerResource = controllerResource;
        this.flowResource = flowResource;
        this.versionsResource = versionsResource;
        this.processGroupResource = processGroupResource;
        this.systemDiagnosticsResource = systemDiagnosticsResource;
        this.countersResource = countersResource;
        this.authenticationResource = authenticationResource;
        this.resourceResource = resourceResource;
        this.accessPolicyResource = accessPolicyResource;
        this.tenantsResource = tenantsResource;
    }

    @Override
    public String getProcessGroupId(final String pgId) {
        if (pgId != null && !"root".equals(pgId) && !pgId.isBlank()) {
            try {
                getProcessGroup(pgId);
                return pgId;
            } catch (RuntimeException e) {
                logger.warn("Process group {} could not be resolved; falling back to root: {}", pgId, e.getMessage());
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
        setProcessorRunStatus(procId, "RUNNING");
    }

    private void stopProcessor(final String procId) {
        setProcessorRunStatus(procId, "STOPPED");
    }

    /**
     * Updates the run status of a processor using the same minimal DTO semantics as
     * {@code PUT /processors/{id}/run-status}: only the processor id and target state
     * are set on the DTO, equivalent to ProcessorResource.createDTOWithDesiredRunStatus.
     * No dedicated run-status method exists on {@link NiFiServiceFacade};
     * the run-status resource endpoint uses the same minimal DTO approach used here.
     * Retries on {@link InvalidRevisionException} up to the configured maximum attempts.
     */
    private void setProcessorRunStatus(final String procId, final String state) {
        executeWithRevisionRetry("processor " + procId + " run-status", () -> {
            final ProcessorEntity entity = serviceFacade.getProcessor(procId);
            final long ver = strictVersion(entity);
            final ProcessorDTO dto = new ProcessorDTO();
            dto.setId(procId);
            dto.setState(state);
            serviceFacade.updateProcessor(revision(ver, procId), dto);
        });
    }

    @Override
    public void deleteProcessor(final String procId) {
        try {
            stopProcessor(procId);
        } catch (RuntimeException e) {
            logger.warn("Could not stop processor {} before deletion: {}", procId, e.getMessage());
        }
        executeWithRevisionRetry("delete processor " + procId, () -> {
            final ProcessorEntity entity = serviceFacade.getProcessor(procId);
            serviceFacade.deleteProcessor(revision(strictVersion(entity), procId), procId);
        });
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
            executeWithRevisionRetry("auto-terminate relationships " + procId, () -> {
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
                serviceFacade.updateProcessor(revision(strictVersion(entity), procId), processor);
            });
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
        executeWithRevisionRetry("delete connection " + connId, () -> {
            final ConnectionEntity entity = serviceFacade.getConnection(connId);
            serviceFacade.deleteConnection(revision(strictVersion(entity), connId), connId);
        });
    }

    @Override
    public List<Map<String, Object>> listControllerServices(final String pgId) {
        final List<Map<String, Object>> out = new ArrayList<>();
        for (ControllerServiceEntity service : serviceFacade.getControllerServices(pgId, true, false, false)) {
            final ControllerServiceDTO component = service.getComponent();
            final Map<String, Object> projected = new HashMap<>();
            projected.put("id", service.getId());
            projected.put("name", component.getName());
            projected.put("type", component.getType());
            projected.put("state", component.getState() == null ? "DISABLED" : component.getState());
            projected.put("parentGroupId", component.getParentGroupId());
            out.add(projected);
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
        executeWithRevisionRetry("controller service " + csId + " run-status", () -> {
            final ControllerServiceEntity entity = serviceFacade.getControllerService(csId, false);
            final ControllerServiceDTO service = entity.getComponent();
            service.setId(csId);
            service.setState(state);
            serviceFacade.updateControllerService(revision(strictVersion(entity), csId), service);
        });
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
        } catch (RuntimeException e) {
            logger.warn("Could not disable controller service {} before deletion: {}", csId, e.getMessage());
        }
        executeWithRevisionRetry("delete controller service " + csId, () -> {
            final ControllerServiceEntity entity = serviceFacade.getControllerService(csId, false);
            serviceFacade.deleteControllerService(revision(strictVersion(entity), csId), csId);
        });
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
        executeWithRevisionRetry("bind parameter context to process group " + pgId, () -> {
            final ProcessGroupEntity entity = serviceFacade.getProcessGroup(pgId);
            final ProcessGroupDTO processGroup = entity.getComponent();
            processGroup.setId(pgId);

            final ParameterContextReferenceDTO reference = new ParameterContextReferenceDTO();
            reference.setId(pcId);
            final ParameterContextReferenceEntity referenceEntity = new ParameterContextReferenceEntity();
            referenceEntity.setId(pcId);
            referenceEntity.setComponent(reference);
            processGroup.setParameterContext(referenceEntity);

            serviceFacade.updateProcessGroup(revision(strictVersion(entity), pgId), processGroup);
        });
    }

    @Override
    public void unbindParameterContextFromProcessGroup(final String pgId) {
        if (pgId == null || pgId.isBlank()) {
            throw new IllegalArgumentException("pgId must not be blank");
        }
        executeWithRevisionRetry("unbind parameter context from process group " + pgId, () -> {
            final ProcessGroupEntity entity = serviceFacade.getProcessGroup(pgId);
            final ProcessGroupDTO processGroup = entity.getComponent();
            processGroup.setId(pgId);
            processGroup.setParameterContext(null);
            serviceFacade.updateProcessGroup(revision(strictVersion(entity), pgId), processGroup);
        });
    }

    @Override
    public void deleteParameterContext(final String pcId) {
        executeWithRevisionRetry("delete parameter context " + pcId, () -> {
            final ParameterContextEntity entity = serviceFacade.getParameterContext(pcId, false, NiFiUserUtils.getNiFiUser());
            serviceFacade.deleteParameterContext(revision(strictVersion(entity), pcId), pcId);
        });
    }

    // =========================================================================
    // Parameter context management
    // =========================================================================

    @Override
    public List<Map<String, Object>> listParameterContexts() {
        return serviceFacade.getParameterContexts().stream()
                .map(e -> objectMapper.<Map<String, Object>>convertValue(e, new TypeReference<>() {}))
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, Object> updateParameterContext(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update parameter context " + id, () -> {
            final ParameterContextEntity entity = serviceFacade.getParameterContext(id, false, NiFiUserUtils.getNiFiUser());
            final long ver = strictVersion(entity);
            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", id);
            final ParameterContextDTO dto = objectMapper.convertValue(componentMap, ParameterContextDTO.class);
            dto.setId(id);
            serviceFacade.verifyUpdateParameterContext(dto, true);
            final ParameterContextEntity updated = serviceFacade.updateParameterContext(revision(ver, id), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> submitParameterContextUpdateRequest(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("submit parameter context update " + id, () -> {
            final ParameterContextEntity entity = serviceFacade.getParameterContext(id, false, NiFiUserUtils.getNiFiUser());
            final long ver = strictVersion(entity);

            final RevisionDTO revisionDTO = new RevisionDTO();
            revisionDTO.setVersion(ver);
            revisionDTO.setClientId(UUID.randomUUID().toString());

            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", id);
            final ParameterContextDTO dto = objectMapper.convertValue(componentMap, ParameterContextDTO.class);
            dto.setId(id);

            final ParameterContextEntity requestEntity = new ParameterContextEntity();
            requestEntity.setRevision(revisionDTO);
            requestEntity.setComponent(dto);

            final Response response = parameterContextResource.submitParameterContextUpdate(id, requestEntity);
            result.set(responseToMap("SUBMIT_PC_UPDATE", "/parameter-contexts/" + id + "/update-requests", response));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getParameterContextUpdateRequest(final String id, final String requestId) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final Response response = parameterContextResource.getParameterContextUpdate(id, requestId);
        return responseToMap("GET_PC_UPDATE", "/parameter-contexts/" + id + "/update-requests/" + requestId, response);
    }

    @Override
    public Map<String, Object> deleteParameterContextUpdateRequest(final String id, final String requestId) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final Response response = parameterContextResource.deleteUpdateRequest(Boolean.FALSE, id, requestId);
        return responseToMap("DELETE_PC_UPDATE",
                "/parameter-contexts/" + id + "/update-requests/" + requestId, response);
    }

    @Override
    public Map<String, Object> updateParameterContextLive(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitParameterContextUpdateRequest(id, updates);
                return NiFiAsyncRequestState.parse(response, "request", "requestId", "complete", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getParameterContextUpdateRequest(id, reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteParameterContextUpdateRequest(id, reqId);
            }
        });
    }

    // =========================================================================
    // FlowFile queue operations
    // =========================================================================

    @Override
    public Map<String, Object> submitFlowFileListingRequest(final String connectionId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        final String listingRequestId = UUID.randomUUID().toString();
        final ListingRequestDTO dto = serviceFacade.createFlowFileListingRequest(connectionId, listingRequestId);
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setListingRequest(dto);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getFlowFileListingRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final ListingRequestDTO dto = serviceFacade.getFlowFileListingRequest(connectionId, requestId);
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setListingRequest(dto);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> deleteFlowFileListingRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final ListingRequestDTO dto = serviceFacade.deleteFlowFileListingRequest(connectionId, requestId);
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setListingRequest(dto);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> listFlowFiles(final String connectionId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitFlowFileListingRequest(connectionId);
                return NiFiAsyncRequestState.parse(response, "listingRequest", "id", "finished", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getFlowFileListingRequest(connectionId, reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "listingRequest", "id", "finished", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "listingRequest", "id", "finished", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteFlowFileListingRequest(connectionId, reqId);
            }
        });
    }

    @Override
    public Map<String, Object> getFlowFileDetails(final String connectionId, final String flowFileUuid) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (flowFileUuid == null || flowFileUuid.isBlank()) {
            throw new IllegalArgumentException("flowFileUuid must not be blank");
        }
        final FlowFileDTO dto = serviceFacade.getFlowFile(connectionId, flowFileUuid);
        final FlowFileEntity entity = new FlowFileEntity();
        entity.setFlowFile(dto);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public InputStream downloadFlowFileContent(final String connectionId, final String flowFileUuid) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (flowFileUuid == null || flowFileUuid.isBlank()) {
            throw new IllegalArgumentException("flowFileUuid must not be blank");
        }
        final String uri = "nifi-api/flowfile-queues/" + connectionId + "/flowfiles/" + flowFileUuid + "/content";
        final DownloadableContent content = serviceFacade.getContent(connectionId, flowFileUuid, uri);
        return content.getContent();
    }

    @Override
    public Map<String, Object> submitQueueDropRequest(final String connectionId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive queue drop operation");
        }
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        final String dropRequestId = UUID.randomUUID().toString();
        final DropRequestDTO dto = serviceFacade.createFlowFileDropRequest(connectionId, dropRequestId);
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setDropRequest(dto);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getQueueDropRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final DropRequestDTO dto = serviceFacade.getFlowFileDropRequest(connectionId, requestId);
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setDropRequest(dto);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> deleteQueueDropRequest(final String connectionId, final String requestId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final DropRequestDTO dto = serviceFacade.deleteFlowFileDropRequest(connectionId, requestId);
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setDropRequest(dto);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> dropFlowFileQueue(final String connectionId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive queue drop operation");
        }
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitQueueDropRequest(connectionId, true);
                return NiFiAsyncRequestState.parse(response, "dropRequest", "id", "finished", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String reqId, final java.time.Duration remainingTime) {
                return getQueueDropRequest(connectionId, reqId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "dropRequest", "id", "finished", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "dropRequest", "id", "finished", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String reqId) {
                deleteQueueDropRequest(connectionId, reqId);
            }
        });
    }

    @Override
    public Map<String, Object> snapshotProcessGroup(final String pgId) {
        final Map<String, Object> flowData = getProcessGroupFlow(pgId);
        final Map<String, Object> processGroupFlow = mapOrEmpty(flowData.get("processGroupFlow"));
        final Map<String, Object> flow = mapOrEmpty(processGroupFlow.get("flow"));

        final ProcessGroupEntity pgEntity = serviceFacade.getProcessGroup(pgId);
        final ProcessGroupDTO pgComp = pgEntity.getComponent();
        String originalPcBindingId = null;
        if (pgComp != null && pgComp.getParameterContext() != null
                && pgComp.getParameterContext().getComponent() != null) {
            originalPcBindingId = pgComp.getParameterContext().getComponent().getId();
        }

        final Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("pg_id", pgId);
        snapshot.put("processors", flow.getOrDefault("processors", List.of()));
        snapshot.put("connections", flow.getOrDefault("connections", List.of()));
        snapshot.put("processGroups", flow.getOrDefault("processGroups", List.of()));
        snapshot.put("createdControllerServiceIds", new ArrayList<String>());
        snapshot.put("createdParameterContextIds", new ArrayList<String>());
        snapshot.put("originalPcBindingId", originalPcBindingId);
        return snapshot;
    }

    @Override
    public void restoreFromSnapshot(final Map<String, Object> snapshot) {
        final String pgId = String.valueOf(snapshot.get("pg_id"));
        final Set<String> snapProcIds = extractIds(listOfMap(snapshot.get("processors")));
        final Set<String> snapConnIds = extractIds(listOfMap(snapshot.get("connections")));
        final Set<String> snapChildPgIds = extractIds(listOfMap(snapshot.get("processGroups")));
        @SuppressWarnings("unchecked")
        final List<String> createdCsIds = (List<String>) snapshot.getOrDefault("createdControllerServiceIds", Collections.emptyList());
        @SuppressWarnings("unchecked")
        final List<String> createdPcIds = (List<String>) snapshot.getOrDefault("createdParameterContextIds", Collections.emptyList());

        final Map<String, Object> currentFlow = getProcessGroupFlow(pgId);
        final Map<String, Object> current = mapOrEmpty(mapOrEmpty(currentFlow.get("processGroupFlow")).get("flow"));

        final List<Exception> failures = new ArrayList<>();

        // Delete newly created connections first (must precede processor deletion)
        for (Map<String, Object> connection : listOfMap(current.get("connections"))) {
            final String id = String.valueOf(connection.get("id"));
            if (!snapConnIds.contains(id)) {
                try {
                    deleteConnection(id);
                } catch (Exception e) {
                    logger.warn("Rollback: failed to delete connection {}: {}", id, e.getMessage());
                    failures.add(e);
                }
            }
        }

        // Delete newly created processors
        for (Map<String, Object> processor : listOfMap(current.get("processors"))) {
            final String id = String.valueOf(processor.get("id"));
            if (!snapProcIds.contains(id)) {
                try {
                    deleteProcessor(id);
                } catch (Exception e) {
                    logger.warn("Rollback: failed to delete processor {}: {}", id, e.getMessage());
                    failures.add(e);
                }
            }
        }

        // Delete newly created child process groups
        for (Map<String, Object> child : listOfMap(current.get("processGroups"))) {
            final String id = String.valueOf(child.get("id"));
            if (!snapChildPgIds.contains(id)) {
                try {
                    executeWithRevisionRetry("delete child process group " + id, () -> {
                        final ProcessGroupEntity processGroup = serviceFacade.getProcessGroup(id);
                        serviceFacade.deleteProcessGroup(revision(strictVersion(processGroup), id), id);
                    });
                } catch (Exception e) {
                    logger.warn("Rollback: failed to delete child process group {}: {}", id, e.getMessage());
                    failures.add(e);
                }
            }
        }

        // Delete newly created controller services
        for (String id : createdCsIds) {
            try {
                deleteControllerService(id);
            } catch (Exception e) {
                logger.warn("Rollback: failed to delete controller service {}: {}", id, e.getMessage());
                failures.add(e);
            }
        }

        // Restore the target process group's original parameter context binding before deleting contexts
        if (snapshot.containsKey("originalPcBindingId")) {
            final String originalPcBindingId = (String) snapshot.get("originalPcBindingId");
            try {
                if (originalPcBindingId != null) {
                    bindParameterContextToProcessGroup(pgId, originalPcBindingId);
                } else {
                    unbindParameterContextFromProcessGroup(pgId);
                }
            } catch (Exception e) {
                logger.warn("Rollback: failed to restore parameter context binding for process group {}: {}", pgId, e.getMessage());
                failures.add(e);
            }
        }

        // Delete newly created parameter contexts
        for (String id : createdPcIds) {
            try {
                deleteParameterContext(id);
            } catch (Exception e) {
                logger.warn("Rollback: failed to delete parameter context {}: {}", id, e.getMessage());
                failures.add(e);
            }
        }

        if (!failures.isEmpty()) {
            final NiFiClientException ex = new NiFiClientException("ROLLBACK", pgId, 0, null, false,
                    "Rollback of process group " + pgId + " completed with " + failures.size() + " error(s)");
            failures.forEach(ex::addSuppressed);
            throw ex;
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

    @Override
    public Map<String, Object> getProcessGroup(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getProcessGroup(processGroupId), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> updateProcessGroup(final String processGroupId, final Map<String, Object> updates) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update process group " + processGroupId, () -> {
            final ProcessGroupEntity entity = serviceFacade.getProcessGroup(processGroupId);
            final long ver = strictVersion(entity);
            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", processGroupId);
            final ProcessGroupDTO dto = objectMapper.convertValue(componentMap, ProcessGroupDTO.class);
            dto.setId(processGroupId);
            final ProcessGroupEntity updated = serviceFacade.updateProcessGroup(revision(ver, processGroupId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public void deleteProcessGroup(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        executeWithRevisionRetry("delete process group " + processGroupId, () -> {
            final ProcessGroupEntity entity = serviceFacade.getProcessGroup(processGroupId);
            serviceFacade.deleteProcessGroup(revision(strictVersion(entity), processGroupId), processGroupId);
        });
    }

    @Override
    public List<Map<String, Object>> listProcessors(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final List<Map<String, Object>> out = new ArrayList<>();
        for (final ProcessorEntity entity : serviceFacade.getProcessors(processGroupId, false)) {
            out.add(objectMapper.convertValue(entity, new TypeReference<>() {}));
        }
        return out;
    }

    @Override
    public List<Map<String, Object>> listConnections(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final List<Map<String, Object>> out = new ArrayList<>();
        for (final ConnectionEntity entity : serviceFacade.getConnections(processGroupId)) {
            out.add(objectMapper.convertValue(entity, new TypeReference<>() {}));
        }
        return out;
    }

    @Override
    public List<Map<String, Object>> listChildProcessGroups(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final List<Map<String, Object>> out = new ArrayList<>();
        for (final ProcessGroupEntity entity : serviceFacade.getProcessGroups(processGroupId, ProcessGroupRecursivity.DIRECT_CHILDREN)) {
            out.add(objectMapper.convertValue(entity, new TypeReference<>() {}));
        }
        return out;
    }

    @Override
    public Map<String, Object> scheduleProcessGroup(final String processGroupId, final String state) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final String normalizedState = NiFiClientOperations.normalizeScheduleState(state);
        final boolean useEnableComponents = "ENABLED".equals(normalizedState) || "DISABLED".equals(normalizedState);
        final ScheduledState scheduledState = "ENABLED".equals(normalizedState)
                ? ScheduledState.STOPPED
                : ScheduledState.valueOf(normalizedState);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("schedule process group " + processGroupId, () -> {
            final Set<Revision> revisions = serviceFacade.getRevisionsFromGroup(processGroupId, group -> {
                final Set<String> ids = new HashSet<>();
                final Set<String> statelessGroupIds = new HashSet<>();
                group.findAllProcessGroups().stream()
                        .filter(child -> child.getExecutionEngine() == ExecutionEngine.STATELESS)
                        .filter(child -> child.getParent() == null || child.getParent().resolveExecutionEngine() == ExecutionEngine.STANDARD)
                        .forEach(child -> {
                            ids.add(child.getIdentifier());
                            statelessGroupIds.add(child.getIdentifier());
                            child.findAllProcessGroups().forEach(descendant -> statelessGroupIds.add(descendant.getIdentifier()));
                        });
                switch (normalizedState) {
                    case "RUNNING" -> {
                        group.findAllProcessors().stream()
                                .filter(ProcessGroup.START_PROCESSORS_FILTER)
                                .filter(processor -> !statelessGroupIds.contains(processor.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllInputPorts().stream()
                                .filter(ProcessGroup.START_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllOutputPorts().stream()
                                .filter(ProcessGroup.START_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                    }
                    case "STOPPED" -> {
                        group.findAllProcessors().stream()
                                .filter(ProcessGroup.STOP_PROCESSORS_FILTER)
                                .filter(processor -> !statelessGroupIds.contains(processor.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllInputPorts().stream()
                                .filter(ProcessGroup.STOP_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllOutputPorts().stream()
                                .filter(ProcessGroup.STOP_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                    }
                    case "ENABLED" -> {
                        group.findAllProcessors().stream()
                                .filter(ProcessGroup.ENABLE_PROCESSORS_FILTER)
                                .filter(processor -> !statelessGroupIds.contains(processor.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllInputPorts().stream()
                                .filter(ProcessGroup.ENABLE_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllOutputPorts().stream()
                                .filter(ProcessGroup.ENABLE_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                    }
                    default -> {
                        group.findAllProcessors().stream()
                                .filter(ProcessGroup.DISABLE_PROCESSORS_FILTER)
                                .filter(processor -> !statelessGroupIds.contains(processor.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllInputPorts().stream()
                                .filter(ProcessGroup.DISABLE_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                        group.findAllOutputPorts().stream()
                                .filter(ProcessGroup.DISABLE_PORTS_FILTER)
                                .filter(port -> !statelessGroupIds.contains(port.getProcessGroupIdentifier()))
                                .forEach(p -> ids.add(p.getIdentifier()));
                    }
                }
                return ids;
            });
            final Map<String, Revision> componentRevisions = revisions.stream()
                    .collect(Collectors.toMap(Revision::getComponentId, r -> r));
            final ScheduleComponentsEntity entity;
            if (useEnableComponents) {
                entity = serviceFacade.enableComponents(processGroupId, scheduledState, componentRevisions);
            } else {
                entity = serviceFacade.scheduleComponents(processGroupId, scheduledState, componentRevisions);
            }
            result.set(objectMapper.convertValue(entity, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getProcessorDiagnostics(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        final ProcessorDiagnosticsEntity entity = serviceFacade.getProcessorDiagnostics(processorId);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getProcessorState(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        final ComponentStateDTO state = serviceFacade.getProcessorState(processorId);
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> clearProcessorState(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        serviceFacade.verifyCanClearProcessorState(processorId);
        final ComponentStateDTO state = serviceFacade.clearProcessorState(processorId, null);
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> terminateProcessorThreads(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        serviceFacade.verifyTerminateProcessor(processorId);
        final ProcessorEntity entity = serviceFacade.terminateProcessor(processorId);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getProcessor(final String processorId) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getProcessor(processorId), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> updateProcessor(final String processorId, final Map<String, Object> updates) {
        if (processorId == null || processorId.isBlank()) {
            throw new IllegalArgumentException("processorId must not be blank");
        }
        if (updates == null) {
            throw new IllegalArgumentException("updates must not be null");
        }
        final Map<String, Object> component = buildProcessorComponent(processorId, updates);
        if (component.size() <= 1) {
            throw new IllegalArgumentException("updates must contain at least one of: name, position, style, config, bundle");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update processor " + processorId, () -> {
            final ProcessorEntity entity = serviceFacade.getProcessor(processorId);
            final long ver = strictVersion(entity);
            final ProcessorDTO dto = objectMapper.convertValue(component, ProcessorDTO.class);
            dto.setId(processorId);
            serviceFacade.verifyUpdateProcessor(dto);
            final ProcessorEntity updated = serviceFacade.updateProcessor(revision(ver, processorId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    private static Map<String, Object> buildProcessorComponent(final String processorId, final Map<String, Object> updates) {
        final Map<String, Object> component = new HashMap<>();
        component.put("id", processorId);
        for (final String field : PROCESSOR_MUTABLE_FIELDS) {
            if (updates.containsKey(field)) {
                component.put(field, updates.get(field));
            }
        }
        return component;
    }

    @Override
    public Map<String, Object> updateConnection(final String connectionId, final Map<String, Object> updates) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update connection " + connectionId, () -> {
            final ConnectionEntity entity = serviceFacade.getConnection(connectionId);
            final long ver = strictVersion(entity);
            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", connectionId);
            final ConnectionDTO dto = objectMapper.convertValue(componentMap, ConnectionDTO.class);
            dto.setId(connectionId);
            serviceFacade.verifyUpdateConnection(dto);
            final ConnectionEntity updated = serviceFacade.updateConnection(revision(ver, connectionId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getConnectionStatistics(final String connectionId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connectionId must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getConnectionStatistics(connectionId), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> updateControllerService(final String controllerServiceId, final Map<String, Object> updates) {
        if (controllerServiceId == null || controllerServiceId.isBlank()) {
            throw new IllegalArgumentException("controllerServiceId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update controller service " + controllerServiceId, () -> {
            final ControllerServiceEntity entity = serviceFacade.getControllerService(controllerServiceId, false);
            final long ver = strictVersion(entity);
            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", controllerServiceId);
            final ControllerServiceDTO dto = objectMapper.convertValue(componentMap, ControllerServiceDTO.class);
            dto.setId(controllerServiceId);
            serviceFacade.verifyUpdateControllerService(dto);
            final ControllerServiceEntity updated = serviceFacade.updateControllerService(revision(ver, controllerServiceId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getControllerServiceReferences(final String controllerServiceId) {
        if (controllerServiceId == null || controllerServiceId.isBlank()) {
            throw new IllegalArgumentException("controllerServiceId must not be blank");
        }
        final ControllerServiceReferencingComponentsEntity entity =
                serviceFacade.getControllerServiceReferencingComponents(controllerServiceId);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> updateControllerServiceReferences(final String controllerServiceId, final String state) {
        if (controllerServiceId == null || controllerServiceId.isBlank()) {
            throw new IllegalArgumentException("controllerServiceId must not be blank");
        }
        final String normalizedState = NiFiClientOperations.normalizeScheduleState(state);
        final ScheduledState scheduledState;
        final ControllerServiceState csState;
        if ("RUNNING".equals(normalizedState) || "STOPPED".equals(normalizedState)) {
            scheduledState = ScheduledState.valueOf(normalizedState);
            csState = null;
        } else {
            scheduledState = null;
            csState = ControllerServiceState.valueOf(normalizedState);
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update controller service references " + controllerServiceId, () -> {
            final ControllerServiceReferencingComponentsEntity refsEntity =
                    serviceFacade.getControllerServiceReferencingComponents(controllerServiceId);
            final Map<String, Object> refsMap = objectMapper.convertValue(refsEntity, new TypeReference<>() {});
            final List<Map<String, Object>> references =
                    listOfMap(refsMap.get("controllerServiceReferencingComponents"));
            final Map<String, Long> revisionVersions =
                    NiFiClientOperations.collectReferenceRevisions(references, normalizedState);
            final Map<String, Revision> revisions = new HashMap<>();
            for (final Map.Entry<String, Long> entry : revisionVersions.entrySet()) {
                revisions.put(entry.getKey(), revision(entry.getValue(), entry.getKey()));
            }
            serviceFacade.verifyUpdateControllerServiceReferencingComponents(
                    controllerServiceId, scheduledState, csState);
            final ControllerServiceReferencingComponentsEntity updated =
                    serviceFacade.updateControllerServiceReferencingComponents(
                            revisions, controllerServiceId, scheduledState, csState);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getFlowStatus() {
        final ControllerStatusDTO controllerStatus = serviceFacade.getControllerStatus();
        final ControllerStatusEntity entity = new ControllerStatusEntity();
        entity.setControllerStatus(controllerStatus);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getCurrentUser() {
        return objectMapper.convertValue(serviceFacade.getCurrentUser(), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getBulletinBoard(final Long after, final String sourceName, final String message,
            final String sourceId, final String groupId, final Integer limit) {
        if (after != null && after < 0) {
            throw new IllegalArgumentException("after must be >= 0 when supplied");
        }
        if (limit != null && limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1 when supplied");
        }
        final BulletinQueryDTO query = new BulletinQueryDTO();
        if (after != null) {
            query.setAfter(after);
        }
        if (sourceName != null) {
            query.setName(sourceName);
        }
        if (message != null) {
            query.setMessage(message);
        }
        if (sourceId != null) {
            query.setSourceId(sourceId);
        }
        if (groupId != null) {
            query.setGroupId(groupId);
        }
        if (limit != null) {
            query.setLimit(limit);
        }
        final BulletinBoardDTO bulletinBoard = serviceFacade.getBulletinBoard(query);
        final BulletinBoardEntity entity = new BulletinBoardEntity();
        entity.setBulletinBoard(bulletinBoard);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> searchFlow(final String query, final String activeGroupId) {
        final String q = query == null ? "" : query;
        final String a = activeGroupId == null ? "" : activeGroupId;
        final SearchResultsDTO results = serviceFacade.searchController(q, a);
        final SearchResultsEntity entity = new SearchResultsEntity();
        entity.setSearchResultsDTO(results);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getAboutInfo() {
        final AboutDTO aboutDTO = new AboutDTO();
        aboutDTO.setTitle("NiFi");
        aboutDTO.setTimezone(new Date());
        final Bundle frameworkBundle = NarClassLoadersHolder.getInstance().getFrameworkBundle();
        if (frameworkBundle != null) {
            final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();
            aboutDTO.setVersion(frameworkDetails.getCoordinate().getVersion());
            aboutDTO.setBuildTag(frameworkDetails.getBuildTag());
            aboutDTO.setBuildRevision(frameworkDetails.getBuildRevision());
            aboutDTO.setBuildBranch(frameworkDetails.getBuildBranch());
            aboutDTO.setBuildTimestamp(frameworkDetails.getBuildTimestampDate());
        }
        final AboutEntity entity = new AboutEntity();
        entity.setAbout(aboutDTO);
        return objectMapper.convertValue(entity, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> createRemoteProcessGroup(final String processGroupId, final String targetUri,
            final double x, final double y, final Map<String, Object> configuration) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (targetUri == null || targetUri.isBlank()) {
            throw new IllegalArgumentException("targetUri must not be blank");
        }
        ClusterUrlParser.parseClusterUrls(targetUri);
        final Map<String, Object> configMap = configuration == null ? Map.of() : configuration;
        final Map<String, Object> componentMap = new HashMap<>(configMap);
        componentMap.remove("id");
        componentMap.remove("revision");
        componentMap.remove("parentGroupId");
        componentMap.put("targetUris", targetUri);
        componentMap.put("position", Map.of("x", x, "y", y));
        final RemoteProcessGroupDTO dto = objectMapper.convertValue(componentMap, RemoteProcessGroupDTO.class);
        final String newId = UUID.randomUUID().toString();
        dto.setId(newId);
        dto.setTargetUri(targetUri);
        dto.setPosition(position(x, y));
        final RemoteProcessGroupEntity created = serviceFacade.createRemoteProcessGroup(revision(0, newId), processGroupId, dto);
        return objectMapper.convertValue(created, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getRemoteProcessGroup(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getRemoteProcessGroup(id), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> updateRemoteProcessGroup(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update remote process group " + id, () -> {
            final RemoteProcessGroupEntity entity = serviceFacade.getRemoteProcessGroup(id);
            final long ver = strictVersion(entity);
            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", id);
            final RemoteProcessGroupDTO dto = objectMapper.convertValue(componentMap, RemoteProcessGroupDTO.class);
            dto.setId(id);
            serviceFacade.verifyUpdateRemoteProcessGroup(dto);
            final RemoteProcessGroupEntity updated = serviceFacade.updateRemoteProcessGroup(revision(ver, id), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> setRemoteProcessGroupTransmission(final String id, final String state) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        final String normalized = NiFiClientOperations.normalizeRemoteTransmissionState(state);
        final boolean transmitting = "TRANSMITTING".equals(normalized);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("set remote process group transmission " + id, () -> {
            final RemoteProcessGroupEntity entity = serviceFacade.getRemoteProcessGroup(id);
            final long ver = strictVersion(entity);
            final RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
            dto.setId(id);
            dto.setTransmitting(transmitting);
            serviceFacade.verifyUpdateRemoteProcessGroup(dto);
            final RemoteProcessGroupEntity updated = serviceFacade.updateRemoteProcessGroup(revision(ver, id), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public void deleteRemoteProcessGroup(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        setRemoteProcessGroupTransmission(id, "STOPPED");
        executeWithRevisionRetry("delete remote process group " + id, () -> {
            final RemoteProcessGroupEntity entity = serviceFacade.getRemoteProcessGroup(id);
            serviceFacade.verifyDeleteRemoteProcessGroup(id);
            serviceFacade.deleteRemoteProcessGroup(revision(strictVersion(entity), id), id);
        });
    }

    @Override
    public Map<String, Object> createInputPort(final String processGroupId, final String name, final double x, final double y) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        final PortDTO port = new PortDTO();
        final String newId = UUID.randomUUID().toString();
        port.setId(newId);
        port.setName(name);
        port.setPosition(position(x, y));
        final PortEntity created = serviceFacade.createInputPort(revision(0, newId), processGroupId, port);
        return objectMapper.convertValue(created, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getInputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getInputPort(portId), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> setInputPortRunStatus(final String portId, final String state) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        final String normalized = NiFiClientOperations.normalizePortRunStatus(state);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("set input port run status " + portId, () -> {
            final PortEntity entity = serviceFacade.getInputPort(portId);
            final long ver = strictVersion(entity);
            final PortDTO dto = new PortDTO();
            dto.setId(portId);
            dto.setState(normalized);
            serviceFacade.verifyUpdateInputPort(dto);
            final PortEntity updated = serviceFacade.updateInputPort(revision(ver, portId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public void deleteInputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        executeWithRevisionRetry("delete input port " + portId, () -> {
            final PortEntity entity = serviceFacade.getInputPort(portId);
            serviceFacade.verifyDeleteInputPort(portId);
            serviceFacade.deleteInputPort(revision(strictVersion(entity), portId), portId);
        });
    }

    @Override
    public Map<String, Object> createOutputPort(final String processGroupId, final String name, final double x, final double y) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        final PortDTO port = new PortDTO();
        final String newId = UUID.randomUUID().toString();
        port.setId(newId);
        port.setName(name);
        port.setPosition(position(x, y));
        final PortEntity created = serviceFacade.createOutputPort(revision(0, newId), processGroupId, port);
        return objectMapper.convertValue(created, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getOutputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getOutputPort(portId), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> setOutputPortRunStatus(final String portId, final String state) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        final String normalized = NiFiClientOperations.normalizePortRunStatus(state);
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("set output port run status " + portId, () -> {
            final PortEntity entity = serviceFacade.getOutputPort(portId);
            final long ver = strictVersion(entity);
            final PortDTO dto = new PortDTO();
            dto.setId(portId);
            dto.setState(normalized);
            serviceFacade.verifyUpdateOutputPort(dto);
            final PortEntity updated = serviceFacade.updateOutputPort(revision(ver, portId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public void deleteOutputPort(final String portId) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("portId must not be blank");
        }
        executeWithRevisionRetry("delete output port " + portId, () -> {
            final PortEntity entity = serviceFacade.getOutputPort(portId);
            serviceFacade.verifyDeleteOutputPort(portId);
            serviceFacade.deleteOutputPort(revision(strictVersion(entity), portId), portId);
        });
    }

    @Override
    public Map<String, Object> createLabel(final String processGroupId, final String text, final double x, final double y,
            final Map<String, String> style, final Double width, final Double height) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (text == null) {
            throw new IllegalArgumentException("text must not be null");
        }
        final LabelDTO label = new LabelDTO();
        final String newId = UUID.randomUUID().toString();
        label.setId(newId);
        label.setLabel(text);
        label.setPosition(position(x, y));
        label.setStyle(style == null ? Map.of() : style);
        if (width != null) {
            label.setWidth(width);
        }
        if (height != null) {
            label.setHeight(height);
        }
        final LabelEntity created = serviceFacade.createLabel(revision(0, newId), processGroupId, label);
        return objectMapper.convertValue(created, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getLabel(final String labelId) {
        if (labelId == null || labelId.isBlank()) {
            throw new IllegalArgumentException("labelId must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getLabel(labelId), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> updateLabel(final String labelId, final Map<String, Object> updates) {
        if (labelId == null || labelId.isBlank()) {
            throw new IllegalArgumentException("labelId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update label " + labelId, () -> {
            final LabelEntity entity = serviceFacade.getLabel(labelId);
            final long ver = strictVersion(entity);
            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", labelId);
            final LabelDTO dto = objectMapper.convertValue(componentMap, LabelDTO.class);
            dto.setId(labelId);
            final LabelEntity updated = serviceFacade.updateLabel(revision(ver, labelId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public void deleteLabel(final String labelId) {
        if (labelId == null || labelId.isBlank()) {
            throw new IllegalArgumentException("labelId must not be blank");
        }
        executeWithRevisionRetry("delete label " + labelId, () -> {
            final LabelEntity entity = serviceFacade.getLabel(labelId);
            serviceFacade.deleteLabel(revision(strictVersion(entity), labelId), labelId);
        });
    }

    @Override
    public Map<String, Object> createFunnel(final String processGroupId, final double x, final double y) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final FunnelDTO funnel = new FunnelDTO();
        final String newId = UUID.randomUUID().toString();
        funnel.setId(newId);
        funnel.setPosition(position(x, y));
        final FunnelEntity created = serviceFacade.createFunnel(revision(0, newId), processGroupId, funnel);
        return objectMapper.convertValue(created, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> getFunnel(final String funnelId) {
        if (funnelId == null || funnelId.isBlank()) {
            throw new IllegalArgumentException("funnelId must not be blank");
        }
        return objectMapper.convertValue(serviceFacade.getFunnel(funnelId), new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> updateFunnel(final String funnelId, final Map<String, Object> updates) {
        if (funnelId == null || funnelId.isBlank()) {
            throw new IllegalArgumentException("funnelId must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update funnel " + funnelId, () -> {
            final FunnelEntity entity = serviceFacade.getFunnel(funnelId);
            final long ver = strictVersion(entity);
            final Map<String, Object> componentMap = new HashMap<>(updates);
            componentMap.remove("id");
            componentMap.remove("revision");
            componentMap.put("id", funnelId);
            final FunnelDTO dto = objectMapper.convertValue(componentMap, FunnelDTO.class);
            dto.setId(funnelId);
            final FunnelEntity updated = serviceFacade.updateFunnel(revision(ver, funnelId), dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public void deleteFunnel(final String funnelId) {
        if (funnelId == null || funnelId.isBlank()) {
            throw new IllegalArgumentException("funnelId must not be blank");
        }
        executeWithRevisionRetry("delete funnel " + funnelId, () -> {
            final FunnelEntity entity = serviceFacade.getFunnel(funnelId);
            serviceFacade.verifyDeleteFunnel(funnelId);
            serviceFacade.deleteFunnel(revision(strictVersion(entity), funnelId), funnelId);
        });
    }

    @Override
    public Map<String, Object> createSnippet(final String parentProcessGroupId, final Map<String, Object> componentSelections) {
        if (parentProcessGroupId == null || parentProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("parentProcessGroupId must not be blank");
        }
        if (componentSelections == null || componentSelections.isEmpty()) {
            throw new IllegalArgumentException("componentSelections must not be null or empty");
        }
        final Set<String> allowedKeys = Set.of("processGroups", "remoteProcessGroups", "processors",
                "inputPorts", "outputPorts", "connections", "labels", "funnels");
        final Map<String, Object> sanitized = new HashMap<>(componentSelections);
        sanitized.remove("id");
        sanitized.remove("uri");
        sanitized.remove("parentGroupId");
        sanitized.keySet().retainAll(allowedKeys);
        if (sanitized.isEmpty()) {
            throw new IllegalArgumentException("componentSelections must contain at least one allowed component type key");
        }
        sanitized.put("parentGroupId", parentProcessGroupId);
        final SnippetDTO dto = objectMapper.convertValue(sanitized, SnippetDTO.class);
        dto.setId(UUID.randomUUID().toString());
        dto.setParentGroupId(parentProcessGroupId);
        final SnippetEntity created = serviceFacade.createSnippet(dto);
        return objectMapper.convertValue(created, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> moveSnippet(final String snippetId, final String destinationProcessGroupId) {
        if (snippetId == null || snippetId.isBlank()) {
            throw new IllegalArgumentException("snippetId must not be blank");
        }
        if (destinationProcessGroupId == null || destinationProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("destinationProcessGroupId must not be blank");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("move snippet " + snippetId, () -> {
            final Set<Revision> revisions = serviceFacade.getRevisionsFromSnippet(snippetId);
            final Set<String> affectedIds = revisions.stream().map(Revision::getComponentId).collect(Collectors.toSet());
            final SnippetDTO dto = new SnippetDTO();
            dto.setId(snippetId);
            dto.setParentGroupId(destinationProcessGroupId);
            serviceFacade.verifyUpdateSnippet(dto, affectedIds);
            final SnippetEntity updated = serviceFacade.updateSnippet(revisions, dto);
            result.set(objectMapper.convertValue(updated, new TypeReference<>() {}));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> copySnippet(final String snippetId, final String destinationProcessGroupId,
            final double originX, final double originY) {
        if (snippetId == null || snippetId.isBlank()) {
            throw new IllegalArgumentException("snippetId must not be blank");
        }
        if (destinationProcessGroupId == null || destinationProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("destinationProcessGroupId must not be blank");
        }
        final FlowEntity created = serviceFacade.copySnippet(destinationProcessGroupId, snippetId, originX, originY, null);
        return objectMapper.convertValue(created, new TypeReference<>() {});
    }

    @Override
    public Map<String, Object> deleteSnippet(final String snippetId) {
        if (snippetId == null || snippetId.isBlank()) {
            throw new IllegalArgumentException("snippetId must not be blank");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("delete snippet " + snippetId, () -> {
            final Set<Revision> revisions = serviceFacade.getRevisionsFromSnippet(snippetId);
            final Set<String> affectedIds = revisions.stream().map(Revision::getComponentId).collect(Collectors.toSet());
            serviceFacade.verifyDeleteSnippet(snippetId, affectedIds);
            final SnippetEntity deleted = serviceFacade.deleteSnippet(revisions, snippetId);
            result.set(objectMapper.convertValue(deleted, new TypeReference<>() {}));
        });
        return result.get();
    }

    private static Revision revision(final long version, final String componentId) {
        return new Revision(version, UUID.randomUUID().toString(), componentId);
    }

    /**
     * Extracts the revision version from an entity for update/delete operations.
     * Throws {@link IllegalStateException} if the revision or version is missing,
     * as defaulting to version 0 for non-creation operations would corrupt the
     * optimistic concurrency control.
     *
     * @param entity the component entity
     * @return the non-negative revision version
     * @throws IllegalStateException if the revision or version is null
     */
    private static long strictVersion(final org.apache.nifi.web.api.entity.ComponentEntity entity) {
        if (entity.getRevision() == null) {
            throw new IllegalStateException("Revision is required for update/delete operations but entity has no revision: " + entity.getId());
        }
        if (entity.getRevision().getVersion() == null) {
            throw new IllegalStateException("Revision version is required for update/delete operations but was null for entity: " + entity.getId());
        }
        final long version = entity.getRevision().getVersion();
        if (version < 0) {
            throw new IllegalStateException("Revision version must be non-negative for update/delete operations: " + entity.getId());
        }
        return version;
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

    private static boolean isBlankOrNull(final Object v) {
        if (v == null) {
            return true;
        }
        return String.valueOf(v).isBlank();
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

    /**
     * Executes a revision-bearing mutation, retrying on {@link InvalidRevisionException}
     * up to {@link NiFiRetryPolicy#maxAttempts()} times. The operation lambda must
     * re-read the latest entity state on each call; no stale state is replayed.
     * No other exceptions are retried.
     *
     * @param description human-readable description for debug logging
     * @param operation   lambda performing a fresh read + mutation
     * @throws InvalidRevisionException after all attempts are exhausted
     */
    private void executeWithRevisionRetry(final String description, final Runnable operation) {
        final int maxAttempts = retryPolicy.maxAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                operation.run();
                return;
            } catch (InvalidRevisionException e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
                logger.debug("Revision conflict on {}, attempt {}/{}", description, attempt, maxAttempts);
                retryPolicy.sleep(retryPolicy.computeDelayMillis(attempt, -1L));
            }
        }
    }

    // =========================================================================
    // Provenance operations
    // =========================================================================

    @Override
    public Map<String, Object> submitProvenanceQuery(final Map<String, Object> request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        final Map<String, Object> wrapper = Map.of("provenance", Map.of("request", normalizedRequest));
        final ProvenanceEntity entity = objectMapper.convertValue(wrapper, ProvenanceEntity.class);
        final Response response = provenanceResource.submitProvenanceRequest(entity);
        return responseToMap("POST", "/provenance", response);
    }

    @Override
    public Map<String, Object> getProvenanceQuery(final String queryId, final String clusterNodeId,
            final boolean summarize, final boolean incrementalResults) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank()) ? clusterNodeId : null;
        final Response response = provenanceResource.getProvenance(effectiveNodeId, summarize, incrementalResults, queryId);
        return responseToMap("GET", "/provenance/" + queryId, response);
    }

    @Override
    public Map<String, Object> deleteProvenanceQuery(final String queryId, final String clusterNodeId) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank()) ? clusterNodeId : null;
        final Response response = provenanceResource.deleteProvenance(effectiveNodeId, queryId);
        return responseToMap("DELETE", "/provenance/" + queryId, response);
    }

    @Override
    public Map<String, Object> queryProvenance(final Map<String, Object> request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        final String clusterNodeId = extractNonblankString(normalizedRequest, "clusterNodeId");
        final boolean summarize = extractBoolean(normalizedRequest, "summarize", false);
        final boolean incrementalResults = extractBoolean(normalizedRequest, "incrementalResults", true);
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitProvenanceQuery(normalizedRequest);
                return NiFiAsyncRequestState.parse(response, "provenance", "id", "finished", null)
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String queryId, final java.time.Duration remainingTime) {
                return getProvenanceQuery(queryId, clusterNodeId, summarize, incrementalResults);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "provenance", "id", "finished", null)
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "provenance", "id", "finished", null)
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String queryId) {
                deleteProvenanceQuery(queryId, clusterNodeId);
            }
        });
    }

    @Override
    public Map<String, Object> submitLineageQuery(final Map<String, Object> request) {
        if (request == null || request.isEmpty()) {
            throw new IllegalArgumentException("request must not be null or empty");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        final Map<String, Object> wrapper = Map.of("lineage", Map.of("request", normalizedRequest));
        final LineageEntity entity = objectMapper.convertValue(wrapper, LineageEntity.class);
        final Response response = provenanceResource.submitLineageRequest(entity);
        return responseToMap("POST", "/provenance/lineage", response);
    }

    @Override
    public Map<String, Object> getLineageQuery(final String lineageId, final String clusterNodeId) {
        if (lineageId == null || lineageId.isBlank()) {
            throw new IllegalArgumentException("lineageId must not be blank");
        }
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank()) ? clusterNodeId : null;
        final Response response = provenanceResource.getLineage(effectiveNodeId, lineageId);
        return responseToMap("GET", "/provenance/lineage/" + lineageId, response);
    }

    @Override
    public Map<String, Object> deleteLineageQuery(final String lineageId, final String clusterNodeId) {
        if (lineageId == null || lineageId.isBlank()) {
            throw new IllegalArgumentException("lineageId must not be blank");
        }
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank()) ? clusterNodeId : null;
        final Response response = provenanceResource.deleteLineage(effectiveNodeId, lineageId);
        return responseToMap("DELETE", "/provenance/lineage/" + lineageId, response);
    }

    @Override
    public Map<String, Object> queryLineage(final Map<String, Object> request) {
        if (request == null || request.isEmpty()) {
            throw new IllegalArgumentException("request must not be null or empty");
        }
        final Map<String, Object> normalizedRequest = normalizeClusterNodeId(request);
        final String clusterNodeId = extractNonblankString(normalizedRequest, "clusterNodeId");
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitLineageQuery(normalizedRequest);
                return NiFiAsyncRequestState.parse(response, "lineage", "id", "finished", null)
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String lineageId, final java.time.Duration remainingTime) {
                return getLineageQuery(lineageId, clusterNodeId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "lineage", "id", "finished", null)
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "lineage", "id", "finished", null)
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String lineageId) {
                deleteLineageQuery(lineageId, clusterNodeId);
            }
        });
    }

    // =========================================================================
    // Registry, versioning, and process-group import/export operations
    // =========================================================================

    @Override
    public List<Map<String, Object>> listRegistryClients() {
        final Response response = controllerResource.getFlowRegistryClients();
        final Map<String, Object> entity = responseToMap("GET", "/controller/registry-clients", response);
        return listOfMap(entity.get("registries"));
    }

    @Override
    public Map<String, Object> createRegistryClient(final Map<String, Object> component) {
        if (component == null || component.isEmpty()) {
            throw new IllegalArgumentException("component must not be null or empty");
        }
        final Map<String, Object> sanitizedComponent = new HashMap<>(component);
        sanitizedComponent.remove("id");
        sanitizedComponent.remove("revision");

        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(UUID.randomUUID().toString());
        revision.setVersion(0L);

        final FlowRegistryClientEntity entity = new FlowRegistryClientEntity();
        entity.setRevision(revision);
        entity.setComponent(objectMapper.convertValue(sanitizedComponent, FlowRegistryClientDTO.class));

        final Response response = controllerResource.createFlowRegistryClient(entity);
        return responseToMap("POST", "/controller/registry-clients", response);
    }

    @Override
    public Map<String, Object> getRegistryClient(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        final Response response = controllerResource.getFlowRegistryClient(id);
        return responseToMap("GET", "/controller/registry-clients/" + id, response);
    }

    @Override
    public Map<String, Object> updateRegistryClient(final String id, final Map<String, Object> updates) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (updates == null || updates.isEmpty()) {
            throw new IllegalArgumentException("updates must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update registry client " + id, () -> {
            final FlowRegistryClientEntity current = serviceFacade.getRegistryClient(id);
            final long version = strictVersion(current);
            final Map<String, Object> sanitizedUpdates = new HashMap<>(updates);
            sanitizedUpdates.remove("id");
            sanitizedUpdates.remove("revision");
            sanitizedUpdates.put("id", id);

            final RevisionDTO revision = new RevisionDTO();
            revision.setVersion(version);
            revision.setClientId(UUID.randomUUID().toString());

            final FlowRegistryClientEntity entity = new FlowRegistryClientEntity();
            entity.setRevision(revision);
            entity.setComponent(objectMapper.convertValue(sanitizedUpdates, FlowRegistryClientDTO.class));

            final Response response = controllerResource.updateFlowRegistryClient(id, entity);
            result.set(responseToMap("PUT", "/controller/registry-clients/" + id, response));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> deleteRegistryClient(final String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("delete registry client " + id, () -> {
            final FlowRegistryClientEntity current = serviceFacade.getRegistryClient(id);
            final long version = strictVersion(current);
            final LongParameter versionParameter = new LongParameter(String.valueOf(version));
            final ClientIdParameter clientIdParameter = new ClientIdParameter(UUID.randomUUID().toString());
            final Response response = controllerResource.deleteFlowRegistryClient(versionParameter, clientIdParameter,
                    false, id);
            result.set(responseToMap("DELETE", "/controller/registry-clients/" + id, response));
        });
        return result.get();
    }

    @Override
    public List<Map<String, Object>> listRegistryBuckets(final String registryId, final String branch) {
        if (registryId == null || registryId.isBlank()) {
            throw new IllegalArgumentException("registryId must not be blank");
        }
        final Response response = flowResource.getBuckets(registryId, branch != null && !branch.isBlank() ? branch : null);
        final Map<String, Object> entity = responseToMap("GET", "/flow/registries/" + registryId + "/buckets", response);
        return listOfMap(entity.get("buckets"));
    }

    @Override
    public List<Map<String, Object>> listRegistryFlows(final String registryId, final String bucketId,
            final String branch) {
        if (registryId == null || registryId.isBlank()) {
            throw new IllegalArgumentException("registryId must not be blank");
        }
        if (bucketId == null || bucketId.isBlank()) {
            throw new IllegalArgumentException("bucketId must not be blank");
        }
        final Response response = flowResource.getFlows(registryId, bucketId,
                branch != null && !branch.isBlank() ? branch : null);
        final Map<String, Object> entity = responseToMap("GET",
                "/flow/registries/" + registryId + "/buckets/" + bucketId + "/flows", response);
        return listOfMap(entity.get("versionedFlows"));
    }

    @Override
    public Map<String, Object> getVersionInformation(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final Response response = versionsResource.getVersionInformation(processGroupId);
        return responseToMap("GET", "/versions/process-groups/" + processGroupId, response);
    }

    @Override
    public String acquireVersionRequest(final String processGroupId) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final CreateActiveRequestEntity entity = new CreateActiveRequestEntity();
        entity.setProcessGroupId(processGroupId);
        final Response response = versionsResource.createVersionControlRequest(entity);
        return responseToString("POST", "/versions/active-requests", response);
    }

    @Override
    public Map<String, Object> updateVersionRequestMapping(final String requestId, final String processGroupId,
            final Map<String, Object> versionControlInformation, final Map<String, Object> componentMapping) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        if (componentMapping == null) {
            throw new IllegalArgumentException("componentMapping must not be null");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("update version request mapping " + requestId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final Map<String, Object> processGroupRevision = mapOrEmpty(versionInfo.get("processGroupRevision"));
            final long version = NiFiClientOperations.strictRevisionVersion(processGroupRevision.get("version"));

            final RevisionDTO revision = new RevisionDTO();
            revision.setVersion(version);
            revision.setClientId(UUID.randomUUID().toString());

            final Map<String, Object> sanitizedVci = new HashMap<>(versionControlInformation);
            sanitizedVci.put("groupId", processGroupId);
            final VersionControlInformationDTO dto = objectMapper.convertValue(sanitizedVci,
                    VersionControlInformationDTO.class);
            dto.setGroupId(processGroupId);

            final VersionControlComponentMappingEntity entity = new VersionControlComponentMappingEntity();
            entity.setProcessGroupRevision(revision);
            entity.setVersionControlInformation(dto);
            entity.setVersionControlComponentMapping(objectMapper.convertValue(componentMapping,
                    new TypeReference<Map<String, String>>() {}));
            entity.setDisconnectedNodeAcknowledged(false);

            final Response response = versionsResource.updateVersionControlRequest(requestId, entity);
            result.set(responseToMap("PUT", "/versions/active-requests/" + requestId, response));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> releaseVersionRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final Response response = versionsResource.deleteVersionControlRequest(false, requestId);
        return responseToMap("DELETE", "/versions/active-requests/" + requestId, response);
    }

    @Override
    public Map<String, Object> startVersionControl(final String processGroupId, final Map<String, Object> versionedFlow) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionedFlow == null || versionedFlow.isEmpty()) {
            throw new IllegalArgumentException("versionedFlow must not be null or empty");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("start version control " + processGroupId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final Map<String, Object> processGroupRevision = mapOrEmpty(versionInfo.get("processGroupRevision"));
            final long version = NiFiClientOperations.strictRevisionVersion(processGroupRevision.get("version"));

            final RevisionDTO revision = new RevisionDTO();
            revision.setVersion(version);
            revision.setClientId(UUID.randomUUID().toString());

            final StartVersionControlRequestEntity entity = new StartVersionControlRequestEntity();
            entity.setProcessGroupRevision(revision);
            entity.setVersionedFlow(objectMapper.convertValue(new HashMap<>(versionedFlow), VersionedFlowDTO.class));
            entity.setDisconnectedNodeAcknowledged(false);

            final Response response = versionsResource.saveToFlowRegistry(processGroupId, entity);
            result.set(responseToMap("POST", "/versions/process-groups/" + processGroupId, response));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> submitVersionUpdate(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("submit version update " + processGroupId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final Map<String, Object> processGroupRevision = mapOrEmpty(versionInfo.get("processGroupRevision"));
            final long version = NiFiClientOperations.strictRevisionVersion(processGroupRevision.get("version"));

            final RevisionDTO revision = new RevisionDTO();
            revision.setVersion(version);
            revision.setClientId(UUID.randomUUID().toString());

            final Map<String, Object> sanitizedVci = new HashMap<>(versionControlInformation);
            sanitizedVci.put("groupId", processGroupId);
            final VersionControlInformationDTO dto = objectMapper.convertValue(sanitizedVci,
                    VersionControlInformationDTO.class);
            dto.setGroupId(processGroupId);

            final VersionControlInformationEntity entity = new VersionControlInformationEntity();
            entity.setProcessGroupRevision(revision);
            entity.setVersionControlInformation(dto);
            entity.setDisconnectedNodeAcknowledged(false);

            final Response response = versionsResource.initiateVersionControlUpdate(processGroupId, entity);
            result.set(responseToMap("POST", "/versions/update-requests/process-groups/" + processGroupId, response));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getVersionUpdateRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final Response response = versionsResource.getUpdateRequest(requestId);
        return responseToMap("GET", "/versions/update-requests/" + requestId, response);
    }

    @Override
    public Map<String, Object> deleteVersionUpdateRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final Response response = versionsResource.deleteUpdateRequest(false, requestId);
        return responseToMap("DELETE", "/versions/update-requests/" + requestId, response);
    }

    @Override
    public Map<String, Object> updateVersionedProcessGroup(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitVersionUpdate(processGroupId, versionControlInformation);
                return NiFiAsyncRequestState.parse(response, "request", "requestId", "complete", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String requestId, final java.time.Duration remainingTime) {
                return getVersionUpdateRequest(requestId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String requestId) {
                deleteVersionUpdateRequest(requestId);
            }
        });
    }

    @Override
    public Map<String, Object> submitVersionRevert(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        final AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        executeWithRevisionRetry("submit version revert " + processGroupId, () -> {
            final Map<String, Object> versionInfo = getVersionInformation(processGroupId);
            final Map<String, Object> processGroupRevision = mapOrEmpty(versionInfo.get("processGroupRevision"));
            final long version = NiFiClientOperations.strictRevisionVersion(processGroupRevision.get("version"));

            final RevisionDTO revision = new RevisionDTO();
            revision.setVersion(version);
            revision.setClientId(UUID.randomUUID().toString());

            final Map<String, Object> sanitizedVci = new HashMap<>(versionControlInformation);
            sanitizedVci.put("groupId", processGroupId);
            final VersionControlInformationDTO dto = objectMapper.convertValue(sanitizedVci,
                    VersionControlInformationDTO.class);
            dto.setGroupId(processGroupId);

            final VersionControlInformationEntity entity = new VersionControlInformationEntity();
            entity.setProcessGroupRevision(revision);
            entity.setVersionControlInformation(dto);
            entity.setDisconnectedNodeAcknowledged(false);

            final Response response = versionsResource.initiateRevertFlowVersion(processGroupId, entity);
            result.set(responseToMap("POST", "/versions/revert-requests/process-groups/" + processGroupId, response));
        });
        return result.get();
    }

    @Override
    public Map<String, Object> getVersionRevertRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final Response response = versionsResource.getRevertRequest(requestId);
        return responseToMap("GET", "/versions/revert-requests/" + requestId, response);
    }

    @Override
    public Map<String, Object> deleteVersionRevertRequest(final String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        final Response response = versionsResource.deleteRevertRequest(false, requestId);
        return responseToMap("DELETE", "/versions/revert-requests/" + requestId, response);
    }

    @Override
    public Map<String, Object> revertVersionedProcessGroup(final String processGroupId,
            final Map<String, Object> versionControlInformation) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        if (versionControlInformation == null) {
            throw new IllegalArgumentException("versionControlInformation must not be null");
        }
        return asyncExecutor.execute(new NiFiAsyncRequestExecutor.RequestLifecycle<Map<String, Object>>() {
            @Override
            public String submit() {
                final Map<String, Object> response = submitVersionRevert(processGroupId, versionControlInformation);
                return NiFiAsyncRequestState.parse(response, "request", "requestId", "complete", "failureReason")
                        .getRequiredRequestId();
            }

            @Override
            public Map<String, Object> poll(final String requestId, final java.time.Duration remainingTime) {
                return getVersionRevertRequest(requestId);
            }

            @Override
            public boolean isComplete(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalSuccess();
            }

            @Override
            public boolean isFailure(final Map<String, Object> status) {
                return NiFiAsyncRequestState.parse(status, "request", "requestId", "complete", "failureReason")
                        .isTerminalFailure();
            }

            @Override
            public void cleanup(final String requestId) {
                deleteVersionRevertRequest(requestId);
            }
        });
    }

    @Override
    public Map<String, Object> exportProcessGroup(final String processGroupId,
            final boolean includeReferencedServices, final boolean includeComponentState) {
        if (processGroupId == null || processGroupId.isBlank()) {
            throw new IllegalArgumentException("processGroupId must not be blank");
        }
        final Response response = processGroupResource.exportProcessGroup(processGroupId, includeReferencedServices,
                includeComponentState);
        return responseToMap("GET", "/process-groups/" + processGroupId + "/download", response);
    }

    @Override
    public Map<String, Object> importProcessGroup(final String parentProcessGroupId, final String groupName,
            final double positionX, final double positionY, final Map<String, Object> flowSnapshot) {
        if (parentProcessGroupId == null || parentProcessGroupId.isBlank()) {
            throw new IllegalArgumentException("parentProcessGroupId must not be blank");
        }
        if (groupName == null || groupName.isBlank()) {
            throw new IllegalArgumentException("groupName must not be blank");
        }
        if (flowSnapshot == null || flowSnapshot.isEmpty()) {
            throw new IllegalArgumentException("flowSnapshot must not be null or empty");
        }

        final RevisionDTO revision = new RevisionDTO();
        revision.setVersion(0L);
        revision.setClientId(UUID.randomUUID().toString());

        final ProcessGroupUploadEntity entity = new ProcessGroupUploadEntity();
        entity.setGroupId(parentProcessGroupId);
        entity.setGroupName(groupName);
        entity.setDisconnectedNodeAcknowledged(false);
        entity.setFlowSnapshot(objectMapper.convertValue(flowSnapshot, RegisteredFlowSnapshot.class));
        entity.setPositionDTO(position(positionX, positionY));
        entity.setRevisionDTO(revision);

        final Response response = processGroupResource.importProcessGroup(parentProcessGroupId, entity);
        return responseToMap("POST", "/process-groups/" + parentProcessGroupId + "/process-groups/import", response);
    }

    // =========================================================================
    // System diagnostics, cluster, counters, and security operations
    // =========================================================================

    @Override
    public Map<String, Object> getSystemDiagnostics(final boolean nodewise, final String diagnosticLevel,
            final String clusterNodeId) {
        final String normalizedLevel = NiFiClientOperations.normalizeDiagnosticLevel(diagnosticLevel);
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank())
                ? clusterNodeId.trim() : null;
        if (nodewise && effectiveNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node");
        }
        try {
            final Response response = systemDiagnosticsResource.getSystemDiagnostics(
                    nodewise, DiagnosticLevel.valueOf(normalizedLevel), effectiveNodeId);
            return responseToMap("GET", "/system-diagnostics", response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NiFiClientException("GET", "/system-diagnostics", 0, null, false,
                    "System diagnostics request interrupted", e);
        }
    }

    @Override
    public Map<String, Object> getJmxMetrics(final String beanNameFilter) {
        final String effectiveFilter = (beanNameFilter != null && !beanNameFilter.isBlank())
                ? beanNameFilter : null;
        final Response response = systemDiagnosticsResource.getJmxMetrics(effectiveFilter);
        return responseToMap("GET", "/system-diagnostics/jmx-metrics", response);
    }

    @Override
    public Map<String, Object> getClusterSummary() {
        final Response response = flowResource.getClusterSummary();
        return responseToMap("GET", "/flow/cluster/summary", response);
    }

    @Override
    public Map<String, Object> getClusterNodes() {
        final Response response = controllerResource.getCluster();
        return responseToMap("GET", "/controller/cluster", response);
    }

    @Override
    public Map<String, Object> getClusterNode(final String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        final Response response = controllerResource.getNode(nodeId);
        return responseToMap("GET", "/controller/cluster/nodes/" + nodeId, response);
    }

    @Override
    public Map<String, Object> listCounters(final boolean nodewise, final String clusterNodeId) {
        final String effectiveNodeId = (clusterNodeId != null && !clusterNodeId.isBlank())
                ? clusterNodeId.trim() : null;
        if (nodewise && effectiveNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node");
        }
        try {
            final Response response = countersResource.getCounters(nodewise, effectiveNodeId);
            return responseToMap("GET", "/counters", response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NiFiClientException("GET", "/counters", 0, null, false,
                    "Counters request interrupted", e);
        }
    }

    @Override
    public Map<String, Object> updateClusterNode(final String nodeId, final String status, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive cluster node status update");
        }
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        final String id = nodeId.trim();
        final String normalized = NiFiClientOperations.normalizeClusterNodeStatus(status);
        final NodeDTO dto = new NodeDTO();
        dto.setNodeId(id);
        dto.setStatus(normalized);
        final NodeEntity entity = new NodeEntity();
        entity.setNode(dto);
        logger.info("Updating cluster node {} to status {}", id, normalized);
        final Response response = controllerResource.updateNode(id, entity);
        return responseToMap("PUT", "/controller/cluster/nodes/" + id, response);
    }

    @Override
    public Map<String, Object> removeClusterNode(final String nodeId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive cluster node removal");
        }
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        final String id = nodeId.trim();
        final Map<String, Object> entity = getClusterNode(id);
        final Object nodeObj = entity.get("node");
        if (!(nodeObj instanceof Map<?, ?>)) {
            throw new IllegalStateException("Cluster node response for " + id + " did not contain a 'node' map");
        }
        final Object statusObj = ((Map<?, ?>) nodeObj).get("status");
        if (!(statusObj instanceof String)) {
            throw new IllegalStateException("Cluster node 'node.status' is missing or not a string for node: " + id);
        }
        final String currentStatus = ((String) statusObj).trim().toUpperCase(Locale.ROOT);
        if (!"DISCONNECTED".equals(currentStatus) && !"OFFLOADED".equals(currentStatus)) {
            throw new IllegalStateException("Cluster node " + id + " cannot be removed: current status is "
                    + currentStatus + " (must be DISCONNECTED or OFFLOADED)");
        }
        logger.info("Removing cluster node {}", id);
        final Response response = controllerResource.deleteNode(id);
        return responseToMap("DELETE", "/controller/cluster/nodes/" + id, response);
    }

    @Override
    public Map<String, Object> resetCounter(final String counterId, final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive counter reset");
        }
        if (counterId == null || counterId.isBlank()) {
            throw new IllegalArgumentException("counterId must not be blank");
        }
        final String id = counterId.trim();
        logger.info("Resetting counter {}", id);
        final Response response = countersResource.updateCounter(id);
        return responseToMap("PUT", "/counters/" + id, response);
    }

    @Override
    public Map<String, Object> resetAllCounters(final boolean confirmed) {
        if (!confirmed) {
            throw new IllegalArgumentException("confirmed must be true to initiate a destructive counter reset");
        }
        logger.info("Resetting all counters");
        final Response response = countersResource.updateAllCounters();
        return responseToMap("PUT", "/counters", response);
    }

    @Override
    public void logout() {
        // No-op: the embedded client operates without an HTTP JWT session token.
    }

    @Override
    public Map<String, Object> getAuthenticationConfiguration() {
        final Response response = authenticationResource.getAuthenticationConfiguration();
        return responseToMap("GET", "/authentication/configuration", response);
    }

    @Override
    public List<Map<String, Object>> listAuthorizableResources() {
        final Response response = resourceResource.getResources();
        final Map<String, Object> entity = responseToMap("GET", "/resources", response);
        return listOfMap(entity.get("resources"));
    }

    @Override
    public Map<String, Object> getAccessPolicy(final String action, final String resource) {
        final String normalizedAction = NiFiClientOperations.normalizeAccessPolicyAction(action);
        if (resource == null || resource.isBlank()) {
            throw new IllegalArgumentException("resource must not be blank");
        }
        // Strip all leading slashes; the resource method prepends its own '/'
        final String strippedResource = resource.replaceAll("^/+", "");
        final Response response = accessPolicyResource.getAccessPolicyForResource(normalizedAction, strippedResource);
        return responseToMap("GET", "/policies/" + normalizedAction + "/" + strippedResource, response);
    }

    @Override
    public List<Map<String, Object>> listUsers() {
        final Response response = tenantsResource.getUsers();
        final Map<String, Object> entity = responseToMap("GET", "/tenants/users", response);
        return listOfMap(entity.get("users"));
    }

    @Override
    public List<Map<String, Object>> listUserGroups() {
        final Response response = tenantsResource.getUserGroups();
        final Map<String, Object> entity = responseToMap("GET", "/tenants/user-groups", response);
        return listOfMap(entity.get("userGroups"));
    }

    private Map<String, Object> responseToMap(final String operation, final String path, final Response response) {
        try {
            final int status = response.getStatus();
            if (status < 200 || status >= 300) {
                throw new NiFiClientException(operation, path, status, null, false,
                        "NiFi " + operation + " " + path + " failed: HTTP " + status);
            }
            final Object entity = response.getEntity();
            if (entity == null) {
                return Map.of();
            }
            return objectMapper.convertValue(entity, new TypeReference<>() {});
        } finally {
            response.close();
        }
    }

    private String responseToString(final String operation, final String path, final Response response) {
        try {
            final int status = response.getStatus();
            if (status < 200 || status >= 300) {
                final Object entity = response.getEntity();
                final String body = entity == null ? null : String.valueOf(entity);
                throw new NiFiClientException(operation, path, status, body, false,
                        "NiFi " + operation + " " + path + " failed: HTTP " + status);
            }
            final Object entity = response.getEntity();
            return entity == null ? "" : String.valueOf(entity);
        } finally {
            response.close();
        }
    }

    private static String extractNonblankString(final Map<String, Object> map, final String key) {
        final Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String stringValue)) {
            throw new IllegalArgumentException(key + " must be a string");
        }
        final String str = stringValue.trim();
        return str.isBlank() ? null : str;
    }

    private static Map<String, Object> normalizeClusterNodeId(final Map<String, Object> request) {
        final Map<String, Object> normalized = new HashMap<>(request);
        final String clusterNodeId = extractNonblankString(normalized, "clusterNodeId");
        if (clusterNodeId == null) {
            normalized.remove("clusterNodeId");
        } else {
            normalized.put("clusterNodeId", clusterNodeId);
        }
        return normalized;
    }

    private static boolean extractBoolean(final Map<String, Object> map, final String key,
            final boolean defaultValue) {
        final Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        final String str = String.valueOf(value).trim();
        if ("true".equalsIgnoreCase(str)) {
            return true;
        }
        if ("false".equalsIgnoreCase(str)) {
            return false;
        }
        throw new IllegalArgumentException("Cannot convert '" + key + "' to boolean: " + value);
    }
}
