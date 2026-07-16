package org.apache.nifi.copilot.api;

import static org.apache.nifi.copilot.api.Dto.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.copilot.auth.AwsAuthManager;
import org.apache.nifi.copilot.builder.FlowBuilder;
import org.apache.nifi.copilot.auth.GitHubAuthManager;
import org.apache.nifi.copilot.llm.LlmClient;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.apache.nifi.copilot.store.SessionStore;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping(value = "/nifi-api/copilot")
public class CopilotController {
    private final GitHubAuthManager githubAuthManager;
    private final AwsAuthManager awsAuthManager;
    private final NiFiClientOperations nifiClient;
    private final LlmClient llmClient;
    private final FlowBuilder flowBuilder;
    private final SessionStore sessionStore;

    public CopilotController(
            final GitHubAuthManager githubAuthManager,
            final AwsAuthManager awsAuthManager,
            final NiFiClientOperations nifiClient,
            final LlmClient llmClient,
            final FlowBuilder flowBuilder,
            final SessionStore sessionStore) {
        this.githubAuthManager = githubAuthManager;
        this.awsAuthManager = awsAuthManager;
        this.nifiClient = nifiClient;
        this.llmClient = llmClient;
        this.flowBuilder = flowBuilder;
        this.sessionStore = sessionStore;
        this.githubAuthManager.validateSavedToken();
    }

    @GetMapping("/api/session/{processGroupId}")
    public Map<String, Object> getSession(@PathVariable("processGroupId") final String processGroupId) {
        final Map<String, Object> data = sessionStore.load(processGroupId);
        if (data == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No session found");
        }
        return data;
    }

    @PutMapping("/api/session/{processGroupId}")
    public Map<String, Object> putSession(@PathVariable("processGroupId") final String processGroupId, @RequestBody final SessionData data) {
        sessionStore.save(processGroupId, data);
        return Map.of("ok", true);
    }

    @DeleteMapping("/api/session/{processGroupId}")
    public Map<String, Object> deleteSession(@PathVariable("processGroupId") final String processGroupId) {
        sessionStore.delete(processGroupId);
        return Map.of("ok", true);
    }

    @GetMapping("/api/auth/status")
    public AuthStatusResponse githubAuthStatus() {
        final AuthStatusResponse r = new AuthStatusResponse();
        final Map<String, Object> status = githubAuthManager.authStatus();
        r.authenticated = bool(status.get("authenticated"));
        r.login = String.valueOf(status.getOrDefault("login", ""));
        r.device_flow_active = bool(status.get("device_flow_active"));
        r.user_code = String.valueOf(status.getOrDefault("user_code", ""));
        r.verification_uri = String.valueOf(status.getOrDefault("verification_uri", ""));
        return r;
    }

    @PostMapping("/api/auth/start")
    public DeviceFlowResponse githubAuthStart() {
        try {
            final Map<String, Object> r = githubAuthManager.startDeviceFlow();
            return new DeviceFlowResponse(String.valueOf(r.get("user_code")), String.valueOf(r.get("verification_uri")), intValue(r.get("expires_in")));
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "GitHub API error: " + e.getMessage());
        }
    }

    @PostMapping("/api/auth/logout")
    public Map<String, Object> githubAuthLogout() {
        githubAuthManager.logout();
        return Map.of("status", "logged out");
    }

    @GetMapping("/api/auth/aws/status")
    public AwsAuthStatusResponse awsAuthStatus() {
        final AwsAuthStatusResponse r = new AwsAuthStatusResponse();
        final Map<String, Object> status = awsAuthManager.authStatus();
        r.authenticated = bool(status.get("authenticated"));
        r.device_flow_active = bool(status.get("device_flow_active"));
        r.user_code = String.valueOf(status.getOrDefault("user_code", ""));
        r.verification_uri = String.valueOf(status.getOrDefault("verification_uri", ""));
        r.role_selection_needed = bool(status.get("role_selection_needed"));
        r.available_roles = (List<Map<String, Object>>) status.getOrDefault("available_roles", List.of());
        r.bedrock_region = String.valueOf(status.getOrDefault("bedrock_region", ""));
        r.account_id = String.valueOf(status.getOrDefault("account_id", ""));
        r.role_name = String.valueOf(status.getOrDefault("role_name", ""));
        return r;
    }

    @PostMapping("/api/auth/aws/start")
    public DeviceFlowResponse awsAuthStart(@RequestBody final AwsStartRequest req) {
        if (req.sso_start_url == null || !req.sso_start_url.startsWith("https://")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "SSO start URL must begin with https://");
        }
        try {
            final Map<String, Object> r = awsAuthManager.startDeviceFlow(req.sso_start_url, req.sso_region, req.bedrock_region);
            return new DeviceFlowResponse(String.valueOf(r.get("user_code")), String.valueOf(r.get("verification_uri")), intValue(r.get("expires_in")));
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "AWS SSO error: " + e.getMessage());
        }
    }

    @PostMapping("/api/auth/aws/select-role")
    public Map<String, Object> awsSelectRole(@RequestBody final AwsSelectRoleRequest req) {
        try {
            awsAuthManager.selectRole(req.account_id, req.role_name);
            return Map.of("status", "role selected");
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "AWS error: " + e.getMessage());
        }
    }

    @PostMapping("/api/auth/aws/logout")
    public Map<String, Object> awsLogout() {
        awsAuthManager.logout();
        return Map.of("status", "logged out");
    }

    @GetMapping("/api/canvas")
    public Map<String, Object> canvas(@RequestParam(name = "process_group_id", defaultValue = "root") final String processGroupId) {
        try {
            return flowBuilder.readCanvas(nifiClient, processGroupId);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "NiFi API error: " + e.getMessage());
        }
    }

    @GetMapping("/api/health")
    public Map<String, Object> health() {
        return Map.of("status", "ok", "github_authenticated", githubAuthManager.isAuthenticated(), "aws_authenticated", awsAuthManager.isAuthenticated());
    }

    @PostMapping("/api/chat")
    public ChatResponse chat(@RequestBody final ChatRequest req) {
        final List<Map<String, String>> history = new ArrayList<>();
        if (req.history != null) {
            for (HistoryEntry h : req.history) {
                history.add(Map.of("role", h.role(), "content", h.content()));
            }
        }

        final List<Map<String, Object>> existing = new ArrayList<>();
        if (req.existing_processors != null) {
            for (ExistingProcessor p : req.existing_processors) {
                existing.add(Map.of(
                        "spec_id", p.spec_id(),
                        "nifi_id", p.nifi_id(),
                        "name", p.name(),
                        "type", p.type()));
            }
        }
        List<Map<String, Object>> existingCs = new ArrayList<>();
        List<Map<String, Object>> existingGroups = new ArrayList<>();
        if (req.read_canvas) {
            try {
                final Map<String, Object> canvas = flowBuilder.readCanvas(nifiClient, req.process_group_id);
                final List<Map<String, Object>> canvasProcs = (List<Map<String, Object>>) canvas.getOrDefault("processors", List.of());
                existingCs = (List<Map<String, Object>>) canvas.getOrDefault("controller_services", List.of());
                existingGroups = (List<Map<String, Object>>) canvas.getOrDefault("process_groups", List.of());
                final List<String> tracked = existing.stream().map(e -> String.valueOf(e.get("nifi_id"))).toList();
                for (Map<String, Object> cp : canvasProcs) {
                    if (!tracked.contains(String.valueOf(cp.get("nifi_id")))) {
                        existing.add(cp);
                    }
                }
            } catch (Exception ignored) {
            }
        }

        final Map<String, Object> spec;
        if ("aws".equalsIgnoreCase(req.provider)) {
            if (!awsAuthManager.isAuthenticated()) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated with AWS Bedrock.");
            }
            spec = llmClient.generateFlowSpecBedrock(req.message, history,
                    awsAuthManager.getBedrockCredentials(), existing, req.model, existingCs, existingGroups);
        } else {
            if (!githubAuthManager.isAuthenticated()) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated with GitHub.");
            }
            spec = llmClient.generateFlowSpec(req.message, history,
                    githubAuthManager.getGitHubToken(), existing, req.model, existingCs, existingGroups);
        }

        String explanation = String.valueOf(spec.getOrDefault("explanation", "Done!"));
        final List<Map<String, Object>> deletions = (List<Map<String, Object>>) spec.getOrDefault("deletions", List.of());
        final List<String> deletedProcessorNames = new ArrayList<>();
        final List<String> deletedProcessGroupNames = new ArrayList<>();
        final List<String> deletedControllerServiceNames = new ArrayList<>();
        final List<String> deletedParameterContextNames = new ArrayList<>();
        final List<Map<String, Object>> existingParameterContexts = deletions.isEmpty()
                ? List.of() : projectParameterContexts(nifiClient.listParameterContexts());
        if (!deletions.isEmpty()) {
            for (Map<String, Object> d : deletions) {
                final String specId = String.valueOf(d.getOrDefault("spec_id", ""));
                final String name = String.valueOf(d.getOrDefault("name", ""));
                final String label = !name.isBlank() ? name : specId;
                final String requestedType = String.valueOf(d.getOrDefault("type", ""));
                final DeletionTarget target = resolveDeletionTarget(
                        requestedType, specId, name, existing, existingGroups,
                        existingCs, existingParameterContexts);
                if (target == null) {
                    explanation += "\n\n⚠️ Could not find '" + label + "' on the canvas to delete.";
                    continue;
                }
                try {
                    if (target.type() == DeletionType.PROCESS_GROUP) {
                        prepareProcessGroupForDeletion(target.nifiId());
                        nifiClient.deleteProcessGroup(target.nifiId());
                        deletedProcessGroupNames.add(label);
                    } else if (target.type() == DeletionType.CONTROLLER_SERVICE) {
                        nifiClient.deleteControllerService(target.nifiId());
                        deletedControllerServiceNames.add(label);
                    } else if (target.type() == DeletionType.PARAMETER_CONTEXT) {
                        nifiClient.deleteParameterContext(target.nifiId());
                        deletedParameterContextNames.add(label);
                    } else {
                        nifiClient.deleteProcessor(target.nifiId());
                        deletedProcessorNames.add(label);
                    }
                } catch (Exception e) {
                    explanation += "\n\n⚠️ Could not delete '" + label + "': " + e.getMessage();
                }
            }
            if (!deletedProcessorNames.isEmpty()) {
                explanation += "\n\n✅ Deleted processor(s): "
                        + String.join(", ", deletedProcessorNames) + ".";
            }
            if (!deletedProcessGroupNames.isEmpty()) {
                explanation += "\n\n✅ Deleted process group(s): "
                        + String.join(", ", deletedProcessGroupNames) + ".";
            }
            if (!deletedControllerServiceNames.isEmpty()) {
                explanation += "\n\n✅ Deleted controller service(s): "
                        + String.join(", ", deletedControllerServiceNames) + ".";
            }
            if (!deletedParameterContextNames.isEmpty()) {
                explanation += "\n\n✅ Deleted parameter context(s): "
                        + String.join(", ", deletedParameterContextNames) + ".";
            }
        }

        final List<Map<String, Object>> csActions = (List<Map<String, Object>>) spec.getOrDefault("cs_actions", List.of());
        if (!csActions.isEmpty()) {
            final Map<String, Map<String, Object>> csNameMap = new HashMap<>();
            for (Map<String, Object> cs : existingCs) {
                csNameMap.put(String.valueOf(cs.getOrDefault("name", "")).toLowerCase(), cs);
            }
            final List<String> csResults = new ArrayList<>();
            for (Map<String, Object> action : csActions) {
                final String csName = String.valueOf(action.getOrDefault("name", ""));
                final String act = String.valueOf(action.getOrDefault("action", "")).toLowerCase();
                final Map<String, Object> csInfo = csNameMap.get(csName.toLowerCase());
                if (csInfo == null) {
                    csResults.add("⚠️ Controller service '" + csName + "' not found on canvas.");
                    continue;
                }
                final String csId = String.valueOf(csInfo.get("nifi_id"));
                try {
                    if ("enable".equals(act)) {
                        nifiClient.enableControllerService(csId);
                        csResults.add("✅ Enabled '" + csName + "'");
                    } else if ("disable".equals(act)) {
                        nifiClient.disableControllerService(csId);
                        csResults.add("✅ Disabled '" + csName + "'");
                    }
                } catch (Exception e) {
                    csResults.add("⚠️ Could not " + act + " '" + csName + "': " + e.getMessage());
                }
            }
            if (!csResults.isEmpty()) {
                explanation += "\n\n" + String.join("\n", csResults);
            }
        }

        List<Map<String, Object>> createdProcessors = List.of();
        int connectionsCreated = 0;
        final List<Map<String, Object>> processorsSpec = (List<Map<String, Object>>) spec.getOrDefault("processors", List.of());
        final Map<String, String> existingIdMap = new HashMap<>();
        for (Map<String, Object> p : existing) {
            final String specId = String.valueOf(p.getOrDefault("spec_id", ""));
            final String nifiId = String.valueOf(p.getOrDefault("nifi_id", ""));
            if (!specId.isBlank() && !nifiId.isBlank()) {
                existingIdMap.put(specId, nifiId);
            }
        }
        try {
            final FlowBuilder.BuildResult res = flowBuilder.buildFlow(
                    spec, req.process_group_id, nifiClient, existingIdMap, existing.size(), false, true);
            createdProcessors = res.createdProcessors();
            connectionsCreated = res.connectionsCreated();
            if (!createdProcessors.isEmpty()) {
                explanation += "\n\n✅ Created " + createdProcessors.size() + " processor(s), "
                        + connectionsCreated + " connection(s) on the canvas.";
            } else if (!processorsSpec.isEmpty()) {
                explanation += "\n\n⚠️ No new processors were created.";
            }
        } catch (Exception e) {
            final String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            explanation += "\n\n⚠️ Could not apply changes on the canvas: " + detail;
        }

        final ChatResponse response = new ChatResponse();
        response.reply = explanation;
        response.connections_created = connectionsCreated;
        response.processors_deleted = deletedProcessorNames;
        response.process_groups_deleted = deletedProcessGroupNames;
        response.controller_services_deleted = deletedControllerServiceNames;
        response.parameter_contexts_deleted = deletedParameterContextNames;
        response.tokens_used = (Map<String, Object>) spec.get("_token_usage");
        response.processors_created = new ArrayList<>();
        for (Map<String, Object> p : createdProcessors) {
            response.processors_created.add(new CreatedProcessor(
                    String.valueOf(p.getOrDefault("id", "")),
                    String.valueOf(p.getOrDefault("spec_id", "")),
                    String.valueOf(p.getOrDefault("name", "")),
                    String.valueOf(p.getOrDefault("type", ""))));
        }
        return response;
    }

    private static List<Map<String, Object>> projectParameterContexts(
            final List<Map<String, Object>> entities) {
        final List<Map<String, Object>> projected = new ArrayList<>();
        for (Map<String, Object> entity : entities) {
            final Map<String, Object> component = entity.get("component") instanceof Map<?, ?> value
                    ? (Map<String, Object>) value : entity;
            final String id = componentId(entity);
            if (id == null) {
                continue;
            }
            final Map<String, Object> item = new HashMap<>();
            item.put("nifi_id", id);
            item.put("spec_id", id);
            item.put("name", String.valueOf(component.getOrDefault("name", "")));
            projected.add(item);
        }
        return projected;
    }

    private void prepareProcessGroupForDeletion(final String processGroupId) {
        nifiClient.scheduleProcessGroup(processGroupId, "STOPPED");
        disableControllerServicesForDeletion(processGroupId);
    }

    private void disableControllerServicesForDeletion(final String processGroupId) {
        for (Map<String, Object> child : nifiClient.listChildProcessGroups(processGroupId)) {
            final String childId = componentId(child);
            if (childId != null) {
                disableControllerServicesForDeletion(childId);
            }
        }
        for (Map<String, Object> service : nifiClient.listControllerServices(processGroupId)) {
            if (!processGroupId.equals(String.valueOf(service.getOrDefault("parentGroupId", "")))) {
                continue;
            }
            final String serviceId = componentId(service);
            final String state = String.valueOf(service.getOrDefault("state", "DISABLED"));
            if (serviceId != null && !"DISABLED".equalsIgnoreCase(state)) {
                nifiClient.updateControllerServiceReferences(serviceId, "DISABLED");
                nifiClient.disableControllerService(serviceId);
            }
        }
    }

    private static String componentId(final Map<String, Object> entity) {
        final Object id = entity.get("id");
        if (id != null && !String.valueOf(id).isBlank()) {
            return String.valueOf(id);
        }
        final Object instanceId = entity.get("instanceIdentifier");
        return instanceId == null || String.valueOf(instanceId).isBlank()
                ? null : String.valueOf(instanceId);
    }

    private static DeletionTarget resolveDeletionTarget(
            final String requestedType,
            final String specId,
            final String name,
            final List<Map<String, Object>> processors,
            final List<Map<String, Object>> processGroups,
            final List<Map<String, Object>> controllerServices,
            final List<Map<String, Object>> parameterContexts) {
        final DeletionType type = DeletionType.from(requestedType);
        final List<DeletionTarget> matches = new ArrayList<>();
        if (type == null || type == DeletionType.PROCESSOR) {
            addDeletionMatches(matches, DeletionType.PROCESSOR, specId, name, processors);
        }
        if (type == null || type == DeletionType.PROCESS_GROUP) {
            addDeletionMatches(matches, DeletionType.PROCESS_GROUP, specId, name, processGroups);
        }
        if (type == null || type == DeletionType.CONTROLLER_SERVICE) {
            addDeletionMatches(
                    matches, DeletionType.CONTROLLER_SERVICE, specId, name, controllerServices);
        }
        if (type == null || type == DeletionType.PARAMETER_CONTEXT) {
            addDeletionMatches(
                    matches, DeletionType.PARAMETER_CONTEXT, specId, name, parameterContexts);
        }
        return matches.size() == 1 ? matches.getFirst() : null;
    }

    private static void addDeletionMatches(
            final List<DeletionTarget> matches,
            final DeletionType type,
            final String specId,
            final String name,
            final List<Map<String, Object>> candidates) {
        for (Map<String, Object> candidate : candidates) {
            final String candidateId = String.valueOf(candidate.getOrDefault("nifi_id", ""));
            final String candidateSpecId = String.valueOf(candidate.getOrDefault("spec_id", ""));
            final String candidateName = String.valueOf(candidate.getOrDefault("name", ""));
            if (!candidateId.isBlank()
                    && ((!specId.isBlank() && specId.equals(candidateSpecId))
                    || (!name.isBlank() && name.equalsIgnoreCase(candidateName)))) {
                matches.add(new DeletionTarget(type, candidateId));
            }
        }
    }

    private enum DeletionType {
        PROCESSOR,
        PROCESS_GROUP,
        CONTROLLER_SERVICE,
        PARAMETER_CONTEXT;

        private static DeletionType from(final String value) {
            if ("processor".equalsIgnoreCase(value)) {
                return PROCESSOR;
            }
            if ("process_group".equalsIgnoreCase(value) || "process group".equalsIgnoreCase(value)) {
                return PROCESS_GROUP;
            }
            if ("controller_service".equalsIgnoreCase(value)
                    || "controller service".equalsIgnoreCase(value)) {
                return CONTROLLER_SERVICE;
            }
            if ("parameter_context".equalsIgnoreCase(value)
                    || "parameter context".equalsIgnoreCase(value)) {
                return PARAMETER_CONTEXT;
            }
            return null;
        }
    }

    private record DeletionTarget(DeletionType type, String nifiId) {
    }

    private static int intValue(final Object v) {
        return NiFiClientOperations.intValue(v);
    }

    private static boolean bool(final Object v) {
        if (v == null) return false;
        return Boolean.parseBoolean(String.valueOf(v));
    }
}
