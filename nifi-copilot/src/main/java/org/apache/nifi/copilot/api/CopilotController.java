package org.apache.nifi.copilot.api;

import static org.apache.nifi.copilot.api.Dto.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.copilot.auth.AwsAuthManager;
import org.apache.nifi.copilot.builder.FlowBuilder;
import org.apache.nifi.copilot.auth.GitHubAuthManager;
import org.apache.nifi.copilot.capability.CapabilityPromptRenderer;
import org.apache.nifi.copilot.capability.CapabilityGraph;
import org.apache.nifi.copilot.capability.CapabilityMetricsRegistry;
import org.apache.nifi.copilot.capability.CapabilityRegistry;
import org.apache.nifi.copilot.capability.CapabilityRegistryManager;
import org.apache.nifi.copilot.capability.FlowSpecificationValidationException;
import org.apache.nifi.copilot.capability.ValidatedFlowPlan;
import org.apache.nifi.copilot.capability.ValidationIssue;
import org.apache.nifi.copilot.llm.LlmClient;
import org.apache.nifi.copilot.service.CapabilityDiscoveryException;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.apache.nifi.copilot.service.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping(value = "/nifi-api/copilot")
public class CopilotController {
    private static final Logger logger = LoggerFactory.getLogger(CopilotController.class);

    private final GitHubAuthManager githubAuthManager;
    private final AwsAuthManager awsAuthManager;
    private final NiFiClientOperations nifiClient;
    private final LlmClient llmClient;
    private final FlowBuilder flowBuilder;
    private final SessionStore sessionStore;
    private final CapabilityRegistryManager capabilityRegistryManager;
    private final CapabilityPromptRenderer capabilityPromptRenderer;
    private final CapabilityMetricsRegistry capabilityMetrics;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public CopilotController(
            final GitHubAuthManager githubAuthManager,
            final AwsAuthManager awsAuthManager,
            final NiFiClientOperations nifiClient,
            final LlmClient llmClient,
            final FlowBuilder flowBuilder,
            final SessionStore sessionStore,
            final CapabilityRegistryManager capabilityRegistryManager,
            final CapabilityPromptRenderer capabilityPromptRenderer,
            final CapabilityMetricsRegistry capabilityMetrics) {
        this.githubAuthManager = githubAuthManager;
        this.awsAuthManager = awsAuthManager;
        this.nifiClient = nifiClient;
        this.llmClient = llmClient;
        this.flowBuilder = flowBuilder;
        this.sessionStore = sessionStore;
        this.capabilityRegistryManager = capabilityRegistryManager;
        this.capabilityPromptRenderer = capabilityPromptRenderer;
        this.capabilityMetrics = capabilityMetrics;
        this.githubAuthManager.validateSavedToken();
    }

    public CopilotController(
            final GitHubAuthManager githubAuthManager,
            final AwsAuthManager awsAuthManager,
            final NiFiClientOperations nifiClient,
            final LlmClient llmClient,
            final FlowBuilder flowBuilder,
            final SessionStore sessionStore) {
        this(githubAuthManager, awsAuthManager, nifiClient, llmClient, flowBuilder, sessionStore,
                new CapabilityRegistryManager(), new CapabilityPromptRenderer(),
                new CapabilityMetricsRegistry());
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
        List<Map<String, Object>> existingConnections = new ArrayList<>();
        if (req.read_canvas) {
            try {
                final Map<String, Object> canvas = flowBuilder.readCanvas(nifiClient, req.process_group_id);
                final List<Map<String, Object>> canvasProcs = (List<Map<String, Object>>) canvas.getOrDefault("processors", List.of());
                existingCs = (List<Map<String, Object>>) canvas.getOrDefault("controller_services", List.of());
                existingGroups = (List<Map<String, Object>>) canvas.getOrDefault("process_groups", List.of());
                existingConnections =
                        (List<Map<String, Object>>) canvas.getOrDefault("connections", List.of());
                final List<String> tracked = existing.stream().map(e -> String.valueOf(e.get("nifi_id"))).toList();
                for (Map<String, Object> cp : canvasProcs) {
                    if (!tracked.contains(String.valueOf(cp.get("nifi_id")))) {
                        existing.add(cp);
                    }
                }
            } catch (Exception e) {
                logger.warn("Could not read canvas context for process group {}: {}",
                        req.process_group_id, e.getMessage());
            }
        }

        final String capabilityContext;
        try {
            final CapabilityRegistry.CapabilitySet capabilities =
                    capabilityRegistryManager.capabilitySet(nifiClient);
            final CapabilityGraph capabilityGraph = capabilities.graph();
            capabilityContext = capabilityPromptRenderer.renderFromGraph(
                    req.message, capabilityGraph);
        } catch (CapabilityDiscoveryException e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_GATEWAY,
                    "NiFi capability discovery unavailable; generation was not attempted: " + e.getMessage(),
                    e);
        }

        final ValidatedFlowPlan plan;
        Map<String, Object> generatedSpec = null;
        try {
            final PreparedGeneration prepared = generateAndPrepare(req, history, existing, existingCs,
                    existingGroups, existingConnections, capabilityContext);
            if (prepared.plan() == null) {
                return validationFailure(prepared.specification(), prepared.issues());
            }
            generatedSpec = prepared.specification();
            plan = prepared.plan();
        } catch (CapabilityDiscoveryException e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_GATEWAY,
                    "NiFi capability validation unavailable; no canvas changes were made: " + e.getMessage(),
                    e);
        } catch (IllegalArgumentException | ClassCastException e) {
            return validationFailure(generatedSpec, List.of(new ValidationIssue(
                    "", "specification", e.getMessage(), "Return a structurally valid flow specification")));
        }
        final Map<String, Object> spec = plan.specification();
        String explanation = String.valueOf(spec.getOrDefault("explanation", "Done!"));
        final List<Map<String, Object>> deletions = (List<Map<String, Object>>) spec.getOrDefault("deletions", List.of());
        final List<String> deletedProcessorNames = new ArrayList<>();
        final List<String> deletedProcessGroupNames = new ArrayList<>();
        final List<String> deletedControllerServiceNames = new ArrayList<>();
        final List<String> deletedParameterContextNames = new ArrayList<>();
        final List<Map<String, Object>> existingParameterContexts = deletions.isEmpty()
                ? List.of() : projectParameterContexts(nifiClient.listParameterContexts());
        if (!deletions.isEmpty()) {
            // Pass 1: Resolve all deletion targets
            final List<DeletionEntry> resolvedDeletions = new ArrayList<>();
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
                resolvedDeletions.add(new DeletionEntry(label, target));
            }

            // Pass 2: Sort by NiFi tear-down order and execute
            resolvedDeletions.sort(Comparator.comparingInt(e -> e.target().type().tearDownOrder()));
            for (DeletionEntry entry : resolvedDeletions) {
                try {
                    executeDeletion(entry.target(), req.process_group_id);
                    switch (entry.target().type()) {
                        case PROCESSOR -> deletedProcessorNames.add(entry.label());
                        case PROCESS_GROUP -> deletedProcessGroupNames.add(entry.label());
                        case CONTROLLER_SERVICE -> deletedControllerServiceNames.add(entry.label());
                        case PARAMETER_CONTEXT -> deletedParameterContextNames.add(entry.label());
                    }
                } catch (Exception e) {
                    explanation += "\n\n⚠️ Could not delete '" + entry.label() + "': " + e.getMessage();
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

        final List<Map<String, Object>> csActions =
                (List<Map<String, Object>>) spec.getOrDefault("cs_actions", List.of());
        List<Map<String, Object>> createdProcessors = List.of();
        int connectionsCreated = 0;
        boolean deploymentSucceeded = false;
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
                    plan, req.process_group_id, nifiClient, existingIdMap, existing.size(), false, true);
            createdProcessors = res.createdProcessors();
            connectionsCreated = res.connectionsCreated();
            deploymentSucceeded = true;
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
        if (deploymentSucceeded && !csActions.isEmpty()) {
            final List<String> csResults = applyControllerServiceActions(
                    csActions, req.process_group_id,
                    (List<Map<String, Object>>) spec.getOrDefault("controller_services", List.of()));
            if (!csResults.isEmpty()) {
                explanation += "\n\n" + String.join("\n", csResults);
            }
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

    private List<String> applyControllerServiceActions(
            final List<Map<String, Object>> actions,
            final String processGroupId,
            final List<Map<String, Object>> declaredServices) {
        final Map<String, String> serviceIdsByName = new HashMap<>();
        final List<Map<String, Object>> services;
        try {
            services = nifiClient.listControllerServices(processGroupId);
        } catch (RuntimeException e) {
            return List.of("⚠️ Could not refresh controller services after deployment: " + e.getMessage());
        }
        for (Map<String, Object> service : services) {
            final Map<String, Object> component = nestedMap(service, "component");
            final String name = String.valueOf(component.getOrDefault(
                    "name", service.getOrDefault("name", "")));
            final String id = String.valueOf(component.getOrDefault(
                    "id", service.getOrDefault("id", "")));
            if (!name.isBlank() && !id.isBlank()) {
                serviceIdsByName.put(name.toLowerCase(), id);
            }
        }
        final Set<String> declaredNames = new HashSet<>();
        for (Map<String, Object> service : declaredServices) {
            declaredNames.add(String.valueOf(service.getOrDefault("name", "")).toLowerCase());
        }

        final List<String> results = new ArrayList<>();
        for (Map<String, Object> action : actions) {
            final String serviceName = String.valueOf(action.getOrDefault("name", ""));
            final String normalizedName = serviceName.toLowerCase();
            final String actionName = String.valueOf(action.getOrDefault("action", "")).toLowerCase();
            final String serviceId = serviceIdsByName.get(normalizedName);
            if (serviceId == null) {
                if (declaredNames.contains(normalizedName)) {
                    results.add("⚠️ Controller service '" + serviceName
                            + "' was declared but not found after deployment.");
                } else {
                    logger.warn("Ignoring controller service action '{}' for undeclared service '{}'",
                            actionName, serviceName);
                }
                continue;
            }
            if ("enable".equals(actionName) && declaredNames.contains(normalizedName)) {
                results.add("✅ Enabled '" + serviceName + "'");
                continue;
            }
            try {
                if ("enable".equals(actionName)) {
                    nifiClient.enableControllerService(serviceId);
                    results.add("✅ Enabled '" + serviceName + "'");
                } else if ("disable".equals(actionName)) {
                    nifiClient.disableControllerService(serviceId);
                    results.add("✅ Disabled '" + serviceName + "'");
                }
            } catch (Exception e) {
                results.add("⚠️ Could not " + actionName + " '" + serviceName + "': " + e.getMessage());
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nestedMap(final Map<String, Object> source, final String key) {
        final Object value = source.get(key);
        return value instanceof Map<?, ?> ? (Map<String, Object>) value : Map.of();
    }

    private PreparedGeneration generateAndPrepare(
            final ChatRequest req,
            final List<Map<String, String>> history,
            final List<Map<String, Object>> existing,
            final List<Map<String, Object>> existingCs,
            final List<Map<String, Object>> existingGroups,
            final List<Map<String, Object>> existingConnections,
            final String capabilityContext) {
        final Map<String, Object> generated = generateSpecification(
                req, req.message, history, existing, existingCs, existingGroups,
                existingConnections, capabilityContext);
        try {
            final ValidatedFlowPlan plan = flowBuilder.prepareFlow(generated, nifiClient, true);
            capabilityMetrics.observeFirstPass(true, List.of());
            return new PreparedGeneration(generated, plan, List.of());
        } catch (FlowSpecificationValidationException firstFailure) {
            capabilityMetrics.observeFirstPass(false, firstFailure.getReport().issues());
            capabilityMetrics.observeRepairAttempt();
            final String repairMessage = repairMessage(
                    req.message, generated, firstFailure.getReport().issues());
            final Map<String, Object> repaired = generateSpecification(
                    req, repairMessage, List.of(), existing, existingCs, existingGroups,
                    existingConnections, capabilityContext);
            final Map<?, ?> repairUsage = repaired.get("_token_usage") instanceof Map<?, ?> value
                    ? value
                    : Map.of();
            capabilityMetrics.observeRepairTokens(
                    intValue(repairUsage.get("input")),
                    intValue(repairUsage.get("output")));
            final Map<String, Object> repairedWithUsage = combineTokenUsage(generated, repaired);
            try {
                final ValidatedFlowPlan plan =
                        flowBuilder.prepareFlow(repairedWithUsage, nifiClient, true);
                capabilityMetrics.observeRepairResult(true, List.of());
                return new PreparedGeneration(repairedWithUsage, plan, List.of());
            } catch (FlowSpecificationValidationException finalFailure) {
                capabilityMetrics.observeRepairResult(
                        false, finalFailure.getReport().issues());
                return new PreparedGeneration(
                        repairedWithUsage, null, finalFailure.getReport().issues());
            }
        }
    }

    private Map<String, Object> generateSpecification(
            final ChatRequest req,
            final String message,
            final List<Map<String, String>> history,
            final List<Map<String, Object>> existing,
            final List<Map<String, Object>> existingCs,
            final List<Map<String, Object>> existingGroups,
            final List<Map<String, Object>> existingConnections,
            final String capabilityContext) {
        if ("aws".equalsIgnoreCase(req.provider)) {
            if (!awsAuthManager.isAuthenticated()) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated with AWS Bedrock.");
            }
            return llmClient.generateFlowSpecBedrock(message, history,
                    awsAuthManager.getBedrockCredentials(), existing, req.model,
                    existingCs, existingGroups, existingConnections, capabilityContext);
        }
        if (!githubAuthManager.isAuthenticated()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated with GitHub.");
        }
        return llmClient.generateFlowSpec(message, history,
                githubAuthManager.getGitHubToken(), existing, req.model,
                existingCs, existingGroups, existingConnections, capabilityContext);
    }

    private String repairMessage(
            final String originalRequest,
            final Map<String, Object> rejected,
            final List<ValidationIssue> issues) {
        final Map<String, Object> rejectedWithoutUsage = new LinkedHashMap<>(rejected);
        rejectedWithoutUsage.remove("_token_usage");
        return """
                Correct the complete NiFi flow specification below. Return one complete JSON object only.
                Preserve the original intent and topology, but fix every validation issue using exact TARGET NIFI
                CAPABILITIES names and values. Supply every required property with no default. Relationship names
                are case-sensitive and must exactly match the source processor.

                ORIGINAL REQUEST:
                %s

                VALIDATION ISSUES:
                %s

                REJECTED SPECIFICATION:
                %s
                """.formatted(
                originalRequest,
                objectMapper.valueToTree(issues),
                objectMapper.valueToTree(rejectedWithoutUsage));
    }

    private Map<String, Object> combineTokenUsage(
            final Map<String, Object> first,
            final Map<String, Object> second) {
        final Map<String, Object> combined = new LinkedHashMap<>(second);
        final Map<?, ?> firstUsage = first.get("_token_usage") instanceof Map<?, ?> value ? value : Map.of();
        final Map<?, ?> secondUsage = second.get("_token_usage") instanceof Map<?, ?> value ? value : Map.of();
        if (firstUsage.isEmpty() && secondUsage.isEmpty()) {
            return combined;
        }
        combined.put("_token_usage", Map.of(
                "input", intValue(firstUsage.get("input")) + intValue(secondUsage.get("input")),
                "output", intValue(firstUsage.get("output")) + intValue(secondUsage.get("output")),
                "total", intValue(firstUsage.get("total")) + intValue(secondUsage.get("total"))));
        return combined;
    }

    private record PreparedGeneration(
            Map<String, Object> specification,
            ValidatedFlowPlan plan,
            List<ValidationIssue> issues) {
    }

    private ChatResponse validationFailure(
            final Map<String, Object> generatedSpec,
            final List<ValidationIssue> issues) {
        final ChatResponse response = new ChatResponse();
        response.validation_issues = issues.stream().sorted().toList();
        final StringBuilder explanation = new StringBuilder(
                "The generated flow was rejected before any NiFi changes were made.");
        for (ValidationIssue issue : response.validation_issues) {
            explanation.append("\n- ").append(issue.componentId().isBlank() ? "<flow>" : issue.componentId())
                    .append(" ").append(issue.path()).append(": ").append(issue.reason());
            if (!issue.suggestedFix().isBlank()) {
                explanation.append(" Fix: ").append(issue.suggestedFix());
            }
        }
        response.reply = explanation.toString();
        final Object usage = generatedSpec == null ? null : generatedSpec.get("_token_usage");
        if (usage instanceof Map<?, ?> map) {
            response.tokens_used = (Map<String, Object>) map;
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
        PROCESSOR(1),
        PROCESS_GROUP(4),
        CONTROLLER_SERVICE(2),
        PARAMETER_CONTEXT(3);

        private final int tearDownOrder;

        DeletionType(final int tearDownOrder) {
            this.tearDownOrder = tearDownOrder;
        }

        int tearDownOrder() {
            return tearDownOrder;
        }

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

    private record DeletionEntry(String label, DeletionTarget target) {
    }

    private void executeDeletion(final DeletionTarget target, final String processGroupId) {
        switch (target.type()) {
            case PROCESS_GROUP -> {
                prepareProcessGroupForDeletion(target.nifiId());
                nifiClient.deleteProcessGroup(target.nifiId());
            }
            case CONTROLLER_SERVICE -> {
                prepareControllerServiceForDeletion(target.nifiId());
                nifiClient.deleteControllerService(target.nifiId());
            }
            case PARAMETER_CONTEXT -> nifiClient.deleteParameterContext(target.nifiId());
            case PROCESSOR -> {
                removeConnectionsForComponent(target.nifiId(), processGroupId);
                nifiClient.deleteProcessor(target.nifiId());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void removeConnectionsForComponent(final String componentId, final String processGroupId) {
        try {
            for (Map<String, Object> conn : nifiClient.listConnections(processGroupId)) {
                final String connId = componentId(conn);
                if (connId == null) continue;
                final Map<String, Object> component =
                        (Map<String, Object>) conn.getOrDefault("component", conn);
                final String sourceId = String.valueOf(
                        ((Map<String, Object>) component.getOrDefault("source", Map.of()))
                                .getOrDefault("id", ""));
                final String destId = String.valueOf(
                        ((Map<String, Object>) component.getOrDefault("destination", Map.of()))
                                .getOrDefault("id", ""));
                if (componentId.equals(sourceId) || componentId.equals(destId)) {
                    try {
                        nifiClient.deleteConnection(connId);
                    } catch (Exception e) {
                        logger.warn("Could not remove connection {} for component {}: {}",
                                connId, componentId, e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Could not list connections for component cleanup in group {}: {}",
                    processGroupId, e.getMessage());
        }
    }

    private void prepareControllerServiceForDeletion(final String csId) {
        try {
            nifiClient.updateControllerServiceReferences(csId, "STOPPED");
        } catch (Exception e) {
            logger.warn("Could not stop references for CS {} before deletion: {}", csId, e.getMessage());
        }
        try {
            nifiClient.updateControllerServiceReferences(csId, "DISABLED");
        } catch (Exception e) {
            logger.warn("Could not disable references for CS {} before deletion: {}", csId, e.getMessage());
        }
    }

    private static int intValue(final Object v) {
        return NiFiClientOperations.intValue(v);
    }

    private static boolean bool(final Object v) {
        if (v == null) return false;
        return Boolean.parseBoolean(String.valueOf(v));
    }
}
