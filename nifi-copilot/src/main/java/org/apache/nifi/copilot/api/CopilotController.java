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
        if (req.read_canvas) {
            try {
                final Map<String, Object> canvas = flowBuilder.readCanvas(nifiClient, req.process_group_id);
                final List<Map<String, Object>> canvasProcs = (List<Map<String, Object>>) canvas.getOrDefault("processors", List.of());
                existingCs = (List<Map<String, Object>>) canvas.getOrDefault("controller_services", List.of());
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
            spec = llmClient.generateFlowSpecBedrock(req.message, history, awsAuthManager.getBedrockCredentials(), existing, req.model, existingCs);
        } else {
            if (!githubAuthManager.isAuthenticated()) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Not authenticated with GitHub.");
            }
            spec = llmClient.generateFlowSpec(req.message, history, githubAuthManager.getGitHubToken(), existing, req.model, existingCs);
        }

        String explanation = String.valueOf(spec.getOrDefault("explanation", "Done!"));
        final List<Map<String, Object>> deletions = (List<Map<String, Object>>) spec.getOrDefault("deletions", List.of());
        final List<String> deletedNames = new ArrayList<>();
        if (!deletions.isEmpty()) {
            final Map<String, String> byName = new HashMap<>();
            final Map<String, String> bySpec = new HashMap<>();
            for (Map<String, Object> p : existing) {
                final String nifiId = String.valueOf(p.getOrDefault("nifi_id", ""));
                if (nifiId.isBlank()) continue;
                byName.put(String.valueOf(p.getOrDefault("name", "")).toLowerCase(), nifiId);
                bySpec.put(String.valueOf(p.getOrDefault("spec_id", "")), nifiId);
            }
            for (Map<String, Object> d : deletions) {
                final String specId = String.valueOf(d.getOrDefault("spec_id", ""));
                final String name = String.valueOf(d.getOrDefault("name", ""));
                final String nifiId = bySpec.getOrDefault(specId, byName.get(name.toLowerCase()));
                final String label = !name.isBlank() ? name : specId;
                if (nifiId == null || nifiId.isBlank()) {
                    explanation += "\n\n⚠️ Could not find '" + label + "' on the canvas to delete.";
                    continue;
                }
                try {
                    nifiClient.deleteProcessor(nifiId);
                    deletedNames.add(label);
                } catch (Exception e) {
                    explanation += "\n\n⚠️ Could not delete '" + label + "': " + e.getMessage();
                }
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
        if (!processorsSpec.isEmpty()) {
            final Map<String, String> existingIdMap = new HashMap<>();
            for (Map<String, Object> p : existing) {
                final String specId = String.valueOf(p.getOrDefault("spec_id", ""));
                final String nifiId = String.valueOf(p.getOrDefault("nifi_id", ""));
                if (!specId.isBlank() && !nifiId.isBlank()) {
                    existingIdMap.put(specId, nifiId);
                }
            }
            try {
                final FlowBuilder.BuildResult res = flowBuilder.buildFlow(spec, req.process_group_id, nifiClient, existingIdMap, existing.size(), false, true);
                createdProcessors = res.createdProcessors();
                connectionsCreated = res.connectionsCreated();
                if (!createdProcessors.isEmpty()) {
                    explanation += "\n\n✅ Created " + createdProcessors.size() + " processor(s), " + connectionsCreated + " connection(s) on the canvas.";
                } else {
                    explanation += "\n\n⚠️ No processors could be created.";
                }
            } catch (Exception e) {
                final String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                explanation += "\n\n⚠️ Could not create flow on canvas: " + detail;
            }
        }

        final ChatResponse response = new ChatResponse();
        response.reply = explanation;
        response.connections_created = connectionsCreated;
        response.processors_deleted = deletedNames;
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

    private static int intValue(final Object v) {
        return NiFiClientOperations.intValue(v);
    }

    private static boolean bool(final Object v) {
        if (v == null) return false;
        return Boolean.parseBoolean(String.valueOf(v));
    }
}
