package org.apache.nifi.copilot.llm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;

@Component
public class LlmClient {
    private static final Logger logger = LoggerFactory.getLogger(LlmClient.class);
    private static final String MODELS_API_URL = "https://models.inference.ai.azure.com/chat/completions";
    private static final String DEFAULT_MODEL = System.getenv().getOrDefault("GITHUB_MODEL", "gpt-4o");
    private static final String DEFAULT_BEDROCK_MODEL = System.getenv().getOrDefault("KIRO_BEDROCK_MODEL", "us.anthropic.claude-sonnet-4-6");
    private static final Set<String> JSON_MODE_MODELS = Set.of("gpt-4o", "gpt-4o-mini", "o1", "o1-mini", "o3", "o3-mini", "o4-mini");
    private static final Pattern FENCE_START = Pattern.compile("^```(?:json)?\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern FENCE_END = Pattern.compile("\\s*```$");
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

    private static final String SYSTEM_PROMPT = """
You are an Apache NiFi expert embedded in the NiFi canvas UI.
Design and AUTO-CONFIGURE data flows. Always return ONE valid JSON object — no markdown, no extra text.

=== OUTPUT SCHEMA ===
{
  "explanation": "<plain-text summary>",
  "process_group": {"name": "...", "x": 400, "y": 300},
  "parameter_context": {"name": "...", "parameters": {"key": "value"}, "description": ""},
  "controller_services": [{"id": "cs1", "type": "<FQN>", "name": "...", "properties": {}}],
  "processors": [{"id": "proc1", "type": "<FQN>", "name": "...", "config": {}}],
  "funnels": [{"id": "funnel1"}],
  "connections": [{"from": "proc1", "to": "proc2", "relationships": ["<exact discovered relationship>"]}],
  "deletions": [{"type": "processor|process_group|controller_service|parameter_context", "spec_id": "...", "name": "..."}],
  "cs_actions": [{"name": "<exact service name>", "action": "enable|disable"}]
}

Include only fields you need:
- process_group: named group requested by user
- parameter_context: when #{param} references help; processors use #{param_name} in property values
- controller_services: NEW services; processors reference by spec id (e.g. "cs1")
- deletions: when user asks to delete/remove; set processors:[] for delete-only requests
- cs_actions: to enable/disable EXISTING services from [CANVAS CONTEXT]; set processors:[] and connections:[]

=== CANVAS LAYOUT ===
Do not include x/y on processors. The backend lays out the connection graph deterministically.

=== CONNECTIONS ===
- Connect distinct components only. Never connect a processor to itself.
- Omit terminal/unused relationships; the backend auto-terminates them.
- Parallel workers sharing result logging must converge into ONE shared terminal logger.
- If user requests separate success/failure logging, set "preserve_separate_terminal": true on both loggers.
- Create a self-loop only when the user explicitly requests it, with "allow_self_loop": true.

=== EXPLANATION RULES ===
Plain text, no markdown. 1-line summary, per-processor what+config, ⚠️ on placeholders.

=== PROCESSOR CONFIG ===
- Keys = NiFi property display names
- Dynamic properties: attribute name as key
- Controller service ref: set value to service spec id (e.g. "cs1")
- Parameter ref: #{param_name}
- Never invent processor types, controller-service types, properties, relationships, or scheduling strategies.
- Supply every discovered required property whose default is <none>. Use a placeholder when value unknown.
- Controller services are allowed only when a discovered property descriptor requires their API.
- Omit a component or property rather than guessing. Prefer the simplest valid flow.

=== CANVAS CONTEXT ===
IF message starts with [CANVAS CONTEXT]: those components exist — do NOT recreate them.
Output only NEW processors; connections may reference existing spec_ids.
Pick fresh ids continuing from existing ones. Never mention spec_ids in explanation.
No [CANVAS CONTEXT] = empty canvas, design the full flow.
Never refuse — always produce the best flow from the description.

=== DELETIONS ===
User says delete/remove/clear: populate "deletions" with {type, spec_id, name} from canvas context.
Set processors:[] unless also creating new ones.
            """;

    public Map<String, Object> generateFlowSpec(
            final String userMessage,
            final List<Map<String, String>> history,
            final String githubToken,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices,
            final List<Map<String, Object>> existingProcessGroups,
            final List<Map<String, Object>> existingConnections,
            final String capabilityContext) {
        final String selectedModel = (model == null || model.isBlank()) ? DEFAULT_MODEL : model;
        final List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", systemPrompt(capabilityContext)));
        final int start = Math.max(0, history.size() - 4);
        for (int i = start; i < history.size(); i++) {
            messages.add(new HashMap<>(history.get(i)));
        }
        messages.add(Map.of("role", "user", "content", buildUserMessage(
                userMessage, existingProcessors, existingControllerServices,
                existingProcessGroups, existingConnections)));

        final Map<String, Object> payload = new HashMap<>();
        payload.put("model", selectedModel);
        payload.put("messages", messages);
        payload.put("temperature", 0.2);
        payload.put("stream", false);
        if (JSON_MODE_MODELS.contains(selectedModel.toLowerCase())) {
            payload.put("response_format", Map.of("type", "json_object"));
        }

        try {
            final HttpRequest req = HttpRequest.newBuilder(URI.create(MODELS_API_URL))
                    .header("Authorization", "Bearer " + githubToken)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
                    .build();
            final HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 401) {
                throw new SecurityException("GitHub token rejected by Models API.");
            }
            if (resp.statusCode() >= 400) {
                throw new RuntimeException("GitHub Models API error: HTTP " + resp.statusCode());
            }
            final Map<String, Object> data = mapper.readValue(resp.body(), new TypeReference<>() {
            });
            final List<Map<String, Object>> choices = (List<Map<String, Object>>) data.getOrDefault("choices", List.of());
            final Map<String, Object> msgObj = choices.isEmpty()
                    ? Map.of()
                    : (Map<String, Object>) choices.getFirst().getOrDefault("message", Map.of());
            final String content = String.valueOf(msgObj.getOrDefault("content", "{}"));
            final Map<String, Object> spec = normalizeGeneratedLayout(extractJson(content));
            final Map<String, Object> usage = data.containsKey("usage")
                    ? (Map<String, Object>) data.get("usage")
                    : Map.of();
            spec.put("_token_usage", Map.of(
                    "input", intValue(usage.get("prompt_tokens")),
                    "output", intValue(usage.get("completion_tokens")),
                    "total", intValue(usage.get("total_tokens"))));
            return spec;
        } catch (SecurityException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Map<String, Object> generateFlowSpecBedrock(
            final String userMessage,
            final List<Map<String, String>> history,
            final Map<String, String> awsCreds,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices,
            final List<Map<String, Object>> existingProcessGroups,
            final List<Map<String, Object>> existingConnections,
            final String capabilityContext) {
        final List<Map<String, Object>> messages = new ArrayList<>();
        final int start = Math.max(0, history.size() - 4);
        for (int i = start; i < history.size(); i++) {
            messages.add(new HashMap<>(history.get(i)));
        }
        messages.add(Map.of("role", "user", "content", buildUserMessage(
                userMessage, existingProcessors, existingControllerServices,
                existingProcessGroups, existingConnections)));
        final Map<String, Object> body = new HashMap<>();
        body.put("anthropic_version", "bedrock-2023-05-31");
        body.put("max_tokens", 4096);
        body.put("system", systemPrompt(capabilityContext));
        body.put("messages", messages);
        body.put("temperature", 0.2);
        final String modelId = (model == null || model.isBlank()) ? DEFAULT_BEDROCK_MODEL : model;
        try (BedrockRuntimeClient br = BedrockRuntimeClient.builder()
                .region(Region.of(awsCreds.get("region_name")))
                .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
                        awsCreds.get("aws_access_key_id"),
                        awsCreds.get("aws_secret_access_key"),
                        awsCreds.get("aws_session_token"))))
                .httpClient(UrlConnectionHttpClient.create())
                .build()) {
            final var request = InvokeModelRequest.builder()
                    .modelId(modelId)
                    .contentType("application/json")
                    .accept("application/json")
                    .body(SdkBytes.fromUtf8String(mapper.writeValueAsString(body)))
                    .build();
            final var response = br.invokeModel(request);
            final Map<String, Object> result = mapper.readValue(response.body().asUtf8String(), new TypeReference<>() {
            });
            final List<?> content = (List<?>) result.get("content");
            final Map<?, ?> first = (Map<?, ?>) content.getFirst();
            final String text = String.valueOf(first.get("text"));
            final Map<String, Object> spec = normalizeGeneratedLayout(extractJson(text));
            final Map<String, Object> usage = result.containsKey("usage")
                    ? (Map<String, Object>) result.get("usage")
                    : Map.of();
            spec.put("_token_usage", Map.of(
                    "input", intValue(usage.get("input_tokens")),
                    "output", intValue(usage.get("output_tokens")),
                    "total", intValue(usage.get("input_tokens")) + intValue(usage.get("output_tokens"))));
            return spec;
        } catch (Exception e) {
            throw new RuntimeException("Amazon Bedrock API error: " + e.getMessage(), e);
        }
    }

    String buildUserMessage(
            final String userMessage,
            final List<Map<String, Object>> existingProcessors,
            final List<Map<String, Object>> existingControllerServices,
            final List<Map<String, Object>> existingProcessGroups,
            final List<Map<String, Object>> existingConnections) {
        final boolean hasProc = existingProcessors != null && !existingProcessors.isEmpty();
        final boolean hasCs = existingControllerServices != null && !existingControllerServices.isEmpty();
        final boolean hasGroups = existingProcessGroups != null && !existingProcessGroups.isEmpty();
        final boolean hasConnections = existingConnections != null && !existingConnections.isEmpty();
        if (!hasProc && !hasCs && !hasGroups && !hasConnections) {
            return userMessage;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("[CANVAS CONTEXT]\n");
        if (hasProc) {
            for (Map<String, Object> p : existingProcessors) {
                sb.append("  spec_id=").append(p.getOrDefault("spec_id", ""))
                        .append(" name=").append(p.getOrDefault("name", ""))
                        .append(" type=").append(p.getOrDefault("type", ""))
                        .append('\n');
            }
        }
        if (hasConnections) {
            sb.append("\nConnections on canvas:\n");
            for (Map<String, Object> connection : existingConnections) {
                sb.append("  from=").append(connection.getOrDefault("from", ""))
                        .append(" to=").append(connection.getOrDefault("to", ""))
                        .append(" relationships=")
                        .append(connection.getOrDefault("relationships", List.of()))
                        .append('\n');
            }
        }
        if (hasGroups) {
            sb.append("\nProcess groups on canvas:\n");
            for (Map<String, Object> group : existingProcessGroups) {
                sb.append("  spec_id=").append(group.getOrDefault("spec_id", ""))
                        .append(" name=").append(group.getOrDefault("name", ""))
                        .append('\n');
            }
        }
        if (hasCs) {
            sb.append("\nController services on canvas:\n");
            for (Map<String, Object> cs : existingControllerServices) {
                sb.append("  name=").append(cs.getOrDefault("name", ""))
                        .append(" type=").append(cs.getOrDefault("type", ""))
                        .append(" state=").append(cs.getOrDefault("state", "DISABLED"))
                        .append('\n');
            }
        }
        sb.append("\n[USER REQUEST]\n").append(userMessage);
        return sb.toString();
    }

    String systemPrompt(final String capabilityContext) {
        if (capabilityContext == null || capabilityContext.isBlank()) {
            return SYSTEM_PROMPT;
        }
        return SYSTEM_PROMPT + "\n\n" + capabilityContext;
    }

    private Map<String, Object> extractJson(String text) {
        text = text.trim();
        text = FENCE_START.matcher(text).replaceFirst("");
        text = FENCE_END.matcher(text).replaceFirst("");
        text = text.trim();
        final int start = text.indexOf('{');
        final int end = text.lastIndexOf('}');
        if (start >= 0 && end > start) {
            text = text.substring(start, end + 1);
        }
        try {
            return mapper.readValue(text, new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("LLM returned invalid JSON: " + e.getMessage(), e);
        }
    }

    Map<String, Object> normalizeGeneratedLayout(final Map<String, Object> specification) {
        final Set<String> terminalLoggerIds = new HashSet<>();
        final List<Map<String, Object>> normalizedProcessors = new ArrayList<>();
        final Object processorsValue = specification.get("processors");
        if (processorsValue instanceof List<?> processors) {
            for (Object processorValue : processors) {
                if (processorValue instanceof Map<?, ?> processor) {
                    final Map<String, Object> normalizedProcessor = copyStringMap(processor);
                    normalizedProcessor.remove("x");
                    normalizedProcessor.remove("y");
                    normalizedProcessors.add(normalizedProcessor);
                    if (isTerminalLogger(normalizedProcessor)) {
                        terminalLoggerIds.add(String.valueOf(normalizedProcessor.get("id")));
                    }
                }
            }
            specification.put("processors", normalizedProcessors);
        }

        // Parse connections into a mutable list for the normalizer, preserving non-map values
        final List<Map<String, Object>> normalizedConnections = new ArrayList<>();
        final List<Object> otherConnectionValues = new ArrayList<>();
        final Object connectionsValue = specification.get("connections");
        if (connectionsValue instanceof List<?> connections) {
            for (Object connectionValue : connections) {
                if (connectionValue instanceof Map<?, ?> connection) {
                    normalizedConnections.add(copyStringMap(connection));
                } else if (connectionValue == null) {
                    logger.warn("Removed null entry from generated connections");
                } else {
                    otherConnectionValues.add(connectionValue);
                }
            }
        }

        expandParallelWorkers(normalizedProcessors, normalizedConnections);

        // Delegate terminal-logger partitioning to the focused helper
        new TerminalLoggerNormalizer().normalize(
                normalizedProcessors, terminalLoggerIds, normalizedConnections);

        // Final pass: remove invalid self-loops and terminal outgoing edges; deduplicate by key
        final Map<String, Map<String, Object>> dedupMap = new LinkedHashMap<>();
        for (final Map<String, Object> conn : normalizedConnections) {
            final Object source = conn.get("from");
            final Object destination = conn.get("to");
            if (source != null && source.equals(destination)
                    && !Boolean.TRUE.equals(conn.get("allow_self_loop"))) {
                logger.warn("Removed unintended generated self-loop for component {}", source);
                continue;
            }
            if (terminalLoggerIds.contains(String.valueOf(source))
                    && !Boolean.TRUE.equals(conn.get("allow_terminal_output"))) {
                logger.warn("Removed unintended outgoing connection from terminal logger {}", source);
                continue;
            }
            final String key = String.valueOf(source) + '\u0000' + destination;
            final Map<String, Object> existing = dedupMap.get(key);
            if (existing == null) {
                dedupMap.put(key, conn);
            } else {
                existing.put("relationships", mergedRelationships(
                        existing.get("relationships"), conn.get("relationships")));
            }
        }

        final List<Object> resultConnections = new ArrayList<>(dedupMap.values());
        for (final Object raw : otherConnectionValues) {
            resultConnections.add(Map.of("value", raw));
        }
        specification.put("connections", resultConnections);
        return specification;
    }

    private List<String> mergedRelationships(final Object first, final Object second) {
        final Set<String> relationships = new java.util.LinkedHashSet<>();
        if (first instanceof List<?> values) values.forEach(value -> relationships.add(String.valueOf(value)));
        if (second instanceof List<?> values) values.forEach(value -> relationships.add(String.valueOf(value)));
        return List.copyOf(relationships);
    }

    private Map<String, Object> copyStringMap(final Map<?, ?> source) {
        final Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(String.valueOf(key), value));
        return copy;
    }

    private boolean isTerminalLogger(final Map<?, ?> processor) {
        final String type = String.valueOf(processor.get("type")).toLowerCase();
        return type.equals("log") || type.equals("logattribute") || type.equals("logmessage")
                || type.endsWith(".logattribute") || type.endsWith(".logmessage");
    }

    private int intValue(final Object value) {
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Expands a generated DistributeLoad connection that routes multiple numbered
     * relationships to one worker into one distinct worker per relationship.
     */
    private void expandParallelWorkers(
            final List<Map<String, Object>> processors,
            final List<Map<String, Object>> connections) {
        final Map<String, Map<String, Object>> processorsById = new HashMap<>();
        final Set<String> usedIds = new HashSet<>();
        for (Map<String, Object> processor : processors) {
            final String id = String.valueOf(processor.getOrDefault("id", ""));
            if (!id.isBlank()) {
                processorsById.put(id, processor);
                usedIds.add(id);
            }
        }

        final List<Map<String, Object>> originalConnections = List.copyOf(connections);
        final List<Map<String, Object>> replacements = new ArrayList<>();
        final Set<Map<String, Object>> replaced = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());

        for (Map<String, Object> connection : originalConnections) {
            final String distributorId = String.valueOf(connection.getOrDefault("from", ""));
            final Map<String, Object> distributor = processorsById.get(distributorId);
            if (!isDistributeLoad(distributor)) {
                continue;
            }
            final List<String> relationships = numberedRelationships(connection.get("relationships"));
            if (relationships.size() < 2) {
                continue;
            }
            final String workerId = String.valueOf(connection.getOrDefault("to", ""));
            final Map<String, Object> worker = processorsById.get(workerId);
            if (worker == null || isDistributeLoad(worker)) {
                continue;
            }

            replaced.add(connection);
            for (int i = 0; i < relationships.size(); i++) {
                final String relationship = relationships.get(i);
                final String targetId;
                if (i == 0) {
                    targetId = workerId;
                } else {
                    targetId = uniqueCloneId(workerId, relationship, usedIds);
                    final Map<String, Object> clone = new LinkedHashMap<>(worker);
                    clone.put("id", targetId);
                    final String name = String.valueOf(worker.getOrDefault("name", workerId));
                    clone.put("name", name + " " + relationship);
                    processors.add(clone);
                    processorsById.put(targetId, clone);
                    for (Map<String, Object> outgoing : originalConnections) {
                        if (workerId.equals(String.valueOf(outgoing.get("from")))) {
                            final Map<String, Object> clonedConnection = new LinkedHashMap<>(outgoing);
                            clonedConnection.put("from", targetId);
                            replacements.add(clonedConnection);
                        }
                    }
                }
                final Map<String, Object> distributedConnection = new LinkedHashMap<>(connection);
                distributedConnection.put("to", targetId);
                distributedConnection.put("relationships", List.of(relationship));
                replacements.add(distributedConnection);
            }
        }

        if (!replaced.isEmpty()) {
            connections.removeIf(replaced::contains);
            connections.addAll(replacements);
        }
    }

    private static boolean isDistributeLoad(final Map<String, Object> processor) {
        return processor != null
                && String.valueOf(processor.getOrDefault("type", ""))
                        .toLowerCase(java.util.Locale.ROOT)
                        .endsWith("distributeload");
    }

    private static List<String> numberedRelationships(final Object value) {
        if (!(value instanceof List<?> relationships)) {
            return List.of();
        }
        final List<String> numbered = new ArrayList<>();
        for (Object relationship : relationships) {
            final String name = String.valueOf(relationship);
            if (!name.matches("\\d+")) {
                return List.of();
            }
            numbered.add(name);
        }
        return numbered;
    }

    private static String uniqueCloneId(
            final String workerId,
            final String relationship,
            final Set<String> usedIds) {
        final String base = workerId + "-parallel-" + relationship;
        String candidate = base;
        int suffix = 2;
        while (!usedIds.add(candidate)) {
            candidate = base + "-" + suffix++;
        }
        return candidate;
    }
}
