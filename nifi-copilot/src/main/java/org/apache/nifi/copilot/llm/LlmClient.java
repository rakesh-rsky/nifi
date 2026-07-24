package org.apache.nifi.copilot.llm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
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
  "explanation": "<plain-text — see EXPLANATION RULES>",
  "process_group": {"name": "...", "x": 400, "y": 300},
  "parameter_context": {"name": "...", "parameters": {"key": "value"}, "description": ""},
  "controller_services": [{"id": "cs1", "type": "<FQN>", "name": "...", "properties": {}}],
  "processors": [{"id": "proc1", "type": "<FQN>", "name": "...", "config": {}}],
  "funnels": [{"id": "funnel1"}],
  "connections": [{"from": "proc1", "to": "proc2", "relationships": ["success"], "allow_self_loop": false}],
  "deletions": [{"type": "processor|process_group|controller_service|parameter_context", "spec_id": "...", "name": "..."}],
  "cs_actions": [{"name": "<exact service name>", "action": "enable|disable"}]
}

Include only fields you need:
- process_group: named group requested by user
- parameter_context: when #{param} references help; processors use #{param_name} in property values
- controller_services: NEW services to create and enable (for new flows)
  Processors reference a service by setting its property value to the service spec id (e.g. "cs1")
- deletions: when user asks to delete/remove processors, process groups, controller services, or parameter contexts
  Use type "processor", "process_group", "controller_service", or "parameter_context";
  parameter contexts are global and are resolved by exact name; set processors:[] for delete-only requests
- cs_actions: to enable or disable EXISTING controller services already on the canvas
  Use the exact name from the [CANVAS CONTEXT] controller services list.
  Set processors:[] and connections:[] unless also building a new flow.

=== CANVAS LAYOUT ===
Do not include x/y on processors. The backend lays out the connection graph deterministically.

=== CONNECTIONS ===
- Connect distinct components only. Never connect a processor to itself to handle an unused relationship.
- Omit terminal and unused relationships from connections; the backend auto-terminates them.
- LogAttribute and LogMessage processors are terminal sinks. Never create outgoing connections from them.
- Parallel workers that share result logging must converge into ONE shared LogMessage or LogAttribute.
  For each worker, use one connection selecting all requested success and failure relationships.
  Never create one logger per worker or chain result loggers.
- Do not share a terminal logger between sequential or non-adjacent pipeline stages when its connection
  would cross intervening processors. Create a stage-specific terminal logger for each distant stage.
- Never create a Funnel only to combine logging routes. Connect each source directly to its terminal logger.
- If the user explicitly requests success and failure logging separately, create exactly two shared
  terminal loggers and set "preserve_separate_terminal": true on both. When a shared result logger
  already exists in CANVAS CONTEXT, reuse its exact spec_id for one outcome and create only one new logger.
- When the user requests N-way load distribution, use
  org.apache.nifi.processors.standard.DistributeLoad with "Number of Relationships" set to N and
  "Distribution Strategy" set to "round robin". Connect relationships "1" through "N" to the N workers.
- Create a self-loop only when the user explicitly requests feedback/retry to the same processor,
  and set "allow_self_loop": true on that connection.

=== EXPLANATION RULES ===
Plain text, no markdown. Include: 1-line summary, per-processor what+config, ⚠️ on placeholders,
note any controller services or parameter contexts and which processors use them.

=== PROCESSOR CONFIG ===
- Keys = NiFi property display names
- Dynamic properties (XPath destinations, UpdateAttribute attrs, RouteOnAttribute routes): attribute name as key
- Controller service ref: set value to service spec id (e.g. "cs1")
- Parameter ref: #{param_name}

=== CANVAS CONTEXT ===
IF the user message starts with [CANVAS CONTEXT]: those processors, connections, process groups, and controller services exist — do NOT recreate them.
Output only NEW processors; connections may reference existing spec_ids.
Pick fresh ids continuing from existing ones. Never mention spec_ids or this rule in explanation.
No [CANVAS CONTEXT] = empty canvas, design the full flow.
Never refuse or explain what you cannot do — always produce the best flow from the description.

=== DELETIONS ===
User says delete/remove/clear: populate "deletions" with {type, spec_id, name} from the canvas context.
Use "process group", not "processor group", when describing a process-group deletion.
Describe deletion as requested, never as already successful; the backend reports the actual result.
Set processors:[] unless also creating new ones.
            """;

    public Map<String, Object> generateFlowSpec(
            final String userMessage,
            final List<Map<String, String>> history,
            final String githubToken,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices) {
        return generateFlowSpec(userMessage, history, githubToken, existingProcessors, model,
                existingControllerServices, List.of());
    }

    public Map<String, Object> generateFlowSpec(
            final String userMessage,
            final List<Map<String, String>> history,
            final String githubToken,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices,
            final List<Map<String, Object>> existingProcessGroups) {
        return generateFlowSpec(userMessage, history, githubToken, existingProcessors, model,
                existingControllerServices, existingProcessGroups, List.of());
    }

    public Map<String, Object> generateFlowSpec(
            final String userMessage,
            final List<Map<String, String>> history,
            final String githubToken,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices,
            final List<Map<String, Object>> existingProcessGroups,
            final List<Map<String, Object>> existingConnections) {
        final String selectedModel = (model == null || model.isBlank()) ? DEFAULT_MODEL : model;
        final List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", SYSTEM_PROMPT));
        final int start = Math.max(0, history.size() - 6);
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
            final List<Map<String, Object>> existingControllerServices) {
        return generateFlowSpecBedrock(userMessage, history, awsCreds, existingProcessors, model,
                existingControllerServices, List.of());
    }

    public Map<String, Object> generateFlowSpecBedrock(
            final String userMessage,
            final List<Map<String, String>> history,
            final Map<String, String> awsCreds,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices,
            final List<Map<String, Object>> existingProcessGroups) {
        return generateFlowSpecBedrock(userMessage, history, awsCreds, existingProcessors, model,
                existingControllerServices, existingProcessGroups, List.of());
    }

    public Map<String, Object> generateFlowSpecBedrock(
            final String userMessage,
            final List<Map<String, String>> history,
            final Map<String, String> awsCreds,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices,
            final List<Map<String, Object>> existingProcessGroups,
            final List<Map<String, Object>> existingConnections) {
        final List<Map<String, Object>> messages = new ArrayList<>();
        final int start = Math.max(0, history.size() - 6);
        for (int i = start; i < history.size(); i++) {
            messages.add(new HashMap<>(history.get(i)));
        }
        messages.add(Map.of("role", "user", "content", buildUserMessage(
                userMessage, existingProcessors, existingControllerServices,
                existingProcessGroups, existingConnections)));
        final Map<String, Object> body = new HashMap<>();
        body.put("anthropic_version", "bedrock-2023-05-31");
        body.put("max_tokens", 4096);
        body.put("system", SYSTEM_PROMPT);
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

        final Object connectionsValue = splitSequentialTerminalFanIn(
                normalizedProcessors, terminalLoggerIds, specification.get("connections"));
        specification.put("connections", connectionsValue);
        final String sharedLoggerId = sharedLoggerId(
                normalizedProcessors, terminalLoggerIds, connectionsValue);
        if (sharedLoggerId != null) {
            normalizedProcessors.removeIf(processor -> terminalLoggerIds.contains(String.valueOf(processor.get("id")))
                    && !sharedLoggerId.equals(String.valueOf(processor.get("id"))));
            normalizedProcessors.stream()
                    .filter(processor -> sharedLoggerId.equals(String.valueOf(processor.get("id"))))
                    .findFirst()
                    .ifPresent(processor -> processor.put("name", "Log API Results"));
        }

        if (connectionsValue instanceof List<?> connections) {
            final Map<String, Map<String, Object>> normalizedConnections = new LinkedHashMap<>();
            for (Object connectionValue : connections) {
                if (connectionValue instanceof Map<?, ?> connection) {
                    final Map<String, Object> normalizedConnection = copyStringMap(connection);
                    final Object source = normalizedConnection.get("from");
                    Object destination = normalizedConnection.get("to");
                    if (source != null && source.equals(destination)
                            && !Boolean.TRUE.equals(normalizedConnection.get("allow_self_loop"))) {
                        logger.warn("Removed unintended generated self-loop for component {}", source);
                        continue;
                    }
                    if (terminalLoggerIds.contains(String.valueOf(source))
                            && !Boolean.TRUE.equals(normalizedConnection.get("allow_terminal_output"))) {
                        logger.warn("Removed unintended outgoing connection from terminal logger {}", source);
                        continue;
                    }
                    if (sharedLoggerId != null && terminalLoggerIds.contains(String.valueOf(destination))) {
                        destination = sharedLoggerId;
                        normalizedConnection.put("to", sharedLoggerId);
                    }
                    final String key = String.valueOf(source) + '\u0000' + destination;
                    final Map<String, Object> existing = normalizedConnections.get(key);
                    if (existing == null) {
                        normalizedConnections.put(key, normalizedConnection);
                    } else {
                        existing.put("relationships", mergedRelationships(
                                existing.get("relationships"), normalizedConnection.get("relationships")));
                    }
                } else {
                    normalizedConnections.put("raw-" + normalizedConnections.size(),
                            Map.of("value", connectionValue));
                }
            }
            specification.put("connections", new ArrayList<>(normalizedConnections.values()));
        }
        return specification;
    }

    private Object splitSequentialTerminalFanIn(
            final List<Map<String, Object>> processors,
            final Set<String> loggerIds,
            final Object connectionsValue) {
        if (!(connectionsValue instanceof List<?> connections) || loggerIds.isEmpty()) {
            return connectionsValue;
        }
        final List<Object> normalizedValues = new ArrayList<>();
        final List<Map<String, Object>> normalizedConnections = new ArrayList<>();
        for (Object connectionValue : connections) {
            if (connectionValue instanceof Map<?, ?> connection) {
                final Map<String, Object> normalizedConnection = copyStringMap(connection);
                normalizedConnections.add(normalizedConnection);
                normalizedValues.add(normalizedConnection);
            } else {
                normalizedValues.add(connectionValue);
            }
        }
        final Map<String, Set<String>> outgoing = outgoingBySource(normalizedConnections);
        final Map<String, Map<String, Object>> processorsById = new HashMap<>();
        processors.forEach(processor ->
                processorsById.put(String.valueOf(processor.get("id")), processor));
        final Set<String> usedIds = new HashSet<>(processorsById.keySet());

        for (String loggerId : List.copyOf(loggerIds)) {
            final List<String> sources = normalizedConnections.stream()
                    .filter(connection -> loggerId.equals(String.valueOf(connection.get("to"))))
                    .filter(connection -> !loggerId.equals(String.valueOf(connection.get("from"))))
                    .map(connection -> String.valueOf(connection.get("from")))
                    .distinct()
                    .toList();
            if (sources.size() < 2 || !containsSequentialSources(sources, outgoing)) {
                continue;
            }
            final Map<String, Object> loggerProcessor = processorsById.get(loggerId);
            if (loggerProcessor == null) {
                continue;
            }
            loggerProcessor.put("preserve_separate_terminal", true);
            for (int sourceIndex = 1; sourceIndex < sources.size(); sourceIndex++) {
                final String sourceId = sources.get(sourceIndex);
                final String cloneId = uniqueLoggerId(loggerId, sourceIndex + 1, usedIds);
                final Map<String, Object> clone = new LinkedHashMap<>(loggerProcessor);
                clone.put("id", cloneId);
                clone.put("name", stageLoggerName(loggerProcessor, processorsById.get(sourceId)));
                processors.add(clone);
                processorsById.put(cloneId, clone);
                loggerIds.add(cloneId);
                normalizedConnections.stream()
                        .filter(connection -> sourceId.equals(String.valueOf(connection.get("from")))
                                && loggerId.equals(String.valueOf(connection.get("to"))))
                        .forEach(connection -> connection.put("to", cloneId));
            }
            logger.warn("Split terminal logger {} across {} sequential pipeline stages",
                    loggerId, sources.size());
        }
        return normalizedValues;
    }

    private Map<String, Set<String>> outgoingBySource(
            final List<Map<String, Object>> connections) {
        final Map<String, Set<String>> outgoing = new HashMap<>();
        for (Map<String, Object> connection : connections) {
            final Object source = connection.get("from");
            final Object destination = connection.get("to");
            if (source != null && destination != null) {
                outgoing.computeIfAbsent(String.valueOf(source), ignored -> new HashSet<>())
                        .add(String.valueOf(destination));
            }
        }
        return outgoing;
    }

    private boolean containsSequentialSources(
            final List<String> sources,
            final Map<String, Set<String>> outgoing) {
        for (int left = 0; left < sources.size(); left++) {
            for (int right = left + 1; right < sources.size(); right++) {
                if (isReachable(sources.get(left), sources.get(right), outgoing)
                        || isReachable(sources.get(right), sources.get(left), outgoing)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isReachable(
            final String source,
            final String destination,
            final Map<String, Set<String>> outgoing) {
        final ArrayDeque<String> pending = new ArrayDeque<>();
        final Set<String> visited = new HashSet<>();
        pending.add(source);
        while (!pending.isEmpty()) {
            final String current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            for (String next : outgoing.getOrDefault(current, Set.of())) {
                if (destination.equals(next)) {
                    return true;
                }
                pending.addLast(next);
            }
        }
        return false;
    }

    private String uniqueLoggerId(
            final String loggerId,
            final int stageNumber,
            final Set<String> usedIds) {
        String candidate = loggerId + "-stage-" + stageNumber;
        int suffix = stageNumber;
        while (!usedIds.add(candidate)) {
            candidate = loggerId + "-stage-" + ++suffix;
        }
        return candidate;
    }

    private String stageLoggerName(
            final Map<String, Object> loggerProcessor,
            final Map<String, Object> sourceProcessor) {
        final String loggerName = String.valueOf(
                loggerProcessor.getOrDefault("name", "Log Failure"));
        if (sourceProcessor == null) {
            return loggerName + " - Stage";
        }
        final String sourceName = String.valueOf(
                sourceProcessor.getOrDefault("name", sourceProcessor.getOrDefault("id", "Stage")));
        return loggerName + " - " + sourceName;
    }

    private String sharedLoggerId(final List<Map<String, Object>> processors,
                                  final Set<String> loggerIds,
                                  final Object connectionsValue) {
        if (loggerIds.size() < 2 || processors.stream()
                .filter(processor -> loggerIds.contains(String.valueOf(processor.get("id"))))
                .anyMatch(processor -> Boolean.TRUE.equals(processor.get("preserve_separate_terminal")))) {
            return null;
        }
        final Set<String> loggerSources = new HashSet<>();
        if (connectionsValue instanceof List<?> connections) {
            for (Object connectionValue : connections) {
                if (connectionValue instanceof Map<?, ?> connection
                        && loggerIds.contains(String.valueOf(connection.get("to")))) {
                    loggerSources.add(String.valueOf(connection.get("from")));
                }
            }
        }
        if (loggerSources.size() < 3) {
            return null;
        }
        return processors.stream()
                .map(processor -> String.valueOf(processor.get("id")))
                .filter(loggerIds::contains)
                .findFirst()
                .orElse(null);
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
}
