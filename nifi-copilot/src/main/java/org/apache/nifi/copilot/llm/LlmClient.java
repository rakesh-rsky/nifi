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
  "processors": [{"id": "proc1", "type": "<FQN>", "name": "...", "x": 100, "y": 200, "config": {}}],
  "connections": [{"from": "proc1", "to": "proc2", "relationships": ["success"]}],
  "deletions": [{"spec_id": "...", "name": "..."}],
  "cs_actions": [{"name": "<exact service name>", "action": "enable|disable"}]
}

Include only fields you need:
- process_group: named group requested by user
- parameter_context: when #{param} references help; processors use #{param_name} in property values
- controller_services: NEW services to create and enable (for new flows)
  Processors reference a service by setting its property value to the service spec id (e.g. "cs1")
- deletions: when user asks to delete/remove processors — set processors:[] for delete-only requests
- cs_actions: to enable or disable EXISTING controller services already on the canvas
  Use the exact name from the [CANVAS CONTEXT] controller services list.
  Set processors:[] and connections:[] unless also building a new flow.

=== CANVAS LAYOUT ===
Always include x/y on every processor (integers). Boxes ≈352×128px.
- Spacing: 400px horizontal, 250px vertical. Fresh flow: x=100, y=200
- Linear: same Y, +400 X each step. Branching: center source/merge, branches ±250 Y
- Appending: start at x = (existing_count × 400) + 100

=== EXPLANATION RULES ===
Plain text, no markdown. Include: 1-line summary, per-processor what+config, ⚠️ on placeholders,
note any controller services or parameter contexts and which processors use them.

=== PROCESSOR CONFIG ===
- Keys = NiFi property display names
- Dynamic properties (XPath destinations, UpdateAttribute attrs, RouteOnAttribute routes): attribute name as key
- Controller service ref: set value to service spec id (e.g. "cs1")
- Parameter ref: #{param_name}

=== CANVAS CONTEXT ===
IF the user message starts with [CANVAS CONTEXT]: those processors and controller services exist — do NOT recreate them.
Output only NEW processors; connections may reference existing spec_ids.
Pick fresh ids continuing from existing ones. Never mention spec_ids or this rule in explanation.
No [CANVAS CONTEXT] = empty canvas, design the full flow.
Never refuse or explain what you cannot do — always produce the best flow from the description.

=== DELETIONS ===
User says delete/remove/clear: populate "deletions" with {spec_id, name} from the canvas context.
Set processors:[] unless also creating new ones.
            """;

    public Map<String, Object> generateFlowSpec(
            final String userMessage,
            final List<Map<String, String>> history,
            final String githubToken,
            final List<Map<String, Object>> existingProcessors,
            final String model,
            final List<Map<String, Object>> existingControllerServices) {
        final String selectedModel = (model == null || model.isBlank()) ? DEFAULT_MODEL : model;
        final List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", SYSTEM_PROMPT));
        final int start = Math.max(0, history.size() - 6);
        for (int i = start; i < history.size(); i++) {
            messages.add(new HashMap<>(history.get(i)));
        }
        messages.add(Map.of("role", "user", "content", buildUserMessage(userMessage, existingProcessors, existingControllerServices)));

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
            final Map<String, Object> spec = extractJson(content);
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
        final List<Map<String, Object>> messages = new ArrayList<>();
        final int start = Math.max(0, history.size() - 6);
        for (int i = start; i < history.size(); i++) {
            messages.add(new HashMap<>(history.get(i)));
        }
        messages.add(Map.of("role", "user", "content", buildUserMessage(userMessage, existingProcessors, existingControllerServices)));
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
            final Map<String, Object> spec = extractJson(text);
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

    private String buildUserMessage(
            final String userMessage,
            final List<Map<String, Object>> existingProcessors,
            final List<Map<String, Object>> existingControllerServices) {
        final boolean hasProc = existingProcessors != null && !existingProcessors.isEmpty();
        final boolean hasCs = existingControllerServices != null && !existingControllerServices.isEmpty();
        if (!hasProc && !hasCs) {
            return userMessage;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("[CANVAS CONTEXT]\n");
        if (hasProc) {
            final int startX = 100 + existingProcessors.size() * 400;
            sb.append("Place new processors starting at x=").append(startX).append(".\n");
            for (Map<String, Object> p : existingProcessors) {
                sb.append("  spec_id=").append(p.getOrDefault("spec_id", ""))
                        .append(" name=").append(p.getOrDefault("name", ""))
                        .append(" type=").append(p.getOrDefault("type", ""))
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
