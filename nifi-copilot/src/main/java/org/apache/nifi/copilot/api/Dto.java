package org.apache.nifi.copilot.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Dto {
    private Dto() {
    }

    public record HistoryEntry(String role, String content) {
    }

    public record ExistingProcessor(String spec_id, String nifi_id, String name, String type) {
    }

    public static final class ChatRequest {
        public String message;
        public String process_group_id = "root";
        public List<HistoryEntry> history = new ArrayList<>();
        public List<ExistingProcessor> existing_processors = new ArrayList<>();
        public String provider = "github";
        public String model = "";
        public boolean read_canvas = true;
    }

    public record CreatedProcessor(String id, String spec_id, String name, String type) {
    }

    public static final class ChatResponse {
        public String reply;
        public List<CreatedProcessor> processors_created = new ArrayList<>();
        public int connections_created;
        public List<String> processors_deleted = new ArrayList<>();
        public Map<String, Object> tokens_used;
    }

    public record DeviceFlowResponse(String user_code, String verification_uri, int expires_in) {
    }

    public static final class AuthStatusResponse {
        public boolean authenticated;
        public String login = "";
        public boolean device_flow_active;
        public String user_code = "";
        public String verification_uri = "";
    }

    public static final class AwsAuthStatusResponse {
        public boolean authenticated;
        public boolean device_flow_active;
        public String user_code = "";
        public String verification_uri = "";
        public boolean role_selection_needed;
        public List<Map<String, Object>> available_roles = new ArrayList<>();
        public String sso_start_url = "";
        public String sso_region = "us-east-1";
        public String bedrock_region = "";
        public String account_id = "";
        public String role_name = "";
    }

    public static final class AwsStartRequest {
        public String sso_start_url;
        public String sso_region = "us-east-1";
        public String bedrock_region = "";
    }

    public static final class AwsSelectRoleRequest {
        public String account_id;
        public String role_name;
    }

    public static final class SessionData {
        public List<Map<String, Object>> messages = new ArrayList<>();
        public List<Map<String, Object>> sessionProcessors = new ArrayList<>();
        public String selectedModel = "";
    }
}
