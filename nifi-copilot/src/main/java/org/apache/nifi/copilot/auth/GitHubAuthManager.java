package org.apache.nifi.copilot.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.awt.Desktop;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class GitHubAuthManager {
    private static final Logger logger = LoggerFactory.getLogger(GitHubAuthManager.class);
    private static final String CLIENT_ID = System.getenv().getOrDefault("GITHUB_CLIENT_ID", "Ov23liyg73EcOb1oAARC");
    private static final String DEVICE_CODE_URL = "https://github.com/login/device/code";
    private static final String OAUTH_TOKEN_URL = "https://github.com/login/oauth/access_token";
    private static final String USER_URL = "https://api.github.com/user";
    private static final String SCOPES = "read:user";
    private static final Path CREDENTIALS = Path.of(System.getProperty("user.home"), ".nifi-copilot", "credentials.json");

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();

    private volatile String githubToken = "";
    private volatile String githubLogin = "";
    private volatile String deviceCode = "";
    private volatile String userCode = "";
    private volatile String verificationUri = "https://github.com/login/device";
    private volatile long deviceExpiresAtEpochSec = 0;
    private volatile int pollIntervalSec = 5;
    private volatile ScheduledFuture<?> pollingTask;
    private volatile int generation = 0;

    public GitHubAuthManager() {
        loadCreds();
    }

    public boolean isAuthenticated() {
        return !githubToken.isBlank();
    }

    public boolean deviceFlowActive() {
        return !userCode.isBlank() && !isAuthenticated() && InstantNow.epochSec() < deviceExpiresAtEpochSec;
    }

    public Map<String, Object> authStatus() {
        final Map<String, Object> status = new HashMap<>();
        status.put("authenticated", isAuthenticated());
        status.put("login", githubLogin);
        status.put("device_flow_active", deviceFlowActive());
        status.put("user_code", deviceFlowActive() ? userCode : "");
        status.put("verification_uri", verificationUri);
        return status;
    }

    public Map<String, Object> startDeviceFlow() {
        try {
            if (pollingTask != null && !pollingTask.isDone()) {
                pollingTask.cancel(true);
            }
            final String body = "client_id=" + CLIENT_ID + "&scope=" + SCOPES;
            final HttpRequest req = HttpRequest.newBuilder(URI.create(DEVICE_CODE_URL))
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            final HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 400) {
                throw new RuntimeException("GitHub device flow failed: HTTP " + resp.statusCode());
            }
            final Map<String, Object> data = mapper.readValue(resp.body(), new TypeReference<>() {
            });
            deviceCode = String.valueOf(data.get("device_code"));
            userCode = String.valueOf(data.get("user_code"));
            verificationUri = String.valueOf(data.getOrDefault("verification_uri", "https://github.com/login/device"));
            final int expires = Integer.parseInt(String.valueOf(data.getOrDefault("expires_in", 900)));
            deviceExpiresAtEpochSec = InstantNow.epochSec() + expires;
            pollIntervalSec = Math.max(Integer.parseInt(String.valueOf(data.getOrDefault("interval", 5))), 5);
            generation++;
            openBrowser(verificationUri);
            pollingTask = poller.schedule(this::pollToken, pollIntervalSec, TimeUnit.SECONDS);
            return Map.of("user_code", userCode, "verification_uri", verificationUri, "expires_in", expires);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getGitHubToken() {
        if (!isAuthenticated()) {
            throw new SecurityException("Not authenticated with GitHub. Sign in first.");
        }
        return githubToken;
    }

    public boolean validateSavedToken() {
        if (!isAuthenticated()) {
            return false;
        }
        try {
            final HttpRequest req = HttpRequest.newBuilder(URI.create(USER_URL))
                    .header("Authorization", "token " + githubToken)
                    .GET()
                    .build();
            final HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                final Map<String, Object> user = mapper.readValue(resp.body(), new TypeReference<>() {
                });
                githubLogin = String.valueOf(user.getOrDefault("login", githubLogin));
                return true;
            }
            githubToken = "";
            githubLogin = "";
            deleteCreds();
            return false;
        } catch (Exception e) {
            logger.warn("Token validation failed: {}", e.getMessage());
            return true;
        }
    }

    public void logout() {
        generation++;
        githubToken = "";
        githubLogin = "";
        userCode = "";
        deviceCode = "";
        if (pollingTask != null) {
            pollingTask.cancel(false);
        }
        deleteCreds();
    }

    private void pollToken() {
        final int myGeneration = generation;
        if (InstantNow.epochSec() >= deviceExpiresAtEpochSec || deviceCode.isBlank()) {
            logger.info("GitHub device flow expired or device code cleared — stopping poll");
            userCode = "";
            return;
        }
        try {
            final String body = "client_id=" + CLIENT_ID
                    + "&device_code=" + deviceCode
                    + "&grant_type=urn:ietf:params:oauth:grant-type:device_code";
            final HttpRequest req = HttpRequest.newBuilder(URI.create(OAUTH_TOKEN_URL))
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            final HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            logger.info("GitHub token poll HTTP {}: {}", resp.statusCode(), resp.body());
            final Map<String, Object> data = mapper.readValue(resp.body(), new TypeReference<>() {
            });
            if (data.containsKey("access_token")) {
                githubToken = String.valueOf(data.get("access_token"));
                userCode = "";
                deviceCode = "";
                fetchUser();
                saveCreds();
                logger.info("GitHub authentication successful, login={}", githubLogin);
                return;
            }
            final String error = String.valueOf(data.getOrDefault("error", ""));
            if ("slow_down".equals(error)) {
                // GitHub mandates waiting for the interval it returns — update and honour it
                final int required = Integer.parseInt(String.valueOf(data.getOrDefault("interval", pollIntervalSec + 5)));
                pollIntervalSec = Math.max(required, pollIntervalSec + 5);
                logger.info("GitHub token poll slow_down: next poll in {}s", pollIntervalSec);
            } else if (!error.isBlank() && !"authorization_pending".equals(error)) {
                logger.warn("GitHub token poll failed with error: {}", error);
                userCode = "";
                return;
            }
        } catch (Exception e) {
            logger.warn("GitHub token poll exception: {}", e.getMessage(), e);
        }
        // Reschedule only if this is still the active flow (no new startDeviceFlow call raced us)
        if (myGeneration == generation) {
            pollingTask = poller.schedule(this::pollToken, pollIntervalSec, TimeUnit.SECONDS);
        }
    }

    private void fetchUser() {
        try {
            final HttpRequest req = HttpRequest.newBuilder(URI.create(USER_URL))
                    .header("Authorization", "token " + githubToken)
                    .GET()
                    .build();
            final HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                final Map<String, Object> user = mapper.readValue(resp.body(), new TypeReference<>() {
                });
                githubLogin = String.valueOf(user.getOrDefault("login", ""));
            }
        } catch (Exception ignored) {
        }
    }

    private void openBrowser(final String url) {
        try {
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(URI.create(url));
            }
        } catch (Exception e) {
            logger.warn("Could not open browser: {}", e.getMessage());
        }
    }

    private void loadCreds() {
        try {
            if (!Files.exists(CREDENTIALS)) {
                return;
            }
            final Map<String, Object> data = mapper.readValue(Files.readString(CREDENTIALS, StandardCharsets.UTF_8), new TypeReference<>() {
            });
            githubToken = String.valueOf(data.getOrDefault("github_token", ""));
            githubLogin = String.valueOf(data.getOrDefault("github_login", ""));
        } catch (Exception e) {
            logger.warn("Could not load credentials: {}", e.getMessage());
        }
    }

    private void saveCreds() {
        try {
            Files.createDirectories(CREDENTIALS.getParent());
            final Map<String, Object> data;
            if (Files.exists(CREDENTIALS)) {
                data = mapper.readValue(Files.readString(CREDENTIALS, StandardCharsets.UTF_8), new TypeReference<>() {
                });
            } else {
                data = new HashMap<>();
            }
            data.put("github_token", githubToken);
            data.put("github_login", githubLogin);
            Files.writeString(CREDENTIALS, mapper.writeValueAsString(data), StandardCharsets.UTF_8);
            try {
                Files.setPosixFilePermissions(CREDENTIALS, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
            } catch (UnsupportedOperationException ignored) {
            }
        } catch (Exception e) {
            logger.warn("Could not save credentials: {}", e.getMessage());
        }
    }

    private void deleteCreds() {
        try {
            if (!Files.exists(CREDENTIALS)) {
                return;
            }
            final Map<String, Object> data = mapper.readValue(Files.readString(CREDENTIALS, StandardCharsets.UTF_8), new TypeReference<>() {
            });
            data.remove("github_token");
            data.remove("github_login");
            if (data.isEmpty()) {
                Files.deleteIfExists(CREDENTIALS);
            } else {
                Files.writeString(CREDENTIALS, mapper.writeValueAsString(data), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            logger.warn("Could not delete credentials: {}", e.getMessage());
        }
    }
}
