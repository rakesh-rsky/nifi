package org.apache.nifi.copilot.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sso.SsoClient;
import software.amazon.awssdk.services.sso.model.GetRoleCredentialsRequest;
import software.amazon.awssdk.services.sso.model.ListAccountRolesRequest;
import software.amazon.awssdk.services.sso.model.ListAccountsRequest;
import software.amazon.awssdk.services.ssooidc.SsoOidcClient;
import software.amazon.awssdk.services.ssooidc.model.CreateTokenRequest;
import software.amazon.awssdk.services.ssooidc.model.ExpiredTokenException;
import software.amazon.awssdk.services.ssooidc.model.RegisterClientRequest;
import software.amazon.awssdk.services.ssooidc.model.StartDeviceAuthorizationRequest;

@Component
public class AwsAuthManager {
    private static final Logger logger = LoggerFactory.getLogger(AwsAuthManager.class);
    private static final Path CREDENTIALS = Path.of(System.getProperty("user.home"), ".nifi-copilot", "credentials.json");
    private static final String DEFAULT_REGION = "us-east-1";
    private static final String CLIENT_NAME = "nifi-copilot";
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();

    private volatile String ssoToken = "";
    private volatile long ssoTokenExpiry = 0L;
    private volatile String ssoStartUrl = "";
    private volatile String ssoRegion = DEFAULT_REGION;
    private volatile String bedrockRegion = DEFAULT_REGION;
    private volatile String accountId = "";
    private volatile String roleName = "";
    private volatile String deviceCode = "";
    private volatile String userCode = "";
    private volatile String verificationUri = "";
    private volatile long deviceExpiresAt = 0L;
    private volatile int pollInterval = 5;
    private volatile String clientId = "";
    private volatile String clientSecret = "";
    private volatile ScheduledFuture<?> pollingTask;
    private volatile List<Map<String, Object>> availableRoles = new ArrayList<>();
    private volatile String tempAccessKey = "";
    private volatile String tempSecretKey = "";
    private volatile String tempSessionToken = "";
    private volatile long tempCredsExpiry = 0L;

    public AwsAuthManager() {
        loadCreds();
    }

    public boolean isAuthenticated() {
        return !ssoToken.isBlank() && epochSec() < ssoTokenExpiry && !accountId.isBlank() && !roleName.isBlank();
    }

    private boolean deviceFlowActive() {
        return !userCode.isBlank() && ssoToken.isBlank() && epochSec() < deviceExpiresAt;
    }

    private boolean roleSelectionNeeded() {
        return !ssoToken.isBlank() && epochSec() < ssoTokenExpiry && !availableRoles.isEmpty() && (accountId.isBlank() || roleName.isBlank());
    }

    public Map<String, Object> authStatus() {
        final Map<String, Object> m = new HashMap<>();
        m.put("authenticated", isAuthenticated());
        m.put("device_flow_active", deviceFlowActive());
        m.put("user_code", deviceFlowActive() ? userCode : "");
        m.put("verification_uri", deviceFlowActive() ? verificationUri : "");
        m.put("role_selection_needed", roleSelectionNeeded());
        m.put("available_roles", roleSelectionNeeded() ? availableRoles : List.of());
        m.put("bedrock_region", isAuthenticated() ? bedrockRegion : "");
        m.put("account_id", isAuthenticated() ? accountId : "");
        m.put("role_name", isAuthenticated() ? roleName : "");
        return m;
    }

    private SsoOidcClient buildOidcClient() {
        return SsoOidcClient.builder()
                .region(Region.of(ssoRegion))
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }

    private SsoClient buildSsoClient() {
        return SsoClient.builder()
                .region(Region.of(ssoRegion))
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }

    public Map<String, Object> startDeviceFlow(final String startUrl, final String inSsoRegion, final String inBedrockRegion) {
        if (pollingTask != null && !pollingTask.isDone()) {
            pollingTask.cancel(true);
        }
        ssoStartUrl = startUrl.trim();
        ssoRegion = (inSsoRegion == null || inSsoRegion.isBlank()) ? DEFAULT_REGION : inSsoRegion.trim();
        bedrockRegion = (inBedrockRegion == null || inBedrockRegion.isBlank()) ? ssoRegion : inBedrockRegion.trim();
        ssoToken = "";
        accountId = "";
        roleName = "";
        availableRoles = new ArrayList<>();
        try (SsoOidcClient oidc = buildOidcClient()) {
            final var reg = oidc.registerClient(RegisterClientRequest.builder().clientName(CLIENT_NAME).clientType("public").build());
            final var auth = oidc.startDeviceAuthorization(StartDeviceAuthorizationRequest.builder()
                    .clientId(reg.clientId())
                    .clientSecret(reg.clientSecret())
                    .startUrl(ssoStartUrl)
                    .build());
            clientId = reg.clientId();
            clientSecret = reg.clientSecret();
            deviceCode = auth.deviceCode();
            userCode = auth.userCode();
            verificationUri = auth.verificationUriComplete() != null ? auth.verificationUriComplete() : auth.verificationUri();
            final int expires = auth.expiresIn() == null ? 900 : auth.expiresIn();
            deviceExpiresAt = epochSec() + expires;
            pollInterval = Math.max(auth.interval() == null ? 5 : auth.interval(), 5);
            pollingTask = poller.scheduleAtFixedRate(this::pollForToken, pollInterval, pollInterval, TimeUnit.SECONDS);
            return Map.of("user_code", userCode, "verification_uri", verificationUri, "expires_in", expires);
        } catch (Exception e) {
            throw new RuntimeException(e.toString(), e);
        }
    }

    private void pollForToken() {
        if (epochSec() >= deviceExpiresAt || deviceCode.isBlank()) {
            userCode = "";
            if (pollingTask != null) {
                pollingTask.cancel(true);
            }
            return;
        }
        try (SsoOidcClient oidc = buildOidcClient()) {
            final var token = oidc.createToken(CreateTokenRequest.builder()
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .grantType("urn:ietf:params:oauth:grant-type:device_code")
                    .deviceCode(deviceCode)
                    .build());
            if (token.accessToken() != null && !token.accessToken().isBlank()) {
                ssoToken = token.accessToken();
                ssoTokenExpiry = epochSec() + (token.expiresIn() == null ? 28800 : token.expiresIn());
                userCode = "";
                deviceCode = "";
                discoverRoles();
                if (pollingTask != null) {
                    pollingTask.cancel(true);
                }
            }
        } catch (software.amazon.awssdk.services.ssooidc.model.AuthorizationPendingException ignored) {
        } catch (software.amazon.awssdk.services.ssooidc.model.SlowDownException ignored) {
            pollInterval += 5;
        } catch (ExpiredTokenException e) {
            userCode = "";
            if (pollingTask != null) {
                pollingTask.cancel(true);
            }
        } catch (Exception e) {
            logger.warn("AWS polling exception: {}", e.getMessage());
        }
    }

    private void discoverRoles() {
        try (SsoClient sso = buildSsoClient()) {
            final List<Map<String, Object>> roles = new ArrayList<>();
            String accountToken = null;
            do {
                final var accountsResp = sso.listAccounts(ListAccountsRequest.builder().accessToken(ssoToken).nextToken(accountToken).build());
                accountToken = accountsResp.nextToken();
                for (var account : accountsResp.accountList()) {
                    String roleToken = null;
                    do {
                        final var roleResp = sso.listAccountRoles(ListAccountRolesRequest.builder()
                                .accessToken(ssoToken)
                                .accountId(account.accountId())
                                .nextToken(roleToken)
                                .build());
                        roleToken = roleResp.nextToken();
                        for (var role : roleResp.roleList()) {
                            roles.add(Map.of(
                                    "account_id", account.accountId(),
                                    "account_name", account.accountName() == null ? account.accountId() : account.accountName(),
                                    "role_name", role.roleName()));
                        }
                    } while (roleToken != null && !roleToken.isBlank());
                }
            } while (accountToken != null && !accountToken.isBlank());
            availableRoles = roles;
            if (roles.size() == 1) {
                final Map<String, Object> r = roles.getFirst();
                selectRole(String.valueOf(r.get("account_id")), String.valueOf(r.get("role_name")));
            }
        } catch (Exception e) {
            logger.error("Failed to list AWS roles: {}", e.getMessage());
        }
    }

    public void selectRole(final String inAccountId, final String inRoleName) {
        accountId = inAccountId;
        roleName = inRoleName;
        availableRoles = new ArrayList<>();
        refreshTempCredentials();
        saveCreds();
    }

    private void refreshTempCredentials() {
        try (SsoClient sso = buildSsoClient()) {
            final var creds = sso.getRoleCredentials(GetRoleCredentialsRequest.builder()
                    .accessToken(ssoToken)
                    .accountId(accountId)
                    .roleName(roleName)
                    .build()).roleCredentials();
            tempAccessKey = creds.accessKeyId();
            tempSecretKey = creds.secretAccessKey();
            tempSessionToken = creds.sessionToken();
            tempCredsExpiry = creds.expiration() / 1000L;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Map<String, String> getBedrockCredentials() {
        if (!isAuthenticated()) {
            throw new SecurityException("Not authenticated with AWS. Sign in first.");
        }
        if (epochSec() + 300 >= tempCredsExpiry) {
            refreshTempCredentials();
        }
        return Map.of(
                "aws_access_key_id", tempAccessKey,
                "aws_secret_access_key", tempSecretKey,
                "aws_session_token", tempSessionToken,
                "region_name", bedrockRegion
        );
    }

    public void logout() {
        ssoToken = "";
        ssoTokenExpiry = 0L;
        accountId = "";
        roleName = "";
        tempAccessKey = "";
        tempSecretKey = "";
        tempSessionToken = "";
        tempCredsExpiry = 0L;
        userCode = "";
        deviceCode = "";
        availableRoles = new ArrayList<>();
        if (pollingTask != null) {
            pollingTask.cancel(true);
        }
        deleteAwsCreds();
    }

    private void loadCreds() {
        try {
            if (!Files.exists(CREDENTIALS)) {
                return;
            }
            final Map<String, Object> data = mapper.readValue(Files.readString(CREDENTIALS, StandardCharsets.UTF_8), new TypeReference<>() {
            });
            ssoToken = String.valueOf(data.getOrDefault("aws_sso_token", ""));
            ssoTokenExpiry = longValue(data.get("aws_sso_token_expiry"), 0L);
            ssoStartUrl = String.valueOf(data.getOrDefault("aws_sso_start_url", ""));
            ssoRegion = String.valueOf(data.getOrDefault("aws_sso_region", DEFAULT_REGION));
            bedrockRegion = String.valueOf(data.getOrDefault("aws_bedrock_region", DEFAULT_REGION));
            accountId = String.valueOf(data.getOrDefault("aws_account_id", ""));
            roleName = String.valueOf(data.getOrDefault("aws_role_name", ""));
        } catch (Exception e) {
            logger.warn("Could not read credentials file: {}", e.getMessage());
        }
    }

    private long longValue(final Object value, final long fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (Exception e) {
            return fallback;
        }
    }

    private void saveCreds() {
        try {
            Files.createDirectories(CREDENTIALS.getParent());
            final Map<String, Object> data = Files.exists(CREDENTIALS)
                    ? mapper.readValue(Files.readString(CREDENTIALS, StandardCharsets.UTF_8), new TypeReference<>() {
                    })
                    : new HashMap<>();
            data.put("aws_sso_token", ssoToken);
            data.put("aws_sso_token_expiry", ssoTokenExpiry);
            data.put("aws_sso_start_url", ssoStartUrl);
            data.put("aws_sso_region", ssoRegion);
            data.put("aws_bedrock_region", bedrockRegion);
            data.put("aws_account_id", accountId);
            data.put("aws_role_name", roleName);
            Files.writeString(CREDENTIALS, mapper.writeValueAsString(data), StandardCharsets.UTF_8);
            try {
                Files.setPosixFilePermissions(CREDENTIALS, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
            } catch (UnsupportedOperationException ignored) {
            }
        } catch (Exception e) {
            logger.warn("Could not save credentials: {}", e.getMessage());
        }
    }

    private void deleteAwsCreds() {
        try {
            if (!Files.exists(CREDENTIALS)) {
                return;
            }
            final Map<String, Object> data = mapper.readValue(Files.readString(CREDENTIALS, StandardCharsets.UTF_8), new TypeReference<>() {
            });
            for (String key : List.of("aws_sso_token", "aws_sso_token_expiry", "aws_sso_start_url", "aws_sso_region",
                    "aws_bedrock_region", "aws_account_id", "aws_role_name")) {
                data.remove(key);
            }
            if (data.isEmpty()) {
                Files.deleteIfExists(CREDENTIALS);
            } else {
                Files.writeString(CREDENTIALS, mapper.writeValueAsString(data), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            logger.warn("Could not delete AWS credentials: {}", e.getMessage());
        }
    }

    private static long epochSec() {
        return System.currentTimeMillis() / 1000L;
    }
}
