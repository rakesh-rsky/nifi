package org.apache.nifi.copilot.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class NiFiConfigResolver {
    private static final Logger logger = LoggerFactory.getLogger(NiFiConfigResolver.class);
    private static final String CONFIG_RESOURCE = "nifi.properties";
    private final Properties properties = loadProperties();

    public String resolveBaseUrl() {
        final String explicit = System.getenv("NIFI_BASE_URL");
        if (explicit != null && !explicit.isBlank()) {
            return stripTrailingSlash(explicit);
        }
        return stripTrailingSlash(properties.getProperty("nifi.base.url", "https://localhost:8443").trim());
    }

    public String resolveUsername() {
        final String explicit = System.getenv("NIFI_USERNAME");
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return properties.getProperty("nifi.username", "");
    }

    public String resolvePassword() {
        final String explicit = System.getenv("NIFI_PASSWORD");
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return properties.getProperty("nifi.password", "");
    }

    public boolean verifySsl() {
        final String raw = System.getenv().getOrDefault("NIFI_VERIFY_SSL",
                properties.getProperty("nifi.verify.ssl", "false")).toLowerCase();
        return !(raw.equals("false") || raw.equals("0") || raw.equals("no"));
    }

    private String stripTrailingSlash(final String value) {
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private Properties loadProperties() {
        final Properties resolved = new Properties();
        try {
            final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try (InputStream inputStream = classLoader.getResourceAsStream(CONFIG_RESOURCE)) {
                if (inputStream != null) {
                    resolved.load(inputStream);
                } else {
                    logger.warn("Classpath resource {} not found; using defaults.", CONFIG_RESOURCE);
                }
            }
        } catch (IOException e) {
            logger.warn("Failed loading classpath resource {}; using defaults.", CONFIG_RESOURCE, e);
        }
        return resolved;
    }
}
