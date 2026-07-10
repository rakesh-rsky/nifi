package org.apache.nifi.copilot.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class NiFiClientSelectionConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(NiFiClientSelectionConfiguration.class);

    @Primary
    @Bean
    public NiFiClientOperations nifiClientOperations(
            @Qualifier("httpNiFiClient") final NiFiClientOperations httpNiFiClient,
            final ApplicationContext applicationContext,
            @Value("${nifi.copilot.client.mode:${NIFI_COPILOT_CLIENT_MODE:auto}}") final String configuredMode
    ) {
        final String mode = configuredMode == null ? "auto" : configuredMode.trim().toLowerCase();
        final NiFiClientOperations internalNiFiClient = resolveInternalClient(applicationContext);

        if ("external".equals(mode)) {
            return httpNiFiClient;
        }

        if ("internal".equals(mode)) {
            if (internalNiFiClient == null) {
                throw new BeanCreationException("nifi.copilot.client.mode=internal requires internalNiFiClient.");
            }
            return internalNiFiClient;
        }

        if (internalNiFiClient != null) {
            return internalNiFiClient;
        }

        logger.warn("Internal NiFi client unavailable; using external HTTP client.");
        return httpNiFiClient;
    }

    private NiFiClientOperations resolveInternalClient(final ApplicationContext applicationContext) {
        if (!applicationContext.containsBean("internalNiFiClient")) {
            return null;
        }
        return applicationContext.getBean("internalNiFiClient", NiFiClientOperations.class);
    }
}
