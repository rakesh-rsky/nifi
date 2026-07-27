package org.apache.nifi.copilot;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Active when the external HTTP NiFi client is selected — mirroring the same
 * logic used by NiFiClientSelectionConfiguration:
 *
 *  - mode=external  → always active
 *  - mode=auto      → active only when internalNiFiClient bean is NOT defined
 *  - mode=internal  → never active
 */
public class ExternalNiFiClientCondition implements Condition {

    @Override
    public boolean matches(final ConditionContext context, final AnnotatedTypeMetadata metadata) {
        final String mode = context.getEnvironment()
                .getProperty("nifi.copilot.client.mode", "auto")
                .trim().toLowerCase();

        return switch (mode) {
            case "external" -> true;
            case "internal" -> false;
            default -> !context.getBeanFactory().containsBeanDefinition("internalNiFiClient");
        };
    }
}
