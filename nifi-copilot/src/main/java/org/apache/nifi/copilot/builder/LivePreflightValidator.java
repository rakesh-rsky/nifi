package org.apache.nifi.copilot.builder;

import java.util.Objects;

/**
 * Intentionally narrow: live identity checks remain in {@link ComponentResolver} at their
 * established call sites so validation does not front-load NiFi requests or failures.
 */
final class LivePreflightValidator {
    void validatePreparedTarget(final DeploymentTarget target) {
        Objects.requireNonNull(target, "deployment target");
    }
}
