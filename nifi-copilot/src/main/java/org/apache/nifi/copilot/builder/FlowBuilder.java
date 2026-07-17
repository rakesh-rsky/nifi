package org.apache.nifi.copilot.builder;

import static org.apache.nifi.copilot.builder.LocalPreflightValidator.validateComponentSpecIds;
import static org.apache.nifi.copilot.builder.LocalPreflightValidator.validateParameterContext;
import static org.apache.nifi.copilot.builder.LocalPreflightValidator.validateSnippetOperations;
import static org.apache.nifi.copilot.builder.LocalPreflightValidator.validateSpecificationCollections;
import static org.apache.nifi.copilot.builder.SpecificationSupport.hasDeployableWork;
import static org.apache.nifi.copilot.builder.SpecificationSupport.listOfMap;
import static org.apache.nifi.copilot.builder.SpecificationSupport.mapOrNull;

import java.util.List;
import java.util.Map;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FlowBuilder {
    private static final Logger logger = LoggerFactory.getLogger(FlowBuilder.class);
    private static final CanvasProjector CANVAS_PROJECTOR = new CanvasProjector();
    private static final FlowDeploymentCoordinator COORDINATOR = new FlowDeploymentCoordinator();

    private final FlowDeploymentMetricsRegistry metrics;

    /** Direct construction uses an isolated metrics registry. */
    public FlowBuilder() {
        this.metrics = new FlowDeploymentMetricsRegistry();
    }

    /** Spring-managed construction: shared metrics registry is injected. */
    @Autowired
    public FlowBuilder(final FlowDeploymentMetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public record BuildResult(List<Map<String, Object>> createdProcessors, int connectionsCreated) {
    }

    public BuildResult buildFlow(
            final Map<String, Object> spec,
            final String processGroupId,
            final NiFiClientOperations nifi,
            final Map<String, String> existingIdMap,
            final int existingCount,
            final boolean autoStart,
            final boolean rollbackOnFailure) {
        final long startNanos = System.nanoTime();
        boolean deploymentSucceeded = false;
        try {
            final DeploymentContext context = new DeploymentContext(
                    spec, nifi, processGroupId, existingIdMap, existingCount, autoStart, rollbackOnFailure);
            final Map<String, Object> deploymentSpec = context.specification();
            validateSpecificationCollections(deploymentSpec);
            if (!hasDeployableWork(deploymentSpec)) {
                deploymentSucceeded = true;
                return new BuildResult(List.of(), 0);
            }
            validateComponentSpecIds(deploymentSpec);
            validateSnippetOperations(listOfMap(deploymentSpec.get("snippets")), context.rollbackOnFailure());
            final Map<String, Object> parameterContextSpec = mapOrNull(deploymentSpec.get("parameter_context"));
            if (parameterContextSpec != null) {
                validateParameterContext(parameterContextSpec);
            }
            final DeploymentState state = COORDINATOR.prepare(context, metrics);
            try {
                final DeploymentReport report = COORDINATOR.deploy(state);
                deploymentSucceeded = true;
                return new BuildResult(report.createdProcessors(), report.connectionsCreated());
            } catch (Exception e) {
                logger.error("buildFlow failed: {} — {}", e.getClass().getSimpleName(), e.getMessage(), e);
                if (context.rollbackOnFailure() && state.parentSnapshot() != null) {
                    RollbackManager.rollback(state.ledger(), context.nifi(), e, metrics);
                }
                throw e;
            }
        } finally {
            metrics.observeDeployment(
                    deploymentSucceeded
                            ? FlowDeploymentMetricsRegistry.DeploymentOutcome.SUCCESS
                            : FlowDeploymentMetricsRegistry.DeploymentOutcome.FAILURE,
                    startNanos);
        }
    }

    public Map<String, Object> readCanvas(final NiFiClientOperations nifi, final String processGroupId) {
        return CANVAS_PROJECTOR.readCanvas(nifi, processGroupId);
    }
}
