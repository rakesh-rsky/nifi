package org.apache.nifi.copilot.capability;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.nifi.copilot.capability.CapabilityGraph.ControllerServiceNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.ProcessorNode;
import org.apache.nifi.copilot.capability.CapabilityGraph.PropertyNode;
import org.springframework.stereotype.Component;

@Component
public final class RepairHintDeriver {
    public List<RepairHint> derive(
            final ValidationReport report,
            final CapabilityGraph graph) {
        if (report == null || graph == null) {
            throw new IllegalArgumentException("Validation report and capability graph are required");
        }
        return report.issues().stream()
                .map(issue -> derive(issue, graph))
                .toList();
    }

    public RepairHint derive(
            final ValidationIssue issue,
            final CapabilityGraph graph) {
        if (issue == null || graph == null) {
            throw new IllegalArgumentException("Validation issue and capability graph are required");
        }

        final ServiceApi requiredApi = issue.requiredApi() == null
                ? apiLikeRejectedType(issue, graph).orElse(null)
                : issue.requiredApi();
        final List<String> implementations = requiredApi == null
                ? List.of()
                : graph.serviceImplementations(requiredApi).stream()
                        .map(ControllerServiceNode::type)
                        .toList();
        return new RepairHint(
                issue.issueType(),
                issue.componentId(),
                issue.path(),
                issue.rejectedValue(),
                requiredApi,
                implementations,
                suggestedProperties(issue, graph),
                supportedRelationships(issue, graph));
    }

    private Optional<ServiceApi> apiLikeRejectedType(
            final ValidationIssue issue,
            final CapabilityGraph graph) {
        if (issue.issueType() != ValidationIssueType.UNKNOWN_CONTROLLER_SERVICE_TYPE
                || issue.rejectedValue().isBlank()) {
            return Optional.empty();
        }
        final String rejected = issue.rejectedValue();
        final String rejectedSimpleName = simpleName(rejected);
        final List<String> matches = graph.serviceImplementationsByApi().keySet().stream()
                .filter(apiType -> apiType.equalsIgnoreCase(rejected)
                        || simpleName(apiType).equalsIgnoreCase(rejectedSimpleName))
                .sorted()
                .toList();
        return matches.size() == 1
                ? Optional.of(new ServiceApi(matches.getFirst(), null))
                : Optional.empty();
    }

    private List<String> suggestedProperties(
            final ValidationIssue issue,
            final CapabilityGraph graph) {
        if (!propertyIssue(issue.issueType()) || issue.capabilityType().isBlank()) {
            return List.of();
        }
        final Optional<PropertyNode> property = graph.resolveProcessor(issue.capabilityType())
                .flatMap(processor -> property(processor, issue.path()))
                .or(() -> graph.resolveControllerService(issue.capabilityType())
                        .flatMap(service -> property(service, issue.path())));
        if (property.isEmpty()) {
            return List.of();
        }
        final List<String> names = new ArrayList<>();
        names.add(property.get().name());
        if (!property.get().displayName().equals(property.get().name())) {
            names.add(property.get().displayName());
        }
        return names;
    }

    private Optional<PropertyNode> property(
            final ProcessorNode processor,
            final String path) {
        return property(processor.properties(), path);
    }

    private Optional<PropertyNode> property(
            final ControllerServiceNode service,
            final String path) {
        return property(service.properties(), path);
    }

    private Optional<PropertyNode> property(
            final java.util.Map<String, PropertyNode> properties,
            final String path) {
        final String propertyName = path.substring(path.lastIndexOf('.') + 1);
        final PropertyNode exact = properties.get(propertyName);
        if (exact != null) {
            return Optional.of(exact);
        }
        final List<PropertyNode> displayMatches = properties.values().stream()
                .filter(property -> property.displayName().equalsIgnoreCase(propertyName))
                .toList();
        return displayMatches.size() == 1
                ? Optional.of(displayMatches.getFirst())
                : Optional.empty();
    }

    private Set<String> supportedRelationships(
            final ValidationIssue issue,
            final CapabilityGraph graph) {
        if (issue.issueType() != ValidationIssueType.UNSUPPORTED_RELATIONSHIP
                || issue.capabilityType().isBlank()) {
            return Set.of();
        }
        return graph.resolveProcessor(issue.capabilityType())
                .map(ProcessorNode::relationships)
                .orElse(Set.of());
    }

    private boolean propertyIssue(final ValidationIssueType type) {
        return type == ValidationIssueType.MISSING_REQUIRED_PROPERTY
                || type == ValidationIssueType.INVALID_ALLOWABLE_VALUE
                || type == ValidationIssueType.UNRESOLVED_CONTROLLER_SERVICE_REFERENCE
                || type == ValidationIssueType.INCOMPATIBLE_CONTROLLER_SERVICE;
    }

    private String simpleName(final String type) {
        final String normalized = type.toLowerCase(Locale.ROOT);
        return normalized.substring(normalized.lastIndexOf('.') + 1);
    }
}
