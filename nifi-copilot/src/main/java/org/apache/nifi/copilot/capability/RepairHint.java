package org.apache.nifi.copilot.capability;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public record RepairHint(
        ValidationIssueType issueType,
        String affectedComponentId,
        String affectedPath,
        String rejectedTypeOrReference,
        ServiceApi requiredApi,
        List<String> compatibleImplementationTypes,
        List<String> suggestedPropertyNames,
        Set<String> supportedRelationships) {
    public RepairHint {
        issueType = issueType == null ? ValidationIssueType.OTHER : issueType;
        affectedComponentId = affectedComponentId == null ? "" : affectedComponentId;
        affectedPath = affectedPath == null ? "" : affectedPath;
        rejectedTypeOrReference =
                rejectedTypeOrReference == null ? "" : rejectedTypeOrReference;
        compatibleImplementationTypes = compatibleImplementationTypes == null
                ? List.of()
                : compatibleImplementationTypes.stream().sorted().distinct().toList();
        suggestedPropertyNames = suggestedPropertyNames == null
                ? List.of()
                : suggestedPropertyNames.stream().sorted().distinct().toList();
        supportedRelationships = supportedRelationships == null
                ? Set.of()
                : Collections.unmodifiableSet(new TreeSet<>(supportedRelationships));
    }
}
