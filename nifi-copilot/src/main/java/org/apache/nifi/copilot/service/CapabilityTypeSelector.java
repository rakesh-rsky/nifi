package org.apache.nifi.copilot.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CapabilityTypeSelector {
    private CapabilityTypeSelector() {
    }

    public static List<Map<String, Object>> preferredTypes(
            final List<Map<String, Object>> discoveredTypes) {
        final List<Map<String, Object>> sorted = new ArrayList<>(discoveredTypes);
        sorted.sort(Comparator
                .comparing(CapabilityTypeSelector::type)
                .thenComparing(CapabilityTypeSelector::version,
                        CapabilityTypeSelector::compareVersions)
                .thenComparing(CapabilityTypeSelector::group)
                .thenComparing(CapabilityTypeSelector::artifact));
        final Map<String, Map<String, Object>> preferred = new LinkedHashMap<>();
        sorted.forEach(entry -> preferred.putIfAbsent(type(entry), entry));
        return List.copyOf(preferred.values());
    }

    private static int compareVersions(
            final String first,
            final String second) {
        final Version firstVersion = Version.parse(first);
        final Version secondVersion = Version.parse(second);
        final String[] firstParts = firstVersion.base().split("\\.");
        final String[] secondParts = secondVersion.base().split("\\.");
        final int count = Math.max(firstParts.length, secondParts.length);
        for (int index = 0; index < count; index++) {
            final String firstPart = index < firstParts.length ? firstParts[index] : "0";
            final String secondPart = index < secondParts.length ? secondParts[index] : "0";
            final int comparison = compareVersionPart(secondPart, firstPart);
            if (comparison != 0) {
                return comparison;
            }
        }
        final int qualifierRank = Integer.compare(
                secondVersion.qualifierRank(), firstVersion.qualifierRank());
        if (qualifierRank != 0) {
            return qualifierRank;
        }
        return secondVersion.qualifier().compareToIgnoreCase(firstVersion.qualifier());
    }

    private static int compareVersionPart(
            final String first,
            final String second) {
        try {
            return Integer.compare(Integer.parseInt(first), Integer.parseInt(second));
        } catch (NumberFormatException e) {
            return first.compareToIgnoreCase(second);
        }
    }

    private static String type(final Map<String, Object> entry) {
        return String.valueOf(entry.get("type"));
    }

    private static String group(final Map<String, Object> entry) {
        return bundleValue(entry, "group");
    }

    private static String artifact(final Map<String, Object> entry) {
        return bundleValue(entry, "artifact");
    }

    private static String version(final Map<String, Object> entry) {
        return bundleValue(entry, "version");
    }

    private static String bundleValue(
            final Map<String, Object> entry,
            final String field) {
        final Object bundle = entry.get("bundle");
        return bundle instanceof Map<?, ?> map
                ? String.valueOf(map.get(field)) : "";
    }

    private record Version(String base, String qualifier) {
        static Version parse(final String value) {
            final int separator = value.indexOf('-');
            return separator < 0
                    ? new Version(value, "")
                    : new Version(value.substring(0, separator),
                            value.substring(separator + 1));
        }

        int qualifierRank() {
            if (qualifier.isBlank()) {
                return 100;
            }
            final String normalized = qualifier.toLowerCase();
            if (normalized.startsWith("sp")) {
                return 90;
            }
            if (normalized.startsWith("rc") || normalized.startsWith("cr")) {
                return 80;
            }
            if (normalized.startsWith("m")) {
                return 70;
            }
            if (normalized.startsWith("beta") || normalized.startsWith("b")) {
                return 60;
            }
            if (normalized.startsWith("alpha") || normalized.startsWith("a")) {
                return 50;
            }
            if (normalized.startsWith("snapshot")) {
                return 10;
            }
            return 40;
        }
    }
}
