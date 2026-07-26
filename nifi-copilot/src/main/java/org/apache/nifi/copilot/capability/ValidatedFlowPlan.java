package org.apache.nifi.copilot.capability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record ValidatedFlowPlan(Map<String, Object> specification) {
    public ValidatedFlowPlan {
        specification = immutableMap(specification);
    }

    private static Map<String, Object> immutableMap(final Map<String, ?> source) {
        final Map<String, Object> copy = new LinkedHashMap<>();
        if (source != null) {
            source.forEach((key, value) -> copy.put(key, immutableCopy(value)));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Object immutableCopy(final Object value) {
        if (value instanceof Map<?, ?> map) {
            final Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, nested) -> copy.put(String.valueOf(key), immutableCopy(nested)));
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) {
            final List<Object> copy = new ArrayList<>(list.size());
            list.forEach(element -> copy.add(immutableCopy(element)));
            return Collections.unmodifiableList(copy);
        }
        return value;
    }
}
