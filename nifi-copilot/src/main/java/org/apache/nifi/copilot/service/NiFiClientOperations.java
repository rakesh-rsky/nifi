package org.apache.nifi.copilot.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface NiFiClientOperations {
    String getProcessGroupId(String pgId);

    Map<String, Object> createProcessGroup(String parentPgId, String name, double x, double y);

    Map<String, Object> getProcessGroupFlow(String pgId);

    void ensureTypeCache();

    Map<String, Object> createProcessor(String pgId, String processorType, String name, Double x, Double y, Map<String, Object> properties);

    void startProcessor(String procId);

    void deleteProcessor(String procId);

    boolean waitForProcessorValid(String procId, int timeoutSec);

    void autoTerminateUnusedRelationships(String procId, Set<String> usedRelationships);

    Map<String, Object> createConnection(String pgId, String sourceId, String sourceType, String destId, String destType, List<String> relationships);

    void deleteConnection(String connId);

    List<Map<String, Object>> listControllerServices(String pgId);

    Map<String, Object> createControllerService(String pgId, String serviceType, String name, Map<String, Object> properties);

    void enableControllerService(String csId);

    void disableControllerService(String csId);

    void deleteControllerService(String csId);

    Map<String, Object> createParameterContext(String name, Map<String, String> parameters, String description);

    void bindParameterContextToProcessGroup(String pgId, String pcId);

    void deleteParameterContext(String pcId);

    Map<String, Object> snapshotProcessGroup(String pgId);

    void restoreFromSnapshot(Map<String, Object> snapshot);

    List<double[]> getOccupiedPositions(String pgId);

    static int intValue(final Object v) {
        if (v == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(v));
        } catch (Exception e) {
            return 0;
        }
    }

    static double doubleValue(final Object v) {
        if (v == null) {
            return 0D;
        }
        try {
            return Double.parseDouble(String.valueOf(v));
        } catch (Exception e) {
            return 0D;
        }
    }
}
