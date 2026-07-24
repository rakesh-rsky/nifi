/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.nifi.copilot.builder;

import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RollbackManagerParameterContextTest {

    @Test
    void deletesOwnedChildBeforeItsOwnedParameterContext() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        final OwnershipLedger ledger = new OwnershipLedger("parent");
        ledger.setChildProcessGroupId("child");
        ledger.registerParameterContext("pc-new", true, "child", null);

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failed"),
                new FlowDeploymentMetricsRegistry());

        final InOrder order = inOrder(nifi);
        order.verify(nifi).deleteProcessGroup("child");
        order.verify(nifi).deleteParameterContext("pc-new");
        verify(nifi, never()).unbindParameterContextFromProcessGroup("child");
    }

    @Test
    void restoresReusedTargetPreviousBindingBeforeDeletingOwnedContext() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        final OwnershipLedger ledger = new OwnershipLedger("target");
        ledger.registerParameterContext("pc-new", true, "target", "pc-old");

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failed"),
                new FlowDeploymentMetricsRegistry());

        final InOrder order = inOrder(nifi);
        order.verify(nifi).bindParameterContextToProcessGroup("target", "pc-old");
        order.verify(nifi).deleteParameterContext("pc-new");
    }

    @Test
    void unbindsPreviouslyUnboundReusedTargetBeforeDeletingOwnedContext() {
        final NiFiClientOperations nifi = mock(NiFiClientOperations.class);
        final OwnershipLedger ledger = new OwnershipLedger("target");
        ledger.registerParameterContext("pc-new", true, "target", null);

        RollbackManager.rollback(ledger, nifi, new RuntimeException("failed"),
                new FlowDeploymentMetricsRegistry());

        final InOrder order = inOrder(nifi);
        order.verify(nifi).unbindParameterContextFromProcessGroup("target");
        order.verify(nifi).deleteParameterContext("pc-new");
    }
}
