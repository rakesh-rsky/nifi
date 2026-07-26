/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package in.shrake.nifi.layout.core.spacing;

public record RankDemand(
        int boundaryIndex,
        int maxOutgoingDegree,
        int maxIncomingDegree,
        int maxParallelEdges,
        int denseBusCount,
        int longEdgeCount,
        boolean labelPresent,
        boolean portPresent) {

    public RankDemand {
        if (boundaryIndex < 0
                || maxOutgoingDegree < 0
                || maxIncomingDegree < 0
                || maxParallelEdges < 0
                || denseBusCount < 0
                || longEdgeCount < 0) {
            throw new IllegalArgumentException("Rank demand values must not be negative");
        }
    }
}
