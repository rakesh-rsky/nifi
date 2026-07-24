/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package in.shrake.nifi.layout.core.model;

import java.util.Objects;

public final class LayoutEdge {
    private final String id;
    private final String sourceNodeId;
    private final String targetNodeId;
    private final String sourcePort;
    private final String targetPort;
    private final boolean reversed;
    private final boolean selfLoop;

    public LayoutEdge(String id, String sourceNodeId, String targetNodeId, String sourcePort, String targetPort, boolean reversed, boolean selfLoop) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.sourceNodeId = Objects.requireNonNull(sourceNodeId, "sourceNodeId must not be null");
        this.targetNodeId = Objects.requireNonNull(targetNodeId, "targetNodeId must not be null");
        this.sourcePort = sourcePort;
        this.targetPort = targetPort;
        this.reversed = reversed;
        this.selfLoop = selfLoop;
    }

    public String getId() { return id; }
    public String getSourceNodeId() { return sourceNodeId; }
    public String getTargetNodeId() { return targetNodeId; }
    public String getSourcePort() { return sourcePort; }
    public String getTargetPort() { return targetPort; }
    public boolean isReversed() { return reversed; }
    public boolean isSelfLoop() { return selfLoop; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LayoutEdge that = (LayoutEdge) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
