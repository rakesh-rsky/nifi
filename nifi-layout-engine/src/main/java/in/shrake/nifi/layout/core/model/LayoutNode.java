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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class LayoutNode {
    private final String id;
    private final NodeType type;
    private final BoundingBox boundingBox;
    private final Map<String, String> attributes;
    private final String parentGroupId;

    public LayoutNode(String id, NodeType type, BoundingBox boundingBox, Map<String, String> attributes, String parentGroupId) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.boundingBox = Objects.requireNonNull(boundingBox, "boundingBox must not be null");
        this.attributes = attributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        this.parentGroupId = parentGroupId;
    }

    public String getId() { return id; }
    public NodeType getType() { return type; }
    public BoundingBox getBoundingBox() { return boundingBox; }
    public Map<String, String> getAttributes() { return attributes; }
    public String getParentGroupId() { return parentGroupId; }

    public LayoutNode withBoundingBox(BoundingBox newBox) {
        return new LayoutNode(id, type, newBox, attributes, parentGroupId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LayoutNode that = (LayoutNode) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
