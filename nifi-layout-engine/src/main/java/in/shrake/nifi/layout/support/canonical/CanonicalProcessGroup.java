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

package in.shrake.nifi.layout.support.canonical;

import in.shrake.nifi.layout.core.model.BoundingBox;
import in.shrake.nifi.layout.core.model.NodeType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record CanonicalProcessGroup(String id, String parentGroupId, BoundingBox bounds,
                                    Map<String, String> attributes,
                                    List<CanonicalComponent> components,
                                    List<CanonicalConnection> connections,
                                    List<CanonicalProcessGroup> childGroups) {
    public CanonicalProcessGroup {
        Objects.requireNonNull(id, "process group id must not be null");
        bounds = bounds == null ? ComponentDefaults.bounds(NodeType.PROCESS_GROUP) : bounds;
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        components = components == null ? List.of() : List.copyOf(components);
        connections = connections == null ? List.of() : List.copyOf(connections);
        childGroups = childGroups == null ? List.of() : List.copyOf(childGroups);
    }
}
