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

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public record CanonicalConnection(String id, String parentGroupId, String sourceId,
                                  String targetId, Collection<String> relationships,
                                  String targetPort) {
    public CanonicalConnection {
        Objects.requireNonNull(id, "connection id must not be null");
        Objects.requireNonNull(sourceId, "connection source id must not be null");
        Objects.requireNonNull(targetId, "connection target id must not be null");
        relationships = relationships == null ? List.of() : List.copyOf(relationships);
        targetPort = targetPort == null ? "" : targetPort;
    }
}
