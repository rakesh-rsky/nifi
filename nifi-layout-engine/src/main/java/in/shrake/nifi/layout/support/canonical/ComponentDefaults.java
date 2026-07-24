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

import java.util.EnumMap;
import java.util.Map;

/** Shared canvas defaults used whenever an adapter source omits geometry. */
public final class ComponentDefaults {
    private static final Map<NodeType, Size> SIZES = new EnumMap<>(NodeType.class);

    static {
        SIZES.put(NodeType.PROCESSOR, new Size(350, 130));
        SIZES.put(NodeType.PORT_INPUT, new Size(240, 48));
        SIZES.put(NodeType.PORT_OUTPUT, new Size(240, 48));
        SIZES.put(NodeType.FUNNEL, new Size(48, 48));
        SIZES.put(NodeType.LABEL, new Size(64, 24));
        SIZES.put(NodeType.PROCESS_GROUP, new Size(384, 176));
        SIZES.put(NodeType.REMOTE_PROCESS_GROUP, new Size(384, 176));
        SIZES.put(NodeType.VIRTUAL, new Size(1, 1));
    }

    private ComponentDefaults() { }

    public static BoundingBox bounds(NodeType type) {
        return bounds(type, null, null, null, null);
    }

    public static BoundingBox bounds(NodeType type, Number x, Number y, Number width, Number height) {
        Size size = SIZES.get(type);
        return new BoundingBox(integer(x, 0), integer(y, 0), integer(width, size.width), integer(height, size.height));
    }

    private static int integer(Number value, int fallback) {
        return value == null ? fallback : (int) Math.round(value.doubleValue());
    }

    private record Size(int width, int height) { }
}
