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

package in.shrake.nifi.layout.core.util;

import in.shrake.nifi.layout.core.model.BoundingBox;

public final class Geometry {
    private Geometry() {}

    public static BoundingBox union(BoundingBox b1, BoundingBox b2) {
        if (b1 == null) return b2;
        if (b2 == null) return b1;

        int minX = Math.min(b1.x(), b2.x());
        int minY = Math.min(b1.y(), b2.y());
        int maxX = Math.max(b1.right(), b2.right());
        int maxY = Math.max(b1.bottom(), b2.bottom());

        return new BoundingBox(minX, minY, maxX - minX, maxY - minY);
    }
}
