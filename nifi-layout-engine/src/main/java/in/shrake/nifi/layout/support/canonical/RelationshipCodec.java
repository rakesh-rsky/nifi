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

/**
 * Reversible encoding for relationship sets. Backslashes and vertical bars are escaped,
 * escaped values are sorted lexicographically, and the result is joined with an unescaped '|'.
 */
public final class RelationshipCodec {
    private RelationshipCodec() { }

    public static String encode(Collection<String> relationships) {
        if (relationships == null) return "";
        return relationships.stream().filter(java.util.Objects::nonNull)
                .map(RelationshipCodec::escape).sorted().reduce((a, b) -> a + "|" + b).orElse("");
    }

    public static List<String> decode(String encoded) {
        if (encoded == null || encoded.isEmpty()) return List.of();
        java.util.ArrayList<String> values = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (char c : encoded.toCharArray()) {
            if (escaped) { current.append(c); escaped = false; }
            else if (c == '\\') escaped = true;
            else if (c == '|') { values.add(current.toString()); current.setLength(0); }
            else current.append(c);
        }
        if (escaped) current.append('\\');
        values.add(current.toString());
        return List.copyOf(values);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("|", "\\|");
    }
}
