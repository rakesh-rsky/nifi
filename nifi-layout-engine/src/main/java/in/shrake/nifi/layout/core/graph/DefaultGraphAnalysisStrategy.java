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

package in.shrake.nifi.layout.core.graph;

import in.shrake.nifi.layout.core.model.FlowDirection;
import in.shrake.nifi.layout.core.model.LayoutGraph;
import in.shrake.nifi.layout.core.model.LayoutNode;

import java.util.List;

/** Default composition-root adapter for the generic graph analysis algorithms. */
public final class DefaultGraphAnalysisStrategy implements GraphAnalysisStrategy {
    @Override
    public List<LayoutNode> detectRoots(LayoutGraph graph) {
        return GraphAnalyzer.detectRoots(graph);
    }

    @Override
    public List<LayoutNode> detectTerminals(LayoutGraph graph) {
        return GraphAnalyzer.detectTerminals(graph);
    }

    @Override
    public FlowDirection detectFlowDirection(LayoutGraph graph) {
        return GraphAnalyzer.detectFlowDirection(graph);
    }

    @Override
    public List<LayoutGraph> findConnectedComponents(LayoutGraph graph) {
        return GraphAnalyzer.findConnectedComponents(graph);
    }
}
