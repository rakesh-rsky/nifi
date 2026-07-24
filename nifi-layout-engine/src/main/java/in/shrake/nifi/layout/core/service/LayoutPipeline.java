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

package in.shrake.nifi.layout.core.service;

import in.shrake.nifi.layout.core.exception.LayoutPipelineException;
import in.shrake.nifi.layout.core.exception.StrategyExecutionException;

import java.util.List;

public class LayoutPipeline {
    private final List<PipelineStage> stages;

    public LayoutPipeline(List<PipelineStage> stages) {
        this.stages = List.copyOf(stages);
    }

    public PipelineContext execute(PipelineContext context, LayoutProgressCallback callback) {
        return executeInternal(context, callback);
    }

    private PipelineContext executeInternal(PipelineContext context, LayoutProgressCallback callback) {
        PipelineContext currentContext = context;

        for (PipelineStage stage : stages) {
            String stageName = stage.getName();
            
            if (callback != null) {
                callback.onStageStarted(stageName);
            }
            
            long startTime = System.currentTimeMillis();
            
            try {
                currentContext = stage.execute(currentContext);
            } catch (StrategyExecutionException e) {
                throw new LayoutPipelineException(stageName, e.getMessage(), e.getCause());
            } catch (Exception e) {
                throw new LayoutPipelineException(stageName, e.getMessage(), e);
            }
            
            long durationMs = System.currentTimeMillis() - startTime;
            
            if (callback != null) {
                callback.onStageCompleted(stageName, currentContext, durationMs);
            }
        }

        return currentContext;
    }
}
