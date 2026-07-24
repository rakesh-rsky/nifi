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

package in.shrake.nifi.layout.core.exception;

public class StrategyExecutionException extends LayoutPipelineException {
    private final String interfaceName;

    /** Full constructor used by LayoutEngine.Builder when wrapping a custom strategy failure. */
    public StrategyExecutionException(String interfaceName, String stageName, Throwable cause) {
        super(stageName, String.format("Strategy execution failed for interface %s", interfaceName), cause);
        this.interfaceName = interfaceName;
    }

    /**
     * Convenience constructor for use inside pipeline stages when the interface name is not
     * available at the throw site.  The pipeline will re-wrap this with the correct stage name.
     */
    public StrategyExecutionException(String message, Throwable cause) {
        super("[strategy]", message, cause);
        this.interfaceName = null;
    }

    public String getInterfaceName() { return interfaceName; }
}
