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

public class WriteBackException extends LayoutEngineException {
    private final String componentId;
    private final String componentType;

    public WriteBackException(String componentId, String message) {
        this(componentId, null, message, null);
    }

    public WriteBackException(String componentId, String message, Throwable cause) {
        this(componentId, null, message, cause);
    }

    public WriteBackException(String componentId, String componentType, String message) {
        this(componentId, componentType, message, null);
    }

    public WriteBackException(String componentId, String componentType, String message, Throwable cause) {
        super(format(componentId, componentType, message), cause);
        this.componentId = componentId;
        this.componentType = componentType;
    }

    private static String format(String id, String type, String message) {
        String description = type == null ? "component" : type + " component";
        return String.format("Write-back failed for %s '%s': %s", description, id, message);
    }

    public String getComponentId() { return componentId; }
    public String getComponentType() { return componentType; }
}
