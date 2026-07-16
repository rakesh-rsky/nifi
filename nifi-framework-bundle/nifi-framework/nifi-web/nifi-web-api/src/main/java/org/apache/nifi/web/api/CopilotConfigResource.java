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
package org.apache.nifi.web.api;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Controller;

/**
 * Always-present endpoint that exposes the copilot backend URL from nifi.properties
 * so the frontend can locate either the embedded or external copilot service.
 *
 * Property: {@code nifi.copilot.url}
 *   - Leave blank (default) when copilot is embedded in NiFi (internal mode).
 *     The frontend will use the relative path {@code /nifi-api/copilot}.
 *   - Set to a full URL (e.g. {@code http://copilot-host:8080}) when running
 *     the standalone nifi-copilot service (external mode).
 */
@Controller
@Path("/copilot")
@Produces(MediaType.APPLICATION_JSON)
public class CopilotConfigResource extends ApplicationResource {

    @GET
    @Path("/api/config")
    public Map<String, Object> getConfig() {
        final Map<String, Object> config = new HashMap<>();
        final String url = getProperties().getCopilotUrl();
        config.put("externalUrl", (url != null && !url.isBlank()) ? url.stripTrailing() : null);
        return config;
    }
}
