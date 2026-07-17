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

import static org.apache.nifi.copilot.api.Dto.AuthStatusResponse;
import static org.apache.nifi.copilot.api.Dto.AwsAuthStatusResponse;
import static org.apache.nifi.copilot.api.Dto.AwsSelectRoleRequest;
import static org.apache.nifi.copilot.api.Dto.AwsStartRequest;
import static org.apache.nifi.copilot.api.Dto.ChatRequest;
import static org.apache.nifi.copilot.api.Dto.ChatResponse;
import static org.apache.nifi.copilot.api.Dto.DeviceFlowResponse;
import static org.apache.nifi.copilot.api.Dto.SessionData;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.nifi.copilot.api.CopilotController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.server.ResponseStatusException;

@Controller
@Path("/copilot")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CopilotResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(CopilotResource.class);
    private final CopilotController copilotController;

    public CopilotResource(final CopilotController copilotController) {
        this.copilotController = copilotController;
    }

    @GET
    @Path("/api/session/{processGroupId}")
    public Map<String, Object> getSession(@PathParam("processGroupId") final String processGroupId) {
        return execute(() -> copilotController.getSession(processGroupId));
    }

    @PUT
    @Path("/api/session/{processGroupId}")
    public Map<String, Object> putSession(
            @PathParam("processGroupId") final String processGroupId,
            final SessionData data
    ) {
        return execute(() -> copilotController.putSession(processGroupId, data));
    }

    @DELETE
    @Path("/api/session/{processGroupId}")
    public Map<String, Object> deleteSession(@PathParam("processGroupId") final String processGroupId) {
        return execute(() -> copilotController.deleteSession(processGroupId));
    }

    @GET
    @Path("/api/auth/status")
    public AuthStatusResponse githubAuthStatus() {
        return execute(copilotController::githubAuthStatus);
    }

    @POST
    @Path("/api/auth/start")
    public DeviceFlowResponse githubAuthStart() {
        return execute(copilotController::githubAuthStart);
    }

    @POST
    @Path("/api/auth/logout")
    public Map<String, Object> githubAuthLogout() {
        return execute(copilotController::githubAuthLogout);
    }

    @GET
    @Path("/api/auth/aws/status")
    public AwsAuthStatusResponse awsAuthStatus() {
        return execute(copilotController::awsAuthStatus);
    }

    @POST
    @Path("/api/auth/aws/start")
    public DeviceFlowResponse awsAuthStart(final AwsStartRequest request) {
        return execute(() -> copilotController.awsAuthStart(request));
    }

    @POST
    @Path("/api/auth/aws/select-role")
    public Map<String, Object> awsSelectRole(final AwsSelectRoleRequest request) {
        return execute(() -> copilotController.awsSelectRole(request));
    }

    @POST
    @Path("/api/auth/aws/logout")
    public Map<String, Object> awsLogout() {
        return execute(copilotController::awsLogout);
    }

    @GET
    @Path("/api/canvas")
    public Map<String, Object> canvas(@QueryParam("process_group_id") final String processGroupId) {
        final String effectiveProcessGroupId = processGroupId == null || processGroupId.isBlank() ? "root" : processGroupId;
        return execute(() -> copilotController.canvas(effectiveProcessGroupId));
    }

    @GET
    @Path("/api/health")
    public Map<String, Object> health() {
        return execute(copilotController::health);
    }

    @POST
    @Path("/api/chat")
    public ChatResponse chat(final ChatRequest request) {
        return execute(() -> copilotController.chat(request));
    }

    private static <T> T execute(final Supplier<T> call) {
        try {
            return call.get();
        } catch (ResponseStatusException e) {
            throw new WebApplicationException(e.getReason(), e.getStatusCode().value());
        } catch (SecurityException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.UNAUTHORIZED.getStatusCode());
        } catch (RuntimeException e) {
            logger.error("Copilot request failed: {}", e.getMessage(), e);
            throw new WebApplicationException(e.getMessage(), Response.Status.BAD_GATEWAY.getStatusCode());
        }
    }
}
