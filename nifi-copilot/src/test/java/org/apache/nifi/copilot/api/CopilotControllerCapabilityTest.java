package org.apache.nifi.copilot.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.copilot.api.Dto.ChatRequest;
import org.apache.nifi.copilot.api.Dto.ChatResponse;
import org.apache.nifi.copilot.auth.AwsAuthManager;
import org.apache.nifi.copilot.auth.GitHubAuthManager;
import org.apache.nifi.copilot.builder.FlowBuilder;
import org.apache.nifi.copilot.capability.CapabilityPromptRenderer;
import org.apache.nifi.copilot.capability.CapabilityGraph;
import org.apache.nifi.copilot.capability.CapabilityMetricsRegistry;
import org.apache.nifi.copilot.capability.CapabilityRegistry;
import org.apache.nifi.copilot.capability.CapabilityRegistryManager;
import org.apache.nifi.copilot.capability.CapabilitySnapshot;
import org.apache.nifi.copilot.capability.FlowSpecificationValidationException;
import org.apache.nifi.copilot.capability.RepairContextExpander;
import org.apache.nifi.copilot.capability.RepairHintDeriver;
import org.apache.nifi.copilot.capability.ValidatedFlowPlan;
import org.apache.nifi.copilot.capability.ValidationIssue;
import org.apache.nifi.copilot.capability.ValidationReport;
import org.apache.nifi.copilot.llm.LlmClient;
import org.apache.nifi.copilot.service.CapabilityDiscoveryException;
import org.apache.nifi.copilot.service.NiFiClientOperations;
import org.apache.nifi.copilot.store.SessionStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.web.server.ResponseStatusException;

class CopilotControllerCapabilityTest {
    private GitHubAuthManager github;
    private AwsAuthManager aws;
    private NiFiClientOperations nifi;
    private LlmClient llm;
    private FlowBuilder flowBuilder;
    private CapabilityRegistryManager manager;
    private CapabilityPromptRenderer renderer;
    private RepairHintDeriver hintDeriver;
    private RepairContextExpander contextExpander;
    private CapabilityMetricsRegistry metrics;
    private CapabilityGraph graph;
    private CopilotController controller;

    @BeforeEach
    void setUp() {
        github = mock(GitHubAuthManager.class);
        aws = mock(AwsAuthManager.class);
        nifi = mock(NiFiClientOperations.class);
        llm = mock(LlmClient.class);
        flowBuilder = mock(FlowBuilder.class);
        manager = mock(CapabilityRegistryManager.class);
        renderer = mock(CapabilityPromptRenderer.class);
        hintDeriver = mock(RepairHintDeriver.class);
        contextExpander = mock(RepairContextExpander.class);
        metrics = mock(CapabilityMetricsRegistry.class);
        graph = new CapabilityGraph(List.of(), List.of());
        when(manager.capabilitySet(nifi)).thenReturn(new CapabilityRegistry.CapabilitySet(
                new CapabilitySnapshot(Map.of(), Map.of(), Instant.now()),
                graph));
        when(renderer.renderFromGraph(anyString(), eq(graph))).thenReturn("target capabilities");
        when(hintDeriver.derive(any(ValidationReport.class), eq(graph))).thenReturn(List.of());
        when(contextExpander.expand(anyString(), eq(graph), anyList(), anyMap()))
                .thenReturn("expanded capabilities");
        controller = new CopilotController(
                github,
                aws,
                nifi,
                llm,
                flowBuilder,
                mock(SessionStore.class),
                manager,
                renderer,
                hintDeriver,
                contextExpander,
                metrics);
    }

    @Test
    void invalidMixedResponseSuppressesDeletionsActionsAndDeployment() {
        when(github.isAuthenticated()).thenReturn(true);
        when(github.getGitHubToken()).thenReturn("token");
        final Map<String, Object> generated = Map.of(
                "controller_services", List.of(Map.of(
                        "id", "bad", "type", "not.Installed", "name", "Bad")),
                "deletions", List.of(Map.of("type", "processor", "spec_id", "old")),
                "cs_actions", List.of(Map.of("name", "Existing", "action", "disable")),
                "processors", List.of(),
                "connections", List.of());
        when(llm.generateFlowSpec(
                anyString(), anyList(), anyString(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("target capabilities")))
                .thenReturn(generated);
        when(llm.generateFlowSpec(
                anyString(), anyList(), anyString(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("expanded capabilities")))
                .thenReturn(generated);
        final ValidationIssue issue = new ValidationIssue(
                "bad", "type", "Unknown controller service", "Use a discovered type");
        when(flowBuilder.prepareFlow(generated, nifi, true))
                .thenThrow(new FlowSpecificationValidationException(new ValidationReport(List.of(issue))));
        final ChatRequest request = request("github");
        request.existing_processors = List.of(
                new Dto.ExistingProcessor("old", "nifi-old", "Old", "GenerateFlowFile"));

        final ChatResponse response = controller.chat(request);

        assertEquals(List.of(issue), response.validation_issues);
        assertTrue(response.reply.contains("before any NiFi changes"));
        verify(nifi, never()).deleteProcessor(anyString());
        verify(nifi, never()).deleteProcessGroup(anyString());
        verify(nifi, never()).enableControllerService(anyString());
        verify(nifi, never()).disableControllerService(anyString());
        verify(nifi, never()).scheduleProcessGroup(anyString(), anyString());
        verify(flowBuilder, never()).buildFlow(
                any(ValidatedFlowPlan.class), anyString(), any(), anyMap(),
                any(Integer.class), any(Boolean.class), any(Boolean.class));
        verify(metrics).observeFirstPass(false, List.of(issue));
        verify(metrics).observeRepairAttempt();
        verify(metrics).observeRepairTokens(0, 0);
        verify(metrics).observeRepairResult(false, List.of(issue));
    }

    @Test
    void bedrockPathReceivesCapabilityContextAndDeploysPreparedPlan() {
        when(aws.isAuthenticated()).thenReturn(true);
        when(aws.getBedrockCredentials()).thenReturn(Map.of("region_name", "us-east-1"));
        final Map<String, Object> generated = Map.of(
                "processors", List.of(), "connections", List.of(), "explanation", "ok");
        when(llm.generateFlowSpecBedrock(
                anyString(), anyList(), anyMap(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("target capabilities")))
                .thenReturn(generated);
        final ValidatedFlowPlan plan = new ValidatedFlowPlan(generated);
        when(flowBuilder.prepareFlow(generated, nifi, true)).thenReturn(plan);
        when(flowBuilder.buildFlow(plan, "root", nifi, Map.of(), 0, false, true))
                .thenReturn(new FlowBuilder.BuildResult(List.of(), 0));

        final ChatResponse response = controller.chat(request("aws"));

        assertEquals("ok", response.reply);
        verify(flowBuilder).buildFlow(plan, "root", nifi, Map.of(), 0, false, true);
        verify(metrics).observeFirstPass(true, List.of());
    }

    @Test
    void appliesControllerServiceActionsAfterDeploymentAndIgnoresUnrelatedActions() {
        when(github.isAuthenticated()).thenReturn(true);
        when(github.getGitHubToken()).thenReturn("token");
        final Map<String, Object> generated = Map.of(
                "controller_services", List.of(Map.of(
                        "id", "csv-reader", "type", "example.CsvReader", "name", "CSV Reader")),
                "cs_actions", List.of(
                        Map.of("name", "CSV Reader", "action", "enable"),
                        Map.of("name", "Existing Service", "action", "disable"),
                        Map.of("name", "Unrelated Service", "action", "enable")),
                "processors", List.of(),
                "connections", List.of(),
                "explanation", "ok");
        when(llm.generateFlowSpec(
                anyString(), anyList(), anyString(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("target capabilities")))
                .thenReturn(generated);
        final ValidatedFlowPlan plan = new ValidatedFlowPlan(generated);
        when(flowBuilder.prepareFlow(generated, nifi, true)).thenReturn(plan);
        when(flowBuilder.buildFlow(plan, "root", nifi, Map.of(), 0, false, true))
                .thenReturn(new FlowBuilder.BuildResult(List.of(), 0));
        when(nifi.listControllerServices("root")).thenReturn(List.of(
                Map.of("component", Map.of("id", "cs-1", "name", "CSV Reader")),
                Map.of("component", Map.of("id", "cs-2", "name", "Existing Service"))));

        final ChatResponse response = controller.chat(request("github"));

        assertTrue(response.reply.contains("Enabled 'CSV Reader'"));
        assertTrue(response.reply.contains("Disabled 'Existing Service'"));
        assertFalse(response.reply.contains("Unrelated Service"));
        final InOrder order = org.mockito.Mockito.inOrder(flowBuilder, nifi);
        order.verify(flowBuilder).buildFlow(plan, "root", nifi, Map.of(), 0, false, true);
        order.verify(nifi).listControllerServices("root");
        order.verify(nifi).disableControllerService("cs-2");
        verify(nifi, never()).enableControllerService("cs-1");
    }

    @Test
    void repairsInvalidGenerationOnceBeforeDeployment() {
        when(github.isAuthenticated()).thenReturn(true);
        when(github.getGitHubToken()).thenReturn("token");
        final Map<String, Object> invalid = Map.of(
                "processors", List.of(), "connections", List.of(),
                "_token_usage", Map.of("input", 10, "output", 5, "total", 15));
        final Map<String, Object> repaired = Map.of(
                "processors", List.of(), "connections", List.of(), "explanation", "repaired",
                "_token_usage", Map.of("input", 7, "output", 3, "total", 10));
        when(llm.generateFlowSpec(
                anyString(), anyList(), anyString(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("target capabilities")))
                .thenReturn(invalid);
        when(llm.generateFlowSpec(
                anyString(), anyList(), anyString(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("expanded capabilities")))
                .thenReturn(repaired);
        final ValidationIssue issue = new ValidationIssue(
                "proc1", "config.Max Queue Size", "Missing required property", "Set the property");
        when(flowBuilder.prepareFlow(invalid, nifi, true))
                .thenThrow(new FlowSpecificationValidationException(new ValidationReport(List.of(issue))));
        final Map<String, Object> repairedWithUsage = new LinkedHashMap<>(repaired);
        repairedWithUsage.put("_token_usage", Map.of("input", 17, "output", 8, "total", 25));
        final ValidatedFlowPlan plan = new ValidatedFlowPlan(repairedWithUsage);
        when(flowBuilder.prepareFlow(repairedWithUsage, nifi, true)).thenReturn(plan);
        when(flowBuilder.buildFlow(plan, "root", nifi, Map.of(), 0, false, true))
                .thenReturn(new FlowBuilder.BuildResult(List.of(), 0));

        final ChatResponse response = controller.chat(request("github"));

        assertEquals("repaired", response.reply);
        assertEquals(Map.of("input", 17, "output", 8, "total", 25), response.tokens_used);
        verify(llm).generateFlowSpec(
                anyString(), anyList(), anyString(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("target capabilities"));
        verify(llm).generateFlowSpec(
                anyString(), anyList(), anyString(), anyList(), anyString(),
                anyList(), anyList(), anyList(), eq("expanded capabilities"));
        verify(hintDeriver).derive(any(ValidationReport.class), eq(graph));
        verify(contextExpander).expand(eq("build flow"), eq(graph), anyList(), eq(invalid));
        verify(metrics).observeFirstPass(false, List.of(issue));
        verify(metrics).observeRepairAttempt();
        verify(metrics).observeRepairTokens(7, 3);
        verify(metrics).observeRepairResult(true, List.of());
        verify(flowBuilder).buildFlow(plan, "root", nifi, Map.of(), 0, false, true);
    }

    @Test
    void capabilityDiscoveryFailureFailsClosedBeforeCallingProvider() {
        when(manager.capabilitySet(nifi))
                .thenThrow(new CapabilityDiscoveryException("target unavailable"));

        final ResponseStatusException exception = assertThrows(
                ResponseStatusException.class, () -> controller.chat(request("github")));

        assertEquals(502, exception.getStatusCode().value());
        assertTrue(exception.getReason().contains("generation was not attempted"));
        verifyNoInteractions(llm);
    }

    private ChatRequest request(final String provider) {
        final ChatRequest request = new ChatRequest();
        request.message = "build flow";
        request.provider = provider;
        request.read_canvas = false;
        request.process_group_id = "root";
        return request;
    }
}
