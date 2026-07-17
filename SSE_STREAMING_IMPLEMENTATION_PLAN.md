# Implementation Plan: Streaming Chat Responses (SSE)

This plan outlines the architecture and changes required to stream the AI's explanation to the UI as it's generated, keeping the user engaged *while* the flow is being built on the backend.

## Architecture Change Overview

Currently, the flow is completely synchronous:
1. Wait for LLM to generate entire JSON response
2. Parse JSON
3. Deploy flow
4. Send `ChatResponse` back to UI

To support streaming, we will change `/api/chat` to use **Server-Sent Events (SSE)** via Spring's `SseEmitter`.
1. Request LLM generation with streaming enabled (`stream: true`).
2. As chunks arrive, dynamically inspect the JSON stream.
3. When the stream is inside the `"explanation"` JSON field, emit SSE `message` events to the UI with the text chunks.
4. Continue buffering the entire JSON string in memory.
5. When the LLM stream finishes, parse the full buffered JSON.
6. Run the deployment logic (`flowBuilder.buildFlow()`).
7. Emit a final SSE `done` event containing the deployment statistics (`processors_created`, `connections_created`).

## Open Questions

> [!WARNING]
> **API Dependency**: Are we okay using the native Javascript `fetch` API to consume the stream in Angular? Standard `EventSource` does not support `POST` requests with JSON bodies. Using `fetch` with `getReader()` is the standard modern approach for this.

## Proposed Changes

---

### Backend (Spring Boot)

#### [MODIFY] `LlmClient.java`
- Modify the `generateFlowSpecGitHub` and `generateFlowSpecBedrock` methods to return a stream of characters/strings instead of a single `Map<String, Object>`. 
- **GitHub**: Use Java 11 `HttpClient.send(req, HttpResponse.BodyHandlers.ofLines())` to get a `Stream<String>` of SSE events from GitHub models.
- **AWS Bedrock**: Change `invokeModel` to `invokeModelWithResponseStream` (using `BedrockRuntimeAsyncClient` or the sync streaming handler) to stream chunks.
- Provide a unified callback interface `Consumer<String>` that receives text chunks as they are generated.

#### [MODIFY] `CopilotController.java`
- Change `@PostMapping("/api/chat")` to return an `SseEmitter`.
- Create a new background thread (or use an `ExecutorService`) to run the LLM call, freeing the HTTP thread.
- Implement a lightweight state machine inside the LLM stream consumer:
  - Accumulate the JSON chunks into a `StringBuilder`.
  - Try to isolate the text specifically being generated for the `"explanation"` field (using basic string boundary checks) and send it to the UI via `SseEmitter.send(SseEmitter.event().name("explanation").data(chunk))`.
- Once the LLM stream is finished, parse the accumulated JSON string into a `Map<String, Object>`.
- Call `flowBuilder.buildFlow()`.
- Send a final event `SseEmitter.send(SseEmitter.event().name("done").data(chatResponseObject))`.
- Call `SseEmitter.complete()`.

---

### Frontend (Angular)

#### [MODIFY] `copilot-chat.component.ts`
- Update the `sendMessage()` method to replace the `this.http.post(...)` call with a native `fetch()` call.
- Read the `ReadableStream` from `response.body.getReader()`.
- Parse the SSE text format (`event: ...\ndata: ...\n\n`).
- If event is `explanation`, append the data chunk to the assistant's message in the UI array. (This gives the "typing" effect in real-time).
- If event is `done`, parse the data as the final `ChatResponse`, update the `sessionProcessors`, save the session, and mark `isLoading = false`.
- Handle error events by displaying them directly in the chat.

## Verification Plan

### Manual Verification
1. Launch the NiFi Copilot UI.
2. Ask the Copilot to create a complex flow (e.g., "Create a flow to fetch from S3 and put to Azure Blob Storage").
3. Verify that the chat bubble appears immediately and starts typing out the explanation chunk-by-chunk.
4. Verify that once the typing stops, a small delay occurs (during deployment), and then the canvas updates with the new processors.
5. Check network logs to ensure the `/api/chat` request remains open and streams `text/event-stream` data.
