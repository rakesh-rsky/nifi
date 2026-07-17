# NiFi API Implementation Plan

# Project Summary

## Current Architecture

NiFi Copilot is integrated into the Apache NiFi monorepo and supports two execution modes behind the shared
`NiFiClientOperations` interface:

```text
NiFi Angular UI
    |
    +-- Embedded mode: /nifi-api/copilot/** -> CopilotResource
    |                                      -> CopilotController
    |                                      -> FlowBuilder
    |                                      -> InternalNiFiClient
    |                                      -> NiFiServiceFacade
    |
    +-- External mode: standalone Spring Boot application
                                           -> CopilotController
                                           -> FlowBuilder
                                           -> HttpNiFiClient
                                           -> /nifi-api/**
```

The main architectural responsibilities are:

| Area | Current responsibility |
|---|---|
| `CopilotController` | Coordinates chat, authentication, session state, LLM calls, and flow deployment |
| `FlowBuilder` | Converts an LLM flow specification into NiFi process groups, parameter contexts, controller services, processors, and connections |
| `NiFiClientOperations` | Defines the operations that must behave consistently in embedded and external modes |
| `HttpNiFiClient` | Uses the NiFi 2.x REST API, JWT authentication, JSON payloads, and optimistic-lock revisions |
| `InternalNiFiClient` | Implements the same contract through `NiFiServiceFacade` when Copilot runs inside NiFi |
| `LlmClient` | Invokes GitHub Models or AWS Bedrock and parses the generated flow specification |
| `GitHubAuthManager` / `AwsAuthManager` | Manage external LLM provider authentication |
| `SessionStore` | Persists chat history in SQLite by process-group ID |
| `CopilotResource` | Exposes embedded JAX-RS endpoints under `/nifi-api/copilot` |
| `CopilotConfigResource` | Tells the frontend whether Copilot is embedded or hosted externally |

All new NiFi operations must be added to `NiFiClientOperations` and implemented with equivalent behavior in
`HttpNiFiClient` and `InternalNiFiClient`. REST-specific concerns such as JWT refresh, HTTP retries, response
decoding, and URL construction remain in `HttpNiFiClient`.

## Working Rules

- Implement requested feature.
- Do not explain the code.

## Current Implementation Status

The current implementation supports flow creation and core process-group lifecycle operations:

- Process-group lookup, creation, update, deletion, direct child/component listings, and bulk scheduling
- Process-group canvas retrieval
- Processor type discovery
- Processor creation, configuration, start, stop, validation polling, and deletion
- Connection creation and deletion
- Controller-service listing, creation, enable/disable, and deletion
- Parameter-context creation, process-group binding, and deletion
- Snapshot-based rollback of newly created processors, connections, and child process groups

The client does not yet provide broad NiFi administration, queue management, provenance, registry, version-control,
diagnostic, cluster, counter, port, label, funnel, snippet, or remote-process-group operations.

## Existing NiFi REST Endpoints Already Implemented

These are the outbound endpoints currently used by `HttpNiFiClient`. Embedded mode performs equivalent operations
through `NiFiServiceFacade`.

| NiFi Endpoint | HTTP Method | Current purpose |
|---|---|---|
| `/nifi-api/access/token` | POST | Obtain a NiFi JWT in external mode |
| `/nifi-api/process-groups/root` | GET | Resolve the root process group |
| `/nifi-api/process-groups/{id}` | GET | Validate/read a process group and its revision |
| `/nifi-api/process-groups/{id}/process-groups` | POST | Create a child process group |
| `/nifi-api/process-groups/{id}` | PUT | Bind a parameter context to a process group |
| `/nifi-api/process-groups/{id}` | DELETE | Delete a newly created child process group during rollback |
| `/nifi-api/process-groups/{id}/processors` | GET | List processors in a process group |
| `/nifi-api/process-groups/{id}/connections` | GET | List connections in a process group |
| `/nifi-api/process-groups/{id}/process-groups` | GET | List direct child process groups |
| `/nifi-api/flow/process-groups/{id}` | GET | Read process-group canvas contents |
| `/nifi-api/flow/process-groups/{id}` | PUT | Bulk schedule or enable/disable process-group components |
| `/nifi-api/flow/processor-types` | GET | Populate the processor type and bundle cache |
| `/nifi-api/process-groups/{id}/processors` | POST | Create a processor |
| `/nifi-api/processors/{id}` | GET | Read processor state, validation errors, relationships, and revision |
| `/nifi-api/processors/{id}` | PUT | Update processor configuration and auto-terminated relationships |
| `/nifi-api/processors/{id}/run-status` | PUT | Start or stop a processor |
| `/nifi-api/processors/{id}` | DELETE | Delete a processor |
| `/nifi-api/processors/{id}/diagnostics` | GET | Read processor diagnostics |
| `/nifi-api/processors/{id}/state` | GET | Read processor component state |
| `/nifi-api/processors/{id}/state/clear-requests` | POST | Clear processor component state |
| `/nifi-api/processors/{id}/threads` | DELETE | Terminate active processor threads |
| `/nifi-api/process-groups/{id}/connections` | POST | Create a connection |
| `/nifi-api/connections/{id}` | GET | Read a connection and revision |
| `/nifi-api/connections/{id}` | PUT | Update connection configuration |
| `/nifi-api/connections/{id}` | DELETE | Delete a connection |
| `/nifi-api/flow/connections/{id}/statistics` | GET | Read aggregate connection statistics |
| `/nifi-api/flow/process-groups/{id}/controller-services` | GET | List process-group controller services |
| `/nifi-api/process-groups/{id}/controller-services` | POST | Create a controller service |
| `/nifi-api/controller-services/{id}` | GET | Read a controller service and revision |
| `/nifi-api/controller-services/{id}` | PUT | Update controller-service configuration |
| `/nifi-api/controller-services/{id}/run-status` | PUT | Enable or disable a controller service |
| `/nifi-api/controller-services/{id}/references` | GET | Read controller-service references |
| `/nifi-api/controller-services/{id}/references` | PUT | Bulk update controller-service references |
| `/nifi-api/controller-services/{id}` | DELETE | Delete a controller service |
| `/nifi-api/flow/status` | GET | Read overall flow status |
| `/nifi-api/flow/current-user` | GET | Read current user and permissions |
| `/nifi-api/flow/about` | GET | Read NiFi build information |
| `/nifi-api/flow/bulletin-board` | GET | Read filtered bulletins |
| `/nifi-api/flow/search-results` | GET | Search flow components |
| `/nifi-api/parameter-contexts` | POST | Create a parameter context |
| `/nifi-api/parameter-contexts/{id}` | GET | Read a parameter context and revision |
| `/nifi-api/parameter-contexts/{id}` | DELETE | Delete a parameter context |
| `/nifi-api/process-groups/{id}/remote-process-groups` | POST | Create a remote process group |
| `/nifi-api/remote-process-groups/{id}` | GET | Read a remote process group |
| `/nifi-api/remote-process-groups/{id}` | PUT | Update a remote process group |
| `/nifi-api/remote-process-groups/{id}/run-status` | PUT | Start or stop remote transmission |
| `/nifi-api/remote-process-groups/{id}` | DELETE | Delete a remote process group |
| `/nifi-api/process-groups/{id}/input-ports` | POST | Create an input port |
| `/nifi-api/input-ports/{id}` | GET | Read an input port |
| `/nifi-api/input-ports/{id}/run-status` | PUT | Update input-port run status |
| `/nifi-api/input-ports/{id}` | DELETE | Delete an input port |
| `/nifi-api/process-groups/{id}/output-ports` | POST | Create an output port |
| `/nifi-api/output-ports/{id}` | GET | Read an output port |
| `/nifi-api/output-ports/{id}/run-status` | PUT | Update output-port run status |
| `/nifi-api/output-ports/{id}` | DELETE | Delete an output port |
| `/nifi-api/process-groups/{id}/labels` | POST | Create a label |
| `/nifi-api/labels/{id}` | GET | Read a label |
| `/nifi-api/labels/{id}` | PUT | Update a label |
| `/nifi-api/labels/{id}` | DELETE | Delete a label |
| `/nifi-api/process-groups/{id}/funnels` | POST | Create a funnel |
| `/nifi-api/funnels/{id}` | GET | Read a funnel |
| `/nifi-api/funnels/{id}` | PUT | Update a funnel |
| `/nifi-api/funnels/{id}` | DELETE | Delete a funnel |
| `/nifi-api/snippets` | POST | Create a temporary snippet |
| `/nifi-api/snippets/{id}` | PUT | Move snippet components |
| `/nifi-api/process-groups/{id}/snippet-instance` | POST | Copy snippet components |
| `/nifi-api/snippets/{id}` | DELETE | Delete snippet components |
| `/nifi-api/controller/registry-clients` | GET, POST | List and create registry clients |
| `/nifi-api/controller/registry-clients/{id}` | GET, PUT, DELETE | Read, update, and delete a registry client |
| `/nifi-api/flow/registries/{id}/buckets` | GET | List registry buckets |
| `/nifi-api/flow/registries/{registryId}/buckets/{bucketId}/flows` | GET | List registry bucket flows |
| `/nifi-api/versions/process-groups/{id}` | GET, POST | Read version information and start version control |
| `/nifi-api/versions/active-requests` | POST | Acquire an active version-control request |
| `/nifi-api/versions/active-requests/{id}` | PUT, DELETE | Apply component mappings and release an active request |
| `/nifi-api/versions/update-requests/process-groups/{id}` | POST | Submit a version update |
| `/nifi-api/versions/update-requests/{id}` | GET, DELETE | Poll and delete a version-update request |
| `/nifi-api/versions/revert-requests/process-groups/{id}` | POST | Submit a version revert |
| `/nifi-api/versions/revert-requests/{id}` | GET, DELETE | Poll and delete a version-revert request |
| `/nifi-api/process-groups/{id}/download` | GET | Export a process-group JSON flow snapshot |
| `/nifi-api/process-groups/{id}/process-groups/import` | POST | Import a process-group JSON flow snapshot |
| `/nifi-api/system-diagnostics` | GET | Read system diagnostics |
| `/nifi-api/system-diagnostics/jmx-metrics` | GET | Read filtered JMX metrics |
| `/nifi-api/flow/cluster/summary` | GET | Read the cluster summary |
| `/nifi-api/controller/cluster` | GET | Read cluster nodes |
| `/nifi-api/controller/cluster/nodes/{id}` | GET, PUT, DELETE | Read, update, and remove a cluster node |
| `/nifi-api/counters` | GET, PUT | Read and reset all counters |
| `/nifi-api/counters/{id}` | PUT | Reset a counter |
| `/nifi-api/access/logout` | DELETE | Invalidate the current JWT session |
| `/nifi-api/authentication/configuration` | GET | Read authentication configuration |
| `/nifi-api/resources` | GET | List authorizable resources |
| `/nifi-api/policies/{action}/{resource}` | GET | Read a resource access policy |
| `/nifi-api/tenants/users` | GET | List users |
| `/nifi-api/tenants/user-groups` | GET | List user groups |

# Feature Roadmap

## File References

The roadmap uses these shortened file names:

| Name | Repository path |
|---|---|
| `Operations` | `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/NiFiClientOperations.java` |
| `HTTP Client` | `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/HttpNiFiClient.java` |
| `Internal Client` | `nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api/src/main/java/org/apache/nifi/web/api/InternalNiFiClient.java` |
| `Flow Builder` | `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/FlowBuilder.java` |
| `DTOs` | `nifi-copilot/src/main/java/org/apache/nifi/copilot/api/Dto.java` |
| `HTTP Tests` | `nifi-copilot/src/test/java/org/apache/nifi/copilot/service/HttpNiFiClientTest.java` |
| `Internal Tests` | `nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api/src/test/java/org/apache/nifi/web/api/InternalNiFiClientTest.java` |

Each endpoint test must cover request/argument mapping, response mapping, error behavior, revision handling where
applicable, and parity between HTTP and embedded modes.

> **Test generation is deferred by project-owner direction.** The `Tests Required` column remains the approved
> future coverage plan, but no test files or test dependencies will be added until explicitly requested.

## 1. HTTP Infrastructure

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| JWT authentication | `/nifi-api/access/token` | POST | ✅ Implemented | Critical | HTTP Client | Token success, rejection, encoding |
| Shared JSON request execution | All JSON endpoints | GET/POST/PUT/DELETE | ✅ Implemented | Critical | HTTP Client | Headers, empty bodies, malformed JSON, error bodies |
| Authentication refresh | All authenticated endpoints | All | ✅ Implemented | Critical | HTTP Client | Retry once after 401; no retry loop |
| Structured NiFi exceptions | All endpoints | All | ✅ Implemented | Critical | HTTP Client | Preserve status, method, path, body |
| Revision-aware mutation helper | Revision-bearing endpoints | PUT/DELETE | ✅ Implemented | Critical | Operations, HTTP Client, Internal Client | Fresh revision, version 0 create, bounded invalid-revision retry |
| Transient retry policy | Read-only endpoints | GET | ✅ Implemented | High | HTTP Client | 429/502/503/504/transport backoff, jitter, and `Retry-After` |
| Async request lifecycle | `*/**-requests/**` | POST/GET/DELETE | ✅ Implemented | Critical | `NiFiAsyncRequestExecutor` | Completion, failure, timeout, interruption, cleanup |
| Contextual reliability logging | All HTTP endpoints | All | ✅ Implemented | Medium | HTTP Client, both clients | Method/path/status/duration and retry attempts without sensitive bodies |
| Registry-backed client metrics | All client operations | All | ✅ Implemented | Medium | Metrics registry, client selection | Bounded operation counters, outcomes, and timers without external dependencies |
| Configurable request timeouts | All endpoints | All | ✅ Implemented | High | HTTP Client, `NiFiConfigResolver` | Connect/request/poll timeout configuration |
| TLS verification and trust configuration | All HTTPS endpoints | All | ✅ Implemented | Critical | HTTP Client, `nifi.properties`, README | Secure default and configured trust material |
| Typed common entities | Revision and async entities | N/A | ✅ Implemented | High | Typed service records, both clients | Strict revision and asynchronous-state parsing |

## 2. Process Groups

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Get process group | `/nifi-api/process-groups/{id}` | GET | ✅ Implemented | High | Operations, both clients | Root, explicit ID, not found |
| Create child process group | `/nifi-api/process-groups/{id}/process-groups` | POST | ✅ Implemented | High | Operations, both clients | Revision 0 and component mapping |
| Update process group | `/nifi-api/process-groups/{id}` | PUT | ✅ Implemented | High | Operations, both clients | Name, position, comments, parameter context |
| Delete process group | `/nifi-api/process-groups/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Revision and non-empty/running failures |
| List processors | `/nifi-api/process-groups/{id}/processors` | GET | ✅ Implemented | High | Operations, both clients | Empty and populated groups |
| List connections | `/nifi-api/process-groups/{id}/connections` | GET | ✅ Implemented | High | Operations, both clients | Empty and populated groups |
| List child process groups | `/nifi-api/process-groups/{id}/process-groups` | GET | ✅ Implemented | Medium | Operations, both clients | Nested group mapping |
| Bulk schedule process group | `/nifi-api/flow/process-groups/{id}` | PUT | ✅ Implemented | High | Operations, both clients | Start/stop and per-component revisions |
| Export versioned flow JSON | `/nifi-api/process-groups/{id}/download` | GET | ✅ Implemented | High | Operations, both clients | JSON flow snapshot and inclusion flags |
| Import versioned flow JSON | `/nifi-api/process-groups/{id}/process-groups/import` | POST | ✅ Implemented | High | Operations, both clients | Valid import and validation failure |

## 3. Processors

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create processor | `/nifi-api/process-groups/{id}/processors` | POST | ✅ Implemented | Critical | Operations, both clients | Type resolution, bundle, properties |
| Get processor | `/nifi-api/processors/{id}` | GET | ✅ Implemented | Critical | Operations, both clients | Component and revision mapping |
| Update processor configuration | `/nifi-api/processors/{id}` | PUT | ✅ Implemented | High | Operations, both clients | Properties, scheduling, auto-termination |
| Set processor run status | `/nifi-api/processors/{id}/run-status` | PUT | ✅ Implemented | Critical | Operations, both clients | STARTED/STOPPED/DISABLED and 409 retry |
| Delete processor | `/nifi-api/processors/{id}` | DELETE | ✅ Implemented | Critical | Operations, both clients | Stop-before-delete and revision |
| Processor diagnostics | `/nifi-api/processors/{id}/diagnostics` | GET | ✅ Implemented | High | Operations, both clients | Diagnostic response mapping |
| Get processor state | `/nifi-api/processors/{id}/state` | GET | ✅ Implemented | High | Operations, both clients | Cluster and local state |
| Clear processor state | `/nifi-api/processors/{id}/state/clear-requests` | POST | ✅ Implemented | High | Operations, both clients | Synchronous clear-all response in NiFi 2.10 |
| Terminate processor threads | `/nifi-api/processors/{id}/threads` | DELETE | ✅ Implemented | Medium | Operations, both clients | Permission and active-thread behavior |

## 4. Connections

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create connection | `/nifi-api/process-groups/{id}/connections` | POST | ✅ Implemented | Critical | Operations, both clients | Source, destination, relationships |
| Get connection | `/nifi-api/connections/{id}` | GET | ✅ Implemented | High | Operations, both clients | Component and revision mapping |
| Update connection | `/nifi-api/connections/{id}` | PUT | ✅ Implemented | High | Operations, both clients | Backpressure, prioritizers, relationships |
| Delete connection | `/nifi-api/connections/{id}` | DELETE | ✅ Implemented | Critical | Operations, both clients | Revision and non-empty queue failure |
| Connection statistics | `/nifi-api/flow/connections/{id}/statistics` | GET | ✅ Implemented | High | Operations, both clients | Throughput and queue metrics |

## 5. Controller Services

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| List process-group services | `/nifi-api/flow/process-groups/{id}/controller-services` | GET | ✅ Implemented | High | Operations, both clients | Recursive and empty results |
| Create controller service | `/nifi-api/process-groups/{id}/controller-services` | POST | ✅ Implemented | Critical | Operations, both clients | Type, name, properties, revision 0 |
| Get controller service | `/nifi-api/controller-services/{id}` | GET | ✅ Implemented | Critical | Operations, both clients | State and revision |
| Update controller service | `/nifi-api/controller-services/{id}` | PUT | ✅ Implemented | High | Operations, both clients | Disabled-state validation and properties |
| Enable/disable service | `/nifi-api/controller-services/{id}/run-status` | PUT | ✅ Implemented | Critical | Operations, both clients | Transition polling and timeout |
| Get service references | `/nifi-api/controller-services/{id}/references` | GET | ✅ Implemented | High | Operations, both clients | Processor and service references |
| Update service references | `/nifi-api/controller-services/{id}/references` | PUT | ✅ Implemented | High | Operations, both clients | Bulk stop/disable/enable and revisions |
| Delete controller service | `/nifi-api/controller-services/{id}` | DELETE | ✅ Implemented | Critical | Operations, both clients | Disable-before-delete and references |

## 6. Parameter Contexts

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create parameter context | `/nifi-api/parameter-contexts` | POST | ✅ Implemented | Critical | Operations, both clients | Sensitive/non-sensitive parameters |
| Get parameter context | `/nifi-api/parameter-contexts/{id}` | GET | ✅ Implemented | High | Operations, both clients | Parameter and revision mapping |
| List parameter contexts | `/nifi-api/flow/parameter-contexts` | GET | ✅ Implemented | High | Operations, both clients | Empty and populated results |
| Direct context update | `/nifi-api/parameter-contexts/{id}` | PUT | ✅ Implemented | Medium | Operations, both clients | Offline update and revision |
| Submit live update | `/nifi-api/parameter-contexts/{id}/update-requests` | POST | ✅ Implemented | High | Operations, both clients | Affected components and request ID |
| Poll live update | `/nifi-api/parameter-contexts/{id}/update-requests/{requestId}` | GET | ✅ Implemented | High | Operations, both clients | Completion and failure state |
| Delete live update request | `/nifi-api/parameter-contexts/{id}/update-requests/{requestId}` | DELETE | ✅ Implemented | High | Operations, both clients | Cleanup after success/failure/timeout |
| Bind context to process group | `/nifi-api/process-groups/{id}` | PUT | ✅ Implemented | Critical | Operations, both clients | Binding and revision |
| Delete parameter context | `/nifi-api/parameter-contexts/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Revision and referenced-context failure |

## 7. Flow APIs

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Get process-group flow | `/nifi-api/flow/process-groups/{id}` | GET | ✅ Implemented | Critical | Operations, both clients | Canvas mapping |
| List processor types | `/nifi-api/flow/processor-types` | GET | ✅ Implemented | Critical | Operations, both clients | Bundle cache and duplicate types |
| Flow status | `/nifi-api/flow/status` | GET | ✅ Implemented | High | Operations, both clients | Standalone and cluster response |
| Current user and permissions | `/nifi-api/flow/current-user` | GET | ✅ Implemented | High | Operations, both clients | Read/write permission mapping |
| NiFi build information | `/nifi-api/flow/about` | GET | ✅ Implemented | Medium | Operations, both clients | Version compatibility data |
| Bulletin board | `/nifi-api/flow/bulletin-board` | GET | ✅ Implemented | High | Operations, both clients | Filters and empty results |
| Search flow components | `/nifi-api/flow/search-results` | GET | ✅ Implemented | Medium | Operations, both clients | Query encoding and grouped results |

## 8. FlowFile Queues

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Submit queue listing | `/nifi-api/flowfile-queues/{id}/listing-requests` | POST | ✅ Implemented | High | Operations, both clients | Request creation |
| Poll queue listing | `/nifi-api/flowfile-queues/{id}/listing-requests/{requestId}` | GET | ✅ Implemented | High | Operations, both clients | Results, timeout, failure |
| Delete queue listing | `/nifi-api/flowfile-queues/{id}/listing-requests/{requestId}` | DELETE | ✅ Implemented | High | Operations, both clients | Guaranteed cleanup |
| Get queued FlowFile details | `/nifi-api/flowfile-queues/{id}/flowfiles/{uuid}` | GET | ✅ Implemented | Medium | Operations, both clients | Attributes and missing FlowFile |
| Download queued content | `/nifi-api/flowfile-queues/{id}/flowfiles/{uuid}/content` | GET | ✅ Implemented | Medium | Operations, both clients | Streaming and content type |
| Submit queue drop | `/nifi-api/flowfile-queues/{id}/drop-requests` | POST | ✅ Implemented | Critical | Operations, both clients | Explicit destructive-operation guard |
| Poll queue drop | `/nifi-api/flowfile-queues/{id}/drop-requests/{requestId}` | GET | ✅ Implemented | Critical | Operations, both clients | Completion and failure |
| Delete queue drop request | `/nifi-api/flowfile-queues/{id}/drop-requests/{requestId}` | DELETE | ✅ Implemented | Critical | Operations, both clients | Cleanup in all outcomes |

## 9. Provenance

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Submit provenance query | `/nifi-api/provenance` | POST | ✅ Implemented | High | Operations, both clients | Query criteria and request ID |
| Poll provenance query | `/nifi-api/provenance/{id}` | GET | ✅ Implemented | High | Operations, both clients | Pagination, completion, cluster node |
| Delete provenance query | `/nifi-api/provenance/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Cleanup |
| Submit lineage query | `/nifi-api/provenance/lineage` | POST | ✅ Implemented | High | Operations, both clients | Parents/children and event lineage |
| Poll lineage query | `/nifi-api/provenance/lineage/{id}` | GET | ✅ Implemented | High | Operations, both clients | Completion and graph mapping |
| Delete lineage query | `/nifi-api/provenance/lineage/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Cleanup |

## 10. Remote Process Groups

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create remote process group | `/nifi-api/process-groups/{id}/remote-process-groups` | POST | ✅ Implemented | High | Operations, both clients | URL, transport, timeout, revision 0 |
| Get remote process group | `/nifi-api/remote-process-groups/{id}` | GET | ✅ Implemented | High | Operations, both clients | Ports and transmission state |
| Update remote process group | `/nifi-api/remote-process-groups/{id}` | PUT | ✅ Implemented | Medium | Operations, both clients | URL, timeout, transport protocol |
| Set transmission status | `/nifi-api/remote-process-groups/{id}/run-status` | PUT | ✅ Implemented | High | Operations, both clients | Start/stop and revision |
| Delete remote process group | `/nifi-api/remote-process-groups/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Stop-before-delete and revision |

## 11. Ports

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create input port | `/nifi-api/process-groups/{id}/input-ports` | POST | ✅ Implemented | High | Operations, both clients | Name, position, revision 0 |
| Get input port | `/nifi-api/input-ports/{id}` | GET | ✅ Implemented | Medium | Operations, both clients | Component and revision |
| Set input-port status | `/nifi-api/input-ports/{id}/run-status` | PUT | ✅ Implemented | High | Operations, both clients | Running, stopped, or disabled |
| Delete input port | `/nifi-api/input-ports/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Connections and revision |
| Create output port | `/nifi-api/process-groups/{id}/output-ports` | POST | ✅ Implemented | High | Operations, both clients | Name, position, revision 0 |
| Get output port | `/nifi-api/output-ports/{id}` | GET | ✅ Implemented | Medium | Operations, both clients | Component and revision |
| Set output-port status | `/nifi-api/output-ports/{id}/run-status` | PUT | ✅ Implemented | High | Operations, both clients | Running, stopped, or disabled |
| Delete output port | `/nifi-api/output-ports/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Connections and revision |

## 12. Labels

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create label | `/nifi-api/process-groups/{id}/labels` | POST | ✅ Implemented | Medium | Operations, both clients | Text, style, position, size |
| Get label | `/nifi-api/labels/{id}` | GET | ✅ Implemented | Medium | Operations, both clients | Component and revision |
| Update label | `/nifi-api/labels/{id}` | PUT | ✅ Implemented | Medium | Operations, both clients | Text, style, position, revision |
| Delete label | `/nifi-api/labels/{id}` | DELETE | ✅ Implemented | Medium | Operations, both clients | Revision |

## 13. Funnels

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create funnel | `/nifi-api/process-groups/{id}/funnels` | POST | ✅ Implemented | Medium | Operations, both clients | Position and revision 0 |
| Get funnel | `/nifi-api/funnels/{id}` | GET | ✅ Implemented | Medium | Operations, both clients | Component and revision |
| Update funnel | `/nifi-api/funnels/{id}` | PUT | ✅ Implemented | Medium | Operations, both clients | Position and revision |
| Delete funnel | `/nifi-api/funnels/{id}` | DELETE | ✅ Implemented | Medium | Operations, both clients | Connections and revision |

## 14. Snippets

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Create snippet | `/nifi-api/snippets` | POST | ✅ Implemented | Medium | Operations, both clients | Component maps and expiration |
| Move snippet | `/nifi-api/snippets/{id}` | PUT | ✅ Implemented | Medium | Operations, both clients | Destination process group |
| Copy snippet | `/nifi-api/process-groups/{id}/snippet-instance` | POST | ✅ Implemented | Medium | Operations, both clients | Destination group and coordinates |
| Delete snippet components | `/nifi-api/snippets/{id}` | DELETE | ✅ Implemented | Medium | Operations, both clients | Per-component revisions and partial failure |

## 15. Registry Clients

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| List registry clients | `/nifi-api/controller/registry-clients` | GET | ✅ Implemented | High | Operations, both clients | Empty and populated results |
| Create registry client | `/nifi-api/controller/registry-clients` | POST | ✅ Implemented | High | Operations, both clients | Type, bundle, properties, revision 0 |
| Get registry client | `/nifi-api/controller/registry-clients/{id}` | GET | ✅ Implemented | High | Operations, both clients | Component and revision |
| Update registry client | `/nifi-api/controller/registry-clients/{id}` | PUT | ✅ Implemented | High | Operations, both clients | Properties and revision conflicts |
| Delete registry client | `/nifi-api/controller/registry-clients/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Referenced client and revision |
| List registry buckets | `/nifi-api/flow/registries/{id}/buckets` | GET | ✅ Implemented | High | Operations, both clients | Branch/bucket mapping |
| List bucket flows | `/nifi-api/flow/registries/{registryId}/buckets/{bucketId}/flows` | GET | ✅ Implemented | High | Operations, both clients | Empty and paginated results |

## 16. Versioned Flows

NiFi 2.x has removed the templates API. Versioned-flow and process-group import/export endpoints are the supported
replacement and no `/nifi-api/templates` operation should be introduced.

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Get version information | `/nifi-api/versions/process-groups/{id}` | GET | ✅ Implemented | High | Operations, both clients | Unversioned, clean, locally modified |
| Acquire version request | `/nifi-api/versions/active-requests` | POST | ✅ Implemented | Critical | Operations, both clients | Lock acquisition and contention |
| Apply version component mapping | `/nifi-api/versions/active-requests/{id}` | PUT | ✅ Implemented | High | Operations, both clients | Revision, selected version, and component mapping |
| Release version request | `/nifi-api/versions/active-requests/{id}` | DELETE | ✅ Implemented | Critical | Operations, both clients | Cleanup after every outcome |
| Start version control | `/nifi-api/versions/process-groups/{id}` | POST | ✅ Implemented | High | Operations, both clients | Registry, bucket, flow identity |
| Submit version update | `/nifi-api/versions/update-requests/process-groups/{id}` | POST | ✅ Implemented | High | Operations, both clients | Target version and request ID |
| Poll version update | `/nifi-api/versions/update-requests/{id}` | GET | ✅ Implemented | High | Operations, both clients | Completion, failure, affected components |
| Delete version update request | `/nifi-api/versions/update-requests/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Cleanup |
| Submit version revert | `/nifi-api/versions/revert-requests/process-groups/{id}` | POST | ✅ Implemented | High | Operations, both clients | Local modification handling |
| Poll version revert | `/nifi-api/versions/revert-requests/{id}` | GET | ✅ Implemented | High | Operations, both clients | Completion and failure |
| Delete version revert request | `/nifi-api/versions/revert-requests/{id}` | DELETE | ✅ Implemented | High | Operations, both clients | Cleanup |
| Export process-group flow | `/nifi-api/process-groups/{id}/download` | GET | ✅ Implemented | High | Operations, both clients | Referenced services and component state flags |
| Import process-group flow | `/nifi-api/process-groups/{id}/process-groups/import` | POST | ✅ Implemented | High | Operations, both clients | Snapshot, name, position, and revision 0 |

## 17. Diagnostics

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| System diagnostics | `/nifi-api/system-diagnostics` | GET | ✅ Implemented | High | Operations, both clients | BASIC/VERBOSE, nodewise, cluster node |
| JMX metrics | `/nifi-api/system-diagnostics/jmx-metrics` | GET | ✅ Implemented | Low | Operations, both clients | Bean filter encoding and unsupported versions |

## 18. Cluster

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| Cluster summary | `/nifi-api/flow/cluster/summary` | GET | ✅ Implemented | High | Operations, both clients | Clustered and standalone instances |
| Get cluster nodes | `/nifi-api/controller/cluster` | GET | ✅ Implemented | High | Operations, both clients | Connected/disconnected nodes |
| Get cluster node | `/nifi-api/controller/cluster/nodes/{id}` | GET | ✅ Implemented | Medium | Operations, both clients | Node details and missing node |
| Update cluster node | `/nifi-api/controller/cluster/nodes/{id}` | PUT | ✅ Implemented | Low | Operations, both clients | Explicit confirmation, valid transitions, and authoritative admin authorization |
| Remove cluster node | `/nifi-api/controller/cluster/nodes/{id}` | DELETE | ✅ Implemented | Low | Operations, both clients | Explicit confirmation and disconnected/offloaded guard |

## 19. Counters

These endpoints are marked non-guaranteed by NiFi and require exact-runtime compatibility tests.

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| List counters | `/nifi-api/counters` | GET | ✅ Implemented | Medium | Operations, both clients | Nodewise and cluster response |
| Reset one counter | `/nifi-api/counters/{id}` | PUT | ✅ Implemented | Low | Operations, both clients | Authorization and explicit confirmation |
| Reset all counters | `/nifi-api/counters` | PUT | ✅ Implemented | Low | Operations, both clients | Destructive-operation guard |

## 20. Access APIs

| Feature | NiFi Endpoint | HTTP Method | Status | Priority | Files to Modify | Tests Required |
|---|---|---|---|---|---|---|
| NiFi login | `/nifi-api/access/token` | POST | ✅ Implemented | Critical | HTTP Client | Credentials, token parsing, rejection |
| NiFi logout | `/nifi-api/access/logout` | DELETE | ✅ Implemented | High | Operations, HTTP Client | Token invalidation and local token clearing |
| Authentication configuration | `/nifi-api/authentication/configuration` | GET | ✅ Implemented | High | Operations, both clients | Local, OIDC, and SAML configurations |
| List authorizable resources | `/nifi-api/resources` | GET | ✅ Implemented | Medium | Operations, both clients | Hierarchical resource mapping |
| Get resource policy | `/nifi-api/policies/{action}/{resource}` | GET | ✅ Implemented | Medium | Operations, both clients | Resource path and permissions |
| List users | `/nifi-api/tenants/users` | GET | ✅ Implemented | Medium | Operations, both clients | Empty and populated tenants |
| List user groups | `/nifi-api/tenants/user-groups` | GET | ✅ Implemented | Medium | Operations, both clients | Membership mapping |

# Technical Debt

The following work should be completed before adding feature endpoints:

1. **Add a regression test baseline — deferred until explicitly requested.** There are currently no Copilot unit or integration tests. Cover the existing
   `NiFiClientOperations` contract, HTTP request shapes, internal facade calls, `FlowBuilder` orchestration, and
   embedded/external controller behavior before expanding the interface.
2. **Correct processor run-status handling — completed in Phase 0.** Replace external-mode state changes through
   `PUT /processors/{id}` with `PUT /processors/{id}/run-status`. Align embedded mode with the same semantic
   operation rather than mutating a full processor DTO.
3. **Introduce structured HTTP errors — completed in Phase 0.** Preserve HTTP status, method, endpoint, NiFi response body, and retryability.
   Do not wrap every failure in an indistinguishable `RuntimeException`.
4. **Centralize revision handling — strict extraction and conflict recovery completed.** Reuse one helper for revision extraction, revision-0 creation, DELETE query
   parameters, and bounded conflict recovery. A retry must re-read and reconstruct the mutation instead of replaying
   a stale body.
5. **Add a safe retry policy — completed in Phase 1.** Automatic transient retries are restricted to GET requests.
   Authentication refresh, invalid revisions, throttling, gateway failures, and transport failures are handled
   separately with bounded attempts, exponential backoff, jitter, and `Retry-After`.
6. **Create a shared asynchronous-operation abstraction — completed in Phase 1.** It supports request submission,
   bounded polling, terminal failure, timeout, interruption, and unconditional request-resource cleanup.
7. **Make external TLS secure by default — completed in Phase 0.** `nifi.verify.ssl=false` must not be the production default. Support
   configured trust stores or standard JVM trust material instead of a trust-all manager.
8. **Remove silent failure paths.** Replace `catch (Exception ignored)` in `FlowBuilder`, both client implementations,
   and rollback code with scoped exceptions and contextual logging. Partial deployment must be reported explicitly.
9. **Complete rollback coverage — completed in Phase 3.** Deployment-created controller services and parameter
   contexts are tracked precisely, original parameter-context bindings are restored, and rollback returns a cleanup report.
10. **Guarantee embedded/external parity.** Add contract tests that execute the same behavioral cases against
    `HttpNiFiClient` and `InternalNiFiClient`. No endpoint should be considered complete until both modes are covered,
    except external authentication endpoints that are intentionally HTTP-only.
11. **Introduce focused DTOs — common infrastructure completed.** Typed revision and asynchronous-request state
    parsing now centralize strict validation while public map-based contracts remain backward compatible.
12. **Fix revision null and numeric safety — completed in Phase 0.** Revision-bearing mutations use strict long
    parsing and reject missing, malformed, or negative versions instead of silently defaulting to `0`.
13. **Add public API JavaDoc.** Document revision behavior, destructive operations, async cleanup, expected states,
    permissions, and error semantics on the expanded client interface.
14. **Add request validation.** Apply Bean Validation to Copilot request DTOs and validate component IDs, process-group
    IDs, URLs, timeouts, state values, and destructive-operation confirmations at the boundary.
15. **Restrict CORS.** Replace wildcard origin patterns with deployment-configurable allowed origins in external mode.
16. **Secure credential storage.** POSIX file permissions do not protect credentials on Windows. Use platform-aware
    file ACLs or an OS credential store and avoid silently accepting insecure persistence.
17. **Manage executor lifecycle.** GitHub and AWS authentication polling executors need explicit shutdown hooks and
    cancellation-safe task management.
18. **Add operational observability — partial.** Phase 1 adds safe request timing and retry/conflict logging.
    Registry-backed counters and timers, rollback metrics, and LLM usage metrics remain future work.
19. **Add rate and concurrency limits.** Protect `/api/chat`, authentication polling, destructive queue operations,
    provenance queries, and version-control operations from unbounded concurrency.
20. **Externalize deployment-specific constants.** OAuth client IDs, model IDs, API URLs, request timeouts, history
    depth, and processor aliases should be configurable while preserving current defaults where safe.
21. **Protect backward compatibility.** Keep existing `NiFiClientOperations` methods and Copilot endpoints stable.
    Add new methods incrementally and use default/adaptor behavior only where semantics remain unambiguous.
22. **Make processor-type caches thread-safe.** Both production clients populate singleton `HashMap` caches lazily.
    Guard initial population or use a concurrent map before allowing parallel processor-creation requests.

# Implementation Phases

Each phase is intended to be independently testable and committable.

## Phase 0: Baseline and Safety Foundation

- Add structured NiFi exceptions and preserve response bodies.
- Correct processor run-status handling.
- Centralize revisions and fail on missing revision versions.
- Add secure TLS defaults and configurable timeouts.
- Refresh authentication once after a `401` without clearing a concurrently refreshed token.

**Exit criteria:** Both production modes compile, safety changes preserve the existing client contract, no new
feature endpoints are added, and test generation remains deferred until explicitly requested.

## Phase 1: HTTP Reliability and Shared Async Infrastructure

- Add bounded transient retries and revision-conflict recovery.
- Implement the submit/poll/delete async lifecycle abstraction.
- Add contextual timing, retry, conflict, and failure logging without sensitive payloads.
- Keep registry-backed metrics deferred until an observability dependency is explicitly approved.

**Dependency:** Phase 0.

## Phase 2: Core Flow Read and Lifecycle Operations

- Complete process-group update/delete semantics and child listings.
- Implement processor run status, diagnostics, and state operations.
- Implement connection updates and statistics.
- Implement controller-service update and reference operations.
- Add flow status, current-user, bulletin, search, and version information reads.

**Dependency:** Phases 0-1.

## Phase 3: Parameter Contexts and FlowFile Queue Operations

- Implement parameter-context listing and live async updates.
- Implement queue listing, FlowFile details/content, and request cleanup.
- Implement guarded queue-drop operations.
- Complete rollback tracking for parameter contexts and controller services.

**Dependency:** Shared async lifecycle from Phase 1.

**Status:** Complete.

## Phase 4: Canvas Component Coverage

- Implement input/output ports.
- Implement remote process groups and transmission status.
- Implement labels.
- Implement funnels.
- Implement snippets.
- Extend `FlowBuilder` only after the corresponding client operations are complete.

**Dependency:** Revision and client-parity infrastructure from Phases 0-2.

## Phase 5: Provenance

- Implement provenance query submit/poll/delete.
- Implement lineage query submit/poll/delete.
- Add pagination, cluster-node selection, timeout, and cleanup handling.

**Dependency:** Shared async lifecycle and structured errors.

**Status:** Complete.

## Phase 6: Registry Clients and Versioned Flows

- Implement registry-client CRUD and registry browsing.
- Implement active version-request acquisition, component-mapping update, and release.
- Implement start-version-control.
- Implement async update and revert lifecycles.
- Implement process-group JSON import/export.
- Do not introduce removed template endpoints.

**Dependency:** Phases 1, 2, and 5; registry operations must precede version-control operations.

**Status:** Complete.

## Phase 7: Diagnostics and Administrative Reads

- Implement system diagnostics and optional JMX metrics.
- Implement cluster summary and node reads.
- Implement counter reads.
- Implement authentication configuration, logout, resources, policies, users, and user groups.

**Dependency:** Structured permissions and error handling from earlier phases.

**Status:** Complete.

## Phase 8: Guarded Administrative Mutations

- Implement cluster-node changes and removal.
- Implement counter resets.
- Add explicit authorization checks, confirmation requirements, audit logging, and non-guaranteed-endpoint
  compatibility tests.

**Dependency:** All previous phases.

**Status:** Complete. Compatibility tests remain deferred by project-owner direction.

# Progress Tracker

## Foundation

- [ ] Existing-operation HTTP tests complete (deferred until requested)
- [ ] Existing-operation internal-client tests complete (deferred until requested)
- [ ] Client parity contract tests complete (deferred until requested)
- [x] Structured NiFi exceptions complete
- [x] Processor run-status correction complete
- [x] Strict revision extraction complete
- [x] Revision-conflict recovery complete
- [x] GET-only transient retry policy complete
- [x] Async lifecycle helper complete
- [x] Contextual timing and retry logging complete
- [x] Registry-backed client metrics complete
- [x] Secure TLS defaults complete
- [x] Configurable connect and request timeouts complete
- [x] Rollback completeness and reporting complete

## Feature Progress

- [x] HTTP Infrastructure
- [x] Process Groups
- [x] Processors
- [x] Connections
- [x] Controller Services
- [x] Parameter Contexts
- [x] Flow APIs
- [x] FlowFile Queues
- [x] Provenance
- [x] Remote Process Groups
- [x] Ports
- [x] Labels
- [x] Funnels
- [x] Snippets
- [x] Registry Clients
- [x] Versioned Flows
- [x] Diagnostics
- [x] Cluster
- [x] Counters
- [x] Access APIs

## Phase Progress

- [x] Phase 0: Baseline and Safety Foundation
- [x] Phase 1: HTTP Reliability and Shared Async Infrastructure
- [x] Phase 2: Core Flow Read and Lifecycle Operations
- [x] Phase 3: Parameter Contexts and FlowFile Queue Operations
- [x] Phase 4: Canvas Component Coverage
- [x] Phase 5: Provenance
- [x] Phase 6: Registry Clients and Versioned Flows
- [x] Phase 7: Diagnostics and Administrative Reads
- [x] Phase 8: Guarded Administrative Mutations

## Completion Record

Update this table whenever a feature or phase is completed.

| Date | Feature/Phase | Endpoints Completed | Tests Added | Commit/PR | Notes |
|---|---|---|---|---|---|
| 2026-07-12 | Phase 0: Baseline and Safety Foundation | Processor run status; HTTP authentication refresh and safety infrastructure | None (deferred by owner) | | Production compilation completed for `nifi-copilot` and `nifi-web-api` |
| 2026-07-12 | Phase 1: HTTP Reliability and Shared Async Infrastructure | No new feature endpoints; GET retries, revision recovery, and async lifecycle infrastructure | None (deferred by owner) | | Both clients compile; registry-backed metrics remain partial |
| 2026-07-12 | Phase 2: Process Groups core lifecycle | Get, update, delete, component listings, child-group listing, and bulk scheduling | None (deferred by owner) | | Import/export remain assigned to Phase 6 |
| 2026-07-12 | Phase 2: Processor lifecycle and state | Diagnostics, state retrieval, state clearing, and thread termination | None (deferred by owner) | | Existing run-status support retained |
| 2026-07-12 | Phase 2: Connections, Controller Services, and Flow reads | Connection update/statistics; controller-service update/references; status, user, about, bulletins, and search | None (deferred by owner) | | Phase 2 complete; both production clients compile |
| 2026-07-12 | Phase 4: Canvas Component Coverage | Remote process groups, input/output ports, labels, funnels, and snippet create/move/copy/delete | None (deferred by owner) | | Phase 4 complete; both production clients compile |
| 2026-07-12 | Phase 3: Parameter Contexts and FlowFile Queue Operations | Parameter context list/direct/live update lifecycle; queue listing; FlowFile details/content; guarded queue drop; rollback tracking | None (deferred by owner) | | Phase 3 complete; both production clients compile |
| 2026-07-13 | Phase 5: Provenance | Provenance and lineage query submit/poll/delete lifecycles with cluster-node selection, result limits, timeout, and cleanup | None (deferred by owner) | | Phase 5 complete; both production clients compile |
| 2026-07-13 | Phase 6: Registry Clients and Versioned Flows | Registry-client CRUD and browsing; active version requests; version-control start/update/revert; process-group JSON import/export | None (deferred by owner) | | Phase 6 complete; both production clients compile |
| 2026-07-14 | Phase 7: Diagnostics and Administrative Reads | System diagnostics and JMX; cluster summary and nodes; counters; logout, authentication configuration, resources, policies, users, and groups | None (deferred by owner) | | Phase 7 complete; Phase 8 mutations remain deferred; both production clients compile |
| 2026-07-14 | Phase 8: Guarded Administrative Mutations | Confirmed cluster-node connect, disconnect, offload, and removal; confirmed single/all counter resets | None (deferred by owner) | | Phase 8 complete; authoritative resource authorization and audit logging retained; both production clients compile |
| 2026-07-14 | HTTP Infrastructure gap closure | Shared transport execution; bounded operation metrics; configurable timeouts; typed revision and async state handling | None (deferred by owner) | | HTTP Infrastructure complete; both production clients compile |
| 2026-07-14 | Processors: general configuration update | `GET /processors/{id}` and `PUT /processors/{id}` with whitelisted mutable fields (name, position, style, config, bundle); revision-retry; id enforced; state excluded | None (deferred by owner) | | Processors group complete; both production clients compile |
