# Processor Capability Validation Implementation

## Status

Completed. NiFi Copilot now discovers the connected NiFi instance's capabilities, constrains flow generation with that metadata, validates and normalizes the complete generated flow, and only then permits canvas mutations.

## Problem Addressed

The previous deployment path trusted LLM-generated processor and controller-service definitions. This allowed unsupported components such as an invented `MQTT Connection Service` to reach NiFi and fail during deployment.

The implementation replaces that behavior with a target-aware validation boundary:

```text
User prompt
  -> target capability discovery
  -> capability-aware LLM generation
  -> flow model
  -> validation and normalization
  -> validated flow plan
  -> NiFi deployment
```

Invalid flows are rejected before controller-service creation, deletion, process-group creation, or any other mutating REST request.

## Implemented Changes

### 1. Dynamic Capability Registry

Added an extensible capability model under:

`nifi-copilot/src/main/java/org/apache/nifi/copilot/capability/`

The registry describes the target NiFi instance's:

- Processor and controller-service types
- Bundle coordinates
- Property descriptors and allowable values
- Required and sensitive properties
- Supported relationships
- Controller-service API requirements
- Scheduling strategies

Capabilities are discovered from the connected NiFi instance instead of being maintained as a static processor list. Snapshots are immutable, cached per client identity, refreshed using a configurable TTL, and atomically published.

If discovery or refresh fails, no incomplete snapshot is published. An existing complete snapshot remains available after a failed refresh.

### 2. NiFi Capability Discovery

Extended both external and embedded clients:

- `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/HttpNiFiClient.java`
- `nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api/src/main/java/org/apache/nifi/web/api/InternalNiFiClient.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/NiFiClientOperations.java`

Processor and controller-service definitions are loaded from the connected target. Duplicate types across bundles are resolved deterministically, with stable releases preferred over prerelease versions such as `SNAPSHOT`.

Capability discovery fails closed. Copilot does not generate or deploy a flow when authoritative target metadata is unavailable.

### 3. Flow Validation and Normalization

Added `FlowSpecificationValidator` and supporting models to validate:

- Processor types
- Controller-service types
- Required and unsupported properties
- Property allowable values
- Controller-service references and API compatibility
- Processor relationships
- Scheduling strategies
- Duplicate or ambiguous service names

The validator also normalizes accepted aliases into deployment-safe values:

- Processor and service aliases to discovered fully qualified class names
- Property display names to internal descriptor names
- Allowable-value display names to internal values
- Controller-service names to service specification IDs
- Missing processor relationships to the effective `success` relationship

Sensitive property values are never included in validation messages.

### 4. Fail-Fast Deployment Boundary

Added a mutation-free preparation stage in:

`nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/FlowBuilder.java`

`prepareFlow()` creates an immutable `ValidatedFlowPlan`. Deployment accepts this validated plan rather than reinterpreting unchecked LLM output.

`CopilotController` now prevalidates the entire generated response before performing:

- Controller-service actions
- Canvas deletions
- Process-group creation
- Processor deployment
- Any other mutating NiFi REST call

Validation failures return structured issues containing the component, property where applicable, reason, and suggested correction.

### 5. Controller-Service Protection

Unsupported services are rejected with `UnsupportedControllerServiceException` before deployment.

For MQTT flows, `ConsumeMQTT` is validated against its actual property descriptors. Broker URI, topic, credentials, and QoS remain processor properties; an invented generic MQTT connection service is not accepted.

Controller services may only be created when the target NiFi instance exposes the requested type and a processor property supports the service API.

### 6. Capability-Aware LLM Prompt

Updated:

`nifi-copilot/src/main/java/org/apache/nifi/copilot/llm/LlmClient.java`

Both GitHub Models and Bedrock prompts now receive bounded, deterministic target capability context and explicitly require the model to:

- Never invent processors, controller services, or properties
- Use controller services only through actual processor property descriptors
- Omit uncertain services instead of inventing generic connection services
- Use `MergeRecord` with a record reader and writer for JSON-array batching
- Use `DistributeLoad` followed by multiple `InvokeHTTP` processors for parallel HTTP calls

Reusable guidance was added for MQTT JSON batching, Kafka batching, and database record flows.

### 7. Scheduling and Error Handling

`ProcessorDeployer` now applies validated scheduling configuration and stops on the first unexpected mutation failure instead of continuing with a partially deployed flow.

`CapabilityDiscoveryException` extends `IllegalStateException` to preserve the existing client contract while providing a dedicated capability-discovery failure type.

### 8. Configuration and Documentation

Added capability snapshot TTL configuration to:

- `nifi-copilot/src/main/resources/application.properties`
- `nifi-copilot/README.md`

TTL expiration also invalidates dependent processor bundle caches so refreshed capability metadata and deployment bundle selection remain consistent.

## Important Classes

| Class | Responsibility |
|---|---|
| `CapabilityRegistry` | Discovers, caches, refreshes, and publishes target capabilities |
| `CapabilityRegistryManager` | Manages registries by NiFi client identity |
| `CapabilityDefinitionParser` | Converts NiFi definition responses into capability models |
| `FlowSpecificationValidator` | Validates and normalizes generated flow specifications |
| `ValidatedFlowPlan` | Carries an immutable, deployment-ready specification |
| `CapabilityPromptRenderer` | Renders target metadata and flow-pattern guidance for the LLM |
| `CapabilityTypeSelector` | Selects the preferred bundle for duplicate component types |
| `ValidationReport` | Collects structured validation issues |
| `UnsupportedControllerServiceException` | Reports unsupported controller services before mutation |

## MQTT Batch Flow Guidance

The recommended structure for the original MQTT requirement is:

```text
ConsumeMQTT
  -> ValidateRecord
  -> MergeRecord (maximum 1000 records)
  -> DistributeLoad
  -> InvokeHTTP x5
  -> separate success and failure loggers
```

`JsonTreeReader` and `JsonRecordSetWriter` should be referenced through the record-oriented processor property descriptors. `MergeContent` is not recommended when the HTTP endpoint expects a JSON array.

The target endpoint must not use Copilot's own listening port. If Copilot runs on `localhost:8080`, the measurement API must use a different reachable port or Copilot must be reconfigured.

## Review Fixes Included

The review cycle also corrected:

- Accepted property aliases not being normalized
- Missing relationships bypassing effective `success` validation
- Duplicate fully qualified component names across bundle versions
- Stale deployment bundle caches after TTL refresh
- Prerelease versions being preferred over stable releases
- Capability discovery breaking an existing exception contract

## Verification

- 186 NiFi Copilot tests passed
- 94 NiFi layout-engine tests passed in the reactor
- Embedded `nifi-web-api` compilation passed
- No whitespace errors were reported by `git diff --check`
- Final code review reported no substantive findings

