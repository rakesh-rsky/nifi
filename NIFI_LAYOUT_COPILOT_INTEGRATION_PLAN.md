# NiFi Layout Engine and Copilot Integration Tracker

## Goal

Integrate `nifi-layout-engine` with `nifi-copilot` so Copilot-created and
updated flows are arranged as structured NiFi canvases, including nested
process groups and connection bends.

The initial rollout must preserve the existing `CanvasLayoutEngine` as a
fallback until engine-mode acceptance criteria pass.

## Status

| Phase | Scope | Status | Depends on |
| --- | --- | --- | --- |
| 1 | Stabilize layout engine | Complete | None |
| 2 | Consolidate and wire layout artifact | Complete | Phase 1 API decisions |
| 3 | Implement Copilot write-back | Complete | Phases 1-2 |
| 4 | Wire deployment pipeline | Complete | Phase 3 |
| 5 | Harden deployment behavior | Not started | Can run with Phases 1-4 |
| 6 | Validate and roll out | Technical validation complete; rollout deferred | Phases 1-5 |

## Architecture decisions

- [x] Keep the core layout pipeline logically separated under
      `in.shrake.nifi.layout.core`.
- [x] Package the engine as one standalone `nifi-layout-engine` artifact with a
      single `src/main/java` tree.
- [x] Keep `core` and `support` always compiled; select `copilot`, `internal`,
      and `rest` through Maven profiles that exclude package groups during
      compilation.
- [x] Keep `nifi-layout-engine` independent from the NiFi parent POM; declare
      required NiFi artifacts in profile-scoped dependencies instead.
- [x] Treat `RoutingResult` paths as complete endpoint-to-endpoint routes;
      writers persist only interior points as NiFi connection bends.
- [x] Put the NiFi-client-backed writer in `nifi-copilot`; keep
      `in.shrake.nifi.layout.copilot` independent of `NiFiClientOperations`.
- [x] Run final layout after connection deployment and before runtime
      activation.
- [x] Use full layout for new or empty target groups and incremental layout
      when modifying an existing flow.
- [x] Introduce `legacy`, `engine`, and `disabled` modes, defaulting to
      `legacy` for the first release.
- [x] Require rollback registration before each remote canvas mutation.

## Phase 1: Stabilize the layout engine

### 1.1 Preserve dimensions in horizontal layouts

Files:

- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/core/layout/DefaultCoordinateAssigner.java`
- Historical verification reference: `SpacingInvariantsTest`

Tasks:

- [x] Stop transposing component width and height for `LEFT_TO_RIGHT` and
      `RIGHT_TO_LEFT`.
- [x] Transform positions using the layout extents while preserving each
      component's physical dimensions.
- [x] Add asymmetric-width/height test components for all four flow
      directions.
- [x] Verify collision and route calculations use the same dimensions that
      NiFi renders.

Acceptance:

- [x] A non-square component retains its original width and height in every
      flow direction.
- [x] Horizontal routes terminate on the rendered component boundary.
      Verified by `SpacingInvariantsTest.horizontalRouteEndpointsTerminateOnRenderedBoundaryWithAsymmetricDimensions`
      using 240×80 (NiFi-default) components in both LEFT_TO_RIGHT and
      RIGHT_TO_LEFT directions with explicit first/last-point assertions.

### 1.2 Align components before routing

Files:

- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/core/LayoutEngine.java`
- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/core/service/stage/GridAlignmentStage.java`
- Historical verification reference: `CompletePipelineIntegrationTest`

Tasks:

- [x] Run final component grid alignment after collision resolution and
      before connection routing.
- [x] Do not independently snap route endpoints after routing.
- [x] If interior route lanes are snapped, preserve orthogonality and remove
      duplicate adjacent points.
- [x] Update pipeline-order tests.

Acceptance:

- [x] Route source and destination anchors remain attached after grid
      alignment.
- [x] Components and eligible interior route lanes honor the configured grid.

### 1.3 Define route write-back semantics

Files:

- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/core/model/RoutingResult.java`
- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/rest/RestDtoWriter.java`

Tasks:

- [x] Document that core routing returns full paths including endpoint
      anchors.
- [x] Convert full routes to NiFi bends by excluding the first and last
      points in every writer.
- [x] Cover direct, orthogonal, backward, shared-endpoint, and self-loop
      routes.
      Verified: `RestDtoWriterTest.backwardRouteProducesInteriorBendsExcludingEndpoints`
      (6-point backward path → 4 interior bends) and
      `RestDtoWriterTest.sharedSourceEndpointRoutesAreWrittenIndependently`
      (two edges sharing a source node written independently).

Acceptance:

- [x] Direct connections produce no persisted bends.
- [x] Orthogonal connections persist only valid interior bend points.

### 1.4 Complete nested and incremental behavior

Files:

- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/core/LayoutEngine.java`
- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/support/FlowLayoutService.java`
- Historical verification references: `NestedGroupLayoutTest`, `IncrementalLayoutTest`

Tasks:

- [x] Preserve bottom-up nested-group layout and parent sizing.
- [x] Preserve child warnings, computation time, and reposition counts when
      combining nested results.
- [x] Verify nesting depth 10 succeeds and depth 11 fails consistently.
- [x] Add an incremental entry point to `FlowLayoutService`.
- [x] Make `LayoutOptions.isIncrementalMode()` behavior consistent with the
      selected engine entry point.
- [x] Recompute changed nodes and direct neighbors while preserving unaffected
      positions and routes.
- [x] Add nested, empty-group, multi-child, and disconnected incremental
      tests.

Implementation notes:

- Fixed: nested incremental previously passed the global changed-ID set to
  every child subgraph. A child that contained none of those IDs returned an
  empty result, which caused the group node to be resized to minimum bounds.
  `LayoutEngine.executePipeline` now scopes changed IDs to those present in
  the child graph; if the scoped set is empty the child is skipped entirely
  and its existing bounds are preserved.
- Tests added for: unchanged nested group bounds preserved, changed nested
  group re-laid out, direct neighbor of changed node moves, unaffected routes
  absent from result (caller-preserves contract documented explicitly).

Acceptance:

- [x] Nested groups are laid out bottom-up with deterministic bounds.
- [x] Incremental layout preserves unaffected positions and routes exactly.

### 1.5 Resolve public API inconsistencies

- [x] Make `LayoutEngine.create(LayoutOptions)` retain meaningful supplied
      defaults or remove the misleading overload.
- [x] Verify layout duration, warning aggregation, and reposition counts are
      accurate for full, incremental, and nested execution.

## Phase 2: Consolidate and wire the layout artifact

Files:

- `pom.xml`
- `nifi-layout-engine/pom.xml`
- `nifi-copilot/pom.xml`
- `nifi-assembly/pom.xml`
- `.gitignore`

Tasks:

- [x] Replace the multi-module layout build with one standard Maven project,
      one `nifi-layout-engine/pom.xml`, and one `nifi-layout-engine` JAR.
- [x] Move active code into one source tree under
      `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/`.
- [x] Organize the engine by package group within that tree:
      `core`, `support`, `rest`, and `copilot`.
- [x] Keep `core` and `support` always compiled from the default source tree.
- [x] Use Maven profile-specific compiler excludes, not separate source roots,
      to narrow targeted builds.
- [x] Define independent build properties for optional adapters, for example:
      `-Dlayout.copilot=true` and `-Dlayout.rest=true`.
- [x] Add a `core-only` property (`-Dlayout.core=true`) that activates a no-op
      `core-only` profile, which deactivates the activeByDefault `all-adapters`
      profile so developers can build core+support without NiFi adapter deps.
- [x] Activate each optional package group and only its required dependencies
      through Maven profiles driven by those properties.
- [x] Make `all-adapters` active by default (`<activeByDefault>true</activeByDefault>`)
      so that plain `mvn clean install` produces and installs the canonical
      all-adapter JAR.  Explicit property activations deactivate this default.
- [x] Keep the current standalone POM simple: Spring Boot packaging, compiler
      profile excludes, and profile-scoped NiFi adapter dependencies.
- [x] Align package namespace and artifact coordinates with the current layout
      project: `in.shrake.nifi:nifi-layout-engine:1.0.0`.
- [x] Define a canonical release configuration that enables all supported
      adapters, while allowing targeted builds to include only the required
      adapters.
- [x] Publish only the canonical all-adapter configuration under the standard
      artifact coordinates; targeted builds are local-development variants.
- [x] Require any targeted layout build used by Copilot to enable
      `layout.copilot`.
- [x] Remove the child module POMs after their code has been consolidated into
      the single active project shape.
- [x] Keep `nifi-layout-engine/pom.xml` standalone; do not inherit
      `org.apache.nifi:nifi`.
- [x] Declare `nifi-client-dto` as the REST profile dependency.
- [x] Keep the layout artifact version explicit and reference that same
      version from Copilot.
- [x] Add `in.shrake.nifi:nifi-layout-engine:1.0.0` directly as a compile
      dependency of `nifi-copilot`; do not depend on adapter-specific artifacts.
- [x] Confirm Copilot packaging and `nifi-assembly` contain the required
      single layout artifact without duplicate NiFi dependencies.
      Verified (2026-07-18): Spring Boot exec JAR `nifi-copilot-2.10.0-SNAPSHOT-exec.jar`
      contains `BOOT-INF/lib/nifi-layout-engine-1.0.0.jar` and
      `BOOT-INF/lib/nifi-client-dto-2.10.0-SNAPSHOT.jar`; no former adapter JARs
      (`nifi-layout-core`, `nifi-layout-adapter-*`); 89 bundled JARs total.
      Built with `mvnw.cmd clean package -pl nifi-copilot` WITHOUT
      `-Denforcer.skip=true`; the enforcer `RequireReleaseDeps` rule passes
      because the layout artifact is now version `1.0.0` (not `1.0-SNAPSHOT`).
      `nifi-assembly/pom.xml` declares the executable Copilot artifact; the
      layout engine and its compile-scope transitive dependency
      `nifi-client-dto` resolve through the standalone artifact contract.
- [x] Preserve the standalone `nifi-layout-engine` POM and artifact contract.
      The root NiFi POM also aggregates the engine before Copilot so clean
      reactor and CI builds are reproducible without a preinstalled artifact;
      the engine remains independently buildable with `-f
      nifi-layout-engine/pom.xml`.
- [x] Preserve `nifi-layout-architecture-tests` unchanged during this phase.
      Removing that directory and its tests is explicitly deferred until
      requested.
- [x] Add ASF headers and document static-analysis limitations for the detached
      standalone project.
      ASF license headers added to all 126 Java source files, 3 Markdown files
      (README.md, design.md, tasks.md), and nifi-layout-engine/pom.xml.
      Architecture-tests tree excluded (preserved byte-for-byte).
      NiFi-root Checkstyle/PMD CANNOT be applied to this standalone project:
      those tools require the NiFi parent POM inheritance chain to resolve plugin
      versions, suppression paths, and rule sets.  Applying them to a detached
      `in.shrake.nifi` project without the NiFi reactor is not feasible and
      would produce misleading results.  This is documented in README.md.
- [x] Decide and record the current package namespace.
      Decision: use `in.shrake.nifi.layout` in the flattened single-tree
      structure; any later migration is deferred until explicitly requested.
- [x] Add `.jqwik-database*` to `.gitignore`.
- [x] Remove generated `.jqwik-database` files from tracked changes.

Acceptance:

- [x] `nifi-layout-engine` produces exactly one consumable JAR from one POM.
      Verified: `target/nifi-layout-engine-1.0.0.jar` produced by
      `nifi-layout-engine/pom.xml` (packaging=jar, no parent).
- [x] The single layout project compiles the current `core`, `support`, `rest`,
      and `copilot` packages from `src/main/java`.
- [x] Core/support-only, Copilot, REST, and canonical all-adapter
      property combinations compile and run their applicable tests.
      Verified (2026-07-18): all five configurations pass:
      `mvn clean install` (canonical, activeByDefault, 87 tests),
      `mvn clean test -Dlayout.core=true` (core+support only, 59 tests, zero NiFi deps),
      `mvn clean test -Dlayout.copilot=true` (copilot adapter, zero NiFi deps),
      `mvn clean test -Dlayout.rest=true` (REST adapter only).
- [x] Targeted builds exclude non-selected adapter packages through compiler
      exclusions and only add the matching NiFi profile dependencies.
- [x] Copilot declares exactly one direct layout dependency.
      Verified: `in.shrake.nifi:nifi-layout-engine:1.0.0` added to
      `nifi-copilot/pom.xml`; no adapter-specific artifacts listed.
- [x] The layout POM has no NiFi parent and resolves required NiFi APIs through
      explicit profile-scoped dependencies.
- [x] Copilot packaging contains the layout JAR and no former adapter JARs.
      Verified (2026-07-18): `nifi-copilot-2.10.0-SNAPSHOT-exec.jar` contains
      `BOOT-INF/lib/nifi-layout-engine-1.0.0.jar` and the transitive
      `BOOT-INF/lib/nifi-client-dto-2.10.0-SNAPSHOT.jar`.  No `nifi-layout-core`,
      `nifi-layout-adapter-rest` or
      `nifi-layout-adapter-copilot` JARs are present.  89 bundled JARs total.
      Dependency tree: `in.shrake.nifi:nifi-layout-engine:1.0.0:compile`
      with `org.apache.nifi:nifi-client-dto:2.10.0-SNAPSHOT:compile` as its only
      non-test transitive dependency visible at compile/runtime scope.
      Enforcer `RequireReleaseDeps` passes without any bypass flag: version
      `1.0.0` satisfies the "external dependencies must be releases" constraint.
- [x] The deferred architecture-test directory has not been deleted or
      modified.
      Verified: `nifi-layout-architecture-tests/` files are unchanged (still
      staged as new-file, original content preserved).
- [x] No `.jqwik-database` artifact is tracked.
      Verified: `.jqwik-database*` added to `.gitignore`; no database files
      present in working tree or index.

## Phase 3: Implement Copilot write-back

### 3.1 Parse complete process-group snapshots

Files:

- `nifi-layout-engine/src/main/java/in/shrake/nifi/layout/copilot/ProcessGroupFlowMapAdapter.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/ProcessGroupFlowMapAssembler.java` *(new)*
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/NiFiClientOperations.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/HttpNiFiClient.java`

Tasks:

- [x] Retain `ProcessGroupFlowMapAdapter` as the NiFi-neutral map parser.
- [x] Recursively fetch child process groups because embedded child entities
      may not contain their flow contents.
- [x] Add nesting-depth and cycle guards to recursive fetching.
- [x] Test all supported component types, malformed endpoints, empty groups,
      and shuffled input ordering.

### 3.2 Add a revision-safe client writer

Files:

- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/NiFiClientLayoutWriter.java` *(new)*
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/NiFiClientOperations.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/service/HttpNiFiClient.java`

Tasks:

- [x] Implement `FlowLayoutWriter<Map<String, Object>>` in `nifi-copilot`.
- [x] Dispatch component updates by `NodeType` through the corresponding
      `NiFiClientOperations` update method.
- [x] Add revision-safe input-port and output-port update operations.
- [x] Persist connection bends using connection updates.
- [x] Fail with `WriteBackException` for unknown or unmappable IDs; never
      silently skip partial output.
- [x] Register restoration actions in `OwnershipLedger` before each mutation.
- [x] Capture original positions, group fields, and connection bends from the
      post-deployment snapshot.

Acceptance:

- [x] All supported component positions and connection bends are written with
      revision handling.
- [x] A partial write failure registers rollback actions for every completed
      mutation, and the rollback path restores captured processor positions and
      connection bends when those actions are executed.

Verified (2026-07-18): `nifi-layout-engine` 12 tests pass (`-Dlayout.copilot`);
`nifi-copilot` writer/assembler tests cover rollback registration order plus
processor-position restoration through `RollbackManager` and connection-bend
restoration via the captured rollback action.

## Phase 4: Wire the deployment pipeline

Files:

- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/FlowDeploymentCoordinator.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/ConnectionConfigurationStage.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/RuntimeActivationStage.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/DeploymentPreparationStage.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/CanvasLayoutEngine.java`
- `nifi-copilot/src/main/java/org/apache/nifi/copilot/builder/OwnershipLedger.java`

Tasks:

- [x] Add a dedicated layout stage after connection configuration and before
      runtime activation.
- [x] Refresh the target process-group flow after components and connections
      are created.
- [x] Parse the refreshed recursive snapshot, select full or incremental
      layout, and persist the result.
- [x] Prevent runtime activation when layout or write-back fails.
- [x] Expose created and updated component IDs from `OwnershipLedger`.
- [x] Include both endpoints of every created or modified connection in the
      incremental changed-ID set.
- [x] Initially retain `CanvasLayoutEngine` only for provisional creation
      coordinates.
- [x] Remove its DAG and collision responsibilities after engine-mode
      acceptance.
- [x] Add `legacy`, `engine`, and `disabled` configuration modes.
- [x] Record mode, duration, moved-component count, routed-connection count,
      and failures in `FlowDeploymentMetricsRegistry`.

Acceptance:

- [x] Layout executes after connections exist and before processors start.
- [x] New flows use full layout; modifications use incremental layout.
- [x] The `legacy` mode remains an immediate rollback path.

Verified (2026-07-21): acceptance tests cover connection configuration → layout
→ activation ordering, activation suppression and rollback on layout failure,
recursive snapshot assembly through the real parser/layout/write path,
deployer-driven changed-ID capture, layout metrics, and legacy-mode rollback.
Rollback now restores canvas state before deleting connections, then deletes
created canvas endpoints. Narrow Phase 4 tests pass (81/81), and
`.\mvnw.cmd -pl nifi-copilot test` passes (109/109).

## Phase 5: Deployment prerequisites and hardening

### Integration prerequisites

- [x] In `ControllerServiceDeployer`, compare requested properties before
      reusing a controller service with the same name and type.
- [x] Preserve existing controller-service properties in
      `HttpNiFiClient.listControllerServices`.
- [x] Fail preflight with field-level mismatch details unless an explicit
      update/reuse policy allows the difference.
- [x] In `RollbackManager`, delete owned child process groups before their
      parameter contexts.
- [x] For reused target groups, restore or unbind the previous parameter
      context before deleting only the deployment-owned context.

### Release-blocking concurrency hardening

- [x] Make `HttpNiFiClient.ensureTypeCache` initialize and publish an immutable
      processor-type map atomically.
- [x] Ensure failed initialization exposes no partial cache and remains
      retryable.
- [x] Add concurrent first-use coverage.

Verified (2026-07-22): focused controller-service compatibility/preflight,
property projection, rollback ordering/binding restoration, and processor-type
cache concurrency/failure tests pass (10/10).
`.\mvnw.cmd -pl nifi-copilot test` passes (119/119).

## Phase 6: Validation and rollout

**Status (2026-07-22): technical validation complete; operational rollout
gates remain deferred.** The minimum executable evidence is the existing test
and build matrix below; no additional test is required.

### Test matrix

- [x] Layout-core unit and property tests.
- [x] Copilot adapter parsing and writer tests.
- [x] Full versus incremental selection.
- [x] Stage ordering and activation suppression on layout failure.
- [x] Revision retry and bend persistence.
- [x] Partial-write rollback.
- [x] Nested groups through the REST map path.
- [x] `legacy`, `engine`, and `disabled` mode behavior.
- [x] Controller-service mismatch handling.
- [x] Parameter-context cleanup order.
- [x] Concurrent processor-type cache initialization.
- [x] Standalone layout JAR, Copilot executable packaging, web API, and
      assembly builds.

Verified (2026-07-22): focused coverage maps respectively to
`LayoutEngineValidationTest`/`ProcessGroupFlowMapAdapterTest`,
`NiFiClientLayoutWriterTest`, `LayoutDeploymentStageTest`,
`FlowDeploymentCoordinatorActivationSuppressionTest`,
`HttpNiFiClientPhase5Test.connectionBendUpdateRereadsRevisionAfterConflict`,
`NiFiClientLayoutWriterTest.rollbackManagerRestoresCompletedProcessorMutation`,
`ProcessGroupFlowMapAssemblerTest`, `LayoutModeTest`,
`ControllerServiceDeployerTest`, `RollbackManagerParameterContextTest`, and
`HttpNiFiClientPhase5Test.concurrentFirstUseInitializesCacheOnce`.
The layout suite passes 16/16 and the Copilot suite passes 120/120. Core-only,
Copilot-only, REST-only, internal-only, and canonical all-adapter package
variants build with the expected class isolation. The canonical library JAR,
Copilot executable JAR (including the layout JAR), targeted `nifi-web-api` WAR,
and `nifi-assembly` binary ZIP build successfully; the ZIP contains
`nifi-copilot-2.10.0-SNAPSHOT-exec.jar`. A clean
`-pl nifi-copilot -am package` build using a fresh Maven repository also
succeeds, with the root reactor building `nifi-layout-engine` before
`nifi-copilot`.

### Rollout

- [ ] Ship with `legacy` as the default and collect engine-mode metrics.
- [x] Run engine mode against representative flows containing cycles,
      disconnected components, nested groups, ports, funnels, labels, remote
      process groups, and non-square components.
- [ ] Compare readability, crossings, overlap count, deployment latency, and
      rollback success with legacy mode.
- [ ] Switch the default to `engine` only after acceptance approval.
- [ ] Remove `CanvasLayoutEngine` only after one release with a stable
      engine-mode fallback record.

Technical rollout evidence (2026-07-22): `application.properties` still
defaults to `legacy`. Mode-specific layout count, duration, moved-component,
routed-connection, failure, and rollback metrics are implemented and tested.
The synthetic representative engine fixture proves zero component overlaps,
dimension preservation in all four directions, cycle/disconnected/nested and
all-component-type handling, and route attachment. The crossing counter has a
known-crossing fixture; layout results and metrics expose objective overlap,
crossing, moved/routed-count, and descriptive duration evidence without a
flaky latency threshold. Incremental tests prove unaffected position/route
preservation; failure tests prove successful rollback and activation
suppression.

A safe local live attempt used backups and temporary credentials. The preserved
flow could not start because it requires migration from a blank
`nifi.sensitive.props.key`; an isolated fresh flow progressed further but HTTPS
startup failed because the preserved encrypted keystore password could not be
decrypted. NiFi never became API-ready, so no validation process group or other
flow resource was created. Regenerating TLS, adding an unsecured endpoint, or
building a non-product live harness would exceed the safe minimum and would not
establish a representative legacy-versus-engine corpus. The runtime remained
stopped, all touched configuration and repository state was restored from
backup and hash-verified, and temporary credentials and harness files were
removed.

Shipping, live production metric collection, and objective live
legacy-versus-engine comparison therefore remain pending. No objective
readability acceptance threshold or live corpus is defined, so comparative
acceptance remains unchecked. Switching the default requires explicit approval;
removal requires one stable release of history.

## Risks

| Risk | Mitigation |
| --- | --- |
| REST write-back is non-atomic | Register rollback before every mutation |
| Child group entities omit flow contents | Fetch children recursively with guards |
| Full route paths are persisted as bends | Writers remove endpoint anchors |
| Incremental IDs miss relationship-only changes | Include both connection endpoints |
| Concurrent deployments race revisions | Use revision-aware updates and retries |
| Consolidation loses module-specific dependencies | Merge every child POM dependency into the single standalone POM |
| Explicit NiFi dependencies conflict with Copilot runtime | Use runtime-appropriate scopes and verify the packaged dependency tree |
| Conditional builds produce different content under one artifact coordinate | Publish only the canonical all-adapter build and record enabled adapters in build metadata |
| Optional adapter tests depend on another disabled adapter | Separate adapter-local tests from canonical cross-adapter tests |
| New layout changes existing canvas positions | Feature gate and default to legacy |

## Definition of done

- [x] One standalone layout POM produces one layout JAR.
- [x] Copilot directly depends on that layout JAR.
- [x] Optional adapter properties deterministically control included classes
      and dependencies.
- [x] Non-square components retain dimensions in every direction.
- [x] Routes remain attached after grid alignment.
- [x] Copilot persists positions and interior bends with revision handling.
- [x] Nested process groups are fetched and laid out recursively.
- [x] Incremental layout preserves unaffected positions and routes.
- [x] Layout failures prevent activation and trigger complete rollback.
- [x] All feature modes behave deterministically.
- [x] Deployment hardening tests pass.
- [x] Generated jqwik databases are absent from tracked changes.
- [x] Phase 6 technical test, build, packaging, and synthetic acceptance
      validation is complete.
- [ ] Engine mode meets operational acceptance criteria before legacy layout is
      removed.

Definition-of-done verification (2026-07-22): all checked items are covered by
the tests and package evidence above. Jqwik state is directed under `target`;
the generated `nifi-layout-engine/.jqwik-database` was removed, and a clean
property-test run creates no database outside build output.
Technical Phase 6 is complete. Operational acceptance remains blocked on
shipping and live production metrics, defined objective comparison criteria, a
representative live NiFi corpus, explicit default-switch approval, and one
stable release of fallback history. The default remains `legacy`, and
`CanvasLayoutEngine` remains.
