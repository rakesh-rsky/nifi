<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# NiFi Layout Engine

Standalone Spring Boot packaged layout engine for arranging Apache NiFi flows
using a Sugiyama-style hierarchical layout pipeline.

## Coordinates

```text
groupId:    in.shrake.nifi
artifactId: nifi-layout-engine
version:    1.0.0
```

## Application entrypoint

```text
in.shrake.nifi.layout.NiFiLayoutApplication
```

The project is packaged as a single Maven module with one source tree under
`src/main/java`.

## Current source structure

| Path | Purpose |
| --- | --- |
| `src/main/java/in/shrake/nifi/layout/NiFiLayoutApplication.java` | Spring Boot entrypoint |
| `src/main/java/in/shrake/nifi/layout/core/**` | Core graph, layout, routing, collision, model, spacing, and pipeline code |
| `src/main/java/in/shrake/nifi/layout/support/**` | Shared adapter orchestration, canonical model, and writer helpers |
| `src/main/java/in/shrake/nifi/layout/rest/**` | REST DTO adapter and writer |
| `src/main/java/in/shrake/nifi/layout/copilot/**` | Copilot flow-map adapter |
| `src/main/resources/application.properties` | Spring Boot configuration |

## Build profiles

The code lives in one source tree. Optional adapter builds are controlled by
Maven profiles that exclude package groups during compilation.

| Build mode | Command | Included packages |
| --- | --- | --- |
| Default canonical | `mvn clean package` | `core`, `support`, `rest`, `copilot` |
| Core only | `mvn clean package -Dlayout.core=true` | `core`, `support` |
| Copilot only | `mvn clean package -Dlayout.copilot=true` | `core`, `support`, `copilot` |
| REST only | `mvn clean package -Dlayout.rest=true` | `core`, `support`, `rest` |

`all-adapters` is active by default. Explicit `layout.core`, `layout.copilot`,
or `layout.rest` activation switches the build to the
requested package set.

## NiFi adapter dependencies

| Profile | Added dependencies |
| --- | --- |
| `layout.rest` | `org.apache.nifi:nifi-client-dto` |
| default `all-adapters` | `org.apache.nifi:nifi-client-dto` |

`layout.core` and `layout.copilot` do not add NiFi adapter dependencies.

## Consuming from `nifi-copilot`

`nifi-copilot` currently depends on:

```xml
<dependency>
    <groupId>in.shrake.nifi</groupId>
    <artifactId>nifi-layout-engine</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Notes

- This project is standalone and does not inherit the NiFi parent POM.
- The structure has been flattened from earlier multi-module and multi-source-root
  layouts into a single `src/main/java` tree with package-based separation.
