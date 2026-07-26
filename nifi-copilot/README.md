# NiFi Copilot

Java MCP service that powers the **GitHub Copilot for NiFi** chat sidebar.

## Architecture

```
NiFi UI (Angular)
  └── Copilot Chat sidebar
        │
        │  POST /nifi-api/copilot/api/chat
        ▼
nifi-copilot
  ├── GitHub Device Flow auth
  ├── AWS IAM Identity Center + Bedrock auth
  ├── NiFi client mode selector (internal facade or external HTTP API)
  └── SQLite session store
```

## Quick Start

### 1. Start NiFi

By default, `nifi-copilot` runs in-process inside NiFi Web API and uses NiFi's internal service facade.
Copilot endpoints are served from **`/nifi-api/copilot`** on the same NiFi host/port.

### 2. Start the NiFi frontend

```bash
cd nifi-frontend/src/main/frontend
npm install
nx serve nifi
```

### 3. Sign in from NiFi

Open NiFi and use the Copilot panel to sign in with GitHub (or AWS for Bedrock).

## Configuration

| Variable | Default | Description |
|---|---|---|
| `GITHUB_CLIENT_ID` | _(bundled)_ | Override only when self-hosting OAuth app |
| `NIFI_COPILOT_CLIENT_MODE` / `nifi.copilot.client.mode` | `auto` | `auto` prefers internal facade if available; `internal` requires internal facade; `external` forces NiFi REST API client |
| `nifi.copilot.capability.cache-ttl` | `PT15M` | Positive ISO-8601 duration for target NiFi processor and controller-service capability snapshots |
| `NIFI_BASE_URL` | from `src/main/resources/nifi.properties` (`https://localhost:8443`) | NiFi URL for external mode |
| `NIFI_USERNAME` | _(empty)_ | NiFi username for external mode |
| `NIFI_PASSWORD` | _(empty)_ | NiFi password for external mode |
| `NIFI_VERIFY_SSL` | `true` | SSL verification for external mode (`true`/`false`) |
| `NIFI_CONNECT_TIMEOUT_SECONDS` / `nifi.connect.timeout.seconds` | `30` | TCP connection timeout for external mode |
| `NIFI_REQUEST_TIMEOUT_SECONDS` / `nifi.request.timeout.seconds` | `60` | Per-request timeout for external mode |
| `NIFI_RETRY_MAX_ATTEMPTS` / `nifi.retry.max.attempts` | `3` | Max attempts for transient GET retries (429/502/503/504/transport) and revision-conflict retries; range 1–10 |
| `NIFI_RETRY_BASE_DELAY_MILLIS` / `nifi.retry.base.delay.millis` | `250` | Base exponential-backoff delay in milliseconds |
| `NIFI_RETRY_MAX_DELAY_MILLIS` / `nifi.retry.max.delay.millis` | `5000` | Maximum backoff delay in milliseconds; must not be less than base delay |
| `NIFI_ASYNC_POLL_INTERVAL_MILLIS` / `nifi.async.poll.interval.millis` | `1000` | Poll interval in milliseconds for async request lifecycle executor |
| `NIFI_ASYNC_TIMEOUT_SECONDS` / `nifi.async.timeout.seconds` | `120` | Timeout in seconds for async request lifecycle executor |

## Client metrics

`NiFiClientMetricsRegistry` records bounded per-operation call counts, outcome counts, total duration, and maximum duration for the selected HTTP or embedded client. Inject the registry and call `getSnapshots()` to retrieve immutable metrics without exposing request arguments, component IDs, or response bodies.

## Example prompts

- *"Create a flow that reads JSON files from `/data/in` and writes them to S3"*
- *"Build a Kafka consumer that routes records by field value"*
- *"Call an HTTP API every minute and log the response"*
