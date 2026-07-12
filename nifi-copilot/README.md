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
| `NIFI_BASE_URL` | from `src/main/resources/nifi.properties` (`https://localhost:8443`) | NiFi URL for external mode |
| `NIFI_USERNAME` | _(empty)_ | NiFi username for external mode |
| `NIFI_PASSWORD` | _(empty)_ | NiFi password for external mode |
| `NIFI_VERIFY_SSL` | `false` | SSL verification for external mode (`true`/`false`) |

## Example prompts

- *"Create a flow that reads JSON files from `/data/in` and writes them to S3"*
- *"Build a Kafka consumer that routes records by field value"*
- *"Call an HTTP API every minute and log the response"*
