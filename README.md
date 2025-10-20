# Databricks Unity Catalog MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-available-blue.svg)](https://github.com/revodatanl/databricks-mcp-server/pkgs/container/databricks-mcp-server)

Access your Databricks workspace through Claude and other LLMs. Query Unity Catalog tables, inspect jobs, and retrieve detailed metadata—all through the Model Context Protocol.

Built on the [Databricks SDK](https://github.com/databricks/databricks-sdk-py) to provide read-only access to your workspace through the [Model Context Protocol](https://modelcontextprotocol.io/). Powered by [FastMCP](https://github.com/jlowin/fastmcp) with async/aiohttp for efficient parallel data retrieval.

Read more about our vision and use cases [here]().

---

## Table of Contents

- [Features](#features)
- [Available Tools](#available-tools)
  - [Unity Catalog](#unity-catalog)
  - [Jobs](#jobs)
- [Quick Start](#quick-start)
  - [Cursor](#cursor)
  - [Continue.dev](#continuedev)
- [Local Development](#local-development)
- [Dependencies](#dependencies)
- [License](#license)

---

## Features

### Capabilities

> **What you can do:**
>
> - Ask Claude to find tables in your Unity Catalog
> - Inspect job configurations and recent runs
> - Generate queries based on your schema

### Limitations

> **What you can't do:**
>
> - Modify tables or jobs (read-only by design)
> - Execute queries directly (retrieves metadata only)

---

## Available Tools

### Unity Catalog

| Tool | Description | Parameters |
|------|-------------|------------|
| `get-all-catalogs-schemas-tables` | List all tables across catalogs and schemas | None |
| `get-table-details` | Retrieve table descriptions, columns, and metadata | `full_table_names` (list of `catalog.schema.table`) |

### Jobs

| Tool | Description | Parameters |
|------|-------------|------------|
| `get-jobs` | List all workspace jobs with IDs and names | None |
| `get-job-details` | Get job settings, configurations, and tasks | `job_ids` (list of job IDs) |
| `get-job-runs` | Fetch recent run history with duration, parameters, and results | `job_ids` (list), `n_recent` (1-5, default: 1) |

---

## Quick Start

**Prerequisites:**

- Docker Desktop installed
- Databricks workspace access (host URL and access token)

### Installation

Choose your editor and follow the configuration steps:

<details>
<summary><strong>Cursor</strong></summary>

<br>

**Step 1:** Add the following configuration to `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "DATABRICKS_HOST",
        "-e",
        "DATABRICKS_TOKEN",
        "ghcr.io/revodatanl/databricks-mcp-server:latest"
      ],
      "env": {
        "DATABRICKS_HOST": "${env:DATABRICKS_HOST}",
        "DATABRICKS_TOKEN": "${env:DATABRICKS_TOKEN}"
      }
    }
  }
}
```

> **Note:** You can either use environment variable references (`${env:VARIABLE}`) or hardcode the values as strings directly in the configuration.

**Step 2:** Create a `.env` file in your project root with your credentials:

```env
DATABRICKS_HOST=your-workspace-url
DATABRICKS_TOKEN=your-access-token
```

**Step 3:** Restart Cursor to load the MCP server.

**Step 4:** Use the [cursor rules](rules/.cursor) to enhance your Databricks development workflow.

[Learn more about MCP in Cursor](https://cursor.com/docs/context/mcp)

</details>

<details>
<summary><strong>Continue.dev</strong></summary>

<br>

**Step 1:** Add the following configuration to `.continue/mcpServers/databricks-mcp.yaml`:

```yaml
name: databricks_mcp_server
version: 0.1.3
schema: v1
mcpServers:
  - name: databricks_mcp_server
    command: docker
    args:
      - run
      - -i
      - --rm
      - -e
      - DATABRICKS_HOST=${{ inputs.DATABRICKS_HOST }}
      - -e
      - DATABRICKS_TOKEN=${{ inputs.DATABRICKS_TOKEN }}
      - ghcr.io/revodatanl/databricks-mcp-server:latest
```

**Step 2:** Set your credentials either:

- On the Continue.dev website (recommended for security)
- Or in a `.env` file in your project root:

  ```env
  DATABRICKS_HOST=your-workspace-url
  DATABRICKS_TOKEN=your-access-token
  ```

**Step 3:** Restart your editor to load the MCP server.

**Step 4:** Use the [Continue.dev rules](rules/.cursor) to enhance your Databricks development workflow.

[Learn more about MCP in Continue.dev](https://docs.continue.dev/customize/deep-dives/mcp)

</details>

---

## Local Development

For contributors and developers who want to run the server locally:

### Setup

1. **Install uv** - Fast Python package installer
   Follow the [installation guide](https://docs.astral.sh/uv/)

2. **Clone the repository**

   ```bash
   git clone https://github.com/revodatanl/databricks-mcp-server.git
   cd databricks-mcp-server
   ```

3. **Install dependencies**

   ```bash
   uv sync
   ```

4. **Set environment variables**

   ```bash
   export DATABRICKS_HOST=your-workspace-url
   export DATABRICKS_TOKEN=your-access-token
   ```

5. **Run the server**

   ```bash
   uv run databricks-mcp
   ```

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| Python | 3.11+ | Runtime environment |
| databricks-sdk | >= 0.54.0 | Databricks API client |
| aiohttp | >= 3.12.14 | Async HTTP requests |
| async-lru | >= 2.0.5 | Async caching |
| mcp[cli] | >= 1.9.1 | Model Context Protocol framework |
| python-dotenv | >= 1.1.0 | Environment variable management (dev) |

---

## License

MIT License - see [LICENSE.md](LICENSE.md) for details.
