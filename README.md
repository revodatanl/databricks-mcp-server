# Databricks MCP Server

This MCP server provides LLMs a set of read-only tools for interacting with Databricks workspaces through the MCP protocol. It enables LLMs to retrieve information about assets within your Databricks workspace and use this to generate answers.


## Features

- **Unity Catalog Integration**
  - List all tables across catalogs and schemas in a Databricks workspace
  - Retrieve detailed table information including descriptions and column definitions
- **Jobs Management**
  - List all jobs in the workspace with detailed information
- **MCP Compliance**
  - Implements the Model Control Protocol for standardized interaction
  - Built on top of FastMCP for robust server implementation
  - Uses Async and AioHttp for efficient prallel information retrieval


## Prerequisites

- Python 3.11 or higher
- Databricks workspace access with appropriate permissions
- Databricks variables (host & secret)


### Available Tools

The server exposes the following MCP tools:

1. **get-all-catalogs-schemas-tables**
   - Returns a comprehensive list of all tables available in all Unity Catalogs assigned to your Databricks Workspace
   - No parameters required

2. **get-table-details**
   - Retrieves detailed information about specific tables
   - Parameters:
     - `full_table_names`: List of fully qualified table names (catalog.schema.table)

3. **get-jobs**
   - Lists all jobs in the workspace with basic information like id and name
   - No parameters required

4. **get-job-details**
   - Retrieve job details like settings, configurations and tasks
   - Parameters:
     - `job_ids`: List of job ids

5. **get-job-runs**
   - Get recent job runs and detailed information like duration, parameters, and result
   - Parameters:
     - `job_ids`: List of job ids
     - `n_recent`: Amount of runs to get, ordered by most recent. Default is 1 and is capped at a maximum of 5


## Dependencies

- aiohttp >= 3.12.14
- async-lru >= 2.0.5
- mcp[cli] >= 1.9.1

## Environment Setup

### Local Usage

Export environment variables using cli. 
```
EXPORT DATABRICKS_HOST=your-workspace-url
EXPORT DATABRICKS_TOKEN=your-access-token 
```

### Usage with Continue

Create a `.env` file in your .continue folder root.

```env
DATABRICKS_HOST=your-workspace-url
DATABRICKS_TOKEN=your-access-token
```

## Dependencies

- databricks-sdk>=0.54.0
- async-lru>=2.0.5
- python-dotenv>=1.1.0 (for development)

## Development

To set up the development environment:

1. Install uv
2. Clone the repository
3. Navigate to the project root
4. Install dependencies using ```uv sync```

## License

MIT


