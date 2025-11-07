from databricks_mcp.server.mcp_server import DatabricksMCPServer


def main() -> None:
    """Start the Databricks MCP server."""
    databricks_mcp_server = DatabricksMCPServer()
    databricks_mcp_server.run(transport="stdio")


if __name__ == "__main__":
    main()
