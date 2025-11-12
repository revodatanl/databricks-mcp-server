from mcp.server import FastMCP

from databricks_mcp.api import jobs_client, unity_catalog_client
from databricks_mcp.api.utils import ToolCallResponse


class DatabricksMCPServer(FastMCP):
    """MCP server for Databricks Unity Catalog and Jobs API."""

    def __init__(self) -> None:
        super().__init__("RevoData Databricks MCP")
        self._register_mcp_tools()

    def _register_mcp_tools(self) -> None:
        @self.tool(name="get-catalogs")
        async def get_catalogs() -> ToolCallResponse:
            """Retrieve a list of all available catalogs in the Databricks workspace"""
            return await unity_catalog_client.get_catalogs()

        @self.tool(name="get-schemas-in-catalogs")
        async def get_schemas_in_catalogs(catalog_names: list[str]) -> ToolCallResponse:
            """
            Retrieve a list of all available schemas in the given catalogs.

            Args:
                - catalog_names: List of catalog names to get schemas for
            """
            return await unity_catalog_client.get_schemas_in_catalogs(catalog_names)

        @self.tool(name="get-tables-in-catalogs-schemas")
        async def get_tables_in_catalogs_schemas(catalog_schemas: list[str]) -> ToolCallResponse:
            """
            Retrieve a list of all tables in the given catalogs and schemas.

            Args:
                - catalog_schemas: List of catalog.schema strings to get tables for
            """
            return await unity_catalog_client.get_tables_in_catalogs_schemas(catalog_schemas)

        @self.tool(name="get-tables-details")
        async def get_tables_details(full_table_names: list[str]) -> ToolCallResponse:
            """
            Retrieve detailed information for the given tables.

            Args:
                - full_table_names: List of full table names in the format catalog.schema.table to get details for
            """
            return await unity_catalog_client.get_tables_details(full_table_names)

        @self.tool(name="find-tables-by-name")
        async def find_tables_by_name(search_term: str, limit: int = 10, force_refresh: bool = False) -> ToolCallResponse:
            """
            Find tables by name in Unity Catalog using fuzzy matching with in-memory caching.

            Args:
                - search_term: The search term to find tables by
                - limit: The maximum number of results to return
                - force_refresh: If True, bypass the cache and fetch fresh data from the API
            """
            return await unity_catalog_client.find_tables_by_name(search_term, limit, force_refresh)

        @self.tool(name="get-jobs")
        async def get_jobs_in_workspace() -> ToolCallResponse:
            """Retrieve a list of all jobs in the workspace"""
            return await jobs_client.get_jobs()

        @self.tool(name="get-job-details")
        async def get_job_details(job_ids: list[int]) -> ToolCallResponse:
            """
            Get job details like settings and tasks by the job id.

            Args:
                - List of integer job IDs
            """
            return await jobs_client.get_job_details(job_ids)

        @self.tool(name="get-job-runs")
        async def get_job_runs(
            job_ids: list[int],
            n_recent: int = 1,
        ) -> ToolCallResponse:
            """
            Get recent job runs by job id.

            Args:
                - job_ids: List of job ids for which to get the runs
                - amount: Amount of runs to get per job, sorted by most recent
            """
            max_n_recent = 5
            if n_recent > max_n_recent:
                raise ValueError(f"n_recent cannot exceed {max_n_recent}")
            return await jobs_client.get_job_runs(job_ids, n_recent)


def main() -> None:
    """Start the Databricks MCP server."""
    server = DatabricksMCPServer()
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
