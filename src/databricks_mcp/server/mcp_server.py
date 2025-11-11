from mcp.server import FastMCP

from databricks_mcp.api import jobs_client, unity_catalog_client
from databricks_mcp.api.utils import ToolCallResponse


class DatabricksMCPServer(FastMCP):
    """MCP server for Databricks Unity Catalog and Jobs API."""

    def __init__(self) -> None:
        super().__init__("RevoData Databricks MCP")
        self._register_mcp_tools()

    def _register_mcp_tools(self) -> None:
        # @self.tool(name="get-all-catalogs-schemas-tables")
        # async def get_all_tables_in_workspace() -> ToolCallResponse:
        #     """Get a hierarchical view of all tables organized by the three-level namespace: catalog, schema, table.

        #     Returns object containing Dict mapping in format {catalog: {schema: [table1, table2, ...]}}
        #     """
        #     return await unity_catalog_client.list_all_tables()

        # @self.tool(name="get-table-details")
        # async def get_table_details_by_full_tablename(
        #     full_table_names: list[str],
        # ) -> ToolCallResponse:
        #     """Get table details like description and columns from the full three-level namespace catalog.schema.tablename.

        #     Args:
        #         - full_table_names: List of tables to get details for
        #     """
        #     return await unity_catalog_client.get_tables_details(full_table_names)

        @self.tool(name="get-catalogs")
        async def get_catalogs() -> ToolCallResponse:
            """Retrieve a list of all catalogs in the workspace"""
            return await unity_catalog_client.get_catalogs()

        @self.tool(name="get-schemas-in-catalogs")
        async def get_schemas_in_catalogs(catalog_names: list[str]) -> ToolCallResponse:
            """Retrieve a list of all schemas in the given catalogs"""
            return await unity_catalog_client.get_schemas_in_catalogs(catalog_names)

        @self.tool(name="get-tables-in-catalogs-schemas")
        async def get_tables_in_catalogs_schemas(catalog_schemas: list[str]) -> ToolCallResponse:
            """Retrieve a list of all tables in the given catalogs and schemas"""
            return await unity_catalog_client.get_tables_in_catalogs_schemas(catalog_schemas)

        @self.tool(name="get-tables-details")
        async def get_tables_details(full_table_names: list[str]) -> ToolCallResponse:
            """Retrieve detailed information for the given tables"""
            return await unity_catalog_client.get_tables_details(full_table_names)

        @self.tool(name="get-jobs")
        async def get_jobs_in_workspace() -> ToolCallResponse:
            """Retrieve a list of all jobs in the workspace"""
            return await jobs_client.get_jobs()

        @self.tool(name="get-job-details")
        async def get_job_details(job_ids: list[int]) -> ToolCallResponse:
            """Get job details like settings and tasks by the job id.

            Args:
                - List of integer job IDs
            """
            return await jobs_client.get_job_details(job_ids)

        @self.tool(name="get-job-runs")
        async def get_job_runs(
            job_ids: list[int],
            n_recent: int = 1,
        ) -> ToolCallResponse:
            """Get recent job runs by job id.

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
