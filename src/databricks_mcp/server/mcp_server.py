from mcp.server import FastMCP

import databricks_mcp.api.unity_catalog_client as unity_catalog_client
import databricks_mcp.api.jobs_client as jobs_client


class DatabricksMCPServer(FastMCP):
    def __init__(self):
        super().__init__("RevoData Databricks MCP")
        self._register_mcp_tools()

    def _register_mcp_tools(self):
        @self.tool(name="get-all-catalogs-schemas-tables")
        async def get_all_tables_in_workspace():
            """Get a hierarchical view of all tables organized by the three-level namespace: catalog, schema, table
            Returns object containing Dict mapping in format {catalog: {schema: [table1, table2, ...]}}
            """
            return await unity_catalog_client.list_all_tables()

        @self.tool(name="get-table-details")
        async def get_table_details_by_full_tablename(full_table_names: list[str]):
            """Get table details like description and columns from the full three-level namespace catalog.schema.tablename
            Takes:
                - full_table_names: List of tables to get details for
            """
            return await unity_catalog_client.get_tables_details(full_table_names)

        @self.tool(name="get-jobs")
        async def get_jobs_in_workspace():
            """Retrieve a list of all jobs in the workspace"""
            return await jobs_client.get_jobs()

        @self.tool(name="get-job-details")
        async def get_job_details(job_ids: list[int]):
            """Get job details like settings and tasks by the job id
            Takes:
                - List of integer job IDs
            """
            return await jobs_client.get_job_details(job_ids)

        @self.tool(name="get-job-runs")
        async def get_job_runs(job_ids: list[int], amount: int = 1):
            """Get recent job runs by job id
            Takes:
                - job_ids: List of job ids for which to get the runs
                - amount: Amount of runs to get per job, sorted by most recent
            """
            return await jobs_client.get_job_runs(job_ids, amount)


def main():
    server = DatabricksMCPServer()
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
