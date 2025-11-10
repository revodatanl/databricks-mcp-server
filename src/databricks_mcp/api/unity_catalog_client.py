import asyncio
import json
from collections import defaultdict
from pathlib import Path

import aiohttp

from databricks_mcp.api.utils import (
    JsonData,
    ToolCallResponse,
    format_toolcall_response,
    get_async_session,
    get_with_backoff,
    mask_api_response,
)

# Load mask from JSON file
_MASKS_DIR = Path(__file__).parent / "masks"

with (_MASKS_DIR / "get_table_details_mask.json").open() as f:
    get_table_details_mask = json.load(f)


async def _get_catalogs(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
) -> list[str]:
    """Get list of non-system catalogs.

    Args:
        session (aiohttp.ClientSession): The HTTP client session
        semaphore (asyncio.Semaphore): Semaphore for concurrency control
    Returns:
        list[str]: List of catalog names excluding system catalogs
    """
    data = await get_with_backoff(session, "unity-catalog/catalogs", semaphore)
    return [c["name"] for c in data.get("catalogs", []) if c["created_by"] != "System user"]


async def _get_schemas_in_catalog(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    catalog_name: str,
) -> list[str]:
    """Get non-system schemas in a catalog.

    Args:
        session (aiohttp.ClientSession): The HTTP client session
        semaphore (asyncio.Semaphore): Semaphore for concurrency control
        catalog_name (str): Name of the catalog to get schemas from
    Returns:
        list[str]: List of schema names excluding information_schema
    """
    endpoint = f"unity-catalog/schemas?catalog_name={catalog_name}"
    data = await get_with_backoff(session, endpoint, semaphore)
    return [s["name"] for s in data.get("schemas", []) if s["name"] != "information_schema"]


async def _get_tables_in_schema(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    catalog_name: str,
    schema_name: str,
) -> list[str]:
    """Get list of fully qualified table names in a schema.

    Args:
        session (aiohttp.ClientSession): The HTTP client session
        semaphore (asyncio.Semaphore): Semaphore for concurrency control
        catalog_name (str): Name of the catalog containing the schema
        schema_name (str): Name of the schema to get tables from

    Returns
    -------
        list[str]: List of fully qualified table names in format catalog.schema.table
    """
    endpoint = f"unity-catalog/tables?catalog_name={catalog_name}&schema_name={schema_name}"
    data = await get_with_backoff(session, endpoint, semaphore)
    return [f"{catalog_name}.{schema_name}.{t['name']}" for t in data.get("tables", [])]


async def list_all_tables() -> ToolCallResponse:
    """Get a hierarchical view of all tables organized by the three-level namespace: catalog, schema, table.

    Returns
    -------
        ToolCallResponse: Response object containing:
            - If successful: Dict mapping catalogs to schemas to table lists
              Format: {catalog: {schema: [table1, table2, ...]}}
            - If failed: Error information
    """
    try:
        async with get_async_session() as (session, semaphore):
            # Get catalogs
            catalogs = await _get_catalogs(session, semaphore)

            # Get schemas for each catalog
            schema_tasks = [_get_schemas_in_catalog(session, semaphore, catalog) for catalog in catalogs]
            schemas_per_catalog = await asyncio.gather(*schema_tasks)

            # Create catalog-schema pairs
            catalog_schema_pairs = [
                (catalog, schema) for catalog, schemas in zip(catalogs, schemas_per_catalog, strict=False) for schema in schemas
            ]

            # Get tables for each schema
            table_tasks = [_get_tables_in_schema(session, semaphore, catalog, schema) for catalog, schema in catalog_schema_pairs]
            tables_nested = await asyncio.gather(*table_tasks)

            # Organize results
            result: dict[str, dict[str, list[str]]] = defaultdict(
                lambda: defaultdict(list),
            )
            for table in (tbl for sublist in tables_nested for tbl in sublist):
                catalog_name, schema_name, table_name = table.split(".")
                result[catalog_name][schema_name].append(table_name)

            return format_toolcall_response(success=True, content=result)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_table_details(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    full_table_name: str,
) -> JsonData:
    """Get detailed information for a single table.

    Args:
        session (aiohttp.ClientSession): The HTTP client session
        semaphore (asyncio.Semaphore): Semaphore for concurrency control
        full_table_name (str): Fully qualified table name in format catalog.schema.table

    Returns
    -------
        JsonData: JSON response containing table details
    """
    endpoint = f"unity-catalog/tables/{full_table_name}"
    return await get_with_backoff(session, endpoint, semaphore)


async def get_tables_details(full_table_names: list[str]) -> ToolCallResponse:
    """Get detailed information for multiple tables.

    Args:
        full_table_names (list[str]): List of fully qualified table names in format catalog.schema.table

    Returns
    -------
        ToolCallResponse
    """
    try:
        async with get_async_session() as (session, semaphore):
            # Fetch details for all tables concurrently
            table_tasks = [_get_table_details(session, semaphore, table_name) for table_name in full_table_names]
            tables_data = await asyncio.gather(*table_tasks)
            masked_data = mask_api_response(tables_data, get_table_details_mask)
            return format_toolcall_response(success=True, content=masked_data)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)
