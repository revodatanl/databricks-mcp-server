import asyncio
import json
from pathlib import Path

import aiohttp

from databricks_mcp.api.utils import (
    ToolCallResponse,
    format_toolcall_response,
    get_async_session,
    get_with_backoff,
    mask_api_response,
)

# Load masks from JSON files
_MASKS_DIR = Path(__file__).parent / "masks"

with (_MASKS_DIR / "catalog_mask.json").open() as f:
    catalog_mask = json.load(f)

with (_MASKS_DIR / "schemas_mask.json").open() as f:
    schemas_mask = json.load(f)

with (_MASKS_DIR / "tables_mask.json").open() as f:
    tables_mask = json.load(f)

with (_MASKS_DIR / "table_details_mask.json").open() as f:
    table_details_mask = json.load(f)


async def _get_catalogs_from_endpoint(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> list[str]:
    data = await get_with_backoff(session, "unity-catalog/catalogs", semaphore)
    masked_data = mask_api_response(data, catalog_mask)["catalogs"]
    return masked_data


async def get_catalogs() -> ToolCallResponse:
    """
    Retrieve a list of all catalogs in the Unity Catalog workspace.

    Returns
    -------
    ToolCallResponse
        A response object containing the list of catalog names if successful,
        or error information if the operation failed.
    """
    try:
        async with get_async_session() as (session, semaphore):
            data = await _get_catalogs_from_endpoint(session, semaphore)
            return format_toolcall_response(success=True, content=data)
    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_schemas_in_catalog_from_endpoint(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    catalog_name: str,
) -> list[str]:
    data = await get_with_backoff(session, f"unity-catalog/schemas?catalog_name={catalog_name}", semaphore)
    masked_data = mask_api_response(data, schemas_mask)["schemas"]
    return masked_data


async def get_schemas_in_catalogs(catalog_names: list[str]) -> ToolCallResponse:
    """
    Retrieve all schemas from the specified catalogs.

    Fetches schema information from multiple catalogs in parallel and returns
    a flattened list of all schemas across the provided catalogs.

    Parameters
    ----------
    catalog_names : list[str]
        A list of catalog names to retrieve schemas from.

    Returns
    -------
    ToolCallResponse
        A response object containing the combined list of all schemas from the
        specified catalogs if successful, or error information if the operation failed.
    """
    try:
        async with get_async_session() as (session, semaphore):
            schema_tasks = [_get_schemas_in_catalog_from_endpoint(session, semaphore, catalog) for catalog in catalog_names]
            schemas_per_catalog = await asyncio.gather(*schema_tasks)
            all_schemas = [schema for catalog_schemas in schemas_per_catalog for schema in catalog_schemas]
            return format_toolcall_response(success=True, content=all_schemas)
    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_tables_in_scema_from_endpoint(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    catalog_schema: str,
) -> list[str]:
    catalog, schema = catalog_schema.split(".")
    data = await get_with_backoff(session, f"unity-catalog/tables?catalog_name={catalog}&schema_name={schema}", semaphore)
    masked_data = mask_api_response(data, tables_mask).get("tables", [])
    return masked_data


async def get_tables_in_catalogs_schemas(catalog_schemas: list[str]) -> ToolCallResponse:
    """
    Retrieve all tables from the specified catalog and schema combinations.

    Fetches table information from multiple catalog.schema pairs in parallel and
    returns a flattened list of all tables.

    Parameters
    ----------
    catalog_schemas : list[str]
        A list of catalog.schema strings (e.g., ["catalog1.schema1",
        "catalog2.schema2"]) to retrieve tables from.

    Returns
    -------
    ToolCallResponse
        A response object containing the combined list of all tables from the
        specified catalog.schema pairs if successful, or error information if
        the operation failed.
    """
    try:
        async with get_async_session() as (session, semaphore):
            table_tasks = [
                _get_tables_in_scema_from_endpoint(session, semaphore, catalog_schema) for catalog_schema in catalog_schemas
            ]
            tables_per_catalog_schema = await asyncio.gather(*table_tasks)
            all_tables = [table for catalog_tables in tables_per_catalog_schema for table in catalog_tables]
            return format_toolcall_response(success=True, content=all_tables)
    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_table_details_from_endpoint(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    full_table_name: str,
) -> list[str]:
    data = await get_with_backoff(session, f"unity-catalog/tables/{full_table_name}", semaphore)
    masked_data = mask_api_response(data, table_details_mask)
    return masked_data


async def get_tables_details(full_table_names: list[str]) -> ToolCallResponse:
    """
    Retrieve detailed information for the specified tables.

    Fetches comprehensive metadata and configuration details for multiple tables
    in parallel.

    Parameters
    ----------
    full_table_names : list[str]
        A list of fully qualified table names in the format catalog.schema.table
        (e.g., ["catalog1.schema1.table1", "catalog2.schema2.table2"]).

    Returns
    -------
    ToolCallResponse
        A response object containing a list of detailed table information objects
        if successful, or error information if the operation failed.
    """
    try:
        async with get_async_session() as (session, semaphore):
            table_tasks = [
                _get_table_details_from_endpoint(session, semaphore, full_table_name) for full_table_name in full_table_names
            ]
            tables_data = await asyncio.gather(*table_tasks)
            return format_toolcall_response(success=True, content=tables_data)
    except Exception as e:
        return format_toolcall_response(success=False, error=e)
