import asyncio
import aiohttp
from typing import Dict, Any

from databricks_mcp.api.utils import (
    get_with_backoff,
    format_toolcall_response,
    get_async_session,
    ToolCallResponse,
)


async def _fetch_jobs_from_endpoint(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """Retrieves a list of jobs"""
    data = await get_with_backoff(session, "jobs/list", semaphore)
    return data


async def list_jobs() -> ToolCallResponse:
    """
    List all jobs in the users workspace
    """
    try:
        async with get_async_session() as (session, semaphore):
            jobs = await _fetch_jobs_from_endpoint(session, semaphore)
            result = jobs["jobs"]

            return format_toolcall_response(success=True, content=result)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)
