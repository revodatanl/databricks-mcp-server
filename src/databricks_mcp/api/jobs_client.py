import asyncio
import aiohttp
from databricks_mcp.api.response_masks import get_jobs_mask
from databricks_mcp.api.utils import (
    get_with_backoff,
    format_toolcall_response,
    get_async_session,
    ToolCallResponse,
    mask_api_response,
    JsonData,
)


async def _get_jobs_from_endpoint(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
) -> JsonData:
    """Retrieves a list of jobs"""
    data = await get_with_backoff(session, "jobs/list", semaphore)
    return data["jobs"]


async def get_jobs() -> ToolCallResponse:
    """
    List all jobs in the users workspace
    """
    try:
        async with get_async_session() as (session, semaphore):
            jobs = await _get_jobs_from_endpoint(session, semaphore)
            masked_data = mask_api_response(jobs, get_jobs_mask)
            return format_toolcall_response(success=True, content=masked_data)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)
