import asyncio
import aiohttp
from databricks_mcp.api.response_masks import (
    get_jobs_mask,
    get_jobs_details_mask,
    get_jobs_runs_mask,
)
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
    """Get a list of jobs from the jobs/list endpoint.

    Args:
        session: The aiohttp client session
        semaphore: Semaphore for rate limiting requests

    Returns:
        List of jobs from the API response
    """
    data = await get_with_backoff(session, "jobs/list", semaphore)
    return data["jobs"]


async def get_jobs() -> ToolCallResponse:
    """Get a list of all jobs.

    Gets all jobs from the jobs/list endpoint and masks the response data according
    to the jobs mask.
    Returns:
        ToolCallResponse
    """
    try:
        async with get_async_session() as (session, semaphore):
            jobs = await _get_jobs_from_endpoint(session, semaphore)
            masked_data = mask_api_response(jobs, get_jobs_mask)
            return format_toolcall_response(success=True, content=masked_data)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_job_details(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, job_id: int
) -> JsonData:
    """Get details for a specific job

    Args:
        session: The aiohttp client session
        semaphore: Semaphore for rate limiting requests
        job_id: ID of the job to get details for
    Returns:
        Dict containing job details
    """
    data = await get_with_backoff(session, f"jobs/get?job_id={job_id}", semaphore)
    return data


async def get_jobs_details(job_ids: list[int]) -> ToolCallResponse:
    """Get detailed information for multiple jobs.

    Gets the details for a list of job IDs by making concurrent API requests.
    The response data is masked according to the jobs details mask.
    Args:
        job_ids: List of job IDs to get details for

    Returns:
        ToolCallResponse
    """
    try:
        async with get_async_session() as (session, semaphore):
            # Get details about multiple jobs concurrently
            job_tasks = [
                _get_job_details(session, semaphore, job_id) for job_id in job_ids
            ]
            jobs_data = await asyncio.gather(*job_tasks)
            masked_jobs_data = mask_api_response(jobs_data, get_jobs_details_mask)
            return format_toolcall_response(success=True, content=masked_jobs_data)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_runs_for_single_job(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, job_id: int, amount: int
) -> JsonData:
    """Get run history for a specific job

    Args:
        session: The aiohttp client session
        semaphore: Semaphore for rate limiting requests
        job_id: ID of the job to get runs for

    Returns:
        List of job runs
    """
    data = await get_with_backoff(session, f"jobs/runs/list?job_id{job_id}", semaphore)
    return data["runs"][:amount]


async def get_job_runs(job_ids: list[int], amount: int) -> ToolCallResponse:
    """Get run history for multiple jobs.

    Gets the run history and details for a list of job IDs by making concurrent API
    requests. The response data is masked according to the jobs runs mask.
    Args:
        job_ids: List of job IDs to get run history for

    Returns:
        ToolCallResponse
    """
    try:
        async with get_async_session() as (session, semaphore):
            job_tasks = [
                _get_runs_for_single_job(session, semaphore, job_id, amount) for job_id in job_ids
            ]
            jobs_data = await asyncio.gather(*job_tasks)
            masked_data = mask_api_response(jobs_data, get_jobs_runs_mask)
            return format_toolcall_response(success=True, content=masked_data)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)
