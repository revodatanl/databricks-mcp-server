import asyncio
import json
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

# Load masks from JSON files
_MASKS_DIR = Path(__file__).parent / "masks"

with (_MASKS_DIR / "jobs_mask.json").open() as f:
    jobs_mask = json.load(f)

with (_MASKS_DIR / "jobs_details_mask.json").open() as f:
    jobs_details_mask = json.load(f)

with (_MASKS_DIR / "jobs_runs_mask.json").open() as f:
    jobs_runs_mask = json.load(f)


async def _get_jobs_from_endpoint(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
) -> JsonData:
    """Get a list of jobs from the jobs/list endpoint.

    Args:
        session: The aiohttp client session
        semaphore: Semaphore for rate limiting requests

    Returns
    -------
        List of jobs from the API response
    """
    data = await get_with_backoff(session, "jobs/list", semaphore)
    masked_data = mask_api_response(data, jobs_mask)["jobs"]
    return masked_data


async def get_jobs() -> ToolCallResponse:
    """Get a list of all jobs.

    Gets all jobs from the jobs/list endpoint and masks the response data according
    to the jobs mask.

    Returns
    -------
        ToolCallResponse
    """
    try:
        async with get_async_session() as (session, semaphore):
            jobs = await _get_jobs_from_endpoint(session, semaphore)
            return format_toolcall_response(success=True, content=jobs)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_single_job_details(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    job_id: int,
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
    masked_data = mask_api_response(data, jobs_details_mask)
    return masked_data


async def get_job_details(job_ids: list[int]) -> ToolCallResponse:
    """Get detailed information for multiple jobs.

    Gets the details for a list of job IDs by making concurrent API requests.
    The response data is masked according to the jobs details mask.
    Args:
        job_ids: List of job IDs to get details for

    Returns
    -------
        ToolCallResponse
    """
    try:
        async with get_async_session() as (session, semaphore):
            # Get details about multiple jobs concurrently
            job_tasks = [_get_single_job_details(session, semaphore, job_id) for job_id in job_ids]
            jobs_data = await asyncio.gather(*job_tasks)
            return format_toolcall_response(success=True, content=jobs_data)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)


async def _get_runs_for_single_job(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    job_id: int,
    amount: int,
) -> JsonData:
    """Get run history for a specific job

    Args:
        session: The aiohttp client session
        semaphore: Semaphore for rate limiting requests
        job_id: ID of the job to get runs for

    Returns
    -------
        List of job runs
    """
    data = await get_with_backoff(session, f"jobs/runs/list?job_id={job_id}", semaphore)
    masked_data = mask_api_response(data, jobs_runs_mask)
    runs = masked_data.get("runs", [])
    return runs[:amount]


async def get_job_runs(job_ids: list[int], amount: int) -> ToolCallResponse:
    """Get run history for multiple jobs.

    Gets the run history and details for a list of job IDs by making concurrent API
    requests. The response data is masked according to the jobs runs mask.
    Args:
        job_ids: List of job IDs to get run history for

    Returns
    -------
        ToolCallResponse
    """
    try:
        async with get_async_session() as (session, semaphore):
            job_tasks = [_get_runs_for_single_job(session, semaphore, job_id, amount) for job_id in job_ids]
            jobs_data = await asyncio.gather(*job_tasks)
            return format_toolcall_response(success=True, content=jobs_data)

    except Exception as e:
        return format_toolcall_response(success=False, error=e)
