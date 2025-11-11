import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeAlias, TypedDict

import aiohttp
from toon_format import encode


@dataclass
class AsyncClientConfig:
    """Configuration for async HTTP client."""

    max_concurrent_requests: int = 8
    max_retries: int = 5
    base_delay: float = 0.5


class ToolCallResponse(TypedDict):
    """Response structure for MCP tool calls."""

    success: bool
    content: Any | None
    error: str | None


@asynccontextmanager
async def get_async_session() -> AsyncIterator[tuple[aiohttp.ClientSession, asyncio.Semaphore]]:
    """Context manager for Unity Catalog session handling."""
    config = AsyncClientConfig()
    semaphore = asyncio.Semaphore(config.max_concurrent_requests)
    async with aiohttp.ClientSession() as session:
        yield session, semaphore


class MaxRetriesExceededError(Exception):
    """Raised when maximum retries are exceeded."""


async def get_with_backoff(
    session: aiohttp.ClientSession,
    endpoint: str,
    semaphore: asyncio.Semaphore,
    max_retries: int = 5,
    base_delay: float = 0.5,
    additional_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Asynchronously fetches JSON data from a given URL using an aiohttp ClientSession.

    Includes automatic retries and exponential backoff on HTTP 429 (Too Many Requests) responses.
    """
    if additional_headers is None:
        additional_headers = {}
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    missing_host_msg = "DATABRICKS_HOST environment variable is not set"
    if databricks_host is None:
        raise ValueError(missing_host_msg)
    missing_auth_msg = "DATABRICKS_TOKEN environment variable is not set"
    if databricks_token is None:
        raise ValueError(missing_auth_msg)

    url = f"{databricks_host}/api/2.1/{endpoint}"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }
    headers.update(additional_headers)
    delay = base_delay
    for _ in range(max_retries):
        async with semaphore, session.get(url, headers=headers) as response:
            if response.status == 429:
                print(
                    f"429 Too Many Requests for {url}. Retrying in {delay} seconds...",
                )
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff
                continue
            response.raise_for_status()
            return await response.json()
    raise MaxRetriesExceededError(f"Max retries {max_retries} exceeded for URL: {url}")


def format_toolcall_response(
    success: bool,
    content: object | None = None,
    error: Exception | None = None,
) -> ToolCallResponse:
    """Format a tool call response into a standardized dictionary structure that gets fed into the LLM."""
    content = encode(content) if content else None
    response: ToolCallResponse = {
        "success": success,
        "content": content,
        "error": str(error) if error else None,
    }
    return response


JsonData: TypeAlias = dict[str, Any] | list["JsonData"]


def mask_api_response(data: JsonData, mask: dict[str, Any]) -> JsonData:
    """
    Recursively filter a nested api json response according to a mask.

    The mask dictionary specifies which keys to keep:
    - Keys in the mask should be kept in the output.
    - If a mask key maps to another dict, the function recurses into that sub-dictionary to filter deeply.
    - If data is list: apply mask to each element in the list.
    - Non-dict and non-list values are returned as is if they match a mask key.
    """
    if isinstance(data, dict) and isinstance(mask, dict):
        filtered = {}
        for key, submask in mask.items():
            # Only keep the key if it exists in input data
            if key in data:
                # If mask[key] is a dict, recurse to filter nested structures
                filtered[key] = mask_api_response(data[key], submask)
        return filtered
    if isinstance(data, list):
        # Apply the mask recursively to each item in the list
        return [mask_api_response(item, mask) for item in data]
    # Base case: data is not a dict or list (leaf node), return it as is
    return data
