"""Base scraper with shared retry/backoff logic."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class BaseScraper:
    """Base class for all scrapers with retry logic."""

    def __init__(self, max_retries: int = 3, delays: tuple = (2, 4, 8)):
        self.max_retries = max_retries
        self.delays = delays
        self.client = httpx.Client(timeout=30.0, follow_redirects=True)

    def _timestamp(self) -> str:
        """Return current ISO timestamp."""
        return datetime.now(timezone.utc).isoformat()

    def fetch_with_retry(self, url: str, **kwargs) -> dict[str, Any] | None:
        """Fetch URL with exponential backoff retry."""
        for attempt in range(self.max_retries):
            try:
                response = self.client.get(url, **kwargs)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP {e.response.status_code} on {url}, attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delays[attempt])
                else:
                    logger.error(f"Failed after {self.max_retries} attempts: {url}")
                    return None
            except httpx.RequestError as e:
                logger.warning(f"Request error on {url}: {e}, attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delays[attempt])
                else:
                    logger.error(f"Failed after {self.max_retries} attempts: {url}")
                    return None
        return None

    def close(self):
        """Close the HTTP client."""
        self.client.close()


class AsyncBaseScraper:
    """Async version of base scraper."""

    def __init__(self, max_retries: int = 3, delays: tuple = (2, 4, 8)):
        self.max_retries = max_retries
        self.delays = delays
        self.client = httpx.AsyncClient(timeout=30.0)

    def _timestamp(self) -> str:
        """Return current ISO timestamp."""
        return datetime.now(timezone.utc).isoformat()

    async def fetch_with_retry(self, url: str, **kwargs) -> dict[str, Any] | None:
        """Fetch URL with exponential backoff retry."""
        for attempt in range(self.max_retries):
            try:
                response = await self.client.get(url, **kwargs)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP {e.response.status_code} on {url}, attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.delays[attempt])
                else:
                    logger.error(f"Failed after {self.max_retries} attempts: {url}")
                    return None
            except httpx.RequestError as e:
                logger.warning(f"Request error on {url}: {e}, attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.delays[attempt])
                else:
                    logger.error(f"Failed after {self.max_retries} attempts: {url}")
                    return None
        return None

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
