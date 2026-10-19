import asyncio
import logging
import random
from typing import Awaitable, Callable, List, Optional, TypeVar

import httpx

log = logging.getLogger(__name__)
T = TypeVar("T")


class HospitalAPIError(Exception):
    def __init__(self, message: str, *, status_code: Optional[int] = None,
                 retryable: bool = False, ambiguous: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        # ambiguous = we don't know if the server applied the request
        # (timeout / dropped connection after sending). Matters for POST.
        self.ambiguous = ambiguous


def _raise_for_response(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    retryable = resp.status_code == 429 or resp.status_code >= 500
    try:
        detail = resp.json()
    except ValueError:
        detail = resp.text[:300]
    raise HospitalAPIError(
        f"Upstream returned {resp.status_code}: {detail}",
        status_code=resp.status_code,
        retryable=retryable,
        # a 5xx might have happened after the write; treat as ambiguous
        ambiguous=resp.status_code >= 500,
    )


async def backoff_sleep(attempt: int, base: float) -> None:
    """Exponential backoff with jitter: ~base, 2*base, 4*base ... (+/- random).
    Jitter stops all 20 rows from retrying at the exact same instant."""
    delay = base * (2 ** (attempt - 1))
    await asyncio.sleep(delay + random.uniform(0, delay / 2))


async def with_retry(fn: Callable[[], Awaitable[T]], *, attempts: int, backoff_base: float,
                     label: str) -> T:
    """Retry helper for IDEMPOTENT calls only (GET, PATCH activate, DELETE).
    Repeating them can't create duplicates, so blind retry is safe."""
    last: Optional[HospitalAPIError] = None
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except HospitalAPIError as exc:
            last = exc
            if not exc.retryable or attempt == attempts:
                raise
            log.warning("%s failed (attempt %d/%d): %s", label, attempt, attempts, exc)
            await backoff_sleep(attempt, backoff_base)
    raise last  # pragma: no cover


class HospitalClient:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def _send(self, method: str, url: str, **kwargs) -> httpx.Response:
        try:
            resp = await self._http.request(method, url, **kwargs)
        except httpx.ConnectError as exc:
            # never reached the server: safe to retry, not ambiguous
            raise HospitalAPIError(f"Connection failed: {exc}", retryable=True) from exc
        except httpx.TransportError as exc:  # timeouts, resets, protocol errors
            raise HospitalAPIError(
                f"{type(exc).__name__}: {exc}", retryable=True, ambiguous=True
            ) from exc
        _raise_for_response(resp)
        return resp

    async def health(self, timeout: float) -> None:
        await self._send("GET", "/", timeout=timeout)

    async def create_hospital(self, payload: dict) -> dict:
        return (await self._send("POST", "/hospitals/", json=payload)).json()

    async def get_batch(self, batch_id: str) -> List[dict]:
        return (await self._send("GET", f"/hospitals/batch/{batch_id}")).json()

    async def activate_batch(self, batch_id: str) -> None:
        await self._send("PATCH", f"/hospitals/batch/{batch_id}/activate")

    async def delete_hospital(self, hospital_id: int) -> None:
        await self._send("DELETE", f"/hospitals/{hospital_id}")

    async def delete_batch(self, batch_id: str) -> None:
        await self._send("DELETE", f"/hospitals/batch/{batch_id}")
