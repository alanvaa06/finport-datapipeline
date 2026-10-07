"""The one HTTP client of the store: per-source pacing, retries on 429/5xx and network errors
(waits 2, 4, 8 s), and secrets scrubbed from every message it raises and from the request log
lines httpx writes (a key sent as a query parameter would otherwise show up at INFO level).

Non-retryable answers (200, 400, 401, 404...) are returned as they are: each source reads its
own error format.
"""

import logging
import time
import types
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx

from data_pipeline.store.errors import NetworkError, RateLimitedError

TIMEOUT = 90.0
RETRIES = 3
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
TOO_MANY_REQUESTS = 429
DEFAULT_PER_MINUTE = 30
USER_AGENT = "finport-datapipeline (public data store)"
HIDDEN = "***"


def scrub(text: str, secrets: Sequence[str]) -> str:
    """Replace every secret found in `text` with ***."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, HIDDEN)
    return text


LOGGERS = ("httpx", "httpcore")


class _Scrubber(logging.Filter):
    """Hides the client's secrets in the log records of the HTTP libraries."""

    def __init__(self, secrets: Sequence[str]) -> None:
        super().__init__()
        self._secrets = tuple(secrets)

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = scrub(record.getMessage(), self._secrets)
        record.args = ()
        return True


class Client:
    def __init__(
        self,
        secrets: Sequence[str] = (),
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        retries: int = RETRIES,
        timeout: float = TIMEOUT,
    ) -> None:
        self._secrets = tuple(secrets)
        self._http = httpx.Client(
            transport=transport,
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        self._sleep = sleep
        self._clock = clock
        self._retries = retries
        self._last: dict[str, float] = {}
        self.calls: dict[str, int] = {}
        self.throttled: dict[str, int] = {}
        self._scrubber = _Scrubber(self._secrets)
        for name in LOGGERS:
            logging.getLogger(name).addFilter(self._scrubber)

    def scrub(self, text: str) -> str:
        return scrub(text, self._secrets)

    def get(
        self,
        source: str,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        per_minute: int = DEFAULT_PER_MINUTE,
    ) -> httpx.Response:
        """GET with pacing and retries. Raises NetworkError when every attempt fails."""
        return self._request("GET", source, url, params=params, headers=headers, body=None, per_minute=per_minute)

    def post(
        self,
        source: str,
        url: str,
        *,
        json: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
        per_minute: int = DEFAULT_PER_MINUTE,
    ) -> httpx.Response:
        """POST a JSON body with the same pacing and retries as `get`."""
        return self._request("POST", source, url, params=None, headers=headers, body=json, per_minute=per_minute)

    def _request(
        self,
        method: str,
        source: str,
        url: str,
        *,
        params: Mapping[str, str] | None,
        headers: Mapping[str, str] | None,
        body: Mapping[str, Any] | None,
        per_minute: int,
    ) -> httpx.Response:
        reason = ""
        limited = False
        for attempt in range(self._retries + 1):
            self._wait_turn(source, per_minute)
            self.calls[source] = self.calls.get(source, 0) + 1
            try:
                response = self._http.request(method, url, params=params, headers=headers, json=body)
            except httpx.TransportError as exc:
                reason = f"{type(exc).__name__}: {exc}"
                limited = False
            else:
                if response.status_code not in RETRY_STATUS:
                    return response
                if response.status_code == TOO_MANY_REQUESTS:
                    self.throttled[source] = self.throttled.get(source, 0) + 1
                reason = f"HTTP {response.status_code}"
                limited = response.status_code == TOO_MANY_REQUESTS
            if attempt < self._retries:
                self._sleep(2.0 ** (attempt + 1))
        msg = self.scrub(f"{source}: {reason} after {self._retries + 1} attempts")
        if limited:
            raise RateLimitedError(msg) from None
        raise NetworkError(msg) from None

    def _wait_turn(self, source: str, per_minute: int) -> None:
        interval = 60.0 / per_minute
        previous = self._last.get(source)
        now = self._clock()
        if previous is not None and now - previous < interval:
            self._sleep(interval - (now - previous))
            now = self._clock()
        self._last[source] = now

    def close(self) -> None:
        for name in LOGGERS:
            logging.getLogger(name).removeFilter(self._scrubber)
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.close()
