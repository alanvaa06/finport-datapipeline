"""The one HTTP client of the store: per-source pacing, retries on 429/5xx and network errors
(waits 2, 4, 8 s, or what the answer's Retry-After asks for, up to RETRY_AFTER_LIMIT; a longer
wait ends the request at once), a stop for a source that is down (after BREAKER requests in a
row fail every attempt, the rest of its requests in this client fail at once, without a call),
and secrets scrubbed from every message it raises and from the log lines
httpx and httpcore write (a key sent as a query parameter would otherwise show up at INFO level,
and in a redirect's Location at DEBUG level).

Non-retryable answers (200, 400, 401, 404...) are returned as they are: each source reads its
own error format. A redirect is followed only within the same host and over https; any other is
refused before anything is sent to it, because the request carries the source's key (in a
header, the URL or a POST body).
"""

import datetime
import email.utils
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
MAX_REDIRECTS = 5
RETRY_AFTER_LIMIT = 60.0  # seconds: a source asking for a longer wait is not asked again in this run
BREAKER = 3  # requests in a row that failed every attempt: the source is down for the rest of the run


def retry_after(response: httpx.Response) -> float | None:
    """The seconds an answer asks to wait before the next request (its Retry-After, in seconds or
    as an HTTP date), or None when it does not say."""
    value = response.headers.get("Retry-After", "").strip()
    if value.isdigit():
        return float(value)
    try:
        moment = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.UTC)
    return max(0.0, (moment - datetime.datetime.now(datetime.UTC)).total_seconds())


def scrub(text: str, secrets: Sequence[str]) -> str:
    """Replace every secret found in `text` with ***."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, HIDDEN)
    return text


# A logger's filter does not see the records of its children: httpcore logs on these.
LOGGERS = (
    "httpx",
    "httpcore",
    "httpcore.connection",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.proxy",
    "httpcore.socks",
)


def _http_loggers() -> list[logging.Logger]:
    """The loggers of httpx and httpcore, children included (any created since, too)."""
    created = (name for name in logging.root.manager.loggerDict if name.startswith(("httpx.", "httpcore.")))
    return [logging.getLogger(name) for name in sorted({*LOGGERS, *created})]


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
            follow_redirects=False,  # followed by _send, only where the key may go
        )
        self._sleep = sleep
        self._clock = clock
        self._retries = retries
        self._last: dict[str, float] = {}
        # Requests made per source, what a daily budget counts. A request asked again after a 429
        # is still one request: the source refused to serve it, and counting each attempt would
        # spend the budget on refusals.
        self.calls: dict[str, int] = {}
        self.throttled: dict[str, int] = {}
        self._failing: dict[str, tuple[int, str]] = {}  # per source: requests failed in a row, last reason
        self._scrubber = _Scrubber(self._secrets)
        self._loggers = _http_loggers()
        for logger in self._loggers:
            logger.addFilter(self._scrubber)

    def scrub(self, text: str) -> str:
        return scrub(text, self._secrets)

    def excerpt(self, text: str, length: int) -> str:
        """The first `length` characters of `text`, secrets hidden. Scrubbed before it is cut: a
        key cut in two would no longer match, and its first characters would show."""
        return self.scrub(text)[:length]

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
        failing, last = self._failing.get(source, (0, ""))
        if failing >= BREAKER:
            msg = self.scrub(f"{source}: not asked: its last {failing} requests failed ({last})")
            raise NetworkError(msg)
        reason = ""
        limited = False
        attempts = 0
        for attempt in range(self._retries + 1):
            self._wait_turn(source, per_minute)
            if not limited:  # the retry of a refused (429) request is not another call
                self.calls[source] = self.calls.get(source, 0) + 1
            attempts += 1
            wait = None
            try:
                request = self._http.build_request(method, url, params=params, headers=headers, json=body)
                response = self._send(source, request, per_minute)
            except httpx.TransportError as exc:
                reason = f"{type(exc).__name__}: {exc}"
                limited = False
            else:
                if response.status_code not in RETRY_STATUS:
                    self._failing.pop(source, None)
                    return response
                if response.status_code == TOO_MANY_REQUESTS:
                    self.throttled[source] = self.throttled.get(source, 0) + 1
                reason = f"HTTP {response.status_code}"
                limited = response.status_code == TOO_MANY_REQUESTS
                wait = retry_after(response)
                if wait is not None and wait > RETRY_AFTER_LIMIT:
                    reason += f", asked to wait {wait:.0f} s"
                    break
            if attempt < self._retries:
                self._sleep(wait if wait is not None else 2.0 ** (attempt + 1))
        msg = self.scrub(f"{source}: {reason} after {attempts} attempt{'s' if attempts > 1 else ''}")
        if limited:  # the source answers: it asks for fewer requests, it is not down
            self._failing.pop(source, None)
            raise RateLimitedError(msg) from None
        self._failing[source] = (failing + 1, reason)
        raise NetworkError(msg) from None

    def _send(self, source: str, request: httpx.Request, per_minute: int) -> httpx.Response:
        """Send `request`, following redirects within its host over https. Each hop is a call,
        paced like any other: it waits its turn at `per_minute`."""
        response = self._http.send(request)
        for _ in range(MAX_REDIRECTS):
            target = response.next_request
            if not response.is_redirect or target is None:
                return response
            same_place = (target.url.host, target.url.port) == (request.url.host, request.url.port)
            if target.url.scheme != "https" or not same_place:
                response.close()
                place = f"{target.url.scheme}://{target.url.host}"
                msg = self.scrub(
                    f"{source}: HTTP {response.status_code} redirect to {place} refused: only redirects "
                    f"within {request.url.host} over https are followed, so the key goes nowhere else"
                )
                raise NetworkError(msg)
            response.close()
            self._wait_turn(source, per_minute)
            self.calls[source] = self.calls.get(source, 0) + 1
            response = self._http.send(target)
        msg = self.scrub(f"{source}: more than {MAX_REDIRECTS} redirects from {request.url.host}")
        raise NetworkError(msg)

    def _wait_turn(self, source: str, per_minute: int) -> None:
        interval = 60.0 / per_minute
        previous = self._last.get(source)
        now = self._clock()
        if previous is not None and now - previous < interval:
            self._sleep(interval - (now - previous))
            now = self._clock()
        self._last[source] = now

    def close(self) -> None:
        for logger in self._loggers:
            logger.removeFilter(self._scrubber)
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
