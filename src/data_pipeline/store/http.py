"""HTTP for the sources of the store.

A run shares one `Client` among its sources. The client:

- keeps each source to its own pace (`per_minute`): the turn of one source says nothing of another's;
- asks again after a dropped connection or a 429, 500, 502, 503 or 504, first after 2 s, then 4,
  then 8, unless the answer names its own wait in Retry-After; a wait above RETRY_AFTER_LIMIT ends
  the request on the spot;
- stops calling a source, for as long as the client lives, once BREAKER requests in a row have
  failed every attempt;
- follows a redirect only to the same host and port over https, because each request carries a key;
- hides its secrets in every error it raises and in what httpx and httpcore log, both as typed and
  as a URL would encode them (a key with "+", "/", "=" or a space changes on its way into a URL).

Every other answer (200, 400, 401, 404...) comes back untouched for the source to read.
"""

import dataclasses
import datetime
import email.utils
import logging
import re
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, NamedTuple

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
RETRY_AFTER_LIMIT = 60.0  # seconds; a source that wants longer is left alone for this run
BREAKER = 3  # failed requests in a row after which a source counts as down

# httpcore writes on child loggers, and a filter set on a parent never sees a child's records.
LOGGERS = (
    "httpx",
    "httpcore",
    "httpcore.connection",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.proxy",
    "httpcore.socks",
)

_Text = Mapping[str, str]
_Body = Mapping[str, Any]
_Transport = httpx.BaseTransport | None
_Sleep = Callable[[float], None]
_Clock = Callable[[], float]
_LogFilter = Callable[[logging.LogRecord], bool]


def retry_after(response: httpx.Response) -> float | None:
    """How long `response` asks the caller to wait, from Retry-After as seconds or as a date.

    None when the header is absent or unreadable. A date already past means no wait at all.
    """
    header = response.headers.get("Retry-After", "").strip()
    # Only ASCII digits count: str.isdigit() also accepts characters such as "2" in superscript.
    if header.isascii() and header.isdigit():
        return float(header)
    return _seconds_until(header)


def _seconds_until(http_date: str) -> float | None:
    try:
        moment = email.utils.parsedate_to_datetime(http_date)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:  # HTTP dates are in GMT
        moment = moment.replace(tzinfo=datetime.UTC)
    left = (moment - datetime.datetime.now(datetime.UTC)).total_seconds()
    return left if left > 0 else 0.0


def _spellings(char: str) -> str:
    """A regex for `char` as it is or percent-encoded (hex in any case; a space may also be "+")."""
    if char.isascii() and char.isalnum():
        return re.escape(char)  # letters and digits are never encoded
    # "surrogatepass" lets a lone surrogate (an undecodable byte of the environment) be encoded too.
    hex_form = "".join(f"%{octet:02x}" for octet in char.encode("utf-8", "surrogatepass"))
    space = r"|\+" if char == " " else ""
    # The encoded spelling is tried first, so "%25" in a text is one encoded "%" and not "%" plus "25".
    return f"(?:(?i:{hex_form})|{re.escape(char)}{space})"


def _finder(secret: str) -> re.Pattern[str]:
    """Matches `secret` in any mix of plain and percent-encoded characters."""
    return re.compile("".join(map(_spellings, secret)))


def scrub(text: str, secrets: Sequence[str]) -> str:
    """`text` with each secret, typed or URL-encoded, replaced by ***. Empty secrets are skipped."""
    for secret in filter(None, secrets):
        text = _finder(secret).sub(HIDDEN, text)
    return text


def _record_scrubber(secrets: tuple[str, ...]) -> _LogFilter:
    """A log filter that rewrites each record with `secrets` hidden; it never drops a record."""

    def hide(record: logging.LogRecord) -> bool:
        record.msg, record.args = scrub(record.getMessage(), secrets), ()
        return True

    return hide


def _http_loggers() -> list[logging.Logger]:
    """The httpx and httpcore loggers: the known ones and any child created by now."""
    children = [name for name in logging.root.manager.loggerDict if name.startswith(("httpx.", "httpcore."))]
    return [logging.getLogger(name) for name in sorted(set(LOGGERS).union(children))]


class _Streak(NamedTuple):
    """Requests to one source that failed one after another, and why the latest did."""

    failed: int
    reason: str


@dataclasses.dataclass(frozen=True)
class _Call:
    method: str
    source: str
    url: str
    params: _Text | None
    headers: _Text | None
    body: _Body | None
    per_minute: int


@dataclasses.dataclass(frozen=True)
class _Miss:
    """An attempt that got no usable answer."""

    reason: str
    rate_limited: bool = False
    wait: float | None = None  # what the source asked for, if it said
    final: bool = False  # the source asked for too long a wait: do not try again


class Client:
    """Paced, retrying HTTP client that keeps its secrets out of messages and logs."""

    def __init__(
        self,
        secrets: Sequence[str] = (),
        transport: _Transport = None,
        sleep: _Sleep = time.sleep,
        clock: _Clock = time.monotonic,
        retries: int = RETRIES,
        timeout: float = TIMEOUT,
    ) -> None:
        # Redirects are left to _send, which only follows those the key may go to.
        self._http = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            transport=transport,
            follow_redirects=False,
        )
        self._retries, self._sleep, self._clock = retries, sleep, clock
        self._secrets = tuple(secrets)
        # Per source, the requests made (what a daily budget counts) and the 429 answers received.
        # Asking again after a 429 is not a new request: the source declined to serve it.
        self.calls: dict[str, int] = {}
        self.throttled: dict[str, int] = {}
        self._sent_at: dict[str, float] = {}
        self._streaks: dict[str, _Streak] = {}
        self._log_filter = _record_scrubber(self._secrets)
        self._filtered = _http_loggers()
        for logger in self._filtered:
            logger.addFilter(self._log_filter)

    def get(
        self,
        source: str,
        url: str,
        *,
        params: _Text | None = None,
        headers: _Text | None = None,
        per_minute: int = DEFAULT_PER_MINUTE,
    ) -> httpx.Response:
        """GET `url`, paced and retried. Raises NetworkError once every attempt has failed."""
        return self._run(_Call("GET", source, url, params, headers, None, per_minute))

    def post(
        self,
        source: str,
        url: str,
        *,
        json: _Body,
        headers: _Text | None = None,
        per_minute: int = DEFAULT_PER_MINUTE,
    ) -> httpx.Response:
        """POST `json` to `url`, paced and retried like `get`."""
        return self._run(_Call("POST", source, url, None, headers, json, per_minute))

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the connections and take the log filter off the HTTP loggers."""
        for logger in self._filtered:
            logger.removeFilter(self._log_filter)
        self._http.close()

    def scrub(self, text: str) -> str:
        """`text` with this client's secrets hidden."""
        return scrub(text, secrets=self._secrets)

    def excerpt(self, text: str, length: int) -> str:
        """At most `length` characters of `text`, secrets hidden.

        The cut comes after the scrub: a key cut first would no longer match and its start would show.
        """
        return self.scrub(text)[:length]

    def _run(self, call: _Call) -> httpx.Response:
        streak = self._streaks.get(call.source)
        if streak is not None and streak.failed >= BREAKER:
            msg = self.scrub(f"{call.source}: not asked: its last {streak.failed} requests failed ({streak.reason})")
            raise NetworkError(msg)
        attempt = 0
        miss = _Miss("")
        while True:
            attempt += 1
            self._take_turn(call.source, call.per_minute)
            if not miss.rate_limited:  # a 429 retried is the same request, not a new one
                _tally(self.calls, call.source)
            try:
                request = self._http.build_request(
                    call.method, call.url, params=call.params, headers=call.headers, json=call.body
                )
                response = self._send(call.source, request, call.per_minute)
            except httpx.TransportError as exc:
                miss = _Miss(f"{type(exc).__name__}: {exc}")
            else:
                if response.status_code not in RETRY_STATUS:
                    self._streaks.pop(call.source, None)
                    return response
                miss = self._read_refusal(call.source, response)
            if miss.final or attempt > self._retries:
                break
            self._sleep(2.0**attempt if miss.wait is None else miss.wait)
        raise self._give_up(call.source, miss, attempt) from None

    def _read_refusal(self, source: str, response: httpx.Response) -> _Miss:
        """What a retryable answer means: rate limited or not, how long it asks to wait."""
        status = response.status_code
        rate_limited = status == TOO_MANY_REQUESTS
        if rate_limited:
            _tally(self.throttled, source)
        wait = retry_after(response)
        if wait is not None and wait > RETRY_AFTER_LIMIT:
            return _Miss(f"HTTP {status}, asked to wait {wait:.0f} s", rate_limited, wait, final=True)
        return _Miss(f"HTTP {status}", rate_limited, wait)

    def _give_up(self, source: str, miss: _Miss, attempts: int) -> NetworkError:
        """The error for a request that ran out of attempts; updates the source's failure streak."""
        noun = "attempt" if attempts == 1 else "attempts"
        msg = self.scrub(f"{source}: {miss.reason} after {attempts} {noun}")
        if miss.rate_limited:
            # The source is up and answering; it only wants fewer requests.
            self._streaks.pop(source, None)
            return RateLimitedError(msg)
        before = self._streaks.get(source, _Streak(0, ""))
        self._streaks[source] = _Streak(before.failed + 1, miss.reason)
        return NetworkError(msg)

    def _send(self, source: str, request: httpx.Request, per_minute: int) -> httpx.Response:
        """Send `request` and follow its redirects that stay on the same host over https.

        Every hop is one more call, paced like the others.
        """
        response = self._http.send(request)
        hops = 0
        while response.is_redirect and response.next_request is not None:
            target = response.next_request
            response.close()
            self._check_redirect(source, request, target, response.status_code)
            self._take_turn(source, per_minute)
            _tally(self.calls, source)
            response = self._http.send(target)
            hops += 1
            if hops == MAX_REDIRECTS:
                msg = self.scrub(f"{source}: more than {MAX_REDIRECTS} redirects from {request.url.host}")
                raise NetworkError(msg)
        return response

    def _check_redirect(self, source: str, origin: httpx.Request, target: httpx.Request, status: int) -> None:
        """Raise NetworkError unless `target` is on the host and port of `origin`, over https."""
        here, there = origin.url, target.url
        if there.scheme == "https" and there.host == here.host and there.port == here.port:
            return
        msg = self.scrub(
            f"{source}: HTTP {status} redirect to {there.scheme}://{there.host} refused: only redirects "
            f"within {here.host} over https are followed, so the key goes nowhere else"
        )
        raise NetworkError(msg)

    def _take_turn(self, source: str, per_minute: int) -> None:
        """Sleep until `source` may be called again at `per_minute`, then note the time."""
        now = self._clock()
        previous = self._sent_at.get(source)
        if previous is not None:
            early_by = 60.0 / per_minute - (now - previous)
            if early_by > 0:
                self._sleep(early_by)
                now = self._clock()
        self._sent_at[source] = now


def _tally(counts: dict[str, int], source: str) -> None:
    counts[source] = counts.get(source, 0) + 1
