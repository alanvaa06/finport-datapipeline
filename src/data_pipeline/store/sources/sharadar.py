"""What the Sharadar sources share: the call, the ticker rule, and how Sharadar refuses.

Key in the `api_key` query parameter. One call asks one table for one ticker, from a first day
on. A call without a first day is answered with a recent window, not the whole history, so the
first day is always sent.

Sharadar does not reject a wrong key: it answers as its free tier, with data for the few
tickers that tier covers and HTTP 403 for every other one. So a 403 means the key is wrong or
the plan lacks the table, and it is the only sign of either.

An answer holds at most 10,000 rows and is cut there without notice: one that long is refused
rather than stored cut short.

Sharadar's license is for personal use and forbids sharing its data: this library ships no
answer of Sharadar's, and each user brings a key and a license of their own.
"""

import datetime
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.store.errors import (
    CatalogError,
    KeyRejectedError,
    NetworkError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Outcome, Request
from data_pipeline.store.sources.base import failures

URL = "https://api.sharadar.com/v1.0/data/{table}"
TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,11}$")
REQUESTS_PER_MINUTE = 120  # assumed: Sharadar publishes no rate for this API
EARLIEST = datetime.date(1900, 1, 1)
MAX_ROWS = 10_000
MISSING_TEXT = "N/A"
OK = 200
FORBIDDEN = 403


class AnswerError(Exception):
    """Sharadar answered, but not with the rows that were asked for."""


def check_ticker(entry: CatalogEntry) -> None:
    """Raise CatalogError when the id is not a ticker in upper case."""
    if not TICKER.match(entry.source_id):
        msg = f"{entry.key}: a Sharadar id is a ticker in upper case, such as AAPL or BRK.B"
        raise CatalogError(msg)


def text(value: object) -> str:
    """A text field as it is stored: Sharadar's `N/A`, like a field it leaves out, is empty."""
    found = "" if value is None else str(value)
    return "" if found == MISSING_TEXT else found


class Sharadar:
    """GET against Sharadar for one source, with the user's key."""

    def __init__(self, client: Client, source: str, key: str) -> None:
        self._client = client
        self._source = source
        self._key = key

    def rows(self, table: str, ticker: str, start: datetime.date | None) -> list[Mapping[str, Any]]:
        """The rows of one ticker in one table from `start` on; the whole history without it."""
        response = self._client.get(
            self._source,
            URL.format(table=table),
            params={
                "ticker": ticker,
                "date.gte": (start or EARLIEST).isoformat(),
                "format": "json",
                "api_key": self._key,
            },
            per_minute=REQUESTS_PER_MINUTE,
        )
        if response.status_code == FORBIDDEN:
            msg = (
                f"Sharadar treated the request as its free tier (HTTP 403): check {keys.SHARADAR} and that "
                f"the plan includes the table {table!r}"
            )
            raise KeyRejectedError(msg)
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {self._client.excerpt(response.text, 200)}"
            raise AnswerError(msg)
        try:
            payload = response.json()
        except ValueError as exc:  # JSONDecodeError, or bytes that are not text
            msg = f"unexpected answer (not JSON: {self._client.excerpt(response.text, 200)})"
            raise AnswerError(msg) from exc
        rows = payload["data"]
        if not isinstance(rows, list):
            msg = "unexpected answer (`data` is not a list of rows)"
            raise AnswerError(msg)
        if len(rows) >= MAX_ROWS:
            msg = (
                f"the answer reached Sharadar's cap of {MAX_ROWS} rows and may be cut short: "
                "set `start` in the entry"
            )
            raise AnswerError(msg)
        return rows


def per_ticker(requests: Sequence[Request], download: Callable[[Request], FetchBatch]) -> Iterator[FetchBatch]:
    """Run `download` once per request and yield its batch. A refused key fails every request
    left; a persistent 429 stops the source; any other failure is that ticker's alone."""
    for position, request in enumerate(requests):
        entry = request.entry
        try:
            yield download(request)
        except KeyRejectedError as exc:
            yield FetchBatch(failures=failures(requests[position:], Outcome.KEY_ERROR, str(exc)))
            return
        except RateLimitedError as exc:
            # Asking again would only prolong the block: stop here, the next run resumes.
            raise QuotaExhaustedError(str(exc)) from exc
        except NetworkError as exc:
            yield FetchBatch(failures=(Failure(entry, Outcome.NETWORK_ERROR, str(exc)),))
        except AnswerError as exc:
            yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, str(exc)),))
        except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
            reason = f"unexpected answer ({type(exc).__name__}: {exc})"
            yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, reason),))
