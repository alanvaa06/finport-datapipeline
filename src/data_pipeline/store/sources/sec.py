"""What the SEC sources share: the User-Agent, the list of tickers, and how the SEC refuses.

The SEC asks every client to identify itself: `SEC_EDGAR_UA` holds a User-Agent such as
`Name name@domain.com`. It blocks for about ten minutes a client that exceeds 10 requests a
second or sends no contact, answering HTTP 403.
"""

import re
from collections.abc import Sequence
from typing import Any

import httpx

from data_pipeline.store.errors import CatalogError, NetworkError, QuotaExhaustedError, RateLimitedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, FetchBatch, Outcome, Request
from data_pipeline.store.sources.base import failures

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,11}$")
REQUESTS_PER_MINUTE = 300  # the SEC allows 10 a second
OK = 200
FORBIDDEN = 403
NOT_FOUND = 404
BLOCKED = (
    "the SEC refused the request (HTTP 403): check that SEC_EDGAR_UA is a User-Agent with a contact, "
    "such as 'Name name@domain.com', or wait ten minutes if the rate was exceeded"
)


class AnswerError(Exception):
    """The SEC answered, but not with what was asked for."""


def check_ticker(entry: CatalogEntry) -> None:
    """Raise CatalogError when the id is not a ticker in upper case."""
    if not TICKER.match(entry.source_id):
        msg = f"{entry.key}: a SEC id is a ticker in upper case, such as AAPL or BRK-B"
        raise CatalogError(msg)


class Edgar:
    """GET against the SEC for one source, identified with the user's User-Agent."""

    def __init__(self, client: Client, source: str, user_agent: str) -> None:
        self._client = client
        self._source = source
        self._user_agent = user_agent

    def get(self, url: str) -> httpx.Response | None:
        """The answer of a SEC page, or None when the SEC answers 404."""
        response = self._client.get(
            self._source,
            url,
            headers={"User-Agent": self._user_agent},
            per_minute=REQUESTS_PER_MINUTE,
        )
        if response.status_code == FORBIDDEN:
            raise QuotaExhaustedError(BLOCKED)
        if response.status_code == NOT_FOUND:
            return None
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {self._client.scrub(response.text[:200])}"
            raise AnswerError(msg)
        return response

    def json(self, url: str) -> Any:
        """The JSON of a SEC page, or None when the SEC answers 404."""
        response = self.get(url)
        return None if response is None else response.json()

    def ciks(self) -> dict[str, str]:
        """Ticker -> CIK as ten digits, from the SEC's list of companies."""
        listed = self.json(TICKERS_URL)
        if listed is None:
            msg = "HTTP 404"
            raise AnswerError(msg)
        return {str(item["ticker"]).upper(): f"{int(item['cik_str']):010d}" for item in listed.values()}


def resolve_tickers(edgar: Edgar, requests: Sequence[Request]) -> dict[str, str] | FetchBatch:
    """The ticker list, or the batch that fails every request when the list cannot be read."""
    try:
        return edgar.ciks()
    except RateLimitedError as exc:
        raise QuotaExhaustedError(str(exc)) from exc
    except NetworkError as exc:
        return FetchBatch(failures=failures(requests, Outcome.NETWORK_ERROR, f"list of tickers: {exc}"))
    except (AnswerError, KeyError, TypeError, ValueError, AttributeError) as exc:
        reason = f"list of tickers: unexpected answer ({type(exc).__name__}: {exc})"
        return FetchBatch(failures=failures(requests, Outcome.SOURCE_ERROR, reason))


def unknown_ticker(entry: CatalogEntry) -> str:
    return f"the SEC lists no company with ticker {entry.source_id!r}"
