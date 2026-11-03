"""World Bank API v2: the WDI and the bank's other databases. It needs no key.

A catalog id joins an indicator and an economy with a slash, as in `NY.GDP.MKTP.CD/CHL`. The
economy is written the World Bank's way: the ISO 3166 alpha-3 code of a country, or the bank's
own code for an aggregate such as `EMU` (the euro area).

The API takes several economies in one path, separated by semicolons
(`country/CHL;ARG/indicator/...`), so the requests of one indicator share a call and come back
as one batch. A single invalid economy makes the API refuse the whole call; the economies are
then asked for one at a time, so that only the invalid one fails.
"""

import dataclasses
import datetime
from collections.abc import Callable, Iterator, Mapping, Sequence
from operator import attrgetter
from typing import Any

from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError, NetworkError, PeriodError, QuotaExhaustedError, RateLimitedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import infer_frequency, read_period
from data_pipeline.store.sources.base import (
    UNSUPPORTED_FREQUENCY,
    HeldBy,
    failures,
    number,
    reject_params,
    utc_today,
)
from data_pipeline.store.sources.repeats import repeated_period

URL = "https://api.worldbank.org/v2/country/{economies}/indicator/{indicator}"
ROWS_PER_PAGE = "20000"
SUCCESS = 200
EXCERPT = 200
NOTHING_FOR_ECONOMY = "the World Bank returned no observations for this economy"
UNREADABLE = (KeyError, IndexError, TypeError, ValueError, PeriodError)

Row = dict[str, Any]


class _WholeCallError(Exception):
    """An indicator's call failed as a whole, not because of one economy."""


@dataclasses.dataclass(frozen=True)
class _Refused:
    """The API declined the call and said why."""

    reason: str


def split_id(entry: CatalogEntry) -> tuple[str, str]:
    """Read a series id such as `NY.GDP.MKTP.CD/CHL` as (indicator, economy in upper case)."""
    head, slash, tail = entry.source_id.rpartition("/")
    if slash and head and tail:
        return head, tail.upper()
    msg = f"{entry.key}: a World Bank id is '<indicator>/<economy>', such as NY.GDP.MKTP.CD/CHL"
    raise CatalogError(msg)


def by_indicator(requests: Sequence[Request]) -> dict[str, list[Request]]:
    """The requests under their indicator, in the order each indicator first appears."""
    groups: dict[str, list[Request]] = {}
    for request in requests:
        indicator = split_id(request.entry)[0]
        if indicator not in groups:
            groups[indicator] = []
        groups[indicator].append(request)
    return groups


def _is_of(economy: str, row: Row) -> bool:
    """Whether a row is about `economy`, by its ISO code or by the bank's own id."""
    codes = {str(row.get("countryiso3code") or "").upper(), str((row.get("country") or {}).get("id") or "").upper()}
    return economy in codes


def _earliest(group: Sequence[Request]) -> datetime.date | None:
    """The earliest `since` of the group; None when any request wants the whole history."""
    starts = [request.since for request in group if request.since is not None]
    return min(starts) if len(starts) == len(group) else None


def _as_series(entry: CatalogEntry, rows: Sequence[Row]) -> SeriesData | Failure:
    """The rows of one economy as a series, or why they cannot be stored."""
    first = rows[0]
    shape = str(first["date"])
    frequency = infer_frequency(shape)
    if frequency is None:
        return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {shape!r}")
    points = [Observation(*read_period(str(row["date"]), frequency), number(row.get("value"))) for row in rows]
    clash = repeated_period(points, frequency)
    if clash:
        return Failure(entry, Outcome.SOURCE_ERROR, clash)
    points.sort(key=attrgetter("date"))
    return SeriesData(
        entry=entry,
        key=entry.key,
        name=f"{first['indicator']['value']} - {first['country']['value']}",
        frequency=frequency,
        units=str(first.get("unit") or ""),
        country=str(first.get("countryiso3code") or ""),
        observations=tuple(points),
    )


def _economy_result(
    entry: CatalogEntry, economy: str, rows: Sequence[Row], reasons: Mapping[str, str]
) -> SeriesData | Failure:
    """What the rows give for one requested economy: its series, or why there is none."""
    own = [row for row in rows if _is_of(economy, row)]
    if own:
        return _as_series(entry, own)
    return Failure(entry, Outcome.NOT_FOUND, reasons.get(economy, NOTHING_FOR_ECONOMY))


class WorldBank:
    held_by: HeldBy = ()
    daily_budget: int | None = None
    requests_per_minute = 60
    kind = Kind.SERIES
    name = "worldbank"

    def __init__(
        self,
        client: Client,
        credentials: Credentials,  # unused: taken only so that all sources are built the same way
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        del credentials
        self._today = today
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)
        split_id(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        for indicator, group in by_indicator(requests).items():
            yield self._guarded(indicator, group)

    def _guarded(self, indicator: str, group: Sequence[Request]) -> FetchBatch:
        """One indicator's batch; a failure of its call fails each of its requests."""
        try:
            return self._batch(indicator, group)
        except RateLimitedError as exc:
            raise QuotaExhaustedError(str(exc)) from exc
        except NetworkError as exc:
            return FetchBatch(failures=failures(group, Outcome.NETWORK_ERROR, str(exc)))
        except _WholeCallError as exc:
            return FetchBatch(failures=failures(group, Outcome.SOURCE_ERROR, str(exc)))
        except UNREADABLE as exc:
            reason = f"unexpected answer ({type(exc).__name__}: {exc})"
            return FetchBatch(failures=failures(group, Outcome.SOURCE_ERROR, reason))

    def _batch(self, indicator: str, group: Sequence[Request]) -> FetchBatch:
        economies = [split_id(request.entry)[1] for request in group]
        since = _earliest(group)
        years = None if since is None else f"{since.year}:{self._today().year}"
        rows, reasons = self._gather(indicator, economies, years)
        results = [
            _economy_result(request.entry, economy, rows, reasons)
            for request, economy in zip(group, economies, strict=True)
        ]
        return FetchBatch(
            series=tuple(result for result in results if isinstance(result, SeriesData)),
            failures=tuple(result for result in results if isinstance(result, Failure)),
        )

    def _gather(self, indicator: str, economies: Sequence[str], years: str | None) -> tuple[list[Row], dict[str, str]]:
        """The rows of every economy, and the API's reason for each economy it refused."""
        together = self._all_pages(indicator, economies, years)
        if not isinstance(together, _Refused):
            return together, {}
        rows: list[Row] = []
        reasons: dict[str, str] = {}
        for economy in economies:
            alone = self._all_pages(indicator, [economy], years)
            if isinstance(alone, _Refused):
                reasons[economy] = alone.reason
            else:
                rows += alone
        return rows, reasons

    def _all_pages(self, indicator: str, economies: Sequence[str], years: str | None) -> list[Row] | _Refused:
        """Every row the API holds for these economies, page after page."""
        address = URL.format(economies=";".join(economies), indicator=indicator)
        collected: list[Row] = []
        number_of_pages = page = 1
        while page <= number_of_pages:
            query = {"format": "json", "per_page": ROWS_PER_PAGE, "page": str(page)}
            if years is not None:
                query["date"] = years
            response = self._client.get(self.name, address, params=query, per_minute=self.requests_per_minute)
            if response.status_code != SUCCESS:
                msg = f"HTTP {response.status_code}: {self._client.excerpt(response.text, EXCERPT)}"
                raise _WholeCallError(msg)
            answer = response.json()
            meta = answer[0]  # paging, or the API's message when it declines
            if "message" in meta:
                return _Refused("; ".join(str(note.get("value") or note) for note in meta["message"]))
            if len(answer) > 1 and answer[1]:
                collected.extend(answer[1])
            number_of_pages = int(meta.get("pages") or 1)
            page += 1
        return collected
