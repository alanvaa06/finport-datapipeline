"""BLS (U.S. Bureau of Labor Statistics), Public Data API v2; key in BLS_API_KEY.

The API takes at most 50 series and 20 years per request, and 500 requests per day. Requests
are grouped by their start year and cut into groups of at most 50; one group is one batch.

A group is all or nothing. If the quota runs out while its windows are being downloaded, nothing
of that group is yielded: storing half a history would make the next sync believe the series is
up to date from its newest observation backwards.

A first load without `start` walks back in 20-year windows, and decides where to stop for each
series on its own: a series stops once a window brings it nothing after an earlier window did
(its history starts there), or once the API says it does not exist; a series that has not shown
data yet keeps walking back, down to EARLIEST_YEAR. Later windows ask only for the series still
walking, so what a series gets never depends on the series it is grouped with. A gap of 20 years
or more inside a series still ends its walk.

Annual averages (period codes M13 and Q05) are ignored: they would share a date with December.
"""

import datetime
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import (
    KeyRejectedError,
    NetworkError,
    PeriodError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import (
    UNSUPPORTED_FREQUENCY,
    HeldBy,
    failures,
    missing_key,
    number,
    reject_params,
    utc_today,
)
from data_pipeline.store.sources.repeats import repeated_period

URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
MAX_SERIES = 50
MAX_YEARS = 20
EARLIEST_YEAR = 1900  # the walk back never asks for years before this one
OK = 200
SUCCEEDED = "REQUEST_SUCCEEDED"
DOES_NOT_EXIST = "does not exist"  # BLS: "Series does not exist for Series <id>"
NO_DATA = "BLS returned no observations for this series"
SEASONALITY: Mapping[str, str] = {"seasonally adjusted": "SA", "not seasonally adjusted": "NSA"}
# first letter of a period code -> (frequency, how to spell the period for read_period)
KINDS: Mapping[str, tuple[Frequency, str]] = {
    "M": (Frequency.MONTHLY, "{year}-{number:02d}"),
    "Q": (Frequency.QUARTERLY, "{year}-Q{number}"),
    "A": (Frequency.ANNUAL, "{year}"),
}
LAST_REAL_NUMBER: Mapping[str, int] = {"M": 12, "Q": 4, "A": 1}  # M13 and Q05 are annual averages
PREFERENCE = ("M", "Q", "A")


class _GroupError(Exception):
    """The whole group failed for a reason that is neither the key nor the quota."""


def start_year(request: Request) -> int | None:
    """First year to ask for, or None when the source has to find where the series begins."""
    if request.since is not None:
        return request.since.year
    return request.entry.start.year if request.entry.start is not None else None


def groups(requests: Sequence[Request]) -> list[tuple[int | None, list[Request]]]:
    """Requests grouped by start year, each group cut into pieces of at most MAX_SERIES."""
    by_year: dict[int | None, list[Request]] = {}
    for request in requests:
        by_year.setdefault(start_year(request), []).append(request)
    return [
        (year, members[position : position + MAX_SERIES])
        for year, members in by_year.items()
        for position in range(0, len(members), MAX_SERIES)
    ]


def mentions(note: str, series_id: str) -> bool:
    """Whether a note of the API names this series: its whole id, not the start of a longer one
    (CUUR0000SA0 is not named by a note about CUUR0000SA0E1)."""
    return re.search(rf"(?<![A-Za-z0-9]){re.escape(series_id)}(?![A-Za-z0-9])", note) is not None


def absent(series_id: str, messages: Sequence[str]) -> bool:
    """Whether the API said that this series does not exist."""
    return any(mentions(note, series_id) and DOES_NOT_EXIST in note.lower() for note in messages)


def windows(first: int, last: int) -> list[tuple[int, int]]:
    """[first, last] cut into consecutive windows of at most MAX_YEARS years."""
    return [(year, min(year + MAX_YEARS - 1, last)) for year in range(first, last + 1, MAX_YEARS)]


class Bls:
    name = "bls"
    kind = Kind.SERIES
    requests_per_minute = 50
    daily_budget: int | None = 450  # the API allows 500; sync checks the budget between batches
    held_by: HeldBy = ()

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._key = credentials.get(keys.BLS)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            yield missing_key(requests, keys.BLS)
            return
        pieces = groups(requests)
        for position, (year, members) in enumerate(pieces):
            try:
                yield self._group(year, members)
            except KeyRejectedError as exc:
                rest = [request for _, later in pieces[position:] for request in later]
                yield FetchBatch(failures=failures(rest, Outcome.KEY_ERROR, str(exc)))
                return
            except RateLimitedError as exc:
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=failures(members, Outcome.NETWORK_ERROR, str(exc)))
            except _GroupError as exc:
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, str(exc)))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, reason))

    def _group(self, year: int | None, members: Sequence[Request]) -> FetchBatch:
        ids = [request.entry.source_id for request in members]
        points: dict[str, list[tuple[str, int, int, float]]] = {}
        titles: dict[str, Mapping[str, Any]] = {}
        messages: list[str] = []
        this_year = self._today().year
        if year is not None:
            for first, last in windows(year, this_year):
                self._window(ids, first, last, points, titles, messages)
        else:
            walking = list(ids)  # series whose first observation is not found yet
            started: set[str] = set()
            last = this_year
            while walking and last >= EARLIEST_YEAR:
                first = max(last - MAX_YEARS + 1, EARLIEST_YEAR)
                brought = self._window(walking, first, last, points, titles, messages)
                walking = [
                    series_id
                    for series_id in walking
                    if series_id in brought or (series_id not in started and not absent(series_id, messages))
                ]
                started |= brought
                last = first - 1
        series = []
        failed = []
        for request in members:
            result = self._series(request, points.get(request.entry.source_id, []), titles, messages)
            if isinstance(result, Failure):
                failed.append(result)
            else:
                series.append(result)
        return FetchBatch(tuple(series), tuple(failed))

    def _window(
        self,
        ids: Sequence[str],
        first: int,
        last: int,
        points: dict[str, list[tuple[str, int, int, float]]],
        titles: dict[str, Mapping[str, Any]],
        messages: list[str],
    ) -> set[str]:
        """Download one window into `points`. Returns the ids of the series it brought observations of."""
        response = self._client.post(
            self.name,
            URL,
            json={
                "seriesid": list(ids),
                "startyear": str(first),
                "endyear": str(last),
                "registrationkey": self._key or "",
                "catalog": True,
            },
            per_minute=self.requests_per_minute,
        )
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {self._client.excerpt(response.text, 300)}"
            raise _GroupError(msg)
        payload: dict[str, Any] = response.json()
        notes = [self._client.scrub(str(note)) for note in payload.get("message") or [] if note]
        messages.extend(note for note in notes if note not in messages)
        status = str(payload.get("status") or "")
        answered = (payload.get("Results") or {}).get("series") or []
        if status != SUCCEEDED and not any(item.get("data") for item in answered):
            # Anything but success that brings no data is a refusal, never "the series is empty".
            text = "; ".join(notes) or status or "the answer carries no status"
            if "daily threshold" in text.lower():
                raise QuotaExhaustedError(text)
            if "key" in text.lower():
                msg = f"BLS rejected the key: {text}"
                raise KeyRejectedError(msg)
            raise _GroupError(text if status in text else f"{status}: {text}")
        found: set[str] = set()
        for item in answered:
            series_id = str(item.get("seriesID") or "")
            if item.get("catalog"):
                titles.setdefault(series_id, item["catalog"])
            for point in item.get("data") or []:
                code = str(point["period"])
                points.setdefault(series_id, []).append(
                    (code[:1], int(point["year"]), int(code[1:]), number(point.get("value")))
                )
                found.add(series_id)
        return found

    def _series(
        self,
        request: Request,
        raw: Sequence[tuple[str, int, int, float]],
        titles: Mapping[str, Mapping[str, Any]],
        messages: Sequence[str],
    ) -> SeriesData | Failure:
        entry = request.entry
        if not raw:
            said = next((note for note in messages if mentions(note, entry.source_id)), NO_DATA)
            return Failure(entry, Outcome.NOT_FOUND, said)
        letters = {letter for letter, _, _, _ in raw}
        letter = next((candidate for candidate in PREFERENCE if candidate in letters), None)
        if letter is None:
            codes = ", ".join(sorted(letters))
            return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} (period codes {codes})")
        frequency, spelling = KINDS[letter]
        observations: list[Observation] = []
        for kind, year, order, value in raw:
            if kind != letter or order > LAST_REAL_NUMBER[letter]:
                continue
            period, day = read_period(spelling.format(year=year, number=order), frequency)
            observations.append(Observation(period, day, value))
        repeated = repeated_period(observations, frequency)
        if repeated:
            return Failure(entry, Outcome.SOURCE_ERROR, repeated)
        catalog = titles.get(entry.source_id, {})
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=str(catalog.get("series_title") or entry.source_id),
            frequency=frequency,
            seasonal_adjustment=SEASONALITY.get(str(catalog.get("seasonality") or "").strip().lower(), ""),
            observations=tuple(sorted(observations, key=lambda observation: observation.date)),
        )
