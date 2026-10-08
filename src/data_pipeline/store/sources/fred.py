"""FRED (St. Louis Fed): one series per request; key in FRED_API_KEY.

Two calls per series: /series (title, units, frequency, seasonal adjustment) and
/series/observations. "." = missing.

Observations are asked for with every vintage (ALFRED): each version of a period comes with
the day FRED first published it, `realtime_start`, so the store can read the series as it was
known on any past day, before the store existed. Asked from `since`, the real-time period also
starts at `since`, which keeps a daily series under FRED's limit. A period published before it
began (a projection) then comes back cut at `since`, dated later than it was known; the store
drops such a row when it repeats the version it already holds.

FRED serves at most MAX_VINTAGES vintage dates in one call. A series with more (a daily series
asked for its whole history) is asked for in windows of real time, from /series/vintagedates;
a window FRED still refuses is split in two.
FRED cuts each row at the start of its window, so a value repeated with a later date is the same
version and is dropped. A series that ALFRED does not keep is asked for once more without
vintages: its rows carry no publication day, and the store dates them by when it fetched them.
"""

import dataclasses
import datetime
import math
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import KeyRejectedError
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
from data_pipeline.store.sources.base import failures, number, per_request, reject_params
from data_pipeline.store.sources.repeats import repeated_period

URL = "https://api.stlouisfed.org/fred"
OK = 200
FREQUENCIES: Mapping[str, Frequency] = {
    "D": Frequency.DAILY,
    "W": Frequency.WEEKLY,
    "M": Frequency.MONTHLY,
    "Q": Frequency.QUARTERLY,
    "A": Frequency.ANNUAL,
}
EARLIEST = "1776-07-04"  # FRED's own start of real time: every vintage
LATEST = "9999-12-31"
MAX_VINTAGES = 2000  # vintage dates FRED serves in one call
NOT_IN_ALFRED = "does not exist in ALFRED"
TOO_MANY_VINTAGES = "exceeds the maximum number of vintage dates"
ONE_DAY = datetime.timedelta(days=1)
UNDATED = datetime.datetime.min.replace(tzinfo=datetime.UTC)  # sorts a row without a publication day first

Row = Mapping[str, Any]


@dataclasses.dataclass(frozen=True)
class _Refusal:
    """FRED answered with an error. `text` is already scrubbed."""

    status: int
    text: str


def _same(before: float, after: float) -> bool:
    return (math.isnan(before) and math.isnan(after)) or before == after


def read_observations(rows: Sequence[Row], frequency: Frequency, *, dated: bool) -> tuple[Observation, ...]:
    """Rows -> observations, oldest period first and, within a period, oldest version first.

    With `dated`, each row is one version published on its `realtime_start`, known from the end
    of that day (UTC): FRED gives the day, not the hour, and a release comes out during the day, so
    an earlier moment could see it before it was out. A version whose value repeats the one before
    it is dropped.
    """
    observations = []
    for row in rows:
        period, date = read_period(str(row["date"]), frequency)
        published = None
        if dated:
            day = datetime.date.fromisoformat(str(row["realtime_start"]))
            published = datetime.datetime.combine(day, datetime.time.max, datetime.UTC)
        observations.append(Observation(period, date, number(row["value"]), published_at=published))
    observations.sort(key=lambda item: (item.date, item.published_at or UNDATED))
    kept: list[Observation] = []
    for observation in observations:
        if dated and kept and kept[-1].period == observation.period and _same(kept[-1].value, observation.value):
            continue
        kept.append(observation)
    return tuple(kept)


def _day_before(date: str) -> str:
    return (datetime.date.fromisoformat(date) - ONE_DAY).isoformat()


def _windows(dates: Sequence[str]) -> list[tuple[Sequence[str], str]]:
    """Back-to-back windows of real time, each with at most MAX_VINTAGES vintage dates: (the
    vintage dates of the window, the window's last day)."""
    chunks = [dates[start : start + MAX_VINTAGES] for start in range(0, len(dates), MAX_VINTAGES)]
    ends = [_day_before(chunk[0]) for chunk in chunks[1:]]
    return list(zip(chunks, [*ends, LATEST], strict=True))


class Fred:
    name = "fred"
    kind = Kind.SERIES
    requests_per_minute = 100  # assumed, not verified against FRED's documentation
    daily_budget: int | None = None

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._key = credentials.get(keys.FRED)

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            reason = keys.missing_message(keys.FRED)
            yield FetchBatch(failures=failures(requests, Outcome.KEY_ERROR, reason))
            return
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        base = {"series_id": entry.source_id, "api_key": self._key or "", "file_type": "json"}
        meta = self._call("series", base)
        if isinstance(meta, _Refusal):
            return self._failure(entry, meta)
        info = meta["seriess"][0]
        short = str(info.get("frequency_short") or "")
        frequency = FREQUENCIES.get(short)
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"unsupported frequency {short!r}")
        params = dict(base)
        if request.since is not None:
            params["observation_start"] = request.since.isoformat()
        start = request.since.isoformat() if request.since is not None else EARLIEST
        rows = self._vintages(base, params, start)
        dated = True
        if isinstance(rows, _Refusal) and NOT_IN_ALFRED in rows.text:
            rows = self._pages("series/observations", params, "observations")
            dated = False
        if isinstance(rows, _Refusal):
            return self._failure(entry, rows)
        observations = read_observations(rows, frequency, dated=dated)
        repeated = repeated_period(observations, frequency)
        if repeated:
            return Failure(entry, Outcome.SOURCE_ERROR, repeated)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=str(info.get("title") or entry.source_id),
            frequency=frequency,
            units=str(info.get("units") or ""),
            seasonal_adjustment=str(info.get("seasonal_adjustment_short") or ""),
            observations=observations,
        )

    def _vintages(self, base: Mapping[str, str], params: Mapping[str, str], start: str) -> list[Row] | _Refusal:
        """Every version of the observations, published from `start` on."""
        every = {**params, "realtime_start": start, "realtime_end": LATEST}
        rows = self._pages("series/observations", every, "observations")
        if not isinstance(rows, _Refusal) or TOO_MANY_VINTAGES not in rows.text:
            return rows
        dates = self._pages("series/vintagedates", {**base, "realtime_start": start}, "vintage_dates")
        if isinstance(dates, _Refusal):
            return dates
        found: list[Row] = []
        for chunk, last in _windows([str(date) for date in dates]):
            window = self._window(params, chunk, last)
            if isinstance(window, _Refusal):
                return window
            found.extend(window)
        return found

    def _window(self, params: Mapping[str, str], dates: Sequence[str], last: str) -> list[Row] | _Refusal:
        """The rows of one window of real time. FRED counts some vintage dates that
        /series/vintagedates does not list (seen 2026-10-07: 2001 in a window of 2000 listed), so a
        window it still refuses is split in two."""
        window = {**params, "realtime_start": dates[0], "realtime_end": last}
        rows = self._pages("series/observations", window, "observations")
        if not isinstance(rows, _Refusal) or TOO_MANY_VINTAGES not in rows.text or len(dates) < 2:
            return rows
        half = len(dates) // 2
        first = self._window(params, dates[:half], _day_before(dates[half]))
        if isinstance(first, _Refusal):
            return first
        second = self._window(params, dates[half:], last)
        if isinstance(second, _Refusal):
            return second
        return [*first, *second]

    def _pages(self, path: str, params: Mapping[str, str], field: str) -> list[Any] | _Refusal:
        """Every item of `field`, following `offset` while the answer counts more than it brought."""
        items: list[Any] = []
        while True:
            page = self._call(path, {**params, "offset": str(len(items))} if items else params)
            if isinstance(page, _Refusal):
                return page
            brought = list(page[field])
            items.extend(brought)
            if not brought or len(items) >= int(page.get("count", len(items))):
                return items

    def _call(self, path: str, params: Mapping[str, str]) -> dict[str, Any] | _Refusal:
        response = self._client.get(
            self.name,
            f"{URL}/{path}",
            params=params,
            per_minute=self.requests_per_minute,
        )
        if response.status_code == OK:
            payload: dict[str, Any] = response.json()
            return payload
        text = self._client.excerpt(response.text, 300)
        if "api_key" in text:
            msg = f"FRED rejected the key: {text}"
            raise KeyRejectedError(msg)
        return _Refusal(response.status_code, text)

    @staticmethod
    def _failure(entry: CatalogEntry, refusal: _Refusal) -> Failure:
        if "does not exist" in refusal.text:
            return Failure(entry, Outcome.NOT_FOUND, refusal.text)
        return Failure(entry, Outcome.SOURCE_ERROR, f"HTTP {refusal.status}: {refusal.text}")
