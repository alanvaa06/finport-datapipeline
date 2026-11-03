"""Banxico's SIE API, one series per call. The token goes in the Bmx-Token header and comes from
BANXICO_TOKEN.

Banxico writes dates as dd/mm/yyyy, marks a missing value with "N/E" and groups thousands with
commas. When the catalog declares no frequency, a first call reads the series' description,
which gives both its periodicity and its unit.

Banxico dates a month, a quarter or a year by the first day of it, so the dates tell whether a
declared frequency is right. A series declared monthly whose dates fall on other days is
refused instead of being cut to one value a month; one declared daily or weekly whose dates are
all firsts of a month is refused too. A period that appears twice fails the series as well.
"""

import datetime
from collections.abc import Callable, Iterator, Mapping, Sequence
from operator import attrgetter
from typing import Any

import httpx

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
from data_pipeline.store.sources.base import (
    UNSUPPORTED_FREQUENCY,
    HeldBy,
    missing_key,
    number,
    per_request,
    reject_params,
    utc_today,
)
from data_pipeline.store.sources.repeats import repeated_period

URL = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
SUCCESS = 200
ABSENT = 404
EXCERPT = 300
# each frequency of the store as Banxico spells its `periodicidad` (compared lower-cased)
SPANISH: Mapping[Frequency, str] = {
    Frequency.DAILY: "diaria",
    Frequency.WEEKLY: "semanal",
    Frequency.MONTHLY: "mensual",
    Frequency.QUARTERLY: "trimestral",
    Frequency.ANNUAL: "anual",
}
FROM_SPANISH = {word: frequency for frequency, word in SPANISH.items()}
# the frequencies Banxico dates by the first day of the period, and how many months one period spans
MONTHS_SPANNED: Mapping[Frequency, int] = {Frequency.MONTHLY: 1, Frequency.QUARTERLY: 3, Frequency.ANNUAL: 12}
PERIOD_NAME: Mapping[Frequency, str] = {
    Frequency.MONTHLY: "month",
    Frequency.QUARTERLY: "quarter",
    Frequency.ANNUAL: "year",
}
EMPTY = "Banxico returned no data for this series"
ADVICE = "declare the series' own frequency, or leave `frequency` out"


def _calendar_day(text: str) -> datetime.date:
    """A dd/mm/yyyy date."""
    day, month, year = text.strip().split("/")
    return datetime.date(int(year), int(month), int(day))


def _misdeclared(declared: Frequency, rows: Sequence[Mapping[str, Any]]) -> str | None:
    """Why the dates in `rows` contradict the frequency the catalog declares; None when they agree."""
    days = [_calendar_day(str(row["fecha"])) for row in rows]
    lead = f"the catalog declares frequency {declared.value}, but"
    months = MONTHS_SPANNED.get(declared)
    if months is None:
        if len(days) > 1 and {day.day for day in days} == {1}:
            return f"{lead} every date of the data is the first of a month: {ADVICE}"
        return None
    stray = [day for day in days if day.day != 1 or (day.month - 1) % months]
    if not stray:
        return None
    return f"{lead} the data is dated {stray[0]:%d/%m/%Y}, which does not start a {PERIOD_NAME[declared]}: {ADVICE}"


def _point(row: Mapping[str, Any], frequency: Frequency) -> Observation:
    label, last_day = read_period(str(row["fecha"]), frequency)
    return Observation(label, last_day, number(row["dato"]))


class Banxico:
    held_by: HeldBy = ()
    daily_budget: int | None = None
    requests_per_minute = 60
    kind = Kind.SERIES
    name = "banxico"

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._today = today
        self._token = credentials.get(keys.BANXICO)
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if self._token:
            yield from per_request(requests, self._series)
        else:
            yield missing_key(requests, keys.BANXICO)

    def _series(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        declared = entry.frequency
        if declared is None:
            described = self._describe(entry)
            if isinstance(described, Failure):
                return described
            frequency, units = described
        else:
            frequency, units = declared, ""
        path = f"{URL}/{entry.source_id}/datos"
        if request.since is not None:
            path = f"{path}/{request.since.isoformat()}/{self._today().isoformat()}"
        found = self._first_series(path, entry)
        if isinstance(found, Failure):
            return found
        if "datos" not in found:
            return Failure(entry, Outcome.NOT_FOUND, EMPTY)
        rows = found["datos"]
        problem = _misdeclared(declared, rows) if declared is not None else None
        observations = tuple(sorted((_point(row, frequency) for row in rows), key=attrgetter("date")))
        problem = problem or repeated_period(observations, frequency)
        if problem:
            return Failure(entry, Outcome.SOURCE_ERROR, problem)
        title = str(found.get("titulo") or entry.source_id)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=" ".join(title.split()),
            frequency=frequency,
            units=units,
            observations=observations,
        )

    def _describe(self, entry: CatalogEntry) -> tuple[Frequency, str] | Failure:
        """The frequency and unit Banxico reports for a series."""
        found = self._first_series(f"{URL}/{entry.source_id}", entry)
        if isinstance(found, Failure):
            return found
        periodicity = str(found.get("periodicidad") or "")
        frequency = FROM_SPANISH.get(periodicity.strip().lower())
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {periodicity!r}")
        return frequency, str(found.get("unidad") or "")

    def _first_series(self, url: str, entry: CatalogEntry) -> Mapping[str, Any] | Failure:
        """The first series Banxico answers with, or the failure its answer means."""
        headers = {"Accept": "application/json", "Bmx-Token": self._token or ""}
        response = self._client.get(self.name, url, headers=headers, per_minute=self.requests_per_minute)
        if response.status_code == SUCCESS:
            return self._unwrap(entry, response.json())
        return self._refusal(entry, response)

    def _unwrap(self, entry: CatalogEntry, body: Mapping[str, Any]) -> Mapping[str, Any] | Failure:
        """A 200 answer either carries an `error` or lists the series under bmx/series."""
        if "error" in body:
            return Failure(entry, Outcome.NOT_FOUND, self._client.excerpt(str(body["error"]), EXCERPT))
        listed: Sequence[Mapping[str, Any]] = body["bmx"]["series"]
        if listed:
            return listed[0]
        return Failure(entry, Outcome.NOT_FOUND, EMPTY)

    def _refusal(self, entry: CatalogEntry, response: httpx.Response) -> Failure:
        detail = self._client.excerpt(response.text, EXCERPT)
        if "token" in detail.lower():
            msg = f"Banxico rejected the token: {detail}"
            raise KeyRejectedError(msg)
        status = response.status_code
        outcome = Outcome.NOT_FOUND if status == ABSENT else Outcome.SOURCE_ERROR
        return Failure(entry, outcome, f"HTTP {status}: {detail}")
