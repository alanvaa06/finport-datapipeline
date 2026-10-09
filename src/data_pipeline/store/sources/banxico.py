"""Banxico SIE: one call per series; token in the Bmx-Token header (BANXICO_TOKEN).

Dates are dd/mm/yyyy; "N/E" is missing; thousands separators are stripped. The frequency is the
catalog's when declared. Otherwise one more call asks for the series' metadata, which also
brings its unit.

A declared frequency is checked against the dates of the data, since Banxico dates a month, a
quarter or a year by its first day: a series declared monthly whose data is daily fails, rather
than keeping one value a month. A period that comes twice fails the series too.
"""

import datetime
from collections.abc import Callable, Iterator, Mapping, Sequence
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
OK = 200
NOT_FOUND = 404
PERIODICITY: Mapping[str, Frequency] = {
    "diaria": Frequency.DAILY,
    "semanal": Frequency.WEEKLY,
    "mensual": Frequency.MONTHLY,
    "trimestral": Frequency.QUARTERLY,
    "anual": Frequency.ANNUAL,
}
NO_DATA = "Banxico returned no data for this series"
# frequency -> (its period, the months a period starts in): Banxico dates a period by its first day
STARTS: Mapping[Frequency, tuple[str, frozenset[int]]] = {
    Frequency.MONTHLY: ("month", frozenset(range(1, 13))),
    Frequency.QUARTERLY: ("quarter", frozenset({1, 4, 7, 10})),
    Frequency.ANNUAL: ("year", frozenset({1})),
}
OWN_FREQUENCY = "declare the series' own frequency, or leave `frequency` out"


def _day(text: str) -> datetime.date:
    day, month, year = (int(part) for part in text.strip().split("/"))
    return datetime.date(year, month, day)


def misfit(series: Mapping[str, Any], declared: Frequency) -> str | None:
    """Why the dates of the data do not fit the frequency the catalog declares, or None."""
    days = [_day(str(row["fecha"])) for row in series["datos"]]
    if declared in STARTS:
        name, months = STARTS[declared]
        odd = next((day for day in days if day.day != 1 or day.month not in months), None)
        if odd is not None:
            return (
                f"the catalog declares frequency {declared.value}, but the data is dated {odd:%d/%m/%Y}, "
                f"which does not start a {name}: {OWN_FREQUENCY}"
            )
    elif len(days) > 1 and all(day.day == 1 for day in days):
        return (
            f"the catalog declares frequency {declared.value}, but every date of the data is the first of a "
            f"month: {OWN_FREQUENCY}"
        )
    return None


def read_data(series: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for row in series["datos"]:
        period, day = read_period(str(row["fecha"]), frequency)
        observations.append(Observation(period, day, number(row["dato"])))
    return tuple(sorted(observations, key=lambda observation: observation.date))


class Banxico:
    name = "banxico"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None
    held_by: HeldBy = ()

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._token = credentials.get(keys.BANXICO)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._token:
            yield missing_key(requests, keys.BANXICO)
            return
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        frequency = entry.frequency
        units = ""
        if frequency is None:
            meta = self._get(f"{URL}/{entry.source_id}", entry)
            if isinstance(meta, Failure):
                return meta
            reported = str(meta.get("periodicidad") or "")
            frequency = PERIODICITY.get(reported.strip().lower())
            if frequency is None:
                return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {reported!r}")
            units = str(meta.get("unidad") or "")
        url = f"{URL}/{entry.source_id}/datos"
        if request.since is not None:
            url += f"/{request.since.isoformat()}/{self._today().isoformat()}"
        series = self._get(url, entry)
        if isinstance(series, Failure):
            return series
        if "datos" not in series:
            return Failure(entry, Outcome.NOT_FOUND, NO_DATA)
        wrong = None if entry.frequency is None else misfit(series, entry.frequency)
        observations = read_data(series, frequency)
        wrong = wrong or repeated_period(observations, frequency)
        if wrong:
            return Failure(entry, Outcome.SOURCE_ERROR, wrong)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=" ".join(str(series.get("titulo") or entry.source_id).split()),
            frequency=frequency,
            units=units,
            observations=observations,
        )

    def _get(self, url: str, entry: CatalogEntry) -> Mapping[str, Any] | Failure:
        """The first series of the answer, or the failure the answer amounts to."""
        response = self._client.get(
            self.name,
            url,
            headers={"Bmx-Token": self._token or "", "Accept": "application/json"},
            per_minute=self.requests_per_minute,
        )
        text = self._client.excerpt(response.text, 300)
        if response.status_code != OK:
            if "token" in text.lower():
                msg = f"Banxico rejected the token: {text}"
                raise KeyRejectedError(msg)
            outcome = Outcome.NOT_FOUND if response.status_code == NOT_FOUND else Outcome.SOURCE_ERROR
            return Failure(entry, outcome, f"HTTP {response.status_code}: {text}")
        payload: dict[str, Any] = response.json()
        if "error" in payload:
            return Failure(entry, Outcome.NOT_FOUND, self._client.excerpt(str(payload["error"]), 300))
        found = payload["bmx"]["series"]
        if not found:
            return Failure(entry, Outcome.NOT_FOUND, NO_DATA)
        first: Mapping[str, Any] = found[0]
        return first
