"""Banxico SIE: one call per series; token in the Bmx-Token header (BANXICO_TOKEN).

Dates are dd/mm/yyyy; "N/E" is missing; thousands separators are stripped. The frequency is the
catalog's when declared. Otherwise one more call asks for the series' metadata, which also
brings its unit.
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
    missing_key,
    number,
    per_request,
    reject_params,
    utc_today,
)

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
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=" ".join(str(series.get("titulo") or entry.source_id).split()),
            frequency=frequency,
            units=units,
            observations=read_data(series, frequency),
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
