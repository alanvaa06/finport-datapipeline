"""INEGI indicators API; token in the URL path (INEGI_TOKEN).

Catalog fields of this source: `bank` (BIE-BISE by default, which serves the BIE indicators such
as 737121; BISE for the BISE ids such as 6207136901) and `area` (00, national, by default).
INEGI returns every period, newest first, so `since` is applied here after the download.
"""

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
from data_pipeline.store.sources.base import (
    DECLARE_FREQUENCY,
    missing_key,
    number,
    per_request,
    reject_params,
)

URL = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
DEFAULT_BANK = "BIE-BISE"
NATIONAL_AREA = "00"
PARAMS = ("bank", "area")
OK = 200
UNAUTHORIZED = frozenset({401, 403})
NO_RESULTS = "ErrorCode:100"
# INEGI's CL_FREQ codes
FREQUENCIES: Mapping[str, Frequency] = {
    "8": Frequency.MONTHLY,
    "4": Frequency.QUARTERLY,
    "3": Frequency.ANNUAL,
}


def read_observations(series: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for row in series["OBSERVATIONS"]:
        period, day = read_period(str(row["TIME_PERIOD"]), frequency)
        observations.append(Observation(period, day, number(row.get("OBS_VALUE"))))
    return tuple(sorted(observations, key=lambda observation: observation.date))


class Inegi:
    name = "inegi"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._token = credentials.get(keys.INEGI)

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry, PARAMS)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._token:
            yield missing_key(requests, keys.INEGI)
            return
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        bank = str(entry.params.get("bank", DEFAULT_BANK))
        area = str(entry.params.get("area", NATIONAL_AREA))
        response = self._client.get(
            self.name,
            f"{URL}/{entry.source_id}/es/{area}/false/{bank}/2.0/{self._token}",
            params={"type": "json"},
            per_minute=self.requests_per_minute,
        )
        text = self._client.scrub(response.text[:300])
        if response.status_code != OK:
            if NO_RESULTS in text:
                return Failure(entry, Outcome.NOT_FOUND, f"{bank}: no results ({NO_RESULTS})")
            if response.status_code in UNAUTHORIZED or "token" in text.lower():
                msg = f"INEGI rejected the token: {text}"
                raise KeyRejectedError(msg)
            return Failure(entry, Outcome.SOURCE_ERROR, f"HTTP {response.status_code}: {text}")
        series = response.json()["Series"][0]
        code = str(series.get("FREQ") or "")
        frequency = FREQUENCIES.get(code) or entry.frequency
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"{DECLARE_FREQUENCY} (INEGI FREQ {code!r})")
        observations = read_observations(series, frequency)
        if request.since is not None:
            observations = tuple(item for item in observations if item.date >= request.since)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=entry.source_id,
            frequency=frequency,
            units=f"INEGI unit {series.get('UNIT', '')}".strip(),
            observations=observations,
        )
