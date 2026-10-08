"""DBnomics (https://db.nomics.world): a keyless aggregator of the IMF, the World Bank, the BIS,
the OECD, Eurostat and many others.

A series id is `<provider>/<dataset>/<series>`, for example
`Eurostat/prc_hicp_midx/M.I15.CP00.EA`. Observations arrive as two parallel arrays, `period`
and `value`. The endpoint has no date filter, so `since` is applied here after the download.
"""

import math
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.credentials import Credentials
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
from data_pipeline.store.sources.base import DECLARE_FREQUENCY, number, per_request, reject_params
from data_pipeline.store.sources.repeats import repeated_period

URL = "https://api.db.nomics.world/v22/series"
OK = 200
NOT_FOUND = 404
NO_SERIES = "DBnomics has no series with this id"
FREQUENCIES: Mapping[str, Frequency] = {
    "annual": Frequency.ANNUAL,
    "quarterly": Frequency.QUARTERLY,
    "monthly": Frequency.MONTHLY,
    "weekly": Frequency.WEEKLY,
    "daily": Frequency.DAILY,
}


def read_observations(document: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for text, raw in zip(document.get("period") or [], document.get("value") or [], strict=True):
        period, day = read_period(str(text), frequency)
        value = number(raw)
        observations.append(Observation(period, day, value if math.isfinite(value) else math.nan))
    return tuple(sorted(observations, key=lambda observation: observation.date))


class Dbnomics:
    name = "dbnomics"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None

    def __init__(self, client: Client, credentials: Credentials) -> None:  # keyless: credentials are not read
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        response = self._client.get(
            self.name,
            URL,
            params={"series_ids": entry.source_id, "observations": "1"},
            per_minute=self.requests_per_minute,
        )
        if response.status_code != OK:
            outcome = Outcome.NOT_FOUND if response.status_code == NOT_FOUND else Outcome.SOURCE_ERROR
            return Failure(entry, outcome, f"HTTP {response.status_code}: {self._client.excerpt(response.text, 300)}")
        documents = response.json()["series"]["docs"]
        if not documents:
            return Failure(entry, Outcome.NOT_FOUND, NO_SERIES)
        document = documents[0]
        reported = str(document.get("@frequency") or "")
        frequency = FREQUENCIES.get(reported.strip().lower()) or entry.frequency
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"{DECLARE_FREQUENCY} (DBnomics said {reported!r})")
        observations = read_observations(document, frequency)
        repeated = repeated_period(observations, frequency)
        if repeated:
            return Failure(entry, Outcome.SOURCE_ERROR, repeated)
        if request.since is not None:
            observations = tuple(item for item in observations if item.date >= request.since)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=str(document.get("series_name") or entry.source_id),
            frequency=frequency,
            observations=observations,
        )
