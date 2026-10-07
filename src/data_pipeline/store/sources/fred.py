"""FRED (St. Louis Fed): one series per request; key in FRED_API_KEY.

Two calls per series: /series (title, units, frequency, seasonal adjustment) and
/series/observations. "." = missing.
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
from data_pipeline.store.sources.base import failures, number, per_request, reject_params

URL = "https://api.stlouisfed.org/fred"
OK = 200
FREQUENCIES: Mapping[str, Frequency] = {
    "D": Frequency.DAILY,
    "W": Frequency.WEEKLY,
    "M": Frequency.MONTHLY,
    "Q": Frequency.QUARTERLY,
    "A": Frequency.ANNUAL,
}


def read_observations(payload: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for row in payload["observations"]:
        period, date = read_period(str(row["date"]), frequency)
        observations.append(Observation(period, date, number(row["value"])))
    return tuple(sorted(observations, key=lambda observation: observation.date))


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
        meta = self._get("series", base, entry)
        if isinstance(meta, Failure):
            return meta
        info = meta["seriess"][0]
        short = str(info.get("frequency_short") or "")
        frequency = FREQUENCIES.get(short)
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"unsupported frequency {short!r}")
        params = dict(base)
        if request.since is not None:
            params["observation_start"] = request.since.isoformat()
        data = self._get("series/observations", params, entry)
        if isinstance(data, Failure):
            return data
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=str(info.get("title") or entry.source_id),
            frequency=frequency,
            units=str(info.get("units") or ""),
            seasonal_adjustment=str(info.get("seasonal_adjustment_short") or ""),
            observations=read_observations(data, frequency),
        )

    def _get(self, path: str, params: Mapping[str, str], entry: CatalogEntry) -> dict[str, Any] | Failure:
        response = self._client.get(
            self.name,
            f"{URL}/{path}",
            params=params,
            per_minute=self.requests_per_minute,
        )
        if response.status_code == OK:
            payload: dict[str, Any] = response.json()
            return payload
        text = self._client.scrub(response.text[:300])
        if "api_key" in text:
            msg = f"FRED rejected the key: {text}"
            raise KeyRejectedError(msg)
        if "does not exist" in text:
            return Failure(entry, Outcome.NOT_FOUND, text)
        return Failure(entry, Outcome.SOURCE_ERROR, f"HTTP {response.status_code}: {text}")
