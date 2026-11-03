"""INEGI's indicator API. The token is part of the URL path and comes from INEGI_TOKEN.

A catalog entry may set two fields. `bank` names the database to ask: BIE-BISE when left out,
which answers for BIE ids such as 737121, or BISE for ids such as 6207136901. `area` is the
geographic code, 00 (the whole country) when left out.

The API takes no date range: it always sends the full history, latest period first. The rows
before `since` are dropped here, once the answer is read.
"""

from collections.abc import Iterator, Mapping, Sequence
from operator import attrgetter
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
    HeldBy,
    missing_key,
    number,
    per_request,
    reject_params,
)
from data_pipeline.store.sources.repeats import repeated_period

URL = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
ACCEPTED_FIELDS = ("bank", "area")
BANK_WHEN_UNSET = "BIE-BISE"
AREA_WHEN_UNSET = "00"
SUCCESS = 200
TOKEN_STATUSES = (401, 403)
MATCHED_NOTHING = "ErrorCode:100"  # how INEGI says that the query found nothing
EXCERPT = 300
# the CL_FREQ code INEGI puts in FREQ -> the store's frequency
BY_FREQ_CODE: Mapping[str, Frequency] = {
    "3": Frequency.ANNUAL,
    "4": Frequency.QUARTERLY,
    "8": Frequency.MONTHLY,
}


def _settings(entry: CatalogEntry) -> tuple[str, str]:
    """(bank, area) of an entry, with the defaults filled in."""
    params = entry.params
    return str(params.get("bank", BANK_WHEN_UNSET)), str(params.get("area", AREA_WHEN_UNSET))


def _observation(row: Mapping[str, Any], frequency: Frequency) -> Observation:
    label, last_day = read_period(str(row["TIME_PERIOD"]), frequency)
    return Observation(label, last_day, number(row.get("OBS_VALUE")))


def _to_series(request: Request, block: Mapping[str, Any]) -> SeriesData | Failure:
    """The series INEGI described in `block`, or why it cannot be stored."""
    entry = request.entry
    code = str(block.get("FREQ") or "")
    frequency = BY_FREQ_CODE.get(code, entry.frequency)
    if frequency is None:
        return Failure(entry, Outcome.SOURCE_ERROR, f"{DECLARE_FREQUENCY} (INEGI FREQ {code!r})")
    rows = sorted((_observation(row, frequency) for row in block["OBSERVATIONS"]), key=attrgetter("date"))
    clash = repeated_period(rows, frequency)
    if clash:
        return Failure(entry, Outcome.SOURCE_ERROR, clash)
    start = request.since
    kept = rows if start is None else [row for row in rows if row.date >= start]
    return SeriesData(
        entry=entry,
        key=entry.key,
        name=entry.source_id,
        frequency=frequency,
        units=f"INEGI unit {block.get('UNIT', '')}".strip(),
        observations=tuple(kept),
    )


class Inegi:
    held_by: HeldBy = ()
    daily_budget: int | None = None
    requests_per_minute = 60
    kind = Kind.SERIES
    name = "inegi"

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._token = credentials.get(keys.INEGI)
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry, ACCEPTED_FIELDS)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        token = self._token
        if token:
            yield from per_request(requests, lambda request: self._series(request, token))
        else:
            yield missing_key(requests, keys.INEGI)

    def _series(self, request: Request, token: str) -> SeriesData | Failure:
        entry = request.entry
        bank, area = _settings(entry)
        address = "/".join((URL, entry.source_id, "es", area, "false", bank, "2.0", token))
        response = self._client.get(self.name, address, params={"type": "json"}, per_minute=self.requests_per_minute)
        if response.status_code == SUCCESS:
            return _to_series(request, response.json()["Series"][0])
        detail = self._client.excerpt(response.text, EXCERPT)
        if MATCHED_NOTHING in detail:
            return Failure(entry, Outcome.NOT_FOUND, f"{bank}: no results ({MATCHED_NOTHING})")
        if response.status_code in TOKEN_STATUSES or "token" in detail.lower():
            msg = f"INEGI rejected the token: {detail}"
            raise KeyRejectedError(msg)
        return Failure(entry, Outcome.SOURCE_ERROR, f"HTTP {response.status_code}: {detail}")
