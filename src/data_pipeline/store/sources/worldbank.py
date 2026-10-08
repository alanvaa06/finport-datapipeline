"""World Bank API v2 (WDI and its other databases). No key.

A series id is `<indicator>/<economy>`, for example `NY.GDP.MKTP.CD/CHL`. The economy is the
World Bank's own code: ISO 3166 alpha-3 for countries, and codes such as `EMU` for aggregates.

Requests are grouped by indicator: one call asks for every requested economy of that indicator
(`country/CHL;ARG/indicator/...`), so one indicator is one batch. When the API refuses the call
because one economy is invalid, each economy is asked on its own so that one fails alone.
"""

import datetime
from collections.abc import Callable, Iterator, Sequence
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
    failures,
    number,
    reject_params,
    utc_today,
)

URL = "https://api.worldbank.org/v2/country/{economies}/indicator/{indicator}"
PER_PAGE = "20000"
OK = 200
NO_ROWS = "the World Bank returned no observations for this economy"

Row = dict[str, Any]


class _IndicatorError(Exception):
    """The call for an indicator failed for a reason that is not one bad economy."""


def split_id(entry: CatalogEntry) -> tuple[str, str]:
    """(indicator, economy) of a series id such as `NY.GDP.MKTP.CD/CHL`."""
    indicator, separator, economy = entry.source_id.rpartition("/")
    if not separator or not indicator or not economy:
        msg = f"{entry.key}: a World Bank id is '<indicator>/<economy>', such as NY.GDP.MKTP.CD/CHL"
        raise CatalogError(msg)
    return indicator, economy.upper()


def by_indicator(requests: Sequence[Request]) -> dict[str, list[Request]]:
    grouped: dict[str, list[Request]] = {}
    for request in requests:
        grouped.setdefault(split_id(request.entry)[0], []).append(request)
    return grouped


def belongs(row: Row, economy: str) -> bool:
    country = row.get("country") or {}
    return economy in (str(row.get("countryiso3code") or "").upper(), str(country.get("id") or "").upper())


class WorldBank:
    name = "worldbank"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None

    def __init__(
        self,
        client: Client,
        credentials: Credentials,  # no key: the argument exists so every source is built alike
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        del credentials
        self._client = client
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)
        split_id(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        for indicator, members in by_indicator(requests).items():
            try:
                yield self._indicator(indicator, members)
            except RateLimitedError as exc:
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=failures(members, Outcome.NETWORK_ERROR, str(exc)))
            except _IndicatorError as exc:
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, str(exc)))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, reason))

    def _indicator(self, indicator: str, members: Sequence[Request]) -> FetchBatch:
        economies = [split_id(request.entry)[1] for request in members]
        since = None if any(request.since is None for request in members) else min(
            request.since for request in members if request.since is not None
        )
        answer = self._rows(indicator, economies, since)
        refused: dict[str, str] = {}
        if isinstance(answer, str):
            # One economy (or the indicator) is invalid: ask one by one so that it fails alone.
            rows: list[Row] = []
            for economy in economies:
                single = self._rows(indicator, [economy], since)
                if isinstance(single, str):
                    refused[economy] = single
                else:
                    rows.extend(single)
        else:
            rows = answer
        series = []
        failed = []
        for request, economy in zip(members, economies, strict=True):
            own = [row for row in rows if belongs(row, economy)]
            if not own:
                failed.append(Failure(request.entry, Outcome.NOT_FOUND, refused.get(economy, NO_ROWS)))
                continue
            result = self._series(request.entry, own)
            if isinstance(result, Failure):
                failed.append(result)
            else:
                series.append(result)
        return FetchBatch(tuple(series), tuple(failed))

    def _rows(self, indicator: str, economies: Sequence[str], since: datetime.date | None) -> list[Row] | str:
        """Every row of the indicator for these economies, or the API's message when it refuses."""
        rows: list[Row] = []
        page = 1
        while True:
            params = {"format": "json", "per_page": PER_PAGE, "page": str(page)}
            if since is not None:
                params["date"] = f"{since.year}:{self._today().year}"
            response = self._client.get(
                self.name,
                URL.format(economies=";".join(economies), indicator=indicator),
                params=params,
                per_minute=self.requests_per_minute,
            )
            if response.status_code != OK:
                msg = f"HTTP {response.status_code}: {self._client.excerpt(response.text, 200)}"
                raise _IndicatorError(msg)
            payload = response.json()
            head = payload[0]
            if "message" in head:
                return "; ".join(str(item.get("value") or item) for item in head["message"])
            if len(payload) > 1 and payload[1]:
                rows.extend(payload[1])
            if page >= int(head.get("pages") or 1):
                return rows
            page += 1

    def _series(self, entry: CatalogEntry, rows: Sequence[Row]) -> SeriesData | Failure:
        text = str(rows[0]["date"])
        frequency = infer_frequency(text)
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {text!r}")
        observations = {}
        for row in rows:
            period, day = read_period(str(row["date"]), frequency)
            observations[period] = Observation(period, day, number(row.get("value")))
        first = rows[0]
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=f"{first['indicator']['value']} - {first['country']['value']}",
            frequency=frequency,
            units=str(first.get("unit") or ""),
            country=str(first.get("countryiso3code") or ""),
            observations=tuple(sorted(observations.values(), key=lambda observation: observation.date)),
        )
