"""UN Comtrade: goods trade by HS product. Key in the `Ocp-Apim-Subscription-Key` header.

One catalog id is one reporting country, written as ISO 3166 alpha-3 (`MEX`). Its table holds
value and weight by partner, flow, product and period, annual and monthly, and the HS level
of each row.

A table has no "since". Each request carries the periods already stored with their partner,
flow and level, and the source asks each partner for what it is missing (a period counts as
stored when every chosen flow has it at the chosen level) plus a revision window: the last 2
years and the last 12 closed months. Periods go 12 to a call, the API's limit (4 at 6 digits,
to stay under its cap of 100,000 rows an answer); one call is one batch. Nothing remembers
pending calls: the next run works out again what is missing, so a run stopped by the quota, or
an entry given a new partner or flow, is filled in by itself. A partner and flow that had no
trade in a period are asked for it again on every run, like a period not yet published.

A table holds one HS level. An entry whose `level` differs from the one stored fails without a
call: mixing 2- and 4-digit products in one table would count trade twice in any sum.

Only the total of a key is kept. Comtrade also answers with breakdowns by mode of transport,
customs procedure and second partner; those rows are dropped.

A weight of 0 on a row with trade is a weight not reported, and is stored as missing: summed as
zero it would cut weight totals short and make value per kilogram infinite.
"""

import dataclasses
import datetime
import functools
import importlib.resources
import json
import math
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import (
    CatalogError,
    KeyRejectedError,
    NetworkError,
    PeriodError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Frequency, Kind, Outcome, Request, TableData
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import failures, missing_key, number, reject_params, utc_today

URL = "https://comtradeapi.un.org/data/v1/get/C/{frequency}/HS"
KEY_HEADER = "Ocp-Apim-Subscription-Key"
REPORTERS_FILE = "comtrade_reporters.json"
WORLD = "WLD"
WORLD_CODE = "0"
LEVELS: Mapping[str, int] = {"AG2": 12, "AG4": 12, "AG6": 4}  # level -> periods in one call
FLOWS = ("X", "M")
FIELDS = ("level", "partners", "flows", "annual_from", "months")
DEFAULT_LEVEL = "AG2"
DEFAULT_ANNUAL_FROM = 2000
DEFAULT_MONTHS = 75
ANNUAL_WINDOW = 2
MONTHLY_WINDOW = 12
MONTHS_IN_YEAR = 12
MAX_ROWS = 100_000
STALE_AFTER_DAYS = 190  # Comtrade publishes a month two to five months late
KEY_COLUMNS = ("reporter", "partner", "flow", "product", "frequency", "period")
VALUE_COLUMNS = ("value_usd", "weight_kg")
ATTRIBUTE_COLUMNS = ("level",)
HELD_BY = ("partner", "flow", "level")  # what sync adds to each stored (frequency, period)
# The total of a key, not its breakdowns by mode of transport, customs procedure or second partner.
TOTAL_ONLY: Mapping[str, str] = {"motCode": "0", "customsCode": "C00", "partner2Code": "0"}
OK = 200
UNAUTHORIZED = 401
FORBIDDEN = 403

Row = dict[str, object]


class _AnswerError(Exception):
    """Comtrade answered, but not with the rows that were asked for."""


@dataclasses.dataclass(frozen=True, slots=True)
class Settings:
    """The fields of a Comtrade entry, with their defaults filled in."""

    level: str
    partners: tuple[str, ...]
    flows: tuple[str, ...]
    annual_from: int
    months: int


@dataclasses.dataclass(frozen=True, slots=True)
class Query:
    """One call: up to 12 periods of one frequency for one partner."""

    frequency: Frequency
    partner: str
    periods: tuple[str, ...]  # as stored: "2024", "2024-06"


@functools.cache
def reporter_codes() -> Mapping[str, str]:
    """ISO3 -> Comtrade's own reporter code (the United States is 842, not 840)."""
    text = importlib.resources.files(__package__).joinpath(REPORTERS_FILE).read_text(encoding="utf-8")
    return {str(iso): str(code) for iso, code in json.loads(text).items()}


def _texts(entry: CatalogEntry, field: str, default: tuple[str, ...], allowed: Collection[str]) -> tuple[str, ...]:
    value = entry.params.get(field)
    if value is None:
        return default
    if not isinstance(value, list | tuple) or not value:
        msg = f"{entry.key}: '{field}' must be a non-empty list"
        raise CatalogError(msg)
    items = tuple(dict.fromkeys(str(item).upper() for item in value))
    unknown = sorted(set(items) - set(allowed))
    if unknown:
        msg = f"{entry.key}: unknown value(s) in '{field}': {', '.join(unknown)}"
        raise CatalogError(msg)
    return items


def _whole(entry: CatalogEntry, field: str, default: int) -> int:
    value = entry.params.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        msg = f"{entry.key}: '{field}' must be a whole number, zero or more"
        raise CatalogError(msg)
    return value


def settings(entry: CatalogEntry) -> Settings:
    """Read and check the entry's fields. Raises CatalogError on anything this source rejects."""
    reject_params(entry, FIELDS)
    codes = reporter_codes()
    if entry.source_id not in codes:
        msg = f"{entry.key}: unknown reporter {entry.source_id!r}; a Comtrade id is an ISO3 code such as MEX"
        raise CatalogError(msg)
    level = str(entry.params.get("level", DEFAULT_LEVEL)).upper()
    if level not in LEVELS:
        msg = f"{entry.key}: 'level' must be one of {', '.join(LEVELS)}"
        raise CatalogError(msg)
    return Settings(
        level=level,
        partners=_texts(entry, "partners", (WORLD,), {WORLD, *codes}),
        flows=_texts(entry, "flows", FLOWS, FLOWS),
        annual_from=_whole(entry, "annual_from", DEFAULT_ANNUAL_FROM),
        months=_whole(entry, "months", DEFAULT_MONTHS),
    )


def closed_months(today: datetime.date, count: int) -> list[str]:
    """The last `count` months that have ended, oldest first, as `2024-06`."""
    last = today.year * MONTHS_IN_YEAR + today.month - 2  # the month before today's, counted from year 0
    numbers = range(last - count + 1, last + 1)
    return [f"{number // MONTHS_IN_YEAR:04d}-{number % MONTHS_IN_YEAR + 1:02d}" for number in numbers]


def held_by_partner(held: Collection[tuple[str, ...]], chosen: Settings, partner: str) -> set[tuple[str, str]]:
    """The (frequency, period) pairs the store holds for `partner` in every chosen flow, at the
    chosen level. `held` is what sync sends: (frequency, period, partner, flow, level). A row
    stored before the level was recorded has an empty level and counts as the chosen one."""
    by_flow: dict[str, set[tuple[str, str]]] = {flow: set() for flow in chosen.flows}
    for frequency, period, owner, flow, level in held:
        if owner == partner and flow in by_flow and level in (chosen.level, ""):
            by_flow[flow].add((frequency, period))
    first, *others = by_flow.values()
    return first.intersection(*others)


def stored_levels(held: Collection[tuple[str, ...]]) -> set[str]:
    """The HS levels recorded in the rows of a stored table."""
    return {level for *_, level in held if level}


def wanted_periods(
    held: Collection[tuple[str, str]],
    chosen: Settings,
    today: datetime.date,
) -> dict[Frequency, list[str]]:
    """What to ask one partner for: every period it does not hold, plus the revision window.
    `held` are the partner's (frequency, period) pairs. Oldest first."""
    years = [f"{year:04d}" for year in range(chosen.annual_from, today.year)]
    months = closed_months(today, chosen.months)
    annual = {year for year in years if (Frequency.ANNUAL.value, year) not in held} | set(years[-ANNUAL_WINDOW:])
    monthly = {month for month in months if (Frequency.MONTHLY.value, month) not in held}
    monthly |= set(months[-MONTHLY_WINDOW:])
    return {Frequency.ANNUAL: sorted(annual), Frequency.MONTHLY: sorted(monthly)}


def queries(held: Collection[tuple[str, ...]], chosen: Settings, today: datetime.date) -> list[Query]:
    """The calls of one reporter: per partner, annual then monthly, in blocks of periods."""
    size = LEVELS[chosen.level]
    result: list[Query] = []
    for partner in chosen.partners:
        for frequency, periods in wanted_periods(held_by_partner(held, chosen, partner), chosen, today).items():
            result.extend(
                Query(frequency, partner, tuple(periods[start : start + size]))
                for start in range(0, len(periods), size)
            )
    return result


def is_total(row: Mapping[str, Any]) -> bool:
    """A row without these fields (older answers) counts as the total."""
    return (
        str(row.get("motCode", 0)) == "0"
        and str(row.get("customsCode", "C00")) == "C00"
        and str(row.get("partner2Code", 0)) == "0"
    )


def weight(item: Mapping[str, Any]) -> float:
    """Net weight in kg; 0 with a positive trade value is a weight not reported (NaN)."""
    kilograms = number(item.get("netWgt"))
    value = number(item.get("primaryValue"))
    return math.nan if kilograms == 0 and value > 0 else kilograms


def read_rows(payload: Mapping[str, Any], reporter: str, query: Query, chosen: Settings) -> tuple[Row, ...]:
    """The answer of one call as table rows; breakdown rows and other flows are dropped."""
    rows: list[Row] = []
    for item in payload.get("data") or []:
        flow = str(item["flowCode"])
        if not is_total(item) or flow not in chosen.flows:
            continue
        period, day = read_period(str(item["period"]), query.frequency)
        rows.append(
            {
                "reporter": reporter,
                "partner": query.partner,
                "flow": flow,
                "product": str(item["cmdCode"]),
                "frequency": query.frequency.value,
                "period": period,
                "date": day,
                "value_usd": number(item.get("primaryValue")),
                "weight_kg": weight(item),
                "level": chosen.level,
            }
        )
    return tuple(rows)


class Comtrade:
    name = "comtrade"
    kind = Kind.TABLE
    requests_per_minute = 30
    daily_budget: int | None = 450  # the free tier allows 500 calls a day
    held_by = HELD_BY

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._key = credentials.get(keys.COMTRADE)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        settings(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            yield missing_key(requests, keys.COMTRADE)
            return
        for position, request in enumerate(requests):
            try:
                yield from self._reporter(request, self._key)
            except KeyRejectedError as exc:
                yield FetchBatch(failures=failures(requests[position:], Outcome.KEY_ERROR, str(exc)))
                return
            except RateLimitedError as exc:
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=(Failure(request.entry, Outcome.NETWORK_ERROR, str(exc)),))
            except _AnswerError as exc:
                yield FetchBatch(failures=(Failure(request.entry, Outcome.SOURCE_ERROR, str(exc)),))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=(Failure(request.entry, Outcome.SOURCE_ERROR, reason),))

    def _reporter(self, request: Request, key: str) -> Iterator[FetchBatch]:
        """One batch per call. A call that fails ends this reporter's run; what it would have
        brought is still missing from the store, so the next run asks for it again."""
        entry = request.entry
        chosen = settings(entry)
        other = sorted(stored_levels(request.held) - {chosen.level})
        if other:
            msg = (
                f"the stored table holds HS level {', '.join(other)}, not {chosen.level}, and one table never "
                f"mixes levels: set `level` back, or delete tables/comtrade/{entry.source_id}.parquet in the "
                f"store to load {chosen.level} from the start"
            )
            raise _AnswerError(msg)
        for query in queries(request.held, chosen, self._today()):
            rows = self._rows(entry.source_id, chosen, query, key)
            table = TableData(
                entry=entry,
                key=entry.key,
                name=f"Goods trade of {entry.source_id} by HS product ({chosen.level})",
                rows=rows,
                key_columns=KEY_COLUMNS,
                value_columns=VALUE_COLUMNS,
                stale_after_days=STALE_AFTER_DAYS if chosen.months else None,
                attribute_columns=ATTRIBUTE_COLUMNS,
            )
            yield FetchBatch(tables=(table,))

    def _rows(self, reporter: str, chosen: Settings, query: Query, key: str) -> tuple[Row, ...]:
        codes = reporter_codes()
        params = {
            "reporterCode": codes[reporter],
            "period": ",".join(period.replace("-", "") for period in query.periods),
            "partnerCode": WORLD_CODE if query.partner == WORLD else codes[query.partner],
            "cmdCode": chosen.level,
            "flowCode": ",".join(chosen.flows),
            **TOTAL_ONLY,
        }
        response = self._client.get(
            self.name,
            URL.format(frequency=query.frequency.value),
            params=params,
            headers={KEY_HEADER: key},
            per_minute=self.requests_per_minute,
        )
        text = self._client.excerpt(response.text, 300)
        if response.status_code == UNAUTHORIZED:
            msg = f"Comtrade rejected the key: {text}"
            raise KeyRejectedError(msg)
        if response.status_code == FORBIDDEN:
            msg = f"HTTP 403: {text}"
            raise QuotaExhaustedError(msg)
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {text}"
            raise _AnswerError(msg)
        payload = response.json()
        if payload.get("error"):
            raise _AnswerError(self._client.excerpt(str(payload["error"]), 300))
        if len(payload.get("data") or []) >= MAX_ROWS:
            msg = f"the answer reached Comtrade's cap of {MAX_ROWS} rows and may be cut short: narrow the entry"
            raise _AnswerError(msg)
        return read_rows(payload, reporter, query, chosen)
