"""Sharadar fundamentals: the indicators of one company, in Sharadar's six dimensions.

One catalog id is one ticker. One call brings every row of the ticker in Sharadar's
`fundamentals` table, on every run.

A row is told apart by its `ticker` (the catalog id), its `dimension` and its `calendardate`, the
calendar quarter or year it is about; `date` is that day. The indicators come wide, one column
each: the 105 of INDICATORS, in Sharadar's order, are read as numbers, whether they come as JSON
numbers or as text. A listed column an answer lacks is missing; a column Sharadar adds later is
not read until the list grows. `reportperiod`, `fiscalperiod` and `datekey` (Sharadar's `date` of
the row) are stored as text and never compared.

The table is versioned. An as-reported row (`ARQ`, `ARY`, `ART`) is known from the end of the day
of its `datekey`, the day of the filing. A most-recent row (`MRQ`, `MRY`, `MRT`) carries in
`datekey` the end of its period, not a day it was known: it is known from the end of the day of
its `lastupdated`, the latest day Sharadar says it touched the ticker.

Two costs follow. Sharadar stamps every row of a ticker with a new `lastupdated` when it updates
it, so each update stores another version of every most-recent row, with the same numbers. And an
as-reported row Sharadar corrects without a new `datekey` keeps the day of the original: `as_of`
a day between the two reads the correction.
"""

import datetime
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Kind, Outcome, Request, TableData
from data_pipeline.store.sources.base import HeldBy, missing_key, number, reject_params
from data_pipeline.store.sources.sharadar import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Sharadar,
    check_ticker,
    per_ticker,
    text,
)

TABLE = "fundamentals"
AS_REPORTED = ("ARQ", "ARY", "ART")
MOST_RECENT = ("MRQ", "MRY", "MRT")
DIMENSIONS = (*AS_REPORTED, *MOST_RECENT)
KEY_COLUMNS = ("ticker", "dimension", "calendardate")
INDICATORS = (
    "accoci", "assets", "assetsavg", "assetsc", "assetsnc", "assetturnover", "bvps", "capex", "cashneq",
    "cashnequsd", "cor", "consolinc", "currentratio", "de", "debt", "debtc", "debtnc", "debtusd", "deferredrev",
    "depamor", "deposits", "divyield", "dps", "ebit", "ebitda", "ebitdamargin", "ebitdausd", "ebitusd", "ebt",
    "eps", "epsdil", "epsusd", "equity", "equityavg", "equityusd", "ev", "evebit", "evebitda", "fcf", "fcfps",
    "fxusd", "gp", "grossmargin", "intangibles", "intexp", "invcap", "invcapavg", "inventory", "investments",
    "investmentsc", "investmentsnc", "liabilities", "liabilitiesc", "liabilitiesnc", "marketcap", "ncf",
    "ncfbus", "ncfcommon", "ncfdebt", "ncfdiv", "ncff", "ncfi", "ncfinv", "ncfo", "ncfx", "netinc", "netinccmn",
    "netinccmnusd", "netincdis", "netincnci", "netmargin", "opex", "opinc", "payables", "payoutratio", "pb", "pe",
    "pe1", "ppnenet", "prefdivis", "price", "ps", "ps1", "receivables", "retearn", "revenue", "revenueusd", "rnd",
    "roa", "roe", "roic", "ros", "sbcomp", "sgna", "sharefactor", "sharesbas", "shareswa", "shareswadil", "sps",
    "tangibles", "taxassets", "taxexp", "taxliabilities", "tbvps", "workingcapital",
)  # fmt: skip
VALUE_COLUMNS = INDICATORS
ATTRIBUTE_COLUMNS = ("reportperiod", "fiscalperiod", "datekey")
STALE_AFTER_DAYS = 200  # a quarter, the filing deadline, and slack, as for SEC XBRL facts

Row = dict[str, object]


def known_from(dimension: str, item: Mapping[str, Any]) -> datetime.datetime:
    """When a row is known: the end of the day of its `datekey` (Sharadar's `date`) for an
    as-reported row, of its `lastupdated` for a most-recent one."""
    if dimension not in DIMENSIONS:
        msg = f"a row whose dimension is not one of {', '.join(DIMENSIONS)}: {dimension!r}"
        raise AnswerError(msg)
    field = "date" if dimension in AS_REPORTED else "lastupdated"
    day = datetime.date.fromisoformat(str(item[field]))
    # Sharadar gives the day, not the hour: known from the end of that day, so no moment within it sees it early
    return datetime.datetime.combine(day, datetime.time.max, datetime.UTC)


def read_rows(items: Sequence[Mapping[str, Any]], ticker: str) -> tuple[Row, ...]:
    """The rows of a fundamentals answer as versioned table rows, each with the ticker that was
    asked for. A dimension and period that come twice, known from the same moment, are an
    AnswerError: nothing would tell the two apart."""
    rows: list[Row] = []
    seen: set[tuple[str, str, datetime.datetime]] = set()
    for item in items:
        dimension = str(item["dimension"])
        period = datetime.date.fromisoformat(str(item["calendardate"]))
        published_at = known_from(dimension, item)
        if (dimension, period.isoformat(), published_at) in seen:
            msg = f"the answer holds {dimension} {period.isoformat()} twice"
            raise AnswerError(msg)
        seen.add((dimension, period.isoformat(), published_at))
        rows.append(
            {
                "ticker": ticker,
                "dimension": dimension,
                "calendardate": period.isoformat(),
                "date": period,
                **{column: number(item.get(column)) for column in INDICATORS},
                "reportperiod": text(item.get("reportperiod")),
                "fiscalperiod": text(item.get("fiscalperiod")),
                "datekey": text(item.get("date")),
                "published_at": published_at,
            }
        )
    return tuple(rows)


class SharadarFundamentals:
    name = "sharadar_fundamentals"
    kind = Kind.TABLE
    requests_per_minute = REQUESTS_PER_MINUTE
    daily_budget: int | None = None
    held_by: HeldBy = ()

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._key = credentials.get(keys.SHARADAR)

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)
        check_ticker(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            yield missing_key(requests, keys.SHARADAR)
            return
        api = Sharadar(self._client, self.name, self._key)
        yield from per_ticker(requests, lambda request: self._ticker(api, request.entry))

    def _ticker(self, api: Sharadar, entry: CatalogEntry) -> FetchBatch:
        rows = read_rows(api.rows(TABLE, entry.source_id, entry.start), entry.source_id)
        if not rows:
            since = f" from {entry.start.isoformat()} on" if entry.start else ""
            reason = f"Sharadar has no fundamentals for {entry.source_id!r}{since}"
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Fundamentals of {entry.source_id} (Sharadar)",
            rows=rows,
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            stale_after_days=STALE_AFTER_DAYS,
            attribute_columns=ATTRIBUTE_COLUMNS,
            versioned=True,
        )
        return FetchBatch(tables=(data,))
