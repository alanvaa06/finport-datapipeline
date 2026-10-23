"""Sharadar tickers: the record Sharadar keeps of one ticker, once per table it appears in.

One catalog id is one ticker, of a stock or of an ETF. One call brings its rows of Sharadar's
`tickers` table, on every run: one row per Sharadar table that has the ticker (`stocks`, `funds`,
`fundamentals`, ...).

A row is told apart by its `ticker` (the catalog id) and its `table`. `permaticker`, Sharadar's
permanent id, is the one number; everything else Sharadar says of the ticker (name, exchange,
sector, the first and last days it has, ...) is stored as sent, as text, with `N/A` and `null`
empty, and never compared.

The table is versioned: a row is known from the end of the day of its `lastupdated`, and `date` is
that day. A record that changes, a new sector or the delisted flag, comes with a new `lastupdated`:
the store keeps it as a new version, and `as_of` reads the record as it was. The same record sent
again stores nothing. A ticker's record has no period to grow old by, so it is never stale.
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

TABLE = "tickers"
KEY_COLUMNS = ("ticker", "table")
VALUE_COLUMNS = ("permaticker",)
ATTRIBUTE_COLUMNS = (
    "name", "exchange", "isdelisted", "category", "cusips", "siccode", "sicsector", "sicindustry", "figi",
    "famaindustry", "sector", "industry", "scalemarketcap", "scalerevenue", "relatedtickers", "currency",
    "location", "lastupdated", "firstadded", "firstpricedate", "lastpricedate", "firstquarter", "lastquarter",
    "secfilings", "companysite",
)  # fmt: skip

Row = dict[str, object]


def read_rows(items: Sequence[Mapping[str, Any]], ticker: str) -> tuple[Row, ...]:
    """The rows of a tickers answer as versioned table rows, each with the ticker that was asked
    for. A table that comes twice is an AnswerError: one of the two would be dropped without
    notice."""
    rows: list[Row] = []
    seen: set[str] = set()
    for item in items:
        table = text(item["table"])
        if not table:
            msg = f"a row without the table it describes: {item['table']!r}"
            raise AnswerError(msg)
        if table in seen:
            msg = f"the answer holds the table {table!r} twice"
            raise AnswerError(msg)
        seen.add(table)
        day = datetime.date.fromisoformat(str(item["lastupdated"]))
        rows.append(
            {
                "ticker": ticker,
                "table": table,
                "date": day,
                "permaticker": number(item.get("permaticker")),
                **{column: text(item.get(column)) for column in ATTRIBUTE_COLUMNS},
                # Sharadar gives the day, not the hour: known from the end of that day
                "published_at": datetime.datetime.combine(day, datetime.time.max, datetime.UTC),
            }
        )
    return tuple(rows)


class SharadarTickers:
    name = "sharadar_tickers"
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
            reason = f"Sharadar has no ticker record for {entry.source_id!r}{since}"
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Ticker record of {entry.source_id} (Sharadar)",
            rows=rows,
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            attribute_columns=ATTRIBUTE_COLUMNS,
            versioned=True,
        )
        return FetchBatch(tables=(data,))
