"""Sharadar daily prices: one table per ticker, as Sharadar publishes it.

One catalog id is one ticker (`AAPL`, `BRK.B`). Stocks and ETFs live in two tables of
Sharadar's, `stocks` and `funds`: an entry says which with its `table` field, `stocks` when it
says nothing.

One call brings the whole history of a ticker, on every run: there is no "since". After a run
the latest version of every day is what Sharadar publishes that day. An entry with a `start`
asks from that day on: the days stored before it are not asked again and keep the values they had.

A day is told apart by `period` alone. The table has no `frequency` column on purpose: with one,
the store would build for every ticker, on every run, the set of the days it holds, for a source
that never reads it.

`open`, `high`, `low`, `close` and `volume` come adjusted for splits, and Sharadar restates
them on every earlier day when a ticker splits again: the next run stores those days once more,
and the rows before stay. `closeunadj` is the close as it traded and never changes. Sharadar's
`closeadj`, the close adjusted for dividends too, is not stored: it is restated over the whole
history on every dividend. Nor is `lastupdated`. Nothing is derived: a series adjusted for
dividends is the consumer's work, from `close` and the dividends of `sharadar_actions`.
"""

import datetime
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Outcome,
    Request,
    TableData,
    stale_after,
)
from data_pipeline.store.sources.base import HeldBy, missing_key, number, reject_params
from data_pipeline.store.sources.sharadar import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Sharadar,
    check_ticker,
    per_ticker,
)

TABLES = ("stocks", "funds")
DEFAULT_TABLE = "stocks"
FIELDS = ("table",)
KEY_COLUMNS = ("period",)
VALUE_COLUMNS = ("open", "high", "low", "close", "volume", "closeunadj")
STALE_AFTER_DAYS = stale_after(Frequency.DAILY)

Row = dict[str, object]


def table_of(entry: CatalogEntry) -> str:
    """The table of Sharadar's an entry asks: its `table` field, `stocks` without one. Raises
    CatalogError on any other field or value."""
    reject_params(entry, FIELDS)
    table = str(entry.params.get("table", DEFAULT_TABLE))
    if table not in TABLES:
        msg = f"{entry.key}: 'table' must be one of {', '.join(TABLES)}"
        raise CatalogError(msg)
    return table


def read_rows(items: Sequence[Mapping[str, Any]]) -> tuple[Row, ...]:
    """The rows of a price answer as table rows. A day that comes twice is an AnswerError: one
    of the two would be dropped without notice."""
    rows: list[Row] = []
    seen: set[datetime.date] = set()
    for item in items:
        day = datetime.date.fromisoformat(str(item["date"]))
        if day in seen:
            msg = f"the answer holds the day {day.isoformat()} twice"
            raise AnswerError(msg)
        seen.add(day)
        rows.append(
            {
                "period": day.isoformat(),
                "date": day,
                **{column: number(item[column]) for column in VALUE_COLUMNS},
            }
        )
    return tuple(rows)


class SharadarPrices:
    name = "sharadar_prices"
    kind = Kind.TABLE
    requests_per_minute = REQUESTS_PER_MINUTE
    daily_budget: int | None = None
    held_by: HeldBy = ()

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._key = credentials.get(keys.SHARADAR)

    def validate(self, entry: CatalogEntry) -> None:
        check_ticker(entry)
        table_of(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            yield missing_key(requests, keys.SHARADAR)
            return
        api = Sharadar(self._client, self.name, self._key)
        for table in TABLES:  # a refusal is about one table: the entries of the other are still asked
            asked = [request for request in requests if table_of(request.entry) == table]
            yield from per_ticker(asked, lambda request: self._ticker(api, request.entry))

    def _ticker(self, api: Sharadar, entry: CatalogEntry) -> FetchBatch:
        table = table_of(entry)
        rows = read_rows(api.rows(table, entry.source_id, entry.start))
        if not rows:
            hint = "; an ETF needs `table: funds`" if table == DEFAULT_TABLE else ""
            since = f" from {entry.start.isoformat()} on" if entry.start else ""
            reason = f"Sharadar has no prices for {entry.source_id!r} in its table {table!r}{since}{hint}"
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Daily prices of {entry.source_id} (Sharadar {table})",
            rows=rows,
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            stale_after_days=STALE_AFTER_DAYS,
            attrs={"table": table},
        )
        return FetchBatch(tables=(data,))
