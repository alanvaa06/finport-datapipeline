"""Sharadar daily metrics: the valuation of one company, day by day, as Sharadar computes it.

One catalog id is one ticker. One call brings every day of the ticker in Sharadar's `daily` table,
on every run.

A row is told apart by its `ticker` (the catalog id) and its day (`period`). It holds `ev`,
`evebit`, `evebitda`, `marketcap`, `pb`, `pe` and `ps`, in Sharadar's units; `lastupdated` is not
stored. As for prices, the table has no `frequency` column, so the store builds no set of the
days it holds for a source that never reads it.

Sharadar does not say when it computed a day, so the table is not versioned: rows are dated by
when they were downloaded, and a day Sharadar restates is stored again, the row before it kept.
"""

import datetime
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
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
from data_pipeline.store.sources.sharadar import REQUESTS_PER_MINUTE, AnswerError, Sharadar, check_ticker, per_ticker

TABLE = "daily"
KEY_COLUMNS = ("ticker", "period")
VALUE_COLUMNS = ("ev", "evebit", "evebitda", "marketcap", "pb", "pe", "ps")
STALE_AFTER_DAYS = stale_after(Frequency.DAILY)

Row = dict[str, object]


def read_rows(items: Sequence[Mapping[str, Any]], ticker: str) -> tuple[Row, ...]:
    """The rows of a daily answer as table rows, each with the ticker that was asked for. A day
    that comes twice is an AnswerError: one of the two would be dropped without notice."""
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
                "ticker": ticker,
                "period": day.isoformat(),
                "date": day,
                **{column: number(item[column]) for column in VALUE_COLUMNS},
            }
        )
    return tuple(rows)


class SharadarDaily:
    name = "sharadar_daily"
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
            reason = f"Sharadar has no daily metrics for {entry.source_id!r}{since}"
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Daily metrics of {entry.source_id} (Sharadar)",
            rows=rows,
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            stale_after_days=STALE_AFTER_DAYS,
        )
        return FetchBatch(tables=(data,))
