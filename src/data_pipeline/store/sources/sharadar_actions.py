"""Sharadar corporate actions: the dividends, splits and other events of one ticker.

One catalog id is one ticker, of a stock or of an ETF: Sharadar keeps the actions of both in
one table. One call brings every action of a ticker, on every run.

An action is told apart by its day, Sharadar's word for it (`dividend`, `split`, ...), the
other company it involves, if any, and `item`: `1`, `2`, ... among the actions that share the
other three. They are numbered by ascending value, so the numbering does not depend on the
order of the answer. Without `item` a second dividend on the same day would be dropped without
notice.

A ticker without actions is not a failure: its table is empty.
"""

import datetime
import math
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, FetchBatch, Kind, Request, TableData
from data_pipeline.store.sources.base import HeldBy, missing_key, number, reject_params
from data_pipeline.store.sources.sharadar import REQUESTS_PER_MINUTE, Sharadar, check_ticker, per_ticker, text

TABLE = "actions"
KEY_COLUMNS = ("day", "action", "contraticker", "item")
VALUE_COLUMNS = ("value",)
ATTRIBUTE_COLUMNS = ("name", "contraname")

Row = dict[str, object]


def read_rows(items: Sequence[Mapping[str, Any]]) -> tuple[Row, ...]:
    """The rows of an actions answer as table rows, each with its `item`."""
    alike: dict[tuple[str, str, str], list[tuple[float, Row]]] = {}
    for item in items:
        day = datetime.date.fromisoformat(str(item["date"]))
        action, contraticker = str(item["action"]), text(item.get("contraticker"))
        value = number(item.get("value"))
        row: Row = {
            "day": day.isoformat(),
            "action": action,
            "contraticker": contraticker,
            "date": day,
            "value": value,
            "name": text(item.get("name")),
            "contraname": text(item.get("contraname")),
        }
        alike.setdefault((day.isoformat(), action, contraticker), []).append((value, row))
    rows: list[Row] = []
    for group in alike.values():
        # a missing value goes last; the sort is stable, so equal values keep the answer's order
        ordered = sorted(group, key=lambda pair: (math.isnan(pair[0]), 0.0 if math.isnan(pair[0]) else pair[0]))
        rows.extend({**row, "item": str(position)} for position, (_, row) in enumerate(ordered, start=1))
    return tuple(rows)


class SharadarActions:
    name = "sharadar_actions"
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
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Corporate actions of {entry.source_id} (Sharadar)",
            rows=read_rows(api.rows(TABLE, entry.source_id, entry.start)),
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            attribute_columns=ATTRIBUTE_COLUMNS,
        )
        return FetchBatch(tables=(data,))
