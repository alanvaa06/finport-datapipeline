"""Sharadar corporate actions: the dividends, splits and other events of one ticker.

One catalog id is one ticker, of a stock or of an ETF: both are asked of Sharadar's `actions`
table. One call brings every action of a ticker, on every run.

An action is told apart by its `period` (its day, as `YYYY-MM-DD`), Sharadar's word for it
(`dividend`, `split`, ...), the other company it involves, if any, and `item`: `1`, `2`, ...
among the actions that share the other three. They are numbered by ascending value, then by the
names, so the numbering does not depend on the order of the answer. Without `item` a second
dividend on the same day would be dropped without notice.

`item` is text, like every key. `name` and `contraname` are stored as sent with the row and
never compared: a row keeps the names it came with until its value changes.

The store never deletes, and what describes an action is part of its key: an action whose day
Sharadar corrects, or that it stops sending, would stay in the table as a current row next to
the new one, and a sum over the table would count it twice. So after each answer the source adds,
for every stored action the answer no longer has, a row with the same key, no value and
`withdrawn` 1. Every row Sharadar sends has `withdrawn` 0. A reader asks for `withdrawn=0`; the
rows before are not touched, and `as_of` still reads them. An action that comes back is stored
again with `withdrawn` 0. A mark that still holds is sent again on every run and stores nothing.

The store tells a source what it holds only of a table with a `frequency` and a `period`: that
is why `period` is in the key and why `frequency` is too, always `D`, though nothing reads it.
For the same reason the table carries its own threshold, `NEVER_STALE`: a daily table would be
called stale after ten days, and actions come when they come.

An answer speaks only for the days it covers, from its earliest action on. An action stored
before that day is left alone, and an answer without actions marks nothing. Two reasons: an
answer may be shorter than what is stored with nothing withdrawn, when a key that does not give
the whole history is answered with a recent part of it instead of an error, and when the `start`
of an entry moves forward. The price: if Sharadar withdraws the earliest action of a ticker, or
moves it to a later day, it is not noticed.

A ticker without actions is not a failure: its entry is ok in the index and no table is written.
"""

import datetime
import math
from collections.abc import Collection, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, FetchBatch, Frequency, Kind, Request, TableData
from data_pipeline.store.sources.base import HeldBy, missing_key, number, reject_params
from data_pipeline.store.sources.sharadar import REQUESTS_PER_MINUTE, Sharadar, check_ticker, per_ticker, text

TABLE = "actions"
KEY_COLUMNS = ("frequency", "period", "action", "contraticker", "item")
VALUE_COLUMNS = ("value", "withdrawn")
ATTRIBUTE_COLUMNS = ("name", "contraname")
# days: the table has a frequency only so that the store says what it holds; actions come when they come
NEVER_STALE = 36_500

Row = dict[str, object]


def numbering(pair: tuple[float, Row]) -> tuple[bool, float, str, str]:
    """How the actions that share a day, a word and an other company are put in order."""
    value, row = pair
    missing = math.isnan(value)
    return missing, 0.0 if missing else value, str(row["contraname"]), str(row["name"])


def read_rows(items: Sequence[Mapping[str, Any]]) -> tuple[Row, ...]:
    """The rows of an actions answer as table rows, each with its `item`."""
    alike: dict[tuple[str, str, str], list[tuple[float, Row]]] = {}
    for item in items:
        day = datetime.date.fromisoformat(str(item["date"]))
        action = item["action"]
        if not (isinstance(action, str) and text(action)):
            msg = f"a row whose `action` is not a word: {action!r}"
            raise ValueError(msg)
        contraticker = text(item["contraticker"])
        value = number(item["value"])
        row: Row = {
            "frequency": Frequency.DAILY.value,
            "period": day.isoformat(),
            "action": action,
            "contraticker": contraticker,
            "date": day,
            "value": value,
            "withdrawn": 0.0,
            "name": text(item.get("name")),
            "contraname": text(item.get("contraname")),
        }
        alike.setdefault((day.isoformat(), action, contraticker), []).append((value, row))
    rows: list[Row] = []
    for group in alike.values():
        # a missing value goes last; rows alike in value and names are the same row, so their order is moot
        ordered = sorted(group, key=numbering)
        rows.extend({**row, "item": str(position)} for position, (_, row) in enumerate(ordered, start=1))
    return tuple(rows)


def withdrawals(held: Collection[tuple[str, ...]], rows: Sequence[Row]) -> tuple[Row, ...]:
    """One row, marked as withdrawn, for every stored action the answer no longer has among those
    it speaks for: the ones on or after its earliest day. `held` is `Request.held`."""
    if not rows:
        return ()
    answered = {tuple(str(row[column]) for column in KEY_COLUMNS) for row in rows}
    earliest = min(str(row["period"]) for row in rows)
    gone = sorted(key for key in held if key not in answered and key[1] >= earliest)
    return tuple(
        {
            "frequency": frequency,
            "period": period,
            "date": datetime.date.fromisoformat(period),
            "action": action,
            "contraticker": contraticker,
            "item": item,
            "value": math.nan,
            "withdrawn": 1.0,
            "name": "",
            "contraname": "",
        }
        for frequency, period, action, contraticker, item in gone
    )


class SharadarActions:
    name = "sharadar_actions"
    kind = Kind.TABLE
    requests_per_minute = REQUESTS_PER_MINUTE
    daily_budget: int | None = None
    held_by: HeldBy = ("action", "contraticker", "item")

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
        yield from per_ticker(requests, lambda request: self._ticker(api, request))

    def _ticker(self, api: Sharadar, request: Request) -> FetchBatch:
        entry = request.entry
        rows = read_rows(api.rows(TABLE, entry.source_id, entry.start))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Corporate actions of {entry.source_id} (Sharadar)",
            rows=(*rows, *withdrawals(request.held, rows)),
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            stale_after_days=NEVER_STALE,
            attribute_columns=ATTRIBUTE_COLUMNS,
        )
        return FetchBatch(tables=(data,))
