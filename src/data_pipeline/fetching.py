"""Downloading from any source without a store: a data frame, and a file when asked.

    import data_pipeline as dp

    dp.fetch("fred", ["UNRATE", "DGS10"])                 # id, date, period, value
    dp.fetch("sharadar_prices", "AAPL", to="aapl.parquet")  # id, then the columns of Store.table

Nothing is stored: each call downloads again and spends the source's quota again, and there are
no versions and no `as_of`. For data read often, keep it in a store (`Store.sync`).

A series comes in long form, one row per id and period, as `Store.series` reads it: the latest
version of each period, without periods that have no value and without projections. A table comes
with `id` first and then the columns `Store.table` returns, the latest version of each key.
Documents are not fetched.

When an id fails, `FetchError` is raised once everything was asked; it carries what did arrive
(`.data`) and one failure per id (`.failures`). With `to=`, what arrived is written first. The
file is `.parquet` or `.csv`, by its extension, and replaces any file of that name at once.
"""

import dataclasses
import datetime
import os
import pathlib
import time
from collections.abc import Callable, Mapping, Sequence

import httpx
import pandas as pd

from data_pipeline import _files
from data_pipeline.credentials import resolve
from data_pipeline.store import sources as source_registry
from data_pipeline.store import storage as st
from data_pipeline.store.catalog import build_entries
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError, StoreError
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, Kind, Outcome, Request, SeriesData, TableData
from data_pipeline.store.sources.base import Source
from data_pipeline.store.sync import observation_rows

WHERE = "fetch()"
SERIES_DTYPES: Mapping[str, str] = {"id": "object", "date": st.DAY, "period": "object", "value": "float64"}
NEVER_RETURNED = "the source did not return this id"
QUOTA_REACHED = "the source's quota ran out before this id was fetched"
STOPPED = "the download stopped"
DOCUMENTS = (
    "{source} is a source of documents: fetch returns data frames; "
    "keep its documents in a store with Store.sync and read them with Store.documents"
)

Encode = Callable[[pd.DataFrame], bytes]


def _parquet(frame: pd.DataFrame) -> bytes:
    return frame.to_parquet(index=False)


def _csv(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


# The kinds of file `to=` writes, by extension. A destination of another kind (a SQL file, say)
# joins here without changing fetch's signature.
ENCODERS: Mapping[str, Encode] = {".parquet": _parquet, ".csv": _csv}


@dataclasses.dataclass(frozen=True)
class _File:
    path: pathlib.Path
    encode: Encode

    def write(self, frame: pd.DataFrame) -> None:
        _files.write_atomic(self.path, self.encode(frame))


class FetchError(StoreError):
    """Some ids failed. `data` holds what arrived, shaped as a fetch that worked; `failures` one
    failure per id; `quota_exhausted` whether the source's quota stopped the download."""

    def __init__(
        self,
        source: str,
        data: pd.DataFrame,
        failures: Sequence[Failure],
        *,
        asked: int,
        quota_exhausted: bool,
    ) -> None:
        self.data = data
        self.failures = tuple(failures)
        self.quota_exhausted = quota_exhausted
        arrived = f"the {len(data)} rows that arrived are in .data" if len(data) else "nothing arrived"
        head = f"{source}: {len(self.failures)} of {asked} ids failed; {arrived}"
        lines = [f"{failure.entry.key}  {failure.outcome.value}: {failure.reason}" for failure in self.failures]
        super().__init__("\n".join([head, *lines]))


@dataclasses.dataclass
class _Received:
    """What one download brought, by store key: the first series, every table, the first failure."""

    fetched_at: datetime.datetime
    series: dict[str, SeriesData] = dataclasses.field(default_factory=dict)
    tables: dict[str, list[TableData]] = dataclasses.field(default_factory=dict)
    failures: dict[str, Failure] = dataclasses.field(default_factory=dict)
    quota_exhausted: bool = False


def fetch(
    source: str,
    ids: str | Sequence[str],
    *,
    to: str | os.PathLike[str] | None = None,
    start: datetime.date | str | None = None,
    frequency: str | None = None,
    credentials: Mapping[str, str] | None = None,
    env_file: str | os.PathLike[str] | None = None,
    transport: httpx.BaseTransport | None = None,
    sleep: Callable[[float], None] = time.sleep,
    **params: object,
) -> pd.DataFrame:
    """Download `ids` of `source` without a store and return them as a data frame.

    `ids` is one id or a list of them. `start`, `frequency` and `params` are the catalog's fields
    (`table="funds"` for a Sharadar ETF, `partners=["USA"]` for Comtrade). `to` also writes the
    frame to a `.parquet` or `.csv` file, replacing one of that name. Keys come from
    `credentials`, else the environment, else the nearest `.env` (or `env_file`).

    A series source gives the columns `id, date, period, value`; a table source `id` and then the
    columns `Store.table` gives. Raises CatalogError, before any request, for an unknown source, a
    field the source does not accept, a source of documents or a file it cannot write; raises
    FetchError, after the download, when any id failed. Each call spends the source's quota.
    """
    destination = _destination(to)
    entries = _entries(source, ids, start, frequency, params)
    try:
        resolved = resolve(credentials, env_file=None if env_file is None else pathlib.Path(env_file))
    except ValueError as exc:
        raise StoreError(str(exc)) from exc
    with Client(secrets=resolved.secrets(), transport=transport, sleep=sleep) as client:
        chosen = source_registry.create(source, client, resolved)
        if chosen.kind is Kind.DOCUMENT:
            raise CatalogError(DOCUMENTS.format(source=source))
        for entry in entries:
            chosen.validate(entry)
        received = _download(chosen, entries, client)
    data = _series_frame(entries, received) if chosen.kind is Kind.SERIES else _tables_frame(entries, received)
    if destination is not None:
        destination.write(data)
    if received.failures:
        failures = [received.failures[entry.key] for entry in entries if entry.key in received.failures]
        raise FetchError(source, data, failures, asked=len(entries), quota_exhausted=received.quota_exhausted)
    return data


def _destination(to: str | os.PathLike[str] | None) -> _File | None:
    """The file `to` names, checked before anything is downloaded."""
    if to is None:
        return None
    path = pathlib.Path(to)
    encode = ENCODERS.get(path.suffix.lower())
    if encode is None:
        msg = f"{WHERE}: cannot write {path.name!r}: the file must end in {' or '.join(ENCODERS)}"
        raise CatalogError(msg)
    if not path.parent.is_dir():
        msg = f"{WHERE}: the folder {str(path.parent)!r} does not exist"
        raise CatalogError(msg)
    return _File(path, encode)


def _entries(
    source: str,
    ids: str | Sequence[str],
    start: datetime.date | str | None,
    frequency: str | None,
    params: Mapping[str, object],
) -> list[CatalogEntry]:
    """One catalog entry per distinct id, in the order given."""
    asked = [ids] if isinstance(ids, str) else list(ids)
    if isinstance(start, str):
        try:
            start = datetime.date.fromisoformat(start)
        except ValueError as exc:
            msg = f"{WHERE}: 'start' must be a date such as 1990-01-01, not {start!r}"
            raise CatalogError(msg) from exc
    distinct = list(dict.fromkeys(str(item) for item in asked))
    return list(build_entries(source, distinct, where=WHERE, frequency=frequency, start=start, params=params))


def _download(source: Source, entries: Sequence[CatalogEntry], client: Client) -> _Received:
    """Ask for every entry and keep what comes back, by the rules of a sync: the first series or
    failure of a key counts, every table of a key is kept, an id never returned is not found."""
    received = _Received(fetched_at=datetime.datetime.now(datetime.UTC))
    stopped = ""
    try:
        for batch in source.fetch([Request(entry) for entry in entries]):
            for series in batch.series:
                if series.key not in received.failures:
                    received.series.setdefault(series.key, series)
            for table in batch.tables:
                received.tables.setdefault(table.key, []).append(table)
            for failure in batch.failures:
                if failure.entry.key not in received.series:
                    scrubbed = dataclasses.replace(failure, reason=client.scrub(failure.reason))
                    received.failures.setdefault(failure.entry.key, scrubbed)
    except QuotaExhaustedError as exc:
        received.quota_exhausted = True
        stopped = client.scrub(f"{QUOTA_REACHED}: {exc}")
    except Exception as exc:  # noqa: BLE001 - a broken source fails the ids it did not finish
        stopped = client.scrub(f"{STOPPED}: {type(exc).__name__}: {exc}")
    for entry in entries:
        if entry.key in received.series or entry.key in received.tables or entry.key in received.failures:
            continue
        if received.quota_exhausted:
            failure = Failure(entry, Outcome.QUOTA_EXHAUSTED, stopped)
        elif stopped:
            failure = Failure(entry, Outcome.SOURCE_ERROR, stopped)
        else:
            failure = Failure(entry, Outcome.NOT_FOUND, NEVER_RETURNED)
        received.failures[entry.key] = failure
    return received


def _series_frame(entries: Sequence[CatalogEntry], received: _Received) -> pd.DataFrame:
    """id, date, period, value: per id the latest version of each period, as Store.series reads
    it, without missing values and without projections."""
    today = received.fetched_at.date()
    parts = []
    for entry in entries:
        series = received.series.get(entry.key)
        if series is None:
            continue
        current = st.latest(st.typed(observation_rows(series, received.fetched_at, today), st.OBS_DTYPES))
        current = current[current["value"].notna() & ~current["projection"].astype(bool)]
        parts.append(current.assign(id=entry.source_id)[list(SERIES_DTYPES)])
    if not parts:
        return st.typed([], SERIES_DTYPES)
    return pd.concat(parts, ignore_index=True)


def _tables_frame(entries: Sequence[CatalogEntry], received: _Received) -> pd.DataFrame:
    """`id`, then the columns of Store.table: per id the latest version of each key."""
    parts = []
    for entry in entries:
        tables = received.tables.get(entry.key)
        if not tables:
            continue
        first = tables[0]
        schema = st.TableSchema(first.key_columns, first.value_columns, first.attribute_columns, first.versioned)
        rows = [row for table in tables for row in table.rows]
        frame = st.table_frame(rows, schema, received.fetched_at)
        current = st.latest(frame, schema.key_columns, schema.row_order, by_publication=schema.versioned)
        part = current[schema.columns].reset_index(drop=True)
        part.insert(0, "id", entry.source_id)
        parts.append(part)
    if not parts:
        return pd.DataFrame({"id": pd.Series(dtype="object")})
    return pd.concat(parts, ignore_index=True)
