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
(`.data`) and one failure per id (`.failures`). With `to=`, what arrived is written first; a
fetch that failed and brought nothing leaves the file as it was. The file is `.parquet` or
`.csv`, by its extension, and replaces any file of that name at once. A file that cannot be
written (open in another program, say) is a FetchError too, with the rows in `.data`. A CSV keeps
no types: text that looks like a number (`0123`, a product code `01`) reads back as a number
unless read as text; Parquet keeps them.
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
from data_pipeline.store.answers import Answers
from data_pipeline.store.errors import CatalogError, StoreError
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, Kind, Request, SeriesData, TableData
from data_pipeline.store.sources.base import Source
from data_pipeline.store.sync import observation_rows

WHERE = "fetch()"
SERIES_DTYPES: Mapping[str, str] = {"id": "object", "date": st.DAY, "period": "object", "value": "float64"}
NEVER_RETURNED = "the source did not return this id"
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
    """The fetch did not fully work: some ids failed, or the file could not be written.

    `data` holds what arrived, shaped as a fetch that worked; `failures` one failure per id;
    `quota_exhausted` whether the source's quota stopped the download; `written` the file written,
    None when none was; `write_error` why the file could not be written, "" when it could. Every
    argument but the message has a default, so the error survives pickling.
    """

    def __init__(
        self,
        message: str,
        *,
        data: pd.DataFrame | None = None,
        failures: Sequence[Failure] = (),
        quota_exhausted: bool = False,
        written: pathlib.Path | None = None,
        write_error: str = "",
    ) -> None:
        super().__init__(message)
        self.data = pd.DataFrame() if data is None else data
        self.failures = tuple(failures)
        self.quota_exhausted = quota_exhausted
        self.written = written
        self.write_error = write_error


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
    FetchError, after the download, when any id failed or the file could not be written, with
    the rows that arrived in `.data`. A failed fetch that brought nothing leaves the file as it
    was. A CSV keeps no types (an id `0123` reads back as 123); Parquet does. Each call spends
    the source's quota.
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
    failures = [received.failures[entry.key] for entry in entries if entry.key in received.failures]
    written: pathlib.Path | None = None
    write_error = ""
    cause: OSError | None = None
    # A fetch that failed and brought nothing leaves the file as it was: a missing key must not
    # replace yesterday's rows with none.
    if destination is not None and (len(data) or not failures):
        try:
            destination.write(data)
        except OSError as exc:  # the rows already downloaded travel with the error
            write_error = f"{type(exc).__name__}: {exc}"
            cause = exc
        else:
            written = destination.path
    if failures or write_error:
        message = _message(source, len(data), failures, len(entries), destination, written, write_error)
        raise FetchError(
            message,
            data=data,
            failures=failures,
            quota_exhausted=received.quota_exhausted,
            written=written,
            write_error=write_error,
        ) from cause
    return data


def _message(
    source: str,
    rows: int,
    failures: Sequence[Failure],
    asked: int,
    destination: _File | None,
    written: pathlib.Path | None,
    write_error: str,
) -> str:
    """What FetchError says: the ids that failed, one line each, then what became of the file."""
    arrived = f"the {rows} rows that arrived are in .data" if rows else "nothing arrived"
    lines = []
    if failures:
        lines.append(f"{source}: {len(failures)} of {asked} ids failed; {arrived}")
        lines.extend(f"{failure.entry.key}  {failure.outcome.value}: {failure.reason}" for failure in failures)
    if destination is not None and write_error:
        lines.append(f"{source}: could not write {destination.path}: {write_error}; {arrived}")
    elif destination is not None and written is None:
        lines.append(f"nothing was written to {destination.path}")
    return "\n".join(lines)


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
    """Ask for every entry and keep what comes back, by the rules a sync follows (see Answers)."""
    received = _Received(fetched_at=datetime.datetime.now(datetime.UTC))
    answers = Answers(client, stopped=STOPPED, not_returned=NEVER_RETURNED)
    try:
        for batch in source.fetch([Request(entry) for entry in entries]):
            taken = answers.take(batch)
            for series in taken.series:
                received.series[series.key] = series
            for table in taken.tables:
                received.tables.setdefault(table.key, []).append(table)
            for failure in taken.failures:
                received.failures[failure.entry.key] = failure
    except Exception as exc:  # noqa: BLE001 - a broken source fails the ids it did not finish
        answers.stop(exc)
    for failure in answers.unanswered(entries):
        received.failures[failure.entry.key] = failure
    received.quota_exhausted = answers.quota_exhausted
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
