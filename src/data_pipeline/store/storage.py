"""The store on disk: parquet in long format plus two small JSON files.

  store.json               {"schema_version": 1}
  index.parquet            one row per series: provenance and last result
  series/<source>.parquet  observations (key, period, date, value, projection, fetched_at, published_at)
  runs.json                per source: last run and its counts

Observations are append-only: a changed value adds a row, nothing is rewritten or deleted.
Every write goes to a temporary file first and then replaces the target, so a run that dies
halfway never leaves a half-written file. Missing files read as empty frames.
"""

import dataclasses
import datetime
import hashlib
import json
import pathlib
import time
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from data_pipeline.store.errors import StoreError

SCHEMA_VERSION = 1
STORE_FILE = "store.json"
INDEX_FILE = "index.parquet"
RUNS_FILE = "runs.json"
SERIES_DIR = "series"
TABLES_DIR = "tables"
TABLE_SCHEMA_FILE = "schema.json"
KIND_SERIES = "series"
KIND_TABLE = "table"
KIND_DOCUMENT = "document"
DOCUMENTS_DIR = "documents"
DOCUMENT_COLUMNS = ["group", "date", "file", "role", "url", "size", "sha256", "fetched_at"]
KEY = ["key", "period"]
SERIES_ORDER = ["key", "date"]
STAMP_COLUMNS = ["fetched_at", "published_at"]
TOLERANCE = 1e-9  # relative: a value re-sent with float noise is not a revision
UTC = "datetime64[ns, UTC]"
DAY = "datetime64[ns]"
OBS_DTYPES: Mapping[str, str] = {
    "key": "object",
    "period": "object",
    "date": DAY,
    "value": "float64",
    "projection": "bool",
    "fetched_at": UTC,
    "published_at": UTC,
}
INDEX_DTYPES: Mapping[str, str] = {
    "key": "object",
    "alias": "object",
    "source": "object",
    "source_id": "object",
    "name": "object",
    "country": "object",
    "frequency": "object",
    "units": "object",
    "seasonal_adjustment": "object",
    "stale_after_days": "Int64",
    "attrs": "object",
    "first_fetched_at": UTC,
    "last_fetched_at": UTC,
    "last_period": "object",
    "last_date": DAY,
    "status": "object",
    "reason": "object",
    "kind": "object",
}
OBS_COLUMNS = list(OBS_DTYPES)
INDEX_COLUMNS = list(INDEX_DTYPES)
_DATE_ONLY_LENGTH = len("2026-06-15")


def typed(rows: Sequence[Mapping[str, Any]], dtypes: Mapping[str, str]) -> pd.DataFrame:
    """A frame with exactly the columns of `dtypes`, each in its dtype. No rows -> empty frame."""
    frame = pd.DataFrame(list(rows), columns=list(dtypes))
    for column, dtype in dtypes.items():
        if dtype == UTC:
            frame[column] = pd.to_datetime(frame[column], utc=True).dt.as_unit("ns")
        elif dtype == DAY:
            frame[column] = pd.to_datetime(frame[column]).dt.as_unit("ns")
    plain = {column: dtype for column, dtype in dtypes.items() if dtype not in (UTC, DAY)}
    return frame.astype(plain)


def records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """The rows of a frame as dictionaries keyed by column name."""
    return [{str(name): value for name, value in row.items()} for row in frame.to_dict(orient="records")]


def empty_observations() -> pd.DataFrame:
    return typed([], OBS_DTYPES)


def empty_index() -> pd.DataFrame:
    return typed([], INDEX_DTYPES)


def to_moment(value: str | datetime.date | datetime.datetime) -> pd.Timestamp:
    """An as-of argument as a UTC instant. A date without a time means the end of that day."""
    if isinstance(value, datetime.datetime):
        moment = pd.Timestamp(value)
    elif isinstance(value, datetime.date) or len(value) == _DATE_ONLY_LENGTH:
        day = value if isinstance(value, datetime.date) else datetime.date.fromisoformat(value)
        moment = pd.Timestamp(datetime.datetime.combine(day, datetime.time.max, tzinfo=datetime.UTC))
    else:
        moment = pd.Timestamp(value)
    return moment.tz_localize("UTC") if moment.tzinfo is None else moment.tz_convert("UTC")


@dataclasses.dataclass(frozen=True, slots=True)
class TableSchema:
    """Which columns of a source's tables are key, value and attribute, and how versions arrive."""

    key_columns: tuple[str, ...]
    value_columns: tuple[str, ...]
    attribute_columns: tuple[str, ...] = ()
    versioned: bool = False

    @property
    def columns(self) -> list[str]:
        """The columns a reader gets, in order."""
        return [*self.key_columns, "date", *self.value_columns, *self.attribute_columns]


def latest(
    observations: pd.DataFrame,
    key: Sequence[str] = KEY,
    order: Sequence[str] = SERIES_ORDER,
    *,
    by_publication: bool = False,
) -> pd.DataFrame:
    """Per key (for a series: key and period), the row fetched last. Sorted by `order`.

    With `by_publication`, the row published last: for tables whose source dates every version.
    """
    if observations.empty:
        return observations
    if by_publication:
        known_at = observations["published_at"].fillna(observations["fetched_at"])
        ordered = observations.assign(known_at=known_at).sort_values(["known_at", "fetched_at"], kind="stable")
        ordered = ordered.drop(columns="known_at")
    else:
        ordered = observations.sort_values("fetched_at", kind="stable")
    current = ordered.drop_duplicates(list(key), keep="last")
    return current.sort_values(list(order), kind="stable").reset_index(drop=True)


def as_of(
    observations: pd.DataFrame,
    moment: pd.Timestamp,
    key: Sequence[str] = KEY,
    order: Sequence[str] = SERIES_ORDER,
) -> pd.DataFrame:
    """Per key (for a series: key and period), the row that was known at `moment`.

    A row is known from its `published_at` when the source gives one, otherwise from its
    `fetched_at`. Rows known later than `moment` are invisible.
    """
    if observations.empty:
        return observations
    known_at = observations["published_at"].fillna(observations["fetched_at"])
    visible = observations.assign(known_at=known_at)[(known_at <= moment).to_numpy()]
    ordered = visible.sort_values(["known_at", "fetched_at"], kind="stable")
    current = ordered.drop_duplicates(list(key), keep="last").drop(columns="known_at")
    return current.sort_values(list(order), kind="stable").reset_index(drop=True)


def append_changes(old: pd.DataFrame, new: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Append to `old` the rows of `new` that differ from what is stored. Never touches a stored row.

    A received (key, period) is appended when it has no stored row, or when its value or its
    projection flag differs from the latest stored row. Two values are the same when both are
    NaN or both are numbers within TOLERANCE.

    Returns (merged, added, revised):
      added   = appended rows that carry a number where the store had none
      revised = appended rows that replace a stored number (with another number or with NaN)
    """
    if new.empty:
        return old, 0, 0
    received = new.drop_duplicates(KEY, keep="last").reset_index(drop=True)
    current = latest(old)[[*KEY, "value", "projection"]]
    both = received.merge(current, on=KEY, how="left", suffixes=("", "_old"), indicator=True)
    known = (both["_merge"] == "both").to_numpy()
    before = both["value_old"].to_numpy(dtype=float)
    after = both["value"].to_numpy(dtype=float)
    same_value = (np.isnan(before) & np.isnan(after)) | np.isclose(before, after, rtol=TOLERANCE, atol=0.0)
    flag_before = both["projection_old"].where(known, both["projection"]).astype(bool).to_numpy()
    same_flag = flag_before == both["projection"].astype(bool).to_numpy()
    unchanged = known & same_value & same_flag
    had_number = known & ~np.isnan(before)
    added = int((~unchanged & ~had_number & ~np.isnan(after)).sum())
    revised = int((~unchanged & had_number).sum())
    to_append = received[~unchanged]
    if to_append.empty:
        return old, 0, 0
    merged = to_append if old.empty else pd.concat([old, to_append], ignore_index=True)
    merged = merged.sort_values(["key", "date", "fetched_at"], kind="stable").reset_index(drop=True)
    return merged, added, revised


def table_frame(
    rows: Sequence[Mapping[str, Any]],
    schema: TableSchema,
    fetched_at: datetime.datetime,
) -> pd.DataFrame:
    """The rows of a table as a frame: key and attribute columns as text, value columns as
    numbers, `date` as a day, and the two stamps every stored row carries. A versioned table
    brings its own `published_at`."""
    published = [row["published_at"] for row in rows] if schema.versioned else [pd.NaT] * len(rows)
    frame = pd.DataFrame(list(rows), columns=schema.columns)
    for column in (*schema.key_columns, *schema.attribute_columns):
        frame[column] = frame[column].astype("object")
    for column in schema.value_columns:
        frame[column] = frame[column].astype("float64")
    frame["date"] = pd.to_datetime(frame["date"]).dt.as_unit("ns")
    frame["fetched_at"] = pd.Series([fetched_at] * len(frame), dtype=UTC)
    frame["published_at"] = pd.Series(published, dtype=UTC)
    return frame


def append_versions(
    old: pd.DataFrame,
    new: pd.DataFrame,
    key: Sequence[str],
    values: Sequence[str],
) -> tuple[pd.DataFrame, int, int]:
    """Append to `old` the versions of `new` that are not stored, for a versioned table.

    A version is a key, the moment it was published and its values; the same version received
    again adds nothing. Returns (merged, added, revised): `added` counts keys the store did not
    have, `revised` counts the other versions appended. No stored row is touched.
    """
    if new.empty:
        return old, 0, 0
    columns = list(key)
    identity = [*columns, "published_at", *values]
    received = new.drop_duplicates(identity, keep="first")
    if old.empty:
        to_append = received
        fresh_keys = len(received.drop_duplicates(columns))
    else:
        marked = received.merge(old[identity].drop_duplicates(), on=identity, how="left", indicator=True)
        to_append = received[(marked["_merge"] == "left_only").to_numpy()]
        if to_append.empty:
            return old, 0, 0
        stored_keys = old[columns].drop_duplicates()
        known = to_append[columns].drop_duplicates().merge(stored_keys, on=columns, how="left", indicator=True)
        fresh_keys = int((known["_merge"] == "left_only").sum())
    merged = to_append if old.empty else pd.concat([old, to_append], ignore_index=True)
    merged = merged.sort_values([*columns, "published_at"], kind="stable").reset_index(drop=True)
    return merged, fresh_keys, len(to_append) - fresh_keys


def append_rows(
    old: pd.DataFrame,
    new: pd.DataFrame,
    key: Sequence[str],
    values: Sequence[str],
) -> tuple[pd.DataFrame, int, int]:
    """Append to `old` the rows of `new` that differ from what is stored, for any table.

    A received key is appended when it is not stored, or when any value column differs from the
    latest stored row. Two values are the same when both are NaN or both are numbers within
    TOLERANCE. Returns (merged, added, revised): `added` counts keys the store did not have,
    `revised` counts keys whose values changed. No stored row is touched.
    """
    if new.empty:
        return old, 0, 0
    columns = list(key)
    received = new.drop_duplicates(columns, keep="last").reset_index(drop=True)
    if old.empty:
        fresh = received.sort_values([*columns, "fetched_at"], kind="stable").reset_index(drop=True)
        return fresh, len(fresh), 0
    current = latest(old, columns, columns)[[*columns, *values]]
    both = received.merge(current, on=columns, how="left", suffixes=("", "_old"), indicator=True)
    known = (both["_merge"] == "both").to_numpy()
    unchanged = known.copy()
    for column in values:
        before = both[f"{column}_old"].to_numpy(dtype=float)
        after = both[column].to_numpy(dtype=float)
        unchanged &= (np.isnan(before) & np.isnan(after)) | np.isclose(before, after, rtol=TOLERANCE, atol=0.0)
    to_append = received[~unchanged]
    if to_append.empty:
        return old, 0, 0
    merged = pd.concat([old, to_append], ignore_index=True)
    merged = merged.sort_values([*columns, "fetched_at"], kind="stable").reset_index(drop=True)
    return merged, int((~known).sum()), int((known & ~unchanged).sum())


def document_rows(
    group: str,
    date: datetime.date,
    files: Sequence[tuple[str, str, str, bytes]],
    attributes: Mapping[str, str],
    fetched_at: datetime.datetime,
) -> pd.DataFrame:
    """The rows of one document for the list of its id. `files` are (name, role, url, content)."""
    frame = pd.DataFrame(
        [
            {
                "group": group,
                "date": date,
                "file": name,
                "role": role,
                "url": url,
                "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                **{str(column): str(value) for column, value in attributes.items()},
            }
            for name, role, url, content in files
        ]
    )
    frame["date"] = pd.to_datetime(frame["date"]).dt.as_unit("ns")
    frame["size"] = frame["size"].astype("int64")
    frame["fetched_at"] = pd.Series([fetched_at] * len(frame), dtype=UTC)
    return frame


def _read(path: pathlib.Path, dtypes: Mapping[str, str]) -> pd.DataFrame:
    if not path.exists():
        return typed([], dtypes)
    return pd.read_parquet(path)


REPLACE_ATTEMPTS = 10
REPLACE_WAIT = 0.5  # seconds between attempts: about five seconds in all
_sleep = time.sleep


def _replace(temporary: pathlib.Path, path: pathlib.Path) -> None:
    """Put the finished temporary file in place of the target.

    On Windows the replacement is refused while another program has the target open, which
    happens when something reads the store during a sync. The reader is given a few seconds to
    finish; after that the write fails with a clear error and the target keeps its old content.
    """
    for attempt in range(1, REPLACE_ATTEMPTS + 1):
        try:
            temporary.replace(path)
        except PermissionError:
            if attempt == REPLACE_ATTEMPTS:
                temporary.unlink(missing_ok=True)
                msg = f"{path}: could not be written because another program has it open; close it and sync again"
                raise StoreError(msg) from None
            _sleep(REPLACE_WAIT)
        else:
            return


def _write(frame: pd.DataFrame, path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    _replace(temporary, path)


def _write_json(data: Mapping[str, Any], path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
    _replace(temporary, path)


@dataclasses.dataclass(frozen=True)
class Storage:
    root: pathlib.Path

    def series_path(self, source: str) -> pathlib.Path:
        return self.root / SERIES_DIR / f"{source}.parquet"

    def prepare(self) -> None:
        """Create the store marker on first use; refuse a store written by another schema."""
        path = self.root / STORE_FILE
        if not path.exists():
            _write_json({"schema_version": SCHEMA_VERSION}, path)
            return
        found = json.loads(path.read_text(encoding="utf-8")).get("schema_version")
        if found != SCHEMA_VERSION:
            msg = f"{path}: schema_version {found!r}, this library reads version {SCHEMA_VERSION}"
            raise StoreError(msg)

    def read_observations(self, source: str) -> pd.DataFrame:
        return _read(self.series_path(source), OBS_DTYPES)

    def write_observations(self, source: str, frame: pd.DataFrame) -> None:
        _write(frame[OBS_COLUMNS], self.series_path(source))

    def read_index(self) -> pd.DataFrame:
        frame = _read(self.root / INDEX_FILE, INDEX_DTYPES)
        if "kind" not in frame.columns:  # a store written before tables existed
            frame["kind"] = KIND_SERIES
        frame["kind"] = frame["kind"].fillna(KIND_SERIES)
        return frame

    def table_path(self, source: str, name: str) -> pathlib.Path:
        return self.root / TABLES_DIR / source / f"{name}.parquet"

    def table_names(self, source: str) -> list[str]:
        """The tables stored for a source, by the id of their catalog entry."""
        folder = self.root / TABLES_DIR / source
        return sorted(path.stem for path in folder.glob("*.parquet")) if folder.exists() else []

    def read_table(self, source: str, name: str) -> pd.DataFrame:
        """One stored table with every version of every row; an empty frame when there is none."""
        path = self.table_path(source, name)
        return pd.read_parquet(path) if path.exists() else pd.DataFrame()

    def write_table(self, source: str, name: str, frame: pd.DataFrame, schema: TableSchema) -> None:
        """Write one table and, beside it, the schema of the source's tables."""
        described = {
            "key_columns": list(schema.key_columns),
            "value_columns": list(schema.value_columns),
            "attribute_columns": list(schema.attribute_columns),
            "versioned": schema.versioned,
        }
        path = self.root / TABLES_DIR / source / TABLE_SCHEMA_FILE
        if not path.exists() or json.loads(path.read_text(encoding="utf-8")) != described:
            _write_json(described, path)
        _write(frame, self.table_path(source, name))

    def table_schema(self, source: str) -> TableSchema:
        """The schema of a source's tables."""
        path = self.root / TABLES_DIR / source / TABLE_SCHEMA_FILE
        if not path.exists():
            msg = f"no table is stored for source {source!r}"
            raise StoreError(msg)
        described = json.loads(path.read_text(encoding="utf-8"))
        return TableSchema(
            key_columns=tuple(described["key_columns"]),
            value_columns=tuple(described["value_columns"]),
            attribute_columns=tuple(described.get("attribute_columns", ())),  # absent before versions existed
            versioned=bool(described.get("versioned", False)),
        )

    def document_path(self, source: str, name: str, group: str, file: str) -> pathlib.Path:
        return self.root / DOCUMENTS_DIR / source / name / group / file

    def document_names(self, source: str) -> list[str]:
        """The ids of a source that have a list of documents."""
        folder = self.root / DOCUMENTS_DIR / source
        return sorted(path.stem for path in folder.glob("*.parquet")) if folder.exists() else []

    def write_document(self, source: str, name: str, group: str, file: str, content: bytes) -> None:
        """Write one file of a document, once: a file that exists is never written again."""
        path = self.document_path(source, name, group, file)
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_bytes(content)
        _replace(temporary, path)

    def read_documents(self, source: str, name: str) -> pd.DataFrame:
        """The list of an id's documents, one row per file; an empty frame when there is none."""
        path = self.root / DOCUMENTS_DIR / source / f"{name}.parquet"
        return pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=DOCUMENT_COLUMNS)

    def write_documents(self, source: str, name: str, frame: pd.DataFrame) -> None:
        _write(frame, self.root / DOCUMENTS_DIR / source / f"{name}.parquet")

    def write_index(self, frame: pd.DataFrame) -> None:
        _write(frame[INDEX_COLUMNS], self.root / INDEX_FILE)

    def read_runs(self) -> dict[str, Any]:
        path = self.root / RUNS_FILE
        if not path.exists():
            return {}
        data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return data

    def write_runs(self, data: Mapping[str, Any]) -> None:
        _write_json(data, self.root / RUNS_FILE)
