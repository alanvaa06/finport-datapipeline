"""The Store facade: the one object other code imports.

    store = Store("D:/data")                       # read only: no keys, no catalog, no network
    store.series("fred:UNRATE")
    store.table("comtrade", "MEX", flow="X")
    store.documents("sec_filings", "AAPL", form="10-K")
    store = Store("D:/data", catalog="catalog.yaml")
    store.sync()

Reading never touches the network. `sync` takes each key from `credentials=` passed to Store, else the
process environment, else the nearest `.env` (this folder or a parent) or the file given as `env_file`.
A missing key fails only the series of its source, with a `key_error` reason; nothing is filled in.
"""

import dataclasses
import datetime
import pathlib
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx
import pandas as pd

from data_pipeline.credentials import NAMES as CREDENTIAL_NAMES
from data_pipeline.credentials import resolve
from data_pipeline.store import sources as source_registry
from data_pipeline.store import storage as st
from data_pipeline.store.catalog import build_entries, load_catalog
from data_pipeline.store.errors import StoreError, UnknownSeriesError
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Frequency, stale_after
from data_pipeline.store.sync import STATUS_FAILED, SyncReport
from data_pipeline.store.sync import sync as run_sync

STATE_OK = "ok"
STATE_STALE = "stale"
STATE_MISSING = "missing"
STATE_FAILED = "failed"
STATUS_COLUMNS = ["key", "alias", "source", "last_period", "last_fetched_at", "state", "reason"]
NEVER = "never"
NO_DATA = "no data"

Moment = str | datetime.date | datetime.datetime


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value)  # type: ignore[call-overload]


@dataclasses.dataclass(frozen=True)
class SeriesInfo:
    key: str
    alias: str
    source: str
    source_id: str
    name: str
    country: str
    frequency: str
    units: str
    seasonal_adjustment: str
    status: str
    reason: str
    last_period: str
    last_fetched_at: datetime.datetime | None
    kind: str = st.KIND_SERIES

    @property
    def label(self) -> str:
        """Citation of the series: source, id, last real period, retrieval date."""
        title = source_registry.TITLES.get(self.source, self.source)
        fetched = self.last_fetched_at.date().isoformat() if self.last_fetched_at else NEVER
        return f"[{title}: {self.source_id}, {self.last_period or NO_DATA}, fetched {fetched}]"


class Store:
    def __init__(
        self,
        root: str | pathlib.Path,
        catalog: str | pathlib.Path | None = None,
        *,
        credentials: Mapping[str, str] | None = None,
        env_file: str | pathlib.Path | None = None,
        clock: Callable[[], datetime.datetime] = _utc_now,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        # credentials: keys given in code, ahead of the environment. env_file: a .env to read instead of
        # searching for the nearest one.
        self._storage = st.Storage(pathlib.Path(root))
        self._entries: list[CatalogEntry] = list(load_catalog(pathlib.Path(catalog))) if catalog else []
        self._credentials = dict(credentials or {})
        unknown = sorted(set(self._credentials) - set(CREDENTIAL_NAMES))
        if unknown:
            msg = f"Unknown credential: {', '.join(unknown)}. Known: {', '.join(CREDENTIAL_NAMES)}"
            raise StoreError(msg)
        self._env_file = pathlib.Path(env_file) if env_file else None
        self._clock = clock
        self._transport = transport
        self._sleep = sleep

    @property
    def entries(self) -> tuple[CatalogEntry, ...]:
        """The catalog entries this store declares."""
        return tuple(self._entries)

    # -- writing -----------------------------------------------------------------------------

    def add(
        self,
        source: str,
        ids: Sequence[object],
        *,
        alias: Mapping[str, str] | None = None,
        name: str | None = None,
        frequency: str | None = None,
        start: datetime.date | None = None,
        stale_after_days: int | None = None,
        attrs: Mapping[str, str] | None = None,
        **params: object,
    ) -> None:
        """Add catalog entries for this session. The YAML file is not rewritten."""
        self._entries.extend(
            build_entries(
                source,
                ids,
                where="add()",
                alias=alias,
                name=name,
                frequency=frequency,
                start=start,
                stale_after_days=stale_after_days,
                attrs=attrs,
                params=params,
            )
        )

    def sync(
        self,
        sources: Sequence[str] | None = None,
        keys: Sequence[str] | None = None,
        *,
        full: bool = False,
    ) -> SyncReport:
        """Download what the catalog declares and store what changed: of the `sources` and the
        `keys` given, or all of it.

        `full` asks each series for its whole history again. It is also how a series whose
        frequency changed (say a catalog's `frequency: Q` corrected to `M`) is stored again, which
        any other sync refuses: its old periods get a missing value, so `series` and `frame` read
        only the new frequency, while `as_of` a moment before still reads the old periods.
        """
        credentials = resolve(self._credentials, env_file=self._env_file)
        with Client(secrets=credentials.secrets(), transport=self._transport, sleep=self._sleep) as client:
            instances = {
                name: source_registry.create(name, client, credentials)
                for name in sorted({entry.source for entry in self._entries})
            }
            return run_sync(
                self._storage,
                self._entries,
                instances,
                client,
                self._clock(),
                only_sources=tuple(sources or ()),
                only_keys=tuple(keys or ()),
                full=full,
                clock=self._clock,
            )

    # -- reading -----------------------------------------------------------------------------

    def index(self) -> pd.DataFrame:
        """One row per stored series, table or set of documents: provenance and last result."""
        return self._storage.read_index()

    def _row(self, name: str) -> dict[str, Any]:
        """The index row of a key or an alias. An alias is the catalog's when this store has one:
        it names the series the catalog gives it, even if an older catalog left it on another."""
        index = self._storage.read_index()
        if ":" in name:
            found = index[index["key"] == name]
            if found.empty:
                msg = f"no stored series has key {name!r}"
                raise UnknownSeriesError(msg)
            return st.records(found)[0]
        declared = next((entry.key for entry in self._entries if entry.alias == name), None)
        if declared is not None:
            found = index[index["key"] == declared]
            if found.empty:
                msg = f"alias {name!r} names {declared}, which is not stored yet: run sync"
                raise UnknownSeriesError(msg)
            return st.records(found)[0]
        found = index[index["alias"] == name]
        if found.empty:
            msg = f"no stored series has alias {name!r}"
            raise UnknownSeriesError(msg)
        if len(found) > 1:
            keys = ", ".join(sorted(str(key) for key in found["key"]))
            msg = (
                f"alias {name!r} is on several stored series ({keys}): "
                "read one by its key, or open the store with a catalog"
            )
            raise StoreError(msg)
        return st.records(found)[0]

    def _observations(self, name: str) -> pd.DataFrame:
        row = self._row(name)
        if row["kind"] == st.KIND_TABLE:
            msg = f"{row['key']} is a table: read it with table({row['source']!r}, {row['source_id']!r})"
            raise StoreError(msg)
        if row["kind"] == st.KIND_DOCUMENT:
            msg = f"{row['key']} is a set of documents: read it with documents({row['source']!r}, {row['source_id']!r})"
            raise StoreError(msg)
        stored = self._storage.read_observations(str(row["source"]))
        selected: pd.DataFrame = stored[stored["key"] == str(row["key"])]
        return selected

    def series(
        self,
        key: str,
        *,
        start: Moment | None = None,
        end: Moment | None = None,
        as_of: Moment | None = None,
        projections: bool = False,
    ) -> pd.DataFrame:
        """One series, oldest first: date, period, value (and projection when asked for).

        Periods whose current value is missing are left out. Projections are left out unless
        `projections=True`.
        """
        stored = self._observations(key)
        rows = st.latest(stored) if as_of is None else st.as_of(stored, st.to_moment(as_of))
        rows = rows[rows["value"].notna()]
        columns = ["date", "period", "value"]
        if projections:
            columns.append("projection")
        else:
            rows = rows[~rows["projection"].astype(bool)]
        if start is not None:
            rows = rows[rows["date"] >= pd.Timestamp(start)]
        if end is not None:
            rows = rows[rows["date"] <= pd.Timestamp(end)]
        return rows[columns].reset_index(drop=True)

    def frame(
        self,
        keys: Sequence[str],
        *,
        start: Moment | None = None,
        end: Moment | None = None,
        as_of: Moment | None = None,
    ) -> pd.DataFrame:
        """Several series side by side, one column per key, joined on date. Gaps stay empty."""
        columns = {
            key: self.series(key, start=start, end=end, as_of=as_of).set_index("date")["value"] for key in keys
        }
        if not columns:
            return pd.DataFrame()
        return pd.concat(columns, axis=1).sort_index()

    def table(
        self,
        source: str,
        id: str | None = None,  # noqa: A002 - the catalog calls it `id`
        *,
        as_of: Moment | None = None,
        **filters: object,
    ) -> pd.DataFrame:
        """The rows of a source's tables: of one catalog id, or of all when `id` is None.

        One row per key, in its latest version or as it was known at `as_of`. `filters` keep the
        rows whose column equals the value: `table("comtrade", "MEX", flow="X", frequency="A")`.
        They apply to that version, so `form="10-K"` leaves out a 10-K value a 10-K/A replaced
        (a filter on a key column is applied first, since it keeps or drops a key whole).
        Columns: the key columns, `date`, the value columns, the attribute columns.
        """
        names = self._storage.table_names(source)
        if id is not None and id not in names:
            msg = f"no stored table of source {source!r} has id {id!r}"
            raise UnknownSeriesError(msg)
        if not names:
            msg = f"no table is stored for source {source!r}"
            raise UnknownSeriesError(msg)
        schema = self._storage.table_schema(source)
        key = list(schema.key_columns)
        columns = schema.columns
        unknown = sorted(set(filters) - set(columns))
        if unknown:
            msg = f"unknown column(s) for a {source} table: {', '.join(unknown)} (columns: {', '.join(columns)})"
            raise StoreError(msg)
        order = [*(column for column in key if column != "period"), "date"]
        # Every version of a key shares its key columns, so a filter on them drops whole keys and
        # can go first, before the work of choosing versions. Any other filter goes after the
        # version is chosen: never revive a replaced one.
        on_key = {column: value for column, value in filters.items() if column in key}
        on_version = {column: value for column, value in filters.items() if column not in key}
        parts = []
        for name in names if id is None else [id]:
            stored = self._storage.read_table(source, name)
            # a column added after this table was written reads as missing
            stored = stored.reindex(columns=[*stored.columns, *(c for c in columns if c not in stored)])
            for column, value in on_key.items():
                stored = stored[stored[column] == value]
            if as_of is None:
                current = st.latest(stored, key, order, by_publication=schema.versioned)
            else:
                current = st.as_of(stored, st.to_moment(as_of), key, order)
            for column, value in on_version.items():
                current = current[current[column] == value]
            parts.append(current[columns].reset_index(drop=True))
        filled = [part for part in parts if not part.empty] or parts[:1]
        return filled[0] if len(filled) == 1 else pd.concat(filled, ignore_index=True)

    def documents(
        self,
        source: str,
        id: str | None = None,  # noqa: A002 - the catalog calls it `id`
        *,
        as_of: Moment | None = None,
        **filters: object,
    ) -> pd.DataFrame:
        """The list of a source's documents, one row per file: of one catalog id, or of all.

        Columns: `id`, the columns of the list (`group`, `date`, `file`, `role`, `url`, `size`,
        `sha256`, `fetched_at` and the source's own, such as `form`) and `path`, the file on
        disk. Oldest document first. `as_of` keeps what had been published by then, each document
        known from the end of its day; `filters` keep the rows whose column equals the value.
        """
        names = self._storage.document_names(source)
        if id is not None and id not in names:
            msg = f"no stored documents of source {source!r} have id {id!r}"
            raise UnknownSeriesError(msg)
        if not names:
            msg = f"no documents are stored for source {source!r}"
            raise UnknownSeriesError(msg)
        parts = []
        for name in names if id is None else [id]:
            listed = self._storage.read_documents(source, name)
            listed.insert(0, "id", name)
            listed["path"] = [
                str(self._storage.document_path(source, name, str(group), str(file)))
                for group, file in zip(listed["group"], listed["file"], strict=True)
            ]
            parts.append(listed)
        filled = [part for part in parts if not part.empty] or parts[:1]
        found = filled[0] if len(filled) == 1 else pd.concat(filled, ignore_index=True)
        unknown = sorted(set(filters) - set(found.columns))
        if unknown:
            msg = f"unknown column(s) for {source} documents: {', '.join(unknown)}"
            raise StoreError(msg)
        for column, value in filters.items():
            found = found[found[column] == value]
        if as_of is not None:
            # a document gives the day it was published, not the hour: known from the end of that day
            known_at = found["date"] + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            found = found[known_at <= st.to_moment(as_of).tz_localize(None)]
        return found.sort_values(["date", "id", "group", "role", "file"], kind="stable").reset_index(drop=True)

    def revisions(self, key: str) -> pd.DataFrame:
        """Every stored version of every period: within a period, oldest fetch first and, among
        the versions of one fetch, oldest publication first."""
        stored = self._observations(key)
        ordered = stored.sort_values(["date", "fetched_at", "published_at"], kind="stable", na_position="first")
        return ordered.drop(columns="key").reset_index(drop=True)

    def info(self, key: str) -> SeriesInfo:
        row = self._row(key)
        fetched = row["last_fetched_at"]
        return SeriesInfo(
            key=str(row["key"]),
            alias=_text(row["alias"]),
            source=str(row["source"]),
            source_id=str(row["source_id"]),
            name=_text(row["name"]),
            country=_text(row["country"]),
            frequency=_text(row["frequency"]),
            units=_text(row["units"]),
            seasonal_adjustment=_text(row["seasonal_adjustment"]),
            status=str(row["status"]),
            reason=_text(row["reason"]),
            last_period=_text(row["last_period"]),
            last_fetched_at=None if pd.isna(fetched) else pd.Timestamp(fetched).to_pydatetime(),
            kind=str(row["kind"]),
        )

    def status(self) -> pd.DataFrame:
        """Freshness per series or table: ok, stale, missing or failed."""
        index = self._storage.read_index()
        today = self._clock().date()
        rows = []
        for row in st.records(index):
            state, reason = _state(row, today)
            rows.append(
                {
                    "key": row["key"],
                    "alias": row["alias"],
                    "source": row["source"],
                    "last_period": row["last_period"],
                    "last_fetched_at": row["last_fetched_at"],
                    "state": state,
                    "reason": reason,
                }
            )
        stored = set(index["key"])
        rows.extend(
            {
                "key": entry.key,
                "alias": entry.alias,
                "source": entry.source,
                "last_period": None,
                "last_fetched_at": None,
                "state": STATE_MISSING,
                "reason": "declared in the catalog, never downloaded",
            }
            for entry in self._entries
            if entry.key not in stored
        )
        return pd.DataFrame(rows, columns=STATUS_COLUMNS)


def _state(row: Mapping[str, Any], today: datetime.date) -> tuple[str, str]:
    if row["status"] == STATUS_FAILED:
        return STATE_FAILED, _text(row["reason"])
    if pd.isna(row["last_date"]):
        return STATE_MISSING, NO_DATA
    last = pd.Timestamp(row["last_date"]).date()
    days = (today - last).days
    override = None if pd.isna(row["stale_after_days"]) else int(row["stale_after_days"])
    if override is None and not _text(row["frequency"]):
        return STATE_OK, ""  # a table without a frequency and without a threshold cannot be stale
    if days > stale_after(Frequency(str(row["frequency"]) or Frequency.ANNUAL), override):
        return STATE_STALE, f"last data {last.isoformat()} ({days} days ago)"
    return STATE_OK, ""
