# UN Comtrade and the Table Kind Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store goods trade from UN Comtrade in `data_pipeline.store` as a table per reporting country, loaded in full once and kept up to date, with the store's append-only revision history.

**Architecture:** The core learns a second kind of data. A table source yields `TableData` (rows plus the names of its key and value columns); `sync` merges each batch into `tables/<source>/<id>.parquet` with a generic append-only merge and passes back, on the next run, the `(frequency, period)` pairs already stored so the source asks only for what is missing plus a revision window. The series code path is not rewritten. `Store.table()` reads the latest version of each key, or the one known at a date.

**Tech Stack:** Python 3.12, pandas, pyarrow, httpx, click, pytest (`httpx.MockTransport`, `monkeypatch`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-10-05-comtrade-tables-design.md`

**Verified:** every file below was built and run in a scratch clone first: 1,333 tests pass (1,258 before), ruff and mypy clean.

**Conventions:** work on branch `feat/comtrade-tables`. Run tools with `.venv/Scripts/python`; run mypy as `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`. Console output is ASCII only. Commits carry the user's name only (no co-author line).

**Two details the spec leaves open, decided here:**
- A table file does not say which of its columns are key and which are value. `write_table` keeps that beside the tables, in `tables/<source>/schema.json`, so reading needs no source and no network.
- At `AG6` twelve periods can pass Comtrade's cap of 100,000 rows an answer (assumed from its documentation, not measured). `AG6` asks 4 periods a call, each partner is its own call, and an answer that reaches the cap is refused rather than stored cut short.

---

## File structure

| File | Responsibility |
|---|---|
| `src/data_pipeline/store/model.py` | `TableData`, `FetchBatch.tables`, `Request.held` |
| `src/data_pipeline/store/storage.py` | `table_frame`, `append_rows`, `latest`/`as_of` on any key, `read_table`/`write_table`/`table_schema`/`table_names`, index `kind` |
| `src/data_pipeline/store/sync.py` | `held_periods`, `_table_row`, `_sync_tables` |
| `src/data_pipeline/store/sources/comtrade.py` | the source: settings, what to ask, the call, the rows |
| `src/data_pipeline/store/sources/comtrade_reporters.json` | ISO3 -> Comtrade reporter code (219), built by `scripts/build_comtrade_reporters.py` |
| `src/data_pipeline/store/api.py`, `cli.py` | `Store.table`, `SeriesInfo.kind`, `show` for a table |
| `tests/unit/store/{tables,sync_tables,comtrade,table_api}_test.py` | unit tests |
| `tests/live/comtrade_live_test.py` | one real call, off by default |
| `scripts/compare_comtrade_with_investment_process.py` | acceptance comparison |

---

### Task 1: Table storage (model and storage)

**Files:**
- Modify: `src/data_pipeline/store/model.py`, `src/data_pipeline/store/storage.py`
- Test: `tests/unit/store/tables_test.py`

- [ ] **Step 1: Create the branch**

````bash
git checkout -b feat/comtrade-tables
````

- [ ] **Step 2: Write the failing test**

`tests/unit/store/tables_test.py`:

````python
import datetime
import math

import pandas as pd
import pytest

from data_pipeline.store.errors import StoreError
from data_pipeline.store.storage import (
    INDEX_DTYPES,
    INDEX_FILE,
    KIND_SERIES,
    Storage,
    append_rows,
    as_of,
    latest,
    table_frame,
    typed,
)

from .helpers import NOW

LATER = NOW + datetime.timedelta(days=30)
KEY = ("reporter", "product", "frequency", "period")
VALUES = ("value_usd", "weight_kg")
ORDER = ("reporter", "product", "frequency", "date")


def row(product="27", period="2024", value=100.0, weight=5.0, reporter="MEX"):
    return {
        "reporter": reporter,
        "product": product,
        "frequency": "A",
        "period": period,
        "date": datetime.date(int(period), 12, 31),
        "value_usd": value,
        "weight_kg": weight,
    }


def frame(rows, moment=NOW):
    return table_frame(rows, KEY, VALUES, moment)


def test_table_frame_types_the_columns_and_stamps_the_rows():
    built = frame([row(), row("87", value=7.0, weight=math.nan)])
    assert list(built.columns) == [*KEY, "date", *VALUES, "fetched_at", "published_at"]
    assert str(built["value_usd"].dtype) == "float64"
    assert str(built["date"].dtype) == "datetime64[ns]"
    assert list(built["fetched_at"]) == [pd.Timestamp(NOW)] * 2
    assert built["published_at"].isna().all()
    assert frame([]).empty


def test_new_keys_are_appended_and_counted_as_added():
    merged, added, revised = append_rows(pd.DataFrame(), frame([row(), row("87")]), KEY, VALUES)
    assert (len(merged), added, revised) == (2, 2, 0)
    merged, added, revised = append_rows(merged, frame([row(), row("06")], LATER), KEY, VALUES)
    assert (len(merged), added, revised) == (3, 1, 0)


def test_the_same_values_add_nothing_and_return_the_stored_frame_itself():
    old = frame([row(weight=math.nan)])
    merged, added, revised = append_rows(old, frame([row(weight=math.nan)], LATER), KEY, VALUES)
    assert merged is old
    assert (added, revised) == (0, 0)


def test_values_within_the_tolerance_are_the_same():
    old = frame([row(value=1e12)])
    merged, _added, revised = append_rows(old, frame([row(value=1e12 + 1e-3)], LATER), KEY, VALUES)
    assert merged is old
    assert revised == 0


@pytest.mark.parametrize(
    "changed",
    [row(value=101.0), row(weight=6.0), row(weight=math.nan), row(value=math.nan)],
)
def test_a_change_in_any_value_column_appends_a_version(changed):
    old = frame([row()])
    merged, added, revised = append_rows(old, frame([changed], LATER), KEY, VALUES)
    assert (len(merged), added, revised) == (2, 0, 1)
    assert list(merged["fetched_at"]) == [pd.Timestamp(NOW), pd.Timestamp(LATER)]
    pd.testing.assert_frame_equal(merged.iloc[[0]], old)  # the stored row is untouched


def test_a_key_received_twice_in_one_answer_keeps_its_last_row():
    merged, added, _revised = append_rows(pd.DataFrame(), frame([row(value=1.0), row(value=2.0)]), KEY, VALUES)
    assert added == 1
    assert list(merged["value_usd"]) == [2.0]


def test_latest_and_as_of_work_on_any_key():
    stored, _added, _revised = append_rows(frame([row(), row("87")]), frame([row(value=150.0)], LATER), KEY, VALUES)
    now = latest(stored, KEY, ORDER)
    assert dict(zip(now["product"], now["value_usd"], strict=True)) == {"27": 150.0, "87": 100.0}
    before = as_of(stored, pd.Timestamp(NOW), KEY, ORDER)
    assert dict(zip(before["product"], before["value_usd"], strict=True)) == {"27": 100.0, "87": 100.0}
    assert as_of(stored, pd.Timestamp(NOW - datetime.timedelta(days=1)), KEY, ORDER).empty


def test_a_table_survives_a_round_trip_and_remembers_its_columns(tmp_path):
    storage = Storage(tmp_path)
    storage.prepare()
    assert storage.read_table("comtrade", "MEX").empty
    assert storage.table_names("comtrade") == []
    built = frame([row(), row("87", weight=math.nan)])
    storage.write_table("comtrade", "MEX", built, KEY, VALUES)
    pd.testing.assert_frame_equal(storage.read_table("comtrade", "MEX"), built)
    assert storage.table_names("comtrade") == ["MEX"]
    assert storage.table_schema("comtrade") == (list(KEY), list(VALUES))
    assert (tmp_path / "tables" / "comtrade" / "MEX.parquet").exists()


def test_the_schema_of_a_source_without_tables_is_an_error(tmp_path):
    with pytest.raises(StoreError, match="no table is stored for source 'comtrade'"):
        Storage(tmp_path).table_schema("comtrade")


def test_an_index_written_before_tables_existed_reads_as_series(tmp_path):
    storage = Storage(tmp_path)
    storage.prepare()
    old = typed([{"key": "fred:UNRATE", "source": "fred", "source_id": "UNRATE", "status": "ok"}], INDEX_DTYPES)
    old.drop(columns="kind").to_parquet(tmp_path / INDEX_FILE)
    assert list(storage.read_index()["kind"]) == [KIND_SERIES]
````

- [ ] **Step 3: Run it to make sure it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/tables_test.py -q`
Expected: collection error, `ImportError: cannot import name 'INDEX_FILE'` or `'KIND_SERIES'` from `data_pipeline.store.storage`

- [ ] **Step 4: Implement**

`src/data_pipeline/store/model.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/model.py b/src/data_pipeline/store/model.py
index 8868f3a..e40886c 100644
--- a/src/data_pipeline/store/model.py
+++ b/src/data_pipeline/store/model.py
@@ -65,10 +65,15 @@ def key(self) -> str:
 
 @dataclasses.dataclass(frozen=True, slots=True)
 class Request:
-    """A catalog entry plus the first date to ask for. `since=None` means full history."""
+    """A catalog entry plus what the store already has of it.
+
+    For a series: `since`, the first date to ask for (`None` means full history).
+    For a table: `held`, the (frequency, period) pairs already stored.
+    """
 
     entry: CatalogEntry
     since: datetime.date | None = None
+    held: frozenset[tuple[str, str]] = frozenset()
 
 
 @dataclasses.dataclass(frozen=True, slots=True)
@@ -92,6 +97,23 @@ class SeriesData:
     observations: tuple[Observation, ...] = ()
 
 
+@dataclasses.dataclass(frozen=True, slots=True)
+class TableData:
+    """Rows of one table of one catalog entry, as one call brought them.
+
+    Every row is a mapping with the key columns, the value columns, and `frequency`, `period`
+    and `date` (the last day of the period). Values are numbers; NaN is a missing value.
+    """
+
+    entry: CatalogEntry
+    key: str
+    name: str
+    rows: tuple[Mapping[str, object], ...]
+    key_columns: tuple[str, ...]
+    value_columns: tuple[str, ...]
+    stale_after_days: int | None = None  # the source's own threshold, used when the entry has none
+
+
 @dataclasses.dataclass(frozen=True, slots=True)
 class Failure:
     entry: CatalogEntry
@@ -103,3 +125,4 @@ class Failure:
 class FetchBatch:
     series: tuple[SeriesData, ...] = ()
     failures: tuple[Failure, ...] = ()
+    tables: tuple[TableData, ...] = ()
````

`src/data_pipeline/store/storage.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/storage.py b/src/data_pipeline/store/storage.py
index 8f64f8d..e633578 100644
--- a/src/data_pipeline/store/storage.py
+++ b/src/data_pipeline/store/storage.py
@@ -28,7 +28,13 @@
 INDEX_FILE = "index.parquet"
 RUNS_FILE = "runs.json"
 SERIES_DIR = "series"
+TABLES_DIR = "tables"
+TABLE_SCHEMA_FILE = "schema.json"
+KIND_SERIES = "series"
+KIND_TABLE = "table"
 KEY = ["key", "period"]
+SERIES_ORDER = ["key", "date"]
+STAMP_COLUMNS = ["fetched_at", "published_at"]
 TOLERANCE = 1e-9  # relative: a value re-sent with float noise is not a revision
 UTC = "datetime64[ns, UTC]"
 DAY = "datetime64[ns]"
@@ -59,6 +65,7 @@
     "last_date": DAY,
     "status": "object",
     "reason": "object",
+    "kind": "object",
 }
 OBS_COLUMNS = list(OBS_DTYPES)
 INDEX_COLUMNS = list(INDEX_DTYPES)
@@ -102,17 +109,26 @@ def to_moment(value: str | datetime.date | datetime.datetime) -> pd.Timestamp:
     return moment.tz_localize("UTC") if moment.tzinfo is None else moment.tz_convert("UTC")
 
 
-def latest(observations: pd.DataFrame) -> pd.DataFrame:
-    """Per (key, period), the row fetched last. Sorted by key and date."""
+def latest(
+    observations: pd.DataFrame,
+    key: Sequence[str] = KEY,
+    order: Sequence[str] = SERIES_ORDER,
+) -> pd.DataFrame:
+    """Per key (for a series: key and period), the row fetched last. Sorted by `order`."""
     if observations.empty:
         return observations
     ordered = observations.sort_values("fetched_at", kind="stable")
-    current = ordered.drop_duplicates(KEY, keep="last")
-    return current.sort_values(["key", "date"], kind="stable").reset_index(drop=True)
+    current = ordered.drop_duplicates(list(key), keep="last")
+    return current.sort_values(list(order), kind="stable").reset_index(drop=True)
 
 
-def as_of(observations: pd.DataFrame, moment: pd.Timestamp) -> pd.DataFrame:
-    """Per (key, period), the row that was known at `moment`.
+def as_of(
+    observations: pd.DataFrame,
+    moment: pd.Timestamp,
+    key: Sequence[str] = KEY,
+    order: Sequence[str] = SERIES_ORDER,
+) -> pd.DataFrame:
+    """Per key (for a series: key and period), the row that was known at `moment`.
 
     A row is known from its `published_at` when the source gives one, otherwise from its
     `fetched_at`. Rows known later than `moment` are invisible.
@@ -122,8 +138,8 @@ def as_of(observations: pd.DataFrame, moment: pd.Timestamp) -> pd.DataFrame:
     known_at = observations["published_at"].fillna(observations["fetched_at"])
     visible = observations.assign(known_at=known_at)[(known_at <= moment).to_numpy()]
     ordered = visible.sort_values(["known_at", "fetched_at"], kind="stable")
-    current = ordered.drop_duplicates(KEY, keep="last").drop(columns="known_at")
-    return current.sort_values(["key", "date"], kind="stable").reset_index(drop=True)
+    current = ordered.drop_duplicates(list(key), keep="last").drop(columns="known_at")
+    return current.sort_values(list(order), kind="stable").reset_index(drop=True)
 
 
 def append_changes(old: pd.DataFrame, new: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
@@ -160,6 +176,62 @@ def append_changes(old: pd.DataFrame, new: pd.DataFrame) -> tuple[pd.DataFrame,
     return merged, added, revised
 
 
+def table_frame(
+    rows: Sequence[Mapping[str, Any]],
+    key_columns: Sequence[str],
+    value_columns: Sequence[str],
+    fetched_at: datetime.datetime,
+) -> pd.DataFrame:
+    """The rows of a table as a frame: key columns as text, value columns as numbers, `date` as
+    a day, and the two stamps every stored row carries."""
+    columns = [*key_columns, "date", *value_columns]
+    frame = pd.DataFrame(list(rows), columns=columns)
+    for column in key_columns:
+        frame[column] = frame[column].astype("object")
+    for column in value_columns:
+        frame[column] = frame[column].astype("float64")
+    frame["date"] = pd.to_datetime(frame["date"]).dt.as_unit("ns")
+    frame["fetched_at"] = pd.Series([fetched_at] * len(frame), dtype=UTC)
+    frame["published_at"] = pd.Series([pd.NaT] * len(frame), dtype=UTC)
+    return frame
+
+
+def append_rows(
+    old: pd.DataFrame,
+    new: pd.DataFrame,
+    key: Sequence[str],
+    values: Sequence[str],
+) -> tuple[pd.DataFrame, int, int]:
+    """Append to `old` the rows of `new` that differ from what is stored, for any table.
+
+    A received key is appended when it is not stored, or when any value column differs from the
+    latest stored row. Two values are the same when both are NaN or both are numbers within
+    TOLERANCE. Returns (merged, added, revised): `added` counts keys the store did not have,
+    `revised` counts keys whose values changed. No stored row is touched.
+    """
+    if new.empty:
+        return old, 0, 0
+    columns = list(key)
+    received = new.drop_duplicates(columns, keep="last").reset_index(drop=True)
+    if old.empty:
+        fresh = received.sort_values([*columns, "fetched_at"], kind="stable").reset_index(drop=True)
+        return fresh, len(fresh), 0
+    current = latest(old, columns, columns)[[*columns, *values]]
+    both = received.merge(current, on=columns, how="left", suffixes=("", "_old"), indicator=True)
+    known = (both["_merge"] == "both").to_numpy()
+    unchanged = known.copy()
+    for column in values:
+        before = both[f"{column}_old"].to_numpy(dtype=float)
+        after = both[column].to_numpy(dtype=float)
+        unchanged &= (np.isnan(before) & np.isnan(after)) | np.isclose(before, after, rtol=TOLERANCE, atol=0.0)
+    to_append = received[~unchanged]
+    if to_append.empty:
+        return old, 0, 0
+    merged = pd.concat([old, to_append], ignore_index=True)
+    merged = merged.sort_values([*columns, "fetched_at"], kind="stable").reset_index(drop=True)
+    return merged, int((~known).sum()), int((known & ~unchanged).sum())
+
+
 def _read(path: pathlib.Path, dtypes: Mapping[str, str]) -> pd.DataFrame:
     if not path.exists():
         return typed([], dtypes)
@@ -230,7 +302,48 @@ def write_observations(self, source: str, frame: pd.DataFrame) -> None:
         _write(frame[OBS_COLUMNS], self.series_path(source))
 
     def read_index(self) -> pd.DataFrame:
-        return _read(self.root / INDEX_FILE, INDEX_DTYPES)
+        frame = _read(self.root / INDEX_FILE, INDEX_DTYPES)
+        if "kind" not in frame.columns:  # a store written before tables existed
+            frame["kind"] = KIND_SERIES
+        frame["kind"] = frame["kind"].fillna(KIND_SERIES)
+        return frame
+
+    def table_path(self, source: str, name: str) -> pathlib.Path:
+        return self.root / TABLES_DIR / source / f"{name}.parquet"
+
+    def table_names(self, source: str) -> list[str]:
+        """The tables stored for a source, by the id of their catalog entry."""
+        folder = self.root / TABLES_DIR / source
+        return sorted(path.stem for path in folder.glob("*.parquet")) if folder.exists() else []
+
+    def read_table(self, source: str, name: str) -> pd.DataFrame:
+        """One stored table with every version of every row; an empty frame when there is none."""
+        path = self.table_path(source, name)
+        return pd.read_parquet(path) if path.exists() else pd.DataFrame()
+
+    def write_table(
+        self,
+        source: str,
+        name: str,
+        frame: pd.DataFrame,
+        key_columns: Sequence[str],
+        value_columns: Sequence[str],
+    ) -> None:
+        """Write one table and, beside it, which of the source's columns are key and value."""
+        schema = {"key_columns": list(key_columns), "value_columns": list(value_columns)}
+        path = self.root / TABLES_DIR / source / TABLE_SCHEMA_FILE
+        if not path.exists() or json.loads(path.read_text(encoding="utf-8")) != schema:
+            _write_json(schema, path)
+        _write(frame, self.table_path(source, name))
+
+    def table_schema(self, source: str) -> tuple[list[str], list[str]]:
+        """(key columns, value columns) of a source's tables."""
+        path = self.root / TABLES_DIR / source / TABLE_SCHEMA_FILE
+        if not path.exists():
+            msg = f"no table is stored for source {source!r}"
+            raise StoreError(msg)
+        schema = json.loads(path.read_text(encoding="utf-8"))
+        return list(schema["key_columns"]), list(schema["value_columns"])
 
     def write_index(self, frame: pd.DataFrame) -> None:
         _write(frame[INDEX_COLUMNS], self.root / INDEX_FILE)
````

- [ ] **Step 5: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: every test passes (the existing storage tests too: `latest` and `as_of` keep their defaults)

- [ ] **Step 6: Commit**

````bash
git add src/data_pipeline/store/model.py src/data_pipeline/store/storage.py tests/unit/store/tables_test.py
git commit -m "feat(store): store tables with an append-only merge on any key"
````

---

### Task 2: Sync of a table source

**Files:**
- Modify: `src/data_pipeline/store/sync.py`
- Test: `tests/unit/store/sync_tables_test.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/store/sync_tables_test.py`:

````python
import datetime

import pandas as pd

from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.model import Failure, FetchBatch, Kind, Outcome, TableData
from data_pipeline.store.storage import Storage, latest
from data_pipeline.store.sync import NOT_RETURNED, SourceReport, held_periods, sync

from .helpers import NOW, client, entry

LATER = NOW + datetime.timedelta(days=30)
NEXT_DAY = NOW + datetime.timedelta(days=1)
KEY = ("reporter", "product", "frequency", "period")
VALUES = ("value_usd",)
MEX = entry("MEX", "trade")
USA = entry("USA", "trade")


def row(reporter="MEX", product="27", period="2024", value=100.0):
    monthly = "-" in period
    year, month = (int(part) for part in period.split("-")) if monthly else (int(period), 12)
    day = datetime.date(year, month, 28 if monthly else 31)
    return {
        "reporter": reporter,
        "product": product,
        "frequency": "M" if monthly else "A",
        "period": period,
        "date": day,
        "value_usd": value,
    }


def table(catalog_entry, rows, stale_after_days=190):
    return TableData(
        entry=catalog_entry,
        key=catalog_entry.key,
        name=f"Trade of {catalog_entry.source_id}",
        rows=tuple(rows),
        key_columns=KEY,
        value_columns=VALUES,
        stale_after_days=stale_after_days,
    )


class FakeTables:
    """A table source that answers from a dictionary: source_id -> a list of calls, each one a
    list of rows, a Failure, or the text "quota". Every call counts against the client."""

    name = "trade"
    kind = Kind.TABLE
    requests_per_minute = 6000

    def __init__(self, http, daily_budget=None):
        self.daily_budget = daily_budget
        self.calls = {}
        self.seen = []
        self._http = http

    def validate(self, entry):
        del entry

    def fetch(self, requests):
        for request in requests:
            self.seen.append(request)
            for call in self.calls.get(request.entry.source_id, []):
                self._http.calls[self.name] = self._http.calls.get(self.name, 0) + 1
                if isinstance(call, str):
                    raise QuotaExhaustedError(call)
                if isinstance(call, Failure):
                    yield FetchBatch(failures=(call,))
                    break
                yield FetchBatch(tables=(table(request.entry, call),))


def run(tmp_path, source, http, entries=(MEX,), now=NOW, **options):
    return sync(Storage(tmp_path), list(entries), {source.name: source}, http, now, **options)


def setup():
    http = client()
    return FakeTables(http), http


def stored(tmp_path, name="MEX"):
    return Storage(tmp_path).read_table("trade", name)


def index_row(tmp_path, key="trade:MEX"):
    frame = Storage(tmp_path).read_index()
    return frame[frame["key"] == key].iloc[0]


def test_held_periods_are_the_pairs_of_a_stored_table():
    assert held_periods(pd.DataFrame()) == frozenset()
    frame = pd.DataFrame([row(), row(product="87"), row(period="2026-05")])
    assert held_periods(frame) == {("A", "2024"), ("M", "2026-05")}


def test_first_sync_merges_every_batch_into_the_table(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(), row(product="87")], [row(period="2026-05", value=7.0)]]
    report = run(tmp_path, source, http)
    assert source.seen[0].held == frozenset()
    assert len(stored(tmp_path)) == 3
    assert report.sources == (SourceReport("trade", 1, (), 3, 0, 2, False, 0),)
    assert report.exit_code == 0


def test_the_index_row_describes_the_table(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(), row(period="2026-04"), row(period="2026-05")]]
    run(tmp_path, source, http)
    found = index_row(tmp_path)
    assert (found["kind"], found["status"], found["name"]) == ("table", "ok", "Trade of MEX")
    assert (found["frequency"], found["last_period"]) == ("M", "2026-05")  # the newest of the highest frequency
    assert found["last_date"] == pd.Timestamp("2026-05-28")
    assert found["stale_after_days"] == 190
    assert (found["source"], found["source_id"]) == ("trade", "MEX")


def test_the_entrys_own_threshold_and_name_win(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()]]
    run(tmp_path, source, http, entries=[entry("MEX", "trade", stale_after_days=400, name="Mexico")])
    found = index_row(tmp_path)
    assert (found["stale_after_days"], found["name"]) == (400, "Mexico")


def test_second_sync_carries_what_is_stored_and_adds_nothing(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(), row(period="2026-05")]]
    run(tmp_path, source, http)
    report = run(tmp_path, source, http, now=LATER)
    assert source.seen[1].held == {("A", "2024"), ("M", "2026-05")}
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(stored(tmp_path)) == 2
    found = index_row(tmp_path)
    assert found["first_fetched_at"] < found["last_fetched_at"]


def test_a_revised_value_appends_a_version_and_keeps_the_old_one(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(value=100.0)]]
    run(tmp_path, source, http)
    source.calls["MEX"] = [[row(value=120.0)]]
    report = run(tmp_path, source, http, now=LATER)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    assert list(stored(tmp_path)["value_usd"]) == [100.0, 120.0]
    assert list(latest(stored(tmp_path), KEY, KEY)["value_usd"]) == [120.0]


def test_full_sends_nothing_held_and_stores_only_what_changed(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()]]
    run(tmp_path, source, http)
    report = run(tmp_path, source, http, now=LATER, full=True)
    assert source.seen[1].held == frozenset()
    assert report.sources[0].new == 0
    assert len(stored(tmp_path)) == 1


def test_each_entry_has_its_own_file(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()]]
    source.calls["USA"] = [[row("USA"), row("USA", product="87")]]
    report = run(tmp_path, source, http, entries=[MEX, USA])
    assert (len(stored(tmp_path)), len(stored(tmp_path, "USA"))) == (1, 2)
    assert report.sources[0].ok == 2
    assert Storage(tmp_path).table_names("trade") == ["MEX", "USA"]


def test_a_failure_after_a_stored_batch_keeps_the_batch_and_fails_the_entry(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()], Failure(MEX, Outcome.NETWORK_ERROR, "timeout")]
    source.calls["USA"] = [[row("USA")]]
    report = run(tmp_path, source, http, entries=[MEX, USA])
    assert len(stored(tmp_path)) == 1
    assert report.sources[0].ok == 1
    assert report.sources[0].failed == (("trade:MEX", "network_error: timeout"),)
    found = index_row(tmp_path)
    assert (found["status"], found["kind"], found["last_period"]) == ("failed", "table", "2024")
    assert report.exit_code == 1


def test_an_entry_the_source_never_answers_is_a_failure(tmp_path):
    source, http = setup()
    report = run(tmp_path, source, http)
    assert report.sources[0].failed == (("trade:MEX", f"not_found: {NOT_RETURNED}"),)
    assert index_row(tmp_path)["kind"] == "table"


def test_an_answer_without_rows_is_ok_and_has_no_data(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[]]
    report = run(tmp_path, source, http)
    assert report.sources[0].ok == 1
    found = index_row(tmp_path)
    assert (found["status"], found["frequency"]) == ("ok", "")
    assert pd.isna(found["last_date"])
    assert stored(tmp_path).empty


def test_a_quota_stop_keeps_what_arrived_and_the_next_run_resumes(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()], "quota used up"]
    source.calls["USA"] = [[row("USA")]]
    report = run(tmp_path, source, http, entries=[MEX, USA])
    assert report.sources[0].quota_exhausted
    assert (report.sources[0].ok, report.sources[0].pending, report.sources[0].failed) == (1, 1, ())
    assert report.exit_code == 3
    assert len(stored(tmp_path)) == 1
    source.calls["MEX"] = [[row(period="2025")]]
    report = run(tmp_path, source, http, entries=[MEX, USA], now=NEXT_DAY)
    assert source.seen[-2].held == {("A", "2024")}
    assert (report.sources[0].ok, report.sources[0].new) == (2, 2)


def test_the_daily_budget_stops_the_source_between_batches(tmp_path):
    http = client()
    source = FakeTables(http, daily_budget=2)
    source.calls["MEX"] = [[row()], [row(period="2025")], [row(period="2026-05")]]
    report = run(tmp_path, source, http)
    assert (report.sources[0].calls, report.sources[0].quota_exhausted) == (2, True)
    assert len(stored(tmp_path)) == 2
    report = run(tmp_path, source, http, now=NOW + datetime.timedelta(hours=1))
    assert (report.sources[0].calls, report.sources[0].quota_exhausted) == (0, True)
    report = run(tmp_path, source, http, now=NEXT_DAY)
    assert report.sources[0].calls == 2
````

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sync_tables_test.py -q`
Expected: collection error, `ImportError: cannot import name 'held_periods'`

- [ ] **Step 3: Implement**

`src/data_pipeline/store/sync.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/sync.py b/src/data_pipeline/store/sync.py
index d7897bc..3f98d3e 100644
--- a/src/data_pipeline/store/sync.py
+++ b/src/data_pipeline/store/sync.py
@@ -23,18 +23,24 @@
 from data_pipeline.store.model import (
     CatalogEntry,
     Frequency,
+    Kind,
     Outcome,
     Request,
     SeriesData,
+    TableData,
 )
 from data_pipeline.store.sources.base import Source
 from data_pipeline.store.storage import (
     INDEX_DTYPES,
+    KIND_SERIES,
+    KIND_TABLE,
     OBS_DTYPES,
     Storage,
     append_changes,
+    append_rows,
     latest,
     records,
+    table_frame,
     typed,
 )
 
@@ -188,10 +194,11 @@ def _ok_row(series: SeriesData, previous: Row | None, last: LastReal | None, now
         "last_date": pd.Timestamp(last[1]) if last else None,
         "status": STATUS_OK,
         "reason": "",
+        "kind": KIND_SERIES,
     }
 
 
-def _failed_row(entry: CatalogEntry, previous: Row | None, reason: str) -> Row:
+def _failed_row(entry: CatalogEntry, previous: Row | None, reason: str, kind: str = KIND_SERIES) -> Row:
     if previous is not None:  # keep its provenance and data, record the failure
         return {**previous, "alias": entry.alias, "status": STATUS_FAILED, "reason": reason}
     return {
@@ -212,9 +219,118 @@ def _failed_row(entry: CatalogEntry, previous: Row | None, reason: str) -> Row:
         "last_date": None,
         "status": STATUS_FAILED,
         "reason": reason,
+        "kind": kind,
     }
 
 
+def held_periods(table: pd.DataFrame) -> frozenset[tuple[str, str]]:
+    """The (frequency, period) pairs a stored table holds, whatever their version."""
+    if table.empty:
+        return frozenset()
+    return frozenset(zip(table["frequency"].astype(str), table["period"].astype(str), strict=True))
+
+
+def _table_row(table: TableData, previous: Row | None, stored: pd.DataFrame, now: datetime.datetime) -> Row:
+    """The index row of a table: its newest period is the newest of its highest frequency."""
+    entry = table.entry
+    first = previous.get("first_fetched_at") if previous else None
+    frequency = ""
+    last_period = None
+    last_date = None
+    if not stored.empty:
+        present = set(stored["frequency"].astype(str))
+        frequency = next((item.value for item in Frequency if item.value in present), "")
+        newest = stored[stored["frequency"] == frequency].sort_values("date", kind="stable").iloc[-1]
+        last_period, last_date = str(newest["period"]), pd.Timestamp(newest["date"])
+    return {
+        "key": table.key,
+        "alias": entry.alias,
+        "source": entry.source,
+        "source_id": entry.source_id,
+        "name": entry.name or table.name,
+        "country": "",
+        "frequency": frequency,
+        "units": "",
+        "seasonal_adjustment": "",
+        "stale_after_days": entry.stale_after_days if entry.stale_after_days is not None else table.stale_after_days,
+        "attrs": json.dumps(dict(entry.attrs), sort_keys=True),
+        "first_fetched_at": first if first is not None and pd.notna(first) else now,
+        "last_fetched_at": now,
+        "last_period": last_period,
+        "last_date": last_date,
+        "status": STATUS_OK,
+        "reason": "",
+        "kind": KIND_TABLE,
+    }
+
+
+def _sync_tables(
+    storage: Storage,
+    source: Source,
+    client: Client,
+    wanted: Sequence[CatalogEntry],
+    index: dict[str, Row],
+    spent_today: int,
+    now: datetime.datetime,
+    *,
+    full: bool,
+) -> SourceReport:
+    """Sync a source of kind table. There is no `since`: each request carries what is stored."""
+    name = source.name
+    requests = [
+        Request(entry, held=frozenset() if full else held_periods(storage.read_table(name, entry.source_id)))
+        for entry in wanted
+    ]
+    calls_before = client.calls.get(name, 0)
+    remaining = None if source.daily_budget is None else source.daily_budget - spent_today
+    stored: set[str] = set()
+    failed: dict[str, str] = {}
+    added = revised = 0
+    quota = remaining is not None and remaining <= 0
+    if not quota:
+        try:
+            for batch in source.fetch(requests):
+                for table in batch.tables:
+                    existing = storage.read_table(name, table.entry.source_id)
+                    received = table_frame(table.rows, table.key_columns, table.value_columns, now)
+                    merged, more, changed = append_rows(existing, received, table.key_columns, table.value_columns)
+                    if merged is not existing:
+                        storage.write_table(name, table.entry.source_id, merged, table.key_columns, table.value_columns)
+                    added += more
+                    revised += changed
+                    stored.add(table.key)
+                    if table.key not in failed:
+                        index[table.key] = _table_row(table, index.get(table.key), merged, now)
+                for failure in batch.failures:
+                    key = failure.entry.key
+                    reason = f"{failure.outcome.value}: {failure.reason}"
+                    failed.setdefault(key, reason)
+                    index[key] = _failed_row(failure.entry, index.get(key), failed[key], KIND_TABLE)
+                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
+                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
+                    quota = True
+                    break
+        except QuotaExhaustedError:
+            quota = True
+    untouched = [request.entry for request in requests if request.entry.key not in stored | set(failed)]
+    if not quota:
+        for entry in untouched:
+            failed[entry.key] = f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
+            index[entry.key] = _failed_row(entry, index.get(entry.key), failed[entry.key], KIND_TABLE)
+        if untouched:
+            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
+    return SourceReport(
+        source=name,
+        ok=len(stored - set(failed)),
+        failed=tuple(failed.items()),
+        new=added,
+        revised=revised,
+        calls=client.calls.get(name, 0) - calls_before,
+        quota_exhausted=quota,
+        pending=len(untouched) if quota else 0,
+    )
+
+
 def _calls_today(record: Mapping[str, Any] | None, today: datetime.date) -> int:
     budget = (record or {}).get("budget") or {}
     return int(budget.get("calls", 0)) if budget.get("day") == today.isoformat() else 0
@@ -231,6 +347,8 @@ def _sync_source(
     *,
     full: bool,
 ) -> SourceReport:
+    if source.kind is Kind.TABLE:
+        return _sync_tables(storage, source, client, wanted, index, spent_today, now, full=full)
     name = source.name
     today = now.date()
     observations = storage.read_observations(name)
````

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: every test passes

- [ ] **Step 5: Commit**

````bash
git add src/data_pipeline/store/sync.py tests/unit/store/sync_tables_test.py
git commit -m "feat(store): sync table sources, carrying what is stored to the source"
````

---

### Task 3: The Comtrade source

**Files:**
- Create: `scripts/build_comtrade_reporters.py`, `src/data_pipeline/store/sources/comtrade_reporters.json`, `src/data_pipeline/store/sources/comtrade.py`
- Create: `tests/unit/store/fixtures/comtrade_hs2.json`, `tests/unit/store/fixtures/comtrade_bad_key.json`
- Modify: `src/data_pipeline/store/sources/__init__.py`
- Test: `tests/unit/store/comtrade_test.py`

- [ ] **Step 1: Copy the recorded answers**

Copy `C:/Proyectos/Investment_Process/tests/macro/publicos/fixtures/comtrade_hs2.json` to `tests/unit/store/fixtures/comtrade_hs2.json`, and `comtrade_llave_invalida.json` from the same folder to `tests/unit/store/fixtures/comtrade_bad_key.json`. Read only: nothing in Investment_Process changes.

- [ ] **Step 2: Write the failing test**

`tests/unit/store/comtrade_test.py`:

````python
import datetime
import math
import re

import httpx
import pytest

from data_pipeline.store import keys
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Kind, Outcome, Request
from data_pipeline.store.sources.comtrade import (
    KEY_COLUMNS,
    VALUE_COLUMNS,
    Comtrade,
    Query,
    closed_months,
    is_total,
    queries,
    reporter_codes,
    settings,
)

from .helpers import client, entry, fixture

TODAY = datetime.date(2026, 10, 5)
KEY_VALUE = "0123456789abcdef0123456789abcdef"
CREDENTIALS = Credentials({keys.COMTRADE: KEY_VALUE})
ALL_YEARS = frozenset(("A", str(year)) for year in range(2000, 2026))
ALL_MONTHS = frozenset(("M", month) for month in closed_months(TODAY, 75))


def reporter(identifier="MEX", **params):
    return entry(identifier, "comtrade", params=params)


def data_row(period="2024", flow="X", product="27", value=1.0, weight=2.0, **more):
    return {"period": period, "flowCode": flow, "cmdCode": product, "primaryValue": value, "netWgt": weight, **more}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows, "error": ""})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = Comtrade(client(handler, secrets=(KEY_VALUE,)), credentials, today=lambda: TODAY)
    return list(source.fetch(requests))


def test_it_is_a_table_source_within_the_free_quota():
    assert Comtrade.kind is Kind.TABLE
    assert Comtrade.daily_budget == 450
    assert Comtrade.requests_per_minute == 30


def test_the_reporter_map_uses_comtrades_own_codes():
    codes = reporter_codes()
    assert (codes["MEX"], codes["USA"], codes["FRA"], codes["IND"], codes["CHN"]) == ("484", "842", "251", "699", "156")
    assert len(codes) > 200


def test_settings_have_the_defaults_of_the_spec():
    chosen = settings(reporter())
    assert (chosen.level, chosen.partners, chosen.flows) == ("AG2", ("WLD",), ("X", "M"))
    assert (chosen.annual_from, chosen.months) == (2000, 75)


def test_settings_read_every_field():
    chosen = settings(reporter(level="ag4", partners=["usa", "WLD"], flows=["m"], annual_from=2015, months=0))
    assert (chosen.level, chosen.partners, chosen.flows) == ("AG4", ("USA", "WLD"), ("M",))
    assert (chosen.annual_from, chosen.months) == (2015, 0)


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("XXX", {}, "unknown reporter 'XXX'"),
        ("MEX", {"level": "AG3"}, "'level' must be one of AG2, AG4, AG6"),
        ("MEX", {"partners": ["ZZZ"]}, "unknown value\\(s\\) in 'partners': ZZZ"),
        ("MEX", {"partners": "USA"}, "'partners' must be a non-empty list"),
        ("MEX", {"flows": ["RX"]}, "unknown value\\(s\\) in 'flows': RX"),
        ("MEX", {"flows": []}, "'flows' must be a non-empty list"),
        ("MEX", {"annual_from": "2000"}, "'annual_from' must be a whole number"),
        ("MEX", {"months": -1}, "'months' must be a whole number"),
        ("MEX", {"months": True}, "'months' must be a whole number"),
        ("MEX", {"hs": "27"}, "unknown field\\(s\\) for source 'comtrade': hs"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = Comtrade(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(reporter(identifier, **params))


def test_closed_months_end_with_last_month_and_cross_years():
    assert closed_months(datetime.date(2026, 1, 15), 3) == ["2025-10", "2025-11", "2025-12"]
    assert closed_months(TODAY, 2) == ["2026-08", "2026-09"]
    assert closed_months(TODAY, 0) == []


def test_an_empty_store_asks_for_everything_in_blocks_of_twelve():
    asked = queries(frozenset(), settings(reporter()), TODAY)
    shape = [(query.frequency.value, query.periods[0], query.periods[-1], len(query.periods)) for query in asked]
    assert shape == [
        ("A", "2000", "2011", 12),
        ("A", "2012", "2023", 12),
        ("A", "2024", "2025", 2),
        ("M", "2020-07", "2021-06", 12),
        ("M", "2021-07", "2022-06", 12),
        ("M", "2022-07", "2023-06", 12),
        ("M", "2023-07", "2024-06", 12),
        ("M", "2024-07", "2025-06", 12),
        ("M", "2025-07", "2026-06", 12),
        ("M", "2026-07", "2026-09", 3),
    ]
    assert {query.partner for query in asked} == {"WLD"}


def test_a_full_store_asks_only_for_the_revision_windows():
    asked = queries(ALL_YEARS | ALL_MONTHS, settings(reporter()), TODAY)
    assert asked == [
        Query(Frequency.ANNUAL, "WLD", ("2024", "2025")),
        Query(Frequency.MONTHLY, "WLD", tuple(closed_months(TODAY, 12))),
    ]


def test_a_period_missing_from_the_store_is_asked_for_again():
    held = (ALL_YEARS | ALL_MONTHS) - {("A", "2003"), ("M", "2021-02")}
    annual, *monthly = queries(held, settings(reporter()), TODAY)
    assert annual.periods == ("2003", "2024", "2025")
    assert monthly[0].periods[0] == "2021-02"
    assert [len(query.periods) for query in monthly] == [12, 1]


def test_months_zero_turns_the_monthly_data_off_and_partners_multiply_calls():
    asked = queries(frozenset(), settings(reporter(months=0, annual_from=2020, partners=["WLD", "USA"])), TODAY)
    assert [(query.frequency.value, query.partner, len(query.periods)) for query in asked] == [
        ("A", "WLD", 6),
        ("A", "USA", 6),
    ]


def test_six_digits_go_four_periods_to_a_call():
    asked = queries(frozenset(), settings(reporter(level="AG6", months=0, annual_from=2020)), TODAY)
    assert [len(query.periods) for query in asked] == [4, 2]


def test_a_breakdown_row_is_not_the_total():
    assert is_total({})
    assert is_total({"motCode": 0, "customsCode": "C00", "partner2Code": 0})
    assert not is_total({"motCode": 2100})
    assert not is_total({"customsCode": "C01"})
    assert not is_total({"partner2Code": 842})


def test_a_call_is_spelled_the_way_comtrade_expects():
    seen = []

    def handler(request):
        seen.append(request)
        return answer([])

    held = ALL_YEARS | ALL_MONTHS
    fetch(handler, [Request(reporter("USA", partners=["MEX"], flows=["M"]), held=held)])
    annual, monthly = seen
    assert annual.url.path == "/data/v1/get/C/A/HS"
    assert monthly.url.path == "/data/v1/get/C/M/HS"
    assert dict(annual.url.params) == {
        "reporterCode": "842",
        "period": "2024,2025",
        "partnerCode": "484",
        "cmdCode": "AG2",
        "flowCode": "M",
        "motCode": "0",
        "customsCode": "C00",
        "partner2Code": "0",
    }
    assert monthly.url.params["period"].split(",")[:2] == ["202510", "202511"]
    assert annual.headers["Ocp-Apim-Subscription-Key"] == KEY_VALUE
    assert KEY_VALUE not in str(annual.url)


def test_the_world_is_partner_zero():
    seen = []

    def handler(request):
        seen.append(request.url.params["partnerCode"])
        return answer([])

    fetch(handler, [Request(reporter(), held=ALL_YEARS | ALL_MONTHS)])
    assert seen == ["0", "0"]


def test_each_call_is_one_batch_with_the_rows_of_the_recorded_answer():
    def handler(request):
        if request.url.path.endswith("/A/HS"):
            return httpx.Response(200, text=fixture("comtrade_hs2.json"))
        return answer([data_row("202606", "M", "87", 5.0, None)])

    annual, monthly = fetch(handler, [Request(reporter(), held=ALL_YEARS | ALL_MONTHS)])
    table = annual.tables[0]
    assert (table.key, table.key_columns, table.value_columns) == ("comtrade:MEX", KEY_COLUMNS, VALUE_COLUMNS)
    assert table.stale_after_days == 190
    assert table.name == "Goods trade of MEX by HS product (AG2)"
    assert [(row["flow"], row["product"], row["value_usd"]) for row in table.rows] == [
        ("M", "06", 198635735.0),
        ("X", "27", 1000000.0),
        ("X", "87", 2500000.0),
    ]
    first = table.rows[0]
    assert (first["reporter"], first["partner"], first["frequency"], first["period"]) == ("MEX", "WLD", "A", "2024")
    assert first["date"] == datetime.date(2024, 12, 31)
    assert math.isnan(table.rows[2]["weight_kg"])  # null weight is missing, never zero
    month = monthly.tables[0].rows[0]
    assert (month["frequency"], month["period"], month["date"]) == ("M", "2026-06", datetime.date(2026, 6, 30))
    assert math.isnan(month["weight_kg"])


def test_breakdown_rows_and_flows_not_asked_for_are_dropped():
    rows = [
        data_row(value=10.0),
        data_row(value=4.0, motCode=2100),
        data_row(value=3.0, customsCode="C01"),
        data_row(value=2.0, partner2Code=842),
        data_row(flow="RX", value=1.0),
    ]
    batches = fetch(lambda _request: answer(rows), [Request(reporter(months=0), held=ALL_YEARS)])
    assert [row["value_usd"] for row in batches[0].tables[0].rows] == [10.0]


def test_an_answer_without_rows_is_still_a_table_so_the_reporter_is_not_a_failure():
    batches = fetch(lambda _request: answer([]), [Request(reporter(months=0), held=ALL_YEARS)])
    assert [len(batch.tables[0].rows) for batch in batches] == [0]
    assert batches[0].failures == ()
    assert batches[0].tables[0].stale_after_days is None  # annual only: the annual threshold applies


def test_without_a_key_nothing_is_requested():
    calls = []
    batches = fetch(calls.append, [Request(reporter()), Request(reporter("USA"))], Credentials())
    assert calls == []
    assert [(failure.entry.source_id, failure.outcome) for failure in batches[0].failures] == [
        ("MEX", Outcome.KEY_ERROR),
        ("USA", Outcome.KEY_ERROR),
    ]
    assert "COMTRADE_API_KEY missing" in batches[0].failures[0].reason


def test_a_rejected_key_fails_every_reporter_left_and_stops():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(401, text=fixture("comtrade_bad_key.json"))

    batches = fetch(handler, [Request(reporter()), Request(reporter("USA"))])
    assert len(calls) == 1
    assert [failure.outcome for failure in batches[0].failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert "Comtrade rejected the key" in batches[0].failures[0].reason


def test_http_403_means_the_quota_is_used_up():
    with pytest.raises(QuotaExhaustedError, match="HTTP 403: Out of call volume quota"):
        fetch(lambda _request: httpx.Response(403, text="Out of call volume quota"), [Request(reporter())])


def test_a_persistent_429_means_the_quota_is_used_up():
    with pytest.raises(QuotaExhaustedError, match="HTTP 429"):
        fetch(lambda _request: httpx.Response(429), [Request(reporter())])


def test_a_failed_call_fails_its_reporter_and_the_next_one_goes_on():
    def handler(request):
        if request.url.params["reporterCode"] == "484":
            return httpx.Response(500)
        return answer([])

    held = ALL_YEARS | ALL_MONTHS
    batches = fetch(handler, [Request(reporter(), held=held), Request(reporter("USA"), held=held)])
    assert [(failure.entry.source_id, failure.outcome) for failure in batches[0].failures] == [
        ("MEX", Outcome.NETWORK_ERROR)
    ]
    assert [batch.tables[0].key for batch in batches[1:]] == ["comtrade:USA", "comtrade:USA"]


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (httpx.Response(400, text="bad period"), "HTTP 400: bad period"),
        (httpx.Response(200, json={"data": [], "error": "Invalid cmdCode"}), "Invalid cmdCode"),
        (httpx.Response(200, json={"data": [{"period": "2024"}]}), "unexpected answer \\(KeyError"),
        (httpx.Response(200, json={"data": [data_row(period="20x4")]}), "unexpected answer \\(PeriodError"),
    ],
)
def test_an_unexpected_answer_is_a_source_error(response, reason):
    batches = fetch(lambda _request: response, [Request(reporter(months=0), held=ALL_YEARS)])
    failure = batches[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, failure.reason)


def test_an_answer_at_the_row_cap_is_refused_rather_than_stored_cut_short(monkeypatch):
    monkeypatch.setattr("data_pipeline.store.sources.comtrade.MAX_ROWS", 2)
    batches = fetch(
        lambda _request: answer([data_row(), data_row(product="87")]),
        [Request(reporter(months=0), held=ALL_YEARS)],
    )
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "may be cut short" in batches[0].failures[0].reason


def test_the_key_never_shows_in_a_reason():
    batches = fetch(
        lambda _request: httpx.Response(400, text=f"key {KEY_VALUE} refused"),
        [Request(reporter(months=0), held=ALL_YEARS)],
    )
    assert KEY_VALUE not in batches[0].failures[0].reason
````

- [ ] **Step 3: Run it to make sure it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/comtrade_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.comtrade'`

- [ ] **Step 4: Write the script that builds the reporter map, and run it**

`scripts/build_comtrade_reporters.py`:

````python
"""Rebuild the map from ISO3 to Comtrade's reporter code that the store ships.

    python scripts/build_comtrade_reporters.py

Reads Comtrade's public reference list (no key) and writes
`src/data_pipeline/store/sources/comtrade_reporters.json`. Groups and reporters that no longer
exist are left out. Run it when Comtrade adds a reporter.
"""

import json
import pathlib
import sys

import httpx

URL = "https://comtradeapi.un.org/files/v1/app/reference/Reporters.json"
TARGET = pathlib.Path(__file__).parent.parent / "src" / "data_pipeline" / "store" / "sources" / "comtrade_reporters.json"
ISO3_LENGTH = 3


def current_reporters(rows: list[dict[str, object]]) -> dict[str, int]:
    """ISO3 -> reporter code, for the countries that report today."""
    codes: dict[str, int] = {}
    for row in rows:
        iso = str(row.get("reporterCodeIsoAlpha3") or "")
        if row.get("isGroup") or row.get("entryExpiredDate") or len(iso) != ISO3_LENGTH or not iso.isalpha():
            continue
        if iso in codes:
            msg = f"{iso} appears twice among the current reporters"
            raise ValueError(msg)
        codes[iso] = int(str(row["reporterCode"]))
    return dict(sorted(codes.items()))


def main() -> int:
    response = httpx.get(URL, timeout=60, follow_redirects=True)
    response.raise_for_status()
    codes = current_reporters(json.loads(response.content.decode("utf-8-sig"))["results"])
    TARGET.write_text(json.dumps(codes, indent=0) + "\n", encoding="utf-8")
    print(f"[ok] {len(codes)} reporters written to {TARGET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
````

Run: `.venv/Scripts/python scripts/build_comtrade_reporters.py`
Expected: `[ok] 219 reporters written to ...comtrade_reporters.json` (the count grows when Comtrade adds a reporter). It reads a public file; no key.

- [ ] **Step 5: Write the source and register it**

`src/data_pipeline/store/sources/comtrade.py`:

````python
"""UN Comtrade: goods trade by HS product. Key in the `Ocp-Apim-Subscription-Key` header.

One catalog id is one reporting country, written as ISO 3166 alpha-3 (`MEX`). Its table holds
value and weight by partner, flow, product and period, annual and monthly.

A table has no "since". Each request carries the (frequency, period) pairs already stored, and
the source asks for what is missing plus a revision window: the last 2 years and the last 12
closed months. Periods go 12 to a call, the API's limit (4 at 6 digits, to stay under its cap
of 100,000 rows an answer); one call is one batch. Nothing remembers pending calls: the next
run works out again what is missing, so a run stopped by the quota resumes by itself.

Only the total of a key is kept. Comtrade also answers with breakdowns by mode of transport,
customs procedure and second partner; those rows are dropped.
"""

import dataclasses
import datetime
import functools
import importlib.resources
import json
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.store import keys
from data_pipeline.store.errors import (
    CatalogError,
    KeyRejectedError,
    NetworkError,
    PeriodError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Frequency, Kind, Outcome, Request, TableData
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import failures, missing_key, number, reject_params, utc_today

URL = "https://comtradeapi.un.org/data/v1/get/C/{frequency}/HS"
KEY_HEADER = "Ocp-Apim-Subscription-Key"
REPORTERS_FILE = "comtrade_reporters.json"
WORLD = "WLD"
WORLD_CODE = "0"
LEVELS: Mapping[str, int] = {"AG2": 12, "AG4": 12, "AG6": 4}  # level -> periods in one call
FLOWS = ("X", "M")
FIELDS = ("level", "partners", "flows", "annual_from", "months")
DEFAULT_LEVEL = "AG2"
DEFAULT_ANNUAL_FROM = 2000
DEFAULT_MONTHS = 75
ANNUAL_WINDOW = 2
MONTHLY_WINDOW = 12
MONTHS_IN_YEAR = 12
MAX_ROWS = 100_000
STALE_AFTER_DAYS = 190  # Comtrade publishes a month two to five months late
KEY_COLUMNS = ("reporter", "partner", "flow", "product", "frequency", "period")
VALUE_COLUMNS = ("value_usd", "weight_kg")
# The total of a key, not its breakdowns by mode of transport, customs procedure or second partner.
TOTAL_ONLY: Mapping[str, str] = {"motCode": "0", "customsCode": "C00", "partner2Code": "0"}
OK = 200
UNAUTHORIZED = 401
FORBIDDEN = 403

Row = dict[str, object]


class _AnswerError(Exception):
    """Comtrade answered, but not with the rows that were asked for."""


@dataclasses.dataclass(frozen=True, slots=True)
class Settings:
    """The fields of a Comtrade entry, with their defaults filled in."""

    level: str
    partners: tuple[str, ...]
    flows: tuple[str, ...]
    annual_from: int
    months: int


@dataclasses.dataclass(frozen=True, slots=True)
class Query:
    """One call: up to 12 periods of one frequency for one partner."""

    frequency: Frequency
    partner: str
    periods: tuple[str, ...]  # as stored: "2024", "2024-06"


@functools.cache
def reporter_codes() -> Mapping[str, str]:
    """ISO3 -> Comtrade's own reporter code (the United States is 842, not 840)."""
    text = importlib.resources.files(__package__).joinpath(REPORTERS_FILE).read_text(encoding="utf-8")
    return {str(iso): str(code) for iso, code in json.loads(text).items()}


def _texts(entry: CatalogEntry, field: str, default: tuple[str, ...], allowed: Collection[str]) -> tuple[str, ...]:
    value = entry.params.get(field)
    if value is None:
        return default
    if not isinstance(value, list | tuple) or not value:
        msg = f"{entry.key}: '{field}' must be a non-empty list"
        raise CatalogError(msg)
    items = tuple(dict.fromkeys(str(item).upper() for item in value))
    unknown = sorted(set(items) - set(allowed))
    if unknown:
        msg = f"{entry.key}: unknown value(s) in '{field}': {', '.join(unknown)}"
        raise CatalogError(msg)
    return items


def _whole(entry: CatalogEntry, field: str, default: int) -> int:
    value = entry.params.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        msg = f"{entry.key}: '{field}' must be a whole number, zero or more"
        raise CatalogError(msg)
    return value


def settings(entry: CatalogEntry) -> Settings:
    """Read and check the entry's fields. Raises CatalogError on anything this source rejects."""
    reject_params(entry, FIELDS)
    codes = reporter_codes()
    if entry.source_id not in codes:
        msg = f"{entry.key}: unknown reporter {entry.source_id!r}; a Comtrade id is an ISO3 code such as MEX"
        raise CatalogError(msg)
    level = str(entry.params.get("level", DEFAULT_LEVEL)).upper()
    if level not in LEVELS:
        msg = f"{entry.key}: 'level' must be one of {', '.join(LEVELS)}"
        raise CatalogError(msg)
    return Settings(
        level=level,
        partners=_texts(entry, "partners", (WORLD,), {WORLD, *codes}),
        flows=_texts(entry, "flows", FLOWS, FLOWS),
        annual_from=_whole(entry, "annual_from", DEFAULT_ANNUAL_FROM),
        months=_whole(entry, "months", DEFAULT_MONTHS),
    )


def closed_months(today: datetime.date, count: int) -> list[str]:
    """The last `count` months that have ended, oldest first, as `2024-06`."""
    last = today.year * MONTHS_IN_YEAR + today.month - 2  # the month before today's, counted from year 0
    numbers = range(last - count + 1, last + 1)
    return [f"{number // MONTHS_IN_YEAR:04d}-{number % MONTHS_IN_YEAR + 1:02d}" for number in numbers]


def wanted_periods(
    held: Collection[tuple[str, str]],
    chosen: Settings,
    today: datetime.date,
) -> dict[Frequency, list[str]]:
    """What to ask for: every period not stored, plus the revision window. Oldest first."""
    years = [f"{year:04d}" for year in range(chosen.annual_from, today.year)]
    months = closed_months(today, chosen.months)
    annual = {year for year in years if (Frequency.ANNUAL.value, year) not in held} | set(years[-ANNUAL_WINDOW:])
    monthly = {month for month in months if (Frequency.MONTHLY.value, month) not in held}
    monthly |= set(months[-MONTHLY_WINDOW:])
    return {Frequency.ANNUAL: sorted(annual), Frequency.MONTHLY: sorted(monthly)}


def queries(held: Collection[tuple[str, str]], chosen: Settings, today: datetime.date) -> list[Query]:
    """The calls of one reporter: per partner, annual then monthly, in blocks of periods."""
    size = LEVELS[chosen.level]
    result: list[Query] = []
    for partner in chosen.partners:
        for frequency, periods in wanted_periods(held, chosen, today).items():
            result.extend(
                Query(frequency, partner, tuple(periods[start : start + size]))
                for start in range(0, len(periods), size)
            )
    return result


def is_total(row: Mapping[str, Any]) -> bool:
    """A row without these fields (older answers) counts as the total."""
    return (
        str(row.get("motCode", 0)) == "0"
        and str(row.get("customsCode", "C00")) == "C00"
        and str(row.get("partner2Code", 0)) == "0"
    )


def read_rows(payload: Mapping[str, Any], reporter: str, query: Query, flows: Collection[str]) -> tuple[Row, ...]:
    """The answer of one call as table rows; breakdown rows and other flows are dropped."""
    rows: list[Row] = []
    for item in payload.get("data") or []:
        flow = str(item["flowCode"])
        if not is_total(item) or flow not in flows:
            continue
        period, day = read_period(str(item["period"]), query.frequency)
        rows.append(
            {
                "reporter": reporter,
                "partner": query.partner,
                "flow": flow,
                "product": str(item["cmdCode"]),
                "frequency": query.frequency.value,
                "period": period,
                "date": day,
                "value_usd": number(item.get("primaryValue")),
                "weight_kg": number(item.get("netWgt")),
            }
        )
    return tuple(rows)


class Comtrade:
    name = "comtrade"
    kind = Kind.TABLE
    requests_per_minute = 30
    daily_budget: int | None = 450  # the free tier allows 500 calls a day

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._key = credentials.get(keys.COMTRADE)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        settings(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            yield missing_key(requests, keys.COMTRADE)
            return
        for position, request in enumerate(requests):
            try:
                yield from self._reporter(request, self._key)
            except KeyRejectedError as exc:
                yield FetchBatch(failures=failures(requests[position:], Outcome.KEY_ERROR, str(exc)))
                return
            except RateLimitedError as exc:
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=(Failure(request.entry, Outcome.NETWORK_ERROR, str(exc)),))
            except _AnswerError as exc:
                yield FetchBatch(failures=(Failure(request.entry, Outcome.SOURCE_ERROR, str(exc)),))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=(Failure(request.entry, Outcome.SOURCE_ERROR, reason),))

    def _reporter(self, request: Request, key: str) -> Iterator[FetchBatch]:
        """One batch per call. A call that fails ends this reporter's run; what it would have
        brought is still missing from the store, so the next run asks for it again."""
        entry = request.entry
        chosen = settings(entry)
        for query in queries(request.held, chosen, self._today()):
            rows = self._rows(entry.source_id, chosen, query, key)
            table = TableData(
                entry=entry,
                key=entry.key,
                name=f"Goods trade of {entry.source_id} by HS product ({chosen.level})",
                rows=rows,
                key_columns=KEY_COLUMNS,
                value_columns=VALUE_COLUMNS,
                stale_after_days=STALE_AFTER_DAYS if chosen.months else None,
            )
            yield FetchBatch(tables=(table,))

    def _rows(self, reporter: str, chosen: Settings, query: Query, key: str) -> tuple[Row, ...]:
        codes = reporter_codes()
        params = {
            "reporterCode": codes[reporter],
            "period": ",".join(period.replace("-", "") for period in query.periods),
            "partnerCode": WORLD_CODE if query.partner == WORLD else codes[query.partner],
            "cmdCode": chosen.level,
            "flowCode": ",".join(chosen.flows),
            **TOTAL_ONLY,
        }
        response = self._client.get(
            self.name,
            URL.format(frequency=query.frequency.value),
            params=params,
            headers={KEY_HEADER: key},
            per_minute=self.requests_per_minute,
        )
        text = self._client.scrub(response.text[:300])
        if response.status_code == UNAUTHORIZED:
            msg = f"Comtrade rejected the key: {text}"
            raise KeyRejectedError(msg)
        if response.status_code == FORBIDDEN:
            msg = f"HTTP 403: {text}"
            raise QuotaExhaustedError(msg)
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {text}"
            raise _AnswerError(msg)
        payload = response.json()
        if payload.get("error"):
            raise _AnswerError(self._client.scrub(str(payload["error"])[:300]))
        if len(payload.get("data") or []) >= MAX_ROWS:
            msg = f"the answer reached Comtrade's cap of {MAX_ROWS} rows and may be cut short: narrow the entry"
            raise _AnswerError(msg)
        return read_rows(payload, reporter, query, chosen.flows)
````

`src/data_pipeline/store/sources/__init__.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/sources/__init__.py b/src/data_pipeline/store/sources/__init__.py
index 49c36e4..55960f6 100644
--- a/src/data_pipeline/store/sources/__init__.py
+++ b/src/data_pipeline/store/sources/__init__.py
@@ -8,6 +8,7 @@
 from data_pipeline.store.sources.banxico import Banxico
 from data_pipeline.store.sources.base import Source
 from data_pipeline.store.sources.bls import Bls
+from data_pipeline.store.sources.comtrade import Comtrade
 from data_pipeline.store.sources.dbnomics import Dbnomics
 from data_pipeline.store.sources.fred import Fred
 from data_pipeline.store.sources.inegi import Inegi
@@ -30,6 +31,7 @@ def build(client: Client, credentials: Credentials) -> Source:
 REGISTRY: Mapping[str, Factory] = {
     "banxico": Banxico,
     "bls": Bls,
+    "comtrade": Comtrade,
     "dbnomics": Dbnomics,
     "fred": Fred,
     "inegi": Inegi,
@@ -40,6 +42,7 @@ def build(client: Client, credentials: Credentials) -> Source:
     "banxico": "Banxico SIE",
     "bis": "BIS",
     "bls": "BLS",
+    "comtrade": "UN Comtrade",
     "dbnomics": "DBnomics",
     "ecb": "ECB",
     "eurostat": "Eurostat",
````

- [ ] **Step 6: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: every test passes

- [ ] **Step 7: Commit**

````bash
git add scripts/build_comtrade_reporters.py src/data_pipeline/store/sources tests/unit/store/comtrade_test.py tests/unit/store/fixtures
git commit -m "feat(store): add the UN Comtrade source"
````

---

### Task 4: Reading tables (API and CLI)

**Files:**
- Modify: `src/data_pipeline/store/api.py`, `src/data_pipeline/store/cli.py`
- Test: `tests/unit/store/table_api_test.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/store/table_api_test.py`:

````python
import datetime

import click.testing
import httpx
import pytest

from data_pipeline.store import cli as cli_module
from data_pipeline.store import sources as source_registry
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import StoreError, UnknownSeriesError
from data_pipeline.store.sources.comtrade import Comtrade

from .helpers import NOW

LATER = NOW + datetime.timedelta(days=30)
CATALOG = """
- source: comtrade
  ids: [MEX, USA]
  annual_from: 2024
  months: 2
- source: fred
  ids: [UNRATE]
"""
ENV = "COMTRADE_API_KEY=0123456789abcdef0123456789abcdef\nFRED_API_KEY=0123456789abcdef0123456789abcdef\n"
REPORTERS = {"484": "MEX", "842": "USA"}


class Services:
    """Fake Comtrade and FRED. `values` maps (reporter, period as asked, flow, product) to a value."""

    def __init__(self):
        self.values = {
            ("MEX", "2024", "X", "27"): 100.0,
            ("MEX", "2024", "M", "27"): 40.0,
            ("MEX", "2025", "X", "27"): 110.0,
            ("MEX", "202604", "X", "27"): 9.0,
            ("MEX", "202605", "X", "27"): 10.0,
            ("USA", "2025", "X", "87"): 500.0,
        }
        self.calls = 0

    def __call__(self, request):
        if "stlouisfed" in request.url.host:
            if request.url.path.endswith("/observations"):
                return httpx.Response(200, json={"observations": [{"date": "2026-05-01", "value": "4.1"}]})
            meta = {"title": "Unemployment", "units": "Percent", "frequency_short": "M"}
            return httpx.Response(200, json={"seriess": [{**meta, "seasonal_adjustment_short": "SA"}]})
        self.calls += 1
        reporter = REPORTERS[request.url.params["reporterCode"]]
        periods = request.url.params["period"].split(",")
        rows = [
            {"period": period, "flowCode": flow, "cmdCode": product, "primaryValue": value, "netWgt": 1.0}
            for (owner, period, flow, product), value in self.values.items()
            if owner == reporter and period in periods
        ]
        return httpx.Response(200, json={"data": rows, "error": ""})


@pytest.fixture
def services():
    return Services()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, services, clock, monkeypatch):
    def comtrade(client, credentials):
        return Comtrade(client, credentials, today=NOW.date)

    monkeypatch.setitem(source_registry.REGISTRY, "comtrade", comtrade)
    (tmp_path / "catalog.yaml").write_text(CATALOG, encoding="utf-8")
    (tmp_path / ".env").write_text(ENV, encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(services),
        sleep=lambda _seconds: None,
    )
    built.sync()
    return built


def test_sync_stores_a_table_per_reporter(store, services):
    assert services.calls == 4  # per reporter: one annual call (2024, 2025) and one monthly (2 months)
    assert len(store.table("comtrade", "MEX")) == 5
    assert len(store.table("comtrade", "USA")) == 1
    assert len(store.table("comtrade")) == 6


def test_table_returns_the_key_columns_the_date_and_the_values(store):
    table = store.table("comtrade", "MEX")
    assert list(table.columns) == [
        "reporter", "partner", "flow", "product", "frequency", "period", "date", "value_usd", "weight_kg",
    ]  # fmt: skip
    assert list(table["period"]) == ["2024", "2024", "2025", "2026-04", "2026-05"]


def test_filters_keep_the_rows_whose_column_equals_the_value(store):
    exports = store.table("comtrade", "MEX", flow="X", frequency="A")
    assert dict(zip(exports["period"], exports["value_usd"], strict=True)) == {"2024": 100.0, "2025": 110.0}
    assert list(store.table("comtrade", product="87")["reporter"]) == ["USA"]
    assert store.table("comtrade", "MEX", flow="RX").empty


def test_an_unknown_filter_column_is_an_error(store):
    with pytest.raises(StoreError, match="unknown column\\(s\\) for a comtrade table: hs"):
        store.table("comtrade", "MEX", hs="27")


def test_an_unknown_table_is_an_error(store):
    with pytest.raises(UnknownSeriesError, match="no stored table of source 'comtrade' has id 'CHN'"):
        store.table("comtrade", "CHN")
    with pytest.raises(UnknownSeriesError, match="no table is stored for source 'fred'"):
        store.table("fred")


def test_as_of_shows_the_table_as_it_was_known(store, services, clock):
    services.values[("MEX", "2025", "X", "27")] = 125.0
    clock["now"] = LATER
    report = store.sync(sources=["comtrade"])
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    now = store.table("comtrade", "MEX", flow="X", period="2025")
    before = store.table("comtrade", "MEX", flow="X", period="2025", as_of=NOW)
    assert (list(now["value_usd"]), list(before["value_usd"])) == ([125.0], [110.0])
    assert store.table("comtrade", "MEX", as_of=NOW - datetime.timedelta(days=1)).empty


def test_a_second_sync_adds_nothing(store, clock):
    clock["now"] = NOW + datetime.timedelta(hours=1)
    report = store.sync(sources=["comtrade"])
    assert (report.sources[0].new, report.sources[0].revised, report.sources[0].calls) == (0, 0, 4)


def test_info_cites_a_table_and_series_refuses_it(store):
    info = store.info("comtrade:MEX")
    assert info.kind == "table"
    assert info.label == "[UN Comtrade: MEX, 2026-05, fetched 2026-06-06]"
    assert store.info("fred:UNRATE").kind == "series"
    with pytest.raises(StoreError, match="comtrade:MEX is a table: read it with table\\('comtrade', 'MEX'\\)"):
        store.series("comtrade:MEX")


def test_status_covers_tables(store, clock):
    states = store.status().set_index("key")["state"].to_dict()
    assert states == {"comtrade:MEX": "ok", "comtrade:USA": "ok", "fred:UNRATE": "ok"}
    clock["now"] = datetime.datetime(2026, 12, 8, tzinfo=datetime.UTC)  # 191 days after 2026-05-31
    row = store.status().set_index("key").loc["comtrade:MEX"]
    assert (row["state"], row["reason"]) == ("stale", "last data 2026-05-31 (191 days ago)")


def test_the_index_marks_each_kind(store):
    kinds = store.index().set_index("key")["kind"].to_dict()
    assert kinds == {"comtrade:MEX": "table", "comtrade:USA": "table", "fred:UNRATE": "series"}


def test_show_prints_the_citation_and_the_newest_rows_of_a_table(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    result = click.testing.CliRunner().invoke(cli, ["show", "--root", str(tmp_path / "store"), "comtrade:MEX"])
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[UN Comtrade: MEX, 2026-05, fetched 2026-06-06]"
    assert lines[1] == "Goods trade of MEX by HS product (AG2) | 5 rows"
    assert lines[2] == "reporter  partner  flow  product  frequency  period  value_usd  weight_kg"
    assert lines[-1] == "MEX  WLD  X  27  M  2026-05  10.0  1.0"
    assert len(lines) == 8


def test_show_a_table_as_of_a_date(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    arguments = ["show", "--root", str(tmp_path / "store"), "comtrade:MEX", "--as-of", "2026-01-01"]
    result = click.testing.CliRunner().invoke(cli, arguments)
    assert result.exit_code == 0
    assert result.output.splitlines()[1].endswith("| 0 rows")
````

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/table_api_test.py -q`
Expected: the tests fail with `AttributeError: 'Store' object has no attribute 'table'`

- [ ] **Step 3: Implement**

`src/data_pipeline/store/api.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/api.py b/src/data_pipeline/store/api.py
index 88f9706..55188b2 100644
--- a/src/data_pipeline/store/api.py
+++ b/src/data_pipeline/store/api.py
@@ -2,6 +2,7 @@
 
     store = Store("D:/data")                       # read only: no keys, no catalog, no network
     store.series("fred:UNRATE")
+    store.table("comtrade", "MEX", flow="X")
     store = Store("D:/data", catalog="catalog.yaml")
     store.sync()
 
@@ -21,7 +22,7 @@
 from data_pipeline.store import sources as source_registry
 from data_pipeline.store import storage as st
 from data_pipeline.store.catalog import build_entries, load_catalog
-from data_pipeline.store.errors import UnknownSeriesError
+from data_pipeline.store.errors import StoreError, UnknownSeriesError
 from data_pipeline.store.http import Client
 from data_pipeline.store.keys import ENV_FILE, load_credentials
 from data_pipeline.store.model import CatalogEntry, Frequency, stale_after
@@ -62,6 +63,7 @@ class SeriesInfo:
     reason: str
     last_period: str
     last_fetched_at: datetime.datetime | None
+    kind: str = st.KIND_SERIES
 
     @property
     def label(self) -> str:
@@ -148,7 +150,7 @@ def sync(
     # -- reading -----------------------------------------------------------------------------
 
     def index(self) -> pd.DataFrame:
-        """One row per stored series: provenance and last result."""
+        """One row per stored series or table: provenance and last result."""
         return self._storage.read_index()
 
     def _row(self, name: str) -> dict[str, Any]:
@@ -162,6 +164,9 @@ def _row(self, name: str) -> dict[str, Any]:
 
     def _observations(self, name: str) -> pd.DataFrame:
         row = self._row(name)
+        if row["kind"] == st.KIND_TABLE:
+            msg = f"{row['key']} is a table: read it with table({row['source']!r}, {row['source_id']!r})"
+            raise StoreError(msg)
         stored = self._storage.read_observations(str(row["source"]))
         selected: pd.DataFrame = stored[stored["key"] == str(row["key"])]
         return selected
@@ -210,6 +215,47 @@ def frame(
             return pd.DataFrame()
         return pd.concat(columns, axis=1).sort_index()
 
+    def table(
+        self,
+        source: str,
+        id: str | None = None,  # noqa: A002 - the catalog calls it `id`
+        *,
+        as_of: Moment | None = None,
+        **filters: object,
+    ) -> pd.DataFrame:
+        """The rows of a source's tables: of one catalog id, or of all when `id` is None.
+
+        One row per key, in its latest version or as it was known at `as_of`. `filters` keep the
+        rows whose column equals the value: `table("comtrade", "MEX", flow="X", frequency="A")`.
+        Columns: the key columns, `date`, the value columns.
+        """
+        names = self._storage.table_names(source)
+        if id is not None and id not in names:
+            msg = f"no stored table of source {source!r} has id {id!r}"
+            raise UnknownSeriesError(msg)
+        if not names:
+            msg = f"no table is stored for source {source!r}"
+            raise UnknownSeriesError(msg)
+        key, values = self._storage.table_schema(source)
+        columns = [*key, "date", *values]
+        unknown = sorted(set(filters) - set(columns))
+        if unknown:
+            msg = f"unknown column(s) for a {source} table: {', '.join(unknown)} (columns: {', '.join(columns)})"
+            raise StoreError(msg)
+        order = [*(column for column in key if column != "period"), "date"]
+        parts = []
+        for name in names if id is None else [id]:
+            stored = self._storage.read_table(source, name)
+            for column, value in filters.items():
+                stored = stored[stored[column] == value]
+            if as_of is None:
+                current = st.latest(stored, key, order)
+            else:
+                current = st.as_of(stored, st.to_moment(as_of), key, order)
+            parts.append(current[columns])
+        filled = [part for part in parts if not part.empty] or parts[:1]
+        return filled[0] if len(filled) == 1 else pd.concat(filled, ignore_index=True)
+
     def revisions(self, key: str) -> pd.DataFrame:
         """Every stored version of every period, oldest fetch first within a period."""
         stored = self._observations(key)
@@ -233,10 +279,11 @@ def info(self, key: str) -> SeriesInfo:
             reason=_text(row["reason"]),
             last_period=_text(row["last_period"]),
             last_fetched_at=None if pd.isna(fetched) else pd.Timestamp(fetched).to_pydatetime(),
+            kind=str(row["kind"]),
         )
 
     def status(self) -> pd.DataFrame:
-        """Freshness per series: ok, stale, missing or failed."""
+        """Freshness per series or table: ok, stale, missing or failed."""
         index = self._storage.read_index()
         today = self._clock().date()
         rows = []
@@ -275,6 +322,8 @@ def _state(row: Mapping[str, Any], today: datetime.date) -> tuple[str, str]:
         return STATE_FAILED, _text(row["reason"])
     if pd.isna(row["last_date"]):
         return STATE_MISSING, NO_DATA
+    if not _text(row["frequency"]):
+        return STATE_MISSING, NO_DATA
     last = pd.Timestamp(row["last_date"]).date()
     days = (today - last).days
     override = None if pd.isna(row["stale_after_days"]) else int(row["stale_after_days"])
````

`src/data_pipeline/store/cli.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/cli.py b/src/data_pipeline/store/cli.py
index 9359fbb..497e80a 100644
--- a/src/data_pipeline/store/cli.py
+++ b/src/data_pipeline/store/cli.py
@@ -9,6 +9,7 @@
 import click
 
 from data_pipeline.store.api import STATE_OK, Store
+from data_pipeline.store.storage import KIND_TABLE
 from data_pipeline.store.errors import StoreError
 from data_pipeline.store.sync import EXIT_CONFIGURATION, EXIT_FAILURES, EXIT_OK
 
@@ -90,16 +91,26 @@ def status_command(*, root: pathlib.Path, catalog: pathlib.Path | None) -> None:
 @cli.command("show")
 @root_option
 @click.argument("key")
-@click.option("--as-of", "as_of", default=None, help="Show the series as it was known on this date (YYYY-MM-DD).")
+@click.option("--as-of", "as_of", default=None, help="Show the data as it was known on this date (YYYY-MM-DD).")
 def show_command(*, root: pathlib.Path, key: str, as_of: str | None) -> None:
-    """Print a series' citation and its last observations."""
+    """Print the citation of a series or a table and its newest rows."""
     try:
         store = open_store(root, None, None)
         info = store.info(key)
-        rows = store.series(key, as_of=as_of)
+        if info.kind == KIND_TABLE:
+            table = store.table(info.source, info.source_id, as_of=as_of)
+        else:
+            rows = store.series(key, as_of=as_of)
     except StoreError as exc:
         raise fail(exc) from exc
     echo(info.label)
+    if info.kind == KIND_TABLE:
+        echo(f"{info.name} | {len(table)} rows")
+        newest = table.sort_values("date", kind="stable").tail(SHOWN_ROWS).drop(columns="date")
+        echo("  ".join(newest.columns))
+        for values in newest.itertuples(index=False):
+            echo("  ".join(str(value) for value in values))
+        return
     echo(f"{info.name} | {info.units} | {info.frequency} | {info.seasonal_adjustment}")
     for row in rows.tail(SHOWN_ROWS).to_dict(orient="records"):
         echo(f"{row['period']}  {row['value']}")
````

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: 379 passed

- [ ] **Step 5: Commit**

````bash
git add src/data_pipeline/store/api.py src/data_pipeline/store/cli.py tests/unit/store/table_api_test.py
git commit -m "feat(store): read tables with Store.table, status and show"
````

---

### Task 5: Live test, comparison script and documentation

**Files:**
- Create: `tests/live/comtrade_live_test.py`, `scripts/compare_comtrade_with_investment_process.py`
- Modify: `README.md`, `CHANGELOG.md`

- [ ] **Step 1: Write the live test**

`tests/live/comtrade_live_test.py`:

````python
"""One real call to UN Comtrade. Off by default; run with:  pytest -m live tests/live

Needs COMTRADE_API_KEY in the environment or in ./.env, and network access. It spends one call
of the daily quota. It exists to notice when Comtrade changes the shape of its answers.
"""

import pytest

from data_pipeline.store import keys
from data_pipeline.store.api import Store
from data_pipeline.store.keys import load_credentials


@pytest.mark.live
def test_comtrade_still_answers_in_the_expected_shape(tmp_path):
    if load_credentials().get(keys.COMTRADE) is None:
        pytest.skip("COMTRADE_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("comtrade", ["MEX"], annual_from=2022, months=0)
    report = store.sync()
    assert report.exit_code == 0
    assert report.sources[0].calls == 1
    table = store.table("comtrade", "MEX", period="2022")
    assert set(table["flow"]) == {"X", "M"}
    assert set(table["partner"]) == {"WLD"}
    assert table["product"].str.fullmatch(r"\d{2}").all()
    assert table["product"].nunique() > 90  # the HS has 97 chapters
    assert not table.duplicated(["flow", "product"]).any()  # one total per key, no breakdowns
    oil = table[(table["flow"] == "X") & (table["product"] == "27")]["value_usd"].iloc[0]
    assert oil > 1e10  # Mexico exported about 39 billion dollars of mineral fuels in 2022
````

- [ ] **Step 2: Write the comparison script**

`scripts/compare_comtrade_with_investment_process.py`:

````python
"""Acceptance check: Comtrade tables of the store against Investment_Process's own store.

    python scripts/compare_comtrade_with_investment_process.py C:/Proyectos/Investment_Process D:/datos/store MEX USA

Compares value and weight on the keys both stores hold (flow, product, frequency, period; partner
world). Keys only one side holds are counted, not failed: the two stores are filled on different
days. Exit code 0 when every common key matches, 1 otherwise.
"""

import pathlib
import sys

import numpy as np
import pandas as pd

import data_pipeline

TOLERANCE = 1e-9
KEY = ["flow", "product", "frequency", "period"]
VALUES = ["value_usd", "weight_kg"]
THEIR_NAMES = {
    "flujo": "flow",
    "hs": "product",
    "frecuencia": "frequency",
    "periodo": "period",
    "valor_usd": "value_usd",
    "peso_kg": "weight_kg",
}
SHOWN = 5


def their_table(origin: pathlib.Path, reporter: str) -> pd.DataFrame:
    path = origin / "inputs" / "publicos" / "comtrade" / f"{reporter.lower()}.parquet"
    frame = pd.read_parquet(path)
    frame = frame[frame["socio"].astype(str) == "0"].sort_values("consultado_en", kind="stable")
    frame = frame.rename(columns=THEIR_NAMES).drop_duplicates(KEY, keep="last")
    return frame[[*KEY, *VALUES]].astype(dict.fromkeys(KEY, str))


def same(left: pd.Series, right: pd.Series) -> np.ndarray:
    a = left.to_numpy(dtype=float)
    b = right.to_numpy(dtype=float)
    return (np.isnan(a) & np.isnan(b)) | np.isclose(a, b, rtol=TOLERANCE, atol=0.0)


def compare(origin: pathlib.Path, root: pathlib.Path, reporters: list[str]) -> int:
    store = data_pipeline.Store(root)
    failures = 0
    for reporter in reporters:
        theirs = their_table(origin, reporter)
        ours = store.table("comtrade", reporter, partner="WLD")[[*KEY, *VALUES]]
        both = theirs.merge(ours, on=KEY, how="outer", suffixes=("_theirs", "_ours"), indicator=True)
        common = both[both["_merge"] == "both"]
        equal = np.ones(len(common), dtype=bool)
        for column in VALUES:
            equal &= same(common[f"{column}_theirs"], common[f"{column}_ours"])
        different = common[~equal]
        only_theirs = int((both["_merge"] == "left_only").sum())
        only_ours = int((both["_merge"] == "right_only").sum())
        failures += 0 if different.empty else 1
        print(
            f"[{'ok' if different.empty else 'x'}] {reporter}  {len(common)} keys in common, "
            f"{len(different)} different, {only_theirs} only theirs, {only_ours} only ours"
        )
        for row in different.head(SHOWN).to_dict(orient="records"):
            print(
                f"      {row['flow']} {row['product']} {row['frequency']} {row['period']}: "
                f"theirs {row['value_usd_theirs']} / {row['weight_kg_theirs']}, "
                f"ours {row['value_usd_ours']} / {row['weight_kg_ours']}"
            )
    print(f"{failures} reporters differ" if failures else "every common key matches")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3:]))
````

- [ ] **Step 3: Document**

`README.md` (apply this diff):

````diff
diff --git a/README.md b/README.md
index cdb4f24..a084019 100644
--- a/README.md
+++ b/README.md
@@ -69,7 +69,8 @@ ## Public-data store (preview)
 economic series. It downloads full history the first time, then only what is new, and it never
 overwrites a value: a revision adds a row, so a series can be read as it was known on a past date.
 Sources: FRED, BLS, Banxico SIE, INEGI, the World Bank, the BIS, the ECB, Eurostat, the OECD and
-the IMF, read directly from each publisher, plus DBnomics for what has no direct id yet.
+the IMF, read directly from each publisher, plus DBnomics for what has no direct id yet. It also
+keeps tables: goods trade from UN Comtrade, one table per reporting country.
 
 Declare what you want in a YAML catalog:
 
@@ -81,7 +82,7 @@ ## Public-data store (preview)
 ```
 
 Put the keys of the sources you use in a `.env` file in the folder you run from: `FRED_API_KEY`,
-`BLS_API_KEY`, `BANXICO_TOKEN`, `INEGI_TOKEN`. DBnomics needs none. A curated catalog of 1,294
+`BLS_API_KEY`, `BANXICO_TOKEN`, `INEGI_TOKEN`, `COMTRADE_API_KEY`. DBnomics needs none. A curated catalog of 1,294
 macro series ships with the library; use it by name:
 
 ```
@@ -108,6 +109,7 @@ ## Public-data store (preview)
 store.series("fred:UNRATE", as_of="2026-06-15")  # as it was known that day
 store.frame(["fred:UNRATE", "fred:DGS10"])       # one column per series
 store.info("fred:UNRATE").label                  # "[FRED: UNRATE, 2026-05, fetched 2026-06-06]"
+store.table("comtrade", "MEX", flow="X")         # a table: one row per partner, product and period
 ```
 
 The same FRED terms of use described above apply.
````

`CHANGELOG.md` (apply this diff):

````diff
diff --git a/CHANGELOG.md b/CHANGELOG.md
index e203bd4..0293b94 100644
--- a/CHANGELOG.md
+++ b/CHANGELOG.md
@@ -8,6 +8,7 @@ # Changelog
 
 ## [Unreleased]
 ### Added
+- Public-data store: a second kind of data, the **table**, and its first source, **UN Comtrade** (`COMTRADE_API_KEY`). One catalog id is one reporting country (`- source: comtrade` / `ids: [MEX, USA]`); its table holds trade value and net weight by partner, flow, HS product and period, annual from 2000 and the last 75 closed months, with the same append-only revision history as series. A sync asks only for the periods the store lacks plus a revision window (2 years, 12 months), 12 periods a call, within a persisted budget of 450 calls a day; a run stopped by the quota resumes on the next one. Optional catalog fields: `level` (`AG2`, `AG4`, `AG6`), `partners`, `flows`, `annual_from`, `months`. Read with `Store.table("comtrade", "MEX", flow="X", as_of=...)`; `status` and `show comtrade:MEX` cover tables. The index gains a `kind` column; stores written before read as all series.
 - Public-data store: the SDMX source asks for several series in one call. Series of a dataflow that differ in one position of the key (usually the country) travel together, joined by `+`, up to 50 a call; the answer is split back by the column that carries those values, and a group that cannot be read as a group is asked for series by series. The 458 SDMX series of the macro catalog take about 17 calls instead of 458, and the OECD, which allows about 60 requests an hour, no longer takes an hour.
 - Public-data store: direct sources for the **World Bank** (`<indicator>/<economy>`, one call per indicator) and, through one SDMX class, the **BIS**, the **ECB**, **Eurostat**, the **OECD** and the **IMF** (`<flow>/<key>`). None needs a key. The bundled macro catalog now reads 762 of its series from their publisher instead of the DBnomics mirror, which lags by months or years; aliases are unchanged. `scripts/macro_direct_exclusions.txt` keeps a series on DBnomics when its origin refuses it.
 - Public-data store: four more sources, **BLS** (`BLS_API_KEY`; 50 series and 20 years per request, a persisted daily budget, native series ids), **Banxico SIE** (`BANXICO_TOKEN`), **INEGI** (`INEGI_TOKEN`; catalog fields `bank` and `area`) and **DBnomics** (no key). A catalog entry can now be written for one series (`id`) with its own `alias`, `name` and `frequency`; `frequency` is used when the source does not report one. The curated macro catalog (1,294 series) ships inside the package and is loaded by name: `--catalog macro`, with the `e_*` column names as aliases.
````

- [ ] **Step 4: Verify everything**

Run: `.venv/Scripts/python -m ruff check .`
Expected: `All checks passed!`
Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 91 source files`
Run: `.venv/Scripts/python -m pytest -q`
Expected: `1333 passed, 13 deselected`

- [ ] **Step 5: Commit**

````bash
git add tests/live/comtrade_live_test.py scripts/compare_comtrade_with_investment_process.py README.md CHANGELOG.md
git commit -m "docs(store): document tables and Comtrade; add the live test and the comparison"
````

---

### Task 6: Acceptance against the real service

Uses the user's Comtrade key (about 35 of the 500 daily calls). The key is read from Investment_Process's `.env` and never printed. The store goes in a scratch folder, not in the repository.

- [ ] **Step 1: Live test** (1 call)

Run, with `COMTRADE_API_KEY` taken from `C:/Proyectos/Investment_Process/.env` into the environment: `.venv/Scripts/python -m pytest -m live tests/live/comtrade_live_test.py -q`
Expected: `1 passed`.

- [ ] **Step 2: First load of three reporters** (30 calls)

Catalog `acceptance.yaml` in the scratch folder:

````yaml
- source: comtrade
  ids: [MEX, USA, CHN]
````

Run: `.venv/Scripts/python -m data_pipeline.store sync --root <scratch>/store --catalog <scratch>/acceptance.yaml --env-file C:/Proyectos/Investment_Process/.env`
Expected: `[ok]  comtrade  3 ...`, 30 calls, exit code 0.

- [ ] **Step 3: Second sync** (6 calls)

Run the same command again.
Expected: 6 calls (per reporter, one annual and one monthly revision window), 0 new, 0 revised.

- [ ] **Step 4: Compare with Investment_Process**

Run: `.venv/Scripts/python scripts/compare_comtrade_with_investment_process.py C:/Proyectos/Investment_Process <scratch>/store MEX USA CHN`
Expected: `every common key matches`, or each difference explained by a revision at the source (their rows were fetched on an earlier day).

- [ ] **Step 5: Check status and show**

Run: `.venv/Scripts/python -m data_pipeline.store status --root <scratch>/store` and `.venv/Scripts/python -m data_pipeline.store show comtrade:MEX --root <scratch>/store`
Expected: three `[ok]` lines; the citation and the ten newest rows of Mexico.
