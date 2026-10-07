# SEC Filings and the Document Kind Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the filings a company sends to the SEC as the files the SEC publishes, downloaded once and never changed, with a list that says what each file is.

**Architecture:** The core learns the third kind of data. A document source yields `DocumentData` (one entry's documents, each a group of files published together); `sync` writes every file once, atomically, under `documents/<source>/<id>/<group>/` and keeps a list per id, and passes back on the next run the groups already stored. `Store.documents()` returns the list with the path of each file. The SEC plumbing shared with `sec_xbrl` (tickers, User-Agent, 403/404) moves to `sources/sec.py`.

**Tech Stack:** Python 3.12, pandas, pyarrow, httpx, click, pytest (`httpx.MockTransport`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-10-05-sec-filings-design.md`

**Verified:** every file below was built and run in a scratch clone first: 1,427 tests pass (1,373 before), ruff and mypy clean.

**Conventions:** work on branch `feat/sec-filings`. Run tools with `.venv/Scripts/python`; run mypy as `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`. Console output is ASCII only. Commits carry the user's name only (no co-author line).

**Decided here, beyond the spec:**
- A company's run starts with a batch without documents, so a company with nothing new counts as reached and is not a failure.
- The list of an id keeps the columns `group, date, file, role, url, size, sha256` plus the source's attributes (`form`, `period`) and `fetched_at`; `Store.documents` adds `id` first and `path` last.
- `Store.table` on a document source answers as for any source without tables.

---

## File structure

| File | Responsibility |
|---|---|
| `src/data_pipeline/store/model.py` | `DocumentFile`, `Document`, `DocumentData`, `FetchBatch.documents`, `Request.groups`, `check_name` |
| `src/data_pipeline/store/storage.py` | `write_document` (once, atomic), `read/write_documents`, `document_rows`, `KIND_DOCUMENT` |
| `src/data_pipeline/store/sync.py` | `_sync_documents`, `_document_row` |
| `src/data_pipeline/store/api.py`, `cli.py` | `Store.documents`, `show` for documents, `series` refusing a document key |
| `src/data_pipeline/store/sources/sec.py` | shared SEC plumbing (`Edgar`, `check_ticker`, `resolve_tickers`) |
| `src/data_pipeline/store/sources/sec_xbrl.py` | moved onto `sec.py`; behavior and tests unchanged |
| `src/data_pipeline/store/sources/sec_filings.py` | the source: settings, the list of filings, what is missing, exhibits, downloads |
| `tests/unit/store/documents_test.py`, `sec_filings_test.py` | unit tests |
| `tests/live/sec_filings_live_test.py` | a few real calls, off by default |
| `scripts/compare_sec_filings_with_research_analyst.py` | acceptance comparison |

---

### Task 1: The document kind (core)

**Files:**
- Modify: `src/data_pipeline/store/model.py`, `storage.py`, `sync.py`, `api.py`, `cli.py`
- Test: `tests/unit/store/documents_test.py`

- [ ] **Step 1: Create the branch**

````bash
git checkout -b feat/sec-filings
````

- [ ] **Step 2: Write the failing test**

`tests/unit/store/documents_test.py`:

````python
import datetime
import hashlib

import pandas as pd
import pytest

from data_pipeline.store.model import Document, DocumentFile, check_name
from data_pipeline.store.storage import DOCUMENT_COLUMNS, Storage, document_rows

from .helpers import NOW


@pytest.mark.parametrize("name", ["aapl-20230930.htm", "0000320193-23-000106", "a_b.c-d", "R2.htm"])
def test_a_plain_name_is_safe(name):
    check_name(name)


@pytest.mark.parametrize("name", ["", ".", "..", "../x", "a/b", "a\\b", "C:x", "a b", "x\n"])
def test_a_name_that_could_leave_its_folder_is_refused(name):
    with pytest.raises(ValueError, match="unsafe file name"):
        check_name(name)


def test_files_and_documents_check_their_names_when_built():
    with pytest.raises(ValueError, match="unsafe file name"):
        DocumentFile("../secrets.htm", b"", "https://x", "primary")
    with pytest.raises(ValueError, match="unsafe file name"):
        Document("a/b", datetime.date(2023, 11, 3), ())


def test_a_file_is_written_once_and_never_rewritten(tmp_path):
    storage = Storage(tmp_path)
    storage.write_document("sec_filings", "AAPL", "0000320193-23-000106", "aapl.htm", b"<html>first</html>")
    path = tmp_path / "documents" / "sec_filings" / "AAPL" / "0000320193-23-000106" / "aapl.htm"
    assert path.read_bytes() == b"<html>first</html>"
    assert path == storage.document_path("sec_filings", "AAPL", "0000320193-23-000106", "aapl.htm")
    storage.write_document("sec_filings", "AAPL", "0000320193-23-000106", "aapl.htm", b"<html>second</html>")
    assert path.read_bytes() == b"<html>first</html>"
    assert [item.name for item in path.parent.iterdir()] == ["aapl.htm"]  # no temporary file is left


def test_the_rows_of_a_document_describe_each_file():
    files = [("aapl.htm", "primary", "https://sec/aapl.htm", b"12345"), ("ex99.htm", "exhibit", "https://sec/e", b"")]
    attributes = {"form": "8-K", "period": ""}
    rows = document_rows("0000320193-23-000106", datetime.date(2023, 11, 3), files, attributes, NOW)
    assert list(rows.columns) == [*DOCUMENT_COLUMNS[:7], "form", "period", "fetched_at"]
    first = rows.iloc[0]
    assert (first["group"], first["file"], first["role"], first["size"]) == (
        "0000320193-23-000106",
        "aapl.htm",
        "primary",
        5,
    )
    assert first["sha256"] == hashlib.sha256(b"12345").hexdigest()
    assert first["date"] == pd.Timestamp("2023-11-03")
    assert first["fetched_at"] == pd.Timestamp(NOW)
    assert list(rows["form"]) == ["8-K", "8-K"]


def test_the_list_survives_a_round_trip(tmp_path):
    storage = Storage(tmp_path)
    empty = storage.read_documents("sec_filings", "AAPL")
    assert empty.empty
    assert list(empty.columns) == DOCUMENT_COLUMNS
    assert storage.document_names("sec_filings") == []
    rows = document_rows("g1", datetime.date(2023, 11, 3), [("a.htm", "primary", "u", b"x")], {"form": "10-K"}, NOW)
    storage.write_documents("sec_filings", "AAPL", rows)
    pd.testing.assert_frame_equal(storage.read_documents("sec_filings", "AAPL"), rows)
    assert storage.document_names("sec_filings") == ["AAPL"]
````

- [ ] **Step 3: Run it to make sure it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/documents_test.py -q`
Expected: collection error, `ImportError: cannot import name 'Document' from 'data_pipeline.store.model'`

- [ ] **Step 4: Implement**

`src/data_pipeline/store/model.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/model.py b/src/data_pipeline/store/model.py
index 8476660..4b174ee 100644
--- a/src/data_pipeline/store/model.py
+++ b/src/data_pipeline/store/model.py
@@ -3,6 +3,7 @@
 import dataclasses
 import datetime
 import enum
+import re
 from collections.abc import Mapping
 
 
@@ -69,11 +70,13 @@ class Request:
 
     For a series: `since`, the first date to ask for (`None` means full history).
     For a table: `held`, the (frequency, period) pairs already stored.
+    For documents: `groups`, the names of the documents already stored.
     """
 
     entry: CatalogEntry
     since: datetime.date | None = None
     held: frozenset[tuple[str, str]] = frozenset()
+    groups: frozenset[str] = frozenset()
 
 
 @dataclasses.dataclass(frozen=True, slots=True)
@@ -122,6 +125,56 @@ class TableData:
     attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)  # added to the entry's in the index
 
 
+SAFE_NAME = re.compile(r"[A-Za-z0-9._-]+")
+
+
+def check_name(name: str) -> None:
+    """Raise ValueError unless `name` can be a file or folder name inside the store: letters,
+    digits, `.`, `_` and `-`, and not `.` or `..`. A source's name never chooses another place."""
+    if not SAFE_NAME.fullmatch(name) or name in {".", ".."}:
+        msg = f"unsafe file name {name!r}"
+        raise ValueError(msg)
+
+
+@dataclasses.dataclass(frozen=True, slots=True)
+class DocumentFile:
+    """One file of a document, as the source publishes it."""
+
+    name: str
+    content: bytes
+    url: str
+    role: str  # what the file is within its document, such as "primary" or "exhibit"
+
+    def __post_init__(self) -> None:
+        check_name(self.name)
+
+
+@dataclasses.dataclass(frozen=True, slots=True)
+class Document:
+    """A group of files published together and never changed, such as one filing."""
+
+    group: str  # the name of the document within its catalog entry
+    date: datetime.date  # the day it was published
+    files: tuple[DocumentFile, ...]
+    attributes: Mapping[str, str] = dataclasses.field(default_factory=dict)  # columns of the list
+
+    def __post_init__(self) -> None:
+        check_name(self.group)
+
+
+@dataclasses.dataclass(frozen=True, slots=True)
+class DocumentData:
+    """Documents of one catalog entry, as one call brought them. No documents means the entry
+    was reached and had nothing new."""
+
+    entry: CatalogEntry
+    key: str
+    name: str
+    documents: tuple[Document, ...] = ()
+    stale_after_days: int | None = None  # the source's own threshold, used when the entry has none
+    attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)  # added to the entry's in the index
+
+
 @dataclasses.dataclass(frozen=True, slots=True)
 class Failure:
     entry: CatalogEntry
@@ -134,3 +187,4 @@ class FetchBatch:
     series: tuple[SeriesData, ...] = ()
     failures: tuple[Failure, ...] = ()
     tables: tuple[TableData, ...] = ()
+    documents: tuple[DocumentData, ...] = ()
````

`src/data_pipeline/store/storage.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/storage.py b/src/data_pipeline/store/storage.py
index 547b6d2..9470038 100644
--- a/src/data_pipeline/store/storage.py
+++ b/src/data_pipeline/store/storage.py
@@ -12,6 +12,7 @@
 
 import dataclasses
 import datetime
+import hashlib
 import json
 import pathlib
 import time
@@ -32,6 +33,9 @@
 TABLE_SCHEMA_FILE = "schema.json"
 KIND_SERIES = "series"
 KIND_TABLE = "table"
+KIND_DOCUMENT = "document"
+DOCUMENTS_DIR = "documents"
+DOCUMENT_COLUMNS = ["group", "date", "file", "role", "url", "size", "sha256", "fetched_at"]
 KEY = ["key", "period"]
 SERIES_ORDER = ["key", "date"]
 STAMP_COLUMNS = ["fetched_at", "published_at"]
@@ -290,6 +294,35 @@ def append_rows(
     return merged, int((~known).sum()), int((known & ~unchanged).sum())
 
 
+def document_rows(
+    group: str,
+    date: datetime.date,
+    files: Sequence[tuple[str, str, str, bytes]],
+    attributes: Mapping[str, str],
+    fetched_at: datetime.datetime,
+) -> pd.DataFrame:
+    """The rows of one document for the list of its id. `files` are (name, role, url, content)."""
+    frame = pd.DataFrame(
+        [
+            {
+                "group": group,
+                "date": date,
+                "file": name,
+                "role": role,
+                "url": url,
+                "size": len(content),
+                "sha256": hashlib.sha256(content).hexdigest(),
+                **{str(column): str(value) for column, value in attributes.items()},
+            }
+            for name, role, url, content in files
+        ]
+    )
+    frame["date"] = pd.to_datetime(frame["date"]).dt.as_unit("ns")
+    frame["size"] = frame["size"].astype("int64")
+    frame["fetched_at"] = pd.Series([fetched_at] * len(frame), dtype=UTC)
+    return frame
+
+
 def _read(path: pathlib.Path, dtypes: Mapping[str, str]) -> pd.DataFrame:
     if not path.exists():
         return typed([], dtypes)
@@ -406,6 +439,32 @@ def table_schema(self, source: str) -> TableSchema:
             versioned=bool(described.get("versioned", False)),
         )
 
+    def document_path(self, source: str, name: str, group: str, file: str) -> pathlib.Path:
+        return self.root / DOCUMENTS_DIR / source / name / group / file
+
+    def document_names(self, source: str) -> list[str]:
+        """The ids of a source that have a list of documents."""
+        folder = self.root / DOCUMENTS_DIR / source
+        return sorted(path.stem for path in folder.glob("*.parquet")) if folder.exists() else []
+
+    def write_document(self, source: str, name: str, group: str, file: str, content: bytes) -> None:
+        """Write one file of a document, once: a file that exists is never written again."""
+        path = self.document_path(source, name, group, file)
+        if path.exists():
+            return
+        path.parent.mkdir(parents=True, exist_ok=True)
+        temporary = path.with_name(path.name + ".tmp")
+        temporary.write_bytes(content)
+        _replace(temporary, path)
+
+    def read_documents(self, source: str, name: str) -> pd.DataFrame:
+        """The list of an id's documents, one row per file; an empty frame when there is none."""
+        path = self.root / DOCUMENTS_DIR / source / f"{name}.parquet"
+        return pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=DOCUMENT_COLUMNS)
+
+    def write_documents(self, source: str, name: str, frame: pd.DataFrame) -> None:
+        _write(frame, self.root / DOCUMENTS_DIR / source / f"{name}.parquet")
+
     def write_index(self, frame: pd.DataFrame) -> None:
         _write(frame[INDEX_COLUMNS], self.root / INDEX_FILE)
 
````

`src/data_pipeline/store/sync.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/sync.py b/src/data_pipeline/store/sync.py
index 70ee9f1..b5cff8d 100644
--- a/src/data_pipeline/store/sync.py
+++ b/src/data_pipeline/store/sync.py
@@ -22,6 +22,7 @@
 from data_pipeline.store.http import Client
 from data_pipeline.store.model import (
     CatalogEntry,
+    DocumentData,
     Frequency,
     Kind,
     Outcome,
@@ -32,6 +33,7 @@
 from data_pipeline.store.sources.base import Source
 from data_pipeline.store.storage import (
     INDEX_DTYPES,
+    KIND_DOCUMENT,
     KIND_SERIES,
     KIND_TABLE,
     OBS_DTYPES,
@@ -40,6 +42,7 @@
     append_changes,
     append_rows,
     append_versions,
+    document_rows,
     latest,
     records,
     table_frame,
@@ -341,6 +344,103 @@ def _sync_tables(
     )
 
 
+def _document_row(data: DocumentData, previous: Row | None, listed: pd.DataFrame, now: datetime.datetime) -> Row:
+    """The index row of an entry's documents: its last period is the day of the newest one."""
+    entry = data.entry
+    first = previous.get("first_fetched_at") if previous else None
+    last_date = None if listed.empty else pd.Timestamp(listed["date"].max())
+    return {
+        "key": data.key,
+        "alias": entry.alias,
+        "source": entry.source,
+        "source_id": entry.source_id,
+        "name": entry.name or data.name,
+        "country": "",
+        "frequency": "",
+        "units": "",
+        "seasonal_adjustment": "",
+        "stale_after_days": entry.stale_after_days if entry.stale_after_days is not None else data.stale_after_days,
+        "attrs": json.dumps({**data.attrs, **entry.attrs}, sort_keys=True),
+        "first_fetched_at": first if first is not None and pd.notna(first) else now,
+        "last_fetched_at": now,
+        "last_period": None if last_date is None else last_date.date().isoformat(),
+        "last_date": last_date,
+        "status": STATUS_OK,
+        "reason": "",
+        "kind": KIND_DOCUMENT,
+    }
+
+
+def _sync_documents(
+    storage: Storage,
+    source: Source,
+    client: Client,
+    wanted: Sequence[CatalogEntry],
+    index: dict[str, Row],
+    spent_today: int,
+    now: datetime.datetime,
+) -> SourceReport:
+    """Sync a source of kind document. Each request carries the documents already stored; a
+    stored document is never asked for again, so `full` means nothing here."""
+    name = source.name
+    requests = [
+        Request(entry, groups=frozenset(storage.read_documents(name, entry.source_id)["group"].astype(str)))
+        for entry in wanted
+    ]
+    calls_before = client.calls.get(name, 0)
+    remaining = None if source.daily_budget is None else source.daily_budget - spent_today
+    stored: set[str] = set()
+    failed: dict[str, str] = {}
+    added = 0
+    quota = remaining is not None and remaining <= 0
+    if not quota:
+        try:
+            for batch in source.fetch(requests):
+                for data in batch.documents:
+                    identifier = data.entry.source_id
+                    listed = storage.read_documents(name, identifier)
+                    for document in data.documents:
+                        for file in document.files:
+                            storage.write_document(name, identifier, document.group, file.name, file.content)
+                        files = [(file.name, file.role, file.url, file.content) for file in document.files]
+                        rows = document_rows(document.group, document.date, files, document.attributes, now)
+                        listed = rows if listed.empty else pd.concat([listed, rows], ignore_index=True)
+                        added += len(rows)
+                    if data.documents:
+                        storage.write_documents(name, identifier, listed)
+                    stored.add(data.key)
+                    if data.key not in failed:
+                        index[data.key] = _document_row(data, index.get(data.key), listed, now)
+                for failure in batch.failures:
+                    key = failure.entry.key
+                    reason = f"{failure.outcome.value}: {failure.reason}"
+                    failed.setdefault(key, reason)
+                    index[key] = _failed_row(failure.entry, index.get(key), failed[key], KIND_DOCUMENT)
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
+            index[entry.key] = _failed_row(entry, index.get(entry.key), failed[entry.key], KIND_DOCUMENT)
+        if untouched:
+            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
+    return SourceReport(
+        source=name,
+        ok=len(stored - set(failed)),
+        failed=tuple(failed.items()),
+        new=added,
+        revised=0,
+        calls=client.calls.get(name, 0) - calls_before,
+        quota_exhausted=quota,
+        pending=len(untouched) if quota else 0,
+    )
+
+
 def _calls_today(record: Mapping[str, Any] | None, today: datetime.date) -> int:
     budget = (record or {}).get("budget") or {}
     return int(budget.get("calls", 0)) if budget.get("day") == today.isoformat() else 0
@@ -359,6 +459,8 @@ def _sync_source(
 ) -> SourceReport:
     if source.kind is Kind.TABLE:
         return _sync_tables(storage, source, client, wanted, index, spent_today, now, full=full)
+    if source.kind is Kind.DOCUMENT:
+        return _sync_documents(storage, source, client, wanted, index, spent_today, now)
     name = source.name
     today = now.date()
     observations = storage.read_observations(name)
````

`src/data_pipeline/store/api.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/api.py b/src/data_pipeline/store/api.py
index 29de0b1..d7c55c2 100644
--- a/src/data_pipeline/store/api.py
+++ b/src/data_pipeline/store/api.py
@@ -3,6 +3,7 @@
     store = Store("D:/data")                       # read only: no keys, no catalog, no network
     store.series("fred:UNRATE")
     store.table("comtrade", "MEX", flow="X")
+    store.documents("sec_filings", "AAPL", form="10-K")
     store = Store("D:/data", catalog="catalog.yaml")
     store.sync()
 
@@ -150,7 +151,7 @@ def sync(
     # -- reading -----------------------------------------------------------------------------
 
     def index(self) -> pd.DataFrame:
-        """One row per stored series or table: provenance and last result."""
+        """One row per stored series, table or set of documents: provenance and last result."""
         return self._storage.read_index()
 
     def _row(self, name: str) -> dict[str, Any]:
@@ -167,6 +168,9 @@ def _observations(self, name: str) -> pd.DataFrame:
         if row["kind"] == st.KIND_TABLE:
             msg = f"{row['key']} is a table: read it with table({row['source']!r}, {row['source_id']!r})"
             raise StoreError(msg)
+        if row["kind"] == st.KIND_DOCUMENT:
+            msg = f"{row['key']} is a set of documents: read it with documents({row['source']!r}, {row['source_id']!r})"
+            raise StoreError(msg)
         stored = self._storage.read_observations(str(row["source"]))
         selected: pd.DataFrame = stored[stored["key"] == str(row["key"])]
         return selected
@@ -257,6 +261,49 @@ def table(
         filled = [part for part in parts if not part.empty] or parts[:1]
         return filled[0] if len(filled) == 1 else pd.concat(filled, ignore_index=True)
 
+    def documents(
+        self,
+        source: str,
+        id: str | None = None,  # noqa: A002 - the catalog calls it `id`
+        *,
+        as_of: Moment | None = None,
+        **filters: object,
+    ) -> pd.DataFrame:
+        """The list of a source's documents, one row per file: of one catalog id, or of all.
+
+        Columns: `id`, the columns of the list (`group`, `date`, `file`, `role`, `url`, `size`,
+        `sha256`, `fetched_at` and the source's own, such as `form`) and `path`, the file on
+        disk. Oldest document first. `as_of` keeps what had been published by that day;
+        `filters` keep the rows whose column equals the value.
+        """
+        names = self._storage.document_names(source)
+        if id is not None and id not in names:
+            msg = f"no stored documents of source {source!r} have id {id!r}"
+            raise UnknownSeriesError(msg)
+        if not names:
+            msg = f"no documents are stored for source {source!r}"
+            raise UnknownSeriesError(msg)
+        parts = []
+        for name in names if id is None else [id]:
+            listed = self._storage.read_documents(source, name)
+            listed.insert(0, "id", name)
+            listed["path"] = [
+                str(self._storage.document_path(source, name, str(group), str(file)))
+                for group, file in zip(listed["group"], listed["file"], strict=True)
+            ]
+            parts.append(listed)
+        filled = [part for part in parts if not part.empty] or parts[:1]
+        found = filled[0] if len(filled) == 1 else pd.concat(filled, ignore_index=True)
+        unknown = sorted(set(filters) - set(found.columns))
+        if unknown:
+            msg = f"unknown column(s) for {source} documents: {', '.join(unknown)}"
+            raise StoreError(msg)
+        for column, value in filters.items():
+            found = found[found[column] == value]
+        if as_of is not None:
+            found = found[found["date"] <= st.to_moment(as_of).tz_localize(None)]
+        return found.sort_values(["date", "id", "group", "role", "file"], kind="stable").reset_index(drop=True)
+
     def revisions(self, key: str) -> pd.DataFrame:
         """Every stored version of every period, oldest fetch first within a period."""
         stored = self._observations(key)
````

`src/data_pipeline/store/cli.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/cli.py b/src/data_pipeline/store/cli.py
index 497e80a..eb0ebaa 100644
--- a/src/data_pipeline/store/cli.py
+++ b/src/data_pipeline/store/cli.py
@@ -9,7 +9,7 @@
 import click
 
 from data_pipeline.store.api import STATE_OK, Store
-from data_pipeline.store.storage import KIND_TABLE
+from data_pipeline.store.storage import KIND_DOCUMENT, KIND_TABLE
 from data_pipeline.store.errors import StoreError
 from data_pipeline.store.sync import EXIT_CONFIGURATION, EXIT_FAILURES, EXIT_OK
 
@@ -93,17 +93,27 @@ def status_command(*, root: pathlib.Path, catalog: pathlib.Path | None) -> None:
 @click.argument("key")
 @click.option("--as-of", "as_of", default=None, help="Show the data as it was known on this date (YYYY-MM-DD).")
 def show_command(*, root: pathlib.Path, key: str, as_of: str | None) -> None:
-    """Print the citation of a series or a table and its newest rows."""
+    """Print the citation of a series, a table or a set of documents and its newest rows."""
     try:
         store = open_store(root, None, None)
         info = store.info(key)
         if info.kind == KIND_TABLE:
             table = store.table(info.source, info.source_id, as_of=as_of)
+        elif info.kind == KIND_DOCUMENT:
+            files = store.documents(info.source, info.source_id, as_of=as_of)
         else:
             rows = store.series(key, as_of=as_of)
     except StoreError as exc:
         raise fail(exc) from exc
     echo(info.label)
+    if info.kind == KIND_DOCUMENT:
+        echo(f"{info.name} | {len(files)} files")
+        shown = files.drop(columns=["id", "url", "size", "sha256", "fetched_at", "path"]).tail(SHOWN_ROWS)
+        shown = shown.assign(date=shown["date"].dt.date)
+        echo("  ".join(shown.columns))
+        for values in shown.itertuples(index=False):
+            echo("  ".join(str(value) for value in values))
+        return
     if info.kind == KIND_TABLE:
         echo(f"{info.name} | {len(table)} rows")
         newest = table.sort_values("date", kind="stable").tail(SHOWN_ROWS).drop(columns="date")
````

- [ ] **Step 5: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: `436 passed`

- [ ] **Step 6: Commit**

````bash
git add src/data_pipeline/store tests/unit/store/documents_test.py
git commit -m "feat(store): the document kind: files written once, with a list per id"
````

---

### Task 2: Shared SEC plumbing

**Files:**
- Create: `src/data_pipeline/store/sources/sec.py`
- Modify: `src/data_pipeline/store/sources/sec_xbrl.py`

No new test: the tests of `sec_xbrl` must keep passing unchanged.

- [ ] **Step 1: Write the shared module and move `sec_xbrl` onto it**

`src/data_pipeline/store/sources/sec.py`:

````python
"""What the SEC sources share: the User-Agent, the list of tickers, and how the SEC refuses.

The SEC asks every client to identify itself: `SEC_EDGAR_UA` holds a User-Agent such as
`Name name@domain.com`. It blocks for about ten minutes a client that exceeds 10 requests a
second or sends no contact, answering HTTP 403.
"""

import re
from collections.abc import Sequence
from typing import Any

import httpx

from data_pipeline.store.errors import CatalogError, NetworkError, QuotaExhaustedError, RateLimitedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, FetchBatch, Outcome, Request
from data_pipeline.store.sources.base import failures

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,11}$")
REQUESTS_PER_MINUTE = 300  # the SEC allows 10 a second
OK = 200
FORBIDDEN = 403
NOT_FOUND = 404
BLOCKED = (
    "the SEC refused the request (HTTP 403): check that SEC_EDGAR_UA is a User-Agent with a contact, "
    "such as 'Name name@domain.com', or wait ten minutes if the rate was exceeded"
)


class AnswerError(Exception):
    """The SEC answered, but not with what was asked for."""


def check_ticker(entry: CatalogEntry) -> None:
    """Raise CatalogError when the id is not a ticker in upper case."""
    if not TICKER.match(entry.source_id):
        msg = f"{entry.key}: a SEC id is a ticker in upper case, such as AAPL or BRK-B"
        raise CatalogError(msg)


class Edgar:
    """GET against the SEC for one source, identified with the user's User-Agent."""

    def __init__(self, client: Client, source: str, user_agent: str) -> None:
        self._client = client
        self._source = source
        self._user_agent = user_agent

    def get(self, url: str) -> httpx.Response | None:
        """The answer of a SEC page, or None when the SEC answers 404."""
        response = self._client.get(
            self._source,
            url,
            headers={"User-Agent": self._user_agent},
            per_minute=REQUESTS_PER_MINUTE,
        )
        if response.status_code == FORBIDDEN:
            raise QuotaExhaustedError(BLOCKED)
        if response.status_code == NOT_FOUND:
            return None
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {self._client.scrub(response.text[:200])}"
            raise AnswerError(msg)
        return response

    def json(self, url: str) -> Any:
        """The JSON of a SEC page, or None when the SEC answers 404."""
        response = self.get(url)
        return None if response is None else response.json()

    def ciks(self) -> dict[str, str]:
        """Ticker -> CIK as ten digits, from the SEC's list of companies."""
        listed = self.json(TICKERS_URL)
        if listed is None:
            msg = "HTTP 404"
            raise AnswerError(msg)
        return {str(item["ticker"]).upper(): f"{int(item['cik_str']):010d}" for item in listed.values()}


def resolve_tickers(edgar: Edgar, requests: Sequence[Request]) -> dict[str, str] | FetchBatch:
    """The ticker list, or the batch that fails every request when the list cannot be read."""
    try:
        return edgar.ciks()
    except RateLimitedError as exc:
        raise QuotaExhaustedError(str(exc)) from exc
    except NetworkError as exc:
        return FetchBatch(failures=failures(requests, Outcome.NETWORK_ERROR, f"list of tickers: {exc}"))
    except (AnswerError, KeyError, TypeError, ValueError, AttributeError) as exc:
        reason = f"list of tickers: unexpected answer ({type(exc).__name__}: {exc})"
        return FetchBatch(failures=failures(requests, Outcome.SOURCE_ERROR, reason))


def unknown_ticker(entry: CatalogEntry) -> str:
    return f"the SEC lists no company with ticker {entry.source_id!r}"
````

`src/data_pipeline/store/sources/sec_xbrl.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/sources/sec_xbrl.py b/src/data_pipeline/store/sources/sec_xbrl.py
index 30c87da..75a1fc6 100644
--- a/src/data_pipeline/store/sources/sec_xbrl.py
+++ b/src/data_pipeline/store/sources/sec_xbrl.py
@@ -12,41 +12,35 @@
 """
 
 import datetime
-import re
 from collections.abc import Iterator, Mapping, Sequence
 from typing import Any
 
 from data_pipeline.store import keys
-from data_pipeline.store.errors import CatalogError, NetworkError, QuotaExhaustedError, RateLimitedError
+from data_pipeline.store.errors import NetworkError, QuotaExhaustedError, RateLimitedError
 from data_pipeline.store.http import Client
 from data_pipeline.store.keys import Credentials
 from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Kind, Outcome, Request, TableData
-from data_pipeline.store.sources.base import failures, missing_key, reject_params
+from data_pipeline.store.sources.base import missing_key, reject_params
+from data_pipeline.store.sources.sec import (
+    REQUESTS_PER_MINUTE,
+    AnswerError,
+    Edgar,
+    check_ticker,
+    resolve_tickers,
+    unknown_ticker,
+)
 
-TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
 FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
-TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,11}$")
 KEY_COLUMNS = ("taxonomy", "concept", "unit", "start", "end")
 VALUE_COLUMNS = ("value",)
 ATTRIBUTE_COLUMNS = ("form", "accession", "filed", "fiscal_year", "fiscal_period", "frame")
 STALE_AFTER_DAYS = 200  # a quarter, the filing deadline, and slack
 TOLERANCE = 1e-9  # relative: the same number reported again is not a restatement
-OK = 200
-FORBIDDEN = 403
-NOT_FOUND = 404
-BLOCKED = (
-    "the SEC refused the request (HTTP 403): check that SEC_EDGAR_UA is a User-Agent with a contact, "
-    "such as 'Name name@domain.com', or wait ten minutes if the rate was exceeded"
-)
 NO_FACTS = "the SEC has no XBRL facts for this company"
 
 Row = dict[str, object]
 
 
-class _AnswerError(Exception):
-    """The SEC answered, but not with what was asked for."""
-
-
 def _text(value: object) -> str:
     return "" if value is None else str(value)
 
@@ -112,7 +106,7 @@ def versions(payload: Mapping[str, Any]) -> tuple[Row, ...]:
 class SecXbrl:
     name = "sec_xbrl"
     kind = Kind.TABLE
-    requests_per_minute = 300  # the SEC allows 10 a second
+    requests_per_minute = REQUESTS_PER_MINUTE
     daily_budget: int | None = None
 
     def __init__(self, client: Client, credentials: Credentials) -> None:
@@ -121,71 +115,37 @@ def __init__(self, client: Client, credentials: Credentials) -> None:
 
     def validate(self, entry: CatalogEntry) -> None:
         reject_params(entry)
-        if not TICKER.match(entry.source_id):
-            msg = f"{entry.key}: a SEC id is a ticker in upper case, such as AAPL or BRK-B"
-            raise CatalogError(msg)
+        check_ticker(entry)
 
     def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
         if not self._user_agent:
             yield missing_key(requests, keys.SEC_UA)
             return
-        try:
-            ciks = self._ciks()
-        except RateLimitedError as exc:
-            raise QuotaExhaustedError(str(exc)) from exc
-        except NetworkError as exc:
-            yield FetchBatch(failures=failures(requests, Outcome.NETWORK_ERROR, f"list of tickers: {exc}"))
-            return
-        except (_AnswerError, KeyError, TypeError, ValueError, AttributeError) as exc:
-            reason = f"list of tickers: unexpected answer ({type(exc).__name__}: {exc})"
-            yield FetchBatch(failures=failures(requests, Outcome.SOURCE_ERROR, reason))
+        edgar = Edgar(self._client, self.name, self._user_agent)
+        ciks = resolve_tickers(edgar, requests)
+        if isinstance(ciks, FetchBatch):
+            yield ciks
             return
         for request in requests:
             entry = request.entry
             cik = ciks.get(entry.source_id)
             if cik is None:
-                reason = f"the SEC lists no company with ticker {entry.source_id!r}"
-                yield FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
+                yield FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, unknown_ticker(entry)),))
                 continue
             try:
-                yield self._company(entry, cik)
+                yield self._company(edgar, entry, cik)
             except RateLimitedError as exc:
                 raise QuotaExhaustedError(str(exc)) from exc
             except NetworkError as exc:
                 yield FetchBatch(failures=(Failure(entry, Outcome.NETWORK_ERROR, str(exc)),))
-            except _AnswerError as exc:
+            except AnswerError as exc:
                 yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, str(exc)),))
             except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
                 reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                 yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, reason),))
 
-    def _get(self, url: str) -> Any:
-        """The JSON of a SEC page, or None when the SEC answers 404."""
-        response = self._client.get(
-            self.name,
-            url,
-            headers={"User-Agent": str(self._user_agent)},
-            per_minute=self.requests_per_minute,
-        )
-        if response.status_code == FORBIDDEN:
-            raise QuotaExhaustedError(BLOCKED)
-        if response.status_code == NOT_FOUND:
-            return None
-        if response.status_code != OK:
-            msg = f"HTTP {response.status_code}: {self._client.scrub(response.text[:200])}"
-            raise _AnswerError(msg)
-        return response.json()
-
-    def _ciks(self) -> dict[str, str]:
-        """Ticker -> CIK as ten digits, from the SEC's list of companies."""
-        listed = self._get(TICKERS_URL)
-        if listed is None:
-            msg = "HTTP 404"
-            raise _AnswerError(msg)
-        return {str(item["ticker"]).upper(): f"{int(item['cik_str']):010d}" for item in listed.values()}
-
-    def _company(self, entry: CatalogEntry, cik: str) -> FetchBatch:
-        payload = self._get(FACTS_URL.format(cik=cik))
+    def _company(self, edgar: Edgar, entry: CatalogEntry, cik: str) -> FetchBatch:
+        payload = edgar.json(FACTS_URL.format(cik=cik))
         if payload is None:
             return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, NO_FACTS),))
         table = TableData(
````

- [ ] **Step 2: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sec_xbrl_test.py -q`
Expected: every test passes

- [ ] **Step 3: Commit**

````bash
git add src/data_pipeline/store/sources/sec.py src/data_pipeline/store/sources/sec_xbrl.py
git commit -m "refactor(store): share the SEC plumbing between sources"
````

---

### Task 3: The SEC filings source

**Files:**
- Create: `src/data_pipeline/store/sources/sec_filings.py`
- Modify: `src/data_pipeline/store/sources/__init__.py`
- Test: `tests/unit/store/sec_filings_test.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/store/sec_filings_test.py`:

````python
import datetime
import hashlib
import json
import pathlib

import click.testing
import httpx
import pytest

from data_pipeline.store import cli as cli_module
from data_pipeline.store import keys
from data_pipeline.store import sources as source_registry
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError, StoreError, UnknownSeriesError
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sec_filings import (
    Filing,
    SecFilings,
    Settings,
    exhibits,
    missing,
    read_filings,
    settings,
    years_before,
)

from .helpers import NOW, client, entry

TODAY = NOW.date()  # 2026-06-06
AGENT = "Jane Doe jane@example.com"
CREDENTIALS = Credentials({keys.SEC_UA: AGENT})
TICKERS = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
CIK = "0000320193"
ANNUAL = "0000320193-25-000079"
EVENT = "0000320193-26-000005"
AMENDED = "0000320193-26-000009"
OLD = "0000320193-15-000001"
INSIDER = "0000320193-26-000002"


def page(*rows):
    """A page of filings the way the SEC sends it: one list per column."""
    columns = ("accessionNumber", "form", "filingDate", "reportDate", "primaryDocument")
    return {column: [row[position] for row in rows] for position, column in enumerate(columns)}


RECENT = page(
    (AMENDED, "10-K/A", "2026-02-10", "2025-09-27", "aapl-10ka.htm"),
    (EVENT, "8-K", "2026-01-29", "2026-01-29", "aapl-8k.htm"),
    (INSIDER, "4", "2026-01-15", "2026-01-13", "xslF345X05/form4.xml"),
    (ANNUAL, "10-K", "2025-10-31", "2025-09-27", "aapl-20250927.htm"),
    (OLD, "10-K", "2015-10-28", "2015-09-26", "aapl-2015.htm"),
)
FOLDER = [
    {"name": "aapl-8k.htm"},
    {"name": "a8-kex991q1.htm"},
    {"name": "0000320193-26-000005-index.html"},
    {"name": "R1.htm"},
    {"name": "aapl-8k_htm.xml"},
    {"name": "logo.jpg"},
]


class Sec:
    """A fake SEC. `files` maps the path of a document to its bytes."""

    def __init__(self):
        self.recent = RECENT
        self.older = {}
        self.pages = []
        self.files = {
            "/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm": b"<html>annual report</html>",
            "/Archives/edgar/data/320193/000032019326000005/aapl-8k.htm": b"<html>8-K wrapper</html>",
            "/Archives/edgar/data/320193/000032019326000005/a8-kex991q1.htm": b"<html>earnings release</html>",
            "/Archives/edgar/data/320193/000032019326000009/aapl-10ka.htm": b"<html>amended</html>",
            "/Archives/edgar/data/320193/000032019315000001/aapl-2015.htm": b"<html>2015</html>",
        }
        self.seen = []
        self.fail = None

    def __call__(self, request):
        self.seen.append(request)
        path = request.url.path
        if self.fail is not None and self.fail in path:
            return httpx.Response(500)
        if path == "/files/company_tickers.json":
            return httpx.Response(200, json=TICKERS)
        if path == f"/submissions/CIK{CIK}.json":
            filings = {"recent": self.recent, "files": self.pages}
            return httpx.Response(200, json={"name": "Apple Inc.", "filings": filings})
        if path.startswith("/submissions/") and path.split("/")[-1] in self.older:
            return httpx.Response(200, json=self.older[path.split("/")[-1]])
        if path.endswith("/000032019326000005/index.json"):
            return httpx.Response(200, json={"directory": {"item": FOLDER}})
        if path in self.files:
            return httpx.Response(200, content=self.files[path])
        return httpx.Response(404)

    def paths(self):
        return [request.url.path.split("/")[-1] for request in self.seen]


def company(ticker="AAPL", **fields):
    return entry(ticker, "sec_filings", **fields)


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SecFilings(client(handler), credentials, today=lambda: TODAY)
    return list(source.fetch(requests))


def groups(batches):
    return [document.group for batch in batches for data in batch.documents for document in data.documents]


# -- settings and what is missing --------------------------------------------------------------


def test_it_is_a_document_source_paced_under_the_secs_limit():
    assert SecFilings.kind is Kind.DOCUMENT
    assert SecFilings.requests_per_minute == 300
    assert SecFilings.daily_budget is None


def test_the_defaults_are_the_periodic_and_current_forms_with_amendments_for_ten_years():
    chosen = settings(company(), TODAY)
    assert chosen.forms == {"10-K", "10-Q", "8-K", "20-F", "40-F", "10-K/A", "10-Q/A", "8-K/A", "20-F/A", "40-F/A"}
    assert chosen.start == datetime.date(2016, 6, 6)


def test_every_field_is_read():
    chosen = settings(
        company(start=datetime.date(2020, 1, 1), params={"forms": ["10-k", "6-K"], "amendments": False}), TODAY
    )
    assert chosen == Settings(frozenset({"10-K", "6-K"}), datetime.date(2020, 1, 1))
    assert settings(company(params={"forms": ["10-K/A"]}), TODAY).forms == {"10-K/A"}


def test_ten_years_before_a_leap_day():
    assert years_before(datetime.date(2024, 2, 29), 10) == datetime.date(2014, 2, 28)


@pytest.mark.parametrize(
    ("ticker", "params", "message"),
    [
        ("aapl", {}, "a SEC id is a ticker in upper case"),
        ("AAPL", {"forms": "10-K"}, "'forms' must be a non-empty list"),
        ("AAPL", {"forms": []}, "'forms' must be a non-empty list"),
        ("AAPL", {"forms": ["10-K", "../x"]}, "not a SEC form: ../X"),
        ("AAPL", {"amendments": "yes"}, "'amendments' must be true or false"),
        ("AAPL", {"since": "2020"}, "unknown field\\(s\\) for source 'sec_filings': since"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(ticker, params, message):
    source = SecFilings(client(), CREDENTIALS, today=lambda: TODAY)
    with pytest.raises(CatalogError, match=message):
        source.validate(company(ticker, params=params))


def test_a_page_of_the_sec_is_read_row_by_row():
    filings = read_filings(RECENT)
    assert filings[0] == Filing(AMENDED, "10-K/A", datetime.date(2026, 2, 10), "2025-09-27", "aapl-10ka.htm")
    assert len(filings) == 5
    blank = read_filings(page(("a", "8-K", "2026-01-01", "", None)))[0]
    assert (blank.period, blank.primary) == ("", "")


def test_missing_filings_are_wanted_recent_not_stored_and_oldest_first():
    chosen = settings(company(), TODAY)
    filings = read_filings(RECENT)
    assert [filing.accession for filing in missing(filings, chosen, frozenset())] == [ANNUAL, EVENT, AMENDED]
    assert [filing.accession for filing in missing(filings, chosen, {ANNUAL, AMENDED})] == [EVENT]
    plain = settings(company(params={"amendments": False}), TODAY)
    assert [filing.accession for filing in missing(filings, plain, frozenset())] == [ANNUAL, EVENT]
    longer = settings(company(start=datetime.date(2015, 1, 1), params={"forms": ["10-K"], "amendments": False}), TODAY)
    assert [filing.accession for filing in missing(filings, longer, frozenset())] == [OLD, ANNUAL]


def test_the_exhibits_of_a_folder_leave_out_the_wrapper_indexes_and_viewer_pages():
    assert exhibits(FOLDER, "aapl-8k.htm") == ["a8-kex991q1.htm"]
    assert exhibits([{"name": "EX99.HTML"}, {"name": "r12.htm"}, {"name": "AAPL-8K.HTM"}, {}], "aapl-8k.htm") == [
        "EX99.HTML"
    ]


# -- the source --------------------------------------------------------------------------------


def test_a_company_is_reached_first_and_then_gives_one_batch_per_filing():
    sec = Sec()
    batches = fetch(sec, [Request(company())])
    first = batches[0].documents[0]
    assert (first.key, first.name, first.documents) == ("sec_filings:AAPL", "Apple Inc.", ())
    assert (first.stale_after_days, first.attrs) == (140, {"cik": CIK})
    assert groups(batches) == [ANNUAL, EVENT, AMENDED]
    assert [len(batch.documents[0].documents) for batch in batches] == [0, 1, 1, 1]


def test_a_filing_is_its_primary_document_byte_for_byte():
    sec = Sec()
    document = fetch(sec, [Request(company())])[1].documents[0].documents[0]
    assert (document.group, document.date) == (ANNUAL, datetime.date(2025, 10, 31))
    assert document.attributes == {"form": "10-K", "period": "2025-09-27"}
    (file,) = document.files
    assert (file.name, file.role, file.content) == ("aapl-20250927.htm", "primary", b"<html>annual report</html>")
    assert file.url == "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm"


def test_an_8k_brings_its_exhibits():
    sec = Sec()
    document = fetch(sec, [Request(company())])[2].documents[0].documents[0]
    assert [(file.name, file.role) for file in document.files] == [
        ("aapl-8k.htm", "primary"),
        ("a8-kex991q1.htm", "exhibit"),
    ]
    assert document.files[1].content == b"<html>earnings release</html>"
    assert sec.paths().count("index.json") == 1  # only the 8-K has its folder listed


def test_every_call_carries_the_user_agent_and_stored_filings_are_not_asked_for():
    sec = Sec()
    batches = fetch(sec, [Request(company(), groups=frozenset({ANNUAL, EVENT, AMENDED}))])
    assert groups(batches) == []
    assert sec.paths() == ["company_tickers.json", f"CIK{CIK}.json"]
    assert {request.headers["User-Agent"] for request in sec.seen} == {AGENT}


def test_older_pages_are_asked_for_only_when_they_reach_the_start():
    sec = Sec()
    sec.pages = [
        {"name": "CIK0000320193-submissions-001.json", "filingFrom": "2012-01-01", "filingTo": "2017-12-31"},
        {"name": "CIK0000320193-submissions-002.json", "filingFrom": "1994-01-01", "filingTo": "2011-12-31"},
    ]
    older = "0000320193-17-000070"
    found = page((older, "10-K", "2017-11-03", "2017-09-30", "a10-k2017.htm"))
    sec.older = {"CIK0000320193-submissions-001.json": found}
    sec.files["/Archives/edgar/data/320193/000032019317000070/a10-k2017.htm"] = b"<html>2017</html>"
    batches = fetch(sec, [Request(company())])
    assert groups(batches)[0] == older
    assert "CIK0000320193-submissions-001.json" in sec.paths()
    assert "CIK0000320193-submissions-002.json" not in sec.paths()


def test_a_filing_without_a_primary_document_is_its_complete_text():
    sec = Sec()
    sec.recent = page((ANNUAL, "10-K", "2025-10-31", "2025-09-27", ""))
    sec.files[f"/Archives/edgar/data/320193/000032019325000079/{ANNUAL}.txt"] = b"<SEC-DOCUMENT>"
    document = fetch(sec, [Request(company())])[1].documents[0].documents[0]
    assert [(file.name, file.content) for file in document.files] == [(f"{ANNUAL}.txt", b"<SEC-DOCUMENT>")]


def test_without_the_user_agent_nothing_is_requested():
    calls = []
    (batch,) = fetch(calls.append, [Request(company())], Credentials())
    assert calls == []
    assert batch.failures[0].outcome is Outcome.KEY_ERROR


def test_an_unknown_ticker_fails_alone():
    sec = Sec()
    batches = fetch(sec, [Request(company("ZZZZ")), Request(company())])
    assert (batches[0].failures[0].outcome, batches[0].failures[0].reason) == (
        Outcome.NOT_FOUND,
        "the SEC lists no company with ticker 'ZZZZ'",
    )
    assert groups(batches) == [ANNUAL, EVENT, AMENDED]


def test_a_failed_download_ends_the_company_after_what_already_arrived():
    sec = Sec()
    sec.fail = "a8-kex991q1.htm"
    batches = fetch(sec, [Request(company())])
    assert groups(batches) == [ANNUAL]
    assert batches[-1].failures[0].outcome is Outcome.NETWORK_ERROR


def test_a_document_the_sec_lists_but_does_not_have_is_a_source_error():
    sec = Sec()
    del sec.files["/Archives/edgar/data/320193/000032019326000009/aapl-10ka.htm"]
    batches = fetch(sec, [Request(company())])
    assert groups(batches) == [ANNUAL, EVENT]
    failure = batches[-1].failures[0]
    assert (failure.outcome, failure.reason) == (
        Outcome.SOURCE_ERROR,
        f"{AMENDED}: the SEC lists aapl-10ka.htm but does not have it",
    )


def test_a_block_by_the_sec_stops_the_source():
    with pytest.raises(QuotaExhaustedError, match="check that SEC_EDGAR_UA"):
        fetch(lambda _request: httpx.Response(403), [Request(company())])


def test_an_unexpected_list_of_filings_is_a_source_error():
    def handler(request):
        if request.url.path == "/files/company_tickers.json":
            return httpx.Response(200, json=TICKERS)
        return httpx.Response(200, json={"name": "Apple Inc.", "filings": {}})

    (batch,) = fetch(handler, [Request(company())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR


# -- through the store -------------------------------------------------------------------------


@pytest.fixture
def sec():
    return Sec()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, sec, clock, monkeypatch):
    def filings(http, credentials):
        return SecFilings(http, credentials, today=TODAY.replace)

    monkeypatch.setitem(source_registry.REGISTRY, "sec_filings", filings)
    (tmp_path / "catalog.yaml").write_text("- source: sec_filings\n  ids: [AAPL]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SEC_EDGAR_UA={AGENT}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(sec),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def test_first_sync_writes_the_files_and_the_list(store, tmp_path):
    report = store.report.sources[0]
    assert (report.ok, report.new, report.revised, report.calls) == (1, 4, 0, 7)
    folder = tmp_path / "store" / "documents" / "sec_filings" / "AAPL"
    assert (folder / ANNUAL / "aapl-20250927.htm").read_bytes() == b"<html>annual report</html>"
    assert (folder / EVENT / "a8-kex991q1.htm").read_bytes() == b"<html>earnings release</html>"
    assert sorted(path.name for path in folder.iterdir()) == [ANNUAL, EVENT, AMENDED]
    assert (folder.parent / "AAPL.parquet").exists()


def test_documents_lists_every_file_with_its_path(store, tmp_path):
    listed = store.documents("sec_filings", "AAPL")
    assert list(listed.columns) == [
        "id", "group", "date", "file", "role", "url", "size", "sha256", "form", "period", "fetched_at", "path",
    ]  # fmt: skip
    assert list(zip(listed["form"], listed["role"], listed["file"], strict=True)) == [
        ("10-K", "primary", "aapl-20250927.htm"),
        ("8-K", "exhibit", "a8-kex991q1.htm"),
        ("8-K", "primary", "aapl-8k.htm"),
        ("10-K/A", "primary", "aapl-10ka.htm"),
    ]
    first = listed.iloc[0]
    assert (first["id"], first["group"], first["period"]) == ("AAPL", ANNUAL, "2025-09-27")
    assert first["sha256"] == hashlib.sha256(b"<html>annual report</html>").hexdigest()
    assert first["size"] == len(b"<html>annual report</html>")
    assert pathlib.Path(first["path"]).read_bytes() == b"<html>annual report</html>"
    assert len(store.documents("sec_filings")) == 4


def test_filters_and_as_of(store):
    assert list(store.documents("sec_filings", "AAPL", form="8-K", role="exhibit")["file"]) == ["a8-kex991q1.htm"]
    assert list(store.documents("sec_filings", "AAPL", as_of="2026-01-28")["form"]) == ["10-K"]
    assert len(store.documents("sec_filings", "AAPL", as_of="2026-01-29")) == 3
    assert store.documents("sec_filings", "AAPL", as_of="2020-01-01").empty
    with pytest.raises(StoreError, match="unknown column\\(s\\) for sec_filings documents: kind"):
        store.documents("sec_filings", "AAPL", kind="10-K")


def test_unknown_documents_are_an_error(store):
    with pytest.raises(UnknownSeriesError, match="no stored documents of source 'sec_filings' have id 'MSFT'"):
        store.documents("sec_filings", "MSFT")
    with pytest.raises(UnknownSeriesError, match="no documents are stored for source 'fred'"):
        store.documents("fred")


def test_a_second_sync_downloads_nothing(store, sec, clock):
    before = len(sec.seen)
    clock["now"] = NOW + datetime.timedelta(days=1)
    report = store.sync().sources[0]
    assert (report.ok, report.new, report.calls) == (1, 0, 2)
    assert [request.url.path.split("/")[-1] for request in sec.seen[before:]] == [
        "company_tickers.json",
        f"CIK{CIK}.json",
    ]
    assert len(store.documents("sec_filings", "AAPL")) == 4


def test_full_does_not_download_a_stored_filing_again(store, sec):
    before = len(sec.seen)
    report = store.sync(full=True).sources[0]
    assert (report.new, len(sec.seen) - before) == (0, 2)


def test_a_new_filing_is_added_and_the_old_files_stay(store, sec, clock, tmp_path):
    newer = "0000320193-26-000040"
    quarter = (newer, "10-Q", "2026-05-01", "2026-03-28", "aapl-20260328.htm")
    sec.recent = page(quarter, *zip(*RECENT.values(), strict=True))
    sec.files["/Archives/edgar/data/320193/000032019326000040/aapl-20260328.htm"] = b"<html>quarter</html>"
    sec.files["/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm"] = b"<html>changed at the SEC</html>"
    clock["now"] = NOW + datetime.timedelta(days=1)
    report = store.sync().sources[0]
    assert (report.new, report.calls) == (1, 3)
    listed = store.documents("sec_filings", "AAPL")
    assert listed.iloc[-1]["group"] == newer
    folder = tmp_path / "store" / "documents" / "sec_filings" / "AAPL"
    assert (folder / ANNUAL / "aapl-20250927.htm").read_bytes() == b"<html>annual report</html>"
    assert store.info("sec_filings:AAPL").last_period == "2026-05-01"


def test_a_run_that_fails_in_the_middle_keeps_what_arrived_and_the_next_resumes(tmp_path, sec, clock, monkeypatch):
    def filings(http, credentials):
        return SecFilings(http, credentials, today=TODAY.replace)

    monkeypatch.setitem(source_registry.REGISTRY, "sec_filings", filings)
    (tmp_path / ".env").write_text(f"SEC_EDGAR_UA={AGENT}\n", encoding="utf-8")
    store = Store(
        tmp_path / "store",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(sec),
        sleep=lambda _seconds: None,
    )
    store.add("sec_filings", ["AAPL"])
    sec.fail = "a8-kex991q1.htm"
    report = store.sync()
    assert report.exit_code == 1
    assert report.sources[0].new == 1
    assert list(store.documents("sec_filings", "AAPL")["group"]) == [ANNUAL]
    info = store.info("sec_filings:AAPL")
    assert (info.status, info.kind) == ("failed", "document")
    sec.fail = None
    report = store.sync()
    assert (report.exit_code, report.sources[0].new) == (0, 3)
    assert len(store.documents("sec_filings", "AAPL")) == 4


def test_the_index_describes_the_company(store):
    info = store.info("sec_filings:AAPL")
    assert (info.kind, info.name, info.last_period) == ("document", "Apple Inc.", "2026-02-10")
    assert info.label == "[SEC EDGAR: AAPL, 2026-02-10, fetched 2026-06-06]"
    row = store.index().iloc[0]
    assert json.loads(row["attrs"]) == {"cik": CIK}
    assert row["stale_after_days"] == 140


def test_status_is_stale_after_140_days_without_a_filing(store, clock):
    assert store.status().iloc[0]["state"] == "ok"  # 116 days after 2026-02-10
    clock["now"] = datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2026-02-10 (141 days ago)"


def test_series_and_table_refuse_a_set_of_documents(store):
    message = "sec_filings:AAPL is a set of documents: read it with documents\\('sec_filings', 'AAPL'\\)"
    with pytest.raises(StoreError, match=message):
        store.series("sec_filings:AAPL")
    with pytest.raises(UnknownSeriesError, match="no stored table of source 'sec_filings' has id 'AAPL'"):
        store.table("sec_filings", "AAPL")


def test_show_prints_the_citation_and_the_newest_files(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    result = click.testing.CliRunner().invoke(cli, ["show", "--root", str(tmp_path / "store"), "sec_filings:AAPL"])
    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "[SEC EDGAR: AAPL, 2026-02-10, fetched 2026-06-06]",
        "Apple Inc. | 4 files",
        "group  date  file  role  form  period",
        f"{ANNUAL}  2025-10-31  aapl-20250927.htm  primary  10-K  2025-09-27",
        f"{EVENT}  2026-01-29  a8-kex991q1.htm  exhibit  8-K  2026-01-29",
        f"{EVENT}  2026-01-29  aapl-8k.htm  primary  8-K  2026-01-29",
        f"{AMENDED}  2026-02-10  aapl-10ka.htm  primary  10-K/A  2025-09-27",
    ]
````

- [ ] **Step 2: Run it to make sure it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sec_filings_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.sec_filings'`

- [ ] **Step 3: Write the source and register it**

`src/data_pipeline/store/sources/sec_filings.py`:

````python
"""SEC filings: the documents a company files, as the files the SEC publishes.

One catalog id is one company, written as its ticker (`AAPL`). By default: forms 10-K, 10-Q,
8-K, 20-F and 40-F with their amendments, filed in the last 10 years. Each filing is one
document: its primary file and, for an 8-K, its exhibits (the earnings release is one).

A filing never changes at the SEC. Each request carries the accession numbers already stored;
the source lists the company's filings and downloads the ones that are missing, oldest first,
one filing a batch, so a run that stops resumes by itself. A filing that fails ends that
company's run; the next run asks for it again.
"""

import dataclasses
import datetime
import pathlib
import re
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.store import keys
from data_pipeline.store.errors import CatalogError, NetworkError, QuotaExhaustedError, RateLimitedError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import (
    CatalogEntry,
    Document,
    DocumentData,
    DocumentFile,
    Failure,
    FetchBatch,
    Kind,
    Outcome,
    Request,
)
from data_pipeline.store.sources.base import missing_key, reject_params, utc_today
from data_pipeline.store.sources.sec import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Edgar,
    check_ticker,
    resolve_tickers,
    unknown_ticker,
)

SUBMISSIONS_URL = "https://data.sec.gov/submissions/{name}"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{folder}/{file}"
FIELDS = ("forms", "amendments")
DEFAULT_FORMS = ("10-K", "10-Q", "8-K", "20-F", "40-F")
DEFAULT_YEARS = 10
AMENDMENT = "/A"
FORM = re.compile(r"^[0-9A-Z][0-9A-Z \-]{0,18}$")
EXHIBITS_OF = "8-K"
VIEWER_PAGE = re.compile(r"r\d+\.html?")  # a page of the SEC's XBRL viewer, not a document
PRIMARY = "primary"
EXHIBIT = "exhibit"
STALE_AFTER_DAYS = 140  # a quarter and slack


@dataclasses.dataclass(frozen=True, slots=True)
class Settings:
    """The fields of an entry, with their defaults filled in."""

    forms: frozenset[str]  # with the amendments when they are wanted
    start: datetime.date


@dataclasses.dataclass(frozen=True, slots=True)
class Filing:
    accession: str
    form: str
    filed: datetime.date
    period: str
    primary: str  # empty when the SEC names no primary document


def years_before(day: datetime.date, years: int) -> datetime.date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:  # 29 February
        return day.replace(year=day.year - years, day=28)


def settings(entry: CatalogEntry, today: datetime.date) -> Settings:
    """Read and check the entry's fields. Raises CatalogError on anything this source rejects."""
    reject_params(entry, FIELDS)
    check_ticker(entry)
    forms = entry.params.get("forms", list(DEFAULT_FORMS))
    if not isinstance(forms, list | tuple) or not forms:
        msg = f"{entry.key}: 'forms' must be a non-empty list"
        raise CatalogError(msg)
    wanted = {str(form).upper() for form in forms}
    unknown = sorted(form for form in wanted if not FORM.match(form.removesuffix(AMENDMENT)))
    if unknown:
        msg = f"{entry.key}: not a SEC form: {', '.join(unknown)}"
        raise CatalogError(msg)
    amendments = entry.params.get("amendments", True)
    if not isinstance(amendments, bool):
        msg = f"{entry.key}: 'amendments' must be true or false"
        raise CatalogError(msg)
    if amendments:
        wanted |= {form + AMENDMENT for form in wanted if not form.endswith(AMENDMENT)}
    return Settings(frozenset(wanted), entry.start or years_before(today, DEFAULT_YEARS))


def read_filings(page: Mapping[str, Any]) -> list[Filing]:
    """The filings of one page of a company's list (the SEC sends one list per column)."""
    return [
        Filing(
            accession=str(accession),
            form=str(form),
            filed=datetime.date.fromisoformat(str(filed)),
            period=str(period or ""),
            primary=str(primary or ""),
        )
        for accession, form, filed, period, primary in zip(
            page["accessionNumber"],
            page["form"],
            page["filingDate"],
            page["reportDate"],
            page["primaryDocument"],
            strict=True,
        )
    ]


def missing(filings: Sequence[Filing], chosen: Settings, stored: Collection[str]) -> list[Filing]:
    """The filings to download: wanted form, filed since the start, not stored. Oldest first."""
    found = {
        filing.accession: filing
        for filing in filings
        if filing.form.upper() in chosen.forms and filing.filed >= chosen.start and filing.accession not in stored
    }
    return sorted(found.values(), key=lambda filing: (filing.filed, filing.accession))


def exhibits(items: Sequence[Mapping[str, Any]], primary: str) -> list[str]:
    """The exhibit documents of a filing's folder: every .htm that is not the primary document,
    an index page or a page of the XBRL viewer."""
    names = []
    for item in items:
        name = str(item.get("name") or "")
        low = name.lower()
        if not low.endswith((".htm", ".html")) or low == primary.lower() or "index" in low:
            continue
        if VIEWER_PAGE.fullmatch(low):
            continue
        names.append(name)
    return names


class SecFilings:
    name = "sec_filings"
    kind = Kind.DOCUMENT
    requests_per_minute = REQUESTS_PER_MINUTE
    daily_budget: int | None = None

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._user_agent = credentials.get(keys.SEC_UA)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        settings(entry, self._today())

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._user_agent:
            yield missing_key(requests, keys.SEC_UA)
            return
        edgar = Edgar(self._client, self.name, self._user_agent)
        ciks = resolve_tickers(edgar, requests)
        if isinstance(ciks, FetchBatch):
            yield ciks
            return
        for request in requests:
            entry = request.entry
            cik = ciks.get(entry.source_id)
            if cik is None:
                yield FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, unknown_ticker(entry)),))
                continue
            try:
                yield from self._company(edgar, request, cik)
            except RateLimitedError as exc:
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=(Failure(entry, Outcome.NETWORK_ERROR, str(exc)),))
            except AnswerError as exc:
                yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, str(exc)),))
            except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, reason),))

    def _company(self, edgar: Edgar, request: Request, cik: str) -> Iterator[FetchBatch]:
        """First a batch without documents, so a company with nothing new is not a failure;
        then one batch per filing."""
        entry = request.entry
        chosen = settings(entry, self._today())
        name, filings = self._filings(edgar, cik, chosen.start)

        def batch(*documents: Document) -> FetchBatch:
            data = DocumentData(
                entry=entry,
                key=entry.key,
                name=name or entry.source_id,
                documents=documents,
                stale_after_days=STALE_AFTER_DAYS,
                attrs={"cik": cik},
            )
            return FetchBatch(documents=(data,))

        yield batch()
        for filing in missing(filings, chosen, request.groups):
            yield batch(self._document(edgar, cik, filing))

    def _filings(self, edgar: Edgar, cik: str, start: datetime.date) -> tuple[str, list[Filing]]:
        """(company name, its filings). Older pages are asked for only when they reach `start`."""
        root = edgar.json(SUBMISSIONS_URL.format(name=f"CIK{cik}.json"))
        if root is None:
            msg = "the SEC has no list of filings for this company"
            raise AnswerError(msg)
        filings = read_filings(root["filings"]["recent"])
        for page in root["filings"].get("files") or []:
            if datetime.date.fromisoformat(str(page["filingTo"])) < start:
                continue
            older = edgar.json(SUBMISSIONS_URL.format(name=page["name"]))
            if older is None:
                msg = f"the SEC lists the page {page['name']} but does not have it"
                raise AnswerError(msg)
            filings.extend(read_filings(older))
        return str(root.get("name") or ""), filings

    def _document(self, edgar: Edgar, cik: str, filing: Filing) -> Document:
        folder = filing.accession.replace("-", "")
        primary = filing.primary or f"{filing.accession}.txt"  # no primary document: the complete filing
        names = [(primary, PRIMARY)]
        if filing.form.upper().startswith(EXHIBITS_OF):
            listing = edgar.json(ARCHIVE_URL.format(cik=int(cik), folder=folder, file="index.json"))
            items = [] if listing is None else listing["directory"]["item"]
            names.extend((name, EXHIBIT) for name in exhibits(items, pathlib.PurePosixPath(primary).name))
        files = []
        for name, role in names:
            url = ARCHIVE_URL.format(cik=int(cik), folder=folder, file=name)
            response = edgar.get(url)
            if response is None:
                msg = f"{filing.accession}: the SEC lists {name} but does not have it"
                raise AnswerError(msg)
            files.append(DocumentFile(pathlib.PurePosixPath(name).name, response.content, url, role))
        return Document(
            group=filing.accession,
            date=filing.filed,
            files=tuple(files),
            attributes={"form": filing.form, "period": filing.period},
        )
````

`src/data_pipeline/store/sources/__init__.py` (apply this diff):

````diff
diff --git a/src/data_pipeline/store/sources/__init__.py b/src/data_pipeline/store/sources/__init__.py
index bb7cea4..6441d7d 100644
--- a/src/data_pipeline/store/sources/__init__.py
+++ b/src/data_pipeline/store/sources/__init__.py
@@ -13,6 +13,7 @@
 from data_pipeline.store.sources.fred import Fred
 from data_pipeline.store.sources.inegi import Inegi
 from data_pipeline.store.sources.sdmx import PROVIDERS, Sdmx
+from data_pipeline.store.sources.sec_filings import SecFilings
 from data_pipeline.store.sources.sec_xbrl import SecXbrl
 from data_pipeline.store.sources.worldbank import WorldBank
 
@@ -36,6 +37,7 @@ def build(client: Client, credentials: Credentials) -> Source:
     "dbnomics": Dbnomics,
     "fred": Fred,
     "inegi": Inegi,
+    "sec_filings": SecFilings,
     "sec_xbrl": SecXbrl,
     "worldbank": WorldBank,
     **{name: _sdmx(name) for name in PROVIDERS},
@@ -52,6 +54,7 @@ def build(client: Client, credentials: Credentials) -> Source:
     "imf": "IMF",
     "inegi": "INEGI",
     "oecd": "OECD",
+    "sec_filings": "SEC EDGAR",
     "sec_xbrl": "SEC EDGAR",
     "worldbank": "World Bank",
 }
````

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: `473 passed`

- [ ] **Step 5: Commit**

````bash
git add src/data_pipeline/store/sources tests/unit/store/sec_filings_test.py
git commit -m "feat(store): add the SEC filings source"
````

---

### Task 4: Live test, comparison script and documentation

**Files:**
- Create: `tests/live/sec_filings_live_test.py`, `scripts/compare_sec_filings_with_research_analyst.py`
- Modify: `README.md`, `CHANGELOG.md`

- [ ] **Step 1: Write the live test**

`tests/live/sec_filings_live_test.py`:

````python
"""A few real calls to the SEC. Off by default; run with:  pytest -m live tests/live

Needs SEC_EDGAR_UA (a User-Agent with a contact) in the environment or in ./.env, and network
access. It downloads Apple's 10-K filings of one year: the list of tickers, the list of filings
and one or two documents.
"""

import datetime
import hashlib
import pathlib

import pytest

from data_pipeline.store import keys
from data_pipeline.store.api import Store
from data_pipeline.store.keys import load_credentials


@pytest.mark.live
def test_the_sec_still_publishes_filings_in_the_expected_shape(tmp_path):
    if load_credentials().get(keys.SEC_UA) is None:
        pytest.skip("SEC_EDGAR_UA is not set")
    store = Store(tmp_path / "store")
    store.add("sec_filings", ["AAPL"], start=datetime.date(2023, 10, 1), forms=["10-K"], amendments=False)
    report = store.sync()
    assert report.exit_code == 0
    listed = store.documents("sec_filings", "AAPL")
    assert set(listed["form"]) == {"10-K"}
    assert "0000320193-23-000106" in set(listed["group"])  # the 10-K for fiscal 2023, filed 2023-11-03
    annual = listed[listed["group"] == "0000320193-23-000106"].iloc[0]
    assert (annual["file"], annual["role"]) == ("aapl-20230930.htm", "primary")
    assert str(annual["date"].date()) == "2023-11-03"
    content = pathlib.Path(annual["path"]).read_bytes()
    assert content.startswith(b"<?xml") or b"<html" in content[:2000].lower()
    assert hashlib.sha256(content).hexdigest() == annual["sha256"]
    assert len(content) == annual["size"] > 100_000
    again = store.sync()
    assert (again.sources[0].new, again.sources[0].calls) == (0, 2)
````

- [ ] **Step 2: Write the comparison script**

`scripts/compare_sec_filings_with_research_analyst.py`:

````python
"""Acceptance check: SEC filings of the store against research_analyst's own download.

    python C:/Proyectos/research_analyst/tools/sec_fetch.py AAPL --dest D:/tmp/filings --since 2023-10-05 --amendments --ua "..."
    python scripts/compare_sec_filings_with_research_analyst.py D:/tmp/filings/AAPL_filings_manifest.csv D:/datos/store AAPL

Both downloads are matched by source URL. Every file of the tool's manifest must be in the
store with identical bytes; files only one side holds are listed. Exit code 0 when every common
file is identical and nothing is missing from the store, 1 otherwise.
"""

import hashlib
import pathlib
import sys

import pandas as pd

import data_pipeline

SHOWN = 10


def compare(manifest: pathlib.Path, root: pathlib.Path, ticker: str) -> int:
    theirs = pd.read_csv(manifest, dtype=str, keep_default_na=False)
    ours = data_pipeline.Store(root).documents("sec_filings", ticker)
    both = theirs.merge(ours, left_on="source", right_on="url", how="outer", suffixes=("_theirs", "_ours"), indicator=True)
    missing = both[both["_merge"] == "left_only"]
    extra = both[both["_merge"] == "right_only"]
    common = both[both["_merge"] == "both"]
    different = []
    for row in common.to_dict(orient="records"):
        their_bytes = (manifest.parent / row["file_theirs"]).read_bytes()
        if hashlib.sha256(their_bytes).hexdigest() != row["sha256"]:
            different.append(row)
    print(
        f"{len(common)} files in common, {len(different)} with different bytes, "
        f"{len(missing)} only in the tool's download, {len(extra)} only in the store"
    )
    for row in different[:SHOWN]:
        print(f"  [x] {row['source']}: bytes differ")
    for row in missing.head(SHOWN).to_dict(orient="records"):
        print(f"  [x] {row['source']}: not in the store")
    for row in extra.head(SHOWN).to_dict(orient="records"):
        print(f"  [i] {row['url']}: only in the store ({row['form_ours']})")
    failed = len(different) + len(missing)
    print("[x] the downloads differ" if failed else "[ok] every file of the tool's download is in the store, identical")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]))
````

- [ ] **Step 3: Document**

`README.md` (apply this diff):

````diff
diff --git a/README.md b/README.md
index 1f72a53..0e0f09c 100644
--- a/README.md
+++ b/README.md
@@ -72,7 +72,8 @@ ## Public-data store (preview)
 the IMF, read directly from each publisher, plus DBnomics for what has no direct id yet. It also
 keeps tables: goods trade from UN Comtrade, one table per reporting country, and the facts
 companies report to the SEC in XBRL, one table per company, each version dated with the day it
-was filed.
+was filed. And it keeps documents: the filings a company sends to the SEC (10-K, 10-Q, 8-K with
+their exhibits, 20-F, 40-F), as the files the SEC publishes, with a list of what each file is.
 
 Declare what you want in a YAML catalog:
 
@@ -114,6 +115,7 @@ ## Public-data store (preview)
 store.info("fred:UNRATE").label                  # "[FRED: UNRATE, 2026-05, fetched 2026-06-06]"
 store.table("comtrade", "MEX", flow="X")         # a table: one row per partner, product and period
 store.table("sec_xbrl", "AAPL", concept="Assets", as_of="2024-03-01")  # as filed by that day
+store.documents("sec_filings", "AAPL", form="10-K")  # the files on disk, with their path
 ```
 
 The same FRED terms of use described above apply.
````

`CHANGELOG.md` (apply this diff):

````diff
diff --git a/CHANGELOG.md b/CHANGELOG.md
index befa953..9950730 100644
--- a/CHANGELOG.md
+++ b/CHANGELOG.md
@@ -8,6 +8,7 @@ # Changelog
 
 ## [Unreleased]
 ### Added
+- Public-data store: a third kind of data, the **document**, and its first source, **SEC filings** (`- source: sec_filings` / `ids: [AAPL, MSFT]`; `SEC_EDGAR_UA`). Each filing is stored as the files the SEC publishes, byte for byte: its primary document and, for an 8-K, its exhibits, under `documents/sec_filings/<ticker>/<accession>/`, with a list (`<ticker>.parquet`) that says the form, filing day, report date, size and hash of every file. Default scope: 10-K, 10-Q, 8-K, 20-F and 40-F with their amendments, filed in the last 10 years; catalog fields `forms`, `amendments` and `start` widen it. A filing is downloaded once and never rewritten; a sync asks only for what is missing, one filing a batch, and a run that stops resumes by itself. Read with `Store.documents("sec_filings", "AAPL", form="10-K", as_of=...)`, which returns the list with the path of each file; `status` and `show sec_filings:AAPL` cover documents.
 - Public-data store: **SEC XBRL company facts** (`- source: sec_xbrl` / `ids: [AAPL, MSFT]`; `SEC_EDGAR_UA`, a User-Agent with a contact). One table per company with every fact it has reported, raw: taxonomy, concept, unit, start, end, value, and the form, accession number and filing day of the filing that reported it. A fact that several filings repeat is stored once; a restatement adds a version dated with its own filing day, so `Store.table("sec_xbrl", "AAPL", concept="Revenues", as_of="2024-03-01")` returns what had been filed by that day. One call per company per run. Tables can now carry attribute columns and versions dated by the source.
 - Public-data store: a second kind of data, the **table**, and its first source, **UN Comtrade** (`COMTRADE_API_KEY`). One catalog id is one reporting country (`- source: comtrade` / `ids: [MEX, USA]`); its table holds trade value and net weight by partner, flow, HS product and period, annual from 2000 and the last 75 closed months, with the same append-only revision history as series. A sync asks only for the periods the store lacks plus a revision window (2 years, 12 months), 12 periods a call, within a persisted budget of 450 calls a day; a run stopped by the quota resumes on the next one. Optional catalog fields: `level` (`AG2`, `AG4`, `AG6`), `partners`, `flows`, `annual_from`, `months`. Read with `Store.table("comtrade", "MEX", flow="X", as_of=...)`; `status` and `show comtrade:MEX` cover tables. The index gains a `kind` column; stores written before read as all series.
 - Public-data store: the SDMX source asks for several series in one call. Series of a dataflow that differ in one position of the key (usually the country) travel together, joined by `+`, up to 50 a call; the answer is split back by the column that carries those values, and a group that cannot be read as a group is asked for series by series. The 458 SDMX series of the macro catalog take about 17 calls instead of 458, and the OECD, which allows about 60 requests an hour, no longer takes an hour.
````

- [ ] **Step 4: Verify everything**

Run: `.venv/Scripts/python -m ruff check .`
Expected: `All checks passed!`
Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 94 source files`
Run: `.venv/Scripts/python -m pytest -q`
Expected: `1427 passed, 15 deselected`

- [ ] **Step 5: Commit**

````bash
git add tests/live/sec_filings_live_test.py scripts/compare_sec_filings_with_research_analyst.py README.md CHANGELOG.md
git commit -m "docs(store): document SEC filings; add the live test and the comparison"
````

---

### Task 5: Acceptance against the SEC

Calls the SEC with the User-Agent the user approved (about 250 calls). The store goes in a scratch folder, not in the repository.

- [ ] **Step 1: Live test** (about 5 calls)

Run with `SEC_EDGAR_UA` in the environment: `.venv/Scripts/python -m pytest -m live tests/live/sec_filings_live_test.py -q`
Expected: `1 passed`.

- [ ] **Step 2: Load two companies, three years back**

Catalog `acceptance.yaml` in the scratch folder (the date is today minus three years):

````yaml
- source: sec_filings
  ids: [AAPL, SAP]
  start: 2023-10-05
````

Run: `.venv/Scripts/python -m data_pipeline.store sync --root <scratch>/store --catalog <scratch>/acceptance.yaml --env-file <scratch>/.env`
Expected: `[ok]  sec_filings  2 ...`, exit code 0; files under `<scratch>/store/documents/sec_filings/AAPL/` and `SAP/`.

- [ ] **Step 3: Second sync**

Run the same command again.
Expected: 0 new, 3 calls (the list of tickers and one list of filings per company).

- [ ] **Step 4: Compare with research_analyst**

Run: `.venv/Scripts/python C:/Proyectos/research_analyst/tools/sec_fetch.py AAPL --dest <scratch>/filings --since 2023-10-05 --amendments --ua "<the same User-Agent>"`, then
`.venv/Scripts/python scripts/compare_sec_filings_with_research_analyst.py <scratch>/filings/AAPL_filings_manifest.csv <scratch>/store AAPL`
Expected: `[ok] every file of the tool's download is in the store, identical`.

- [ ] **Step 5: Check status and show**

Run: `.venv/Scripts/python -m data_pipeline.store status --root <scratch>/store` and `.venv/Scripts/python -m data_pipeline.store show sec_filings:AAPL --root <scratch>/store`
Expected: `[ok]` for both companies; the citation and the ten newest files of Apple.
