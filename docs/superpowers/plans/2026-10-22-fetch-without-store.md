# Fetching Without a Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `dp.fetch(source, ids, to=...)` and `data-pipeline fetch SOURCE ID... --out FILE` download from any registered source without writing a store, returning one long pandas DataFrame (and a `.parquet` or `.csv` file when asked).

**Architecture:** A new module `data_pipeline/fetching.py` drives a source directly, as `keys_check.py` already does: catalog entries from `catalog.build_entries`, the source from `sources.create`, then `source.fetch([Request(entry), ...])` with no `since` and no `held`. The batches are shaped with the store's own functions (`sync.observation_rows`, `storage.typed`, `storage.table_frame`, `storage.latest`), so a fetched frame equals what `Store.series` / `Store.table` read after a sync of the same answers. A click command in `data_pipeline/fetch_cli.py` joins the `data-pipeline` console script.

**Tech Stack:** Python 3.12+, pandas, pyarrow, httpx, click, PyYAML, pytest (`httpx.MockTransport`, `monkeypatch`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-10-22-fetch-without-store-design.md`

**Verified:** every file below was built in a scratch clone of `feat/fetch` and the tasks were replayed there, ending green: 1033 tests pass (989 before), `ruff check src tests` and mypy clean, and the new files pass `ruff format --check`. No real source was called: every answer is a fake.

**Conventions:** work on branch `feat/fetch` (it holds the spec and this plan). Run tools with `D:/finport-datapipeline/.venv/Scripts/python`; in a worktree set `PYTHONPATH=src` so the worktree's code is imported. Run mypy as `PYTHONIOENCODING=utf-8 python -m mypy`. Console output is ASCII only. Commit messages follow the repository's style (`feat(fetch): ...`) and end with the co-author line. The repository does not run `ruff format` on old files; do not reformat them.

**The rule this plan cannot break:** the repository is public and Sharadar's license forbids sharing its data and publishing assessments of it. Every id and number in a test is invented (`AAA`, `XTEST`, `Test Corp`); the Sharadar parity test reuses the hand-written fixture `tests/unit/store/fixtures/sharadar_prices.json`. Never call a real source, never paste a vendor's answer, never run a sync of the real store (`D:/finport-store/store`).

**Decided here, beyond the spec** (the spec was brought up to date in the same commit as this plan):
- `fetch` also takes `sleep=` (default `time.sleep`), as `Store` does: the HTTP client paces requests with it, and the tests pass `lambda _s: None`.
- `FetchError`'s first line reads `fake: 1 of 2 ids failed; the 1 rows that arrived are in .data`, or `...; nothing arrived`.
- The command prints `[i]   the first 10 of N rows; use --out FILE for all of them` only when it cut rows; with `--out` it prints `[ok]  written to <path>`.
- `--param` cannot name one of `fetch`'s own arguments: `--param start=...` answers `use --start, not --param start=`; `transport`, `sleep` and `credentials` are refused. Each is a usage error (exit 2).
- Exit 3 only when every failure is `quota_exhausted`; any other failure makes it 1. That is `sync`'s rule: there, ids left by a quota are pending, not failed.
- `TableSchema.row_order` (new) holds the sort rule `Store.table` wrote inline, so `Store.table` and `fetch` sort table rows by one definition.
- The projection test uses a period in 2200: pandas cannot hold a day past 2262 in `datetime64[ns]`.

---

## File structure

| File | What it does |
|---|---|
| `src/data_pipeline/store/storage.py` | Modify: `TableSchema.row_order` |
| `src/data_pipeline/store/api.py` | Modify: `Store.table` sorts by `schema.row_order` |
| `src/data_pipeline/fetching.py` | Create: `fetch`, `FetchError`, the file encoders |
| `src/data_pipeline/__init__.py` | Modify: export `fetch` and `FetchError` |
| `src/data_pipeline/fetch_cli.py` | Create: the `fetch` command |
| `src/data_pipeline/cli.py` | Modify: add `fetch_cli.cli` to the `CommandCollection` |
| `tests/unit/store/storage_test.py` | Modify: a test of `row_order` |
| `tests/unit/fetching_test.py` | Create: `fetch` against fake sources, files, and parity with the store |
| `tests/unit/fetch_cli_test.py` | Create: the command |
| `README.md` | Modify: section "Fetching without a store" |

The module is `fetching.py`, not `fetch.py`: `import data_pipeline.fetch` would set the package attribute `fetch` to the module and hide the function.

---

### Task 1: One sort rule for table rows

**Files:**
- Modify: `src/data_pipeline/store/storage.py` (class `TableSchema`, after the `columns` property)
- Modify: `src/data_pipeline/store/api.py` (`Store.table`, the line `order = [...]`)
- Test: `tests/unit/store/storage_test.py`

- [ ] **Step 1: Write the failing test**

In `tests/unit/store/storage_test.py`, add `TableSchema` to the import from `data_pipeline.store.storage` (between `Storage,` and `append_changes,`):

```python
from data_pipeline.store.storage import (
    INDEX_COLUMNS,
    OBS_COLUMNS,
    OBS_DTYPES,
    Storage,
    TableSchema,
    append_changes,
    as_of,
    empty_index,
    empty_observations,
    latest,
    to_moment,
    typed,
)
```

and append at the end of the file:

```python
def test_a_reader_sorts_table_rows_by_the_key_columns_but_period_then_by_date():
    schema = TableSchema(("ticker", "period", "item"), ("value",))
    assert schema.row_order == ["ticker", "item", "date"]
```

- [ ] **Step 2: Run it to see it fail**

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m pytest -q tests/unit/store/storage_test.py -k row`
Expected: FAIL with `AttributeError: 'TableSchema' object has no attribute 'row_order'`

- [ ] **Step 3: Add the property**

In `src/data_pipeline/store/storage.py`, class `TableSchema`, after the `columns` property:

```python
    @property
    def row_order(self) -> list[str]:
        """The columns a reader's rows are sorted by: the key columns but `period`, then `date`."""
        return [*(column for column in self.key_columns if column != "period"), "date"]
```

In `src/data_pipeline/store/api.py`, `Store.table`, replace

```python
        order = [*(column for column in key if column != "period"), "date"]
```

with

```python
        order = schema.row_order
```

- [ ] **Step 4: Run the store's tests**

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m pytest -q tests/unit/store`
Expected: all pass (the new test included; `Store.table` sorts as before).

- [ ] **Step 5: Commit**

```bash
git add src/data_pipeline/store/storage.py src/data_pipeline/store/api.py tests/unit/store/storage_test.py
git commit -m "refactor(store): one sort rule for the rows of a table

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `fetch` in Python

**Files:**
- Create: `tests/unit/fetching_test.py`
- Create: `src/data_pipeline/fetching.py`
- Modify: `src/data_pipeline/__init__.py`

The tests install fake sources in the registry with `monkeypatch.setitem(sources.REGISTRY, "fake", factory)`; `sources.create` reads the registry at call time. `FakeSource` and `monthly` come from `tests/unit/store/helpers.py` (already there). The conftest of `tests/unit` removes every real key from the environment, so a test without `credentials=` has no key.

What the tests cover: the long shape of a series and its order; missing values, projections and older versions left out; an id asked twice asked once; the request (no `since`, `start` read from text); refusals before any request (bad `start`, unknown field, unknown source, documents source, no ids, a file it cannot write); a missing key (the real `fred` source, with a transport that fails the test if called); `FetchError` with partial data, never-returned ids, a quota, a source that breaks, a scrubbed key; tables with `id` first, several batches of one key, the version published last, an id that brings rows and fails, the empty frame of a table source; `.parquet` and `.csv` written equal to the frame, a file replaced, a failed fetch writing what arrived; and parity: the same answers through `Store.sync` and `fetch` read the same (`sharadar_prices`, `sec_xbrl`, `fred`).

- [ ] **Step 1: Write the tests**

Create `tests/unit/fetching_test.py`:

```python
import dataclasses
import datetime
import json
import math

import httpx
import pandas as pd
import pytest

import data_pipeline as dp
from data_pipeline import credentials as keys
from data_pipeline.store import sources
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
    TableData,
)

from .store.helpers import NOW, FakeSource, fixture, monthly

# Every id, name and number here is invented.


def entry(source_id, source="fake"):
    return CatalogEntry(source=source, source_id=source_id)


def install(monkeypatch, source, name="fake"):
    """Put `source` in the registry under `name`; returns the list of the times it was built."""
    built = []

    def factory(client, credentials):
        built.append(credentials)
        return source

    monkeypatch.setitem(sources.REGISTRY, name, factory)
    return built


def refuse(request):
    msg = f"no request was expected, got {request.url}"
    raise AssertionError(msg)


NO_NETWORK = httpx.MockTransport(refuse)


class FakeTables:
    """A table source that answers from a dictionary: source_id -> list of batches."""

    name = "fake"
    kind = Kind.TABLE
    requests_per_minute = 6000
    daily_budget = None
    held_by = ()

    def __init__(self, versioned=False):
        self.versioned = versioned
        self.answers: dict[str, list[FetchBatch]] = {}

    def validate(self, catalog_entry):
        pass

    def table(self, source_id, rows):
        return TableData(
            entry=entry(source_id),
            key=f"fake:{source_id}",
            name=f"Table of {source_id}",
            rows=tuple(rows),
            key_columns=("ticker", "period"),
            value_columns=("close",),
            attribute_columns=("note",),
            versioned=self.versioned,
        )

    def fetch(self, requests):
        for request in requests:
            yield from self.answers.get(request.entry.source_id, [])


def day(ticker, period, close, note="", published_at=None):
    date = datetime.date.fromisoformat(period)
    row = {"ticker": ticker, "period": period, "date": date, "close": close, "note": note}
    if published_at is not None:
        row["published_at"] = published_at
    return row


# -- series ------------------------------------------------------------------------------------


def test_a_series_comes_long_one_row_per_id_and_period_in_the_order_asked(monkeypatch):
    fake = FakeSource()
    fake.answers = {
        "BBB": monthly(entry("BBB"), {"2026-01": 1.0, "2026-02": 2.0}),
        "AAA": monthly(entry("AAA"), {"2026-01": 3.0}),
    }
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["BBB", "AAA"])
    assert list(frame.columns) == ["id", "date", "period", "value"]
    assert frame.to_dict("list") == {
        "id": ["BBB", "BBB", "AAA"],
        "date": [pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28"), pd.Timestamp("2026-01-31")],
        "period": ["2026-01", "2026-02", "2026-01"],
        "value": [1.0, 2.0, 3.0],
    }


def test_a_series_leaves_out_missing_values_and_projections(monkeypatch):
    fake = FakeSource()
    series = monthly(entry("AAA"), {"2026-01": 1.0, "2026-02": math.nan})
    later = Observation("2200-01", datetime.date(2200, 1, 31), 9.0)  # a period still open is a projection
    projected = Observation("2026-03", datetime.date(2026, 3, 31), 8.0, projection=True)
    fake.answers = {"AAA": dataclasses.replace(series, observations=(*series.observations, projected, later))}
    install(monkeypatch, fake)
    assert list(dp.fetch("fake", "AAA")["period"]) == ["2026-01"]


def test_a_series_gives_the_latest_version_of_each_period(monkeypatch):
    fake = FakeSource()
    first = datetime.datetime(2026, 2, 5, tzinfo=datetime.UTC)
    revised = datetime.datetime(2026, 3, 5, tzinfo=datetime.UTC)
    observations = (
        Observation("2026-01", datetime.date(2026, 1, 31), 5.0, published_at=revised),
        Observation("2026-01", datetime.date(2026, 1, 31), 4.0, published_at=first),
    )
    fake.answers = {"AAA": SeriesData(entry("AAA"), "fake:AAA", "A", Frequency.MONTHLY, observations=observations)}
    install(monkeypatch, fake)
    assert list(dp.fetch("fake", ["AAA"])["value"]) == [5.0]


def test_an_id_asked_twice_is_asked_once(monkeypatch):
    fake = FakeSource()
    fake.answers = {"AAA": monthly(entry("AAA"), {"2026-01": 1.0})}
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["AAA", "AAA"])
    assert [request.entry.source_id for request in fake.seen] == ["AAA"]
    assert len(frame) == 1


def test_the_request_carries_no_since_and_the_start_given_as_text():
    seen = []

    class Recording(FakeSource):
        def fetch(self, requests):
            seen.extend(requests)
            return iter(())

    with pytest.MonkeyPatch.context() as monkeypatch:
        install(monkeypatch, Recording())
        with pytest.raises(dp.FetchError):
            dp.fetch("fake", ["AAA"], start="2020-01-01")
    (request,) = seen
    assert request == Request(CatalogEntry("fake", "AAA", start=datetime.date(2020, 1, 1)))


def test_a_start_that_is_not_a_date_is_refused_before_any_request(monkeypatch):
    built = install(monkeypatch, FakeSource())
    with pytest.raises(CatalogError, match="'start' must be a date such as 1990-01-01, not '2020-13-01'"):
        dp.fetch("fake", ["AAA"], start="2020-13-01")
    assert built == []


def test_a_field_the_source_does_not_accept_is_refused_before_any_request(monkeypatch):
    fake = FakeSource()
    install(monkeypatch, fake)
    with pytest.raises(CatalogError, match="unknown field\\(s\\) for source 'fake': colour"):
        dp.fetch("fake", ["AAA"], colour="red")
    assert fake.seen == []


def test_an_unknown_source_is_refused_with_the_list_of_known_ones():
    with pytest.raises(CatalogError, match=r"unknown source 'nope' \(known sources: .*fred"):
        dp.fetch("nope", ["AAA"], transport=NO_NETWORK)


def test_a_source_of_documents_is_refused_before_any_request():
    with pytest.raises(CatalogError, match="sec_filings is a source of documents: fetch returns data frames"):
        dp.fetch("sec_filings", ["AAPL"], transport=NO_NETWORK)


def test_no_ids_is_refused():
    with pytest.raises(CatalogError, match="'ids' must be a non-empty list"):
        dp.fetch("fred", [], transport=NO_NETWORK)


def test_a_missing_key_fails_every_id_without_a_request():
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fred", ["UNRATE", "DGS10"], transport=NO_NETWORK)
    assert [failure.outcome for failure in caught.value.failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert keys.missing_message(keys.FRED) in str(caught.value)
    assert caught.value.data.empty
    assert list(caught.value.data.columns) == ["id", "date", "period", "value"]


# -- failures ----------------------------------------------------------------------------------


def test_a_failed_id_raises_with_what_arrived_and_one_line_per_failure(monkeypatch):
    fake = FakeSource()
    fake.answers = {
        "AAA": monthly(entry("AAA"), {"2026-01": 1.0}),
        "BAD": Failure(entry("BAD"), Outcome.NOT_FOUND, "no such series"),
    }
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BAD"])
    error = caught.value
    assert list(error.data["id"]) == ["AAA"]
    assert [(failure.entry.key, failure.outcome) for failure in error.failures] == [("fake:BAD", Outcome.NOT_FOUND)]
    assert not error.quota_exhausted
    assert str(error).splitlines() == [
        "fake: 1 of 2 ids failed; the 1 rows that arrived are in .data",
        "fake:BAD  not_found: no such series",
    ]


def test_an_id_the_source_never_returned_is_not_found(monkeypatch):
    install(monkeypatch, FakeSource())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA"])
    (failure,) = caught.value.failures
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "the source did not return this id")
    assert str(caught.value).splitlines()[0] == "fake: 1 of 1 ids failed; nothing arrived"


def test_a_quota_fails_the_ids_not_yet_fetched_and_keeps_the_others(monkeypatch):
    fake = FakeSource(quota_after=1)
    fake.answers = {"AAA": monthly(entry("AAA"), {"2026-01": 1.0}), "BBB": monthly(entry("BBB"), {"2026-01": 2.0})}
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BBB"])
    assert caught.value.quota_exhausted
    assert list(caught.value.data["id"]) == ["AAA"]
    (failure,) = caught.value.failures
    assert failure.entry.source_id == "BBB"
    assert failure.outcome is Outcome.QUOTA_EXHAUSTED
    assert failure.reason == "the source's quota ran out before this id was fetched: quota used up"


def test_a_source_that_breaks_fails_the_ids_it_did_not_finish(monkeypatch):
    class Breaking(FakeSource):
        def fetch(self, requests):
            yield FetchBatch(series=(monthly(entry("AAA"), {"2026-01": 1.0}),))
            msg = "the answer made no sense"
            raise RuntimeError(msg)

    install(monkeypatch, Breaking())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BBB"])
    (failure,) = caught.value.failures
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason == "the download stopped: RuntimeError: the answer made no sense"
    assert list(caught.value.data["id"]) == ["AAA"]


def test_a_reason_never_shows_a_key(monkeypatch):
    secret = "abcdef0123456789abcdef0123456789"  # noqa: S105 - an invented key
    fake = FakeSource()
    fake.answers = {"BAD": Failure(entry("BAD"), Outcome.SOURCE_ERROR, f"refused api_key={secret}")}
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["BAD"], credentials={keys.FRED: secret})
    assert secret not in str(caught.value)
    assert secret not in caught.value.failures[0].reason


def test_a_quota_raised_before_anything_arrived_is_a_fetch_error(monkeypatch):
    class Exhausted(FakeSource):
        def fetch(self, requests):
            msg = "429 on every attempt"
            raise QuotaExhaustedError(msg)
            yield  # a generator, as every source is

    install(monkeypatch, Exhausted())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA"])
    assert caught.value.quota_exhausted
    assert caught.value.failures[0].outcome is Outcome.QUOTA_EXHAUSTED


# -- tables ------------------------------------------------------------------------------------


def test_a_table_comes_with_its_id_first_and_the_columns_of_store_table(monkeypatch):
    fake = FakeTables()
    fake.answers = {
        "XBBB": [FetchBatch(tables=(fake.table("XBBB", [day("XBBB", "2026-03-03", 2.0)]),))],
        "XAAA": [
            # one key in two batches, newest day first
            FetchBatch(tables=(fake.table("XAAA", [day("XAAA", "2026-03-04", 1.5, "late")]),)),
            FetchBatch(tables=(fake.table("XAAA", [day("XAAA", "2026-03-03", 1.0)]),)),
        ],
    }
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["XBBB", "XAAA"])
    assert list(frame.columns) == ["id", "ticker", "period", "date", "close", "note"]
    assert list(zip(frame["id"], frame["period"], frame["close"], strict=True)) == [
        ("XBBB", "2026-03-03", 2.0),
        ("XAAA", "2026-03-03", 1.0),
        ("XAAA", "2026-03-04", 1.5),
    ]


def test_a_versioned_table_gives_the_version_published_last(monkeypatch):
    fake = FakeTables(versioned=True)
    first = datetime.datetime(2026, 3, 5, tzinfo=datetime.UTC)
    restated = datetime.datetime(2026, 9, 5, tzinfo=datetime.UTC)
    rows = [day("XAAA", "2026-03-03", 9.0, "restated", restated), day("XAAA", "2026-03-03", 1.0, "", first)]
    fake.answers = {"XAAA": [FetchBatch(tables=(fake.table("XAAA", rows),))]}
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["XAAA"])
    assert list(zip(frame["close"], frame["note"], strict=True)) == [(9.0, "restated")]


def test_a_table_id_can_bring_rows_and_fail(monkeypatch):
    fake = FakeTables()
    fake.answers = {
        "XAAA": [
            FetchBatch(tables=(fake.table("XAAA", [day("XAAA", "2026-03-03", 1.0)]),)),
            FetchBatch(failures=(Failure(entry("XAAA"), Outcome.NETWORK_ERROR, "timed out"),)),
        ]
    }
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["XAAA"])
    assert list(caught.value.data["close"]) == [1.0]
    assert caught.value.failures[0].outcome is Outcome.NETWORK_ERROR


def test_a_table_source_that_brought_nothing_gives_an_id_column_only(monkeypatch):
    install(monkeypatch, FakeTables())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["XAAA"])
    assert list(caught.value.data.columns) == ["id"]
    assert caught.value.data.empty


# -- files -------------------------------------------------------------------------------------


@pytest.fixture
def two_series(monkeypatch):
    fake = FakeSource()
    fake.answers = {
        "AAA": monthly(entry("AAA"), {"2026-01": 1.0, "2026-02": 2.0}),
        "BBB": monthly(entry("BBB"), {"2026-01": 3.0}),
    }
    install(monkeypatch, fake)
    return fake


def test_to_parquet_writes_the_frame_it_returns(two_series, tmp_path):
    path = tmp_path / "out.parquet"
    frame = dp.fetch("fake", ["AAA", "BBB"], to=path)
    pd.testing.assert_frame_equal(pd.read_parquet(path), frame)


def test_to_csv_writes_the_frame_it_returns(two_series, tmp_path):
    path = tmp_path / "out.csv"
    frame = dp.fetch("fake", ["AAA", "BBB"], to=str(path))
    assert path.read_text(encoding="utf-8").splitlines()[:2] == ["id,date,period,value", "AAA,2026-01-31,2026-01,1.0"]
    pd.testing.assert_frame_equal(pd.read_csv(path, parse_dates=["date"]), frame)


def test_a_file_of_that_name_is_replaced(two_series, tmp_path):
    path = tmp_path / "out.csv"
    path.write_text("old content\n", encoding="utf-8")
    dp.fetch("fake", ["AAA"], to=path)
    assert path.read_text(encoding="utf-8").startswith("id,date,period,value\n")
    assert [item.name for item in tmp_path.iterdir()] == ["out.csv"]  # no temporary file left


def test_a_failed_fetch_writes_what_arrived_then_raises(two_series, tmp_path):
    path = tmp_path / "out.parquet"
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "MISSING"], to=path)
    pd.testing.assert_frame_equal(pd.read_parquet(path), caught.value.data)


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("out.xlsx", "cannot write 'out.xlsx': the file must end in .parquet or .csv"),
        ("nowhere/out.csv", "the folder .*nowhere.* does not exist"),
    ],
)
def test_a_file_that_cannot_be_written_is_refused_before_any_request(monkeypatch, tmp_path, name, message):
    built = install(monkeypatch, FakeSource())
    with pytest.raises(CatalogError, match=message):
        dp.fetch("fake", ["AAA"], to=tmp_path / name)
    assert built == []


# -- the same as the store ---------------------------------------------------------------------

SHARADAR_KEY = "sharadar-key-of-the-tests"
AGENT = "Jane Doe jane@example.com"


def sharadar(request):
    rows = json.loads(fixture("sharadar_prices.json"))["data"] if request.url.params["ticker"] == "XTEST" else []
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def sec(request):
    if request.url.host == "www.sec.gov":
        return httpx.Response(200, json={"0": {"cik_str": 999, "ticker": "XTEST", "title": "Test Corp"}})

    def appearance(value, filed, accession):
        return {
            "start": "2025-01-01",
            "end": "2025-12-31",
            "val": value,
            "accn": accession,
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "filed": filed,
        }

    revenues = [
        appearance(100, "2026-02-01", "0000000999-26-000001"),
        appearance(90, "2026-08-01", "0000000999-26-000002"),  # restated by a later filing
    ]
    facts = {"us-gaap": {"Revenues": {"label": "Revenues", "units": {"USD": revenues}}}}
    return httpx.Response(200, json={"cik": 999, "entityName": "Test Corp", "facts": facts})


def fred(request):
    name = "fred_observations.json" if request.url.path.endswith("/observations") else "fred_series.json"
    return httpx.Response(200, text=fixture(name))


def stored(tmp_path, source, source_id, handler, credentials):
    store = Store(
        tmp_path / "store",
        credentials=credentials,
        clock=lambda: NOW,
        transport=httpx.MockTransport(handler),
        sleep=lambda _seconds: None,
    )
    store.add(source, [source_id])
    assert store.sync().exit_code == 0
    return store


@pytest.mark.parametrize(
    ("source", "handler", "credentials"),
    [("sharadar_prices", sharadar, {keys.SHARADAR: SHARADAR_KEY}), ("sec_xbrl", sec, {keys.SEC_UA: AGENT})],
)
def test_a_table_reads_as_store_table_reads_it(tmp_path, source, handler, credentials):
    store = stored(tmp_path, source, "XTEST", handler, credentials)
    fetched = dp.fetch(
        source, ["XTEST"], credentials=credentials, transport=httpx.MockTransport(handler), sleep=lambda _s: None
    )
    assert list(fetched["id"].unique()) == ["XTEST"]
    pd.testing.assert_frame_equal(fetched.drop(columns="id"), store.table(source, "XTEST"))


def test_the_versioned_table_keeps_only_the_restated_value(tmp_path):
    fetched = dp.fetch(
        "sec_xbrl", "XTEST", credentials={keys.SEC_UA: AGENT}, transport=httpx.MockTransport(sec), sleep=lambda _s: None
    )
    assert list(fetched["value"]) == [90.0]


def test_a_series_reads_as_store_series_reads_it(tmp_path):
    credentials = {keys.FRED: "0123456789abcdef0123456789abcdef"}
    store = stored(tmp_path, "fred", "UNRATE", fred, credentials)
    fetched = dp.fetch(
        "fred", ["UNRATE"], credentials=credentials, transport=httpx.MockTransport(fred), sleep=lambda _s: None
    )
    pd.testing.assert_frame_equal(fetched[["date", "period", "value"]], store.series("fred:UNRATE"))
    assert list(fetched["id"].unique()) == ["UNRATE"]
```

- [ ] **Step 2: Run them to see them fail**

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m pytest -q tests/unit/fetching_test.py`
Expected: every test FAILS with `AttributeError: module 'data_pipeline' has no attribute 'fetch'` (the attribute is read inside each test, so collection succeeds).

- [ ] **Step 3: Write the module**

Create `src/data_pipeline/fetching.py`:

```python
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
```

Replace `src/data_pipeline/__init__.py` with:

```python
"""finport-datapipeline: a local, versioned store of public economic and company data."""

from data_pipeline.fetching import FetchError, fetch
from data_pipeline.store.api import Store

__version__ = "0.1.0"
__all__ = ["FetchError", "Store", "__version__", "fetch"]
```

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m pytest -q tests/unit/fetching_test.py`
Expected: `31 passed`.

If a parity test (`test_a_table_reads_as_store_table_reads_it`, `test_a_series_reads_as_store_series_reads_it`) fails on a dtype or an order, fix `fetching.py` so it shapes the frame as the store does. Never loosen the test: equality with the store is the point of the design.

- [ ] **Step 5: Lint and types**

Run: `D:/finport-datapipeline/.venv/Scripts/python -m ruff check src tests` -> `All checks passed!`
Run: `PYTHONIOENCODING=utf-8 D:/finport-datapipeline/.venv/Scripts/python -m mypy` -> `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/fetching.py src/data_pipeline/__init__.py tests/unit/fetching_test.py
git commit -m "feat(fetch): download from any source without a store

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The `fetch` command

**Files:**
- Create: `tests/unit/fetch_cli_test.py`
- Create: `src/data_pipeline/fetch_cli.py`
- Modify: `src/data_pipeline/cli.py`

The tests invoke the whole console script (`data_pipeline.cli.cli`), so they also prove the command is registered. Fake sources are installed as in Task 2.

- [ ] **Step 1: Write the tests**

Create `tests/unit/fetch_cli_test.py`:

```python
import datetime

import click.testing
import pandas as pd
import pytest

from data_pipeline.cli import cli
from data_pipeline.store import sources
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Kind, Outcome, TableData

from .store.helpers import FakeSource, monthly

# Every id, name and number here is invented.
YEAR = {f"2025-{month:02d}": float(month) for month in range(1, 13)}


def install(monkeypatch, source):
    monkeypatch.setitem(sources.REGISTRY, "fake", lambda _client, _credentials: source)
    return source


def run(*arguments):
    return click.testing.CliRunner().invoke(cli, ["fetch", *arguments])


def answered(**answers):
    fake = FakeSource()
    fake.answers = {name: monthly(CatalogEntry("fake", name), values) for name, values in answers.items()}
    return fake


def test_without_out_it_prints_a_summary_and_the_first_rows(monkeypatch):
    install(monkeypatch, answered(AAA=YEAR))
    result = run("fake", "AAA")
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "fake: 1 ids, 12 rows"
    assert lines[1] == "id  date  period  value"
    assert lines[2] == "AAA  2025-01-31  2025-01  1.0"
    assert len(lines) == 2 + 10 + 1
    assert lines[-1] == "[i]   the first 10 of 12 rows; use --out FILE for all of them"


def test_with_out_it_writes_every_row_and_prints_where(monkeypatch, tmp_path):
    install(monkeypatch, answered(AAA=YEAR, BBB={"2025-01": 5.0}))
    path = tmp_path / "out.csv"
    result = run("fake", "AAA", "BBB", "--out", str(path))
    assert result.exit_code == 0
    assert result.output.splitlines() == ["fake: 2 ids, 13 rows", f"[ok]  written to {path}"]
    assert len(pd.read_csv(path)) == 13


def test_a_param_is_read_as_yaml_and_start_as_a_date(monkeypatch):
    class Open(FakeSource):
        def validate(self, entry):
            pass

    fake = install(monkeypatch, Open())
    fake.answers = {"AAA": monthly(CatalogEntry("fake", "AAA"), {"2025-01": 1.0})}
    params = ["--param", "annual_from=2020", "--param", "partners=[USA, CAN]"]
    result = run("fake", "AAA", *params, "--start", "2020-01-01")
    assert result.exit_code == 0, result.output
    (request,) = fake.seen
    assert dict(request.entry.params) == {"annual_from": 2020, "partners": ["USA", "CAN"]}
    assert request.entry.start == datetime.date(2020, 1, 1)


@pytest.mark.parametrize(
    ("param", "message"),
    [
        ("annual_from", "'annual_from' is not NAME=VALUE"),
        ("start=2020-01-01", "use --start, not --param start="),
        ("transport=x", "'transport' cannot be given as --param"),
        ("partners=[USA", "the value of 'partners' is not valid YAML"),
    ],
)
def test_a_param_it_cannot_read_is_a_usage_error(monkeypatch, param, message):
    install(monkeypatch, answered(AAA=YEAR))
    result = run("fake", "AAA", "--param", param)
    assert result.exit_code == 2
    assert message in result.output


def test_a_failed_id_is_listed_and_exits_with_1(monkeypatch):
    fake = install(monkeypatch, answered(AAA={"2025-01": 1.0}))
    fake.answers["BAD"] = Failure(CatalogEntry("fake", "BAD"), Outcome.NOT_FOUND, "no such series")
    result = run("fake", "AAA", "BAD")
    assert result.exit_code == 1
    lines = result.output.splitlines()
    assert lines[0] == "fake: 1 ids, 1 rows"
    assert lines[-1] == "[x]   fake:BAD  not_found: no such series"


def test_a_quota_alone_exits_with_3(monkeypatch):
    fake = install(monkeypatch, answered(AAA={"2025-01": 1.0}, BBB={"2025-01": 2.0}))
    fake.quota_after = 1
    result = run("fake", "AAA", "BBB")
    assert result.exit_code == 3
    assert "[x]   fake:BBB  quota_exhausted: " in result.output


def test_a_quota_with_another_failure_exits_with_1(monkeypatch):
    fake = install(monkeypatch, answered(BBB={"2025-01": 2.0}))
    fake.answers["BAD"] = Failure(CatalogEntry("fake", "BAD"), Outcome.NOT_FOUND, "no such series")
    fake.quota_after = 1
    assert run("fake", "BAD", "BBB").exit_code == 1


def test_a_configuration_error_exits_with_2():
    result = run("nope", "AAA")
    assert result.exit_code == 2
    assert result.output.startswith("[x]   unknown source 'nope'")


def test_what_it_prints_is_ascii(monkeypatch):
    class Tables:
        name = "fake"
        kind = Kind.TABLE
        requests_per_minute = 6000
        daily_budget = None
        held_by = ()

        def validate(self, entry):
            pass

        def fetch(self, requests):
            row = {"ticker": "XAAA", "period": "2026-03-03", "date": datetime.date(2026, 3, 3), "note": "café"}
            table = TableData(
                entry=CatalogEntry("fake", "XAAA"),
                key="fake:XAAA",
                name="T",
                rows=(row,),
                key_columns=("ticker", "period"),
                value_columns=(),
                attribute_columns=("note",),
            )
            yield FetchBatch(tables=(table,))

    install(monkeypatch, Tables())
    result = run("fake", "XAAA")
    assert result.exit_code == 0
    assert result.output.isascii()
    assert "XAAA  XAAA  2026-03-03  2026-03-03  caf?" in result.output
```

- [ ] **Step 2: Run them to see them fail**

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m pytest -q tests/unit/fetch_cli_test.py`
Expected: FAIL; every test exits with 2 and `No such command 'fetch'`.

- [ ] **Step 3: Write the command**

Create `src/data_pipeline/fetch_cli.py`:

```python
"""The `fetch` command: download from a source without a store. Output is ASCII only.

Exit codes, those of sync: 0 every id arrived, 1 some id failed, 2 configuration error, 3 the
source's quota ran out (and nothing else failed).
"""

import pathlib
from typing import Any

import click
import pandas as pd
import yaml

from data_pipeline.fetching import FetchError, fetch
from data_pipeline.store.cli import PATH, SHOWN_ROWS, echo, fail
from data_pipeline.store.errors import StoreError
from data_pipeline.store.model import Outcome
from data_pipeline.store.sync import EXIT_FAILURES, EXIT_OK, EXIT_QUOTA

# fetch's own arguments, which a --param cannot name: those the command has an option for, and the rest
OWN_OPTIONS = {"to": "--out", "start": "--start", "frequency": "--frequency", "env_file": "--env-file"}
RESERVED = frozenset({*OWN_OPTIONS, "credentials", "transport", "sleep"})


def read_params(pairs: tuple[str, ...]) -> dict[str, Any]:
    """NAME=VALUE pairs as the source's fields, each value read as YAML, as in a catalog."""
    params: dict[str, Any] = {}
    for pair in pairs:
        name, equals, value = pair.partition("=")
        name = name.strip()
        if not equals or not name:
            msg = f"{pair!r} is not NAME=VALUE"
            raise click.BadParameter(msg, param_hint="--param")
        if name in RESERVED:
            own = OWN_OPTIONS.get(name)
            msg = f"use {own}, not --param {name}=" if own else f"{name!r} cannot be given as --param"
            raise click.BadParameter(msg, param_hint="--param")
        try:
            params[name] = yaml.safe_load(value)
        except yaml.YAMLError as exc:
            msg = f"the value of {name!r} is not valid YAML: {value!r}"
            raise click.BadParameter(msg, param_hint="--param") from exc
    return params


def _cell(value: object) -> str:
    return value.date().isoformat() if isinstance(value, pd.Timestamp) else str(value)


def show(source: str, data: pd.DataFrame, out: pathlib.Path | None) -> None:
    """The summary line, then the path written or the first rows."""
    ids = data["id"].nunique() if "id" in data else 0
    echo(f"{source}: {ids} ids, {len(data)} rows")
    if out is not None:
        echo(f"[ok]  written to {out}")
        return
    if data.empty:
        return
    echo("  ".join(data.columns))
    for values in data.head(SHOWN_ROWS).itertuples(index=False):
        echo("  ".join(_cell(value) for value in values))
    if len(data) > SHOWN_ROWS:
        echo(f"[i]   the first {SHOWN_ROWS} of {len(data)} rows; use --out FILE for all of them")


@click.group()
def cli() -> None:
    """Downloading without a store."""


@cli.command("fetch")
@click.argument("source")
@click.argument("ids", nargs=-1, required=True)
@click.option("--out", type=PATH, default=None, help="Write every row to this .parquet or .csv file, replacing it.")
@click.option("--start", default=None, help="Ask from this date on (YYYY-MM-DD), where the source takes one.")
@click.option("--frequency", default=None, help="D, W, M, Q or A, for a source that does not report one.")
@click.option("--param", "params", multiple=True, help="A field of the source, NAME=VALUE, read as YAML. Repeatable.")
@click.option(
    "--env-file",
    type=PATH,
    default=None,
    help="File with the credentials. Defaults to the nearest .env (this folder or a parent).",
)
def fetch_command(
    *,
    source: str,
    ids: tuple[str, ...],
    out: pathlib.Path | None,
    start: str | None,
    frequency: str | None,
    params: tuple[str, ...],
    env_file: pathlib.Path | None,
) -> None:
    """Download IDS of SOURCE without a store: the first rows, or every row to --out.

    A series comes as id, date, period, value; a table as id and the columns of its source.
    Nothing is stored and there are no versions: every run downloads again and spends the
    source's quota again. For data read often, keep a store with sync.
    """
    fields = read_params(params)
    try:
        data = fetch(source, list(ids), to=out, start=start, frequency=frequency, env_file=env_file, **fields)
    except FetchError as exc:
        show(source, exc.data, out)
        for failure in exc.failures:
            echo(f"[x]   {failure.entry.key}  {failure.outcome.value}: {failure.reason}")
        only_quota = all(failure.outcome is Outcome.QUOTA_EXHAUSTED for failure in exc.failures)
        raise SystemExit(EXIT_QUOTA if exc.quota_exhausted and only_quota else EXIT_FAILURES) from exc
    except StoreError as exc:
        raise fail(exc) from exc
    show(source, data, out)
    raise SystemExit(EXIT_OK)
```

Replace `src/data_pipeline/cli.py` with:

```python
"""The `data-pipeline` console script: the store's commands, the key commands and fetch under one name."""

import click

from data_pipeline import __version__, fetch_cli, keys_cli
from data_pipeline.store.cli import cli as store_cli

cli = click.version_option(__version__, prog_name="data-pipeline")(
    click.CommandCollection(
        sources=[store_cli, keys_cli.cli, fetch_cli.cli],
        help="Public economic and company data, kept locally with every revision.",
    )
)
```

- [ ] **Step 4: Run the tests and look at the help**

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m pytest -q tests/unit/fetch_cli_test.py`
Expected: `12 passed`.

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m data_pipeline fetch --help`
Expected: the usage line `Usage: python -m data_pipeline fetch [OPTIONS] SOURCE IDS...`, the docstring, and the options `--out`, `--start`, `--frequency`, `--param`, `--env-file`. No request is made.

- [ ] **Step 5: Lint and types**

Run: `D:/finport-datapipeline/.venv/Scripts/python -m ruff check src tests` -> `All checks passed!`
Run: `PYTHONIOENCODING=utf-8 D:/finport-datapipeline/.venv/Scripts/python -m mypy` -> `Success: no issues found`

`read_params` returns `dict[str, Any]` on purpose: YAML values are of any type, and a `dict[str, object]` unpacked into `fetch(**fields)` fails mypy against `fetch`'s typed keywords.

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/fetch_cli.py src/data_pipeline/cli.py tests/unit/fetch_cli_test.py
git commit -m "feat(fetch): the fetch command

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: README

**Files:**
- Modify: `README.md` (a new section right before `## The macro catalog`)

- [ ] **Step 1: Add the section**

Insert before the line `## The macro catalog`:

````markdown
## Fetching without a store

`fetch` downloads from any source and gives you the data at once, without writing a store:

```python
import data_pipeline as dp

dp.fetch("fred", ["UNRATE", "DGS10"])                        # a pandas DataFrame
dp.fetch("sharadar_prices", "AAPL", to="aapl.parquet")       # also written to a file
dp.fetch("sharadar_prices", ["SPY"], table="funds", start="2020-01-01")  # the catalog's fields
```

```bash
data-pipeline fetch fred UNRATE DGS10                        # a summary and the first 10 rows
data-pipeline fetch sharadar_prices AAPL MSFT --out prices.parquet
data-pipeline fetch comtrade MEX --param annual_from=2020 --param "partners=[USA, CAN]"
```

What comes back is one frame for all the ids, long rather than wide:

| Source | Columns |
|---|---|
| A series source (FRED, BLS, Banxico, INEGI, World Bank, SDMX, DBnomics) | `id`, `date`, `period`, `value` |
| A table source (Sharadar, Comtrade, SEC XBRL) | `id`, then the columns `Store.table` gives for that source |

A series gives the latest version of each period, without periods that have no value and
without projections, as `store.series` reads it. A table gives the latest version of each row, as
`store.table` reads it. `id` is the id you asked for (`UNRATE`, not `fred:UNRATE`). Documents
(`sec_filings`) are not fetched: keep them in a store.

`to=` (`--out`) writes the same frame to a `.parquet` or `.csv` file, replacing a file of that
name. The fields of a catalog entry (`start`, `frequency` and each source's own, such as
`table` or `partners`) are keyword arguments, or `--start`, `--frequency` and `--param NAME=VALUE`
(the value read as YAML, as in a catalog). Keys come from where `sync` takes them.

When an id fails, `fetch` raises `dp.FetchError` once everything was asked: `error.data` holds
what arrived, `error.failures` one failure per id, `error.quota_exhausted` whether the quota
stopped it. With `to=`, what arrived is written first. The command prints one `[x]` line per
failed id and exits with 1 (3 when only the quota stopped it, 2 for a configuration error such
as an unknown source or a field the source does not take).

What `fetch` does not do: it keeps no versions and has no `as_of`, and every call downloads the
whole history (or from `start`) and spends the source's quota again, Sharadar's included. For
data you read more than once, keep a store and `sync` it.
````

- [ ] **Step 2: Check the examples against the code, without the network**

Each example must be accepted by the source it names. Run (no key is read and no request is made):

```bash
PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -c "import datetime; from data_pipeline.store.catalog import build_entries; from data_pipeline.store.sources.comtrade import settings; from data_pipeline.store.sources.sharadar import check_ticker; from data_pipeline.store.sources.sharadar_prices import table_of; (c,) = build_entries('comtrade', ['MEX'], where='check', params={'annual_from': 2020, 'partners': ['USA', 'CAN']}); print(settings(c)); (s,) = build_entries('sharadar_prices', ['SPY'], where='check', start=datetime.date(2020, 1, 1), params={'table': 'funds'}); check_ticker(s); print(table_of(s))"
```

Expected:

```
Settings(level='AG2', partners=('USA', 'CAN'), flows=('X', 'M'), annual_from=2020, months=75)
funds
```

Do not try the examples with `data-pipeline fetch` itself: with a key in the environment or in a `.env`, that is a real download.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(fetch): fetching without a store in the README

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Final check

- [ ] **Step 1: The whole suite**

Run: `PYTHONPATH=src D:/finport-datapipeline/.venv/Scripts/python -m pytest -q`
Expected: `1033 passed, 14 skipped, 18 deselected` (the counts of the scratch run on Windows; the skips are POSIX-only tests).

- [ ] **Step 2: Lint, types, and the format of the new files**

Run: `D:/finport-datapipeline/.venv/Scripts/python -m ruff check src tests` -> `All checks passed!`
Run: `PYTHONIOENCODING=utf-8 D:/finport-datapipeline/.venv/Scripts/python -m mypy` -> `Success: no issues found in 38 source files`
Run: `D:/finport-datapipeline/.venv/Scripts/python -m ruff format --check src/data_pipeline/fetching.py src/data_pipeline/fetch_cli.py src/data_pipeline/cli.py src/data_pipeline/__init__.py tests/unit/fetching_test.py tests/unit/fetch_cli_test.py` -> `6 files already formatted`

- [ ] **Step 3: Against the spec**

Read `docs/superpowers/specs/2026-10-22-fetch-without-store-design.md` once more and tick each locked decision against the code. Then stop: do not push and do not open a pull request without asking the owner.
