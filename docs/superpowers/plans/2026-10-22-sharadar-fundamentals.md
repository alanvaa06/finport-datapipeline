# Sharadar Fundamentals, Tickers and Daily Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Read this first:** the core stores these tables correctly, but it is noisy with the widest one. `storage.table_frame` builds a stored frame one column at a time; with the 105 indicators of a fundamentals table, pandas emits a `PerformanceWarning` ("DataFrame is highly fragmented") at two lines of `table_frame`. A check with two invented tickers printed it twice per ticker on stderr, so a catalog of 880 tickers would print it about 1,760 times per sync (assuming it holds per ticker). What is stored and read is right, and the tests see it only as 22 more warnings. The spec says the core does not change, so this plan leaves `storage.py` alone. One line in `table_frame`, `frame = frame.copy()` after the loop over the columns and before the two stamps, silences it: that was checked in the scratch clone and is not part of this plan. The owner decides whether it goes in first, as a change of its own.

**Goal:** Store, for the tickers listed in the catalog, three more tables of Sharadar's: the fundamentals in its six dimensions, its record of each ticker, and its daily valuation metrics, readable at their latest value and as of a past day where Sharadar says when a value was known. No change to the core.

**Architecture:** Three table sources, `sharadar_fundamentals`, `sharadar_tickers` and `sharadar_daily`, built on `sharadar.py` as the prices and actions sources are: one call per ticker and table, the whole history on every run, refusals through `per_ticker`. Fundamentals and ticker records are versioned tables, as `sec_xbrl` is: each row carries its own `published_at`, the end of the UTC day of its `datekey` (as-reported rows) or of its `lastupdated` (most-recent rows, ticker records), and the store's `append_versions` keeps a version whenever that moment or a value is new. Daily metrics are a plain table like prices, merged by `append_rows`. Task 1 pins what the spec assumes of the versioned core before any source is written.

**Tech Stack:** Python 3.12+, pandas, pyarrow, httpx, click, pytest (`httpx.MockTransport`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-10-22-sharadar-fundamentals-design.md`

**Verified:** every file below was built in a scratch clone, and the tasks were replayed there from `docs/sharadar-fundamentals-design` (b6027d5) in this order, each one ending with the whole suite, ruff and mypy green; the tree after Task 7 is the one that was verified. 1070 tests pass (989 before), ruff and mypy clean (39 source files). The live test of Task 6 was run only against a fake Sharadar with invented rows, outside the repository, to check its mechanics. Not verified: anything that needs the paid key (Task 9).

**Conventions:** work on branch `feat/sharadar-fundamentals`, created from `docs/sharadar-fundamentals-design` (which holds the spec and this plan). Run tools with `.venv/Scripts/python` from the root of the checkout; a worktree has no `.venv`, so there use `D:/finport-datapipeline/.venv/Scripts/python` from the worktree's root (pytest and mypy read the worktree's own `src`). Run mypy as `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`. Console output is ASCII only. On this machine a Bash heredoc that holds Python triple quotes together with apostrophes fails: write files with the editor, not with a heredoc. Commit messages follow the repository's style (`feat(sources): ...`) and end with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `pytest -m live` before Task 9: the checkout's `.env` may hold the paid key, and the live test would call Sharadar with it.

**The rule this plan cannot break:** the repository is public and Sharadar's license forbids sharing its data and publishing assessments of it. Every fixture and every number in a test is invented (`XTEST`, `TEST CORP`, `Testland`). Never paste an answer of Sharadar's, or a judgment of its data, into a file, a test, a comment, a commit message or the README. The live test keeps the rows it reads in memory and checks their shape only. What any probe finds stays outside the repository.

**Decided here, beyond the spec:**
- Task 1 adds two tests to `tests/unit/store/versions_test.py` that pin what the spec assumes of the core: the same values published later are another version and keep their own attributes, `as_of` reads the attributes of the version before, and a missing value stored and received again is the same version.
- Fundamentals: a `dimension` other than the six fails the ticker (`SOURCE_ERROR`), since its rows cannot be dated.
- Fundamentals: a dimension and `calendardate` that come twice in one answer fail the ticker only when both are known from the same moment, which is the spec's "no versioning to tell them apart". An as-reported period that comes with two different `datekey`s is kept as two versions. Whether Sharadar ever sends that is not known (see Task 9).
- Fundamentals: `datekey` is required only of an as-reported row and `lastupdated` only of a most-recent one, since each dates its row; `lastupdated` is not stored (the spec lists three attributes). `datekey` is stored as sent, as text.
- Ticker records: a row whose `table` is missing, empty, `N/A` or `null` fails the ticker, as does a row without `lastupdated`. A `permaticker` sent as `null` is stored as missing. An attribute that comes as a number (`siccode`) is stored as its text.
- Daily metrics: a row that lacks one of the seven metrics fails the ticker, as a price row does; a metric sent as `null` is stored as missing.
- The reasons of `NOT_FOUND` add ` from <start> on` when the entry has a `start`, as prices do. The tickers reason is "Sharadar has no ticker record for 'X'".
- `sharadar.py` does not change: each versioned source writes the end of the day inline, `datetime.datetime.combine(day, datetime.time.max, datetime.UTC)`, as `sec_xbrl` does.
- The live test wraps the real transport to keep the rows in memory, and checks two shapes the spec does not list: every column a source reads is in every row, and a fundamentals row has no column the source does not know. It also checks the whole history of the daily metrics, which the spec's "Not verified" names.
- The README tells the cost in calls (five per ticker listed in all five Sharadar sources) and the lever, `sync --source`.
- `credentials.py` keeps the key's label "US equities - prices & actions": the spec says nothing changes there. The README's table of keys does name fundamentals.

---

## File structure

| File | Responsibility |
|---|---|
| `tests/unit/store/versions_test.py` | two tests of the versioned core, as the spec assumes it |
| `src/data_pipeline/store/sources/sharadar_fundamentals.py` | `SharadarFundamentals`, the 105 indicators, and the dating of each row by its dimension |
| `src/data_pipeline/store/sources/sharadar_tickers.py` | `SharadarTickers` and the reading of ticker records |
| `src/data_pipeline/store/sources/sharadar_daily.py` | `SharadarDaily` and the reading of daily metrics |
| `src/data_pipeline/store/sources/__init__.py` | the three names in `REGISTRY` and `TITLES` |
| `tests/unit/store/fixtures/sharadar_fundamentals.json`, `sharadar_tickers.json`, `sharadar_daily.json` | hand-written answers, invented |
| `tests/unit/store/sharadar_fundamentals_test.py`, `sharadar_tickers_test.py`, `sharadar_daily_test.py` | unit tests, through the store too |
| `tests/live/sharadar_live_test.py` | three more real calls, off by default, shapes only |
| `README.md`, `CHANGELOG.md` | documentation |

`sync.py`, `storage.py`, `api.py`, `sharadar.py`, `credentials.py` and `keys_check.py` are not touched.

---

### Task 1: The core does what the spec assumes

No production code changes here. These tests pin what the store already does with a versioned table that has attribute columns. If one fails, stop and report it: the spec assumed the core needs no change.

**Files:**
- Modify: `tests/unit/store/versions_test.py`

- [ ] **Step 1: Create the branch**

```bash
git checkout docs/sharadar-fundamentals-design
git checkout -b feat/sharadar-fundamentals
```

- [ ] **Step 2: Add the import**

In `tests/unit/store/versions_test.py`, after the line `import json`, add:

```python
import math
```

- [ ] **Step 3: Append the tests**

At the end of `tests/unit/store/versions_test.py`, add:

```python
# -- what the Sharadar fundamentals and tickers ask of a versioned table ------------------------


def test_the_same_values_published_later_are_another_version_and_keep_their_own_attributes():
    old, _added, _revised = merge(pd.DataFrame(), [version(1.0, "2026-02-06", form="Q1")])
    merged, added, revised = merge(old, [version(1.0, "2026-05-02", form="Q1R")], LATER)
    assert (len(merged), added, revised) == (2, 0, 1)
    assert list(latest(merged, KEY, ORDER, by_publication=True)["form"]) == ["Q1R"]
    assert list(as_of(merged, pd.Timestamp("2026-05-01", tz="UTC"), KEY, ORDER)["form"]) == ["Q1"]


def test_a_missing_value_stored_and_received_again_is_the_same_version(tmp_path):
    storage = Storage(tmp_path)
    storage.prepare()
    first, _added, _revised = merge(pd.DataFrame(), [version(math.nan, "2026-02-06")])
    storage.write_table("sharadar_fundamentals", "XTEST", first, SCHEMA)
    read_back = storage.read_table("sharadar_fundamentals", "XTEST")
    merged, added, revised = merge(read_back, [version(math.nan, "2026-02-06")], LATER)
    assert (len(merged), added, revised) == (1, 0, 0)
```

- [ ] **Step 4: Run them**

Run: `.venv/Scripts/python -m pytest tests/unit/store/versions_test.py -q`
Expected: `56 passed`

- [ ] **Step 5: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `991 passed, 14 skipped, 18 deselected, 22 warnings` (skips and warnings as before)

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 36 source files`

- [ ] **Step 6: Commit**

```bash
git add tests/unit/store/versions_test.py
git commit -m "test(store): what the Sharadar fundamentals and tickers ask of a versioned table" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The fundamentals source

**Files:**
- Create: `tests/unit/store/fixtures/sharadar_fundamentals.json`
- Create: `tests/unit/store/sharadar_fundamentals_test.py`
- Create: `src/data_pipeline/store/sources/sharadar_fundamentals.py`
- Modify: `src/data_pipeline/store/sources/__init__.py`

- [ ] **Step 1: Write the fixture**

Create `tests/unit/store/fixtures/sharadar_fundamentals.json`. It is written by hand in the shape of an answer of the `fundamentals` table: one as-reported and one most-recent row of an invented ticker for one quarter, with the 105 indicators in Sharadar's order. The numbers are invented; three large ones come as text, as Sharadar sends large values, and one is `null`.

```json
{
  "count": 2,
  "data": [
    {
      "ticker": "XTEST",
      "dimension": "ARQ",
      "calendardate": "2025-12-31",
      "date": "2026-02-05",
      "reportperiod": "2025-12-31",
      "fiscalperiod": "2025-Q4",
      "lastupdated": "2026-02-06",
      "accoci": 1.5,
      "assets": "4500000000000",
      "assetsavg": 3.5,
      "assetsc": 4.5,
      "assetsnc": 5.5,
      "assetturnover": 6.5,
      "bvps": 7.5,
      "capex": 8.5,
      "cashneq": 9.5,
      "cashnequsd": 10.5,
      "cor": 11.5,
      "consolinc": 12.5,
      "currentratio": 13.5,
      "de": 14.5,
      "debt": 15.5,
      "debtc": 16.5,
      "debtnc": 17.5,
      "debtusd": 18.5,
      "deferredrev": 19.5,
      "depamor": 20.5,
      "deposits": 21.5,
      "divyield": 22.5,
      "dps": 23.5,
      "ebit": 24.5,
      "ebitda": 25.5,
      "ebitdamargin": 26.5,
      "ebitdausd": 27.5,
      "ebitusd": 28.5,
      "ebt": 29.5,
      "eps": 30.5,
      "epsdil": 31.5,
      "epsusd": 32.5,
      "equity": 33.5,
      "equityavg": 34.5,
      "equityusd": 35.5,
      "ev": "31000000000000",
      "evebit": 37.5,
      "evebitda": 38.5,
      "fcf": 39.5,
      "fcfps": 40.5,
      "fxusd": 41.5,
      "gp": 42.5,
      "grossmargin": 43.5,
      "intangibles": 44.5,
      "intexp": 45.5,
      "invcap": 46.5,
      "invcapavg": 47.5,
      "inventory": 48.5,
      "investments": 49.5,
      "investmentsc": 50.5,
      "investmentsnc": 51.5,
      "liabilities": 52.5,
      "liabilitiesc": 53.5,
      "liabilitiesnc": 54.5,
      "marketcap": "30000000000000",
      "ncf": 56.5,
      "ncfbus": 57.5,
      "ncfcommon": 58.5,
      "ncfdebt": 59.5,
      "ncfdiv": 60.5,
      "ncff": 61.5,
      "ncfi": 62.5,
      "ncfinv": 63.5,
      "ncfo": 64.5,
      "ncfx": 65.5,
      "netinc": 66.5,
      "netinccmn": 67.5,
      "netinccmnusd": 68.5,
      "netincdis": 69.5,
      "netincnci": 70.5,
      "netmargin": 71.5,
      "opex": 72.5,
      "opinc": 73.5,
      "payables": 74.5,
      "payoutratio": 75.5,
      "pb": 76.5,
      "pe": 77.5,
      "pe1": 78.5,
      "ppnenet": 79.5,
      "prefdivis": null,
      "price": 81.5,
      "ps": 82.5,
      "ps1": 83.5,
      "receivables": 84.5,
      "retearn": 85.5,
      "revenue": 1200000000,
      "revenueusd": 87.5,
      "rnd": 88.5,
      "roa": 89.5,
      "roe": 90.5,
      "roic": 91.5,
      "ros": 92.5,
      "sbcomp": 93.5,
      "sgna": 94.5,
      "sharefactor": 95.5,
      "sharesbas": 96.5,
      "shareswa": 97.5,
      "shareswadil": 98.5,
      "sps": 99.5,
      "tangibles": 100.5,
      "taxassets": 101.5,
      "taxexp": 102.5,
      "taxliabilities": 103.5,
      "tbvps": 104.5,
      "workingcapital": 105.5
    },
    {
      "ticker": "XTEST",
      "dimension": "MRQ",
      "calendardate": "2025-12-31",
      "date": "2025-12-31",
      "reportperiod": "2025-12-31",
      "fiscalperiod": "2025-Q4",
      "lastupdated": "2026-02-06",
      "accoci": 1.5,
      "assets": "4500000000000",
      "assetsavg": 3.5,
      "assetsc": 4.5,
      "assetsnc": 5.5,
      "assetturnover": 6.5,
      "bvps": 7.5,
      "capex": 8.5,
      "cashneq": 9.5,
      "cashnequsd": 10.5,
      "cor": 11.5,
      "consolinc": 12.5,
      "currentratio": 13.5,
      "de": 14.5,
      "debt": 15.5,
      "debtc": 16.5,
      "debtnc": 17.5,
      "debtusd": 18.5,
      "deferredrev": 19.5,
      "depamor": 20.5,
      "deposits": 21.5,
      "divyield": 22.5,
      "dps": 23.5,
      "ebit": 24.5,
      "ebitda": 25.5,
      "ebitdamargin": 26.5,
      "ebitdausd": 27.5,
      "ebitusd": 28.5,
      "ebt": 29.5,
      "eps": 30.5,
      "epsdil": 31.5,
      "epsusd": 32.5,
      "equity": 33.5,
      "equityavg": 34.5,
      "equityusd": 35.5,
      "ev": "31000000000000",
      "evebit": 37.5,
      "evebitda": 38.5,
      "fcf": 39.5,
      "fcfps": 40.5,
      "fxusd": 41.5,
      "gp": 42.5,
      "grossmargin": 43.5,
      "intangibles": 44.5,
      "intexp": 45.5,
      "invcap": 46.5,
      "invcapavg": 47.5,
      "inventory": 48.5,
      "investments": 49.5,
      "investmentsc": 50.5,
      "investmentsnc": 51.5,
      "liabilities": 52.5,
      "liabilitiesc": 53.5,
      "liabilitiesnc": 54.5,
      "marketcap": "30000000000000",
      "ncf": 56.5,
      "ncfbus": 57.5,
      "ncfcommon": 58.5,
      "ncfdebt": 59.5,
      "ncfdiv": 60.5,
      "ncff": 61.5,
      "ncfi": 62.5,
      "ncfinv": 63.5,
      "ncfo": 64.5,
      "ncfx": 65.5,
      "netinc": 66.5,
      "netinccmn": 67.5,
      "netinccmnusd": 68.5,
      "netincdis": 69.5,
      "netincnci": 70.5,
      "netmargin": 71.5,
      "opex": 72.5,
      "opinc": 73.5,
      "payables": 74.5,
      "payoutratio": 75.5,
      "pb": 76.5,
      "pe": 77.5,
      "pe1": 78.5,
      "ppnenet": 79.5,
      "prefdivis": null,
      "price": 81.5,
      "ps": 82.5,
      "ps1": 83.5,
      "receivables": 84.5,
      "retearn": 85.5,
      "revenue": 1200000000,
      "revenueusd": 87.5,
      "rnd": 88.5,
      "roa": 89.5,
      "roe": 90.5,
      "roic": 91.5,
      "ros": 92.5,
      "sbcomp": 93.5,
      "sgna": 94.5,
      "sharefactor": 95.5,
      "sharesbas": 96.5,
      "shareswa": 97.5,
      "shareswadil": 98.5,
      "sps": 99.5,
      "tangibles": 100.5,
      "taxassets": 101.5,
      "taxexp": 102.5,
      "taxliabilities": 103.5,
      "tbvps": 104.5,
      "workingcapital": 105.5
    }
  ]
}
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/store/sharadar_fundamentals_test.py`:

```python
import datetime
import json
import math
import re

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sharadar_fundamentals import (
    ATTRIBUTE_COLUMNS,
    INDICATORS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SharadarFundamentals,
    read_rows,
)

from .helpers import client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
INVENTED = json.loads(fixture("sharadar_fundamentals.json"))["data"]
AS_REPORTED, MOST_RECENT = INVENTED
DESCRIBED = ("ticker", "dimension", "calendardate", "date", "reportperiod", "fiscalperiod", "lastupdated")


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_fundamentals", **fields)


def end_of(day):
    return datetime.datetime.combine(datetime.date.fromisoformat(day), datetime.time.max, datetime.UTC)


def without(row, field):
    return {name: found for name, found in row.items() if name != field}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarFundamentals(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarFundamentals.kind is Kind.TABLE
    assert SharadarFundamentals.daily_budget is None
    assert SharadarFundamentals.requests_per_minute == 120
    assert SharadarFundamentals.held_by == ()
    assert sources.TITLES["sharadar_fundamentals"] == "Sharadar"


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"dimension": "ARQ"}, "unknown field\\(s\\) for source 'sharadar_fundamentals': dimension"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarFundamentals(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_the_indicators_are_the_105_numeric_columns_of_the_answer_in_its_order():
    assert len(INDICATORS) == 105
    in_the_answer = tuple(name for name in AS_REPORTED if name not in DESCRIBED)
    assert in_the_answer == INDICATORS
    assert VALUE_COLUMNS == INDICATORS
    assert KEY_COLUMNS == ("ticker", "dimension", "calendardate")
    assert ATTRIBUTE_COLUMNS == ("reportperiod", "fiscalperiod", "datekey")


def test_a_row_keeps_every_indicator_as_a_number_and_describes_its_period_as_text():
    row, _most_recent = read_rows(INVENTED, "XTEST")
    assert set(row) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS, "published_at"}
    assert [row[column] for column in KEY_COLUMNS] == ["XTEST", "ARQ", "2025-12-31"]
    assert row["date"] == datetime.date(2025, 12, 31)
    assert (row["reportperiod"], row["fiscalperiod"], row["datekey"]) == ("2025-12-31", "2025-Q4", "2026-02-05")
    assert row["assets"] == 4_500_000_000_000.0  # a large value comes as text
    assert row["revenue"] == 1_200_000_000.0  # and a smaller one as a JSON number
    assert row["eps"] == 30.5
    assert math.isnan(row["prefdivis"])  # null is missing, never zero
    assert "lastupdated" not in row


def test_a_listed_column_the_answer_lacks_is_missing_and_an_unlisted_one_is_ignored():
    (row,) = read_rows([{**without(AS_REPORTED, "ncfo"), "newindicator": 7.0}], "XTEST")
    assert math.isnan(row["ncfo"])
    assert "newindicator" not in row
    assert row["netinc"] == 66.5


def test_an_as_reported_row_is_known_from_the_end_of_the_day_of_its_filing():
    as_reported, _most_recent = read_rows(INVENTED, "XTEST")
    assert as_reported["published_at"] == end_of("2026-02-05")  # its datekey, Sharadar's `date`


def test_a_most_recent_row_is_known_from_the_end_of_the_day_sharadar_last_touched_the_ticker():
    _as_reported, most_recent = read_rows(INVENTED, "XTEST")
    assert most_recent["datekey"] == "2025-12-31"  # the end of its period, not a day it was known
    assert most_recent["published_at"] == end_of("2026-02-06")  # its lastupdated


@pytest.mark.parametrize("dimension", ["ARQ", "ARY", "ART", "MRQ", "MRY", "MRT"])
def test_each_of_the_six_dimensions_is_dated_by_its_kind(dimension):
    (row,) = read_rows([{**AS_REPORTED, "dimension": dimension}], "XTEST")
    assert row["published_at"] == end_of("2026-02-05" if dimension.startswith("AR") else "2026-02-06")


def test_the_ticker_of_a_row_is_the_one_that_was_asked_for_whatever_the_answer_says():
    rows = read_rows([{**row, "ticker": "OTHER"} for row in INVENTED], "XTEST")
    assert {row["ticker"] for row in rows} == {"XTEST"}


def test_an_as_reported_period_filed_twice_is_two_versions():
    amended = {**AS_REPORTED, "date": "2026-03-10", "revenue": 1_250_000_000}
    first, second = read_rows([AS_REPORTED, amended], "XTEST")
    assert (first["published_at"], second["published_at"]) == (end_of("2026-02-05"), end_of("2026-03-10"))


def test_a_period_twice_known_from_the_same_moment_is_refused():
    (batch,) = fetch(lambda _request: answer([MOST_RECENT, {**MOST_RECENT, "revenue": 1.0}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds MRQ 2025-12-31 twice"


def test_a_dimension_sharadar_does_not_have_is_refused():
    (batch,) = fetch(lambda _request: answer([{**AS_REPORTED, "dimension": "XRQ"}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "a row whose dimension is not one of ARQ, ARY, ART, MRQ, MRY, MRT: 'XRQ'"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (without(AS_REPORTED, "calendardate"), "unexpected answer \\(KeyError: 'calendardate'"),
        (without(AS_REPORTED, "dimension"), "unexpected answer \\(KeyError: 'dimension'"),
        (without(AS_REPORTED, "date"), "unexpected answer \\(KeyError: 'date'"),
        (without(MOST_RECENT, "lastupdated"), "unexpected answer \\(KeyError: 'lastupdated'"),
        ({**AS_REPORTED, "calendardate": "2025Q4"}, "unexpected answer \\(ValueError"),
        ({**AS_REPORTED, "revenue": "lots"}, "unexpected answer \\(ValueError"),
    ],
)
def test_a_row_without_what_tells_it_apart_or_dates_it_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_the_fundamentals_of_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(INVENTED)

    fetch(handler, [Request(ticker()), Request(ticker("XOTHER", start=datetime.date(2020, 1, 1)))])
    first, second = seen
    assert first.url.path == "/v1.0/data/fundamentals"
    assert dict(first.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (second.url.params["ticker"], second.url.params["date.gte"]) == ("XOTHER", "2020-01-01")


def test_an_answer_is_one_versioned_table_of_the_ticker():
    (batch,) = fetch(lambda _request: answer(INVENTED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_fundamentals:XTEST", "Fundamentals of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == (ATTRIBUTE_COLUMNS, True)
    assert (table.stale_after_days, table.attrs) == (200, {})
    assert len(table.rows) == 2


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker()), Request(ticker("XOTHER"))], Credentials())
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left_and_names_the_table():
    def handler(_request):
        return httpx.Response(403, json={"error": "refused"})

    (batch,) = fetch(handler, [Request(ticker()), Request(ticker("XOTHER"))])
    assert [(failure.entry.source_id, failure.outcome) for failure in batch.failures] == [
        ("XTEST", Outcome.KEY_ERROR),
        ("XOTHER", Outcome.KEY_ERROR),
    ]
    assert "the table 'fundamentals'" in batch.failures[0].reason


def test_a_ticker_without_fundamentals_is_not_found():
    requests = [Request(ticker("XGONE")), Request(ticker("XLATE", start=datetime.date(2020, 1, 1)))]
    gone, late = fetch(lambda _request: answer([]), requests)
    assert gone.failures[0].outcome is Outcome.NOT_FOUND
    assert gone.failures[0].reason == "Sharadar has no fundamentals for 'XGONE'"
    assert late.failures[0].reason == "Sharadar has no fundamentals for 'XLATE' from 2020-01-01 on"


def test_a_failed_ticker_fails_alone():
    def handler(request):
        if request.url.params["ticker"] == "XBAD":
            return httpx.Response(200, text="<html>down</html>")
        return answer(INVENTED)

    failed, found = fetch(handler, [Request(ticker("XBAD")), Request(ticker())])
    assert failed.failures[0].outcome is Outcome.SOURCE_ERROR
    assert "not JSON" in failed.failures[0].reason
    assert found.tables[0].key == "sharadar_fundamentals:XTEST"
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_fundamentals_test.py -q`
Expected: an error while collecting, `E   ModuleNotFoundError: No module named 'data_pipeline.store.sources.sharadar_fundamentals'`, and `1 error`

- [ ] **Step 4: Write the source**

Create `src/data_pipeline/store/sources/sharadar_fundamentals.py`:

```python
"""Sharadar fundamentals: the indicators of one company, in Sharadar's six dimensions.

One catalog id is one ticker. One call brings every row of the ticker in Sharadar's
`fundamentals` table, on every run.

A row is told apart by its `ticker` (the catalog id), its `dimension` and its `calendardate`, the
calendar quarter or year it is about; `date` is that day. The indicators come wide, one column
each: the 105 of INDICATORS, in Sharadar's order, are read as numbers, whether they come as JSON
numbers or as text. A listed column an answer lacks is missing; a column Sharadar adds later is
not read until the list grows. `reportperiod`, `fiscalperiod` and `datekey` (Sharadar's `date` of
the row) are stored as text and never compared.

The table is versioned. An as-reported row (`ARQ`, `ARY`, `ART`) is known from the end of the day
of its `datekey`, the day of the filing. A most-recent row (`MRQ`, `MRY`, `MRT`) carries in
`datekey` the end of its period, not a day it was known: it is known from the end of the day of
its `lastupdated`, the latest day Sharadar says it touched the ticker.

Two costs follow. Sharadar stamps every row of a ticker with a new `lastupdated` when it updates
it, so each update stores another version of every most-recent row, with the same numbers. And an
as-reported row Sharadar corrects without a new `datekey` keeps the day of the original: `as_of`
a day between the two reads the correction.
"""

import datetime
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Kind, Outcome, Request, TableData
from data_pipeline.store.sources.base import HeldBy, missing_key, number, reject_params
from data_pipeline.store.sources.sharadar import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Sharadar,
    check_ticker,
    per_ticker,
    text,
)

TABLE = "fundamentals"
AS_REPORTED = ("ARQ", "ARY", "ART")
MOST_RECENT = ("MRQ", "MRY", "MRT")
DIMENSIONS = (*AS_REPORTED, *MOST_RECENT)
KEY_COLUMNS = ("ticker", "dimension", "calendardate")
INDICATORS = (
    "accoci", "assets", "assetsavg", "assetsc", "assetsnc", "assetturnover", "bvps", "capex", "cashneq",
    "cashnequsd", "cor", "consolinc", "currentratio", "de", "debt", "debtc", "debtnc", "debtusd", "deferredrev",
    "depamor", "deposits", "divyield", "dps", "ebit", "ebitda", "ebitdamargin", "ebitdausd", "ebitusd", "ebt",
    "eps", "epsdil", "epsusd", "equity", "equityavg", "equityusd", "ev", "evebit", "evebitda", "fcf", "fcfps",
    "fxusd", "gp", "grossmargin", "intangibles", "intexp", "invcap", "invcapavg", "inventory", "investments",
    "investmentsc", "investmentsnc", "liabilities", "liabilitiesc", "liabilitiesnc", "marketcap", "ncf",
    "ncfbus", "ncfcommon", "ncfdebt", "ncfdiv", "ncff", "ncfi", "ncfinv", "ncfo", "ncfx", "netinc", "netinccmn",
    "netinccmnusd", "netincdis", "netincnci", "netmargin", "opex", "opinc", "payables", "payoutratio", "pb", "pe",
    "pe1", "ppnenet", "prefdivis", "price", "ps", "ps1", "receivables", "retearn", "revenue", "revenueusd", "rnd",
    "roa", "roe", "roic", "ros", "sbcomp", "sgna", "sharefactor", "sharesbas", "shareswa", "shareswadil", "sps",
    "tangibles", "taxassets", "taxexp", "taxliabilities", "tbvps", "workingcapital",
)  # fmt: skip
VALUE_COLUMNS = INDICATORS
ATTRIBUTE_COLUMNS = ("reportperiod", "fiscalperiod", "datekey")
STALE_AFTER_DAYS = 200  # a quarter, the filing deadline, and slack, as for SEC XBRL facts

Row = dict[str, object]


def known_from(dimension: str, item: Mapping[str, Any]) -> datetime.datetime:
    """When a row is known: the end of the day of its `datekey` (Sharadar's `date`) for an
    as-reported row, of its `lastupdated` for a most-recent one."""
    if dimension not in DIMENSIONS:
        msg = f"a row whose dimension is not one of {', '.join(DIMENSIONS)}: {dimension!r}"
        raise AnswerError(msg)
    field = "date" if dimension in AS_REPORTED else "lastupdated"
    day = datetime.date.fromisoformat(str(item[field]))
    # Sharadar gives the day, not the hour: known from the end of that day, so no moment within it sees it early
    return datetime.datetime.combine(day, datetime.time.max, datetime.UTC)


def read_rows(items: Sequence[Mapping[str, Any]], ticker: str) -> tuple[Row, ...]:
    """The rows of a fundamentals answer as versioned table rows, each with the ticker that was
    asked for. A dimension and period that come twice, known from the same moment, are an
    AnswerError: nothing would tell the two apart."""
    rows: list[Row] = []
    seen: set[tuple[str, str, datetime.datetime]] = set()
    for item in items:
        dimension = str(item["dimension"])
        period = datetime.date.fromisoformat(str(item["calendardate"]))
        published_at = known_from(dimension, item)
        if (dimension, period.isoformat(), published_at) in seen:
            msg = f"the answer holds {dimension} {period.isoformat()} twice"
            raise AnswerError(msg)
        seen.add((dimension, period.isoformat(), published_at))
        rows.append(
            {
                "ticker": ticker,
                "dimension": dimension,
                "calendardate": period.isoformat(),
                "date": period,
                **{column: number(item.get(column)) for column in INDICATORS},
                "reportperiod": text(item.get("reportperiod")),
                "fiscalperiod": text(item.get("fiscalperiod")),
                "datekey": text(item.get("date")),
                "published_at": published_at,
            }
        )
    return tuple(rows)


class SharadarFundamentals:
    name = "sharadar_fundamentals"
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
            reason = f"Sharadar has no fundamentals for {entry.source_id!r}{since}"
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Fundamentals of {entry.source_id} (Sharadar)",
            rows=rows,
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            stale_after_days=STALE_AFTER_DAYS,
            attribute_columns=ATTRIBUTE_COLUMNS,
            versioned=True,
        )
        return FetchBatch(tables=(data,))
```

- [ ] **Step 5: Register the source**

In `src/data_pipeline/store/sources/__init__.py`, after the line `from data_pipeline.store.sources.sharadar_actions import SharadarActions`, add:

```python
from data_pipeline.store.sources.sharadar_fundamentals import SharadarFundamentals
```

In `REGISTRY`, after the line `    "sharadar_actions": SharadarActions,`, add:

```python
    "sharadar_fundamentals": SharadarFundamentals,
```

In `TITLES`, after the line `    "sharadar_actions": "Sharadar",`, add:

```python
    "sharadar_fundamentals": "Sharadar",
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_fundamentals_test.py -q`
Expected: `30 passed`

- [ ] **Step 7: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1021 passed, 14 skipped, 18 deselected, 22 warnings`

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 37 source files`

- [ ] **Step 8: Commit**

```bash
git add src/data_pipeline/store/sources/__init__.py src/data_pipeline/store/sources/sharadar_fundamentals.py tests/unit/store/fixtures/sharadar_fundamentals.json tests/unit/store/sharadar_fundamentals_test.py
git commit -m "feat(sources): Sharadar fundamentals in its six dimensions, each row dated by when it was known" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Fundamentals through the store

No production code changes here. These tests pin what the store does with the rows of Task 2: a second sync with the same answer adds nothing, a new filing adds its rows and another version of every most-recent row, a restated most-recent value is a new version, `as_of` before a filing does not see it, the index and `status` work. If one fails, stop and report it.

**Files:**
- Modify: `tests/unit/store/sharadar_fundamentals_test.py`

- [ ] **Step 1: Replace the imports**

In `tests/unit/store/sharadar_fundamentals_test.py`, replace everything from the first line down to the line `from .helpers import client, entry, fixture` with:

```python
import datetime
import json
import math
import re

import httpx
import pandas as pd
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sharadar_fundamentals import (
    ATTRIBUTE_COLUMNS,
    INDICATORS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SharadarFundamentals,
    read_rows,
)

from .helpers import NOW, client, entry, fixture
```

- [ ] **Step 2: Append the tests**

At the end of the file, add:

```python
# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


def filed(row, calendardate, datekey, lastupdated, **values):
    """`row` for another period, filed on `datekey`, as sent after the update of `lastupdated`."""
    return {**row, "calendardate": calendardate, "date": datekey, "lastupdated": lastupdated, **values}


def updated(row, lastupdated, **values):
    """`row` as Sharadar sends it after a later update of the ticker."""
    return {**row, "lastupdated": lastupdated, **values}


NEXT_QUARTER = [
    filed(AS_REPORTED, "2026-03-31", "2026-05-01", "2026-05-02", revenue=1_300_000_000),
    filed(MOST_RECENT, "2026-03-31", "2026-03-28", "2026-05-02", revenue=1_300_000_000),
    updated(AS_REPORTED, "2026-05-02"),
    updated(MOST_RECENT, "2026-05-02"),
]


class Vendor:
    """A fake Sharadar whose answer the test can change between syncs."""

    def __init__(self):
        self.rows = INVENTED
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        assert request.url.path == "/v1.0/data/fundamentals"
        return answer(self.rows if request.url.params["ticker"] == "XTEST" else [])


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_fundamentals\n  ids: [XTEST]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def stored(tmp_path):
    """Every version the store holds of XTEST."""
    return pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_fundamentals" / "XTEST.parquet")


def revenue(table):
    """{(dimension, calendardate): revenue} of a read."""
    return {(row.dimension, row.calendardate): row.revenue for row in table.itertuples()}


def test_a_first_sync_stores_both_rows_and_a_second_with_the_same_answer_adds_nothing(store, vendor, clock):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (2, 0)
    table = store.table("sharadar_fundamentals", "XTEST")
    assert list(table.columns) == [*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS]
    assert revenue(table) == {("ARQ", "2025-12-31"): 1_200_000_000.0, ("MRQ", "2025-12-31"): 1_200_000_000.0}
    # every fixture row has a null prefdivis: a missing value received again is the same version
    assert table["prefdivis"].isna().all()
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert vendor.calls == 2  # one call per ticker per run, the whole history each time


def test_each_row_is_stored_with_the_moment_it_was_known(store, tmp_path):
    known = dict(zip(stored(tmp_path)["dimension"], stored(tmp_path)["published_at"], strict=True))
    assert known == {"ARQ": pd.Timestamp(end_of("2026-02-05")), "MRQ": pd.Timestamp(end_of("2026-02-06"))}


def test_a_new_filing_adds_its_rows_and_another_version_of_every_most_recent_row(store, vendor, clock, tmp_path):
    vendor.rows = NEXT_QUARTER
    clock["now"] = LATER
    report = store.sync()
    # the two rows of the new quarter, and the old MRQ row once more: same numbers, a later lastupdated
    assert (report.sources[0].new, report.sources[0].revised) == (2, 1)
    versions = stored(tmp_path)
    assert len(versions) == 5
    old_quarter = versions[versions["calendardate"] == "2025-12-31"]
    assert sorted(old_quarter["published_at"]) == [
        pd.Timestamp(end_of("2026-02-05")),  # the ARQ row, dated by its filing whatever lastupdated says
        pd.Timestamp(end_of("2026-02-06")),
        pd.Timestamp(end_of("2026-05-02")),
    ]
    assert revenue(store.table("sharadar_fundamentals", "XTEST", dimension="ARQ")) == {
        ("ARQ", "2025-12-31"): 1_200_000_000.0,
        ("ARQ", "2026-03-31"): 1_300_000_000.0,
    }


def test_as_of_a_day_before_a_filing_does_not_see_it(store, vendor, clock):
    vendor.rows = NEXT_QUARTER
    clock["now"] = LATER
    store.sync()
    # the as-reported panel as it stood that day, with no look-ahead
    before = store.table("sharadar_fundamentals", "XTEST", dimension="ARQ", as_of="2026-04-30")
    assert list(before["calendardate"]) == ["2025-12-31"]
    on_the_day = store.table("sharadar_fundamentals", "XTEST", dimension="ARQ", as_of="2026-05-01")
    assert list(on_the_day["calendardate"]) == ["2025-12-31", "2026-03-31"]
    # the most-recent rows of the new quarter are known only from the day of Sharadar's update
    most_recent = store.table("sharadar_fundamentals", "XTEST", dimension="MRQ", as_of="2026-05-01")
    assert list(most_recent["calendardate"]) == ["2025-12-31"]
    assert store.table("sharadar_fundamentals", "XTEST", as_of="2026-02-04").empty


def test_a_restated_most_recent_value_is_a_new_version_and_as_of_reads_the_one_before(store, vendor, clock):
    vendor.rows = [AS_REPORTED, updated(MOST_RECENT, "2026-03-15", revenue=1_150_000_000, fiscalperiod="2025-Q4R")]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    now = store.table("sharadar_fundamentals", "XTEST", dimension="MRQ")
    assert (now["revenue"].iloc[0], now["fiscalperiod"].iloc[0]) == (1_150_000_000.0, "2025-Q4R")
    before = store.table("sharadar_fundamentals", "XTEST", dimension="MRQ", as_of="2026-03-14")
    assert (before["revenue"].iloc[0], before["fiscalperiod"].iloc[0]) == (1_200_000_000.0, "2025-Q4")
    # the as-reported row keeps what was filed
    assert store.table("sharadar_fundamentals", "XTEST", dimension="ARQ")["revenue"].iloc[0] == 1_200_000_000.0


def test_the_index_row_of_a_ticker(store):
    info = store.info("sharadar_fundamentals:XTEST")
    assert (info.kind, info.name) == ("table", "Fundamentals of XTEST (Sharadar)")
    assert (info.frequency, info.last_period) == ("", "2025-12-31")
    assert info.label == "[Sharadar: XTEST, 2025-12-31, fetched 2026-06-06]"
    row = store.index().iloc[0]
    assert json.loads(row["attrs"]) == {}
    assert row["stale_after_days"] == 200


def test_status_uses_the_two_hundred_days(store, clock):
    clock["now"] = datetime.datetime(2026, 7, 19, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"  # two hundred days after the last period
    clock["now"] = datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2025-12-31 (201 days ago)"


def test_a_ticker_sharadar_does_not_have_fails_in_the_index(tmp_path, vendor, clock):
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    store = Store(
        tmp_path / "store",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    store.add("sharadar_fundamentals", ["XGONE"])
    report = store.sync()
    assert report.sources[0].failed == (
        ("sharadar_fundamentals:XGONE", "not_found: Sharadar has no fundamentals for 'XGONE'"),
    )
```

- [ ] **Step 3: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_fundamentals_test.py -q`
Expected: `38 passed, 22 warnings`. The warnings are the `PerformanceWarning` of `storage.table_frame` that "Read this first" describes, two for each sync of the tests; no other warning.

- [ ] **Step 4: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1029 passed, 14 skipped, 18 deselected, 44 warnings`

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 37 source files`

- [ ] **Step 5: Commit**

```bash
git add tests/unit/store/sharadar_fundamentals_test.py
git commit -m "test(sharadar): fundamentals through the store: a new filing, a restatement, as_of and status" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The tickers source

**Files:**
- Create: `tests/unit/store/fixtures/sharadar_tickers.json`
- Create: `tests/unit/store/sharadar_tickers_test.py`
- Create: `src/data_pipeline/store/sources/sharadar_tickers.py`
- Modify: `src/data_pipeline/store/sources/__init__.py`

- [ ] **Step 1: Write the fixture**

Create `tests/unit/store/fixtures/sharadar_tickers.json`. It is written by hand in the shape of an answer of the `tickers` table: the record of an invented ticker in two of Sharadar's tables. Every value is invented.

```json
{
  "count": 2,
  "data": [
    {"table": "stocks", "permaticker": 900001, "ticker": "XTEST", "name": "TEST CORP", "exchange": "TESTEX", "isdelisted": "N", "category": "Test Stock", "cusips": "000000001", "siccode": 9999, "sicsector": "Test Sector", "sicindustry": "Test Industry", "figi": "TESTFIGI0001", "famaindustry": "N/A", "sector": "Testing", "industry": "Test Equipment", "scalemarketcap": "3 - Test", "scalerevenue": "2 - Test", "relatedtickers": null, "currency": "USD", "location": "Testland", "lastupdated": "2026-02-06", "firstadded": "2014-01-02", "firstpricedate": "2001-03-01", "lastpricedate": "2026-02-06", "firstquarter": "2001-03-31", "lastquarter": "2025-12-31", "secfilings": "https://www.example.com/filings/XTEST", "companysite": "https://www.example.com"},
    {"table": "fundamentals", "permaticker": 900001, "ticker": "XTEST", "name": "TEST CORP", "exchange": "TESTEX", "isdelisted": "N", "category": "Test Stock", "cusips": "000000001", "siccode": 9999, "sicsector": "Test Sector", "sicindustry": "Test Industry", "figi": "TESTFIGI0001", "famaindustry": "N/A", "sector": "Testing", "industry": "Test Equipment", "scalemarketcap": "3 - Test", "scalerevenue": "2 - Test", "relatedtickers": null, "currency": "USD", "location": "Testland", "lastupdated": "2026-02-06", "firstadded": "2014-01-02", "firstpricedate": "N/A", "lastpricedate": "N/A", "firstquarter": "2001-03-31", "lastquarter": "2025-12-31", "secfilings": "https://www.example.com/filings/XTEST", "companysite": "https://www.example.com"}
  ]
}
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/store/sharadar_tickers_test.py`:

```python
import datetime
import json
import math
import re

import httpx
import pandas as pd
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sharadar_tickers import (
    ATTRIBUTE_COLUMNS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SharadarTickers,
    read_rows,
)

from .helpers import NOW, client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
INVENTED = json.loads(fixture("sharadar_tickers.json"))["data"]
STOCKS, FUNDAMENTALS = INVENTED


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_tickers", **fields)


def end_of(day):
    return datetime.datetime.combine(datetime.date.fromisoformat(day), datetime.time.max, datetime.UTC)


def without(row, field):
    return {name: found for name, found in row.items() if name != field}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarTickers(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarTickers.kind is Kind.TABLE
    assert SharadarTickers.daily_budget is None
    assert SharadarTickers.requests_per_minute == 120
    assert SharadarTickers.held_by == ()
    assert sources.TITLES["sharadar_tickers"] == "Sharadar"


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "stocks"}, "unknown field\\(s\\) for source 'sharadar_tickers': table"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarTickers(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_there_is_one_row_per_table_the_ticker_is_in():
    rows = read_rows(INVENTED, "XTEST")
    assert [(row["ticker"], row["table"]) for row in rows] == [("XTEST", "stocks"), ("XTEST", "fundamentals")]
    assert KEY_COLUMNS == ("ticker", "table")


def test_a_row_keeps_the_permaticker_as_a_number_and_the_rest_as_text():
    row, _fundamentals = read_rows(INVENTED, "XTEST")
    assert set(row) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS, "published_at"}
    assert VALUE_COLUMNS == ("permaticker",)
    assert len(ATTRIBUTE_COLUMNS) == 25
    assert row["permaticker"] == 900001.0
    assert (row["name"], row["sector"], row["isdelisted"]) == ("TEST CORP", "Testing", "N")
    assert row["siccode"] == "9999"  # sent as a number, stored as text
    assert (row["famaindustry"], row["relatedtickers"]) == ("", "")  # N/A and null are empty
    assert row["lastupdated"] == "2026-02-06"


def test_a_row_is_dated_and_known_by_its_lastupdated():
    row, _fundamentals = read_rows(INVENTED, "XTEST")
    assert row["date"] == datetime.date(2026, 2, 6)
    assert row["published_at"] == end_of("2026-02-06")


def test_the_ticker_of_a_row_is_the_one_that_was_asked_for_whatever_the_answer_says():
    rows = read_rows([{**row, "ticker": "OTHER"} for row in INVENTED], "XTEST")
    assert {row["ticker"] for row in rows} == {"XTEST"}


def test_a_permaticker_sharadar_leaves_empty_is_missing():
    (row,) = read_rows([{**STOCKS, "permaticker": None}], "XTEST")
    assert math.isnan(row["permaticker"])


def test_a_table_twice_in_one_answer_is_refused():
    (batch,) = fetch(lambda _request: answer([STOCKS, {**STOCKS, "sector": "Other"}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds the table 'stocks' twice"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (without(STOCKS, "table"), "unexpected answer \\(KeyError: 'table'"),
        ({**STOCKS, "table": None}, "a row without the table it describes: None"),
        ({**STOCKS, "table": "N/A"}, "a row without the table it describes: 'N/A'"),
        (without(STOCKS, "lastupdated"), "unexpected answer \\(KeyError: 'lastupdated'"),
        ({**STOCKS, "lastupdated": "February"}, "unexpected answer \\(ValueError"),
    ],
)
def test_a_row_without_its_table_or_its_day_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_the_record_of_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(INVENTED)

    fetch(handler, [Request(ticker()), Request(ticker("XOTHER", start=datetime.date(2020, 1, 1)))])
    first, second = seen
    assert first.url.path == "/v1.0/data/tickers"
    assert dict(first.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (second.url.params["ticker"], second.url.params["date.gte"]) == ("XOTHER", "2020-01-01")


def test_an_answer_is_one_versioned_table_of_the_ticker_that_never_grows_stale():
    (batch,) = fetch(lambda _request: answer(INVENTED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_tickers:XTEST", "Ticker record of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == (ATTRIBUTE_COLUMNS, True)
    assert (table.stale_after_days, table.attrs) == (None, {})


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker())], Credentials())
    assert batch.failures[0].outcome is Outcome.KEY_ERROR
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left_and_names_the_table():
    (batch,) = fetch(lambda _request: httpx.Response(403), [Request(ticker()), Request(ticker("XOTHER"))])
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "the table 'tickers'" in batch.failures[0].reason


def test_a_ticker_sharadar_has_no_record_of_is_not_found():
    requests = [Request(ticker("XGONE")), Request(ticker("XLATE", start=datetime.date(2020, 1, 1)))]
    gone, late = fetch(lambda _request: answer([]), requests)
    assert gone.failures[0].outcome is Outcome.NOT_FOUND
    assert gone.failures[0].reason == "Sharadar has no ticker record for 'XGONE'"
    assert late.failures[0].reason == "Sharadar has no ticker record for 'XLATE' from 2020-01-01 on"


# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


class Vendor:
    """A fake Sharadar whose answer the test can change between syncs."""

    def __init__(self):
        self.rows = INVENTED

    def __call__(self, request):
        assert request.url.path == "/v1.0/data/tickers"
        return answer(self.rows)


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_tickers\n  ids: [XTEST]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def test_a_first_sync_stores_a_row_per_table_and_the_same_answer_again_adds_nothing(store, clock, tmp_path):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (2, 0)
    record = store.table("sharadar_tickers", "XTEST")
    assert list(record.columns) == [*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS]
    assert list(record["table"]) == ["fundamentals", "stocks"]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_tickers" / "XTEST.parquet")) == 2


def test_a_new_sector_with_a_new_lastupdated_is_a_version_and_as_of_reads_the_old_one(store, vendor, clock):
    vendor.rows = [
        {**STOCKS, "sector": "Retesting", "lastupdated": "2026-05-04"},
        {**FUNDAMENTALS, "sector": "Retesting", "lastupdated": "2026-05-04"},
    ]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 2)
    now = store.table("sharadar_tickers", "XTEST", table="stocks")
    assert (now["sector"].iloc[0], now["lastupdated"].iloc[0]) == ("Retesting", "2026-05-04")
    assert now["date"].iloc[0] == pd.Timestamp("2026-05-04")
    before = store.table("sharadar_tickers", "XTEST", table="stocks", as_of="2026-05-03")
    assert (before["sector"].iloc[0], before["lastupdated"].iloc[0]) == ("Testing", "2026-02-06")
    assert store.table("sharadar_tickers", "XTEST", as_of="2026-02-05").empty


def test_a_change_sharadar_does_not_stamp_with_a_new_lastupdated_stores_nothing(store, vendor, clock):
    # attributes are never compared: only a new lastupdated makes a new version
    vendor.rows = [{**STOCKS, "sector": "Retesting"}, FUNDAMENTALS]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert store.table("sharadar_tickers", "XTEST", table="stocks")["sector"].iloc[0] == "Testing"


def test_the_index_row_of_a_ticker_and_its_record_never_grows_stale(store, clock):
    info = store.info("sharadar_tickers:XTEST")
    assert (info.kind, info.name) == ("table", "Ticker record of XTEST (Sharadar)")
    assert (info.frequency, info.last_period) == ("", "2026-02-06")
    assert info.label == "[Sharadar: XTEST, 2026-02-06, fetched 2026-06-06]"
    assert pd.isna(store.index().iloc[0]["stale_after_days"])
    clock["now"] = datetime.datetime(2036, 1, 1, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_tickers_test.py -q`
Expected: an error while collecting, `E   ModuleNotFoundError: No module named 'data_pipeline.store.sources.sharadar_tickers'`, and `1 error`

- [ ] **Step 4: Write the source**

Create `src/data_pipeline/store/sources/sharadar_tickers.py`:

```python
"""Sharadar tickers: the record Sharadar keeps of one ticker, once per table it appears in.

One catalog id is one ticker, of a stock or of an ETF. One call brings its rows of Sharadar's
`tickers` table, on every run: one row per Sharadar table that has the ticker (`stocks`, `funds`,
`fundamentals`, ...).

A row is told apart by its `ticker` (the catalog id) and its `table`. `permaticker`, Sharadar's
permanent id, is the one number; everything else Sharadar says of the ticker (name, exchange,
sector, the first and last days it has, ...) is stored as sent, as text, with `N/A` and `null`
empty, and never compared.

The table is versioned: a row is known from the end of the day of its `lastupdated`, and `date` is
that day. A record that changes, a new sector or the delisted flag, comes with a new `lastupdated`:
the store keeps it as a new version, and `as_of` reads the record as it was. The same record sent
again stores nothing. A ticker's record has no period to grow old by, so it is never stale.
"""

import datetime
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Kind, Outcome, Request, TableData
from data_pipeline.store.sources.base import HeldBy, missing_key, number, reject_params
from data_pipeline.store.sources.sharadar import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Sharadar,
    check_ticker,
    per_ticker,
    text,
)

TABLE = "tickers"
KEY_COLUMNS = ("ticker", "table")
VALUE_COLUMNS = ("permaticker",)
ATTRIBUTE_COLUMNS = (
    "name", "exchange", "isdelisted", "category", "cusips", "siccode", "sicsector", "sicindustry", "figi",
    "famaindustry", "sector", "industry", "scalemarketcap", "scalerevenue", "relatedtickers", "currency",
    "location", "lastupdated", "firstadded", "firstpricedate", "lastpricedate", "firstquarter", "lastquarter",
    "secfilings", "companysite",
)  # fmt: skip

Row = dict[str, object]


def read_rows(items: Sequence[Mapping[str, Any]], ticker: str) -> tuple[Row, ...]:
    """The rows of a tickers answer as versioned table rows, each with the ticker that was asked
    for. A table that comes twice is an AnswerError: one of the two would be dropped without
    notice."""
    rows: list[Row] = []
    seen: set[str] = set()
    for item in items:
        table = text(item["table"])
        if not table:
            msg = f"a row without the table it describes: {item['table']!r}"
            raise AnswerError(msg)
        if table in seen:
            msg = f"the answer holds the table {table!r} twice"
            raise AnswerError(msg)
        seen.add(table)
        day = datetime.date.fromisoformat(str(item["lastupdated"]))
        rows.append(
            {
                "ticker": ticker,
                "table": table,
                "date": day,
                "permaticker": number(item.get("permaticker")),
                **{column: text(item.get(column)) for column in ATTRIBUTE_COLUMNS},
                # Sharadar gives the day, not the hour: known from the end of that day
                "published_at": datetime.datetime.combine(day, datetime.time.max, datetime.UTC),
            }
        )
    return tuple(rows)


class SharadarTickers:
    name = "sharadar_tickers"
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
            reason = f"Sharadar has no ticker record for {entry.source_id!r}{since}"
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Ticker record of {entry.source_id} (Sharadar)",
            rows=rows,
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            attribute_columns=ATTRIBUTE_COLUMNS,
            versioned=True,
        )
        return FetchBatch(tables=(data,))
```

- [ ] **Step 5: Register the source**

In `src/data_pipeline/store/sources/__init__.py`, after the line `from data_pipeline.store.sources.sharadar_prices import SharadarPrices`, add:

```python
from data_pipeline.store.sources.sharadar_tickers import SharadarTickers
```

In `REGISTRY`, after the line `    "sharadar_prices": SharadarPrices,`, add:

```python
    "sharadar_tickers": SharadarTickers,
```

In `TITLES`, after the line `    "sharadar_prices": "Sharadar",`, add:

```python
    "sharadar_tickers": "Sharadar",
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_tickers_test.py -q`
Expected: `23 passed`

- [ ] **Step 7: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1052 passed, 14 skipped, 18 deselected, 44 warnings`

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 38 source files`

- [ ] **Step 8: Commit**

```bash
git add src/data_pipeline/store/sources/__init__.py src/data_pipeline/store/sources/sharadar_tickers.py tests/unit/store/fixtures/sharadar_tickers.json tests/unit/store/sharadar_tickers_test.py
git commit -m "feat(sources): Sharadar's record of each ticker, a version for each lastupdated" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The daily metrics source

**Files:**
- Create: `tests/unit/store/fixtures/sharadar_daily.json`
- Create: `tests/unit/store/sharadar_daily_test.py`
- Create: `src/data_pipeline/store/sources/sharadar_daily.py`
- Modify: `src/data_pipeline/store/sources/__init__.py`

- [ ] **Step 1: Write the fixture**

Create `tests/unit/store/fixtures/sharadar_daily.json`. It is written by hand in the shape of an answer of the `daily` table: three days of an invented ticker, newest first, one metric `null`.

```json
{
  "count": 3,
  "data": [
    {"ticker": "XTEST", "date": "2026-03-04", "lastupdated": "2026-03-04", "ev": 1250.5, "evebit": 20.1, "evebitda": 15.2, "marketcap": 1200.0, "pb": 4.1, "pe": 25.3, "ps": 6.2},
    {"ticker": "XTEST", "date": "2026-03-03", "lastupdated": "2026-03-04", "ev": 1240.0, "evebit": 19.9, "evebitda": 15.0, "marketcap": 1190.5, "pb": 4.0, "pe": 25.1, "ps": 6.1},
    {"ticker": "XTEST", "date": "2026-03-02", "lastupdated": "2026-03-04", "ev": 1230.0, "evebit": null, "evebitda": 14.9, "marketcap": 1180.0, "pb": 4.0, "pe": 24.9, "ps": 6.0}
  ]
}
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/store/sharadar_daily_test.py`:

```python
import datetime
import json
import math
import re

import httpx
import pandas as pd
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sharadar_daily import KEY_COLUMNS, VALUE_COLUMNS, SharadarDaily, read_rows
from data_pipeline.store.sync import held_periods

from .helpers import NOW, client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
INVENTED = json.loads(fixture("sharadar_daily.json"))["data"]


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_daily", **fields)


def without(row, field):
    return {name: found for name, found in row.items() if name != field}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarDaily(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarDaily.kind is Kind.TABLE
    assert SharadarDaily.daily_budget is None
    assert SharadarDaily.requests_per_minute == 120
    assert SharadarDaily.held_by == ()
    assert sources.TITLES["sharadar_daily"] == "Sharadar"


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "daily"}, "unknown field\\(s\\) for source 'sharadar_daily': table"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarDaily(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_a_row_keeps_the_seven_metrics_and_drops_the_rest():
    newest, _middle, oldest = read_rows(INVENTED, "XTEST")
    assert KEY_COLUMNS == ("ticker", "period")
    assert set(newest) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS}  # no lastupdated, no frequency
    assert (newest["ticker"], newest["period"], newest["date"]) == ("XTEST", "2026-03-04", datetime.date(2026, 3, 4))
    assert [newest[column] for column in VALUE_COLUMNS] == [1250.5, 20.1, 15.2, 1200.0, 4.1, 25.3, 6.2]
    assert math.isnan(oldest["evebit"])  # null is missing, never zero


def test_the_ticker_of_a_row_is_the_one_that_was_asked_for_whatever_the_answer_says():
    rows = read_rows([{**row, "ticker": "OTHER"} for row in INVENTED], "XTEST")
    assert {row["ticker"] for row in rows} == {"XTEST"}


def test_a_day_that_comes_twice_is_refused_rather_than_stored_once():
    first = INVENTED[0]
    (batch,) = fetch(lambda _request: answer([first, {**first, "pe": 30.0}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds the day 2026-03-04 twice"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (without(INVENTED[0], "date"), "unexpected answer \\(KeyError: 'date'"),
        (without(INVENTED[0], "pe"), "unexpected answer \\(KeyError: 'pe'"),
        ({**INVENTED[0], "date": "4 March"}, "unexpected answer \\(ValueError"),
    ],
)
def test_a_row_without_its_day_or_one_of_its_metrics_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_the_daily_metrics_of_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(INVENTED)

    fetch(handler, [Request(ticker()), Request(ticker("XOTHER", start=datetime.date(2020, 1, 1)))])
    first, second = seen
    assert first.url.path == "/v1.0/data/daily"
    assert dict(first.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (second.url.params["ticker"], second.url.params["date.gte"]) == ("XOTHER", "2020-01-01")


def test_an_answer_is_one_table_of_the_ticker():
    (batch,) = fetch(lambda _request: answer(INVENTED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_daily:XTEST", "Daily metrics of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == ((), False)
    assert (table.stale_after_days, table.attrs) == (10, {})
    assert len(table.rows) == 3


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker())], Credentials())
    assert batch.failures[0].outcome is Outcome.KEY_ERROR
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left_and_names_the_table():
    (batch,) = fetch(lambda _request: httpx.Response(403), [Request(ticker()), Request(ticker("XOTHER"))])
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "the table 'daily'" in batch.failures[0].reason


def test_a_ticker_without_daily_metrics_is_not_found():
    requests = [Request(ticker("XGONE")), Request(ticker("XLATE", start=datetime.date(2020, 1, 1)))]
    gone, late = fetch(lambda _request: answer([]), requests)
    assert gone.failures[0].outcome is Outcome.NOT_FOUND
    assert gone.failures[0].reason == "Sharadar has no daily metrics for 'XGONE'"
    assert late.failures[0].reason == "Sharadar has no daily metrics for 'XLATE' from 2020-01-01 on"


# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


class Vendor:
    """A fake Sharadar whose answer the test can change between syncs."""

    def __init__(self):
        self.rows = INVENTED

    def __call__(self, request):
        assert request.url.path == "/v1.0/data/daily"
        return answer(self.rows)


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_daily\n  ids: [XTEST]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def stored(tmp_path):
    return pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_daily" / "XTEST.parquet")


def test_a_first_sync_stores_every_day_and_a_second_adds_nothing(store, clock, tmp_path):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (3, 0)
    table = store.table("sharadar_daily", "XTEST")
    assert list(table.columns) == [*KEY_COLUMNS, "date", *VALUE_COLUMNS]
    assert list(table["period"]) == ["2026-03-02", "2026-03-03", "2026-03-04"]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert "frequency" not in stored(tmp_path).columns
    assert held_periods(stored(tmp_path), SharadarDaily.held_by) == frozenset()


def test_a_restated_day_is_stored_again_and_as_of_reads_what_was_held_before(store, vendor, clock, tmp_path):
    newest, *earlier = INVENTED
    vendor.rows = [{**newest, "date": "2026-03-05", "pe": 26.0}, {**newest, "pe": 25.0}, *earlier]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 1)
    day = store.table("sharadar_daily", "XTEST", period="2026-03-04")
    assert day["pe"].iloc[0] == 25.0
    before = store.table("sharadar_daily", "XTEST", period="2026-03-04", as_of=NOW)
    assert before["pe"].iloc[0] == 25.3
    assert len(stored(tmp_path)) == 5  # nothing is deleted
    assert stored(tmp_path)["published_at"].isna().all()  # dated by the fetch: Sharadar does not say when


def test_the_index_row_of_a_ticker(store):
    info = store.info("sharadar_daily:XTEST")
    assert (info.kind, info.name) == ("table", "Daily metrics of XTEST (Sharadar)")
    assert (info.frequency, info.last_period) == ("", "2026-03-04")
    assert info.label == "[Sharadar: XTEST, 2026-03-04, fetched 2026-06-06]"
    assert store.index().iloc[0]["stale_after_days"] == 10


def test_status_uses_the_ten_days_of_a_daily_series(store, clock):
    clock["now"] = datetime.datetime(2026, 3, 14, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"
    clock["now"] = datetime.datetime(2026, 3, 15, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2026-03-04 (11 days ago)"
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_daily_test.py -q`
Expected: an error while collecting, `E   ModuleNotFoundError: No module named 'data_pipeline.store.sources.sharadar_daily'`, and `1 error`

- [ ] **Step 4: Write the source**

Create `src/data_pipeline/store/sources/sharadar_daily.py`:

```python
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
```

- [ ] **Step 5: Register the source**

The three lines go after those of `sharadar_actions`, so that the names stay in order (`sharadar_daily` comes before `sharadar_fundamentals`).

In `src/data_pipeline/store/sources/__init__.py`, after the line `from data_pipeline.store.sources.sharadar_actions import SharadarActions`, add:

```python
from data_pipeline.store.sources.sharadar_daily import SharadarDaily
```

In `REGISTRY`, after the line `    "sharadar_actions": SharadarActions,`, add:

```python
    "sharadar_daily": SharadarDaily,
```

In `TITLES`, after the line `    "sharadar_actions": "Sharadar",`, add:

```python
    "sharadar_daily": "Sharadar",
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_daily_test.py -q`
Expected: `18 passed`

- [ ] **Step 7: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1070 passed, 14 skipped, 18 deselected, 44 warnings`

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 39 source files`

- [ ] **Step 8: Commit**

```bash
git add src/data_pipeline/store/sources/__init__.py src/data_pipeline/store/sources/sharadar_daily.py tests/unit/store/fixtures/sharadar_daily.json tests/unit/store/sharadar_daily_test.py
git commit -m "feat(sources): Sharadar daily metrics, one row per ticker and day" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The live test

**Files:**
- Modify: `tests/live/sharadar_live_test.py`

- [ ] **Step 1: Write the test**

Replace the whole content of `tests/live/sharadar_live_test.py` with the following. The two tests already there do not change; the docstring, the imports, `Recording` and the third test are new.

```python
"""Eight real calls to Sharadar. Off by default; run with:  pytest -m live tests/live

Needs SHARADAR_API_KEY in the environment or in ./.env, a plan with prices, actions,
fundamentals, tickers and daily metrics, and network access. It exists to notice when Sharadar
changes the shape of its answers, and it settles what the free tier does not show: the `funds`
table, a split among the actions, the actions of an ETF, the whole history of the actions, of
the fundamentals and of the daily metrics, that the `datekey` of every as-reported row comes
after its period, and what a ticker Sharadar does not have is answered with.

It checks shapes only, never a value: Sharadar's license forbids publishing its data. Whether a
dividend's value is the amount as paid is not a shape: the acceptance run settles it.
"""

import datetime

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store
from data_pipeline.store.sources import sharadar_daily as daily
from data_pipeline.store.sources import sharadar_fundamentals as fundamentals
from data_pipeline.store.sources import sharadar_tickers as tickers
from data_pipeline.store.sources.sharadar_prices import VALUE_COLUMNS

DEFAULT_WINDOW_YEARS = 5  # what a call without a first day is answered with
NO_SUCH_TICKER = "ZZZZQQ"


@pytest.mark.live
def test_sharadar_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.SHARADAR) is None:
        pytest.skip("SHARADAR_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("sharadar_prices", ["AAPL"])
    store.add("sharadar_prices", ["SPY"], table="funds")
    store.add("sharadar_actions", ["AAPL", "SPY"])
    report = store.sync()
    assert report.exit_code == 0
    assert {source.source: source.calls for source in report.sources} == {"sharadar_actions": 2, "sharadar_prices": 2}
    today = datetime.datetime.now(datetime.UTC).date()
    for ticker in ("AAPL", "SPY"):  # a stock, and an ETF from the table the free tier does not show
        prices = store.table("sharadar_prices", ticker)
        assert prices["period"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()
        assert prices[list(VALUE_COLUMNS)].notna().any().all()  # every column is there and holds numbers
        first = prices["date"].min().date()
        assert first < today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))  # the whole history came
    actions = store.table("sharadar_actions", "AAPL")
    oldest = actions["date"].min().date()
    assert oldest < today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))  # not a recent window only
    assert {"dividend", "split"} <= set(actions["action"])  # the words the README recipe reads
    amounts = actions[actions["action"].isin(["dividend", "split"])]["value"].dropna()
    assert (amounts > 0).all()
    # an ETF has actions in the same table: the catalog example lists funds under sharadar_actions
    assert "dividend" in set(store.table("sharadar_actions", "SPY")["action"])


@pytest.mark.live
def test_a_ticker_sharadar_does_not_have_is_not_found(tmp_path):
    # with a paid key an unknown ticker is taken to answer with no rows, not with a refusal: a
    # refusal would fail every ticker after it as a rejected key
    if keys.resolve().get(keys.SHARADAR) is None:
        pytest.skip("SHARADAR_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("sharadar_prices", [NO_SUCH_TICKER])
    report = store.sync()
    (source,) = report.sources
    ((key, reason),) = source.failed
    assert key == f"sharadar_prices:{NO_SUCH_TICKER}"
    assert reason.startswith("not_found")


class Recording(httpx.BaseTransport):
    """A transport that keeps in memory, by Sharadar table, the rows of the last answer it
    passed on. Nothing it keeps is written anywhere: the test reads their shape, never a value."""

    def __init__(self, inner=None):
        self._inner = inner or httpx.HTTPTransport()
        self.rows = {}

    def handle_request(self, request):
        response = self._inner.handle_request(request)
        response.read()
        if response.status_code == httpx.codes.OK:
            self.rows[request.url.path.rsplit("/", 1)[-1]] = response.json().get("data") or []
        return response

    def close(self):
        self._inner.close()


@pytest.mark.live
def test_the_fundamentals_tickers_and_daily_metrics_still_answer_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.SHARADAR) is None:
        pytest.skip("SHARADAR_API_KEY is not set")
    recording = Recording()
    store = Store(tmp_path / "store", transport=recording)
    for source in ("sharadar_fundamentals", "sharadar_tickers", "sharadar_daily"):
        store.add(source, ["AAPL"])
    report = store.sync()
    assert report.exit_code == 0
    assert {source.source: source.calls for source in report.sources} == {
        "sharadar_daily": 1,
        "sharadar_fundamentals": 1,
        "sharadar_tickers": 1,
    }
    # every column a source reads is in every row Sharadar sends, and the fundamentals have no column
    # the source does not know: a renamed indicator would otherwise be stored as missing without notice
    described = {"ticker", "dimension", "calendardate", "date", "reportperiod", "fiscalperiod", "lastupdated"}
    for row in recording.rows["fundamentals"]:
        assert set(row) == {*described, *fundamentals.INDICATORS}
    for row in recording.rows["tickers"]:
        assert {"ticker", "table", *tickers.VALUE_COLUMNS, *tickers.ATTRIBUTE_COLUMNS} <= set(row)
    for row in recording.rows["daily"]:
        assert {"ticker", "date", *daily.VALUE_COLUMNS} <= set(row)

    today = datetime.datetime.now(datetime.UTC).date()
    whole_history = today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))
    panel = store.table("sharadar_fundamentals", "AAPL")
    assert set(panel["dimension"]) == set(fundamentals.DIMENSIONS)
    assert panel["date"].min().date() < whole_history  # not the recent window of the free tier
    as_reported = panel[panel["dimension"].isin(fundamentals.AS_REPORTED)]
    # the day an as-reported row is known from comes after the end of the period it reports
    assert (as_reported["datekey"] > as_reported["reportperiod"]).all()
    record = store.table("sharadar_tickers", "AAPL")
    assert not record.empty
    assert record["permaticker"].notna().all()
    metrics = store.table("sharadar_daily", "AAPL")
    assert metrics["period"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()
    assert metrics["date"].min().date() < whole_history
```

- [ ] **Step 2: Check that it is collected, without running it**

Do not run it: the checkout's `.env` may hold the paid key, and the test would make real calls with it. Task 9 runs it, with the user's permission.

Run: `.venv/Scripts/python -m pytest -m live tests/live/sharadar_live_test.py --collect-only -q`
Expected:

```
tests/live/sharadar_live_test.py::test_sharadar_still_answers_in_the_expected_shape
tests/live/sharadar_live_test.py::test_a_ticker_sharadar_does_not_have_is_not_found
tests/live/sharadar_live_test.py::test_the_fundamentals_tickers_and_daily_metrics_still_answer_in_the_expected_shape

3 tests collected
```

(with the time it took after `collected`)

- [ ] **Step 3: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1070 passed, 14 skipped, 19 deselected, 44 warnings` (one more live test deselected)

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 39 source files`

- [ ] **Step 4: Commit**

```bash
git add tests/live/sharadar_live_test.py
git commit -m "test(live): the fundamentals, tickers and daily tables of one real ticker, shapes only" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: README and changelog

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: The list at the top of the README**

In `README.md`, after the bullet on equity prices, which ends with `your own subscription. See [Equity prices](#equity-prices).`, add:

```markdown
- **Equity fundamentals:** Sharadar's fundamentals in its six dimensions, its record of each ticker
  and its daily valuation metrics; the as-reported figures read as they stood on any past day.
  See [Equity fundamentals](#equity-fundamentals).
```

- [ ] **Step 2: The table of keys**

Find:

```markdown
| `SHARADAR_API_KEY` | Sharadar | US stock and ETF prices and corporate actions; a paid subscription |
```

Replace with:

```markdown
| `SHARADAR_API_KEY` | Sharadar | US stock and ETF prices, corporate actions and fundamentals; a paid subscription |
```

- [ ] **Step 3: The Python example**

After the line `store.table("sharadar_prices", "AAPL")           # one row per day: open, high, low, close, volume`, add:

```python
store.table("sharadar_fundamentals", "AAPL", dimension="ARQ", as_of="2024-03-01")  # as filed by that day
```

- [ ] **Step 4: Reading as of a past date**

In the list under `### Reading as of a past date`, after the bullet on SEC XBRL facts and SEC filings, which ends with "Nothing to re-download.", add:

```markdown
- **Sharadar fundamentals and ticker records:** an as-reported row is dated with the day of its
  filing, a most-recent row and a ticker record with the day Sharadar last updated the ticker,
  each known from the end of that day. See [Equity fundamentals](#equity-fundamentals).
```

- [ ] **Step 5: The section on equity fundamentals**

Just before the heading `## Data terms`, after the section `## Equity prices`, add this section:

````markdown
## Equity fundamentals

`sharadar_fundamentals`, `sharadar_tickers` and `sharadar_daily` store three more tables of
Sharadar's for the tickers a catalog lists, with the same key and the same license:

```yaml
- source: sharadar_fundamentals
  ids: [AAPL, MSFT, BRK.B]
  attrs: {commercial_ok: "no"}
- source: sharadar_tickers
  ids: [AAPL, MSFT, BRK.B, SPY]
  attrs: {commercial_ok: "no"}
- source: sharadar_daily
  ids: [AAPL, MSFT, BRK.B]
  attrs: {commercial_ok: "no"}
```

Each run asks for the whole history of each ticker, one call per ticker and table, and stores
what changed. A ticker listed in all five Sharadar sources costs five calls a run: `sync --source
sharadar_tickers`, say, runs one of them alone, at a rhythm of its own.

- **Fundamentals:** one row per `ticker`, `dimension` and `calendardate`, the calendar quarter
  or year the row is about. The dimensions are `ARQ`, `ARY` and `ART` (as reported, by quarter,
  by year and over the trailing twelve months) and `MRQ`, `MRY` and `MRT` (the most recent
  figures, restated). Each row holds Sharadar's 105 indicators (`revenue`, `netinc`, `assets`,
  `ncfo`, `eps`, `pe`, ...) as numbers, in Sharadar's units, and `reportperiod`, `fiscalperiod`
  and `datekey` as text.
- **Ticker records:** one row per `ticker` and Sharadar `table` the ticker is in (`stocks`,
  `funds`, `fundamentals`, ...), with `permaticker`, Sharadar's permanent id, and what Sharadar
  says of the ticker (`name`, `exchange`, `sector`, `industry`, `isdelisted`, ...) as text. A
  record has no period, so `status` never calls it stale.
- **Daily metrics:** one row per `ticker` and day (`period`) with `ev`, `evebit`, `evebitda`,
  `marketcap`, `pb`, `pe` and `ps`. As with prices, a day Sharadar restates is stored again, and
  they read as of a date from your first sync on.

Fundamentals and ticker records are dated by Sharadar. An as-reported row counts as known from
the end of the day of its `datekey`, the day of the filing, so the as-reported panel reads as it
stood on any past day, with no look-ahead, back to the first filing:

```python
store.table("sharadar_fundamentals", "AAPL", dimension="ARQ", as_of="2024-03-01")
```

A most-recent row carries in `datekey` the end of its period, not a day it was known: it counts
as known from the end of the day of its `lastupdated`, the latest day Sharadar touched the
ticker. A ticker record counts as known from its `lastupdated` too, so as of an earlier day a
ticker that changed sector reads with the old one. Two costs come with this:

- Sharadar stamps every row of a ticker with a new `lastupdated` when it updates it. Each time,
  the store keeps another version of every most-recent row of that ticker, with the same numbers
  and a later date: a few dozen rows per ticker per filing.
- If Sharadar corrects an as-reported row without changing its `datekey`, the correction counts
  as known from the day of the original, so `as_of` a day between the two reads the correction.
  Without `as_of`, a read gives the version downloaded last.
````

- [ ] **Step 6: The changelog**

In `CHANGELOG.md`, after the bullet on Sharadar prices and actions, which ends with `holds no Sharadar data: the fixtures are invented.`, add:

```markdown
- Tables: Sharadar fundamentals in its six dimensions, its record of each ticker and its daily
  valuation metrics (`sharadar_fundamentals`, `sharadar_tickers`, `sharadar_daily`). Fundamentals
  and ticker records are versioned: an as-reported row is known from the day of its filing, a
  most-recent row and a ticker record from the day Sharadar last updated the ticker.
```

- [ ] **Step 7: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1070 passed, 14 skipped, 19 deselected, 44 warnings`

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

- [ ] **Step 8: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: Sharadar fundamentals, tickers and daily metrics in the README and the changelog" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: The whole suite

- [ ] **Step 1: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1070 passed, 14 skipped, 19 deselected, 44 warnings`

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 39 source files`

- [ ] **Step 2: Check that nothing of Sharadar's went in**

Run: `git diff docs/sharadar-fundamentals-design --stat`
Expected: these 14 files and no other, `14 files changed, 1713 insertions(+), 7 deletions(-)`:

```
 CHANGELOG.md                                       |   4 +
 README.md                                          |  64 +++-
 src/data_pipeline/store/sources/__init__.py        |   9 +
 src/data_pipeline/store/sources/sharadar_daily.py  | 103 ++++++
 .../store/sources/sharadar_fundamentals.py         | 148 ++++++++
 .../store/sources/sharadar_tickers.py              | 117 ++++++
 tests/live/sharadar_live_test.py                   |  77 +++-
 tests/unit/store/fixtures/sharadar_daily.json      |   8 +
 .../unit/store/fixtures/sharadar_fundamentals.json | 233 ++++++++++++
 tests/unit/store/fixtures/sharadar_tickers.json    |   7 +
 tests/unit/store/sharadar_daily_test.py            | 244 ++++++++++++
 tests/unit/store/sharadar_fundamentals_test.py     | 409 +++++++++++++++++++++
 tests/unit/store/sharadar_tickers_test.py          | 275 ++++++++++++++
 tests/unit/store/versions_test.py                  |  22 ++
```

Read the diff of the fixtures and of the README once more: every ticker, name and number in a fixture is invented, and no sentence measures or judges Sharadar's data.

- [ ] **Step 3: Tell the user**

Acceptance criterion 1 of the spec is met. Criteria 2 and 3 need the user's paid key: stop here and ask before Task 9. Remind the user of the decision in "Read this first".

---

### Task 9: Acceptance with a paid key (only after the user says so)

Nothing in this task is committed but the write-back of Step 4. The test reads Sharadar's data; the license forbids publishing it.

- [ ] **Step 1: The user says yes**

Ask the user, in chat, for permission to make three real calls to Sharadar with the paid key, one per table for AAPL. Never ask for the key in the chat and never type it into a command: it is already in the user's `.env`.

- [ ] **Step 2: The live test**

Run: `.venv/Scripts/python -m pytest -m live tests/live/sharadar_live_test.py -k fundamentals_tickers_and_daily -q`
Expected: `1 passed, 2 deselected`, with the warnings of "Read this first". The two tests of prices and actions are left out: they make five calls more, and the user's permission covers three.

- [ ] **Step 3: If it fails**

Report which assertion failed, in terms of shape only: a column missing or new, a dimension absent, a first period less than six years old, a `datekey` not after its `reportperiod`, a key twice in one answer (`SOURCE_ERROR` with "twice"). Never paste a value of Sharadar's into the chat, a file or a commit. A key twice with two different `datekey`s is stored as two versions by Task 2; a key twice with the same moment fails the ticker: if the live run shows the second, stop and bring the decision to the user.

- [ ] **Step 4: Write back what the shapes turned out to be**

Acceptance criterion 3: in the spec's "Limits this spec leaves", replace the bullet **Not verified with a paid key** with what the run showed, in terms of shape only: whether fundamentals and daily metrics came with their whole history from `date.gte` on, and whether `datekey` came after `reportperiod` on every as-reported row of the ticker probed. Then:

```bash
git add docs/superpowers/specs/2026-10-22-sharadar-fundamentals-design.md
git commit -m "docs(specs): what the live test showed of the Sharadar fundamentals, in shapes only" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Report**

Tell the user what passed and what was written back. The user runs any sync of the real store (`D:/finport-store/store`); never run it yourself.
