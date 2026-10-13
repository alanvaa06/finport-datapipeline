# Sharadar Prices and Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Read this first:** the plan below is as it was written. Review and three decisions of the owner changed what was built: see "After the plan" at the end before using any task as a reference. Task 8 was brought up to date.

**Goal:** Store the daily prices and the corporate actions of the US stocks and ETFs listed in the catalog, as Sharadar publishes them, in two table sources, with no change to the core.

**Architecture:** Two table sources, `sharadar_prices` and `sharadar_actions`, share one module for the call and for the ways Sharadar refuses. Each asks one table for the whole history of one ticker on every run; the store's own `append_rows` keeps what changed, so a split shows up as new rows for the earlier days. A new key, `SHARADAR_API_KEY`, joins the registry with a check that asks for a ticker outside Sharadar's free tier.

**Tech Stack:** Python 3.12+, pandas, pyarrow, httpx, click, pytest (`httpx.MockTransport`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-10-12-sharadar-prices-design.md`

**Verified:** every file below was built in a scratch clone, and the tasks were replayed there in this order, each one ending green: 919 tests pass (869 before), ruff and mypy clean. The script of Task 8 was run against a fake Sharadar only. Not verified: anything that needs a paid key (Task 8), which is where the spec's "Assumptions the live test settles" get settled.

**Conventions:** work on branch `feat/sharadar-prices`, created from `docs/sharadar-prices-design` (which holds the spec and this plan). Run tools with `.venv/Scripts/python`; run mypy as `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`. Console output is ASCII only. Commit messages follow the repository's style (`feat(sources): ...`).

**The rule this plan cannot break:** the repository is public and Sharadar's license forbids sharing its data and publishing assessments of it. Every fixture and every number in a test is invented (`XTEST`, `TEST CORP`). Never paste an answer of Sharadar's, or a judgment of its data, into a file, a test, a comment, a commit message or the README. What the probes of 2026-10-12 found is in `reports/Sharadar pruebas con key publica.md`, which git ignores.

**Decided here, beyond the spec:**
- A ticker without actions is recorded as reached (`ok` in the index) and writes no table file, as an empty answer does for every table source. `Store.table("sharadar_actions", "<TICKER>")` then raises `UnknownSeriesError`. The spec was corrected to say so.
- The loop over tickers, with its mapping of failures to outcomes, is one function shared by both sources: `sharadar.per_ticker`.
- A price row that lacks one of the six value columns fails its ticker (`SOURCE_ERROR`); a value sent as `null` is stored as missing.
- The hint "an ETF needs `table: funds`" is given only when the entry asked the `stocks` table.
- The check of the key asks for 400 days of GOOGL, the window every other check uses.
- `requests_per_minute = 120` is an assumption: Sharadar publishes no rate for this API.

---

## File structure

| File | Responsibility |
|---|---|
| `src/data_pipeline/credentials.py` | the constant `SHARADAR` and its `Key` record, last in the registry |
| `src/data_pipeline/store/sources/sharadar.py` | what both sources share: the call, the ticker rule, the refusals, the loop over tickers |
| `src/data_pipeline/store/sources/sharadar_prices.py` | `SharadarPrices` and the reading of price rows |
| `src/data_pipeline/store/sources/sharadar_actions.py` | `SharadarActions` and the reading of action rows, with `item` |
| `src/data_pipeline/store/sources/__init__.py` | the two names in `REGISTRY` and `TITLES` |
| `src/data_pipeline/keys_check.py` | the check of the key |
| `tests/unit/store/fixtures/sharadar_prices.json`, `sharadar_actions.json` | hand-written answers, invented |
| `tests/unit/store/sharadar_prices_test.py`, `sharadar_actions_test.py` | unit tests |
| `tests/unit/credentials_test.py`, `keys_cli_test.py`, `keys_check_test.py` | the tests that count the keys, and the check |
| `tests/unit/store/tables_test.py` | a table named `CON` |
| `tests/live/sharadar_live_test.py` | four real calls, off by default, shapes only |
| `README.md`, `CHANGELOG.md` | documentation |

`sync.py`, `storage.py` and `api.py` are not touched.

---

### Task 1: The key, the call and the prices source

**Files:**
- Create: `tests/unit/store/fixtures/sharadar_prices.json`
- Create: `tests/unit/store/sharadar_prices_test.py`
- Create: `src/data_pipeline/store/sources/sharadar.py`
- Create: `src/data_pipeline/store/sources/sharadar_prices.py`
- Modify: `src/data_pipeline/credentials.py`
- Modify: `src/data_pipeline/store/sources/__init__.py`
- Modify: `src/data_pipeline/keys_check.py`
- Modify: `tests/unit/credentials_test.py`, `tests/unit/keys_cli_test.py`, `tests/unit/keys_check_test.py`

- [ ] **Step 1: Create the branch**

```bash
git checkout docs/sharadar-prices-design
git checkout -b feat/sharadar-prices
```

- [ ] **Step 2: Write the fixture**

Create `tests/unit/store/fixtures/sharadar_prices.json`. It is written by hand in the shape of an answer of the `stocks` table: an invented ticker that split 2-for-1 on 2026-03-03, newest day first, as Sharadar sends them.

```json
{
  "count": 4,
  "data": [
    {"ticker": "XTEST", "date": "2026-03-04", "open": 20.5, "high": 21.0, "low": 20.1, "close": 20.8, "volume": 1500000, "closeadj": 20.8, "closeunadj": 20.8, "lastupdated": "2026-03-04"},
    {"ticker": "XTEST", "date": "2026-03-03", "open": 20.0, "high": 20.6, "low": 19.9, "close": 20.4, "volume": 1200000, "closeadj": 20.4, "closeunadj": 20.4, "lastupdated": "2026-03-04"},
    {"ticker": "XTEST", "date": "2026-03-02", "open": 19.5, "high": 20.2, "low": 19.4, "close": 20.0, "volume": 2000000, "closeadj": 19.9, "closeunadj": 40.0, "lastupdated": "2026-03-04"},
    {"ticker": "XTEST", "date": "2026-02-27", "open": 19.0, "high": 19.6, "low": 18.9, "close": 19.5, "volume": 1800000, "closeadj": 19.4, "closeunadj": 39.0, "lastupdated": "2026-03-04"}
  ]
}
```

- [ ] **Step 3: Write the failing tests**

Create `tests/unit/store/sharadar_prices_test.py`:

```python
import datetime
import json
import math
import re

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources import sharadar
from data_pipeline.store.sources.sharadar_prices import KEY_COLUMNS, VALUE_COLUMNS, SharadarPrices, read_rows

from .helpers import client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
RECORDED = json.loads(fixture("sharadar_prices.json"))["data"]


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_prices", **fields)


def price(day, close=20.0, raw=None, **more):
    """One row of a price answer. `raw` is the close as it traded, `close` when not given."""
    return {
        "ticker": "XTEST",
        "date": day,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000,
        "closeadj": close,
        "closeunadj": close if raw is None else raw,
        "lastupdated": "2026-03-04",
        **more,
    }


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarPrices(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarPrices.kind is Kind.TABLE
    assert SharadarPrices.daily_budget is None
    assert SharadarPrices.requests_per_minute == 120
    assert SharadarPrices.held_by == ()


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("aapl", {}, "a Sharadar id is a ticker in upper case"),
        ("BRK B", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "bonds"}, "'table' must be one of stocks, funds"),
        ("XTEST", {"adjusted": True}, "unknown field\\(s\\) for source 'sharadar_prices': adjusted"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarPrices(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


def test_validate_accepts_a_ticker_with_a_dot_and_both_tables():
    source = SharadarPrices(client(), CREDENTIALS)
    source.validate(ticker("BRK.B"))
    source.validate(ticker("XFUND", params={"table": "funds"}))


# -- reading rows ------------------------------------------------------------------------------


def test_a_price_row_keeps_the_six_values_and_drops_the_rest():
    newest, *_others, oldest = read_rows(RECORDED)
    assert set(newest) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS}  # no closeadj, no lastupdated
    assert (newest["frequency"], newest["period"], newest["date"]) == ("D", "2026-03-04", datetime.date(2026, 3, 4))
    assert [newest[column] for column in VALUE_COLUMNS] == [20.5, 21.0, 20.1, 20.8, 1500000.0, 20.8]
    assert (oldest["close"], oldest["closeunadj"]) == (19.5, 39.0)  # adjusted for the split, and as it traded


def test_a_value_sharadar_leaves_empty_is_missing():
    (row,) = read_rows([price("2026-03-04", volume=None)])
    assert math.isnan(row["volume"])
    assert row["close"] == 20.0


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_one_table_for_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(RECORDED)

    fetch(handler, [Request(ticker()), Request(ticker("XFUND", params={"table": "funds"}))])
    stocks, funds = seen
    assert stocks.url.path == "/v1.0/data/stocks"
    assert dict(stocks.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",  # always sent: without it Sharadar answers with a recent window only
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (funds.url.path, funds.url.params["ticker"]) == ("/v1.0/data/funds", "XFUND")


def test_the_start_of_an_entry_is_the_first_day_asked_for():
    seen = []

    def handler(request):
        seen.append(request.url.params["date.gte"])
        return answer(RECORDED)

    fetch(handler, [Request(ticker(start=datetime.date(2020, 1, 1)))])
    assert seen == ["2020-01-01"]


def test_an_answer_is_one_table_of_the_ticker():
    (batch,) = fetch(lambda _request: answer(RECORDED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_prices:XTEST", "Daily prices of XTEST (Sharadar stocks)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == ((), False)
    assert (table.stale_after_days, table.attrs) == (10, {"table": "stocks"})
    assert len(table.rows) == 4


# -- how Sharadar refuses ----------------------------------------------------------------------


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker()), Request(ticker("XOTHER"))], Credentials())
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left_and_stops():
    calls = []

    def handler(request):
        calls.append(request.url.params["ticker"])
        if request.url.params["ticker"] == "XFREE":
            return answer(RECORDED)
        return httpx.Response(403, json={"error": "Exceeds free tier"})

    requests = [Request(ticker("XFREE")), Request(ticker("XPAID")), Request(ticker("XNEXT"))]
    stored, refused = fetch(handler, requests)
    assert stored.tables[0].key == "sharadar_prices:XFREE"  # a wrong key still gets the free tier's tickers
    assert [(failure.entry.source_id, failure.outcome) for failure in refused.failures] == [
        ("XPAID", Outcome.KEY_ERROR),
        ("XNEXT", Outcome.KEY_ERROR),
    ]
    assert "free tier (HTTP 403): check SHARADAR_API_KEY" in refused.failures[0].reason
    assert "the table 'stocks'" in refused.failures[0].reason
    assert calls == ["XFREE", "XPAID"]


def test_a_persistent_429_stops_the_source_as_an_exhausted_quota():
    with pytest.raises(QuotaExhaustedError, match="HTTP 429"):
        fetch(lambda _request: httpx.Response(429), [Request(ticker())])


def test_a_ticker_without_prices_is_not_found_and_an_etf_is_pointed_to_its_table():
    stocks, funds = fetch(
        lambda _request: answer([]),
        [Request(ticker("XFUND")), Request(ticker("XGONE", params={"table": "funds"}))],
    )
    assert stocks.failures[0].outcome is Outcome.NOT_FOUND
    assert stocks.failures[0].reason == (
        "Sharadar has no prices for 'XFUND' in its table 'stocks'; an ETF needs `table: funds`"
    )
    assert funds.failures[0].reason == "Sharadar has no prices for 'XGONE' in its table 'funds'"


def test_a_failed_call_fails_its_ticker_and_the_next_one_goes_on():
    def handler(request):
        if request.url.params["ticker"] == "XDOWN":
            return httpx.Response(500)
        return answer(RECORDED)

    failed, found = fetch(handler, [Request(ticker("XDOWN")), Request(ticker())])
    assert failed.failures[0].outcome is Outcome.NETWORK_ERROR
    assert found.tables[0].key == "sharadar_prices:XTEST"


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (httpx.Response(400, text="bad request"), "HTTP 400: bad request"),
        (httpx.Response(200, text="<html>down</html>"), "not JSON"),
        (httpx.Response(200, json={"count": 0}), "unexpected answer \\(KeyError"),
        (httpx.Response(200, json={"data": "none"}), "`data` is not a list of rows"),
        (httpx.Response(200, json={"data": [{"date": "2026-03-04", "close": 1.0}]}), "unexpected answer \\(KeyError"),
        (httpx.Response(200, json={"data": [{"date": "4 March"}]}), "unexpected answer \\(ValueError"),
    ],
)
def test_an_unexpected_answer_is_a_source_error(response, reason):
    (batch,) = fetch(lambda _request: response, [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


def test_a_day_that_comes_twice_is_refused_rather_than_stored_once():
    twice = [price("2026-03-04"), price("2026-03-04", close=21.0)]
    (batch,) = fetch(lambda _request: answer(twice), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds the day 2026-03-04 twice"


def test_an_answer_at_the_row_cap_is_refused_rather_than_stored_cut_short(monkeypatch):
    monkeypatch.setattr(sharadar, "MAX_ROWS", 4)
    (batch,) = fetch(lambda _request: answer(RECORDED), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert "cap of 4 rows and may be cut short: set `start` in the entry" in batch.failures[0].reason


def test_the_key_never_shows_in_a_reason():
    def handler(request):
        return httpx.Response(400, text=f"no such thing: {request.url}")

    (batch,) = fetch(handler, [Request(ticker())])
    assert KEY_VALUE not in batch.failures[0].reason
    assert "api_key=***" in batch.failures[0].reason
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_prices_test.py -q`
Expected: an error while collecting, `ImportError: cannot import name 'sharadar' from 'data_pipeline.store.sources'`

- [ ] **Step 5: Add the key to the registry**

In `src/data_pipeline/credentials.py`, find:

```python
SEC_UA = "SEC_EDGAR_UA"
```

Replace with:

```python
SEC_UA = "SEC_EDGAR_UA"
SHARADAR = "SHARADAR_API_KEY"
```

In the same file, find the end of `REGISTRY`:

```python
        "Set it to 'Your Name you@domain.com' (https://www.sec.gov/os/accessing-edgar-data)",
        secret=False,
    ),
)
```

Replace with:

```python
        "Set it to 'Your Name you@domain.com' (https://www.sec.gov/os/accessing-edgar-data)",
        secret=False,
    ),
    Key(
        SHARADAR,
        "Sharadar",
        "US equities - prices & actions",
        "Subscribe at https://sharadar.com/subscribe and copy the key from your account",
    ),
)
```

- [ ] **Step 6: Update the three tests that count the keys**

In `tests/unit/credentials_test.py`, find:

```python
def test_the_registry_holds_the_six_keys_in_setup_order():
    assert credentials.NAMES == (
        "FRED_API_KEY",
        "BLS_API_KEY",
        "BANXICO_TOKEN",
        "INEGI_TOKEN",
        "COMTRADE_API_KEY",
        "SEC_EDGAR_UA",
    )
```

Replace with:

```python
def test_the_registry_holds_the_seven_keys_in_setup_order():
    assert credentials.NAMES == (
        "FRED_API_KEY",
        "BLS_API_KEY",
        "BANXICO_TOKEN",
        "INEGI_TOKEN",
        "COMTRADE_API_KEY",
        "SEC_EDGAR_UA",
        "SHARADAR_API_KEY",
    )
```

In `tests/unit/keys_cli_test.py`, in `test_setup_refuses_numbers_outside_the_list`, find:

```python
    assert "1 to 6" in result.output
```

Replace with:

```python
    assert "1 to 7" in result.output
```

In the same file, in `test_setup_enter_picks_every_missing_key_and_enter_skips_each_one`, find:

```python
        result = runner.invoke(cli, ["setup"], input="\n" * 7)
```

Replace with:

```python
        result = runner.invoke(cli, ["setup"], input="\n" * 8)  # the choice, then one Enter per key
```

- [ ] **Step 7: Write the shared module**

Create `src/data_pipeline/store/sources/sharadar.py`:

```python
"""What the Sharadar sources share: the call, the ticker rule, and how Sharadar refuses.

Key in the `api_key` query parameter. One call asks one table for one ticker, from a first day
on. A call without a first day is answered with a recent window, not the whole history, so the
first day is always sent.

Sharadar does not reject a wrong key: it answers as its free tier, with data for the few
tickers that tier covers and HTTP 403 for every other one. So a 403 means the key is wrong or
the plan lacks the table, and it is the only sign of either.

An answer holds at most 10,000 rows and is cut there without notice: one that long is refused
rather than stored cut short.

Sharadar's license is for personal use and forbids sharing its data: this library ships no
answer of Sharadar's, and each user brings a key and a license of their own.
"""

import datetime
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.store.errors import (
    CatalogError,
    KeyRejectedError,
    NetworkError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Outcome, Request
from data_pipeline.store.sources.base import failures

URL = "https://api.sharadar.com/v1.0/data/{table}"
TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,11}$")
REQUESTS_PER_MINUTE = 120  # assumed: Sharadar publishes no rate for this API
EARLIEST = datetime.date(1900, 1, 1)
MAX_ROWS = 10_000
MISSING_TEXT = "N/A"
OK = 200
FORBIDDEN = 403


class AnswerError(Exception):
    """Sharadar answered, but not with the rows that were asked for."""


def check_ticker(entry: CatalogEntry) -> None:
    """Raise CatalogError when the id is not a ticker in upper case."""
    if not TICKER.match(entry.source_id):
        msg = f"{entry.key}: a Sharadar id is a ticker in upper case, such as AAPL or BRK.B"
        raise CatalogError(msg)


def text(value: object) -> str:
    """A text field as it is stored: Sharadar's `N/A`, like a field it leaves out, is empty."""
    found = "" if value is None else str(value)
    return "" if found == MISSING_TEXT else found


class Sharadar:
    """GET against Sharadar for one source, with the user's key."""

    def __init__(self, client: Client, source: str, key: str) -> None:
        self._client = client
        self._source = source
        self._key = key

    def rows(self, table: str, ticker: str, start: datetime.date | None) -> list[Mapping[str, Any]]:
        """The rows of one ticker in one table from `start` on; the whole history without it."""
        response = self._client.get(
            self._source,
            URL.format(table=table),
            params={
                "ticker": ticker,
                "date.gte": (start or EARLIEST).isoformat(),
                "format": "json",
                "api_key": self._key,
            },
            per_minute=REQUESTS_PER_MINUTE,
        )
        if response.status_code == FORBIDDEN:
            msg = (
                f"Sharadar treated the request as its free tier (HTTP 403): check {keys.SHARADAR} and that "
                f"the plan includes the table {table!r}"
            )
            raise KeyRejectedError(msg)
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {self._client.excerpt(response.text, 200)}"
            raise AnswerError(msg)
        try:
            payload = response.json()
        except ValueError as exc:  # JSONDecodeError, or bytes that are not text
            msg = f"unexpected answer (not JSON: {self._client.excerpt(response.text, 200)})"
            raise AnswerError(msg) from exc
        rows = payload["data"]
        if not isinstance(rows, list):
            msg = "unexpected answer (`data` is not a list of rows)"
            raise AnswerError(msg)
        if len(rows) >= MAX_ROWS:
            msg = (
                f"the answer reached Sharadar's cap of {MAX_ROWS} rows and may be cut short: "
                "set `start` in the entry"
            )
            raise AnswerError(msg)
        return rows


def per_ticker(requests: Sequence[Request], download: Callable[[Request], FetchBatch]) -> Iterator[FetchBatch]:
    """Run `download` once per request and yield its batch. A refused key fails every request
    left; a persistent 429 stops the source; any other failure is that ticker's alone."""
    for position, request in enumerate(requests):
        entry = request.entry
        try:
            yield download(request)
        except KeyRejectedError as exc:
            yield FetchBatch(failures=failures(requests[position:], Outcome.KEY_ERROR, str(exc)))
            return
        except RateLimitedError as exc:
            # Asking again would only prolong the block: stop here, the next run resumes.
            raise QuotaExhaustedError(str(exc)) from exc
        except NetworkError as exc:
            yield FetchBatch(failures=(Failure(entry, Outcome.NETWORK_ERROR, str(exc)),))
        except AnswerError as exc:
            yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, str(exc)),))
        except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
            reason = f"unexpected answer ({type(exc).__name__}: {exc})"
            yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, reason),))
```

- [ ] **Step 8: Write the prices source**

Create `src/data_pipeline/store/sources/sharadar_prices.py`:

```python
"""Sharadar daily prices: one table per ticker, as Sharadar publishes it.

One catalog id is one ticker (`AAPL`, `BRK.B`). Stocks and ETFs live in two tables of
Sharadar's, `stocks` and `funds`: an entry says which with its `table` field, `stocks` when it
says nothing.

One call brings the whole history of a ticker, on every run: there is no "since". After a run
the latest version of every day is what Sharadar publishes that day.

`open`, `high`, `low`, `close` and `volume` come adjusted for splits, and Sharadar restates
them on every earlier day when a ticker splits again: the next run stores those days once more,
and the rows before stay. `closeunadj` is the close as it traded and never changes. Sharadar's
`closeadj`, the close adjusted for dividends too, is not stored: it is restated over the whole
history on every dividend. Nor is `lastupdated`. Nothing is derived: an adjusted series is the
consumer's work, from `closeunadj` and the actions of `sharadar_actions`.
"""

import datetime
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError
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
from data_pipeline.store.sources.sharadar import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Sharadar,
    check_ticker,
    per_ticker,
)

TABLES = ("stocks", "funds")
DEFAULT_TABLE = "stocks"
FIELDS = ("table",)
KEY_COLUMNS = ("frequency", "period")
VALUE_COLUMNS = ("open", "high", "low", "close", "volume", "closeunadj")
STALE_AFTER_DAYS = stale_after(Frequency.DAILY)

Row = dict[str, object]


def table_of(entry: CatalogEntry) -> str:
    """The table of Sharadar's an entry asks: its `table` field, `stocks` without one. Raises
    CatalogError on any other field or value."""
    reject_params(entry, FIELDS)
    table = str(entry.params.get("table", DEFAULT_TABLE))
    if table not in TABLES:
        msg = f"{entry.key}: 'table' must be one of {', '.join(TABLES)}"
        raise CatalogError(msg)
    return table


def read_rows(items: Sequence[Mapping[str, Any]]) -> tuple[Row, ...]:
    """The rows of a price answer as table rows. A day that comes twice is an AnswerError: one
    of the two would be dropped without notice."""
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
                "frequency": Frequency.DAILY.value,
                "period": day.isoformat(),
                "date": day,
                **{column: number(item[column]) for column in VALUE_COLUMNS},
            }
        )
    return tuple(rows)


class SharadarPrices:
    name = "sharadar_prices"
    kind = Kind.TABLE
    requests_per_minute = REQUESTS_PER_MINUTE
    daily_budget: int | None = None
    held_by: HeldBy = ()

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._key = credentials.get(keys.SHARADAR)

    def validate(self, entry: CatalogEntry) -> None:
        check_ticker(entry)
        table_of(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            yield missing_key(requests, keys.SHARADAR)
            return
        api = Sharadar(self._client, self.name, self._key)
        yield from per_ticker(requests, lambda request: self._ticker(api, request.entry))

    def _ticker(self, api: Sharadar, entry: CatalogEntry) -> FetchBatch:
        table = table_of(entry)
        rows = read_rows(api.rows(table, entry.source_id, entry.start))
        if not rows:
            hint = "; an ETF needs `table: funds`" if table == DEFAULT_TABLE else ""
            reason = f"Sharadar has no prices for {entry.source_id!r} in its table {table!r}{hint}"
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, reason),))
        data = TableData(
            entry=entry,
            key=entry.key,
            name=f"Daily prices of {entry.source_id} (Sharadar {table})",
            rows=rows,
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            stale_after_days=STALE_AFTER_DAYS,
            attrs={"table": table},
        )
        return FetchBatch(tables=(data,))
```

- [ ] **Step 9: Register the source**

In `src/data_pipeline/store/sources/__init__.py`, after the line `from data_pipeline.store.sources.sec_xbrl import SecXbrl`, add:

```python
from data_pipeline.store.sources.sharadar_prices import SharadarPrices
```

In `REGISTRY`, after the line `    "sec_xbrl": SecXbrl,`, add:

```python
    "sharadar_prices": SharadarPrices,
```

In `TITLES`, after the line `    "sec_xbrl": "SEC EDGAR",`, add:

```python
    "sharadar_prices": "Sharadar",
```

- [ ] **Step 10: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_prices_test.py -q`
Expected: `25 passed`

- [ ] **Step 11: Write the failing test of the check of the key**

In `tests/unit/keys_check_test.py`, just before `def test_a_sec_404_cannot_tell():`, add:

```python
def sharadar(status=200, rows=1, seen=None):
    """A fake Sharadar that answers every call with `status` and `rows` invented price rows."""
    row = {"date": "2026-10-05", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 10, "closeunadj": 1.0}

    def handler(request):
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, json={"count": rows, "data": [row] * rows})

    return httpx.MockTransport(handler)


def test_sharadar_end_to_end():
    seen = []
    assert check("SHARADAR_API_KEY", KEY, transport=sharadar(seen=seen), today=TODAY).verdict is Verdict.OK
    (request,) = seen
    # a ticker outside the free tier, which answers a wrong key as if it were right; about a year of it
    assert (request.url.params["ticker"], request.url.params["date.gte"]) == ("GOOGL", "2025-09-01")
    rejected = check("SHARADAR_API_KEY", KEY, transport=sharadar(403), today=TODAY)
    assert rejected.verdict is Verdict.REJECTED
    assert "free tier" in rejected.detail
    assert KEY not in rejected.detail
    assert check("SHARADAR_API_KEY", KEY, transport=sharadar(rows=0), today=TODAY).verdict is Verdict.UNKNOWN
```

- [ ] **Step 12: Run it to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/keys_check_test.py -q`
Expected: `2 failed, 18 passed`. The two failures are `test_sharadar_end_to_end` and `test_every_registry_key_has_a_check`: the registry has a key that nothing checks yet.

- [ ] **Step 13: Write the check**

In `src/data_pipeline/keys_check.py`, find:

```python
CHECK_TIMEOUT = 15.0  # seconds: someone is waiting for the verdict, unlike a sync
```

Replace with:

```python
CHECK_TIMEOUT = 15.0  # seconds: someone is waiting for the verdict, unlike a sync
# Sharadar answers a wrong key as its free tier, so its probe asks for a ticker that tier does
# not cover (verified 2026-10-12): an answer with rows means a paid key. If this ticker ever
# joins the free tier, a wrong key passes the check.
SHARADAR_PROBE = "GOOGL"
```

In `_probes`, find:

```python
            CatalogEntry("comtrade", "MEX", params={"annual_from": today.year - 2, "months": 0}),
        ),
    }
```

Replace with:

```python
            CatalogEntry("comtrade", "MEX", params={"annual_from": today.year - 2, "months": 0}),
        ),
        keys.SHARADAR: (
            "sharadar_prices",
            CatalogEntry("sharadar_prices", SHARADAR_PROBE, start=today - datetime.timedelta(days=PROBE_DAYS)),
        ),
    }
```

- [ ] **Step 14: Run everything**

Run: `.venv/Scripts/python -m pytest tests/unit/keys_check_test.py tests/unit/credentials_test.py tests/unit/keys_cli_test.py -q`
Expected: no failures.

Run: `.venv/Scripts/python -m pytest -q`
Expected: `895 passed, 14 skipped, 16 deselected, 22 warnings` (skips and warnings as before)

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 35 source files`

- [ ] **Step 15: Commit**

```bash
git add src/data_pipeline/credentials.py src/data_pipeline/keys_check.py src/data_pipeline/store/sources/__init__.py src/data_pipeline/store/sources/sharadar.py src/data_pipeline/store/sources/sharadar_prices.py tests/unit/credentials_test.py tests/unit/keys_cli_test.py tests/unit/keys_check_test.py tests/unit/store/sharadar_prices_test.py tests/unit/store/fixtures/sharadar_prices.json
git commit -m "feat(sources): Sharadar daily prices, with their key and its check"
```

---

### Task 2: Prices through the store

No production code changes here. These tests pin what the store already does with the rows of Task 1: a split stores every earlier day again, `as_of` reads what was held before, `status` and `show` work. If one fails, stop and report it: the design assumed the core needs no change.

**Files:**
- Modify: `tests/unit/store/sharadar_prices_test.py`

- [ ] **Step 1: Replace the imports**

In `tests/unit/store/sharadar_prices_test.py`, replace everything above the comment `# Every ticker, name and number here is invented` with:

```python
import datetime
import json
import math
import re

import click.testing
import httpx
import pandas as pd
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import cli as cli_module
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources import sharadar
from data_pipeline.store.sources.sharadar_prices import KEY_COLUMNS, VALUE_COLUMNS, SharadarPrices, read_rows

from .helpers import NOW, client, entry, fixture
```

- [ ] **Step 2: Append the tests**

At the end of the same file, add:

```python
# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


class Vendor:
    """A fake Sharadar whose answers the test can change between syncs: (table, ticker) -> rows."""

    def __init__(self):
        self.rows = {("stocks", "XTEST"): RECORDED}
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        table = request.url.path.rsplit("/", 1)[-1]
        return answer(self.rows.get((table, request.url.params["ticker"]), []))


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_prices\n  ids: [XTEST]\n", encoding="utf-8")
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


def split_three_for_one():
    """The recorded answer after a later 3-for-1 split, with one new day: every earlier day comes
    restated, and its close as it traded does not change."""
    restated = [
        {**row, **{column: row[column] / 3 for column in ("open", "high", "low", "close")}, "volume": row["volume"] * 3}
        for row in RECORDED
    ]
    return [price("2026-06-30", close=7.0), *restated]


def test_a_first_sync_stores_the_whole_history_and_a_second_adds_nothing(store, vendor):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (4, 0)
    table = store.table("sharadar_prices", "XTEST")
    assert list(table["period"]) == ["2026-02-27", "2026-03-02", "2026-03-03", "2026-03-04"]
    assert list(table["closeunadj"]) == [39.0, 40.0, 20.4, 20.8]
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert vendor.calls == 2  # one call per ticker per run, the whole history each time


def test_a_split_stores_every_earlier_day_again_and_as_of_reads_what_was_held_before(store, vendor, clock, tmp_path):
    vendor.rows[("stocks", "XTEST")] = split_three_for_one()
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 4)
    after = store.table("sharadar_prices", "XTEST", period="2026-03-04")
    assert (after["close"].iloc[0], after["closeunadj"].iloc[0]) == (pytest.approx(20.8 / 3), 20.8)
    before = store.table("sharadar_prices", "XTEST", period="2026-03-04", as_of=NOW)
    assert (before["close"].iloc[0], before["closeunadj"].iloc[0]) == (20.8, 20.8)
    stored = pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_prices" / "XTEST.parquet")
    assert len(stored) == 9  # four days twice, and the new one: nothing is deleted
    assert set(stored["fetched_at"]) == {pd.Timestamp(NOW), pd.Timestamp(LATER)}
    assert stored["published_at"].isna().all()  # dated by the fetch: Sharadar does not say when it restated


def test_as_of_before_the_first_sync_reads_no_prices(store):
    assert store.table("sharadar_prices", "XTEST", as_of=NOW - datetime.timedelta(days=1)).empty


def test_the_index_row_of_a_ticker(store):
    info = store.info("sharadar_prices:XTEST")
    assert (info.kind, info.name) == ("table", "Daily prices of XTEST (Sharadar stocks)")
    assert (info.frequency, info.last_period) == ("D", "2026-03-04")
    assert info.label == "[Sharadar: XTEST, 2026-03-04, fetched 2026-06-06]"
    row = store.index().iloc[0]
    assert json.loads(row["attrs"]) == {"table": "stocks"}
    assert row["stale_after_days"] == 10


def test_status_uses_the_ten_days_of_a_daily_series(store, clock):
    clock["now"] = datetime.datetime(2026, 3, 14, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"  # ten days after the last one
    clock["now"] = datetime.datetime(2026, 3, 15, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2026-03-04 (11 days ago)"


def test_a_delisted_ticker_is_not_stale_with_its_own_threshold(tmp_path, vendor, clock):
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    store = Store(
        tmp_path / "store",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    store.add("sharadar_prices", ["XTEST"], stale_after_days=100_000)
    store.sync()
    assert store.status().iloc[0]["state"] == "ok"  # 94 days after its last price


def test_show_prints_the_citation_and_the_newest_rows(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    result = click.testing.CliRunner().invoke(cli, ["show", "--root", str(tmp_path / "store"), "sharadar_prices:XTEST"])
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[Sharadar: XTEST, 2026-03-04, fetched 2026-06-06]"
    assert lines[1] == "Daily prices of XTEST (Sharadar stocks) | 4 rows"
    assert lines[2].split("  ")[:8] == ["frequency", "period", "open", "high", "low", "close", "volume", "closeunadj"]
```

- [ ] **Step 3: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_prices_test.py -q`
Expected: `32 passed`

Run: `.venv/Scripts/python -m ruff check tests`
Expected: `All checks passed!`

- [ ] **Step 4: Commit**

```bash
git add tests/unit/store/sharadar_prices_test.py
git commit -m "test(sharadar): prices through the store: a split, as_of, status and show"
```

---

### Task 3: The actions source

**Files:**
- Create: `tests/unit/store/fixtures/sharadar_actions.json`
- Create: `tests/unit/store/sharadar_actions_test.py`
- Create: `src/data_pipeline/store/sources/sharadar_actions.py`
- Modify: `src/data_pipeline/store/sources/__init__.py`

- [ ] **Step 1: Write the fixture**

Create `tests/unit/store/fixtures/sharadar_actions.json`, by hand, in the shape of an answer of the `actions` table: a split, two dividends on one day, and a spin-off whose value Sharadar leaves as `N/A`.

```json
{
  "count": 4,
  "data": [
    {"date": "2026-03-03", "action": "split", "ticker": "XTEST", "name": "TEST CORP", "value": 2, "contraticker": "N/A", "contraname": "N/A"},
    {"date": "2026-02-10", "action": "dividend", "ticker": "XTEST", "name": "TEST CORP", "value": 0.5, "contraticker": "N/A", "contraname": "N/A"},
    {"date": "2026-02-10", "action": "dividend", "ticker": "XTEST", "name": "TEST CORP", "value": 0.1, "contraticker": "N/A", "contraname": "N/A"},
    {"date": "2025-06-30", "action": "spinoff", "ticker": "XTEST", "name": "TEST CORP", "value": "N/A", "contraticker": "XSPUN", "contraname": "SPUN CORP"}
  ]
}
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/store/sharadar_actions_test.py`:

```python
import json
import math

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError, UnknownSeriesError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sharadar_actions import (
    ATTRIBUTE_COLUMNS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SharadarActions,
    read_rows,
)

from .helpers import NOW, client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
RECORDED = json.loads(fixture("sharadar_actions.json"))["data"]


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_actions", **fields)


def action(day, word="dividend", value=0.25, contraticker="N/A", contraname="N/A"):
    return {
        "date": day,
        "action": word,
        "ticker": "XTEST",
        "name": "TEST CORP",
        "value": value,
        "contraticker": contraticker,
        "contraname": contraname,
    }


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarActions(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def keyed(rows):
    """{(day, action, contraticker, item): value}, the way the store tells the rows apart."""
    return {tuple(row[column] for column in KEY_COLUMNS): row["value"] for row in rows}


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarActions.kind is Kind.TABLE
    assert SharadarActions.daily_budget is None
    assert SharadarActions.requests_per_minute == 120
    assert SharadarActions.held_by == ()


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "funds"}, "unknown field\\(s\\) for source 'sharadar_actions': table"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarActions(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_an_action_row_keeps_what_sharadar_says_and_n_a_is_empty():
    rows = {row["action"]: row for row in read_rows(RECORDED) if row["action"] != "dividend"}
    split, spinoff = rows["split"], rows["spinoff"]
    assert set(split) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS}
    assert (split["day"], split["contraticker"], split["item"], split["value"]) == ("2026-03-03", "", "1", 2.0)
    assert (split["name"], split["contraname"]) == ("TEST CORP", "")
    assert (spinoff["contraticker"], spinoff["contraname"]) == ("XSPUN", "SPUN CORP")
    assert math.isnan(spinoff["value"])  # Sharadar's N/A is a missing value, never zero


def test_two_dividends_on_one_day_are_both_kept_and_numbered_by_value():
    expected = {
        ("2026-02-10", "dividend", "", "1"): 0.1,
        ("2026-02-10", "dividend", "", "2"): 0.5,
    }
    dividends = [row for row in RECORDED if row["action"] == "dividend"]
    assert keyed(read_rows(dividends)) == expected
    assert keyed(read_rows(dividends[::-1])) == expected  # whatever the order of the answer


def test_a_missing_value_is_numbered_last_among_its_likes():
    rows = read_rows([action("2026-02-10", value="N/A"), action("2026-02-10", value=0.3)])
    assert [(row["item"], row["value"] == 0.3) for row in rows] == [("1", True), ("2", False)]


def test_the_other_company_tells_two_actions_of_a_day_apart():
    rows = read_rows(
        [
            action("2025-06-30", "spinoff", 1.0, contraticker="XONE"),
            action("2025-06-30", "spinoff", 2.0, contraticker="XTWO"),
        ]
    )
    assert [(row["contraticker"], row["item"]) for row in rows] == [("XONE", "1"), ("XTWO", "1")]


# -- the call and how Sharadar refuses ---------------------------------------------------------


def test_a_call_asks_the_actions_table_for_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(RECORDED)

    (batch,) = fetch(handler, [Request(ticker())])
    (request,) = seen
    assert request.url.path == "/v1.0/data/actions"
    assert dict(request.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_actions:XTEST", "Corporate actions of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned, table.stale_after_days) == (ATTRIBUTE_COLUMNS, False, None)
    assert len(table.rows) == 4


def test_a_ticker_without_actions_is_an_empty_table_not_a_failure():
    (batch,) = fetch(lambda _request: answer([]), [Request(ticker())])
    assert batch.failures == ()
    assert batch.tables[0].rows == ()


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker())], Credentials())
    assert batch.failures[0].outcome is Outcome.KEY_ERROR
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left():
    (batch,) = fetch(lambda _request: httpx.Response(403), [Request(ticker()), Request(ticker("XNEXT"))])
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "the table 'actions'" in batch.failures[0].reason


def test_an_action_without_a_day_is_a_source_error():
    (batch,) = fetch(lambda _request: answer([{"action": "dividend", "value": 1.0}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason.startswith("unexpected answer (KeyError")


# -- through the store -------------------------------------------------------------------------


class Vendor:
    """A fake Sharadar whose actions the test can change between syncs: ticker -> rows."""

    def __init__(self):
        self.rows = {"XTEST": RECORDED}

    def __call__(self, request):
        return answer(self.rows.get(request.url.params["ticker"], []))


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def store(tmp_path, vendor):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_actions\n  ids: [XTEST, XQUIET]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: NOW,
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def test_a_sync_stores_the_actions_and_a_ticker_without_any_is_not_a_failure(store):
    report = store.report.sources[0]
    assert store.report.exit_code == 0
    assert (report.ok, report.failed, report.new) == (2, (), 4)
    assert store.info("sharadar_actions:XTEST").last_period == "2026-03-03"
    assert store.info("sharadar_actions:XQUIET").status == "ok"
    # an empty table writes no file, as in every table source: reading it says so
    with pytest.raises(UnknownSeriesError, match="no stored table of source 'sharadar_actions' has id 'XQUIET'"):
        store.table("sharadar_actions", "XQUIET")


def test_a_table_that_is_not_versioned_keeps_its_attribute_columns(store):
    # the first table in the store that has attributes and no versions dated by its source
    dividends = store.table("sharadar_actions", "XTEST", action="dividend")
    assert list(dividends["value"]) == [0.1, 0.5]
    assert list(dividends["item"]) == ["1", "2"]
    assert set(dividends["name"]) == {"TEST CORP"}
    spinoff = store.table("sharadar_actions", "XTEST", contraticker="XSPUN")
    assert list(spinoff["contraname"]) == ["SPUN CORP"]


def test_a_second_sync_adds_nothing_and_a_new_action_adds_one_row(store, vendor):
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    vendor.rows["XTEST"] = [action("2026-05-12", value=0.5), *RECORDED]
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 0)
    assert store.info("sharadar_actions:XTEST").last_period == "2026-05-12"


def test_status_never_calls_the_actions_stale(store):
    status = store.status()
    states = dict(zip(status["key"], status["state"], strict=True))
    assert states["sharadar_actions:XTEST"] == "ok"  # no threshold: actions come when they come
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_actions_test.py -q`
Expected: an error while collecting, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.sharadar_actions'`

- [ ] **Step 4: Write the source**

Create `src/data_pipeline/store/sources/sharadar_actions.py`:

```python
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
```

- [ ] **Step 5: Register the source**

In `src/data_pipeline/store/sources/__init__.py`, just before the line `from data_pipeline.store.sources.sharadar_prices import SharadarPrices`, add:

```python
from data_pipeline.store.sources.sharadar_actions import SharadarActions
```

In `REGISTRY`, just before the line `    "sharadar_prices": SharadarPrices,`, add:

```python
    "sharadar_actions": SharadarActions,
```

In `TITLES`, just before the line `    "sharadar_prices": "Sharadar",`, add:

```python
    "sharadar_actions": "Sharadar",
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sharadar_actions_test.py -q`
Expected: `16 passed`

Run: `.venv/Scripts/python -m pytest tests/unit/store/base_test.py -q`
Expected: `24 passed` (one of them builds every registered source without credentials and reads its `held_by`)

- [ ] **Step 7: Commit**

```bash
git add src/data_pipeline/store/sources/__init__.py src/data_pipeline/store/sources/sharadar_actions.py tests/unit/store/sharadar_actions_test.py tests/unit/store/fixtures/sharadar_actions.json
git commit -m "feat(sources): Sharadar corporate actions"
```

---

### Task 4: A table named like a Windows device

`CON` and `PRN` are tickers, and device names on Windows. On Windows 11 (build 26200, tested 2026-10-12) `CON.parquet` is an ordinary file, so `Storage.table_path` is left alone and this test keeps the fact. If it fails on another system, report it and stop: escaping the name is a change to the core that this plan does not make.

**Files:**
- Modify: `tests/unit/store/tables_test.py`

- [ ] **Step 1: Write the test**

In `tests/unit/store/tables_test.py`, just before `def test_the_schema_of_a_source_without_tables_is_an_error(tmp_path):`, add:

```python
def test_a_table_named_like_a_windows_device_survives_a_round_trip(tmp_path):
    # CON and PRN are tickers. Windows 11 writes and reads `CON.parquet` (build 26200); an older
    # Windows keeps the name for a device, and this test is how one finds out.
    storage = Storage(tmp_path)
    storage.prepare()
    built = frame([row()])
    for name in ("CON", "PRN"):
        storage.write_table("sharadar_prices", name, built, SCHEMA)
        pd.testing.assert_frame_equal(storage.read_table("sharadar_prices", name), built)
    assert storage.table_names("sharadar_prices") == ["CON", "PRN"]
```

- [ ] **Step 2: Run it**

Run: `.venv/Scripts/python -m pytest tests/unit/store/tables_test.py -q -k windows_device`
Expected: `1 passed, 13 deselected`

- [ ] **Step 3: Commit**

```bash
git add tests/unit/store/tables_test.py
git commit -m "test(storage): a table named like a Windows device survives a round trip"
```

---

### Task 5: The live test

**Files:**
- Create: `tests/live/sharadar_live_test.py`

- [ ] **Step 1: Write the test**

Create `tests/live/sharadar_live_test.py`:

```python
"""Four real calls to Sharadar. Off by default; run with:  pytest -m live tests/live

Needs SHARADAR_API_KEY in the environment or in ./.env, a plan with prices and actions, and
network access. It exists to notice when Sharadar changes the shape of its answers, and it
settles what the free tier does not show: the `funds` table and a split among the actions.

It checks shapes only, never a value: Sharadar's license forbids publishing its data.
"""

import datetime

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store
from data_pipeline.store.sources.sharadar_prices import VALUE_COLUMNS

DEFAULT_WINDOW_YEARS = 5  # what a call without a first day is answered with


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
    assert sum(source.calls for source in report.sources) == 4
    today = datetime.datetime.now(datetime.UTC).date()
    for ticker in ("AAPL", "SPY"):  # a stock, and an ETF from the table the free tier does not show
        prices = store.table("sharadar_prices", ticker)
        assert prices["period"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()
        assert not prices.duplicated(["period"]).any()
        assert prices[list(VALUE_COLUMNS)].notna().any().all()  # every column is there and holds numbers
        first = prices["date"].min().date()
        assert first < today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))  # the whole history came
    actions = store.table("sharadar_actions", "AAPL")
    assert {"dividend", "split"} <= set(actions["action"])  # the words the README recipe reads
    assert (actions[actions["action"].isin(["dividend", "split"])]["value"] > 0).all()
    assert not actions.duplicated(["day", "action", "contraticker", "item"]).any()
```

- [ ] **Step 2: Run it without a key**

Run: `.venv/Scripts/python -m pytest -m live tests/live/sharadar_live_test.py -q`
Expected: `1 skipped` (`SHARADAR_API_KEY is not set`). With a key it makes four real calls: leave that to Task 8.

- [ ] **Step 3: Commit**

```bash
git add tests/live/sharadar_live_test.py
git commit -m "test(live): one real run against Sharadar, shapes only"
```

---

### Task 6: README and changelog

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: The list at the top of the README**

In `README.md`, find:

```markdown
  Each version of a fact is dated with the day it was filed.
- **Documents:**
```

Replace with:

```markdown
  Each version of a fact is dated with the day it was filed.
- **Equity prices:** daily prices and corporate actions of US stocks and ETFs from Sharadar, with
  your own subscription. See [Equity prices](#equity-prices).
- **Documents:**
```

- [ ] **Step 2: The table of keys**

In the table of keys, after the row of `SEC_EDGAR_UA`, add:

```markdown
| `SHARADAR_API_KEY` | Sharadar | US stock and ETF prices and corporate actions; a paid subscription |
```

- [ ] **Step 3: The Python example**

After the line `store.table("sec_xbrl", "AAPL", concept="Assets", as_of="2024-03-01")  # as filed by that day`, add:

```python
store.table("sharadar_prices", "AAPL")           # one row per day: open, high, low, close, volume
```

- [ ] **Step 4: The section on equity prices**

Just before the heading `## Data terms`, add this section:

````markdown
## Equity prices

`sharadar_prices` and `sharadar_actions` store the daily prices and the corporate actions of US
stocks and ETFs from [Sharadar](https://sharadar.com), with your own subscription. A catalog
lists the tickers:

```yaml
- source: sharadar_prices
  ids: [AAPL, MSFT, BRK.B]
  attrs: {commercial_ok: "no"}
- source: sharadar_prices
  table: funds            # ETFs are in Sharadar's `funds` table; stocks, the default, in `stocks`
  ids: [SPY, QQQ]
  attrs: {commercial_ok: "no"}
- source: sharadar_actions
  ids: [AAPL, MSFT, BRK.B, SPY, QQQ]
  attrs: {commercial_ok: "no"}
```

Each run asks for the whole history of each ticker, two calls per ticker, and stores what
changed. A price row holds `open`, `high`, `low`, `close` and `volume` adjusted for splits, as
Sharadar sends them, and `closeunadj`, the close as it traded. When a ticker splits, Sharadar
restates the adjusted columns of every earlier day: the next sync stores those days again and
keeps the rows it had, so `as_of` still reads what the store held before. `closeunadj` never
changes. Sharadar does not say when it restated a row, so prices read as of a date from your
first sync on.

An action row holds the day, Sharadar's word for the action (`dividend`, `split`, ...), its
value, and the other company it involves, if any. A ticker without actions has no table:
`store.table("sharadar_actions", ticker)` raises `UnknownSeriesError` for it.

The store keeps no close adjusted for dividends: Sharadar restates it over the whole history on
every dividend. Work it out from the raw close and the actions:

```python
import pandas as pd

close = store.table("sharadar_prices", "AAPL").set_index("date")["closeunadj"]
actions = store.table("sharadar_actions", "AAPL")
factor = pd.Series(1.0, index=close.index)
for day, action, value in actions[["date", "action", "value"]].itertuples(index=False):
    before = close.index < day
    if not before.any():
        continue
    if action == "split":  # `value` shares after the split for each one before
        factor[before] /= value
    elif action == "dividend":  # paid to whoever held the share the day before
        factor[before] *= 1 - value / close[before].iloc[-1]
adjusted = close * factor
```

A ticker that is no longer listed never gets a new day: give its entry a large
`stale_after_days`, or `status` calls it stale.
````

- [ ] **Step 5: The data terms**

In the list under `## Data terms`, find:

```markdown
- `no`: FRED.
```

Replace with:

```markdown
- `no`: FRED, Sharadar.
```

At the end of that section, after the paragraph about FRED and just before `## Development`, add:

```markdown
Sharadar's [Personal Use License](https://sharadar.com/terms) is for one person's own use. It
forbids sharing its data and publishing assessments of it, which is why this repository holds
none of either: the fixtures of its tests are invented. A professional or commercial use needs a
commercial license, which Sharadar sells through Nasdaq Data Link; that service has another API,
which this library does not read.
```

- [ ] **Step 6: The changelog**

In `CHANGELOG.md`, after the line `- Tables: UN Comtrade goods trade and SEC XBRL company facts, each version dated with its filing day.`, add:

```markdown
- Tables: daily prices and corporate actions of US stocks and ETFs from Sharadar (`sharadar_prices`,
  `sharadar_actions`), read with the user's own subscription. `SHARADAR_API_KEY` joins
  `data-pipeline setup`. The repository holds no Sharadar data: the fixtures are invented.
```

- [ ] **Step 7: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: Sharadar prices and actions in the README and the changelog"
```

---

### Task 7: The whole suite

- [ ] **Step 1: Run everything**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `919 passed, 14 skipped, 17 deselected, 22 warnings`

Run: `.venv/Scripts/python -m ruff check src tests`
Expected: `All checks passed!`

Run: `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `Success: no issues found in 36 source files`

- [ ] **Step 2: Check that nothing of Sharadar's went in**

Run: `git diff docs/sharadar-prices-design --stat`
Expected: the files of the table above and no other. Read the diff of the fixtures and of the README once more: every ticker in a fixture is invented, and no sentence measures or judges Sharadar's data.

- [ ] **Step 3: Tell the user**

Acceptance criterion 1 of the spec is met. Criteria 2 to 6 need the user's paid key: stop here and ask before Task 8.

---

### Task 8: Acceptance with a paid key (only after the user says so)

Nothing in this task is committed. It reads Sharadar's data and prints it to the console for the user; the license forbids publishing it.

- [ ] **Step 1: The user saves the key**

The user runs `data-pipeline setup`, chooses `7` and pastes the key. Never ask for the key in the chat, and never type it into a command.
Expected: `[ok]  SHARADAR_API_KEY  works` (criterion 6, first half).

- [ ] **Step 2: An invented key is rejected**

Run: `.venv/Scripts/python -c "from data_pipeline.keys_check import check; print(check('SHARADAR_API_KEY', 'not-a-real-key').line())"`
Expected: a line that starts with `[x]` and says Sharadar treated the request as its free tier (criterion 6, second half).

- [ ] **Step 3: The live test**

Run: `.venv/Scripts/python -m pytest -m live tests/live/sharadar_live_test.py -q`
Expected: `2 passed` (criterion 2; five real calls). If the assertion on `{"dividend", "split"}` fails, Sharadar names a split another way: read the words the script of Step 4 prints, then correct the README recipe, the live test and the spec's assumptions.

- [ ] **Step 4: The acceptance script**

Save this file outside the repository (the session's scratch folder), and run it from the root of the repository: `.venv/Scripts/python <path>/acceptance_sharadar.py`. Its first argument, optional, is a delisted ticker (`TWTR` by default; if Sharadar has no prices for it, pick another delisted company).

```python
"""Acceptance run of the Sharadar sources against the real API. NOT part of the repository.

Run it from the root of the repository, once SHARADAR_API_KEY is saved by `data-pipeline setup`:

    .venv/Scripts/python <path to this file> [DELISTED_TICKER]

It prints to the console and writes only inside a temporary folder. Sharadar's license forbids
publishing its data: read the output, tell the user whether each criterion passed, and commit
nothing of it.
"""

import pathlib
import sys
import tempfile

import httpx
import pandas as pd

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store

STOCKS = ["AAPL", "MSFT", "GOOGL", "BRK.B"]
FUNDS = ["SPY", "QQQ"]
DELISTED = sys.argv[1] if len(sys.argv) > 1 else "TWTR"
TOLERANCE = 0.002
URL = "https://api.sharadar.com/v1.0/data/stocks"


def adjusted_close(store: Store, ticker: str) -> pd.Series:
    """The recipe of the README, word for word."""
    close = store.table("sharadar_prices", ticker).set_index("date")["closeunadj"]
    actions = store.table("sharadar_actions", ticker, withdrawn=0)
    factor = pd.Series(1.0, index=close.index)
    for day, action, value in actions[["date", "action", "value"]].itertuples(index=False):
        before = close.index < day
        if pd.isna(value) or not before.any():
            continue
        if action == "split":  # `value` shares after the split for each one before
            factor[before] /= value
        elif action == "dividend":  # paid to whoever held the share the day before
            factor[before] *= 1 - value / close[before].iloc[-1]
    return close * factor


def show(report) -> None:
    for source in report.sources:
        print(f"  {source.source}: ok {source.ok}, new {source.new}, revised {source.revised}, calls {source.calls}")
        for key, reason in source.failed:
            print(f"    FAILED {key}: {reason}")


def main() -> int:
    key = keys.resolve().get(keys.SHARADAR)
    if key is None:
        print("SHARADAR_API_KEY is not set: run data-pipeline setup")
        return 2
    with tempfile.TemporaryDirectory() as folder:
        store = Store(pathlib.Path(folder) / "store")
        store.add("sharadar_prices", STOCKS)
        store.add("sharadar_prices", [DELISTED], stale_after_days=100_000)
        store.add("sharadar_prices", FUNDS, table="funds")
        store.add("sharadar_actions", [*STOCKS, DELISTED, *FUNDS])

        first = store.sync()
        print(f"criterion 3, first sync: exit code {first.exit_code}")
        show(first)
        second = store.sync()
        added = sum(source.new + source.revised for source in second.sources)
        print(f"criterion 3, second sync: exit code {second.exit_code}, rows added {added}")
        print("criterion 3:", "PASS" if (first.exit_code, second.exit_code, added) == (0, 0, 0) else "FAIL")

        aapl = store.table("sharadar_prices", "AAPL")
        print("criterion 4: compare with the probe notes of 2026-10-12 (kept outside the repository)")
        print(f"  first day stored: {aapl['period'].min()}, rows: {len(aapl)}")
        around = aapl[aapl["period"].isin(["2020-08-28", "2020-08-31"])]
        print(around[["period", "close", "closeunadj"]].to_string(index=False))

        answer = httpx.get(
            URL,
            params={"ticker": "AAPL", "date.gte": "1900-01-01", "format": "json", "api_key": key},
            timeout=90,
        )
        theirs = pd.Series({pd.Timestamp(row["date"]): row["closeadj"] for row in answer.json()["data"]})
        ours = adjusted_close(store, "AAPL")
        gap = (ours - theirs.reindex(ours.index)).abs()
        print(f"criterion 5: largest gap {gap.max():.4f}, on {gap.idxmax().date()}, over {len(gap)} days")
        print("criterion 5:", "PASS" if gap.max() <= TOLERANCE else "FAIL")

        actions = store.table("sharadar_actions", "AAPL")
        print("the words Sharadar uses for AAPL's actions:", sorted(set(actions["action"])))
        repeated = actions.duplicated(["period", "action", "contraticker"], keep=False)
        print(f"actions of AAPL that share day, action and contraticker: {int(repeated.sum())}")
        print("columns of a funds table:", list(store.table("sharadar_prices", "SPY").columns))
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Expected:
- `criterion 3: PASS`: both syncs exit 0 and the second adds no rows.
- Criterion 4: the first day and the two rows printed agree with `reports/Sharadar pruebas con key publica.md`.
- `criterion 5: PASS`: the README recipe agrees with Sharadar's `closeadj` to within 0.002 on every day.

- [ ] **Step 5: If criterion 5 fails**

It is the one most likely to fail: the recipe was written before a split was seen in the actions table. Look at the day of the largest gap and at the actions near it, correct the recipe in the README (and in the script) or take it out, and say what was found to the user in the chat. Storing the data is not blocked.

- [ ] **Step 6: Write back what the shapes turned out to be**

In the spec, under "Assumptions the live test settles", turn each assumption into what was seen, in terms of shape only: the columns of `funds`, the word for a split and what its `value` holds, whether two rows can share day, action and contraticker, whether an unknown ticker answers with no rows. No values, no judgments. Commit that with `docs(specs): what a paid key showed of Sharadar's answers`.

- [ ] **Step 7: Look for withdrawals a few weeks later**

Nobody has measured how often Sharadar corrects or withdraws an action. After the store has synced for a few weeks, count the marks: `store.table("sharadar_actions", "<TICKER>", withdrawn=1)` for each ticker of the catalog (a ticker without actions raises `UnknownSeriesError`: skip it). Tell the user the count; it says whether the marking earns its keep.

- [ ] **Step 8: Report**

Tell the user, criterion by criterion, what passed. Paste no Sharadar value into anything that is committed.

---

## After the plan

What was built differs from Tasks 1 to 7 above. The spec was brought up to date and is the
reference; the code blocks above are not.

Decided by the owner during the reviews:

- Prices have no `frequency` column: `KEY_COLUMNS = ("period",)`. With it, a sync built the set
  of days held for every ticker on every run, for a source that never reads it. Task 2's tests
  were adapted (the index row has no frequency; `show` starts with `period`).
- An action Sharadar no longer sends is marked as withdrawn. The actions table has the keys
  `frequency, period, action, contraticker, item` (`period` was `day`), the values `value,
  withdrawn`, `held_by = ("action", "contraticker", "item")` and a staleness threshold that
  never triggers. A stored action is marked only from the answer's earliest day on.
- A ticker without actions stays as it is: no table, `missing` in `status`. Documented.

Asked for by the reviewers:

- A refusal (401 or 403) fails the entries left of its table, not of the whole source; the
  prices source asks `stocks` first, then `funds`.
- `check_ticker` uses `fullmatch`. An answer without a list of rows in `data` reports an
  excerpt of what Sharadar said. A not-found reason names the first day asked for.
- An action row must carry `value` and `contraticker`, and an `action` that is a word. `item`
  is numbered by value, then by the names.
- `SourceReport.lines` in `sync.py` leaves a space after a source name of 12 characters or
  more: the one line of the core this branch changes.
- The live test became two tests and five calls: it also reads the actions of an ETF and asks
  for a ticker that does not exist.
- The README recipe reads the actions with `withdrawn=0` and skips a missing value.
