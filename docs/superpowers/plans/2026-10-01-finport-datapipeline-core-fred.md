# finport-datapipeline: store core + FRED Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the series engine of the public-data store (`data_pipeline.store`) and prove it with one source, FRED: declare series in a catalog, sync them (full history first, incremental afterwards, revisions kept), and read them back with provenance.

**Architecture:** A new top-level package `src/data_pipeline/` that lives beside the existing `src/data_pipeline/equity/` and shares nothing with it. Sources download and never touch the disk; `storage` writes parquet and never touches the network; `sync` is the only module that knows both. Observations are append-only, so a changed value adds a row and "as of a date" reads are possible. The engine is a port of `C:/Proyectos/Investment_Process/investment_tools/macro/publicos/` with identifiers translated to English.

**Tech Stack:** Python 3.12+, httpx, pandas + pyarrow (parquet), PyYAML, python-dotenv, click; pytest, hypothesis, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-10-01-finport-datapipeline-design.md`

---

## Before you start

**How this plan was checked.** Every source and test file in this plan was run together in a scratch copy on 2026-10-01 (Python 3.14.2, pandas 2.3.3, the repository's own ruff and mypy configuration): 149 tests passed, `ruff check` was clean and `mypy` was clean with the strict override of Task 1. Three things were *not* run and are marked where they appear: the live FRED test (Task 14), the wheel build (Task 14) and the acceptance run against real FRED (Task 16).

**Environment.** Windows, commands written for Git Bash. The virtual environment is `.venv`, created by `uv`; it has no `pip`, so packages are installed with `uv pip install --python .venv/Scripts/python.exe ...`.

| What | Command |
|---|---|
| One test file | `.venv/Scripts/python -m pytest tests/unit/store/model_test.py -q` |
| The whole suite | `.venv/Scripts/python -m pytest -q` |
| Lint | `.venv/Scripts/python -m ruff check .` |
| Types | `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy` |

`PYTHONIOENCODING=utf-8` matters for mypy: when mypy has an error to print about pandas, the pandas stubs include a Greek letter in a note and mypy crashes with `INTERNAL ERROR ... UnicodeEncodeError` on a Windows console. With the variable set it prints the real error.

**Lint rules of this repository that will bite.** `pyproject.toml` enables a strict ruff set. The code in this plan already follows it; keep to it if you change anything.

- Build an exception message in a variable first: `msg = f"..."` then `raise SomeError(msg)`.
- Exception class names end in `Error`.
- No boolean positional parameters: put booleans after a `*`.
- No `print`: the CLI uses `click.echo`.
- `pathlib`, never `os.path`; timezone-aware datetimes, never naive ones.
- `unittest.mock` is banned: tests use `monkeypatch` and `httpx.MockTransport`.
- Absolute imports in `src/`; lines up to 120 characters.

**Other rules.**

- Console output is ASCII only (Windows consoles default to cp1252).
- Do not modify anything under `src/data_pipeline/equity/`. The store and the equity engine must not import each other; Task 7 adds a test that enforces it.
- The Investment_Process folder is a read-only reference. Never import from it.
- Test files are named `*_test.py`, as in the rest of `tests/unit/`.

## File structure

| File | Responsibility |
|---|---|
| `src/data_pipeline/__init__.py` | Exports `Store` |
| `src/data_pipeline/store/errors.py` | Every exception the store raises on purpose |
| `src/data_pipeline/store/model.py` | Shared types: catalog entry, request, observation, series, failure, batch |
| `src/data_pipeline/store/periods.py` | Period text of any source to a canonical label and last day |
| `src/data_pipeline/store/http.py` | The one HTTP client: retries, pacing, secrets scrubbed |
| `src/data_pipeline/store/keys.py` | Credentials from the environment and `.env` |
| `src/data_pipeline/store/sources/base.py` | `Source` protocol, failure policy, number parsing |
| `src/data_pipeline/store/sources/fred.py` | The FRED source |
| `src/data_pipeline/store/sources/__init__.py` | Registry: name to class, and citation titles |
| `src/data_pipeline/store/catalog.py` | Loads and validates the YAML catalog |
| `src/data_pipeline/store/storage.py` | Parquet on disk, append-only merge, latest and as-of reads |
| `src/data_pipeline/store/sync.py` | Orchestrator: what to ask, lock, quotas, report |
| `src/data_pipeline/store/api.py` | The `Store` facade |
| `src/data_pipeline/store/cli.py` | `sync`, `status`, `show` |
| `src/data_pipeline/store/__main__.py` | `python -m data_pipeline.store` |
| `tests/unit/store/helpers.py` | Test client, entries, a fake source |
| `tests/unit/store/*_test.py` | One test file per module, plus properties and boundaries |
| `tests/live/fred_live_test.py` | One real call, off by default |
| `scripts/compare_fred_with_investment_process.py` | Acceptance check against Investment_Process's store |

---

### Task 1: Branch, dependencies and package skeleton

**Files:**
- Modify: `pyproject.toml`
- Create: `src/data_pipeline/__init__.py`
- Create: `src/data_pipeline/store/__init__.py`
- Create: `src/data_pipeline/store/errors.py`
- Create: `tests/unit/store/__init__.py`

- [ ] **Step 1: Create the branch**

The spec and this plan live on `docs/finport-datapipeline-design`; branch from it.

```bash
git switch -c feat/datapipeline-store-core docs/finport-datapipeline-design
```

- [ ] **Step 2: Add the dependencies and the checks to `pyproject.toml`**

Five edits. In `[project] dependencies`, after the `python-dotenv` line:

```toml
    "python-dotenv>=1.0.1",
    "pyyaml>=6.0",    # the catalog of the public-data store (data_pipeline.store)
```

In `[dependency-groups]`, the `test` group gains hypothesis:

```toml
test = [
    "hypothesis>=6.100",
    "pytest>=8.3.3",
```

and the `typing` group gains the YAML stubs:

```toml
typing = [
    "mypy>=1.13",
    "pandas-stubs>=2.2.3",
    "types-PyYAML>=6.0",
]
```

Immediately before the existing `[[tool.mypy.overrides]]` block for `module = "pyarrow.*"`, add:

```toml
# The public-data store is new code: checked as `mypy --strict` would.
[[tool.mypy.overrides]]
module = "data_pipeline.*"
check_untyped_defs = true
disallow_any_generics = true
disallow_incomplete_defs = true
disallow_subclassing_any = true
disallow_untyped_calls = true
disallow_untyped_decorators = true
disallow_untyped_defs = true
extra_checks = true
no_implicit_reexport = true
strict_equality = true
warn_return_any = true
```

In `[tool.pytest.ini_options]`, replace the `addopts` line:

```toml
addopts = "-ra --strict-config --strict-markers -m 'not live'"
markers = [
    "live: calls the real API of a source; needs its key and network (run with: pytest -m live tests/live)",
]
```

- [ ] **Step 3: Install the new packages into the virtual environment**

```bash
uv pip install --python .venv/Scripts/python.exe "pyyaml>=6.0" "hypothesis>=6.100" "types-PyYAML>=6.0"
```

Run: `.venv/Scripts/python -c "import yaml, hypothesis; print('[ok]')"`
Expected: `[ok]`

- [ ] **Step 4: Create the package skeleton**

Create `src/data_pipeline/__init__.py` (Task 12 replaces it with the `Store` export):

```python
"""finport-datapipeline: local store of public economic and financial data."""
```

Create `src/data_pipeline/store/__init__.py`:

```python
"""The public-data store: sources, storage, sync and the Store facade."""
```

Create `src/data_pipeline/store/errors.py`:

```python
"""Exceptions raised by the store."""


class StoreError(Exception):
    """Base class of every error the store raises on purpose."""


class CatalogError(StoreError):
    """The catalog is invalid: the message names the entry and the field."""


class KeyRejectedError(StoreError):
    """The source rejected (or lacks) its credential: the rest of its entries are skipped."""


class LockHeldError(StoreError):
    """Another sync holds the lock of this store."""


class NetworkError(StoreError):
    """The source did not answer after every retry. The message is already scrubbed."""


class PeriodError(StoreError):
    """The source's period text does not match the frequency."""


class QuotaExhaustedError(StoreError):
    """The source reported that its quota is used up."""


class UnknownSeriesError(StoreError):
    """No stored series has this key or alias."""
```

Create `tests/unit/store/__init__.py` as an empty file.

- [ ] **Step 5: Verify nothing broke**

Run: `PYTHONPATH=src .venv/Scripts/python -c "import data_pipeline.store.errors; print('[ok]')"`
Expected: `[ok]`

Run: `.venv/Scripts/python -m pytest -q`
Expected: `951 passed` (the suite as it was; the count grows with each task)

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/data_pipeline tests/unit/store
git commit -m "build(store): add data_pipeline package skeleton and its dependencies"
```

### Task 2: Shared types

Frozen dataclasses and enums every other module uses. Port of `modelo.py`, plus `Kind`, `Request`, `published_at`, `seasonal_adjustment` and the `QUOTA_EXHAUSTED` outcome. A series key is `<source>:<native id>`. The frequency is not part of a catalog entry: the source reports it.

**Files:**
- Create: `src/data_pipeline/store/model.py`
- Test: `tests/unit/store/model_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/model_test.py`:

```python
from data_pipeline.store.model import CatalogEntry, Frequency, stale_after


def test_key_joins_source_and_native_id():
    assert CatalogEntry(source="fred", source_id="UNRATE").key == "fred:UNRATE"


def test_stale_after_uses_the_frequency_table():
    assert stale_after(Frequency.MONTHLY) == 124
    assert stale_after(Frequency.DAILY) == 10


def test_stale_after_prefers_the_entry_override():
    assert stale_after(Frequency.MONTHLY, 35) == 35
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/model_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.model'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/model.py`:

```python
"""Types shared by the store: catalog entries, requests, downloaded series, failures."""

import dataclasses
import datetime
import enum
from collections.abc import Mapping


class Kind(enum.StrEnum):
    SERIES = "series"
    TABLE = "table"
    DOCUMENT = "document"


class Frequency(enum.StrEnum):
    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    ANNUAL = "A"


class Outcome(enum.StrEnum):
    OK = "ok"
    NOT_FOUND = "not_found"
    KEY_ERROR = "key_error"
    NETWORK_ERROR = "network_error"
    SOURCE_ERROR = "source_error"
    QUOTA_EXHAUSTED = "quota_exhausted"


# A series is stale when its last real period ended more than this many days ago.
STALE_AFTER_DAYS: Mapping[Frequency, int] = {
    Frequency.DAILY: 10,
    Frequency.WEEKLY: 28,
    Frequency.MONTHLY: 124,
    Frequency.QUARTERLY: 183,
    Frequency.ANNUAL: 730,
}


def stale_after(frequency: Frequency, override: int | None = None) -> int:
    """Days after which a series is stale: the entry's own threshold, else the frequency's."""
    return override if override is not None else STALE_AFTER_DAYS[frequency]


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One series the user wants. `params` holds the fields only its source understands."""

    source: str
    source_id: str
    alias: str | None = None
    start: datetime.date | None = None
    stale_after_days: int | None = None
    attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)
    params: Mapping[str, object] = dataclasses.field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.source}:{self.source_id}"


@dataclasses.dataclass(frozen=True, slots=True)
class Request:
    """A catalog entry plus the first date to ask for. `since=None` means full history."""

    entry: CatalogEntry
    since: datetime.date | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class Observation:
    period: str  # canonical label: "2026-09-22", "2026-08", "2026Q2", "2026"
    date: datetime.date  # last day of the period
    value: float  # NaN when the source lists the period without a value
    projection: bool = False
    published_at: datetime.datetime | None = None  # when the source published it, if it says so


@dataclasses.dataclass(frozen=True, slots=True)
class SeriesData:
    entry: CatalogEntry
    key: str
    name: str
    frequency: Frequency
    units: str = ""
    seasonal_adjustment: str = ""
    country: str = ""
    observations: tuple[Observation, ...] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class Failure:
    entry: CatalogEntry
    outcome: Outcome
    reason: str


@dataclasses.dataclass(frozen=True, slots=True)
class FetchBatch:
    series: tuple[SeriesData, ...] = ()
    failures: tuple[Failure, ...] = ()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/model_test.py -q`
Expected: `3 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/model.py tests/unit/store/model_test.py
git commit -m "feat(store): add the shared model types"
```

### Task 3: Period labels

Every source spells periods its own way. `read_period` turns the text into one canonical label (`2026-09-22`, `2026-08`, `2026Q2`, `2026`) and the last day of the period. Port of `periodos.py`.

**Files:**
- Create: `src/data_pipeline/store/periods.py`
- Test: `tests/unit/store/periods_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/periods_test.py`:

```python
import datetime

import pytest

from data_pipeline.store.errors import PeriodError
from data_pipeline.store.model import Frequency
from data_pipeline.store.periods import read_period


@pytest.mark.parametrize(
    ("text", "frequency", "label", "day"),
    [
        ("2026-09-22", Frequency.DAILY, "2026-09-22", datetime.date(2026, 9, 22)),
        ("22/09/2026", Frequency.DAILY, "2026-09-22", datetime.date(2026, 9, 22)),
        ("2026-09-18", Frequency.WEEKLY, "2026-09-18", datetime.date(2026, 9, 18)),
        ("2026-08-01", Frequency.MONTHLY, "2026-08", datetime.date(2026, 8, 31)),
        ("2026-M02", Frequency.MONTHLY, "2026-02", datetime.date(2026, 2, 28)),
        ("2024/02", Frequency.MONTHLY, "2024-02", datetime.date(2024, 2, 29)),
        ("2026-04-01", Frequency.QUARTERLY, "2026Q2", datetime.date(2026, 6, 30)),
        ("2026-Q4", Frequency.QUARTERLY, "2026Q4", datetime.date(2026, 12, 31)),
        ("2026-01-01", Frequency.ANNUAL, "2026", datetime.date(2026, 12, 31)),
        ("2026", Frequency.ANNUAL, "2026", datetime.date(2026, 12, 31)),
    ],
)
def test_reads_each_spelling(text, frequency, label, day):
    assert read_period(text, frequency) == (label, day)


@pytest.mark.parametrize(
    ("text", "frequency"),
    [
        ("2026-08", Frequency.DAILY),
        ("2026-13", Frequency.MONTHLY),
        ("soon", Frequency.QUARTERLY),
        ("26", Frequency.ANNUAL),
        ("2026-02-31", Frequency.MONTHLY),
    ],
)
def test_rejects_text_that_does_not_match_the_frequency(text, frequency):
    with pytest.raises(PeriodError, match=text):
        read_period(text, frequency)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/periods_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.periods'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/periods.py`:

```python
"""Period text of any source -> canonical period label + last day of the period.

Sources spell periods their own way (FRED "2026-08-01" for a month, Banxico "01/08/2026",
IMF "2026-M08", SDMX "2026-Q2"). The frequency says how to read the text.
"""

import calendar
import datetime
import re

from data_pipeline.store.errors import PeriodError
from data_pipeline.store.model import Frequency

_ISO_DAY = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_MX_DAY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_MONTH = re.compile(r"^(\d{4})[-/]?M?(\d{1,2})$")
_QUARTER = re.compile(r"^(\d{4})[-/]?Q?0?([1-4])$")
_YEAR = re.compile(r"^(\d{4})$")
_MONTHS_IN_YEAR = 12


def _day(text: str) -> datetime.date | None:
    iso = _ISO_DAY.match(text)
    if iso:
        return datetime.date(int(iso[1]), int(iso[2]), int(iso[3]))
    mx = _MX_DAY.match(text)
    if mx:
        return datetime.date(int(mx[3]), int(mx[2]), int(mx[1]))
    return None


def _month_end(year: int, month: int) -> datetime.date:
    return datetime.date(year, month, calendar.monthrange(year, month)[1])


def read_period(text: str, frequency: Frequency) -> tuple[str, datetime.date]:
    """Return (label, last day of the period) for a source's period text."""
    stripped = text.strip()
    try:
        day = _day(stripped)
    except ValueError as exc:
        msg = f"invalid date {text!r}"
        raise PeriodError(msg) from exc
    if frequency in (Frequency.DAILY, Frequency.WEEKLY):
        if day is None:
            msg = f"{text!r} is not a daily date"
            raise PeriodError(msg)
        return day.isoformat(), day
    if frequency is Frequency.MONTHLY:
        if day is not None:
            year, month = day.year, day.month
        else:
            found = _MONTH.match(stripped)
            if found is None:
                msg = f"{text!r} is not a month"
                raise PeriodError(msg)
            year, month = int(found[1]), int(found[2])
        if not 1 <= month <= _MONTHS_IN_YEAR:
            msg = f"month out of range in {text!r}"
            raise PeriodError(msg)
        return f"{year:04d}-{month:02d}", _month_end(year, month)
    if frequency is Frequency.QUARTERLY:
        if day is not None:
            year, quarter = day.year, (day.month - 1) // 3 + 1
        else:
            found = _QUARTER.match(stripped)
            if found is None:
                msg = f"{text!r} is not a quarter"
                raise PeriodError(msg)
            year, quarter = int(found[1]), int(found[2])
        return f"{year:04d}Q{quarter}", _month_end(year, 3 * quarter)
    if day is not None:
        year = day.year
    else:
        found = _YEAR.match(stripped)
        if found is None:
            msg = f"{text!r} is not a year"
            raise PeriodError(msg)
        year = int(found[1])
    return f"{year:04d}", datetime.date(year, 12, 31)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/periods_test.py -q`
Expected: `15 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/periods.py tests/unit/store/periods_test.py
git commit -m "feat(store): read period text into canonical labels"
```

### Task 4: The HTTP client

One client for every source: three retries on 429/5xx and transport errors (waiting 2, 4, 8 s), a minimum interval between calls to the same source, call counters, and every secret replaced by `***` in the messages it raises. The clock and the sleep function are injected so tests never wait. Port of `http.py`; the pacing rate now comes from the caller (`per_minute`) instead of a central table.

**Files:**
- Create: `src/data_pipeline/store/http.py`
- Test: `tests/unit/store/http_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/http_test.py`:

```python
import httpx
import pytest

from data_pipeline.store.errors import NetworkError
from data_pipeline.store.http import Client, scrub


def test_returns_a_non_retryable_answer_as_it_is():
    waits = []
    client = Client(transport=httpx.MockTransport(lambda _request: httpx.Response(400, text="bad")), sleep=waits.append)
    response = client.get("fred", "https://example.test/x")
    assert response.status_code == 400
    assert waits == []
    assert client.calls == {"fred": 1}


def test_retries_then_succeeds():
    answers = iter([httpx.Response(503), httpx.Response(429), httpx.Response(200, text="ok")])
    waits = []
    client = Client(
        transport=httpx.MockTransport(lambda _request: next(answers)),
        sleep=waits.append,
        clock=lambda: 0.0,
    )
    response = client.get("fred", "https://example.test/x", per_minute=6_000_000)
    assert response.text == "ok"
    assert [wait for wait in waits if wait >= 2.0] == [2.0, 4.0]
    assert client.calls == {"fred": 3}
    assert client.throttled == {"fred": 1}


def test_gives_up_after_every_retry_with_a_scrubbed_message():
    def handler(_request):
        msg = "no route to host for key SECRET123"
        raise httpx.ConnectError(msg)

    client = Client(secrets=("SECRET123",), transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    with pytest.raises(NetworkError, match="after 4 attempts") as raised:
        client.get("fred", "https://example.test/x")
    assert "SECRET123" not in str(raised.value)
    assert "***" in str(raised.value)


def test_waits_between_calls_to_the_same_source_only():
    now = [100.0]
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        now[0] += seconds

    client = Client(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
        sleep=sleep,
        clock=lambda: now[0],
    )
    client.get("fred", "https://example.test/x", per_minute=60)
    client.get("fred", "https://example.test/x", per_minute=60)
    client.get("bls", "https://example.test/x", per_minute=60)
    assert waits == [1.0]


def test_scrub_hides_every_secret_and_ignores_empty_ones():
    assert scrub("key=abc&token=xyz", ["abc", "", "xyz"]) == "key=***&token=***"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/http_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.http'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/http.py`:

```python
"""The one HTTP client of the store: per-source pacing, retries on 429/5xx and network errors
(waits 2, 4, 8 s), and secrets scrubbed from every message it raises.

Non-retryable answers (200, 400, 401, 404...) are returned as they are: each source reads its
own error format.
"""

import time
import types
from collections.abc import Callable, Mapping, Sequence

import httpx

from data_pipeline.store.errors import NetworkError

TIMEOUT = 90.0
RETRIES = 3
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
TOO_MANY_REQUESTS = 429
DEFAULT_PER_MINUTE = 30
USER_AGENT = "finport-datapipeline (public data store)"
HIDDEN = "***"


def scrub(text: str, secrets: Sequence[str]) -> str:
    """Replace every secret found in `text` with ***."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, HIDDEN)
    return text


class Client:
    def __init__(
        self,
        secrets: Sequence[str] = (),
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        retries: int = RETRIES,
    ) -> None:
        self._secrets = tuple(secrets)
        self._http = httpx.Client(
            transport=transport,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        self._sleep = sleep
        self._clock = clock
        self._retries = retries
        self._last: dict[str, float] = {}
        self.calls: dict[str, int] = {}
        self.throttled: dict[str, int] = {}

    def scrub(self, text: str) -> str:
        return scrub(text, self._secrets)

    def get(
        self,
        source: str,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        per_minute: int = DEFAULT_PER_MINUTE,
    ) -> httpx.Response:
        """GET with pacing and retries. Raises NetworkError when every attempt fails."""
        reason = ""
        for attempt in range(self._retries + 1):
            self._wait_turn(source, per_minute)
            self.calls[source] = self.calls.get(source, 0) + 1
            try:
                response = self._http.get(url, params=params, headers=headers)
            except httpx.TransportError as exc:
                reason = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code not in RETRY_STATUS:
                    return response
                if response.status_code == TOO_MANY_REQUESTS:
                    self.throttled[source] = self.throttled.get(source, 0) + 1
                reason = f"HTTP {response.status_code}"
            if attempt < self._retries:
                self._sleep(2.0 ** (attempt + 1))
        msg = self.scrub(f"{source}: {reason} after {self._retries + 1} attempts")
        raise NetworkError(msg) from None

    def _wait_turn(self, source: str, per_minute: int) -> None:
        interval = 60.0 / per_minute
        previous = self._last.get(source)
        now = self._clock()
        if previous is not None and now - previous < interval:
            self._sleep(interval - (now - previous))
            now = self._clock()
        self._last[source] = now

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.close()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/http_test.py -q`
Expected: `5 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/http.py tests/unit/store/http_test.py
git commit -m "feat(store): add the HTTP client with pacing, retries and scrubbing"
```

### Task 5: Credentials

Credentials come from the process environment first, then from a `.env` file. The variable names are the ones the user's other repositories already use. `repr` never shows a value. The SEC User-Agent is a credential but not a secret. Port of `llaves.py`.

**Files:**
- Create: `src/data_pipeline/store/keys.py`
- Test: `tests/unit/store/keys_test.py`
- Create: `tests/unit/store/conftest.py`

- [ ] **Step 0: Keep the developer's own keys out of the tests**

Create `tests/unit/store/conftest.py`. It imports `keys`, so it is created together with it; until Step 3 the whole folder fails to collect, which is the failure Step 2 expects.

```python
import pytest

from data_pipeline.store import keys


@pytest.fixture(autouse=True)
def no_real_credentials(monkeypatch):
    """Tests never see the developer's own keys."""
    for name in keys.NAMES:
        monkeypatch.delenv(name, raising=False)
```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/keys_test.py`:

```python
from data_pipeline.store import keys
from data_pipeline.store.keys import Credentials, load_credentials


def test_environment_wins_over_the_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("FRED_API_KEY=from_file\nBLS_API_KEY=bls_file\n", encoding="utf-8")
    credentials = load_credentials(env_file, environ={"FRED_API_KEY": "from_env"})
    assert credentials.get(keys.FRED) == "from_env"
    assert credentials.get(keys.BLS) == "bls_file"
    assert credentials.get(keys.BANXICO) is None


def test_a_missing_file_is_not_an_error(tmp_path):
    assert load_credentials(tmp_path / "absent.env", environ={}).values == {}


def test_blank_values_count_as_absent(tmp_path):
    credentials = load_credentials(tmp_path / "absent.env", environ={"FRED_API_KEY": "   "})
    assert credentials.get(keys.FRED) is None


def test_repr_never_shows_a_value():
    text = repr(Credentials({keys.FRED: "abc123"}))
    assert "abc123" not in text
    assert "FRED_API_KEY=yes" in text
    assert "BLS_API_KEY=no" in text


def test_the_sec_user_agent_is_not_a_secret():
    credentials = Credentials({keys.FRED: "abc123", keys.SEC_UA: "Name name@example.com"})
    assert credentials.secrets() == ("abc123",)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/keys_test.py -q`
Expected: collection error, `ImportError: cannot import name 'keys' from 'data_pipeline.store'` (raised by `conftest.py`)

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/keys.py`:

```python
"""Credentials of the public sources, from the process environment and a `.env` file.

A variable already set in the process environment wins over the file. Credentials are personal:
`repr` shows only which ones are present, never their values.
"""

import dataclasses
import os
import pathlib
from collections.abc import Mapping

import dotenv

ENV_FILE = pathlib.Path(".env")
FRED = "FRED_API_KEY"
BLS = "BLS_API_KEY"
BANXICO = "BANXICO_TOKEN"
INEGI = "INEGI_TOKEN"
COMTRADE = "COMTRADE_API_KEY"
SEC_UA = "SEC_EDGAR_UA"
NAMES = (FRED, BLS, BANXICO, INEGI, COMTRADE, SEC_UA)
NOT_SECRET = frozenset({SEC_UA})  # a User-Agent with an e-mail address, sent in clear by design


@dataclasses.dataclass(frozen=True, repr=False)
class Credentials:
    values: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def get(self, name: str) -> str | None:
        return self.values.get(name) or None

    def secrets(self) -> tuple[str, ...]:
        return tuple(value for name, value in self.values.items() if value and name not in NOT_SECRET)

    def __repr__(self) -> str:
        present = ", ".join(f"{name}={'yes' if self.get(name) else 'no'}" for name in NAMES)
        return f"Credentials({present})"


def load_credentials(
    env_file: pathlib.Path = ENV_FILE,
    environ: Mapping[str, str] | None = None,
) -> Credentials:
    """Read every known credential; the environment wins over `env_file`."""
    from_file = dotenv.dotenv_values(env_file) if env_file.exists() else {}
    env = os.environ if environ is None else environ
    values = {}
    for name in NAMES:
        value = (env.get(name) or from_file.get(name) or "").strip()
        if value:
            values[name] = value
    return Credentials(values)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/keys_test.py -q`
Expected: `5 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/keys.py tests/unit/store/keys_test.py tests/unit/store src/data_pipeline
git commit -m "feat(store): load credentials from the environment and .env"
```

### Task 6: The source contract and the failure policy

The `Source` protocol, and `per_request`, the runner that applies the failure policy for sources that download one series per call: a missing series is recorded and the run continues; a rejected key fails that request and every remaining one; a network error or an unexpected answer fails only that request. `fetch` yields batches so `sync` can store each one as it arrives. Port of `fuentes/base.py`.

**Files:**
- Create: `src/data_pipeline/store/sources/base.py`
- Test: `tests/unit/store/base_test.py`
- Create: `tests/unit/store/helpers.py`
- Create: `src/data_pipeline/store/sources/__init__.py`

- [ ] **Step 0: Create the test helpers**

Create `tests/unit/store/helpers.py`. Every later test file uses it: a client that never waits, a catalog-entry shortcut, a monthly-series shortcut and a fake source the sync tests drive.

```python
import datetime
import pathlib
from collections.abc import Callable, Iterator, Mapping, Sequence

import httpx

from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import reject_params

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
NOW = datetime.datetime(2026, 6, 6, 12, 0, tzinfo=datetime.UTC)
Handler = Callable[[httpx.Request], httpx.Response]


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def client(handler: Handler | None = None, secrets: tuple[str, ...] = ()) -> Client:
    """A client served by `handler`, with no real waits and no retries."""
    transport = httpx.MockTransport(handler) if handler else None
    return Client(secrets=secrets, transport=transport, sleep=lambda _seconds: None, retries=0)


def entry(source_id: str = "UNRATE", source: str = "fake", **fields) -> CatalogEntry:
    return CatalogEntry(source=source, source_id=source_id, **fields)


def monthly(catalog_entry: CatalogEntry, values: Mapping[str, float]) -> SeriesData:
    """A monthly series from {"2026-05": 4.1, ...}."""
    observations = tuple(
        Observation(*read_period(period, Frequency.MONTHLY), value) for period, value in values.items()
    )
    return SeriesData(
        entry=catalog_entry,
        key=catalog_entry.key,
        name=f"Name of {catalog_entry.source_id}",
        frequency=Frequency.MONTHLY,
        units="Percent",
        seasonal_adjustment="SA",
        observations=observations,
    )


class FakeSource:
    """A source that answers from a dictionary: source_id -> SeriesData or Failure.

    An id without an answer is not returned at all. `quota_after=n` raises QuotaExhaustedError
    before the request at position n. With a `client`, every request counts as one call.
    """

    kind = Kind.SERIES
    requests_per_minute = 6000

    def __init__(
        self,
        name: str = "fake",
        *,
        daily_budget: int | None = None,
        quota_after: int | None = None,
        client: Client | None = None,
    ) -> None:
        self.name = name
        self.daily_budget = daily_budget
        self.quota_after = quota_after
        self.answers: dict[str, SeriesData | Failure] = {}
        self.seen: list[Request] = []
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        for position, request in enumerate(requests):
            if self.quota_after is not None and position >= self.quota_after:
                msg = "quota used up"
                raise QuotaExhaustedError(msg)
            self.seen.append(request)
            if self._client is not None:
                self._client.calls[self.name] = self._client.calls.get(self.name, 0) + 1
            answer = self.answers.get(request.entry.source_id)
            if answer is None:
                continue
            if isinstance(answer, Failure):
                yield FetchBatch(failures=(answer,))
            else:
                yield FetchBatch(series=(answer,))
```

Create `src/data_pipeline/store/sources/__init__.py` with only a docstring for now (Task 7 replaces it with the registry):

```python
"""Registry of sources: name -> class, and the title each one is cited with."""
```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/base_test.py`:

```python
import math

import pytest

from data_pipeline.store.errors import CatalogError, KeyRejectedError, NetworkError
from data_pipeline.store.model import Failure, Outcome, Request
from data_pipeline.store.sources.base import number, per_request, reject_params

from .helpers import entry, monthly


@pytest.mark.parametrize("text", ["", ".", "N/E", "na", "NaN", "n/a", "-", None])
def test_missing_sentinels_become_nan(text):
    assert math.isnan(number(text))


@pytest.mark.parametrize(("value", "expected"), [("4.1", 4.1), ("1,234.5", 1234.5), (3, 3.0), (" 2 ", 2.0)])
def test_numbers_are_parsed(value, expected):
    assert number(value) == expected


def test_reject_params_names_the_entry_and_the_field():
    with pytest.raises(CatalogError, match=r"fake:UNRATE.*reporters"):
        reject_params(entry(params={"reporters": ["MEX"]}))


def run(download, ids=("A", "B", "C")):
    requests = [Request(entry(source_id)) for source_id in ids]
    return list(per_request(requests, download))


def test_one_batch_per_request():
    batches = run(lambda request: monthly(request.entry, {"2026-05": 1.0}))
    assert [batch.series[0].key for batch in batches] == ["fake:A", "fake:B", "fake:C"]


def test_a_returned_failure_is_recorded_and_the_run_continues():
    def download(request):
        if request.entry.source_id == "B":
            return Failure(request.entry, Outcome.NOT_FOUND, "does not exist")
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert [len(batch.series) for batch in batches] == [1, 0, 1]
    assert batches[1].failures[0].outcome is Outcome.NOT_FOUND


def test_a_network_error_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            msg = "fake: HTTP 503 after 4 attempts"
            raise NetworkError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.NETWORK_ERROR
    assert len(batches[1].series) == 1


def test_an_unexpected_answer_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            return {}["observations"]
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "KeyError" in batches[0].failures[0].reason
    assert len(batches[2].series) == 1


def test_a_rejected_key_skips_the_rest_of_the_source():
    def download(request):
        if request.entry.source_id == "B":
            msg = "the key was rejected"
            raise KeyRejectedError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert len(batches) == 2
    assert [failure.entry.key for failure in batches[1].failures] == ["fake:B", "fake:C"]
    assert {failure.outcome for failure in batches[1].failures} == {Outcome.KEY_ERROR}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/base_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.base'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/sources/base.py`:

```python
"""What every source shares: the `Source` protocol, the per-request runner and the parsing of
numbers with each source's missing-value sentinels.

Failure policy: a series that does not exist is recorded and skipped; a rejected credential
skips the rest of that source; a network failure or an unexpected answer fails only that
request; an exhausted quota stops the source. Nothing is filled in.
"""

import math
from collections.abc import Callable, Iterator, Sequence
from typing import Protocol

from data_pipeline.store.errors import CatalogError, KeyRejectedError, NetworkError
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Kind,
    Outcome,
    Request,
    SeriesData,
)

MISSING = frozenset({"", ".", "N/E", "NA", "NAN", "N/A", "-"})


class Source(Protocol):
    name: str
    kind: Kind
    requests_per_minute: int
    daily_budget: int | None  # None when the source has no daily quota

    def validate(self, entry: CatalogEntry) -> None:
        """Raise CatalogError when the entry carries a field this source does not accept."""
        ...

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        """Yield batches as they are downloaded. Raise QuotaExhaustedError to stop early."""
        ...


def number(value: object) -> float:
    """Source value -> float; the source's missing sentinels -> NaN (never 0)."""
    if value is None:
        return math.nan
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip()
    if text.upper() in MISSING:
        return math.nan
    return float(text.replace(",", ""))


def reject_params(entry: CatalogEntry) -> None:
    """For sources that accept no source-specific fields."""
    if entry.params:
        fields = ", ".join(sorted(entry.params))
        msg = f"{entry.key}: unknown field(s) for source {entry.source!r}: {fields}"
        raise CatalogError(msg)


def failures(requests: Sequence[Request], outcome: Outcome, reason: str) -> tuple[Failure, ...]:
    return tuple(Failure(request.entry, outcome, reason) for request in requests)


Download = Callable[[Request], SeriesData | Failure]


def per_request(requests: Sequence[Request], download: Download) -> Iterator[FetchBatch]:
    """Run `download` once per request and yield one batch per request."""
    for position, request in enumerate(requests):
        try:
            result = download(request)
        except KeyRejectedError as exc:
            yield FetchBatch(failures=failures(requests[position:], Outcome.KEY_ERROR, str(exc)))
            return
        except NetworkError as exc:
            yield FetchBatch(failures=(Failure(request.entry, Outcome.NETWORK_ERROR, str(exc)),))
            continue
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            reason = f"unexpected answer ({type(exc).__name__}: {exc})"
            yield FetchBatch(failures=(Failure(request.entry, Outcome.SOURCE_ERROR, reason),))
            continue
        if isinstance(result, Failure):
            yield FetchBatch(failures=(result,))
        else:
            yield FetchBatch(series=(result,))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/base_test.py -q`
Expected: `18 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/base.py tests/unit/store/base_test.py tests/unit/store src/data_pipeline
git commit -m "feat(store): add the source protocol and the per-request failure policy"
```

### Task 7: The FRED source, the registry and the boundary tests

FRED needs two calls per series: `/fred/series` for the name, units, frequency and seasonal adjustment, then `/fred/series/observations`. The frequency comes from FRED, not from the catalog. Port of `fuentes/fred.py`.

**Files:**
- Create: `src/data_pipeline/store/sources/fred.py`
- Modify: `src/data_pipeline/store/sources/__init__.py` (replace the docstring-only file)
- Create: `tests/unit/store/fixtures/fred_series.json`, `fred_observations.json`, `fred_not_found.json`, `fred_bad_key.json`
- Test: `tests/unit/store/fred_test.py`
- Test: `tests/unit/store/boundaries_test.py`

- [ ] **Step 1: Create the fixtures**

The two error answers are FRED's real ones, copied from `Investment_Process/tests/macro/publicos/fixtures/`. The two successful answers are trimmed to the fields the source reads. Each file is a single line.

`tests/unit/store/fixtures/fred_series.json`:

```json
{"seriess": [{"id": "UNRATE", "title": "Unemployment Rate", "frequency": "Monthly", "frequency_short": "M", "units": "Percent", "units_short": "%", "seasonal_adjustment": "Seasonally Adjusted", "seasonal_adjustment_short": "SA"}]}
```

`tests/unit/store/fixtures/fred_observations.json`:

```json
{"observations": [{"date": "2026-03-01", "value": "4.0"}, {"date": "2026-04-01", "value": "."}, {"date": "2026-05-01", "value": "4.1"}]}
```

`tests/unit/store/fixtures/fred_not_found.json`:

```json
{"error_code":400,"error_message":"Bad Request.  The series does not exist."}
```

`tests/unit/store/fixtures/fred_bad_key.json`:

```json
{"error_code":400,"error_message":"Bad Request.  The value for variable api_key is not registered.  Read https:\/\/fred.stlouisfed.org\/docs\/api\/api_key.html for more information."}
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/store/fred_test.py`:

```python
import datetime
import math

import httpx

from data_pipeline.store import keys
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.fred import Fred

from .helpers import client, entry, fixture

KEY = "0123456789abcdef0123456789abcdef"
WITH_KEY = Credentials({keys.FRED: KEY})


def serve(seen=None):
    """Answer /series and /series/observations from the fixtures."""

    def handler(request):
        if seen is not None:
            seen.append(request)
        name = "fred_observations.json" if request.url.path.endswith("/observations") else "fred_series.json"
        return httpx.Response(200, text=fixture(name))

    return handler


def fetch(handler, requests, credentials=WITH_KEY):
    source = Fred(client(handler, secrets=credentials.secrets()), credentials)
    return list(source.fetch(requests))


def test_downloads_metadata_and_observations():
    seen = []
    batches = fetch(serve(seen), [Request(entry("UNRATE", "fred"))])
    series = batches[0].series[0]
    assert series.key == "fred:UNRATE"
    assert series.name == "Unemployment Rate"
    assert series.units == "Percent"
    assert series.frequency is Frequency.MONTHLY
    assert series.seasonal_adjustment == "SA"
    assert [observation.period for observation in series.observations] == ["2026-03", "2026-04", "2026-05"]
    assert series.observations[0].date == datetime.date(2026, 3, 31)
    assert math.isnan(series.observations[1].value)
    assert series.observations[2].value == 4.1
    assert [request.url.path for request in seen] == ["/fred/series", "/fred/series/observations"]
    assert "observation_start" not in seen[1].url.params


def test_since_becomes_observation_start():
    seen = []
    fetch(serve(seen), [Request(entry("UNRATE", "fred"), datetime.date(2024, 5, 1))])
    assert seen[1].url.params["observation_start"] == "2024-05-01"


def test_an_unknown_series_is_not_found_and_the_next_one_still_runs():
    def handler(request):
        if request.url.params["series_id"] == "NOPE":
            return httpx.Response(400, text=fixture("fred_not_found.json"))
        return serve()(request)

    batches = fetch(handler, [Request(entry("NOPE", "fred")), Request(entry("UNRATE", "fred"))])
    assert batches[0].failures[0].outcome is Outcome.NOT_FOUND
    assert batches[1].series[0].key == "fred:UNRATE"


def test_a_rejected_key_fails_every_remaining_request_without_leaking_it():
    batches = fetch(
        lambda _request: httpx.Response(400, text=fixture("fred_bad_key.json")),
        [Request(entry("UNRATE", "fred")), Request(entry("DGS10", "fred"))],
    )
    failures = batches[0].failures
    assert [failure.outcome for failure in failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert KEY not in failures[0].reason


def test_without_a_key_nothing_is_requested():
    seen = []
    batches = fetch(serve(seen), [Request(entry("UNRATE", "fred"))], credentials=Credentials())
    assert seen == []
    assert batches[0].failures[0].outcome is Outcome.KEY_ERROR
    assert batches[0].failures[0].reason == "FRED_API_KEY missing in .env"


def test_an_unsupported_frequency_is_a_source_error():
    def handler(request):
        if request.url.path.endswith("/series"):
            return httpx.Response(200, json={"seriess": [{"title": "T", "frequency_short": "BW"}]})
        return serve()(request)

    batches = fetch(handler, [Request(entry("WEIRD", "fred"))])
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert batches[0].failures[0].reason == "unsupported frequency 'BW'"


def test_any_other_http_error_is_a_source_error():
    batches = fetch(lambda _request: httpx.Response(418, text="teapot"), [Request(entry("UNRATE", "fred"))])
    assert batches[0].failures[0].reason == "HTTP 418: teapot"
```

Create `tests/unit/store/boundaries_test.py`. It fails the build if a fixture ever contains something shaped like a key, or if the store and the equity engine start importing each other:

```python
"""Rules that keep the store honest: no recorded keys, and no imports across the two engines."""

import pathlib
import re

from .helpers import FIXTURES

ROOT = pathlib.Path(__file__).resolve().parents[3]
KEY_SHAPED = re.compile(r"\b[0-9a-f]{32}\b|\b[0-9a-f]{64}\b")
IMPORT = re.compile(r"^\s*(?:from|import)\s+([\w.]+)", re.MULTILINE)


def imports_of(folder):
    found = set()
    for path in sorted(folder.rglob("*.py")):
        found.update(IMPORT.findall(path.read_text(encoding="utf-8")))
    return found


def test_fixtures_hold_nothing_shaped_like_a_key():
    offenders = [
        path.name for path in sorted(FIXTURES.iterdir()) if KEY_SHAPED.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_the_store_imports_nothing_from_the_equity_engine():
    store = ROOT / "src" / "data_pipeline" / "store"
    assert [name for name in imports_of(store) if name.startswith("data_pipeline.equity")] == []


def test_the_equity_engine_imports_nothing_from_the_store():
    equity = ROOT / "src" / "data_pipeline" / "equity"
    assert [name for name in imports_of(equity) if name.startswith("data_pipeline")] == []
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/fred_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.fred'`

- [ ] **Step 4: Write the source**

Create `src/data_pipeline/store/sources/fred.py`:

```python
"""FRED (St. Louis Fed): one series per request; key in FRED_API_KEY.

Two calls per series: /series (title, units, frequency, seasonal adjustment) and
/series/observations. "." = missing.
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.store import keys
from data_pipeline.store.errors import KeyRejectedError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
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
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import failures, number, per_request, reject_params

URL = "https://api.stlouisfed.org/fred"
OK = 200
FREQUENCIES: Mapping[str, Frequency] = {
    "D": Frequency.DAILY,
    "W": Frequency.WEEKLY,
    "M": Frequency.MONTHLY,
    "Q": Frequency.QUARTERLY,
    "A": Frequency.ANNUAL,
}


def read_observations(payload: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for row in payload["observations"]:
        period, date = read_period(str(row["date"]), frequency)
        observations.append(Observation(period, date, number(row["value"])))
    return tuple(sorted(observations, key=lambda observation: observation.date))


class Fred:
    name = "fred"
    kind = Kind.SERIES
    requests_per_minute = 100  # assumed, not verified against FRED's documentation
    daily_budget: int | None = None

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._key = credentials.get(keys.FRED)

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            reason = f"{keys.FRED} missing in .env"
            yield FetchBatch(failures=failures(requests, Outcome.KEY_ERROR, reason))
            return
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        base = {"series_id": entry.source_id, "api_key": self._key or "", "file_type": "json"}
        meta = self._get("series", base, entry)
        if isinstance(meta, Failure):
            return meta
        info = meta["seriess"][0]
        short = str(info.get("frequency_short") or "")
        frequency = FREQUENCIES.get(short)
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"unsupported frequency {short!r}")
        params = dict(base)
        if request.since is not None:
            params["observation_start"] = request.since.isoformat()
        data = self._get("series/observations", params, entry)
        if isinstance(data, Failure):
            return data
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=str(info.get("title") or entry.source_id),
            frequency=frequency,
            units=str(info.get("units") or ""),
            seasonal_adjustment=str(info.get("seasonal_adjustment_short") or ""),
            observations=read_observations(data, frequency),
        )

    def _get(self, path: str, params: Mapping[str, str], entry: CatalogEntry) -> dict[str, Any] | Failure:
        response = self._client.get(
            self.name,
            f"{URL}/{path}",
            params=params,
            per_minute=self.requests_per_minute,
        )
        if response.status_code == OK:
            payload: dict[str, Any] = response.json()
            return payload
        text = self._client.scrub(response.text[:300])
        if "api_key" in text:
            msg = f"FRED rejected the key: {text}"
            raise KeyRejectedError(msg)
        if "does not exist" in text:
            return Failure(entry, Outcome.NOT_FOUND, text)
        return Failure(entry, Outcome.SOURCE_ERROR, f"HTTP {response.status_code}: {text}")
```

- [ ] **Step 5: Write the registry**

Replace `src/data_pipeline/store/sources/__init__.py` with:

```python
"""Registry of sources: name -> class, and the title each one is cited with."""

from collections.abc import Callable, Mapping

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.sources.base import Source
from data_pipeline.store.sources.fred import Fred

Factory = Callable[[Client, Credentials], Source]

REGISTRY: Mapping[str, Factory] = {
    "fred": Fred,
}
TITLES: Mapping[str, str] = {
    "fred": "FRED",
}


def create(name: str, client: Client, credentials: Credentials) -> Source:
    """Build the source called `name`."""
    factory = REGISTRY.get(name)
    if factory is None:
        known = ", ".join(sorted(REGISTRY))
        msg = f"unknown source {name!r} (known sources: {known})"
        raise CatalogError(msg)
    return factory(client, credentials)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/fred_test.py tests/unit/store/boundaries_test.py -q`
Expected: `10 passed`

- [ ] **Step 7: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 8: Commit**

```bash
git add src/data_pipeline/store/sources tests/unit/store
git commit -m "feat(store): add the FRED source and the source registry"
```

### Task 8: The catalog

A YAML list of entries. Six fields are shared by every source (`source`, `ids`, `alias`, `start`, `stale_after_days`, `attrs`); anything else goes into `params` and is judged by the source's `validate`. `parse_catalog` checks the structure of each entry; `check_catalog` checks what needs the whole catalog (unknown sources, duplicates, aliases). Ids are turned into text because YAML reads `737121` as a number. Simplified port of `catalogo.py`.

**Files:**
- Create: `src/data_pipeline/store/catalog.py`
- Test: `tests/unit/store/catalog_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/catalog_test.py`:

```python
import datetime

import pytest

from data_pipeline.store.catalog import build_entries, check_catalog, load_catalog, parse_catalog
from data_pipeline.store.errors import CatalogError

from .helpers import FakeSource, entry

YAML = """
# labour
- source: fred
  ids: [UNRATE, DGS10]
  alias:
    UNRATE: usa.empleo.desempleo
  start: 1990-01-01
  stale_after_days: 45
  attrs:
    theme: labour
- source: inegi
  ids: [737121]
  bank: BIE
"""


def test_loads_a_yaml_file(tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text(YAML, encoding="utf-8")
    entries = load_catalog(path)
    assert [item.key for item in entries] == ["fred:UNRATE", "fred:DGS10", "inegi:737121"]
    unrate, dgs10, inegi = entries
    assert unrate.alias == "usa.empleo.desempleo"
    assert dgs10.alias is None
    assert unrate.start == datetime.date(1990, 1, 1)
    assert unrate.stale_after_days == 45
    assert unrate.attrs == {"theme": "labour"}
    assert unrate.params == {}
    assert inegi.source_id == "737121"
    assert inegi.params == {"bank": "BIE"}


def test_an_empty_file_is_an_empty_catalog(tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text("# nothing yet\n", encoding="utf-8")
    assert load_catalog(path) == ()


def test_a_missing_file_is_an_error(tmp_path):
    with pytest.raises(CatalogError, match="does not exist"):
        load_catalog(tmp_path / "absent.yaml")


def test_broken_yaml_is_an_error(tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text("- source: [unclosed\n", encoding="utf-8")
    with pytest.raises(CatalogError, match="not valid YAML"):
        load_catalog(path)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"source": "fred"}, "must be a list of entries"),
        (["fred"], "entry 1: an entry must be a mapping"),
        ([{"ids": ["A"]}], "entry 1: missing 'source'"),
        ([{"source": "fred"}], "entry 1: missing 'ids'"),
        ([{"source": "fred", "ids": []}], "entry 1: 'ids' must be a non-empty list"),
        ([{"source": "fred", "ids": "UNRATE"}], "entry 1: 'ids' must be a non-empty list"),
        ([{"source": "fred", "ids": ["A"], "alias": {"B": "x"}}], "entry 1: 'alias' names ids that are not in 'ids'"),
        ([{"source": "fred", "ids": ["A"], "alias": "x"}], "entry 1: 'alias' must be a mapping"),
        ([{"source": "fred", "ids": ["A"], "start": "soon"}], "entry 1: 'start' must be a date"),
        ([{"source": "fred", "ids": ["A"], "stale_after_days": "ten"}], "entry 1: 'stale_after_days' must be a whole"),
        ([{"source": "fred", "ids": ["A"]}, {"source": "", "ids": ["A"]}], "entry 2: 'source' must be"),
    ],
)
def test_structural_errors_name_the_entry_and_the_field(raw, message):
    with pytest.raises(CatalogError, match=message):
        parse_catalog(raw)


def test_build_entries_is_what_store_add_uses():
    entries = build_entries("fred", ["UNRATE"], alias={"UNRATE": "u"}, params={"x": 1})
    assert entries[0].alias == "u"
    assert entries[0].params == {"x": 1}


def test_check_accepts_a_clean_catalog():
    check_catalog([entry("A"), entry("B", alias="b")], {"fake": FakeSource()})


@pytest.mark.parametrize(
    ("entries", "message"),
    [
        ([entry("A", source="nope")], "nope:A: unknown source 'nope'"),
        ([entry("A"), entry("A")], "fake:A: declared more than once"),
        ([entry("A", alias="x:y")], "fake:A: alias 'x:y' may not contain ':'"),
        ([entry("A", alias="x"), entry("B", alias="x")], "fake:B: alias 'x' is used more than once"),
        ([entry("A", params={"bank": "BIE"})], "fake:A: unknown field.s. for source 'fake': bank"),
    ],
)
def test_check_rejects(entries, message):
    with pytest.raises(CatalogError, match=message):
        check_catalog(entries, {"fake": FakeSource()})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/catalog_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.catalog'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/catalog.py`:

```python
"""The catalog: what the user wants downloaded. A YAML list of entries, or entries built in code.

    - source: fred
      ids: [UNRATE, DGS10]
      alias:
        UNRATE: usa.empleo.desempleo
      start: 1990-01-01

Fields every source shares: source, ids, alias, start, stale_after_days, attrs. Any other field
belongs to the source and is checked by its `validate`.
"""

import datetime
import pathlib
from collections.abc import Mapping, Sequence
from typing import Any

import yaml

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import CatalogEntry
from data_pipeline.store.sources.base import Source

SHARED_FIELDS = frozenset({"source", "ids", "alias", "start", "stale_after_days", "attrs"})


def _fail(where: str, problem: str) -> CatalogError:
    return CatalogError(f"{where}: {problem}")


def build_entries(
    source: str,
    ids: Sequence[object],
    *,
    where: str = "catalog",
    alias: Mapping[str, str] | None = None,
    start: datetime.date | None = None,
    stale_after_days: int | None = None,
    attrs: Mapping[str, str] | None = None,
    params: Mapping[str, object] | None = None,
) -> tuple[CatalogEntry, ...]:
    """One CatalogEntry per id. Ids are turned into text (YAML reads 737121 as a number)."""
    if not isinstance(source, str) or not source:
        raise _fail(where, "'source' must be a non-empty text")
    if isinstance(ids, str) or not isinstance(ids, Sequence) or not ids:
        raise _fail(where, "'ids' must be a non-empty list")
    names = [str(item) for item in ids]
    aliases = dict(alias or {})
    unknown = sorted(set(map(str, aliases)) - set(names))
    if unknown:
        raise _fail(where, f"'alias' names ids that are not in 'ids': {', '.join(unknown)}")
    if start is not None and not isinstance(start, datetime.date):
        raise _fail(where, "'start' must be a date such as 1990-01-01")
    if stale_after_days is not None and (isinstance(stale_after_days, bool) or not isinstance(stale_after_days, int)):
        raise _fail(where, "'stale_after_days' must be a whole number")
    return tuple(
        CatalogEntry(
            source=source,
            source_id=name,
            alias=str(aliases[name]) if name in aliases else None,
            start=start,
            stale_after_days=stale_after_days,
            attrs={str(k): str(v) for k, v in (attrs or {}).items()},
            params=dict(params or {}),
        )
        for name in names
    )


def parse_catalog(raw: object, where: str = "catalog") -> tuple[CatalogEntry, ...]:
    """Turn the parsed YAML (a list of mappings) into entries."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise _fail(where, "the catalog must be a list of entries")
    entries: list[CatalogEntry] = []
    for position, item in enumerate(raw, start=1):
        here = f"{where}, entry {position}"
        if not isinstance(item, dict):
            raise _fail(here, "an entry must be a mapping with 'source' and 'ids'")
        fields: dict[str, Any] = {str(k): v for k, v in item.items()}
        for required in ("source", "ids"):
            if required not in fields:
                raise _fail(here, f"missing {required!r}")
        for name in ("alias", "attrs"):
            if fields.get(name) is not None and not isinstance(fields[name], dict):
                raise _fail(here, f"{name!r} must be a mapping")
        entries.extend(
            build_entries(
                fields["source"],
                fields["ids"],
                where=here,
                alias={str(k): str(v) for k, v in (fields.get("alias") or {}).items()},
                start=fields.get("start"),
                stale_after_days=fields.get("stale_after_days"),
                attrs=fields.get("attrs"),
                params={k: v for k, v in fields.items() if k not in SHARED_FIELDS},
            )
        )
    return tuple(entries)


def load_catalog(path: pathlib.Path) -> tuple[CatalogEntry, ...]:
    """Read a YAML catalog file."""
    if not path.exists():
        raise _fail(str(path), "the catalog file does not exist")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise _fail(str(path), f"not valid YAML ({exc})") from exc
    return parse_catalog(raw, where=str(path))


def check_catalog(entries: Sequence[CatalogEntry], sources: Mapping[str, Source]) -> None:
    """Checks that need the whole catalog: duplicates, aliases, and each source's own fields."""
    keys: set[str] = set()
    aliases: set[str] = set()
    for entry in entries:
        if entry.source not in sources:
            msg = f"{entry.key}: unknown source {entry.source!r}"
            raise CatalogError(msg)
        if entry.key in keys:
            msg = f"{entry.key}: declared more than once"
            raise CatalogError(msg)
        keys.add(entry.key)
        if entry.alias is not None:
            if ":" in entry.alias:
                msg = f"{entry.key}: alias {entry.alias!r} may not contain ':'"
                raise CatalogError(msg)
            if entry.alias in aliases:
                msg = f"{entry.key}: alias {entry.alias!r} is used more than once"
                raise CatalogError(msg)
            aliases.add(entry.alias)
        sources[entry.source].validate(entry)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/catalog_test.py -q`
Expected: `22 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/catalog.py tests/unit/store/catalog_test.py
git commit -m "feat(store): load and validate the YAML catalog"
```

### Task 9: Storage: parquet, append-only merge, latest and as-of reads

The heart of the store. `append_changes` appends a received `(key, period)` only when it has no stored row or differs from the latest stored row, and never touches a stored row. `latest` and `as_of` are the two ways to read. `typed` builds frames with fixed dtypes so frames from memory and from parquet always combine. Port of `almacen.py`, with the upsert replaced by the append-only rule.

**Files:**
- Create: `src/data_pipeline/store/storage.py`
- Test: `tests/unit/store/storage_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/storage_test.py`:

```python
import datetime
import json
import math

import pandas as pd
import pytest

from data_pipeline.store.errors import StoreError
from data_pipeline.store.storage import (
    INDEX_COLUMNS,
    OBS_COLUMNS,
    OBS_DTYPES,
    Storage,
    append_changes,
    as_of,
    empty_index,
    empty_observations,
    latest,
    to_moment,
    typed,
)

JUNE_6 = datetime.datetime(2026, 6, 6, 12, 0, tzinfo=datetime.UTC)
JULY_4 = datetime.datetime(2026, 7, 4, 12, 0, tzinfo=datetime.UTC)
AUGUST_8 = datetime.datetime(2026, 8, 8, 12, 0, tzinfo=datetime.UTC)
DAYS = {"2026-03": "2026-03-31", "2026-04": "2026-04-30", "2026-05": "2026-05-31", "2026-06": "2026-06-30"}


def rows(values, fetched_at, *, key="fred:UNRATE", projection=False, published_at=None):
    """Observation frame from {"2026-05": 4.1, ...}."""
    return typed(
        [
            {
                "key": key,
                "period": period,
                "date": DAYS[period],
                "value": value,
                "projection": projection,
                "fetched_at": fetched_at,
                "published_at": published_at,
            }
            for period, value in values.items()
        ],
        OBS_DTYPES,
    )


def values_of(frame):
    return dict(zip(frame["period"], frame["value"], strict=True))


def test_empty_frames_have_the_declared_columns():
    assert list(empty_observations().columns) == OBS_COLUMNS
    assert list(empty_index().columns) == INDEX_COLUMNS
    assert empty_observations().empty


def test_first_rows_are_all_added():
    merged, added, revised = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": 4.1}, JUNE_6))
    assert (len(merged), added, revised) == (2, 2, 0)


def test_the_same_data_again_adds_nothing():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": math.nan}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-04": 4.0, "2026-05": math.nan}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 0)
    assert merged is stored


def test_a_changed_value_adds_a_row_and_keeps_the_old_one():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-05": 4.2}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 1)
    assert list(merged["value"]) == [4.1, 4.2]


def test_float_noise_is_not_a_revision():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-05": 4.1 * (1 + 1e-12)}, JULY_4))
    assert (len(merged), added, revised) == (1, 0, 0)


def test_a_missing_value_that_arrives_later_is_new_not_revised():
    stored, added, _ = append_changes(empty_observations(), rows({"2026-05": math.nan}, JUNE_6))
    assert added == 0
    merged, added, revised = append_changes(stored, rows({"2026-05": 4.1}, JULY_4))
    assert (len(merged), added, revised) == (2, 1, 0)


def test_a_number_that_becomes_missing_is_a_revision():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-05": math.nan}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 1)


def test_a_projection_that_becomes_actual_is_a_change_even_with_the_same_value():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-06": 4.3}, JUNE_6, projection=True))
    merged, added, revised = append_changes(stored, rows({"2026-06": 4.3}, JULY_4, projection=False))
    assert (len(merged), added, revised) == (2, 0, 1)


def test_keys_do_not_interfere():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6, key="fred:A"))
    merged, added, _ = append_changes(stored, rows({"2026-05": 9.9}, JUNE_6, key="fred:B"))
    assert (len(merged), added) == (2, 1)


def test_latest_returns_the_row_fetched_last():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": 4.1}, JUNE_6))
    stored, _, _ = append_changes(stored, rows({"2026-05": 4.2}, JULY_4))
    assert values_of(latest(stored)) == {"2026-04": 4.0, "2026-05": 4.2}


def test_as_of_returns_what_was_known_then():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": 4.1}, JUNE_6))
    stored, _, _ = append_changes(stored, rows({"2026-05": 4.2, "2026-06": 4.3}, JULY_4))
    assert values_of(as_of(stored, to_moment("2026-06-15"))) == {"2026-04": 4.0, "2026-05": 4.1}
    assert values_of(as_of(stored, to_moment("2026-07-04"))) == {"2026-04": 4.0, "2026-05": 4.2, "2026-06": 4.3}
    assert as_of(stored, to_moment("2026-01-01")).empty


def test_as_of_prefers_the_source_publication_date():
    published = datetime.datetime(2026, 5, 2, tzinfo=datetime.UTC)
    stored, _, _ = append_changes(empty_observations(), rows({"2026-03": 4.0}, AUGUST_8, published_at=published))
    assert values_of(as_of(stored, to_moment("2026-05-02"))) == {"2026-03": 4.0}
    assert as_of(stored, to_moment("2026-05-01")).empty


def test_to_moment_reads_dates_as_end_of_day_utc():
    end = pd.Timestamp("2026-06-15 23:59:59.999999", tz="UTC")
    assert to_moment("2026-06-15") == end
    assert to_moment(datetime.date(2026, 6, 15)) == end
    assert to_moment(datetime.datetime(2026, 6, 15, 8, 0)) == pd.Timestamp("2026-06-15 08:00", tz="UTC")
    assert to_moment("2026-06-15T08:00:00+02:00") == pd.Timestamp("2026-06-15 06:00", tz="UTC")


def test_observations_survive_a_round_trip(tmp_path):
    storage = Storage(tmp_path)
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": math.nan}, JUNE_6))
    storage.write_observations("fred", stored)
    again = storage.read_observations("fred")
    assert list(again.columns) == OBS_COLUMNS
    merged, added, revised = append_changes(again, rows({"2026-04": 4.0, "2026-05": math.nan}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 0)
    assert not list(tmp_path.rglob("*.tmp"))


def test_missing_files_read_as_empty(tmp_path):
    storage = Storage(tmp_path / "nowhere")
    assert storage.read_observations("fred").empty
    assert storage.read_index().empty
    assert storage.read_runs() == {}


def test_runs_round_trip(tmp_path):
    storage = Storage(tmp_path)
    storage.write_runs({"fred": {"ok": 3}})
    assert storage.read_runs() == {"fred": {"ok": 3}}


def test_prepare_writes_the_schema_marker_once(tmp_path):
    storage = Storage(tmp_path / "store")
    storage.prepare()
    storage.prepare()
    assert json.loads((tmp_path / "store" / "store.json").read_text(encoding="utf-8")) == {"schema_version": 1}


def test_prepare_refuses_another_schema(tmp_path):
    (tmp_path / "store.json").write_text('{"schema_version": 99}', encoding="utf-8")
    with pytest.raises(StoreError, match="schema_version 99"):
        Storage(tmp_path).prepare()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/storage_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.storage'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/storage.py`:

```python
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
import json
import pathlib
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
KEY = ["key", "period"]
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


def latest(observations: pd.DataFrame) -> pd.DataFrame:
    """Per (key, period), the row fetched last. Sorted by key and date."""
    if observations.empty:
        return observations
    ordered = observations.sort_values("fetched_at", kind="stable")
    current = ordered.drop_duplicates(KEY, keep="last")
    return current.sort_values(["key", "date"], kind="stable").reset_index(drop=True)


def as_of(observations: pd.DataFrame, moment: pd.Timestamp) -> pd.DataFrame:
    """Per (key, period), the row that was known at `moment`.

    A row is known from its `published_at` when the source gives one, otherwise from its
    `fetched_at`. Rows known later than `moment` are invisible.
    """
    if observations.empty:
        return observations
    known_at = observations["published_at"].fillna(observations["fetched_at"])
    visible = observations.assign(known_at=known_at)[(known_at <= moment).to_numpy()]
    ordered = visible.sort_values(["known_at", "fetched_at"], kind="stable")
    current = ordered.drop_duplicates(KEY, keep="last").drop(columns="known_at")
    return current.sort_values(["key", "date"], kind="stable").reset_index(drop=True)


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


def _read(path: pathlib.Path, dtypes: Mapping[str, str]) -> pd.DataFrame:
    if not path.exists():
        return typed([], dtypes)
    return pd.read_parquet(path)


def _write(frame: pd.DataFrame, path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _write_json(data: Mapping[str, Any], path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
    temporary.replace(path)


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
        return _read(self.root / INDEX_FILE, INDEX_DTYPES)

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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/storage_test.py -q`
Expected: `18 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/storage.py tests/unit/store/storage_test.py
git commit -m "feat(store): add append-only parquet storage with latest and as-of reads"
```

### Task 10: The four invariants of the store, as properties

These tests generate random sync histories with hypothesis and check the invariants the spec promises. They exercise code that already exists, so they are expected to pass at once; if one fails, the defect is in `append_changes`, `latest` or `as_of`, and hypothesis prints the smallest history that breaks it.

**Files:**
- Test: `tests/unit/store/storage_properties_test.py`

- [ ] **Step 1: Write the property tests**

Create `tests/unit/store/storage_properties_test.py`:

```python
"""The four invariants of the store, checked on generated sync histories."""

import datetime
import math

import pandas as pd
from hypothesis import given
from hypothesis import strategies as st

from data_pipeline.store.storage import OBS_DTYPES, TOLERANCE, append_changes, as_of, empty_observations, latest, typed

START = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
DAYS = {"2026-01": "2026-01-31", "2026-02": "2026-02-28", "2026-03": "2026-03-31", "2026-04": "2026-04-30"}
VALUES = st.one_of(st.just(math.nan), st.floats(min_value=-1e6, max_value=1e6, allow_nan=False))
BATCHES = st.lists(st.dictionaries(st.sampled_from(sorted(DAYS)), VALUES, min_size=1), min_size=1, max_size=6)


def moment(step):
    return START + datetime.timedelta(days=step)


def frame(batch, step):
    return typed(
        [
            {
                "key": "fred:X",
                "period": period,
                "date": DAYS[period],
                "value": value,
                "projection": False,
                "fetched_at": moment(step),
                "published_at": None,
            }
            for period, value in batch.items()
        ],
        OBS_DTYPES,
    )


def same(left, right):
    return (math.isnan(left) and math.isnan(right)) or math.isclose(left, right, rel_tol=TOLERANCE, abs_tol=0.0)


def replay(batches):
    """Apply the batches one sync after another. Returns the store after each sync."""
    stored = empty_observations()
    history = []
    for step, batch in enumerate(batches):
        stored, _, _ = append_changes(stored, frame(batch, step))
        history.append(stored)
    return history


@given(BATCHES)
def test_syncing_the_same_data_twice_adds_no_rows(batches):
    stored = replay(batches)[-1]
    again, added, revised = append_changes(stored, frame(batches[-1], len(batches)))
    assert (len(again), added, revised) == (len(stored), 0, 0)


@given(BATCHES)
def test_a_normal_read_returns_the_last_value_received(batches):
    expected = {}
    for batch in batches:
        expected.update(batch)
    current = latest(replay(batches)[-1])
    found = dict(zip(current["period"], current["value"], strict=True))
    assert found.keys() == expected.keys()
    assert all(same(found[period], expected[period]) for period in expected)


@given(BATCHES)
def test_an_as_of_read_never_returns_something_known_later(batches):
    history = replay(batches)
    final = history[-1]
    for step, then in enumerate(history):
        seen = as_of(final, pd.Timestamp(moment(step)))
        pd.testing.assert_frame_equal(seen, latest(then))


@given(BATCHES)
def test_no_stored_row_is_ever_changed_or_deleted(batches):
    history = replay(batches)
    final = history[-1]
    for step, then in enumerate(history):
        kept = final[final["fetched_at"] <= pd.Timestamp(moment(step))].reset_index(drop=True)
        pd.testing.assert_frame_equal(kept, then)
```

- [ ] **Step 2: Run them**

Run: `.venv/Scripts/python -m pytest tests/unit/store/storage_properties_test.py -q`
Expected: `4 passed` (about ten seconds: each test runs a hundred generated histories)

- [ ] **Step 3: Check that the tests can fail**

A property test that cannot fail proves nothing. In `src/data_pipeline/store/storage.py`, inside `append_changes`, temporarily change `unchanged = known & same_value & same_flag` to `unchanged = known & same_flag` and run the file again.

Expected: `test_a_normal_read_returns_the_last_value_received` fails, showing a history where a changed value was not stored.

Undo the change and run again. Expected: `4 passed`.

- [ ] **Step 4: Lint and commit**

Run: `.venv/Scripts/python -m ruff check tests/unit/store`
Expected: `All checks passed!`

```bash
git add tests/unit/store/storage_properties_test.py
git commit -m "test(store): check the four store invariants with generated histories"
```

### Task 11: The sync engine

`sync` holds the lock, validates the catalog, and for each source works out what to ask (`since`), stores every batch as it arrives, and records failures in the index while keeping the previous data. Quotas stop a source in two ways: its own persisted daily budget, and the `QuotaExhaustedError` a source raises. Nothing is kept in a pending list: the next run recomputes what is missing. The report is returned, never discarded. Port of `actualizar.py`.

**Files:**
- Create: `src/data_pipeline/store/sync.py`
- Test: `tests/unit/store/sync_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/sync_test.py`:

```python
import datetime
import math

import pytest

from data_pipeline.store.errors import CatalogError, LockHeldError
from data_pipeline.store.model import Failure, Frequency, Observation, Outcome, SeriesData
from data_pipeline.store.storage import Storage, latest
from data_pipeline.store.sync import LOCK_FILE, NOT_RETURNED, SourceReport, SyncReport, sync, window_start

from .helpers import NOW, FakeSource, client, entry, monthly

LATER = NOW + datetime.timedelta(days=28)
NEXT_DAY = NOW + datetime.timedelta(days=1)
UNRATE = entry("UNRATE")
DGS10 = entry("DGS10")


def run(tmp_path, source, entries=(UNRATE,), now=NOW, http=None, **options):
    return sync(Storage(tmp_path), list(entries), {source.name: source}, http or client(), now, **options)


def stored_values(tmp_path, key="fake:UNRATE"):
    frame = latest(Storage(tmp_path).read_observations("fake"))
    frame = frame[frame["key"] == key]
    return dict(zip(frame["period"], frame["value"], strict=True))


def index_row(tmp_path, key="fake:UNRATE"):
    frame = Storage(tmp_path).read_index()
    return frame[frame["key"] == key].iloc[0]


@pytest.mark.parametrize(
    ("frequency", "last", "expected"),
    [
        (Frequency.DAILY, datetime.date(2026, 9, 25), datetime.date(2026, 8, 26)),
        (Frequency.WEEKLY, datetime.date(2026, 9, 25), datetime.date(2026, 6, 26)),
        (Frequency.MONTHLY, datetime.date(2026, 5, 31), datetime.date(2024, 5, 1)),
        (Frequency.QUARTERLY, datetime.date(2026, 6, 30), datetime.date(2023, 6, 1)),
        (Frequency.ANNUAL, datetime.date(2025, 12, 31), datetime.date(2020, 12, 1)),
    ],
)
def test_window_start(frequency, last, expected):
    assert window_start(last, frequency) == expected


def test_first_sync_asks_for_full_history_and_stores_it(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-04": 4.0, "2026-05": 4.1})
    report = run(tmp_path, source)
    assert source.seen[0].since is None
    assert stored_values(tmp_path) == {"2026-04": 4.0, "2026-05": 4.1}
    assert report.sources == (SourceReport("fake", 1, (), 2, 0, 0, False, 0),)
    assert report.exit_code == 0
    row = index_row(tmp_path)
    assert (row["status"], row["name"], row["frequency"], row["units"]) == ("ok", "Name of UNRATE", "M", "Percent")
    assert row["last_period"] == "2026-05"
    assert row["first_fetched_at"] == row["last_fetched_at"]


def test_a_declared_start_is_used_on_the_first_sync(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    started = entry("UNRATE", start=datetime.date(1990, 1, 1))
    run(tmp_path, source, entries=[started])
    assert source.seen[0].since == datetime.date(1990, 1, 1)


def test_second_sync_asks_from_the_revision_window_and_adds_nothing(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-04": 4.0, "2026-05": 4.1})
    run(tmp_path, source)
    report = run(tmp_path, source, now=LATER)
    assert source.seen[1].since == datetime.date(2024, 5, 1)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(Storage(tmp_path).read_observations("fake")) == 2
    row = index_row(tmp_path)
    assert row["first_fetched_at"] < row["last_fetched_at"]


def test_a_revision_adds_a_row_and_is_counted(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.2})
    report = run(tmp_path, source, now=LATER)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    assert list(Storage(tmp_path).read_observations("fake")["value"]) == [4.1, 4.2]
    assert stored_values(tmp_path) == {"2026-05": 4.2}


def test_full_asks_for_everything_again(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    run(tmp_path, source, now=LATER, full=True)
    assert source.seen[1].since is None


def test_an_open_period_is_stored_as_a_projection(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1, "2026-06": 4.3})
    run(tmp_path, source)
    frame = Storage(tmp_path).read_observations("fake")
    assert dict(zip(frame["period"], frame["projection"], strict=True)) == {"2026-05": False, "2026-06": True}
    assert index_row(tmp_path)["last_period"] == "2026-05"


def test_a_failure_is_recorded_and_the_other_series_continue(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = Failure(UNRATE, Outcome.NOT_FOUND, "The series does not exist.")
    source.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    report = run(tmp_path, source, entries=[UNRATE, DGS10])
    assert report.sources[0].ok == 1
    assert report.sources[0].failed == (("fake:UNRATE", "not_found: The series does not exist."),)
    assert report.exit_code == 1
    assert stored_values(tmp_path, "fake:DGS10") == {"2026-05": 4.4}
    row = index_row(tmp_path)
    assert (row["status"], row["reason"]) == ("failed", "not_found: The series does not exist.")


def test_a_series_that_fails_later_keeps_its_data(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    source.answers["UNRATE"] = Failure(UNRATE, Outcome.NETWORK_ERROR, "fake: HTTP 503 after 4 attempts")
    run(tmp_path, source, now=LATER)
    assert stored_values(tmp_path) == {"2026-05": 4.1}
    row = index_row(tmp_path)
    assert (row["status"], row["name"], row["last_period"]) == ("failed", "Name of UNRATE", "2026-05")


def test_a_series_the_source_does_not_return_is_a_failure(tmp_path):
    report = run(tmp_path, FakeSource())
    assert report.sources[0].failed == (("fake:UNRATE", f"not_found: {NOT_RETURNED}"),)


def test_quota_signalled_by_the_source_stops_it_and_the_next_run_resumes(tmp_path):
    source = FakeSource(quota_after=1)
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    source.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    report = run(tmp_path, source, entries=[UNRATE, DGS10])
    assert (report.sources[0].ok, report.sources[0].quota_exhausted, report.sources[0].pending) == (1, True, 1)
    assert report.sources[0].failed == ()
    assert report.exit_code == 3
    assert stored_values(tmp_path) == {"2026-05": 4.1}
    source.quota_after = None
    report = run(tmp_path, source, entries=[UNRATE, DGS10], now=NEXT_DAY)
    assert (report.sources[0].ok, report.exit_code) == (2, 0)
    assert stored_values(tmp_path, "fake:DGS10") == {"2026-05": 4.4}


def test_the_daily_budget_persists_between_runs_of_the_same_day(tmp_path):
    http = client()
    source = FakeSource(daily_budget=1, client=http)
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    source.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    first = run(tmp_path, source, entries=[UNRATE, DGS10], http=http)
    assert (first.sources[0].ok, first.sources[0].calls, first.sources[0].pending) == (1, 1, 1)
    assert Storage(tmp_path).read_runs()["fake"]["budget"] == {"day": "2026-06-06", "calls": 1}
    second = run(tmp_path, source, entries=[UNRATE, DGS10], http=http, now=NOW + datetime.timedelta(hours=2))
    assert (second.sources[0].ok, second.sources[0].calls, second.sources[0].pending) == (0, 0, 2)
    assert second.exit_code == 3
    third = run(tmp_path, source, entries=[UNRATE, DGS10], http=http, now=NEXT_DAY)
    assert third.sources[0].ok == 1
    assert Storage(tmp_path).read_runs()["fake"]["budget"] == {"day": "2026-06-07", "calls": 1}


def test_an_interrupted_run_keeps_what_it_stored_and_frees_the_lock(tmp_path):
    class Dies(FakeSource):
        def fetch(self, requests):
            yield from super().fetch(requests[:1])
            msg = "power cut"
            raise RuntimeError(msg)

    source = Dies()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    with pytest.raises(RuntimeError, match="power cut"):
        run(tmp_path, source, entries=[UNRATE, DGS10])
    assert stored_values(tmp_path) == {"2026-05": 4.1}
    assert not (tmp_path / LOCK_FILE).exists()
    healthy = FakeSource()
    healthy.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    healthy.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    report = run(tmp_path, healthy, entries=[UNRATE, DGS10], now=LATER)
    assert (report.sources[0].ok, report.sources[0].new) == (2, 1)


def test_a_second_sync_is_refused_while_the_lock_exists(tmp_path):
    (tmp_path / LOCK_FILE).write_text("123", encoding="ascii")
    with pytest.raises(LockHeldError, match="Delete it by hand"):
        run(tmp_path, FakeSource())
    assert (tmp_path / LOCK_FILE).exists()


def test_an_invalid_catalog_stops_before_any_download(tmp_path):
    source = FakeSource()
    with pytest.raises(CatalogError, match="declared more than once"):
        run(tmp_path, source, entries=[UNRATE, UNRATE])
    assert source.seen == []
    assert not (tmp_path / LOCK_FILE).exists()


def test_only_sources_and_only_keys_restrict_the_run(tmp_path):
    fake = FakeSource()
    other = FakeSource("other")
    wanted = entry("X", source="other")
    storage = Storage(tmp_path)
    sources = {"fake": fake, "other": other}
    sync(storage, [UNRATE, DGS10, wanted], sources, client(), NOW, only_sources=["other"])
    assert (fake.seen, [request.entry.key for request in other.seen]) == ([], ["other:X"])
    sync(storage, [UNRATE, DGS10, wanted], sources, client(), LATER, only_keys=["fake:DGS10"])
    assert [request.entry.key for request in fake.seen] == ["fake:DGS10"]


def test_the_run_log_records_each_source(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    record = Storage(tmp_path).read_runs()["fake"]
    assert record["last_run"] == "2026-06-06T12:00:00+00:00"
    assert (record["ok"], record["failed"], record["new"], record["revised"]) == (1, 0, 1, 0)
    assert (record["quota_exhausted"], record["pending"]) == (False, 0)


def test_a_missing_value_is_stored_and_does_not_count_as_new(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = SeriesData(
        entry=UNRATE,
        key=UNRATE.key,
        name="U",
        frequency=Frequency.MONTHLY,
        observations=(Observation("2026-05", datetime.date(2026, 5, 31), math.nan),),
    )
    report = run(tmp_path, source)
    assert report.sources[0].new == 0
    assert len(Storage(tmp_path).read_observations("fake")) == 1
    assert index_row(tmp_path)["last_period"] is None


def test_report_lines_are_ascii_and_say_what_happened():
    report = SyncReport(
        NOW,
        (
            SourceReport("fred", 33, (), 41, 3, 66, False, 0),
            SourceReport("bls", 1100, (), 9, 0, 450, True, 43),
            SourceReport("banxico", 0, (("banxico:SF1", "key_error: BANXICO_TOKEN missing"),), 0, 0, 0, False, 0),
            SourceReport("inegi", 1, (("inegi:9", "not_found: no such indicator"),), 5, 0, 2, False, 0),
        ),
    )
    assert report.lines() == [
        "[ok]  fred      33 series, 41 new, 3 revised, 66 calls",
        "[x]   bls       1100 of 1143 series, 9 new, 0 revised, 450 calls; quota exhausted, 43 pending",
        "[x]   banxico   key_error: BANXICO_TOKEN missing",
        "[x]   inegi     1 of 2 series, 5 new, 0 revised, 2 calls",
        "      inegi:9  not_found: no such indicator",
    ]
    assert all(line.isascii() for line in report.lines())
    assert report.exit_code == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sync_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sync'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/sync.py`:

```python
"""Sync: download what the catalog declares and store what changed.

An entry never downloaded is asked for its full history (or from its `start`). An entry already
stored is asked from its last real period minus a revision window. Each batch a source yields is
stored as it arrives, so an interrupted run keeps what it already downloaded. A series that fails
keeps its previous data; the failure is recorded in the index.
"""

import contextlib
import dataclasses
import datetime
import json
import os
import pathlib
from collections.abc import Collection, Iterator, Mapping, Sequence
from typing import Any

import pandas as pd

from data_pipeline.store.catalog import check_catalog
from data_pipeline.store.errors import LockHeldError, QuotaExhaustedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Frequency,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.sources.base import Source
from data_pipeline.store.storage import (
    INDEX_DTYPES,
    OBS_DTYPES,
    Storage,
    append_changes,
    latest,
    records,
    typed,
)

LOCK_FILE = "sync.lock"
WINDOW_DAYS: Mapping[Frequency, int] = {Frequency.DAILY: 30, Frequency.WEEKLY: 91}
WINDOW_MONTHS: Mapping[Frequency, int] = {
    Frequency.MONTHLY: 24,
    Frequency.QUARTERLY: 36,
    Frequency.ANNUAL: 60,
}
NOT_RETURNED = "the source did not return this series in this run"
STATUS_OK = "ok"
STATUS_FAILED = "failed"
EXIT_OK = 0
EXIT_FAILURES = 1
EXIT_CONFIGURATION = 2
EXIT_QUOTA = 3

Row = dict[str, Any]
LastReal = tuple[str, datetime.date]  # (period label, last day) of a series' last real observation


def window_start(last: datetime.date, frequency: Frequency) -> datetime.date:
    """First date asked again: 30 days (daily), 13 weeks (weekly), 24 months (monthly),
    36 months (quarterly) or 60 months (annual) before the last stored real period."""
    if frequency in WINDOW_DAYS:
        return last - datetime.timedelta(days=WINDOW_DAYS[frequency])
    months = last.year * 12 + last.month - 1 - WINDOW_MONTHS[frequency]
    return datetime.date(months // 12, months % 12 + 1, 1)


@dataclasses.dataclass(frozen=True)
class SourceReport:
    source: str
    ok: int
    failed: tuple[tuple[str, str], ...]  # (key, reason)
    new: int
    revised: int
    calls: int
    quota_exhausted: bool
    pending: int

    @property
    def requested(self) -> int:
        return self.ok + len(self.failed) + self.pending

    def lines(self) -> list[str]:
        """ASCII lines for the console."""
        label = f"{self.source:<10}"
        counts = f"{self.new} new, {self.revised} revised, {self.calls} calls"
        if not self.failed and not self.quota_exhausted:
            return [f"[ok]  {label}{self.ok} series, {counts}"]
        reasons = {reason for _, reason in self.failed}
        if self.ok == 0 and len(reasons) == 1 and not self.quota_exhausted:
            return [f"[x]   {label}{reasons.pop()}"]
        head = f"[x]   {label}{self.ok} of {self.requested} series, {counts}"
        if self.quota_exhausted:
            head += f"; quota exhausted, {self.pending} pending"
        return [head, *(f"      {key}  {reason}" for key, reason in self.failed)]


@dataclasses.dataclass(frozen=True)
class SyncReport:
    started_at: datetime.datetime
    sources: tuple[SourceReport, ...]

    @property
    def exit_code(self) -> int:
        if any(report.failed for report in self.sources):
            return EXIT_FAILURES
        if any(report.quota_exhausted for report in self.sources):
            return EXIT_QUOTA
        return EXIT_OK

    def lines(self) -> list[str]:
        return [line for report in self.sources for line in report.lines()]


@contextlib.contextmanager
def lock(root: pathlib.Path) -> Iterator[None]:
    """Hold <root>/sync.lock for the duration of a sync. A second sync is refused."""
    path = root / LOCK_FILE
    root.mkdir(parents=True, exist_ok=True)
    try:
        handle = path.open("x", encoding="ascii")
    except FileExistsError:
        msg = f"another sync is running, or one died: {path} exists. Delete it by hand if no sync is running."
        raise LockHeldError(msg) from None
    with handle:
        handle.write(str(os.getpid()))
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


def observation_rows(series: SeriesData, fetched_at: datetime.datetime, today: datetime.date) -> list[Row]:
    """A period still open (its last day is after today) is never data: it is stored as a projection."""
    return [
        {
            "key": series.key,
            "period": observation.period,
            "date": pd.Timestamp(observation.date),
            "value": observation.value,
            "projection": observation.projection or observation.date > today,
            "fetched_at": fetched_at,
            "published_at": observation.published_at,
        }
        for observation in series.observations
    ]


def last_real(observations: pd.DataFrame, today: datetime.date) -> dict[str, LastReal]:
    """Per key, (period, date) of the last observation that is a number, not a projection and closed."""
    if observations.empty:
        return {}
    current = latest(observations)
    closed = (current["date"] <= pd.Timestamp(today)).to_numpy()
    real = current[current["value"].notna().to_numpy() & ~current["projection"].astype(bool).to_numpy() & closed]
    last = real.sort_values("date", kind="stable").drop_duplicates("key", keep="last")
    return {
        str(key): (str(period), pd.Timestamp(date).date())
        for key, period, date in zip(last["key"], last["period"], last["date"], strict=True)
    }


def _since(entry: CatalogEntry, row: Row | None, last: LastReal | None) -> datetime.date | None:
    if row is None or last is None or not row.get("frequency"):
        return entry.start
    return window_start(last[1], Frequency(str(row["frequency"])))


def _ok_row(series: SeriesData, previous: Row | None, last: LastReal | None, now: datetime.datetime) -> Row:
    entry = series.entry
    first = previous.get("first_fetched_at") if previous else None
    return {
        "key": series.key,
        "alias": entry.alias,
        "source": entry.source,
        "source_id": entry.source_id,
        "name": series.name,
        "country": series.country,
        "frequency": series.frequency.value,
        "units": series.units,
        "seasonal_adjustment": series.seasonal_adjustment,
        "stale_after_days": entry.stale_after_days,
        "attrs": json.dumps(dict(entry.attrs), sort_keys=True),
        "first_fetched_at": first if first is not None and pd.notna(first) else now,
        "last_fetched_at": now,
        "last_period": last[0] if last else None,
        "last_date": pd.Timestamp(last[1]) if last else None,
        "status": STATUS_OK,
        "reason": "",
    }


def _failed_row(entry: CatalogEntry, previous: Row | None, reason: str) -> Row:
    if previous is not None:  # keep its provenance and data, record the failure
        return {**previous, "alias": entry.alias, "status": STATUS_FAILED, "reason": reason}
    return {
        "key": entry.key,
        "alias": entry.alias,
        "source": entry.source,
        "source_id": entry.source_id,
        "name": "",
        "country": "",
        "frequency": "",
        "units": "",
        "seasonal_adjustment": "",
        "stale_after_days": entry.stale_after_days,
        "attrs": json.dumps(dict(entry.attrs), sort_keys=True),
        "first_fetched_at": None,
        "last_fetched_at": None,
        "last_period": None,
        "last_date": None,
        "status": STATUS_FAILED,
        "reason": reason,
    }


def _calls_today(record: Mapping[str, Any] | None, today: datetime.date) -> int:
    budget = (record or {}).get("budget") or {}
    return int(budget.get("calls", 0)) if budget.get("day") == today.isoformat() else 0


def _sync_source(
    storage: Storage,
    source: Source,
    client: Client,
    wanted: Sequence[CatalogEntry],
    index: dict[str, Row],
    spent_today: int,
    now: datetime.datetime,
    *,
    full: bool,
) -> SourceReport:
    name = source.name
    today = now.date()
    observations = storage.read_observations(name)
    known = last_real(observations, today)
    requests = [
        Request(entry, None if full else _since(entry, index.get(entry.key), known.get(entry.key)))
        for entry in wanted
    ]
    calls_before = client.calls.get(name, 0)
    remaining = None if source.daily_budget is None else source.daily_budget - spent_today
    done: set[str] = set()
    failed: list[tuple[str, str]] = []
    ok = added = revised = 0
    quota = remaining is not None and remaining <= 0
    if not quota:
        try:
            for batch in source.fetch(requests):
                fresh = [series for series in batch.series if series.key not in done]
                rows = [row for series in fresh for row in observation_rows(series, now, today)]
                if rows:
                    observations, more, changed = append_changes(observations, typed(rows, OBS_DTYPES))
                    added += more
                    revised += changed
                    storage.write_observations(name, observations)
                if fresh:
                    known = last_real(observations, today)
                for series in fresh:
                    done.add(series.key)
                    ok += 1
                    index[series.key] = _ok_row(series, index.get(series.key), known.get(series.key), now)
                for failure in batch.failures:
                    key = failure.entry.key
                    if key in done:
                        continue
                    done.add(key)
                    reason = f"{failure.outcome.value}: {failure.reason}"
                    failed.append((key, reason))
                    index[key] = _failed_row(failure.entry, index.get(key), reason)
                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
                    quota = True
                    break
        except QuotaExhaustedError:
            quota = True
    missing = [request.entry for request in requests if request.entry.key not in done]
    if not quota:
        for entry in missing:
            reason = f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
            failed.append((entry.key, reason))
            index[entry.key] = _failed_row(entry, index.get(entry.key), reason)
        if missing:
            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
    return SourceReport(
        source=name,
        ok=ok,
        failed=tuple(failed),
        new=added,
        revised=revised,
        calls=client.calls.get(name, 0) - calls_before,
        quota_exhausted=quota,
        pending=len(missing) if quota else 0,
    )


def sync(
    storage: Storage,
    entries: Sequence[CatalogEntry],
    sources: Mapping[str, Source],
    client: Client,
    now: datetime.datetime,
    *,
    only_sources: Collection[str] = (),
    only_keys: Collection[str] = (),
    full: bool = False,
) -> SyncReport:
    """Run one sync. `now` (UTC) stamps every row fetched in this run."""
    with lock(storage.root):
        storage.prepare()
        check_catalog(entries, sources)
        index: dict[str, Row] = {str(row["key"]): row for row in records(storage.read_index())}
        runs = storage.read_runs()
        today = now.date()
        reports = []
        for name in sorted({entry.source for entry in entries}):
            if only_sources and name not in only_sources:
                continue
            wanted = [entry for entry in entries if entry.source == name and (not only_keys or entry.key in only_keys)]
            if not wanted:
                continue
            spent = _calls_today(runs.get(name), today)
            report = _sync_source(storage, sources[name], client, wanted, index, spent, now, full=full)
            reports.append(report)
            runs[name] = {
                "last_run": now.isoformat(),
                "ok": report.ok,
                "failed": len(report.failed),
                "new": report.new,
                "revised": report.revised,
                "calls": report.calls,
                "quota_exhausted": report.quota_exhausted,
                "pending": report.pending,
                "budget": {"day": today.isoformat(), "calls": spent + report.calls},
            }
            storage.write_runs(runs)
        return SyncReport(now, tuple(reports))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sync_test.py -q`
Expected: `23 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sync.py tests/unit/store/sync_test.py
git commit -m "feat(store): add the sync engine with revision windows, quotas and a run report"
```

### Task 12: The Store facade

The one object other code imports. Reading never touches the network: a `Store` built with only a folder needs no keys and no catalog. Replaces `lector.py`, adding as-of reads, `frame` and `revisions`.

**Files:**
- Create: `src/data_pipeline/store/api.py`
- Modify: `src/data_pipeline/__init__.py`
- Test: `tests/unit/store/api_test.py`

- [ ] **Step 1: Write the failing test**

The test runs the real FRED source against a fake FRED server (`httpx.MockTransport`) whose observations the test changes between syncs, with an injected clock.

Create `tests/unit/store/api_test.py`:

```python
import datetime
import json

import httpx
import pandas as pd
import pytest

import data_pipeline
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError, UnknownSeriesError

from .helpers import NOW

CATALOG = """
- source: fred
  ids: [UNRATE, DGS10]
  alias:
    UNRATE: usa.empleo.desempleo
"""
META = {
    "UNRATE": {
        "title": "Unemployment Rate",
        "units": "Percent",
        "frequency_short": "M",
        "seasonal_adjustment_short": "SA",
    },
    "DGS10": {
        "title": "10-Year Treasury",
        "units": "Percent",
        "frequency_short": "D",
        "seasonal_adjustment_short": "NSA",
    },
}


class Fred:
    """A fake FRED server whose observations the test can change between syncs."""

    def __init__(self):
        self.observations = {
            "UNRATE": {"2026-04-01": "4.0", "2026-05-01": "4.1"},
            "DGS10": {"2026-06-04": "4.40", "2026-06-05": "4.45"},
        }
        self.requests = 0

    def __call__(self, request):
        self.requests += 1
        series_id = request.url.params["series_id"]
        if series_id not in META:
            return httpx.Response(400, text='{"error_message":"Bad Request.  The series does not exist."}')
        if request.url.path.endswith("/observations"):
            rows = [{"date": date, "value": value} for date, value in self.observations[series_id].items()]
            return httpx.Response(200, text=json.dumps({"observations": rows}))
        return httpx.Response(200, text=json.dumps({"seriess": [META[series_id]]}))


class Clock:
    def __init__(self):
        self.now = NOW

    def __call__(self):
        return self.now


@pytest.fixture
def world(tmp_path):
    """(store, fake FRED, clock) with a catalog and a key on disk."""
    (tmp_path / "catalog.yaml").write_text(CATALOG, encoding="utf-8")
    (tmp_path / ".env").write_text("FRED_API_KEY=0123456789abcdef0123456789abcdef\n", encoding="utf-8")
    server = Fred()
    clock = Clock()
    store = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=clock,
        transport=httpx.MockTransport(server),
        sleep=lambda _seconds: None,
    )
    return store, server, clock


def test_the_package_exports_store():
    assert data_pipeline.Store is Store


def test_sync_then_read_a_series(world):
    store, _, _ = world
    report = store.sync()
    assert report.exit_code == 0
    frame = store.series("fred:UNRATE")
    assert list(frame.columns) == ["date", "period", "value"]
    assert list(frame["period"]) == ["2026-04", "2026-05"]
    assert list(frame["value"]) == [4.0, 4.1]
    assert frame["date"].iloc[-1] == pd.Timestamp("2026-05-31")


def test_reading_needs_no_catalog_no_key_and_no_network(world, tmp_path):
    store, server, _ = world
    store.sync()
    calls = server.requests
    reader = Store(tmp_path / "store")
    assert list(reader.series("fred:UNRATE")["value"]) == [4.0, 4.1]
    assert server.requests == calls


def test_an_alias_reads_the_same_series(world):
    store, _, _ = world
    store.sync()
    pd.testing.assert_frame_equal(store.series("usa.empleo.desempleo"), store.series("fred:UNRATE"))


@pytest.mark.parametrize(("name", "message"), [("fred:NOPE", "key 'fred:NOPE'"), ("nope", "alias 'nope'")])
def test_an_unknown_name_raises(world, name, message):
    store, _, _ = world
    store.sync()
    with pytest.raises(UnknownSeriesError, match=message):
        store.series(name)


def test_as_of_returns_the_value_known_then_and_revisions_show_both(world):
    store, server, clock = world
    store.sync()
    clock.now = NOW + datetime.timedelta(days=28)
    server.observations["UNRATE"]["2026-05-01"] = "4.2"
    report = store.sync()
    assert report.sources[0].revised == 1
    assert list(store.series("fred:UNRATE")["value"]) == [4.0, 4.2]
    assert list(store.series("fred:UNRATE", as_of="2026-06-15")["value"]) == [4.0, 4.1]
    assert store.series("fred:UNRATE", as_of="2026-01-01").empty
    history = store.revisions("fred:UNRATE")
    assert list(history["period"]) == ["2026-04", "2026-05", "2026-05"]
    assert list(history["value"]) == [4.0, 4.1, 4.2]
    assert "key" not in history.columns


def test_missing_values_and_open_periods_are_left_out(world):
    store, server, _ = world
    server.observations["UNRATE"]["2026-03-01"] = "."
    server.observations["UNRATE"]["2026-06-01"] = "4.3"
    store.sync()
    assert list(store.series("fred:UNRATE")["period"]) == ["2026-04", "2026-05"]
    with_projections = store.series("fred:UNRATE", projections=True)
    assert list(with_projections["period"]) == ["2026-04", "2026-05", "2026-06"]
    assert list(with_projections["projection"]) == [False, False, True]
    assert len(store.revisions("fred:UNRATE")) == 4


def test_start_and_end_filter_by_date(world):
    store, _, _ = world
    store.sync()
    assert list(store.series("fred:UNRATE", start="2026-05-01")["period"]) == ["2026-05"]
    assert list(store.series("fred:UNRATE", end=datetime.date(2026, 4, 30))["period"]) == ["2026-04"]


def test_frame_joins_on_date_and_leaves_gaps(world):
    store, _, _ = world
    store.sync()
    frame = store.frame(["fred:UNRATE", "fred:DGS10"])
    assert list(frame.columns) == ["fred:UNRATE", "fred:DGS10"]
    assert len(frame) == 4
    assert frame["fred:UNRATE"].notna().sum() == 2
    assert pd.isna(frame.loc[pd.Timestamp("2026-06-05"), "fred:UNRATE"])
    assert store.frame([]).empty


def test_info_and_its_label(world):
    store, _, _ = world
    store.sync()
    info = store.info("usa.empleo.desempleo")
    assert (info.key, info.alias, info.name) == ("fred:UNRATE", "usa.empleo.desempleo", "Unemployment Rate")
    assert (info.units, info.frequency, info.seasonal_adjustment, info.status) == ("Percent", "M", "SA", "ok")
    assert info.label == "[FRED: UNRATE, 2026-05, fetched 2026-06-06]"
    assert store.info("fred:DGS10").alias == ""


def test_status_reports_ok_stale_failed_and_missing(world):
    store, _, clock = world
    store.add("fred", ["NOPE"])
    store.sync()
    store.add("fred", ["LATER"])
    clock.now = NOW + datetime.timedelta(days=20)
    status = store.status().set_index("key")
    assert list(status.columns) == ["alias", "source", "last_period", "last_fetched_at", "state", "reason"]
    assert status.loc["fred:UNRATE", "state"] == "ok"
    assert status.loc["fred:DGS10", "state"] == "stale"
    assert status.loc["fred:DGS10", "reason"] == "last data 2026-06-05 (21 days ago)"
    assert status.loc["fred:NOPE", "state"] == "failed"
    assert status.loc["fred:LATER", "state"] == "missing"


def test_index_returns_the_whole_index(world):
    store, _, _ = world
    store.sync()
    assert sorted(store.index()["key"]) == ["fred:DGS10", "fred:UNRATE"]


def test_sync_can_be_restricted(world):
    store, server, _ = world
    store.sync(keys=["fred:DGS10"])
    assert sorted(store.index()["key"]) == ["fred:DGS10"]
    assert server.requests == 2
    assert store.sync(sources=["bls"]).sources == ()


def test_add_with_an_unknown_source_fails_at_sync(world):
    store, _, _ = world
    store.add("nowhere", ["X"])
    with pytest.raises(CatalogError, match="unknown source 'nowhere'"):
        store.sync()


def test_a_missing_key_fails_every_series_with_a_clear_reason(world, tmp_path):
    _, server, clock = world
    store = Store(
        tmp_path / "other",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / "absent.env",
        clock=clock,
        transport=httpx.MockTransport(server),
    )
    report = store.sync()
    assert report.exit_code == 1
    assert report.lines() == ["[x]   fred      key_error: FRED_API_KEY missing in .env"]
    assert server.requests == 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/api_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.api'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/api.py`:

```python
"""The Store facade: the one object other code imports.

    store = Store("D:/data")                       # read only: no keys, no catalog, no network
    store.series("fred:UNRATE")
    store = Store("D:/data", catalog="catalog.yaml")
    store.sync()

Reading never touches the network. A missing key raises; nothing is filled in.
"""

import dataclasses
import datetime
import pathlib
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx
import pandas as pd

from data_pipeline.store import sources as source_registry
from data_pipeline.store import storage as st
from data_pipeline.store.catalog import build_entries, load_catalog
from data_pipeline.store.errors import UnknownSeriesError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import ENV_FILE, load_credentials
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
        env_file: str | pathlib.Path | None = None,
        clock: Callable[[], datetime.datetime] = _utc_now,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._storage = st.Storage(pathlib.Path(root))
        self._entries: list[CatalogEntry] = list(load_catalog(pathlib.Path(catalog))) if catalog else []
        self._env_file = pathlib.Path(env_file) if env_file else ENV_FILE
        self._clock = clock
        self._transport = transport
        self._sleep = sleep

    # -- writing -----------------------------------------------------------------------------

    def add(
        self,
        source: str,
        ids: Sequence[object],
        *,
        alias: Mapping[str, str] | None = None,
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
        """Download what the catalog declares and store what changed."""
        credentials = load_credentials(self._env_file)
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
            )

    # -- reading -----------------------------------------------------------------------------

    def index(self) -> pd.DataFrame:
        """One row per stored series: provenance and last result."""
        return self._storage.read_index()

    def _row(self, name: str) -> dict[str, Any]:
        index = self._storage.read_index()
        column = "key" if ":" in name else "alias"
        found = index[index[column] == name]
        if found.empty:
            msg = f"no stored series has {column} {name!r}"
            raise UnknownSeriesError(msg)
        return st.records(found)[0]

    def _observations(self, name: str) -> pd.DataFrame:
        row = self._row(name)
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

    def revisions(self, key: str) -> pd.DataFrame:
        """Every stored version of every period, oldest fetch first within a period."""
        stored = self._observations(key)
        ordered = stored.sort_values(["date", "fetched_at"], kind="stable")
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
        )

    def status(self) -> pd.DataFrame:
        """Freshness per series: ok, stale, missing or failed."""
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
    if days > stale_after(Frequency(str(row["frequency"])), override):
        return STATE_STALE, f"last data {last.isoformat()} ({days} days ago)"
    return STATE_OK, ""
```

- [ ] **Step 4: Export `Store` from the package**

Replace `src/data_pipeline/__init__.py` with:

```python
"""finport-datapipeline: local store of public economic and financial data."""

from data_pipeline.store.api import Store

__all__ = ["Store"]
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/api_test.py -q`
Expected: `16 passed`

- [ ] **Step 6: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 7: Commit**

```bash
git add src/data_pipeline/__init__.py src/data_pipeline/store/api.py tests/unit/store/api_test.py
git commit -m "feat(store): add the Store facade with as-of reads and provenance"
```

### Task 13: The console commands

`sync`, `status` and `show`, as a click group of their own. They are *not* added to the existing `data_pipeline.equity` command group: that would make the equity engine import the store. Sub-project 0 (the rename) merges the two command sets.

**Files:**
- Create: `src/data_pipeline/store/cli.py`
- Create: `src/data_pipeline/store/__main__.py`
- Modify: `pyproject.toml`
- Test: `tests/unit/store/cli_test.py`

- [ ] **Step 1: Write the failing test**

The tests replace `cli.open_store` with one that builds the `Store` on a fake FRED server and a fixed clock. One series name contains accented letters and an arrow, to check that the output stays ASCII.

Create `tests/unit/store/cli_test.py`:

```python
import datetime
import json

import click.testing
import httpx
import pytest

from data_pipeline.store import cli as cli_module
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.sync import LOCK_FILE

from .helpers import NOW

META = {
    "title": "Tasa de desempleo áéí →",
    "units": "Percent",
    "frequency_short": "M",
    "seasonal_adjustment_short": "SA",
}
OBSERVATIONS = [{"date": "2026-04-01", "value": "4.0"}, {"date": "2026-05-01", "value": "4.1"}]


def handler(request):
    if request.url.params["series_id"] != "UNRATE":
        return httpx.Response(400, text='{"error_message":"Bad Request.  The series does not exist."}')
    if request.url.path.endswith("/observations"):
        return httpx.Response(200, text=json.dumps({"observations": OBSERVATIONS}))
    return httpx.Response(200, text=json.dumps({"seriess": [META]}))


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """A folder with a catalog and a key; the CLI talks to a fake FRED at a fixed time."""
    (tmp_path / "catalog.yaml").write_text("- source: fred\n  ids: [UNRATE]\n", encoding="utf-8")
    (tmp_path / ".env").write_text("FRED_API_KEY=0123456789abcdef0123456789abcdef\n", encoding="utf-8")
    state = {"now": NOW}

    def open_store(root, catalog, env_file):
        return Store(
            root,
            catalog,
            env_file=env_file or tmp_path / ".env",
            clock=lambda: state["now"],
            transport=httpx.MockTransport(handler),
            sleep=lambda _seconds: None,
        )

    monkeypatch.setattr(cli_module, "open_store", open_store)
    monkeypatch.chdir(tmp_path)
    return tmp_path, state


def invoke(*arguments, env=None):
    return click.testing.CliRunner().invoke(cli, list(arguments), env=env)


def test_sync_prints_the_report_and_exits_zero(workspace):
    root, _ = workspace
    result = invoke("sync", "--root", str(root / "store"))
    assert result.exit_code == 0
    assert result.output == "[ok]  fred      1 series, 2 new, 0 revised, 2 calls\n"


def test_the_root_can_come_from_the_environment(workspace):
    root, _ = workspace
    result = invoke("sync", env={"DATA_PIPELINE_ROOT": str(root / "store")})
    assert result.exit_code == 0
    assert (root / "store" / "index.parquet").exists()


def test_without_a_root_the_command_is_refused(workspace):
    result = invoke("sync")
    assert result.exit_code == 2
    assert "--root" in result.output


def test_failures_exit_one(workspace):
    root, _ = workspace
    (root / "catalog.yaml").write_text("- source: fred\n  ids: [UNRATE, NOPE]\n", encoding="utf-8")
    result = invoke("sync", "--root", str(root / "store"))
    assert result.exit_code == 1
    assert "[x]   fred      1 of 2 series" in result.output
    assert "fred:NOPE  not_found:" in result.output


def test_a_missing_key_is_a_clear_message_without_a_traceback(workspace):
    root, _ = workspace
    result = invoke("sync", "--root", str(root / "store"), "--env-file", str(root / "absent.env"))
    assert result.exit_code == 1
    assert result.output == "[x]   fred      key_error: FRED_API_KEY missing in .env\n"
    assert "Traceback" not in result.output


def test_configuration_errors_exit_two(workspace):
    root, _ = workspace
    missing = invoke("sync", "--root", str(root / "store"), "--catalog", str(root / "absent.yaml"))
    assert missing.exit_code == 2
    assert "the catalog file does not exist" in missing.output
    (root / "store").mkdir()
    (root / "store" / LOCK_FILE).write_text("1", encoding="ascii")
    locked = invoke("sync", "--root", str(root / "store"))
    assert locked.exit_code == 2
    assert "Delete it by hand" in locked.output


def test_status_lists_each_series_and_exits_by_freshness(workspace):
    root, state = workspace
    invoke("sync", "--root", str(root / "store"))
    fresh = invoke("status", "--root", str(root / "store"))
    assert fresh.exit_code == 0
    assert fresh.output == "[ok]  fred:UNRATE  2026-05  fetched 2026-06-06\n"
    state["now"] = NOW + datetime.timedelta(days=200)
    stale = invoke("status", "--root", str(root / "store"))
    assert stale.exit_code == 1
    assert stale.output.startswith("[x]   fred:UNRATE  stale  last data 2026-05-31")


def test_status_with_a_catalog_reports_missing_series(workspace):
    root, _ = workspace
    result = invoke("status", "--root", str(root / "store"), "--catalog", str(root / "catalog.yaml"))
    assert result.exit_code == 1
    assert result.output == "[x]   fred:UNRATE  missing  declared in the catalog, never downloaded\n"


def test_show_prints_the_citation_and_the_last_rows_in_ascii(workspace):
    root, _ = workspace
    invoke("sync", "--root", str(root / "store"))
    result = invoke("show", "fred:UNRATE", "--root", str(root / "store"))
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[FRED: UNRATE, 2026-05, fetched 2026-06-06]"
    assert lines[1] == "Tasa de desempleo ??? ? | Percent | M | SA"
    assert lines[2:] == ["2026-04  4.0", "2026-05  4.1"]
    assert result.output.isascii()


def test_show_as_of_and_unknown_keys(workspace):
    root, _ = workspace
    invoke("sync", "--root", str(root / "store"))
    before = invoke("show", "fred:UNRATE", "--root", str(root / "store"), "--as-of", "2026-01-01")
    assert before.output.splitlines()[2:] == []
    unknown = invoke("show", "fred:NOPE", "--root", str(root / "store"))
    assert unknown.exit_code == 2
    assert unknown.output == "[x]   no stored series has key 'fred:NOPE'\n"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/cli_test.py -q`
Expected: collection error, `ImportError: cannot import name 'cli' from 'data_pipeline.store'`

- [ ] **Step 3: Write the implementation**

Create `src/data_pipeline/store/cli.py`:

```python
"""Console commands of the store: sync, status, show. Output is ASCII only.

Exit codes: 0 everything is up to date, 1 there were failures, 2 configuration error,
3 incomplete because of a quota (run again tomorrow).
"""

import pathlib

import click

from data_pipeline.store.api import STATE_OK, Store
from data_pipeline.store.errors import StoreError
from data_pipeline.store.sync import EXIT_CONFIGURATION, EXIT_FAILURES, EXIT_OK

ROOT_VARIABLE = "DATA_PIPELINE_ROOT"
SHOWN_ROWS = 10
PATH = click.Path(path_type=pathlib.Path)

root_option = click.option(
    "--root",
    envvar=ROOT_VARIABLE,
    required=True,
    type=PATH,
    help=f"Folder of the store. Defaults to the {ROOT_VARIABLE} environment variable.",
)


def echo(line: str) -> None:
    """Print one line, replacing anything outside ASCII (Windows consoles default to cp1252)."""
    click.echo(line.encode("ascii", "replace").decode("ascii"))


def open_store(root: pathlib.Path, catalog: pathlib.Path | None, env_file: pathlib.Path | None) -> Store:
    return Store(root, catalog, env_file=env_file)


def fail(error: Exception) -> SystemExit:
    echo(f"[x]   {error}")
    return SystemExit(EXIT_CONFIGURATION)


@click.group()
def cli() -> None:
    """finport-datapipeline: local store of public economic data."""


@cli.command("sync")
@root_option
@click.option("--catalog", type=PATH, default=pathlib.Path("catalog.yaml"), show_default=True, help="YAML catalog.")
@click.option("--source", "sources", multiple=True, help="Sync only this source. Repeatable.")
@click.option("--full", is_flag=True, help="Ask for every series' whole history again; store only what changed.")
@click.option("--env-file", type=PATH, default=None, help="File with the credentials. Defaults to ./.env")
def sync_command(
    *,
    root: pathlib.Path,
    catalog: pathlib.Path,
    sources: tuple[str, ...],
    full: bool,
    env_file: pathlib.Path | None,
) -> None:
    """Download what the catalog declares and store what changed."""
    try:
        report = open_store(root, catalog, env_file).sync(sources=sources or None, full=full)
    except StoreError as exc:
        raise fail(exc) from exc
    for line in report.lines():
        echo(line)
    raise SystemExit(report.exit_code)


@cli.command("status")
@root_option
@click.option("--catalog", type=PATH, default=None, help="YAML catalog; needed to report 'missing' series.")
def status_command(*, root: pathlib.Path, catalog: pathlib.Path | None) -> None:
    """Freshness of every stored series."""
    try:
        table = open_store(root, catalog, None).status()
    except StoreError as exc:
        raise fail(exc) from exc
    for row in table.to_dict(orient="records"):
        if row["state"] == STATE_OK:
            fetched = row["last_fetched_at"].date().isoformat()
            echo(f"[ok]  {row['key']}  {row['last_period']}  fetched {fetched}")
        else:
            echo(f"[x]   {row['key']}  {row['state']}  {row['reason']}")
    all_ok = bool((table["state"] == STATE_OK).all())
    raise SystemExit(EXIT_OK if all_ok else EXIT_FAILURES)


@cli.command("show")
@root_option
@click.argument("key")
@click.option("--as-of", "as_of", default=None, help="Show the series as it was known on this date (YYYY-MM-DD).")
def show_command(*, root: pathlib.Path, key: str, as_of: str | None) -> None:
    """Print a series' citation and its last observations."""
    try:
        store = open_store(root, None, None)
        info = store.info(key)
        rows = store.series(key, as_of=as_of)
    except StoreError as exc:
        raise fail(exc) from exc
    echo(info.label)
    echo(f"{info.name} | {info.units} | {info.frequency} | {info.seasonal_adjustment}")
    for row in rows.tail(SHOWN_ROWS).to_dict(orient="records"):
        echo(f"{row['period']}  {row['value']}")
```

Create `src/data_pipeline/store/__main__.py`:

```python
"""Run the store commands without the console script:  python -m data_pipeline.store sync ..."""

from data_pipeline.store.cli import cli

cli()
```

- [ ] **Step 4: Register the console script**

In `pyproject.toml`, under `[project.scripts]`, add the second line:

```toml
"data_pipeline.equity" = "data_pipeline.equity:cli"
"data-pipeline" = "data_pipeline.store.cli:cli"
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/cli_test.py -q`
Expected: `10 passed`

- [ ] **Step 6: Run the command by hand**

Run: `PYTHONPATH=src .venv/Scripts/python -m data_pipeline.store --help`
Expected: a usage text listing the commands `show`, `status` and `sync`.

- [ ] **Step 7: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check src/data_pipeline tests/unit/store && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/data_pipeline/store/cli.py src/data_pipeline/store/__main__.py tests/unit/store/cli_test.py
git commit -m "feat(store): add the sync, status and show commands"
```

### Task 14: The live test and the packaging check

**Files:**
- Create: `tests/live/__init__.py`
- Create: `tests/live/fred_live_test.py`

- [ ] **Step 1: Write the live test**

Create `tests/live/__init__.py` as an empty file.

Create `tests/live/fred_live_test.py`. *This test was not run while the plan was written*: it needs a key and the network.

```python
"""One real call to FRED. Off by default; run with:  pytest -m live tests/live

Needs FRED_API_KEY in the environment or in ./.env, and network access. It exists to notice
when FRED changes the shape of its answers; the unit suite cannot see that.
"""

import pytest

from data_pipeline.store import keys
from data_pipeline.store.api import Store
from data_pipeline.store.keys import load_credentials


@pytest.mark.live
def test_fred_still_answers_in_the_expected_shape(tmp_path):
    if load_credentials().get(keys.FRED) is None:
        pytest.skip("FRED_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("fred", ["UNRATE"])
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  fred      1 series")
    info = store.info("fred:UNRATE")
    assert (info.frequency, info.units, info.seasonal_adjustment) == ("M", "Percent", "SA")
    series = store.series("fred:UNRATE")
    assert len(series) > 900
    assert series["period"].iloc[0] == "1948-01"
```

- [ ] **Step 2: Check that the default run leaves it out**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1100 passed, 1 deselected`

- [ ] **Step 3: Run it for real**

This sends two requests to FRED with the key in `FRED_API_KEY` (environment or `./.env`). If no key is available, ask the user for one or skip this step and say so in the hand-off; do not invent a key.

Run: `.venv/Scripts/python -m pytest -m live tests/live -q`
Expected: `1 passed` (or `1 skipped` with the reason `FRED_API_KEY is not set`)

If it fails on the frequency, units or first period, FRED changed its answer: compare the real answer with `tests/unit/store/fixtures/fred_series.json`, update the fixture and the source together, and rerun the unit tests.

- [ ] **Step 4: Check that the wheel ships `data_pipeline`**

*Not run while the plan was written.* `[tool.pdm.build]` already includes all of `src`, so the new package should be picked up without configuration.

```bash
uv build --wheel --out-dir dist-check
.venv/Scripts/python -m zipfile -l dist-check/*.whl | grep -E "data_pipeline/store/(api|cli)\.py"
unzip -p dist-check/*.whl "*.dist-info/entry_points.txt"
rm -rf dist-check
```

Expected: two lines naming `data_pipeline/store/api.py` and `data_pipeline/store/cli.py`, and an `entry_points.txt` that contains `data-pipeline = data_pipeline.store.cli:cli`.

If the two files are missing from the wheel, add `"src/data_pipeline"` to `includes` in `[tool.pdm.build]`, rebuild and check again.

- [ ] **Step 5: Lint and commit**

Run: `.venv/Scripts/python -m ruff check tests/live`
Expected: `All checks passed!`

```bash
git add tests/live
git commit -m "test(store): add an opt-in live test against FRED"
```

### Task 15: Documentation

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add a README section**

In `README.md`, immediately before the line `## Running on Local Python`, insert:

````markdown
## Public-data store (preview)

A second engine lives beside the ticker pipeline: `data_pipeline.store`, a local store of public
economic series. It downloads full history the first time, then only what is new, and it never
overwrites a value: a revision adds a row, so a series can be read as it was known on a past date.
FRED is the first source.

Declare what you want in a YAML catalog:

```yaml
- source: fred
  ids: [UNRATE, DGS10]
  alias:
    UNRATE: usa.empleo.desempleo
```

Put `FRED_API_KEY=...` in a `.env` file in the folder you run from, then:

```
python -m data_pipeline.store sync --root D:/data --catalog catalog.yaml
python -m data_pipeline.store status --root D:/data
python -m data_pipeline.store show fred:UNRATE --root D:/data
```

Exit codes of `sync`: 0 up to date, 1 failures, 2 configuration error, 3 incomplete because of a
quota. From Python, reading needs no key and no network:

```python
import data_pipeline as dp

store = dp.Store("D:/data")
store.series("fred:UNRATE")                      # date, period, value
store.series("fred:UNRATE", as_of="2026-06-15")  # as it was known that day
store.frame(["fred:UNRATE", "fred:DGS10"])       # one column per series
store.info("fred:UNRATE").label                  # "[FRED: UNRATE, 2026-05, fetched 2026-06-06]"
```

The same FRED terms of use described above apply.

````

- [ ] **Step 2: Add a CHANGELOG entry**

In `CHANGELOG.md`, as the first bullet under `## [Unreleased]` / `### Added`, insert:

```markdown
- Public-data store (preview): a new package `data_pipeline` with `data_pipeline.store`, a local parquet store of public economic series that is independent of the ticker pipeline. A YAML catalog declares the series; `sync` loads full history the first time and afterwards asks only from a revision window; observations are append-only, so revisions are kept and `Store.series(key, as_of=...)` returns a series as it was known on a date. Includes per-source pacing and retries, credentials scrubbed from every message, a run report with exit codes (0 ok, 1 failures, 2 configuration, 3 quota), freshness status per series and the commands `sync`, `status` and `show` (`python -m data_pipeline.store`, or the `data-pipeline` script). First source: **FRED** (`FRED_API_KEY`).
```

- [ ] **Step 3: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs(store): document the public-data store preview"
```

### Task 16: Full verification and the acceptance run

**Files:**
- Create: `scripts/compare_fred_with_investment_process.py`

- [ ] **Step 1: Run every check**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1100 passed, 1 deselected`

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 2: Write the comparison script**

Acceptance criterion 1 of the spec: a full load of the 33 FRED series in Investment_Process's catalog gives values identical to its store on the same day. The script compares the two stores series by series. Its logic was checked while writing the plan against a store seeded from Investment_Process's own parquet files (33 series, 132,438 observations, every series matched); it was not run against a store filled from real FRED.

Create `scripts/compare_fred_with_investment_process.py`:

```python
"""Acceptance check: the store's FRED values against Investment_Process's own store.

    python scripts/compare_fred_with_investment_process.py C:/Proyectos/Investment_Process D:/datos

Sync both stores on the same day first. Exit code 0 when every series matches, 1 otherwise.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline

TOLERANCE = 1e-9


def compare(origin: pathlib.Path, root: pathlib.Path) -> int:
    public = origin / "inputs" / "publicos"
    series = pd.read_parquet(public / "series.parquet")
    theirs_all = pd.read_parquet(public / "obs" / "fred.parquet")
    store = data_pipeline.Store(root)
    failures = 0
    for row in series[series["fuente"] == "fred"].sort_values("id_fuente").to_dict(orient="records"):
        rows = theirs_all[theirs_all["clave"] == row["clave"]]
        rows = rows[rows["valor"].notna() & ~rows["proyeccion"].astype(bool)]
        theirs = rows.set_index("periodo")["valor"]
        ours = store.series(f"fred:{row['id_fuente']}").set_index("period")["value"]
        common = theirs.index.intersection(ours.index)
        gap = (theirs[common] - ours[common]).abs()
        limit = TOLERANCE * pd.concat([theirs[common].abs(), ours[common].abs()], axis=1).max(axis=1)
        different = int((gap > limit).sum())
        only_theirs = len(theirs.index.difference(ours.index))
        only_ours = len(ours.index.difference(theirs.index))
        ok = different == 0 and only_theirs == 0 and only_ours == 0
        failures += 0 if ok else 1
        print(
            f"[{'ok' if ok else 'x'}] {row['id_fuente']:<14} {len(common)} periods in common, "
            f"{different} different, {only_theirs} only theirs, {only_ours} only ours"
        )
    print(f"{failures} series differ" if failures else "every series matches")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])))
```

- [ ] **Step 3: Load the 33 series from real FRED**

This step calls FRED 66 times with the user's key and writes outside the repository. Confirm the folder with the user before running; `D:/datos` below is an example.

Create a catalog file outside the repository, for example `D:/datos/catalog.yaml`:

```yaml
- source: fred
  ids: [BAMLH0A0HYM2, BOPGSTB, CPIAUCSL, CPILFESL, DCOILBRENTEU, DCOILWTICO, DFEDTARU, DFII10,
        DGS10, DGS2, DGS30, DGS3MO, DHHNGSP, DTWEXBGS, FEDFUNDS, GDPC1, GFDEGDQ188S, HOUST, ICSA,
        INDPRO, M2SL, MTSDS133FMS, PAYEMS, PCEPILFE, RSAFS, SOFR, T10Y2Y, T10YIE, TCU, TOTALSA,
        UMCSENT, UNRATE, WALCL]
```

Run (from the repository, with `FRED_API_KEY` in the environment or in `./.env`):

```bash
PYTHONPATH=src .venv/Scripts/python -m data_pipeline.store sync --root D:/datos/store --catalog D:/datos/catalog.yaml
```

Expected: one line, `[ok]  fred      33 series, <about 132000> new, 0 revised, 66 calls`, exit code 0.

Run it a second time.
Expected: `[ok]  fred      33 series, 0 new, 0 revised, 66 calls`. A handful of new or revised observations is legitimate if FRED published between the two runs.

- [ ] **Step 4: Refresh Investment_Process's store the same day**

In `C:/Proyectos/Investment_Process`, with that project's own environment:

```bash
python -m investment_tools.macro.publicos actualizar --fuente fred --sin-comercio
```

- [ ] **Step 5: Compare**

```bash
PYTHONPATH=src .venv/Scripts/python scripts/compare_fred_with_investment_process.py C:/Proyectos/Investment_Process D:/datos/store
```

Expected: 33 lines starting with `[ok]`, then `every series matches`, exit code 0.

If a series differs, look before changing anything. `only ours` periods usually mean Investment_Process has not been refreshed today; `different` values on the latest periods usually mean a revision published between the two runs. A difference on old periods, or the same difference on every series, is a defect: stop and report it with the script's output.

- [ ] **Step 6: Commit**

```bash
git add scripts/compare_fred_with_investment_process.py
git commit -m "chore(store): add the acceptance comparison against Investment_Process"
```

- [ ] **Step 7: Report**

State plainly which of the spec's seven acceptance criteria were verified and how: criteria 2 to 6 by the test suite, criterion 7 by Task 14 Step 4, criterion 1 by Step 5 above. If Steps 3 to 5 or the live test were skipped for lack of a key, say so; do not report them as done.
