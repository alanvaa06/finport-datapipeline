# Store direct sources, group B1 (World Bank, SDMX, macro catalog moved) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read the World Bank, the BIS, the ECB, Eurostat, the OECD and the IMF directly, and point the bundled macro catalog at them wherever a DBnomics id translates mechanically (762 of 1,262 series).

**Architecture:** Two new files under `store/sources/`: `worldbank.py` (one call per indicator for every requested economy) and `sdmx.py` (one class, five registered providers, one series per call). One id stays one series, so the sync engine does not change. The catalog conversion script gains a translation step and an exclusions file as a safety net.

**Tech Stack:** Python 3.12+, httpx, pandas, PyYAML; pytest, ruff, mypy. No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-05-store-direct-sources-b1-design.md`

---

## Before you start

**How this plan was checked.** Every file below was run in a scratch clone on 2026-10-05: 1,239 tests passed (1,175 that existed plus 64 new), `ruff check` and `mypy` clean. The seven live tests of Task 5 were run against the real services and passed. Every id translation was first tried by hand against its origin (World Bank, BIS, ECB, each Eurostat dataset of the catalog, both OECD dataflows, IMF WEO). Not run: Task 8, the full acceptance sync.

**Environment.** Windows, commands for Git Bash, run from `C:/Proyectos/data_pipeline`, virtual environment `.venv` (uv). Call `.venv/Scripts/python`, never a bare `python`.

**Rules of this repository.** Commits carry no `Co-Authored-By` line. Ruff is strict (messages in a `msg` variable, exception names end in `Error`, no boolean positional parameters, lines up to 120). Console output is ASCII only. `data_pipeline.store` and `data_pipeline.equity` never import each other.

## File structure

| File | Responsibility |
|---|---|
| `src/data_pipeline/store/periods.py` | Modified: `infer_frequency`, the frequency a period text is written in |
| `src/data_pipeline/store/sources/base.py` | Modified: `:` is a missing value (Eurostat) |
| `src/data_pipeline/store/sources/worldbank.py` | New: the World Bank source |
| `src/data_pipeline/store/sources/sdmx.py` | New: one SDMX class, five providers |
| `src/data_pipeline/store/sources/__init__.py` | Modified: six more sources and their citation titles |
| `scripts/convert_macro_catalog.py` | Modified: DBnomics ids pointed at their origin |
| `scripts/macro_direct_exclusions.txt` | New: ids that stay on DBnomics |
| `src/data_pipeline/store/catalogs/macro.yaml` | Regenerated |
| `scripts/macro_freshness.py`, `scripts/compare_direct_with_dbnomics.py` | New: acceptance |
| `tests/unit/store/*_test.py`, `tests/live/direct_sources_live_test.py` | Tests |

---

### Task 1: Read the frequency from how a period is written

SDMX providers and the World Bank do not need a declared frequency: `2025` is a year, `2026-Q1` a quarter, `2026-05` a month, `2026-09-23` a day. Eurostat writes a missing value as `:`.

**Files:**
- Modify: `src/data_pipeline/store/periods.py`, `src/data_pipeline/store/sources/base.py`
- Test: `tests/unit/store/periods_test.py`, `tests/unit/store/base_test.py`

- [ ] **Step 1: Write the failing tests**

Replace `tests/unit/store/periods_test.py` with (the last two tests are new):

```python
import datetime

import pytest

from data_pipeline.store.errors import PeriodError
from data_pipeline.store.model import Frequency
from data_pipeline.store.periods import infer_frequency, read_period


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


@pytest.mark.parametrize(
    ("text", "frequency"),
    [
        ("2025", Frequency.ANNUAL),
        ("2026-Q1", Frequency.QUARTERLY),
        ("2026Q1", Frequency.QUARTERLY),
        ("2026-05", Frequency.MONTHLY),
        ("2026-M05", Frequency.MONTHLY),
        ("2026M05", Frequency.MONTHLY),
        (" 2026-09-23 ", Frequency.DAILY),
        ("2026-09-23T00:00:00", Frequency.DAILY),
        ("2026-W05", None),
        ("2026-S1", None),
        ("soon", None),
        ("", None),
    ],
)
def test_the_frequency_is_inferred_from_how_a_period_is_spelled(text, frequency):
    assert infer_frequency(text) is frequency


@pytest.mark.parametrize("text", ["2025", "2026-Q1", "2026Q1", "2026-05", "2026-M05", "2026M05", "2026-09-23"])
def test_whatever_infer_frequency_accepts_read_period_can_read(text):
    label, day = read_period(text, infer_frequency(text))
    assert label
    assert day.year in (2025, 2026)
```

In `tests/unit/store/base_test.py`, append at the end of the file:

```python
def test_a_colon_is_a_missing_value():
    assert math.isnan(number(":"))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/periods_test.py tests/unit/store/base_test.py -q`
Expected: a collection error in `periods_test.py` (`ImportError: cannot import name 'infer_frequency'`) and one failure in `base_test.py` (`ValueError: could not convert string to float: ':'`).

- [ ] **Step 3: Write the implementation**

Replace `src/data_pipeline/store/periods.py` with:

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


_SHAPES: tuple[tuple[re.Pattern[str], Frequency], ...] = (
    (re.compile(r"^\d{4}$"), Frequency.ANNUAL),
    (re.compile(r"^\d{4}-?Q[1-4]$"), Frequency.QUARTERLY),
    (re.compile(r"^\d{4}-?M?\d{2}$"), Frequency.MONTHLY),
    (re.compile(r"^\d{4}-\d{2}-\d{2}"), Frequency.DAILY),
)


def infer_frequency(text: str) -> Frequency | None:
    """The frequency a period text is written in, for sources that spell it SDMX style.

    "2025" is annual, "2026-Q1" or "2026Q1" quarterly, "2026-05", "2026-M05" or "2026M05"
    monthly, "2026-09-23" daily. Anything else (a week such as "2026-W05") is None.
    """
    stripped = text.strip()
    for shape, frequency in _SHAPES:
        if shape.match(stripped):
            return frequency
    return None
```

In `src/data_pipeline/store/sources/base.py`, the `MISSING` line becomes:

```python
MISSING = frozenset({"", ".", ":", "N/E", "NA", "NAN", "N/A", "-"})
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/periods_test.py tests/unit/store/base_test.py -q`
Expected: `56 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/periods.py src/data_pipeline/store/sources/base.py tests/unit/store
git commit -m "feat(store): infer the frequency from how a period is written"
```

### Task 2: The World Bank source

A series id is `<indicator>/<economy>`. Requests are grouped by indicator and one call brings every requested economy, so the 304 World Bank series of the macro catalog cost seven calls. When the API refuses a call because one economy is invalid, each economy is asked on its own.

**Files:**
- Create: `src/data_pipeline/store/sources/worldbank.py`
- Create: `tests/unit/store/fixtures/worldbank_pib.json`, `tests/unit/store/fixtures/worldbank_error.json`
- Test: `tests/unit/store/worldbank_test.py`

- [ ] **Step 0: Create the fixtures**

Both are recorded answers, copied from `Investment_Process/tests/macro/publicos/fixtures/`.

`tests/unit/store/fixtures/worldbank_pib.json`:

```json
[{"page":1,"pages":1,"per_page":20000,"total":4,"sourceid":"2","lastupdated":"2026-07-13"},[{"indicator":{"id":"NY.GDP.MKTP.CD","value":"GDP (current US$)"},"country":{"id":"MX","value":"Mexico"},"countryiso3code":"MEX","date":"2025","value":1832641364775.52,"unit":"","obs_status":"","decimal":0},{"indicator":{"id":"NY.GDP.MKTP.CD","value":"GDP (current US$)"},"country":{"id":"MX","value":"Mexico"},"countryiso3code":"MEX","date":"2024","value":1830489311088.89,"unit":"","obs_status":"","decimal":0},{"indicator":{"id":"NY.GDP.MKTP.CD","value":"GDP (current US$)"},"country":{"id":"XC","value":"Euro area"},"countryiso3code":"EMU","date":"2025","value":null,"unit":"","obs_status":"","decimal":0},{"indicator":{"id":"NY.GDP.MKTP.CD","value":"GDP (current US$)"},"country":{"id":"XX","value":"Not classified"},"countryiso3code":"","date":"2025","value":5.0,"unit":"","obs_status":"","decimal":0}]]
```

`tests/unit/store/fixtures/worldbank_error.json`:

```json
[{"message":[{"id":"120","key":"Invalid value","value":"The provided parameter value is not valid"}]}]
```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/worldbank_test.py`:

```python
import datetime
import json
import math

import httpx
import pytest

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.worldbank import WorldBank, by_indicator, split_id

from .helpers import client, entry, fixture

TODAY = datetime.date(2026, 6, 6)
GDP = "NY.GDP.MKTP.CD"


def row(economy, date, value, indicator=GDP):
    return {
        "indicator": {"id": indicator, "value": "GDP (current US$)"},
        "country": {"id": economy[:2], "value": f"Economy {economy}"},
        "countryiso3code": economy,
        "date": date,
        "value": value,
        "unit": "",
    }


def answer(rows, pages=1, page=1):
    return httpx.Response(200, json=[{"page": page, "pages": pages, "per_page": 20000, "total": len(rows)}, rows])


def refusal():
    return httpx.Response(200, text=fixture("worldbank_error.json"))


def fetch(handler, requests):
    source = WorldBank(client(handler), Credentials(), today=lambda: TODAY)
    return list(source.fetch(requests))


def wb(identifier, **fields):
    return entry(identifier, "worldbank", **fields)


def economies_of(request):
    return request.url.path.split("/country/")[1].split("/indicator/")[0]


def test_an_id_is_an_indicator_and_an_economy():
    assert split_id(wb("NY.GDP.MKTP.CD/chl")) == ("NY.GDP.MKTP.CD", "CHL")
    for bad in ("NY.GDP.MKTP.CD", "/CHL", "NY.GDP.MKTP.CD/"):
        with pytest.raises(CatalogError, match="<indicator>/<economy>"):
            split_id(wb(bad))


def test_validate_rejects_a_malformed_id_and_unknown_fields():
    source = WorldBank(client(), Credentials())
    source.validate(wb("NY.GDP.MKTP.CD/CHL"))
    with pytest.raises(CatalogError, match="<indicator>/<economy>"):
        source.validate(wb("NY.GDP.MKTP.CD"))
    with pytest.raises(CatalogError, match="for source 'worldbank': bank"):
        source.validate(wb("NY.GDP.MKTP.CD/CHL", params={"bank": "x"}))


def test_requests_are_grouped_by_indicator():
    requests = [Request(wb(f"{GDP}/CHL")), Request(wb("NE.EXP.GNFS.CD/CHL")), Request(wb(f"{GDP}/ARG"))]
    grouped = by_indicator(requests)
    assert {name: [split_id(item.entry)[1] for item in members] for name, members in grouped.items()} == {
        GDP: ["CHL", "ARG"],
        "NE.EXP.GNFS.CD": ["CHL"],
    }


def test_one_call_brings_every_economy_of_an_indicator():
    seen = []

    def handler(request):
        seen.append(request)
        return answer([row("CHL", "2025", 3.5e11), row("CHL", "2024", 3.3e11), row("ARG", "2025", None)])

    batches = fetch(handler, [Request(wb(f"{GDP}/CHL")), Request(wb(f"{GDP}/ARG"))])
    assert len(seen) == 1
    assert economies_of(seen[0]) == "CHL;ARG"
    assert seen[0].url.path.endswith(f"/indicator/{GDP}")
    assert dict(seen[0].url.params) == {"format": "json", "per_page": "20000", "page": "1"}
    chile, argentina = batches[0].series
    assert (chile.key, chile.frequency, chile.country) == (f"worldbank:{GDP}/CHL", Frequency.ANNUAL, "CHL")
    assert chile.name == "GDP (current US$) - Economy CHL"
    assert [(item.period, item.value) for item in chile.observations] == [("2024", 3.3e11), ("2025", 3.5e11)]
    assert chile.observations[1].date == datetime.date(2025, 12, 31)
    assert math.isnan(argentina.observations[0].value)


def test_the_recorded_answer_is_read_and_an_aggregate_is_matched_by_its_code():
    handler = lambda _request: httpx.Response(200, text=fixture("worldbank_pib.json"))  # noqa: E731
    batch = fetch(handler, [Request(wb(f"{GDP}/MEX")), Request(wb(f"{GDP}/EMU")), Request(wb(f"{GDP}/CHL"))])[0]
    mexico, euro_area = batch.series
    assert [item.period for item in mexico.observations] == ["2024", "2025"]
    assert (euro_area.key, euro_area.name) == (f"worldbank:{GDP}/EMU", "GDP (current US$) - Euro area")
    failure = batch.failures[0]
    assert (failure.entry.key, failure.outcome) == (f"worldbank:{GDP}/CHL", Outcome.NOT_FOUND)
    assert failure.reason == "the World Bank returned no observations for this economy"


def test_since_asks_from_the_earliest_year_only_when_every_request_has_one():
    seen = []

    def handler(request):
        seen.append(request)
        return answer([row("CHL", "2025", 1.0), row("ARG", "2025", 2.0)])

    both = [Request(wb(f"{GDP}/CHL"), datetime.date(2021, 1, 1)), Request(wb(f"{GDP}/ARG"), datetime.date(2019, 5, 1))]
    fetch(handler, both)
    assert seen[0].url.params["date"] == "2019:2026"
    fetch(handler, [both[0], Request(wb(f"{GDP}/ARG"))])
    assert "date" not in seen[1].url.params


def test_pages_are_followed_to_the_last():
    def handler(request):
        page = int(request.url.params["page"])
        return answer([row("CHL", str(2023 + page), float(page))], pages=2, page=page)

    series = fetch(handler, [Request(wb(f"{GDP}/CHL"))])[0].series[0]
    assert [item.period for item in series.observations] == ["2024", "2025"]


def test_a_refused_call_is_retried_one_economy_at_a_time_so_that_one_fails_alone():
    seen = []

    def handler(request):
        seen.append(economies_of(request))
        if "XXX" in economies_of(request):
            return refusal()
        return answer([row("CHL", "2025", 1.0)])

    batch = fetch(handler, [Request(wb(f"{GDP}/CHL")), Request(wb(f"{GDP}/XXX"))])[0]
    assert seen == ["CHL;XXX", "CHL", "XXX"]
    assert [series.key for series in batch.series] == [f"worldbank:{GDP}/CHL"]
    failure = batch.failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "The provided parameter value is not valid")


def test_quarterly_and_monthly_dates_are_read():
    handler = lambda _request: answer([row("CHL", "2025Q4", 1.0), row("CHL", "2026Q1", 2.0)])  # noqa: E731
    quarterly = fetch(handler, [Request(wb("X.Q/CHL"))])[0].series[0]
    assert (quarterly.frequency, [item.period for item in quarterly.observations]) == (
        Frequency.QUARTERLY,
        ["2025Q4", "2026Q1"],
    )
    handler = lambda _request: answer([row("CHL", "2026M02", 1.0)])  # noqa: E731
    monthly = fetch(handler, [Request(wb("X.M/CHL"))])[0].series[0]
    assert (monthly.frequency, monthly.observations[0].period) == (Frequency.MONTHLY, "2026-02")


def test_a_date_the_store_cannot_read_is_an_unsupported_frequency():
    handler = lambda _request: answer([row("CHL", "2026W05", 1.0)])  # noqa: E731
    failure = fetch(handler, [Request(wb("X.W/CHL"))])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "unsupported frequency '2026W05'")


def test_an_http_error_fails_the_indicator_and_the_next_one_continues():
    def handler(request):
        if "/indicator/BAD" in request.url.path:
            return httpx.Response(400, text="bad request")
        return answer([row("CHL", "2025", 1.0)])

    batches = fetch(handler, [Request(wb("BAD/CHL")), Request(wb(f"{GDP}/CHL"))])
    assert (batches[0].failures[0].outcome, batches[0].failures[0].reason) == (
        Outcome.SOURCE_ERROR,
        "HTTP 400: bad request",
    )
    assert batches[1].series[0].key == f"worldbank:{GDP}/CHL"


def test_a_server_error_is_a_network_error_once_the_retries_are_spent():
    failure = fetch(lambda _request: httpx.Response(503), [Request(wb(f"{GDP}/CHL"))])[0].failures[0]
    assert failure.outcome is Outcome.NETWORK_ERROR


def test_rows_without_an_economy_code_match_no_request():
    handler = lambda _request: httpx.Response(200, text=json.dumps([{"pages": 1}, [{"date": "2025"}]]))  # noqa: E731
    failure = fetch(handler, [Request(wb(f"{GDP}/CHL"))])[0].failures[0]
    assert failure.outcome is Outcome.NOT_FOUND
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/worldbank_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.worldbank'`

- [ ] **Step 3: Write the source**

Create `src/data_pipeline/store/sources/worldbank.py`:

```python
"""World Bank API v2 (WDI and its other databases). No key.

A series id is `<indicator>/<economy>`, for example `NY.GDP.MKTP.CD/CHL`. The economy is the
World Bank's own code: ISO 3166 alpha-3 for countries, and codes such as `EMU` for aggregates.

Requests are grouped by indicator: one call asks for every requested economy of that indicator
(`country/CHL;ARG/indicator/...`), so one indicator is one batch. When the API refuses the call
because one economy is invalid, each economy is asked on its own so that one fails alone.
"""

import datetime
from collections.abc import Callable, Iterator, Sequence
from typing import Any

from data_pipeline.store.errors import CatalogError, NetworkError, PeriodError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import infer_frequency, read_period
from data_pipeline.store.sources.base import (
    UNSUPPORTED_FREQUENCY,
    failures,
    number,
    reject_params,
    utc_today,
)

URL = "https://api.worldbank.org/v2/country/{economies}/indicator/{indicator}"
PER_PAGE = "20000"
OK = 200
NO_ROWS = "the World Bank returned no observations for this economy"

Row = dict[str, Any]


class _IndicatorError(Exception):
    """The call for an indicator failed for a reason that is not one bad economy."""


def split_id(entry: CatalogEntry) -> tuple[str, str]:
    """(indicator, economy) of a series id such as `NY.GDP.MKTP.CD/CHL`."""
    indicator, separator, economy = entry.source_id.rpartition("/")
    if not separator or not indicator or not economy:
        msg = f"{entry.key}: a World Bank id is '<indicator>/<economy>', such as NY.GDP.MKTP.CD/CHL"
        raise CatalogError(msg)
    return indicator, economy.upper()


def by_indicator(requests: Sequence[Request]) -> dict[str, list[Request]]:
    grouped: dict[str, list[Request]] = {}
    for request in requests:
        grouped.setdefault(split_id(request.entry)[0], []).append(request)
    return grouped


def belongs(row: Row, economy: str) -> bool:
    country = row.get("country") or {}
    return economy in (str(row.get("countryiso3code") or "").upper(), str(country.get("id") or "").upper())


class WorldBank:
    name = "worldbank"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None

    def __init__(
        self,
        client: Client,
        credentials: Credentials,  # no key: the argument exists so every source is built alike
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        del credentials
        self._client = client
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)
        split_id(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        for indicator, members in by_indicator(requests).items():
            try:
                yield self._indicator(indicator, members)
            except NetworkError as exc:
                yield FetchBatch(failures=failures(members, Outcome.NETWORK_ERROR, str(exc)))
            except _IndicatorError as exc:
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, str(exc)))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, reason))

    def _indicator(self, indicator: str, members: Sequence[Request]) -> FetchBatch:
        economies = [split_id(request.entry)[1] for request in members]
        since = None if any(request.since is None for request in members) else min(
            request.since for request in members if request.since is not None
        )
        answer = self._rows(indicator, economies, since)
        refused: dict[str, str] = {}
        if isinstance(answer, str):
            # One economy (or the indicator) is invalid: ask one by one so that it fails alone.
            rows: list[Row] = []
            for economy in economies:
                single = self._rows(indicator, [economy], since)
                if isinstance(single, str):
                    refused[economy] = single
                else:
                    rows.extend(single)
        else:
            rows = answer
        series = []
        failed = []
        for request, economy in zip(members, economies, strict=True):
            own = [row for row in rows if belongs(row, economy)]
            if not own:
                failed.append(Failure(request.entry, Outcome.NOT_FOUND, refused.get(economy, NO_ROWS)))
                continue
            result = self._series(request.entry, own)
            if isinstance(result, Failure):
                failed.append(result)
            else:
                series.append(result)
        return FetchBatch(tuple(series), tuple(failed))

    def _rows(self, indicator: str, economies: Sequence[str], since: datetime.date | None) -> list[Row] | str:
        """Every row of the indicator for these economies, or the API's message when it refuses."""
        rows: list[Row] = []
        page = 1
        while True:
            params = {"format": "json", "per_page": PER_PAGE, "page": str(page)}
            if since is not None:
                params["date"] = f"{since.year}:{self._today().year}"
            response = self._client.get(
                self.name,
                URL.format(economies=";".join(economies), indicator=indicator),
                params=params,
                per_minute=self.requests_per_minute,
            )
            if response.status_code != OK:
                msg = f"HTTP {response.status_code}: {self._client.scrub(response.text[:200])}"
                raise _IndicatorError(msg)
            payload = response.json()
            head = payload[0]
            if "message" in head:
                return "; ".join(str(item.get("value") or item) for item in head["message"])
            if len(payload) > 1 and payload[1]:
                rows.extend(payload[1])
            if page >= int(head.get("pages") or 1):
                return rows
            page += 1

    def _series(self, entry: CatalogEntry, rows: Sequence[Row]) -> SeriesData | Failure:
        text = str(rows[0]["date"])
        frequency = infer_frequency(text)
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {text!r}")
        observations = {}
        for row in rows:
            period, day = read_period(str(row["date"]), frequency)
            observations[period] = Observation(period, day, number(row.get("value")))
        first = rows[0]
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=f"{first['indicator']['value']} - {first['country']['value']}",
            frequency=frequency,
            units=str(first.get("unit") or ""),
            country=str(first.get("countryiso3code") or ""),
            observations=tuple(sorted(observations.values(), key=lambda observation: observation.date)),
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/worldbank_test.py -q`
Expected: `13 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/worldbank.py tests/unit/store
git commit -m "feat(store): add the World Bank source"
```

### Task 3: The SDMX source and the registry

One class serves the BIS, the ECB, Eurostat, the OECD and the IMF; a table holds what differs between them (address, how CSV is asked for). The key of an id must select exactly one series. Port of `Investment_Process/.../fuentes/sdmx.py`, without its one-entry-many-countries mode.

**Files:**
- Create: `src/data_pipeline/store/sources/sdmx.py`
- Modify: `src/data_pipeline/store/sources/__init__.py`
- Create: five fixtures under `tests/unit/store/fixtures/`
- Test: `tests/unit/store/sdmx_test.py`

- [ ] **Step 0: Create the fixtures**

`sdmx_bis.csv`, `sdmx_ecb.csv`, `sdmx_imf_weo.csv` and `sdmx_imf_empty.csv` are recorded answers copied from `Investment_Process/tests/macro/publicos/fixtures/` (the last one is `sdmx_imf_vacio.csv` there). `sdmx_bis_series.csv` is the two rows of one country taken from `sdmx_bis.csv`.

`tests/unit/store/fixtures/sdmx_bis.csv`:

```text
FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE
M,MX,2026-07,
M,MX,2026-08,6.5
M,XM,2026-08,2.25
M,BR,2026-08,15
```

`tests/unit/store/fixtures/sdmx_bis_series.csv`:

```text
FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE
M,MX,2026-07,
M,MX,2026-08,6.5
```

`tests/unit/store/fixtures/sdmx_ecb.csv`:

```text
KEY,FREQ,BENCHMARK_ITEM,DATA_TYPE_EST,TIME_PERIOD,OBS_VALUE
EST.B.EU000A2X2A25.WT,B,EU000A2X2A25,WT,2026-09-23,2.43
EST.B.EU000A2X2A25.WT,B,EU000A2X2A25,WT,2026-09-24,2.44
```

`tests/unit/store/fixtures/sdmx_imf_weo.csv`:

```text
DATAFLOW,COUNTRY,INDICATOR,FREQUENCY,TIME_PERIOD,OBS_VALUE,UNIT,SERIES_NAME,LATEST_ACTUAL_ANNUAL_DATA
IMF.RES:WEO(9.0.0),MEX,NGDP_RPCH,A,2024,1.350576,PT,"Gross domestic product (GDP), Constant prices, Percent change",2024
IMF.RES:WEO(9.0.0),MEX,NGDP_RPCH,A,2025,0.561683,PT,"Gross domestic product (GDP), Constant prices, Percent change",2024
IMF.RES:WEO(9.0.0),MEX,NGDP_RPCH,A,2026,1.638836,PT,"Gross domestic product (GDP), Constant prices, Percent change",2024
```

`tests/unit/store/fixtures/sdmx_imf_empty.csv`:

```text
DATAFLOW,COUNTRY,INDICATOR,FREQUENCY,TIME_PERIOD,OBS_VALUE
,,,,,
```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/sdmx_test.py`:

```python
import datetime
import math

import httpx
import pytest

from data_pipeline.store import sources
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.sdmx import PROVIDERS, Sdmx, read_csv, split_id

from .helpers import client, entry, fixture

CREDIT = "WS_TC/Q.AR.P.A.M.770.A"


def serve(name, seen=None, status=200):
    def handler(request):
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, text=fixture(name) if status == 200 else "nope")

    return handler


def fetch(provider, handler, requests):
    return list(Sdmx(provider, client(handler)).fetch(requests))


def test_the_five_providers_are_registered_as_sources():
    assert set(PROVIDERS) == {"bis", "ecb", "eurostat", "oecd", "imf"}
    for name in PROVIDERS:
        source = sources.create(name, client(), Credentials())
        assert (source.name, source.requests_per_minute, source.daily_budget) == (
            name,
            PROVIDERS[name].per_minute,
            None,
        )
        assert name in sources.TITLES


def test_an_id_is_a_flow_and_a_key_split_at_the_first_slash():
    assert split_id(entry(CREDIT, "bis")) == ("WS_TC", "Q.AR.P.A.M.770.A")
    assert split_id(entry("IMF.RES,WEO/ARG.GGXCNL_NGDP.A", "imf")) == ("IMF.RES,WEO", "ARG.GGXCNL_NGDP.A")
    for bad in ("WS_TC", "/Q.AR", "WS_TC/"):
        with pytest.raises(CatalogError, match="<flow>/<key>"):
            split_id(entry(bad, "bis"))


def test_validate_rejects_a_malformed_id_and_unknown_fields():
    source = Sdmx("bis", client())
    source.validate(entry(CREDIT, "bis"))
    with pytest.raises(CatalogError, match="<flow>/<key>"):
        source.validate(entry("WS_TC", "bis"))
    with pytest.raises(CatalogError, match="for source 'bis': column"):
        source.validate(entry(CREDIT, "bis", params={"column": "x"}))


@pytest.mark.parametrize(
    ("provider", "identifier", "url", "params"),
    [
        (
            "bis",
            CREDIT,
            "https://stats.bis.org/api/v1/data/WS_TC/Q.AR.P.A.M.770.A/all",
            {"format": "csv", "detail": "dataonly"},
        ),
        (
            "ecb",
            "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
            "https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
            {"format": "csvdata", "detail": "dataonly"},
        ),
        (
            "eurostat",
            "une_rt_m/M.SA.TOTAL.PC_ACT.T.AT",
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/une_rt_m/M.SA.TOTAL.PC_ACT.T.AT",
            {"format": "SDMX-CSV"},
        ),
        (
            "oecd",
            "OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H",
            "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H",
            {"format": "csvfile"},
        ),
        (
            "imf",
            "IMF.RES,WEO/ARG.GGXCNL_NGDP.A",
            "https://api.imf.org/external/sdmx/2.1/data/IMF.RES,WEO/ARG.GGXCNL_NGDP.A",
            {},
        ),
    ],
)
def test_each_provider_is_asked_at_its_own_address(provider, identifier, url, params):
    seen = []
    fetch(provider, serve("sdmx_bis_series.csv", seen), [Request(entry(identifier, provider))])
    assert str(seen[0].url.copy_with(query=None)) == url
    assert dict(seen[0].url.params) == params
    assert ("version=1.0.0" in seen[0].headers.get("accept", "")) == (provider == "imf")


def test_a_series_is_read_with_its_frequency_from_the_period_text():
    series = fetch("bis", serve("sdmx_bis_series.csv"), [Request(entry(CREDIT, "bis"))])[0].series[0]
    assert (series.key, series.name, series.frequency, series.units) == (f"bis:{CREDIT}", CREDIT, Frequency.MONTHLY, "")
    assert [item.period for item in series.observations] == ["2026-07", "2026-08"]
    assert math.isnan(series.observations[0].value)
    assert series.observations[1].value == 6.5
    assert series.observations[1].date == datetime.date(2026, 8, 31)


def test_daily_periods_are_read():
    series = fetch("ecb", serve("sdmx_ecb.csv"), [Request(entry("EST/B.EU000A2X2A25.WT", "ecb"))])[0].series[0]
    assert (series.frequency, [item.period for item in series.observations]) == (
        Frequency.DAILY,
        ["2026-09-23", "2026-09-24"],
    )


def test_since_becomes_a_start_period_in_years():
    seen = []
    fetch("bis", serve("sdmx_bis_series.csv", seen), [Request(entry(CREDIT, "bis"), datetime.date(2024, 5, 1))])
    assert seen[0].url.params["startPeriod"] == "2024"


def test_weo_years_after_the_latest_actual_are_projections_and_the_name_and_unit_are_kept():
    series = fetch("imf", serve("sdmx_imf_weo.csv"), [Request(entry("IMF.RES,WEO/MEX.NGDP_RPCH.A", "imf"))])[0].series[
        0
    ]
    assert series.frequency is Frequency.ANNUAL
    assert series.name == "Gross domestic product (GDP), Constant prices, Percent change"
    assert series.units == "PT"
    assert [(item.period, item.projection) for item in series.observations] == [
        ("2024", False),
        ("2025", True),
        ("2026", True),
    ]


def test_the_empty_row_of_an_unknown_imf_key_is_not_found():
    failure = fetch("imf", serve("sdmx_imf_empty.csv"), [Request(entry("IMF.RES,WEO/XXX.NOPE.A", "imf"))])[0].failures[
        0
    ]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "the query returned no observations")


def test_a_key_that_returns_several_series_is_refused():
    failure = fetch("bis", serve("sdmx_bis.csv"), [Request(entry("WS_CBPOL/M.", "bis"))])[0].failures[0]
    assert (failure.outcome, failure.reason) == (
        Outcome.SOURCE_ERROR,
        "the key returns several series: fix every dimension",
    )


def test_weekly_periods_are_an_unsupported_frequency():
    text = "FREQ,TIME_PERIOD,OBS_VALUE\nW,2026-W05,1\n"
    failure = read_csv(text, entry("X/W.1", "ecb"))
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "unsupported frequency '2026-W05'")


def test_the_unit_is_the_first_unit_column_present_and_a_colon_is_a_missing_value():
    text = "freq,unit,geo,TIME_PERIOD,OBS_VALUE\nQ,CLV20_MEUR,DE,2026-Q1,:\nQ,CLV20_MEUR,DE,2026-Q2,812.5\n"
    series = read_csv(text, entry("namq_10_gdp/Q.CLV20_MEUR.SCA.B1GQ.DE", "eurostat"))
    assert (series.units, series.frequency) == ("CLV20_MEUR", Frequency.QUARTERLY)
    assert [item.period for item in series.observations] == ["2026Q1", "2026Q2"]
    assert math.isnan(series.observations[0].value)


@pytest.mark.parametrize(
    ("status", "outcome"), [(404, Outcome.NOT_FOUND), (400, Outcome.NOT_FOUND), (418, Outcome.SOURCE_ERROR)]
)
def test_http_errors(status, outcome):
    failure = fetch("bis", serve("sdmx_bis_series.csv", status=status), [Request(entry(CREDIT, "bis"))])[0].failures[0]
    assert (failure.outcome, failure.reason) == (outcome, f"HTTP {status}: nope")


def test_one_series_failing_does_not_stop_the_next():
    def handler(request):
        if "NOPE" in request.url.path:
            return httpx.Response(404, text="nope")
        return httpx.Response(200, text=fixture("sdmx_bis_series.csv"))

    batches = fetch("bis", handler, [Request(entry("WS_TC/NOPE", "bis")), Request(entry(CREDIT, "bis"))])
    assert batches[0].failures[0].outcome is Outcome.NOT_FOUND
    assert batches[1].series[0].key == f"bis:{CREDIT}"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sdmx_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.sdmx'`

- [ ] **Step 3: Write the source**

Create `src/data_pipeline/store/sources/sdmx.py`:

```python
"""One SDMX source class for the BIS, the ECB, Eurostat, the OECD and the IMF. No key.

A series id is `<flow>/<key>`, split at the first `/`:

    bis       WS_TC/Q.AR.P.A.M.770.A
    ecb       FM/B.U2.EUR.4F.KR.MRR_FR.LEV
    eurostat  une_rt_m/M.SA.TOTAL.PC_ACT.T.AT
    oecd      OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H
    imf       IMF.RES,WEO/ARG.GGXCNL_NGDP.A

Every provider is asked for CSV and answers with the columns TIME_PERIOD and OBS_VALUE. The key
must select exactly one series: a key with an open dimension returns several and is refused.
IMF WEO years after LATEST_ACTUAL_ANNUAL_DATA are projections.
"""

import csv
import dataclasses
import io
import re
from collections.abc import Iterator, Mapping, Sequence

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import infer_frequency, read_period
from data_pipeline.store.sources.base import UNSUPPORTED_FREQUENCY, number, per_request, reject_params

OK = 200
NOT_FOUND = frozenset({400, 404})
UNIT_COLUMNS = ("UNIT_MEASURE", "UNIT", "unit")
NO_OBSERVATIONS = "the query returned no observations"
SEVERAL_SERIES = "the key returns several series: fix every dimension"
_YEAR = re.compile(r"\d{4}")  # WEO latest actual year: "2024", or fiscal "FY2024/25" -> first year


@dataclasses.dataclass(frozen=True)
class Provider:
    url: str  # with {flow} and {key}
    params: Mapping[str, str]
    per_minute: int  # assumed, not taken from the provider's documentation
    headers: Mapping[str, str] = dataclasses.field(default_factory=dict)


PROVIDERS: Mapping[str, Provider] = {
    "bis": Provider(
        "https://stats.bis.org/api/v1/data/{flow}/{key}/all",
        {"format": "csv", "detail": "dataonly"},
        30,
    ),
    "ecb": Provider(
        "https://data-api.ecb.europa.eu/service/data/{flow}/{key}",
        {"format": "csvdata", "detail": "dataonly"},
        30,
    ),
    "eurostat": Provider(
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{flow}/{key}",
        {"format": "SDMX-CSV"},
        30,
    ),
    "oecd": Provider("https://sdmx.oecd.org/public/rest/data/{flow}/{key}", {"format": "csvfile"}, 20),
    "imf": Provider(
        "https://api.imf.org/external/sdmx/2.1/data/{flow}/{key}",
        {},
        20,
        {"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"},
    ),
}


def split_id(entry: CatalogEntry) -> tuple[str, str]:
    """(flow, key) of a series id such as `WS_TC/Q.AR.P.A.M.770.A`."""
    flow, separator, key = entry.source_id.partition("/")
    if not separator or not flow or not key:
        msg = f"{entry.key}: an SDMX id is '<flow>/<key>', such as WS_TC/Q.AR.P.A.M.770.A"
        raise CatalogError(msg)
    return flow, key


def read_csv(text: str, entry: CatalogEntry) -> SeriesData | Failure:
    """The one series of an SDMX-CSV answer, or the failure the answer amounts to."""
    rows = [row for row in csv.DictReader(io.StringIO(text)) if (row.get("TIME_PERIOD") or "").strip()]
    if not rows:
        return Failure(entry, Outcome.NOT_FOUND, NO_OBSERVATIONS)
    first = rows[0]
    spelling = first["TIME_PERIOD"].strip()
    frequency = infer_frequency(spelling)
    if frequency is None:
        return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {spelling!r}")
    observations: dict[str, Observation] = {}
    for row in rows:
        period, day = read_period(row["TIME_PERIOD"], frequency)
        if period in observations:
            return Failure(entry, Outcome.SOURCE_ERROR, SEVERAL_SERIES)
        latest_actual = _YEAR.search(row.get("LATEST_ACTUAL_ANNUAL_DATA") or "")
        projection = latest_actual is not None and day.year > int(latest_actual[0])
        observations[period] = Observation(period, day, number(row.get("OBS_VALUE")), projection)
    return SeriesData(
        entry=entry,
        key=entry.key,
        name=(first.get("SERIES_NAME") or "").strip() or entry.source_id,
        frequency=frequency,
        units=next((first[column] for column in UNIT_COLUMNS if first.get(column)), ""),
        observations=tuple(sorted(observations.values(), key=lambda observation: observation.date)),
    )


class Sdmx:
    kind = Kind.SERIES
    daily_budget: int | None = None

    def __init__(self, name: str, client: Client) -> None:
        self.name = name
        self._provider = PROVIDERS[name]
        self.requests_per_minute = self._provider.per_minute
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)
        split_id(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        flow, key = split_id(entry)
        params = dict(self._provider.params)
        if request.since is not None:
            params["startPeriod"] = str(request.since.year)
        response = self._client.get(
            self.name,
            self._provider.url.format(flow=flow, key=key),
            params=params,
            headers=self._provider.headers,
            per_minute=self.requests_per_minute,
        )
        if response.status_code != OK:
            outcome = Outcome.NOT_FOUND if response.status_code in NOT_FOUND else Outcome.SOURCE_ERROR
            return Failure(entry, outcome, f"HTTP {response.status_code}: {self._client.scrub(response.text[:200])}")
        return read_csv(response.text, entry)
```

- [ ] **Step 4: Register the six sources**

Replace `src/data_pipeline/store/sources/__init__.py` with:

```python
"""Registry of sources: name -> class, and the title each one is cited with."""

from collections.abc import Callable, Mapping

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.sources.banxico import Banxico
from data_pipeline.store.sources.base import Source
from data_pipeline.store.sources.bls import Bls
from data_pipeline.store.sources.dbnomics import Dbnomics
from data_pipeline.store.sources.fred import Fred
from data_pipeline.store.sources.inegi import Inegi
from data_pipeline.store.sources.sdmx import PROVIDERS, Sdmx
from data_pipeline.store.sources.worldbank import WorldBank

Factory = Callable[[Client, Credentials], Source]


def _sdmx(name: str) -> Factory:
    """The factory of one SDMX provider. None of them takes a key."""

    def build(client: Client, credentials: Credentials) -> Source:
        del credentials
        return Sdmx(name, client)

    return build


REGISTRY: Mapping[str, Factory] = {
    "banxico": Banxico,
    "bls": Bls,
    "dbnomics": Dbnomics,
    "fred": Fred,
    "inegi": Inegi,
    "worldbank": WorldBank,
    **{name: _sdmx(name) for name in PROVIDERS},
}
TITLES: Mapping[str, str] = {
    "banxico": "Banxico SIE",
    "bis": "BIS",
    "bls": "BLS",
    "dbnomics": "DBnomics",
    "ecb": "ECB",
    "eurostat": "Eurostat",
    "fred": "FRED",
    "imf": "IMF",
    "inegi": "INEGI",
    "oecd": "OECD",
    "worldbank": "World Bank",
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

- [ ] **Step 5: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sdmx_test.py -q`
Expected: `20 passed`

- [ ] **Step 6: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 7: Commit**

```bash
git add src/data_pipeline/store/sources tests/unit/store
git commit -m "feat(store): add the SDMX source for BIS, ECB, Eurostat, OECD and IMF"
```

### Task 4: Point the macro catalog at the origins

**Files:**
- Modify: `scripts/convert_macro_catalog.py`
- Create: `scripts/macro_direct_exclusions.txt`
- Regenerate: `src/data_pipeline/store/catalogs/macro.yaml`
- Test: `tests/unit/store/macro_catalog_test.py`

- [ ] **Step 1: Write the failing test**

Replace `tests/unit/store/macro_catalog_test.py` with:

```python
"""The catalog shipped with the library: in step with its source, and valid for every source."""

import collections
import importlib.util
import json
import pathlib

import pytest

from data_pipeline.store import sources
from data_pipeline.store.catalog import check_catalog, load_catalog
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency

from .helpers import client

ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "convert_macro_catalog.py"
JSON = ROOT / "src" / "data_pipeline" / "equity" / "config_handlers" / "macro_catalog.json"
BUNDLED = ROOT / "src" / "data_pipeline" / "store" / "catalogs" / "macro.yaml"
DIRECT = ("worldbank", "bis", "eurostat", "ecb", "imf", "oecd")


def converter():
    spec = importlib.util.spec_from_file_location("convert_macro_catalog", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_bundled_catalog_is_in_step_with_the_json_and_the_exclusions_it_comes_from():
    module = converter()
    rows = json.loads(JSON.read_text(encoding="utf-8"))
    expected = module.convert(rows, module.read_exclusions())
    assert BUNDLED.read_text(encoding="utf-8") == expected, "rerun scripts/convert_macro_catalog.py"


@pytest.mark.parametrize(
    ("dbnomics_id", "expected"),
    [
        ("WB/WDI/A-NY.GDP.MKTP.CD-CHL", ("worldbank", "NY.GDP.MKTP.CD/CHL")),
        ("BIS/WS_TC/Q.AR.P.A.M.770.A", ("bis", "WS_TC/Q.AR.P.A.M.770.A")),
        ("Eurostat/une_rt_m/M.SA.TOTAL.PC_ACT.T.AT", ("eurostat", "une_rt_m/M.SA.TOTAL.PC_ACT.T.AT")),
        ("ECB/FM/B.U2.EUR.4F.KR.MRR_FR.LEV", ("ecb", "FM/B.U2.EUR.4F.KR.MRR_FR.LEV")),
        ("IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp", ("imf", "IMF.RES,WEO/ARG.GGXCNL_NGDP.A")),
        (
            "OECD/DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H",
            ("oecd", "OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H"),
        ),
        (
            "OECD/DSD_STES@DF_BTS/AUT.M.BCICP.PB.C.Y._Z._Z.N",
            ("oecd", "OECD.SDD.STES,DSD_STES@DF_BTS/AUT.M.BCICP.PB.C.Y._Z._Z.N"),
        ),
        ("IMF/IFS/M.AR.PCPI_IX", None),
        ("IMF/DOT/A.AR.TXG_FOB_USD.W00", None),
        ("OECD/MEI/AUT.CSCICP03.IXNSA.M", None),
    ],
)
def test_a_dbnomics_id_is_pointed_at_its_origin_when_the_translation_is_mechanical(dbnomics_id, expected):
    assert converter().direct(dbnomics_id) == expected


def test_an_excluded_id_stays_on_dbnomics(tmp_path):
    module = converter()
    row = {
        "column": "e_cl_gdp",
        "provider": "dbnomics",
        "series_id": "WB/WDI/A-NY.GDP.MKTP.CD-CHL",
        "name": "Chile GDP",
        "region": "CL",
        "frequency": "annual",
        "commercial_ok": "yes",
    }
    assert "source: worldbank\n  id: NY.GDP.MKTP.CD/CHL" in module.convert([row])
    listing = tmp_path / "exclusions.txt"
    listing.write_text(
        "# stays on the mirror\n\nWB/WDI/A-NY.GDP.MKTP.CD-CHL  # the origin refuses it\n", encoding="utf-8"
    )
    excluded = module.read_exclusions(listing)
    assert excluded == {"WB/WDI/A-NY.GDP.MKTP.CD-CHL"}
    assert "source: dbnomics\n  id: WB/WDI/A-NY.GDP.MKTP.CD-CHL" in module.convert([row], excluded)
    assert module.read_exclusions(tmp_path / "absent.txt") == frozenset()


def test_every_entry_is_accepted_by_its_source():
    entries = load_catalog(pathlib.Path("macro"))
    http = client()
    instances = {name: sources.create(name, http, Credentials()) for name in sources.REGISTRY}
    check_catalog(entries, instances)
    counts = collections.Counter(entry.source for entry in entries)
    assert (counts["fred"], counts["banxico"], counts["inegi"]) == (24, 6, 2)
    assert counts["dbnomics"] + sum(counts[name] for name in DIRECT) == 1262
    assert counts["worldbank"] > 0
    assert counts["bis"] > 0


def test_every_entry_keeps_the_column_name_as_its_alias_and_declares_a_frequency():
    entries = load_catalog(pathlib.Path("macro"))
    by_alias = {entry.alias: entry for entry in entries}
    assert len(by_alias) == len(entries) == 1294
    assert all(entry.alias.startswith("e_") and entry.name and entry.frequency for entry in entries)
    igae = by_alias["e_mx_igae"]
    assert (igae.key, igae.frequency, igae.params) == ("inegi:6207136901", Frequency.MONTHLY, {"bank": "BISE"})
    assert igae.attrs == {"region": "MX", "commercial_ok": "unverified"}
    assert by_alias["e_ar_cpi"].key == "dbnomics:IMF/IFS/M.AR.PCPI_IX"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/macro_catalog_test.py -q`
Expected: failures with `AttributeError: module 'convert_macro_catalog' has no attribute 'read_exclusions'` (and `'direct'`).

- [ ] **Step 3: Write the translation**

Replace `scripts/convert_macro_catalog.py` with:

```python
"""Convert the equity engine's macro catalog (JSON) into the store's bundled catalog (YAML).

    python scripts/convert_macro_catalog.py

A series the JSON reads through DBnomics is pointed at its origin when the id translates
mechanically (World Bank, BIS, Eurostat, ECB, IMF WEO, two OECD dataflows): the mirrors lag
their origin by months or years. Ids listed in scripts/macro_direct_exclusions.txt keep their
DBnomics entry.

Rerun it whenever the JSON or the exclusions change; a test fails while the files are out of step.
"""

import json
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src" / "data_pipeline" / "equity" / "config_handlers" / "macro_catalog.json"
TARGET = ROOT / "src" / "data_pipeline" / "store" / "catalogs" / "macro.yaml"
EXCLUSIONS = ROOT / "scripts" / "macro_direct_exclusions.txt"
HEADER = (
    "# Generated by scripts/convert_macro_catalog.py from\n"
    "# src/data_pipeline/equity/config_handlers/macro_catalog.json. Do not edit by hand.\n"
)
SOURCES = {"dbnomics": "dbnomics", "fred": "fred", "banxico_sie": "banxico", "inegi": "inegi"}
FREQUENCIES = {"daily": "D", "weekly": "W", "monthly": "M", "quarterly": "Q", "annual": "A"}
INEGI_BANK = "BISE"  # every INEGI id of the macro catalog is a BISE id
SAME_KEY = {"BIS": "bis", "Eurostat": "eurostat", "ECB": "ecb"}  # the origin uses the same flow and key
OECD_FLOWS = ("DSD_STES@DF_CLI", "DSD_STES@DF_BTS")
OECD_AGENCY = "OECD.SDD.STES"
WEO_FLOW = "IMF.RES,WEO"


def direct(series_id):
    """(source, id) at the origin for a DBnomics id, or None when no mechanical translation exists."""
    provider, dataset, key = series_id.split("/", 2)
    if provider in SAME_KEY:
        return SAME_KEY[provider], f"{dataset}/{key}"
    if provider == "WB" and dataset == "WDI" and key.startswith("A-"):
        indicator, economy = key[2:].rsplit("-", 1)
        return "worldbank", f"{indicator}/{economy}"
    if provider == "IMF" and dataset.startswith("WEO:"):
        economy, indicator, _unit = key.split(".")
        return "imf", f"{WEO_FLOW}/{economy}.{indicator}.A"
    if provider == "OECD" and dataset in OECD_FLOWS:
        return "oecd", f"{OECD_AGENCY},{dataset}/{key}"
    return None


def read_exclusions(path=EXCLUSIONS):
    """The DBnomics ids that stay on DBnomics: one per line, `#` starts a comment."""
    if not path.exists():
        return frozenset()
    lines = (line.split("#", 1)[0].strip() for line in path.read_text(encoding="utf-8").splitlines())
    return frozenset(line for line in lines if line)


def convert(rows, excluded=frozenset()):
    """The YAML text of the bundled catalog for the rows of the JSON catalog."""
    entries = []
    for row in rows:
        source = SOURCES[row["provider"]]
        identifier = str(row["series_id"])
        if source == "dbnomics" and identifier not in excluded:
            source, identifier = direct(identifier) or (source, identifier)
        entry = {
            "source": source,
            "id": identifier,
            "alias": row["column"],
            "name": row["name"],
            "frequency": FREQUENCIES[row["frequency"]],
        }
        if source == "inegi":
            entry["bank"] = INEGI_BANK
        entry["attrs"] = {"region": row["region"], "commercial_ok": row["commercial_ok"]}
        entries.append(entry)
    return HEADER + yaml.safe_dump(entries, sort_keys=False, allow_unicode=True, width=1000)


def main():
    rows = json.loads(SOURCE.read_text(encoding="utf-8"))
    TARGET.write_text(convert(rows, read_exclusions()), encoding="utf-8", newline="\n")
    print(f"[ok] wrote {TARGET} ({len(rows)} entries)")


if __name__ == "__main__":
    main()
```

Create `scripts/macro_direct_exclusions.txt`:

```text
# DBnomics ids of the macro catalog that keep their DBnomics entry even though
# scripts/convert_macro_catalog.py could point them at their origin. One id per line.
# An id is added here when the direct source fails for it; say why in a comment.
```

- [ ] **Step 4: Regenerate the bundled catalog**

Run: `.venv/Scripts/python scripts/convert_macro_catalog.py`
Expected: `[ok] wrote ...macro.yaml (1294 entries)`

Run: `grep "^- source:" src/data_pipeline/store/catalogs/macro.yaml | sort | uniq -c`
Expected: `banxico 6, bis 211, dbnomics 500, ecb 1, eurostat 96, fred 24, imf 86, inegi 2, oecd 64, worldbank 304`

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: `285 passed`

- [ ] **Step 6: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 7: Commit**

```bash
git add scripts src/data_pipeline/store/catalogs/macro.yaml tests/unit/store/macro_catalog_test.py
git commit -m "feat(store): point 762 series of the macro catalog at their origin instead of DBnomics"
```

### Task 5: Live tests and acceptance scripts

**Files:**
- Create: `tests/live/direct_sources_live_test.py`
- Create: `scripts/macro_freshness.py`, `scripts/compare_direct_with_dbnomics.py`

- [ ] **Step 1: Write the live tests**

`tests/live/direct_sources_live_test.py` (one test per source, and one for the WEO projections; all seven passed on 2026-10-05):

```python
"""One real call to each direct source. Off by default; run with:  pytest -m live tests/live

None of these sources needs a key, only network access. Each test asks for one series of the
bundled macro catalog and checks that its origin still answers in the shape the source reads.
"""

import pytest

from data_pipeline.store.api import Store

SERIES = [
    ("worldbank", "NY.GDP.MKTP.CD/CHL", "A"),
    ("bis", "WS_TC/Q.AR.P.A.M.770.A", "Q"),
    ("ecb", "FM/B.U2.EUR.4F.KR.MRR_FR.LEV", "D"),
    ("eurostat", "une_rt_m/M.SA.TOTAL.PC_ACT.T.AT", "M"),
    ("oecd", "OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H", "M"),
    ("imf", "IMF.RES,WEO/ARG.GGXCNL_NGDP.A", "A"),
]


@pytest.mark.live
@pytest.mark.parametrize(("source", "identifier", "frequency"), SERIES, ids=[item[0] for item in SERIES])
def test_the_origin_still_answers_in_the_expected_shape(tmp_path, source, identifier, frequency):
    store = Store(tmp_path / "store")
    store.add(source, [identifier])
    report = store.sync()
    assert report.exit_code == 0, report.lines()
    key = f"{source}:{identifier}"
    assert store.info(key).frequency == frequency
    assert len(store.series(key, projections=True)) > 5


@pytest.mark.live
def test_weo_marks_the_years_ahead_as_projections(tmp_path):
    store = Store(tmp_path / "store")
    store.add("imf", ["IMF.RES,WEO/ARG.GGXCNL_NGDP.A"])
    store.sync()
    with_projections = store.series("imf:IMF.RES,WEO/ARG.GGXCNL_NGDP.A", projections=True)
    assert with_projections["projection"].any()
    assert not with_projections["projection"].all()
```

- [ ] **Step 2: Write the two scripts**

`scripts/macro_freshness.py`:

```python
"""How fresh the macro catalog is in a store, by the origin of each series.

    python scripts/macro_freshness.py D:/datos/macro

Prints, per origin (the family the series comes from, whatever source reads it), how many series
the store holds, how many are stale and the typical age in days of the last observation.
"""

import json
import pathlib
import sys

import pandas as pd

import data_pipeline

ROOT = pathlib.Path(__file__).resolve().parent.parent
CATALOG = ROOT / "src" / "data_pipeline" / "equity" / "config_handlers" / "macro_catalog.json"


def origin(row):
    """The publisher of a catalog row: the DBnomics provider prefix, or the provider itself."""
    return row["series_id"].split("/")[0] if row["provider"] == "dbnomics" else row["provider"]


def report(root: pathlib.Path) -> int:
    origins = {row["column"]: origin(row) for row in json.loads(CATALOG.read_text(encoding="utf-8"))}
    store = data_pipeline.Store(root, "macro")
    status = store.status()
    last = store.index().set_index("key")["last_date"]
    today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    status["origin"] = status["alias"].map(origins)
    status["age"] = (today - status["key"].map(last)).dt.days
    table = status.groupby("origin").agg(
        series=("key", "size"),
        ok=("state", lambda states: int((states == "ok").sum())),
        stale=("state", lambda states: int((states == "stale").sum())),
        missing=("state", lambda states: int((states == "missing").sum())),
        failed=("state", lambda states: int((states == "failed").sum())),
        median_age_days=("age", "median"),
    )
    print(table.to_string())
    totals = status["state"].value_counts()
    print("total: " + ", ".join(f"{count} {state}" for state, count in totals.items()))
    return 0


if __name__ == "__main__":
    sys.exit(report(pathlib.Path(sys.argv[1])))
```

`scripts/compare_direct_with_dbnomics.py`:

```python
"""Acceptance check: series read from their origin against the same series read through DBnomics.

    python scripts/compare_direct_with_dbnomics.py D:/datos/direct D:/datos/dbnomics

The first store was synced with the bundled macro catalog (direct sources); the second holds the
same series under their DBnomics keys. For every series that moved, the values are compared on
the periods both stores hold. Prints one line per origin and the series that disagree.
"""

import importlib.util
import json
import pathlib
import sys

import pandas as pd

import data_pipeline
from data_pipeline.store.errors import UnknownSeriesError

ROOT = pathlib.Path(__file__).resolve().parent.parent
CATALOG = ROOT / "src" / "data_pipeline" / "equity" / "config_handlers" / "macro_catalog.json"
TOLERANCE = 1e-6  # the mirror and the origin round differently


def converter():
    spec = importlib.util.spec_from_file_location("convert_macro_catalog", ROOT / "scripts" / "convert_macro_catalog.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def compare(direct_root: pathlib.Path, mirror_root: pathlib.Path) -> int:
    module = converter()
    excluded = module.read_exclusions()
    direct_store = data_pipeline.Store(direct_root)
    mirror_store = data_pipeline.Store(mirror_root)
    totals: dict[str, list[int]] = {}
    disagreeing = []
    for row in json.loads(CATALOG.read_text(encoding="utf-8")):
        identifier = row["series_id"]
        moved = module.direct(identifier) if row["provider"] == "dbnomics" and identifier not in excluded else None
        if moved is None:
            continue
        source, direct_id = moved
        counts = totals.setdefault(source, [0, 0, 0, 0])  # compared, agree, disagree, unreadable
        try:
            ours = direct_store.series(f"{source}:{direct_id}", projections=True).set_index("period")["value"]
            theirs = mirror_store.series(f"dbnomics:{identifier}").set_index("period")["value"]
        except UnknownSeriesError:
            counts[3] += 1
            continue
        common = ours.index.intersection(theirs.index)
        gap = (ours[common] - theirs[common]).abs()
        limit = TOLERANCE * pd.concat([ours[common].abs(), theirs[common].abs()], axis=1).max(axis=1) + 1e-12
        different = int((gap > limit).sum())
        counts[0] += 1
        counts[1 if different == 0 else 2] += 1
        if different:
            newer = len(ours.index.difference(theirs.index))
            disagreeing.append(f"  {row['column']:<34} {different} of {len(common)} periods differ; {newer} periods only at the origin")
    for source, (compared, agree, disagree, unreadable) in sorted(totals.items()):
        print(f"{source:<10} compared {compared}, agree {agree}, disagree {disagree}, not in both stores {unreadable}")
    print("\n".join(disagreeing))
    return 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])))
```

- [ ] **Step 3: Check the default run and the live run**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1239 passed, 12 deselected`

Run: `.venv/Scripts/python -m pytest -m live tests/live/direct_sources_live_test.py -q`
Expected: `7 passed` (public services, no key)

Run: `.venv/Scripts/python -m ruff check .`
Expected: `All checks passed!`

- [ ] **Step 4: Commit**

```bash
git add tests/live scripts
git commit -m "test(store): add live tests for the direct sources and the acceptance scripts"
```

### Task 6: Documentation

**Files:**
- Modify: `README.md`, `CHANGELOG.md`

- [ ] **Step 1: Update the README**

In `README.md`, in the section `## Public-data store (preview)`, replace the two lines

```markdown
Sources: FRED, BLS, Banxico SIE, INEGI and DBnomics (which reaches the IMF, the World Bank, the
BIS, the OECD and Eurostat).
```

with

```markdown
Sources: FRED, BLS, Banxico SIE, INEGI, the World Bank, the BIS, the ECB, Eurostat, the OECD and
the IMF, read directly from each publisher, plus DBnomics for what has no direct id yet.
```

- [ ] **Step 2: Add a CHANGELOG entry**

In `CHANGELOG.md`, as the first bullet under `## [Unreleased]` / `### Added`, insert:

```markdown
- Public-data store: direct sources for the **World Bank** (`<indicator>/<economy>`, one call per indicator) and, through one SDMX class, the **BIS**, the **ECB**, **Eurostat**, the **OECD** and the **IMF** (`<flow>/<key>`). None needs a key. The bundled macro catalog now reads 762 of its series from their publisher instead of the DBnomics mirror, which lags by months or years; aliases are unchanged. `scripts/macro_direct_exclusions.txt` keeps a series on DBnomics when its origin refuses it.
```

- [ ] **Step 3: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs(store): document the direct sources"
```

### Task 7: Full verification and packaging

- [ ] **Step 1: Run every check**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1239 passed, 12 deselected`

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 2: Check that the wheel ships the new sources**

```bash
uv build --wheel --out-dir dist-check
.venv/Scripts/python -m zipfile -l dist-check/*.whl | grep -E "store/sources/(worldbank|sdmx).py|store/catalogs/macro.yaml"
rm -rf dist-check
```

Expected: three lines.

### Task 8: Acceptance with the real services

Public services, no keys, a temporary folder. `<A>` is that folder; `<OLD>` is a store that holds the macro catalog read through DBnomics (the one synced on 2026-10-05).

- [ ] **Step 1: Freshness before**

Run: `PYTHONPATH=src .venv/Scripts/python scripts/macro_freshness.py <OLD>`
Expected: the table measured on 2026-10-05 (World Bank 304 stale of 304, BIS 211 of 211, OECD 147 of 147, IMF 325 of 503, Eurostat 78 of 96).

`<OLD>` was synced with the catalog as it was then, so its keys are DBnomics keys. The script reads the store through today's catalog; series that moved show as `missing` there. Use the 2026-10-05 measurement as the "before" figure if so.

- [ ] **Step 2: Sync the direct sources**

```bash
PYTHONPATH=src .venv/Scripts/python -m data_pipeline sync --root <A>/macro --catalog macro --source worldbank --source bis --source ecb --source eurostat --source oecd --source imf
```

About 460 calls at 20 to 30 per minute for the SDMX providers: 20 to 25 minutes. Run it detached if the tool limits a command to 30 minutes.

- [ ] **Step 3: Move what failed to the exclusions**

For every failed series, find its DBnomics id (the catalog row with the same alias) and add it to `scripts/macro_direct_exclusions.txt` with a comment giving the reason. Rerun `scripts/convert_macro_catalog.py` and the test suite, and commit. Report the list.

- [ ] **Step 4: Freshness after, and agreement with the mirror**

```bash
PYTHONPATH=src .venv/Scripts/python scripts/macro_freshness.py <A>/macro
PYTHONPATH=src .venv/Scripts/python scripts/compare_direct_with_dbnomics.py <A>/macro <OLD>
```

Report the stale counts by origin before and after. For the comparison, a disagreement on the newest periods is a revision the mirror has not picked up; IMF WEO disagrees by design (the mirror is pinned to the April 2025 edition). A disagreement across the whole history of a series means the translated id is a different series: move it to the exclusions and report it.

- [ ] **Step 5: Report**

State which acceptance criteria of the spec were verified and how. Do not report a skipped step as done.

## What the acceptance run changed (2026-10-05)

Task 8 was run. All 762 series downloaded from their origin; the exclusions file stays empty.
Three things came out of it and are in the code, each with its test:

- **A reader no longer crashes a sync on Windows.** Replacing a parquet file fails while another
  program has it open. `storage._replace` now waits about five seconds for the reader and then
  fails with a `StoreError` that leaves the old file intact (`storage_test.py`).
- **A persistent 429 stops the source.** `Client` raises `RateLimitedError` when every attempt was
  answered 429; `per_request`, BLS and the World Bank turn it into `QuotaExhaustedError`, so the
  source stops, the rest stays pending and the exit code is 3. Before, each remaining series was
  retried four times and failed, which only prolonged the block (`http_test.py`, `base_test.py`).
- **The OECD is paced at one request a minute.** At 20 a minute, 55 calls went through and every
  later one was refused: about 60 requests an hour, inferred from that behaviour and not read in
  its documentation. The 64 OECD series of the catalog now take a little over an hour.

Measured after the run, for the 762 series that moved: 680 fresh, 82 stale, none missing or
failed. Read through DBnomics, at least 658 of them were stale. Of the 82 that remain, most are
BIS quarterly series whose newest quarter is itself about 190 days old (the generic quarterly
threshold is 183), and a few are series their publisher discontinued.

A sample of 24 series was also downloaded through DBnomics and compared: every pair is the same
series (correlation 0.94 or higher, 0.999 for most), and where values differ the origin carries
revisions the mirror does not have.
