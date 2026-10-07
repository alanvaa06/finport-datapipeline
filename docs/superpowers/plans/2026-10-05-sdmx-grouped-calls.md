# SDMX: several series per call Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the SDMX source ask for several series in one call, so the 458 SDMX series of the macro catalog take about 17 calls.

**Architecture:** Only `store/sources/sdmx.py` changes. `group_requests` puts together the requests of a flow that differ in one position of the key; `Sdmx._group` asks for them with the values joined by `+` and splits the answer by the column that carries those values. A group that cannot be read as a group falls back to one call per series, which is today's behaviour.

**Tech Stack:** Python 3.12+, httpx; pytest, ruff, mypy. No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-05-sdmx-grouped-calls-design.md`

---

## Before you start

**How this plan was checked.** The files below were run in a scratch clone on 2026-10-05: 1,258 tests passed (1,243 that existed plus 15 new), `ruff check` and `mypy` clean, and the seven live tests of the direct sources passed with the grouped code. Before that, each provider was probed with a real grouped key taken from the macro catalog; all five accept `+` and return a column that tells the series apart. Not run: Task 3, the acceptance sync.

**Environment.** Windows, Git Bash, from `C:/Proyectos/data_pipeline`, virtual environment `.venv`. Call `.venv/Scripts/python`, never a bare `python`. Commits carry no `Co-Authored-By` line.

## File structure

| File | Responsibility |
|---|---|
| `src/data_pipeline/store/sources/sdmx.py` | Modified: grouping, one call per group, the split, the fallback |
| `tests/unit/store/sdmx_test.py` | Modified: fifteen new tests after the existing twenty |
| `scripts/compare_stores.py` | New: compare the series two stores have in common |

---

### Task 1: Group the requests and ask once per group

**Files:**
- Modify: `src/data_pipeline/store/sources/sdmx.py`
- Test: `tests/unit/store/sdmx_test.py`

- [ ] **Step 1: Write the failing tests**

Replace `tests/unit/store/sdmx_test.py` with (everything below the comment `several series in one call` is new; the twenty tests above it are unchanged):

```python
import datetime
import math

import httpx
import pytest

from data_pipeline.store import sources
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.sdmx import (
    PROVIDERS,
    Sdmx,
    group_requests,
    read_csv,
    split_id,
    splitting_column,
)

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


# -- several series in one call ----------------------------------------------------------------

GROUPED = (
    "FREQ,BORROWERS_CTY,UNIT_TYPE,TIME_PERIOD,OBS_VALUE\n"
    "Q,AR,770,2025-Q4,10.5\nQ,AR,770,2026-Q1,11\nQ,BR,770,2026-Q1,70.2\n"
)


def credit(country, **fields):
    return entry(f"WS_TC/Q.{country}.P.A.M.770.A", "bis", **fields)


def keys_of(groups):
    return [[request.entry.source_id for request in group.members] for group in groups]


def test_series_that_differ_in_one_position_form_one_group():
    groups = group_requests([Request(credit("AR")), Request(credit("BR")), Request(credit("CL"))])
    assert len(groups) == 1
    assert (groups[0].flow, groups[0].position, groups[0].parts[1]) == ("WS_TC", 1, "AR")
    assert len(groups[0].members) == 3


def test_two_key_patterns_of_one_flow_are_two_groups():
    countries = ("AR", "BR", "CL")
    nominal = [Request(entry(f"WS_EER/M.N.B.{country}", "bis")) for country in countries]
    real = [Request(entry(f"WS_EER/M.R.B.{country}", "bis")) for country in countries]
    groups = group_requests([nominal[0], real[0], nominal[1], real[1], nominal[2], real[2]])
    assert keys_of(groups) == [
        ["WS_EER/M.N.B.AR", "WS_EER/M.N.B.BR", "WS_EER/M.N.B.CL"],
        ["WS_EER/M.R.B.AR", "WS_EER/M.R.B.BR", "WS_EER/M.R.B.CL"],
    ]
    assert {group.position for group in groups} == {3}


def test_flows_and_key_lengths_are_never_mixed_and_a_lone_series_keeps_its_own_key():
    requests = [
        Request(credit("AR")),
        Request(entry("WS_CBPOL/M.AR", "bis")),
        Request(credit("BR")),
        Request(entry("X/A.B.C", "bis")),
    ]
    groups = group_requests(requests)
    assert keys_of(groups) == [
        ["WS_TC/Q.AR.P.A.M.770.A", "WS_TC/Q.BR.P.A.M.770.A"],
        ["WS_CBPOL/M.AR"],
        ["X/A.B.C"],
    ]
    assert [group.position for group in groups] == [1, None, None]


def test_a_group_is_cut_at_fifty_series():
    groups = group_requests([Request(credit(f"C{number:02d}")) for number in range(120)])
    assert [len(group.members) for group in groups] == [50, 50, 20]


def test_a_key_with_an_or_or_an_open_position_is_never_grouped():
    requests = [Request(credit("AR")), Request(credit("BR")), Request(credit("CL+MX")), Request(credit(""))]
    groups = group_requests(requests)
    assert [(len(group.members), group.position) for group in groups] == [(2, 1), (1, None), (1, None)]


def test_the_splitting_column_is_the_one_that_holds_the_requested_values():
    rows = [
        {"FREQ": "Q", "CTY": "AR", "TIME_PERIOD": "2026-Q1", "OBS_VALUE": "1"},
        {"FREQ": "Q", "CTY": "BR", "TIME_PERIOD": "2026-Q1", "OBS_VALUE": "2"},
    ]
    assert splitting_column(rows, ["AR", "BR", "CL"]) == "CTY"
    assert splitting_column(rows, ["XX", "YY"]) is None
    twins = [{"A": "AR", "B": "AR", "TIME_PERIOD": "2026", "OBS_VALUE": "1"}]
    assert splitting_column(twins, ["AR", "BR"]) is None


def test_a_group_is_one_call_and_its_answer_is_split_by_series():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, text=GROUPED)

    batches = fetch("bis", handler, [Request(credit("AR")), Request(credit("BR")), Request(credit("CL"))])
    assert len(seen) == 1
    address = str(seen[0].url.copy_with(query=None))
    assert address == "https://stats.bis.org/api/v1/data/WS_TC/Q.AR+BR+CL.P.A.M.770.A/all"
    assert len(batches) == 1
    argentina, brazil = batches[0].series
    assert argentina.key == "bis:WS_TC/Q.AR.P.A.M.770.A"
    assert [item.period for item in argentina.observations] == ["2025Q4", "2026Q1"]
    assert (brazil.key, brazil.frequency) == ("bis:WS_TC/Q.BR.P.A.M.770.A", Frequency.QUARTERLY)
    assert brazil.observations[0].value == 70.2
    failure = batches[0].failures[0]
    assert (failure.entry.key, failure.outcome) == ("bis:WS_TC/Q.CL.P.A.M.770.A", Outcome.NOT_FOUND)
    assert failure.reason == "the query returned no observations"


def test_a_group_asks_from_the_earliest_since_only_when_every_request_has_one():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, text=GROUPED)

    both = [Request(credit("AR"), datetime.date(2024, 5, 1)), Request(credit("BR"), datetime.date(2022, 1, 1))]
    fetch("bis", handler, both)
    assert seen[0].url.params["startPeriod"] == "2022"
    fetch("bis", handler, [both[0], Request(credit("BR"))])
    assert "startPeriod" not in seen[1].url.params


def test_weo_projections_are_flagged_inside_a_group():
    text = (
        "COUNTRY,INDICATOR,TIME_PERIOD,OBS_VALUE,LATEST_ACTUAL_ANNUAL_DATA\n"
        "ARG,GGXCNL_NGDP,2024,-0.3,2024\nARG,GGXCNL_NGDP,2026,-0.1,2024\nBRA,GGXCNL_NGDP,2026,-7.5,2025\n"
    )
    requests = [Request(entry(f"IMF.RES,WEO/{country}.GGXCNL_NGDP.A", "imf")) for country in ("ARG", "BRA")]
    argentina, brazil = fetch("imf", lambda _request: httpx.Response(200, text=text), requests)[0].series
    assert [item.projection for item in argentina.observations] == [False, True]
    assert [item.projection for item in brazil.observations] == [True]


def test_a_refused_group_is_asked_for_series_by_series():
    seen = []

    def handler(request):
        seen.append(request.url.path)
        if "+" in request.url.path or ".XX." in request.url.path:
            return httpx.Response(404, text="nope")
        return httpx.Response(200, text="FREQ,BORROWERS_CTY,TIME_PERIOD,OBS_VALUE\nQ,AR,2026-Q1,11\n")

    result = fetch("bis", handler, [Request(credit("AR")), Request(credit("XX"))])[0]
    asked = [path.rsplit("/", 2)[1] for path in seen]
    assert asked == ["Q.AR+XX.P.A.M.770.A", "Q.AR.P.A.M.770.A", "Q.XX.P.A.M.770.A"]
    assert [series.key for series in result.series] == ["bis:WS_TC/Q.AR.P.A.M.770.A"]
    assert (result.failures[0].outcome, result.failures[0].reason) == (Outcome.NOT_FOUND, "HTTP 404: nope")


def test_an_answer_that_cannot_be_split_is_asked_for_series_by_series():
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if "+" in request.url.path:
            return httpx.Response(200, text="FREQ,TIME_PERIOD,OBS_VALUE\nQ,2026-Q1,1\nQ,2026-Q1,2\n")
        return httpx.Response(200, text="FREQ,TIME_PERIOD,OBS_VALUE\nQ,2026-Q1,1\n")

    result = fetch("bis", handler, [Request(credit("AR")), Request(credit("BR"))])[0]
    assert len(calls) == 3
    assert len(result.series) == 2


def test_an_empty_grouped_answer_fails_every_series_without_more_calls():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="FREQ,BORROWERS_CTY,TIME_PERIOD,OBS_VALUE\n")

    result = fetch("bis", handler, [Request(credit("AR")), Request(credit("BR"))])[0]
    assert len(calls) == 1
    assert [failure.outcome for failure in result.failures] == [Outcome.NOT_FOUND, Outcome.NOT_FOUND]


def test_a_persistent_rate_limit_stops_the_source():
    source = Sdmx("oecd", client(lambda _request: httpx.Response(429)))
    stream = source.fetch([Request(credit("AR")), Request(credit("BR"))])
    with pytest.raises(QuotaExhaustedError, match="HTTP 429"):
        next(stream)


def test_a_network_failure_fails_the_whole_group_and_the_next_group_continues():
    def handler(request):
        if "WS_TC" in request.url.path:
            return httpx.Response(503)
        return httpx.Response(200, text=fixture("sdmx_bis_series.csv"))

    requests = [Request(credit("AR")), Request(credit("BR")), Request(entry("WS_CBPOL/M.MX", "bis"))]
    batches = fetch("bis", handler, requests)
    assert [failure.outcome for failure in batches[0].failures] == [Outcome.NETWORK_ERROR, Outcome.NETWORK_ERROR]
    assert batches[1].series[0].key == "bis:WS_CBPOL/M.MX"


def test_one_unreadable_series_in_a_group_fails_alone():
    text = "FREQ,BORROWERS_CTY,TIME_PERIOD,OBS_VALUE\nQ,AR,2026-Q1,11\nQ,BR,2026-Q1,not a number\n"
    requests = [Request(credit("AR")), Request(credit("BR"))]
    result = fetch("bis", lambda _request: httpx.Response(200, text=text), requests)[0]
    assert [series.key for series in result.series] == ["bis:WS_TC/Q.AR.P.A.M.770.A"]
    assert result.failures[0].outcome is Outcome.SOURCE_ERROR
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sdmx_test.py -q`
Expected: collection error, `ImportError: cannot import name 'group_requests'`

- [ ] **Step 3: Write the implementation**

Replace `src/data_pipeline/store/sources/sdmx.py` with:

```python
"""One SDMX source class for the BIS, the ECB, Eurostat, the OECD and the IMF. No key.

A series id is `<flow>/<key>`, split at the first `/`:

    bis       WS_TC/Q.AR.P.A.M.770.A
    ecb       FM/B.U2.EUR.4F.KR.MRR_FR.LEV
    eurostat  une_rt_m/M.SA.TOTAL.PC_ACT.T.AT
    oecd      OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H
    imf       IMF.RES,WEO/ARG.GGXCNL_NGDP.A

Every provider is asked for CSV and answers with the columns TIME_PERIOD and OBS_VALUE. The key
of an id must select exactly one series: a key with an open dimension returns several and is
refused. IMF WEO years after LATEST_ACTUAL_ANNUAL_DATA are projections.

Series of one flow that differ in a single position of the key (usually the country) are asked
for in one call, with the values of that position joined by `+`:

    WS_TC/Q.AR.P.A.M.770.A, WS_TC/Q.BR.P.A.M.770.A  ->  WS_TC/Q.AR+BR.P.A.M.770.A

The answer is split back into its series by the column that carries those values. A group that
cannot be read as a group is asked for series by series.
"""

import csv
import dataclasses
import datetime
import io
import re
from collections.abc import Iterator, Mapping, Sequence

import httpx

from data_pipeline.store.errors import (
    CatalogError,
    NetworkError,
    PeriodError,
    QuotaExhaustedError,
    RateLimitedError,
)
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
from data_pipeline.store.sources.base import UNSUPPORTED_FREQUENCY, failures, number, reject_params

OK = 200
NOT_FOUND = frozenset({400, 404})
UNIT_COLUMNS = ("UNIT_MEASURE", "UNIT", "unit")
MEASURE_COLUMNS = frozenset({"TIME_PERIOD", "OBS_VALUE"})
NO_OBSERVATIONS = "the query returned no observations"
SEVERAL_SERIES = "the key returns several series: fix every dimension"
MAX_PER_CALL = 50  # series in one grouped call: keeps the address short
OR = "+"
_YEAR = re.compile(r"\d{4}")  # WEO latest actual year: "2024", or fiscal "FY2024/25" -> first year

Row = dict[str, str]


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
    # About 60 requests an hour, inferred from its 429 answers on 2026-10-05: 55 calls went
    # through at 20 a minute and every later one was refused.
    "oecd": Provider("https://sdmx.oecd.org/public/rest/data/{flow}/{key}", {"format": "csvfile"}, 1),
    "imf": Provider(
        "https://api.imf.org/external/sdmx/2.1/data/{flow}/{key}",
        {},
        20,
        {"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"},
    ),
}


@dataclasses.dataclass(frozen=True)
class Group:
    """Requests asked for in one call. `position` is the part of the key that varies among them;
    it is None when the group is a single request, asked for with its own key."""

    flow: str
    parts: tuple[str, ...]  # the key of the first member, split at the dots
    position: int | None
    members: tuple[Request, ...]


def split_id(entry: CatalogEntry) -> tuple[str, str]:
    """(flow, key) of a series id such as `WS_TC/Q.AR.P.A.M.770.A`."""
    flow, separator, key = entry.source_id.partition("/")
    if not separator or not flow or not key:
        msg = f"{entry.key}: an SDMX id is '<flow>/<key>', such as WS_TC/Q.AR.P.A.M.770.A"
        raise CatalogError(msg)
    return flow, key


def group_requests(requests: Sequence[Request]) -> list[Group]:
    """Requests of one flow that differ in a single position of the key, put together.

    Among the positions of the key, the one that leaves the fewest distinct keys when ignored is
    taken as the varying one. A key that already holds `+` or an empty part goes alone.
    """
    alone: list[Group] = []
    shapes: dict[tuple[str, int], list[tuple[Request, tuple[str, ...]]]] = {}
    for request in requests:
        flow, key = split_id(request.entry)
        parts = tuple(key.split("."))
        if OR in key or "" in parts:
            alone.append(Group(flow, parts, None, (request,)))
        else:
            shapes.setdefault((flow, len(parts)), []).append((request, parts))
    grouped: list[Group] = []
    for (flow, size), members in shapes.items():
        best: dict[tuple[str, ...], list[int]] = {}
        position = 0
        for candidate in range(size):
            buckets: dict[tuple[str, ...], list[int]] = {}
            for index, (_, parts) in enumerate(members):
                buckets.setdefault(parts[:candidate] + parts[candidate + 1 :], []).append(index)
            if not best or len(buckets) < len(best):
                best, position = buckets, candidate
        for indexes in best.values():
            for start in range(0, len(indexes), MAX_PER_CALL):
                chunk = indexes[start : start + MAX_PER_CALL]
                grouped.append(
                    Group(
                        flow,
                        members[chunk[0]][1],
                        position if len(chunk) > 1 else None,
                        tuple(members[index][0] for index in chunk),
                    )
                )
    return [*grouped, *alone]


def rows_of(text: str) -> list[Row]:
    """The rows of an SDMX-CSV answer that carry a period (the IMF answers an unknown key with
    one empty row)."""
    return [row for row in csv.DictReader(io.StringIO(text)) if (row.get("TIME_PERIOD") or "").strip()]


def splitting_column(rows: Sequence[Row], values: Sequence[str]) -> str | None:
    """The column that tells the series of a grouped answer apart: the one whose values are all
    among the requested ones. None when no column qualifies or two qualify equally."""
    wanted = set(values)
    candidates = []
    for column in rows[0]:
        if column in MEASURE_COLUMNS:
            continue
        seen = {row[column] for row in rows}
        if seen <= wanted:
            candidates.append((len(seen), column))
    candidates.sort(reverse=True)
    if not candidates or (len(candidates) > 1 and candidates[0][0] == candidates[1][0]):
        return None
    return candidates[0][1]


def read_rows(rows: Sequence[Row], entry: CatalogEntry) -> SeriesData | Failure:
    """The one series these rows hold, or the failure they amount to."""
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


def read_safely(rows: Sequence[Row], entry: CatalogEntry) -> SeriesData | Failure:
    """`read_rows`, with rows that cannot be parsed turned into the failure of this one series."""
    try:
        return read_rows(rows, entry)
    except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
        return Failure(entry, Outcome.SOURCE_ERROR, f"unexpected answer ({type(exc).__name__}: {exc})")


def read_csv(text: str, entry: CatalogEntry) -> SeriesData | Failure:
    """The one series of an SDMX-CSV answer, or the failure the answer amounts to."""
    return read_safely(rows_of(text), entry)


def batch(results: Sequence[SeriesData | Failure]) -> FetchBatch:
    return FetchBatch(
        tuple(item for item in results if isinstance(item, SeriesData)),
        tuple(item for item in results if isinstance(item, Failure)),
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
        for group in group_requests(requests):
            try:
                yield self._group(group)
            except RateLimitedError as exc:
                # Asking again would only prolong the block: stop here, the next run resumes.
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=failures(group.members, Outcome.NETWORK_ERROR, str(exc)))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=failures(group.members, Outcome.SOURCE_ERROR, reason))

    def _group(self, group: Group) -> FetchBatch:
        if group.position is None:
            return batch([self._download(group.members[0])])
        position = group.position
        values = [split_id(request.entry)[1].split(".")[position] for request in group.members]
        key = ".".join((*group.parts[:position], OR.join(values), *group.parts[position + 1 :]))
        every_since = [request.since for request in group.members if request.since is not None]
        since = min(every_since) if len(every_since) == len(group.members) else None
        response = self._get(group.flow, key, since)
        if response.status_code != OK:
            return self._one_by_one(group)  # one bad value can make the provider refuse them all
        rows = rows_of(response.text)
        if not rows:
            return FetchBatch(failures=failures(group.members, Outcome.NOT_FOUND, NO_OBSERVATIONS))
        column = splitting_column(rows, values)
        if column is None:
            return self._one_by_one(group)
        own: dict[str, list[Row]] = {}
        for row in rows:
            own.setdefault(row[column], []).append(row)
        return batch(
            [
                read_safely(own.get(value, []), request.entry)
                for request, value in zip(group.members, values, strict=True)
            ]
        )

    def _one_by_one(self, group: Group) -> FetchBatch:
        """The safety net: each request of the group asked for with its own key."""
        results: list[SeriesData | Failure] = []
        for request in group.members:
            try:
                results.append(self._download(request))
            except RateLimitedError:
                raise
            except NetworkError as exc:
                results.append(Failure(request.entry, Outcome.NETWORK_ERROR, str(exc)))
        return batch(results)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        flow, key = split_id(entry)
        response = self._get(flow, key, request.since)
        if response.status_code != OK:
            outcome = Outcome.NOT_FOUND if response.status_code in NOT_FOUND else Outcome.SOURCE_ERROR
            return Failure(entry, outcome, f"HTTP {response.status_code}: {self._client.scrub(response.text[:200])}")
        return read_csv(response.text, entry)

    def _get(self, flow: str, key: str, since: datetime.date | None) -> httpx.Response:
        params = dict(self._provider.params)
        if since is not None:
            params["startPeriod"] = str(since.year)
        return self._client.get(
            self.name,
            self._provider.url.format(flow=flow, key=key),
            params=params,
            headers=self._provider.headers,
            per_minute=self.requests_per_minute,
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/sdmx_test.py -q`
Expected: `35 passed`

- [ ] **Step 5: Lint, type-check, run everything**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1258 passed, 12 deselected`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/sdmx.py tests/unit/store/sdmx_test.py
git commit -m "feat(store): ask for several SDMX series in one call"
```

### Task 2: The comparison script, the changelog and the live tests

**Files:**
- Create: `scripts/compare_stores.py`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Write the script**

Create `scripts/compare_stores.py`:

```python
"""Compare the series two stores have in common, key by key.

    python scripts/compare_stores.py D:/datos/new D:/datos/old bis ecb eurostat oecd imf

For each source named (every source of the first store when none is), prints how many series
both stores hold, how many agree on every period they share, and the series that do not.
Projections are included. Exit code 0 when every shared series agrees, 1 otherwise.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline
from data_pipeline.store.errors import UnknownSeriesError

TOLERANCE = 1e-9


def compare(new_root: pathlib.Path, old_root: pathlib.Path, sources: list[str]) -> int:
    new, old = data_pipeline.Store(new_root), data_pipeline.Store(old_root)
    index = new.index()
    disagreeing = 0
    for source in sources or sorted(index["source"].unique()):
        shared = agree = only_new = 0
        for key in sorted(index[index["source"] == source]["key"]):
            ours = new.series(key, projections=True).set_index("period")["value"]
            try:
                theirs = old.series(key, projections=True).set_index("period")["value"]
            except UnknownSeriesError:
                only_new += 1
                continue
            shared += 1
            common = ours.index.intersection(theirs.index)
            gap = (ours[common] - theirs[common]).abs()
            limit = TOLERANCE * pd.concat([ours[common].abs(), theirs[common].abs()], axis=1).max(axis=1)
            different = int((gap > limit).sum())
            missing = len(theirs.index.difference(ours.index))
            if different or missing:
                disagreeing += 1
                print(f"  [x] {key}: {different} of {len(common)} periods differ, {missing} periods only in the old store")
            else:
                agree += 1
        print(f"{source:<10} in both {shared}, agree {agree}, only in the new store {only_new}")
    return 1 if disagreeing else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3:]))
```

- [ ] **Step 2: Add a CHANGELOG entry**

In `CHANGELOG.md`, as the first bullet under `## [Unreleased]` / `### Added`, insert:

```markdown
- Public-data store: the SDMX source asks for several series in one call. Series of a dataflow that differ in one position of the key (usually the country) travel together, joined by `+`, up to 50 a call; the answer is split back by the column that carries those values, and a group that cannot be read as a group is asked for series by series. The 458 SDMX series of the macro catalog take about 17 calls instead of 458, and the OECD, which allows about 60 requests an hour, no longer takes an hour.
```

- [ ] **Step 3: Run the live tests**

Run: `.venv/Scripts/python -m pytest -m live tests/live/direct_sources_live_test.py -q`
Expected: `7 passed`

Run: `.venv/Scripts/python -m ruff check .`
Expected: `All checks passed!`

- [ ] **Step 4: Commit**

```bash
git add scripts/compare_stores.py CHANGELOG.md
git commit -m "docs(store): record grouped SDMX calls; add a script to compare two stores"
```

### Task 3: Acceptance with the real services

Public services, no keys. `<NEW>` is a new temporary store; `<OLD>` is the store filled series by series on 2026-10-05.

- [ ] **Step 1: Sync the direct sources**

```bash
PYTHONPATH=src .venv/Scripts/python -m data_pipeline sync --root <NEW> --catalog macro --source worldbank --source bis --source ecb --source eurostat --source oecd --source imf
```

Expected: six `[ok]` lines, 762 series in all, and about 24 calls in total (BIS 5, ECB 1, Eurostat 7, IMF 2, OECD 2, World Bank 7). More calls on one source mean a group fell back to series by series: say which and why.

- [ ] **Step 2: Compare with the store filled series by series**

```bash
PYTHONPATH=src .venv/Scripts/python scripts/compare_stores.py <NEW> <OLD> bis ecb eurostat oecd imf
```

Expected: every shared series agrees. A difference on the newest period of a series is an observation published or revised between the two runs; a difference across a series means the split put rows in the wrong series, which is a defect: stop and report it.

- [ ] **Step 3: Report**

State the calls per source, the outcome of the comparison, and anything that was not run.

## Outcome of the acceptance run (2026-10-05)

Task 3 was run. The sync of the six direct sources stored all 762 series in 24 calls and 100
seconds: BIS 5, ECB 1, Eurostat 7, IMF 2, OECD 2, World Bank 7. No group fell back to series by
series. `scripts/compare_stores.py` against the store filled series by series the same morning:
762 series in both stores, 762 agree on every period.
