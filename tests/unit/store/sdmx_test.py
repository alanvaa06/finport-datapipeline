import datetime
import math

import httpx
import pytest

from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.sdmx import (
    PROVIDERS,
    Sdmx,
    group_requests,
    read_csv,
    rows_of,
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


def test_unit_mult_is_recorded_and_the_values_are_kept_as_published():
    header = "REF_AREA,TIME_PERIOD,OBS_VALUE,UNIT_MULT"
    series = read_csv(header + "\nMX,2025,1.5,6\nMX,2026,2.5,6\n", entry("ITG/MX.A", "imf"))
    assert series.attrs == {"unit_mult": "6"}
    assert [item.value for item in series.observations] == [1.5, 2.5]
    mixed = read_csv(header + "\nMX,2025,1500,3\nMX,2026,2.5,6\n", entry("ITG/MX.A", "imf"))
    assert mixed.attrs == {"unit_mult": "3,6"}
    assert read_csv("REF_AREA,TIME_PERIOD,OBS_VALUE\nMX,2025,1\n", entry("ITG/MX.A", "imf")).attrs == {}


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


def test_only_a_dimension_can_split_an_answer_never_an_attribute():
    # SDMX-CSV puts the dimensions before TIME_PERIOD and the attributes after OBS_VALUE
    after = rows_of("REF_AREA,TIME_PERIOD,OBS_VALUE,OBS_STATUS\nA,2024,1,B\nA,2025,2,C\n")
    assert splitting_column(after, ["A", "B", "C"]) == "REF_AREA"
    # a known attribute is left out wherever the provider puts it
    before = rows_of("REF_AREA,OBS_STATUS,TIME_PERIOD,OBS_VALUE\nA,B,2024,1\nA,C,2025,2\n")
    assert splitting_column(before, ["A", "B", "C"]) == "REF_AREA"


def test_rows_of_an_omitted_series_are_never_given_to_another_one():
    answer = "FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE,OBS_STATUS\nA,AR,2024,1,BR\nA,AR,2025,2,CL\n"
    requests = [Request(entry(f"FLOW/A.{country}", "bis")) for country in ("AR", "BR", "CL")]
    (batch,) = fetch("bis", lambda _request: httpx.Response(200, text=answer), requests)
    assert [(series.key, len(series.observations)) for series in batch.series] == [("bis:FLOW/A.AR", 2)]
    assert [failure.outcome for failure in batch.failures] == [Outcome.NOT_FOUND, Outcome.NOT_FOUND]


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
