import datetime
import json
import math

import httpx

from data_pipeline.credentials import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.dbnomics import Dbnomics

from .helpers import client, entry, fixture

HICP = "Eurostat/prc_hicp_midx/M.I15.CP00.EA"


def fetch(handler, requests):
    return list(Dbnomics(client(handler), Credentials()).fetch(requests))


def documents(*docs):
    # json.dumps by hand: httpx 0.28 refuses NaN and Infinity in json=, and a test sends Infinity.
    body = json.dumps({"series": {"docs": list(docs)}})
    return lambda _request: httpx.Response(200, text=body, headers={"content-type": "application/json"})


def hicp(**fields):
    return entry(HICP, "dbnomics", **fields)


def test_downloads_a_series_without_a_key():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, text=fixture("dbnomics_series.json"))

    series = fetch(handler, [Request(hicp())])[0].series[0]
    assert dict(seen[0].url.params) == {"series_ids": HICP, "observations": "1"}
    assert (series.key, series.frequency) == (f"dbnomics:{HICP}", Frequency.MONTHLY)
    assert series.name == "Monthly - Index, 2015=100 - All-items HICP - Euro area"
    assert [item.period for item in series.observations] == ["2026-03", "2026-04", "2026-05"]
    assert series.observations[0].value == 128.1
    assert math.isnan(series.observations[1].value)
    assert series.observations[2].date == datetime.date(2026, 5, 31)


def test_missing_and_non_finite_values_become_missing():
    handler = documents(
        {"@frequency": "annual", "period": ["2021", "2022", "2023", "2024"], "value": [None, "", math.inf, 2.5]}
    )
    series = fetch(handler, [Request(hicp())])[0].series[0]
    assert [math.isnan(item.value) for item in series.observations] == [True, True, True, False]


def test_every_period_spelling_is_read():
    for reported, periods, labels in [
        ("annual", ["2025"], ["2025"]),
        ("quarterly", ["2026-Q2"], ["2026Q2"]),
        ("daily", ["2026-06-05"], ["2026-06-05"]),
    ]:
        series = fetch(documents({"@frequency": reported, "period": periods, "value": [1.0]}), [Request(hicp())])[
            0
        ].series[0]
        assert [item.period for item in series.observations] == labels


def test_a_missing_frequency_falls_back_to_the_catalog():
    handler = documents({"period": ["2025"], "value": [1.0]})
    assert fetch(handler, [Request(hicp(frequency=Frequency.ANNUAL))])[0].series[0].frequency is Frequency.ANNUAL
    failure = fetch(handler, [Request(hicp())])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason.endswith("declare `frequency` in the catalog (DBnomics said '')")


def test_since_is_applied_after_the_download():
    handler = lambda _request: httpx.Response(200, text=fixture("dbnomics_series.json"))  # noqa: E731
    series = fetch(handler, [Request(hicp(), datetime.date(2026, 4, 15))])[0].series[0]
    assert [item.period for item in series.observations] == ["2026-04", "2026-05"]


def test_no_documents_is_not_found():
    failure = fetch(documents(), [Request(hicp())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "DBnomics has no series with this id")


def test_http_errors():
    not_found = fetch(lambda _request: httpx.Response(404, text="nope"), [Request(hicp())])[0].failures[0]
    assert (not_found.outcome, not_found.reason) == (Outcome.NOT_FOUND, "HTTP 404: nope")
    broken = fetch(lambda _request: httpx.Response(400, text="bad"), [Request(hicp())])[0].failures[0]
    assert broken.outcome is Outcome.SOURCE_ERROR


def test_a_period_that_does_not_match_the_frequency_fails_only_that_series():
    def handler(request):
        if request.url.params["series_ids"] == "X/Y/Z":
            return httpx.Response(
                200, json={"series": {"docs": [{"@frequency": "monthly", "period": ["2026-W05"], "value": [1.0]}]}}
            )
        return httpx.Response(200, text=fixture("dbnomics_series.json"))

    batches = fetch(handler, [Request(entry("X/Y/Z", "dbnomics")), Request(hicp())])
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "PeriodError" in batches[0].failures[0].reason
    assert batches[1].series[0].key == f"dbnomics:{HICP}"


def test_an_id_with_a_colon_keeps_its_source_as_the_part_before_the_first_colon():
    series_id = "IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp"
    series = fetch(
        documents({"@frequency": "annual", "period": ["2025"], "value": [1.0]}), [Request(entry(series_id, "dbnomics"))]
    )
    assert series[0].series[0].key == f"dbnomics:{series_id}"


def test_a_period_that_comes_twice_fails_the_series():
    failure = fetch(documents({"@frequency": "monthly", "period": ["2026-05", "2026-05"], "value": [1.0, 2.0]}), [
        Request(hicp())
    ])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason.startswith("period 2026-05 comes more than once")
