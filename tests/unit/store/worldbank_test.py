import datetime
import json
import math

import httpx
import pytest

from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError
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
