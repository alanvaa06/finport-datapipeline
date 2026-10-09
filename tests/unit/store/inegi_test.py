import datetime
import json
import math

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.inegi import Inegi

from .helpers import client, entry, fixture

INEGI_VALUE = "01234567-89ab-cdef-0123-456789abcdef"
WITH_TOKEN = Credentials({keys.INEGI: INEGI_VALUE})


def serve(seen=None, name="inegi_bise.json", status=200):
    def handler(request):
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, text=fixture(name))

    return handler


def fetch(handler, requests, credentials=WITH_TOKEN):
    source = Inegi(client(handler, secrets=credentials.secrets()), credentials)
    return list(source.fetch(requests))


def igae(**fields):
    return entry("6207136901", "inegi", **fields)


def test_downloads_a_series_and_reads_its_frequency_from_the_answer():
    seen = []
    series = fetch(serve(seen), [Request(igae(params={"bank": "BISE"}))])[0].series[0]
    assert seen[0].url.path.endswith(f"/INDICATOR/6207136901/es/00/false/BISE/2.0/{INEGI_VALUE}")
    assert seen[0].url.params["type"] == "json"
    assert (series.key, series.frequency, series.units) == ("inegi:6207136901", Frequency.MONTHLY, "INEGI unit 1058")
    assert [item.period for item in series.observations] == ["2026-05", "2026-06", "2026-07"]
    assert math.isnan(series.observations[0].value)
    assert series.observations[2].value == pytest.approx(109.21013588490848)
    assert series.observations[2].date == datetime.date(2026, 7, 31)


def test_the_default_bank_and_area_are_used():
    seen = []
    fetch(serve(seen), [Request(igae())])
    assert "/es/00/false/BIE-BISE/2.0/" in seen[0].url.path


def test_since_is_applied_after_the_download():
    series = fetch(serve(), [Request(igae(), datetime.date(2026, 6, 15))])[0].series[0]
    assert [item.period for item in series.observations] == ["2026-06", "2026-07"]


def test_only_bank_and_area_are_accepted_as_fields():
    source = Inegi(client(), WITH_TOKEN)
    source.validate(igae(params={"bank": "BISE", "area": "09"}))
    with pytest.raises(CatalogError, match="for source 'inegi': banco"):
        source.validate(igae(params={"banco": "BISE"}))


def test_no_results_is_not_found():
    failure = fetch(serve(name="inegi_no_results.json", status=400), [Request(igae())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "BIE-BISE: no results (ErrorCode:100)")


def test_a_rejected_token_fails_every_remaining_request_without_leaking_it():
    batches = fetch(
        lambda request: httpx.Response(401, text=f"token {request.url.path.rsplit('/', 1)[-1]} no valido"),
        [Request(igae()), Request(entry("737121", "inegi"))],
    )
    failures = batches[0].failures
    assert [failure.outcome for failure in failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert INEGI_VALUE not in failures[0].reason


def test_a_server_error_is_a_network_error_once_the_retries_are_spent():
    failure = fetch(lambda _request: httpx.Response(500, text="boom"), [Request(igae())])[0].failures[0]
    assert failure.outcome is Outcome.NETWORK_ERROR


def test_any_other_http_error_is_a_source_error():
    failure = fetch(lambda _request: httpx.Response(418, text="teapot"), [Request(igae())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "HTTP 418: teapot")


def unknown_frequency(_request):
    payload = json.loads(fixture("inegi_bise.json"))
    payload["Series"][0]["FREQ"] = "99"
    return httpx.Response(200, json=payload)


def test_an_unknown_frequency_code_falls_back_to_the_catalog():
    series = fetch(unknown_frequency, [Request(igae(frequency=Frequency.MONTHLY))])[0].series[0]
    assert series.frequency is Frequency.MONTHLY


def test_an_unknown_frequency_code_without_a_catalog_frequency_fails():
    failure = fetch(unknown_frequency, [Request(igae())])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason.endswith("declare `frequency` in the catalog (INEGI FREQ '99')")


def test_without_a_token_nothing_is_requested():
    seen = []
    batch = fetch(serve(seen), [Request(igae())], credentials=Credentials())[0]
    assert seen == []
    assert batch.failures[0].reason == (
        "INEGI_TOKEN is missing. "
        "Get one at https://www.inegi.org.mx/app/api/indicadores/interna_v1_1/tokenVerify.aspx "
        "and run: data-pipeline setup"
    )


def test_a_period_that_comes_twice_fails_the_series():
    repeated = {"FREQ": "8", "UNIT": "1058", "OBSERVATIONS": [{"TIME_PERIOD": "2026/05", "OBS_VALUE": "1"}] * 2}
    failure = fetch(lambda _request: httpx.Response(200, json={"Series": [repeated]}), [Request(igae())])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason.startswith("period 2026-05 comes more than once")
