import datetime
import math

import httpx

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.banxico import Banxico

from .helpers import client, entry, fixture

BMX_VALUE = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
WITH_TOKEN = Credentials({keys.BANXICO: BMX_VALUE})
TODAY = datetime.date(2026, 6, 6)


def serve(seen=None, metadata="banxico_metadata.json"):
    """Answer the metadata call and the data call from the fixtures."""

    def handler(request):
        if seen is not None:
            seen.append(request)
        name = "banxico_data.json" if "/datos" in request.url.path else metadata
        return httpx.Response(200, text=fixture(name))

    return handler


def fetch(handler, requests, credentials=WITH_TOKEN):
    source = Banxico(client(handler, secrets=credentials.secrets()), credentials, today=lambda: TODAY)
    return list(source.fetch(requests))


def fix(**fields):
    return entry("SF43718", "banxico", **fields)


def test_with_a_declared_frequency_one_call_downloads_the_series():
    seen = []
    series = fetch(serve(seen), [Request(fix(frequency=Frequency.DAILY))])[0].series[0]
    assert [request.url.path for request in seen] == ["/SieAPIRest/service/v1/series/SF43718/datos"]
    assert seen[0].headers["Bmx-Token"] == BMX_VALUE
    assert (series.key, series.frequency, series.units) == ("banxico:SF43718", Frequency.DAILY, "")
    assert series.name.startswith("Tipo de cambio Pesos por dolar")
    assert [item.period for item in series.observations] == ["2026-09-22", "2026-09-23", "2026-09-24"]
    assert series.observations[0].value == 18.4512
    assert math.isnan(series.observations[1].value)
    assert series.observations[2].value == 1018.3907


def test_since_becomes_a_date_range_in_the_path():
    seen = []
    fetch(serve(seen), [Request(fix(frequency=Frequency.DAILY), datetime.date(2026, 1, 1))])
    assert seen[0].url.path.endswith("/series/SF43718/datos/2026-01-01/2026-06-06")


def test_without_a_declared_frequency_the_metadata_gives_it_and_the_unit():
    seen = []
    series = fetch(serve(seen), [Request(fix())])[0].series[0]
    assert [request.url.path.rsplit("/", 1)[-1] for request in seen] == ["SF43718", "datos"]
    assert (series.frequency, series.units) == (Frequency.DAILY, "Pesos por Dolar")


def test_a_periodicity_the_store_does_not_know_is_an_unsupported_frequency():
    def handler(request):
        if "/datos" in request.url.path:
            return serve()(request)
        return httpx.Response(200, json={"bmx": {"series": [{"idSerie": "SF1", "periodicidad": "Quincenal"}]}})

    failure = fetch(handler, [Request(fix())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "unsupported frequency 'Quincenal'")


def test_an_unknown_series_is_not_found_and_the_next_one_still_runs():
    def handler(request):
        if "/NOPE" in request.url.path:
            return httpx.Response(404, text="no existe")
        return serve()(request)

    batches = fetch(
        handler, [Request(entry("NOPE", "banxico", frequency=Frequency.DAILY)), Request(fix(frequency=Frequency.DAILY))]
    )
    assert (batches[0].failures[0].outcome, batches[0].failures[0].reason) == (Outcome.NOT_FOUND, "HTTP 404: no existe")
    assert batches[1].series[0].key == "banxico:SF43718"


def test_an_error_payload_is_not_found():
    failure = fetch(
        lambda _request: httpx.Response(200, json={"error": {"mensaje": "Serie no encontrada"}}),
        [Request(fix(frequency=Frequency.DAILY))],
    )[0].failures[0]
    assert failure.outcome is Outcome.NOT_FOUND
    assert "Serie no encontrada" in failure.reason


def test_an_answer_without_data_is_not_found():
    failure = fetch(
        lambda _request: httpx.Response(200, json={"bmx": {"series": [{"idSerie": "SF43718", "titulo": "T"}]}}),
        [Request(fix(frequency=Frequency.DAILY))],
    )[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "Banxico returned no data for this series")


def test_a_rejected_token_fails_every_remaining_request_without_leaking_it():
    batches = fetch(
        lambda _request: httpx.Response(401, text=fixture("banxico_bad_token.json")),
        [Request(fix(frequency=Frequency.DAILY)), Request(entry("SP1", "banxico", frequency=Frequency.MONTHLY))],
    )
    failures = batches[0].failures
    assert [failure.outcome for failure in failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert BMX_VALUE not in failures[0].reason


def test_without_a_token_nothing_is_requested():
    seen = []
    batch = fetch(serve(seen), [Request(fix())], credentials=Credentials())[0]
    assert seen == []
    assert batch.failures[0].reason == (
        "BANXICO_TOKEN is missing. Get one at https://www.banxico.org.mx/SieAPIRest/service/v1/token "
        "and run: data-pipeline setup"
    )
