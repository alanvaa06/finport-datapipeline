import datetime
import math

import httpx

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
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
    assert batches[0].failures[0].reason == (
        "FRED_API_KEY is missing. Get one at https://fredaccount.stlouisfed.org/apikey and run: data-pipeline setup"
    )


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
