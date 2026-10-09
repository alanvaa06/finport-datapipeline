import datetime
import json
import math

import httpx

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.fred import Fred

from .helpers import NOT_IN_ALFRED, client, entry, fixture

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
    assert [observation.period for observation in series.observations] == ["2026-03", "2026-04", "2026-05", "2026-05"]
    assert series.observations[0].date == datetime.date(2026, 3, 31)
    assert math.isnan(series.observations[1].value)
    assert [observation.value for observation in series.observations[2:]] == [4.1, 4.2]
    assert [request.url.path for request in seen] == ["/fred/series", "/fred/series/observations"]
    assert "observation_start" not in seen[1].url.params


def test_every_vintage_is_asked_for_and_dated_by_the_day_it_was_published():
    seen = []
    series = fetch(serve(seen), [Request(entry("UNRATE", "fred"))])[0].series[0]
    params = seen[1].url.params
    assert (params["realtime_start"], params["realtime_end"]) == ("1776-07-04", "9999-12-31")
    # known by the end of the day FRED published it, never earlier: a release comes out during that day
    end = datetime.time.max
    assert [observation.published_at for observation in series.observations] == [
        datetime.datetime.combine(datetime.date(2026, 4, 3), end, datetime.UTC),
        datetime.datetime.combine(datetime.date(2026, 5, 8), end, datetime.UTC),
        datetime.datetime.combine(datetime.date(2026, 6, 5), end, datetime.UTC),
        datetime.datetime.combine(datetime.date(2026, 7, 3), end, datetime.UTC),
    ]


def test_since_also_starts_the_real_time_period():
    seen = []
    fetch(serve(seen), [Request(entry("UNRATE", "fred"), datetime.date(2024, 5, 1))])
    assert seen[1].url.params["realtime_start"] == "2024-05-01"


def test_a_series_outside_alfred_is_asked_again_without_vintages():
    seen = []

    def handler(request):
        seen.append(request)
        if "realtime_start" in request.url.params:
            return httpx.Response(400, text=fixture("fred_not_in_alfred.json"))
        if request.url.path.endswith("/observations"):  # without vintages: the current value of each date
            rows = json.loads(fixture("fred_observations.json"))["observations"]
            current = [row for row in rows if row["realtime_end"] == "9999-12-31"]
            return httpx.Response(200, json={"count": len(current), "observations": current})
        return serve()(request)

    series = fetch(handler, [Request(entry("SP500", "fred"))])[0].series[0]
    assert "realtime_start" not in seen[2].url.params
    assert all(observation.published_at is None for observation in series.observations)
    assert [observation.period for observation in series.observations] == ["2026-03", "2026-04", "2026-05"]
    assert series.observations[-1].value == 4.2


def vintage_row(date, value, published):
    return {"realtime_start": published, "realtime_end": "9999-12-31", "date": date, "value": value}


def test_too_many_vintage_dates_are_asked_for_in_windows_and_clipped_repeats_dropped(monkeypatch):
    monkeypatch.setattr("data_pipeline.store.sources.fred.MAX_VINTAGES", 2)
    dates = ["2026-01-05", "2026-02-06", "2026-03-06", "2026-04-03", "2026-05-08"]
    answers = {
        "2026-01-05": [vintage_row("2025-12-01", "4.4", "2026-01-05")],
        "2026-03-06": [vintage_row("2025-12-01", "4.4", "2026-03-06"), vintage_row("2026-02-01", "4.5", "2026-03-06")],
        "2026-05-08": [vintage_row("2025-12-01", "4.3", "2026-05-08"), vintage_row("2026-02-01", "4.5", "2026-05-08")],
    }
    windows = []

    def handler(request):
        params = request.url.params
        if request.url.path.endswith("/series"):
            return serve()(request)
        if request.url.path.endswith("/vintagedates"):
            return httpx.Response(200, json={"count": len(dates), "vintage_dates": dates})
        if params["realtime_start"] == "1776-07-04":
            return httpx.Response(400, text=fixture("fred_too_many_vintages.json"))
        windows.append((params["realtime_start"], params["realtime_end"]))
        rows = answers[params["realtime_start"]]
        return httpx.Response(200, json={"count": len(rows), "observations": rows})

    series = fetch(handler, [Request(entry("DGS10", "fred"))])[0].series[0]
    assert windows == [("2026-01-05", "2026-03-05"), ("2026-03-06", "2026-05-07"), ("2026-05-08", "9999-12-31")]
    assert [(o.period, o.value, o.published_at.date().isoformat()) for o in series.observations] == [
        ("2025-12", 4.4, "2026-01-05"),
        ("2025-12", 4.3, "2026-05-08"),
        ("2026-02", 4.5, "2026-03-06"),
    ]


def test_every_page_of_a_long_answer_is_read():
    first = [vintage_row("2026-03-01", "4.0", "2026-04-03"), vintage_row("2026-04-01", "4.1", "2026-05-08")]
    last = [vintage_row("2026-05-01", "4.2", "2026-06-05")]
    offsets = []

    def handler(request):
        if request.url.path.endswith("/series"):
            return serve()(request)
        offset = int(request.url.params.get("offset", "0"))
        offsets.append(offset)
        return httpx.Response(200, json={"count": 3, "observations": first if offset == 0 else last})

    series = fetch(handler, [Request(entry("UNRATE", "fred"))])[0].series[0]
    assert offsets == [0, 2]
    assert [observation.period for observation in series.observations] == ["2026-03", "2026-04", "2026-05"]


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


def test_a_window_still_refused_for_too_many_vintages_is_split_in_two(monkeypatch):
    monkeypatch.setattr("data_pipeline.store.sources.fred.MAX_VINTAGES", 4)
    dates = ["2026-01-05", "2026-02-06", "2026-03-06", "2026-04-03"]
    windows = []

    def handler(request):
        params = request.url.params
        if request.url.path.endswith("/series"):
            return serve()(request)
        if request.url.path.endswith("/vintagedates"):
            return httpx.Response(200, json={"count": len(dates), "vintage_dates": dates})
        if params["realtime_start"] in ("1776-07-04", "2026-01-05") and params["realtime_end"] == "9999-12-31":
            return httpx.Response(400, text=fixture("fred_too_many_vintages.json"))
        windows.append((params["realtime_start"], params["realtime_end"]))
        rows = [vintage_row("2025-12-01", "4.4", params["realtime_start"])]
        return httpx.Response(200, json={"count": 1, "observations": rows})

    series = fetch(handler, [Request(entry("DGS10", "fred"))])[0].series[0]
    assert windows == [("2026-01-05", "2026-03-05"), ("2026-03-06", "9999-12-31")]
    assert [observation.value for observation in series.observations] == [4.4]


def test_a_key_echoed_across_the_cut_of_the_message_leaves_no_prefix():
    def echo(request):
        echoed = " echoed: api_key=" + request.url.params["api_key"]
        return httpx.Response(400, text='{"error_message":"Bad Request. ' + "." * 240 + echoed + '"}')

    batches = fetch(echo, [Request(entry("UNRATE", "fred"))])
    reason = batches[0].failures[0].reason
    assert KEY[:8] not in reason
    assert "api_key=***" in reason


def observations(rows, *, in_alfred=True):
    """A FRED that answers /series from the fixture and /series/observations with `rows`."""

    def handler(request):
        if not request.url.path.endswith("/observations"):
            return httpx.Response(200, text=fixture("fred_series.json"))
        if "realtime_start" in request.url.params and not in_alfred:
            return httpx.Response(400, json=NOT_IN_ALFRED)
        return httpx.Response(200, json={"count": len(rows), "observations": rows})

    return handler


def test_a_period_that_comes_twice_fails_the_series():
    rows = [{"date": "2026-05-01", "value": "4.1"}, {"date": "2026-05-15", "value": "4.3"}]
    failure = fetch(observations(rows, in_alfred=False), [Request(entry("UNRATE", "fred"))])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason.startswith("period 2026-05 comes more than once")
    dated = [{**row, "realtime_start": "2026-06-05"} for row in rows]
    assert fetch(observations(dated), [Request(entry("UNRATE", "fred"))])[0].failures[0].outcome is Outcome.SOURCE_ERROR
