import datetime
import json
import math

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.bls import Bls, groups, windows

from .helpers import client, entry

KEY = "0123456789abcdef0123456789abcdef"
WITH_KEY = Credentials({keys.BLS: KEY})
TODAY = datetime.date(2026, 6, 6)
UNRATE = {(2026, "M04"): "4.0", (2026, "M05"): "4.1", (2025, "M13"): "4.2", (2001, "M01"): "4.2"}
TITLE = {"series_title": "(Seas) Unemployment Rate", "seasonality": "Seasonally Adjusted"}


class Server:
    """A fake BLS. `data` maps a series id to {(year, period code): value}."""

    def __init__(self, data, titles=None):
        self.data = data
        self.titles = titles or {}
        self.calls = []
        self.refusals = {}  # call number (from 1) -> message of a REQUEST_NOT_PROCESSED answer
        self.statuses = {}  # call number -> status to answer with instead of REQUEST_SUCCEEDED
        self.blank = set()  # call numbers answered without a single observation
        self.status = 200

    def __call__(self, request):
        body = json.loads(request.content)
        self.calls.append(body)
        if self.status != 200:
            return httpx.Response(self.status, text="broken")
        refusal = self.refusals.get(len(self.calls))
        if refusal:
            return httpx.Response(200, json={"status": "REQUEST_NOT_PROCESSED", "message": [refusal], "Results": {}})
        first, last = int(body["startyear"]), int(body["endyear"])
        series = []
        messages = []
        for series_id in body["seriesid"]:
            if series_id not in self.data:
                messages.append(f"Series does not exist for Series {series_id}")
            points = [
                {"year": str(year), "period": code, "periodName": "", "value": value, "footnotes": [{}]}
                for (year, code), value in sorted(self.data.get(series_id, {}).items(), reverse=True)
                if first <= year <= last and len(self.calls) not in self.blank
            ]
            item = {"seriesID": series_id, "data": points}
            if series_id in self.titles:
                item["catalog"] = self.titles[series_id]
            series.append(item)
        return httpx.Response(
            200,
            json={
                "status": self.statuses.get(len(self.calls), "REQUEST_SUCCEEDED"),
                "message": messages,
                "Results": {"series": series},
            },
        )

    def spans(self):
        return [(call["startyear"], call["endyear"]) for call in self.calls]


def source(server, credentials=WITH_KEY):
    return Bls(client(server, secrets=credentials.secrets()), credentials, today=lambda: TODAY)


def fetch(server, requests, credentials=WITH_KEY):
    return list(source(server, credentials).fetch(requests))


def bls(series_id, **fields):
    return entry(series_id, "bls", **fields)


def test_windows_cut_a_span_into_twenty_year_pieces():
    assert windows(1990, 2026) == [(1990, 2009), (2010, 2026)]
    assert windows(2020, 2026) == [(2020, 2026)]
    assert windows(2027, 2026) == []


def test_groups_split_by_start_year_and_then_into_fifties():
    recent = [Request(bls(f"A{number}"), datetime.date(2024, 5, 1)) for number in range(120)]
    older = [Request(bls("B"), datetime.date(2019, 1, 1)), Request(bls("C"))]
    found = groups([*recent, *older])
    assert [(year, len(members)) for year, members in found] == [
        (2024, 50),
        (2024, 50),
        (2024, 20),
        (2019, 1),
        (None, 1),
    ]


def test_an_incremental_request_asks_one_window_from_the_since_year():
    server = Server({"LNS14000000": UNRATE}, {"LNS14000000": TITLE})
    batches = fetch(server, [Request(bls("LNS14000000"), datetime.date(2024, 5, 1))])
    assert server.spans() == [("2024", "2026")]
    assert server.calls[0]["registrationkey"] == KEY
    assert server.calls[0]["catalog"] is True
    series = batches[0].series[0]
    assert (series.key, series.name, series.seasonal_adjustment) == (
        "bls:LNS14000000",
        "(Seas) Unemployment Rate",
        "SA",
    )
    assert series.frequency is Frequency.MONTHLY
    assert [(item.period, item.value) for item in series.observations] == [("2026-04", 4.0), ("2026-05", 4.1)]
    assert series.observations[1].date == datetime.date(2026, 5, 31)


def test_annual_averages_are_ignored():
    server = Server({"X": {(2025, "M12"): "4.0", (2025, "M13"): "9.9"}})
    series = fetch(server, [Request(bls("X"), datetime.date(2025, 1, 1))])[0].series[0]
    assert [(item.period, item.value) for item in series.observations] == [("2025-12", 4.0)]


def test_a_first_load_walks_back_until_a_window_is_empty():
    server = Server({"LNS14000000": UNRATE})
    series = fetch(server, [Request(bls("LNS14000000"))])[0].series[0]
    assert server.spans() == [("2007", "2026"), ("1987", "2006"), ("1967", "1986")]
    assert [item.period for item in series.observations] == ["2001-01", "2026-04", "2026-05"]
    assert series.name == "LNS14000000"


OLD = {(year, "M01"): "1.0" for year in range(1990, 2003)}  # discontinued in 2002
LIVE = {(year, "M01"): "2.0" for year in range(1980, 2027)}


def test_a_series_discontinued_long_ago_loads_alone_as_it_does_in_a_group():
    alone = Server({"OLD": OLD})
    (series,) = fetch(alone, [Request(bls("OLD"))])[0].series
    assert len(series.observations) == 13
    assert alone.spans() == [("2007", "2026"), ("1987", "2006"), ("1967", "1986")]
    grouped = fetch(Server({"OLD": OLD, "LIVE": LIVE}), [Request(bls("OLD")), Request(bls("LIVE"))])[0]
    assert [len(item.observations) for item in grouped.series] == [13, 47]


def test_each_series_stops_walking_back_where_its_own_history_starts():
    server = Server({"OLD": OLD, "LIVE": {**LIVE, (1955, "M01"): "2.0"}})
    fetch(server, [Request(bls("OLD")), Request(bls("LIVE"))])
    assert [(call["startyear"], call["seriesid"]) for call in server.calls] == [
        ("2007", ["OLD", "LIVE"]),
        ("1987", ["OLD", "LIVE"]),
        ("1967", ["OLD", "LIVE"]),
        ("1947", ["LIVE"]),
        ("1927", ["LIVE"]),
    ]


def test_a_series_the_api_says_does_not_exist_ends_its_walk_at_once():
    server = Server({})
    failure = fetch(server, [Request(bls("NOPE"))])[0].failures[0]
    assert len(server.calls) == 1
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "Series does not exist for Series NOPE")


def test_a_declared_start_replaces_the_walk_back():
    server = Server({"LNS14000000": UNRATE})
    fetch(server, [Request(bls("LNS14000000", start=datetime.date(1990, 1, 1)))])
    assert server.spans() == [("1990", "2009"), ("2010", "2026")]


def test_fifty_series_travel_in_one_request():
    data = {f"S{number}": {(2026, "M05"): "1.0"} for number in range(60)}
    server = Server(data)
    batches = fetch(server, [Request(bls(series_id), datetime.date(2026, 1, 1)) for series_id in data])
    assert [len(call["seriesid"]) for call in server.calls] == [50, 10]
    assert [len(batch.series) for batch in batches] == [50, 10]


def test_quarterly_and_annual_series_are_read_by_their_period_codes():
    server = Server({"Q": {(2026, "Q01"): "5", (2025, "Q05"): "9"}, "A": {(2025, "A01"): "7"}})
    batch = fetch(server, [Request(bls("Q"), datetime.date(2025, 1, 1)), Request(bls("A"), datetime.date(2025, 1, 1))])[
        0
    ]
    quarterly, annual = batch.series
    assert (quarterly.frequency, [item.period for item in quarterly.observations]) == (Frequency.QUARTERLY, ["2026Q1"])
    assert (annual.frequency, [item.period for item in annual.observations]) == (Frequency.ANNUAL, ["2025"])


def test_a_semiannual_series_is_an_unsupported_frequency():
    server = Server({"S": {(2025, "S01"): "1", (2025, "S02"): "2"}})
    failure = fetch(server, [Request(bls("S"), datetime.date(2025, 1, 1))])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason == "unsupported frequency (period codes S)"


def test_a_missing_value_is_kept_as_missing():
    server = Server({"X": {(2026, "M05"): "-"}})
    series = fetch(server, [Request(bls("X"), datetime.date(2026, 1, 1))])[0].series[0]
    assert math.isnan(series.observations[0].value)


def test_an_unknown_series_fails_alone_with_the_message_of_the_api():
    server = Server({"LNS14000000": UNRATE})
    batch = fetch(
        server,
        [Request(bls("NOPE"), datetime.date(2026, 1, 1)), Request(bls("LNS14000000"), datetime.date(2026, 1, 1))],
    )[0]
    assert [series.key for series in batch.series] == ["bls:LNS14000000"]
    assert (batch.failures[0].outcome, batch.failures[0].reason) == (
        Outcome.NOT_FOUND,
        "Series does not exist for Series NOPE",
    )


def test_without_a_key_nothing_is_requested():
    server = Server({})
    batch = fetch(server, [Request(bls("X"))], credentials=Credentials())[0]
    assert server.calls == []
    assert (batch.failures[0].outcome, batch.failures[0].reason) == (
        Outcome.KEY_ERROR,
        "BLS_API_KEY is missing. Get one at https://data.bls.gov/registrationEngine/ and run: data-pipeline setup",
    )


def test_the_daily_threshold_raises_quota_and_yields_nothing_of_that_group():
    server = Server({"A": {(2026, "M05"): "1"}, "B": {(2026, "M05"): "2", (2006, "M05"): "3"}})
    server.refusals[3] = "REQUEST_NOT_PROCESSED: daily threshold for total number of requests has been reached"
    stream = source(server).fetch([Request(bls("A"), datetime.date(2026, 1, 1)), Request(bls("B"))])
    assert [series.key for series in next(stream).series] == ["bls:A"]
    with pytest.raises(QuotaExhaustedError, match="daily threshold"):
        next(stream)


def test_a_rejected_key_fails_every_remaining_request_without_leaking_it():
    server = Server({"A": {(2026, "M05"): "1"}})
    server.refusals[1] = f"The registration key {KEY} is invalid."
    batches = fetch(
        server, [Request(bls("A"), datetime.date(2026, 1, 1)), Request(bls("B"), datetime.date(2020, 1, 1))]
    )
    assert len(server.calls) == 1
    failures = batches[0].failures
    assert [(failure.entry.key, failure.outcome) for failure in failures] == [
        ("bls:A", Outcome.KEY_ERROR),
        ("bls:B", Outcome.KEY_ERROR),
    ]
    assert KEY not in failures[0].reason


def test_any_other_refusal_fails_the_group_and_the_next_group_continues():
    server = Server({"A": {(2026, "M05"): "1"}, "B": {(2026, "M05"): "2"}})
    server.refusals[1] = "Invalid Series for Series A"
    batches = fetch(
        server, [Request(bls("A"), datetime.date(2026, 1, 1)), Request(bls("B"), datetime.date(2020, 1, 1))]
    )
    assert (batches[0].failures[0].outcome, batches[0].failures[0].reason) == (
        Outcome.SOURCE_ERROR,
        "REQUEST_NOT_PROCESSED: Invalid Series for Series A",
    )
    assert [series.key for series in batches[1].series] == ["bls:B"]


def test_a_network_failure_fails_the_group():
    server = Server({})
    server.status = 503
    failure = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].failures[0]
    assert failure.outcome is Outcome.NETWORK_ERROR


def test_a_plain_http_error_fails_the_group():
    server = Server({})
    server.status = 400
    failure = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "HTTP 400: broken")


def test_an_answer_that_is_not_a_success_and_brings_no_data_fails_the_group():
    server = Server({"A": {(2026, "M05"): "1"}})
    server.statuses[1] = "REQUEST_FAILED"
    server.blank.add(1)
    failure = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "REQUEST_FAILED")


def test_an_unusual_status_that_still_brings_data_is_read():
    server = Server({"A": {(2026, "M05"): "1"}})
    server.statuses[1] = "REQUEST_PARTIALLY_PROCESSED"
    series = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].series[0]
    assert [item.period for item in series.observations] == ["2026-05"]


def test_a_period_that_comes_twice_fails_only_that_series():
    def handler(_request):
        twice = [{"year": "2026", "period": "M05", "value": value} for value in ("1.0", "2.0")]
        series = [{"seriesID": "X", "data": twice}, {"seriesID": "Y", "data": twice[:1]}]
        return httpx.Response(200, json={"status": "REQUEST_SUCCEEDED", "message": [], "Results": {"series": series}})

    batch = fetch(
        handler, [Request(bls("X"), datetime.date(2026, 1, 1)), Request(bls("Y"), datetime.date(2026, 1, 1))]
    )[0]
    assert [series.key for series in batch.series] == ["bls:Y"]
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason.startswith("period 2026-05 comes more than once")
