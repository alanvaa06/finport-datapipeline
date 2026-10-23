import datetime
import json
import math
import re

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sharadar_fundamentals import (
    ATTRIBUTE_COLUMNS,
    INDICATORS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SharadarFundamentals,
    read_rows,
)

from .helpers import client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
INVENTED = json.loads(fixture("sharadar_fundamentals.json"))["data"]
AS_REPORTED, MOST_RECENT = INVENTED
DESCRIBED = ("ticker", "dimension", "calendardate", "date", "reportperiod", "fiscalperiod", "lastupdated")


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_fundamentals", **fields)


def end_of(day):
    return datetime.datetime.combine(datetime.date.fromisoformat(day), datetime.time.max, datetime.UTC)


def without(row, field):
    return {name: found for name, found in row.items() if name != field}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarFundamentals(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarFundamentals.kind is Kind.TABLE
    assert SharadarFundamentals.daily_budget is None
    assert SharadarFundamentals.requests_per_minute == 120
    assert SharadarFundamentals.held_by == ()
    assert sources.TITLES["sharadar_fundamentals"] == "Sharadar"


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"dimension": "ARQ"}, "unknown field\\(s\\) for source 'sharadar_fundamentals': dimension"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarFundamentals(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_the_indicators_are_the_105_numeric_columns_of_the_answer_in_its_order():
    assert len(INDICATORS) == 105
    in_the_answer = tuple(name for name in AS_REPORTED if name not in DESCRIBED)
    assert in_the_answer == INDICATORS
    assert VALUE_COLUMNS == INDICATORS
    assert KEY_COLUMNS == ("ticker", "dimension", "calendardate")
    assert ATTRIBUTE_COLUMNS == ("reportperiod", "fiscalperiod", "datekey")


def test_a_row_keeps_every_indicator_as_a_number_and_describes_its_period_as_text():
    row, _most_recent = read_rows(INVENTED, "XTEST")
    assert set(row) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS, "published_at"}
    assert [row[column] for column in KEY_COLUMNS] == ["XTEST", "ARQ", "2025-12-31"]
    assert row["date"] == datetime.date(2025, 12, 31)
    assert (row["reportperiod"], row["fiscalperiod"], row["datekey"]) == ("2025-12-27", "2026-Q1", "2026-02-05")
    assert row["assets"] == 4_500_000_000_000.0  # a large value comes as text
    assert row["revenue"] == 1_200_000_000.0  # and a smaller one as a JSON number
    assert row["eps"] == 30.5
    assert math.isnan(row["prefdivis"])  # null is missing, never zero
    assert "lastupdated" not in row


def test_a_listed_column_the_answer_lacks_is_missing_and_an_unlisted_one_is_ignored():
    (row,) = read_rows([{**without(AS_REPORTED, "ncfo"), "newindicator": 7.0}], "XTEST")
    assert math.isnan(row["ncfo"])
    assert "newindicator" not in row
    assert row["netinc"] == 66.5


def test_an_as_reported_row_is_known_from_the_end_of_the_day_of_its_filing():
    as_reported, _most_recent = read_rows(INVENTED, "XTEST")
    assert as_reported["published_at"] == end_of("2026-02-05")  # its datekey, Sharadar's `date`


def test_a_most_recent_row_is_known_from_the_end_of_the_day_sharadar_last_touched_the_ticker():
    _as_reported, most_recent = read_rows(INVENTED, "XTEST")
    assert most_recent["datekey"] == "2025-12-27"  # the end of its period, not a day it was known
    assert most_recent["published_at"] == end_of("2026-02-06")  # its lastupdated


@pytest.mark.parametrize("dimension", ["ARQ", "ARY", "ART", "MRQ", "MRY", "MRT"])
def test_each_of_the_six_dimensions_is_dated_by_its_kind(dimension):
    (row,) = read_rows([{**AS_REPORTED, "dimension": dimension}], "XTEST")
    assert row["published_at"] == end_of("2026-02-05" if dimension.startswith("AR") else "2026-02-06")


def test_the_ticker_of_a_row_is_the_one_that_was_asked_for_whatever_the_answer_says():
    rows = read_rows([{**row, "ticker": "OTHER"} for row in INVENTED], "XTEST")
    assert {row["ticker"] for row in rows} == {"XTEST"}


def test_an_as_reported_period_filed_twice_is_two_versions():
    amended = {**AS_REPORTED, "date": "2026-03-10", "revenue": 1_250_000_000}
    first, second = read_rows([AS_REPORTED, amended], "XTEST")
    assert (first["published_at"], second["published_at"]) == (end_of("2026-02-05"), end_of("2026-03-10"))


def test_a_period_twice_known_from_the_same_moment_is_refused():
    (batch,) = fetch(lambda _request: answer([MOST_RECENT, {**MOST_RECENT, "revenue": 1.0}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds MRQ 2025-12-31 twice"


def test_a_dimension_sharadar_does_not_have_is_refused():
    (batch,) = fetch(lambda _request: answer([{**AS_REPORTED, "dimension": "XRQ"}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "a row whose dimension is not one of ARQ, ARY, ART, MRQ, MRY, MRT: 'XRQ'"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (without(AS_REPORTED, "calendardate"), "unexpected answer \\(KeyError: 'calendardate'"),
        (without(AS_REPORTED, "dimension"), "unexpected answer \\(KeyError: 'dimension'"),
        (without(AS_REPORTED, "date"), "unexpected answer \\(KeyError: 'date'"),
        (without(MOST_RECENT, "lastupdated"), "unexpected answer \\(KeyError: 'lastupdated'"),
        ({**AS_REPORTED, "calendardate": "2025Q4"}, "unexpected answer \\(ValueError"),
        ({**AS_REPORTED, "revenue": "lots"}, "unexpected answer \\(ValueError"),
    ],
)
def test_a_row_without_what_tells_it_apart_or_dates_it_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_the_fundamentals_of_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(INVENTED)

    fetch(handler, [Request(ticker()), Request(ticker("XOTHER", start=datetime.date(2020, 1, 1)))])
    first, second = seen
    assert first.url.path == "/v1.0/data/fundamentals"
    assert dict(first.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (second.url.params["ticker"], second.url.params["date.gte"]) == ("XOTHER", "2020-01-01")


def test_an_answer_is_one_versioned_table_of_the_ticker():
    (batch,) = fetch(lambda _request: answer(INVENTED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_fundamentals:XTEST", "Fundamentals of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == (ATTRIBUTE_COLUMNS, True)
    assert (table.stale_after_days, table.attrs) == (200, {})
    assert len(table.rows) == 2


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker()), Request(ticker("XOTHER"))], Credentials())
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left_and_names_the_table():
    def handler(_request):
        return httpx.Response(403, json={"error": "refused"})

    (batch,) = fetch(handler, [Request(ticker()), Request(ticker("XOTHER"))])
    assert [(failure.entry.source_id, failure.outcome) for failure in batch.failures] == [
        ("XTEST", Outcome.KEY_ERROR),
        ("XOTHER", Outcome.KEY_ERROR),
    ]
    assert "the table 'fundamentals'" in batch.failures[0].reason


def test_a_ticker_without_fundamentals_is_not_found():
    requests = [Request(ticker("XGONE")), Request(ticker("XLATE", start=datetime.date(2020, 1, 1)))]
    gone, late = fetch(lambda _request: answer([]), requests)
    assert gone.failures[0].outcome is Outcome.NOT_FOUND
    assert gone.failures[0].reason == "Sharadar has no fundamentals for 'XGONE'"
    assert late.failures[0].reason == "Sharadar has no fundamentals for 'XLATE' from 2020-01-01 on"


def test_a_failed_ticker_fails_alone():
    def handler(request):
        if request.url.params["ticker"] == "XBAD":
            return httpx.Response(200, text="<html>down</html>")
        return answer(INVENTED)

    failed, found = fetch(handler, [Request(ticker("XBAD")), Request(ticker())])
    assert failed.failures[0].outcome is Outcome.SOURCE_ERROR
    assert "not JSON" in failed.failures[0].reason
    assert found.tables[0].key == "sharadar_fundamentals:XTEST"
