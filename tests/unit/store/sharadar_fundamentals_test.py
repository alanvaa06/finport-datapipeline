import datetime
import json
import math
import re

import httpx
import pandas as pd
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.api import Store
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

from .helpers import NOW, client, entry, fixture

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


def test_the_list_holds_105_distinct_indicators_and_the_fixture_row_has_exactly_those_and_the_descriptive_columns():
    # only the live test can check the list against what Sharadar really sends
    assert len(INDICATORS) == 105
    assert len(set(INDICATORS)) == 105
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
    assert (row["reportperiod"], row["fiscalperiod"], row["datekey"]) == ("2025-12-31", "2025-Q4", "2026-02-05")
    assert row["assets"] == 4_500_000_000_000.0  # a large value comes as text
    assert row["revenue"] == 1_200_000_000.0  # and a smaller one as a JSON number
    for column in INDICATORS:  # every one of the 105, against what the fixture holds for it
        sent = AS_REPORTED[column]
        if sent is None:
            assert math.isnan(row[column]), column  # null is missing, never zero
        else:
            assert row[column] == float(sent), column
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
    assert most_recent["datekey"] == "2025-12-31"  # the end of its period, not a day it was known
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
    ],
)
def test_a_row_without_what_tells_it_apart_or_dates_it_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


def test_a_row_with_an_indicator_that_is_not_a_number_fails_its_ticker():
    (batch,) = fetch(lambda _request: answer([{**AS_REPORTED, "revenue": "lots"}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search("unexpected answer \\(ValueError", batch.failures[0].reason)


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


# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


def filed(row, calendardate, datekey, lastupdated, **values):
    """`row` for another period, filed on `datekey`, as sent after the update of `lastupdated`."""
    return {**row, "calendardate": calendardate, "date": datekey, "lastupdated": lastupdated, **values}


def updated(row, lastupdated, **values):
    """`row` as Sharadar sends it after a later update of the ticker."""
    return {**row, "lastupdated": lastupdated, **values}


NEXT_QUARTER = [
    filed(
        AS_REPORTED,
        "2026-03-31",
        "2026-05-01",
        "2026-05-02",
        reportperiod="2026-03-31",
        fiscalperiod="2026-Q1",
        revenue=1_300_000_000,
    ),
    filed(
        MOST_RECENT,
        "2026-03-31",
        "2026-03-31",
        "2026-05-02",
        reportperiod="2026-03-31",
        fiscalperiod="2026-Q1",
        revenue=1_300_000_000,
    ),
    updated(AS_REPORTED, "2026-05-02"),
    updated(MOST_RECENT, "2026-05-02"),
]


class Vendor:
    """A fake Sharadar whose answer the test can change between syncs."""

    def __init__(self):
        self.rows = INVENTED
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        assert request.url.path == "/v1.0/data/fundamentals"
        return answer(self.rows if request.url.params["ticker"] == "XTEST" else [])


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_fundamentals\n  ids: [XTEST]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def stored(tmp_path):
    """Every version the store holds of XTEST."""
    return pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_fundamentals" / "XTEST.parquet")


def revenue(table):
    """{(dimension, calendardate): revenue} of a read."""
    return {(row.dimension, row.calendardate): row.revenue for row in table.itertuples()}


def test_a_first_sync_stores_both_rows_and_a_second_with_the_same_answer_adds_nothing(store, vendor, clock):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (2, 0)
    table = store.table("sharadar_fundamentals", "XTEST")
    assert list(table.columns) == [*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS]
    assert revenue(table) == {("ARQ", "2025-12-31"): 1_200_000_000.0, ("MRQ", "2025-12-31"): 1_200_000_000.0}
    # every fixture row has a null prefdivis: a missing value received again is the same version
    assert table["prefdivis"].isna().all()
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert vendor.calls == 2  # one call per ticker per run, the whole history each time


def test_each_row_is_stored_with_the_moment_it_was_known(store, tmp_path):
    known = dict(zip(stored(tmp_path)["dimension"], stored(tmp_path)["published_at"], strict=True))
    assert known == {"ARQ": pd.Timestamp(end_of("2026-02-05")), "MRQ": pd.Timestamp(end_of("2026-02-06"))}


def test_a_new_filing_adds_its_rows_and_another_version_of_every_most_recent_row(store, vendor, clock, tmp_path):
    vendor.rows = NEXT_QUARTER
    clock["now"] = LATER
    report = store.sync()
    # the two rows of the new quarter, and the old MRQ row once more: same numbers, a later lastupdated
    assert (report.sources[0].new, report.sources[0].revised) == (2, 1)
    versions = stored(tmp_path)
    assert len(versions) == 5
    old_quarter = versions[versions["calendardate"] == "2025-12-31"]
    assert sorted(old_quarter["published_at"]) == [
        pd.Timestamp(end_of("2026-02-05")),  # the ARQ row, dated by its filing whatever lastupdated says
        pd.Timestamp(end_of("2026-02-06")),
        pd.Timestamp(end_of("2026-05-02")),
    ]
    assert revenue(store.table("sharadar_fundamentals", "XTEST", dimension="ARQ")) == {
        ("ARQ", "2025-12-31"): 1_200_000_000.0,
        ("ARQ", "2026-03-31"): 1_300_000_000.0,
    }


def test_as_of_a_day_before_a_filing_does_not_see_it(store, vendor, clock):
    vendor.rows = NEXT_QUARTER
    clock["now"] = LATER
    store.sync()
    # the as-reported panel as it stood that day, with no look-ahead
    before = store.table("sharadar_fundamentals", "XTEST", dimension="ARQ", as_of="2026-04-30")
    assert list(before["calendardate"]) == ["2025-12-31"]
    on_the_day = store.table("sharadar_fundamentals", "XTEST", dimension="ARQ", as_of="2026-05-01")
    assert list(on_the_day["calendardate"]) == ["2025-12-31", "2026-03-31"]
    # the most-recent rows of the new quarter are known only from the day of Sharadar's update
    most_recent = store.table("sharadar_fundamentals", "XTEST", dimension="MRQ", as_of="2026-05-01")
    assert list(most_recent["calendardate"]) == ["2025-12-31"]
    assert store.table("sharadar_fundamentals", "XTEST", as_of="2026-02-04").empty


def test_a_restated_most_recent_value_is_a_new_version_and_as_of_reads_the_one_before(store, vendor, clock):
    vendor.rows = [AS_REPORTED, updated(MOST_RECENT, "2026-03-15", revenue=1_150_000_000, fiscalperiod="2025-Q4R")]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    now = store.table("sharadar_fundamentals", "XTEST", dimension="MRQ")
    assert (now["revenue"].iloc[0], now["fiscalperiod"].iloc[0]) == (1_150_000_000.0, "2025-Q4R")
    before = store.table("sharadar_fundamentals", "XTEST", dimension="MRQ", as_of="2026-03-14")
    assert (before["revenue"].iloc[0], before["fiscalperiod"].iloc[0]) == (1_200_000_000.0, "2025-Q4")
    # the as-reported row keeps what was filed
    assert store.table("sharadar_fundamentals", "XTEST", dimension="ARQ")["revenue"].iloc[0] == 1_200_000_000.0


def test_an_as_reported_row_corrected_without_a_new_datekey_is_known_from_the_day_of_the_original(
    store, vendor, clock, tmp_path
):
    vendor.rows = [{**AS_REPORTED, "revenue": 1_100_000_000}, MOST_RECENT]  # same datekey: Sharadar's `date`
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    versions = stored(tmp_path)
    arq = versions[versions["dimension"] == "ARQ"]
    assert len(arq) == 2
    assert set(arq["published_at"]) == {pd.Timestamp(end_of("2026-02-05"))}  # both versions, the same moment
    assert store.table("sharadar_fundamentals", "XTEST", dimension="ARQ")["revenue"].iloc[0] == 1_100_000_000.0
    # The look-ahead the spec accepts: the correction was downloaded long after this day, but it counts
    # as known from the day of the original filing, so an as_of between the two reads it, not the original.
    between = store.table("sharadar_fundamentals", "XTEST", dimension="ARQ", as_of="2026-03-01")
    assert between["revenue"].iloc[0] == 1_100_000_000.0


def test_the_index_row_of_a_ticker(store):
    info = store.info("sharadar_fundamentals:XTEST")
    assert (info.kind, info.name) == ("table", "Fundamentals of XTEST (Sharadar)")
    assert (info.frequency, info.last_period) == ("", "2025-12-31")
    assert info.label == "[Sharadar: XTEST, 2025-12-31, fetched 2026-06-06]"
    row = store.index().iloc[0]
    assert json.loads(row["attrs"]) == {}
    assert row["stale_after_days"] == 200


def test_status_uses_the_two_hundred_days(store, clock):
    clock["now"] = datetime.datetime(2026, 7, 19, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"  # two hundred days after the last period
    clock["now"] = datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2025-12-31 (201 days ago)"


def test_a_ticker_sharadar_does_not_have_fails_in_the_index(tmp_path, vendor, clock):
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    store = Store(
        tmp_path / "store",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    store.add("sharadar_fundamentals", ["XGONE"])
    report = store.sync()
    assert report.sources[0].failed == (
        ("sharadar_fundamentals:XGONE", "not_found: Sharadar has no fundamentals for 'XGONE'"),
    )
