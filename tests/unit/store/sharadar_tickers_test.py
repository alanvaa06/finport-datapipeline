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
from data_pipeline.store.sources.sharadar_tickers import (
    ATTRIBUTE_COLUMNS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SharadarTickers,
    read_rows,
)

from .helpers import NOW, client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
INVENTED = json.loads(fixture("sharadar_tickers.json"))["data"]
STOCKS, FUNDAMENTALS = INVENTED


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_tickers", **fields)


def end_of(day):
    return datetime.datetime.combine(datetime.date.fromisoformat(day), datetime.time.max, datetime.UTC)


def without(row, field):
    return {name: found for name, found in row.items() if name != field}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarTickers(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarTickers.kind is Kind.TABLE
    assert SharadarTickers.daily_budget is None
    assert SharadarTickers.requests_per_minute == 120
    assert SharadarTickers.held_by == ()
    assert sources.TITLES["sharadar_tickers"] == "Sharadar"


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "stocks"}, "unknown field\\(s\\) for source 'sharadar_tickers': table"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarTickers(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_there_is_one_row_per_table_the_ticker_is_in():
    rows = read_rows(INVENTED, "XTEST")
    assert [(row["ticker"], row["table"]) for row in rows] == [("XTEST", "stocks"), ("XTEST", "fundamentals")]
    assert KEY_COLUMNS == ("ticker", "table")


def test_a_row_keeps_the_permaticker_as_a_number_and_the_rest_as_text():
    row, _fundamentals = read_rows(INVENTED, "XTEST")
    assert set(row) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS, "published_at"}
    assert VALUE_COLUMNS == ("permaticker",)
    assert len(ATTRIBUTE_COLUMNS) == 25
    assert row["permaticker"] == 900001.0
    assert (row["name"], row["sector"], row["isdelisted"]) == ("TEST CORP", "Testing", "N")
    assert row["siccode"] == "9999"  # sent as a number, stored as text
    assert (row["famaindustry"], row["relatedtickers"]) == ("", "")  # N/A and null are empty
    assert row["lastupdated"] == "2026-02-06"


def test_a_row_is_dated_and_known_by_its_lastupdated():
    row, _fundamentals = read_rows(INVENTED, "XTEST")
    assert row["date"] == datetime.date(2026, 2, 6)
    assert row["published_at"] == end_of("2026-02-06")


def test_the_ticker_of_a_row_is_the_one_that_was_asked_for_whatever_the_answer_says():
    rows = read_rows([{**row, "ticker": "OTHER"} for row in INVENTED], "XTEST")
    assert {row["ticker"] for row in rows} == {"XTEST"}


def test_a_permaticker_sharadar_leaves_empty_is_missing():
    (row,) = read_rows([{**STOCKS, "permaticker": None}], "XTEST")
    assert math.isnan(row["permaticker"])


def test_a_table_twice_in_one_answer_is_refused():
    (batch,) = fetch(lambda _request: answer([STOCKS, {**STOCKS, "sector": "Other"}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds the table 'stocks' twice"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (without(STOCKS, "table"), "unexpected answer \\(KeyError: 'table'"),
        ({**STOCKS, "table": None}, "a row without the table it describes: None"),
        ({**STOCKS, "table": "N/A"}, "a row without the table it describes: 'N/A'"),
        (without(STOCKS, "lastupdated"), "unexpected answer \\(KeyError: 'lastupdated'"),
        ({**STOCKS, "lastupdated": "February"}, "unexpected answer \\(ValueError"),
    ],
)
def test_a_row_without_its_table_or_its_day_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_the_record_of_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(INVENTED)

    fetch(handler, [Request(ticker()), Request(ticker("XOTHER", start=datetime.date(2020, 1, 1)))])
    first, second = seen
    assert first.url.path == "/v1.0/data/tickers"
    assert dict(first.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (second.url.params["ticker"], second.url.params["date.gte"]) == ("XOTHER", "2020-01-01")


def test_an_answer_is_one_versioned_table_of_the_ticker_that_never_grows_stale():
    (batch,) = fetch(lambda _request: answer(INVENTED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_tickers:XTEST", "Ticker record of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == (ATTRIBUTE_COLUMNS, True)
    assert (table.stale_after_days, table.attrs) == (None, {})


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker())], Credentials())
    assert batch.failures[0].outcome is Outcome.KEY_ERROR
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left_and_names_the_table():
    (batch,) = fetch(lambda _request: httpx.Response(403), [Request(ticker()), Request(ticker("XOTHER"))])
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "the table 'tickers'" in batch.failures[0].reason


def test_a_ticker_sharadar_has_no_record_of_is_not_found():
    requests = [Request(ticker("XGONE")), Request(ticker("XLATE", start=datetime.date(2020, 1, 1)))]
    gone, late = fetch(lambda _request: answer([]), requests)
    assert gone.failures[0].outcome is Outcome.NOT_FOUND
    assert gone.failures[0].reason == "Sharadar has no ticker record for 'XGONE'"
    assert late.failures[0].reason == "Sharadar has no ticker record for 'XLATE' from 2020-01-01 on"


# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


class Vendor:
    """A fake Sharadar whose answer the test can change between syncs."""

    def __init__(self):
        self.rows = INVENTED

    def __call__(self, request):
        assert request.url.path == "/v1.0/data/tickers"
        return answer(self.rows)


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_tickers\n  ids: [XTEST]\n", encoding="utf-8")
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


def test_a_first_sync_stores_a_row_per_table_and_the_same_answer_again_adds_nothing(store, clock, tmp_path):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (2, 0)
    record = store.table("sharadar_tickers", "XTEST")
    assert list(record.columns) == [*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS]
    assert list(record["table"]) == ["fundamentals", "stocks"]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_tickers" / "XTEST.parquet")) == 2


def test_a_new_sector_with_a_new_lastupdated_is_a_version_and_as_of_reads_the_old_one(store, vendor, clock):
    vendor.rows = [
        {**STOCKS, "sector": "Retesting", "lastupdated": "2026-05-04"},
        {**FUNDAMENTALS, "sector": "Retesting", "lastupdated": "2026-05-04"},
    ]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 2)
    now = store.table("sharadar_tickers", "XTEST", table="stocks")
    assert (now["sector"].iloc[0], now["lastupdated"].iloc[0]) == ("Retesting", "2026-05-04")
    assert now["date"].iloc[0] == pd.Timestamp("2026-05-04")
    before = store.table("sharadar_tickers", "XTEST", table="stocks", as_of="2026-05-03")
    assert (before["sector"].iloc[0], before["lastupdated"].iloc[0]) == ("Testing", "2026-02-06")
    assert store.table("sharadar_tickers", "XTEST", as_of="2026-02-05").empty


def test_a_change_sharadar_does_not_stamp_with_a_new_lastupdated_stores_nothing(store, vendor, clock):
    # attributes are never compared: only a new lastupdated makes a new version
    vendor.rows = [{**STOCKS, "sector": "Retesting"}, FUNDAMENTALS]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert store.table("sharadar_tickers", "XTEST", table="stocks")["sector"].iloc[0] == "Testing"


def test_the_index_row_of_a_ticker_and_its_record_never_grows_stale(store, clock):
    info = store.info("sharadar_tickers:XTEST")
    assert (info.kind, info.name) == ("table", "Ticker record of XTEST (Sharadar)")
    assert (info.frequency, info.last_period) == ("", "2026-02-06")
    assert info.label == "[Sharadar: XTEST, 2026-02-06, fetched 2026-06-06]"
    assert pd.isna(store.index().iloc[0]["stale_after_days"])
    clock["now"] = datetime.datetime(2036, 1, 1, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"
