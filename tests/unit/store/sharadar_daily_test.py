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
from data_pipeline.store.sources.sharadar_daily import KEY_COLUMNS, VALUE_COLUMNS, SharadarDaily, read_rows
from data_pipeline.store.sync import held_periods

from .helpers import NOW, client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
INVENTED = json.loads(fixture("sharadar_daily.json"))["data"]


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_daily", **fields)


def without(row, field):
    return {name: found for name, found in row.items() if name != field}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarDaily(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarDaily.kind is Kind.TABLE
    assert SharadarDaily.daily_budget is None
    assert SharadarDaily.requests_per_minute == 120
    assert SharadarDaily.held_by == ()
    assert sources.TITLES["sharadar_daily"] == "Sharadar"


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "daily"}, "unknown field\\(s\\) for source 'sharadar_daily': table"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarDaily(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_a_row_keeps_the_seven_metrics_and_drops_the_rest():
    newest, _middle, oldest = read_rows(INVENTED, "XTEST")
    assert KEY_COLUMNS == ("ticker", "period")
    assert set(newest) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS}  # no lastupdated, no frequency
    assert (newest["ticker"], newest["period"], newest["date"]) == ("XTEST", "2026-03-04", datetime.date(2026, 3, 4))
    assert [newest[column] for column in VALUE_COLUMNS] == [1250.5, 20.1, 15.2, 1200.0, 4.1, 25.3, 6.2]
    assert math.isnan(oldest["evebit"])  # null is missing, never zero


def test_the_ticker_of_a_row_is_the_one_that_was_asked_for_whatever_the_answer_says():
    rows = read_rows([{**row, "ticker": "OTHER"} for row in INVENTED], "XTEST")
    assert {row["ticker"] for row in rows} == {"XTEST"}


def test_a_day_that_comes_twice_is_refused_rather_than_stored_once():
    first = INVENTED[0]
    (batch,) = fetch(lambda _request: answer([first, {**first, "pe": 30.0}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds the day 2026-03-04 twice"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (without(INVENTED[0], "date"), "unexpected answer \\(KeyError: 'date'"),
        (without(INVENTED[0], "pe"), "unexpected answer \\(KeyError: 'pe'"),
        ({**INVENTED[0], "date": "4 March"}, "unexpected answer \\(ValueError"),
    ],
)
def test_a_row_without_its_day_or_one_of_its_metrics_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_the_daily_metrics_of_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(INVENTED)

    fetch(handler, [Request(ticker()), Request(ticker("XOTHER", start=datetime.date(2020, 1, 1)))])
    first, second = seen
    assert first.url.path == "/v1.0/data/daily"
    assert dict(first.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (second.url.params["ticker"], second.url.params["date.gte"]) == ("XOTHER", "2020-01-01")


def test_an_answer_is_one_table_of_the_ticker():
    (batch,) = fetch(lambda _request: answer(INVENTED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_daily:XTEST", "Daily metrics of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == ((), False)
    assert (table.stale_after_days, table.attrs) == (10, {})
    assert len(table.rows) == 3


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
    assert "the table 'daily'" in batch.failures[0].reason


def test_a_ticker_without_daily_metrics_is_not_found():
    requests = [Request(ticker("XGONE")), Request(ticker("XLATE", start=datetime.date(2020, 1, 1)))]
    gone, late = fetch(lambda _request: answer([]), requests)
    assert gone.failures[0].outcome is Outcome.NOT_FOUND
    assert gone.failures[0].reason == "Sharadar has no daily metrics for 'XGONE'"
    assert late.failures[0].reason == "Sharadar has no daily metrics for 'XLATE' from 2020-01-01 on"


# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


class Vendor:
    """A fake Sharadar whose answer the test can change between syncs."""

    def __init__(self):
        self.rows = INVENTED

    def __call__(self, request):
        assert request.url.path == "/v1.0/data/daily"
        return answer(self.rows)


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_daily\n  ids: [XTEST]\n", encoding="utf-8")
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
    return pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_daily" / "XTEST.parquet")


def test_a_first_sync_stores_every_day_and_a_second_adds_nothing(store, clock, tmp_path):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (3, 0)
    table = store.table("sharadar_daily", "XTEST")
    assert list(table.columns) == [*KEY_COLUMNS, "date", *VALUE_COLUMNS]
    assert list(table["period"]) == ["2026-03-02", "2026-03-03", "2026-03-04"]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert "frequency" not in stored(tmp_path).columns
    assert held_periods(stored(tmp_path), SharadarDaily.held_by) == frozenset()


def test_a_restated_day_is_stored_again_and_as_of_reads_what_was_held_before(store, vendor, clock, tmp_path):
    newest, *earlier = INVENTED
    vendor.rows = [{**newest, "date": "2026-03-05", "pe": 26.0}, {**newest, "pe": 25.0}, *earlier]
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 1)
    day = store.table("sharadar_daily", "XTEST", period="2026-03-04")
    assert day["pe"].iloc[0] == 25.0
    before = store.table("sharadar_daily", "XTEST", period="2026-03-04", as_of=NOW)
    assert before["pe"].iloc[0] == 25.3
    assert len(stored(tmp_path)) == 5  # nothing is deleted
    assert stored(tmp_path)["published_at"].isna().all()  # dated by the fetch: Sharadar does not say when


def test_the_index_row_of_a_ticker(store):
    info = store.info("sharadar_daily:XTEST")
    assert (info.kind, info.name) == ("table", "Daily metrics of XTEST (Sharadar)")
    assert (info.frequency, info.last_period) == ("", "2026-03-04")
    assert info.label == "[Sharadar: XTEST, 2026-03-04, fetched 2026-06-06]"
    assert store.index().iloc[0]["stale_after_days"] == 10


def test_status_uses_the_ten_days_of_a_daily_series(store, clock):
    clock["now"] = datetime.datetime(2026, 3, 14, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"
    clock["now"] = datetime.datetime(2026, 3, 15, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2026-03-04 (11 days ago)"
