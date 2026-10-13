import datetime
import json
import math
import re

import click.testing
import httpx
import pandas as pd
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import cli as cli_module
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources import sharadar
from data_pipeline.store.sources.sharadar_prices import KEY_COLUMNS, VALUE_COLUMNS, SharadarPrices, read_rows
from data_pipeline.store.sync import held_periods

from .helpers import NOW, client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
RECORDED = json.loads(fixture("sharadar_prices.json"))["data"]


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_prices", **fields)


def price(day, close=20.0, raw=None, **more):
    """One row of a price answer. `raw` is the close as it traded, `close` when not given."""
    return {
        "ticker": "XTEST",
        "date": day,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000,
        "closeadj": close,
        "closeunadj": close if raw is None else raw,
        "lastupdated": "2026-03-04",
        **more,
    }


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarPrices(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarPrices.kind is Kind.TABLE
    assert SharadarPrices.daily_budget is None
    assert SharadarPrices.requests_per_minute == 120
    assert SharadarPrices.held_by == ()


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("aapl", {}, "a Sharadar id is a ticker in upper case"),
        ("BRK B", {}, "a Sharadar id is a ticker in upper case"),
        ("AAPL\n", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "bonds"}, "'table' must be one of stocks, funds"),
        ("XTEST", {"adjusted": True}, "unknown field\\(s\\) for source 'sharadar_prices': adjusted"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarPrices(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


def test_validate_accepts_a_ticker_with_a_dot_and_both_tables():
    source = SharadarPrices(client(), CREDENTIALS)
    source.validate(ticker("BRK.B"))
    source.validate(ticker("XFUND", params={"table": "funds"}))


# -- reading rows ------------------------------------------------------------------------------


def test_a_price_row_keeps_the_six_values_and_drops_the_rest():
    newest, *_others, oldest = read_rows(RECORDED)
    assert set(newest) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS}  # no closeadj, no lastupdated
    assert (newest["period"], newest["date"]) == ("2026-03-04", datetime.date(2026, 3, 4))
    assert "frequency" not in newest
    assert [newest[column] for column in VALUE_COLUMNS] == [20.5, 21.0, 20.1, 20.8, 1500000.0, 20.8]
    assert (oldest["close"], oldest["closeunadj"]) == (19.5, 39.0)  # adjusted for the split, and as it traded


def test_a_price_row_is_told_apart_by_its_day_alone():
    # without a `frequency` column the store builds no set of the days it holds, for a source that reads none
    assert KEY_COLUMNS == ("period",)


def test_a_value_sharadar_leaves_empty_is_missing():
    (row,) = read_rows([price("2026-03-04", volume=None)])
    assert math.isnan(row["volume"])
    assert row["close"] == 20.0


# -- the call ----------------------------------------------------------------------------------


def test_a_call_asks_one_table_for_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(RECORDED)

    fetch(handler, [Request(ticker()), Request(ticker("XFUND", params={"table": "funds"}))])
    stocks, funds = seen
    assert stocks.url.path == "/v1.0/data/stocks"
    assert dict(stocks.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",  # always sent: without it Sharadar answers with a recent window only
        "format": "json",
        "api_key": KEY_VALUE,
    }
    assert (funds.url.path, funds.url.params["ticker"]) == ("/v1.0/data/funds", "XFUND")


def test_the_start_of_an_entry_is_the_first_day_asked_for():
    seen = []

    def handler(request):
        seen.append(request.url.params["date.gte"])
        return answer(RECORDED)

    fetch(handler, [Request(ticker(start=datetime.date(2020, 1, 1)))])
    assert seen == ["2020-01-01"]


def test_an_answer_is_one_table_of_the_ticker():
    (batch,) = fetch(lambda _request: answer(RECORDED), [Request(ticker())])
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_prices:XTEST", "Daily prices of XTEST (Sharadar stocks)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned) == ((), False)
    assert (table.stale_after_days, table.attrs) == (10, {"table": "stocks"})
    assert len(table.rows) == 4


# -- how Sharadar refuses ----------------------------------------------------------------------


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker()), Request(ticker("XOTHER"))], Credentials())
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left_and_stops():
    calls = []

    def handler(request):
        calls.append(request.url.params["ticker"])
        if request.url.params["ticker"] == "XFREE":
            return answer(RECORDED)
        return httpx.Response(403, json={"error": "Exceeds free tier"})

    requests = [Request(ticker("XFREE")), Request(ticker("XPAID")), Request(ticker("XNEXT"))]
    stored, refused = fetch(handler, requests)
    assert stored.tables[0].key == "sharadar_prices:XFREE"  # a wrong key still gets the free tier's tickers
    assert [(failure.entry.source_id, failure.outcome) for failure in refused.failures] == [
        ("XPAID", Outcome.KEY_ERROR),
        ("XNEXT", Outcome.KEY_ERROR),
    ]
    assert "free tier (HTTP 403): check SHARADAR_API_KEY" in refused.failures[0].reason
    assert "the table 'stocks'" in refused.failures[0].reason
    assert calls == ["XFREE", "XPAID"]


def test_a_401_is_a_rejected_key_and_fails_every_ticker_left():
    calls = []

    def handler(request):
        calls.append(request.url.params["ticker"])
        return httpx.Response(401, json={"error": "Unauthorized"})

    batches = fetch(handler, [Request(ticker()), Request(ticker("XOTHER"))])
    refused = [failure for batch in batches for failure in batch.failures]
    assert [(failure.entry.source_id, failure.outcome) for failure in refused] == [
        ("XTEST", Outcome.KEY_ERROR),
        ("XOTHER", Outcome.KEY_ERROR),
    ]
    assert "rejected the key (HTTP 401)" in refused[0].reason
    assert calls == ["XTEST"]


def test_a_403_on_one_table_does_not_fail_the_tickers_of_the_other():
    def handler(request):
        if request.url.path.endswith("/funds"):
            return httpx.Response(403, json={"error": "Exceeds free tier"})
        return answer(RECORDED)

    requests = [Request(ticker("XFUND", params={"table": "funds"})), Request(ticker()), Request(ticker("XOTHER"))]
    batches = fetch(handler, requests)
    stored = [table.key for batch in batches for table in batch.tables]
    refused = [failure for batch in batches for failure in batch.failures]
    assert stored == ["sharadar_prices:XTEST", "sharadar_prices:XOTHER"]
    assert [(failure.entry.source_id, failure.outcome) for failure in refused] == [("XFUND", Outcome.KEY_ERROR)]
    assert "the table 'funds'" in refused[0].reason


def test_a_persistent_429_stops_the_source_as_an_exhausted_quota():
    with pytest.raises(QuotaExhaustedError, match="HTTP 429"):
        fetch(lambda _request: httpx.Response(429), [Request(ticker())])


def test_a_ticker_without_prices_is_not_found_and_an_etf_is_pointed_to_its_table():
    stocks, funds = fetch(
        lambda _request: answer([]),
        [Request(ticker("XFUND")), Request(ticker("XGONE", params={"table": "funds"}))],
    )
    assert stocks.failures[0].outcome is Outcome.NOT_FOUND
    assert stocks.failures[0].reason == (
        "Sharadar has no prices for 'XFUND' in its table 'stocks'; an ETF needs `table: funds`"
    )
    assert funds.failures[0].reason == "Sharadar has no prices for 'XGONE' in its table 'funds'"


def test_a_ticker_without_prices_since_a_start_names_the_first_day_asked_for():
    (batch,) = fetch(lambda _request: answer([]), [Request(ticker("XGONE", start=datetime.date(2020, 1, 1)))])
    assert batch.failures[0].outcome is Outcome.NOT_FOUND
    assert batch.failures[0].reason == (
        "Sharadar has no prices for 'XGONE' in its table 'stocks' from 2020-01-01 on; an ETF needs `table: funds`"
    )


def test_a_failed_call_fails_its_ticker_and_the_next_one_goes_on():
    def handler(request):
        if request.url.params["ticker"] == "XDOWN":
            return httpx.Response(500)
        return answer(RECORDED)

    failed, found = fetch(handler, [Request(ticker("XDOWN")), Request(ticker())])
    assert failed.failures[0].outcome is Outcome.NETWORK_ERROR
    assert found.tables[0].key == "sharadar_prices:XTEST"


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (httpx.Response(400, text="bad request"), "HTTP 400: bad request"),
        (httpx.Response(200, text="<html>down</html>"), "not JSON"),
        (httpx.Response(200, json={"count": 0}), "no list of rows in `data`"),
        (httpx.Response(200, json={"data": "none"}), "no list of rows in `data`"),
        (httpx.Response(200, json=[1, 2]), "no list of rows in `data`"),
        (httpx.Response(200, json={"error": "Table not found"}), "Table not found"),
        (httpx.Response(200, json={"data": [1]}), "unexpected answer \\(TypeError"),
        (httpx.Response(200, json={"data": [{"date": "2026-03-04", "close": 1.0}]}), "unexpected answer \\(KeyError"),
        (httpx.Response(200, json={"data": [{"date": "4 March"}]}), "unexpected answer \\(ValueError"),
    ],
)
def test_an_unexpected_answer_is_a_source_error(response, reason):
    (batch,) = fetch(lambda _request: response, [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, batch.failures[0].reason)


def test_a_source_error_is_the_error_of_one_ticker_alone():
    def handler(request):
        if request.url.params["ticker"] == "XBAD":
            return httpx.Response(400, text="bad request")
        return answer(RECORDED)

    failed, found = fetch(handler, [Request(ticker("XBAD")), Request(ticker())])
    assert failed.failures[0].outcome is Outcome.SOURCE_ERROR
    assert found.tables[0].key == "sharadar_prices:XTEST"


def test_a_day_that_comes_twice_is_refused_rather_than_stored_once():
    twice = [price("2026-03-04"), price("2026-03-04", close=21.0)]
    (batch,) = fetch(lambda _request: answer(twice), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason == "the answer holds the day 2026-03-04 twice"


def test_an_answer_at_the_row_cap_is_refused_rather_than_stored_cut_short(monkeypatch):
    monkeypatch.setattr(sharadar, "MAX_ROWS", 4)
    (batch,) = fetch(lambda _request: answer(RECORDED), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert "cap of 4 rows and may be cut short: set `start` in the entry" in batch.failures[0].reason


def test_the_key_never_shows_in_a_reason():
    def handler(request):
        return httpx.Response(400, text=f"no such thing: {request.url}")

    (batch,) = fetch(handler, [Request(ticker())])
    assert KEY_VALUE not in batch.failures[0].reason
    assert "api_key=***" in batch.failures[0].reason


# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)


class Vendor:
    """A fake Sharadar whose answers the test can change between syncs: (table, ticker) -> rows."""

    def __init__(self):
        self.rows = {("stocks", "XTEST"): RECORDED}
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        table = request.url.path.rsplit("/", 1)[-1]
        return answer(self.rows.get((table, request.url.params["ticker"]), []))


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_prices\n  ids: [XTEST]\n", encoding="utf-8")
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


def split_three_for_one():
    """The recorded answer after a later 3-for-1 split, with one new day: every earlier day comes
    restated, and its close as it traded does not change."""
    restated = [
        {**row, **{column: row[column] / 3 for column in ("open", "high", "low", "close")}, "volume": row["volume"] * 3}
        for row in RECORDED
    ]
    return [price("2026-06-30", close=7.0), *restated]


def test_a_first_sync_stores_the_whole_history_and_a_second_adds_nothing(store, vendor):
    assert store.report.exit_code == 0
    assert (store.report.sources[0].new, store.report.sources[0].revised) == (4, 0)
    table = store.table("sharadar_prices", "XTEST")
    assert list(table["period"]) == ["2026-02-27", "2026-03-02", "2026-03-03", "2026-03-04"]
    assert list(table["closeunadj"]) == [39.0, 40.0, 20.4, 20.8]
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert vendor.calls == 2  # one call per ticker per run, the whole history each time


def test_sync_builds_no_set_of_held_days_for_a_source_that_reads_none(store, tmp_path):
    # without a `frequency` column the store has nothing to tell this source about what it holds
    stored = pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_prices" / "XTEST.parquet")
    assert "frequency" not in stored.columns
    assert held_periods(stored, SharadarPrices.held_by) == frozenset()


def test_a_split_stores_every_earlier_day_again_and_as_of_reads_what_was_held_before(store, vendor, clock, tmp_path):
    vendor.rows[("stocks", "XTEST")] = split_three_for_one()
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 4)
    after = store.table("sharadar_prices", "XTEST", period="2026-03-04")
    assert (after["close"].iloc[0], after["closeunadj"].iloc[0]) == (pytest.approx(20.8 / 3), 20.8)
    before = store.table("sharadar_prices", "XTEST", period="2026-03-04", as_of=NOW)
    assert (before["close"].iloc[0], before["closeunadj"].iloc[0]) == (20.8, 20.8)
    stored = pd.read_parquet(tmp_path / "store" / "tables" / "sharadar_prices" / "XTEST.parquet")
    assert len(stored) == 9  # four days twice, and the new one: nothing is deleted
    assert set(stored["fetched_at"]) == {pd.Timestamp(NOW), pd.Timestamp(LATER)}
    assert stored["published_at"].isna().all()  # dated by the fetch: Sharadar does not say when it restated


def test_as_of_before_the_first_sync_reads_no_prices(store):
    assert store.table("sharadar_prices", "XTEST", as_of=NOW - datetime.timedelta(days=1)).empty


def test_the_index_row_of_a_ticker(store):
    info = store.info("sharadar_prices:XTEST")
    assert (info.kind, info.name) == ("table", "Daily prices of XTEST (Sharadar stocks)")
    assert (info.frequency, info.last_period) == ("", "2026-03-04")
    assert info.label == "[Sharadar: XTEST, 2026-03-04, fetched 2026-06-06]"
    row = store.index().iloc[0]
    assert json.loads(row["attrs"]) == {"table": "stocks"}
    assert row["stale_after_days"] == 10


def test_status_uses_the_ten_days_of_a_daily_series(store, clock):
    clock["now"] = datetime.datetime(2026, 3, 14, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"  # ten days after the last one
    clock["now"] = datetime.datetime(2026, 3, 15, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2026-03-04 (11 days ago)"


def test_a_delisted_ticker_is_not_stale_with_its_own_threshold(tmp_path, vendor, clock):
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    store = Store(
        tmp_path / "store",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    store.add("sharadar_prices", ["XTEST"], stale_after_days=100_000)
    store.sync()
    assert store.status().iloc[0]["state"] == "ok"  # 94 days after its last price


def test_show_prints_the_citation_and_the_newest_rows(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    result = click.testing.CliRunner().invoke(cli, ["show", "--root", str(tmp_path / "store"), "sharadar_prices:XTEST"])
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[Sharadar: XTEST, 2026-03-04, fetched 2026-06-06]"
    assert lines[1] == "Daily prices of XTEST (Sharadar stocks) | 4 rows"
    assert lines[2].split("  ")[:7] == ["period", "open", "high", "low", "close", "volume", "closeunadj"]
