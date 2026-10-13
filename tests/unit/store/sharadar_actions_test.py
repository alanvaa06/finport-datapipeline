import datetime
import json
import math

import click.testing
import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import cli as cli_module
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import CatalogError, UnknownSeriesError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sharadar_actions import (
    ATTRIBUTE_COLUMNS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SharadarActions,
    read_rows,
)

from .helpers import NOW, client, entry, fixture

# Every ticker, name and number here is invented: Sharadar's license forbids sharing its data.
KEY_VALUE = "sharadar-key-of-the-tests"
CREDENTIALS = Credentials({keys.SHARADAR: KEY_VALUE})
RECORDED = json.loads(fixture("sharadar_actions.json"))["data"]


def ticker(identifier="XTEST", **fields):
    return entry(identifier, "sharadar_actions", **fields)


def action(day, word="dividend", value=0.25, contraticker="N/A", contraname="N/A"):
    return {
        "date": day,
        "action": word,
        "ticker": "XTEST",
        "name": "TEST CORP",
        "value": value,
        "contraticker": contraticker,
        "contraname": contraname,
    }


def without(row, field):
    return {name: found for name, found in row.items() if name != field}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SharadarActions(client(handler, secrets=(KEY_VALUE,)), credentials)
    return list(source.fetch(requests))


def keyed(rows):
    """{(day, action, contraticker, item): value}, the way the store tells the rows apart."""
    return {tuple(row[column] for column in KEY_COLUMNS): row["value"] for row in rows}


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarActions.kind is Kind.TABLE
    assert SharadarActions.daily_budget is None
    assert SharadarActions.requests_per_minute == 120
    assert SharadarActions.held_by == ()


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("xtest", {}, "a Sharadar id is a ticker in upper case"),
        ("XTEST", {"table": "funds"}, "unknown field\\(s\\) for source 'sharadar_actions': table"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = SharadarActions(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(ticker(identifier, params=params))


# -- reading rows ------------------------------------------------------------------------------


def test_an_action_row_keeps_what_sharadar_says_and_n_a_is_empty():
    rows = {row["action"]: row for row in read_rows(RECORDED) if row["action"] != "dividend"}
    split, spinoff = rows["split"], rows["spinoff"]
    assert set(split) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS}
    assert (split["day"], split["contraticker"], split["item"], split["value"]) == ("2026-03-03", "", "1", 2.0)
    assert (split["name"], split["contraname"]) == ("TEST CORP", "")
    assert (spinoff["contraticker"], spinoff["contraname"]) == ("XSPUN", "SPUN CORP")
    assert math.isnan(spinoff["value"])  # Sharadar's N/A is a missing value, never zero


def test_two_dividends_on_one_day_are_both_kept_and_numbered_by_value():
    expected = {
        ("2026-02-10", "dividend", "", "1"): 0.1,
        ("2026-02-10", "dividend", "", "2"): 0.5,
    }
    dividends = [row for row in RECORDED if row["action"] == "dividend"]
    assert keyed(read_rows(dividends)) == expected
    assert keyed(read_rows(dividends[::-1])) == expected  # whatever the order of the answer


def test_a_missing_value_is_numbered_last_among_its_likes():
    rows = read_rows([action("2026-02-10", value="N/A"), action("2026-02-10", value=0.3)])
    assert [(row["item"], row["value"] == 0.3) for row in rows] == [("1", True), ("2", False)]


def test_the_other_company_tells_two_actions_of_a_day_apart():
    rows = read_rows(
        [
            action("2025-06-30", "spinoff", 1.0, contraticker="XONE"),
            action("2025-06-30", "spinoff", 2.0, contraticker="XTWO"),
        ]
    )
    assert [(row["contraticker"], row["item"]) for row in rows] == [("XONE", "1"), ("XTWO", "1")]


@pytest.mark.parametrize("field", ["contraname", "name"])
def test_two_actions_alike_but_for_a_name_are_numbered_the_same_whatever_the_order(field):
    first = {**action("2026-02-10", value=0.5), field: "ALPHA CORP"}
    second = {**action("2026-02-10", value=0.5), field: "BETA CORP"}
    for answered in ([first, second], [second, first]):
        assert {row[field]: row["item"] for row in read_rows(answered)} == {"ALPHA CORP": "1", "BETA CORP": "2"}


def test_two_rows_alike_in_every_field_are_both_kept():
    rows = read_rows([action("2026-02-10", value=0.25), action("2026-02-10", value=0.25)])
    assert [(row["item"], row["value"]) for row in rows] == [("1", 0.25), ("2", 0.25)]


def test_a_value_or_an_other_company_sent_as_null_is_stored_as_missing():
    (row,) = read_rows([action("2026-02-10", value=None, contraticker=None, contraname=None)])
    assert math.isnan(row["value"])
    assert (row["contraticker"], row["contraname"]) == ("", "")


# -- the call and how Sharadar refuses ---------------------------------------------------------


def test_a_call_asks_the_actions_table_for_one_ticker_from_the_first_day():
    seen = []

    def handler(request):
        seen.append(request)
        return answer(RECORDED)

    (batch,) = fetch(handler, [Request(ticker())])
    (request,) = seen
    assert request.url.path == "/v1.0/data/actions"
    assert dict(request.url.params) == {
        "ticker": "XTEST",
        "date.gte": "1900-01-01",
        "format": "json",
        "api_key": KEY_VALUE,
    }
    (table,) = batch.tables
    assert (table.key, table.name) == ("sharadar_actions:XTEST", "Corporate actions of XTEST (Sharadar)")
    assert (table.key_columns, table.value_columns) == (KEY_COLUMNS, VALUE_COLUMNS)
    assert (table.attribute_columns, table.versioned, table.stale_after_days) == (ATTRIBUTE_COLUMNS, False, None)
    assert len(table.rows) == 4


def test_the_start_of_the_entry_is_the_first_day_asked():
    seen = []

    def handler(request):
        seen.append(request)
        return answer([])

    fetch(handler, [Request(ticker(start=datetime.date(2020, 1, 1)))])
    (request,) = seen
    assert request.url.params["date.gte"] == "2020-01-01"


def test_a_ticker_without_actions_is_an_empty_table_not_a_failure():
    (batch,) = fetch(lambda _request: answer([]), [Request(ticker())])
    assert batch.failures == ()
    assert batch.tables[0].rows == ()


def test_without_a_key_nothing_is_requested():
    def handler(_request):
        msg = "a request was sent without a key"
        raise AssertionError(msg)

    (batch,) = fetch(handler, [Request(ticker())], Credentials())
    assert batch.failures[0].outcome is Outcome.KEY_ERROR
    assert "SHARADAR_API_KEY is missing" in batch.failures[0].reason


def test_a_403_fails_every_ticker_left():
    (batch,) = fetch(lambda _request: httpx.Response(403), [Request(ticker()), Request(ticker("XNEXT"))])
    assert [failure.outcome for failure in batch.failures] == [Outcome.KEY_ERROR] * 2
    assert "the table 'actions'" in batch.failures[0].reason


def test_an_action_without_a_day_is_a_source_error():
    (batch,) = fetch(lambda _request: answer([{"action": "dividend", "value": 1.0}]), [Request(ticker())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR
    assert batch.failures[0].reason.startswith("unexpected answer (KeyError")


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (without(action("2026-02-10"), "value"), "unexpected answer (KeyError: 'value')"),
        (without(action("2026-02-10"), "contraticker"), "unexpected answer (KeyError: 'contraticker')"),
        (action("2026-02-10", word=None), "unexpected answer (ValueError: an action without a name: None)"),
        (action("2026-02-10", word=""), "unexpected answer (ValueError: an action without a name: '')"),
    ],
    ids=["no-value", "no-contraticker", "action-is-null", "action-is-empty"],
)
def test_a_row_that_lacks_what_its_key_or_its_value_is_made_of_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert [(failure.outcome, failure.reason) for failure in batch.failures] == [(Outcome.SOURCE_ERROR, reason)]
    assert batch.tables == ()


# -- through the store -------------------------------------------------------------------------


class Vendor:
    """A fake Sharadar whose actions the test can change between syncs: ticker -> rows."""

    def __init__(self):
        self.rows = {"XTEST": RECORDED}

    def __call__(self, request):
        return answer(self.rows.get(request.url.params["ticker"], []))


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def store(tmp_path, vendor):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_actions\n  ids: [XTEST, XQUIET]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: NOW,
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def test_a_sync_stores_the_actions_and_a_ticker_without_any_is_not_a_failure(store):
    report = store.report.sources[0]
    assert store.report.exit_code == 0
    assert (report.ok, report.failed, report.new) == (2, (), 4)
    assert store.info("sharadar_actions:XTEST").last_period == "2026-03-03"
    assert store.info("sharadar_actions:XQUIET").status == "ok"
    # an empty table writes no file, as in every table source: reading it says so
    with pytest.raises(UnknownSeriesError, match="no stored table of source 'sharadar_actions' has id 'XQUIET'"):
        store.table("sharadar_actions", "XQUIET")


def test_a_table_that_is_not_versioned_keeps_its_attribute_columns(store):
    # the first table in the store that has attributes and no versions dated by its source
    dividends = store.table("sharadar_actions", "XTEST", action="dividend")
    assert list(dividends["value"]) == [0.1, 0.5]
    assert list(dividends["item"]) == ["1", "2"]
    assert set(dividends["name"]) == {"TEST CORP"}
    spinoff = store.table("sharadar_actions", "XTEST", contraticker="XSPUN")
    assert list(spinoff["contraname"]) == ["SPUN CORP"]


def test_a_second_sync_adds_nothing_and_a_new_action_adds_one_row(store, vendor):
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    vendor.rows["XTEST"] = [action("2026-05-12", value=0.5), *RECORDED]
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 0)
    assert store.info("sharadar_actions:XTEST").last_period == "2026-05-12"


def test_a_dividend_added_on_a_day_that_has_one_renumbers_the_dividends_after_it(store, vendor):
    # Items go by ascending value. The new 0.05 is the smallest of 2026-02-10, so it takes item 1
    # and the stored 0.1 and 0.5 move to items 2 and 3. Against the store, items 1 and 2 are keys it
    # already has with another value (2 revised) and item 3 is a key it has not (1 new). No stored
    # row is touched, and a read gives the newest row of each key: the three dividends, once each.
    vendor.rows["XTEST"] = [action("2026-02-10", value=0.05), *RECORDED]
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 2)
    dividends = store.table("sharadar_actions", "XTEST", action="dividend")
    assert list(dividends["value"]) == [0.05, 0.1, 0.5]
    assert list(dividends["item"]) == ["1", "2", "3"]


def test_show_prints_the_citation_and_the_newest_rows(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    arguments = ["show", "--root", str(tmp_path / "store"), "sharadar_actions:XTEST"]
    result = click.testing.CliRunner().invoke(cli, arguments)
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[Sharadar: XTEST, 2026-03-03, fetched 2026-06-06]"
    assert lines[1].startswith("Corporate actions of XTEST (Sharadar) | 4 rows")


def test_status_never_calls_the_actions_stale(store):
    status = store.status()
    states = dict(zip(status["key"], status["state"], strict=True))
    assert states["sharadar_actions:XTEST"] == "ok"  # no threshold: actions come when they come
