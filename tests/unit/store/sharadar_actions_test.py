import datetime
import json
import math

import click.testing
import httpx
import pandas as pd
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
    NEVER_STALE,
    VALUE_COLUMNS,
    SharadarActions,
    read_rows,
    withdrawals,
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
    """{(frequency, period, ticker, action, contraticker, item): value}, the way the store tells the rows apart."""
    return {tuple(row[column] for column in KEY_COLUMNS): row["value"] for row in rows}


def of(identifier, rows):
    """The rows of an answer about `identifier`."""
    return [{**row, "ticker": identifier} for row in rows]


def test_it_is_a_table_source_without_a_daily_budget():
    assert SharadarActions.kind is Kind.TABLE
    assert SharadarActions.daily_budget is None
    assert SharadarActions.requests_per_minute == 120
    assert SharadarActions.held_by == ("ticker", "action", "contraticker", "item")


def test_the_store_holds_an_action_by_the_columns_the_source_reads_back():
    # Request.held is (frequency, period, *held_by) in this order: withdrawals() reads it that way
    assert ("frequency", "period", *SharadarActions.held_by) == KEY_COLUMNS


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
    rows = {row["action"]: row for row in read_rows(RECORDED, "XTEST") if row["action"] != "dividend"}
    split, spinoff = rows["split"], rows["spinoff"]
    assert set(split) == {*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS}
    assert [split[column] for column in KEY_COLUMNS] == ["D", "2026-03-03", "XTEST", "split", "", "1"]
    assert (split["value"], split["withdrawn"], split["date"]) == (2.0, 0.0, datetime.date(2026, 3, 3))
    assert (split["name"], split["contraname"]) == ("TEST CORP", "")
    assert (spinoff["contraticker"], spinoff["contraname"]) == ("XSPUN", "SPUN CORP")
    assert math.isnan(spinoff["value"])  # Sharadar's N/A is a missing value, never zero
    assert spinoff["withdrawn"] == 0.0  # a row Sharadar sends is never a withdrawn one


def test_two_dividends_on_one_day_are_both_kept_and_numbered_by_value():
    expected = {
        ("D", "2026-02-10", "XTEST", "dividend", "", "1"): 0.1,
        ("D", "2026-02-10", "XTEST", "dividend", "", "2"): 0.5,
    }
    dividends = [row for row in RECORDED if row["action"] == "dividend"]
    assert keyed(read_rows(dividends, "XTEST")) == expected
    assert keyed(read_rows(dividends[::-1], "XTEST")) == expected  # whatever the order of the answer


def test_the_ticker_of_a_row_is_the_one_that_was_asked_for_whatever_the_answer_says():
    rows = read_rows(of("OTHER", RECORDED), "XTEST")
    assert {row["ticker"] for row in rows} == {"XTEST"}


def test_a_missing_value_is_numbered_last_among_its_likes():
    rows = read_rows([action("2026-02-10", value="N/A"), action("2026-02-10", value=0.3)], "XTEST")
    assert [(row["item"], row["value"] == 0.3) for row in rows] == [("1", True), ("2", False)]


def test_the_other_company_tells_two_actions_of_a_day_apart():
    rows = read_rows(
        [
            action("2025-06-30", "spinoff", 1.0, contraticker="XONE"),
            action("2025-06-30", "spinoff", 2.0, contraticker="XTWO"),
        ],
        "XTEST",
    )
    assert [(row["contraticker"], row["item"]) for row in rows] == [("XONE", "1"), ("XTWO", "1")]


@pytest.mark.parametrize("field", ["contraname", "name"])
def test_two_actions_alike_but_for_a_name_are_numbered_the_same_whatever_the_order(field):
    first = {**action("2026-02-10", value=0.5), field: "ALPHA CORP"}
    second = {**action("2026-02-10", value=0.5), field: "BETA CORP"}
    for answered in ([first, second], [second, first]):
        numbered = {row[field]: row["item"] for row in read_rows(answered, "XTEST")}
        assert numbered == {"ALPHA CORP": "1", "BETA CORP": "2"}


def test_two_rows_alike_in_every_field_are_both_kept():
    rows = read_rows([action("2026-02-10", value=0.25), action("2026-02-10", value=0.25)], "XTEST")
    assert [(row["item"], row["value"]) for row in rows] == [("1", 0.25), ("2", 0.25)]


def test_a_value_or_an_other_company_sent_as_null_is_stored_as_missing():
    (row,) = read_rows([action("2026-02-10", value=None, contraticker=None, contraname=None)], "XTEST")
    assert math.isnan(row["value"])
    assert (row["contraticker"], row["contraname"]) == ("", "")


# -- the actions Sharadar no longer sends ------------------------------------------------------

# what the store holds after an answer of the recorded actions: Request.held, in the order of KEY_COLUMNS
HELD = frozenset(
    {
        ("D", "2025-06-30", "XTEST", "spinoff", "XSPUN", "1"),
        ("D", "2026-02-10", "XTEST", "dividend", "", "1"),
        ("D", "2026-02-10", "XTEST", "dividend", "", "2"),
        ("D", "2026-03-03", "XTEST", "split", "", "1"),
    }
)


def test_nothing_is_withdrawn_when_nothing_is_held_or_everything_held_is_still_sent():
    rows = read_rows(RECORDED, "XTEST")
    assert withdrawals(frozenset(), rows) == ()
    assert withdrawals(HELD, rows) == ()


def test_a_held_action_the_answer_no_longer_has_comes_back_marked_as_withdrawn():
    # the answer starts on 2026-02-10, the day of its only dividend left: the second one of that day,
    # on the answer's first day itself, is gone. The spin-off, older than the answer, is not touched.
    rows = read_rows(RECORDED[:2], "XTEST")
    (marked,) = withdrawals(HELD, rows)
    assert math.isnan(marked.pop("value"))
    assert marked == {
        "frequency": "D",
        "period": "2026-02-10",
        "ticker": "XTEST",
        "date": datetime.date(2026, 2, 10),
        "action": "dividend",
        "contraticker": "",
        "item": "2",
        "withdrawn": 1.0,
        "name": "",
        "contraname": "",
    }


def test_the_actions_withdrawn_after_the_earliest_day_of_the_answer_are_marked_in_order():
    rows = read_rows(RECORDED[:1], "XTEST")  # only the split of 2026-03-03
    held = {
        ("D", "2026-03-03", "XTEST", "split", "", "1"),
        ("D", "2026-05-12", "XTEST", "dividend", "", "2"),
        ("D", "2026-05-12", "XTEST", "dividend", "", "1"),
        ("D", "2026-03-03", "XTEST", "dividend", "", "1"),
    }
    marked = withdrawals(held, rows)
    assert [(row["period"], row["action"], row["item"]) for row in marked] == [
        ("2026-03-03", "dividend", "1"),
        ("2026-05-12", "dividend", "1"),
        ("2026-05-12", "dividend", "2"),
    ]
    assert {row["withdrawn"] for row in marked} == {1.0}


def test_an_answer_speaks_only_for_the_days_it_covers():
    rows = read_rows(RECORDED[:1], "XTEST")  # a short, recent answer: only the split of 2026-03-03
    assert withdrawals(HELD, rows) == ()  # the dividends and the spin-off are older than its first day


def test_an_answer_without_actions_marks_nothing_whatever_is_held():
    assert withdrawals(HELD, ()) == ()


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
    assert (table.attribute_columns, table.versioned) == (ATTRIBUTE_COLUMNS, False)
    assert table.stale_after_days == NEVER_STALE
    assert len(table.rows) == 4  # nothing was held: nothing to withdraw


def test_what_the_store_holds_decides_which_actions_are_marked_as_withdrawn():
    held = frozenset({("D", "2026-02-10", "XTEST", "dividend", "", "3")})
    (batch,) = fetch(lambda _request: answer(RECORDED), [Request(ticker(), held=held)])
    *sent, marked = batch.tables[0].rows
    assert len(sent) == 4
    assert [marked[column] for column in KEY_COLUMNS] == ["D", "2026-02-10", "XTEST", "dividend", "", "3"]
    assert marked["withdrawn"] == 1.0
    assert {row["withdrawn"] for row in sent} == {0.0}


def test_the_ticker_stored_is_the_one_asked_for_whatever_the_answer_says():
    (batch,) = fetch(lambda _request: answer(of("OTHER", RECORDED)), [Request(ticker())])
    assert {row["ticker"] for row in batch.tables[0].rows} == {"XTEST"}


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
        (action("2026-02-10", word=None), "unexpected answer (ValueError: a row whose `action` is not a word: None)"),
        (action("2026-02-10", word=""), "unexpected answer (ValueError: a row whose `action` is not a word: '')"),
        (action("2026-02-10", word="N/A"), "unexpected answer (ValueError: a row whose `action` is not a word: 'N/A')"),
    ],
    ids=["no-value", "no-contraticker", "action-is-null", "action-is-empty", "action-is-n-a"],
)
def test_a_row_that_lacks_what_its_key_or_its_value_is_made_of_fails_its_ticker(row, reason):
    (batch,) = fetch(lambda _request: answer([row]), [Request(ticker())])
    assert [(failure.outcome, failure.reason) for failure in batch.failures] == [(Outcome.SOURCE_ERROR, reason)]
    assert batch.tables == ()


# -- through the store -------------------------------------------------------------------------

LATER = NOW + datetime.timedelta(days=30)
LATEST = NOW + datetime.timedelta(days=60)


class Vendor:
    """A fake Sharadar whose actions the test can change between syncs: ticker -> rows.
    It keeps the rows from the first day asked (`date.gte`) on."""

    def __init__(self):
        self.rows = {"XTEST": RECORDED}
        self.asked = []  # the first day of each call

    def __call__(self, request):
        rows = self.rows.get(request.url.params["ticker"], [])
        self.asked.append(request.url.params["date.gte"])
        return answer([row for row in rows if row["date"] >= request.url.params["date.gte"]])


@pytest.fixture
def vendor():
    return Vendor()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, vendor, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sharadar_actions\n  ids: [XTEST, XQUIET]\n", encoding="utf-8")
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


def open_store(tmp_path, vendor, clock, root="store"):
    """A Store with no catalog, on `root` under tmp_path: the test adds its entries."""
    (tmp_path / ".env").write_text(f"SHARADAR_API_KEY={KEY_VALUE}\n", encoding="utf-8")
    return Store(
        tmp_path / root,
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(vendor),
        sleep=lambda _seconds: None,
    )


def sync_at(store, clock, moment):
    """Sync once at `moment`: (new, revised) of the run."""
    clock["now"] = moment
    report = store.sync().sources[0]
    return report.new, report.revised


def current(store, **filters):
    """The actions of XTEST that Sharadar has not withdrawn, as the store reads them now."""
    return store.table("sharadar_actions", "XTEST", withdrawn=0, **filters)


def original_four():
    """The recorded actions as (period, action, item), whatever the order they are read in."""
    return [
        ("2025-06-30", "spinoff", "1"),
        ("2026-02-10", "dividend", "1"),
        ("2026-02-10", "dividend", "2"),
        ("2026-03-03", "split", "1"),
    ]


def keys_of(table):
    return sorted(zip(table["period"], table["action"], table["item"], strict=True))


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


def test_a_second_sync_adds_nothing_and_a_new_action_adds_one_row(store, vendor, clock):
    assert sync_at(store, clock, LATER) == (0, 0)
    vendor.rows["XTEST"] = [action("2026-05-12", value=0.5), *RECORDED]
    assert sync_at(store, clock, LATEST) == (1, 0)
    assert store.info("sharadar_actions:XTEST").last_period == "2026-05-12"


def test_a_dividend_added_on_a_day_that_has_one_renumbers_the_dividends_after_it(store, vendor, clock):
    # Items go by ascending value. The new 0.05 is the smallest of 2026-02-10, so it takes item 1
    # and the stored 0.1 and 0.5 move to items 2 and 3. Against the store, items 1 and 2 are keys it
    # already has with another value (2 revised) and item 3 is a key it has not (1 new). No stored
    # row is touched, and a read gives the newest row of each key: the three dividends, once each.
    vendor.rows["XTEST"] = [action("2026-02-10", value=0.05), *RECORDED]
    assert sync_at(store, clock, LATER) == (1, 2)
    dividends = current(store, action="dividend")
    assert list(dividends["value"]) == [0.05, 0.1, 0.5]
    assert list(dividends["item"]) == ["1", "2", "3"]


def test_a_dividend_whose_day_is_corrected_is_withdrawn_from_the_old_day(store, vendor, clock):
    # The 0.5 moves from 2026-02-10 to 2026-02-11. The answer has the 0.1 as the only dividend of the
    # 10th (item 1, as stored) and the 0.5 as the first of the 11th: a key the store has not (1 new).
    # The stored 0.5 sits at item 2 of the 10th, a day the answer covers, and it is not in the answer:
    # it is stored again, marked (1 revised). Everything else is unchanged.
    vendor.rows["XTEST"] = [{**row, "date": "2026-02-11"} if row["value"] == 0.5 else row for row in RECORDED]
    assert sync_at(store, clock, LATER) == (1, 1)
    dividends = current(store, action="dividend")
    assert list(zip(dividends["period"], dividends["value"], strict=True)) == [
        ("2026-02-10", 0.1),
        ("2026-02-11", 0.5),
    ]
    # nothing is deleted: the old row is still there, now marked, with no value
    marked = store.table("sharadar_actions", "XTEST", action="dividend", withdrawn=1)
    assert list(zip(marked["period"], marked["item"], strict=True)) == [("2026-02-10", "2")]
    assert marked["value"].isna().all()
    # and what the store held before the correction can still be read
    before = current(store, action="dividend", as_of=NOW)
    assert list(zip(before["period"], before["item"], before["value"], strict=True)) == [
        ("2026-02-10", "1", 0.1),
        ("2026-02-10", "2", 0.5),
    ]


def test_the_tables_of_several_tickers_read_together_say_whose_each_row_is(tmp_path, vendor, clock):
    # both tickers have an item 1 on 2026-02-10: without the ticker the rows could not be told apart
    vendor.rows["XOTHER"] = of("XOTHER", [action("2026-01-15", value=0.3), action("2026-02-10", value=0.7)])
    both = open_store(tmp_path, vendor, clock)
    both.add("sharadar_actions", ["XTEST", "XOTHER"])
    both.sync()
    dividends = both.table("sharadar_actions", withdrawn=0, action="dividend")
    rows = zip(dividends["ticker"], dividends["period"], dividends["item"], dividends["value"], strict=True)
    assert sorted(rows) == [
        ("XOTHER", "2026-01-15", "1", 0.3),
        ("XOTHER", "2026-02-10", "1", 0.7),
        ("XTEST", "2026-02-10", "1", 0.1),
        ("XTEST", "2026-02-10", "2", 0.5),
    ]


def test_a_withdrawal_in_one_ticker_marks_nothing_in_the_other(tmp_path, vendor, clock):
    vendor.rows["XOTHER"] = of("XOTHER", [action("2026-01-15", value=0.3), action("2026-02-10", value=0.7)])
    both = open_store(tmp_path, vendor, clock)
    both.add("sharadar_actions", ["XTEST", "XOTHER"])
    assert sync_at(both, clock, NOW) == (6, 0)
    path = tmp_path / "store" / "tables" / "sharadar_actions" / "XOTHER.parquet"
    before = pd.read_parquet(path)
    # XTEST loses its 0.1: its 0.5 becomes item 1 (1 revised) and its item 2 is marked (1 revised);
    # XOTHER answers what it answered, and has nothing to add
    vendor.rows["XTEST"] = [row for row in RECORDED if row["value"] != 0.1]
    assert sync_at(both, clock, LATER) == (0, 2)
    marked = both.table("sharadar_actions", withdrawn=1)
    assert list(zip(marked["ticker"], marked["period"], marked["item"], strict=True)) == [("XTEST", "2026-02-10", "2")]
    assert both.table("sharadar_actions", "XOTHER", withdrawn=1).empty
    assert pd.read_parquet(path).equals(before)  # not a row of XOTHER was written
    other = both.table("sharadar_actions", "XOTHER", withdrawn=0)
    assert list(zip(other["period"], other["value"], strict=True)) == [("2026-01-15", 0.3), ("2026-02-10", 0.7)]


def test_the_lower_of_two_dividends_of_a_day_withdrawn_leaves_the_other_once(store, vendor, clock):
    # The answer keeps the 0.5, which is now the only dividend of the day, so item 1: the stored item 1
    # (0.1) takes another value (1 revised). The stored item 2 (0.5) is held, on a day the answer
    # covers, and is not in it: marked (1 revised). Without the mark the table would show 0.5 twice.
    vendor.rows["XTEST"] = [row for row in RECORDED if row["value"] != 0.1]
    assert sync_at(store, clock, LATER) == (0, 2)
    dividends = current(store, action="dividend")
    assert list(zip(dividends["item"], dividends["value"], strict=True)) == [("1", 0.5)]


def test_a_short_answer_marks_nothing_of_the_days_before_it(store, vendor, clock):
    # What Sharadar answers a key it does not accept is a short, recent history, not an error
    vendor.rows["XTEST"] = RECORDED[:1]  # only the split of 2026-03-03
    assert sync_at(store, clock, LATER) == (0, 0)
    assert keys_of(current(store)) == original_four()
    assert store.table("sharadar_actions", "XTEST", withdrawn=1).empty


def test_an_answer_without_actions_marks_nothing_of_what_is_held(store, vendor, clock):
    vendor.rows["XTEST"] = []
    assert sync_at(store, clock, LATER) == (0, 0)
    assert keys_of(current(store)) == original_four()
    assert store.table("sharadar_actions", "XTEST", withdrawn=1).empty


def test_a_withdrawn_action_that_comes_back_is_stored_again(store, vendor, clock):
    vendor.rows["XTEST"] = [{**row, "date": "2026-02-11"} if row["value"] == 0.5 else row for row in RECORDED]
    sync_at(store, clock, LATER)
    vendor.rows["XTEST"] = RECORDED
    # the 0.5 is back at item 2 of the 10th (1 revised: its key was marked) and the copy of the 11th is
    # now the one gone (1 revised: it is held, within the days covered, and no longer in the answer)
    assert sync_at(store, clock, LATEST) == (0, 2)
    assert keys_of(current(store)) == original_four()
    back = current(store, action="dividend", item="2")
    assert list(zip(back["period"], back["value"], strict=True)) == [("2026-02-10", 0.5)]
    assert list(store.table("sharadar_actions", "XTEST", period="2026-02-11")["withdrawn"]) == [1.0]


def test_the_same_answer_twice_adds_nothing_the_second_time_also_when_it_marks_a_withdrawal(
    store, vendor, clock, tmp_path
):
    vendor.rows["XTEST"] = [row for row in RECORDED if row["value"] != 0.1]
    assert sync_at(store, clock, LATER) == (0, 2)
    path = tmp_path / "store" / "tables" / "sharadar_actions" / "XTEST.parquet"
    assert len(pd.read_parquet(path)) == 6  # the four of the first run, and the two it changed
    assert sync_at(store, clock, LATEST) == (0, 0)
    assert len(pd.read_parquet(path)) == 6  # the mark is sent again and stores nothing


def test_the_index_row_says_a_daily_frequency_and_the_threshold_of_the_source(store):
    assert store.info("sharadar_actions:XTEST").frequency == "D"
    row = store.index().set_index("key").loc["sharadar_actions:XTEST"]
    assert row["stale_after_days"] == NEVER_STALE


def test_an_entry_own_threshold_wins_over_the_one_of_the_source(tmp_path, vendor, clock):
    clock["now"] = datetime.datetime(2026, 4, 2, tzinfo=datetime.UTC)  # 30 days after the last action
    own = open_store(tmp_path, vendor, clock)
    own.add("sharadar_actions", ["XTEST"], stale_after_days=30)
    own.sync()
    assert own.index().iloc[0]["stale_after_days"] == 30
    assert own.status().iloc[0]["state"] == "ok"
    clock["now"] = datetime.datetime(2026, 4, 3, tzinfo=datetime.UTC)
    row = own.status().iloc[0]
    assert (row["state"], row["reason"]) == ("stale", "last data 2026-03-03 (31 days ago)")


def test_an_entry_with_a_start_marks_only_the_days_it_asks_for(store, vendor, clock, tmp_path):
    # The first run, with no start, stored the four actions. The entry now asks from 2026-02-10 on,
    # and the split of 2026-03-03 is not in the answer: it is marked (1 revised). The spin-off of
    # 2025-06-30 is not in the answer either, but it is before the first day asked: it stays current.
    vendor.rows["XTEST"] = [row for row in RECORDED if row["action"] != "split"]
    since = open_store(tmp_path, vendor, clock)
    since.add("sharadar_actions", ["XTEST"], start=datetime.date(2026, 2, 10))
    assert sync_at(since, clock, LATER) == (0, 1)
    assert vendor.asked[-1] == "2026-02-10"  # the spin-off was not in the answer because of this
    assert keys_of(current(since)) == [key for key in original_four() if key[1] != "split"]
    marked = since.table("sharadar_actions", "XTEST", withdrawn=1)
    assert list(zip(marked["period"], marked["action"], strict=True)) == [("2026-03-03", "split")]


def test_show_prints_the_citation_and_the_newest_rows(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    arguments = ["show", "--root", str(tmp_path / "store"), "sharadar_actions:XTEST"]
    result = click.testing.CliRunner().invoke(cli, arguments)
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[Sharadar: XTEST, 2026-03-03, fetched 2026-06-06]"
    assert lines[1].startswith("Corporate actions of XTEST (Sharadar) | 4 rows")
    header = ["frequency", "period", "ticker", "action", "contraticker", "item", "value", "withdrawn", "name"]
    assert lines[2].split() == [*header, "contraname"]
    rows = [line.split("  ") for line in lines[3:]]  # the cells are told apart by two spaces
    assert len(rows) == 4
    assert rows[-1][:8] == ["D", "2026-03-03", "XTEST", "split", "", "1", "2.0", "0.0"]  # the newest action, as sent


def test_show_prints_a_withdrawn_action_as_a_row_of_its_own(store, vendor, clock, tmp_path, monkeypatch):
    # the 0.5 moves to the next day: the old place is marked, and the store holds five rows, four current
    vendor.rows["XTEST"] = [{**row, "date": "2026-02-11"} if row["value"] == 0.5 else row for row in RECORDED]
    sync_at(store, clock, LATER)
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    arguments = ["show", "--root", str(tmp_path / "store"), "sharadar_actions:XTEST"]
    result = click.testing.CliRunner().invoke(cli, arguments)
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[1].startswith("Corporate actions of XTEST (Sharadar) | 5 rows")
    rows = [line.split("  ") for line in lines[3:]]
    assert len(rows) == 5
    marks = [row for row in rows if row[7] == "1.0"]
    assert marks == [["D", "2026-02-10", "XTEST", "dividend", "", "2", "nan", "1.0", "", ""]]


def test_status_never_calls_the_actions_stale(store, clock):
    # 95 days after the last action, where a table with a daily frequency would be stale after 10
    status = store.status()
    states = dict(zip(status["key"], status["state"], strict=True))
    assert states["sharadar_actions:XTEST"] == "ok"
    clock["now"] = NOW + datetime.timedelta(days=3650)  # actions come when they come
    assert store.status().set_index("key").loc["sharadar_actions:XTEST", "state"] == "ok"
