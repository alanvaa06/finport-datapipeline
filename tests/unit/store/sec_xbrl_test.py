import datetime
import json

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
from data_pipeline.store.sources.sec_xbrl import (
    ATTRIBUTE_COLUMNS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    SecXbrl,
    versions,
)

from .helpers import NOW, client, entry

AGENT = "Jane Doe jane@example.com"
CREDENTIALS = Credentials({keys.SEC_UA: AGENT})
TICKERS = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 1067983, "ticker": "BRK-B", "title": "BERKSHIRE HATHAWAY INC"},
    "2": {"cik_str": 99, "ticker": "NOXB", "title": "No XBRL Corp"},
}
LATER = NOW + datetime.timedelta(days=30)


def appearance(value, filed, accession, end="2023-09-30", start="2022-09-25", form="10-K", **more):
    item = {"end": end, "val": value, "accn": accession, "fy": 2023, "fp": "FY", "form": form, "filed": filed, **more}
    if start:
        item["start"] = start
    return item


def facts(revenues, assets=()):
    return {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "Revenues": {"label": "Revenues", "units": {"USD": list(revenues)}},
                "Assets": {"label": "Assets", "units": {"USD": list(assets)}},
            }
        },
    }


ORIGINAL = appearance(383285000000, "2023-11-03", "0000320193-23-000106", frame="CY2023")
REPEATED = appearance(383285000000, "2024-11-01", "0000320193-24-000123")
RESTATED = appearance(383000000000, "2025-10-31", "0000320193-25-000079")
BALANCE = appearance(352583000000, "2023-11-03", "0000320193-23-000106", start=None)


def aapl(*extra):
    return facts([ORIGINAL, REPEATED, *extra], [BALANCE])


def values(rows):
    return [(row["concept"], row["filed"], row["value"]) for row in rows]


# -- versions ----------------------------------------------------------------------------------


def test_a_fact_reported_once_is_one_version_with_everything_the_sec_says():
    (row,) = versions(facts([ORIGINAL]))
    assert {name: row[name] for name in KEY_COLUMNS} == {
        "taxonomy": "us-gaap",
        "concept": "Revenues",
        "unit": "USD",
        "start": "2022-09-25",
        "end": "2023-09-30",
    }
    assert (row["value"], row["date"]) == (383285000000.0, datetime.date(2023, 9, 30))
    assert {name: row[name] for name in ATTRIBUTE_COLUMNS} == {
        "form": "10-K",
        "accession": "0000320193-23-000106",
        "filed": "2023-11-03",
        "fiscal_year": "2023",
        "fiscal_period": "FY",
        "frame": "CY2023",
    }
    # known from the end of the day the SEC received it: a filing comes in during the day
    assert row["published_at"] == datetime.datetime.combine(datetime.date(2023, 11, 3), datetime.time.max, datetime.UTC)


def test_a_later_filing_that_repeats_the_value_is_not_a_version():
    assert values(versions(facts([REPEATED, ORIGINAL]))) == [("Revenues", "2023-11-03", 383285000000.0)]


def test_a_restatement_is_a_version_dated_with_its_own_filing():
    rows = versions(facts([RESTATED, ORIGINAL, REPEATED]))
    assert values(rows) == [("Revenues", "2023-11-03", 383285000000.0), ("Revenues", "2025-10-31", 383000000000.0)]


def test_a_value_restated_and_restated_back_is_three_versions():
    back = appearance(383285000000, "2026-10-30", "0000320193-26-000001")
    assert [row["filed"] for row in versions(facts([ORIGINAL, RESTATED, back]))] == [
        "2023-11-03",
        "2025-10-31",
        "2026-10-30",
    ]


def test_a_version_takes_the_frame_from_whichever_of_its_appearances_carries_it():
    # the SEC sets `frame` on one appearance of a fact, the latest filed, often a comparative
    original = appearance(5601000000, "2009-10-27", "0001193125-09-214859", fy=2009, fp="FY")
    comparative = appearance(5601000000, "2010-01-25", "0001193125-10-012085", form="10-Q", fy=2010, fp="Q1")
    (row,) = versions(facts([original, {**comparative, "frame": "CY2009Q3I"}]))
    assert row["frame"] == "CY2009Q3I"
    # everything else describes the filing that first reported the version
    assert (row["accession"], row["form"], row["fiscal_year"], row["fiscal_period"]) == (
        "0001193125-09-214859",
        "10-K",
        "2009",
        "FY",
    )


def test_a_frame_on_a_restatement_stays_with_the_restated_version():
    framed = {**appearance(383000000000, "2026-10-30", "0000320193-26-000001"), "frame": "CY2023"}
    rows = versions(facts([{**ORIGINAL, "frame": ""}, RESTATED, framed]))
    assert [(row["filed"], row["frame"]) for row in rows] == [("2023-11-03", ""), ("2025-10-31", "CY2023")]


def test_filings_of_the_same_day_are_ordered_by_accession():
    first = appearance(1.0, "2024-02-01", "0000320193-24-000001")
    amended = appearance(2.0, "2024-02-01", "0000320193-24-000002", form="10-K/A")
    assert [row["value"] for row in versions(facts([amended, first]))] == [1.0, 2.0]


def test_float_noise_is_not_a_restatement():
    noisy = appearance(383285000000.00001, "2024-11-01", "0000320193-24-000123")
    assert len(versions(facts([ORIGINAL, noisy]))) == 1


def test_a_balance_has_no_start_and_periods_are_separate_facts():
    quarter = appearance(89498000000, "2023-11-03", "0000320193-23-000106", start="2023-07-02")
    rows = versions(facts([ORIGINAL, quarter], [BALANCE]))
    assert [(row["concept"], row["start"], row["end"]) for row in rows] == [
        ("Revenues", "2022-09-25", "2023-09-30"),
        ("Revenues", "2023-07-02", "2023-09-30"),
        ("Assets", "", "2023-09-30"),
    ]


def test_what_is_not_a_dated_number_is_skipped():
    broken = [
        appearance("n/a", "2023-11-03", "a"),
        appearance(True, "2023-11-03", "b"),
        appearance(None, "2023-11-03", "c"),
        appearance(1.0, None, "d"),
        {"val": 1.0, "filed": "2023-11-03", "accn": "e"},
    ]
    assert versions(facts(broken)) == ()
    assert versions({}) == ()
    assert versions({"facts": {"dei": {"EntityCommonStockSharesOutstanding": {"units": None}}}}) == ()


def test_missing_labels_of_the_sec_are_stored_as_empty_text():
    bare = {"end": "2023-09-30", "val": 5, "filed": "2023-11-03"}
    (row,) = versions(facts([bare]))
    assert (row["start"], row["form"], row["accession"], row["fiscal_year"], row["frame"]) == ("", "", "", "", "")


# -- the source --------------------------------------------------------------------------------


def server(companies, seen=None):
    def handler(request):
        if seen is not None:
            seen.append(request)
        if request.url.host == "www.sec.gov":
            return httpx.Response(200, json=TICKERS)
        cik = request.url.path.split("CIK")[1].removesuffix(".json")
        if cik not in companies:
            return httpx.Response(404, text="<Error><Code>NoSuchKey</Code></Error>")
        return httpx.Response(200, json=companies[cik])

    return handler


def fetch(handler, requests, credentials=CREDENTIALS):
    return list(SecXbrl(client(handler), credentials).fetch(requests))


def company(ticker="AAPL"):
    return entry(ticker, "sec_xbrl")


def test_it_is_a_table_source_paced_under_the_secs_limit():
    assert SecXbrl.kind is Kind.TABLE
    assert SecXbrl.requests_per_minute == 300
    assert SecXbrl.daily_budget is None


def test_validate_wants_a_ticker_in_upper_case_and_no_fields():
    source = SecXbrl(client(), CREDENTIALS)
    source.validate(company("AAPL"))
    source.validate(company("BRK-B"))
    source.validate(company("BF.B"))
    for bad in ("aapl", "320193X!", "", "A B"):
        with pytest.raises(CatalogError, match="a SEC id is a ticker in upper case"):
            source.validate(company(bad))
    with pytest.raises(CatalogError, match="unknown field\\(s\\) for source 'sec_xbrl': form"):
        source.validate(entry("AAPL", "sec_xbrl", params={"form": "10-K"}))


def test_the_ticker_is_resolved_once_and_every_call_carries_the_user_agent():
    seen = []
    companies = {"0000320193": aapl(), "0001067983": facts([ORIGINAL])}
    batches = fetch(server(companies, seen), [Request(company()), Request(company("BRK-B"))])
    assert [str(request.url) for request in seen] == [
        "https://www.sec.gov/files/company_tickers.json",
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0001067983.json",
    ]
    assert {request.headers["User-Agent"] for request in seen} == {AGENT}
    assert [batch.tables[0].key for batch in batches] == ["sec_xbrl:AAPL", "sec_xbrl:BRK-B"]


def test_a_company_is_one_versioned_table():
    (batch,) = fetch(server({"0000320193": aapl(RESTATED)}), [Request(company())])
    table = batch.tables[0]
    assert (table.name, table.versioned, table.stale_after_days) == ("Apple Inc.", True, 200)
    assert (table.key_columns, table.value_columns, table.attribute_columns) == (
        KEY_COLUMNS,
        VALUE_COLUMNS,
        ATTRIBUTE_COLUMNS,
    )
    assert table.attrs == {"cik": "0000320193"}
    assert len(table.rows) == 3  # revenues, its restatement, assets


def test_without_the_user_agent_nothing_is_requested():
    calls = []
    (batch,) = fetch(calls.append, [Request(company())], Credentials())
    assert calls == []
    assert batch.failures[0].outcome is Outcome.KEY_ERROR
    assert "SEC_EDGAR_UA is missing" in batch.failures[0].reason


def test_an_unknown_ticker_and_a_company_without_facts_fail_alone():
    requests = [Request(company("ZZZZ")), Request(company("NOXB")), Request(company())]
    unknown, empty, found = fetch(server({"0000320193": aapl()}), requests)
    assert (unknown.failures[0].outcome, unknown.failures[0].reason) == (
        Outcome.NOT_FOUND,
        "the SEC lists no company with ticker 'ZZZZ'",
    )
    assert (empty.failures[0].outcome, empty.failures[0].reason) == (
        Outcome.NOT_FOUND,
        "the SEC has no XBRL facts for this company",
    )
    assert found.tables[0].key == "sec_xbrl:AAPL"


def test_a_block_by_the_sec_stops_the_source():
    with pytest.raises(QuotaExhaustedError, match="check that SEC_EDGAR_UA is a User-Agent with a contact"):
        fetch(lambda _request: httpx.Response(403, text="Request Rate Threshold Exceeded"), [Request(company())])

    def blocked_later(request):
        if request.url.host == "www.sec.gov":
            return httpx.Response(200, json=TICKERS)
        return httpx.Response(429)

    with pytest.raises(QuotaExhaustedError, match="HTTP 429"):
        fetch(blocked_later, [Request(company())])


def test_a_failed_list_of_tickers_fails_every_company():
    requests = [Request(company()), Request(company("BRK-B"))]
    (batch,) = fetch(lambda _request: httpx.Response(500), requests)
    assert [failure.outcome for failure in batch.failures] == [Outcome.NETWORK_ERROR] * 2
    assert batch.failures[0].reason.startswith("list of tickers: ")
    (batch,) = fetch(lambda _request: httpx.Response(200, json=["not", "a", "mapping"]), requests)
    assert [failure.outcome for failure in batch.failures] == [Outcome.SOURCE_ERROR] * 2


def test_a_failed_company_fails_alone():
    def handler(request):
        if request.url.host == "www.sec.gov":
            return httpx.Response(200, json=TICKERS)
        if "0000320193" in request.url.path:
            return httpx.Response(500)
        return httpx.Response(200, json=facts([ORIGINAL]))

    failed, found = fetch(handler, [Request(company()), Request(company("BRK-B"))])
    assert failed.failures[0].outcome is Outcome.NETWORK_ERROR
    assert found.tables[0].key == "sec_xbrl:BRK-B"


@pytest.mark.parametrize(
    "response",
    [httpx.Response(400, text="bad"), httpx.Response(200, json={"facts": {"us-gaap": []}})],
)
def test_an_unexpected_answer_is_a_source_error(response):
    def handler(request):
        return httpx.Response(200, json=TICKERS) if request.url.host == "www.sec.gov" else response

    (batch,) = fetch(handler, [Request(company())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR


# -- through the store -------------------------------------------------------------------------


class Sec:
    """A fake SEC whose facts the test can change between syncs."""

    def __init__(self):
        self.companies = {"0000320193": aapl()}
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        return server(self.companies)(request)


@pytest.fixture
def sec():
    return Sec()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, sec, clock):
    (tmp_path / "catalog.yaml").write_text("- source: sec_xbrl\n  ids: [AAPL]\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"SEC_EDGAR_UA={AGENT}\n", encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(sec),
        sleep=lambda _seconds: None,
    )
    built.report = built.sync()
    return built


def test_first_sync_stores_the_facts_and_describes_the_company(store, tmp_path):
    assert (store.report.sources[0].ok, store.report.sources[0].new, store.report.sources[0].calls) == (1, 2, 2)
    assert (tmp_path / "store" / "tables" / "sec_xbrl" / "AAPL.parquet").exists()
    info = store.info("sec_xbrl:AAPL")
    assert (info.kind, info.name, info.frequency) == ("table", "Apple Inc.", "")
    assert info.label == "[SEC EDGAR: AAPL, 2023-09-30, fetched 2026-06-06]"
    row = store.index().iloc[0]
    assert json.loads(row["attrs"]) == {"cik": "0000320193"}
    assert row["stale_after_days"] == 200


def test_table_returns_keys_date_value_and_attributes(store):
    table = store.table("sec_xbrl", "AAPL")
    assert list(table.columns) == [*KEY_COLUMNS, "date", *VALUE_COLUMNS, *ATTRIBUTE_COLUMNS]
    assert dict(zip(table["concept"], table["value"], strict=True)) == {
        "Assets": 352583000000.0,
        "Revenues": 383285000000.0,
    }
    assert list(store.table("sec_xbrl", "AAPL", concept="Revenues", form="10-K")["filed"]) == ["2023-11-03"]
    assert store.table("sec_xbrl", "AAPL", form="10-Q").empty


def test_a_second_sync_adds_nothing(store, clock):
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(store.table("sec_xbrl", "AAPL")) == 2


def test_a_restatement_is_a_new_version_and_as_of_reads_what_was_known(store, sec, clock):
    sec.companies["0000320193"] = aapl(RESTATED)
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    now = store.table("sec_xbrl", "AAPL", concept="Revenues")
    assert (list(now["value"]), list(now["filed"])) == ([383000000000.0], ["2025-10-31"])
    before = store.table("sec_xbrl", "AAPL", concept="Revenues", as_of="2025-10-30")
    assert (list(before["value"]), list(before["filed"])) == ([383285000000.0], ["2023-11-03"])
    same_day = store.table("sec_xbrl", "AAPL", concept="Revenues", as_of="2025-10-31")
    assert list(same_day["value"]) == [383000000000.0]
    assert store.table("sec_xbrl", "AAPL", as_of="2023-11-02").empty


def test_a_first_load_with_a_restatement_reads_the_same_as_two_loads(tmp_path, sec, clock):
    sec.companies["0000320193"] = aapl(RESTATED)
    (tmp_path / ".env").write_text(f"SEC_EDGAR_UA={AGENT}\n", encoding="utf-8")
    store = Store(
        tmp_path / "once",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(sec),
        sleep=lambda _seconds: None,
    )
    store.add("sec_xbrl", ["AAPL"])
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (2, 1)  # two facts, one earlier restatement
    assert list(store.table("sec_xbrl", "AAPL", concept="Revenues")["value"]) == [383000000000.0]
    before = store.table("sec_xbrl", "AAPL", concept="Revenues", as_of="2024-01-01")
    assert list(before["value"]) == [383285000000.0]


def test_a_stored_version_without_a_frame_gets_it_when_the_same_version_comes_again(tmp_path, sec, clock):
    # stored by a build that kept the frame of the first appearance only: the SEC had put it on the repeat
    unframed = {key: value for key, value in ORIGINAL.items() if key != "frame"}
    sec.companies["0000320193"] = facts([unframed, REPEATED], [BALANCE])
    (tmp_path / ".env").write_text(f"SEC_EDGAR_UA={AGENT}\n", encoding="utf-8")
    store = Store(
        tmp_path / "store",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(sec),
        sleep=lambda _seconds: None,
    )
    store.add("sec_xbrl", ["AAPL"])
    store.sync()
    path = tmp_path / "store" / "tables" / "sec_xbrl" / "AAPL.parquet"
    before = pd.read_parquet(path)
    assert store.table("sec_xbrl", "AAPL", frame="CY2023").empty
    sec.companies["0000320193"] = facts([unframed, {**REPEATED, "frame": "CY2023"}], [BALANCE])
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    after = pd.read_parquet(path)
    assert list(after["frame"]) == ["", "CY2023"]  # Assets, then Revenues
    pd.testing.assert_frame_equal(after.drop(columns="frame"), before.drop(columns="frame"))  # no new version
    assert list(store.table("sec_xbrl", "AAPL", frame="CY2023")["concept"]) == ["Revenues"]
    assert list(store.table("sec_xbrl", "AAPL", frame="CY2023", as_of="2024-01-01")["concept"]) == ["Revenues"]


def test_a_new_period_is_a_new_fact(store, sec, clock):
    newer = appearance(391035000000, "2024-11-01", "0000320193-24-000123", end="2024-09-28", start="2023-10-01")
    sec.companies["0000320193"] = aapl(newer)
    clock["now"] = LATER
    report = store.sync()
    assert (report.sources[0].new, report.sources[0].revised) == (1, 0)
    assert store.info("sec_xbrl:AAPL").last_period == "2024-09-28"


def test_status_uses_the_two_hundred_days(store, clock):
    assert store.status().iloc[0]["state"] == "stale"  # the newest fact ended 2023-09-30
    clock["now"] = datetime.datetime(2024, 4, 17, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["state"] == "ok"  # 200 days after
    clock["now"] = datetime.datetime(2024, 4, 18, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2023-09-30 (201 days ago)"


def test_show_prints_the_citation_and_the_newest_rows(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    result = click.testing.CliRunner().invoke(cli, ["show", "--root", str(tmp_path / "store"), "sec_xbrl:AAPL"])
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[SEC EDGAR: AAPL, 2023-09-30, fetched 2026-06-06]"
    assert lines[1] == "Apple Inc. | 2 rows"
    assert lines[2].split("  ")[:6] == ["taxonomy", "concept", "unit", "start", "end", "value"]
    assert "us-gaap  Revenues  USD  2022-09-25  2023-09-30  383285000000.0  10-K" in lines[3] + lines[4]


def test_the_stored_table_keeps_every_version(store, sec, clock, tmp_path):
    sec.companies["0000320193"] = aapl(RESTATED)
    clock["now"] = LATER
    store.sync()
    stored = pd.read_parquet(tmp_path / "store" / "tables" / "sec_xbrl" / "AAPL.parquet")
    revenues = stored[stored["concept"] == "Revenues"]
    assert list(revenues["value"]) == [383285000000.0, 383000000000.0]
    published = [
        pd.Timestamp(datetime.datetime.combine(datetime.date(*day), datetime.time.max, datetime.UTC))
        for day in ((2023, 11, 3), (2025, 10, 31))
    ]
    assert list(revenues["published_at"]) == published
    assert list(revenues["fetched_at"]) == [pd.Timestamp(NOW), pd.Timestamp(LATER)]
