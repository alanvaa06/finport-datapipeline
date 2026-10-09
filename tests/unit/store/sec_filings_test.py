import datetime
import hashlib
import json
import pathlib

import click.testing
import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store import cli as cli_module
from data_pipeline.store import sources as source_registry
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError, StoreError, UnknownSeriesError
from data_pipeline.store.model import Kind, Outcome, Request
from data_pipeline.store.sources.sec_filings import (
    Filing,
    SecFilings,
    Settings,
    exhibits,
    missing,
    read_filings,
    settings,
    years_before,
)

from .helpers import NOW, client, entry

TODAY = NOW.date()  # 2026-06-06
AGENT = "Jane Doe jane@example.com"
CREDENTIALS = Credentials({keys.SEC_UA: AGENT})
TICKERS = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
CIK = "0000320193"
ANNUAL = "0000320193-25-000079"
EVENT = "0000320193-26-000005"
AMENDED = "0000320193-26-000009"
OLD = "0000320193-15-000001"
INSIDER = "0000320193-26-000002"


def page(*rows):
    """A page of filings the way the SEC sends it: one list per column."""
    columns = ("accessionNumber", "form", "filingDate", "reportDate", "primaryDocument")
    return {column: [row[position] for row in rows] for position, column in enumerate(columns)}


RECENT = page(
    (AMENDED, "10-K/A", "2026-02-10", "2025-09-27", "aapl-10ka.htm"),
    (EVENT, "8-K", "2026-01-29", "2026-01-29", "aapl-8k.htm"),
    (INSIDER, "4", "2026-01-15", "2026-01-13", "xslF345X05/form4.xml"),
    (ANNUAL, "10-K", "2025-10-31", "2025-09-27", "aapl-20250927.htm"),
    (OLD, "10-K", "2015-10-28", "2015-09-26", "aapl-2015.htm"),
)
FOLDER = [
    {"name": "aapl-8k.htm"},
    {"name": "a8-kex991q1.htm"},
    {"name": "0000320193-26-000005-index.html"},
    {"name": "R1.htm"},
    {"name": "aapl-8k_htm.xml"},
    {"name": "logo.jpg"},
]


class Sec:
    """A fake SEC. `files` maps the path of a document to its bytes."""

    def __init__(self):
        self.recent = RECENT
        self.older = {}
        self.pages = []
        self.files = {
            "/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm": b"<html>annual report</html>",
            "/Archives/edgar/data/320193/000032019326000005/aapl-8k.htm": b"<html>8-K wrapper</html>",
            "/Archives/edgar/data/320193/000032019326000005/a8-kex991q1.htm": b"<html>earnings release</html>",
            "/Archives/edgar/data/320193/000032019326000009/aapl-10ka.htm": b"<html>amended</html>",
            "/Archives/edgar/data/320193/000032019315000001/aapl-2015.htm": b"<html>2015</html>",
        }
        self.seen = []
        self.fail = None

    def __call__(self, request):
        self.seen.append(request)
        path = request.url.path
        if self.fail is not None and self.fail in path:
            return httpx.Response(500)
        if path == "/files/company_tickers.json":
            return httpx.Response(200, json=TICKERS)
        if path == f"/submissions/CIK{CIK}.json":
            filings = {"recent": self.recent, "files": self.pages}
            return httpx.Response(200, json={"name": "Apple Inc.", "filings": filings})
        if path.startswith("/submissions/") and path.split("/")[-1] in self.older:
            return httpx.Response(200, json=self.older[path.split("/")[-1]])
        if path.endswith("/000032019326000005/index.json"):
            return httpx.Response(200, json={"directory": {"item": FOLDER}})
        if path in self.files:
            return httpx.Response(200, content=self.files[path])
        return httpx.Response(404)

    def paths(self):
        return [request.url.path.split("/")[-1] for request in self.seen]


def company(ticker="AAPL", **fields):
    return entry(ticker, "sec_filings", **fields)


def fetch(handler, requests, credentials=CREDENTIALS):
    source = SecFilings(client(handler), credentials, today=lambda: TODAY)
    return list(source.fetch(requests))


def groups(batches):
    return [document.group for batch in batches for data in batch.documents for document in data.documents]


# -- settings and what is missing --------------------------------------------------------------


def test_it_is_a_document_source_paced_under_the_secs_limit():
    assert SecFilings.kind is Kind.DOCUMENT
    assert SecFilings.requests_per_minute == 300
    assert SecFilings.daily_budget is None


def test_the_defaults_are_the_periodic_and_current_forms_with_amendments_for_ten_years():
    chosen = settings(company(), TODAY)
    assert chosen.forms == {"10-K", "10-Q", "8-K", "20-F", "40-F", "10-K/A", "10-Q/A", "8-K/A", "20-F/A", "40-F/A"}
    assert chosen.start == datetime.date(2016, 6, 6)


def test_every_field_is_read():
    chosen = settings(
        company(start=datetime.date(2020, 1, 1), params={"forms": ["10-k", "6-K"], "amendments": False}), TODAY
    )
    assert chosen == Settings(frozenset({"10-K", "6-K"}), datetime.date(2020, 1, 1))
    assert settings(company(params={"forms": ["10-K/A"]}), TODAY).forms == {"10-K/A"}


def test_ten_years_before_a_leap_day():
    assert years_before(datetime.date(2024, 2, 29), 10) == datetime.date(2014, 2, 28)


@pytest.mark.parametrize(
    ("ticker", "params", "message"),
    [
        ("aapl", {}, "a SEC id is a ticker in upper case"),
        ("AAPL", {"forms": "10-K"}, "'forms' must be a non-empty list"),
        ("AAPL", {"forms": []}, "'forms' must be a non-empty list"),
        ("AAPL", {"forms": ["10-K", "../x"]}, "not a SEC form: ../X"),
        ("AAPL", {"amendments": "yes"}, "'amendments' must be true or false"),
        ("AAPL", {"since": "2020"}, "unknown field\\(s\\) for source 'sec_filings': since"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(ticker, params, message):
    source = SecFilings(client(), CREDENTIALS, today=lambda: TODAY)
    with pytest.raises(CatalogError, match=message):
        source.validate(company(ticker, params=params))


def test_a_page_of_the_sec_is_read_row_by_row():
    filings = read_filings(RECENT)
    assert filings[0] == Filing(AMENDED, "10-K/A", datetime.date(2026, 2, 10), "2025-09-27", "aapl-10ka.htm")
    assert len(filings) == 5
    blank = read_filings(page(("a", "8-K", "2026-01-01", "", None)))[0]
    assert (blank.period, blank.primary) == ("", "")


def test_missing_filings_are_wanted_recent_not_stored_and_oldest_first():
    chosen = settings(company(), TODAY)
    filings = read_filings(RECENT)
    assert [filing.accession for filing in missing(filings, chosen, frozenset())] == [ANNUAL, EVENT, AMENDED]
    assert [filing.accession for filing in missing(filings, chosen, {ANNUAL, AMENDED})] == [EVENT]
    plain = settings(company(params={"amendments": False}), TODAY)
    assert [filing.accession for filing in missing(filings, plain, frozenset())] == [ANNUAL, EVENT]
    longer = settings(company(start=datetime.date(2015, 1, 1), params={"forms": ["10-K"], "amendments": False}), TODAY)
    assert [filing.accession for filing in missing(filings, longer, frozenset())] == [OLD, ANNUAL]


def test_the_exhibits_of_a_folder_leave_out_the_wrapper_indexes_and_viewer_pages():
    assert exhibits(FOLDER, "aapl-8k.htm") == ["a8-kex991q1.htm"]
    assert exhibits([{"name": "EX99.HTML"}, {"name": "r12.htm"}, {"name": "AAPL-8K.HTM"}, {}], "aapl-8k.htm") == [
        "EX99.HTML"
    ]


# -- the source --------------------------------------------------------------------------------


def test_a_company_is_reached_first_and_then_gives_one_batch_per_filing():
    sec = Sec()
    batches = fetch(sec, [Request(company())])
    first = batches[0].documents[0]
    assert (first.key, first.name, first.documents) == ("sec_filings:AAPL", "Apple Inc.", ())
    assert (first.stale_after_days, first.attrs) == (140, {"cik": CIK})
    assert groups(batches) == [ANNUAL, EVENT, AMENDED]
    assert [len(batch.documents[0].documents) for batch in batches] == [0, 1, 1, 1]


def test_a_filing_is_its_primary_document_byte_for_byte():
    sec = Sec()
    document = fetch(sec, [Request(company())])[1].documents[0].documents[0]
    assert (document.group, document.date) == (ANNUAL, datetime.date(2025, 10, 31))
    assert document.attributes == {"form": "10-K", "period": "2025-09-27"}
    (file,) = document.files
    assert (file.name, file.role, file.content) == ("aapl-20250927.htm", "primary", b"<html>annual report</html>")
    assert file.url == "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm"


def test_an_8k_brings_its_exhibits():
    sec = Sec()
    document = fetch(sec, [Request(company())])[2].documents[0].documents[0]
    assert [(file.name, file.role) for file in document.files] == [
        ("aapl-8k.htm", "primary"),
        ("a8-kex991q1.htm", "exhibit"),
    ]
    assert document.files[1].content == b"<html>earnings release</html>"
    assert sec.paths().count("index.json") == 1  # only the 8-K has its folder listed


def test_every_call_carries_the_user_agent_and_stored_filings_are_not_asked_for():
    sec = Sec()
    batches = fetch(sec, [Request(company(), groups=frozenset({ANNUAL, EVENT, AMENDED}))])
    assert groups(batches) == []
    assert sec.paths() == ["company_tickers.json", f"CIK{CIK}.json"]
    assert {request.headers["User-Agent"] for request in sec.seen} == {AGENT}


def test_older_pages_are_asked_for_only_when_they_reach_the_start():
    sec = Sec()
    sec.pages = [
        {"name": "CIK0000320193-submissions-001.json", "filingFrom": "2012-01-01", "filingTo": "2017-12-31"},
        {"name": "CIK0000320193-submissions-002.json", "filingFrom": "1994-01-01", "filingTo": "2011-12-31"},
    ]
    older = "0000320193-17-000070"
    found = page((older, "10-K", "2017-11-03", "2017-09-30", "a10-k2017.htm"))
    sec.older = {"CIK0000320193-submissions-001.json": found}
    sec.files["/Archives/edgar/data/320193/000032019317000070/a10-k2017.htm"] = b"<html>2017</html>"
    batches = fetch(sec, [Request(company())])
    assert groups(batches)[0] == older
    assert "CIK0000320193-submissions-001.json" in sec.paths()
    assert "CIK0000320193-submissions-002.json" not in sec.paths()


def test_a_filing_without_a_primary_document_is_its_complete_text():
    sec = Sec()
    sec.recent = page((ANNUAL, "10-K", "2025-10-31", "2025-09-27", ""))
    sec.files[f"/Archives/edgar/data/320193/000032019325000079/{ANNUAL}.txt"] = b"<SEC-DOCUMENT>"
    document = fetch(sec, [Request(company())])[1].documents[0].documents[0]
    assert [(file.name, file.content) for file in document.files] == [(f"{ANNUAL}.txt", b"<SEC-DOCUMENT>")]


def test_without_the_user_agent_nothing_is_requested():
    calls = []
    (batch,) = fetch(calls.append, [Request(company())], Credentials())
    assert calls == []
    assert batch.failures[0].outcome is Outcome.KEY_ERROR


def test_an_unknown_ticker_fails_alone():
    sec = Sec()
    batches = fetch(sec, [Request(company("ZZZZ")), Request(company())])
    assert (batches[0].failures[0].outcome, batches[0].failures[0].reason) == (
        Outcome.NOT_FOUND,
        "the SEC lists no company with ticker 'ZZZZ'",
    )
    assert groups(batches) == [ANNUAL, EVENT, AMENDED]


def failures(batches):
    return [(failure.outcome, failure.reason) for batch in batches for failure in batch.failures]


def test_a_filing_that_fails_is_recorded_and_the_later_ones_still_arrive():
    sec = Sec()
    sec.fail = "a8-kex991q1.htm"
    batches = fetch(sec, [Request(company())])
    assert groups(batches) == [ANNUAL, AMENDED]
    ((outcome, reason),) = failures(batches)
    assert outcome is Outcome.NETWORK_ERROR
    assert reason.startswith(f"{EVENT}: ")


def test_an_exhibit_the_sec_lists_but_does_not_have_no_longer_blocks_the_later_filings():
    sec = Sec()
    del sec.files["/Archives/edgar/data/320193/000032019326000005/a8-kex991q1.htm"]
    batches = fetch(sec, [Request(company())])
    assert groups(batches) == [ANNUAL, AMENDED]
    assert failures(batches) == [(Outcome.SOURCE_ERROR, f"{EVENT}: the SEC lists a8-kex991q1.htm but does not have it")]


def test_a_name_the_store_refuses_fails_only_its_filing():
    sec = Sec()
    odd = "0000320193-25-000050"
    quarter = (odd, "10-Q", "2025-08-01", "2025-06-28", "quarterly report.htm")
    sec.recent = page(quarter, *zip(*RECENT.values(), strict=True))
    sec.files["/Archives/edgar/data/320193/000032019325000050/quarterly report.htm"] = b"<html>q</html>"
    batches = fetch(sec, [Request(company())])
    assert groups(batches) == [ANNUAL, EVENT, AMENDED]
    ((outcome, reason),) = failures(batches)
    assert outcome is Outcome.SOURCE_ERROR
    assert reason == f"{odd}: the SEC lists a file named 'quarterly report.htm', not a plain file name"
    assert "quarterly report.htm" not in sec.paths()  # refused before it is downloaded


def test_an_exhibit_named_to_leave_its_folder_fails_its_filing_before_any_download():
    escape = chr(92).join(["..", "..", "evil.htm"])  # a backslash path: on Windows it leaves the folder
    sec = Sec()

    def handler(request):
        if request.url.path.endswith("/000032019326000005/index.json"):
            return httpx.Response(200, json={"directory": {"item": [*FOLDER, {"name": escape}]}})
        return sec(request)

    batches = fetch(handler, [Request(company())])
    assert groups(batches) == [ANNUAL, AMENDED]
    assert failures(batches) == [
        (Outcome.SOURCE_ERROR, f"{EVENT}: the SEC lists a file named {escape!r}, not a plain file name")
    ]
    assert {"aapl-8k.htm", "a8-kex991q1.htm"}.isdisjoint(sec.paths())  # nothing of that filing was downloaded


def test_three_network_failures_end_the_company_for_this_run():
    sec = Sec()
    sec.fail = "/Archives/"
    chosen = company(start=datetime.date(2015, 1, 1), params={"forms": ["10-K", "8-K"]})
    batches = fetch(sec, [Request(chosen)])
    assert [outcome for outcome, _ in failures(batches)] == [Outcome.NETWORK_ERROR] * 3
    assert "aapl-10ka.htm" not in sec.paths()  # the fourth filing waits for the next run


def test_a_document_the_sec_lists_but_does_not_have_is_a_source_error():
    sec = Sec()
    del sec.files["/Archives/edgar/data/320193/000032019326000009/aapl-10ka.htm"]
    batches = fetch(sec, [Request(company())])
    assert groups(batches) == [ANNUAL, EVENT]
    failure = batches[-1].failures[0]
    assert (failure.outcome, failure.reason) == (
        Outcome.SOURCE_ERROR,
        f"{AMENDED}: the SEC lists aapl-10ka.htm but does not have it",
    )


def test_a_block_by_the_sec_stops_the_source():
    with pytest.raises(QuotaExhaustedError, match="check that SEC_EDGAR_UA"):
        fetch(lambda _request: httpx.Response(403), [Request(company())])


def test_an_unexpected_list_of_filings_is_a_source_error():
    def handler(request):
        if request.url.path == "/files/company_tickers.json":
            return httpx.Response(200, json=TICKERS)
        return httpx.Response(200, json={"name": "Apple Inc.", "filings": {}})

    (batch,) = fetch(handler, [Request(company())])
    assert batch.failures[0].outcome is Outcome.SOURCE_ERROR


# -- through the store -------------------------------------------------------------------------


@pytest.fixture
def sec():
    return Sec()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, sec, clock, monkeypatch):
    def filings(http, credentials):
        return SecFilings(http, credentials, today=TODAY.replace)

    monkeypatch.setitem(source_registry.REGISTRY, "sec_filings", filings)
    (tmp_path / "catalog.yaml").write_text("- source: sec_filings\n  ids: [AAPL]\n", encoding="utf-8")
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


def test_first_sync_writes_the_files_and_the_list(store, tmp_path):
    report = store.report.sources[0]
    assert (report.ok, report.new, report.revised, report.calls) == (1, 4, 0, 7)
    folder = tmp_path / "store" / "documents" / "sec_filings" / "AAPL"
    assert (folder / ANNUAL / "aapl-20250927.htm").read_bytes() == b"<html>annual report</html>"
    assert (folder / EVENT / "a8-kex991q1.htm").read_bytes() == b"<html>earnings release</html>"
    assert sorted(path.name for path in folder.iterdir()) == [ANNUAL, EVENT, AMENDED]
    assert (folder.parent / "AAPL.parquet").exists()


def test_documents_lists_every_file_with_its_path(store, tmp_path):
    listed = store.documents("sec_filings", "AAPL")
    assert list(listed.columns) == [
        "id", "group", "date", "file", "role", "url", "size", "sha256", "form", "period", "fetched_at", "path",
    ]  # fmt: skip
    assert list(zip(listed["form"], listed["role"], listed["file"], strict=True)) == [
        ("10-K", "primary", "aapl-20250927.htm"),
        ("8-K", "exhibit", "a8-kex991q1.htm"),
        ("8-K", "primary", "aapl-8k.htm"),
        ("10-K/A", "primary", "aapl-10ka.htm"),
    ]
    first = listed.iloc[0]
    assert (first["id"], first["group"], first["period"]) == ("AAPL", ANNUAL, "2025-09-27")
    assert first["sha256"] == hashlib.sha256(b"<html>annual report</html>").hexdigest()
    assert first["size"] == len(b"<html>annual report</html>")
    assert pathlib.Path(first["path"]).read_bytes() == b"<html>annual report</html>"
    assert len(store.documents("sec_filings")) == 4


def test_filters_and_as_of(store):
    assert list(store.documents("sec_filings", "AAPL", form="8-K", role="exhibit")["file"]) == ["a8-kex991q1.htm"]
    assert list(store.documents("sec_filings", "AAPL", as_of="2026-01-28")["form"]) == ["10-K"]
    assert len(store.documents("sec_filings", "AAPL", as_of="2026-01-29")) == 3
    assert store.documents("sec_filings", "AAPL", as_of="2020-01-01").empty
    with pytest.raises(StoreError, match="unknown column\\(s\\) for sec_filings documents: kind"):
        store.documents("sec_filings", "AAPL", kind="10-K")


def test_a_filing_counts_as_known_from_the_end_of_its_day(store):
    # the SEC gives the day it received a filing, not the hour, as for the XBRL facts it carries
    assert list(store.documents("sec_filings", "AAPL", as_of="2026-01-29T12:00:00+00:00")["form"]) == ["10-K"]
    assert len(store.documents("sec_filings", "AAPL", as_of="2026-01-29T23:59:59.999999+00:00")) == 3


def test_unknown_documents_are_an_error(store):
    with pytest.raises(UnknownSeriesError, match="no stored documents of source 'sec_filings' have id 'MSFT'"):
        store.documents("sec_filings", "MSFT")
    with pytest.raises(UnknownSeriesError, match="no documents are stored for source 'fred'"):
        store.documents("fred")


def test_a_second_sync_downloads_nothing(store, sec, clock):
    before = len(sec.seen)
    clock["now"] = NOW + datetime.timedelta(days=1)
    report = store.sync().sources[0]
    assert (report.ok, report.new, report.calls) == (1, 0, 2)
    assert [request.url.path.split("/")[-1] for request in sec.seen[before:]] == [
        "company_tickers.json",
        f"CIK{CIK}.json",
    ]
    assert len(store.documents("sec_filings", "AAPL")) == 4


def test_full_does_not_download_a_stored_filing_again(store, sec):
    before = len(sec.seen)
    report = store.sync(full=True).sources[0]
    assert (report.new, len(sec.seen) - before) == (0, 2)


def test_a_new_filing_is_added_and_the_old_files_stay(store, sec, clock, tmp_path):
    newer = "0000320193-26-000040"
    quarter = (newer, "10-Q", "2026-05-01", "2026-03-28", "aapl-20260328.htm")
    sec.recent = page(quarter, *zip(*RECENT.values(), strict=True))
    sec.files["/Archives/edgar/data/320193/000032019326000040/aapl-20260328.htm"] = b"<html>quarter</html>"
    sec.files["/Archives/edgar/data/320193/000032019325000079/aapl-20250927.htm"] = b"<html>changed at the SEC</html>"
    clock["now"] = NOW + datetime.timedelta(days=1)
    report = store.sync().sources[0]
    assert (report.new, report.calls) == (1, 3)
    listed = store.documents("sec_filings", "AAPL")
    assert listed.iloc[-1]["group"] == newer
    folder = tmp_path / "store" / "documents" / "sec_filings" / "AAPL"
    assert (folder / ANNUAL / "aapl-20250927.htm").read_bytes() == b"<html>annual report</html>"
    assert store.info("sec_filings:AAPL").last_period == "2026-05-01"


def test_a_run_with_a_failed_filing_keeps_the_others_and_the_next_run_brings_it(tmp_path, sec, clock, monkeypatch):
    def filings(http, credentials):
        return SecFilings(http, credentials, today=TODAY.replace)

    monkeypatch.setitem(source_registry.REGISTRY, "sec_filings", filings)
    (tmp_path / ".env").write_text(f"SEC_EDGAR_UA={AGENT}\n", encoding="utf-8")
    store = Store(
        tmp_path / "store",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(sec),
        sleep=lambda _seconds: None,
    )
    store.add("sec_filings", ["AAPL"])
    sec.fail = "a8-kex991q1.htm"
    report = store.sync()
    assert report.exit_code == 1
    assert report.sources[0].new == 2
    assert list(store.documents("sec_filings", "AAPL")["group"]) == [ANNUAL, AMENDED]
    info = store.info("sec_filings:AAPL")
    assert (info.status, info.kind) == ("failed", "document")
    assert info.reason.startswith(f"network_error: {EVENT}: ")
    sec.fail = None
    report = store.sync()
    assert (report.exit_code, report.sources[0].new) == (0, 2)
    assert len(store.documents("sec_filings", "AAPL")) == 4


def test_the_index_describes_the_company(store):
    info = store.info("sec_filings:AAPL")
    assert (info.kind, info.name, info.last_period) == ("document", "Apple Inc.", "2026-02-10")
    assert info.label == "[SEC EDGAR: AAPL, 2026-02-10, fetched 2026-06-06]"
    row = store.index().iloc[0]
    assert json.loads(row["attrs"]) == {"cik": CIK}
    assert row["stale_after_days"] == 140


def test_status_is_stale_after_140_days_without_a_filing(store, clock):
    assert store.status().iloc[0]["state"] == "ok"  # 116 days after 2026-02-10
    clock["now"] = datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC)
    assert store.status().iloc[0]["reason"] == "last data 2026-02-10 (141 days ago)"


def test_series_and_table_refuse_a_set_of_documents(store):
    message = "sec_filings:AAPL is a set of documents: read it with documents\\('sec_filings', 'AAPL'\\)"
    with pytest.raises(StoreError, match=message):
        store.series("sec_filings:AAPL")
    with pytest.raises(UnknownSeriesError, match="no stored table of source 'sec_filings' has id 'AAPL'"):
        store.table("sec_filings", "AAPL")


def test_show_prints_the_citation_and_the_newest_files(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    result = click.testing.CliRunner().invoke(cli, ["show", "--root", str(tmp_path / "store"), "sec_filings:AAPL"])
    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "[SEC EDGAR: AAPL, 2026-02-10, fetched 2026-06-06]",
        "Apple Inc. | 4 files",
        "group  date  file  role  form  period",
        f"{ANNUAL}  2025-10-31  aapl-20250927.htm  primary  10-K  2025-09-27",
        f"{EVENT}  2026-01-29  a8-kex991q1.htm  exhibit  8-K  2026-01-29",
        f"{EVENT}  2026-01-29  aapl-8k.htm  primary  8-K  2026-01-29",
        f"{AMENDED}  2026-02-10  aapl-10ka.htm  primary  10-K/A  2025-09-27",
    ]
