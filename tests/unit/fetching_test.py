import dataclasses
import datetime
import json
import math
import pickle

import httpx
import pandas as pd
import pytest

import data_pipeline as dp
from data_pipeline import credentials as keys
from data_pipeline import fetching
from data_pipeline.store import sources
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
    TableData,
)

from .store.helpers import NOW, FakeSource, fixture, monthly

# Every id, name and number here is invented.


def entry(source_id, source="fake"):
    return CatalogEntry(source=source, source_id=source_id)


def install(monkeypatch, source, name="fake"):
    """Put `source` in the registry under `name`; returns the list of the times it was built."""
    built = []

    def factory(client, credentials):
        built.append(credentials)
        return source

    monkeypatch.setitem(sources.REGISTRY, name, factory)
    return built


def refuse(request):
    msg = f"no request was expected, got {request.url}"
    raise AssertionError(msg)


NO_NETWORK = httpx.MockTransport(refuse)


class FakeTables:
    """A table source that answers from a dictionary: source_id -> list of batches."""

    name = "fake"
    kind = Kind.TABLE
    requests_per_minute = 6000
    daily_budget = None
    held_by = ()

    def __init__(self, versioned=False):
        self.versioned = versioned
        self.answers: dict[str, list[FetchBatch]] = {}

    def validate(self, catalog_entry):
        pass

    def table(self, source_id, rows):
        return TableData(
            entry=entry(source_id),
            key=f"fake:{source_id}",
            name=f"Table of {source_id}",
            rows=tuple(rows),
            key_columns=("ticker", "period"),
            value_columns=("close",),
            attribute_columns=("note",),
            versioned=self.versioned,
        )

    def fetch(self, requests):
        for request in requests:
            yield from self.answers.get(request.entry.source_id, [])


def day(ticker, period, close, note="", published_at=None):
    date = datetime.date.fromisoformat(period)
    row = {"ticker": ticker, "period": period, "date": date, "close": close, "note": note}
    if published_at is not None:
        row["published_at"] = published_at
    return row


# -- series ------------------------------------------------------------------------------------


def test_a_series_comes_long_one_row_per_id_and_period_in_the_order_asked(monkeypatch):
    fake = FakeSource()
    fake.answers = {
        "BBB": monthly(entry("BBB"), {"2026-01": 1.0, "2026-02": 2.0}),
        "AAA": monthly(entry("AAA"), {"2026-01": 3.0}),
    }
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["BBB", "AAA"])
    assert list(frame.columns) == ["id", "date", "period", "value"]
    assert frame.to_dict("list") == {
        "id": ["BBB", "BBB", "AAA"],
        "date": [pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28"), pd.Timestamp("2026-01-31")],
        "period": ["2026-01", "2026-02", "2026-01"],
        "value": [1.0, 2.0, 3.0],
    }


def test_a_series_leaves_out_missing_values_and_projections(monkeypatch):
    fake = FakeSource()
    series = monthly(entry("AAA"), {"2026-01": 1.0, "2026-02": math.nan})
    later = Observation("2200-01", datetime.date(2200, 1, 31), 9.0)  # a period still open is a projection
    projected = Observation("2026-03", datetime.date(2026, 3, 31), 8.0, projection=True)
    fake.answers = {"AAA": dataclasses.replace(series, observations=(*series.observations, projected, later))}
    install(monkeypatch, fake)
    assert list(dp.fetch("fake", "AAA")["period"]) == ["2026-01"]


def test_a_series_gives_the_latest_version_of_each_period(monkeypatch):
    fake = FakeSource()
    first = datetime.datetime(2026, 2, 5, tzinfo=datetime.UTC)
    revised = datetime.datetime(2026, 3, 5, tzinfo=datetime.UTC)
    observations = (
        Observation("2026-01", datetime.date(2026, 1, 31), 5.0, published_at=revised),
        Observation("2026-01", datetime.date(2026, 1, 31), 4.0, published_at=first),
    )
    fake.answers = {"AAA": SeriesData(entry("AAA"), "fake:AAA", "A", Frequency.MONTHLY, observations=observations)}
    install(monkeypatch, fake)
    assert list(dp.fetch("fake", ["AAA"])["value"]) == [5.0]


def test_an_id_asked_twice_is_asked_once(monkeypatch):
    fake = FakeSource()
    fake.answers = {"AAA": monthly(entry("AAA"), {"2026-01": 1.0})}
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["AAA", "AAA"])
    assert [request.entry.source_id for request in fake.seen] == ["AAA"]
    assert len(frame) == 1


def test_the_request_carries_no_since_and_the_start_given_as_text():
    seen = []

    class Recording(FakeSource):
        def fetch(self, requests):
            seen.extend(requests)
            return iter(())

    with pytest.MonkeyPatch.context() as monkeypatch:
        install(monkeypatch, Recording())
        with pytest.raises(dp.FetchError):
            dp.fetch("fake", ["AAA"], start="2020-01-01")
    (request,) = seen
    assert request == Request(CatalogEntry("fake", "AAA", start=datetime.date(2020, 1, 1)))


def test_a_start_that_is_not_a_date_is_refused_before_any_request(monkeypatch):
    built = install(monkeypatch, FakeSource())
    with pytest.raises(CatalogError, match="'start' must be a date such as 1990-01-01, not '2020-13-01'"):
        dp.fetch("fake", ["AAA"], start="2020-13-01")
    assert built == []


def test_a_field_the_source_does_not_accept_is_refused_before_any_request(monkeypatch):
    fake = FakeSource()
    install(monkeypatch, fake)
    with pytest.raises(CatalogError, match="unknown field\\(s\\) for source 'fake': colour"):
        dp.fetch("fake", ["AAA"], colour="red")
    assert fake.seen == []


def test_an_unknown_source_is_refused_with_the_list_of_known_ones():
    with pytest.raises(CatalogError, match=r"unknown source 'nope' \(known sources: .*fred"):
        dp.fetch("nope", ["AAA"], transport=NO_NETWORK)


def test_a_source_of_documents_is_refused_before_any_request():
    with pytest.raises(CatalogError, match="sec_filings is a source of documents: fetch returns data frames"):
        dp.fetch("sec_filings", ["AAPL"], transport=NO_NETWORK)


def test_no_ids_is_refused():
    with pytest.raises(CatalogError, match="'ids' must be a non-empty list"):
        dp.fetch("fred", [], transport=NO_NETWORK)


def test_a_missing_key_fails_every_id_without_a_request():
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fred", ["UNRATE", "DGS10"], transport=NO_NETWORK)
    assert [failure.outcome for failure in caught.value.failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert keys.missing_message(keys.FRED) in str(caught.value)
    assert caught.value.data.empty
    assert list(caught.value.data.columns) == ["id", "date", "period", "value"]


# -- failures ----------------------------------------------------------------------------------


def test_a_failed_id_raises_with_what_arrived_and_one_line_per_failure(monkeypatch):
    fake = FakeSource()
    fake.answers = {
        "AAA": monthly(entry("AAA"), {"2026-01": 1.0}),
        "BAD": Failure(entry("BAD"), Outcome.NOT_FOUND, "no such series"),
    }
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BAD"])
    error = caught.value
    assert list(error.data["id"]) == ["AAA"]
    assert [(failure.entry.key, failure.outcome) for failure in error.failures] == [("fake:BAD", Outcome.NOT_FOUND)]
    assert not error.quota_exhausted
    assert str(error).splitlines() == [
        "fake: 1 of 2 ids failed; the 1 rows that arrived are in .data",
        "fake:BAD  not_found: no such series",
    ]


def test_an_id_the_source_never_returned_is_not_found(monkeypatch):
    install(monkeypatch, FakeSource())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA"])
    (failure,) = caught.value.failures
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "the source did not return this id")
    assert str(caught.value).splitlines()[0] == "fake: 1 of 1 ids failed; nothing arrived"


def test_a_quota_fails_the_ids_not_yet_fetched_and_keeps_the_others(monkeypatch):
    fake = FakeSource(quota_after=1)
    fake.answers = {"AAA": monthly(entry("AAA"), {"2026-01": 1.0}), "BBB": monthly(entry("BBB"), {"2026-01": 2.0})}
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BBB"])
    assert caught.value.quota_exhausted
    assert list(caught.value.data["id"]) == ["AAA"]
    (failure,) = caught.value.failures
    assert failure.entry.source_id == "BBB"
    assert failure.outcome is Outcome.QUOTA_EXHAUSTED
    assert failure.reason == "the source's quota ran out before this id was fetched: quota used up"


def test_a_source_that_breaks_fails_the_ids_it_did_not_finish(monkeypatch):
    class Breaking(FakeSource):
        def fetch(self, requests):
            yield FetchBatch(series=(monthly(entry("AAA"), {"2026-01": 1.0}),))
            msg = "the answer made no sense"
            raise RuntimeError(msg)

    install(monkeypatch, Breaking())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BBB"])
    (failure,) = caught.value.failures
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason == "the download stopped: RuntimeError: the answer made no sense"
    assert list(caught.value.data["id"]) == ["AAA"]


def test_a_reason_never_shows_a_key(monkeypatch):
    secret = "abcdef0123456789abcdef0123456789"  # noqa: S105 - an invented key
    fake = FakeSource()
    fake.answers = {"BAD": Failure(entry("BAD"), Outcome.SOURCE_ERROR, f"refused api_key={secret}")}
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["BAD"], credentials={keys.FRED: secret})
    assert secret not in str(caught.value)
    assert secret not in caught.value.failures[0].reason


def test_a_quota_raised_before_anything_arrived_is_a_fetch_error(monkeypatch):
    class Exhausted(FakeSource):
        def fetch(self, requests):
            msg = "429 on every attempt"
            raise QuotaExhaustedError(msg)
            yield  # a generator, as every source is

    install(monkeypatch, Exhausted())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA"])
    assert caught.value.quota_exhausted
    assert caught.value.failures[0].outcome is Outcome.QUOTA_EXHAUSTED


# -- tables ------------------------------------------------------------------------------------


def test_a_table_comes_with_its_id_first_and_the_columns_of_store_table(monkeypatch):
    fake = FakeTables()
    fake.answers = {
        "XBBB": [FetchBatch(tables=(fake.table("XBBB", [day("XBBB", "2026-03-03", 2.0)]),))],
        "XAAA": [
            # one key in two batches, newest day first
            FetchBatch(tables=(fake.table("XAAA", [day("XAAA", "2026-03-04", 1.5, "late")]),)),
            FetchBatch(tables=(fake.table("XAAA", [day("XAAA", "2026-03-03", 1.0)]),)),
        ],
    }
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["XBBB", "XAAA"])
    assert list(frame.columns) == ["id", "ticker", "period", "date", "close", "note"]
    assert list(zip(frame["id"], frame["period"], frame["close"], strict=True)) == [
        ("XBBB", "2026-03-03", 2.0),
        ("XAAA", "2026-03-03", 1.0),
        ("XAAA", "2026-03-04", 1.5),
    ]


def test_a_versioned_table_gives_the_version_published_last(monkeypatch):
    fake = FakeTables(versioned=True)
    first = datetime.datetime(2026, 3, 5, tzinfo=datetime.UTC)
    restated = datetime.datetime(2026, 9, 5, tzinfo=datetime.UTC)
    rows = [day("XAAA", "2026-03-03", 9.0, "restated", restated), day("XAAA", "2026-03-03", 1.0, "", first)]
    fake.answers = {"XAAA": [FetchBatch(tables=(fake.table("XAAA", rows),))]}
    install(monkeypatch, fake)
    frame = dp.fetch("fake", ["XAAA"])
    assert list(zip(frame["close"], frame["note"], strict=True)) == [(9.0, "restated")]


def test_a_table_id_can_bring_rows_and_fail(monkeypatch):
    fake = FakeTables()
    fake.answers = {
        "XAAA": [
            FetchBatch(tables=(fake.table("XAAA", [day("XAAA", "2026-03-03", 1.0)]),)),
            FetchBatch(failures=(Failure(entry("XAAA"), Outcome.NETWORK_ERROR, "timed out"),)),
        ]
    }
    install(monkeypatch, fake)
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["XAAA"])
    assert list(caught.value.data["close"]) == [1.0]
    assert caught.value.failures[0].outcome is Outcome.NETWORK_ERROR


def test_a_table_source_that_brought_nothing_gives_an_id_column_only(monkeypatch):
    install(monkeypatch, FakeTables())
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["XAAA"])
    assert list(caught.value.data.columns) == ["id"]
    assert caught.value.data.empty


# -- files -------------------------------------------------------------------------------------


@pytest.fixture
def two_series(monkeypatch):
    fake = FakeSource()
    fake.answers = {
        "AAA": monthly(entry("AAA"), {"2026-01": 1.0, "2026-02": 2.0}),
        "BBB": monthly(entry("BBB"), {"2026-01": 3.0}),
    }
    install(monkeypatch, fake)
    return fake


def test_to_parquet_writes_the_frame_it_returns(two_series, tmp_path):
    path = tmp_path / "out.parquet"
    frame = dp.fetch("fake", ["AAA", "BBB"], to=path)
    pd.testing.assert_frame_equal(pd.read_parquet(path), frame)


def test_to_csv_writes_the_frame_it_returns(two_series, tmp_path):
    path = tmp_path / "out.csv"
    frame = dp.fetch("fake", ["AAA", "BBB"], to=str(path))
    assert path.read_text(encoding="utf-8").splitlines()[:2] == ["id,date,period,value", "AAA,2026-01-31,2026-01,1.0"]
    pd.testing.assert_frame_equal(pd.read_csv(path, parse_dates=["date"]), frame)


def test_a_file_of_that_name_is_replaced(two_series, tmp_path):
    path = tmp_path / "out.csv"
    path.write_text("old content\n", encoding="utf-8")
    dp.fetch("fake", ["AAA"], to=path)
    assert path.read_text(encoding="utf-8").startswith("id,date,period,value\n")
    assert [item.name for item in tmp_path.iterdir()] == ["out.csv"]  # no temporary file left


def test_a_failed_fetch_writes_what_arrived_then_raises(two_series, tmp_path):
    path = tmp_path / "out.parquet"
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "MISSING"], to=path)
    pd.testing.assert_frame_equal(pd.read_parquet(path), caught.value.data)
    assert caught.value.written == path


def test_a_failed_fetch_that_brought_nothing_leaves_the_file_as_it_was(monkeypatch, tmp_path):
    install(monkeypatch, FakeSource())
    path = tmp_path / "out.parquet"
    path.write_bytes(b"yesterday's rows")
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA"], to=path)
    assert path.read_bytes() == b"yesterday's rows"
    assert caught.value.written is None
    assert f"nothing was written to {path}" in str(caught.value)


def test_a_fetch_that_worked_and_brought_no_rows_writes_its_empty_frame(monkeypatch, tmp_path):
    fake = FakeSource()
    fake.answers = {"AAA": monthly(entry("AAA"), {"2026-01": math.nan})}
    install(monkeypatch, fake)
    path = tmp_path / "out.parquet"
    path.write_bytes(b"yesterday's rows")
    assert dp.fetch("fake", ["AAA"], to=path).empty
    assert pd.read_parquet(path).empty


def test_a_file_it_cannot_write_raises_with_the_rows_that_arrived(two_series, tmp_path, monkeypatch):
    def refused(path, content):
        msg = f"{path} is open in another program"
        raise PermissionError(msg)

    monkeypatch.setattr(fetching._files, "write_atomic", refused)
    path = tmp_path / "out.csv"
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BBB"], to=path)
    error = caught.value
    assert list(error.data["id"]) == ["AAA", "AAA", "BBB"]
    assert error.failures == ()
    assert error.written is None
    assert isinstance(error.__cause__, PermissionError)
    assert str(error).splitlines() == [
        f"fake: could not write {path}: PermissionError: {path} is open in another program; "
        "the 3 rows that arrived are in .data"
    ]


def test_a_fetch_error_survives_pickling(two_series, tmp_path):
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "MISSING"], to=tmp_path / "out.parquet")
    copy = pickle.loads(pickle.dumps(caught.value))  # noqa: S301 - our own object, made a line above
    assert str(copy) == str(caught.value)
    pd.testing.assert_frame_equal(copy.data, caught.value.data)
    assert copy.failures == caught.value.failures
    assert (copy.quota_exhausted, copy.written) == (caught.value.quota_exhausted, caught.value.written)


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("out.xlsx", "cannot write 'out.xlsx': the file must end in .parquet or .csv"),
        ("nowhere/out.csv", "the folder .*nowhere.* does not exist"),
    ],
)
def test_a_file_that_cannot_be_written_is_refused_before_any_request(monkeypatch, tmp_path, name, message):
    built = install(monkeypatch, FakeSource())
    with pytest.raises(CatalogError, match=message):
        dp.fetch("fake", ["AAA"], to=tmp_path / name)
    assert built == []


# -- the same as the store ---------------------------------------------------------------------

SHARADAR_KEY = "sharadar-key-of-the-tests"
AGENT = "Jane Doe jane@example.com"


def sharadar(request):
    rows = json.loads(fixture("sharadar_prices.json"))["data"] if request.url.params["ticker"] == "XTEST" else []
    return httpx.Response(200, json={"count": len(rows), "data": rows})


def sec(request):
    if request.url.host == "www.sec.gov":
        return httpx.Response(200, json={"0": {"cik_str": 999, "ticker": "XTEST", "title": "Test Corp"}})

    def appearance(value, filed, accession):
        return {
            "start": "2025-01-01",
            "end": "2025-12-31",
            "val": value,
            "accn": accession,
            "fy": 2025,
            "fp": "FY",
            "form": "10-K",
            "filed": filed,
        }

    revenues = [
        appearance(100, "2026-02-01", "0000000999-26-000001"),
        appearance(90, "2026-08-01", "0000000999-26-000002"),  # restated by a later filing
    ]
    facts = {"us-gaap": {"Revenues": {"label": "Revenues", "units": {"USD": revenues}}}}
    return httpx.Response(200, json={"cik": 999, "entityName": "Test Corp", "facts": facts})


def fred(request):
    name = "fred_observations.json" if request.url.path.endswith("/observations") else "fred_series.json"
    return httpx.Response(200, text=fixture(name))


def stored(tmp_path, source, source_id, handler, credentials):
    store = Store(
        tmp_path / "store",
        credentials=credentials,
        clock=lambda: NOW,
        transport=httpx.MockTransport(handler),
        sleep=lambda _seconds: None,
    )
    store.add(source, [source_id])
    assert store.sync().exit_code == 0
    return store


@pytest.mark.parametrize(
    ("source", "handler", "credentials"),
    [("sharadar_prices", sharadar, {keys.SHARADAR: SHARADAR_KEY}), ("sec_xbrl", sec, {keys.SEC_UA: AGENT})],
)
def test_a_table_reads_as_store_table_reads_it(tmp_path, source, handler, credentials):
    store = stored(tmp_path, source, "XTEST", handler, credentials)
    fetched = dp.fetch(
        source, ["XTEST"], credentials=credentials, transport=httpx.MockTransport(handler), sleep=lambda _s: None
    )
    assert list(fetched["id"].unique()) == ["XTEST"]
    pd.testing.assert_frame_equal(fetched.drop(columns="id"), store.table(source, "XTEST"))


def test_the_versioned_table_keeps_only_the_restated_value(tmp_path):
    fetched = dp.fetch(
        "sec_xbrl", "XTEST", credentials={keys.SEC_UA: AGENT}, transport=httpx.MockTransport(sec), sleep=lambda _s: None
    )
    assert list(fetched["value"]) == [90.0]


def test_a_series_reads_as_store_series_reads_it(tmp_path):
    credentials = {keys.FRED: "0123456789abcdef0123456789abcdef"}
    store = stored(tmp_path, "fred", "UNRATE", fred, credentials)
    fetched = dp.fetch(
        "fred", ["UNRATE"], credentials=credentials, transport=httpx.MockTransport(fred), sleep=lambda _s: None
    )
    pd.testing.assert_frame_equal(fetched[["date", "period", "value"]], store.series("fred:UNRATE"))
    assert list(fetched["id"].unique()) == ["UNRATE"]
