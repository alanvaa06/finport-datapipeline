import dataclasses
import datetime
import json
import math
import pickle
import subprocess
import sys

import duckdb
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

    def __init__(self, versioned=False, values=("close",)):
        self.versioned = versioned
        self.values = values
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
            value_columns=self.values,
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
        ("out.xlsx", "cannot write 'out.xlsx': the file must end in .parquet, .csv or .duckdb"),
        ("nowhere/out.csv", "the folder .*nowhere.* does not exist"),
    ],
)
def test_a_file_that_cannot_be_written_is_refused_before_any_request(monkeypatch, tmp_path, name, message):
    built = install(monkeypatch, FakeSource())
    with pytest.raises(CatalogError, match=message):
        dp.fetch("fake", ["AAA"], to=tmp_path / name)
    assert built == []


# -- a DuckDB file -----------------------------------------------------------------------------


def read_duckdb(path, table="fake"):
    """Every row of `table`, in the order stored, read with times in UTC."""
    with duckdb.connect(str(path), read_only=True, config={"TimeZone": "UTC"}) as connection:
        return connection.table(table).df()


def install_tables(monkeypatch, rows_by_id):
    fake = FakeTables()
    fake.answers = {name: [FetchBatch(tables=(fake.table(name, rows),))] for name, rows in rows_by_id.items()}
    install(monkeypatch, fake)
    return fake


def test_to_duckdb_writes_the_frame_it_returns_to_a_table_named_after_the_source(two_series, tmp_path):
    path = tmp_path / "out.duckdb"
    frame = dp.fetch("fake", ["AAA", "BBB"], to=path)
    pd.testing.assert_frame_equal(read_duckdb(path), frame)


def test_a_second_fetch_keeps_the_rows_of_other_ids_and_replaces_the_rows_of_its_own(two_series, tmp_path):
    path = tmp_path / "out.duckdb"
    dp.fetch("fake", ["AAA", "BBB"], to=path)
    two_series.answers["AAA"] = monthly(entry("AAA"), {"2026-03": 9.0})
    dp.fetch("fake", ["AAA"], to=path)
    stored = read_duckdb(path)
    assert list(zip(stored["id"], stored["period"], stored["value"], strict=True)) == [
        ("BBB", "2026-01", 3.0),
        ("AAA", "2026-03", 9.0),
    ]


def test_a_new_column_is_added_and_a_column_the_frame_lacks_stays_empty(monkeypatch, tmp_path):
    path = tmp_path / "out.duckdb"
    plain = FakeTables()
    plain.answers = {"XAAA": [FetchBatch(tables=(plain.table("XAAA", [day("XAAA", "2026-03-03", 1.0)]),))]}
    wider = FakeTables(values=("close", "open"))
    row = {**day("XBBB", "2026-03-03", 2.0), "open": 7.0}
    wider.answers = {"XBBB": [FetchBatch(tables=(wider.table("XBBB", [row]),))]}
    install(monkeypatch, plain)
    dp.fetch("fake", ["XAAA"], to=path)
    install(monkeypatch, wider)
    dp.fetch("fake", ["XBBB"], to=path)
    install(monkeypatch, plain)
    dp.fetch("fake", ["XAAA"], to=path)  # the source no longer sends `open`
    stored = read_duckdb(path)
    assert list(stored.columns) == ["id", "ticker", "period", "date", "close", "note", "open"]
    assert list(stored["id"]) == ["XBBB", "XAAA"]
    assert stored["open"].isna().tolist() == [False, True]


def test_text_missing_values_and_times_read_back_as_they_were_written(tmp_path):
    frame = pd.DataFrame(
        {
            "id": ["XAAA", "XBBB"],
            "date": pd.to_datetime(["2026-03-03", "2026-03-04"]).as_unit("ns"),
            "value": [1.5, math.nan],
            "note": ["café", None],
            "empty": [None, None],
            "fetched_at": pd.Series(pd.to_datetime(["2026-06-06T12:00:00.123456Z"] * 2)).dt.as_unit("ns"),
        }
    )
    path = tmp_path / "out.duckdb"
    fetching.destination(path).write(frame, table="rows", by_id=True)
    stored = read_duckdb(path, "rows")
    assert stored["value"].isna().tolist() == [False, True]
    assert stored["note"].tolist() == ["café", None]
    assert str(stored["fetched_at"].dt.tz) == "UTC"
    pd.testing.assert_frame_equal(stored, frame, check_dtype=False)


def test_a_column_that_came_without_any_value_takes_text_later(monkeypatch, tmp_path):
    path = tmp_path / "out.duckdb"
    fake = install_tables(monkeypatch, {"XAAA": [day("XAAA", "2026-03-03", 1.0, note=None)]})
    dp.fetch("fake", ["XAAA"], to=path)
    fake.answers["XBBB"] = [FetchBatch(tables=(fake.table("XBBB", [day("XBBB", "2026-03-03", 2.0, "late")]),))]
    dp.fetch("fake", ["XBBB"], to=path)
    dp.fetch("fake", ["XAAA"], to=path)  # without any value again
    assert read_duckdb(path)["note"].tolist() == ["late", None]


def test_a_table_named_in_other_case_is_the_same_table_and_keeps_its_rows(two_series, tmp_path):
    path = tmp_path / "out.duckdb"
    with duckdb.connect(str(path)) as connection:  # DuckDB does not tell FAKE from fake
        connection.execute('CREATE TABLE FAKE ("ID" VARCHAR, "VALUE" DOUBLE)')
        connection.execute("INSERT INTO FAKE VALUES ('BBB', 3.5)")
    dp.fetch("fake", ["AAA"], to=path)
    stored = read_duckdb(path)
    assert list(stored.columns) == ["ID", "VALUE", "date", "period"]
    assert list(zip(stored["ID"], stored["VALUE"], strict=True)) == [("BBB", 3.5), ("AAA", 1.0), ("AAA", 2.0)]


def test_a_failed_fetch_writes_what_arrived_and_keeps_the_rows_of_the_ids_that_failed(two_series, tmp_path):
    path = tmp_path / "out.duckdb"
    dp.fetch("fake", ["AAA", "BBB"], to=path)
    two_series.answers["AAA"] = monthly(entry("AAA"), {"2026-03": 9.0})
    del two_series.answers["BBB"]
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA", "BBB"], to=path)
    assert caught.value.written == path
    stored = read_duckdb(path)
    assert list(zip(stored["id"], stored["period"], strict=True)) == [("BBB", "2026-01"), ("AAA", "2026-03")]


def test_a_fetch_that_brought_nothing_does_not_touch_the_duckdb_file(two_series, tmp_path):
    path = tmp_path / "out.duckdb"
    dp.fetch("fake", ["AAA"], to=path)
    before = path.read_bytes()
    two_series.answers.clear()
    for target in (path, tmp_path / "new.duckdb"):
        with pytest.raises(dp.FetchError):
            dp.fetch("fake", ["AAA"], to=target)
    assert path.read_bytes() == before
    assert not (tmp_path / "new.duckdb").exists()


def test_a_write_that_fails_halfway_leaves_the_file_as_it_was(two_series, tmp_path):
    path = tmp_path / "out.duckdb"
    with duckdb.connect(str(path)) as connection:  # a table of that name whose `value` holds days
        connection.execute("CREATE TABLE fake (id VARCHAR, value DATE)")
        connection.execute("INSERT INTO fake VALUES ('AAA', DATE '2020-01-01')")
    before = path.read_bytes()
    with pytest.raises(dp.FetchError) as caught:
        dp.fetch("fake", ["AAA"], to=path)
    assert caught.value.written is None
    assert caught.value.write_error.startswith("ConversionException: ")
    assert list(caught.value.data["id"]) == ["AAA", "AAA"]
    assert path.read_bytes() == before
    stored = read_duckdb(path)
    assert list(stored.columns) == ["id", "value"]  # the columns added in the transaction are gone too
    assert stored["id"].tolist() == ["AAA"]


def test_a_duckdb_file_another_program_holds_is_a_fetch_error_with_the_rows(two_series, tmp_path):
    path = tmp_path / "out.duckdb"
    dp.fetch("fake", ["BBB"], to=path)
    before = path.read_bytes()
    hold = "import duckdb, sys; connection = duckdb.connect(sys.argv[1]); print('ready', flush=True); sys.stdin.read()"
    holder = subprocess.Popen(  # another program with the file open
        [sys.executable, "-c", hold, str(path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        with pytest.raises(dp.FetchError) as caught:
            dp.fetch("fake", ["AAA", "BBB"], to=path)
    finally:
        holder.communicate()
    error = caught.value
    assert list(error.data["id"]) == ["AAA", "AAA", "BBB"]
    assert error.written is None
    assert isinstance(error.__cause__, duckdb.IOException)
    assert error.write_error.startswith("IOException: ")
    assert "\n" not in error.write_error
    assert str(error).startswith(f"fake: could not write {path}: IOException: ")
    assert path.read_bytes() == before


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
