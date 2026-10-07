import datetime
import math

import pytest

from data_pipeline.store.errors import CatalogError, LockHeldError
from data_pipeline.store.model import Failure, Frequency, Observation, Outcome, SeriesData
from data_pipeline.store.storage import Storage, latest
from data_pipeline.store.sync import LOCK_FILE, NOT_RETURNED, SourceReport, SyncReport, sync, window_start

from .helpers import NOW, FakeSource, client, entry, monthly

LATER = NOW + datetime.timedelta(days=28)
NEXT_DAY = NOW + datetime.timedelta(days=1)
UNRATE = entry("UNRATE")
DGS10 = entry("DGS10")


def run(tmp_path, source, entries=(UNRATE,), now=NOW, http=None, **options):
    return sync(Storage(tmp_path), list(entries), {source.name: source}, http or client(), now, **options)


def stored_values(tmp_path, key="fake:UNRATE"):
    frame = latest(Storage(tmp_path).read_observations("fake"))
    frame = frame[frame["key"] == key]
    return dict(zip(frame["period"], frame["value"], strict=True))


def index_row(tmp_path, key="fake:UNRATE"):
    frame = Storage(tmp_path).read_index()
    return frame[frame["key"] == key].iloc[0]


@pytest.mark.parametrize(
    ("frequency", "last", "expected"),
    [
        (Frequency.DAILY, datetime.date(2026, 9, 25), datetime.date(2026, 8, 26)),
        (Frequency.WEEKLY, datetime.date(2026, 9, 25), datetime.date(2026, 6, 26)),
        (Frequency.MONTHLY, datetime.date(2026, 5, 31), datetime.date(2024, 5, 1)),
        (Frequency.QUARTERLY, datetime.date(2026, 6, 30), datetime.date(2023, 6, 1)),
        (Frequency.ANNUAL, datetime.date(2025, 12, 31), datetime.date(2020, 12, 1)),
    ],
)
def test_window_start(frequency, last, expected):
    assert window_start(last, frequency) == expected


def test_first_sync_asks_for_full_history_and_stores_it(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-04": 4.0, "2026-05": 4.1})
    report = run(tmp_path, source)
    assert source.seen[0].since is None
    assert stored_values(tmp_path) == {"2026-04": 4.0, "2026-05": 4.1}
    assert report.sources == (SourceReport("fake", 1, (), 2, 0, 0, False, 0),)
    assert report.exit_code == 0
    row = index_row(tmp_path)
    assert (row["status"], row["name"], row["frequency"], row["units"]) == ("ok", "Name of UNRATE", "M", "Percent")
    assert row["last_period"] == "2026-05"
    assert row["first_fetched_at"] == row["last_fetched_at"]


def test_a_declared_start_is_used_on_the_first_sync(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    started = entry("UNRATE", start=datetime.date(1990, 1, 1))
    run(tmp_path, source, entries=[started])
    assert source.seen[0].since == datetime.date(1990, 1, 1)


def test_second_sync_asks_from_the_revision_window_and_adds_nothing(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-04": 4.0, "2026-05": 4.1})
    run(tmp_path, source)
    report = run(tmp_path, source, now=LATER)
    assert source.seen[1].since == datetime.date(2024, 5, 1)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(Storage(tmp_path).read_observations("fake")) == 2
    row = index_row(tmp_path)
    assert row["first_fetched_at"] < row["last_fetched_at"]


def test_a_revision_adds_a_row_and_is_counted(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.2})
    report = run(tmp_path, source, now=LATER)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    assert list(Storage(tmp_path).read_observations("fake")["value"]) == [4.1, 4.2]
    assert stored_values(tmp_path) == {"2026-05": 4.2}


def test_full_asks_for_everything_again(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    run(tmp_path, source, now=LATER, full=True)
    assert source.seen[1].since is None


def test_an_open_period_is_stored_as_a_projection(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1, "2026-06": 4.3})
    run(tmp_path, source)
    frame = Storage(tmp_path).read_observations("fake")
    assert dict(zip(frame["period"], frame["projection"], strict=True)) == {"2026-05": False, "2026-06": True}
    assert index_row(tmp_path)["last_period"] == "2026-05"


def test_a_failure_is_recorded_and_the_other_series_continue(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = Failure(UNRATE, Outcome.NOT_FOUND, "The series does not exist.")
    source.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    report = run(tmp_path, source, entries=[UNRATE, DGS10])
    assert report.sources[0].ok == 1
    assert report.sources[0].failed == (("fake:UNRATE", "not_found: The series does not exist."),)
    assert report.exit_code == 1
    assert stored_values(tmp_path, "fake:DGS10") == {"2026-05": 4.4}
    row = index_row(tmp_path)
    assert (row["status"], row["reason"]) == ("failed", "not_found: The series does not exist.")


def test_a_series_that_fails_later_keeps_its_data(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    source.answers["UNRATE"] = Failure(UNRATE, Outcome.NETWORK_ERROR, "fake: HTTP 503 after 4 attempts")
    run(tmp_path, source, now=LATER)
    assert stored_values(tmp_path) == {"2026-05": 4.1}
    row = index_row(tmp_path)
    assert (row["status"], row["name"], row["last_period"]) == ("failed", "Name of UNRATE", "2026-05")


def test_a_series_the_source_does_not_return_is_a_failure(tmp_path):
    report = run(tmp_path, FakeSource())
    assert report.sources[0].failed == (("fake:UNRATE", f"not_found: {NOT_RETURNED}"),)


def test_quota_signalled_by_the_source_stops_it_and_the_next_run_resumes(tmp_path):
    source = FakeSource(quota_after=1)
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    source.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    report = run(tmp_path, source, entries=[UNRATE, DGS10])
    assert (report.sources[0].ok, report.sources[0].quota_exhausted, report.sources[0].pending) == (1, True, 1)
    assert report.sources[0].failed == ()
    assert report.exit_code == 3
    assert stored_values(tmp_path) == {"2026-05": 4.1}
    source.quota_after = None
    report = run(tmp_path, source, entries=[UNRATE, DGS10], now=NEXT_DAY)
    assert (report.sources[0].ok, report.exit_code) == (2, 0)
    assert stored_values(tmp_path, "fake:DGS10") == {"2026-05": 4.4}


def test_the_daily_budget_persists_between_runs_of_the_same_day(tmp_path):
    http = client()
    source = FakeSource(daily_budget=1, client=http)
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    source.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    first = run(tmp_path, source, entries=[UNRATE, DGS10], http=http)
    assert (first.sources[0].ok, first.sources[0].calls, first.sources[0].pending) == (1, 1, 1)
    assert Storage(tmp_path).read_runs()["fake"]["budget"] == {"day": "2026-06-06", "calls": 1}
    second = run(tmp_path, source, entries=[UNRATE, DGS10], http=http, now=NOW + datetime.timedelta(hours=2))
    assert (second.sources[0].ok, second.sources[0].calls, second.sources[0].pending) == (0, 0, 2)
    assert second.exit_code == 3
    third = run(tmp_path, source, entries=[UNRATE, DGS10], http=http, now=NEXT_DAY)
    assert third.sources[0].ok == 1
    assert Storage(tmp_path).read_runs()["fake"]["budget"] == {"day": "2026-06-07", "calls": 1}


def test_an_interrupted_run_keeps_what_it_stored_and_frees_the_lock(tmp_path):
    class Dies(FakeSource):
        def fetch(self, requests):
            yield from super().fetch(requests[:1])
            msg = "power cut"
            raise RuntimeError(msg)

    source = Dies()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    with pytest.raises(RuntimeError, match="power cut"):
        run(tmp_path, source, entries=[UNRATE, DGS10])
    assert stored_values(tmp_path) == {"2026-05": 4.1}
    assert not (tmp_path / LOCK_FILE).exists()
    healthy = FakeSource()
    healthy.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    healthy.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    report = run(tmp_path, healthy, entries=[UNRATE, DGS10], now=LATER)
    assert (report.sources[0].ok, report.sources[0].new) == (2, 1)


def test_a_second_sync_is_refused_while_the_lock_exists(tmp_path):
    (tmp_path / LOCK_FILE).write_text("123", encoding="ascii")
    with pytest.raises(LockHeldError, match="Delete it by hand"):
        run(tmp_path, FakeSource())
    assert (tmp_path / LOCK_FILE).exists()


def test_an_invalid_catalog_stops_before_any_download(tmp_path):
    source = FakeSource()
    with pytest.raises(CatalogError, match="declared more than once"):
        run(tmp_path, source, entries=[UNRATE, UNRATE])
    assert source.seen == []
    assert not (tmp_path / LOCK_FILE).exists()


def test_only_sources_and_only_keys_restrict_the_run(tmp_path):
    fake = FakeSource()
    other = FakeSource("other")
    wanted = entry("X", source="other")
    storage = Storage(tmp_path)
    sources = {"fake": fake, "other": other}
    sync(storage, [UNRATE, DGS10, wanted], sources, client(), NOW, only_sources=["other"])
    assert (fake.seen, [request.entry.key for request in other.seen]) == ([], ["other:X"])
    sync(storage, [UNRATE, DGS10, wanted], sources, client(), LATER, only_keys=["fake:DGS10"])
    assert [request.entry.key for request in fake.seen] == ["fake:DGS10"]


def test_the_run_log_records_each_source(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    run(tmp_path, source)
    record = Storage(tmp_path).read_runs()["fake"]
    assert record["last_run"] == "2026-06-06T12:00:00+00:00"
    assert (record["ok"], record["failed"], record["new"], record["revised"]) == (1, 0, 1, 0)
    assert (record["quota_exhausted"], record["pending"]) == (False, 0)


def test_a_missing_value_is_stored_and_does_not_count_as_new(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = SeriesData(
        entry=UNRATE,
        key=UNRATE.key,
        name="U",
        frequency=Frequency.MONTHLY,
        observations=(Observation("2026-05", datetime.date(2026, 5, 31), math.nan),),
    )
    report = run(tmp_path, source)
    assert report.sources[0].new == 0
    assert len(Storage(tmp_path).read_observations("fake")) == 1
    assert index_row(tmp_path)["last_period"] is None


def test_report_lines_are_ascii_and_say_what_happened():
    report = SyncReport(
        NOW,
        (
            SourceReport("fred", 33, (), 41, 3, 66, False, 0),
            SourceReport("bls", 1100, (), 9, 0, 450, True, 43),
            SourceReport("banxico", 0, (("banxico:SF1", "key_error: BANXICO_TOKEN missing"),), 0, 0, 0, False, 0),
            SourceReport("inegi", 1, (("inegi:9", "not_found: no such indicator"),), 5, 0, 2, False, 0),
        ),
    )
    assert report.lines() == [
        "[ok]  fred        33 series, 41 new, 3 revised, 66 calls",
        "[x]   bls         1100 of 1143 series, 9 new, 0 revised, 450 calls; quota exhausted, 43 pending",
        "[x]   banxico     key_error: BANXICO_TOKEN missing",
        "[x]   inegi       1 of 2 series, 5 new, 0 revised, 2 calls",
        "      inegi:9  not_found: no such indicator",
    ]
    assert all(line.isascii() for line in report.lines())
    assert report.exit_code == 1


def test_a_name_declared_in_the_catalog_replaces_the_name_of_the_source(tmp_path):
    named = entry("UNRATE", name="Unemployment rate, curated")
    source = FakeSource()
    source.answers["UNRATE"] = monthly(named, {"2026-05": 4.1})
    run(tmp_path, source, entries=[named])
    assert index_row(tmp_path)["name"] == "Unemployment rate, curated"
