import datetime
import math
import os
import signal
import subprocess
import sys
import time

import pandas as pd
import pytest

from data_pipeline.store.errors import CatalogError, LockHeldError, StoreError
from data_pipeline.store.model import Failure, Frequency, Observation, Outcome, SeriesData
from data_pipeline.store.periods import read_period
from data_pipeline.store.storage import Storage, as_of, latest, to_moment
from data_pipeline.store.sync import LOCK_FILE, NOT_RETURNED, SourceReport, SyncReport, lock, sync, window_start

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


class Breaks(FakeSource):
    """Answers the first request, then fails with `error` as a bug or a stopped process would."""

    def __init__(self, error, name="fake", **options):
        super().__init__(name, **options)
        self.error = error

    def fetch(self, requests):
        yield from super().fetch(requests[:1])
        raise self.error


def test_an_interrupted_run_keeps_what_it_stored_and_frees_the_lock(tmp_path):
    source = Breaks(KeyboardInterrupt())
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, source, entries=[UNRATE, DGS10])
    assert stored_values(tmp_path) == {"2026-05": 4.1}
    assert not (tmp_path / LOCK_FILE).exists()
    healthy = FakeSource()
    healthy.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    healthy.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})
    report = run(tmp_path, healthy, entries=[UNRATE, DGS10], now=LATER)
    assert (report.sources[0].ok, report.sources[0].new) == (2, 1)


def test_the_calls_of_an_interrupted_run_count_against_the_daily_budget(tmp_path):
    http = client()
    source = Breaks(KeyboardInterrupt(), daily_budget=3, client=http)
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, source, entries=[UNRATE, DGS10], http=http)
    assert Storage(tmp_path).read_runs()["fake"]["budget"] == {"day": "2026-06-06", "calls": 1}


def test_a_source_that_breaks_fails_its_own_series_and_the_next_source_still_runs(tmp_path):
    broken = Breaks(RuntimeError("a bug in the parser"))
    broken.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    other = FakeSource("other")
    wanted = entry("X", source="other")
    other.answers["X"] = monthly(wanted, {"2026-05": 1.0})
    report = sync(Storage(tmp_path), [UNRATE, DGS10, wanted], {"fake": broken, "other": other}, client(), NOW)
    fake, healthy = report.sources
    assert fake.ok == 1
    reason = "source_error: the sync of this source stopped: RuntimeError: a bug in the parser"
    assert fake.failed == (("fake:DGS10", reason),)
    assert (healthy.source, healthy.ok) == ("other", 1)
    assert report.exit_code == 1
    assert index_row(tmp_path, "fake:DGS10")["status"] == "failed"
    assert Storage(tmp_path).read_runs()["fake"]["failed"] == 1
    assert not (tmp_path / LOCK_FILE).exists()


def test_a_damaged_file_of_one_source_fails_that_source_only(tmp_path):
    fake, other = FakeSource(), FakeSource("other")
    wanted = entry("X", source="other")
    fake.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    other.answers["X"] = monthly(wanted, {"2026-05": 1.0})
    sources = {"fake": fake, "other": other}
    sync(Storage(tmp_path), [UNRATE, wanted], sources, client(), NOW)
    Storage(tmp_path).series_path("fake").write_bytes(b"\x00" * 4096)  # torn by a power cut
    report = sync(Storage(tmp_path), [UNRATE, wanted], sources, client(), LATER)
    (key, reason), = report.sources[0].failed
    assert key == "fake:UNRATE"
    assert "fake.parquet: cannot be read" in reason
    assert (report.sources[1].ok, len(other.seen)) == (1, 2)
    assert report.exit_code == 1


def test_a_second_sync_is_refused_while_another_holds_the_lock(tmp_path):
    source = FakeSource()
    with lock(tmp_path), pytest.raises(LockHeldError, match=f"another sync is running .*pid {os.getpid()}"):
        run(tmp_path, source)
    assert source.seen == []


def test_a_lock_left_by_a_sync_that_died_is_taken_over(tmp_path):
    (tmp_path / LOCK_FILE).write_text("123", encoding="ascii")  # nothing holds it any more
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    assert run(tmp_path, source).exit_code == 0
    assert not (tmp_path / LOCK_FILE).exists()


HOLD_THE_LOCK = """
import os, pathlib, sys, time
from data_pipeline.store.sync import lock
with lock(pathlib.Path(sys.argv[1])):
    print(os.getpid(), flush=True)
    time.sleep(120)
"""


def run_once_the_lock_is_free(tmp_path, source, seconds=20.0):
    """The system frees the lock of a process that died soon after, not at the same instant."""
    deadline = time.monotonic() + seconds
    while True:
        try:
            return run(tmp_path, source)
        except LockHeldError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


def test_a_sync_killed_without_any_cleanup_does_not_block_the_next_one(tmp_path):
    # The way a panel's Stop button ends a run: no finally, no atexit, the lock file stays behind.
    holder = subprocess.Popen([sys.executable, "-c", HOLD_THE_LOCK, str(tmp_path)], stdout=subprocess.PIPE, text=True)
    try:
        pid = int(holder.stdout.readline())
        with pytest.raises(LockHeldError, match=rf"another sync is running on this store \(pid {pid} on "):
            run(tmp_path, FakeSource())
        os.kill(pid, signal.SIGTERM)  # TerminateProcess on Windows; on POSIX the default action, no finally
    finally:
        holder.kill()
        holder.wait()
    assert (tmp_path / LOCK_FILE).exists()
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    assert run_once_the_lock_is_free(tmp_path, source).exit_code == 0


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


def vintages(catalog_entry, versions):
    """A monthly series whose source dates every version: [("2026-04", 4.0, "2026-05-08"), ...]."""
    observations = tuple(
        Observation(
            *read_period(period, Frequency.MONTHLY),
            value,
            published_at=datetime.datetime.fromisoformat(published).replace(tzinfo=datetime.UTC),
        )
        for period, value, published in versions
    )
    return SeriesData(catalog_entry, catalog_entry.key, "Name", Frequency.MONTHLY, observations=observations)


UNRATE_VINTAGES = [("2026-04", 4.0, "2026-05-08"), ("2026-04", 4.1, "2026-06-05"), ("2026-05", 4.2, "2026-06-05")]


def test_every_dated_version_is_stored_and_read_as_of_its_publication(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = vintages(UNRATE, UNRATE_VINTAGES)
    report = run(tmp_path, source)
    assert (report.sources[0].new, report.sources[0].revised) == (2, 1)
    stored = Storage(tmp_path).read_observations("fake")
    assert len(stored) == 3
    assert stored_values(tmp_path) == {"2026-04": 4.1, "2026-05": 4.2}
    known = as_of(stored, to_moment("2026-05-31"))
    assert dict(zip(known["period"], known["value"], strict=True)) == {"2026-04": 4.0}


def test_dated_versions_received_again_add_nothing(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = vintages(UNRATE, UNRATE_VINTAGES)
    run(tmp_path, source)
    report = run(tmp_path, source, now=LATER)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(Storage(tmp_path).read_observations("fake")) == 3


def test_a_new_dated_version_is_a_revision(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = vintages(UNRATE, UNRATE_VINTAGES)
    run(tmp_path, source)
    source.answers["UNRATE"] = vintages(UNRATE, [*UNRATE_VINTAGES, ("2026-05", 4.3, "2026-07-03")])
    report = run(tmp_path, source, now=LATER)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    assert stored_values(tmp_path) == {"2026-04": 4.1, "2026-05": 4.3}


def test_a_dated_missing_value_received_again_adds_nothing(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = vintages(UNRATE, [("2026-04", math.nan, "2026-05-08")])
    run(tmp_path, source)
    run(tmp_path, source, now=LATER)
    assert len(Storage(tmp_path).read_observations("fake")) == 1


def test_an_alias_that_moves_to_another_series_leaves_the_old_one(tmp_path):
    source = FakeSource()
    old, new = entry("OLD", alias="e_us_x"), entry("NEW", alias="e_us_x")
    source.answers["OLD"] = monthly(old, {"2026-04": 1.0})
    source.answers["NEW"] = monthly(new, {"2026-05": 2.0})
    run(tmp_path, source, entries=[old])
    run(tmp_path, source, entries=[new], now=LATER)
    index = Storage(tmp_path).read_index().set_index("key")["alias"]
    assert index["fake:NEW"] == "e_us_x"
    assert index.isna()["fake:OLD"]


class CountingStorage(Storage):
    """A storage that counts how many times the observations and the index are written."""

    def __init__(self, root):
        super().__init__(root)
        object.__setattr__(self, "writes", {"observations": 0, "index": 0})

    def write_observations(self, source, frame):
        self.writes["observations"] += 1
        super().write_observations(source, frame)

    def write_index(self, frame):
        self.writes["index"] += 1
        super().write_index(frame)


def many(count):
    entries = [entry(f"S{i}") for i in range(count)]
    source = FakeSource()
    for item in entries:
        source.answers[item.source_id] = monthly(item, {"2026-04": 1.0, "2026-05": float(len(item.source_id))})
    return entries, source


def test_a_quick_run_writes_each_file_once_per_source(tmp_path):
    entries, source = many(5)
    storage = CountingStorage(tmp_path)
    report = sync(storage, entries, {"fake": source}, client(), NOW)
    assert (report.sources[0].ok, report.sources[0].new) == (5, 10)
    assert storage.writes == {"observations": 1, "index": 1}
    assert stored_values(tmp_path, "fake:S4") == {"2026-04": 1.0, "2026-05": 2.0}
    assert index_row(tmp_path, "fake:S4")["last_period"] == "2026-05"


def test_a_slow_run_keeps_what_it_downloaded_at_every_checkpoint(tmp_path):
    entries, source = many(3)
    storage = CountingStorage(tmp_path)
    ticks = iter(range(0, 10_000, 61))
    sync(storage, entries, {"fake": source}, client(), NOW, monotonic=lambda: float(next(ticks)))
    assert storage.writes["observations"] == 3


def test_a_dated_version_stored_as_a_projection_becomes_actual_when_its_period_closes(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = vintages(UNRATE, [("2026-06", 4.3, "2026-06-05")])  # June still open on NOW
    run(tmp_path, source)
    assert bool(latest(Storage(tmp_path).read_observations("fake"))["projection"].iloc[0])
    run(tmp_path, source, now=LATER)
    current = latest(Storage(tmp_path).read_observations("fake"))
    assert not bool(current["projection"].iloc[0])
    assert index_row(tmp_path)["last_period"] == "2026-06"


def test_a_version_repeated_with_a_later_date_adds_nothing(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = vintages(UNRATE, [("2026-04", 4.0, "2026-05-08")])
    run(tmp_path, source)
    # FRED cuts a row at the start of the real-time period asked for: the same version, dated later
    source.answers["UNRATE"] = vintages(UNRATE, [("2026-04", 4.0, "2026-05-20")])
    report = run(tmp_path, source, now=LATER)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(Storage(tmp_path).read_observations("fake")) == 1


def test_each_series_is_stamped_with_the_time_it_was_downloaded(tmp_path):
    # A run that starts at 12:00 and crosses midnight UTC: DGS10 arrives on the next day.
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-05": 4.1})
    source.answers["DGS10"] = monthly(DGS10, {"2026-05": 4.4})

    def clock():
        return NOW + datetime.timedelta(hours=6 * len(source.seen))

    run(tmp_path, source, entries=[UNRATE, DGS10], clock=clock)
    stored = Storage(tmp_path).read_observations("fake")
    stamps = dict(zip(stored["key"], stored["fetched_at"], strict=True))
    assert stamps == {
        "fake:UNRATE": pd.Timestamp("2026-06-06 18:00", tz="UTC"),
        "fake:DGS10": pd.Timestamp("2026-06-07 00:00", tz="UTC"),
    }
    known = as_of(stored, to_moment("2026-06-06"))
    assert list(known["key"]) == ["fake:UNRATE"]
    assert index_row(tmp_path, "fake:DGS10")["last_fetched_at"] == pd.Timestamp("2026-06-07 00:00", tz="UTC")


def test_failure_reasons_are_scrubbed_before_they_are_stored_or_printed(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = Failure(UNRATE, Outcome.SOURCE_ERROR, "HTTP 500: token=TOPSECRET42 rejected")
    report = run(tmp_path, source, http=client(secrets=("TOPSECRET42",)))
    assert report.sources[0].failed == (("fake:UNRATE", "source_error: HTTP 500: token=*** rejected"),)
    assert index_row(tmp_path)["reason"] == "source_error: HTTP 500: token=*** rejected"


def quarterly(catalog_entry, values):
    observations = tuple(
        Observation(*read_period(period, Frequency.QUARTERLY), value) for period, value in values.items()
    )
    return SeriesData(catalog_entry, catalog_entry.key, "Q", Frequency.QUARTERLY, observations=observations)


def test_a_series_whose_frequency_changes_fails_and_keeps_its_data(tmp_path):
    source = FakeSource()
    source.answers["UNRATE"] = monthly(UNRATE, {"2026-01": 10.0, "2026-02": 11.0, "2026-03": 12.0})
    run(tmp_path, source)
    source.answers["UNRATE"] = quarterly(UNRATE, {"2026Q1": 33.0})
    report = run(tmp_path, source, now=LATER)
    (key, reason), = report.sources[0].failed
    assert key == "fake:UNRATE"
    assert reason.startswith("source_error: the source now sends this series as Q; the store holds it as M")
    assert report.exit_code == 1
    assert stored_values(tmp_path) == {"2026-01": 10.0, "2026-02": 11.0, "2026-03": 12.0}
    row = index_row(tmp_path)
    assert (row["status"], row["frequency"], row["last_period"]) == ("failed", "M", "2026-03")


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"only_sources": ["fredd"]}, r"no catalog entry has source 'fredd' \(sources in the catalog: fake\)"),
        ({"only_keys": ["fake:UNRAT", "fake:DGS10"]}, "no catalog entry has key 'fake:UNRAT'"),
        ({"only_sources": ["fake"], "only_keys": ["other:X"]}, "no catalog entry has key 'other:X'"),
    ],
)
def test_a_source_or_key_the_catalog_does_not_declare_is_an_error_not_an_empty_run(tmp_path, options, message):
    source = FakeSource()
    with pytest.raises(StoreError, match=message):
        run(tmp_path, source, entries=[UNRATE, DGS10], **options)
    assert source.seen == []
    assert not (tmp_path / LOCK_FILE).exists()


def test_sources_and_keys_that_select_nothing_together_are_an_error(tmp_path):
    other = entry("X", source="other")
    with pytest.raises(StoreError, match="select no catalog entry"):
        sync(Storage(tmp_path), [UNRATE, other], {"fake": FakeSource(), "other": FakeSource("other")}, client(), NOW,
             only_sources=["fake"], only_keys=["other:X"])
