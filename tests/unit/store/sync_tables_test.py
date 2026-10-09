import datetime

import pandas as pd

from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.model import Failure, FetchBatch, Kind, Outcome, TableData
from data_pipeline.store.storage import Storage, latest
from data_pipeline.store.sync import NOT_RETURNED, SourceReport, held_periods, sync

from .helpers import NOW, client, entry

LATER = NOW + datetime.timedelta(days=30)
NEXT_DAY = NOW + datetime.timedelta(days=1)
KEY = ("reporter", "product", "frequency", "period")
VALUES = ("value_usd",)
MEX = entry("MEX", "trade")
USA = entry("USA", "trade")


def row(reporter="MEX", product="27", period="2024", value=100.0):
    monthly = "-" in period
    year, month = (int(part) for part in period.split("-")) if monthly else (int(period), 12)
    day = datetime.date(year, month, 28 if monthly else 31)
    return {
        "reporter": reporter,
        "product": product,
        "frequency": "M" if monthly else "A",
        "period": period,
        "date": day,
        "value_usd": value,
    }


def table(catalog_entry, rows, stale_after_days=190):
    return TableData(
        entry=catalog_entry,
        key=catalog_entry.key,
        name=f"Trade of {catalog_entry.source_id}",
        rows=tuple(rows),
        key_columns=KEY,
        value_columns=VALUES,
        stale_after_days=stale_after_days,
    )


class FakeTables:
    """A table source that answers from a dictionary: source_id -> a list of calls, each one a
    list of rows, a Failure, or the text "quota". Every call counts against the client."""

    name = "trade"
    kind = Kind.TABLE
    requests_per_minute = 6000

    def __init__(self, http, daily_budget=None):
        self.daily_budget = daily_budget
        self.calls = {}
        self.seen = []
        self._http = http

    def validate(self, entry):
        del entry

    def fetch(self, requests):
        for request in requests:
            self.seen.append(request)
            for call in self.calls.get(request.entry.source_id, []):
                self._http.calls[self.name] = self._http.calls.get(self.name, 0) + 1
                if isinstance(call, str):
                    raise QuotaExhaustedError(call)
                if isinstance(call, Failure):
                    yield FetchBatch(failures=(call,))
                    break
                yield FetchBatch(tables=(table(request.entry, call),))


def run(tmp_path, source, http, entries=(MEX,), now=NOW, **options):
    return sync(Storage(tmp_path), list(entries), {source.name: source}, http, now, **options)


def setup():
    http = client()
    return FakeTables(http), http


def stored(tmp_path, name="MEX"):
    return Storage(tmp_path).read_table("trade", name)


def index_row(tmp_path, key="trade:MEX"):
    frame = Storage(tmp_path).read_index()
    return frame[frame["key"] == key].iloc[0]


def test_held_periods_are_the_pairs_of_a_stored_table():
    assert held_periods(pd.DataFrame()) == frozenset()
    frame = pd.DataFrame([row(), row(product="87"), row(period="2026-05")])
    assert held_periods(frame) == {("A", "2024"), ("M", "2026-05")}


def test_held_periods_carry_the_values_of_the_columns_named():
    frame = pd.DataFrame([row(), row(product="87"), row("USA", period="2026-05")]).assign(level=["AG2", "AG2", None])
    assert held_periods(frame, ("reporter", "level")) == {("A", "2024", "MEX", "AG2"), ("M", "2026-05", "USA", "")}
    assert held_periods(frame, ("partner",)) == {("A", "2024", ""), ("M", "2026-05", "")}  # a column it lacks


def test_a_source_that_names_its_held_by_columns_gets_their_values(tmp_path):
    source, http = setup()
    source.held_by = ("reporter",)
    source.calls["MEX"] = [[row(), row(period="2026-05")]]
    run(tmp_path, source, http)
    run(tmp_path, source, http, now=LATER)
    assert source.seen[1].held == {("A", "2024", "MEX"), ("M", "2026-05", "MEX")}


def test_first_sync_merges_every_batch_into_the_table(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(), row(product="87")], [row(period="2026-05", value=7.0)]]
    report = run(tmp_path, source, http)
    assert source.seen[0].held == frozenset()
    assert len(stored(tmp_path)) == 3
    assert report.sources == (SourceReport("trade", 1, (), 3, 0, 2, False, 0),)
    assert report.exit_code == 0


def test_the_index_row_describes_the_table(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(), row(period="2026-04"), row(period="2026-05")]]
    run(tmp_path, source, http)
    found = index_row(tmp_path)
    assert (found["kind"], found["status"], found["name"]) == ("table", "ok", "Trade of MEX")
    assert (found["frequency"], found["last_period"]) == ("M", "2026-05")  # the newest of the highest frequency
    assert found["last_date"] == pd.Timestamp("2026-05-28")
    assert found["stale_after_days"] == 190
    assert (found["source"], found["source_id"]) == ("trade", "MEX")


def test_the_entrys_own_threshold_and_name_win(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()]]
    run(tmp_path, source, http, entries=[entry("MEX", "trade", stale_after_days=400, name="Mexico")])
    found = index_row(tmp_path)
    assert (found["stale_after_days"], found["name"]) == (400, "Mexico")


def test_second_sync_carries_what_is_stored_and_adds_nothing(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(), row(period="2026-05")]]
    run(tmp_path, source, http)
    report = run(tmp_path, source, http, now=LATER)
    assert source.seen[1].held == {("A", "2024"), ("M", "2026-05")}
    assert (report.sources[0].new, report.sources[0].revised) == (0, 0)
    assert len(stored(tmp_path)) == 2
    found = index_row(tmp_path)
    assert found["first_fetched_at"] < found["last_fetched_at"]


def test_a_revised_value_appends_a_version_and_keeps_the_old_one(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row(value=100.0)]]
    run(tmp_path, source, http)
    source.calls["MEX"] = [[row(value=120.0)]]
    report = run(tmp_path, source, http, now=LATER)
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    assert list(stored(tmp_path)["value_usd"]) == [100.0, 120.0]
    assert list(latest(stored(tmp_path), KEY, KEY)["value_usd"]) == [120.0]


def test_full_says_so_still_sends_what_is_held_and_stores_only_what_changed(tmp_path):
    # the source asks for everything again, and can still refuse what the stored table cannot take
    source, http = setup()
    source.calls["MEX"] = [[row()]]
    run(tmp_path, source, http)
    report = run(tmp_path, source, http, now=LATER, full=True)
    assert (source.seen[0].full, source.seen[1].full) == (False, True)
    assert source.seen[1].held == {("A", "2024")}
    assert report.sources[0].new == 0
    assert len(stored(tmp_path)) == 1


def test_each_entry_has_its_own_file(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()]]
    source.calls["USA"] = [[row("USA"), row("USA", product="87")]]
    report = run(tmp_path, source, http, entries=[MEX, USA])
    assert (len(stored(tmp_path)), len(stored(tmp_path, "USA"))) == (1, 2)
    assert report.sources[0].ok == 2
    assert Storage(tmp_path).table_names("trade") == ["MEX", "USA"]


def test_a_failure_after_a_stored_batch_keeps_the_batch_and_fails_the_entry(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()], Failure(MEX, Outcome.NETWORK_ERROR, "timeout")]
    source.calls["USA"] = [[row("USA")]]
    report = run(tmp_path, source, http, entries=[MEX, USA])
    assert len(stored(tmp_path)) == 1
    assert report.sources[0].ok == 1
    assert report.sources[0].failed == (("trade:MEX", "network_error: timeout"),)
    found = index_row(tmp_path)
    assert (found["status"], found["kind"], found["last_period"]) == ("failed", "table", "2024")
    assert report.exit_code == 1


def test_an_entry_the_source_never_answers_is_a_failure(tmp_path):
    source, http = setup()
    report = run(tmp_path, source, http)
    assert report.sources[0].failed == (("trade:MEX", f"not_found: {NOT_RETURNED}"),)
    assert index_row(tmp_path)["kind"] == "table"


def test_an_answer_without_rows_is_ok_and_has_no_data(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[]]
    report = run(tmp_path, source, http)
    assert report.sources[0].ok == 1
    found = index_row(tmp_path)
    assert (found["status"], found["frequency"]) == ("ok", "")
    assert pd.isna(found["last_date"])
    assert stored(tmp_path).empty


def test_a_quota_stop_keeps_what_arrived_and_the_next_run_resumes(tmp_path):
    source, http = setup()
    source.calls["MEX"] = [[row()], "quota used up"]
    source.calls["USA"] = [[row("USA")]]
    report = run(tmp_path, source, http, entries=[MEX, USA])
    assert report.sources[0].quota_exhausted
    assert (report.sources[0].ok, report.sources[0].pending, report.sources[0].failed) == (1, 1, ())
    assert report.exit_code == 3
    assert len(stored(tmp_path)) == 1
    source.calls["MEX"] = [[row(period="2025")]]
    report = run(tmp_path, source, http, entries=[MEX, USA], now=NEXT_DAY)
    assert source.seen[-2].held == {("A", "2024")}
    assert (report.sources[0].ok, report.sources[0].new) == (2, 2)


def test_the_daily_budget_stops_the_source_between_batches(tmp_path):
    http = client()
    source = FakeTables(http, daily_budget=2)
    source.calls["MEX"] = [[row()], [row(period="2025")], [row(period="2026-05")]]
    report = run(tmp_path, source, http)
    assert (report.sources[0].calls, report.sources[0].quota_exhausted) == (2, True)
    assert len(stored(tmp_path)) == 2
    report = run(tmp_path, source, http, now=NOW + datetime.timedelta(hours=1))
    assert (report.sources[0].calls, report.sources[0].quota_exhausted) == (0, True)
    report = run(tmp_path, source, http, now=NEXT_DAY)
    assert report.sources[0].calls == 2


def test_each_table_call_is_stamped_with_the_time_it_was_downloaded(tmp_path):
    http = client()
    source = FakeTables(http)
    source.calls["MEX"] = [[row(period="2023")], [row(period="2024")]]

    def clock():
        return NOW + datetime.timedelta(hours=7 * http.calls.get("trade", 0))

    run(tmp_path, source, http, clock=clock)
    stored = Storage(tmp_path).read_table("trade", "MEX")
    stamps = dict(zip(stored["period"], stored["fetched_at"], strict=True))
    assert stamps == {
        "2023": pd.Timestamp("2026-06-06 19:00", tz="UTC"),
        "2024": pd.Timestamp("2026-06-07 02:00", tz="UTC"),
    }
