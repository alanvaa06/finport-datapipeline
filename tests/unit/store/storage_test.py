import datetime
import json
import math

import pandas as pd
import pytest

from data_pipeline.store import storage as storage_module
from data_pipeline.store.errors import StoreError
from data_pipeline.store.storage import (
    INDEX_COLUMNS,
    OBS_COLUMNS,
    OBS_DTYPES,
    Storage,
    append_changes,
    as_of,
    empty_index,
    empty_observations,
    latest,
    to_moment,
    typed,
)

JUNE_6 = datetime.datetime(2026, 6, 6, 12, 0, tzinfo=datetime.UTC)
JULY_4 = datetime.datetime(2026, 7, 4, 12, 0, tzinfo=datetime.UTC)
AUGUST_8 = datetime.datetime(2026, 8, 8, 12, 0, tzinfo=datetime.UTC)
DAYS = {"2026-03": "2026-03-31", "2026-04": "2026-04-30", "2026-05": "2026-05-31", "2026-06": "2026-06-30"}


def rows(values, fetched_at, *, key="fred:UNRATE", projection=False, published_at=None):
    """Observation frame from {"2026-05": 4.1, ...}."""
    return typed(
        [
            {
                "key": key,
                "period": period,
                "date": DAYS[period],
                "value": value,
                "projection": projection,
                "fetched_at": fetched_at,
                "published_at": published_at,
            }
            for period, value in values.items()
        ],
        OBS_DTYPES,
    )


def values_of(frame):
    return dict(zip(frame["period"], frame["value"], strict=True))


def test_empty_frames_have_the_declared_columns():
    assert list(empty_observations().columns) == OBS_COLUMNS
    assert list(empty_index().columns) == INDEX_COLUMNS
    assert empty_observations().empty


def test_first_rows_are_all_added():
    merged, added, revised = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": 4.1}, JUNE_6))
    assert (len(merged), added, revised) == (2, 2, 0)


def test_the_same_data_again_adds_nothing():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": math.nan}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-04": 4.0, "2026-05": math.nan}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 0)
    assert merged is stored


def test_a_changed_value_adds_a_row_and_keeps_the_old_one():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-05": 4.2}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 1)
    assert list(merged["value"]) == [4.1, 4.2]


def test_float_noise_is_not_a_revision():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-05": 4.1 * (1 + 1e-12)}, JULY_4))
    assert (len(merged), added, revised) == (1, 0, 0)


def test_a_missing_value_that_arrives_later_is_new_not_revised():
    stored, added, _ = append_changes(empty_observations(), rows({"2026-05": math.nan}, JUNE_6))
    assert added == 0
    merged, added, revised = append_changes(stored, rows({"2026-05": 4.1}, JULY_4))
    assert (len(merged), added, revised) == (2, 1, 0)


def test_a_number_that_becomes_missing_is_a_revision():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    merged, added, revised = append_changes(stored, rows({"2026-05": math.nan}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 1)


def test_a_projection_that_becomes_actual_is_a_change_even_with_the_same_value():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-06": 4.3}, JUNE_6, projection=True))
    merged, added, revised = append_changes(stored, rows({"2026-06": 4.3}, JULY_4, projection=False))
    assert (len(merged), added, revised) == (2, 0, 1)


def test_keys_do_not_interfere():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6, key="fred:A"))
    merged, added, _ = append_changes(stored, rows({"2026-05": 9.9}, JUNE_6, key="fred:B"))
    assert (len(merged), added) == (2, 1)


def test_latest_returns_the_row_fetched_last():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": 4.1}, JUNE_6))
    stored, _, _ = append_changes(stored, rows({"2026-05": 4.2}, JULY_4))
    assert values_of(latest(stored)) == {"2026-04": 4.0, "2026-05": 4.2}


def test_as_of_returns_what_was_known_then():
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": 4.1}, JUNE_6))
    stored, _, _ = append_changes(stored, rows({"2026-05": 4.2, "2026-06": 4.3}, JULY_4))
    assert values_of(as_of(stored, to_moment("2026-06-15"))) == {"2026-04": 4.0, "2026-05": 4.1}
    assert values_of(as_of(stored, to_moment("2026-07-04"))) == {"2026-04": 4.0, "2026-05": 4.2, "2026-06": 4.3}
    assert as_of(stored, to_moment("2026-01-01")).empty


def test_as_of_prefers_the_source_publication_date():
    published = datetime.datetime(2026, 5, 2, tzinfo=datetime.UTC)
    stored, _, _ = append_changes(empty_observations(), rows({"2026-03": 4.0}, AUGUST_8, published_at=published))
    assert values_of(as_of(stored, to_moment("2026-05-02"))) == {"2026-03": 4.0}
    assert as_of(stored, to_moment("2026-05-01")).empty


def test_to_moment_reads_dates_as_end_of_day_utc():
    end = pd.Timestamp("2026-06-15 23:59:59.999999", tz="UTC")
    assert to_moment("2026-06-15") == end
    assert to_moment(datetime.date(2026, 6, 15)) == end
    assert to_moment(datetime.datetime(2026, 6, 15, 8, 0)) == pd.Timestamp("2026-06-15 08:00", tz="UTC")
    assert to_moment("2026-06-15T08:00:00+02:00") == pd.Timestamp("2026-06-15 06:00", tz="UTC")


def test_observations_survive_a_round_trip(tmp_path):
    storage = Storage(tmp_path)
    stored, _, _ = append_changes(empty_observations(), rows({"2026-04": 4.0, "2026-05": math.nan}, JUNE_6))
    storage.write_observations("fred", stored)
    again = storage.read_observations("fred")
    assert list(again.columns) == OBS_COLUMNS
    merged, added, revised = append_changes(again, rows({"2026-04": 4.0, "2026-05": math.nan}, JULY_4))
    assert (len(merged), added, revised) == (2, 0, 0)
    assert not list(tmp_path.rglob("*.tmp"))


def test_missing_files_read_as_empty(tmp_path):
    storage = Storage(tmp_path / "nowhere")
    assert storage.read_observations("fred").empty
    assert storage.read_index().empty
    assert storage.read_runs() == {}


def test_runs_round_trip(tmp_path):
    storage = Storage(tmp_path)
    storage.write_runs({"fred": {"ok": 3}})
    assert storage.read_runs() == {"fred": {"ok": 3}}


def test_prepare_writes_the_schema_marker_once(tmp_path):
    storage = Storage(tmp_path / "store")
    storage.prepare()
    storage.prepare()
    assert json.loads((tmp_path / "store" / "store.json").read_text(encoding="utf-8")) == {"schema_version": 1}


def test_prepare_refuses_another_schema(tmp_path):
    (tmp_path / "store.json").write_text('{"schema_version": 99}', encoding="utf-8")
    with pytest.raises(StoreError, match="schema_version 99"):
        Storage(tmp_path).prepare()


def test_a_file_held_open_by_a_reader_is_retried_and_then_written(tmp_path, monkeypatch):
    storage = Storage(tmp_path)
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    storage.write_observations("fred", stored)
    real = type(tmp_path).replace
    attempts = []

    def busy_twice(self, target):
        attempts.append(target)
        if len(attempts) <= 2:
            raise PermissionError(13, "the file is being used by another process")
        return real(self, target)

    waits = []
    monkeypatch.setattr(type(tmp_path), "replace", busy_twice)
    monkeypatch.setattr(storage_module, "_sleep", waits.append)
    more, _, _ = append_changes(stored, rows({"2026-06": 4.3}, JULY_4))
    storage.write_observations("fred", more)
    assert len(attempts) == 3
    assert len(waits) == 2
    assert len(storage.read_observations("fred")) == 2


def test_a_file_that_stays_open_is_a_clear_error_and_the_old_data_survives(tmp_path, monkeypatch):
    storage = Storage(tmp_path)
    stored, _, _ = append_changes(empty_observations(), rows({"2026-05": 4.1}, JUNE_6))
    storage.write_observations("fred", stored)

    def always_busy(_self, _target):
        raise PermissionError(13, "the file is being used by another process")

    monkeypatch.setattr(type(tmp_path), "replace", always_busy)
    monkeypatch.setattr(storage_module, "_sleep", lambda _seconds: None)
    more, _, _ = append_changes(stored, rows({"2026-06": 4.3}, JULY_4))
    with pytest.raises(StoreError, match="another program has it open"):
        storage.write_observations("fred", more)
    monkeypatch.undo()
    assert len(storage.read_observations("fred")) == 1
