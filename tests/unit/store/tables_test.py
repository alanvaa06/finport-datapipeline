import datetime
import math

import pandas as pd
import pytest

from data_pipeline.store.errors import StoreError
from data_pipeline.store.storage import (
    INDEX_DTYPES,
    INDEX_FILE,
    KIND_SERIES,
    Storage,
    TableSchema,
    append_rows,
    as_of,
    latest,
    table_frame,
    typed,
)

from .helpers import NOW

LATER = NOW + datetime.timedelta(days=30)
KEY = ("reporter", "product", "frequency", "period")
VALUES = ("value_usd", "weight_kg")
ORDER = ("reporter", "product", "frequency", "date")


def row(product="27", period="2024", value=100.0, weight=5.0, reporter="MEX"):
    return {
        "reporter": reporter,
        "product": product,
        "frequency": "A",
        "period": period,
        "date": datetime.date(int(period), 12, 31),
        "value_usd": value,
        "weight_kg": weight,
    }


SCHEMA = TableSchema(KEY, VALUES)


def frame(rows, moment=NOW):
    return table_frame(rows, SCHEMA, moment)


def test_table_frame_types_the_columns_and_stamps_the_rows():
    built = frame([row(), row("87", value=7.0, weight=math.nan)])
    assert list(built.columns) == [*KEY, "date", *VALUES, "fetched_at", "published_at"]
    assert str(built["value_usd"].dtype) == "float64"
    assert str(built["date"].dtype) == "datetime64[ns]"
    assert list(built["fetched_at"]) == [pd.Timestamp(NOW)] * 2
    assert built["published_at"].isna().all()
    assert frame([]).empty


def test_new_keys_are_appended_and_counted_as_added():
    merged, added, revised = append_rows(pd.DataFrame(), frame([row(), row("87")]), KEY, VALUES)
    assert (len(merged), added, revised) == (2, 2, 0)
    merged, added, revised = append_rows(merged, frame([row(), row("06")], LATER), KEY, VALUES)
    assert (len(merged), added, revised) == (3, 1, 0)


def test_the_same_values_add_nothing_and_return_the_stored_frame_itself():
    old = frame([row(weight=math.nan)])
    merged, added, revised = append_rows(old, frame([row(weight=math.nan)], LATER), KEY, VALUES)
    assert merged is old
    assert (added, revised) == (0, 0)


def test_values_within_the_tolerance_are_the_same():
    old = frame([row(value=1e12)])
    merged, _added, revised = append_rows(old, frame([row(value=1e12 + 1e-3)], LATER), KEY, VALUES)
    assert merged is old
    assert revised == 0


@pytest.mark.parametrize(
    "changed",
    [row(value=101.0), row(weight=6.0), row(weight=math.nan), row(value=math.nan)],
)
def test_a_change_in_any_value_column_appends_a_version(changed):
    old = frame([row()])
    merged, added, revised = append_rows(old, frame([changed], LATER), KEY, VALUES)
    assert (len(merged), added, revised) == (2, 0, 1)
    assert list(merged["fetched_at"]) == [pd.Timestamp(NOW), pd.Timestamp(LATER)]
    pd.testing.assert_frame_equal(merged.iloc[[0]], old)  # the stored row is untouched


def test_a_key_received_twice_in_one_answer_keeps_its_last_row():
    merged, added, _revised = append_rows(pd.DataFrame(), frame([row(value=1.0), row(value=2.0)]), KEY, VALUES)
    assert added == 1
    assert list(merged["value_usd"]) == [2.0]


def test_latest_and_as_of_work_on_any_key():
    stored, _added, _revised = append_rows(frame([row(), row("87")]), frame([row(value=150.0)], LATER), KEY, VALUES)
    now = latest(stored, KEY, ORDER)
    assert dict(zip(now["product"], now["value_usd"], strict=True)) == {"27": 150.0, "87": 100.0}
    before = as_of(stored, pd.Timestamp(NOW), KEY, ORDER)
    assert dict(zip(before["product"], before["value_usd"], strict=True)) == {"27": 100.0, "87": 100.0}
    assert as_of(stored, pd.Timestamp(NOW - datetime.timedelta(days=1)), KEY, ORDER).empty


def test_a_table_survives_a_round_trip_and_remembers_its_columns(tmp_path):
    storage = Storage(tmp_path)
    storage.prepare()
    assert storage.read_table("comtrade", "MEX").empty
    assert storage.table_names("comtrade") == []
    built = frame([row(), row("87", weight=math.nan)])
    storage.write_table("comtrade", "MEX", built, SCHEMA)
    pd.testing.assert_frame_equal(storage.read_table("comtrade", "MEX"), built)
    assert storage.table_names("comtrade") == ["MEX"]
    assert storage.table_schema("comtrade") == SCHEMA
    assert (tmp_path / "tables" / "comtrade" / "MEX.parquet").exists()


def test_the_schema_of_a_source_without_tables_is_an_error(tmp_path):
    with pytest.raises(StoreError, match="no table is stored for source 'comtrade'"):
        Storage(tmp_path).table_schema("comtrade")


def test_an_index_written_before_tables_existed_reads_as_series(tmp_path):
    storage = Storage(tmp_path)
    storage.prepare()
    old = typed([{"key": "fred:UNRATE", "source": "fred", "source_id": "UNRATE", "status": "ok"}], INDEX_DTYPES)
    old.drop(columns="kind").to_parquet(tmp_path / INDEX_FILE)
    assert list(storage.read_index()["kind"]) == [KIND_SERIES]
