import datetime
import json

import pandas as pd

from data_pipeline.store.storage import Storage, TableSchema, append_versions, as_of, latest, table_frame

from .helpers import NOW

LATER = NOW + datetime.timedelta(days=30)
KEY = ("concept", "end")
SCHEMA = TableSchema(KEY, ("value",), ("form",), versioned=True)
ORDER = ("concept", "date")


def version(value, published, concept="Revenues", end="2023-09-30", form="10-K"):
    return {
        "concept": concept,
        "end": end,
        "date": datetime.date.fromisoformat(end),
        "value": value,
        "form": form,
        "published_at": datetime.datetime.fromisoformat(published).replace(tzinfo=datetime.UTC),
    }


def frame(rows, moment=NOW):
    return table_frame(rows, SCHEMA, moment)


def merge(old, rows, moment=NOW):
    return append_versions(old, frame(rows, moment), SCHEMA.key_columns, SCHEMA.value_columns)


def test_the_schema_lists_the_columns_a_reader_gets():
    assert SCHEMA.columns == ["concept", "end", "date", "value", "form"]
    assert TableSchema(KEY, ("value",)).columns == ["concept", "end", "date", "value"]


def test_a_versioned_frame_keeps_the_publication_of_each_row_and_types_the_attributes():
    built = frame([version(1.0, "2023-11-03"), version(2.0, "2025-10-31")])
    assert list(built.columns) == ["concept", "end", "date", "value", "form", "fetched_at", "published_at"]
    assert list(built["published_at"]) == [pd.Timestamp("2023-11-03", tz="UTC"), pd.Timestamp("2025-10-31", tz="UTC")]
    assert str(built["form"].dtype) == "object"
    assert frame([]).empty


def test_a_first_load_keeps_every_version_of_a_key():
    rows = [version(1.0, "2023-11-03"), version(2.0, "2025-10-31"), version(9.0, "2023-11-03", "Assets")]
    merged, added, revised = merge(pd.DataFrame(), rows)
    assert (len(merged), added, revised) == (3, 2, 1)
    assert list(merged["concept"]) == ["Assets", "Revenues", "Revenues"]  # by key, then publication


def test_the_same_versions_again_add_nothing_and_return_the_stored_frame_itself():
    rows = [version(1.0, "2023-11-03"), version(2.0, "2025-10-31")]
    old, _added, _revised = merge(pd.DataFrame(), rows)
    merged, added, revised = merge(old, rows, LATER)
    assert merged is old
    assert (added, revised) == (0, 0)


def test_a_new_version_and_a_new_key_are_appended_and_counted_apart():
    old, _added, _revised = merge(pd.DataFrame(), [version(1.0, "2023-11-03")])
    rows = [version(1.0, "2023-11-03"), version(2.0, "2025-10-31"), version(5.0, "2024-11-01", end="2024-09-28")]
    merged, added, revised = merge(old, rows, LATER)
    assert (len(merged), added, revised) == (3, 1, 1)
    assert list(merged["fetched_at"]) == [pd.Timestamp(NOW), pd.Timestamp(LATER), pd.Timestamp(LATER)]
    pd.testing.assert_frame_equal(merged.iloc[[0]], old)  # the stored row is untouched


def test_an_attribute_that_changes_is_not_a_version():
    old, _added, _revised = merge(pd.DataFrame(), [version(1.0, "2023-11-03")])
    merged, _added, _revised = merge(old, [version(1.0, "2023-11-03", form="10-K405")], LATER)
    assert merged is old


def test_two_values_published_the_same_day_are_both_kept_in_the_order_received():
    rows = [version(1.0, "2024-02-01"), version(2.0, "2024-02-01", form="10-K/A")]
    merged, added, revised = merge(pd.DataFrame(), rows)
    assert (list(merged["value"]), added, revised) == ([1.0, 2.0], 1, 1)
    assert list(latest(merged, KEY, ORDER, by_publication=True)["value"]) == [2.0]


def test_latest_by_publication_ignores_when_a_version_was_fetched():
    """An older version fetched later (a store filled in two runs) does not become the current one."""
    old, _added, _revised = merge(pd.DataFrame(), [version(2.0, "2025-10-31")])
    merged, _added, _revised = merge(old, [version(1.0, "2023-11-03")], LATER)
    assert list(latest(merged, KEY, ORDER, by_publication=True)["value"]) == [2.0]
    assert list(latest(merged, KEY, ORDER)["value"]) == [1.0]  # by fetch, the wrong answer for versions


def test_as_of_reads_the_version_published_by_then():
    merged, _added, _revised = merge(pd.DataFrame(), [version(1.0, "2023-11-03"), version(2.0, "2025-10-31")])
    assert list(as_of(merged, pd.Timestamp("2025-10-30", tz="UTC"), KEY, ORDER)["value"]) == [1.0]
    assert list(as_of(merged, pd.Timestamp("2025-10-31", tz="UTC"), KEY, ORDER)["value"]) == [2.0]
    assert as_of(merged, pd.Timestamp("2023-11-02", tz="UTC"), KEY, ORDER).empty


def test_the_schema_survives_a_round_trip_with_attributes_and_versions(tmp_path):
    storage = Storage(tmp_path)
    storage.prepare()
    built = frame([version(1.0, "2023-11-03")])
    storage.write_table("sec_xbrl", "AAPL", built, SCHEMA)
    assert storage.table_schema("sec_xbrl") == SCHEMA
    pd.testing.assert_frame_equal(storage.read_table("sec_xbrl", "AAPL"), built)


def test_a_schema_written_before_versions_existed_reads_as_not_versioned(tmp_path):
    folder = tmp_path / "tables" / "comtrade"
    folder.mkdir(parents=True)
    old = {"key_columns": ["reporter", "period"], "value_columns": ["value_usd"]}
    (folder / "schema.json").write_text(json.dumps(old), encoding="utf-8")
    assert Storage(tmp_path).table_schema("comtrade") == TableSchema(("reporter", "period"), ("value_usd",))
