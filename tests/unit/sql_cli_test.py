import datetime

import click.testing
import duckdb
import pandas as pd
import pytest

from data_pipeline.cli import cli
from data_pipeline.store import storage as st
from data_pipeline.store.api import Store

# Every key and number here is invented.
FIRST = datetime.datetime(2026, 1, 5, 9, 0, tzinfo=datetime.UTC)
LATER = datetime.datetime(2026, 2, 5, 9, 0, tzinfo=datetime.UTC)
LAST = "SELECT value FROM fake WHERE period = '2025-12'"


def run(*arguments):
    return click.testing.CliRunner().invoke(cli, ["sql", *arguments])


@pytest.fixture
def root(tmp_path):
    """A store with one monthly series of 2025, its December revised a month later."""
    storage = st.Storage(tmp_path / "store")
    storage.prepare()
    months = pd.date_range("2025-01-31", periods=12, freq="ME")
    rows = [
        {
            "key": "fake:AAA",
            "period": f"2025-{day.month:02d}",
            "date": day,
            "value": float(day.month),
            "fetched_at": FIRST,
        }
        for day in months
    ]
    rows.append({**rows[-1], "value": 12.5, "fetched_at": LATER})
    storage.write_observations("fake", st.typed([{**row, "projection": False} for row in rows], st.OBS_DTYPES))
    storage.write_index(st.typed([{"key": "fake:AAA", "source": "fake", "kind": "series"}], st.INDEX_DTYPES))
    return tmp_path / "store"


def test_without_out_it_prints_a_summary_and_the_first_rows(root):
    result = run("--root", str(root), "SELECT period, value, date FROM fake")
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[:3] == ["12 rows, 3 columns", "period  value  date", "2025-01  1.0  2025-01-31"]
    assert len(lines) == 2 + 10 + 1
    assert lines[-1] == "[i]   the first 10 of 12 rows; use --out FILE for all of them"


def test_a_moment_is_shown_with_its_time_and_zone(root):
    result = run("--root", str(root), "SELECT fetched_at FROM fake_versions WHERE value = 12.5")
    assert result.output.splitlines() == ["1 rows, 1 columns", "fetched_at", "2026-02-05T09:00:00+00:00"]


def test_as_of_reads_each_source_as_it_was_known_that_day(root):
    assert run("--root", str(root), LAST).output.splitlines()[-1] == "12.5"
    assert run("--root", str(root), LAST, "--as-of", "2026-01-31").output.splitlines()[-1] == "12.0"


@pytest.mark.parametrize("name", ["rows.parquet", "rows.csv", "rows.duckdb"])
def test_with_out_it_writes_every_row_and_prints_where(root, tmp_path, name):
    path = tmp_path / name
    result = run("--root", str(root), "SELECT period, value FROM fake", "--out", str(path))
    assert result.exit_code == 0
    assert result.output.splitlines() == ["12 rows, 2 columns", f"[ok]  written to {path}"]
    if name.endswith(".duckdb"):
        with duckdb.connect(str(path), read_only=True) as connection:
            written = connection.table("result").df()
    else:
        written = pd.read_parquet(path) if name.endswith(".parquet") else pd.read_csv(path, dtype={"period": str})
    pd.testing.assert_frame_equal(written, Store(root).sql("SELECT period, value FROM fake"))


def test_a_duckdb_file_keeps_its_other_tables_and_the_result_replaces_the_last_one(root, tmp_path):
    path = tmp_path / "rows.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE notes AS SELECT 'kept' AS note")
    assert run("--root", str(root), "SELECT * FROM fake_versions", "--out", str(path)).exit_code == 0
    assert run("--root", str(root), LAST, "--out", str(path)).exit_code == 0
    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.table("result").df().to_dict("list") == {"value": [12.5]}
        assert connection.table("notes").df().to_dict("list") == {"note": ["kept"]}


def test_a_file_it_cannot_write_exits_with_1(root, tmp_path, monkeypatch):
    def refused(path, content):
        msg = "open in another program"
        raise PermissionError(msg)

    monkeypatch.setattr("data_pipeline.fetching._files.write_atomic", refused)
    path = tmp_path / "rows.csv"
    result = run("--root", str(root), LAST, "--out", str(path))
    assert result.exit_code == 1
    assert result.output.splitlines() == [
        "1 rows, 1 columns",
        f"[x]   could not write {path}: PermissionError: open in another program",
    ]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["DROP TABLE fake"], "[x]   only one SELECT is accepted, not DROP"),
        (["SELECT * FROM prices"], "[x]   the query failed: Catalog Error: Table with name prices does not exist!"),
        ([LAST, "--as-of", "2026/01/31"], "[x]   as_of '2026/01/31' is not a date"),
        (
            [LAST, "--out", "rows.xlsx"],
            "[x]   sql: cannot write 'rows.xlsx': the file must end in .parquet, .csv or .duckdb",
        ),
    ],
)
def test_a_query_refused_or_failed_or_a_bad_option_exits_with_2(root, arguments, message):
    result = run("--root", str(root), *arguments)
    assert result.exit_code == 2
    assert result.output.splitlines()[0].startswith(message)


def test_a_failed_query_lists_the_names_a_query_can_read(root):
    result = run("--root", str(root), "SELECT * FROM prices")
    assert result.output.splitlines()[-1] == "the names a query can read: fake, fake_versions, store_index"


def test_what_it_prints_is_ascii(root):
    result = run("--root", str(root), "SELECT 'café' AS word")
    assert result.exit_code == 0
    assert result.output.splitlines() == ["1 rows, 1 columns", "word", "caf?"]


def test_the_command_is_listed_beside_the_others():
    result = click.testing.CliRunner().invoke(cli, ["--help"])
    assert "  sql " in result.output
