import datetime

import click.testing
import duckdb
import pandas as pd
import pytest

from data_pipeline.cli import cli
from data_pipeline.store import sources
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Kind, Outcome, TableData

from .store.helpers import FakeSource, monthly

# Every id, name and number here is invented.
YEAR = {f"2025-{month:02d}": float(month) for month in range(1, 13)}


def install(monkeypatch, source):
    monkeypatch.setitem(sources.REGISTRY, "fake", lambda _client, _credentials: source)
    return source


def run(*arguments):
    return click.testing.CliRunner().invoke(cli, ["fetch", *arguments])


def answered(**answers):
    fake = FakeSource()
    fake.answers = {name: monthly(CatalogEntry("fake", name), values) for name, values in answers.items()}
    return fake


def test_without_out_it_prints_a_summary_and_the_first_rows(monkeypatch):
    install(monkeypatch, answered(AAA=YEAR))
    result = run("fake", "AAA")
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "fake: 1 ids, 12 rows"
    assert lines[1] == "id  date  period  value"
    assert lines[2] == "AAA  2025-01-31  2025-01  1.0"
    assert len(lines) == 2 + 10 + 1
    assert lines[-1] == "[i]   the first 10 of 12 rows; use --out FILE for all of them"


def test_with_out_it_writes_every_row_and_prints_where(monkeypatch, tmp_path):
    install(monkeypatch, answered(AAA=YEAR, BBB={"2025-01": 5.0}))
    path = tmp_path / "out.csv"
    result = run("fake", "AAA", "BBB", "--out", str(path))
    assert result.exit_code == 0
    assert result.output.splitlines() == ["fake: 2 ids, 13 rows", f"[ok]  written to {path}"]
    assert len(pd.read_csv(path)) == 13


def test_with_out_a_duckdb_file_it_writes_the_rows_to_the_table_of_the_source(monkeypatch, tmp_path):
    install(monkeypatch, answered(AAA=YEAR))
    path = tmp_path / "data.duckdb"
    result = run("fake", "AAA", "--out", str(path))
    assert result.exit_code == 0
    assert result.output.splitlines() == ["fake: 1 ids, 12 rows", f"[ok]  written to {path}"]
    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.execute("SELECT count(*), min(period) FROM fake").fetchone() == (12, "2025-01")


def test_a_failed_fetch_with_out_says_what_it_wrote_without_ok(monkeypatch, tmp_path):
    fake = install(monkeypatch, answered(AAA={"2025-01": 1.0}))
    fake.answers["BAD"] = Failure(CatalogEntry("fake", "BAD"), Outcome.NOT_FOUND, "no such series")
    path = tmp_path / "out.csv"
    result = run("fake", "AAA", "BAD", "--out", str(path))
    assert result.exit_code == 1
    assert result.output.splitlines() == [
        "fake: 1 ids, 1 rows",
        f"[i]   what arrived is written to {path}",
        "[x]   fake:BAD  not_found: no such series",
    ]


def test_a_failed_fetch_that_brought_nothing_says_the_file_was_left_alone(monkeypatch, tmp_path):
    install(monkeypatch, FakeSource())
    path = tmp_path / "out.csv"
    path.write_text("yesterday\n", encoding="utf-8")
    result = run("fake", "AAA", "--out", str(path))
    assert result.exit_code == 1
    assert f"[i]   nothing was written to {path}" in result.output.splitlines()
    assert path.read_text(encoding="utf-8") == "yesterday\n"


def test_a_file_it_cannot_write_exits_with_1(monkeypatch, tmp_path):
    def refused(path, content):
        msg = "open in another program"
        raise PermissionError(msg)

    install(monkeypatch, answered(AAA={"2025-01": 1.0}))
    monkeypatch.setattr("data_pipeline.fetching._files.write_atomic", refused)
    path = tmp_path / "out.csv"
    result = run("fake", "AAA", "--out", str(path))
    assert result.exit_code == 1
    assert result.output.splitlines()[-1] == f"[x]   could not write {path}: PermissionError: open in another program"
    assert "[ok]" not in result.output


def test_a_param_is_read_as_yaml_and_start_as_a_date(monkeypatch):
    class Open(FakeSource):
        def validate(self, entry):
            pass

    fake = install(monkeypatch, Open())
    fake.answers = {"AAA": monthly(CatalogEntry("fake", "AAA"), {"2025-01": 1.0})}
    params = ["--param", "annual_from=2020", "--param", "partners=[USA, CAN]"]
    result = run("fake", "AAA", *params, "--start", "2020-01-01")
    assert result.exit_code == 0, result.output
    (request,) = fake.seen
    assert dict(request.entry.params) == {"annual_from": 2020, "partners": ["USA", "CAN"]}
    assert request.entry.start == datetime.date(2020, 1, 1)


@pytest.mark.parametrize(
    ("param", "message"),
    [
        ("annual_from", "'annual_from' is not NAME=VALUE"),
        ("start=2020-01-01", "use --start, not --param start="),
        ("transport=x", "'transport' cannot be given as --param"),
        ("partners=[USA", "the value of 'partners' is not valid YAML"),
    ],
)
def test_a_param_it_cannot_read_is_a_usage_error(monkeypatch, param, message):
    install(monkeypatch, answered(AAA=YEAR))
    result = run("fake", "AAA", "--param", param)
    assert result.exit_code == 2
    assert message in result.output


def test_a_failed_id_is_listed_and_exits_with_1(monkeypatch):
    fake = install(monkeypatch, answered(AAA={"2025-01": 1.0}))
    fake.answers["BAD"] = Failure(CatalogEntry("fake", "BAD"), Outcome.NOT_FOUND, "no such series")
    result = run("fake", "AAA", "BAD")
    assert result.exit_code == 1
    lines = result.output.splitlines()
    assert lines[0] == "fake: 1 ids, 1 rows"
    assert lines[-1] == "[x]   fake:BAD  not_found: no such series"


def test_a_quota_alone_exits_with_3(monkeypatch):
    fake = install(monkeypatch, answered(AAA={"2025-01": 1.0}, BBB={"2025-01": 2.0}))
    fake.quota_after = 1
    result = run("fake", "AAA", "BBB")
    assert result.exit_code == 3
    assert "[x]   fake:BBB  quota_exhausted: " in result.output


def test_a_quota_with_another_failure_exits_with_1(monkeypatch):
    fake = install(monkeypatch, answered(BBB={"2025-01": 2.0}))
    fake.answers["BAD"] = Failure(CatalogEntry("fake", "BAD"), Outcome.NOT_FOUND, "no such series")
    fake.quota_after = 1
    assert run("fake", "BAD", "BBB").exit_code == 1


def test_a_configuration_error_exits_with_2():
    result = run("nope", "AAA")
    assert result.exit_code == 2
    assert result.output.startswith("[x]   unknown source 'nope'")


def test_what_it_prints_is_ascii(monkeypatch):
    class Tables:
        name = "fake"
        kind = Kind.TABLE
        requests_per_minute = 6000
        daily_budget = None
        held_by = ()

        def validate(self, entry):
            pass

        def fetch(self, requests):
            row = {"ticker": "XAAA", "period": "2026-03-03", "date": datetime.date(2026, 3, 3), "note": "café"}
            table = TableData(
                entry=CatalogEntry("fake", "XAAA"),
                key="fake:XAAA",
                name="T",
                rows=(row,),
                key_columns=("ticker", "period"),
                value_columns=(),
                attribute_columns=("note",),
            )
            yield FetchBatch(tables=(table,))

    install(monkeypatch, Tables())
    result = run("fake", "XAAA")
    assert result.exit_code == 0
    assert result.output.isascii()
    assert "XAAA  XAAA  2026-03-03  2026-03-03  caf?" in result.output
