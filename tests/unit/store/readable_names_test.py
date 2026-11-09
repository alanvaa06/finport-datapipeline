import datetime
import json

import click.testing
import pandas as pd
import pytest

from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import StoreError, UnknownSeriesError
from data_pipeline.store.sources import sec_xbrl
from data_pipeline.store.sources.sec_filings import exhibit_tag, label
from data_pipeline.store.storage import Storage, TableSchema, document_rows, readable_names

from .helpers import NOW

TEN_K = "0000320193-25-000079"
TEN_K_AMENDED = "0000320193-25-000090"
EIGHT_K = "0000320193-25-000071"
EIGHT_K_SAME_DAY = "0000320193-25-000072"
XBRL_SCHEMA = TableSchema(sec_xbrl.KEY_COLUMNS, sec_xbrl.VALUE_COLUMNS, sec_xbrl.ATTRIBUTE_COLUMNS, versioned=True)


@pytest.mark.parametrize(
    ("file", "tag"),
    [
        ("d753197dex991.htm", "ex99-1"),
        ("d456699dex9901.htm", "ex99-1"),
        ("tm2629095d1_ex99-2.htm", "ex99-2"),
        ("a18-6447_1ex10d5.htm", "ex10-5"),
        ("orly-20260401xex99d1.htm", "ex99-1"),
        ("amgn-ex99_1.htm", "ex99-1"),
        ("amgn-ex992_2022331xq1.htm", "ex99-2"),
        ("d85627dex101.htm", "ex10-1"),
        ("dp73850_ex1001.htm", "ex10-1"),
        ("d851110dex51.htm", "ex5-1"),
        ("d12082dex511.htm", "ex5-11"),
        ("exhibit31-restatedbylawsxf.htm", "ex3-1"),
        ("a8-kex991q4202509272025.htm", "ex99-1"),
        ("ex9919302020earningsre.htm", "ex99-1"),
        ("a20181231ex-991.htm", "ex99-1"),
        ("exhibit991q32026earningsre.htm", "ex99-1"),
        ("exhibit99.htm", "ex99"),
        ("exhibit10.htm", "ex10"),
        ("pressrelease.htm", "pressrelease"),
    ],
)
def test_an_exhibit_is_named_by_the_number_its_file_name_carries(file, tag):
    assert exhibit_tag(file) == tag


def test_a_filing_is_labelled_by_ticker_form_and_period():
    filed = datetime.date(2025, 12, 1)
    assert label("AAPL", "10-K", "2025-09-27", filed) == "AAPL_10-K_2025-09-27"
    assert label("AAPL", "10-K/A", "", filed) == "AAPL_10-K-A_2025-12-01"  # no period: the filing day
    assert label("BRK-B", "SC 13D", "not a day", filed) == "BRK-B_SC-13D_2025-12-01"


def test_a_name_already_stored_is_numbered_and_never_renames_the_first():
    files = [("aapl.htm", ""), ("ex991.htm", "ex99-1")]
    first = readable_names("AAPL_8-K_2025-10-30", files, set(), set())
    assert first == ("AAPL_8-K_2025-10-30", ["AAPL_8-K_2025-10-30.htm", "AAPL_8-K_2025-10-30_ex99-1.htm"])
    second = readable_names("AAPL_8-K_2025-10-30", [("b.htm", "")], {first[0]}, set(first[1]))
    assert second == ("AAPL_8-K_2025-10-30_2", ["AAPL_8-K_2025-10-30_2.htm"])


def test_a_label_whose_file_name_is_taken_moves_on_to_the_next_number():
    # an exhibit tagged "2" already took the name a second filing's label would give
    taken = {"AAPL_8-K_2025-10-30.htm", "AAPL_8-K_2025-10-30_2.htm"}
    used, names = readable_names("AAPL_8-K_2025-10-30", [("b.htm", "")], {"AAPL_8-K_2025-10-30"}, taken)
    assert (used, names) == ("AAPL_8-K_2025-10-30_3", ["AAPL_8-K_2025-10-30_3.htm"])


def test_two_files_of_a_document_with_the_same_tag_are_numbered():
    files = [("a.htm", ""), ("ex991a.htm", "ex99-1"), ("ex991b.htm", "ex99-1"), ("x.TXT", "")]
    _, names = readable_names("L", files, set(), set())
    assert names == ["L.htm", "L_ex99-1.htm", "L_ex99-1_2.htm", "L.txt"]


def _legacy_filing(storage, accession, filed, form, period, files):
    """A filing as 0.1 stored it: in a folder named by its accession, without label or stored name."""
    for name, _role, content in files:
        storage.write_document("sec_filings", "AAPL", accession, name, content)
    listed = [(name, role, f"https://sec/{accession}/{name}", content) for name, role, content in files]
    rows = document_rows(accession, filed, listed, {"form": form, "period": period}, NOW)
    return rows.drop(columns=["label", "stored_as"])


@pytest.fixture
def legacy(tmp_path):
    """A schema-1 store, written as 0.1 wrote it."""
    root = tmp_path / "store"
    storage = Storage(root)
    rows = [
        _legacy_filing(storage, EIGHT_K, datetime.date(2025, 10, 30), "8-K", "2025-10-30",
                       [("aapl-20251030.htm", "primary", b"8k"), ("a8-kex991q4.htm", "exhibit", b"release")]),
        _legacy_filing(storage, EIGHT_K_SAME_DAY, datetime.date(2025, 10, 30), "8-K", "2025-10-30",
                       [("aapl-20251030b.htm", "primary", b"8k again")]),
        _legacy_filing(storage, TEN_K, datetime.date(2025, 10, 31), "10-K", "2025-09-27",
                       [("aapl-20250927.htm", "primary", b"annual")]),
        _legacy_filing(storage, TEN_K_AMENDED, datetime.date(2025, 12, 1), "10-K/A", "",
                       [("0000320193-25-000090.txt", "primary", b"amended")]),
    ]  # fmt: skip
    storage.write_documents("sec_filings", "AAPL", pd.concat(rows, ignore_index=True))
    (root / "store.json").write_text('{"schema_version": 1}', encoding="utf-8")
    return root


def test_a_schema_1_store_reads_as_it_is(legacy):
    listed = Store(legacy).documents("sec_filings", "AAPL", group=TEN_K)
    assert listed["path"].iloc[0].endswith(f"{TEN_K}\\aapl-20250927.htm") or listed["path"].iloc[0].endswith(
        f"{TEN_K}/aapl-20250927.htm"
    )
    assert open(listed["path"].iloc[0], "rb").read() == b"annual"  # noqa: PTH123, SIM115


def test_migrate_renames_the_files_in_place_and_marks_the_store(legacy):
    [result] = Store(legacy).migrate()
    assert (result.ticker, result.moved, result.missing) == ("AAPL", 5, 0)
    folder = legacy / "documents" / "sec_filings" / "AAPL"
    assert sorted(path.name for path in folder.iterdir()) == [
        "AAPL_10-K-A_2025-12-01.txt",
        "AAPL_10-K_2025-09-27.htm",
        "AAPL_8-K_2025-10-30.htm",
        "AAPL_8-K_2025-10-30_2.htm",  # filed the same day, after the first
        "AAPL_8-K_2025-10-30_ex99-1.htm",
    ]  # the accession folders are gone
    assert (folder / "AAPL_8-K_2025-10-30_ex99-1.htm").read_bytes() == b"release"
    listed = Store(legacy).documents("sec_filings", "AAPL")
    row = listed[listed["stored_as"] == "AAPL_8-K_2025-10-30_ex99-1.htm"].iloc[0]
    assert (row["group"], row["file"], row["label"]) == (EIGHT_K, "a8-kex991q4.htm", "AAPL_8-K_2025-10-30")
    assert all(open(path, "rb").read() for path in listed["path"])  # noqa: PTH123, SIM115
    assert json.loads((legacy / "store.json").read_text(encoding="utf-8")) == {"schema_version": 2}


def test_a_second_migration_has_nothing_to_do(legacy):
    Store(legacy).migrate()
    assert Store(legacy).migrate() == []


def test_a_migration_that_stopped_after_moving_files_finishes_with_the_same_names(legacy):
    folder = legacy / "documents" / "sec_filings" / "AAPL"
    (folder / TEN_K / "aapl-20250927.htm").replace(folder / "AAPL_10-K_2025-09-27.htm")  # moved, list not written
    [result] = Store(legacy).migrate()
    assert (result.moved, result.missing) == (4, 0)
    listed = Store(legacy).documents("sec_filings", "AAPL", group=TEN_K)
    assert list(listed["stored_as"]) == ["AAPL_10-K_2025-09-27.htm"]


def test_a_migration_refuses_to_overwrite_a_file_at_the_new_name(legacy):
    folder = legacy / "documents" / "sec_filings" / "AAPL"
    (folder / "AAPL_10-K_2025-09-27.htm").write_bytes(b"someone else's")
    with pytest.raises(StoreError, match="already exists"):
        Store(legacy).migrate()
    assert (folder / TEN_K / "aapl-20250927.htm").read_bytes() == b"annual"


def _facts(root):
    facts = pd.DataFrame(
        {
            "taxonomy": ["us-gaap", "us-gaap"],
            "concept": ["Revenues", "Assets"],
            "unit": ["USD", "USD"],
            "start": ["2024-09-29", ""],
            "end": ["2025-09-27", "2025-09-27"],
            "date": pd.to_datetime(["2025-09-27", "2025-09-27"]),
            "value": [416.0, 359.0],
            "form": ["10-K", "10-Q"],
            "accession": [TEN_K, "0000320193-25-000057"],  # the 10-Q's filing is not stored
            "filed": ["2025-10-31", "2025-08-01"],
            "fiscal_year": ["2025", "2025"],
            "fiscal_period": ["FY", "Q3"],
            "frame": ["CY2025", ""],
        }
    )
    Storage(root).write_table("sec_xbrl", "AAPL", facts, XBRL_SCHEMA)


def test_each_filing_gets_its_xbrl_facts_under_its_label(legacy, tmp_path):
    Store(legacy).migrate()
    _facts(legacy)
    out = tmp_path / "xbrl"
    [result] = Store(legacy).export_xbrl(out)
    assert (result.files, result.written) == (1, 1)
    assert sorted(path.name for path in (out / "AAPL").iterdir()) == ["AAPL_10-K_2025-09-27_xbrl.csv"]
    facts = pd.read_csv(out / "AAPL" / "AAPL_10-K_2025-09-27_xbrl.csv", dtype=str, keep_default_na=False)
    assert list(facts["concept"]) == ["Revenues"]
    assert list(facts.columns) == XBRL_SCHEMA.columns
    [again] = Store(legacy).export_xbrl(out)
    assert again.written == 0


def test_the_xbrl_export_asks_for_a_migration_first(legacy, tmp_path):
    _facts(legacy)
    with pytest.raises(StoreError, match="run `data-pipeline migrate` first"):
        Store(legacy).export_xbrl(tmp_path / "xbrl")


def test_the_xbrl_export_refuses_a_folder_inside_the_store(legacy):
    _facts(legacy)
    with pytest.raises(StoreError, match="outside the store"):
        Store(legacy).export_xbrl(legacy / "xbrl")


def test_the_xbrl_export_refuses_a_company_without_facts(legacy, tmp_path):
    with pytest.raises(UnknownSeriesError, match="AAPL"):
        Store(legacy).export_xbrl(tmp_path / "xbrl", ["AAPL"])


def test_the_commands_report_what_they_did(legacy, tmp_path):
    runner = click.testing.CliRunner()
    migrated = runner.invoke(cli, ["migrate", "--root", str(legacy)])
    assert migrated.exit_code == 0, migrated.output
    assert "[ok]  AAPL  5 files renamed" in migrated.output
    _facts(legacy)
    out = tmp_path / "xbrl"
    exported = runner.invoke(cli, ["export-xbrl", "--root", str(legacy), "--to", str(out)])
    assert exported.exit_code == 0, exported.output
    assert "[ok]  AAPL  1 files, 1 new or changed" in exported.output


def _html(legacy):
    """Give the stored 10-K some HTML to convert."""
    folder = legacy / "documents" / "sec_filings" / "AAPL"
    (folder / "AAPL_10-K_2025-09-27.htm").write_bytes(
        b"<html><head><title>t</title></head><body><p>Annual report</p>"
        b"<table><tr><td></td><td>2025</td></tr>"
        b"<tr><td>Net sales</td><td>$</td><td>416,161</td></tr></table></body></html>"
    )


def test_each_html_file_is_written_as_markdown_under_its_stored_name(legacy, tmp_path):
    Store(legacy).migrate()
    _html(legacy)
    out = tmp_path / "text"
    [result] = Store(legacy).export_text(out)
    assert (result.ticker, result.files, result.written) == ("AAPL", 4, 4)  # the 10-K/A is a .txt: left out
    assert sorted(path.name for path in (out / "AAPL").iterdir()) == [
        "AAPL_10-K_2025-09-27.md",
        "AAPL_8-K_2025-10-30.md",
        "AAPL_8-K_2025-10-30_2.md",
        "AAPL_8-K_2025-10-30_ex99-1.md",
    ]
    assert (out / "AAPL" / "AAPL_10-K_2025-09-27.md").read_text(encoding="utf-8") == (
        "Annual report\n\n|  | 2025 |\n|---|---|\n| Net sales | $416,161 |\n"
    )
    [again] = Store(legacy).export_text(out)
    assert again.written == 0


def test_the_text_export_asks_for_a_migration_first(legacy, tmp_path):
    with pytest.raises(StoreError, match="run `data-pipeline migrate` first"):
        Store(legacy).export_text(tmp_path / "text")


def test_the_text_export_refuses_a_folder_inside_the_store_and_an_unknown_company(legacy, tmp_path):
    Store(legacy).migrate()
    with pytest.raises(StoreError, match="outside the store"):
        Store(legacy).export_text(legacy / "text")
    with pytest.raises(UnknownSeriesError, match="MSFT"):
        Store(legacy).export_text(tmp_path / "text", ["MSFT"])


def test_the_text_command_reports_what_it_wrote(legacy, tmp_path):
    Store(legacy).migrate()
    out = tmp_path / "text"
    arguments = ["export-text", "--root", str(legacy), "--to", str(out), "--id", "AAPL"]
    exported = click.testing.CliRunner().invoke(cli, arguments)
    assert exported.exit_code == 0, exported.output
    assert "[ok]  AAPL  4 files, 4 new or changed" in exported.output
