import datetime
import hashlib

import pandas as pd
import pytest

from data_pipeline.store.model import Document, DocumentFile, check_name
from data_pipeline.store.storage import DOCUMENT_COLUMNS, Storage, document_rows

from .helpers import NOW

BACK = chr(92)  # a backslash


@pytest.mark.parametrize(
    "name", ["aapl-20230930.htm", "0000320193-23-000106", "a_b.c-d", "R2.htm", "console.htm", "COM10.htm", "nul-1.htm"]
)
def test_a_plain_name_is_safe(name):
    check_name(name)


@pytest.mark.parametrize("name", ["", ".", "..", "../x", "a/b", "a\\b", "C:x", "a b", "x\n"])
def test_a_name_that_could_leave_its_folder_is_refused(name):
    with pytest.raises(ValueError, match="unsafe file name"):
        check_name(name)


@pytest.mark.parametrize("name", ["CON", "nul.htm", "Aux.tar.gz", "com1.htm", "LPT9", "prn", "a.htm.", "..."])
def test_a_name_windows_keeps_for_a_device_or_cuts_short_is_refused(name):
    # NUL.htm is the null device on Windows, and Windows drops a final dot: a.htm. would be a.htm
    with pytest.raises(ValueError, match="unsafe file name"):
        check_name(name)


@pytest.mark.parametrize(
    ("group", "file"),
    [
        ("0001", BACK.join(["..", "..", "x.htm"])),
        ("0001", "../x.htm"),
        ("0001", "C:x.htm"),
        ("0001", ""),
        ("0001", "nul.htm"),
        ("..", "x.htm"),
        (BACK.join(["..", "0001"]), "x.htm"),
    ],
)
def test_the_storage_refuses_a_document_path_that_is_not_plain_names(tmp_path, group, file):
    storage = Storage(tmp_path / "store")
    with pytest.raises(ValueError, match="unsafe file name"):
        storage.document_path("sec_filings", "AAPL", group, file)
    with pytest.raises(ValueError, match="unsafe file name"):
        storage.write_document("sec_filings", "AAPL", group, file, b"x")
    assert not any(path.is_file() for path in tmp_path.rglob("*"))


def test_files_and_documents_check_their_names_when_built():
    with pytest.raises(ValueError, match="unsafe file name"):
        DocumentFile("../secrets.htm", b"", "https://x", "primary")
    with pytest.raises(ValueError, match="unsafe file name"):
        Document("a/b", datetime.date(2023, 11, 3), ())


def test_a_file_is_written_once_and_never_rewritten(tmp_path):
    storage = Storage(tmp_path)
    storage.write_document("sec_filings", "AAPL", "0000320193-23-000106", "aapl.htm", b"<html>first</html>")
    path = tmp_path / "documents" / "sec_filings" / "AAPL" / "0000320193-23-000106" / "aapl.htm"
    assert path.read_bytes() == b"<html>first</html>"
    assert path == storage.document_path("sec_filings", "AAPL", "0000320193-23-000106", "aapl.htm")
    storage.write_document("sec_filings", "AAPL", "0000320193-23-000106", "aapl.htm", b"<html>second</html>")
    assert path.read_bytes() == b"<html>first</html>"
    assert [item.name for item in path.parent.iterdir()] == ["aapl.htm"]  # no temporary file is left


def test_the_rows_of_a_document_describe_each_file():
    files = [("aapl.htm", "primary", "https://sec/aapl.htm", b"12345"), ("ex99.htm", "exhibit", "https://sec/e", b"")]
    attributes = {"form": "8-K", "period": ""}
    rows = document_rows("0000320193-23-000106", datetime.date(2023, 11, 3), files, attributes, NOW)
    assert list(rows.columns) == [*DOCUMENT_COLUMNS[:7], "form", "period", "fetched_at"]
    first = rows.iloc[0]
    assert (first["group"], first["file"], first["role"], first["size"]) == (
        "0000320193-23-000106",
        "aapl.htm",
        "primary",
        5,
    )
    assert first["sha256"] == hashlib.sha256(b"12345").hexdigest()
    assert first["date"] == pd.Timestamp("2023-11-03")
    assert first["fetched_at"] == pd.Timestamp(NOW)
    assert list(rows["form"]) == ["8-K", "8-K"]


def test_the_list_survives_a_round_trip(tmp_path):
    storage = Storage(tmp_path)
    empty = storage.read_documents("sec_filings", "AAPL")
    assert empty.empty
    assert list(empty.columns) == DOCUMENT_COLUMNS
    assert storage.document_names("sec_filings") == []
    rows = document_rows("g1", datetime.date(2023, 11, 3), [("a.htm", "primary", "u", b"x")], {"form": "10-K"}, NOW)
    storage.write_documents("sec_filings", "AAPL", rows)
    pd.testing.assert_frame_equal(storage.read_documents("sec_filings", "AAPL"), rows)
    assert storage.document_names("sec_filings") == ["AAPL"]
