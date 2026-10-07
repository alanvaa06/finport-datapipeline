"""A few real calls to the SEC. Off by default; run with:  pytest -m live tests/live

Needs SEC_EDGAR_UA (a User-Agent with a contact) in the environment or in ./.env, and network
access. It downloads Apple's 10-K filings of one year: the list of tickers, the list of filings
and one or two documents.
"""

import datetime
import hashlib
import pathlib

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store


@pytest.mark.live
def test_the_sec_still_publishes_filings_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.SEC_UA) is None:
        pytest.skip("SEC_EDGAR_UA is not set")
    store = Store(tmp_path / "store")
    store.add("sec_filings", ["AAPL"], start=datetime.date(2023, 10, 1), forms=["10-K"], amendments=False)
    report = store.sync()
    assert report.exit_code == 0
    listed = store.documents("sec_filings", "AAPL")
    assert set(listed["form"]) == {"10-K"}
    assert "0000320193-23-000106" in set(listed["group"])  # the 10-K for fiscal 2023, filed 2023-11-03
    annual = listed[listed["group"] == "0000320193-23-000106"].iloc[0]
    assert (annual["file"], annual["role"]) == ("aapl-20230930.htm", "primary")
    assert str(annual["date"].date()) == "2023-11-03"
    content = pathlib.Path(annual["path"]).read_bytes()
    assert content.startswith(b"<?xml") or b"<html" in content[:2000].lower()
    assert hashlib.sha256(content).hexdigest() == annual["sha256"]
    assert len(content) == annual["size"] > 100_000
    again = store.sync()
    assert (again.sources[0].new, again.sources[0].calls) == (0, 2)
