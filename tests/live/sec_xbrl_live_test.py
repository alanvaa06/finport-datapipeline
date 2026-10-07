"""Two real calls to the SEC. Off by default; run with:  pytest -m live tests/live

Needs SEC_EDGAR_UA (a User-Agent with a contact, such as "Name name@domain.com") in the
environment or in ./.env, and network access. It exists to notice when the SEC changes the shape
of its company facts.
"""

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store


@pytest.mark.live
def test_the_sec_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.SEC_UA) is None:
        pytest.skip("SEC_EDGAR_UA is not set")
    store = Store(tmp_path / "store")
    store.add("sec_xbrl", ["AAPL"])
    report = store.sync()
    assert report.exit_code == 0
    assert report.sources[0].calls == 2  # the list of tickers and the company
    assert store.info("sec_xbrl:AAPL").name == "Apple Inc."
    table = store.table("sec_xbrl", "AAPL")
    assert len(table) > 10_000
    assert {"us-gaap", "dei"} <= set(table["taxonomy"])
    assert not table.duplicated(["taxonomy", "concept", "unit", "start", "end"]).any()  # one current value a fact
    assets = table[(table["concept"] == "Assets") & (table["end"] == "2023-09-30")]
    assert list(assets["value"]) == [352583000000.0]  # Apple's total assets at the end of fiscal 2023
    assert assets["form"].iloc[0] == "10-K"
    assert assets["filed"].iloc[0] == "2023-11-03"
    before = store.table("sec_xbrl", "AAPL", concept="Assets", as_of="2023-11-02")
    assert "2023-09-30" not in set(before["end"])  # not known the day before the 10-K
