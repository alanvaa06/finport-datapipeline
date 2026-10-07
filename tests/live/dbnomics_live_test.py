"""One real call to DBnomics. Off by default; run with:  pytest -m live tests/live

DBnomics needs no key, only network access.
"""

import pytest

from data_pipeline.store.api import Store


@pytest.mark.live
def test_dbnomics_still_answers_in_the_expected_shape(tmp_path):
    store = Store(tmp_path / "store")
    store.add("dbnomics", ["Eurostat/prc_hicp_midx/M.I15.CP00.EA"])
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  dbnomics  1 series")
    info = store.info("dbnomics:Eurostat/prc_hicp_midx/M.I15.CP00.EA")
    assert info.frequency == "M"
    assert info.name != ""
    assert len(store.series("dbnomics:Eurostat/prc_hicp_midx/M.I15.CP00.EA")) > 200
