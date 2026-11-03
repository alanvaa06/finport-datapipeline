"""One real call to BLS. Off by default; run with:  pytest -m live tests/live

Needs BLS_API_KEY in the environment or in ./.env, and network access. It confirms the shape of
the answer the unit tests assume: period codes, the catalog block and the status field.
"""

import datetime

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store


@pytest.mark.live
def test_bls_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.BLS) is None:
        pytest.skip("BLS_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("bls", ["LNS14000000"], start=datetime.date(2020, 1, 1))
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  bls         1 series")
    info = store.info("bls:LNS14000000")
    assert (info.frequency, info.seasonal_adjustment) == ("M", "SA")
    assert "Unemployment Rate" in info.name
    series = store.series("bls:LNS14000000")
    assert series["period"].iloc[0] == "2020-01"
    assert series["value"].between(2, 20).all()
