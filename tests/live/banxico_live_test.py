"""Two real calls to Banxico SIE. Off by default; run with:  pytest -m live tests/live

Needs BANXICO_TOKEN in the environment or in ./.env, and network access. It confirms the shape
of the metadata answer (periodicidad, unidad), which the unit tests take from the documentation.
"""

import datetime

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store


@pytest.mark.live
def test_banxico_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.BANXICO) is None:
        pytest.skip("BANXICO_TOKEN is not set")
    store = Store(tmp_path / "store")
    store.add("banxico", ["SF43718"], start=datetime.date(2024, 1, 1))
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  banxico   1 series")
    info = store.info("banxico:SF43718")
    assert info.frequency == "D"
    assert info.units != ""
    series = store.series("banxico:SF43718")
    assert series["period"].iloc[0] == "2024-01-02"
    assert len(series) > 400
    assert series["value"].between(5, 40).all()
