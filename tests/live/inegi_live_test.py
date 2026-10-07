"""One real call to INEGI. Off by default; run with:  pytest -m live tests/live

Needs INEGI_TOKEN in the environment or in ./.env, and network access.
"""

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store


@pytest.mark.live
def test_inegi_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.INEGI) is None:
        pytest.skip("INEGI_TOKEN is not set")
    store = Store(tmp_path / "store")
    store.add("inegi", ["6207136901"], bank="BISE")
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  inegi     1 series")
    assert store.info("inegi:6207136901").frequency == "M"
    series = store.series("inegi:6207136901")
    assert len(series) > 300
    assert series["period"].iloc[0] == "1993-01"
