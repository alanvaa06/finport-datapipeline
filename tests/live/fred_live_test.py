"""One real call to FRED. Off by default; run with:  pytest -m live tests/live

Needs FRED_API_KEY in the environment or in ./.env, and network access. It exists to notice
when FRED changes the shape of its answers; the unit suite cannot see that.
"""

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store


@pytest.mark.live
def test_fred_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.FRED) is None:
        pytest.skip("FRED_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("fred", ["UNRATE"])
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  fred        1 series")
    info = store.info("fred:UNRATE")
    assert (info.frequency, info.units, info.seasonal_adjustment) == ("M", "Percent", "SA")
    series = store.series("fred:UNRATE")
    assert len(series) > 900
    assert series["period"].iloc[0] == "1948-01"
