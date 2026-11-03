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


@pytest.mark.live
def test_fred_vintages_read_the_series_as_published_before_the_first_sync(tmp_path):
    if keys.resolve().get(keys.FRED) is None:
        pytest.skip("FRED_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("fred", ["UNRATE", "DGS10", "SP500"])  # vintages; too many vintages for one call; not in ALFRED
    report = store.sync()
    assert report.exit_code == 0, report.lines()
    known = store.series("fred:UNRATE", as_of="2020-05-01")
    assert known["period"].iloc[-1] == "2020-03"  # April 2020 was published on 2020-05-08
    assert store.series("fred:DGS10", as_of="2010-01-04")["period"].iloc[-1] < "2010-01-04"
    assert store.series("fred:SP500", as_of="2020-01-01").empty  # no vintages: known from this sync on
    assert not store.series("fred:SP500").empty
