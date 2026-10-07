"""One real call to UN Comtrade. Off by default; run with:  pytest -m live tests/live

Needs COMTRADE_API_KEY in the environment or in ./.env, and network access. It spends one call
of the daily quota. It exists to notice when Comtrade changes the shape of its answers.
"""

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store


@pytest.mark.live
def test_comtrade_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.COMTRADE) is None:
        pytest.skip("COMTRADE_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("comtrade", ["MEX"], annual_from=2022, months=0)
    report = store.sync()
    assert report.exit_code == 0
    assert report.sources[0].calls == 1
    table = store.table("comtrade", "MEX", period="2022")
    assert set(table["flow"]) == {"X", "M"}
    assert set(table["partner"]) == {"WLD"}
    assert table["product"].str.fullmatch(r"\d{2}").all()
    assert table["product"].nunique() > 90  # the HS has 97 chapters
    assert not table.duplicated(["flow", "product"]).any()  # one total per key, no breakdowns
    oil = table[(table["flow"] == "X") & (table["product"] == "27")]["value_usd"].iloc[0]
    assert oil > 1e10  # Mexico exported about 39 billion dollars of mineral fuels in 2022
