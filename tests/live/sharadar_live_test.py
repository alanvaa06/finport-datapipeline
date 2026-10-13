"""Four real calls to Sharadar. Off by default; run with:  pytest -m live tests/live

Needs SHARADAR_API_KEY in the environment or in ./.env, a plan with prices and actions, and
network access. It exists to notice when Sharadar changes the shape of its answers, and it
settles what the free tier does not show: the `funds` table and a split among the actions.

It checks shapes only, never a value: Sharadar's license forbids publishing its data.
"""

import datetime

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store
from data_pipeline.store.sources.sharadar_prices import VALUE_COLUMNS

DEFAULT_WINDOW_YEARS = 5  # what a call without a first day is answered with


@pytest.mark.live
def test_sharadar_still_answers_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.SHARADAR) is None:
        pytest.skip("SHARADAR_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("sharadar_prices", ["AAPL"])
    store.add("sharadar_prices", ["SPY"], table="funds")
    store.add("sharadar_actions", ["AAPL", "SPY"])
    report = store.sync()
    assert report.exit_code == 0
    assert sum(source.calls for source in report.sources) == 4
    today = datetime.datetime.now(datetime.UTC).date()
    for ticker in ("AAPL", "SPY"):  # a stock, and an ETF from the table the free tier does not show
        prices = store.table("sharadar_prices", ticker)
        assert prices["period"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()
        assert not prices.duplicated(["period"]).any()
        assert prices[list(VALUE_COLUMNS)].notna().any().all()  # every column is there and holds numbers
        first = prices["date"].min().date()
        assert first < today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))  # the whole history came
    actions = store.table("sharadar_actions", "AAPL")
    assert {"dividend", "split"} <= set(actions["action"])  # the words the README recipe reads
    assert (actions[actions["action"].isin(["dividend", "split"])]["value"] > 0).all()
    assert not actions.duplicated(["day", "action", "contraticker", "item"]).any()
