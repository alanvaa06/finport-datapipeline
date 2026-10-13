"""Five real calls to Sharadar. Off by default; run with:  pytest -m live tests/live

Needs SHARADAR_API_KEY in the environment or in ./.env, a plan with prices and actions, and
network access. It exists to notice when Sharadar changes the shape of its answers, and it
settles what the free tier does not show: the `funds` table, a split among the actions, the
actions of an ETF, the whole history of the actions, and what a ticker Sharadar does not have is
answered with.

It checks shapes only, never a value: Sharadar's license forbids publishing its data. Whether a
dividend's value is the amount as paid is not a shape: the acceptance run settles it.
"""

import datetime

import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store
from data_pipeline.store.sources.sharadar_prices import VALUE_COLUMNS

DEFAULT_WINDOW_YEARS = 5  # what a call without a first day is answered with
NO_SUCH_TICKER = "ZZZZQQ"


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
    assert {source.source: source.calls for source in report.sources} == {"sharadar_actions": 2, "sharadar_prices": 2}
    today = datetime.datetime.now(datetime.UTC).date()
    for ticker in ("AAPL", "SPY"):  # a stock, and an ETF from the table the free tier does not show
        prices = store.table("sharadar_prices", ticker)
        assert prices["period"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()
        assert prices[list(VALUE_COLUMNS)].notna().any().all()  # every column is there and holds numbers
        first = prices["date"].min().date()
        assert first < today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))  # the whole history came
    actions = store.table("sharadar_actions", "AAPL")
    oldest = actions["date"].min().date()
    assert oldest < today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))  # not a recent window only
    assert {"dividend", "split"} <= set(actions["action"])  # the words the README recipe reads
    amounts = actions[actions["action"].isin(["dividend", "split"])]["value"].dropna()
    assert (amounts > 0).all()
    # an ETF has actions in the same table: the catalog example lists funds under sharadar_actions
    assert "dividend" in set(store.table("sharadar_actions", "SPY")["action"])


@pytest.mark.live
def test_a_ticker_sharadar_does_not_have_is_not_found(tmp_path):
    # with a paid key an unknown ticker is taken to answer with no rows, not with a refusal: a
    # refusal would fail every ticker after it as a rejected key
    if keys.resolve().get(keys.SHARADAR) is None:
        pytest.skip("SHARADAR_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("sharadar_prices", [NO_SUCH_TICKER])
    report = store.sync()
    (source,) = report.sources
    ((key, reason),) = source.failed
    assert key == f"sharadar_prices:{NO_SUCH_TICKER}"
    assert reason.startswith("not_found")
