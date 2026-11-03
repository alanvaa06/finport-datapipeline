"""Eight real calls to Sharadar. Off by default; run with:  pytest -m live tests/live

Needs SHARADAR_API_KEY in the environment or in ./.env, a plan with prices, actions,
fundamentals, tickers and daily metrics, and network access. It exists to notice when Sharadar
changes the shape of its answers, and it settles what the free tier does not show: the `funds`
table, a split among the actions, the actions of an ETF, the whole history of the actions, of
the fundamentals and of the daily metrics, that the `datekey` of every as-reported row comes
after its period, and what a ticker Sharadar does not have is answered with.

It checks shapes only, never a value: Sharadar's license forbids publishing its data. Whether a
dividend's value is the amount as paid is not a shape: the acceptance run settles it.
"""

import datetime

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.store.api import Store
from data_pipeline.store.sources import sharadar_daily as daily
from data_pipeline.store.sources import sharadar_fundamentals as fundamentals
from data_pipeline.store.sources import sharadar_tickers as tickers
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
        period_is_a_date = bool(prices["period"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all())
        assert period_is_a_date, "a price period is not a date"
        every_column_holds_numbers = bool(prices[list(VALUE_COLUMNS)].notna().any().all())
        assert every_column_holds_numbers, "a price column holds no number"  # every column is there
        whole_history_came = prices["date"].min().date() < today - datetime.timedelta(
            days=366 * (DEFAULT_WINDOW_YEARS + 1)
        )
        assert whole_history_came, "the prices start within the last six years"
    actions = store.table("sharadar_actions", "AAPL")
    not_a_recent_window_only = actions["date"].min().date() < today - datetime.timedelta(
        days=366 * (DEFAULT_WINDOW_YEARS + 1)
    )
    assert not_a_recent_window_only, "the actions start within the last six years"
    has_the_words = {"dividend", "split"} <= set(actions["action"])  # the words the README recipe reads
    assert has_the_words, "the actions of AAPL hold no dividend or no split"
    amounts = actions[actions["action"].isin(["dividend", "split"])]["value"].dropna()
    all_positive = bool((amounts > 0).all())
    assert all_positive, "a dividend or split amount is not positive"
    # an ETF has actions in the same table: the catalog example lists funds under sharadar_actions
    etf_pays = "dividend" in set(store.table("sharadar_actions", "SPY")["action"])
    assert etf_pays, "the actions of SPY hold no dividend"


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


class Recording(httpx.BaseTransport):
    """A transport that keeps in memory, by Sharadar table, the rows of the last answer it
    passed on. The rows also reach the temporary store the test syncs into, under pytest's temp
    folder and outside the repository; the transport writes nothing itself, and the test reads
    their shape, never a value."""

    def __init__(self, inner=None):
        self._inner = inner or httpx.HTTPTransport()
        self.rows = {}

    def handle_request(self, request):
        response = self._inner.handle_request(request)
        response.read()
        if response.status_code == httpx.codes.OK:
            self.rows[request.url.path.rsplit("/", 1)[-1]] = response.json().get("data") or []
        return response

    def close(self):
        self._inner.close()


@pytest.mark.live
def test_the_fundamentals_tickers_and_daily_metrics_still_answer_in_the_expected_shape(tmp_path):
    if keys.resolve().get(keys.SHARADAR) is None:
        pytest.skip("SHARADAR_API_KEY is not set")
    recording = Recording()
    store = Store(tmp_path / "store", transport=recording)
    for source in ("sharadar_fundamentals", "sharadar_tickers", "sharadar_daily"):
        store.add(source, ["AAPL"])
    report = store.sync()
    assert report.exit_code == 0
    assert {source.source: source.calls for source in report.sources} == {
        "sharadar_daily": 1,
        "sharadar_fundamentals": 1,
        "sharadar_tickers": 1,
    }
    # every column a source reads is in every row Sharadar sends, and the fundamentals have no column
    # the source does not know: a renamed indicator would otherwise be stored as missing without notice
    described = {"ticker", "dimension", "calendardate", "date", "reportperiod", "fiscalperiod", "lastupdated"}
    for row in recording.rows["fundamentals"]:
        assert set(row) == {*described, *fundamentals.INDICATORS}
    for row in recording.rows["tickers"]:
        assert {"ticker", "table", *tickers.VALUE_COLUMNS, *tickers.ATTRIBUTE_COLUMNS} <= set(row)
    for row in recording.rows["daily"]:
        assert {"ticker", "date", *daily.VALUE_COLUMNS} <= set(row)

    today = datetime.datetime.now(datetime.UTC).date()
    whole_history = today - datetime.timedelta(days=366 * (DEFAULT_WINDOW_YEARS + 1))
    panel = store.table("sharadar_fundamentals", "AAPL")
    has_the_six_dimensions = set(panel["dimension"]) == set(fundamentals.DIMENSIONS)
    assert has_the_six_dimensions, "the fundamentals do not hold exactly the six dimensions"
    first_day_is_old = panel["date"].min().date() < whole_history  # not the recent window of the free tier
    assert first_day_is_old, "fundamentals start within the last six years"
    as_reported = panel[panel["dimension"].isin(fundamentals.AS_REPORTED)]
    # the day an as-reported row is known from comes after the end of the period it reports
    datekey_follows_period = bool((as_reported["datekey"] > as_reported["reportperiod"]).all())
    assert datekey_follows_period, "an as-reported datekey is not after its period end"
    record = store.table("sharadar_tickers", "AAPL")
    record_has_rows = len(record) > 0
    assert record_has_rows, "the ticker record is empty"
    every_permaticker_is_there = bool(record["permaticker"].notna().all())
    assert every_permaticker_is_there, "a permaticker is missing"
    metrics = store.table("sharadar_daily", "AAPL")
    every_metric_holds_numbers = bool(metrics[list(daily.VALUE_COLUMNS)].notna().any().all())
    assert every_metric_holds_numbers, "a daily metric column holds no number"
    daily_first_day_is_old = metrics["date"].min().date() < whole_history
    assert daily_first_day_is_old, "daily metrics start within the last six years"
