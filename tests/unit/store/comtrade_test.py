import datetime
import math
import re

import httpx
import pytest

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError, QuotaExhaustedError
from data_pipeline.store.model import Frequency, Kind, Outcome, Request
from data_pipeline.store.sources.comtrade import (
    FLOWS,
    KEY_COLUMNS,
    VALUE_COLUMNS,
    Comtrade,
    Query,
    closed_months,
    is_total,
    queries,
    reporter_codes,
    settings,
)
from data_pipeline.store.storage import Storage
from data_pipeline.store.sync import sync

from .helpers import NOW, client, entry, fixture

TODAY = datetime.date(2026, 10, 5)
KEY_VALUE = "0123456789abcdef0123456789abcdef"
CREDENTIALS = Credentials({keys.COMTRADE: KEY_VALUE})
ALL_YEARS = frozenset(("A", str(year)) for year in range(2000, 2026))
ALL_MONTHS = frozenset(("M", month) for month in closed_months(TODAY, 75))
EVERY_PERIOD = ALL_YEARS | ALL_MONTHS


def stored(pairs=EVERY_PERIOD, partners=("WLD",), flows=("X", "M"), level="AG2"):
    """What sync sends as `held` for a table holding these periods: (frequency, period, partner, flow, level)."""
    return frozenset(
        (frequency, period, partner, flow, level)
        for frequency, period in pairs
        for partner in partners
        for flow in flows
    )


def reporter(identifier="MEX", **params):
    return entry(identifier, "comtrade", params=params)


def data_row(period="2024", flow="X", product="27", value=1.0, weight=2.0, **more):
    return {"period": period, "flowCode": flow, "cmdCode": product, "primaryValue": value, "netWgt": weight, **more}


def answer(rows):
    return httpx.Response(200, json={"count": len(rows), "data": rows, "error": ""})


def fetch(handler, requests, credentials=CREDENTIALS):
    source = Comtrade(client(handler, secrets=(KEY_VALUE,)), credentials, today=lambda: TODAY)
    return list(source.fetch(requests))


def test_it_is_a_table_source_within_the_free_quota():
    assert Comtrade.kind is Kind.TABLE
    assert Comtrade.daily_budget == 450
    assert Comtrade.requests_per_minute == 30


def test_the_reporter_map_uses_comtrades_own_codes():
    codes = reporter_codes()
    assert (codes["MEX"], codes["USA"], codes["FRA"], codes["IND"], codes["CHN"]) == ("484", "842", "251", "699", "156")
    assert len(codes) > 200


def test_settings_have_the_defaults_of_the_spec():
    chosen = settings(reporter())
    assert (chosen.level, chosen.partners, chosen.flows) == ("AG2", ("WLD",), ("X", "M"))
    assert (chosen.annual_from, chosen.months) == (2000, 75)


def test_settings_read_every_field():
    chosen = settings(reporter(level="ag4", partners=["usa", "WLD"], flows=["m"], annual_from=2015, months=0))
    assert (chosen.level, chosen.partners, chosen.flows) == ("AG4", ("USA", "WLD"), ("M",))
    assert (chosen.annual_from, chosen.months) == (2015, 0)


@pytest.mark.parametrize(
    ("identifier", "params", "message"),
    [
        ("XXX", {}, "unknown reporter 'XXX'"),
        ("MEX", {"level": "AG3"}, "'level' must be one of AG2, AG4, AG6"),
        ("MEX", {"partners": ["ZZZ"]}, "unknown value\\(s\\) in 'partners': ZZZ"),
        ("MEX", {"partners": "USA"}, "'partners' must be a non-empty list"),
        ("MEX", {"flows": ["RX"]}, "unknown value\\(s\\) in 'flows': RX"),
        ("MEX", {"flows": []}, "'flows' must be a non-empty list"),
        ("MEX", {"annual_from": "2000"}, "'annual_from' must be a whole number"),
        ("MEX", {"months": -1}, "'months' must be a whole number"),
        ("MEX", {"months": True}, "'months' must be a whole number"),
        ("MEX", {"hs": "27"}, "unknown field\\(s\\) for source 'comtrade': hs"),
    ],
)
def test_validate_rejects_what_the_source_does_not_accept(identifier, params, message):
    source = Comtrade(client(), CREDENTIALS)
    with pytest.raises(CatalogError, match=message):
        source.validate(reporter(identifier, **params))


def test_closed_months_end_with_last_month_and_cross_years():
    assert closed_months(datetime.date(2026, 1, 15), 3) == ["2025-10", "2025-11", "2025-12"]
    assert closed_months(TODAY, 2) == ["2026-08", "2026-09"]
    assert closed_months(TODAY, 0) == []


def test_an_empty_store_asks_for_everything_in_blocks_of_twelve():
    asked = queries(frozenset(), settings(reporter()), TODAY)
    shape = [(query.frequency.value, query.periods[0], query.periods[-1], len(query.periods)) for query in asked]
    assert shape == [
        ("A", "2000", "2011", 12),
        ("A", "2012", "2023", 12),
        ("A", "2024", "2025", 2),
        ("M", "2020-07", "2021-06", 12),
        ("M", "2021-07", "2022-06", 12),
        ("M", "2022-07", "2023-06", 12),
        ("M", "2023-07", "2024-06", 12),
        ("M", "2024-07", "2025-06", 12),
        ("M", "2025-07", "2026-06", 12),
        ("M", "2026-07", "2026-09", 3),
    ]
    assert {query.partner for query in asked} == {"WLD"}


def test_a_full_store_asks_only_for_the_revision_windows():
    asked = queries(stored(), settings(reporter()), TODAY)
    assert asked == [
        Query(Frequency.ANNUAL, "WLD", ("2024", "2025")),
        Query(Frequency.MONTHLY, "WLD", tuple(closed_months(TODAY, 12))),
    ]


def test_a_period_missing_from_the_store_is_asked_for_again():
    held = stored(EVERY_PERIOD - {("A", "2003"), ("M", "2021-02")})
    annual, *monthly = queries(held, settings(reporter()), TODAY)
    assert annual.periods == ("2003", "2024", "2025")
    assert monthly[0].periods[0] == "2021-02"
    assert [len(query.periods) for query in monthly] == [12, 1]


def test_sync_tells_the_source_what_it_holds_by_partner_flow_and_level():
    assert Comtrade.held_by == ("partner", "flow", "level")


def test_a_partner_the_store_does_not_hold_is_asked_for_its_whole_history():
    # a run stopped (quota, failed call) after the world's calls: every period is stored, for WLD only
    asked = queries(stored(), settings(reporter(partners=["WLD", "USA"])), TODAY)
    world = [query for query in asked if query.partner == "WLD"]
    usa = {(query.frequency.value, period) for query in asked if query.partner == "USA" for period in query.periods}
    assert usa == EVERY_PERIOD
    assert world == queries(stored(), settings(reporter()), TODAY)  # only the revision windows


def test_a_flow_the_store_does_not_hold_is_asked_for_its_whole_history():
    asked = queries(stored(flows=("M",)), settings(reporter()), TODAY)
    assert {(query.frequency.value, period) for query in asked for period in query.periods} == EVERY_PERIOD


def test_a_row_stored_before_the_level_was_recorded_counts_as_the_entrys_level():
    chosen = settings(reporter())
    assert queries(stored(level=""), chosen, TODAY) == queries(stored(), chosen, TODAY)


def test_a_change_of_level_is_refused_before_any_call_and_the_next_reporter_goes_on():
    reporters = []

    def handler(request):
        reporters.append(request.url.params["reporterCode"])
        return answer([])

    held = stored()
    batches = fetch(handler, [Request(reporter(level="AG4"), held=held), Request(reporter("USA"), held=held)])
    assert set(reporters) == {"842"}
    failure = batches[0].failures[0]
    assert (failure.entry.source_id, failure.outcome) == ("MEX", Outcome.SOURCE_ERROR)
    assert "the stored table holds HS level AG2, not AG4" in failure.reason
    assert "tables/comtrade/MEX.parquet" in failure.reason


def test_a_full_request_asks_for_everything_and_still_refuses_a_change_of_level():
    chosen = settings(reporter())
    asked = []

    def handler(request):
        asked.append(request.url.params["period"])
        return answer([])

    fetch(handler, [Request(reporter(), held=stored(), full=True)])
    assert asked == [",".join(query.periods).replace("-", "") for query in queries(frozenset(), chosen, TODAY)]
    asked.clear()
    (batch,) = fetch(handler, [Request(reporter(level="AG4"), held=stored(), full=True)])
    assert asked == []
    assert "the stored table holds HS level AG2, not AG4" in batch.failures[0].reason


def test_months_zero_turns_the_monthly_data_off_and_partners_multiply_calls():
    asked = queries(frozenset(), settings(reporter(months=0, annual_from=2020, partners=["WLD", "USA"])), TODAY)
    assert [(query.frequency.value, query.partner, len(query.periods)) for query in asked] == [
        ("A", "WLD", 6),
        ("A", "USA", 6),
    ]


def test_six_digits_go_four_periods_to_a_call():
    asked = queries(frozenset(), settings(reporter(level="AG6", months=0, annual_from=2020)), TODAY)
    assert [len(query.periods) for query in asked] == [4, 2]


def test_a_breakdown_row_is_not_the_total():
    assert is_total({})
    assert is_total({"motCode": 0, "customsCode": "C00", "partner2Code": 0})
    assert not is_total({"motCode": 2100})
    assert not is_total({"customsCode": "C01"})
    assert not is_total({"partner2Code": 842})


def test_a_call_is_spelled_the_way_comtrade_expects():
    seen = []

    def handler(request):
        seen.append(request)
        return answer([])

    held = stored(partners=("MEX",), flows=("M",))
    fetch(handler, [Request(reporter("USA", partners=["MEX"], flows=["M"]), held=held)])
    annual, monthly = seen
    assert annual.url.path == "/data/v1/get/C/A/HS"
    assert monthly.url.path == "/data/v1/get/C/M/HS"
    assert dict(annual.url.params) == {
        "reporterCode": "842",
        "period": "2024,2025",
        "partnerCode": "484",
        "cmdCode": "AG2",
        "flowCode": "M",
        "motCode": "0",
        "customsCode": "C00",
        "partner2Code": "0",
    }
    assert monthly.url.params["period"].split(",")[:2] == ["202510", "202511"]
    assert annual.headers["Ocp-Apim-Subscription-Key"] == KEY_VALUE
    assert KEY_VALUE not in str(annual.url)


def test_the_world_is_partner_zero():
    seen = []

    def handler(request):
        seen.append(request.url.params["partnerCode"])
        return answer([])

    fetch(handler, [Request(reporter(), held=stored())])
    assert seen == ["0", "0"]


def test_each_call_is_one_batch_with_the_rows_of_the_recorded_answer():
    def handler(request):
        if request.url.path.endswith("/A/HS"):
            return httpx.Response(200, text=fixture("comtrade_hs2.json"))
        return answer([data_row("202606", "M", "87", 5.0, None)])

    annual, monthly = fetch(handler, [Request(reporter(), held=stored())])
    table = annual.tables[0]
    assert (table.key, table.key_columns, table.value_columns) == ("comtrade:MEX", KEY_COLUMNS, VALUE_COLUMNS)
    assert table.stale_after_days == 190
    assert table.attribute_columns == ("level",)
    assert table.name == "Goods trade of MEX by HS product (AG2)"
    assert [(row["flow"], row["product"], row["value_usd"]) for row in table.rows] == [
        ("M", "06", 198635735.0),
        ("X", "27", 1000000.0),
        ("X", "87", 2500000.0),
    ]
    first = table.rows[0]
    assert (first["reporter"], first["partner"], first["frequency"], first["period"]) == ("MEX", "WLD", "A", "2024")
    assert first["level"] == "AG2"
    assert first["date"] == datetime.date(2024, 12, 31)
    assert math.isnan(table.rows[2]["weight_kg"])  # null weight is missing, never zero
    month = monthly.tables[0].rows[0]
    assert (month["frequency"], month["period"], month["date"]) == ("M", "2026-06", datetime.date(2026, 6, 30))
    assert math.isnan(month["weight_kg"])


def test_a_zero_weight_with_trade_is_a_weight_not_reported():
    rows = [data_row(product="06", value=198635735.0, weight=0.0), data_row(product="27", value=0.0, weight=0.0)]
    batches = fetch(lambda _request: answer(rows), [Request(reporter(months=0), held=stored(ALL_YEARS))])
    traded, nothing = batches[0].tables[0].rows
    assert math.isnan(traded["weight_kg"])  # the recorded answer has cmdCode 06 so: 198,635,735 USD and 0 kg
    assert nothing["weight_kg"] == 0.0


def test_breakdown_rows_and_flows_not_asked_for_are_dropped():
    rows = [
        data_row(value=10.0),
        data_row(value=4.0, motCode=2100),
        data_row(value=3.0, customsCode="C01"),
        data_row(value=2.0, partner2Code=842),
        data_row(flow="RX", value=1.0),
    ]
    batches = fetch(lambda _request: answer(rows), [Request(reporter(months=0), held=stored(ALL_YEARS))])
    assert [row["value_usd"] for row in batches[0].tables[0].rows] == [10.0]


def test_an_answer_without_rows_is_still_a_table_so_the_reporter_is_not_a_failure():
    batches = fetch(lambda _request: answer([]), [Request(reporter(months=0), held=stored(ALL_YEARS))])
    assert [len(batch.tables[0].rows) for batch in batches] == [0]
    assert batches[0].failures == ()
    assert batches[0].tables[0].stale_after_days is None  # annual only: the annual threshold applies


def test_without_a_key_nothing_is_requested():
    calls = []
    batches = fetch(calls.append, [Request(reporter()), Request(reporter("USA"))], Credentials())
    assert calls == []
    assert [(failure.entry.source_id, failure.outcome) for failure in batches[0].failures] == [
        ("MEX", Outcome.KEY_ERROR),
        ("USA", Outcome.KEY_ERROR),
    ]
    assert "COMTRADE_API_KEY is missing" in batches[0].failures[0].reason


def test_a_rejected_key_fails_every_reporter_left_and_stops():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(401, text=fixture("comtrade_bad_key.json"))

    batches = fetch(handler, [Request(reporter()), Request(reporter("USA"))])
    assert len(calls) == 1
    assert [failure.outcome for failure in batches[0].failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert "Comtrade rejected the key" in batches[0].failures[0].reason


def test_http_403_means_the_quota_is_used_up():
    with pytest.raises(QuotaExhaustedError, match="HTTP 403: Out of call volume quota"):
        fetch(lambda _request: httpx.Response(403, text="Out of call volume quota"), [Request(reporter())])


def test_a_persistent_429_means_the_quota_is_used_up():
    with pytest.raises(QuotaExhaustedError, match="HTTP 429"):
        fetch(lambda _request: httpx.Response(429), [Request(reporter())])


def test_a_failed_call_fails_its_reporter_and_the_next_one_goes_on():
    def handler(request):
        if request.url.params["reporterCode"] == "484":
            return httpx.Response(500)
        return answer([])

    held = stored()
    batches = fetch(handler, [Request(reporter(), held=held), Request(reporter("USA"), held=held)])
    assert [(failure.entry.source_id, failure.outcome) for failure in batches[0].failures] == [
        ("MEX", Outcome.NETWORK_ERROR)
    ]
    assert [batch.tables[0].key for batch in batches[1:]] == ["comtrade:USA", "comtrade:USA"]


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (httpx.Response(400, text="bad period"), "HTTP 400: bad period"),
        (httpx.Response(200, json={"data": [], "error": "Invalid cmdCode"}), "Invalid cmdCode"),
        (httpx.Response(200, json={"data": [{"period": "2024"}]}), "unexpected answer \\(KeyError"),
        (httpx.Response(200, json={"data": [data_row(period="20x4")]}), "unexpected answer \\(PeriodError"),
    ],
)
def test_an_unexpected_answer_is_a_source_error(response, reason):
    batches = fetch(lambda _request: response, [Request(reporter(months=0), held=stored(ALL_YEARS))])
    failure = batches[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert re.search(reason, failure.reason)


def test_an_answer_at_the_row_cap_is_refused_rather_than_stored_cut_short(monkeypatch):
    monkeypatch.setattr("data_pipeline.store.sources.comtrade.MAX_ROWS", 2)
    batches = fetch(
        lambda _request: answer([data_row(), data_row(product="87")]),
        [Request(reporter(months=0), held=stored(ALL_YEARS))],
    )
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "may be cut short" in batches[0].failures[0].reason


def test_the_key_never_shows_in_a_reason():
    batches = fetch(
        lambda _request: httpx.Response(400, text=f"key {KEY_VALUE} refused"),
        [Request(reporter(months=0), held=stored(ALL_YEARS))],
    )
    assert KEY_VALUE not in batches[0].failures[0].reason


# -- through a sync --------------------------------------------------------------------------------


def test_a_run_stopped_after_the_world_asks_the_next_partner_for_its_whole_history(tmp_path):
    mex = reporter(partners=["WLD", "USA"], months=0, annual_from=2020)

    def rows_asked(request):
        return answer([data_row(period, flow) for period in request.url.params["period"].split(",") for flow in FLOWS])

    def run(handler, now):
        http = client(handler, secrets=(KEY_VALUE,))
        return sync(Storage(tmp_path), [mex], {"comtrade": Comtrade(http, CREDENTIALS, today=lambda: TODAY)}, http, now)

    def quota_after_the_world(request):
        if request.url.params["partnerCode"] == "842":
            return httpx.Response(403, text="Out of call volume quota")
        return rows_asked(request)

    assert run(quota_after_the_world, NOW).sources[0].quota_exhausted
    asked = []

    def recorded(request):
        asked.append((request.url.params["partnerCode"], request.url.params["period"]))
        return rows_asked(request)

    run(recorded, NOW + datetime.timedelta(days=1))
    assert asked == [("0", "2024,2025"), ("842", "2020,2021,2022,2023,2024,2025")]
    table = Storage(tmp_path).read_table("comtrade", "MEX")
    assert set(table["level"]) == {"AG2"}


def rows_asked(request, product="27"):
    """An answer with one row per period and flow asked for."""
    periods = request.url.params["period"].split(",")
    return answer([data_row(period, flow, product) for period in periods for flow in FLOWS])


def sync_comtrade(tmp_path, entries, handler, now=NOW, **options):
    http = client(handler, secrets=(KEY_VALUE,))
    sources = {"comtrade": Comtrade(http, CREDENTIALS, today=lambda: TODAY)}
    return sync(Storage(tmp_path), list(entries), sources, http, now, **options)


def test_a_full_sync_after_a_change_of_level_is_refused_without_a_call(tmp_path):
    # MEX stored at AG2, then `level: AG4` and `sync --full`: AG4 rows next to AG2 rows would count trade twice
    sync_comtrade(tmp_path, [reporter(months=0, annual_from=2023)], rows_asked)
    before = Storage(tmp_path).read_table("comtrade", "MEX")
    calls = []

    def recorded(request):
        calls.append(request)
        return rows_asked(request, product="2709")

    later = NOW + datetime.timedelta(days=1)
    report = sync_comtrade(tmp_path, [reporter(level="AG4", months=0, annual_from=2023)], recorded, later, full=True)
    assert calls == []
    ((key, reason),) = report.sources[0].failed
    assert key == "comtrade:MEX"
    assert "the stored table holds HS level AG2, not AG4" in reason
    assert len(Storage(tmp_path).read_table("comtrade", "MEX")) == len(before)
