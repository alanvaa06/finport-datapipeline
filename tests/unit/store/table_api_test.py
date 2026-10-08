import datetime

import click.testing
import httpx
import pytest

from data_pipeline.store import cli as cli_module
from data_pipeline.store import sources as source_registry
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.errors import StoreError, UnknownSeriesError
from data_pipeline.store.sources.comtrade import Comtrade
from data_pipeline.store.storage import Storage

from .helpers import NOT_IN_ALFRED, NOW

LATER = NOW + datetime.timedelta(days=30)
CATALOG = """
- source: comtrade
  ids: [MEX, USA]
  annual_from: 2024
  months: 2
- source: fred
  ids: [UNRATE]
"""
ENV = "COMTRADE_API_KEY=0123456789abcdef0123456789abcdef\nFRED_API_KEY=0123456789abcdef0123456789abcdef\n"
REPORTERS = {"484": "MEX", "842": "USA"}


class Services:
    """Fake Comtrade and FRED. `values` maps (reporter, period as asked, flow, product) to a value."""

    def __init__(self):
        self.values = {
            ("MEX", "2024", "X", "27"): 100.0,
            ("MEX", "2024", "M", "27"): 40.0,
            ("MEX", "2025", "X", "27"): 110.0,
            ("MEX", "202604", "X", "27"): 9.0,
            ("MEX", "202605", "X", "27"): 10.0,
            ("USA", "2025", "X", "87"): 500.0,
        }
        self.calls = 0

    def __call__(self, request):
        if "stlouisfed" in request.url.host:
            if "realtime_start" in request.url.params:  # kept by FRED, not by ALFRED
                return httpx.Response(400, json=NOT_IN_ALFRED)
            if request.url.path.endswith("/observations"):
                return httpx.Response(200, json={"observations": [{"date": "2026-05-01", "value": "4.1"}]})
            meta = {"title": "Unemployment", "units": "Percent", "frequency_short": "M"}
            return httpx.Response(200, json={"seriess": [{**meta, "seasonal_adjustment_short": "SA"}]})
        self.calls += 1
        reporter = REPORTERS[request.url.params["reporterCode"]]
        periods = request.url.params["period"].split(",")
        rows = [
            {"period": period, "flowCode": flow, "cmdCode": product, "primaryValue": value, "netWgt": 1.0}
            for (owner, period, flow, product), value in self.values.items()
            if owner == reporter and period in periods
        ]
        return httpx.Response(200, json={"data": rows, "error": ""})


@pytest.fixture
def services():
    return Services()


@pytest.fixture
def clock():
    return {"now": NOW}


@pytest.fixture
def store(tmp_path, services, clock, monkeypatch):
    def comtrade(client, credentials):
        return Comtrade(client, credentials, today=NOW.date)

    monkeypatch.setitem(source_registry.REGISTRY, "comtrade", comtrade)
    (tmp_path / "catalog.yaml").write_text(CATALOG, encoding="utf-8")
    (tmp_path / ".env").write_text(ENV, encoding="utf-8")
    built = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: clock["now"],
        transport=httpx.MockTransport(services),
        sleep=lambda _seconds: None,
    )
    built.sync()
    return built


def test_sync_stores_a_table_per_reporter(store, services):
    assert services.calls == 4  # per reporter: one annual call (2024, 2025) and one monthly (2 months)
    assert len(store.table("comtrade", "MEX")) == 5
    assert len(store.table("comtrade", "USA")) == 1
    assert len(store.table("comtrade")) == 6


def test_table_returns_the_key_columns_the_date_and_the_values(store):
    table = store.table("comtrade", "MEX")
    assert list(table.columns) == [
        "reporter", "partner", "flow", "product", "frequency", "period", "date", "value_usd", "weight_kg", "level",
    ]  # fmt: skip
    assert list(table["period"]) == ["2024", "2024", "2025", "2026-04", "2026-05"]


def test_a_table_written_before_a_column_existed_reads_it_as_missing(store, tmp_path):
    storage = Storage(tmp_path / "store")
    older = storage.read_table("comtrade", "USA").drop(columns="level")  # as stored before `level` existed
    older.to_parquet(storage.table_path("comtrade", "USA"))
    both = store.table("comtrade")
    assert list(both["level"].isna()) == [False] * 5 + [True]
    assert list(store.table("comtrade", level="AG2")["reporter"].unique()) == ["MEX"]


def test_filters_keep_the_rows_whose_column_equals_the_value(store):
    exports = store.table("comtrade", "MEX", flow="X", frequency="A")
    assert dict(zip(exports["period"], exports["value_usd"], strict=True)) == {"2024": 100.0, "2025": 110.0}
    assert list(store.table("comtrade", product="87")["reporter"]) == ["USA"]
    assert store.table("comtrade", "MEX", flow="RX").empty


def test_an_unknown_filter_column_is_an_error(store):
    with pytest.raises(StoreError, match="unknown column\\(s\\) for a comtrade table: hs"):
        store.table("comtrade", "MEX", hs="27")


def test_an_unknown_table_is_an_error(store):
    with pytest.raises(UnknownSeriesError, match="no stored table of source 'comtrade' has id 'CHN'"):
        store.table("comtrade", "CHN")
    with pytest.raises(UnknownSeriesError, match="no table is stored for source 'fred'"):
        store.table("fred")


def test_as_of_shows_the_table_as_it_was_known(store, services, clock):
    services.values[("MEX", "2025", "X", "27")] = 125.0
    clock["now"] = LATER
    report = store.sync(sources=["comtrade"])
    assert (report.sources[0].new, report.sources[0].revised) == (0, 1)
    now = store.table("comtrade", "MEX", flow="X", period="2025")
    before = store.table("comtrade", "MEX", flow="X", period="2025", as_of=NOW)
    assert (list(now["value_usd"]), list(before["value_usd"])) == ([125.0], [110.0])
    assert store.table("comtrade", "MEX", as_of=NOW - datetime.timedelta(days=1)).empty


def test_a_second_sync_adds_nothing(store, clock):
    clock["now"] = NOW + datetime.timedelta(hours=1)
    report = store.sync(sources=["comtrade"])
    assert (report.sources[0].new, report.sources[0].revised, report.sources[0].calls) == (0, 0, 4)


def test_info_cites_a_table_and_series_refuses_it(store):
    info = store.info("comtrade:MEX")
    assert info.kind == "table"
    assert info.label == "[UN Comtrade: MEX, 2026-05, fetched 2026-06-06]"
    assert store.info("fred:UNRATE").kind == "series"
    with pytest.raises(StoreError, match="comtrade:MEX is a table: read it with table\\('comtrade', 'MEX'\\)"):
        store.series("comtrade:MEX")


def test_status_covers_tables(store, clock):
    states = store.status().set_index("key")["state"].to_dict()
    assert states == {"comtrade:MEX": "ok", "comtrade:USA": "ok", "fred:UNRATE": "ok"}
    clock["now"] = datetime.datetime(2026, 12, 8, tzinfo=datetime.UTC)  # 191 days after 2026-05-31
    row = store.status().set_index("key").loc["comtrade:MEX"]
    assert (row["state"], row["reason"]) == ("stale", "last data 2026-05-31 (191 days ago)")


def test_the_index_marks_each_kind(store):
    kinds = store.index().set_index("key")["kind"].to_dict()
    assert kinds == {"comtrade:MEX": "table", "comtrade:USA": "table", "fred:UNRATE": "series"}


def test_show_prints_the_citation_and_the_newest_rows_of_a_table(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    result = click.testing.CliRunner().invoke(cli, ["show", "--root", str(tmp_path / "store"), "comtrade:MEX"])
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[UN Comtrade: MEX, 2026-05, fetched 2026-06-06]"
    assert lines[1] == "Goods trade of MEX by HS product (AG2) | 5 rows"
    assert lines[2] == "reporter  partner  flow  product  frequency  period  value_usd  weight_kg  level"
    assert lines[-1] == "MEX  WLD  X  27  M  2026-05  10.0  1.0  AG2"
    assert len(lines) == 8


def test_show_a_table_as_of_a_date(store, tmp_path, monkeypatch):
    monkeypatch.setattr(cli_module, "open_store", lambda root, _catalog, _env: Store(root))
    arguments = ["show", "--root", str(tmp_path / "store"), "comtrade:MEX", "--as-of", "2026-01-01"]
    result = click.testing.CliRunner().invoke(cli, arguments)
    assert result.exit_code == 0
    assert result.output.splitlines()[1].endswith("| 0 rows")
