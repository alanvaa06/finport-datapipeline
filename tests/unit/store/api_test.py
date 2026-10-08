import datetime
import json

import httpx
import pandas as pd
import pytest

import data_pipeline
from data_pipeline.store import storage as st
from data_pipeline.store.api import Store
from data_pipeline.store.errors import CatalogError, StoreError, UnknownSeriesError

from .helpers import NOT_IN_ALFRED, NOW

CATALOG = """
- source: fred
  ids: [UNRATE, DGS10]
  alias:
    UNRATE: usa.empleo.desempleo
"""
META = {
    "UNRATE": {
        "title": "Unemployment Rate",
        "units": "Percent",
        "frequency_short": "M",
        "seasonal_adjustment_short": "SA",
    },
    "DGS10": {
        "title": "10-Year Treasury",
        "units": "Percent",
        "frequency_short": "D",
        "seasonal_adjustment_short": "NSA",
    },
}


class Fred:
    """A fake FRED server whose observations the test can change between syncs."""

    def __init__(self):
        self.observations = {
            "UNRATE": {"2026-04-01": "4.0", "2026-05-01": "4.1"},
            "DGS10": {"2026-06-04": "4.40", "2026-06-05": "4.45"},
        }
        self.vintages: dict[str, list[dict[str, str]]] = {}  # series kept by ALFRED: its rows
        self.requests = 0

    def __call__(self, request):
        self.requests += 1
        series_id = request.url.params["series_id"]
        if series_id not in META:
            return httpx.Response(400, text='{"error_message":"Bad Request.  The series does not exist."}')
        if "realtime_start" in request.url.params:
            if series_id not in self.vintages:
                return httpx.Response(400, json=NOT_IN_ALFRED)
            return httpx.Response(200, json={"observations": self.vintages[series_id]})
        if request.url.path.endswith("/observations"):
            rows = [{"date": date, "value": value} for date, value in self.observations[series_id].items()]
            return httpx.Response(200, text=json.dumps({"observations": rows}))
        return httpx.Response(200, text=json.dumps({"seriess": [META[series_id]]}))


class Clock:
    def __init__(self):
        self.now = NOW

    def __call__(self):
        return self.now


@pytest.fixture
def world(tmp_path):
    """(store, fake FRED, clock) with a catalog and a key on disk."""
    (tmp_path / "catalog.yaml").write_text(CATALOG, encoding="utf-8")
    (tmp_path / ".env").write_text("FRED_API_KEY=0123456789abcdef0123456789abcdef\n", encoding="utf-8")
    server = Fred()
    clock = Clock()
    store = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=clock,
        transport=httpx.MockTransport(server),
        sleep=lambda _seconds: None,
    )
    return store, server, clock


def test_the_package_exports_store():
    assert data_pipeline.Store is Store


def test_sync_then_read_a_series(world):
    store, _, _ = world
    report = store.sync()
    assert report.exit_code == 0
    frame = store.series("fred:UNRATE")
    assert list(frame.columns) == ["date", "period", "value"]
    assert list(frame["period"]) == ["2026-04", "2026-05"]
    assert list(frame["value"]) == [4.0, 4.1]
    assert frame["date"].iloc[-1] == pd.Timestamp("2026-05-31")


def test_reading_needs_no_catalog_no_key_and_no_network(world, tmp_path):
    store, server, _ = world
    store.sync()
    calls = server.requests
    reader = Store(tmp_path / "store")
    assert list(reader.series("fred:UNRATE")["value"]) == [4.0, 4.1]
    assert server.requests == calls


def test_an_alias_reads_the_same_series(world):
    store, _, _ = world
    store.sync()
    pd.testing.assert_frame_equal(store.series("usa.empleo.desempleo"), store.series("fred:UNRATE"))


@pytest.mark.parametrize(("name", "message"), [("fred:NOPE", "key 'fred:NOPE'"), ("nope", "alias 'nope'")])
def test_an_unknown_name_raises(world, name, message):
    store, _, _ = world
    store.sync()
    with pytest.raises(UnknownSeriesError, match=message):
        store.series(name)


def test_as_of_returns_the_value_known_then_and_revisions_show_both(world):
    store, server, clock = world
    store.sync()
    clock.now = NOW + datetime.timedelta(days=28)
    server.observations["UNRATE"]["2026-05-01"] = "4.2"
    report = store.sync()
    assert report.sources[0].revised == 1
    assert list(store.series("fred:UNRATE")["value"]) == [4.0, 4.2]
    assert list(store.series("fred:UNRATE", as_of="2026-06-15")["value"]) == [4.0, 4.1]
    assert store.series("fred:UNRATE", as_of="2026-01-01").empty
    history = store.revisions("fred:UNRATE")
    assert list(history["period"]) == ["2026-04", "2026-05", "2026-05"]
    assert list(history["value"]) == [4.0, 4.1, 4.2]
    assert "key" not in history.columns


def test_missing_values_and_open_periods_are_left_out(world):
    store, server, _ = world
    server.observations["UNRATE"]["2026-03-01"] = "."
    server.observations["UNRATE"]["2026-06-01"] = "4.3"
    store.sync()
    assert list(store.series("fred:UNRATE")["period"]) == ["2026-04", "2026-05"]
    with_projections = store.series("fred:UNRATE", projections=True)
    assert list(with_projections["period"]) == ["2026-04", "2026-05", "2026-06"]
    assert list(with_projections["projection"]) == [False, False, True]
    assert len(store.revisions("fred:UNRATE")) == 4


def test_start_and_end_filter_by_date(world):
    store, _, _ = world
    store.sync()
    assert list(store.series("fred:UNRATE", start="2026-05-01")["period"]) == ["2026-05"]
    assert list(store.series("fred:UNRATE", end=datetime.date(2026, 4, 30))["period"]) == ["2026-04"]


def test_frame_joins_on_date_and_leaves_gaps(world):
    store, _, _ = world
    store.sync()
    frame = store.frame(["fred:UNRATE", "fred:DGS10"])
    assert list(frame.columns) == ["fred:UNRATE", "fred:DGS10"]
    assert len(frame) == 4
    assert frame["fred:UNRATE"].notna().sum() == 2
    assert pd.isna(frame.loc[pd.Timestamp("2026-06-05"), "fred:UNRATE"])
    assert store.frame([]).empty


def test_info_and_its_label(world):
    store, _, _ = world
    store.sync()
    info = store.info("usa.empleo.desempleo")
    assert (info.key, info.alias, info.name) == ("fred:UNRATE", "usa.empleo.desempleo", "Unemployment Rate")
    assert (info.units, info.frequency, info.seasonal_adjustment, info.status) == ("Percent", "M", "SA", "ok")
    assert info.label == "[FRED: UNRATE, 2026-05, fetched 2026-06-06]"
    assert store.info("fred:DGS10").alias == ""


def test_status_reports_ok_stale_failed_and_missing(world):
    store, _, clock = world
    store.add("fred", ["NOPE"])
    store.sync()
    store.add("fred", ["LATER"])
    clock.now = NOW + datetime.timedelta(days=20)
    status = store.status().set_index("key")
    assert list(status.columns) == ["alias", "source", "last_period", "last_fetched_at", "state", "reason"]
    assert status.loc["fred:UNRATE", "state"] == "ok"
    assert status.loc["fred:DGS10", "state"] == "stale"
    assert status.loc["fred:DGS10", "reason"] == "last data 2026-06-05 (21 days ago)"
    assert status.loc["fred:NOPE", "state"] == "failed"
    assert status.loc["fred:LATER", "state"] == "missing"


def test_index_returns_the_whole_index(world):
    store, _, _ = world
    store.sync()
    assert sorted(store.index()["key"]) == ["fred:DGS10", "fred:UNRATE"]


def test_sync_can_be_restricted(world):
    store, server, _ = world
    store.sync(keys=["fred:DGS10"])
    assert sorted(store.index()["key"]) == ["fred:DGS10"]
    assert server.requests == 3  # metadata, the vintages ALFRED does not keep, the observations
    assert store.sync(sources=["bls"]).sources == ()


def test_add_with_an_unknown_source_fails_at_sync(world):
    store, _, _ = world
    store.add("nowhere", ["X"])
    with pytest.raises(CatalogError, match="unknown source 'nowhere'"):
        store.sync()


def test_a_missing_key_fails_every_series_with_a_clear_reason(world, tmp_path):
    _, server, clock = world
    store = Store(
        tmp_path / "other",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / "absent.env",
        clock=clock,
        transport=httpx.MockTransport(server),
    )
    report = store.sync()
    assert report.exit_code == 1
    assert report.lines() == [
        (
            "[x]   fred        key_error: FRED_API_KEY is missing. "
            "Get one at https://fredaccount.stlouisfed.org/apikey and run: data-pipeline setup"
        )
    ]
    assert server.requests == 0


def test_a_key_passed_in_code_is_used_without_any_file(world, tmp_path):
    _, server, clock = world
    store = Store(
        tmp_path / "other",
        tmp_path / "catalog.yaml",
        credentials={"FRED_API_KEY": "0123456789abcdef0123456789abcdef"},
        env_file=tmp_path / "absent.env",
        clock=clock,
        transport=httpx.MockTransport(server),
        sleep=lambda _seconds: None,
    )
    assert store.sync().exit_code == 0
    assert server.requests > 0


def test_the_nearest_env_file_of_a_parent_folder_is_found(world, tmp_path, monkeypatch):
    _, server, clock = world  # the fixture wrote tmp_path/.env with the FRED key
    folder = tmp_path / "sub"
    folder.mkdir()
    monkeypatch.chdir(folder)
    store = Store(
        tmp_path / "other",
        tmp_path / "catalog.yaml",
        clock=clock,
        transport=httpx.MockTransport(server),
        sleep=lambda _seconds: None,
    )
    assert store.sync().exit_code == 0
    assert server.requests > 0


def test_add_accepts_a_name_and_a_frequency(world):
    store, _, _ = world
    store.add("fred", ["UNRATE2"], name="Curated", frequency="M")
    added = store._entries[-1]
    assert (added.name, added.frequency.value) == ("Curated", "M")


def test_a_bundled_catalog_is_loaded_by_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = Store(tmp_path / "store", "macro")
    status = store.status()
    assert len(status) == 1293
    assert set(status["state"]) == {"missing"}
    assert status.set_index("alias").loc["e_us_cpi", "source"] == "fred"


def test_an_unknown_credential_name_is_refused_when_the_store_is_built(tmp_path):
    with pytest.raises(StoreError, match=r"Unknown credential: FRED_KEY. Known: FRED_API_KEY, BLS_API_KEY"):
        Store(tmp_path / "store", credentials={"FRED_KEY": "x"})


def test_a_series_with_vintages_reads_as_it_was_published_before_the_first_sync(world):
    store, server, _clock = world
    server.vintages["UNRATE"] = [
        {"realtime_start": "2026-05-08", "date": "2026-04-01", "value": "4.0"},
        {"realtime_start": "2026-06-05", "date": "2026-04-01", "value": "4.1"},
        {"realtime_start": "2026-06-05", "date": "2026-05-01", "value": "4.2"},
    ]
    store.sync(keys=["fred:UNRATE"])
    assert list(store.series("fred:UNRATE")["value"]) == [4.1, 4.2]
    assert list(store.series("fred:UNRATE", as_of="2026-05-31")["value"]) == [4.0]
    assert store.series("fred:UNRATE", as_of="2026-05-07").empty
    assert list(store.series("fred:UNRATE", as_of="2026-06-05T12:00:00+00:00")["value"]) == [4.0]  # not out yet
    assert list(store.series("fred:UNRATE", as_of="2026-06-05")["value"]) == [4.1, 4.2]
    history = store.revisions("fred:UNRATE")
    assert list(history["value"]) == [4.0, 4.1, 4.2]
    assert list(history["published_at"].dt.date.astype(str)) == ["2026-05-08", "2026-06-05", "2026-06-05"]


def give_alias_to(store_root, key, alias):
    """Leave `alias` on another stored series too, as a store synced with an older catalog has it."""
    storage = st.Storage(store_root)
    index = storage.read_index()
    index.loc[index["key"] == key, "alias"] = alias
    storage.write_index(index)


def test_an_alias_on_several_stored_series_is_refused_without_a_catalog(world, tmp_path):
    store, _server, _clock = world
    store.sync()
    give_alias_to(tmp_path / "store", "fred:DGS10", "usa.empleo.desempleo")
    with pytest.raises(StoreError, match=r"fred:DGS10, fred:UNRATE"):
        Store(tmp_path / "store").series("usa.empleo.desempleo")


def test_the_catalog_decides_which_stored_series_an_alias_names(world, tmp_path):
    store, _server, _clock = world
    store.sync()
    give_alias_to(tmp_path / "store", "fred:DGS10", "usa.empleo.desempleo")
    moved = "- source: fred\n  id: DGS10\n  alias: usa.empleo.desempleo\n"
    (tmp_path / "moved.yaml").write_text(moved, encoding="utf-8")
    reader = Store(tmp_path / "store", tmp_path / "moved.yaml")
    assert reader.info("usa.empleo.desempleo").key == "fred:DGS10"


def test_an_alias_the_catalog_moved_to_a_series_not_stored_yet_asks_for_a_sync(world, tmp_path):
    store, _server, _clock = world
    store.sync()
    moved = "- source: fred\n  id: PAYEMS\n  alias: usa.empleo.desempleo\n"
    (tmp_path / "moved.yaml").write_text(moved, encoding="utf-8")
    with pytest.raises(UnknownSeriesError, match=r"fred:PAYEMS.*sync"):
        Store(tmp_path / "store", tmp_path / "moved.yaml").series("usa.empleo.desempleo")


def test_a_sync_that_crosses_midnight_dates_each_series_by_its_own_download(world, tmp_path):
    _, server, _ = world
    store = Store(
        tmp_path / "store",
        tmp_path / "catalog.yaml",
        env_file=tmp_path / ".env",
        clock=lambda: NOW + datetime.timedelta(hours=3 * server.requests),  # each request takes three hours
        transport=httpx.MockTransport(server),
        sleep=lambda _seconds: None,
    )
    store.sync()
    assert not store.series("fred:UNRATE", as_of="2026-06-06").empty
    assert store.series("fred:DGS10", as_of="2026-06-06").empty  # downloaded after midnight UTC
    assert not store.series("fred:DGS10", as_of="2026-06-07").empty


def test_a_source_that_is_down_is_given_up_after_three_series(tmp_path):
    def down(request):
        msg = "timed out"
        raise httpx.ReadTimeout(msg, request=request)

    env = tmp_path / "empty.env"
    env.write_text("", encoding="utf-8")
    store = Store(tmp_path / "store", env_file=env, transport=httpx.MockTransport(down), sleep=lambda _seconds: None)
    store.add("dbnomics", [f"IMF/IFS/M.X{number}.PCPI_IX" for number in range(30)])
    report = store.sync()
    (dbnomics,) = report.sources
    assert dbnomics.calls == 12  # three series of four attempts, instead of 120
    assert len(dbnomics.failed) == 30
    assert all(reason.startswith("network_error: ") for _, reason in dbnomics.failed)
    assert report.exit_code == 1
