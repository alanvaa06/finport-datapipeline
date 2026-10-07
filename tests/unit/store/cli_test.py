import datetime
import json

import click.testing
import httpx
import pytest

from data_pipeline.store import cli as cli_module
from data_pipeline.store.api import Store
from data_pipeline.store.cli import cli
from data_pipeline.store.sync import LOCK_FILE

from .helpers import NOT_IN_ALFRED, NOW

META = {
    "title": "Tasa de desempleo áéí →",
    "units": "Percent",
    "frequency_short": "M",
    "seasonal_adjustment_short": "SA",
}
OBSERVATIONS = [{"date": "2026-04-01", "value": "4.0"}, {"date": "2026-05-01", "value": "4.1"}]


def handler(request):
    if request.url.params["series_id"] != "UNRATE":
        return httpx.Response(400, text='{"error_message":"Bad Request.  The series does not exist."}')
    if "realtime_start" in request.url.params:  # kept by FRED, not by ALFRED: dated by its fetch
        return httpx.Response(400, json=NOT_IN_ALFRED)
    if request.url.path.endswith("/observations"):
        return httpx.Response(200, text=json.dumps({"observations": OBSERVATIONS}))
    return httpx.Response(200, text=json.dumps({"seriess": [META]}))


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """A folder with a catalog and a key; the CLI talks to a fake FRED at a fixed time."""
    (tmp_path / "catalog.yaml").write_text("- source: fred\n  ids: [UNRATE]\n", encoding="utf-8")
    (tmp_path / ".env").write_text("FRED_API_KEY=0123456789abcdef0123456789abcdef\n", encoding="utf-8")
    state = {"now": NOW}

    def open_store(root, catalog, env_file):
        return Store(
            root,
            catalog,
            env_file=env_file or tmp_path / ".env",
            clock=lambda: state["now"],
            transport=httpx.MockTransport(handler),
            sleep=lambda _seconds: None,
        )

    monkeypatch.setattr(cli_module, "open_store", open_store)
    monkeypatch.chdir(tmp_path)
    return tmp_path, state


def invoke(*arguments, env=None):
    return click.testing.CliRunner().invoke(cli, list(arguments), env=env)


def test_sync_prints_the_report_and_exits_zero(workspace):
    root, _ = workspace
    result = invoke("sync", "--root", str(root / "store"))
    assert result.exit_code == 0
    assert result.output == "[ok]  fred        1 series, 2 new, 0 revised, 3 calls\n"


def test_the_root_can_come_from_the_environment(workspace):
    root, _ = workspace
    result = invoke("sync", env={"DATA_PIPELINE_ROOT": str(root / "store")})
    assert result.exit_code == 0
    assert (root / "store" / "index.parquet").exists()


def test_without_a_root_the_command_is_refused(workspace):
    result = invoke("sync")
    assert result.exit_code == 2
    assert "--root" in result.output


def test_failures_exit_one(workspace):
    root, _ = workspace
    (root / "catalog.yaml").write_text("- source: fred\n  ids: [UNRATE, NOPE]\n", encoding="utf-8")
    result = invoke("sync", "--root", str(root / "store"))
    assert result.exit_code == 1
    assert "[x]   fred        1 of 2 series" in result.output
    assert "fred:NOPE  not_found:" in result.output


def test_a_missing_key_is_a_clear_message_without_a_traceback(workspace):
    root, _ = workspace
    result = invoke("sync", "--root", str(root / "store"), "--env-file", str(root / "absent.env"))
    assert result.exit_code == 1
    assert result.output == (
        "[x]   fred        key_error: FRED_API_KEY is missing. "
        "Get one at https://fredaccount.stlouisfed.org/apikey and run: data-pipeline setup\n"
    )
    assert "Traceback" not in result.output


def test_configuration_errors_exit_two(workspace):
    root, _ = workspace
    missing = invoke("sync", "--root", str(root / "store"), "--catalog", str(root / "absent.yaml"))
    assert missing.exit_code == 2
    assert "the catalog file does not exist" in missing.output
    (root / "store").mkdir()
    (root / "store" / LOCK_FILE).write_text("1", encoding="ascii")
    locked = invoke("sync", "--root", str(root / "store"))
    assert locked.exit_code == 2
    assert "Delete it by hand" in locked.output


def test_status_lists_each_series_and_exits_by_freshness(workspace):
    root, state = workspace
    invoke("sync", "--root", str(root / "store"))
    fresh = invoke("status", "--root", str(root / "store"))
    assert fresh.exit_code == 0
    assert fresh.output == "[ok]  fred:UNRATE  2026-05  fetched 2026-06-06\n"
    state["now"] = NOW + datetime.timedelta(days=200)
    stale = invoke("status", "--root", str(root / "store"))
    assert stale.exit_code == 1
    assert stale.output.startswith("[x]   fred:UNRATE  stale  last data 2026-05-31")


def test_status_with_a_catalog_reports_missing_series(workspace):
    root, _ = workspace
    result = invoke("status", "--root", str(root / "store"), "--catalog", str(root / "catalog.yaml"))
    assert result.exit_code == 1
    assert result.output == "[x]   fred:UNRATE  missing  declared in the catalog, never downloaded\n"


def test_show_prints_the_citation_and_the_last_rows_in_ascii(workspace):
    root, _ = workspace
    invoke("sync", "--root", str(root / "store"))
    result = invoke("show", "fred:UNRATE", "--root", str(root / "store"))
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "[FRED: UNRATE, 2026-05, fetched 2026-06-06]"
    assert lines[1] == "Tasa de desempleo ??? ? | Percent | M | SA"
    assert lines[2:] == ["2026-04  4.0", "2026-05  4.1"]
    assert result.output.isascii()


def test_show_as_of_and_unknown_keys(workspace):
    root, _ = workspace
    invoke("sync", "--root", str(root / "store"))
    before = invoke("show", "fred:UNRATE", "--root", str(root / "store"), "--as-of", "2026-01-01")
    assert before.output.splitlines()[2:] == []
    unknown = invoke("show", "fred:NOPE", "--root", str(root / "store"))
    assert unknown.exit_code == 2
    assert unknown.output == "[x]   no stored series has key 'fred:NOPE'\n"
