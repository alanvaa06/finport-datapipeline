# Store series sources, group A (BLS, Banxico, INEGI, DBnomics, macro catalog) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four sources to `data_pipeline.store` (BLS, Banxico SIE, INEGI, DBnomics) and ship the curated macro catalog inside the library, so one `sync` fills a local store with them.

**Architecture:** Each source is one file under `store/sources/` that implements the existing `Source` protocol; nothing in the sync engine changes shape. The core gains three small things: `Client.post` (BLS needs it), two catalog fields (`frequency`, `name`) with a one-series spelling of an entry, and catalogs bundled in the package and loaded by name.

**Tech Stack:** Python 3.12+, httpx, pandas, pyarrow, PyYAML, click; pytest, hypothesis, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-10-02-store-series-sources-a-design.md`

---

## Before you start

**How this plan was checked.** Every source and test file below was run together in a scratch clone of this repository on 2026-10-02: 1,175 tests pass (the 1,103 that existed plus 72 new), 5 live tests deselected, `ruff check` clean, `mypy` clean. The acceptance scripts were run offline against stores seeded from the other repositories' own data (1,107 BLS series, 13 Banxico, 5 INEGI: every series matched). Not run: the five live tests and every step of Task 12, which call real services with the user's keys.

**Two answer shapes are taken from documentation, not from recorded responses:** BLS (period codes, the `catalog` block, `REQUEST_NOT_PROCESSED`) and Banxico's metadata (`periodicidad`, `unidad`). Their live tests in Task 12 are what confirm them. If one fails, fix the source and its unit test together.

**Environment.** Windows, commands for Git Bash, run from `C:/Proyectos/data_pipeline`. The virtual environment is `.venv` (uv).

| What | Command |
|---|---|
| One test file | `.venv/Scripts/python -m pytest tests/unit/store/bls_test.py -q` |
| The whole suite | `.venv/Scripts/python -m pytest -q` |
| Lint | `.venv/Scripts/python -m ruff check .` |
| Types | `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy` |

**Rules of this repository.**

- Commits carry no `Co-Authored-By` line. Only the user's name appears in this repository.
- Ruff is strict: build exception messages in a `msg` variable, exception names end in `Error`, no boolean positional parameters, no `print` outside `scripts/`, lines up to 120 characters.
- Console output is ASCII only.
- `data_pipeline.store` and `data_pipeline.equity` never import each other; a test enforces it.
- Call the environment's Python (`.venv/Scripts/python`), never a bare `python`: on this machine the bare name opens the Microsoft Store stub and hangs.

**A known cost, accepted for now.** `sync` rewrites a source's parquet file after every batch. For sources that send one series per batch (Banxico, INEGI, DBnomics) the time per batch grows with the file. Measured: 1,107 one-series batches into one file took about seven minutes of pure storage work. BLS is not affected (50 series per batch). For DBnomics it adds a few minutes to a first sync that already takes about 21 minutes of paced requests.

## File structure

| File | Responsibility |
|---|---|
| `src/data_pipeline/store/http.py` | Modified: `post`, sharing the retry loop with `get` |
| `src/data_pipeline/store/model.py` | Modified: `CatalogEntry.name`, `CatalogEntry.frequency` |
| `src/data_pipeline/store/catalog.py` | Modified: the `id` spelling, `frequency`, `name`, bundled catalogs |
| `src/data_pipeline/store/catalogs/__init__.py`, `macro.yaml` | New: the catalogs shipped with the library |
| `src/data_pipeline/store/sync.py`, `src/data_pipeline/store/api.py` | Modified: the catalog's name wins; `add` takes `name` and `frequency` |
| `src/data_pipeline/store/sources/base.py` | Modified: `missing_key`, `utc_today`, allowed fields, unreadable periods |
| `src/data_pipeline/store/sources/bls.py`, `banxico.py`, `inegi.py`, `dbnomics.py` | New: the four sources |
| `src/data_pipeline/store/sources/__init__.py` | Modified: registry and citation titles |
| `scripts/convert_macro_catalog.py` | New: JSON macro catalog to the bundled YAML |
| `scripts/compare_with_investment_process.py` | Renamed and generalized from the FRED-only script |
| `scripts/bls_catalog_from_unemployment_analysis.py`, `compare_bls_with_unemployment_analysis.py` | New: BLS acceptance |
| `tests/unit/store/*_test.py`, `tests/live/*_live_test.py` | Tests |

---

### Task 1: `Client.post`

BLS only accepts POST with a JSON body. `get` and `post` share one private method, so pacing, retries, call counting and scrubbing exist once.

**Files:**
- Modify: `src/data_pipeline/store/http.py`
- Test: `tests/unit/store/http_test.py`

- [ ] **Step 1: Write the failing tests**

Replace `tests/unit/store/http_test.py` with (the last two tests are new):

```python
import json

import httpx
import pytest

from data_pipeline.store.errors import NetworkError
from data_pipeline.store.http import Client, scrub


def test_returns_a_non_retryable_answer_as_it_is():
    waits = []
    client = Client(transport=httpx.MockTransport(lambda _request: httpx.Response(400, text="bad")), sleep=waits.append)
    response = client.get("fred", "https://example.test/x")
    assert response.status_code == 400
    assert waits == []
    assert client.calls == {"fred": 1}


def test_retries_then_succeeds():
    answers = iter([httpx.Response(503), httpx.Response(429), httpx.Response(200, text="ok")])
    waits = []
    client = Client(
        transport=httpx.MockTransport(lambda _request: next(answers)),
        sleep=waits.append,
        clock=lambda: 0.0,
    )
    response = client.get("fred", "https://example.test/x", per_minute=6_000_000)
    assert response.text == "ok"
    assert [wait for wait in waits if wait >= 2.0] == [2.0, 4.0]
    assert client.calls == {"fred": 3}
    assert client.throttled == {"fred": 1}


def test_gives_up_after_every_retry_with_a_scrubbed_message():
    def handler(_request):
        msg = "no route to host for key SECRET123"
        raise httpx.ConnectError(msg)

    client = Client(secrets=("SECRET123",), transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    with pytest.raises(NetworkError, match="after 4 attempts") as raised:
        client.get("fred", "https://example.test/x")
    assert "SECRET123" not in str(raised.value)
    assert "***" in str(raised.value)


def test_waits_between_calls_to_the_same_source_only():
    now = [100.0]
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        now[0] += seconds

    client = Client(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
        sleep=sleep,
        clock=lambda: now[0],
    )
    client.get("fred", "https://example.test/x", per_minute=60)
    client.get("fred", "https://example.test/x", per_minute=60)
    client.get("bls", "https://example.test/x", per_minute=60)
    assert waits == [1.0]


def test_scrub_hides_every_secret_and_ignores_empty_ones():
    assert scrub("key=abc&token=xyz", ["abc", "", "xyz"]) == "key=***&token=***"


def test_post_sends_a_json_body_and_counts_as_a_call():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"ok": True})

    client = Client(transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    response = client.post("bls", "https://example.test/x", json={"seriesid": ["A"]})
    assert response.json() == {"ok": True}
    assert seen[0].method == "POST"
    assert json.loads(seen[0].content) == {"seriesid": ["A"]}
    assert client.calls == {"bls": 1}


def test_post_retries_and_scrubs_like_get():
    answers = iter([httpx.Response(503), httpx.Response(200, text="ok")])
    client = Client(transport=httpx.MockTransport(lambda _request: next(answers)), sleep=lambda _seconds: None)
    assert client.post("bls", "https://example.test/x", json={}, per_minute=6_000_000).text == "ok"
    assert client.calls == {"bls": 2}

    def refuse(_request):
        msg = "connection refused for key SECRET123"
        raise httpx.ConnectError(msg)

    failing = Client(secrets=("SECRET123",), transport=httpx.MockTransport(refuse), sleep=lambda _seconds: None)
    with pytest.raises(NetworkError, match="after 4 attempts") as raised:
        failing.post("bls", "https://example.test/x", json={"registrationkey": "SECRET123"})
    assert "SECRET123" not in str(raised.value)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/http_test.py -q`
Expected: `2 failed, 5 passed`, both with `AttributeError: 'Client' object has no attribute 'post'`

- [ ] **Step 3: Write the implementation**

Replace `src/data_pipeline/store/http.py` with:

```python
"""The one HTTP client of the store: per-source pacing, retries on 429/5xx and network errors
(waits 2, 4, 8 s), and secrets scrubbed from every message it raises.

Non-retryable answers (200, 400, 401, 404...) are returned as they are: each source reads its
own error format.
"""

import time
import types
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx

from data_pipeline.store.errors import NetworkError

TIMEOUT = 90.0
RETRIES = 3
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
TOO_MANY_REQUESTS = 429
DEFAULT_PER_MINUTE = 30
USER_AGENT = "finport-datapipeline (public data store)"
HIDDEN = "***"


def scrub(text: str, secrets: Sequence[str]) -> str:
    """Replace every secret found in `text` with ***."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, HIDDEN)
    return text


class Client:
    def __init__(
        self,
        secrets: Sequence[str] = (),
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        retries: int = RETRIES,
    ) -> None:
        self._secrets = tuple(secrets)
        self._http = httpx.Client(
            transport=transport,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        self._sleep = sleep
        self._clock = clock
        self._retries = retries
        self._last: dict[str, float] = {}
        self.calls: dict[str, int] = {}
        self.throttled: dict[str, int] = {}

    def scrub(self, text: str) -> str:
        return scrub(text, self._secrets)

    def get(
        self,
        source: str,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        per_minute: int = DEFAULT_PER_MINUTE,
    ) -> httpx.Response:
        """GET with pacing and retries. Raises NetworkError when every attempt fails."""
        return self._request("GET", source, url, params=params, headers=headers, body=None, per_minute=per_minute)

    def post(
        self,
        source: str,
        url: str,
        *,
        json: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
        per_minute: int = DEFAULT_PER_MINUTE,
    ) -> httpx.Response:
        """POST a JSON body with the same pacing and retries as `get`."""
        return self._request("POST", source, url, params=None, headers=headers, body=json, per_minute=per_minute)

    def _request(
        self,
        method: str,
        source: str,
        url: str,
        *,
        params: Mapping[str, str] | None,
        headers: Mapping[str, str] | None,
        body: Mapping[str, Any] | None,
        per_minute: int,
    ) -> httpx.Response:
        reason = ""
        for attempt in range(self._retries + 1):
            self._wait_turn(source, per_minute)
            self.calls[source] = self.calls.get(source, 0) + 1
            try:
                response = self._http.request(method, url, params=params, headers=headers, json=body)
            except httpx.TransportError as exc:
                reason = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code not in RETRY_STATUS:
                    return response
                if response.status_code == TOO_MANY_REQUESTS:
                    self.throttled[source] = self.throttled.get(source, 0) + 1
                reason = f"HTTP {response.status_code}"
            if attempt < self._retries:
                self._sleep(2.0 ** (attempt + 1))
        msg = self.scrub(f"{source}: {reason} after {self._retries + 1} attempts")
        raise NetworkError(msg) from None

    def _wait_turn(self, source: str, per_minute: int) -> None:
        interval = 60.0 / per_minute
        previous = self._last.get(source)
        now = self._clock()
        if previous is not None and now - previous < interval:
            self._sleep(interval - (now - previous))
            now = self._clock()
        self._last[source] = now

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.close()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/http_test.py -q`
Expected: `7 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/http.py tests/unit/store/http_test.py
git commit -m "feat(store): add Client.post with the same pacing and retries as get"
```

### Task 2: Catalog fields, the one-series spelling, bundled catalogs and the macro catalog

One task because the pieces only make sense together: an entry gains `frequency` and `name`; a new spelling (`id`) lets one series carry its own alias and name; a catalog can be loaded by name from the package; and the macro catalog, which needs all of that, is generated and shipped.

**Files:**
- Modify: `src/data_pipeline/store/model.py`, `src/data_pipeline/store/catalog.py`, `src/data_pipeline/store/sync.py`, `src/data_pipeline/store/api.py`
- Create: `src/data_pipeline/store/catalogs/__init__.py`, `src/data_pipeline/store/catalogs/macro.yaml` (generated), `scripts/convert_macro_catalog.py`
- Test: `tests/unit/store/model_test.py`, `tests/unit/store/catalog_test.py`, `tests/unit/store/sync_test.py`, `tests/unit/store/api_test.py`

- [ ] **Step 1: Write the failing tests**

Replace `tests/unit/store/model_test.py` with (the last test is new):

```python
from data_pipeline.store.model import CatalogEntry, Frequency, stale_after


def test_key_joins_source_and_native_id():
    assert CatalogEntry(source="fred", source_id="UNRATE").key == "fred:UNRATE"


def test_stale_after_uses_the_frequency_table():
    assert stale_after(Frequency.MONTHLY) == 124
    assert stale_after(Frequency.DAILY) == 10


def test_stale_after_prefers_the_entry_override():
    assert stale_after(Frequency.MONTHLY, 35) == 35


def test_an_entry_has_no_name_or_frequency_unless_declared():
    plain = CatalogEntry(source="fred", source_id="UNRATE")
    assert (plain.name, plain.frequency) == (None, None)
    declared = CatalogEntry(source="banxico", source_id="SP1", name="INPC", frequency=Frequency.MONTHLY)
    assert (declared.name, declared.frequency) == ("INPC", Frequency.MONTHLY)
```

Replace `tests/unit/store/catalog_test.py` with (everything from `SINGLE = ...` down is new):

```python
import datetime
import pathlib

import pytest

from data_pipeline.store.catalog import build_entries, bundled_catalogs, check_catalog, load_catalog, parse_catalog
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import Frequency

from .helpers import FakeSource, entry

YAML = """
# labour
- source: fred
  ids: [UNRATE, DGS10]
  alias:
    UNRATE: usa.empleo.desempleo
  start: 1990-01-01
  stale_after_days: 45
  attrs:
    theme: labour
- source: inegi
  ids: [737121]
  bank: BIE
"""


def test_loads_a_yaml_file(tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text(YAML, encoding="utf-8")
    entries = load_catalog(path)
    assert [item.key for item in entries] == ["fred:UNRATE", "fred:DGS10", "inegi:737121"]
    unrate, dgs10, inegi = entries
    assert unrate.alias == "usa.empleo.desempleo"
    assert dgs10.alias is None
    assert unrate.start == datetime.date(1990, 1, 1)
    assert unrate.stale_after_days == 45
    assert unrate.attrs == {"theme": "labour"}
    assert unrate.params == {}
    assert inegi.source_id == "737121"
    assert inegi.params == {"bank": "BIE"}


def test_an_empty_file_is_an_empty_catalog(tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text("# nothing yet\n", encoding="utf-8")
    assert load_catalog(path) == ()


def test_a_missing_file_is_an_error(tmp_path):
    with pytest.raises(CatalogError, match="does not exist"):
        load_catalog(tmp_path / "absent.yaml")


def test_broken_yaml_is_an_error(tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text("- source: [unclosed\n", encoding="utf-8")
    with pytest.raises(CatalogError, match="not valid YAML"):
        load_catalog(path)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"source": "fred"}, "must be a list of entries"),
        (["fred"], "entry 1: an entry must be a mapping"),
        ([{"ids": ["A"]}], "entry 1: missing 'source'"),
        ([{"source": "fred"}], "entry 1: missing 'ids'"),
        ([{"source": "fred", "ids": []}], "entry 1: 'ids' must be a non-empty list"),
        ([{"source": "fred", "ids": "UNRATE"}], "entry 1: 'ids' must be a non-empty list"),
        ([{"source": "fred", "ids": ["A"], "alias": {"B": "x"}}], "entry 1: 'alias' names ids that are not in 'ids'"),
        ([{"source": "fred", "ids": ["A"], "alias": "x"}], "entry 1: 'alias' must be a mapping"),
        ([{"source": "fred", "ids": ["A"], "start": "soon"}], "entry 1: 'start' must be a date"),
        ([{"source": "fred", "ids": ["A"], "stale_after_days": "ten"}], "entry 1: 'stale_after_days' must be a whole"),
        ([{"source": "fred", "ids": ["A"]}, {"source": "", "ids": ["A"]}], "entry 2: 'source' must be"),
    ],
)
def test_structural_errors_name_the_entry_and_the_field(raw, message):
    with pytest.raises(CatalogError, match=message):
        parse_catalog(raw)


def test_build_entries_is_what_store_add_uses():
    entries = build_entries("fred", ["UNRATE"], alias={"UNRATE": "u"}, params={"x": 1})
    assert entries[0].alias == "u"
    assert entries[0].params == {"x": 1}


def test_check_accepts_a_clean_catalog():
    check_catalog([entry("A"), entry("B", alias="b")], {"fake": FakeSource()})


@pytest.mark.parametrize(
    ("entries", "message"),
    [
        ([entry("A", source="nope")], "nope:A: unknown source 'nope'"),
        ([entry("A"), entry("A")], "fake:A: declared more than once"),
        ([entry("A", alias="x:y")], "fake:A: alias 'x:y' may not contain ':'"),
        ([entry("A", alias="x"), entry("B", alias="x")], "fake:B: alias 'x' is used more than once"),
        ([entry("A", params={"bank": "BIE"})], "fake:A: unknown field.s. for source 'fake': bank"),
    ],
)
def test_check_rejects(entries, message):
    with pytest.raises(CatalogError, match=message):
        check_catalog(entries, {"fake": FakeSource()})


SINGLE = """
- source: dbnomics
  id: IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp
  alias: e_ar_budget_balance_gdp
  name: Argentina general govt budget balance (% GDP)
  frequency: A
  attrs: {region: AR, commercial_ok: restricted}
- source: inegi
  id: 6207136901
  frequency: M
  bank: BISE
- source: bls
  ids: [LNS14000000, CES0000000001]
  frequency: M
"""


def test_the_single_series_spelling_carries_its_own_alias_name_and_frequency(tmp_path):
    path = tmp_path / "catalog.yaml"
    path.write_text(SINGLE, encoding="utf-8")
    weo, igae, unrate, payrolls = load_catalog(path)
    assert weo.key == "dbnomics:IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp"
    assert (weo.alias, weo.name) == ("e_ar_budget_balance_gdp", "Argentina general govt budget balance (% GDP)")
    assert weo.frequency is Frequency.ANNUAL
    assert weo.attrs == {"region": "AR", "commercial_ok": "restricted"}
    assert (igae.source_id, igae.frequency, igae.params) == ("6207136901", Frequency.MONTHLY, {"bank": "BISE"})
    assert (unrate.frequency, payrolls.frequency) == (Frequency.MONTHLY, Frequency.MONTHLY)
    assert (unrate.name, unrate.alias) == (None, None)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ([{"source": "fred", "id": "A", "ids": ["A"]}], "entry 1: use 'ids' or 'id', not both"),
        ([{"source": "fred", "id": ["A"]}], "entry 1: 'id' must be one series id"),
        ([{"source": "fred", "id": ""}], "entry 1: 'id' must be one series id"),
        ([{"source": "fred", "id": "A", "alias": {"A": "x"}}], "entry 1: with 'id', 'alias' must be a text"),
        ([{"source": "fred", "ids": ["A", "B"], "name": "x"}], "entry 1: 'name' fits one series"),
        ([{"source": "fred", "id": "A", "frequency": "monthly"}], "entry 1: 'frequency' must be one of D, W, M, Q, A"),
    ],
)
def test_errors_of_the_new_fields_name_the_entry(raw, message):
    with pytest.raises(CatalogError, match=message):
        parse_catalog(raw)


def test_a_name_is_allowed_with_ids_when_there_is_only_one():
    assert parse_catalog([{"source": "fred", "ids": ["A"], "name": "Only one"}])[0].name == "Only one"


def test_build_entries_accepts_a_frequency_as_text_or_as_the_enum():
    assert build_entries("fred", ["A"], frequency="Q")[0].frequency is Frequency.QUARTERLY
    assert build_entries("fred", ["A"], frequency=Frequency.DAILY)[0].frequency is Frequency.DAILY


def test_a_bundled_catalog_is_found_by_its_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert "macro" in bundled_catalogs()
    assert len(load_catalog(pathlib.Path("macro"))) > 1000


def test_a_file_wins_over_a_bundled_catalog_of_the_same_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "macro").write_text("- source: fred\n  ids: [UNRATE]\n", encoding="utf-8")
    assert [item.key for item in load_catalog(pathlib.Path("macro"))] == ["fred:UNRATE"]


def test_an_unknown_catalog_lists_the_bundled_ones(tmp_path):
    with pytest.raises(CatalogError, match="bundled catalogs: macro"):
        load_catalog(tmp_path / "absent.yaml")
```

In `tests/unit/store/sync_test.py`, append this test at the end of the file:

```python
def test_a_name_declared_in_the_catalog_replaces_the_name_of_the_source(tmp_path):
    named = entry("UNRATE", name="Unemployment rate, curated")
    source = FakeSource()
    source.answers["UNRATE"] = monthly(named, {"2026-05": 4.1})
    run(tmp_path, source, entries=[named])
    assert index_row(tmp_path)["name"] == "Unemployment rate, curated"
```

In `tests/unit/store/api_test.py`, append these two tests at the end of the file:

```python
def test_add_accepts_a_name_and_a_frequency(world):
    store, _, _ = world
    store.add("fred", ["UNRATE2"], name="Curated", frequency="M")
    added = store._entries[-1]
    assert (added.name, added.frequency.value) == ("Curated", "M")


def test_a_bundled_catalog_is_loaded_by_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = Store(tmp_path / "store", "macro")
    status = store.status()
    assert len(status) == 1294
    assert set(status["state"]) == {"missing"}
    assert status.set_index("alias").loc["e_us_cpi", "source"] == "fred"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/model_test.py tests/unit/store/catalog_test.py tests/unit/store/sync_test.py tests/unit/store/api_test.py -q`
Expected: a collection error in `catalog_test.py` (`ImportError: cannot import name 'bundled_catalogs'`) and failures in the other three files (`TypeError: ... unexpected keyword argument 'name'`).

- [ ] **Step 3: Add the two fields to the entry**

In `src/data_pipeline/store/model.py`, the `CatalogEntry` class becomes:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One series the user wants. `params` holds the fields only its source understands."""

    source: str
    source_id: str
    alias: str | None = None
    start: datetime.date | None = None
    stale_after_days: int | None = None
    attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)
    params: Mapping[str, object] = dataclasses.field(default_factory=dict)
    name: str | None = None  # replaces the source's own name in the index
    frequency: Frequency | None = None  # used only when the source does not report one

    @property
    def key(self) -> str:
        return f"{self.source}:{self.source_id}"
```

- [ ] **Step 4: Rewrite the catalog module**

Replace `src/data_pipeline/store/catalog.py` with:

```python
"""The catalog: what the user wants downloaded. A YAML list of entries, or entries built in code.

Several ids that share their fields:

    - source: fred
      ids: [UNRATE, DGS10]
      alias:
        UNRATE: usa.empleo.desempleo
      start: 1990-01-01

One series with fields of its own:

    - source: dbnomics
      id: Eurostat/prc_hicp_midx/M.I15.CP00.EA
      alias: e_ea_hicp
      name: Euro area HICP (index, 2015=100)
      frequency: M

Fields every source shares: source, ids or id, alias, name, frequency, start, stale_after_days,
attrs. Any other field belongs to the source and is checked by its `validate`.

A catalog can also be one of those shipped with the library, named without a path: "macro".
"""

import datetime
import importlib.resources
import pathlib
from collections.abc import Mapping, Sequence
from typing import Any

import yaml

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import CatalogEntry, Frequency
from data_pipeline.store.sources.base import Source

SHARED_FIELDS = frozenset(
    {"source", "ids", "id", "alias", "name", "frequency", "start", "stale_after_days", "attrs"}
)
BUNDLED_PACKAGE = "data_pipeline.store.catalogs"
BUNDLED_SUFFIX = ".yaml"


def _fail(where: str, problem: str) -> CatalogError:
    return CatalogError(f"{where}: {problem}")


def _frequency(value: object, where: str) -> Frequency | None:
    if value is None or isinstance(value, Frequency):
        return value
    try:
        return Frequency(str(value))
    except ValueError:
        allowed = ", ".join(item.value for item in Frequency)
        raise _fail(where, f"'frequency' must be one of {allowed}") from None


def build_entries(
    source: str,
    ids: Sequence[object],
    *,
    where: str = "catalog",
    alias: Mapping[str, str] | None = None,
    name: str | None = None,
    frequency: object = None,
    start: datetime.date | None = None,
    stale_after_days: int | None = None,
    attrs: Mapping[str, str] | None = None,
    params: Mapping[str, object] | None = None,
) -> tuple[CatalogEntry, ...]:
    """One CatalogEntry per id. Ids are turned into text (YAML reads 737121 as a number)."""
    if not isinstance(source, str) or not source:
        raise _fail(where, "'source' must be a non-empty text")
    if isinstance(ids, str) or not isinstance(ids, Sequence) or not ids:
        raise _fail(where, "'ids' must be a non-empty list")
    names = [str(item) for item in ids]
    aliases = dict(alias or {})
    unknown = sorted(set(map(str, aliases)) - set(names))
    if unknown:
        raise _fail(where, f"'alias' names ids that are not in 'ids': {', '.join(unknown)}")
    if name is not None and len(names) > 1:
        raise _fail(where, "'name' fits one series: use 'id' instead of 'ids'")
    if start is not None and not isinstance(start, datetime.date):
        raise _fail(where, "'start' must be a date such as 1990-01-01")
    if stale_after_days is not None and (isinstance(stale_after_days, bool) or not isinstance(stale_after_days, int)):
        raise _fail(where, "'stale_after_days' must be a whole number")
    declared = _frequency(frequency, where)
    return tuple(
        CatalogEntry(
            source=source,
            source_id=item,
            alias=str(aliases[item]) if item in aliases else None,
            start=start,
            stale_after_days=stale_after_days,
            attrs={str(k): str(v) for k, v in (attrs or {}).items()},
            params=dict(params or {}),
            name=None if name is None else str(name),
            frequency=declared,
        )
        for item in names
    )


def _ids_and_alias(fields: Mapping[str, Any], where: str) -> tuple[Sequence[object], dict[str, str]]:
    """Read either spelling: `ids` with an alias mapping, or `id` with an alias text."""
    if "ids" in fields and "id" in fields:
        raise _fail(where, "use 'ids' or 'id', not both")
    alias = fields.get("alias")
    if "id" in fields:
        if isinstance(fields["id"], list | dict) or fields["id"] in (None, ""):
            raise _fail(where, "'id' must be one series id")
        if isinstance(alias, list | dict):
            raise _fail(where, "with 'id', 'alias' must be a text")
        one = str(fields["id"])
        return [one], ({} if alias is None else {one: str(alias)})
    if "ids" not in fields:
        raise _fail(where, "missing 'ids' (or 'id')")
    if alias is not None and not isinstance(alias, dict):
        raise _fail(where, "'alias' must be a mapping")
    return fields["ids"], {str(k): str(v) for k, v in (alias or {}).items()}


def parse_catalog(raw: object, where: str = "catalog") -> tuple[CatalogEntry, ...]:
    """Turn the parsed YAML (a list of mappings) into entries."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise _fail(where, "the catalog must be a list of entries")
    entries: list[CatalogEntry] = []
    for position, item in enumerate(raw, start=1):
        here = f"{where}, entry {position}"
        if not isinstance(item, dict):
            raise _fail(here, "an entry must be a mapping with 'source' and 'ids'")
        fields: dict[str, Any] = {str(k): v for k, v in item.items()}
        if "source" not in fields:
            raise _fail(here, "missing 'source'")
        ids, alias = _ids_and_alias(fields, here)
        if fields.get("attrs") is not None and not isinstance(fields["attrs"], dict):
            raise _fail(here, "'attrs' must be a mapping")
        entries.extend(
            build_entries(
                fields["source"],
                ids,
                where=here,
                alias=alias,
                name=fields.get("name"),
                frequency=fields.get("frequency"),
                start=fields.get("start"),
                stale_after_days=fields.get("stale_after_days"),
                attrs=fields.get("attrs"),
                params={k: v for k, v in fields.items() if k not in SHARED_FIELDS},
            )
        )
    return tuple(entries)


def bundled_catalogs() -> tuple[str, ...]:
    """Names of the catalogs shipped with the library."""
    folder = importlib.resources.files(BUNDLED_PACKAGE)
    names = [item.name for item in folder.iterdir() if item.name.endswith(BUNDLED_SUFFIX)]
    return tuple(sorted(name.removesuffix(BUNDLED_SUFFIX) for name in names))


def _read(reference: pathlib.Path) -> tuple[str, str]:
    """(text, where) of a catalog: an existing file wins, then a bundled catalog of that name."""
    if reference.exists():
        return reference.read_text(encoding="utf-8"), str(reference)
    name = str(reference)
    if name in bundled_catalogs():
        resource = importlib.resources.files(BUNDLED_PACKAGE).joinpath(name + BUNDLED_SUFFIX)
        return resource.read_text(encoding="utf-8"), f"bundled catalog {name!r}"
    known = ", ".join(bundled_catalogs()) or "none"
    raise _fail(str(reference), f"the catalog file does not exist (bundled catalogs: {known})")


def load_catalog(reference: pathlib.Path) -> tuple[CatalogEntry, ...]:
    """Read a YAML catalog: a file, or the name of a catalog shipped with the library."""
    text, where = _read(reference)
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise _fail(where, f"not valid YAML ({exc})") from exc
    return parse_catalog(raw, where=where)


def check_catalog(entries: Sequence[CatalogEntry], sources: Mapping[str, Source]) -> None:
    """Checks that need the whole catalog: duplicates, aliases, and each source's own fields."""
    keys: set[str] = set()
    aliases: set[str] = set()
    for entry in entries:
        if entry.source not in sources:
            msg = f"{entry.key}: unknown source {entry.source!r}"
            raise CatalogError(msg)
        if entry.key in keys:
            msg = f"{entry.key}: declared more than once"
            raise CatalogError(msg)
        keys.add(entry.key)
        if entry.alias is not None:
            if ":" in entry.alias:
                msg = f"{entry.key}: alias {entry.alias!r} may not contain ':'"
                raise CatalogError(msg)
            if entry.alias in aliases:
                msg = f"{entry.key}: alias {entry.alias!r} is used more than once"
                raise CatalogError(msg)
            aliases.add(entry.alias)
        sources[entry.source].validate(entry)
```

Create `src/data_pipeline/store/catalogs/__init__.py`:

```python
"""Catalogs shipped with the library. `Store(root, catalog="macro")` loads `macro.yaml`."""
```

- [ ] **Step 5: Let the catalog's name win, and let `add` take the new fields**

In `src/data_pipeline/store/sync.py`, inside `_ok_row`, change the line `"name": series.name,` to:

```python
        "name": entry.name or series.name,
```

In `src/data_pipeline/store/api.py`, the `add` method becomes:

```python
    def add(
        self,
        source: str,
        ids: Sequence[object],
        *,
        alias: Mapping[str, str] | None = None,
        name: str | None = None,
        frequency: str | None = None,
        start: datetime.date | None = None,
        stale_after_days: int | None = None,
        attrs: Mapping[str, str] | None = None,
        **params: object,
    ) -> None:
        """Add catalog entries for this session. The YAML file is not rewritten."""
        self._entries.extend(
            build_entries(
                source,
                ids,
                where="add()",
                alias=alias,
                name=name,
                frequency=frequency,
                start=start,
                stale_after_days=stale_after_days,
                attrs=attrs,
                params=params,
            )
        )
```

- [ ] **Step 6: Write the conversion script and generate the macro catalog**

Create `scripts/convert_macro_catalog.py`:

```python
"""Convert the equity engine's macro catalog (JSON) into the store's bundled catalog (YAML).

    python scripts/convert_macro_catalog.py

Rerun it whenever src/data_pipeline/equity/config_handlers/macro_catalog.json changes; a test
fails while the two files are out of step.
"""

import json
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src" / "data_pipeline" / "equity" / "config_handlers" / "macro_catalog.json"
TARGET = ROOT / "src" / "data_pipeline" / "store" / "catalogs" / "macro.yaml"
HEADER = (
    "# Generated by scripts/convert_macro_catalog.py from\n"
    "# src/data_pipeline/equity/config_handlers/macro_catalog.json. Do not edit by hand.\n"
)
SOURCES = {"dbnomics": "dbnomics", "fred": "fred", "banxico_sie": "banxico", "inegi": "inegi"}
FREQUENCIES = {"daily": "D", "weekly": "W", "monthly": "M", "quarterly": "Q", "annual": "A"}
INEGI_BANK = "BISE"  # every INEGI id of the macro catalog is a BISE id


def convert(rows):
    """The YAML text of the bundled catalog for the rows of the JSON catalog."""
    entries = []
    for row in rows:
        entry = {
            "source": SOURCES[row["provider"]],
            "id": str(row["series_id"]),
            "alias": row["column"],
            "name": row["name"],
            "frequency": FREQUENCIES[row["frequency"]],
        }
        if entry["source"] == "inegi":
            entry["bank"] = INEGI_BANK
        entry["attrs"] = {"region": row["region"], "commercial_ok": row["commercial_ok"]}
        entries.append(entry)
    return HEADER + yaml.safe_dump(entries, sort_keys=False, allow_unicode=True, width=1000)


def main():
    rows = json.loads(SOURCE.read_text(encoding="utf-8"))
    TARGET.write_text(convert(rows), encoding="utf-8", newline="\n")
    print(f"[ok] wrote {TARGET} ({len(rows)} entries)")


if __name__ == "__main__":
    main()
```

Run: `.venv/Scripts/python scripts/convert_macro_catalog.py`
Expected: `[ok] wrote ...macro.yaml (1294 entries)`

The generated file starts like this (10,356 lines in all):

```yaml
# Generated by scripts/convert_macro_catalog.py from
# src/data_pipeline/equity/config_handlers/macro_catalog.json. Do not edit by hand.
- source: dbnomics
  id: IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp
  alias: e_ar_budget_balance_gdp
  name: Argentina general govt budget balance (% GDP)
  frequency: A
  attrs:
    region: AR
    commercial_ok: restricted
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/model_test.py tests/unit/store/catalog_test.py tests/unit/store/sync_test.py tests/unit/store/api_test.py -q`
Expected: `80 passed`

- [ ] **Step 8: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 9: Commit**

```bash
git add src/data_pipeline/store tests/unit/store scripts/convert_macro_catalog.py
git commit -m "feat(store): add catalog name and frequency, the one-series spelling and the bundled macro catalog"
```

### Task 3: Helpers every new source shares

`missing_key` (the batch a source yields when its credential is absent), `utc_today`, `reject_params` with a list of allowed fields (INEGI accepts `bank` and `area`), two shared messages, and one defect fixed: a period a source cannot read raised `PeriodError`, which `per_request` did not catch, so one bad period would have stopped the whole sync.

**Files:**
- Modify: `src/data_pipeline/store/sources/base.py`
- Test: `tests/unit/store/base_test.py`

- [ ] **Step 1: Write the failing tests**

Replace `tests/unit/store/base_test.py` with (the last three tests are new):

```python
import math

import pytest

from data_pipeline.store.errors import CatalogError, KeyRejectedError, NetworkError, PeriodError
from data_pipeline.store.model import Failure, Outcome, Request
from data_pipeline.store.sources.base import missing_key, number, per_request, reject_params

from .helpers import entry, monthly


@pytest.mark.parametrize("text", ["", ".", "N/E", "na", "NaN", "n/a", "-", None])
def test_missing_sentinels_become_nan(text):
    assert math.isnan(number(text))


@pytest.mark.parametrize(("value", "expected"), [("4.1", 4.1), ("1,234.5", 1234.5), (3, 3.0), (" 2 ", 2.0)])
def test_numbers_are_parsed(value, expected):
    assert number(value) == expected


def test_reject_params_names_the_entry_and_the_field():
    with pytest.raises(CatalogError, match=r"fake:UNRATE.*reporters"):
        reject_params(entry(params={"reporters": ["MEX"]}))


def run(download, ids=("A", "B", "C")):
    requests = [Request(entry(source_id)) for source_id in ids]
    return list(per_request(requests, download))


def test_one_batch_per_request():
    batches = run(lambda request: monthly(request.entry, {"2026-05": 1.0}))
    assert [batch.series[0].key for batch in batches] == ["fake:A", "fake:B", "fake:C"]


def test_a_returned_failure_is_recorded_and_the_run_continues():
    def download(request):
        if request.entry.source_id == "B":
            return Failure(request.entry, Outcome.NOT_FOUND, "does not exist")
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert [len(batch.series) for batch in batches] == [1, 0, 1]
    assert batches[1].failures[0].outcome is Outcome.NOT_FOUND


def test_a_network_error_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            msg = "fake: HTTP 503 after 4 attempts"
            raise NetworkError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.NETWORK_ERROR
    assert len(batches[1].series) == 1


def test_an_unexpected_answer_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            return {}["observations"]
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "KeyError" in batches[0].failures[0].reason
    assert len(batches[2].series) == 1


def test_a_rejected_key_skips_the_rest_of_the_source():
    def download(request):
        if request.entry.source_id == "B":
            msg = "the key was rejected"
            raise KeyRejectedError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert len(batches) == 2
    assert [failure.entry.key for failure in batches[1].failures] == ["fake:B", "fake:C"]
    assert {failure.outcome for failure in batches[1].failures} == {Outcome.KEY_ERROR}


def test_reject_params_lets_the_allowed_fields_through():
    reject_params(entry(params={"bank": "BISE"}), ("bank", "area"))
    with pytest.raises(CatalogError, match="for source 'fake': banco"):
        reject_params(entry(params={"bank": "BISE", "banco": "x"}), ("bank", "area"))


def test_missing_key_fails_every_request_and_names_the_variable():
    batch = missing_key([Request(entry("A")), Request(entry("B"))], "BLS_API_KEY")
    assert [failure.entry.key for failure in batch.failures] == ["fake:A", "fake:B"]
    assert {failure.reason for failure in batch.failures} == {"BLS_API_KEY missing in .env"}
    assert {failure.outcome for failure in batch.failures} == {Outcome.KEY_ERROR}


def test_a_period_the_source_cannot_read_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            msg = "'2026-W05' is not a month"
            raise PeriodError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "PeriodError" in batches[0].failures[0].reason
    assert len(batches[1].series) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/unit/store/base_test.py -q`
Expected: collection error, `ImportError: cannot import name 'missing_key'`

- [ ] **Step 3: Write the implementation**

Replace `src/data_pipeline/store/sources/base.py` with:

```python
"""What every source shares: the `Source` protocol, the per-request runner and the parsing of
numbers with each source's missing-value sentinels.

Failure policy: a series that does not exist is recorded and skipped; a rejected credential
skips the rest of that source; a network failure or an unexpected answer fails only that
request; an exhausted quota stops the source. Nothing is filled in.
"""

import datetime
import math
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import Protocol

from data_pipeline.store.errors import CatalogError, KeyRejectedError, NetworkError, PeriodError
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Kind,
    Outcome,
    Request,
    SeriesData,
)

MISSING = frozenset({"", ".", "N/E", "NA", "NAN", "N/A", "-"})
UNSUPPORTED_FREQUENCY = "unsupported frequency"
DECLARE_FREQUENCY = "the source does not report a frequency: declare `frequency` in the catalog"


def utc_today() -> datetime.date:
    return datetime.datetime.now(datetime.UTC).date()


class Source(Protocol):
    name: str
    kind: Kind
    requests_per_minute: int
    daily_budget: int | None  # None when the source has no daily quota

    def validate(self, entry: CatalogEntry) -> None:
        """Raise CatalogError when the entry carries a field this source does not accept."""
        ...

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        """Yield batches as they are downloaded. Raise QuotaExhaustedError to stop early."""
        ...


def number(value: object) -> float:
    """Source value -> float; the source's missing sentinels -> NaN (never 0)."""
    if value is None:
        return math.nan
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip()
    if text.upper() in MISSING:
        return math.nan
    return float(text.replace(",", ""))


def reject_params(entry: CatalogEntry, allowed: Collection[str] = ()) -> None:
    """Raise when the entry carries a source-specific field outside `allowed`."""
    unknown = sorted(set(entry.params) - set(allowed))
    if unknown:
        fields = ", ".join(unknown)
        msg = f"{entry.key}: unknown field(s) for source {entry.source!r}: {fields}"
        raise CatalogError(msg)


def failures(requests: Sequence[Request], outcome: Outcome, reason: str) -> tuple[Failure, ...]:
    return tuple(Failure(request.entry, outcome, reason) for request in requests)


def missing_key(requests: Sequence[Request], variable: str) -> FetchBatch:
    """The batch a source yields when its credential is absent: nothing is requested."""
    return FetchBatch(failures=failures(requests, Outcome.KEY_ERROR, f"{variable} missing in .env"))


Download = Callable[[Request], SeriesData | Failure]


def per_request(requests: Sequence[Request], download: Download) -> Iterator[FetchBatch]:
    """Run `download` once per request and yield one batch per request."""
    for position, request in enumerate(requests):
        try:
            result = download(request)
        except KeyRejectedError as exc:
            yield FetchBatch(failures=failures(requests[position:], Outcome.KEY_ERROR, str(exc)))
            return
        except NetworkError as exc:
            yield FetchBatch(failures=(Failure(request.entry, Outcome.NETWORK_ERROR, str(exc)),))
            continue
        except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
            reason = f"unexpected answer ({type(exc).__name__}: {exc})"
            yield FetchBatch(failures=(Failure(request.entry, Outcome.SOURCE_ERROR, reason),))
            continue
        if isinstance(result, Failure):
            yield FetchBatch(failures=(result,))
        else:
            yield FetchBatch(series=(result,))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/unit/store/base_test.py -q`
Expected: `21 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/base.py tests/unit/store/base_test.py
git commit -m "feat(store): share missing-key and allowed-field helpers; an unreadable period fails one series"
```

### Task 4: BLS

The API takes at most 50 series and 20 years per request. Requests are grouped by start year and cut into groups of at most 50; one group is one batch, and a group is all or nothing. A first load without a declared `start` walks back in 20-year windows until one comes back empty. Frequency comes from the period codes; annual averages (`M13`, `Q05`) are ignored. Port of the batching and windowing in `Unemployment_Analysis/unemployment_pipeline/bls_client.py`, without its two defects (a budget that did not persist, and annual averages that shared a date with December).

**Files:**
- Create: `src/data_pipeline/store/sources/bls.py`
- Test: `tests/unit/store/bls_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/bls_test.py`:

```python
import datetime
import json
import math

import httpx
import pytest

from data_pipeline.store import keys
from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.bls import Bls, groups, windows

from .helpers import client, entry

KEY = "0123456789abcdef0123456789abcdef"
WITH_KEY = Credentials({keys.BLS: KEY})
TODAY = datetime.date(2026, 6, 6)
UNRATE = {(2026, "M04"): "4.0", (2026, "M05"): "4.1", (2025, "M13"): "4.2", (2001, "M01"): "4.2"}
TITLE = {"series_title": "(Seas) Unemployment Rate", "seasonality": "Seasonally Adjusted"}


class Server:
    """A fake BLS. `data` maps a series id to {(year, period code): value}."""

    def __init__(self, data, titles=None):
        self.data = data
        self.titles = titles or {}
        self.calls = []
        self.refusals = {}  # call number (from 1) -> message of a REQUEST_NOT_PROCESSED answer
        self.statuses = {}  # call number -> status to answer with instead of REQUEST_SUCCEEDED
        self.blank = set()  # call numbers answered without a single observation
        self.status = 200

    def __call__(self, request):
        body = json.loads(request.content)
        self.calls.append(body)
        if self.status != 200:
            return httpx.Response(self.status, text="broken")
        refusal = self.refusals.get(len(self.calls))
        if refusal:
            return httpx.Response(200, json={"status": "REQUEST_NOT_PROCESSED", "message": [refusal], "Results": {}})
        first, last = int(body["startyear"]), int(body["endyear"])
        series = []
        messages = []
        for series_id in body["seriesid"]:
            if series_id not in self.data:
                messages.append(f"Series does not exist for Series {series_id}")
            points = [
                {"year": str(year), "period": code, "periodName": "", "value": value, "footnotes": [{}]}
                for (year, code), value in sorted(self.data.get(series_id, {}).items(), reverse=True)
                if first <= year <= last and len(self.calls) not in self.blank
            ]
            item = {"seriesID": series_id, "data": points}
            if series_id in self.titles:
                item["catalog"] = self.titles[series_id]
            series.append(item)
        return httpx.Response(
            200,
            json={
                "status": self.statuses.get(len(self.calls), "REQUEST_SUCCEEDED"),
                "message": messages,
                "Results": {"series": series},
            },
        )

    def spans(self):
        return [(call["startyear"], call["endyear"]) for call in self.calls]


def source(server, credentials=WITH_KEY):
    return Bls(client(server, secrets=credentials.secrets()), credentials, today=lambda: TODAY)


def fetch(server, requests, credentials=WITH_KEY):
    return list(source(server, credentials).fetch(requests))


def bls(series_id, **fields):
    return entry(series_id, "bls", **fields)


def test_windows_cut_a_span_into_twenty_year_pieces():
    assert windows(1990, 2026) == [(1990, 2009), (2010, 2026)]
    assert windows(2020, 2026) == [(2020, 2026)]
    assert windows(2027, 2026) == []


def test_groups_split_by_start_year_and_then_into_fifties():
    recent = [Request(bls(f"A{number}"), datetime.date(2024, 5, 1)) for number in range(120)]
    older = [Request(bls("B"), datetime.date(2019, 1, 1)), Request(bls("C"))]
    found = groups([*recent, *older])
    assert [(year, len(members)) for year, members in found] == [
        (2024, 50),
        (2024, 50),
        (2024, 20),
        (2019, 1),
        (None, 1),
    ]


def test_an_incremental_request_asks_one_window_from_the_since_year():
    server = Server({"LNS14000000": UNRATE}, {"LNS14000000": TITLE})
    batches = fetch(server, [Request(bls("LNS14000000"), datetime.date(2024, 5, 1))])
    assert server.spans() == [("2024", "2026")]
    assert server.calls[0]["registrationkey"] == KEY
    assert server.calls[0]["catalog"] is True
    series = batches[0].series[0]
    assert (series.key, series.name, series.seasonal_adjustment) == (
        "bls:LNS14000000",
        "(Seas) Unemployment Rate",
        "SA",
    )
    assert series.frequency is Frequency.MONTHLY
    assert [(item.period, item.value) for item in series.observations] == [("2026-04", 4.0), ("2026-05", 4.1)]
    assert series.observations[1].date == datetime.date(2026, 5, 31)


def test_annual_averages_are_ignored():
    server = Server({"X": {(2025, "M12"): "4.0", (2025, "M13"): "9.9"}})
    series = fetch(server, [Request(bls("X"), datetime.date(2025, 1, 1))])[0].series[0]
    assert [(item.period, item.value) for item in series.observations] == [("2025-12", 4.0)]


def test_a_first_load_walks_back_until_a_window_is_empty():
    server = Server({"LNS14000000": UNRATE})
    series = fetch(server, [Request(bls("LNS14000000"))])[0].series[0]
    assert server.spans() == [("2007", "2026"), ("1987", "2006"), ("1967", "1986")]
    assert [item.period for item in series.observations] == ["2001-01", "2026-04", "2026-05"]
    assert series.name == "LNS14000000"


def test_a_declared_start_replaces_the_walk_back():
    server = Server({"LNS14000000": UNRATE})
    fetch(server, [Request(bls("LNS14000000", start=datetime.date(1990, 1, 1)))])
    assert server.spans() == [("1990", "2009"), ("2010", "2026")]


def test_fifty_series_travel_in_one_request():
    data = {f"S{number}": {(2026, "M05"): "1.0"} for number in range(60)}
    server = Server(data)
    batches = fetch(server, [Request(bls(series_id), datetime.date(2026, 1, 1)) for series_id in data])
    assert [len(call["seriesid"]) for call in server.calls] == [50, 10]
    assert [len(batch.series) for batch in batches] == [50, 10]


def test_quarterly_and_annual_series_are_read_by_their_period_codes():
    server = Server({"Q": {(2026, "Q01"): "5", (2025, "Q05"): "9"}, "A": {(2025, "A01"): "7"}})
    batch = fetch(server, [Request(bls("Q"), datetime.date(2025, 1, 1)), Request(bls("A"), datetime.date(2025, 1, 1))])[
        0
    ]
    quarterly, annual = batch.series
    assert (quarterly.frequency, [item.period for item in quarterly.observations]) == (Frequency.QUARTERLY, ["2026Q1"])
    assert (annual.frequency, [item.period for item in annual.observations]) == (Frequency.ANNUAL, ["2025"])


def test_a_semiannual_series_is_an_unsupported_frequency():
    server = Server({"S": {(2025, "S01"): "1", (2025, "S02"): "2"}})
    failure = fetch(server, [Request(bls("S"), datetime.date(2025, 1, 1))])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason == "unsupported frequency (period codes S)"


def test_a_missing_value_is_kept_as_missing():
    server = Server({"X": {(2026, "M05"): "-"}})
    series = fetch(server, [Request(bls("X"), datetime.date(2026, 1, 1))])[0].series[0]
    assert math.isnan(series.observations[0].value)


def test_an_unknown_series_fails_alone_with_the_message_of_the_api():
    server = Server({"LNS14000000": UNRATE})
    batch = fetch(
        server,
        [Request(bls("NOPE"), datetime.date(2026, 1, 1)), Request(bls("LNS14000000"), datetime.date(2026, 1, 1))],
    )[0]
    assert [series.key for series in batch.series] == ["bls:LNS14000000"]
    assert (batch.failures[0].outcome, batch.failures[0].reason) == (
        Outcome.NOT_FOUND,
        "Series does not exist for Series NOPE",
    )


def test_without_a_key_nothing_is_requested():
    server = Server({})
    batch = fetch(server, [Request(bls("X"))], credentials=Credentials())[0]
    assert server.calls == []
    assert (batch.failures[0].outcome, batch.failures[0].reason) == (Outcome.KEY_ERROR, "BLS_API_KEY missing in .env")


def test_the_daily_threshold_raises_quota_and_yields_nothing_of_that_group():
    server = Server({"A": {(2026, "M05"): "1"}, "B": {(2026, "M05"): "2", (2006, "M05"): "3"}})
    server.refusals[3] = "REQUEST_NOT_PROCESSED: daily threshold for total number of requests has been reached"
    stream = source(server).fetch([Request(bls("A"), datetime.date(2026, 1, 1)), Request(bls("B"))])
    assert [series.key for series in next(stream).series] == ["bls:A"]
    with pytest.raises(QuotaExhaustedError, match="daily threshold"):
        next(stream)


def test_a_rejected_key_fails_every_remaining_request_without_leaking_it():
    server = Server({"A": {(2026, "M05"): "1"}})
    server.refusals[1] = f"The registration key {KEY} is invalid."
    batches = fetch(
        server, [Request(bls("A"), datetime.date(2026, 1, 1)), Request(bls("B"), datetime.date(2020, 1, 1))]
    )
    assert len(server.calls) == 1
    failures = batches[0].failures
    assert [(failure.entry.key, failure.outcome) for failure in failures] == [
        ("bls:A", Outcome.KEY_ERROR),
        ("bls:B", Outcome.KEY_ERROR),
    ]
    assert KEY not in failures[0].reason


def test_any_other_refusal_fails_the_group_and_the_next_group_continues():
    server = Server({"A": {(2026, "M05"): "1"}, "B": {(2026, "M05"): "2"}})
    server.refusals[1] = "Invalid Series for Series A"
    batches = fetch(
        server, [Request(bls("A"), datetime.date(2026, 1, 1)), Request(bls("B"), datetime.date(2020, 1, 1))]
    )
    assert (batches[0].failures[0].outcome, batches[0].failures[0].reason) == (
        Outcome.SOURCE_ERROR,
        "REQUEST_NOT_PROCESSED: Invalid Series for Series A",
    )
    assert [series.key for series in batches[1].series] == ["bls:B"]


def test_a_network_failure_fails_the_group():
    server = Server({})
    server.status = 503
    failure = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].failures[0]
    assert failure.outcome is Outcome.NETWORK_ERROR


def test_a_plain_http_error_fails_the_group():
    server = Server({})
    server.status = 400
    failure = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "HTTP 400: broken")


def test_an_answer_that_is_not_a_success_and_brings_no_data_fails_the_group():
    server = Server({"A": {(2026, "M05"): "1"}})
    server.statuses[1] = "REQUEST_FAILED"
    server.blank.add(1)
    failure = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "REQUEST_FAILED")


def test_an_unusual_status_that_still_brings_data_is_read():
    server = Server({"A": {(2026, "M05"): "1"}})
    server.statuses[1] = "REQUEST_PARTIALLY_PROCESSED"
    series = fetch(server, [Request(bls("A"), datetime.date(2026, 1, 1))])[0].series[0]
    assert [item.period for item in series.observations] == ["2026-05"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/bls_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.bls'`

- [ ] **Step 3: Write the source**

Create `src/data_pipeline/store/sources/bls.py`:

```python
"""BLS (U.S. Bureau of Labor Statistics), Public Data API v2; key in BLS_API_KEY.

The API takes at most 50 series and 20 years per request, and 500 requests per day. Requests
are grouped by their start year and cut into groups of at most 50; one group is one batch.

A group is all or nothing. If the quota runs out while its windows are being downloaded, nothing
of that group is yielded: storing half a history would make the next sync believe the series is
up to date from its newest observation backwards.

Annual averages (period codes M13 and Q05) are ignored: they would share a date with December.
"""

import datetime
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.store import keys
from data_pipeline.store.errors import KeyRejectedError, NetworkError, PeriodError, QuotaExhaustedError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import (
    UNSUPPORTED_FREQUENCY,
    failures,
    missing_key,
    number,
    reject_params,
    utc_today,
)

URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
MAX_SERIES = 50
MAX_YEARS = 20
EARLIEST_YEAR = 1900  # the walk back never asks for years before this one
OK = 200
SUCCEEDED = "REQUEST_SUCCEEDED"
NO_DATA = "BLS returned no observations for this series"
SEASONALITY: Mapping[str, str] = {"seasonally adjusted": "SA", "not seasonally adjusted": "NSA"}
# first letter of a period code -> (frequency, how to spell the period for read_period)
KINDS: Mapping[str, tuple[Frequency, str]] = {
    "M": (Frequency.MONTHLY, "{year}-{number:02d}"),
    "Q": (Frequency.QUARTERLY, "{year}-Q{number}"),
    "A": (Frequency.ANNUAL, "{year}"),
}
LAST_REAL_NUMBER: Mapping[str, int] = {"M": 12, "Q": 4, "A": 1}  # M13 and Q05 are annual averages
PREFERENCE = ("M", "Q", "A")


class _GroupError(Exception):
    """The whole group failed for a reason that is neither the key nor the quota."""


def start_year(request: Request) -> int | None:
    """First year to ask for, or None when the source has to find where the series begins."""
    if request.since is not None:
        return request.since.year
    return request.entry.start.year if request.entry.start is not None else None


def groups(requests: Sequence[Request]) -> list[tuple[int | None, list[Request]]]:
    """Requests grouped by start year, each group cut into pieces of at most MAX_SERIES."""
    by_year: dict[int | None, list[Request]] = {}
    for request in requests:
        by_year.setdefault(start_year(request), []).append(request)
    return [
        (year, members[position : position + MAX_SERIES])
        for year, members in by_year.items()
        for position in range(0, len(members), MAX_SERIES)
    ]


def windows(first: int, last: int) -> list[tuple[int, int]]:
    """[first, last] cut into consecutive windows of at most MAX_YEARS years."""
    return [(year, min(year + MAX_YEARS - 1, last)) for year in range(first, last + 1, MAX_YEARS)]


class Bls:
    name = "bls"
    kind = Kind.SERIES
    requests_per_minute = 50
    daily_budget: int | None = 450  # the API allows 500; sync checks the budget between batches

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._key = credentials.get(keys.BLS)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._key:
            yield missing_key(requests, keys.BLS)
            return
        pieces = groups(requests)
        for position, (year, members) in enumerate(pieces):
            try:
                yield self._group(year, members)
            except KeyRejectedError as exc:
                rest = [request for _, later in pieces[position:] for request in later]
                yield FetchBatch(failures=failures(rest, Outcome.KEY_ERROR, str(exc)))
                return
            except NetworkError as exc:
                yield FetchBatch(failures=failures(members, Outcome.NETWORK_ERROR, str(exc)))
            except _GroupError as exc:
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, str(exc)))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=failures(members, Outcome.SOURCE_ERROR, reason))

    def _group(self, year: int | None, members: Sequence[Request]) -> FetchBatch:
        ids = [request.entry.source_id for request in members]
        points: dict[str, list[tuple[str, int, int, float]]] = {}
        titles: dict[str, Mapping[str, Any]] = {}
        messages: list[str] = []
        this_year = self._today().year
        if year is not None:
            for first, last in windows(year, this_year):
                self._window(ids, first, last, points, titles, messages)
        else:
            last = this_year
            while last >= EARLIEST_YEAR:
                first = max(last - MAX_YEARS + 1, EARLIEST_YEAR)
                if self._window(ids, first, last, points, titles, messages) == 0:
                    break
                last = first - 1
        series = []
        failed = []
        for request in members:
            result = self._series(request, points.get(request.entry.source_id, []), titles, messages)
            if isinstance(result, Failure):
                failed.append(result)
            else:
                series.append(result)
        return FetchBatch(tuple(series), tuple(failed))

    def _window(
        self,
        ids: Sequence[str],
        first: int,
        last: int,
        points: dict[str, list[tuple[str, int, int, float]]],
        titles: dict[str, Mapping[str, Any]],
        messages: list[str],
    ) -> int:
        """Download one window into `points`. Returns how many observations it brought."""
        response = self._client.post(
            self.name,
            URL,
            json={
                "seriesid": list(ids),
                "startyear": str(first),
                "endyear": str(last),
                "registrationkey": self._key or "",
                "catalog": True,
            },
            per_minute=self.requests_per_minute,
        )
        if response.status_code != OK:
            msg = f"HTTP {response.status_code}: {self._client.scrub(response.text[:300])}"
            raise _GroupError(msg)
        payload: dict[str, Any] = response.json()
        notes = [self._client.scrub(str(note)) for note in payload.get("message") or [] if note]
        messages.extend(note for note in notes if note not in messages)
        status = str(payload.get("status") or "")
        answered = (payload.get("Results") or {}).get("series") or []
        if status != SUCCEEDED and not any(item.get("data") for item in answered):
            # Anything but success that brings no data is a refusal, never "the series is empty".
            text = "; ".join(notes) or status or "the answer carries no status"
            if "daily threshold" in text.lower():
                raise QuotaExhaustedError(text)
            if "key" in text.lower():
                msg = f"BLS rejected the key: {text}"
                raise KeyRejectedError(msg)
            raise _GroupError(text if status in text else f"{status}: {text}")
        found = 0
        for item in answered:
            series_id = str(item.get("seriesID") or "")
            if item.get("catalog"):
                titles.setdefault(series_id, item["catalog"])
            for point in item.get("data") or []:
                code = str(point["period"])
                points.setdefault(series_id, []).append(
                    (code[:1], int(point["year"]), int(code[1:]), number(point.get("value")))
                )
                found += 1
        return found

    def _series(
        self,
        request: Request,
        raw: Sequence[tuple[str, int, int, float]],
        titles: Mapping[str, Mapping[str, Any]],
        messages: Sequence[str],
    ) -> SeriesData | Failure:
        entry = request.entry
        if not raw:
            said = next((note for note in messages if entry.source_id in note), NO_DATA)
            return Failure(entry, Outcome.NOT_FOUND, said)
        letters = {letter for letter, _, _, _ in raw}
        letter = next((candidate for candidate in PREFERENCE if candidate in letters), None)
        if letter is None:
            codes = ", ".join(sorted(letters))
            return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} (period codes {codes})")
        frequency, spelling = KINDS[letter]
        observations: dict[str, Observation] = {}
        for kind, year, order, value in raw:
            if kind != letter or order > LAST_REAL_NUMBER[letter]:
                continue
            period, day = read_period(spelling.format(year=year, number=order), frequency)
            observations[period] = Observation(period, day, value)
        catalog = titles.get(entry.source_id, {})
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=str(catalog.get("series_title") or entry.source_id),
            frequency=frequency,
            seasonal_adjustment=SEASONALITY.get(str(catalog.get("seasonality") or "").strip().lower(), ""),
            observations=tuple(sorted(observations.values(), key=lambda observation: observation.date)),
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/bls_test.py -q`
Expected: `19 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/bls.py tests/unit/store
git commit -m "feat(store): add the BLS source"
```

### Task 5: Banxico SIE

One call per series. With a `frequency` declared in the catalog that is the only call; without it, a first call asks for the series' metadata, which gives the periodicity and the unit. Port of `Investment_Process/.../fuentes/banxico.py`.

**Files:**
- Create: `src/data_pipeline/store/sources/banxico.py`
- Test: `tests/unit/store/banxico_test.py`
- Create: `tests/unit/store/fixtures/banxico_data.json`, `banxico_bad_token.json`, `banxico_metadata.json`

- [ ] **Step 0: Create the fixtures**

`banxico_data.json` and `banxico_bad_token.json` are recorded answers, copied from
`Investment_Process/tests/macro/publicos/fixtures/` (`banxico_fix.json`, `banxico_token_invalido.json`).
`banxico_metadata.json` is written to the shape the API documents; the live test of Task 9 confirms it.

`tests/unit/store/fixtures/banxico_data.json`:

```json
{"bmx": {"series": [{"idSerie": "SF43718", "titulo": "Tipo de cambio Pesos por dolar E.U.A. para solventar obligaciones denominadas en moneda extranjera. Fecha de determinacion (FIX)", "datos": [{"fecha": "22/09/2026", "dato": "18.4512"}, {"fecha": "23/09/2026", "dato": "N/E"}, {"fecha": "24/09/2026", "dato": "1,018.3907"}]}]}}
```

`tests/unit/store/fixtures/banxico_bad_token.json`:

```json
{"error":{"url":"https://www.banxico.org.mx/SieAPIRest/service/v1/token","mensaje":"Token invalido","detalle":"El token enviado no es valido, favor de verificar. Para obtener un token consultar la url adjunta."}}
```

`tests/unit/store/fixtures/banxico_metadata.json`:

```json
{"bmx": {"series": [{"idSerie": "SF43718", "titulo": "Tipo de cambio Pesos por dolar E.U.A. para solventar obligaciones denominadas en moneda extranjera. Fecha de determinacion (FIX)", "fechaInicio": "12/11/1991", "fechaFin": "24/09/2026", "periodicidad": "Diaria", "cifra": "Tipo de Cambio", "unidad": "Pesos por Dolar", "versionada": false}]}}
```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/banxico_test.py`:

```python
import datetime
import math

import httpx

from data_pipeline.store import keys
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.banxico import Banxico

from .helpers import client, entry, fixture

BMX_VALUE = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
WITH_TOKEN = Credentials({keys.BANXICO: BMX_VALUE})
TODAY = datetime.date(2026, 6, 6)


def serve(seen=None, metadata="banxico_metadata.json"):
    """Answer the metadata call and the data call from the fixtures."""

    def handler(request):
        if seen is not None:
            seen.append(request)
        name = "banxico_data.json" if "/datos" in request.url.path else metadata
        return httpx.Response(200, text=fixture(name))

    return handler


def fetch(handler, requests, credentials=WITH_TOKEN):
    source = Banxico(client(handler, secrets=credentials.secrets()), credentials, today=lambda: TODAY)
    return list(source.fetch(requests))


def fix(**fields):
    return entry("SF43718", "banxico", **fields)


def test_with_a_declared_frequency_one_call_downloads_the_series():
    seen = []
    series = fetch(serve(seen), [Request(fix(frequency=Frequency.DAILY))])[0].series[0]
    assert [request.url.path for request in seen] == ["/SieAPIRest/service/v1/series/SF43718/datos"]
    assert seen[0].headers["Bmx-Token"] == BMX_VALUE
    assert (series.key, series.frequency, series.units) == ("banxico:SF43718", Frequency.DAILY, "")
    assert series.name.startswith("Tipo de cambio Pesos por dolar")
    assert [item.period for item in series.observations] == ["2026-09-22", "2026-09-23", "2026-09-24"]
    assert series.observations[0].value == 18.4512
    assert math.isnan(series.observations[1].value)
    assert series.observations[2].value == 1018.3907


def test_since_becomes_a_date_range_in_the_path():
    seen = []
    fetch(serve(seen), [Request(fix(frequency=Frequency.DAILY), datetime.date(2026, 1, 1))])
    assert seen[0].url.path.endswith("/series/SF43718/datos/2026-01-01/2026-06-06")


def test_without_a_declared_frequency_the_metadata_gives_it_and_the_unit():
    seen = []
    series = fetch(serve(seen), [Request(fix())])[0].series[0]
    assert [request.url.path.rsplit("/", 1)[-1] for request in seen] == ["SF43718", "datos"]
    assert (series.frequency, series.units) == (Frequency.DAILY, "Pesos por Dolar")


def test_a_periodicity_the_store_does_not_know_is_an_unsupported_frequency():
    def handler(request):
        if "/datos" in request.url.path:
            return serve()(request)
        return httpx.Response(200, json={"bmx": {"series": [{"idSerie": "SF1", "periodicidad": "Quincenal"}]}})

    failure = fetch(handler, [Request(fix())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "unsupported frequency 'Quincenal'")


def test_an_unknown_series_is_not_found_and_the_next_one_still_runs():
    def handler(request):
        if "/NOPE" in request.url.path:
            return httpx.Response(404, text="no existe")
        return serve()(request)

    batches = fetch(
        handler, [Request(entry("NOPE", "banxico", frequency=Frequency.DAILY)), Request(fix(frequency=Frequency.DAILY))]
    )
    assert (batches[0].failures[0].outcome, batches[0].failures[0].reason) == (Outcome.NOT_FOUND, "HTTP 404: no existe")
    assert batches[1].series[0].key == "banxico:SF43718"


def test_an_error_payload_is_not_found():
    failure = fetch(
        lambda _request: httpx.Response(200, json={"error": {"mensaje": "Serie no encontrada"}}),
        [Request(fix(frequency=Frequency.DAILY))],
    )[0].failures[0]
    assert failure.outcome is Outcome.NOT_FOUND
    assert "Serie no encontrada" in failure.reason


def test_an_answer_without_data_is_not_found():
    failure = fetch(
        lambda _request: httpx.Response(200, json={"bmx": {"series": [{"idSerie": "SF43718", "titulo": "T"}]}}),
        [Request(fix(frequency=Frequency.DAILY))],
    )[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "Banxico returned no data for this series")


def test_a_rejected_token_fails_every_remaining_request_without_leaking_it():
    batches = fetch(
        lambda _request: httpx.Response(401, text=fixture("banxico_bad_token.json")),
        [Request(fix(frequency=Frequency.DAILY)), Request(entry("SP1", "banxico", frequency=Frequency.MONTHLY))],
    )
    failures = batches[0].failures
    assert [failure.outcome for failure in failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert BMX_VALUE not in failures[0].reason


def test_without_a_token_nothing_is_requested():
    seen = []
    batch = fetch(serve(seen), [Request(fix())], credentials=Credentials())[0]
    assert seen == []
    assert batch.failures[0].reason == "BANXICO_TOKEN missing in .env"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/banxico_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.banxico'`

- [ ] **Step 3: Write the source**

Create `src/data_pipeline/store/sources/banxico.py`:

```python
"""Banxico SIE: one call per series; token in the Bmx-Token header (BANXICO_TOKEN).

Dates are dd/mm/yyyy; "N/E" is missing; thousands separators are stripped. The frequency is the
catalog's when declared. Otherwise one more call asks for the series' metadata, which also
brings its unit.
"""

import datetime
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.store import keys
from data_pipeline.store.errors import KeyRejectedError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import (
    UNSUPPORTED_FREQUENCY,
    missing_key,
    number,
    per_request,
    reject_params,
    utc_today,
)

URL = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
OK = 200
NOT_FOUND = 404
PERIODICITY: Mapping[str, Frequency] = {
    "diaria": Frequency.DAILY,
    "semanal": Frequency.WEEKLY,
    "mensual": Frequency.MONTHLY,
    "trimestral": Frequency.QUARTERLY,
    "anual": Frequency.ANNUAL,
}
NO_DATA = "Banxico returned no data for this series"


def read_data(series: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for row in series["datos"]:
        period, day = read_period(str(row["fecha"]), frequency)
        observations.append(Observation(period, day, number(row["dato"])))
    return tuple(sorted(observations, key=lambda observation: observation.date))


class Banxico:
    name = "banxico"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._token = credentials.get(keys.BANXICO)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._token:
            yield missing_key(requests, keys.BANXICO)
            return
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        frequency = entry.frequency
        units = ""
        if frequency is None:
            meta = self._get(f"{URL}/{entry.source_id}", entry)
            if isinstance(meta, Failure):
                return meta
            reported = str(meta.get("periodicidad") or "")
            frequency = PERIODICITY.get(reported.strip().lower())
            if frequency is None:
                return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {reported!r}")
            units = str(meta.get("unidad") or "")
        url = f"{URL}/{entry.source_id}/datos"
        if request.since is not None:
            url += f"/{request.since.isoformat()}/{self._today().isoformat()}"
        series = self._get(url, entry)
        if isinstance(series, Failure):
            return series
        if "datos" not in series:
            return Failure(entry, Outcome.NOT_FOUND, NO_DATA)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=" ".join(str(series.get("titulo") or entry.source_id).split()),
            frequency=frequency,
            units=units,
            observations=read_data(series, frequency),
        )

    def _get(self, url: str, entry: CatalogEntry) -> Mapping[str, Any] | Failure:
        """The first series of the answer, or the failure the answer amounts to."""
        response = self._client.get(
            self.name,
            url,
            headers={"Bmx-Token": self._token or "", "Accept": "application/json"},
            per_minute=self.requests_per_minute,
        )
        text = self._client.scrub(response.text[:300])
        if response.status_code != OK:
            if "token" in text.lower():
                msg = f"Banxico rejected the token: {text}"
                raise KeyRejectedError(msg)
            outcome = Outcome.NOT_FOUND if response.status_code == NOT_FOUND else Outcome.SOURCE_ERROR
            return Failure(entry, outcome, f"HTTP {response.status_code}: {text}")
        payload: dict[str, Any] = response.json()
        if "error" in payload:
            return Failure(entry, Outcome.NOT_FOUND, self._client.scrub(str(payload["error"])[:300]))
        found = payload["bmx"]["series"]
        if not found:
            return Failure(entry, Outcome.NOT_FOUND, NO_DATA)
        first: Mapping[str, Any] = found[0]
        return first
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/banxico_test.py -q`
Expected: `9 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/banxico.py tests/unit/store
git commit -m "feat(store): add the Banxico SIE source"
```

### Task 6: INEGI

The token travels in the URL path, so the client's scrubbing matters here. Frequency comes from the answer's `FREQ` code. The source accepts two catalog fields of its own, `bank` and `area`. Port of `Investment_Process/.../fuentes/inegi.py`.

**Files:**
- Create: `src/data_pipeline/store/sources/inegi.py`
- Test: `tests/unit/store/inegi_test.py`
- Create: `tests/unit/store/fixtures/inegi_bise.json`, `inegi_no_results.json`

- [ ] **Step 0: Create the fixtures**

Both are recorded answers, copied from `Investment_Process/tests/macro/publicos/fixtures/`
(`inegi_bise.json`, `inegi_sin_resultados.json`).

`tests/unit/store/fixtures/inegi_bise.json`:

```json
{"Header":{"Name":"Datos compactos BISE","Email":"atencion.usuarios@inegi.org.mx"},"Series":[{"INDICADOR":"6207136901","FREQ":"8","TOPIC":"95","UNIT":"1058","UNIT_MULT":"","NOTE":"","SOURCE":"3251","LASTUPDATE":"24/09/2026 12:00:00 a. m.","STATUS":null,"OBSERVATIONS":[{"TIME_PERIOD":"2026/07","OBS_VALUE":"109.21013588490848000000","OBS_EXCEPTION":null,"OBS_STATUS":"1","OBS_SOURCE":"","OBS_NOTE":"","COBER_GEO":"0"},{"TIME_PERIOD":"2026/06","OBS_VALUE":"107.39083134651233000000","OBS_EXCEPTION":null,"OBS_STATUS":"2","OBS_SOURCE":"","OBS_NOTE":"","COBER_GEO":"0"},{"TIME_PERIOD":"2026/05","OBS_VALUE":"","OBS_EXCEPTION":null,"OBS_STATUS":"2","OBS_SOURCE":"","OBS_NOTE":"","COBER_GEO":"0"}]}]}
```

`tests/unit/store/fixtures/inegi_no_results.json`:

```json
["ErrorInfo:No se encontraron resultados","ErrorDetails:No se encontraron resultados","ErrorCode:100"]
```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/inegi_test.py`:

```python
import datetime
import json
import math

import httpx
import pytest

from data_pipeline.store import keys
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.inegi import Inegi

from .helpers import client, entry, fixture

INEGI_VALUE = "01234567-89ab-cdef-0123-456789abcdef"
WITH_TOKEN = Credentials({keys.INEGI: INEGI_VALUE})


def serve(seen=None, name="inegi_bise.json", status=200):
    def handler(request):
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, text=fixture(name))

    return handler


def fetch(handler, requests, credentials=WITH_TOKEN):
    source = Inegi(client(handler, secrets=credentials.secrets()), credentials)
    return list(source.fetch(requests))


def igae(**fields):
    return entry("6207136901", "inegi", **fields)


def test_downloads_a_series_and_reads_its_frequency_from_the_answer():
    seen = []
    series = fetch(serve(seen), [Request(igae(params={"bank": "BISE"}))])[0].series[0]
    assert seen[0].url.path.endswith(f"/INDICATOR/6207136901/es/00/false/BISE/2.0/{INEGI_VALUE}")
    assert seen[0].url.params["type"] == "json"
    assert (series.key, series.frequency, series.units) == ("inegi:6207136901", Frequency.MONTHLY, "INEGI unit 1058")
    assert [item.period for item in series.observations] == ["2026-05", "2026-06", "2026-07"]
    assert math.isnan(series.observations[0].value)
    assert series.observations[2].value == pytest.approx(109.21013588490848)
    assert series.observations[2].date == datetime.date(2026, 7, 31)


def test_the_default_bank_and_area_are_used():
    seen = []
    fetch(serve(seen), [Request(igae())])
    assert "/es/00/false/BIE-BISE/2.0/" in seen[0].url.path


def test_since_is_applied_after_the_download():
    series = fetch(serve(), [Request(igae(), datetime.date(2026, 6, 15))])[0].series[0]
    assert [item.period for item in series.observations] == ["2026-06", "2026-07"]


def test_only_bank_and_area_are_accepted_as_fields():
    source = Inegi(client(), WITH_TOKEN)
    source.validate(igae(params={"bank": "BISE", "area": "09"}))
    with pytest.raises(CatalogError, match="for source 'inegi': banco"):
        source.validate(igae(params={"banco": "BISE"}))


def test_no_results_is_not_found():
    failure = fetch(serve(name="inegi_no_results.json", status=400), [Request(igae())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "BIE-BISE: no results (ErrorCode:100)")


def test_a_rejected_token_fails_every_remaining_request_without_leaking_it():
    batches = fetch(
        lambda request: httpx.Response(401, text=f"token {request.url.path.rsplit('/', 1)[-1]} no valido"),
        [Request(igae()), Request(entry("737121", "inegi"))],
    )
    failures = batches[0].failures
    assert [failure.outcome for failure in failures] == [Outcome.KEY_ERROR, Outcome.KEY_ERROR]
    assert INEGI_VALUE not in failures[0].reason


def test_a_server_error_is_a_network_error_once_the_retries_are_spent():
    failure = fetch(lambda _request: httpx.Response(500, text="boom"), [Request(igae())])[0].failures[0]
    assert failure.outcome is Outcome.NETWORK_ERROR


def test_any_other_http_error_is_a_source_error():
    failure = fetch(lambda _request: httpx.Response(418, text="teapot"), [Request(igae())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.SOURCE_ERROR, "HTTP 418: teapot")


def unknown_frequency(_request):
    payload = json.loads(fixture("inegi_bise.json"))
    payload["Series"][0]["FREQ"] = "99"
    return httpx.Response(200, json=payload)


def test_an_unknown_frequency_code_falls_back_to_the_catalog():
    series = fetch(unknown_frequency, [Request(igae(frequency=Frequency.MONTHLY))])[0].series[0]
    assert series.frequency is Frequency.MONTHLY


def test_an_unknown_frequency_code_without_a_catalog_frequency_fails():
    failure = fetch(unknown_frequency, [Request(igae())])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason.endswith("declare `frequency` in the catalog (INEGI FREQ '99')")


def test_without_a_token_nothing_is_requested():
    seen = []
    batch = fetch(serve(seen), [Request(igae())], credentials=Credentials())[0]
    assert seen == []
    assert batch.failures[0].reason == "INEGI_TOKEN missing in .env"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/inegi_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.inegi'`

- [ ] **Step 3: Write the source**

Create `src/data_pipeline/store/sources/inegi.py`:

```python
"""INEGI indicators API; token in the URL path (INEGI_TOKEN).

Catalog fields of this source: `bank` (BIE-BISE by default, which serves the BIE indicators such
as 737121; BISE for the BISE ids such as 6207136901) and `area` (00, national, by default).
INEGI returns every period, newest first, so `since` is applied here after the download.
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.store import keys
from data_pipeline.store.errors import KeyRejectedError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import (
    DECLARE_FREQUENCY,
    missing_key,
    number,
    per_request,
    reject_params,
)

URL = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
DEFAULT_BANK = "BIE-BISE"
NATIONAL_AREA = "00"
PARAMS = ("bank", "area")
OK = 200
UNAUTHORIZED = frozenset({401, 403})
NO_RESULTS = "ErrorCode:100"
# INEGI's CL_FREQ codes
FREQUENCIES: Mapping[str, Frequency] = {
    "8": Frequency.MONTHLY,
    "4": Frequency.QUARTERLY,
    "3": Frequency.ANNUAL,
}


def read_observations(series: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for row in series["OBSERVATIONS"]:
        period, day = read_period(str(row["TIME_PERIOD"]), frequency)
        observations.append(Observation(period, day, number(row.get("OBS_VALUE"))))
    return tuple(sorted(observations, key=lambda observation: observation.date))


class Inegi:
    name = "inegi"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._token = credentials.get(keys.INEGI)

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry, PARAMS)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._token:
            yield missing_key(requests, keys.INEGI)
            return
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        bank = str(entry.params.get("bank", DEFAULT_BANK))
        area = str(entry.params.get("area", NATIONAL_AREA))
        response = self._client.get(
            self.name,
            f"{URL}/{entry.source_id}/es/{area}/false/{bank}/2.0/{self._token}",
            params={"type": "json"},
            per_minute=self.requests_per_minute,
        )
        text = self._client.scrub(response.text[:300])
        if response.status_code != OK:
            if NO_RESULTS in text:
                return Failure(entry, Outcome.NOT_FOUND, f"{bank}: no results ({NO_RESULTS})")
            if response.status_code in UNAUTHORIZED or "token" in text.lower():
                msg = f"INEGI rejected the token: {text}"
                raise KeyRejectedError(msg)
            return Failure(entry, Outcome.SOURCE_ERROR, f"HTTP {response.status_code}: {text}")
        series = response.json()["Series"][0]
        code = str(series.get("FREQ") or "")
        frequency = FREQUENCIES.get(code) or entry.frequency
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"{DECLARE_FREQUENCY} (INEGI FREQ {code!r})")
        observations = read_observations(series, frequency)
        if request.since is not None:
            observations = tuple(item for item in observations if item.date >= request.since)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=entry.source_id,
            frequency=frequency,
            units=f"INEGI unit {series.get('UNIT', '')}".strip(),
            observations=observations,
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/inegi_test.py -q`
Expected: `11 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/inegi.py tests/unit/store
git commit -m "feat(store): add the INEGI source"
```

### Task 7: DBnomics

No key. A series id is `<provider>/<dataset>/<series>` and may contain colons. Observations arrive as two parallel arrays. Port of the equity engine's `data_providers/dbnomics.py`.

**Files:**
- Create: `src/data_pipeline/store/sources/dbnomics.py`
- Test: `tests/unit/store/dbnomics_test.py`
- Create: `tests/unit/store/fixtures/dbnomics_series.json`

- [ ] **Step 0: Create the fixture**

`tests/unit/store/fixtures/dbnomics_series.json`, in the shape the API returns (two parallel arrays):

```json
{"series": {"docs": [{"@frequency": "monthly", "dataset_code": "prc_hicp_midx", "provider_code": "Eurostat", "series_code": "M.I15.CP00.EA", "series_name": "Monthly - Index, 2015=100 - All-items HICP - Euro area", "period": ["2026-03", "2026-04", "2026-05"], "value": [128.1, "NA", 128.9]}], "num_found": 1}}
```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/dbnomics_test.py`:

```python
import datetime
import math

import httpx

from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency, Outcome, Request
from data_pipeline.store.sources.dbnomics import Dbnomics

from .helpers import client, entry, fixture

HICP = "Eurostat/prc_hicp_midx/M.I15.CP00.EA"


def fetch(handler, requests):
    return list(Dbnomics(client(handler), Credentials()).fetch(requests))


def documents(*docs):
    return lambda _request: httpx.Response(200, json={"series": {"docs": list(docs)}})


def hicp(**fields):
    return entry(HICP, "dbnomics", **fields)


def test_downloads_a_series_without_a_key():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, text=fixture("dbnomics_series.json"))

    series = fetch(handler, [Request(hicp())])[0].series[0]
    assert dict(seen[0].url.params) == {"series_ids": HICP, "observations": "1"}
    assert (series.key, series.frequency) == (f"dbnomics:{HICP}", Frequency.MONTHLY)
    assert series.name == "Monthly - Index, 2015=100 - All-items HICP - Euro area"
    assert [item.period for item in series.observations] == ["2026-03", "2026-04", "2026-05"]
    assert series.observations[0].value == 128.1
    assert math.isnan(series.observations[1].value)
    assert series.observations[2].date == datetime.date(2026, 5, 31)


def test_missing_and_non_finite_values_become_missing():
    handler = documents(
        {"@frequency": "annual", "period": ["2021", "2022", "2023", "2024"], "value": [None, "", math.inf, 2.5]}
    )
    series = fetch(handler, [Request(hicp())])[0].series[0]
    assert [math.isnan(item.value) for item in series.observations] == [True, True, True, False]


def test_every_period_spelling_is_read():
    for reported, periods, labels in [
        ("annual", ["2025"], ["2025"]),
        ("quarterly", ["2026-Q2"], ["2026Q2"]),
        ("daily", ["2026-06-05"], ["2026-06-05"]),
    ]:
        series = fetch(documents({"@frequency": reported, "period": periods, "value": [1.0]}), [Request(hicp())])[
            0
        ].series[0]
        assert [item.period for item in series.observations] == labels


def test_a_missing_frequency_falls_back_to_the_catalog():
    handler = documents({"period": ["2025"], "value": [1.0]})
    assert fetch(handler, [Request(hicp(frequency=Frequency.ANNUAL))])[0].series[0].frequency is Frequency.ANNUAL
    failure = fetch(handler, [Request(hicp())])[0].failures[0]
    assert failure.outcome is Outcome.SOURCE_ERROR
    assert failure.reason.endswith("declare `frequency` in the catalog (DBnomics said '')")


def test_since_is_applied_after_the_download():
    handler = lambda _request: httpx.Response(200, text=fixture("dbnomics_series.json"))  # noqa: E731
    series = fetch(handler, [Request(hicp(), datetime.date(2026, 4, 15))])[0].series[0]
    assert [item.period for item in series.observations] == ["2026-04", "2026-05"]


def test_no_documents_is_not_found():
    failure = fetch(documents(), [Request(hicp())])[0].failures[0]
    assert (failure.outcome, failure.reason) == (Outcome.NOT_FOUND, "DBnomics has no series with this id")


def test_http_errors():
    not_found = fetch(lambda _request: httpx.Response(404, text="nope"), [Request(hicp())])[0].failures[0]
    assert (not_found.outcome, not_found.reason) == (Outcome.NOT_FOUND, "HTTP 404: nope")
    broken = fetch(lambda _request: httpx.Response(400, text="bad"), [Request(hicp())])[0].failures[0]
    assert broken.outcome is Outcome.SOURCE_ERROR


def test_a_period_that_does_not_match_the_frequency_fails_only_that_series():
    def handler(request):
        if request.url.params["series_ids"] == "X/Y/Z":
            return httpx.Response(
                200, json={"series": {"docs": [{"@frequency": "monthly", "period": ["2026-W05"], "value": [1.0]}]}}
            )
        return httpx.Response(200, text=fixture("dbnomics_series.json"))

    batches = fetch(handler, [Request(entry("X/Y/Z", "dbnomics")), Request(hicp())])
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "PeriodError" in batches[0].failures[0].reason
    assert batches[1].series[0].key == f"dbnomics:{HICP}"


def test_an_id_with_a_colon_keeps_its_source_as_the_part_before_the_first_colon():
    series_id = "IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp"
    series = fetch(
        documents({"@frequency": "annual", "period": ["2025"], "value": [1.0]}), [Request(entry(series_id, "dbnomics"))]
    )
    assert series[0].series[0].key == f"dbnomics:{series_id}"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/dbnomics_test.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'data_pipeline.store.sources.dbnomics'`

- [ ] **Step 3: Write the source**

Create `src/data_pipeline/store/sources/dbnomics.py`:

```python
"""DBnomics (https://db.nomics.world): a keyless aggregator of the IMF, the World Bank, the BIS,
the OECD, Eurostat and many others.

A series id is `<provider>/<dataset>/<series>`, for example
`Eurostat/prc_hicp_midx/M.I15.CP00.EA`. Observations arrive as two parallel arrays, `period`
and `value`. The endpoint has no date filter, so `since` is applied here after the download.
"""

import math
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import DECLARE_FREQUENCY, number, per_request, reject_params

URL = "https://api.db.nomics.world/v22/series"
OK = 200
NOT_FOUND = 404
NO_SERIES = "DBnomics has no series with this id"
FREQUENCIES: Mapping[str, Frequency] = {
    "annual": Frequency.ANNUAL,
    "quarterly": Frequency.QUARTERLY,
    "monthly": Frequency.MONTHLY,
    "weekly": Frequency.WEEKLY,
    "daily": Frequency.DAILY,
}


def read_observations(document: Mapping[str, Any], frequency: Frequency) -> tuple[Observation, ...]:
    observations = []
    for text, raw in zip(document.get("period") or [], document.get("value") or [], strict=True):
        period, day = read_period(str(text), frequency)
        value = number(raw)
        observations.append(Observation(period, day, value if math.isfinite(value) else math.nan))
    return tuple(sorted(observations, key=lambda observation: observation.date))


class Dbnomics:
    name = "dbnomics"
    kind = Kind.SERIES
    requests_per_minute = 60
    daily_budget: int | None = None

    def __init__(self, client: Client, credentials: Credentials) -> None:  # keyless: credentials are not read
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        yield from per_request(requests, self._download)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        response = self._client.get(
            self.name,
            URL,
            params={"series_ids": entry.source_id, "observations": "1"},
            per_minute=self.requests_per_minute,
        )
        if response.status_code != OK:
            outcome = Outcome.NOT_FOUND if response.status_code == NOT_FOUND else Outcome.SOURCE_ERROR
            return Failure(entry, outcome, f"HTTP {response.status_code}: {self._client.scrub(response.text[:300])}")
        documents = response.json()["series"]["docs"]
        if not documents:
            return Failure(entry, Outcome.NOT_FOUND, NO_SERIES)
        document = documents[0]
        reported = str(document.get("@frequency") or "")
        frequency = FREQUENCIES.get(reported.strip().lower()) or entry.frequency
        if frequency is None:
            return Failure(entry, Outcome.SOURCE_ERROR, f"{DECLARE_FREQUENCY} (DBnomics said {reported!r})")
        observations = read_observations(document, frequency)
        if request.since is not None:
            observations = tuple(item for item in observations if item.date >= request.since)
        return SeriesData(
            entry=entry,
            key=entry.key,
            name=str(document.get("series_name") or entry.source_id),
            frequency=frequency,
            observations=observations,
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/dbnomics_test.py -q`
Expected: `9 passed`

- [ ] **Step 5: Lint and type-check**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/dbnomics.py tests/unit/store
git commit -m "feat(store): add the DBnomics source"
```

### Task 8: Register the sources and check the macro catalog against them

**Files:**
- Modify: `src/data_pipeline/store/sources/__init__.py`
- Test: `tests/unit/store/macro_catalog_test.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/store/macro_catalog_test.py`. Its first test fails whenever the bundled YAML and the JSON it comes from are out of step; the other two check that every entry is valid for its source.

```python
"""The catalog shipped with the library: in step with its source, and valid for every source."""

import collections
import importlib.util
import json
import pathlib

from data_pipeline.store import sources
from data_pipeline.store.catalog import check_catalog, load_catalog
from data_pipeline.store.keys import Credentials
from data_pipeline.store.model import Frequency

from .helpers import client

ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "convert_macro_catalog.py"
JSON = ROOT / "src" / "data_pipeline" / "equity" / "config_handlers" / "macro_catalog.json"
BUNDLED = ROOT / "src" / "data_pipeline" / "store" / "catalogs" / "macro.yaml"


def converter():
    spec = importlib.util.spec_from_file_location("convert_macro_catalog", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_bundled_catalog_is_in_step_with_the_json_it_comes_from():
    rows = json.loads(JSON.read_text(encoding="utf-8"))
    expected = converter().convert(rows)
    assert BUNDLED.read_text(encoding="utf-8") == expected, "rerun scripts/convert_macro_catalog.py"


def test_every_entry_is_accepted_by_its_source():
    entries = load_catalog(pathlib.Path("macro"))
    http = client()
    instances = {name: sources.create(name, http, Credentials()) for name in sources.REGISTRY}
    check_catalog(entries, instances)
    assert collections.Counter(entry.source for entry in entries) == {
        "dbnomics": 1262,
        "fred": 24,
        "banxico": 6,
        "inegi": 2,
    }


def test_every_entry_keeps_the_column_name_as_its_alias_and_declares_a_frequency():
    entries = load_catalog(pathlib.Path("macro"))
    by_alias = {entry.alias: entry for entry in entries}
    assert len(by_alias) == len(entries) == 1294
    assert all(entry.alias.startswith("e_") and entry.name and entry.frequency for entry in entries)
    igae = by_alias["e_mx_igae"]
    assert (igae.key, igae.frequency, igae.params) == ("inegi:6207136901", Frequency.MONTHLY, {"bank": "BISE"})
    assert igae.attrs == {"region": "MX", "commercial_ok": "unverified"}
    assert by_alias["e_ar_budget_balance_gdp"].key == "dbnomics:IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/unit/store/macro_catalog_test.py -q`
Expected: `1 failed, 2 passed`; the failure is `CatalogError: ... unknown source 'dbnomics'`

- [ ] **Step 3: Register the four sources**

Replace `src/data_pipeline/store/sources/__init__.py` with:

```python
"""Registry of sources: name -> class, and the title each one is cited with."""

from collections.abc import Callable, Mapping

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.http import Client
from data_pipeline.store.keys import Credentials
from data_pipeline.store.sources.banxico import Banxico
from data_pipeline.store.sources.base import Source
from data_pipeline.store.sources.bls import Bls
from data_pipeline.store.sources.dbnomics import Dbnomics
from data_pipeline.store.sources.fred import Fred
from data_pipeline.store.sources.inegi import Inegi

Factory = Callable[[Client, Credentials], Source]

REGISTRY: Mapping[str, Factory] = {
    "banxico": Banxico,
    "bls": Bls,
    "dbnomics": Dbnomics,
    "fred": Fred,
    "inegi": Inegi,
}
TITLES: Mapping[str, str] = {
    "banxico": "Banxico SIE",
    "bls": "BLS",
    "dbnomics": "DBnomics",
    "fred": "FRED",
    "inegi": "INEGI",
}


def create(name: str, client: Client, credentials: Credentials) -> Source:
    """Build the source called `name`."""
    factory = REGISTRY.get(name)
    if factory is None:
        known = ", ".join(sorted(REGISTRY))
        msg = f"unknown source {name!r} (known sources: {known})"
        raise CatalogError(msg)
    return factory(client, credentials)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/Scripts/python -m pytest tests/unit/store/macro_catalog_test.py -q`
Expected: `3 passed`

- [ ] **Step 5: Lint, type-check, run every store test**

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

Run: `.venv/Scripts/python -m pytest tests/unit/store -q`
Expected: `221 passed`

- [ ] **Step 6: Commit**

```bash
git add src/data_pipeline/store/sources/__init__.py tests/unit/store/macro_catalog_test.py
git commit -m "feat(store): register BLS, Banxico, INEGI and DBnomics"
```

### Task 9: Live tests and acceptance scripts

**Files:**
- Create: `tests/live/bls_live_test.py`, `banxico_live_test.py`, `inegi_live_test.py`, `dbnomics_live_test.py`
- Rename and modify: `scripts/compare_fred_with_investment_process.py` to `scripts/compare_with_investment_process.py`
- Create: `scripts/bls_catalog_from_unemployment_analysis.py`, `scripts/compare_bls_with_unemployment_analysis.py`

- [ ] **Step 1: Write the four live tests**

None of them was run while this plan was written. They are off by default.

`tests/live/bls_live_test.py`:

```python
"""One real call to BLS. Off by default; run with:  pytest -m live tests/live

Needs BLS_API_KEY in the environment or in ./.env, and network access. It confirms the shape of
the answer the unit tests assume: period codes, the catalog block and the status field.
"""

import datetime

import pytest

from data_pipeline.store import keys
from data_pipeline.store.api import Store
from data_pipeline.store.keys import load_credentials


@pytest.mark.live
def test_bls_still_answers_in_the_expected_shape(tmp_path):
    if load_credentials().get(keys.BLS) is None:
        pytest.skip("BLS_API_KEY is not set")
    store = Store(tmp_path / "store")
    store.add("bls", ["LNS14000000"], start=datetime.date(2020, 1, 1))
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  bls       1 series")
    info = store.info("bls:LNS14000000")
    assert (info.frequency, info.seasonal_adjustment) == ("M", "SA")
    assert "Unemployment Rate" in info.name
    series = store.series("bls:LNS14000000")
    assert series["period"].iloc[0] == "2020-01"
    assert series["value"].between(2, 20).all()
```

`tests/live/banxico_live_test.py`:

```python
"""Two real calls to Banxico SIE. Off by default; run with:  pytest -m live tests/live

Needs BANXICO_TOKEN in the environment or in ./.env, and network access. It confirms the shape
of the metadata answer (periodicidad, unidad), which the unit tests take from the documentation.
"""

import datetime

import pytest

from data_pipeline.store import keys
from data_pipeline.store.api import Store
from data_pipeline.store.keys import load_credentials


@pytest.mark.live
def test_banxico_still_answers_in_the_expected_shape(tmp_path):
    if load_credentials().get(keys.BANXICO) is None:
        pytest.skip("BANXICO_TOKEN is not set")
    store = Store(tmp_path / "store")
    store.add("banxico", ["SF43718"], start=datetime.date(2024, 1, 1))
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  banxico   1 series")
    info = store.info("banxico:SF43718")
    assert info.frequency == "D"
    assert info.units != ""
    series = store.series("banxico:SF43718")
    assert series["period"].iloc[0] == "2024-01-02"
    assert len(series) > 400
    assert series["value"].between(5, 40).all()
```

`tests/live/inegi_live_test.py`:

```python
"""One real call to INEGI. Off by default; run with:  pytest -m live tests/live

Needs INEGI_TOKEN in the environment or in ./.env, and network access.
"""

import pytest

from data_pipeline.store import keys
from data_pipeline.store.api import Store
from data_pipeline.store.keys import load_credentials


@pytest.mark.live
def test_inegi_still_answers_in_the_expected_shape(tmp_path):
    if load_credentials().get(keys.INEGI) is None:
        pytest.skip("INEGI_TOKEN is not set")
    store = Store(tmp_path / "store")
    store.add("inegi", ["6207136901"], bank="BISE")
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  inegi     1 series")
    assert store.info("inegi:6207136901").frequency == "M"
    series = store.series("inegi:6207136901")
    assert len(series) > 300
    assert series["period"].iloc[0] == "1993-01"
```

`tests/live/dbnomics_live_test.py`:

```python
"""One real call to DBnomics. Off by default; run with:  pytest -m live tests/live

DBnomics needs no key, only network access.
"""

import pytest

from data_pipeline.store.api import Store


@pytest.mark.live
def test_dbnomics_still_answers_in_the_expected_shape(tmp_path):
    store = Store(tmp_path / "store")
    store.add("dbnomics", ["Eurostat/prc_hicp_midx/M.I15.CP00.EA"])
    report = store.sync()
    assert report.lines()[0].startswith("[ok]  dbnomics  1 series")
    info = store.info("dbnomics:Eurostat/prc_hicp_midx/M.I15.CP00.EA")
    assert info.frequency == "M"
    assert info.name != ""
    assert len(store.series("dbnomics:Eurostat/prc_hicp_midx/M.I15.CP00.EA")) > 200
```

- [ ] **Step 2: Generalize the comparison script**

```bash
git mv scripts/compare_fred_with_investment_process.py scripts/compare_with_investment_process.py
```

Replace its content with (the source is now the first argument):

```python
"""Acceptance check: one source of the store against Investment_Process's own store.

    python scripts/compare_with_investment_process.py fred C:/Proyectos/Investment_Process D:/datos/store

Works for the sources both stores keep one series per id: fred, banxico, inegi. Sync both stores
on the same day first. Exit code 0 when every series matches, 1 otherwise.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline
from data_pipeline.store.errors import UnknownSeriesError

TOLERANCE = 1e-9


def compare(source: str, origin: pathlib.Path, root: pathlib.Path) -> int:
    public = origin / "inputs" / "publicos"
    series = pd.read_parquet(public / "series.parquet")
    theirs_all = pd.read_parquet(public / "obs" / f"{source}.parquet")
    store = data_pipeline.Store(root)
    failures = 0
    for row in series[series["fuente"] == source].sort_values("id_fuente").to_dict(orient="records"):
        rows = theirs_all[theirs_all["clave"] == row["clave"]]
        rows = rows[rows["valor"].notna() & ~rows["proyeccion"].astype(bool)]
        theirs = rows.set_index("periodo")["valor"]
        try:
            ours = store.series(f"{source}:{row['id_fuente']}").set_index("period")["value"]
        except UnknownSeriesError:
            failures += 1
            print(f"[x] {row['id_fuente']:<14} not in the store")
            continue
        common = theirs.index.intersection(ours.index)
        gap = (theirs[common] - ours[common]).abs()
        limit = TOLERANCE * pd.concat([theirs[common].abs(), ours[common].abs()], axis=1).max(axis=1)
        different = int((gap > limit).sum())
        only_theirs = len(theirs.index.difference(ours.index))
        only_ours = len(ours.index.difference(theirs.index))
        ok = different == 0 and only_theirs == 0 and only_ours == 0
        failures += 0 if ok else 1
        print(
            f"[{'ok' if ok else 'x'}] {row['id_fuente']:<14} {len(common)} periods in common, "
            f"{different} different, {only_theirs} only theirs, {only_ours} only ours"
        )
    print(f"{failures} series differ" if failures else "every series matches")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(compare(sys.argv[1], pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])))
```

- [ ] **Step 3: Write the two BLS scripts**

`scripts/bls_catalog_from_unemployment_analysis.py`:

```python
"""Write a store catalog with every BLS series Unemployment_Analysis keeps.

    python scripts/bls_catalog_from_unemployment_analysis.py C:/Proyectos/Unemployment_Analysis D:/datos/bls.yaml

Series are grouped by the year their history starts, so the source asks for exactly the years
that exist instead of walking back to find them. The start year is the earlier of the one that
repository declares and the first year its cache actually holds: fourteen series are declared
as starting in 1990 but have data back to 1939.
"""

import pathlib
import sys

import pandas as pd
import yaml


def write(repository: pathlib.Path, target: pathlib.Path) -> int:
    cache = repository / "data" / "cache"
    meta = pd.read_parquet(cache / "series_meta.parquet").set_index("series_id")
    seen = pd.read_parquet(cache / "observations.parquet").groupby("series_id")["date"].min().dt.year
    starts = pd.concat([meta["history_start_year"], seen], axis=1).min(axis=1).astype(int)
    entries = [
        {"source": "bls", "ids": sorted(group.index), "start": f"{year}-01-01"}
        for year, group in starts.groupby(starts)
    ]
    text = yaml.safe_dump(entries, sort_keys=False, width=100).replace("start: '", "start: ").replace("-01-01'", "-01-01")
    target.write_text(text, encoding="utf-8", newline="\n")
    print(f"[ok] wrote {target}: {len(starts)} series in {len(entries)} groups")
    return 0


if __name__ == "__main__":
    sys.exit(write(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])))
```

`scripts/compare_bls_with_unemployment_analysis.py`:

```python
"""Acceptance check: the store's BLS series against Unemployment_Analysis's cache.

    python scripts/compare_bls_with_unemployment_analysis.py C:/Proyectos/Unemployment_Analysis D:/datos/store

Prints one line per series that differs and a summary. BLS revises recent months, so differences
on periods close to the date that cache was written are expected; the summary gives the oldest
period that differs so that anything older than that can be looked at. Exit code 0 when every
series matches, 1 otherwise.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline
from data_pipeline.store.errors import UnknownSeriesError

TOLERANCE = 1e-9


def compare(repository: pathlib.Path, root: pathlib.Path) -> int:
    cache = pd.read_parquet(repository / "data" / "cache" / "observations.parquet")
    cache = cache[cache["value"].notna()]
    store = data_pipeline.Store(root)
    series_ok = series_bad = values_different = 0
    oldest = None
    for series_id, rows in cache.groupby("series_id"):
        theirs = rows.set_index("date")["value"]
        try:
            ours = store.series(f"bls:{series_id}").set_index("date")["value"]
        except UnknownSeriesError:
            series_bad += 1
            print(f"[x] {series_id:<22} not in the store")
            continue
        common = theirs.index.intersection(ours.index)
        gap = (theirs[common] - ours[common]).abs()
        limit = TOLERANCE * pd.concat([theirs[common].abs(), ours[common].abs()], axis=1).max(axis=1)
        changed = gap[gap > limit]
        only_theirs = len(theirs.index.difference(ours.index))
        only_ours = len(ours.index.difference(theirs.index))
        if changed.empty and only_theirs == 0:
            series_ok += 1
            continue
        series_bad += 1
        values_different += len(changed)
        first = changed.index.min().date().isoformat() if len(changed) else "-"
        if len(changed) and (oldest is None or changed.index.min() < oldest):
            oldest = changed.index.min()
        print(
            f"[x] {series_id:<22} {len(common)} in common, {len(changed)} different (oldest {first}), "
            f"{only_theirs} only theirs, {only_ours} only ours"
        )
    print(
        f"{series_ok} series match, {series_bad} differ; {values_different} values differ"
        + (f", the oldest on {oldest.date().isoformat()}" if oldest is not None else "")
    )
    return 1 if series_bad else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])))
```

- [ ] **Step 4: Check that the default run leaves the live tests out**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1175 passed, 5 deselected`

Run: `.venv/Scripts/python -m ruff check .`
Expected: `All checks passed!`

- [ ] **Step 5: Commit**

```bash
git add tests/live scripts
git commit -m "test(store): add live tests for the new sources and the acceptance scripts"
```

### Task 10: Documentation

**Files:**
- Modify: `README.md`, `CHANGELOG.md`

- [ ] **Step 1: Update the README section**

In `README.md`, in the section `## Public-data store (preview)`, replace the sentence `FRED is the first source.` with:

```markdown
Sources: FRED, BLS, Banxico SIE, INEGI and DBnomics (which reaches the IMF, the World Bank, the
BIS, the OECD and Eurostat).
```

and replace the paragraph that starts with ``Put `FRED_API_KEY=...` `` (one line) with:

````markdown
Put the keys of the sources you use in a `.env` file in the folder you run from: `FRED_API_KEY`,
`BLS_API_KEY`, `BANXICO_TOKEN`, `INEGI_TOKEN`. DBnomics needs none. A curated catalog of 1,294
macro series ships with the library; use it by name:

```
python -m data_pipeline.store sync --root D:/data --catalog macro
python -m data_pipeline.store show e_us_cpi --root D:/data
```

With your own catalog:
````

- [ ] **Step 2: Add a CHANGELOG entry**

In `CHANGELOG.md`, as the first bullet under `## [Unreleased]` / `### Added`, insert:

```markdown
- Public-data store: four more sources, **BLS** (`BLS_API_KEY`; 50 series and 20 years per request, a persisted daily budget, native series ids), **Banxico SIE** (`BANXICO_TOKEN`), **INEGI** (`INEGI_TOKEN`; catalog fields `bank` and `area`) and **DBnomics** (no key). A catalog entry can now be written for one series (`id`) with its own `alias`, `name` and `frequency`; `frequency` is used when the source does not report one. The curated macro catalog (1,294 series) ships inside the package and is loaded by name: `--catalog macro`, with the `e_*` column names as aliases.
```

- [ ] **Step 3: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs(store): document the new sources and the bundled macro catalog"
```

### Task 11: Full verification and packaging

- [ ] **Step 1: Run every check**

Run: `.venv/Scripts/python -m pytest -q`
Expected: `1175 passed, 5 deselected`

Run: `.venv/Scripts/python -m ruff check . && PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 2: Check that the wheel ships the bundled catalog**

```bash
uv build --wheel --out-dir dist-check
.venv/Scripts/python -m zipfile -l dist-check/*.whl | grep -E "store/catalogs/macro.yaml|store/sources/(bls|banxico|inegi|dbnomics).py"
rm -rf dist-check
```

Expected: five lines, one for `macro.yaml` and one for each of the four sources.

### Task 12: Acceptance with real services

Every step here calls a real service with the user's keys or writes outside the repository. Run them only after the user says so, and confirm with the user where the keys are and which folder to use for the store. The keys are not in this repository; `BLS_API_KEY` is expected in `C:/Proyectos/Unemployment_Analysis/.env`, `BANXICO_TOKEN` and `INEGI_TOKEN` in `C:/Proyectos/Investment_Process/.env`. Pass them with `--env-file`, or export them for the session. Never print them.

`<STORE>` below is the folder the user chose for the acceptance store.

- [ ] **Step 1: The live tests**

Run: `.venv/Scripts/python -m pytest -m live tests/live -q`
Expected: `5 passed` (a source without its key shows as skipped; say which).

If the BLS or the Banxico test fails on the shape of the answer, compare the real answer with what the source expects, fix the source and its unit test together, and rerun the unit suite.

- [ ] **Step 2: BLS against Unemployment_Analysis**

```bash
.venv/Scripts/python scripts/bls_catalog_from_unemployment_analysis.py C:/Proyectos/Unemployment_Analysis <STORE>/bls.yaml
PYTHONPATH=src .venv/Scripts/python -m data_pipeline sync --root <STORE>/store --catalog <STORE>/bls.yaml --env-file C:/Proyectos/Unemployment_Analysis/.env
PYTHONPATH=src .venv/Scripts/python scripts/compare_bls_with_unemployment_analysis.py C:/Proyectos/Unemployment_Analysis <STORE>/store
```

Expected from the sync: about 1,107 of 1,143 series stored (that repository records 36 ids BLS does not publish), in roughly 100 requests, exit code 1 because of those 36.

Expected from the comparison: most series match. BLS revises recent months, so differences on periods near the date that cache was written (mid 2026) are expected. Any difference on a period older than 24 months before that date is a finding: stop and report it with the script's output.

- [ ] **Step 3: Banxico and INEGI against Investment_Process**

Write `<STORE>/mx.yaml` with the 13 Banxico ids and the 5 INEGI ids of Investment_Process's catalog:

```yaml
- source: banxico
  ids: [SF61745, SF43783, SF43936, SF43883, SF43886, SF44071, SF45384, SF60696, SF43718, SF43707,
        SP1, SP74625, SE27803]
- source: inegi
  ids: [737121, 736939, 444612]
- source: inegi
  ids: [6207136901, 6200093973]
  bank: BISE
```

```bash
PYTHONPATH=src .venv/Scripts/python -m data_pipeline sync --root <STORE>/store --catalog <STORE>/mx.yaml --env-file C:/Proyectos/Investment_Process/.env
PYTHONPATH=src .venv/Scripts/python scripts/compare_with_investment_process.py banxico C:/Proyectos/Investment_Process <STORE>/store
PYTHONPATH=src .venv/Scripts/python scripts/compare_with_investment_process.py inegi C:/Proyectos/Investment_Process <STORE>/store
```

Read the differences as was done for FRED: `only ours` on recent periods means that store has not been refreshed today; `different` on recent periods usually means a revision. A difference on old periods is a finding.

- [ ] **Step 4: DBnomics and the whole macro catalog**

```bash
PYTHONPATH=src .venv/Scripts/python -m data_pipeline sync --root <STORE>/macro --catalog macro --env-file C:/Proyectos/Investment_Process/.env
```

About 25 minutes the first time. Expected: one report line per source; the failed ids are listed. Ids fail when a provider retires a dataset (the catalog pins `IMF/WEO:2025-04`, for example). Give the user the list; do not edit the catalog without asking.

- [ ] **Step 5: A second sync of everything adds nothing**

Rerun the three sync commands of Steps 2 to 4.
Expected: `0 new` for every source, apart from observations published between the two runs.

- [ ] **Step 6: Report**

State which acceptance criteria of the spec were verified and how, and which were skipped and why. Do not report a skipped step as done.
