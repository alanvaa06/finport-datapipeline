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
