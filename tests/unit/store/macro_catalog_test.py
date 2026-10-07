"""The macro catalog shipped with the library: valid for every source, one alias per e_* column."""

import collections
import pathlib

from data_pipeline.credentials import Credentials
from data_pipeline.store import sources
from data_pipeline.store.catalog import check_catalog, load_catalog
from data_pipeline.store.model import Frequency

from .helpers import client

DIRECT = ("worldbank", "bis", "eurostat", "ecb", "imf", "oecd")


def test_every_entry_is_accepted_by_its_source():
    entries = load_catalog(pathlib.Path("macro"))
    http = client()
    instances = {name: sources.create(name, http, Credentials()) for name in sources.REGISTRY}
    check_catalog(entries, instances)
    counts = collections.Counter(entry.source for entry in entries)
    assert (counts["fred"], counts["banxico"], counts["inegi"]) == (24, 6, 2)
    assert counts["dbnomics"] + sum(counts[name] for name in DIRECT) == 1261
    assert counts["worldbank"] > 0
    assert counts["bis"] > 0


def test_every_entry_keeps_the_column_name_as_its_alias_and_declares_a_frequency():
    entries = load_catalog(pathlib.Path("macro"))
    by_alias = {entry.alias: entry for entry in entries}
    assert len(by_alias) == len(entries) == 1293
    assert all(entry.alias.startswith("e_") and entry.name and entry.frequency for entry in entries)
    igae = by_alias["e_mx_igae"]
    assert (igae.key, igae.frequency, igae.params) == ("inegi:6207136901", Frequency.MONTHLY, {"bank": "BISE"})
    assert igae.attrs == {"region": "MX", "commercial_ok": "unverified"}
    assert by_alias["e_ar_cpi"].key == "dbnomics:IMF/IFS/M.AR.PCPI_IX"
    assert by_alias["e_ar_fx_usd"].key == "imf:IMF.STA,ER/ARG.XDC_USD.EOP_RT.M"  # moved off the retired IFS


def test_every_series_still_on_dbnomics_says_why_it_is_stale():
    entries = load_catalog(pathlib.Path("macro"))
    on_dbnomics = [entry for entry in entries if entry.source == "dbnomics"]
    assert len(on_dbnomics) == 400
    assert all(entry.attrs.get("stale", "").startswith("2026-10: ") for entry in on_dbnomics)
    assert all("stale" not in entry.attrs for entry in entries if entry.source != "dbnomics")


def test_every_entry_names_its_region_for_the_column_picker():
    entries = load_catalog(pathlib.Path("macro"))
    assert all(entry.attrs.get("region") for entry in entries)
    assert len({entry.attrs["region"] for entry in entries}) == 44


def test_the_hicp_columns_are_named_for_what_they_hold():
    """IFS `PCPIHA_IX` is the harmonized all-items CPI; the catalog once called those columns core CPI."""
    entries = load_catalog(pathlib.Path("macro"))
    hicp = [entry for entry in entries if entry.alias.endswith("_hicp")]
    assert len(hicp) == 21
    assert all("HICP" in entry.name for entry in hicp)
    assert [entry.alias for entry in entries if "core_cpi" in entry.alias] == ["e_us_core_cpi"]

