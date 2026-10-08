"""The macro catalog shipped with the library: valid for every source, one alias per e_* column."""

import collections
import pathlib
import re

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
    assert by_alias["e_ar_cpi"].key == "imf:IMF.STA,CPI/ARG.CPI._T.IX.M"  # the IFS mirror held no values
    assert by_alias["e_ar_fx_usd"].key == "imf:IMF.STA,ER/ARG.XDC_USD.EOP_RT.M"  # moved off the retired IFS


def test_every_series_still_on_dbnomics_says_why_it_is_stale():
    entries = load_catalog(pathlib.Path("macro"))
    on_dbnomics = [entry for entry in entries if entry.source == "dbnomics"]
    assert len(on_dbnomics) == 96
    assert all(entry.attrs.get("stale", "").startswith("2026-10: ") for entry in on_dbnomics)


def test_every_series_moved_close_but_not_equal_says_how_close():
    entries = load_catalog(pathlib.Path("macro"))
    close = [entry for entry in entries if "close_match" in entry.attrs]
    assert len(close) == 282
    assert all(entry.source in DIRECT for entry in close)
    assert all(entry.attrs["close_match"].startswith("2026-10: ") for entry in close)
    by_alias = {entry.alias: entry for entry in entries}
    assert by_alias["e_ar_exports_goods"].key == "imf:IMF.STA,ITG/ARG.XG.FOB_USD.A"


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



EURO_MEMBERS = {"AT", "BE", "DE", "ES", "FI", "FR", "GR", "IE", "IT", "NL", "PT"}


def test_the_euro_members_fx_columns_read_the_euro_against_the_dollar():
    """The IFS mirror held the legacy currencies, which end in 1998 (Greece 2000); since then a
    member's local currency per dollar is the euro per dollar, which the BIS publishes per member."""
    by_alias = {entry.alias: entry for entry in load_catalog(pathlib.Path("macro"))}
    for region in EURO_MEMBERS | {"EZ"}:
        entry = by_alias[f"e_{region.lower()}_fx_usd"]
        code = "XM" if region == "EZ" else region
        assert entry.key == f"bis:WS_XRU/M.{code}.EUR.E"
        assert "euro per USD" in entry.name
        assert "stale" not in entry.attrs


def test_the_short_rates_that_ended_at_the_imf_read_a_live_rate_and_say_which():
    """The IFS bill and money-market rates of these economies end between 2013 and 2025 (or hold no
    values); the OECD's 3-month interbank rate, or the IMF's money-market rate, continues."""
    by_alias = {entry.alias: entry for entry in load_catalog(pathlib.Path("macro"))}
    for region in ("au", "ca", "cl", "cn", "id", "il", "in", "jp", "pl", "uk"):
        entry = by_alias[f"e_{region}_short_rate"]
        assert entry.source == "oecd"
        assert ".M.IR3TIB.PA." in entry.source_id
        assert "3-month interbank rate" in entry.name
    assert by_alias["e_my_short_rate"].key == "imf:IMF.STA,MFS_IR/MYS.MMRT_RT_PT_A_PT.M"
    assert "money-market rate" in by_alias["e_my_short_rate"].name


def test_the_series_the_mirror_holds_no_values_for_say_so():
    """DBnomics holds no values for these and no publisher has a replacement: the alias stays, with
    a note that it has no data, rather than the "no candidate" that read like a lookup miss."""
    by_alias = {entry.alias: entry for entry in load_catalog(pathlib.Path("macro"))}
    for alias in ("e_tr_short_rate", "e_ph_ind_prod"):
        assert by_alias[alias].attrs["stale"].startswith("2026-10: no data: the mirror holds no values")


def test_a_stale_series_at_its_publisher_says_what_its_last_period_was_and_when():
    """Outside DBnomics a series is marked stale only from a check against its publisher: the note
    names the day of the check, and the last period unless it says why the series ended."""
    entries = load_catalog(pathlib.Path("macro"))
    at_publishers = [entry for entry in entries if entry.source != "dbnomics"]
    stale = {entry.alias: entry.attrs["stale"] for entry in at_publishers if "stale" in entry.attrs}
    assert len(stale) == 62
    checked = re.compile(r"2026-10: .*\(checked \d{4}-\d{2}-\d{2}\)")
    assert all(checked.match(note) for note in stale.values())
    euro = ("at", "be", "de", "es", "fr", "gr", "it", "nl", "pt")
    for region in euro:  # the national central banks' rates end with the euro
        assert "e_ecb_rate" in stale[f"e_{region}_policy_rate"]
    assert stale["e_de_unemployment"].startswith("2026-10: last period at the publisher 2026-02")
