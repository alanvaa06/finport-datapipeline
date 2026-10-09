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


# The units a name may end with, in parentheses: "(<unit>)", "(<unit>, <detail>)" or "(<unit>; <detail>)".
UNITS = frozenset(
    {
        "USD", "USD millions", "USD billions", "current USD", "constant USD", "EUR millions", "LCU per USD",
        "MXN per USD", "index", "%", "% p.a.", "% GDP", "% of active", "% balance", "YoY %",
        "percentage points", "thousands", "persons",
    }
)  # fmt: skip


def concept_of(alias):
    return re.sub(r"^e_[a-z]{2,3}_", "", alias)


def unit_of(name):
    found = re.search(r"\(([^()]*)\)$", name)
    return re.split(r"[;,]", found.group(1))[0].strip() if found else None


def test_every_name_ends_with_a_unit_its_family_shares_unless_it_says_it_differs():
    """Goods trade and reserves mixed USD (the IMF) and millions of USD (the DBnomics mirror of DOT
    and IFS) under the same "(USD)" name: a factor of 10^6 inside one indicator family. Now every
    name ends with its real unit, every family shares one, and an entry in another unit carries
    attrs.units_differ saying which."""
    entries = load_catalog(pathlib.Path("macro"))
    families = collections.defaultdict(list)
    for entry in entries:
        assert unit_of(entry.name) in UNITS, (entry.alias, entry.name)
        families[concept_of(entry.alias)].append(entry)
    for concept, members in families.items():
        shared = {unit_of(entry.name) for entry in members if "units_differ" not in entry.attrs}
        assert len(shared) == 1, (concept, shared)
        for entry in members:
            if "units_differ" in entry.attrs:
                assert unit_of(entry.name) not in shared, entry.alias
                assert entry.attrs["units_differ"].startswith(unit_of(entry.name)), entry.alias
    differ = collections.Counter(concept_of(entry.alias) for entry in entries if "units_differ" in entry.attrs)
    assert differ == {
        "exports_goods": 10, "imports_goods": 13, "trade_balance_goods": 7, "reserves": 7, "gdp_nominal": 1
    }  # fmt: skip
    mirror = [entry for entry in entries if "as the retired IMF" in entry.attrs.get("units_differ", "")]
    assert len(mirror) == 36
    assert all(entry.source == "dbnomics" for entry in mirror)


def adjusted(entry):
    """Whether the id selects a seasonally adjusted series, for the sources whose ids say so."""
    flow, _, key = entry.source_id.partition("/")
    parts = key.split(".")
    if entry.source == "oecd":
        return parts[5 if "DSD_STES@" in flow else 4] == "Y"
    if entry.source == "eurostat":
        return bool({"SA", "SCA"} & set(parts))
    if entry.source == "dbnomics" and entry.source_id.startswith("OECD/MEI/"):
        return parts[-2].endswith("OBSA")
    return None


def test_a_name_says_seasonally_adjusted_only_of_an_adjusted_series():
    """Twenty-two OECD money aggregates were named "(index, SA)" while their id asks for the series
    neither seasonally nor calendar adjusted."""
    for entry in load_catalog(pathlib.Path("macro")):
        says = re.search(r"\bSA\b", entry.name) is not None
        if adjusted(entry) is not None and (says or "not seasonally adjusted" in entry.name):
            assert says == adjusted(entry), (entry.alias, entry.name)


def test_the_short_rates_say_which_rate_and_the_pmi_columns_that_they_are_not_pmis():
    entries = load_catalog(pathlib.Path("macro"))
    rates = [entry for entry in entries if concept_of(entry.alias) == "short_rate"]
    kinds = collections.Counter(entry.name.split(": ", 1)[1].removesuffix(" (% p.a.)") for entry in rates)
    assert set(kinds) == {
        "3-month interbank rate, OECD", "money-market rate", "Treasury bill yield", "Treasury bill rate", "deposit rate"
    }  # fmt: skip
    assert kinds["deposit rate"] == 1  # e_ch_short_rate: the IMF's MFS135 for Switzerland is its deposit rate
    pmi = [entry for entry in entries if concept_of(entry.alias) == "pmi_mfg"]
    assert all(".BCICP.PB." in entry.source_id and "not a PMI (% balance)" in entry.name for entry in pmi)


# The commercial_ok flag the README documents for each source.
COMMERCIAL_OK = {
    "worldbank": "yes", "eurostat": "yes", "imf": "restricted", "bis": "restricted", "oecd": "restricted",
    "dbnomics": "restricted", "fred": "no", "banxico": "unverified", "inegi": "unverified", "ecb": "unverified",
}  # fmt: skip


def test_commercial_ok_follows_the_rule_the_readme_documents_for_each_source():
    entries = load_catalog(pathlib.Path("macro"))
    assert {entry.source for entry in entries} == set(COMMERCIAL_OK)
    assert all(entry.attrs["commercial_ok"] == COMMERCIAL_OK[entry.source] for entry in entries)
    readme = (pathlib.Path(__file__).resolve().parents[3] / "README.md").read_text(encoding="utf-8")
    assert "- `unverified`: Banxico, INEGI and the ECB." in readme
