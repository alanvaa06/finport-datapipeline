"""The finder that moves the macro catalog's DBnomics series to their publisher: its rule, its
catalog rewrite and its parsers. No network."""

import importlib.util
import pathlib

import pytest
import yaml

from data_pipeline.store.catalog import load_catalog

ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "replace_dbnomics_ids.py"

CATALOG = """# a comment
- source: imf
  id: IMF.RES,WEO/ARG.GGXCNL_NGDP.A
  alias: e_ar_budget_balance_gdp
  name: Argentina budget balance
  frequency: A
  attrs:
    region: AR
    commercial_ok: restricted
- source: dbnomics
  id: IMF/IFS/M.AR.PCPI_IX
  alias: e_ar_cpi
  name: Argentina CPI (all items, index)
  frequency: M
  attrs:
    region: AR
    commercial_ok: restricted
- source: dbnomics
  id: IMF/DOT/A.AR.TXG_FOB_USD.W00
  alias: e_ar_exports_goods
  name: Argentina goods exports, FOB (USD)
  frequency: A
  attrs:
    region: AR
    commercial_ok: restricted
    stale: "2026-09: an older note"
- source: dbnomics
  id: OECD/MEI/AUS.MANMM101.IXOBSA.M
  alias: e_au_m1
  name: Australia M1
  frequency: M
"""


@pytest.fixture(scope="module")
def finder():
    spec = importlib.util.spec_from_file_location("replace_dbnomics_ids", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def months(values, start=(2020, 1)):
    year, month = start
    out = {}
    for value in values:
        out[f"{year:04d}-{month:02d}"] = value
        month += 1
        if month > 12:
            year, month = year + 1, 1
    return out


def test_periods_are_spelled_one_way(finder):
    assert finder.normalize("2024-M05") == "2024-05"
    assert finder.normalize("2024M05") == "2024-05"
    assert finder.normalize("2024-05") == "2024-05"
    assert finder.normalize("2024-Q1") == "2024-Q1"
    assert finder.normalize("2024") == "2024"


def test_a_candidate_equal_on_enough_periods_and_newer_is_accepted(finder):
    reference = months([100.0 + i for i in range(30)])
    candidate = {f"{p[:4]}-M{p[5:]}": v for p, v in months([100.0 + i for i in range(36)]).items()}
    outcome = finder.compare(reference, candidate)
    assert (outcome["common"], outcome["gap"], outcome["newer"], outcome["ratio"]) == (30, 0.0, True, 1.0)
    assert finder.accepted(outcome)
    assert finder.reason(outcome) == "accepted"


def test_rounding_differences_pass_the_tolerance_and_revisions_do_not(finder):
    reference = months([100.0 + i for i in range(30)])
    rounded = months([round((100.0 + i) * (1 + 5e-5), 4) for i in range(36)])
    assert finder.accepted(finder.compare(reference, rounded), 1e-4)
    assert not finder.accepted(finder.compare(reference, rounded), 1e-6)
    revised = months([(100.0 + i) * 1.01 for i in range(36)])
    outcome = finder.compare(reference, revised)
    assert not finder.accepted(outcome)
    assert finder.reason(outcome).startswith("values differ (max relative gap 9.9e-03")


def test_too_few_common_periods_not_newer_and_other_units_are_told_apart(finder):
    reference = months([100.0 + i for i in range(30)])
    short = months([100.0 + i for i in range(10)])
    assert finder.reason(finder.compare(reference, short)) == "only 10 periods in common"
    same_end = months([100.0 + i for i in range(30)])
    assert finder.reason(finder.compare(reference, same_end)) == "publisher has nothing newer (last 2022-06)"
    millions = months([(100.0 + i) * 1e6 for i in range(36)])
    outcome = finder.compare(reference, millions)
    assert finder.reason(outcome) == "same values in other units (x1e-6)"
    assert (outcome["scale"], outcome["scaled_gap"]) == (-6, 0.0)
    assert not finder.accepted(outcome)
    assert finder.accepted(outcome, units=True)
    assert finder.reason(None) == "no candidate"
    assert finder.power_of_ten(1e6) == 6
    assert finder.power_of_ten(1.0) is None
    assert finder.power_of_ten(0.5) is None
    assert finder.power_of_ten(None) is None


def test_best_prefers_an_accepted_candidate_and_then_the_closest(finder):
    reference = months([100.0 + i for i in range(30)])
    candidates = {
        "A.far": months([(100.0 + i) * 1.1 for i in range(36)]),
        "B.close": months([(100.0 + i) * 1.001 for i in range(36)]),
        "C.exact": months([100.0 + i for i in range(36)]),
        "D.short": months([100.0 + i for i in range(5)]),
    }
    key, outcome = finder.best(reference, candidates)
    assert (key, finder.accepted(outcome)) == ("C.exact", True)
    del candidates["C.exact"]
    key, outcome = finder.best(reference, candidates)
    assert key == "B.close"
    assert finder.best(reference, {}) is None


def test_every_region_of_the_catalog_has_a_country_code(finder):
    regions = {entry.attrs["region"] for entry in load_catalog(pathlib.Path("macro"))}
    assert regions <= set(finder.ISO3)
    assert finder.ISO3["MX"] == "MEX"


def test_every_concept_on_dbnomics_has_a_template(finder):
    text = (ROOT / "src/data_pipeline/store/catalogs/macro.yaml").read_text(encoding="utf-8")
    entries = finder.dbnomics_entries(text)
    concepts = {finder.concept_of(entry["alias"]) for entry in entries}
    assert concepts - set(finder.FLOWS) == set()
    assert finder.FLOWS["ind_prod"] == ("oecd", "DSD_STES@DF_INDSERV", ".{f}.PRVM.IX.BTE....")


def test_rewrite_moves_accepted_series_and_annotates_the_rest_leaving_everything_else_alone(finder):
    results = {
        "e_ar_cpi": {
            "status": "candidate",
            "source": "imf",
            "id": "IMF.STA,CPI/ARG.CPI._T.SRP_IX.M",
            "outcome": {"common": 100, "gap": 0.0, "newer": True, "last": "2026-M07", "ratio": 1.0},
        },
        "e_ar_exports_goods": {
            "status": "candidate",
            "source": "imf",
            "id": "IMF.STA,ITG/ARG.XG.FOB_USD.A",
            "outcome": {"common": 70, "gap": 1.0, "newer": True, "last": "2025", "ratio": 1e-6, "scale": -6},
        },
    }
    results["e_ar_exports_goods"]["outcome"]["scaled_gap"] = 0.0
    rewritten = finder.rewrite(CATALOG, results, 1e-4, stamp="2026-10")
    entries = {entry["alias"]: entry for entry in yaml.safe_load(rewritten)}
    assert (entries["e_ar_cpi"]["source"], entries["e_ar_cpi"]["id"]) == ("imf", "IMF.STA,CPI/ARG.CPI._T.SRP_IX.M")
    assert "stale" not in entries["e_ar_cpi"]["attrs"]
    assert entries["e_ar_exports_goods"]["source"] == "dbnomics"
    assert entries["e_ar_exports_goods"]["attrs"] == {
        "region": "AR",
        "commercial_ok": "restricted",
        "stale": "2026-10: same values in other units (x1e-6)",
    }
    assert entries["e_au_m1"]["attrs"] == {"stale": "2026-10: no candidate"}
    assert entries["e_ar_budget_balance_gdp"]["source"] == "imf"
    assert rewritten.startswith("# a comment\n- source: imf\n")
    assert "  name: Argentina CPI (all items, index)\n  frequency: M\n  attrs:\n    region: AR\n" in rewritten
    assert finder.rewrite(rewritten, results, 1e-4, stamp="2026-10") == rewritten  # idempotent


def test_parsers_merge_a_series_split_in_several_elements(finder):
    imf = b"""<?xml version="1.0"?>
<message:StructureSpecificData xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message">
<message:DataSet>
<Series COUNTRY="ARG" INDICATOR="XDC_USD" TYPE_OF_TRANSFORMATION="EOP_RT" FREQUENCY="M" SCALE="0">
<Obs TIME_PERIOD="2024-M01" OBS_VALUE="800.5"/><Obs TIME_PERIOD="2024-M02" OBS_VALUE="NaN"/>
</Series>
<Series COUNTRY="ARG" INDICATOR="XDC_USD" TYPE_OF_TRANSFORMATION="EOP_RT" FREQUENCY="M" SCALE="0">
<Obs TIME_PERIOD="2024-M03" OBS_VALUE="850"/>
</Series>
</message:DataSet>
</message:StructureSpecificData>"""
    series = finder.parse_imf(imf, finder.DIMENSIONS["ER"])
    assert series == {"ARG.XDC_USD.EOP_RT.M": {"2024-M01": 800.5, "2024-M03": 850.0}}
    generic = b"""<?xml version="1.0"?>
<message:GenericData xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
 xmlns:generic="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic">
<message:DataSet>
<generic:Series>
<generic:SeriesKey><generic:Value id="REF_AREA" value="AUS"/><generic:Value id="FREQ" value="M"/></generic:SeriesKey>
<generic:Obs><generic:ObsDimension value="2024-01"/><generic:ObsValue value="101.5"/></generic:Obs>
</generic:Series>
<generic:Series>
<generic:SeriesKey><generic:Value id="REF_AREA" value="AUS"/><generic:Value id="FREQ" value="M"/></generic:SeriesKey>
<generic:Obs><generic:ObsDimension value="2024-02"/><generic:ObsValue value="102"/></generic:Obs>
</generic:Series>
</message:DataSet>
</message:GenericData>"""
    assert finder.parse_generic(generic) == {"AUS.M": {"2024-01": 101.5, "2024-02": 102.0}}


def test_rewrite_with_units_moves_the_scaled_series_and_records_the_factor(finder):
    outcome = {"common": 70, "gap": 1.0, "newer": True, "last": "2025", "ratio": 1e-6}
    outcome.update(scale=-6, scaled_gap=0.0)
    results = {
        "e_ar_exports_goods": {
            "status": "candidate",
            "source": "imf",
            "id": "IMF.STA,ITG/ARG.XG.FOB_USD.A",
            "outcome": outcome,
        },
    }
    rewritten = finder.rewrite(CATALOG, results, 1e-4, stamp="2026-10", units=True)
    entries = {entry['alias']: entry for entry in yaml.safe_load(rewritten)}
    moved = entries["e_ar_exports_goods"]
    assert (moved["source"], moved["id"]) == ("imf", "IMF.STA,ITG/ARG.XG.FOB_USD.A")
    assert moved["attrs"] == {
        "region": "AR",
        "commercial_ok": "restricted",
        "units_changed": "2026-10: the publisher's values are 1e6 times the mirror's (other units)",
    }
    assert finder.rewrite(rewritten, results, 1e-4, stamp="2026-10", units=True) == rewritten
    assert "replaced, other units" in finder.report(results, units=True)


def test_publisher_calls_are_kept_on_disk_so_a_second_run_needs_no_network(finder, tmp_path, monkeypatch):
    import httpx

    monkeypatch.setattr(finder.time, "sleep", lambda _seconds: None)
    body = b"""<?xml version="1.0"?>
<message:StructureSpecificData xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message">
<message:DataSet><Series COUNTRY="ARG" INDICATOR="XDC_USD" TYPE_OF_TRANSFORMATION="EOP_RT" FREQUENCY="M">
<Obs TIME_PERIOD="2024-M01" OBS_VALUE="800.5"/></Series></message:DataSet></message:StructureSpecificData>"""
    calls = []

    def answer(request):
        calls.append(request)
        return httpx.Response(200, content=body)

    with httpx.Client(transport=httpx.MockTransport(answer)) as client:
        first = finder.Publishers(client, tmp_path).series("imf", "ER", "ARG...M", "2000")

    def offline(_request):
        msg = "offline"
        raise httpx.ConnectError(msg)

    with httpx.Client(transport=httpx.MockTransport(offline)) as client:
        again = finder.Publishers(client, tmp_path).series("imf", "ER", "ARG...M", "2000")
    assert len(calls) == 1
    assert again == first == (200, {"ARG.XDC_USD.EOP_RT.M": {"2024-M01": 800.5}})


def test_a_rebased_index_is_close_but_a_rescaled_level_is_not(finder):
    reference = months([100.0 + i for i in range(30)])
    rebased = months([(100.0 + i) * 0.8 for i in range(36)])
    outcome = finder.compare(reference, rebased, "cpi")
    assert (outcome["how"], outcome["median_gap"]) == ("rebased", pytest.approx(0.0, abs=1e-12))
    assert finder.close(outcome)
    assert not finder.accepted(outcome)
    assert finder.accepted(outcome, near=True)
    assert not finder.close(finder.compare(reference, rebased, "reserves"))


def test_other_units_are_close_for_any_concept(finder):
    reference = months([100.0 + i for i in range(30)])
    outcome = finder.compare(reference, months([(100.0 + i) * 1e6 for i in range(36)]), "reserves")
    assert (outcome["how"], finder.close(outcome)) == ("other units", True)


def test_close_bounds_both_the_typical_and_the_worst_gaps(finder):
    reference = months([100.0 + i for i in range(30)])
    slightly = months([(100.0 + i) * (1 + finder.CLOSE_MEDIAN / 2) for i in range(36)])
    outcome = finder.compare(reference, slightly, "consumer_confidence")
    assert (outcome["how"], finder.close(outcome)) == ("same units", True)
    typical = months([(100.0 + i) * (1 + finder.CLOSE_MEDIAN * 2) for i in range(36)])
    assert not finder.close(finder.compare(reference, typical, "consumer_confidence"))
    worst = months([(100.0 + i) * (1.5 if i % 3 == 0 else 1.0) for i in range(36)])
    outcome = finder.compare(reference, worst, "consumer_confidence")
    assert outcome["median_gap"] == 0.0
    assert not finder.close(outcome)
    assert not finder.close(finder.compare(reference, months([100.0 + i for i in range(30)]), "cpi"))  # not newer


def test_a_rate_gap_is_measured_in_points_below_one(finder):
    reference = months([0.10] * 30)
    candidate = months([0.10 + finder.CLOSE_MEDIAN / 2] * 36)
    assert finder.close(finder.compare(reference, candidate, "short_rate"))
    assert not finder.close(finder.compare(reference, candidate, "reserves"))


def test_best_prefers_a_close_candidate_to_a_far_one(finder):
    reference = months([100.0 + i for i in range(30)])
    candidates = {
        "A.far": months([(100.0 + i) * (1.1 if i % 2 else 0.9) for i in range(36)]),
        "B.rebased": months([(100.0 + i) * 0.5 for i in range(36)]),
    }
    key, outcome = finder.best(reference, candidates, "cpi")
    assert (key, finder.close(outcome)) == ("B.rebased", True)


def test_rewrite_with_close_moves_the_series_and_says_how_close_it_is(finder):
    outcome = {"common": 120, "gap": 0.2, "newer": True, "last": "2026-M07", "ratio": 1.25}
    outcome.update(how="rebased", factor=0.8, median_gap=0.002, p90_gap=0.01)
    results = {
        "e_ar_cpi": {"status": "candidate", "source": "imf", "id": "IMF.STA,CPI/ARG.CPI._T.IX.M", "outcome": outcome},
    }
    kept = {entry["alias"]: entry for entry in yaml.safe_load(finder.rewrite(CATALOG, results, 1e-4, stamp="2026-10"))}
    assert kept["e_ar_cpi"]["source"] == "dbnomics"
    rewritten = finder.rewrite(CATALOG, results, 1e-4, stamp="2026-10", near=True)
    moved = {entry["alias"]: entry for entry in yaml.safe_load(rewritten)}["e_ar_cpi"]
    assert (moved["source"], moved["id"]) == ("imf", "IMF.STA,CPI/ARG.CPI._T.IX.M")
    assert moved["attrs"] == {
        "region": "AR",
        "commercial_ok": "restricted",
        "close_match": (
            "2026-10: rebased; relative gap to the mirror: median 2.0e-03, 90th percentile 1.0e-02 over 120 periods"
        ),
    }
    assert finder.rewrite(rewritten, results, 1e-4, stamp="2026-10", near=True) == rewritten
    assert "replaced, close" in finder.report(results, near=True)


def test_a_concept_only_takes_candidates_of_its_own_index(finder):
    assert finder.fits("hicp", "FRA.HICP._T.IX.M")
    assert not finder.fits("hicp", "FRA.CPI._T.IX.M")
    assert finder.fits("cpi", "FRA.CPI._T.IX.M")
    assert not finder.fits("cpi", "FRA.HICP._T.IX.M")
    assert finder.fits("reserves", "FRA.IRFCLDT1_IRFCL65_USD.S1X.M")
