"""Point the macro catalog's DBnomics series at the dataset that replaced theirs at the publisher.

    python scripts/replace_dbnomics_ids.py reference --cache D:/tmp/b2
    python scripts/replace_dbnomics_ids.py find --cache D:/tmp/b2 [--regions AR AU] [--recent 10]
    python scripts/replace_dbnomics_ids.py report --cache D:/tmp/b2 [--tolerance 1e-4]
    python scripts/replace_dbnomics_ids.py write --cache D:/tmp/b2 [--tolerance 1e-4] [--accept-units] [--close]

`reference` caches the values DBnomics still serves for every catalog series on DBnomics (several
series a call). `find` asks the publisher, one wildcard call per country and dataflow (the IMF),
or per dataflow for every country (the OECD), and keeps for each series the candidate whose
values are closest to DBnomics'. `report` tabulates the outcome per concept. `write` rewrites
`src/data_pipeline/store/catalogs/macro.yaml`: a series whose candidate equals DBnomics on at
least 24 common periods, and reaches a newer period, gets the new source and id; every other
series gets `attrs.stale` saying why. Equal means that on every common period the two values
differ by no more than their published rounding (half a unit of the last decimal each series
publishes, so an index published with one decimal may differ by 0.05 from the same index
published with two), or by the relative `--tolerance` (float noise in unrounded series).
`--recent YEARS` (given to `find`) measures the close rule below on the last YEARS years of
common periods only, so a revision of the 1950s does not keep a series that agrees today on the
mirror. `--accept-units` also moves a series whose
values equal DBnomics' once scaled by a power of ten (the publisher changed units), recording the
factor in `attrs.units_changed`. `--close` also moves a series that the publisher re-estimated:
scaled by one, a power of ten or (an index) any rebasing factor, its relative gap to DBnomics has
a median within CLOSE_MEDIAN and a 90th percentile within CLOSE_P90; `attrs.close_match` records
how and how close. Public services, no keys; the OECD at one call a minute. Each publisher answer
is kept under `--cache`/calls, so `find` runs again offline (delete results.json first).
"""

import argparse
import collections
import decimal
import json
import math
import pathlib
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import httpx
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
CATALOG = ROOT / "src" / "data_pipeline" / "store" / "catalogs" / "macro.yaml"
DBNOMICS_URL = "https://api.db.nomics.world/v22/series"
IMF_URL = "https://api.imf.org/external/sdmx/2.1/data/IMF.STA,{flow}/{key}?startPeriod={start}"
OECD_URL = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,{flow}/{key}?startPeriod={start}"
GENERIC = "application/vnd.sdmx.genericdata+xml;version=2.1"
DBNOMICS_BATCH = 20
OECD_INTERVAL = 61.0
# Equal: within the published rounding of both series (see `within_rounding`), or within this
# relative gap, which only absorbs float noise in series published unrounded. On its own, 1e-4
# over the whole history kept on the mirror series that differ only by rounding: an index of
# about 100 published with one decimal carries up to 5e-4 of relative rounding error.
TOLERANCE = 1e-4
MAX_DECIMALS = 12  # beyond this a decimal is float noise, not a published digit
MIN_COMMON = 24
# --close: the publisher re-estimates what the retired dataset held (the OECD seasonally adjusts
# again, an index is rebased), so its values are close to the mirror's, not equal
CLOSE_MEDIAN = 0.01  # the typical relative gap on the common periods
CLOSE_P90 = 0.05  # the 90th percentile of the relative gaps: a few outliers, not a different series
INDEXES = frozenset({"cpi", "core_cpi", "hicp", "ppi", "ind_prod", "m1", "broad_money"})  # may be rebased
RATES = frozenset({"short_rate", "unemployment"})  # per cent: a gap below one is measured in points
STAMP = "2026-10"
ISO3: Mapping[str, str] = {
    "AR": "ARG", "AT": "AUT", "AU": "AUS", "BE": "BEL", "BR": "BRA", "CA": "CAN", "CH": "CHE", "CL": "CHL",
    "CN": "CHN", "CO": "COL", "CZ": "CZE", "DE": "DEU", "DK": "DNK", "ES": "ESP", "EZ": "U2", "FI": "FIN",
    "FR": "FRA", "GR": "GRC", "HK": "HKG", "HU": "HUN", "ID": "IDN", "IE": "IRL", "IL": "ISR", "IN": "IND",
    "IT": "ITA", "JP": "JPN", "KR": "KOR", "MX": "MEX", "MY": "MYS", "NL": "NLD", "NO": "NOR", "NZ": "NZL",
    "PE": "PER", "PH": "PHL", "PL": "POL", "PT": "PRT", "RU": "RUS", "SA": "SAU", "SE": "SWE", "TH": "THA",
    "TR": "TUR", "UK": "GBR", "US": "USA", "ZA": "ZAF",
}  # fmt: skip
# IMF dataflow -> its dimensions in key order (the IMF answers structure-specific XML: the key is in attributes)
DIMENSIONS: Mapping[str, Sequence[str]] = {
    "CPI": ("COUNTRY", "INDEX_TYPE", "COICOP_1999", "TYPE_OF_TRANSFORMATION", "FREQUENCY"),
    "PPI": ("COUNTRY", "INDICATOR", "TYPE_OF_TRANSFORMATION", "FREQUENCY"),
    "ER": ("COUNTRY", "INDICATOR", "TYPE_OF_TRANSFORMATION", "FREQUENCY"),
    "IRFCL": ("COUNTRY", "INDICATOR", "SECTOR", "FREQUENCY"),
    "LS": ("COUNTRY", "INDICATOR", "TYPE_OF_TRANSFORMATION", "FREQUENCY"),
    "MFS_IR": ("COUNTRY", "INDICATOR", "FREQUENCY"),
    "IMTS": ("COUNTRY", "INDICATOR", "COUNTERPART_COUNTRY", "FREQUENCY"),
    "ITG": ("COUNTRY", "INDICATOR", "TYPE_OF_TRANSFORMATION", "FREQUENCY"),
}
# concept (the alias without its country) -> (source, dataflow, wildcard key with {c} country, {f} frequency)
FLOWS: Mapping[str, tuple[str, str, str]] = {
    "cpi": ("imf", "CPI", "{c}....{f}"),
    "core_cpi": ("imf", "CPI", "{c}....{f}"),
    "hicp": ("imf", "CPI", "{c}....{f}"),
    "ppi": ("imf", "PPI", "{c}...{f}"),
    "fx_usd": ("imf", "ER", "{c}...{f}"),
    "reserves": ("imf", "IRFCL", "{c}...{f}"),
    "unemployment": ("imf", "LS", "{c}...{f}"),
    "short_rate": ("imf", "MFS_IR", "{c}..{f}"),
    "exports_goods": ("imf", "ITG", "{c}...{f}"),
    "imports_goods": ("imf", "ITG", "{c}...{f}"),
    "trade_balance_goods": ("imf", "IMTS", "{c}.TBG_USD..{f}"),
    "consumer_confidence": ("oecd", "DSD_STES@DF_CLI", ".{f}.CCICP......"),
    "m1": ("oecd", "DSD_STES@DF_MONAGG", ".{f}.MANM......"),
    "broad_money": ("oecd", "DSD_STES@DF_MONAGG", ".{f}.MABM......"),
    "ind_prod": ("oecd", "DSD_STES@DF_INDSERV", ".{f}.PRVM.IX.BTE...."),  # industry B-E, any adjustment
}
AGENCY = {"imf": "IMF.STA", "oecd": "OECD.SDD.STES"}
# concept -> a part its candidate's key must have: the IMF's CPI flow holds both the national CPI
# and the HICP, and the closest of the two is not necessarily the one the concept means
REQUIRED: Mapping[str, str] = {"cpi": ".CPI.", "core_cpi": ".CPI.", "hicp": ".HICP."}
Series = dict[str, float]
Result = dict[str, Any]


def fits(concept: str, key: str) -> bool:
    """Whether a candidate key can stand for the concept at all."""
    return REQUIRED.get(concept, "") in f".{key}."


def concept_of(alias: str) -> str:
    return re.sub(r"^e_[a-z]{2}_", "", alias)


def normalize(period: str) -> str:
    """SDMX and DBnomics period spellings to one form: 2024-05, 2024-Q1, 2024."""
    if re.fullmatch(r"\d{4}-?M\d{2}", period):
        return period.replace("-M", "-").replace("M", "-")
    return period


# -- the rule ----------------------------------------------------------------------------------


def decimals(value: float) -> int:
    """The decimals a value was published with, read from its shortest spelling: 101.3 has one."""
    exponent = decimal.Decimal(repr(float(value))).as_tuple().exponent
    return min(max(0, -int(exponent)), MAX_DECIMALS)


def half_unit(values: Iterable[float]) -> float:
    """Half a unit of the last decimal a series publishes, the most decimals any of its values
    shows (a value that ends in zero drops it: 101.30 reads as 101.3)."""
    return 0.5 * 10.0 ** -max((decimals(value) for value in values), default=MAX_DECIMALS)


def compare(
    reference: Mapping[str, float], candidate: Mapping[str, float], concept: str = "", recent: int | None = None
) -> Result:
    """How a candidate relates to the reference: common periods, worst relative gap, whether every
    common value is equal within the published rounding of both series, whether it reaches a newer
    period, and the median ratio of the values (a unit change shows as a power of ten). Also the
    factor that brings the candidate closest to the reference (one, a power of ten, or, for an
    index of the series' concept, any ratio: it may be rebased), `how` it was chosen, and the
    median and 90th percentile of the relative gaps once scaled: over every common period, or with
    `recent`, over the common periods of the last `recent` years (`window` of them)."""
    theirs = {normalize(p): float(v) for p, v in reference.items()}
    ours = {normalize(p): float(v) for p, v in candidate.items()}
    common = sorted(set(theirs) & set(ours))
    out: Result = {"common": len(common), "last": max(ours) if ours else None}
    out["newer"] = bool(ours) and bool(theirs) and max(ours) > max(theirs)
    if common:
        out["gap"] = max(abs(theirs[p] - ours[p]) / max(abs(ours[p]), 1e-12) for p in common)
        allowed = half_unit(theirs[p] for p in common) + half_unit(ours[p] for p in common)
        out["within_rounding"] = all(abs(theirs[p] - ours[p]) <= allowed + 1e-12 * abs(ours[p]) for p in common)
        ratios = sorted(theirs[p] / ours[p] for p in common if ours[p])
        out["ratio"] = ratios[len(ratios) // 2] if ratios else None
        scale = power_of_ten(out["ratio"])
        if scale is not None:
            # the same series in other units: the gap once the candidate is scaled to the reference
            out["scale"] = scale
            out["scaled_gap"] = max(abs(theirs[p] - ours[p] * 10**scale) / max(abs(ours[p] * 10**scale), 1e-12) for p in common)
        window = common
        if recent is not None:
            newest = int(common[-1][:4])
            window = [p for p in common if int(p[:4]) > newest - recent]
            out.update(recent=recent, window=len(window))
        # the factors are read off the periods the rule measures: the window's median ratio
        recent_ratios = sorted(theirs[p] / ours[p] for p in window if ours[p])
        ratio = recent_ratios[len(recent_ratios) // 2] if recent_ratios else None
        factors = {"same units": 1.0}
        if power_of_ten(ratio) is not None:
            factors["other units"] = 10.0 ** power_of_ten(ratio)
        if concept in INDEXES and ratio:
            factors["rebased"] = ratio
        floor = 1.0 if concept in RATES else 1e-12
        spreads = {}
        for how, factor in factors.items():
            gaps = sorted(abs(theirs[p] - ours[p] * factor) / max(abs(ours[p] * factor), floor) for p in window)
            spreads[how] = (gaps[len(gaps) // 2], gaps[min(len(gaps) - 1, math.ceil(0.9 * len(gaps)) - 1)], factor)
        how = min(spreads, key=lambda name: spreads[name][:2])
        out.update(how=how, median_gap=spreads[how][0], p90_gap=spreads[how][1], factor=spreads[how][2])
    return out


def close(outcome: Mapping[str, Any]) -> bool:
    """The --close rule: enough common periods, newer, and the scaled gaps within CLOSE_MEDIAN
    (typical) and CLOSE_P90 (90th percentile)."""
    return (
        outcome.get("common", 0) >= MIN_COMMON
        and bool(outcome.get("newer"))
        and outcome.get("median_gap") is not None
        and outcome["median_gap"] <= CLOSE_MEDIAN
        and outcome["p90_gap"] <= CLOSE_P90
    )


def power_of_ten(ratio: float | None) -> int | None:
    """The exponent when `ratio` is a power of ten other than one (within 1 per cent), else None."""
    if not ratio or ratio <= 0:
        return None
    power = math.log10(ratio)
    if abs(power - round(power)) < 0.01 and round(power) != 0:
        return round(power)
    return None


def accepted(outcome: Mapping[str, Any], tolerance: float = TOLERANCE, *, units: bool = False, near: bool = False) -> bool:
    """The rule: enough common periods, equal (within the published rounding of both series, or
    within the relative tolerance), and newer than DBnomics.

    With `units`, a candidate that equals the reference once scaled by a power of ten (the same
    series published in other units) is accepted too. With `near`, so is a candidate that passes
    the close rule.
    """
    if near and close(outcome):
        return True
    gap = outcome.get("gap")
    equal = gap is not None and (gap <= tolerance or bool(outcome.get("within_rounding")))
    if units and outcome.get("scaled_gap") is not None:
        equal = equal or outcome["scaled_gap"] <= tolerance
    return outcome.get("common", 0) >= MIN_COMMON and equal and bool(outcome.get("newer"))


def reason(outcome: Mapping[str, Any] | None, tolerance: float = TOLERANCE) -> str:
    """Why a series stays on DBnomics, for the catalog note."""
    if outcome is None:
        return "no candidate"
    if outcome.get("common", 0) < MIN_COMMON:
        return f"only {outcome.get('common', 0)} periods in common"
    if outcome["gap"] > tolerance and not outcome.get("within_rounding"):
        if outcome.get("scaled_gap") is not None and outcome["scaled_gap"] <= tolerance:
            return f"same values in other units (x1e{outcome['scale']})"
        return f"values differ (max relative gap {outcome['gap']:.1e})"
    if not outcome.get("newer"):
        return f"publisher has nothing newer (last {outcome.get('last')})"
    return "accepted"


def best(
    reference: Mapping[str, float], candidates: Mapping[str, Series], concept: str = "", recent: int | None = None
) -> tuple[str, Result] | None:
    """The candidate closest to the reference: accepted ones first, then close ones, then by the
    median gap."""
    ranked = []
    for key, series in candidates.items():
        outcome = compare(reference, series, concept, recent)
        tier = 0 if accepted(outcome, TOLERANCE) else 1 if close(outcome) else 2
        spread = outcome.get("median_gap", 9e9) if outcome.get("common", 0) >= MIN_COMMON else 9e9
        ranked.append(((tier, spread), key, outcome))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[0])
    _order, key, outcome = ranked[0]
    return key, outcome


# -- the catalog -------------------------------------------------------------------------------


def dbnomics_entries(text: str) -> list[dict[str, Any]]:
    return [entry for entry in yaml.safe_load(text) if entry["source"] == "dbnomics"]


def rewrite(
    text: str,
    results: Mapping[str, Result],
    tolerance: float = TOLERANCE,
    stamp: str = STAMP,
    *,
    units: bool = False,
    near: bool = False,
) -> str:
    """The catalog text with accepted series moved to their publisher and the others annotated.

    Only `source`, `id` and the `stale` and `units_changed` attrs change; everything else is left
    byte for byte. With `units`, a series equal to DBnomics' in other units is moved too and
    `attrs.units_changed` records the factor.
    """
    blocks = re.split(r"(?m)^(?=- source: )", text)
    out = []
    for original in blocks:
        block = original
        found = re.match(r"- source: dbnomics\n  id: (.+)\n  alias: (\S+)\n", block)
        if found is None:
            out.append(block)
            continue
        alias = found.group(2)
        result = results.get(alias)
        block = re.sub(r"(?m)^    (stale|units_changed|close_match): .*\n", "", block)
        outcome = result.get("outcome") if result else None
        if result is not None and outcome and accepted(outcome, tolerance, units=units, near=near):
            block = block.replace(f"- source: dbnomics\n  id: {found.group(1)}\n", f"- source: {result['source']}\n  id: {result['id']}\n", 1)
            if not accepted(outcome, tolerance, units=units):  # moved only by the close rule: say how close
                over = f"{outcome['common']} periods"
                if outcome.get("window") is not None:
                    over = f"the last {outcome['recent']} years ({outcome['window']} of {outcome['common']} common periods)"
                note = (
                    f"{stamp}: {outcome['how']}; relative gap to the mirror: median {outcome['median_gap']:.1e}, "
                    f"90th percentile {outcome['p90_gap']:.1e} over {over}"
                )
                block = block.rstrip("\n") + f"\n    close_match: {json.dumps(note)}\n"
            elif not accepted(outcome, tolerance):  # moved only thanks to the unit change: say so
                note = f"{stamp}: the publisher's values are 1e{-outcome['scale']} times the mirror's (other units)"
                block = block.rstrip("\n") + f"\n    units_changed: {json.dumps(note)}\n"
        else:
            note = f"{stamp}: {reason(result.get('outcome') if result else None, tolerance)}"
            if "  attrs:\n" in block:
                block = block.rstrip("\n") + f"\n    stale: {json.dumps(note)}\n"
            else:
                block = block.rstrip("\n") + f"\n  attrs:\n    stale: {json.dumps(note)}\n"
        out.append(block)
    return "".join(out)


# -- the network -------------------------------------------------------------------------------


def fetch_reference(entries: Sequence[Mapping[str, Any]], client: httpx.Client) -> dict[str, Result]:
    """{alias: {id, periods}} from DBnomics, several series a call."""
    by_id = {entry["id"]: entry["alias"] for entry in entries}
    ids = list(by_id)
    out: dict[str, Result] = {}
    for start in range(0, len(ids), DBNOMICS_BATCH):
        chunk = ids[start : start + DBNOMICS_BATCH]
        response = client.get(DBNOMICS_URL, params={"series_ids": ",".join(chunk), "observations": "1", "limit": str(DBNOMICS_BATCH)})
        response.raise_for_status()
        for series in response.json()["series"]["docs"]:
            identifier = f"{series['provider_code']}/{series['dataset_code']}/{series['series_code']}"
            periods = {p: v for p, v in zip(series["period"], series["value"], strict=True) if v not in (None, "NA")}
            out[by_id[identifier]] = {"id": identifier, "periods": periods}
    return out


def parse_imf(content: bytes, dimensions: Sequence[str]) -> dict[str, Series]:
    series: dict[str, Series] = {}
    for node in ET.fromstring(content).iter():
        if node.tag.endswith("}Series") or node.tag == "Series":
            key = ".".join(node.get(dimension, "") for dimension in dimensions)
            found = series.setdefault(key, {})
            for child in node:
                if child.tag.endswith("}Obs") or child.tag == "Obs":
                    period, value = child.get("TIME_PERIOD"), child.get("OBS_VALUE")
                    if period is not None and value not in (None, "", "NaN"):
                        found[period] = float(value)
    return series


def parse_generic(content: bytes) -> dict[str, Series]:
    series: dict[str, Series] = {}
    for node in ET.fromstring(content).iter():
        if not node.tag.endswith("}Series"):
            continue
        key_values: list[str] = []
        observations: Series = {}
        for child in node:
            if child.tag.endswith("}SeriesKey"):
                key_values = [str(value.get("value")) for value in child]
            if child.tag.endswith("}Obs"):
                period = value = None
                for part in child:
                    if part.tag.endswith("}ObsDimension"):
                        period = part.get("value")
                    if part.tag.endswith("}ObsValue"):
                        value = part.get("value")
                if period is not None and value not in (None, "", "NaN"):
                    observations[period] = float(value)
        series.setdefault(".".join(key_values), {}).update(observations)  # the OECD splits a series by attribute
    return series


class Publishers:
    """Wildcard calls to the IMF and the OECD, the OECD paced at one a minute. Each answer is kept
    in memory and, with a `folder`, on disk (one JSON file a call), so `find` can run again with
    another rule without asking the publishers again. An answer that is not 200 is not kept on disk."""

    def __init__(self, client: httpx.Client, folder: pathlib.Path | None = None) -> None:
        self._client = client
        self._folder = folder
        self._cache: dict[tuple[str, str, str, str], tuple[int | str, dict[str, Series]]] = {}
        self._last_oecd = 0.0

    def _path(self, cache_key: tuple[str, str, str, str]) -> pathlib.Path | None:
        if self._folder is None:
            return None
        return self._folder / (re.sub(r"[^A-Za-z0-9._-]", "_", "_".join(cache_key)) + ".json")

    def series(self, source: str, flow: str, key: str, start: str) -> tuple[int | str, dict[str, Series]]:
        cache_key = (source, flow, key, start)
        path = self._path(cache_key)
        if cache_key not in self._cache and path is not None and path.exists():
            self._cache[cache_key] = (200, json.loads(path.read_text(encoding="utf-8")))
        if cache_key not in self._cache:
            self._cache[cache_key] = self._fetch(source, flow, key, start)
            code, found = self._cache[cache_key]
            print(f"  {source} {flow} {key} from {start}: {code}, {len(found)} series")
            if path is not None and code == 200:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(found), encoding="utf-8")
        return self._cache[cache_key]

    def _fetch(self, source: str, flow: str, key: str, start: str) -> tuple[int | str, dict[str, Series]]:
        try:
            if source == "imf":
                response = self._client.get(IMF_URL.format(flow=flow, key=key, start=start))
                time.sleep(1)
                if response.status_code != 200:
                    return response.status_code, {}
                return 200, parse_imf(response.content, DIMENSIONS[flow])
            wait = OECD_INTERVAL - (time.monotonic() - self._last_oecd)
            if wait > 0:
                time.sleep(wait)
            self._last_oecd = time.monotonic()
            response = self._client.get(OECD_URL.format(flow=flow, key=key, start=start), headers={"Accept": GENERIC})
            if response.status_code != 200:
                return response.status_code, {}
            return 200, parse_generic(response.content)
        except (httpx.HTTPError, ET.ParseError) as error:
            return f"error {type(error).__name__}", {}


def find(
    entries: Sequence[Mapping[str, Any]],
    reference: Mapping[str, Result],
    publishers: Publishers,
    regions: Iterable[str] = (),
    done: Mapping[str, Result] | None = None,
    recent: int | None = None,
) -> Iterable[tuple[str, Result]]:
    """Yield (alias, result) for every series: the closest candidate at the publisher, or why none."""
    wanted = set(regions)
    for entry in entries:
        alias = entry["alias"]
        region = entry["attrs"]["region"]
        if (wanted and region not in wanted) or (done and alias in done):
            continue
        concept = concept_of(alias)
        periods = reference.get(alias, {}).get("periods", {})
        result: Result = {"concept": concept, "region": region, "dbnomics": entry["id"]}
        if not periods:
            result["status"] = "no reference"
        elif concept not in FLOWS:
            result["status"] = "no template"
        else:
            source, flow, pattern = FLOWS[concept]
            country = ISO3[region]
            key = pattern.format(c=country, f=entry["frequency"])
            start = "1990" if source == "oecd" else min(periods)[:4]
            code, series = publishers.series(source, flow, key, start)
            if source == "oecd":
                series = {k: v for k, v in series.items() if k.split(".")[0] == country}
            series = {k: v for k, v in series.items() if fits(concept, k)}
            chosen = best(periods, series, concept, recent) if series else None
            if chosen is None:
                result["status"] = f"no data ({code})"
            else:
                candidate, outcome = chosen
                result.update(
                    status="candidate",
                    source=source,
                    id=f"{AGENCY[source]},{flow}/{candidate}",
                    outcome=outcome,
                )
        yield alias, result


def report(results: Mapping[str, Result], tolerance: float = TOLERANCE, *, units: bool = False, near: bool = False) -> str:
    """A table per concept: replaced, and the reasons of the rest."""
    table: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    for alias, result in results.items():
        outcome = result.get("outcome")
        if outcome and accepted(outcome, tolerance, units=units, near=near):
            if accepted(outcome, tolerance):
                label = "replaced"
            elif accepted(outcome, tolerance, units=units):
                label = "replaced, other units"
            else:
                label = "replaced, close"
        elif result["status"] in ("no reference", "no template") or result["status"].startswith("no data"):
            label = result["status"].split(" (")[0]
        else:
            label = reason(outcome, tolerance).split(" (")[0]
            if label.startswith("only "):
                label = "too few periods in common"
        table[concept_of(alias)][label] += 1
    labels = sorted({label for counter in table.values() for label in counter}, key=lambda s: (s != "replaced", s))
    lines = [f"{'concept':<22}" + "".join(f"{label:>28}" for label in labels) + f"{'total':>7}"]
    totals: collections.Counter[str] = collections.Counter()
    for concept in sorted(table):
        counter = table[concept]
        totals.update(counter)
        lines.append(f"{concept:<22}" + "".join(f"{counter.get(label, 0):>28}" for label in labels) + f"{sum(counter.values()):>7}")
    lines.append(f"{'total':<22}" + "".join(f"{totals.get(label, 0):>28}" for label in labels) + f"{sum(totals.values()):>7}")
    return "\n".join(lines)


# -- commands ----------------------------------------------------------------------------------


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=("reference", "find", "report", "write"))
    parser.add_argument("--cache", type=pathlib.Path, required=True, help="folder for reference.json and results.json")
    parser.add_argument("--catalog", type=pathlib.Path, default=CATALOG)
    parser.add_argument("--tolerance", type=float, default=TOLERANCE)
    parser.add_argument("--regions", nargs="*", default=())
    parser.add_argument("--recent", type=int, default=None, help="find: measure the close rule on the last N years only")
    parser.add_argument("--accept-units", action="store_true", help="also move a series equal in other units (x10^n)")
    parser.add_argument("--close", action="store_true", help="also move a series close to the mirror's (CLOSE_MEDIAN, CLOSE_P90)")
    args = parser.parse_args(argv)
    args.cache.mkdir(parents=True, exist_ok=True)
    reference_path = args.cache / "reference.json"
    results_path = args.cache / "results.json"
    text = args.catalog.read_text(encoding="utf-8")
    entries = dbnomics_entries(text)
    if args.command == "reference":
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            reference = fetch_reference(entries, client)
        reference_path.write_text(json.dumps(reference, indent=0), encoding="utf-8")
        print(f"[ok] {len(reference)} series cached")
        return 0
    reference = json.loads(reference_path.read_text(encoding="utf-8"))
    if args.command == "find":
        results = json.loads(results_path.read_text(encoding="utf-8")) if results_path.exists() else {}
        with httpx.Client(timeout=300, follow_redirects=True) as client:
            for alias, result in find(entries, reference, Publishers(client, args.cache / "calls"), args.regions, results, args.recent):
                results[alias] = result
                results_path.write_text(json.dumps(results, indent=1), encoding="utf-8")
                print(f"{alias:<30} {result['status']} {result.get('id', '')}")
        return 0
    results = json.loads(results_path.read_text(encoding="utf-8"))
    if args.command == "report":
        print(report(results, args.tolerance, units=args.accept_units, near=args.close))
        return 0
    rewritten = rewrite(text, results, args.tolerance, units=args.accept_units, near=args.close)
    args.catalog.write_text(rewritten, encoding="utf-8", newline="\n")
    replaced = sum(
        1
        for r in results.values()
        if r.get("outcome") and accepted(r["outcome"], args.tolerance, units=args.accept_units, near=args.close)
    )
    print(f"[ok] {replaced} series moved to their publisher, {len(results) - replaced} annotated as stale")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
