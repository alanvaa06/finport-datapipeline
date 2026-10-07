"""Acceptance check: SEC facts of the store against research_analyst's own download.

    python C:/Proyectos/research_analyst/tools/xbrl_fetch.py AAPL --dest D:/tmp/xbrl
    python scripts/compare_sec_xbrl_with_research_analyst.py D:/tmp/xbrl/xbrl_facts_AAPL.csv D:/datos/store AAPL

Compares the rows that tool took as reported (tag "observado"): each is a fact (concept, unit,
start, end) whose value must equal the store's current value. Rows the tool derived (quarters
taken out of year-to-date figures, the fourth quarter) are its own arithmetic and are not
compared. Exit code 0 when every compared fact matches, 1 otherwise.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline

TOLERANCE = 1e-9
KEY = ["concept", "unit", "start", "end"]
OBSERVED = "observado"
SHOWN = 10


def compare(csv: pathlib.Path, root: pathlib.Path, ticker: str) -> int:
    theirs = pd.read_csv(csv, dtype=str, keep_default_na=False)
    theirs = theirs[theirs["tag"] == OBSERVED].drop_duplicates(KEY, keep="last")
    theirs = theirs.assign(value=theirs["value"].astype(float))
    ours = data_pipeline.Store(root).table("sec_xbrl", ticker, taxonomy="us-gaap")[[*KEY, "value", "filed"]]
    both = theirs.merge(ours, on=KEY, how="left", suffixes=("_theirs", "_ours"), indicator=True)
    missing = both[both["_merge"] == "left_only"]
    common = both[both["_merge"] == "both"]
    gap = (common["value_theirs"] - common["value_ours"]).abs()
    limit = TOLERANCE * common[["value_theirs", "value_ours"]].abs().max(axis=1)
    different = common[gap > limit]
    annual = int((common["period"].str.startswith("FY")).sum())
    print(f"{len(common)} facts in common ({annual} annual), {len(different)} different, {len(missing)} not in the store")
    for row in different.head(SHOWN).to_dict(orient="records"):
        print(
            f"  [x] {row['concept']} {row['start']}..{row['end']}: theirs {row['value_theirs']} "
            f"(filed {row['filed_theirs']}), ours {row['value_ours']} (filed {row['filed_ours']})"
        )
    for row in missing.head(SHOWN).to_dict(orient="records"):
        print(f"  [x] {row['concept']} {row['start']}..{row['end']}: not in the store")
    failed = len(different) + len(missing)
    print("[x] the stores differ" if failed else "[ok] every compared fact matches")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]))
