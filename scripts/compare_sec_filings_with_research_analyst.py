"""Acceptance check: SEC filings of the store against research_analyst's own download.

    python C:/Proyectos/research_analyst/tools/sec_fetch.py AAPL --dest D:/tmp/filings --since 2023-10-05 --amendments --ua "..."
    python scripts/compare_sec_filings_with_research_analyst.py D:/tmp/filings/AAPL_filings_manifest.csv D:/datos/store AAPL

Both downloads are matched by source URL. Every file of the tool's manifest must be in the
store with identical bytes; files only one side holds are listed. Exit code 0 when every common
file is identical and nothing is missing from the store, 1 otherwise.
"""

import hashlib
import pathlib
import sys

import pandas as pd

import data_pipeline

SHOWN = 10


def compare(manifest: pathlib.Path, root: pathlib.Path, ticker: str) -> int:
    theirs = pd.read_csv(manifest, dtype=str, keep_default_na=False)
    ours = data_pipeline.Store(root).documents("sec_filings", ticker)
    both = theirs.merge(ours, left_on="source", right_on="url", how="outer", suffixes=("_theirs", "_ours"), indicator=True)
    missing = both[both["_merge"] == "left_only"]
    extra = both[both["_merge"] == "right_only"]
    common = both[both["_merge"] == "both"]
    different = []
    for row in common.to_dict(orient="records"):
        their_bytes = (manifest.parent / row["file_theirs"]).read_bytes()
        if hashlib.sha256(their_bytes).hexdigest() != row["sha256"]:
            different.append(row)
    print(
        f"{len(common)} files in common, {len(different)} with different bytes, "
        f"{len(missing)} only in the tool's download, {len(extra)} only in the store"
    )
    for row in different[:SHOWN]:
        print(f"  [x] {row['source']}: bytes differ")
    for row in missing.head(SHOWN).to_dict(orient="records"):
        print(f"  [x] {row['source']}: not in the store")
    for row in extra.head(SHOWN).to_dict(orient="records"):
        print(f"  [i] {row['url']}: only in the store ({row['form_ours']})")
    failed = len(different) + len(missing)
    print("[x] the downloads differ" if failed else "[ok] every file of the tool's download is in the store, identical")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]))
