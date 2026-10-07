"""Compare the series two stores have in common, key by key.

    python scripts/compare_stores.py D:/datos/new D:/datos/old bis ecb eurostat oecd imf

For each source named (every source of the first store when none is), prints how many series
both stores hold, how many agree on every period they share, and the series that do not.
Projections are included. Exit code 0 when every shared series agrees, 1 otherwise.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline
from data_pipeline.store.errors import UnknownSeriesError

TOLERANCE = 1e-9


def compare(new_root: pathlib.Path, old_root: pathlib.Path, sources: list[str]) -> int:
    new, old = data_pipeline.Store(new_root), data_pipeline.Store(old_root)
    index = new.index()
    disagreeing = 0
    for source in sources or sorted(index["source"].unique()):
        shared = agree = only_new = 0
        for key in sorted(index[index["source"] == source]["key"]):
            ours = new.series(key, projections=True).set_index("period")["value"]
            try:
                theirs = old.series(key, projections=True).set_index("period")["value"]
            except UnknownSeriesError:
                only_new += 1
                continue
            shared += 1
            common = ours.index.intersection(theirs.index)
            gap = (ours[common] - theirs[common]).abs()
            limit = TOLERANCE * pd.concat([ours[common].abs(), theirs[common].abs()], axis=1).max(axis=1)
            different = int((gap > limit).sum())
            missing = len(theirs.index.difference(ours.index))
            if different or missing:
                disagreeing += 1
                print(f"  [x] {key}: {different} of {len(common)} periods differ, {missing} periods only in the old store")
            else:
                agree += 1
        print(f"{source:<10} in both {shared}, agree {agree}, only in the new store {only_new}")
    return 1 if disagreeing else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3:]))
