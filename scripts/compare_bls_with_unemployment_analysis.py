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
