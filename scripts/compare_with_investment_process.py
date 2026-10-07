"""Acceptance check: one source of the store against Investment_Process's own store.

    python scripts/compare_with_investment_process.py fred C:/Proyectos/Investment_Process D:/datos/store

Works for the sources both stores keep one series per id: fred, banxico, inegi. Sync both stores
on the same day first. Exit code 0 when every series matches, 1 otherwise.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline
from data_pipeline.store.errors import UnknownSeriesError

TOLERANCE = 1e-9


def compare(source: str, origin: pathlib.Path, root: pathlib.Path) -> int:
    public = origin / "inputs" / "publicos"
    series = pd.read_parquet(public / "series.parquet")
    theirs_all = pd.read_parquet(public / "obs" / f"{source}.parquet")
    store = data_pipeline.Store(root)
    failures = 0
    for row in series[series["fuente"] == source].sort_values("id_fuente").to_dict(orient="records"):
        rows = theirs_all[theirs_all["clave"] == row["clave"]]
        rows = rows[rows["valor"].notna() & ~rows["proyeccion"].astype(bool)]
        theirs = rows.set_index("periodo")["valor"]
        try:
            ours = store.series(f"{source}:{row['id_fuente']}").set_index("period")["value"]
        except UnknownSeriesError:
            failures += 1
            print(f"[x] {row['id_fuente']:<14} not in the store")
            continue
        common = theirs.index.intersection(ours.index)
        gap = (theirs[common] - ours[common]).abs()
        limit = TOLERANCE * pd.concat([theirs[common].abs(), ours[common].abs()], axis=1).max(axis=1)
        different = int((gap > limit).sum())
        only_theirs = len(theirs.index.difference(ours.index))
        only_ours = len(ours.index.difference(theirs.index))
        ok = different == 0 and only_theirs == 0 and only_ours == 0
        failures += 0 if ok else 1
        print(
            f"[{'ok' if ok else 'x'}] {row['id_fuente']:<14} {len(common)} periods in common, "
            f"{different} different, {only_theirs} only theirs, {only_ours} only ours"
        )
    print(f"{failures} series differ" if failures else "every series matches")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(compare(sys.argv[1], pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])))
