"""How fresh the macro catalog is in a store, by the origin of each series.

    python scripts/macro_freshness.py D:/datos/macro

Prints, per origin (the family the series comes from, whatever source reads it), how many series
the store holds, how many are stale and the typical age in days of the last observation.
"""

import pathlib
import sys

import pandas as pd

import data_pipeline
from data_pipeline.store.catalog import load_catalog


def origin(entry):
    """The publisher of a catalog entry: the DBnomics provider prefix, or the source itself."""
    return entry.source_id.split("/")[0] if entry.source == "dbnomics" else entry.source


def report(root: pathlib.Path) -> int:
    origins = {entry.alias: origin(entry) for entry in load_catalog(pathlib.Path("macro"))}
    store = data_pipeline.Store(root, "macro")
    status = store.status()
    last = store.index().set_index("key")["last_date"]
    today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    status["origin"] = status["alias"].map(origins)
    status["age"] = (today - status["key"].map(last)).dt.days
    table = status.groupby("origin").agg(
        series=("key", "size"),
        ok=("state", lambda states: int((states == "ok").sum())),
        stale=("state", lambda states: int((states == "stale").sum())),
        missing=("state", lambda states: int((states == "missing").sum())),
        failed=("state", lambda states: int((states == "failed").sum())),
        median_age_days=("age", "median"),
    )
    print(table.to_string())
    totals = status["state"].value_counts()
    print("total: " + ", ".join(f"{count} {state}" for state, count in totals.items()))
    return 0


if __name__ == "__main__":
    sys.exit(report(pathlib.Path(sys.argv[1])))
