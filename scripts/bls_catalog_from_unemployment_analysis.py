"""Write a store catalog with every BLS series Unemployment_Analysis keeps.

    python scripts/bls_catalog_from_unemployment_analysis.py C:/Proyectos/Unemployment_Analysis D:/datos/bls.yaml

Series are grouped by the year their history starts, so the source asks for exactly the years
that exist instead of walking back to find them. The start year is the earlier of the one that
repository declares and the first year its cache actually holds: fourteen series are declared
as starting in 1990 but have data back to 1939.
"""

import pathlib
import sys

import pandas as pd
import yaml


def write(repository: pathlib.Path, target: pathlib.Path) -> int:
    cache = repository / "data" / "cache"
    meta = pd.read_parquet(cache / "series_meta.parquet").set_index("series_id")
    seen = pd.read_parquet(cache / "observations.parquet").groupby("series_id")["date"].min().dt.year
    starts = pd.concat([meta["history_start_year"], seen], axis=1).min(axis=1).astype(int)
    entries = [
        {"source": "bls", "ids": sorted(group.index), "start": f"{year}-01-01"}
        for year, group in starts.groupby(starts)
    ]
    text = yaml.safe_dump(entries, sort_keys=False, width=100).replace("start: '", "start: ").replace("-01-01'", "-01-01")
    target.write_text(text, encoding="utf-8", newline="\n")
    print(f"[ok] wrote {target}: {len(starts)} series in {len(entries)} groups")
    return 0


if __name__ == "__main__":
    sys.exit(write(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])))
