"""Acceptance check: Comtrade tables of the store against Investment_Process's own store.

    python scripts/compare_comtrade_with_investment_process.py C:/Proyectos/Investment_Process D:/datos/store MEX USA

Compares value and weight on the keys both stores hold (flow, product, frequency, period; partner
world). Keys only one side holds are counted, not failed: the two stores are filled on different
days. Exit code 0 when every common key matches, 1 otherwise.
"""

import pathlib
import sys

import numpy as np
import pandas as pd

import data_pipeline

TOLERANCE = 1e-9
KEY = ["flow", "product", "frequency", "period"]
VALUES = ["value_usd", "weight_kg"]
THEIR_NAMES = {
    "flujo": "flow",
    "hs": "product",
    "frecuencia": "frequency",
    "periodo": "period",
    "valor_usd": "value_usd",
    "peso_kg": "weight_kg",
}
SHOWN = 5


def their_table(origin: pathlib.Path, reporter: str) -> pd.DataFrame:
    path = origin / "inputs" / "publicos" / "comtrade" / f"{reporter.lower()}.parquet"
    frame = pd.read_parquet(path)
    frame = frame[frame["socio"].astype(str) == "0"].sort_values("consultado_en", kind="stable")
    frame = frame.rename(columns=THEIR_NAMES).drop_duplicates(KEY, keep="last")
    return frame[[*KEY, *VALUES]].astype(dict.fromkeys(KEY, str))


def same(left: pd.Series, right: pd.Series) -> np.ndarray:
    a = left.to_numpy(dtype=float)
    b = right.to_numpy(dtype=float)
    return (np.isnan(a) & np.isnan(b)) | np.isclose(a, b, rtol=TOLERANCE, atol=0.0)


def compare(origin: pathlib.Path, root: pathlib.Path, reporters: list[str]) -> int:
    store = data_pipeline.Store(root)
    failures = 0
    for reporter in reporters:
        theirs = their_table(origin, reporter)
        ours = store.table("comtrade", reporter, partner="WLD")[[*KEY, *VALUES]]
        both = theirs.merge(ours, on=KEY, how="outer", suffixes=("_theirs", "_ours"), indicator=True)
        common = both[both["_merge"] == "both"]
        equal = np.ones(len(common), dtype=bool)
        for column in VALUES:
            equal &= same(common[f"{column}_theirs"], common[f"{column}_ours"])
        different = common[~equal]
        only_theirs = int((both["_merge"] == "left_only").sum())
        only_ours = int((both["_merge"] == "right_only").sum())
        failures += 0 if different.empty else 1
        print(
            f"[{'ok' if different.empty else 'x'}] {reporter}  {len(common)} keys in common, "
            f"{len(different)} different, {only_theirs} only theirs, {only_ours} only ours"
        )
        for row in different.head(SHOWN).to_dict(orient="records"):
            print(
                f"      {row['flow']} {row['product']} {row['frequency']} {row['period']}: "
                f"theirs {row['value_usd_theirs']} / {row['weight_kg_theirs']}, "
                f"ours {row['value_usd_ours']} / {row['weight_kg_ours']}"
            )
    print(f"{failures} reporters differ" if failures else "every common key matches")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(compare(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3:]))
