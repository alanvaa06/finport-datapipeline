"""Rebuild the map from ISO3 to Comtrade's reporter code that the store ships.

    python scripts/build_comtrade_reporters.py

Reads Comtrade's public reference list (no key) and writes
`src/data_pipeline/store/sources/comtrade_reporters.json`. Groups and reporters that no longer
exist are left out. Run it when Comtrade adds a reporter.
"""

import json
import pathlib
import sys

import httpx

URL = "https://comtradeapi.un.org/files/v1/app/reference/Reporters.json"
TARGET = pathlib.Path(__file__).parent.parent / "src" / "data_pipeline" / "store" / "sources" / "comtrade_reporters.json"
ISO3_LENGTH = 3


def current_reporters(rows: list[dict[str, object]]) -> dict[str, int]:
    """ISO3 -> reporter code, for the countries that report today."""
    codes: dict[str, int] = {}
    for row in rows:
        iso = str(row.get("reporterCodeIsoAlpha3") or "")
        if row.get("isGroup") or row.get("entryExpiredDate") or len(iso) != ISO3_LENGTH or not iso.isalpha():
            continue
        if iso in codes:
            msg = f"{iso} appears twice among the current reporters"
            raise ValueError(msg)
        codes[iso] = int(str(row["reporterCode"]))
    return dict(sorted(codes.items()))


def main() -> int:
    response = httpx.get(URL, timeout=60, follow_redirects=True)
    response.raise_for_status()
    codes = current_reporters(json.loads(response.content.decode("utf-8-sig"))["results"])
    TARGET.write_text(json.dumps(codes, indent=0) + "\n", encoding="utf-8")
    print(f"[ok] {len(codes)} reporters written to {TARGET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
