"""Rules that keep the store honest: no recorded keys, and the credentials module stays standalone."""

import pathlib
import re

from .helpers import FIXTURES

ROOT = pathlib.Path(__file__).resolve().parents[3]
KEY_SHAPED = re.compile(r"\b[0-9a-f]{32}\b|\b[0-9a-f]{64}\b")
IMPORT = re.compile(r"^\s*(?:from|import)\s+([\w.]+)", re.MULTILINE)


def imports_of(folder):
    found = set()
    for path in sorted(folder.rglob("*.py")):
        found.update(IMPORT.findall(path.read_text(encoding="utf-8")))
    return found


def test_fixtures_hold_nothing_shaped_like_a_key():
    offenders = [
        path.name for path in sorted(FIXTURES.iterdir()) if KEY_SHAPED.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_the_credentials_module_imports_nothing_from_the_library():
    source = (ROOT / "src" / "data_pipeline" / "credentials.py").read_text(encoding="utf-8")
    assert [name for name in IMPORT.findall(source) if name.startswith("data_pipeline")] == []
