import pathlib
from collections.abc import Callable

import pytest

from data_pipeline import credentials


def confine_env_search(
    real: Callable[[pathlib.Path | None], pathlib.Path | None], bound: pathlib.Path
) -> Callable[[pathlib.Path | None], pathlib.Path | None]:
    """`real`, except that a `.env` outside `bound` is never found (a developer's own file is not a fixture)."""
    resolved_bound = bound.resolve()

    def find(start: pathlib.Path | None = None) -> pathlib.Path | None:
        found = real(start)
        if found is not None and resolved_bound not in found.resolve().parents:
            return None
        return found

    return find


@pytest.fixture(autouse=True)
def isolated_credentials(monkeypatch, tmp_path_factory):
    """No test sees a developer's real keys: not from the environment, not from a `.env` above the temp folder."""
    for name in credentials.NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        credentials,
        "find_env_file",
        confine_env_search(credentials.find_env_file, tmp_path_factory.getbasetemp()),
    )
