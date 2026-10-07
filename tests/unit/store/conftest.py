import pytest

from data_pipeline import credentials as keys


@pytest.fixture(autouse=True)
def no_real_credentials(monkeypatch):
    """Tests never see the developer's own keys."""
    for name in keys.NAMES:
        monkeypatch.delenv(name, raising=False)
