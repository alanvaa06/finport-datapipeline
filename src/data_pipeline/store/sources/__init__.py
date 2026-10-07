"""Registry of sources: name -> class, and the title each one is cited with."""

from collections.abc import Callable, Mapping

from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError
from data_pipeline.store.http import Client
from data_pipeline.store.sources.banxico import Banxico
from data_pipeline.store.sources.base import Source
from data_pipeline.store.sources.bls import Bls
from data_pipeline.store.sources.comtrade import Comtrade
from data_pipeline.store.sources.dbnomics import Dbnomics
from data_pipeline.store.sources.fred import Fred
from data_pipeline.store.sources.inegi import Inegi
from data_pipeline.store.sources.sdmx import PROVIDERS, Sdmx
from data_pipeline.store.sources.sec_filings import SecFilings
from data_pipeline.store.sources.sec_xbrl import SecXbrl
from data_pipeline.store.sources.worldbank import WorldBank

Factory = Callable[[Client, Credentials], Source]


def _sdmx(name: str) -> Factory:
    """The factory of one SDMX provider. None of them takes a key."""

    def build(client: Client, credentials: Credentials) -> Source:
        del credentials
        return Sdmx(name, client)

    return build


REGISTRY: Mapping[str, Factory] = {
    "banxico": Banxico,
    "bls": Bls,
    "comtrade": Comtrade,
    "dbnomics": Dbnomics,
    "fred": Fred,
    "inegi": Inegi,
    "sec_filings": SecFilings,
    "sec_xbrl": SecXbrl,
    "worldbank": WorldBank,
    **{name: _sdmx(name) for name in PROVIDERS},
}
TITLES: Mapping[str, str] = {
    "banxico": "Banxico SIE",
    "bis": "BIS",
    "bls": "BLS",
    "comtrade": "UN Comtrade",
    "dbnomics": "DBnomics",
    "ecb": "ECB",
    "eurostat": "Eurostat",
    "fred": "FRED",
    "imf": "IMF",
    "inegi": "INEGI",
    "oecd": "OECD",
    "sec_filings": "SEC EDGAR",
    "sec_xbrl": "SEC EDGAR",
    "worldbank": "World Bank",
}


def create(name: str, client: Client, credentials: Credentials) -> Source:
    """Build the source called `name`."""
    factory = REGISTRY.get(name)
    if factory is None:
        known = ", ".join(sorted(REGISTRY))
        msg = f"unknown source {name!r} (known sources: {known})"
        raise CatalogError(msg)
    return factory(client, credentials)
