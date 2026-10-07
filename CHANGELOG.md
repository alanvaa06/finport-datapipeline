# Changelog

## 0.1.0 (unreleased)

First version.

- A local store of public economic series that keeps every revision, readable as of any date.
- FRED series are downloaded with every ALFRED vintage, each dated with the day it was published,
  so they read as of any date back to ALFRED's first vintage. Other series read as of a date from
  the store's first sync on.
- When the catalog moves an alias to another series (from a mirror to its publisher), `sync` takes
  it away from the old one, so reading by alias returns the new series.
- Sources: FRED, BLS, Banxico SIE, INEGI, the World Bank, the BIS, the ECB, Eurostat, the OECD,
  the IMF and DBnomics.
- Tables: UN Comtrade goods trade and SEC XBRL company facts, each version dated with its filing day.
- Documents: SEC filings (10-K, 10-Q, 8-K, 20-F, 40-F) with their exhibits.
- A bundled `macro` catalog of 1,293 series across 44 economies.
- `data-pipeline setup` and `data-pipeline keys`: one `.env` per project, each key checked before it is saved.
