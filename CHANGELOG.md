# Changelog

## 0.1.0 (unreleased)

First version.

- A local store of public economic series that keeps every revision, readable as of any date.
- FRED series are downloaded with every ALFRED vintage, each dated with the day it was published,
  so they read as of any date back to ALFRED's first vintage. Other series read as of a date from
  the store's first sync on.
- When the catalog moves an alias to another series (from a mirror to its publisher), `sync` takes
  it away from the old one, so reading by alias returns the new series.
- A store opened with a catalog reads an alias as the series that catalog gives it, and asks for a
  `sync` when that series is not stored yet; without a catalog, an alias left on several stored
  series is refused instead of returning the first one.
- SEC XBRL versions count as known from the end of the day the filing was received, as FRED
  vintages do, so an `as_of` with a time on that day does not see them early. A store synced
  before this change stores each version once more, with the same value, on its next sync.
- Sources: FRED, BLS, Banxico SIE, INEGI, the World Bank, the BIS, the ECB, Eurostat, the OECD,
  the IMF and DBnomics.
- Tables: UN Comtrade goods trade and SEC XBRL company facts, each version dated with its filing day.
- Documents: SEC filings (10-K, 10-Q, 8-K, 20-F, 40-F) with their exhibits.
- A bundled `macro` catalog of 1,293 series across 44 economies.
  252 series that DBnomics only mirrored from retired datasets (IMF IFS and DOT, OECD MEI) now
  come from the dataset that replaced theirs at the publisher, each with `attrs.close_match`
  saying how close it is to the mirror; 148 stay on DBnomics, marked `stale`.
- `data-pipeline catalog`: find series of a catalog by alias, name or key, by source or region.
- A sync stores a source's series at checkpoints (every minute, at the end, and when interrupted)
  instead of after every batch: 400 series sync about 50 times faster on the local side.
- `data-pipeline setup` and `data-pipeline keys`: one `.env` per project, each key checked before it is saved.
