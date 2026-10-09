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
  279 series that DBnomics only mirrored from retired datasets (IMF IFS and DOT, OECD MEI) now
  come from the dataset that replaced theirs at the publisher, each with `attrs.close_match`
  saying how close it is to the mirror; 25 more left DBnomics for another live series, and 96
  stay on DBnomics, marked `stale`.
- `data-pipeline catalog`: find series of a catalog by alias, name or key, by source or region.
- A sync stores a source's series at checkpoints (every minute, at the end, and when interrupted)
  instead of after every batch: 400 series sync about 50 times faster on the local side.
- `data-pipeline setup` and `data-pipeline keys`: one `.env` per project, each key checked before it is saved.

### Fixed

Store core:

- A sync that died without cleaning up (a killed process, a crash) no longer blocks the next one:
  `sync.lock` is held with the operating system's lock, released when the process ends, and a file
  left behind is taken over. A live sync is still refused, and the message names its pid, host and
  start time.
- A source that breaks (an unexpected error, a damaged file) fails its own unfinished series and the
  run goes on with the next source. Each source's calls for the day are written to `runs.json` even
  when the run is interrupted, so the daily budget is not spent twice.
- Every file is flushed to disk before it replaces the old one. A file that cannot be read is an
  error that names it (exit 2 for the index or `runs.json`), not a traceback.
- Each downloaded batch is stamped with the time it arrived, not the start of the run, so a run that
  crosses midnight UTC does not make `as_of` see values early.
- Error excerpts are scrubbed of keys before they are cut to length, so no prefix of a key reaches
  the index or `status`; every failure reason is scrubbed before it is stored.
- `Store.table` applies filters on other columns after choosing each key's current (or `as_of`)
  version, so a filter such as `form="10-K"` no longer revives a value that a 10-K/A replaced.
  Filters on key columns, which cannot mix versions, run first: the same rows, less work.
- A series that the source starts sending with another frequency fails with a reason that names
  both and says how to store it again: `sync --full --key <key>` (or
  `Store.sync(keys=[...], full=True)`). That full sync stores it at the new frequency and gives
  each old period a missing value, so `series` and `frame` read only the new periods while
  `as_of` a date before it, and `revisions`, still show the old ones. Nothing is deleted.
- Redirects are followed only within the same host over https; any other is refused before anything
  is sent. httpcore's own loggers are scrubbed of keys too.
- `Retry-After` is honoured (up to 60 s; a longer wait stops the source for this run), and the retry
  of a request refused with 429 does not count again against the daily budget.
- After three requests in a row to a source fail every attempt, the rest of its series are not asked
  in that run and fail with `network_error`: a source that is down costs minutes, not hours.
- `sync --source` or `Store.sync(keys=...)` with a name the catalog does not declare is a
  configuration error (exit 2) that names it, not an empty run with exit 0.
- `as_of` reads every date without a time as the end of that day, UTC (`"2026-06-15"`,
  `"20260615"`, a `datetime.date`); text with a time, or a `datetime`, is that instant. Other text
  such as `"2026/06/15"` is refused (exit 2 for `show --as-of`).
- Catalog ids and aliases are read as written: YAML 1.1 no longer turns `0123` into 83, `12:30` into
  750 or `NO` into False. `bundled:macro` always names the bundled catalog, and a folder named
  `macro` no longer hides it.
- Moving an entry's `start` earlier, or removing it, makes the next sync ask for the missing
  history; the index records the earliest date asked (`asked_from`). `Store(clock=...)` also sets
  the sources' idea of today and each source's budget day.
- `sync --key fred:UNRATE` (repeatable) syncs only the series named; a key the catalog does not
  declare is a configuration error (exit 2).
- `sync.lock` is never deleted: on POSIX, a sync that opened it just as another released it could
  end up running beside a third one. The file is emptied when the lock is released.
- On POSIX the folder is flushed after a file is replaced, so the replacement itself survives a
  power cut.
- A store file the system will not open (held by OneDrive, an antivirus or Excel) is reported as
  "could not be opened" with the reason, not as damaged; JSON files too, instead of a traceback.
- A followed redirect waits its turn like any other request, so `per_minute` holds.
- A `Retry-After` that is not plain ASCII digits (such as "²") no longer crashes the request; the
  usual wait applies.
- Ids and aliases an entry gets through a YAML merge key (`<<: *defaults`) keep the text written.
- The store and the credentials share one implementation of the operating system's lock and of
  the atomic file write (`data_pipeline._files`).
- A sync removes the temporary files that a process killed mid-write left in the store
  (`<file>.<8 hex>.tmp`), once they are an hour old.

Sources:

- Comtrade asks each partner and flow for the periods it is missing. A run cut after the world's
  calls, or an entry given a new partner or flow, used to leave the other partners asking only for
  the revision windows. A table holds one HS level, read from the digits of each product code (2,
  4 or 6; nothing is stored, so rows synced by an earlier release have their real level too): an
  entry whose `level` differs from the stored one fails without a call, in a full sync as well,
  instead of mixing 2- and 4-digit products in one table.
- Comtrade stores a weight of 0 on a row with trade as missing: it is a weight not reported.
- FRED, BLS, Banxico, INEGI, the World Bank and DBnomics fail a series whose answer holds one period
  twice, instead of keeping one of the values. Banxico also checks a declared `frequency` against
  the dates of the data.
- SEC XBRL: a version takes its `frame` from whichever filing repeating it carries it. A version
  stored without one gets it on the next sync: the stored row is filled in, no version is added and
  `as_of` reads the same rows. Nothing has to be downloaded again.
- SEC filings: a filing that fails is recorded and skipped, so it no longer holds back the filings
  after it; three network failures in one company's run still end that run. File names are checked
  before a filing downloads: one that is not a plain file name (a separator of either kind, `..`, a
  drive, a final dot, a Windows device name such as `NUL.htm`) fails that filing alone, and the
  store refuses such a document path whatever sends it. An error that is not in the SEC's answer is
  no longer reported as "unexpected answer": it is named on the company it hit and stops the
  source. Filings count as known from the end of the day the SEC received them, like the XBRL facts
  they carry.
- BLS decides where each series' walk back stops on its own, so a series discontinued before the
  first 20-year window loads alone as it does in a group. A note of the API is about a series only
  when it names its whole id: `CUUR0000SA0` is no longer taken for absent by a note about
  `CUUR0000SA0E1`.
- SDMX: a grouped answer is split by a dimension only, never by an attribute such as `OBS_STATUS`,
  and a series' `UNIT_MULT` is recorded as `attrs.unit_mult` in the index; values are never rescaled.

Bundled `macro` catalog:

- The euro members' `e_<member>_fx_usd` columns read the euro per dollar from the BIS
  (`WS_XRU/M.<CC>.EUR.E`, the legacy currency at its euro conversion rate before 1999), and
  `e_ez_fx_usd` the euro area's. They held the legacy currencies, which end in 1998.
- Series that ended or held no values now read a live equivalent, checked against the publisher:
  ten short rates the OECD's 3-month interbank rate and Malaysia's the IMF's money-market rate;
  `e_ar_cpi` and `e_ez_hicp` the IMF's CPI and euro-area HICP; `e_il_ind_prod` the OECD's industrial
  production; `e_uk_10y` and `e_uk_unemployment_harmonised` the OECD's series, since Eurostat
  stopped the UK. Six money aggregates read the OECD's seasonally adjusted index instead of
  national-currency levels.
- 27 goods-trade series move from the DBnomics mirror to the IMF (ITG, IMTS). The replacement script
  measures closeness over a recent window (`--recent YEARS`) and calls two series equal when they
  differ by no more than their published rounding.
- `attrs.stale` also marks 62 series at their publisher whose last period, checked on 2026-10-08,
  was more than one period behind the staleness threshold. `e_tr_short_rate` and `e_ph_ind_prod`,
  with no data anywhere, stay, and their note says so.
- Every catalog name ends with the unit of its values; series in another unit than the rest of their
  indicator carry `attrs.units_differ`. Short rates name their instrument, and `pmi_mfg` says it is
  the OECD's confidence balance, not a PMI.
- `commercial_ok`: DBnomics entries are `restricted` and Banxico, INEGI and ECB entries
  `unverified`, as the README documents.

Keys:

- The root of a project is the closest folder with `.git`; only when there is no `.git` above is it
  the closest folder with `pyproject.toml`. The `.env` search stops there and never reaches the home
  folder from below it, so in a monorepo a package's `pyproject.toml` no longer hides the
  repository's `.env`; outside a project only the current folder's `.env` counts. `setup` asks
  before saving into a parent folder's `.env`.
- With no `.env` in the project yet, `setup` creates it at the project root, where it is found from
  every subfolder (outside a project, in the current folder, as before).
- `setup` stops before asking for any key when git already tracks the `.env`, since no ignore rule
  applies to a tracked file, and says to run `git rm --cached .env`.
- `setup` asks git whether `.env` and `.env.lock` are already ignored (any `.gitignore`,
  `.git/info/exclude`, the global excludes file) and appends to the `.env` folder's `.gitignore` only
  the names no rule matches. It stops instead of overriding a rule that un-ignores `.env`
  (`!.env`), and writes no `.gitignore` outside a git repository.
- Saving keys replaces `.env` atomically, under a lock on `.env.lock`, so two saves at once no longer
  lose lines. On POSIX a new `.env` is readable and writable by its owner only (0600); a save keeps
  the mode and group of an existing one, minus any access for others, and drops the group's access
  too when the group cannot be kept.
- A key with a tab or another control character inside is rejected while it is typed, before it is
  checked with its source, and `setup` asks for it again or skips it; whitespace around a key is
  dropped. When the `.env` cannot be saved (another program holds it), `setup` says so and offers
  to try again or skip that key, and goes on with the next.
- The repository's `.gitignore` lists `.env.lock`.
- A key with a control character or a Unicode line separator is refused, and `.env` lines are split
  only on CR LF, CR and LF, so a saved value can no longer turn into a line that sets another key.
