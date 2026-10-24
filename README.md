# finport-datapipeline

A local store of public economic and company data that never forgets. It downloads a series'
full history once, then only what changed. A revision never overwrites a value: it adds a new
row, so you can read a series **as it was known on a past date**. That is what a backtest needs
to avoid look-ahead bias. How far back that works depends on the source: see
[Reading as of a past date](#reading-as-of-a-past-date).

- **Macro:** a bundled catalog of **1,293 curated series across 44 economies**, 61 indicators. It
  covers rates, money and credit, prices, activity, sentiment, labour and the external sector.
  Every series has a stable `e_*` name, like `e_us_cpi` or `e_mx_target_rate`. See
  [The macro catalog](#the-macro-catalog).
- **Sources, read directly from each publisher:** FRED, BLS, Banxico SIE, INEGI, the World Bank,
  the BIS, the ECB, Eurostat, the OECD and the IMF: 1,197 of the catalog's series, 861 of them
  through SDMX. The other 96 come from DBnomics, a frozen mirror of datasets their publisher
  retired.
- **Tables:** goods trade from UN Comtrade, and the XBRL facts that companies report to the SEC.
  Each version of a fact is dated with the day it was filed.
- **Equity prices:** daily prices and corporate actions of US stocks and ETFs from Sharadar, with
  your own subscription. See [Equity prices](#equity-prices).
- **Equity fundamentals:** Sharadar's fundamentals in its six dimensions, its record of each ticker
  and its daily valuation metrics; the as-reported figures read as they stood on any past day.
  See [Equity fundamentals](#equity-fundamentals).
- **Documents:** SEC filings (10-K, 10-Q, 8-K with their exhibits, 20-F, 40-F), stored as the
  files the SEC publishes.
- **On your disk:** the store is plain Parquet and JSON in a folder you choose. Reading needs no
  key and no network.

Requires Python 3.12, 3.13 or 3.14.

## Install

```bash
pip install git+https://github.com/alanvaa06/finport-datapipeline.git
```

## Keys

Each source's key lives in one `.env` file at the root of your project: the closest folder that
holds `.git` or, with no `.git` above, the closest that holds `pyproject.toml` (so in a monorepo
the root is the repository's, not a package's). The file is found from any subfolder. The search
stops at that folder: a `.env` above it, in your home folder or in a shared folder is never read
or written. Outside a project only the current folder's `.env` counts. A key passed in code wins
over the environment, which wins over the file.

```bash
data-pipeline setup
```

`setup` asks for each key, hidden as you type it, and checks it with one request to its source
before saving. It shows the file it writes to, and asks first when that file is in a parent folder;
with no `.env` in the project yet, it creates one at the root of the project (outside a project, in
the current folder). Each save replaces the whole file at once, so it is never left half-written,
and two saves take turns on a `.env.lock` next to it. On Linux and macOS a new `.env` is readable by
you only; an existing one keeps its mode and group (0640 for a service's group, say), minus any
access for others. To see which keys are set and where each one comes from, run `data-pipeline
keys`. It never shows the values.

In a git repository, before the first key goes in, `setup` adds `.env` and `.env.lock` to the
`.gitignore` of that folder unless git already ignores them (it asks git, so a rule such as `.env*`
anywhere counts); outside a repository it writes no `.gitignore`. A rule that un-ignores `.env`,
such as `!.env`, stops `setup` instead of being overridden. If git already tracks the `.env`, an
ignore rule cannot keep it out of the next commit: `setup` stops before asking for any key and tells
you to run `git rm --cached .env`.

| Variable | Source | Needed for |
|---|---|---|
| `FRED_API_KEY` | FRED | United States macro |
| `BLS_API_KEY` | BLS | United States labour and prices |
| `BANXICO_TOKEN` | Banxico SIE | Mexico rates and FX |
| `INEGI_TOKEN` | INEGI | Mexico CPI and jobs |
| `COMTRADE_API_KEY` | UN Comtrade | Goods trade |
| `SEC_EDGAR_UA` | SEC EDGAR | Filings and XBRL facts; not a key, a contact: `Your Name you@domain.com` |
| `SHARADAR_API_KEY` | Sharadar | US stock and ETF prices, corporate actions, fundamentals, ticker records and daily metrics; a paid subscription |

The World Bank, the BIS, the ECB, Eurostat, the OECD, the IMF and DBnomics need no key.

## Command line

```bash
data-pipeline catalog cpi --region MX
data-pipeline sync --root D:/data --catalog macro
data-pipeline status --root D:/data
data-pipeline show e_us_cpi --root D:/data
```

`catalog` finds series in a catalog (the bundled `macro` by default) by alias, name or key, and
can narrow them with `--source` and `--region`. It needs no store, no key and no network.

`--catalog` takes the name of a bundled catalog (`macro`) or the path to your own YAML file. A
file of the same name in the folder wins over a bundled catalog; `bundled:macro` always means the
bundled one. Ids and aliases are read as the text you write (`0123` stays `0123`, `NO` stays `NO`):

```yaml
- source: fred
  ids: [UNRATE, DGS10]
  alias:
    UNRATE: us.unemployment
```

`sync --source fred` and `sync --key fred:UNRATE` (both repeatable) sync only part of the
catalog; `--full` asks for each series' whole history again and stores only what changed.

A series the source starts sending at another frequency (say you corrected a catalog's
`frequency: Q` to `M`) fails with a reason that names the way out: `sync --full --key <key>`.
That sync stores it at the new frequency and gives each old period a missing value, so `series`
and `frame` read only the new periods, while `as_of` a date before it still reads the old ones
and `revisions` lists them. Nothing is deleted.

`sync` exits with 0 when everything is up to date, 1 when there were failures, 2 for a
configuration error (an unknown `--source` or `--key` included) and 3 when a quota stopped it (run it again tomorrow). If a source is down,
the store keeps what it already had; after three requests in a row fail, the rest of its series
are not asked in that run and fail with `network_error`. A source that breaks (an unexpected answer, a damaged file)
fails its own series and the run goes on with the next source.

`status` gives each series a state: `ok`, `stale`, `missing` or `failed`, and exits with 1
unless every one is `ok`. A series is stale when its last period ended more than 10 days ago for
a daily series, 28 for a weekly one, 124 monthly, 183 quarterly and 730 annual; an entry's
`stale_after_days` sets its own limit. `catalog` marks with `[stale]` the series whose entry says
the publisher stopped updating them (`attrs.stale`).

`status --catalog catalog.yaml` reports the entries of that catalog, including the ones never
downloaded (`missing`), and only those. An entry you remove from the catalog (a ticker renamed or
dropped, say) keeps its data, readable by its key, but is no longer refreshed, so it no longer
counts: `status` ends with a line saying how many stored entries it left out, and its exit code
does not depend on them. `status` without `--catalog` still lists everything the store holds.
`Store.undeclared()` names those entries in Python. Nothing is deleted.

## Python

```python
import data_pipeline as dp

store = dp.Store("D:/data")
store.series("fred:UNRATE")                      # date, period, value
store.series("fred:UNRATE", as_of="2026-06-15")  # as it was known that day
store.frame(["fred:UNRATE", "fred:DGS10"])       # one column per series
store.revisions("fred:UNRATE")                   # every stored version of every period
store.table("comtrade", "MEX", flow="X")         # one row per partner, product and period
store.table("sec_xbrl", "AAPL", concept="Assets", as_of="2024-03-01")  # as filed by that day
store.table("sharadar_prices", "AAPL")           # one row per day: open, high, low, close, volume
store.table("sharadar_fundamentals", "AAPL", dimension="ARQ", as_of="2024-03-01")  # as filed by that day
store.documents("sec_filings", "AAPL", form="10-K")  # the files on disk, with their path
```

### Reading as of a past date

`as_of` returns, for each period, the last value known by the end of that day. A value is known
from the day its source published it, when the source says so; otherwise from the moment the
store downloaded it.

A date without a time (`"2026-06-15"`, `"20260615"` or a `datetime.date`) means the end of that
day, UTC. With a time (`"2026-06-15T08:00Z"`), or as a `datetime` or pandas `Timestamp` (even at
midnight), it is that instant, UTC when it has no zone. Other text, such as `"2026/06/15"`, is
refused with an error (exit 2 for `show --as-of`).

- **FRED series kept in ALFRED** (most macro series, such as `UNRATE`, `GDP`, `CPIAUCSL`,
  `DGS10`): every vintage is stored with the day FRED published it, so `as_of` works back to
  ALFRED's first vintage, years before your first sync. `store.revisions(...)` lists each one.
  FRED gives the day, not the hour, so a vintage counts as known from the end of that day (UTC):
  `as_of="2026-10-02"` sees what came out that day, `as_of="2026-10-02T12:00Z"` does not.
- **SEC XBRL facts and SEC filings:** each version of a fact, and each filing, is dated with the
  day the SEC received it, and also counts as known from the end of that day. A fact's `frame`
  (`CY2023Q4I`) comes from whichever filing carries it, often a later one that repeats the value.
  A version stored without it, by an older release, gets it on the next sync: the stored row is
  filled in, no version is added, and `as_of` reads the same rows. Nothing to re-download.
- **Sharadar fundamentals and ticker records:** an as-reported row is dated with the day of its
  filing, a most-recent row and a ticker record with the day Sharadar last updated the ticker,
  each known from the end of that day. See [Equity fundamentals](#equity-fundamentals).
- **Every other source,** and FRED series that ALFRED does not keep (such as `SP500`): the
  source does not say when a value was published, so the store dates it by its own fetch. On
  those, `as_of` sees nothing before your first sync, and point-in-time history starts that day.
  Keep syncing regularly: each run records what changed.

`date` is the last day of a value's period, not the day the value came out: September's CPI is
dated 2026-09-30 and published in mid-October. Without `as_of`, `series` and `frame` also give
each period its latest revision. Lined up on market dates as they are, or carried forward, they
put a value on days before anyone knew it. For a backtest, read with `as_of` set to each day you
simulate, or build the history from `store.revisions(...)`: a version is known from its
`published_at`, or from its `fetched_at` when the source gives no publication day. `frame` never
carries a value forward: a gap stays empty, and so do the later dates of a series that stopped.

To download from Python, give the store a catalog, or add entries for the session:

```python
store = dp.Store("D:/data", "macro")
store.add("fred", ["UNRATE"])
report = store.sync()
```

## Fetching without a store

`fetch` downloads from any source and gives you the data at once, without writing a store:

```python
import data_pipeline as dp

dp.fetch("fred", ["UNRATE", "DGS10"])                        # a pandas DataFrame
dp.fetch("sharadar_prices", "AAPL", to="aapl.parquet")       # also written to a file
dp.fetch("sharadar_prices", ["AAPL", "MSFT"], to="research.duckdb")  # into the table sharadar_prices
dp.fetch("sharadar_prices", ["SPY"], table="funds", start="2020-01-01")  # the catalog's fields
```

```bash
data-pipeline fetch fred UNRATE DGS10                        # a summary and the first 10 rows
data-pipeline fetch sharadar_prices AAPL MSFT --out prices.parquet
data-pipeline fetch sharadar_prices NVDA --out research.duckdb  # adds NVDA to the same table
data-pipeline fetch comtrade MEX --param annual_from=2020 --param "partners=[USA, CAN]"
```

What comes back is one frame for all the ids, long rather than wide:

| Source | Columns |
|---|---|
| A series source (FRED, BLS, Banxico, INEGI, World Bank, SDMX, DBnomics) | `id`, `date`, `period`, `value` |
| A table source (Sharadar, Comtrade, SEC XBRL) | `id`, then the columns `Store.table` gives for that source |

A series gives the latest version of each period, without periods that have no value and
without projections, as `store.series` reads it. A table gives the latest version of each row, as
`store.table` reads it. `id` is the id you asked for (`UNRATE`, not `fred:UNRATE`). Documents
(`sec_filings`) are not fetched: keep them in a store.

`to=` (`--out`) writes the same frame to a `.parquet` or `.csv` file, replacing a file of that
name, or into a `.duckdb` file (see below). Prefer Parquet: it keeps each column's type, and a CSV does not. Text that looks like a
number (an INEGI id `0123`, a Comtrade product `01`) reads back from a CSV as 123 and 1 unless you
read it as text: `pd.read_csv(path, dtype={"id": str, "product": str})`. The fields of a catalog entry (`start`, `frequency` and each source's own, such as
`table` or `partners`) are keyword arguments, or `--start`, `--frequency` and `--param NAME=VALUE`
(the value read as YAML, as in a catalog). Keys come from where `sync` takes them.

A `.duckdb` file is a [DuckDB](https://duckdb.org) database that grows a fetch at a time: each
source is a table named after it (`sharadar_prices`), and the rows of a fetch replace that table's
rows of the same ids, in one transaction, while every other row and table stays. All of an id's
rows are replaced, not only the days the fetch brought: fetching AAPL again with a later `start`
leaves only the days from that `start` (fetch it without `start` to have them all back). A source that
gained a column gets it, empty in the rows fetched before. Only one program can write a DuckDB
file at a time: a file another program has open raises `FetchError` with the rows in
`error.data`, and the file is as it was. The database keeps no versions: only a store answers
`as_of`.

When an id fails, `fetch` raises `dp.FetchError` once everything was asked: `error.data` holds
what arrived, `error.failures` one failure per id, `error.quota_exhausted` whether the quota
stopped it, `error.written` the file written (None when none was). With `to=`, what arrived is
written first; a fetch that failed and brought nothing (a missing key, say) leaves the file as it
was. A file that cannot be written (open in Excel, say), or cannot hold the rows (a column Parquet
cannot store, such as text mixed with numbers, or text that is not valid UTF-8), also raises
`FetchError`, with the rows in `error.data` and the reason in `error.write_error`, so the download
is not lost. The command
prints one `[x]` line per failed id and exits with 1 (3 when only the quota stopped it, 2 for a
configuration error such as an unknown source or a field the source does not take).

What `fetch` does not do: it keeps no versions and has no `as_of`, and every call downloads the
whole history (or from `start`) and spends the source's quota again, Sharadar's included. For
data you read more than once, keep a store and `sync` it.

## SQL

The store answers SQL. [DuckDB](https://duckdb.org) reads its Parquet files in place, with no copy
and no index to keep:

```python
store.sql("SELECT ticker, period, close FROM sharadar_prices WHERE period = '2026-10-22'")
store.sql("SELECT * FROM sharadar_fundamentals WHERE dimension = 'ARQ'", as_of="2024-03-01")
store.sql("SELECT * FROM sharadar_prices_versions WHERE ticker = 'AAPL'")  # every stored row
```

```bash
data-pipeline sql --root D:/data "SELECT * FROM fred WHERE key = 'fred:UNRATE'"
data-pipeline sql --root D:/data "SELECT * FROM sec_xbrl" --as-of 2024-03-01 --out facts.parquet
```

`store.sql` returns a pandas DataFrame. Each source with data in the store gives a query two
names, and the index a third:

| Name | Rows |
|---|---|
| `<source>` (`fred`, `sharadar_prices`, ...) | what `store.series` or `store.table` gives: the latest version of each key, or the one in force at `as_of` |
| `<source>_versions` | every row stored, with `fetched_at` and `published_at` |
| `store_index` | the index, as `store.index()` gives it |

A series source has the columns `key` (`fred:UNRATE`), `date`, `period` and `value`, without
missing values and projections, as `store.series` reads it. A table source has `id`, the id of
its catalog entry (`AAPL`), and then the columns `store.table` gives. `as_of` (`--as-of`) takes
what `store.table` takes, with the same meaning. Documents (`sec_filings`) have no name: read them
with `store.documents`.

`sql` runs one `SELECT` (or `WITH ... SELECT`) per call. Anything else (`CREATE`, `INSERT`,
`COPY ... TO`, `ATTACH`, `INSTALL`, `SET`, or two statements) is refused before it runs, a
`WITH` that writes (`WITH x AS (COPY ...)`) is refused by DuckDB, the functions that run SQL
from text (`query()`, `json_execute_serialized_sql()`) are refused, and the connection's
settings are locked, so a query does not write a file or change the store. These checks were
made on DuckDB 2.0: a later version could bring a new way around them. A query DuckDB refuses, such as
a name that does not exist, raises `StoreError` with DuckDB's message and the names a query can
read; the command prints them and exits with 2. Every query reads every source's files, so a
file DuckDB cannot read fails any query, with `StoreError` naming its source. A `SELECT` can still read other files on your computer (`SELECT *
FROM read_csv('C:/notes.csv')`): it runs with your rights, like any program you run.

Without `--out`, the command prints the first rows. `--out` takes the files `fetch` writes: a
`.parquet` or `.csv` file is replaced, and in a `.duckdb` file the rows replace the table
`result`, while the file's other tables stay. The command exits with 1 when the file cannot be
written, and with 2, writing nothing, when `--out` is inside the store or `--root` does not
exist.

## The macro catalog

`data-pipeline catalog` lists it. Each entry's name ends with the unit of its values, such as
`(USD)`, `(index)` or `(% p.a.)`, and the series of one indicator (the alias without its country)
share it. The 38 exceptions carry `attrs.units_differ`: goods trade and reserves still on the
DBnomics mirror, and Banxico's reserves, are in millions of USD where the IMF series of the same
indicator are in USD; FRED's nominal GDP is in billions at an annual rate.

Some indicators are only as uniform as their publishers allow:

- `short_rate` is the 3-month interbank rate (OECD) for ten economies, a money-market rate, a
  Treasury bill yield or rate for most others, and the deposit rate for Switzerland. The name of
  each says which.
- `pmi_mfg` is the OECD's manufacturing confidence balance: a percent balance around 0, not a
  purchasing managers' index around 50.

`attrs.stale` marks 158 series that are not current, with the reason: the 96 on DBnomics, and
62 whose publisher was, when checked on 2026-10-08, more than one period behind the store's
staleness threshold: the euro members' national policy rates, which end in 1998, the IMF's
unemployment rates and producer prices, published months late, and a few more. `catalog` shows
them with `[stale]`.

## Equity prices

`sharadar_prices` and `sharadar_actions` store the daily prices and the corporate actions of US
stocks and ETFs from [Sharadar](https://sharadar.com), with your own subscription. A catalog
lists the tickers:

```yaml
- source: sharadar_prices
  ids: [AAPL, MSFT, BRK.B]
  attrs: {commercial_ok: "no"}
- source: sharadar_prices
  table: funds            # ETFs are in Sharadar's `funds` table; stocks, the default, in `stocks`
  ids: [SPY, QQQ]
  attrs: {commercial_ok: "no"}
- source: sharadar_actions
  ids: [AAPL, MSFT, BRK.B, SPY, QQQ]
  attrs: {commercial_ok: "no"}
```

Each run asks for the whole history of each ticker, two calls per ticker, and stores what
changed. An entry with a `start` asks from that day on, and what is stored before it stays as it
is: after a later split the days before `start` keep the basis they had, while the later ones
are restated.

A price row holds its `ticker` and its day (`period`), then `open`, `high`, `low`, `close` and
`volume` adjusted for splits, as Sharadar sends them, `closeadj` (see below) and `closeunadj`,
the close as it traded.
When a ticker splits, Sharadar restates the adjusted columns of every earlier day: the next sync
stores those days again and keeps the rows it had, so `as_of` still reads what the store held
before. `closeunadj` never changes. Sharadar does not say when it restated a row, so prices read
as of a date from your first sync on.

An action row holds its `ticker`, its day (`period`), Sharadar's word for the action
(`dividend`, `split`, ...) and the other company it involves, if any, then its `value`. `item`
(`1`, `2`, ...) tells apart the actions that share ticker, day, word and other company. The
store never deletes, so an action Sharadar no longer sends, because it corrected its day or
withdrew it, gets a row with `withdrawn` 1 and no value. Read the current actions with
`withdrawn=0`.

An answer speaks only for the days it covers, from its earliest action on: an action stored
before that day is left alone, so a short answer never marks the older history. The rule has a
price: every stored action dated before the first one the answer still has stays current,
whatever became of it, and with a `start` that is everything stored before it. To rebuild the
actions of a ticker from what Sharadar publishes today, delete
`tables/sharadar_actions/<TICKER>.parquet` in the store and sync.

A ticker without actions has no table: `store.table("sharadar_actions", ticker)` raises
`UnknownSeriesError` (from `data_pipeline.store.errors`) for it, and `status` lists it as
`missing`.

Without an id, `table` reads every ticker stored, and `ticker` says whose each row is:
`store.table("sharadar_prices", period="2026-03-04")` is that day of all of them. It opens
every ticker's file to do so.

`closeadj` is Sharadar's close adjusted for splits, dividends and spin-offs, as Sharadar
computes it. Sharadar restates it over the whole history of a ticker on every dividend, so each
dividend stores every earlier day of that ticker again: the table of a ticker that pays every
quarter grows by its whole history four times a year. The rows before stay readable with
`as_of`.

A ticker that is no longer listed never gets a new day: give its entry a large
`stale_after_days`, or `status` calls it stale. Or remove it from the catalog: `status
--catalog` then leaves it out, and its rows stay readable.

## Equity fundamentals

`sharadar_fundamentals`, `sharadar_tickers` and `sharadar_daily` store three more tables of
Sharadar's for the tickers a catalog lists, with the same key and the same license:

```yaml
- source: sharadar_fundamentals
  ids: [AAPL, MSFT, BRK.B]
  attrs: {commercial_ok: "no"}
- source: sharadar_tickers
  ids: [AAPL, MSFT, BRK.B, SPY]
  attrs: {commercial_ok: "no"}
- source: sharadar_daily
  ids: [AAPL, MSFT, BRK.B]
  attrs: {commercial_ok: "no"}
```

Each run asks for the whole history of each ticker (from its `start`, if the entry has one;
a ticker record ignores `start`), one call per ticker and table, and stores what changed. A
ticker listed in all five Sharadar sources costs five calls a run: `sync --source
sharadar_tickers`, say, runs one of them alone, at a rhythm of its own.

- **Fundamentals:** one row per `ticker`, `dimension` and `calendardate`, the calendar quarter
  or year the row is about. The dimensions are `ARQ`, `ARY` and `ART` (as reported, by quarter,
  by year and over the trailing twelve months) and `MRQ`, `MRY` and `MRT` (the most recent
  figures, restated). Each row holds Sharadar's 105 indicators (`revenue`, `netinc`, `assets`,
  `ncfo`, `eps`, `pe`, ...) as numbers, in Sharadar's units, and `reportperiod`, `fiscalperiod`
  and `datekey` as text.
- **Ticker records:** one row per `ticker` and Sharadar `table` the ticker is in (`stocks`,
  `funds`, `fundamentals`, ...), with `permaticker`, Sharadar's permanent id, and what Sharadar
  says of the ticker (`name`, `exchange`, `sector`, `industry`, `isdelisted`, ...) as text. A
  record has no period, so `status` never calls it stale. A row for a Sharadar table the ticker
  is no longer in stays as it was: nothing marks it.
- **Daily metrics:** one row per `ticker` and day (`period`) with `ev`, `evebit`, `evebitda`,
  `marketcap`, `pb`, `pe` and `ps`. As with prices, a day Sharadar restates is stored again, and
  they read as of a date from your first sync on.

Fundamentals and ticker records are dated by Sharadar. An as-reported row counts as known from
the end of the day of its `datekey`, the day of the filing, so the as-reported panel reads as it
stood on any past day, with no look-ahead, back to the first filing:

```python
store.table("sharadar_fundamentals", "AAPL", dimension="ARQ", as_of="2024-03-01")
```

A most-recent row carries in `datekey` the end of its period, not a day it was known: it counts
as known from the end of the day of its `lastupdated`, the latest day Sharadar touched the
ticker. A ticker record counts as known from its `lastupdated` too, so as of an earlier day a
ticker that changed sector reads with the old one. Three costs come with this:

- Sharadar stamps every row of a ticker with a new `lastupdated` when it updates it. Each time,
  the store keeps another version of every most-recent row of that ticker, with the same numbers
  and a later date: one row per most-recent period, about 225 rows for a ticker with 25 years of
  history, each time Sharadar touches it. `sync` counts them as revised although no number
  changed. They compress well on disk.
- If Sharadar corrects an as-reported row without changing its `datekey`, the correction counts
  as known from the day of the original, so `as_of` a day between the two reads the correction.
  Without `as_of`, a read gives the version downloaded last. A correction that moves a
  `datekey` earlier is stored, but a read without `as_of` keeps returning the version dated
  later.
- The text of a ticker record is not compared: only a new `lastupdated` stores a new version,
  and a new `lastupdated` alone stores another version of every row of the record.
  If Sharadar changes a field without one, a field that had a value keeps the old one (a
  delisting would not show), and a field that was empty takes the new value in the stored
  version, so `as_of` an earlier day already reads it.

## Data terms

This library ships code, not data. Each user brings their own keys, and the data goes to that
user's own disk. Each source has its own terms of use. Every catalog entry has a `commercial_ok`
flag:

- `yes`: Eurostat, the World Bank.
- `restricted`: the IMF, the BIS, the OECD, and DBnomics, which mirrors IMF and OECD data under
  their terms.
- `no`: FRED, Sharadar.
- `unverified`: Banxico, INEGI and the ECB. Their terms have not been reviewed for this flag;
  read them before a commercial use.

FRED limits redistribution of large datasets and the use of its data to train machine-learning
models. Check those terms before you build a commercial product on it.

Sharadar's [Personal Use License](https://sharadar.com/terms) is for one person's own use. It
forbids sharing its data and publishing assessments of it, which is why this repository holds
none of either: the fixtures of its tests are invented. A professional or commercial use needs a
commercial license, which Sharadar sells through Nasdaq Data Link; that service has another API,
which this library does not read.

## Development

```bash
uv venv
uv pip install -e . hypothesis mypy pandas-stubs pytest ruff types-PyYAML
pytest
```

Tests that call the real APIs are marked `live`, and only run when you ask for them:
`pytest -m live tests/live`.

## Authors

Alan Vazquez ([@alanvaa06](https://github.com/alanvaa06)) and Eduardo Ramos, CFA
([@EERamos](https://github.com/EERamos)).

## License

MIT, see [LICENSE](LICENSE).
