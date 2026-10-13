# Design — Sharadar daily prices and corporate actions

Date: 2026-10-12
Status: Approved
Builds on: `2026-10-05-comtrade-tables-design.md` (the table kind) and
`2026-10-06-credentials-design.md` (the key registry). This is the first of two pieces of
"Equity into the store" (roadmap row 6); Sharadar fundamentals is the next spec.

## Goal

Store in `data_pipeline.store` the daily prices and the corporate actions of the US stocks and
ETFs the user lists, as Sharadar publishes them, so that a consumer can read a price series and
work out any adjusted series from the raw close and the actions.

## Locked decisions

| Decision | Choice |
|---|---|
| Vendor | Sharadar, direct (`api.sharadar.com`), under its Personal Use License. US only, end of day only |
| Universe | The tickers listed in the catalog, tens to a few thousand. Not the whole Sharadar universe |
| Instruments | Stocks (Sharadar table `stocks`) and ETFs (table `funds`) |
| What is stored of a price row | Every column Sharadar sends except `closeadj` (close adjusted for dividends) and `lastupdated`. Nothing is derived |
| Adjusted series | Left to the consumer: raw close plus the actions table. A recipe goes in the README |
| How a sync asks | One call per ticker with its whole history, every run. No incremental window |
| Reading as of a day | Rows are dated by when they were downloaded, as in Comtrade. Not versioned |
| What the repository holds | Code and hand-written fixtures. No Sharadar data and no assessment of it |

Why `closeadj` is left out: Sharadar restates it over the whole history of a ticker on every
dividend. In a store that never deletes, that is a full copy of the history per dividend.

Why the whole history every run: it is what `sec_xbrl` does, and it cannot miss a split. After
any run the latest version of every row is what Sharadar publishes that day, corrections to old
days included.

## Data terms

This repository is public and Sharadar's license is not. Its Personal Use License
(https://sharadar.com/terms, read on 2026-10-12) is for a natural person's personal use, forbids
sharing the data or any part of it, and forbids publishing conclusions drawn from testing or
evaluating the data or the service without Sharadar's written approval. So:

- The library ships code, as the README already says of every source. Each user brings a key
  and a license of their own, and the data goes to that user's disk.
- No answer of Sharadar's is committed. Fixtures are written by hand in the shape of an answer,
  with invented tickers and numbers.
- The spec, the README, the code and its comments describe how to call the API and what the
  store does with an answer. They do not measure or judge the data. What the probes of
  2026-10-12 found is kept outside the repository.
- The README's "Data terms" lists Sharadar with `commercial_ok: no` and says that a
  professional or commercial use needs a commercial license, which Sharadar sells through
  Nasdaq Data Link. That service has another API; a source for it is out of scope.

## Catalog

```yaml
- source: sharadar_prices
  ids: [AAPL, MSFT, BRK.B]
  attrs: {commercial_ok: "no"}
- source: sharadar_prices
  table: funds
  ids: [SPY, QQQ]
  attrs: {commercial_ok: "no"}
- source: sharadar_actions
  ids: [AAPL, MSFT, BRK.B, SPY, QQQ]
  attrs: {commercial_ok: "no"}
```

- An id is a Sharadar ticker in upper case: letters, digits, `.` and `-`, at most 12
  characters. `validate` rejects anything else.
- `sharadar_prices` takes one field of its own, `table`: `stocks` (the default) or `funds`.
  Any other value, and any other field, is a `CatalogError`.
- `sharadar_actions` takes no field of its own.
- The shared field `start`, when given, is the first day asked for. Without it the source asks
  from 1900-01-01.
- A ticker that is no longer listed never gets a new day, so it would show as stale: its entry
  sets `stale_after_days` to a large number.

The credential is `SHARADAR_API_KEY`, a secret, sent as the query parameter `api_key` and
scrubbed from messages and logs. Without it nothing is requested and every entry fails with
`KEY_ERROR`.

## The tables on disk

### Prices: `tables/sharadar_prices/<TICKER>.parquet`

| Column | Role | Meaning |
|---|---|---|
| `frequency` | key | Always `D` |
| `period` | key | The day, `YYYY-MM-DD` |
| `date` | | The day as a date |
| `open`, `high`, `low`, `close` | value | Adjusted for splits and stock dividends, as Sharadar sends them |
| `volume` | value | Adjusted for splits, as sent |
| `closeunadj` | value | The close as it traded, never adjusted |
| `fetched_at`, `published_at` | | As for every stored row; `published_at` is empty |

`frequency` and `period` are there so that a later change to an incremental sync finds
`Request.held` filled without migrating any stored table.

The split-adjusted columns change when a later split restates them; `closeunadj` does not. A
consumer that needs a series that never moves, or exact returns, reads `closeunadj` and the
actions table.

### Actions: `tables/sharadar_actions/<TICKER>.parquet`

| Column | Role | Meaning |
|---|---|---|
| `day` | key | The day of the action, `YYYY-MM-DD` |
| `action` | key | Sharadar's own word: `dividend`, `split`, and the others it defines |
| `contraticker` | key | The other company of the action (a merger, a spin-off); empty when Sharadar sends `N/A` |
| `item` | key | `1`, `2`, ...: tells apart actions that share day, action and contraticker |
| `date` | | `day` as a date |
| `value` | value | The amount of a dividend, the ratio of a split; missing when Sharadar sends `N/A` |
| `name`, `contraname` | attribute | Company names, as sent; `N/A` is stored empty |

`item` numbers the rows that share `day`, `action` and `contraticker` by ascending `value`
(missing last), so the numbering does not depend on the order of the answer. Without it a
second dividend on the same day would be dropped without notice.

### What a split does

Sharadar restates the split-adjusted columns of every earlier day. The next run brings those
rows with new values; `append_rows` appends them as new rows and keeps the old ones. `Store.table`
returns the latest row of each day; with `as_of` it returns what the store held at that moment.
`closeunadj` does not change, so a split adds one new row per earlier day and nothing else.

`as_of` earlier than the first sync of a ticker returns no prices. Dating a row by its own day
would fix that for `closeunadj` only: the split-adjusted columns of a 2015 row downloaded today
already know every later split.

## The sources

Three modules, as with the SEC:

- `store/sources/sharadar.py`: what both sources share. The URL, the ticker rule, one call
  that returns the rows of an answer, and how Sharadar refuses.
- `store/sources/sharadar_prices.py`: `SharadarPrices` and the reading of price rows.
- `store/sources/sharadar_actions.py`: `SharadarActions` and the reading of action rows.

Both classes: `kind = Kind.TABLE`, `requests_per_minute = 120`, `daily_budget = None`,
`held_by = ()`. `REGISTRY` and `TITLES` gain `sharadar_prices` and `sharadar_actions`, both
titled `Sharadar`.

### The call

`GET https://api.sharadar.com/v1.0/data/<table>` with `ticker`, `date.gte`, `format=json` and
`api_key`. `<table>` is `stocks`, `funds` or `actions`. The answer is
`{"count": N, "data": [ {...}, ... ]}`. A price row has `ticker, date, open, high, low, close,
volume, closeadj, closeunadj, lastupdated`; an action row has `date, action, ticker, name,
value, contraticker, contraname`.

- `date.gte` is always sent. A call without a date filter is answered with a recent window,
  not the whole history.
- One ticker per call. An answer holds at most 10,000 rows and is cut there without notice.
  An answer of 10,000 rows or more fails that ticker with `SOURCE_ERROR`, "the answer reached
  Sharadar's cap of 10000 rows and may be cut short: set `start` in the entry". A daily series
  reaches 10,000 rows after about 40 years.
- A day that comes twice in a price answer fails that ticker with `SOURCE_ERROR`.

The index row of a ticker: name `Daily prices of <TICKER> (Sharadar <table>)` or
`Corporate actions of <TICKER> (Sharadar)`; `attrs.table` for prices; staleness 10 days for
prices (the store's threshold for a daily series) and none for actions, unless the entry sets
its own.

### How Sharadar refuses

| Answer | Outcome |
|---|---|
| HTTP 403 | `KEY_ERROR` for this entry and the rest of the source: "Sharadar treated the request as its free tier: check SHARADAR_API_KEY and that the plan includes the table `<table>`" |
| HTTP 429 after the client's retries | The source stops as for a quota; the next run resumes |
| HTTP 200, no rows, prices | `NOT_FOUND`: "Sharadar has no prices for `<TICKER>` in its table `<table>`; an ETF needs `table: funds`" |
| HTTP 200, no rows, actions | The entry is stored as reached, with an empty table: a company without actions is normal |
| Any other status, an answer that is not JSON, a row without a field | `SOURCE_ERROR` for that ticker |
| A network failure | `NETWORK_ERROR` for that ticker |

Sharadar does not reject a wrong key: it answers as its free tier, HTTP 200 for the few
tickers that tier covers and HTTP 403 for the rest. So a run with a wrong key stores real data
for the free tickers it meets and fails from the first ticker outside them.

### The key

- `credentials.py`: the constant `SHARADAR = "SHARADAR_API_KEY"` and its `Key` record, added
  last in `REGISTRY`: label `Sharadar`, scope `US equities - prices & actions`, how
  `Subscribe at https://sharadar.com/subscribe and copy the key from your account`.
- `keys_check.py`: a probe through `sharadar_prices` with the entry `GOOGL` and `start` one
  year back. GOOGL was outside the free tier on 2026-10-12, so an answer with rows means a
  paid key. The verdict is wrong if GOOGL ever joins the free tier.

## Changes to the core

None. `sync`, `storage` and `api` are used as they are.

The id `CON` (a Russell 2000 ticker, and a device name on Windows) writes and reads as
`CON.parquet` on Windows 11 build 26200, tested on 2026-10-12, so `Storage.table_path` is left
alone. A unit test keeps that fact; older Windows versions reserve the name and are not
supported for such ids.

## Limits this spec leaves

- **Size of the catalog.** Two calls per ticker per run, each with the whole history, so the
  calls and the download of a run grow with the catalog. The quota of a paid key is not
  verified. If a run cannot finish inside it, every run starts again from the first entry and
  the last ones are never refreshed. The way out is the incremental sync, a change to the
  sources only.
- **Account quota.** The two sources are paced and counted separately; Sharadar counts them
  together.

## Error handling

The failure policy of the base spec applies: an unknown ticker fails alone; a network failure
or an unexpected answer fails that ticker; a refused key skips the rest of the source; an
exhausted quota stops the source and the rest waits for the next run. Nothing is filled in.

## Testing

Fixtures are written by hand in the shape of Sharadar's answers, with invented tickers and
numbers (see "Data terms"). The check that fails a fixture holding a real token stays as it is.

- Reading rows: a price answer (the six value columns, `D` and the day, `closeadj` and
  `lastupdated` left out, a missing value as NaN), a repeated day; an action answer (`N/A` as
  empty or missing, `item` numbered by value whatever the order of the answer, two dividends
  on one day both kept).
- Sources: `date.gte` always sent, from `start` or 1900-01-01; the table chosen by `table`;
  the key as `api_key` and scrubbed from errors; missing credential; 403 as `KEY_ERROR` for
  the rest; persistent 429; no rows for prices and for actions; the cap of 10,000 rows; an
  answer that is not JSON; `validate` (a ticker in lower case, an unknown field, a `table`
  that is neither `stocks` nor `funds`); the index row.
- Sync and API: a split (a second answer with the earlier days restated) appends one row per
  earlier day and `as_of` reads across it; a second sync right after a first adds nothing;
  `Store.table` with a filter on `period` and on `action`; `status`; `show`.
- Storage: a table whose id is `CON` is written and read back; a table that is not versioned
  and has attribute columns round-trips (the first one in the store).
- Credentials and checks: the registry test names seven keys; `PROBED` covers the new key; the
  probe gives `ok` on rows and `rejected` on 403.
- One live test, off by default, skipped without the credential. It checks shapes (columns,
  types, a first day before the default window), never values.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing tests stay green.
2. The live test passes with the user's key.
3. A load of AAPL, MSFT, GOOGL and BRK.B (stocks), SPY and QQQ (funds), one delisted ticker,
   and the actions of all of them completes; a second sync right after adds no rows.
4. For AAPL, the first day stored and the closes around its split of 2020-08-31 agree with the
   probe notes of 2026-10-12.
5. For AAPL, the close adjusted for dividends worked out with the README recipe from
   `closeunadj` and the actions agrees with Sharadar's `closeadj`, read with one call outside
   the store, to within 0.002 on every day.
6. `data-pipeline setup` reports the user's key as working and an invented one as rejected.

Criteria 2 to 6 call Sharadar with a paid key and are run only after the user says so. Their
results are reported to the user and are not committed.

Criterion 5 is the one most likely to fail: the recipe is written before a split has been seen
in the actions table. If it fails, the recipe is corrected or taken out of the README; storing
the data is not blocked.

## Documentation

- README: a Sharadar section with the catalog example and the recipe for a close adjusted for
  dividends; Sharadar added to "Data terms" (see above).
- CHANGELOG.
- The plan, in `docs/superpowers/plans/`.

## Assumptions the live test settles

The free tier does not show these, so the design assumes them until a paid key is used:

- The table `funds` has the columns of `stocks`.
- A split appears in `actions` with the action `split` and its ratio in `value`.
- `actions` may hold two rows with the same day, action and contraticker (hence `item`).
- A dividend `value` is the amount as paid.
- An unknown ticker with a paid key answers HTTP 200 with no rows.

What the live test finds about the shape of an answer is written back into this spec.

## Out of scope

- Sharadar fundamentals, daily metrics (`daily`), the `tickers` table (permanent id, FIGI,
  sector, delisting flag) and `events`: the next spec.
- A source for Nasdaq Data Link, where Sharadar sells its commercial license.
- An incremental sync, the whole Sharadar universe, and the bulk files.
- Any derived column in the store, the close adjusted for dividends included.
- Turning the lists in `docs/catalogs/` into catalogs.
- Intraday prices, non-US markets (EODHD), options.
- Index membership over time, currencies, exchange calendars.
