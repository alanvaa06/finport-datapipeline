# Design — Sharadar daily prices and corporate actions

Date: 2026-10-12
Status: Approved; brought up to date with what was built (see "What changed while building")
Builds on: `2026-10-05-comtrade-tables-design.md` (the table kind) and
`2026-10-06-credentials-design.md` (the key registry). This is the first of two pieces of
"Equity into the store" (roadmap row 6); Sharadar fundamentals is the next spec.

## Goal

Store in `data_pipeline.store` the daily prices and the corporate actions of the US stocks and
ETFs the user lists, as Sharadar publishes them, so that a consumer can read a price series and
work out a series adjusted for dividends from the closes and the actions.

## Locked decisions

| Decision | Choice |
|---|---|
| Vendor | Sharadar, direct (`api.sharadar.com`), under its Personal Use License. US only, end of day only |
| Universe | The tickers listed in the catalog, tens to a few thousand. Not the whole Sharadar universe |
| Instruments | Stocks (Sharadar table `stocks`) and ETFs (table `funds`) |
| What is stored of a price row | Every column Sharadar sends except `closeadj` (close adjusted for dividends) and `lastupdated`. Nothing is derived |
| Adjusted series | Left to the consumer: `close` plus the dividends of the actions table. A recipe goes in the README |
| How a sync asks | One call per ticker with its whole history, every run. No incremental window |
| Reading as of a day | Rows are dated by when they were downloaded, as in Comtrade. Not versioned |
| An action Sharadar no longer sends | Marked as withdrawn with a new row; nothing is deleted |
| What the repository holds | Code and hand-written fixtures. No Sharadar data and no assessment of it |

Why `closeadj` is left out: Sharadar restates it over the whole history of a ticker on every
dividend. In a store that never deletes, that is a full copy of the history per dividend.

Why the whole history every run: it is what `sec_xbrl` does, and it cannot miss a split. After
a run, the latest version of every day and of every action Sharadar sends is what it publishes
that day. What it stops sending is another matter: an action is marked as withdrawn, within the
rule given below; a price day stays as it was.

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

- An id is a Sharadar ticker in upper case: a letter or a digit, then letters, digits, `.` and
  `-`, at most 12 characters in all. `validate` rejects anything else.
- `sharadar_prices` takes one field of its own, `table`: `stocks` (the default) or `funds`.
  Any other value, and any other field, is a `CatalogError`.
- `sharadar_actions` takes no field of its own.
- The shared field `start`, when given, is the first day asked for. Without it the source asks
  from 1900-01-01. What is stored before `start` is not asked again and stays as it is.
- A ticker that is no longer listed never gets a new day, so it would show as stale: its entry
  sets `stale_after_days` to a large number.

The credential is `SHARADAR_API_KEY`, a secret, sent as the query parameter `api_key` and
scrubbed from messages and logs. Without it nothing is requested and every entry fails with
`KEY_ERROR`.

## The tables on disk

### Prices: `tables/sharadar_prices/<TICKER>.parquet`

| Column | Role | Meaning |
|---|---|---|
| `period` | key | The day, `YYYY-MM-DD` |
| `date` | | The day as a date |
| `open`, `high`, `low`, `close` | value | Adjusted for splits and stock dividends, as Sharadar sends them |
| `volume` | value | Adjusted for splits, as sent |
| `closeunadj` | value | The close as it traded, never adjusted |
| `fetched_at`, `published_at` | | As for every stored row; `published_at` is empty |

The table has no `frequency` column, on purpose. With `frequency` and `period` together, a
sync builds for every catalog entry, before its first call, the set of periods the store holds
(`Request.held`). This source never reads it, and with thousands of days per ticker that set
is a cost in memory on every run. The index row therefore has no frequency, and its staleness
comes from the threshold the source gives.

The split-adjusted columns change when a later split restates them; `closeunadj` does not. A
consumer that needs a series that never moves reads `closeunadj`.

### Actions: `tables/sharadar_actions/<TICKER>.parquet`

| Column | Role | Meaning |
|---|---|---|
| `frequency` | key | Always `D`. It is there so that the store tells the source what it holds |
| `period` | key | The day of the action, `YYYY-MM-DD` |
| `action` | key | Sharadar's own word: `dividend`, `split`, and the others it defines |
| `contraticker` | key | The other company of the action (a merger, a spin-off); empty when Sharadar sends `N/A` |
| `item` | key | `1`, `2`, ...: tells apart actions that share day, action and contraticker. Text, like every key |
| `date` | | `period` as a date |
| `value` | value | The amount of a dividend, in today's shares like `close`; the ratio of a split; missing when Sharadar sends none |
| `withdrawn` | value | 0 on a row Sharadar sends; 1 on a row that marks an action it no longer sends |
| `name`, `contraname` | attribute | Company names, as sent with the row; `N/A` is stored empty |

`item` numbers the rows that share day, action and contraticker by ascending `value` (missing
last), then by `contraname` and `name`, so the numbering does not depend on the order of the
answer. Without it a second dividend on the same day would be dropped without notice. A row
must carry `date`, `action`, `value` and `contraticker`; an `action` that is not a word (empty,
`N/A`) fails the ticker.

Attributes are stored with the row and never compared: a row keeps the names it came with until
its value changes.

### An action Sharadar no longer sends

Every field that describes an action is part of its key and the store never deletes. So when
Sharadar corrects the day of a dividend, or withdraws one, the stored row would stay current
next to the new one. Instead the source adds a row for the stored action with `withdrawn` 1 and
no value. A reader asks for the current actions with `withdrawn=0`. An action that comes back
is stored again with `withdrawn` 0.

The source knows what is stored through `Request.held` (`held_by` is `action`, `contraticker`
and `item`), and it marks under one rule:

**An answer speaks only for the days it covers, from its earliest action on.** A stored action
earlier than the earliest day in the answer is left alone, and an answer with no actions marks
nothing.

The rule exists because a short answer is not a withdrawal. An entry whose `start` moves
forward gets a shorter answer, and a request Sharadar treats as its free tier may get a shorter
history rather than a refusal. Under the rule neither can mark the older actions.

What the rule costs: the earliest action of a ticker, if Sharadar withdraws it or moves it to a
later day, is not noticed and stays current. To rebuild a ticker from what Sharadar publishes
today, the user deletes `tables/sharadar_actions/<TICKER>.parquet` and syncs.

A mark lands on a key, not on an event: when the lower of two dividends of a day is withdrawn,
item 1 takes the value that remains and item 2 is marked. The current rows are right; the
stored history is not a log of which dividend went away.

The index row of an actions table counts marked rows: its last period can be the day of a
withdrawn action.

### What a split does

Sharadar restates the split-adjusted columns of every earlier day. The next run brings those
rows with new values; `append_rows` appends them as new rows and keeps the old ones. `Store.table`
returns the latest row of each day; with `as_of` it returns what the store held at that moment.
`closeunadj` does not change, so a split adds one new row per earlier day and nothing else.

The amount of a dividend is taken to be in today's shares, as `close` is: a split then
restates the `value` of every earlier dividend, and those action rows are stored again in the
same way.

`as_of` earlier than the first sync of a ticker returns no prices. Dating a row by its own day
would fix that for `closeunadj` only: the split-adjusted columns of a 2015 row downloaded today
already know every later split.

## The sources

Three modules, as with the SEC:

- `store/sources/sharadar.py`: what both sources share. The URL, the ticker rule, one call
  that returns the rows of an answer, how Sharadar refuses, and the loop over tickers.
- `store/sources/sharadar_prices.py`: `SharadarPrices` and the reading of price rows.
- `store/sources/sharadar_actions.py`: `SharadarActions`, the reading of action rows and the
  marking of the ones withdrawn.

Both classes: `kind = Kind.TABLE`, `requests_per_minute = 120` (an assumption: Sharadar
publishes no rate for this API), `daily_budget = None`. `held_by` is `()` for prices and
`("action", "contraticker", "item")` for actions. `REGISTRY` and `TITLES` gain
`sharadar_prices` and `sharadar_actions`, both titled `Sharadar`.

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
- The prices source asks its `stocks` entries first, then its `funds` entries.

The index row of a ticker: name `Daily prices of <TICKER> (Sharadar <table>)` or
`Corporate actions of <TICKER> (Sharadar)`; `attrs.table` for prices. Staleness: 10 days for
prices (the store's threshold for a daily series). An actions table has a frequency only so
that the store says what it holds, so the source gives it a threshold that never triggers
(36,500 days). An entry's own `stale_after_days` wins in both.

### How Sharadar refuses

| Answer | Outcome |
|---|---|
| HTTP 401 | `KEY_ERROR` for this entry and the ones left of its table: "Sharadar rejected the key (HTTP 401): check SHARADAR_API_KEY" |
| HTTP 403 | `KEY_ERROR` for this entry and the ones left of its table: "Sharadar treated the request as its free tier (HTTP 403): check SHARADAR_API_KEY and that the plan includes the table `<table>`". The entries of the other table are still asked |
| HTTP 429 after the client's retries | The source stops as for a quota; the next run resumes |
| HTTP 200, no rows, prices | `NOT_FOUND`: "Sharadar has no prices for `<TICKER>` in its table `<table>`", then "from `<start>` on" when the entry has a `start`, then "; an ETF needs `table: funds`" when the table asked was `stocks` |
| HTTP 200, no rows, actions | The entry is recorded as reached and no table is written: a company without actions is normal. Reading its table raises `UnknownSeriesError`, as for any id without a stored table, and `status` lists it as `missing` |
| HTTP 200 without a list of rows in `data` | `SOURCE_ERROR` for that ticker, with an excerpt of what Sharadar said |
| Any other status, an answer that is not JSON, a row without a field | `SOURCE_ERROR` for that ticker |
| A network failure | `NETWORK_ERROR` for that ticker |

Sharadar does not reject a wrong key: it answers as its free tier, HTTP 200 for the few
tickers that tier covers and HTTP 403 for the rest. So a run with a wrong key stores real data
for the free tickers it meets and fails from the first ticker outside them. The 401 is handled
in case Sharadar ever sends one.

### The key

- `credentials.py`: the constant `SHARADAR = "SHARADAR_API_KEY"` and its `Key` record, added
  last in `REGISTRY`: label `Sharadar`, scope `US equities - prices & actions`, how
  `Subscribe at https://sharadar.com/subscribe and copy the key from your account`.
- `keys_check.py`: a probe through `sharadar_prices` with the entry `GOOGL` and `start` 400
  days back, the window every other probe uses. GOOGL was outside the free tier on 2026-10-12,
  so an answer with rows means a paid key. The verdict is wrong if GOOGL ever joins the free
  tier.

## Changes to the core

One line. `SourceReport.lines` in `sync.py` padded the source name to 12 characters, so a
longer name ran into the text of its line; it now always leaves a space. Names up to 11
characters print as before.

Nothing else: `storage` and `api` are used as they are.

The id `CON` (a Russell 2000 ticker, and a device name on Windows) writes and reads as
`CON.parquet` on Windows 11 build 26200, tested on 2026-10-12, so `Storage.table_path` is left
alone. A unit test keeps that fact; older Windows versions reserve the name and are not
supported for such ids.

## Limits this spec leaves

- **Size of the catalog.** Two calls per ticker per run, each with the whole history, so the
  calls and the download of a run grow with the catalog. The quota of a paid key is not
  verified. If a run cannot finish inside it, every run starts again from the first entry and
  the last ones are never refreshed (the `funds` entries first, since they are asked last).
  The way out is an incremental sync, a change to the sources only.
- **Account quota.** The two sources are paced and counted separately; Sharadar counts them
  together.
- **What Sharadar stops sending.** The earliest action of a ticker, if withdrawn, stays current
  (see above). A price day Sharadar stops sending stays as it was: prices have no marking.
- **A ticker without actions** has no table and shows as `missing` in `status`, which then
  exits with 1. How many tickers of a real catalog are like that is not known yet.
- **Tables of many tickers at once.** `Store.table("sharadar_prices")` without an id returns
  the rows of every ticker with no column that says whose they are, as for every table source.

## Error handling

The failure policy of the base spec applies: an unknown ticker fails alone; a network failure
or an unexpected answer fails that ticker; a refused key skips the rest of its table; an
exhausted quota stops the source and the rest waits for the next run. Nothing is filled in.

## Testing

Fixtures are written by hand in the shape of Sharadar's answers, with invented tickers and
numbers (see "Data terms"). The check that fails a fixture holding a real token stays as it is.

- Reading rows: a price answer (the six value columns and the day, `closeadj` and
  `lastupdated` left out, a missing value as NaN), a repeated day; an action answer (`N/A` as
  empty or missing, `item` numbered by value and names whatever the order of the answer, two
  dividends on one day both kept, the fields a row must carry).
- Sources: `date.gte` always sent, from `start` or 1900-01-01; the table chosen by `table`;
  the key as `api_key` and scrubbed from errors; missing credential; 401 and 403 as `KEY_ERROR`
  for the rest of the table only; persistent 429; no rows for prices and for actions; the cap
  of 10,000 rows; an answer that is not JSON or has no rows in `data`; `validate`; the index
  row.
- Withdrawals: what is marked and what is not (before the answer's earliest day, an empty
  answer), a day corrected, the lower of two dividends withdrawn, a short answer, an action
  that comes back, the same answer twice.
- Sync and API: a split (a second answer with the earlier days restated) appends one row per
  earlier day and `as_of` reads across it; a second sync right after a first adds nothing; no
  set of held days is built for prices; `Store.table` with filters; `status`; `show`.
- Storage: a table whose id is `CON` is written and read back. A table that is not versioned
  and has attribute columns round-trips (the first one in the store).
- Credentials and checks: the registry test names seven keys; `PROBED` covers the new key; the
  probe gives `ok` on rows and `rejected` on 403.
- Two live tests, off by default, skipped without the credential: five calls in all. They check
  shapes (columns, a first day before the default window, the words for a dividend and a
  split, the actions of an ETF, what a ticker Sharadar does not have is answered with), never
  values.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing tests stay green.
2. The live tests pass with the user's key.
3. A load of AAPL, MSFT, GOOGL and BRK.B (stocks), SPY and QQQ (funds), one delisted ticker,
   and the actions of all of them completes; a second sync right after adds no rows.
4. For AAPL, the first day stored and the closes around its split of 2020-08-31 agree with the
   probe notes of 2026-10-12.
5. For AAPL, the close adjusted for dividends worked out with the README recipe from `close`
   and the dividends agrees with Sharadar's `closeadj`, read with one call outside the store,
   to within 0.002 on every day.
6. `data-pipeline setup` reports the user's key as working and an invented one as rejected.

Criteria 2 to 6 call Sharadar with a paid key and are run only after the user says so. Their
results are reported to the user and are not committed.

Criterion 5 is the one most likely to fail: the recipe takes a dividend as stated in today's
shares and leaves spin-offs out, and neither was checked with a paid key. If it fails, the
recipe is corrected or taken out of the README; storing the data is not blocked.

## Documentation

- README: a Sharadar section with the catalog example, what a withdrawn action is and how to
  read the current ones, and the recipe for a close adjusted for dividends; Sharadar added to
  "Data terms" (see above).
- CHANGELOG.
- The plan, in `docs/superpowers/plans/`.

## Assumptions the live tests settle

The free tier does not show these, so the design assumes them until a paid key is used:

- The table `funds` has the columns of `stocks`.
- A paid key gets the whole history of `actions` from `date.gte` on.
- A split appears in `actions` with the action `split` and its ratio in `value`, as it does
  for a ticker of the free tier.
- The `actions` table holds the actions of ETFs too.
- An unknown ticker with a paid key answers HTTP 200 with no rows.

Two more are not shapes, and the acceptance run settles them: whether `actions` ever holds two
rows with the same day, action and contraticker (hence `item`), and whether a dividend `value`
is in today's shares for the whole history, as the recipe takes it (criterion 5).

What the live tests find about the shape of an answer is written back into this spec.

## What changed while building

The design above is what was built. It differs from the one first approved in these points,
each decided during the reviews of the code:

- Prices lost their `frequency` column (the owner's choice), for the cost in memory explained
  above. It had been kept for a later incremental sync.
- Actions gained `frequency`, `period` (it was `day`) and `withdrawn`, and the marking of
  withdrawn actions (the owner's choice). The first design left a corrected action current.
- A refusal (401, 403) is about one table, not the whole source.
- An answer without rows in `data` reports what Sharadar said; a not-found reason names the
  first day asked for; the hint about `funds` is given only for `stocks`.
- The ticker rule refuses a trailing line break; an action row must carry its fields.
- One line of the core changed, for the console line of a long source name.
- A ticker without actions writes no table (the first text said "an empty table").
- The README recipe works from `close` and the dividends, both in today's shares. The first
  one worked from the raw close and took a dividend as the amount paid.

## Out of scope

- Sharadar fundamentals, daily metrics (`daily`), the `tickers` table (permanent id, FIGI,
  sector, delisting flag) and `events`: the next spec.
- A source for Nasdaq Data Link, where Sharadar sells its commercial license.
- An incremental sync, the whole Sharadar universe, and the bulk files.
- Marking a price day Sharadar stops sending.
- Any derived column in the store, the close adjusted for dividends included.
- Turning the lists in `docs/catalogs/` into catalogs.
- Intraday prices, non-US markets (EODHD), options.
- Index membership over time, currencies, exchange calendars.
