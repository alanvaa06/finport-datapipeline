# Design — Sharadar fundamentals, tickers and daily metrics

Date: 2026-10-22
Status: Draft, waiting for review
Builds on: `2026-10-12-sharadar-prices-design.md` (the Sharadar call, the ticker rule, the data
terms) and `2026-10-05-sec-xbrl-design.md` (versioned tables dated by their filing day). This is
the second piece of "Equity into the store" (roadmap row 6).

## Goal

Store in `data_pipeline.store`, for the tickers the user lists, three more tables of Sharadar's
Bundle: the fundamentals of each company in Sharadar's six dimensions, the descriptive record of
each ticker, and Sharadar's daily valuation metrics. Readable at their latest value, and as of a
past day where the source says when a value was known.

## Locked decisions

| Decision | Choice |
|---|---|
| Tables | `fundamentals`, `tickers` and `daily`, as three sources: `sharadar_fundamentals`, `sharadar_tickers`, `sharadar_daily` |
| Universe | The tickers listed in the catalog, as for prices. One catalog id is one ticker |
| Dimensions of the fundamentals | All six: `ARQ`, `ARY`, `ART` (as reported) and `MRQ`, `MRY`, `MRT` (most recent, restated) |
| Shape of the fundamentals | Wide: one row per ticker, dimension and calendar period, one column per indicator, as Sharadar sends them |
| `tickers` | One table per ticker, not one table of the whole market: the whole universe is more rows than one answer holds, and the API does not page |
| How a sync asks | One call per ticker and table, whole history, every run |
| What the repository holds | Code and hand-written fixtures. No Sharadar data and no assessment of it |

The data terms of the prices spec apply unchanged: Sharadar's Personal Use License forbids
sharing its data and publishing assessments of it, so fixtures are invented and what probes
find stays outside the repository.

## Catalog

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

- An id follows the Sharadar ticker rule of `sharadar.py`. None of the three sources takes a
  field of its own. The shared field `start` is the first day asked for, as for prices.
- The credential is `SHARADAR_API_KEY`, already in the registry. Nothing changes in
  `credentials.py` or `keys_check.py`.

## The tables on disk

### Fundamentals: `tables/sharadar_fundamentals/<TICKER>.parquet`

| Column | Role | Meaning |
|---|---|---|
| `ticker` | key | The catalog id asked for |
| `dimension` | key | `ARQ`, `ARY`, `ART`, `MRQ`, `MRY` or `MRT` |
| `calendardate` | key | The calendar quarter or year the row is about, `YYYY-MM-DD` |
| `date` | | `calendardate` as a date |
| 105 indicators | value | `revenue`, `netinc`, `assets`, `ncfo`, `eps`, `pe` and the others, as numbers, in the units Sharadar gives them |
| `reportperiod`, `fiscalperiod`, `datekey` | attribute | The company's own period end, its fiscal label, and Sharadar's `date` of the row, as text |
| `fetched_at`, `published_at` | | As for every stored row; see below for `published_at` |

The indicators are a fixed list in the source, the 105 numeric columns Sharadar's answer has,
in its order. A column Sharadar adds later is ignored until the list grows; a listed column an
answer lacks is stored as missing. Numbers come as JSON numbers or as text (large values come
as text) and both are read; `null` is missing.

The table is versioned: every row carries the moment it was known, and a key may hold several
versions.

- **An as-reported row (`AR*`)** is known from the end of the day of its `datekey`, which for
  these dimensions is the day of the filing. `as_of` therefore reads the as-reported panel as it
  stood on any past day, with no look-ahead, from the first filing on.
- **A most-recent row (`MR*`)** carries, in `datekey`, the end of its period, not a day it was
  known. It is known from the end of the day of its `lastupdated`, the latest day Sharadar says it
  touched the ticker: the earliest moment the store can claim without inventing one.

Two costs, accepted:

- Sharadar stamps every row of a ticker with a new `lastupdated` when it updates it. Each time,
  the store keeps another version of every `MR*` row of that ticker, with the same numbers and a
  later `published_at`: a few dozen rows per ticker per filing.
- If Sharadar ever corrects an as-reported row without changing its `datekey`, the corrected
  version carries the same `published_at` as the original, so `as_of` a day between the two reads
  the correction. The latest read takes the version fetched last.

### Tickers: `tables/sharadar_tickers/<TICKER>.parquet`

Sharadar's `tickers` table has one row per ticker and per Sharadar table it appears in (`stocks`,
`funds`, `fundamentals`, `insiders`, ...).

| Column | Role | Meaning |
|---|---|---|
| `ticker` | key | The catalog id asked for |
| `table` | key | The Sharadar table this row describes the ticker in |
| `date` | | The day of `lastupdated` |
| `permaticker` | value | Sharadar's permanent id, a number |
| `name`, `exchange`, `isdelisted`, `category`, `cusips`, `siccode`, `sicsector`, `sicindustry`, `figi`, `famaindustry`, `sector`, `industry`, `scalemarketcap`, `scalerevenue`, `relatedtickers`, `currency`, `location`, `lastupdated`, `firstadded`, `firstpricedate`, `lastpricedate`, `firstquarter`, `lastquarter`, `secfilings`, `companysite` | attribute | As sent, as text; `N/A` and `null` are empty |

The table is versioned, every row known from the end of the day of its `lastupdated`. Attributes
are never compared, so a change of sector or of the delisted flag is stored because Sharadar
stamps the row with a new `lastupdated`: the store keeps a new version, and `as_of` reads the
record as it was. The table has no staleness threshold.

### Daily metrics: `tables/sharadar_daily/<TICKER>.parquet`

| Column | Role | Meaning |
|---|---|---|
| `ticker`, `period` | key | The catalog id and the day, `YYYY-MM-DD` |
| `date` | | The day as a date |
| `ev`, `evebit`, `evebitda`, `marketcap`, `pb`, `pe`, `ps` | value | As Sharadar sends them, in its units |

Not versioned: rows are dated by when they were downloaded, as prices are, and a day Sharadar
restates is stored again. No `frequency` column, for the reason given in the prices spec.
`lastupdated` is not stored. Staleness: 10 days.

## The sources

- `store/sources/sharadar_fundamentals.py`, `sharadar_tickers.py`, `sharadar_daily.py`: each a
  class and the reading of its rows, built on `sharadar.py` (`Sharadar.rows`, `check_ticker`,
  `text`, `per_ticker`) as the prices and actions sources are. No change to `sharadar.py` is
  expected.
- All three: `kind = Kind.TABLE`, `requests_per_minute = 120`, `daily_budget = None`,
  `held_by = ()`. `REGISTRY` and `TITLES` gain the three names, titled `Sharadar`.
- The call is the one of the prices spec: `GET https://api.sharadar.com/v1.0/data/<table>` with
  `ticker`, `date.gte` (from `start` or 1900-01-01), `format=json` and `api_key`, for the tables
  `fundamentals`, `tickers` and `daily`. A probe with the public test key on 2026-10-22 showed
  that all three accept `date.gte`.
- Refusals, the row cap, an answer that is not JSON and a network failure are handled by
  `per_ticker` exactly as for prices. No rows: `NOT_FOUND` for fundamentals and daily metrics
  ("Sharadar has no fundamentals for 'X'"), `NOT_FOUND` for tickers too.
- A row without one of its key fields fails the ticker with `SOURCE_ERROR`; so does a key that
  comes twice with no versioning to tell them apart (daily metrics: a day twice; tickers: a
  table twice in one answer).
- Index rows: names `Fundamentals of <TICKER> (Sharadar)`, `Ticker record of <TICKER>
  (Sharadar)`, `Daily metrics of <TICKER> (Sharadar)`. Staleness: 200 days for fundamentals (a
  quarter, the filing deadline and slack, as `sec_xbrl`), none for tickers, 10 days for daily.

## Changes to the core

None expected. Versioned tables, their `published_at`, attribute columns and `as_of` exist for
`sec_xbrl` and are used as they are. The implementation plan checks this first.

## Limits this spec leaves

- **Quota.** With prices, actions and these three, a full run of a catalog of about 880 tickers is
  about 4,400 calls. The quota of a paid key is not known; the free key reported 5,000. Running the
  sources at different rhythms (`sync --source sharadar_tickers` weekly, say) is the user's lever.
  The user runs the syncs.
- **Repeated `MR*` versions** and **corrections of as-reported rows**, as above.
- **Not verified with a paid key:** that fundamentals and daily metrics come with their whole
  history from `date.gte` on (the free tier answers about five years), and that `datekey` of the
  as-reported rows is the filing day for every ticker, not only for the one probed.

## Testing

Fixtures written by hand, with invented tickers and numbers.

- Fundamentals: reading a row (the 105 indicators, text and JSON numbers, `null`, a listed column
  missing, an unlisted one ignored); `published_at` from `datekey` for `AR*` and from
  `lastupdated` for `MR*`; through the store: a first load, a second sync with the same answer
  adds nothing, a new filing adds `AR*` rows dated by their filing and a new version of the `MR*`
  rows, a restatement of an `MR*` value, `as_of` before a filing does not see it.
- Tickers: one row per table; a change of sector with a new `lastupdated` adds a version and
  `as_of` reads the old sector; the same answer twice adds nothing.
- Daily metrics: rows, a repeated day, a restated day.
- The three sources: `date.gte` sent, missing key, refusals, no rows, `validate`, index rows;
  the registry test that builds every source without credentials.
- One live test, off by default, shapes only: the three tables for AAPL, the six dimensions
  present, the first `calendardate` older than five years, `datekey` after `reportperiod` on
  every as-reported row.

## Acceptance

1. Unit tests pass; ruff and mypy clean; the existing tests stay green.
2. With the user's permission, the live test passes with the paid key (three calls). The user
   runs any sync of the real store.
3. What the live test shows about the whole history and `datekey` is written back into this
   spec, in terms of shape only.

## Documentation

README: the three tables, how to read the as-reported panel as of a day
(`store.table("sharadar_fundamentals", "AAPL", dimension="ARQ", as_of="2024-03-01")`), and the
two costs. CHANGELOG.

## Out of scope

- A store-less `fetch` API and CLI (another session).
- A SQL destination (later, after `fetch`).
- Sharadar's `events`, `insiders`, institutional holdings and `sf3` tables.
- Derived indicators, any mapping of these indicators to `sec_xbrl` concepts.
