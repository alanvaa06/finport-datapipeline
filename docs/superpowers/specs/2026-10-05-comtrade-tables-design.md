# Design — UN Comtrade and the table kind

Date: 2026-10-05
Status: Approved
Builds on: `2026-10-01-finport-datapipeline-design.md`, which designed three kinds of data
(series, table, document) and built only the first. This spec builds the second, driven by its
first real source.

## Goal

Store goods trade from UN Comtrade in `data_pipeline.store` as a table: for each reporting
country, value and weight by partner, flow, product and period, loaded in full the first time
and kept up to date afterwards, with the store's revision history.

## Scope of the roadmap item

"Comtrade and SEC" is three independent pieces, each with its own spec, plan and verification:

| # | Piece | Kind |
|---|---|---|
| 1 | UN Comtrade (**this spec**) | table |
| 2 | SEC XBRL company facts | table |
| 3 | SEC filings (10-K, 10-Q, 8-K) | document |

## Locked decisions

| Decision | Choice |
|---|---|
| Default scope | Partner world, HS at 2 digits, exports and imports, annual from 2000, the last 75 closed months: what Investment_Process downloads today |
| Wider scope | Available by changing a catalog field; not the default, because it multiplies calls against a quota of 500 a day |
| One catalog id | One reporting country, written as ISO 3166 alpha-3 (`MEX`) |
| Revisions | Append-only, as for series: a changed value adds a row |
| Analytics | None. Sums over chapters and year-on-year changes belong to the consumer |

## Catalog

```yaml
- source: comtrade
  ids: [MEX, USA, CHN]
```

Fields of this source, all optional:

| Field | Default | Meaning |
|---|---|---|
| `level` | `AG2` | HS detail: `AG2`, `AG4` or `AG6` |
| `partners` | `[WLD]` | Partners: `WLD` for the world, or ISO3 codes |
| `flows` | `[X, M]` | Exports and imports |
| `annual_from` | `2000` | First year of the annual history |
| `months` | `75` | Closed months of monthly history; `0` turns the monthly data off |

`validate` rejects an unknown reporter or partner, a level outside the three, a flow outside
`X` and `M`, and numbers that are not whole and non-negative. The key of an entry is
`comtrade:<ISO3>`.

Comtrade numbers countries its own way (the United States is 842, not 840). The package ships
`store/sources/comtrade_reporters.json`, a map from ISO3 to Comtrade's reporter code, generated
by `scripts/build_comtrade_reporters.py` from Comtrade's public reference list (219 current
reporters). The 34 codes Investment_Process uses agree with it. A partner other than the world
is translated with the same map.

## The table on disk

`tables/comtrade/<ISO3>.parquet`, one file per reporter.

| Column | Meaning |
|---|---|
| `reporter` | ISO3 of the reporting country |
| `partner` | `WLD` or the partner's ISO3 |
| `flow` | `X` or `M` |
| `product` | HS code (`27`) |
| `frequency` | `A` or `M` |
| `period` | `2024` or `2024-06` |
| `date` | Last day of the period |
| `value_usd` | Trade value |
| `weight_kg` | Net weight |
| `fetched_at`, `published_at` | As for series |

Key columns: `reporter, partner, flow, product, frequency, period`. Value columns: `value_usd,
weight_kg`.

The append-only rule of the base spec applies on the key: a received row is appended when the
key is not stored or when any value column differs from the latest stored row (both missing
counts as equal; numbers within a relative tolerance of 1e-9 count as equal). No stored row is
changed or deleted.

Only the total of a key is stored. Comtrade also returns breakdowns by mode of transport,
customs procedure and second partner; those rows are discarded.

## What a sync asks for

There is no "since" for a table. For each reporter the source receives the `(frequency, period)`
pairs already stored and works out what to ask:

- annual: every year from `annual_from` to last year that is not stored, plus the last 2 years;
- monthly: every one of the last `months` closed months that is not stored, plus the last 12.

Periods are asked for 12 at a time, the API's limit; one such query is one call and one batch,
stored as soon as it arrives. Nothing keeps a list of pending queries: the next run recomputes
what is missing from the store, so a run stopped by the quota resumes by itself.

A period the reporter has not published yet is asked for again on every run until it appears.

`sync(full=True)` asks for everything again and stores only what changed.

## The source

- `GET https://comtradeapi.un.org/data/v1/get/C/{A|M}/HS` with `reporterCode`, `period` (comma
  list, monthly periods as `202406`), `partnerCode`, `cmdCode` (the level), `flowCode`, and
  `motCode=0`, `customsCode=C00`, `partner2Code=0` to ask for totals.
- Key in the header `Ocp-Apim-Subscription-Key` (`COMTRADE_API_KEY`). Without it nothing is
  requested and every reporter fails with `KEY_ERROR`.
- HTTP 401 is a rejected key. HTTP 403, and a persistent 429, mean the quota is used up: the
  source stops and the rest waits for the next run.
- `requests_per_minute = 30`, `daily_budget = 450` (the free tier allows 500 a day).
- A first load costs about 10 calls for a reporter: 3 for the annual history and 7 for 75 months.

## Changes to the core

- `model`: `TableData` (entry, key, rows, key columns, value columns, name, default staleness
  threshold), `FetchBatch.tables`, and `Request.held`, the `(frequency, period)` pairs already
  stored for a table entry.
- `storage`: `read_table` and `write_table`, and an append-only merge for any key and value
  columns. `latest` and `as_of` take the key columns as an argument. The series code path is
  not rewritten.
- `sync`: for a source of kind table, requests carry `held`; each table of a batch is merged
  into its file and its index row is refreshed. Counts, quota, lock and report work as for
  series.
- `index`: a `kind` column (`series` or `table`). A store written before this change reads as
  all `series`.
- `api`: `Store.table(source, id=None, *, as_of=None, **filters)` returns the rows of one
  reporter or of all, filtered by equality on any column, latest version of each key or as of
  a date. `status` covers tables: a table is stale when its newest monthly period is older than
  190 days (Comtrade publishes two to five months late), unless the entry sets its own
  threshold.
- `cli`: `show comtrade:MEX` prints the citation and the newest rows of the table.

## Error handling

The failure policy of the base spec applies. A query that fails (network, unexpected answer)
fails its reporter for this run; the periods it would have brought are asked for again on the
next run because they are still missing from the store.

## Testing

- Storage: the generic append-only merge (new key, same values, changed value, missing against
  number), latest and as-of on table keys, a round trip through parquet.
- Source: what is asked for an empty store and for a partly filled one, the revision windows,
  the blocks of 12, breakdown rows discarded, monthly period spelling, every catalog field and
  its validation, the reporter map, 401, 403, persistent 429, a missing key.
- Sync: `held` computed from the stored table, a table merged over several batches, the index
  row, counts, quota stop and resume, `full`.
- API and CLI: `table` with and without an id, filters, as-of, `status`, `show`.
- Fixtures: the recorded answers in `Investment_Process/tests/macro/publicos/fixtures/`
  (`comtrade_hs2.json`, `comtrade_llave_invalida.json`).
- One live test, off by default, skipped without the key.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing 1,258 tests stay green.
2. The live test passes with the user's key.
3. A first load of three reporters of Investment_Process's universe (about 30 calls) completes,
   and a second sync right after makes only the revision-window calls and adds no rows.
4. For those three reporters, values agree with Investment_Process's store on the keys both
   hold, or the difference is explained by a revision.

Criteria 2 to 4 use the user's Comtrade key and are run only after the user says so.

## Out of scope

- SEC XBRL and SEC filings: the next two specs.
- Services trade, and classifications other than HS.
- Sums, shares and growth rates.
- Migrating Investment_Process to read from this store.
