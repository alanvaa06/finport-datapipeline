# Design — SEC XBRL company facts

Date: 2026-10-05
Status: Approved
Builds on: `2026-10-05-comtrade-tables-design.md`, which built the table kind. This is the second
of the three pieces of "Comtrade and SEC"; SEC filings (documents) is the next spec.

## Goal

Store in `data_pipeline.store` every fact a company has reported to the SEC in XBRL, as the SEC
publishes it, with the date each version was filed, so that a fact can be read at its current
value or as it was known on a past date.

## Locked decisions

| Decision | Choice |
|---|---|
| What is stored | Every concept of every taxonomy the SEC returns, raw. No mapping to model lines, no de-accumulation of year-to-date figures, no derived fourth quarter: that stays in the consumer (`research_analyst`) |
| One catalog id | One company, written as its ticker (`AAPL`). The SEC's number (CIK) is resolved from the SEC's public list and kept in the index |
| A fact reported by several filings | One fact, and a new version only when the value changes. Each version carries the day the SEC received it |
| Reading as of a day | A version filed on a day is visible when reading as of that day (`filed` has no time of day) |

## Catalog

```yaml
- source: sec_xbrl
  ids: [AAPL, MSFT, COST]
```

The source takes no fields of its own. The key of an entry is `sec_xbrl:<TICKER>`. `validate`
rejects an id that is not a ticker in upper case (letters, digits, `.` and `-`).
Whether the ticker exists is known only at sync time: an unknown ticker fails that entry with
`NOT_FOUND`.

The credential is `SEC_EDGAR_UA`, a User-Agent such as `Name name@domain.com`, which the SEC
requires. Without it nothing is requested and every entry fails with `KEY_ERROR`. It is not a
secret and is not scrubbed from messages.

A ticker that changes (`FB` to `META`) is a new id: it is downloaded again with one call and
produces the same table, because every date in it comes from the SEC. The file of the old ticker
stays until the user deletes it.

## The table on disk

`tables/sec_xbrl/<TICKER>.parquet`, one file per company.

| Column | Role | Meaning |
|---|---|---|
| `taxonomy` | key | `us-gaap`, `dei`, `ifrs-full`, ... |
| `concept` | key | `Revenues`, `Assets`, ... |
| `unit` | key | `USD`, `shares`, `USD/shares`, ... |
| `start` | key | First day the fact covers, `YYYY-MM-DD`; empty for a balance at a date |
| `end` | key | Last day the fact covers, or the date of the balance |
| `date` | | `end` as a date |
| `value` | value | The number reported |
| `form` | attribute | Form of the filing that reported this version (`10-K`, `10-Q`, `20-F`, ...) |
| `accession` | attribute | Accession number of that filing |
| `filed` | attribute | Day the SEC received that filing |
| `fiscal_year`, `fiscal_period` | attribute | The SEC's `fy` and `fp` of that filing, as given. They describe the filing, not the period the fact measures |
| `frame` | attribute | The SEC's `frame` label, when any appearance of this version carries it |
| `fetched_at`, `published_at` | | As for every stored row; `published_at` is `filed` at 00:00 UTC |

### Versions

The SEC lists one appearance of a fact per filing that reports it: the original and every later
filing that repeats it as a comparative. For each fact the appearances are ordered by `filed`,
then by accession. The first is a version; a later one is a version only when its value differs
from the previous version (relative tolerance 1e-9). Repeats of the same value are dropped,
except for their `frame`: the SEC sets it on one appearance of a fact only, the latest filed,
so a version takes it from whichever of its appearances carries it. The other attributes are
those of the version's first appearance. A store synced before this change holds an empty
`frame` on such versions until its table is deleted and synced again (attributes are never
compared, so a sync does not rewrite them).

A version is identified by its key columns and its `published_at`. A sync appends the versions
that are not stored; no stored row is changed or deleted. A second sync right after a first adds
nothing.

The current value of a fact is its version with the latest `published_at`. As of a moment, it is
the latest version with `published_at` at or before that moment.

## The source

- `GET https://www.sec.gov/files/company_tickers.json` once per run: ticker to CIK.
- `GET https://data.sec.gov/api/xbrl/companyfacts/CIK<10 digits>.json` once per company per run.
  The SEC returns the whole history every time; there is no "since".
- Header `User-Agent: <SEC_EDGAR_UA>`.
- `requests_per_minute = 300` (the SEC allows 10 a second), no daily budget.
- HTTP 404 on company facts: `NOT_FOUND`, "the SEC has no XBRL facts for this company".
- HTTP 403, and a persistent 429: the SEC blocks a client for about ten minutes when it exceeds
  the rate or sends a User-Agent without a contact. The source stops as for a quota and the
  message says to check `SEC_EDGAR_UA` or wait.
- A failure of the ticker list fails every entry of the run.
- A fact whose value is not a number, or without an `end`, is skipped.

The index row of a company: name from the SEC (`entityName`), `attrs.cik`, the newest `end` among
its facts as last period, and a staleness threshold of 200 days (a quarter, the filing deadline,
and slack) unless the entry sets its own.

## Changes to the core

- `model.TableData`: `attribute_columns` (stored, never compared), and `versioned`: the rows carry
  their own `published_at` and may hold several versions of a key.
- `storage`: a merge for versioned tables (append the versions whose key and `published_at` are
  not stored), and a "latest" for them that orders by `published_at`. The table's `schema.json`
  records the attribute columns and whether the table is versioned. The merge and "latest" of
  series and of Comtrade are not changed.
- `sync`: a table is not required to have `frequency` and `period`; without them `held` is empty
  and the index row takes its last period from the newest `date`.
- `api`: `Store.table` returns the attribute columns too and reads versioned tables with their
  own "latest"; `as_of` already uses `published_at`.

## Error handling

The failure policy of the base spec applies: an unknown ticker or a company without facts fails
alone; a network failure or an unexpected answer fails that company; a block by the SEC stops
the source and the rest waits for the next run.

## Testing

- Versions: one appearance, repeats dropped, a restatement kept, a value restated and restated
  back, order by filed then accession, facts without `end` or with a non-numeric value skipped,
  balances (no `start`).
- Storage: the versioned merge (first load with several versions of a key, a second load adds
  nothing, a new version is appended, attributes do not count), latest and as-of on versions, the
  schema round trip, a schema written before this change.
- Source: ticker resolution (case, unknown ticker, list failure), the User-Agent header, missing
  credential, 404, 403, persistent 429, the index row.
- Sync, API and CLI: a table without `frequency`, `table` with filters on attributes, as-of
  across a restatement, `status`, `show`.
- One live test, off by default, skipped without the credential.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing 1,333 tests stay green.
2. The live test passes with the user's User-Agent.
3. A load of AAPL, MSFT, COST and one foreign filer that reports in IFRS completes, and a second
   sync right after adds no rows.
4. For AAPL, the annual values of the concepts `research_analyst/tools/xbrl_fetch.py` maps agree
   with what that tool downloads.
5. Reading as of a date before a known restatement returns the earlier value.

Criteria 2 to 5 call the SEC with the user's User-Agent and are run only after the user says so.

## Out of scope

- Mapping concepts to model lines, de-accumulating year-to-date flows, deriving the fourth
  quarter: they stay in `research_analyst`.
- Labels and descriptions of concepts.
- The SEC's frames API (one concept across every company).
- SEC filings as documents: the next spec.
