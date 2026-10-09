# Design — finport-datapipeline: public-data store (architecture, core, FRED)

Date: 2026-10-01
Status: Design approved section by section; written spec pending user review
Branch: `docs/finport-datapipeline-design`
Relates to: `docs/superpowers/specs/2026-06-17-macro-data-layer-design.md`. That spec's
"latest-values-only" decision still describes the existing `e_*` macro layer; it does not apply
to the store designed here, which keeps revision history.

## Goal

Turn data_pipeline into an installable Python library, `finport-datapipeline` (import name
`data_pipeline`), that downloads public economic and financial data in several formats, keeps
it in a local store, loads full history the first time and then runs incrementally.

This spec covers the architecture of the whole store and the detailed design of its first
deliverable: the core engine plus one source, FRED. Other sources, the rename and the bridge to
the existing equity engine are separate sub-projects with their own specs.

## Context: what exists today

Three other repositories each carry their own public-data fetchers. Inventory taken on
2026-10-01 by reading each repository.

| Source | Investment_Process | research_analyst | Unemployment_Analysis | data_pipeline |
|---|---|---|---|---|
| FRED | yes, 33 series | yes, 5 + text file | no | yes, 24 columns |
| Banxico SIE | yes, 13 | roadmap only | no | yes, 6 |
| INEGI | yes, 5 | manual | no | yes, 2 |
| BIS, OECD, ECB, IMF, Eurostat | yes, one SDMX connector | no | no | only through DBnomics |
| World Bank | yes, 18 indicators | no | no | only through DBnomics |
| UN Comtrade | yes, HS2, 34 reporters | no | no | no |
| BLS | no (uses FRED) | no | yes, API v2, 1,143 series | no |
| SEC filings (documents) | no | yes, `tools/sec_fetch.py` | no | no |
| SEC XBRL companyfacts | no | yes, `tools/xbrl_fetch.py` | no | no |

Findings that shape the design:

- data_pipeline's macro layer (`MacroDataProviderInterface`, `e_*` columns) has no retries, no
  pacing and no incremental fetch, and one error aborts the run. Its entity is `date -> value`
  with no metadata. Comtrade and SEC documents cannot be expressed in it.
- `Investment_Process/investment_tools/macro/publicos/` (2,767 lines) was modelled on the
  original equity engine and went further: one HTTP client with retries and pacing, a parquet store, incremental
  updates with revision windows, 112 offline tests, in production use. It is the engine to port.
- `Unemployment_Analysis` depends on native BLS series ids. Its request budget does not persist
  between runs and it discards the fetch report; this design fixes both.
- `research_analyst` is a distributable plugin whose tools are standard-library only. It will
  not depend on this library.

## Locked decisions

| Decision | Choice |
|---|---|
| Role | data_pipeline becomes the central library; other repositories will read its store |
| Distribution name | `finport-datapipeline`, following `finport-optengine` and `finport-ratesengine` |
| Import name | `data_pipeline` (flat package, no namespace) |
| Audience | The user's own repositories first (`pip install` from GitHub); public release later |
| Relation to the equity engine | New store beside it. Neither imports the other until the bridge sub-project |
| Revisions | Append-only history. A changed value adds a row; nothing is overwritten |
| Storage format | Parquet files, atomic writes |
| Origin of the engine | Ported from Investment_Process, identifiers translated to English |
| Series keys | `<source>:<native id>`, with an optional alias |
| Catalog format | YAML |
| Analytics | None. The store returns normalized raw data |

Third-party notices are kept in `THIRD_PARTY_NOTICES`.

## Roadmap

| # | Sub-project | Delivers |
|---|---|---|
| 0 | Rename and packaging | `finport-datapipeline`, import `data_pipeline`, LSEG as an optional extra |
| 1 | Core + FRED (**this spec**) | The series engine, proven with one source |
| 2 | Series sources | BLS, SDMX (BIS, OECD, ECB, IMF, Eurostat), World Bank, Banxico, INEGI |
| 3 | Comtrade | Table storage, driven by its first real source |
| 4 | SEC | XBRL facts (table) and filings (documents) |
| 5 | Bridge | `e_*` columns and the config panel read from the store |
| 6 | Equity into the store | Optional, later |

Sub-projects 0 and 1 are independent. New code is created at `src/data_pipeline/store/` either
way; if the rename has not happened yet, `data_pipeline` ships in the same distribution next to
`data_pipeline.equity`. Migrating Investment_Process and Unemployment_Analysis to read from the
store happens in those repositories and is not part of this roadmap.

### Scope of sub-project 1

Built now: the HTTP client, keys, model, periods, catalog, series storage, sync, read API, CLI
commands and the FRED source.

Designed here but built later, each with its first real source: table storage (sub-project 3)
and document storage (sub-project 4). Building them without a source to drive them would mean
guessing at what Comtrade and SEC need.

## Architecture

```
src/data_pipeline/
  __init__.py      exports Store
  store/
    errors.py      every exception the store raises on purpose
    http.py        the one HTTP client: retries, per-source pacing, secrets scrubbed from errors
    keys.py        credentials from the environment and a .env file
    model.py       shared types
    periods.py     uniform period labels
    catalog.py     loads and validates the catalog
    sources/
      __init__.py  registry: source name -> class
      base.py      Source protocol, failure helpers, number parsing
      fred.py
    storage.py     parquet on disk: atomic writes, append-only merge
    sync.py        orchestrator
    api.py         Store facade
    cli.py         sync / status / show
    __main__.py    python -m data_pipeline.store
```

Dependency rules:

1. A source never touches the disk. It receives requests and returns typed results.
2. `storage` never touches the network.
3. Only `sync` knows both sides.
4. `data_pipeline.store` imports nothing from the equity engine, and the equity engine imports
   nothing from `data_pipeline.store`.

Adding a source is one file under `sources/`, a registry line and its tests.

Three kinds of data share one engine. Each source declares its kind.

| Kind | Sources | On disk |
|---|---|---|
| series | FRED, BLS, Banxico, INEGI, SDMX, World Bank | `series/<source>.parquet` |
| table | Comtrade, SEC XBRL | `tables/<dataset>/<partition>.parquet` |
| document | SEC filings | `documents/<source>/<entity>/` plus a manifest |

New runtime dependency for sub-project 1: `pyyaml`. `pycountry` (country codes) arrives with
sub-project 2, where SDMX and World Bank need it. New development dependency: `hypothesis`.

## Store on disk

```
<root>/
  store.json                 {"schema_version": 1}
  index.parquet              one row per series: provenance and last result
  series/<source>.parquet    observations, long format
  tables/<dataset>/<partition>.parquet      (sub-project 3)
  documents/<source>/<entity>/              (sub-project 4)
  runs.json                  per source: last run and its counts
  sync.lock                  present while a sync is running
```

Every write goes to a temporary file and then replaces the target with `os.replace`. A run that
dies midway never leaves a half-written file. A missing file reads as an empty table.

### Series observations

| Column | Type | Meaning |
|---|---|---|
| `key` | string | `<source>:<native id>`, e.g. `fred:UNRATE` |
| `period` | string | `2026-09-22`, `2026-05`, `2026Q2` or `2026` |
| `date` | date | Last day of the period |
| `value` | float64 | NaN when the source lists the period without a value. Never a made-up 0 |
| `projection` | bool | True for forecasts (IMF WEO years after the latest actual year) |
| `fetched_at` | timestamp, UTC | When this library saw the value: the time its batch was downloaded |
| `published_at` | timestamp, UTC, nullable | When the source published it, when the source says so |

### Append-only rule

For each `(key, period)` received in a sync:

- no stored row: append it;
- the latest stored row has the same `value` and `projection`: write nothing;
- otherwise: append a new row. The old row stays.

Two values are the same when both are NaN, or both are numbers within a relative tolerance of
1e-9 (float noise is not a revision). NaN against a number is a change.

The run report counts an appended row as *new* when it carries a number and the latest stored
row for that period had none (no row, or NaN), and as *revised* when the latest stored row had a
number that changed or became NaN.

No operation deletes or rewrites a stored row.

### Reading the latest value and reading as of a date

- Latest: per `(key, period)`, the row with the greatest `fetched_at`.
- As of X: let `known_at` be `published_at` when present, otherwise `fetched_at`. Keep rows
  with `known_at <= X`, then per `(key, period)` take the row with the greatest `known_at`,
  ties broken by `fetched_at`. A date without a time means the end of that day, UTC.

Limits of as-of reads, stated plainly:

- SEC XBRL facts carry their filing date, so as-of is exact for their whole history.
- For every other source, precision equals the sync cadence and history starts at the first
  sync. An as-of date before the first load returns nothing rather than a wrong value.
- FRED's vintage archive (ALFRED) could backfill `published_at`. Out of scope; the column is
  ready for it.

### Index

`index.parquet`, one row per series: `key`, `alias`, `source`, `source_id`, `name`, `country`,
`frequency`, `units`, `seasonal_adjustment`, `stale_after_days`, `attrs` (JSON text for
source-specific metadata such as BLS program or NAICS code), `first_fetched_at`,
`last_fetched_at`, `last_period`, `last_date` (the last day of the last real observation, used
for freshness), `status` (`ok` or `failed`), `reason` and `asked_from` (the earliest date a
download of the series asked for; empty for its whole history).

A failed series keeps its previous data and its row records the reason.

### Tables and documents (built in sub-projects 3 and 4)

- A table dataset declares its key columns and value columns. The append-only rule applies on
  the key. Comtrade: key `(partner, flow, hs, period, frequency)`, one file per reporter.
- Document files are never overwritten or deleted. The manifest records entity, document id,
  form, period, filing date, path, URL, hash, size and download time. An amendment is a new
  document.

### Resuming

There is no list of pending work. What is missing is recomputed by comparing the catalog with
the store, so nothing can fall out of step.

### Deliberately not covered

- A period that disappears at the source: the stored row stays, unmarked.
- Very large files: each sync rewrites that source's whole file. Fine at today's scale (BLS is
  about 537 thousand rows). Partitioning by year is added only if a file reaches millions of rows.

## Source contract

```python
class Source(Protocol):
    name: str                          # "fred"
    kind: Kind                         # SERIES | TABLE | DOCUMENT
    requests_per_minute: int
    daily_budget: int | None           # None when the source has no daily quota

    def validate(self, entry: CatalogEntry) -> None: ...
    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]: ...
```

- A `Request` is a catalog entry plus its own `since` date (`None` means full history). The
  source decides how to group requests: BLS sends 50 series per call, FRED one.
- `fetch` yields batches. `sync` stores each batch as it arrives, so an interrupted bulk load
  keeps what it already downloaded.
- A `FetchBatch` holds downloaded series and failures. Each failure names its entry, an outcome
  and a reason.
- When the quota runs out the source raises `QuotaExhaustedError` after yielding the completed
  batches.

Types in `model.py`: `Kind`, `Frequency` (`D`, `W`, `M`, `Q`, `A`), `Outcome` (`OK`,
`NOT_FOUND`, `KEY_ERROR`, `NETWORK_ERROR`, `SOURCE_ERROR`, `QUOTA_EXHAUSTED`), `CatalogEntry`,
`Request`, `Observation`, `SeriesData`, `Failure`, `FetchBatch`. All frozen dataclasses.

### Failure policy

| Situation | Effect |
|---|---|
| The series does not exist | Recorded; the run continues |
| The key is rejected or absent | The rest of that source is skipped; other sources continue |
| Network failure or unexpected answer | Only that entry fails |
| Quota exhausted | That source stops cleanly; the rest waits for the next run |

No value is ever filled in.

### HTTP client

One client for every source: 90 s timeout; three retries on 429, 500, 502, 503, 504 and
transport errors, waiting 2, 4 and 8 s; a minimum interval between calls taken from the
source's `requests_per_minute`; calls and 429 answers counted per source. Every secret is
replaced by `***` in any message the client raises. The clock and the sleep function are
injected so tests never wait.

### Credentials

Standard variable names, the same ones the other three repositories already use:
`FRED_API_KEY`, `BLS_API_KEY`, `BANXICO_TOKEN`, `INEGI_TOKEN`, `COMTRADE_API_KEY`,
`SEC_EDGAR_UA`. The process environment wins over the `.env` file. The file is `./.env` unless
another path is given. SEC needs a User-Agent with an e-mail address rather than a key; it is
handled as a credential. The representation of the credentials object shows only which ones are
present.

### Registry

A dictionary from source name to class in `sources/__init__.py`. Third-party sources through
plugins are considered at the public release, not now.

### FRED

- `GET /fred/series` for name, units, frequency and seasonal adjustment; `GET
  /fred/series/observations` with `file_type=json` and `observation_start` set from `since`.
- The key travels as a query parameter and is scrubbed from errors.
- `.` and empty values become NaN.
- HTTP 400 saying the series does not exist is `NOT_FOUND`. HTTP 400 naming the key is
  `KEY_ERROR`.
- Frequencies outside `D`, `W`, `M`, `Q`, `A` (biweekly, semiannual) are `SOURCE_ERROR` with the
  reason "unsupported frequency".
- `published_at` is empty. No realtime parameters are sent.
- `requests_per_minute = 100` (assumed, not verified against FRED's documentation), no daily
  budget.

## Catalog

A YAML file edited by hand, or entries passed in code.

```yaml
- source: fred
  ids: [UNRATE, DGS10, CPIAUCSL]
  alias:
    UNRATE: usa.empleo.desempleo
  start: 1990-01-01
```

- Minimum entry: `source` and `ids`. Name, units and frequency come from the source when it
  provides them. Optional: `alias` (id to alias), `start`, `stale_after_days`, `attrs`.
- Any other field is source-specific and is checked by that source's `validate`. FRED accepts
  none. An error names the entry and the field.
- Unknown source names, duplicate keys and duplicate aliases are errors. An alias may not
  contain a colon.
- The catalog lives wherever the user wants, typically in the consuming repository under
  version control. The store holds only data.
- Removing an entry deletes nothing. Its data stays and stops being refreshed.
- `store.add(...)` in code lasts for the session and does not rewrite the YAML file.

There is no separate discovery stage. The first sync is the verification: a bad entry ends in
the index with a status and a reason, and `status` shows it.

Entries that expand into many series (an SDMX call returns every country) and the form of their
keys are designed in sub-project 2. The core guarantees only that every series has a unique key.

## Sync engine

One command. `sync()` decides per entry:

| State of the entry | What it requests |
|---|---|
| Never downloaded | Full history, or from `start` if declared |
| Already downloaded | From the last stored real period minus the revision window |
| Already downloaded, `start` now earlier than `asked_from` (or removed) | From `start` (or the full history) |

Revision windows: daily 30 days, weekly 13 weeks, monthly 24 months, quarterly 36 months,
annual 60 months.

`sync(full=True)` requests everything again and stores only what changed. It exists because the
window misses deep revisions (BLS reworks five years in its annual benchmark; GDP is sometimes
reworked for its whole history).

`sync(sources=[...])` and `sync(keys=[...])` restrict the run.

Steps of a run:

1. Take the operating system's lock on `sync.lock` (an exclusive `flock` on POSIX, a byte-range
   lock on Windows) and write the holder's pid, host and start time in it. While another sync
   holds it, stop with exit code 2 and a message that names the holder and the file. The system
   frees the lock when its process ends, however it ends, so a file left behind by a sync that
   was killed is taken over by the next one.
2. Load and validate the catalog and the credentials.
3. For each source in turn: compute each request's `since`, call `fetch`, and for every batch
   merge into `series/<source>.parquet` and update the index.
4. Write that source's entry in `runs.json`, also when the source stopped early.
5. Remove the lock, also on failure.

Sources run one after another. Running them in parallel is safe because pacing is per source;
it is added only if the daily run proves slow.

### Quotas

Two defences, because either alone can fail:

1. Own budget. A source with a `daily_budget` has its calls for the day recorded in
   `runs.json`, so a second run on the same day knows what is left. The run stops before the
   limit. A day is a UTC calendar day; a source whose quota resets on another clock states it
   in its own spec.
2. Server signal. When the source reports an exhausted quota the run stops as well. This
   includes sources that answer HTTP 200 with a refusal in the body.

FRED has no daily budget, so both defences are tested in sub-project 1 with a fake source.

### Report

`sync()` returns a report and never discards it. Per source: series ok, series failed with
reasons, new observations, revised observations, calls made, quota state and pending entries.
The same content goes to `runs.json`.

```
[ok]  fred      33 series, 41 new, 3 revised, 66 calls
[x]   bls       1100 of 1143; quota exhausted, 43 pending
[x]   banxico   BANXICO_TOKEN missing in .env
```

Console output is ASCII only.

| Exit code | Meaning |
|---|---|
| 0 | Everything is up to date |
| 1 | There were failures |
| 2 | Configuration error: invalid catalog, another sync is running |
| 3 | Incomplete because of a quota; run again tomorrow |

### Freshness

`status()` returns one row per series with its last period, last fetch and state:

- `ok`
- `stale`: the last real observation (a number, not a projection, period ended by today) is
  older than the threshold. Thresholds: daily 10 days, weekly 28, monthly 124, quarterly 183,
  annual 730; an entry may set its own
- `missing`: declared in the catalog and never downloaded, or downloaded without a single real
  observation
- `failed`: the last attempt failed; earlier data is kept

"Today" is injected so freshness is testable.

## Public API

Reading never touches the network. A repository that only reads needs neither keys nor a
catalog.

```python
import data_pipeline as dp

store = dp.Store("D:/datos")                     # read only
store = dp.Store("D:/datos", catalog="catalog.yaml")
```

| Method | Returns |
|---|---|
| `series(key, start=None, end=None, as_of=None, projections=False)` | One series: `date`, `period`, `value` |
| `frame(keys, start=None, end=None, as_of=None)` | Several series, one column each, joined on date, gaps left empty |
| `info(key)` | Name, units, frequency, seasonal adjustment, source, status; `.label` gives a citation such as `[FRED: UNRATE, 2026-05, fetched 2026-06-06]` |
| `revisions(key)` | Every stored version of every period |
| `index()` | The whole index as a DataFrame |
| `status()` | Freshness per series |
| `add(source, ids, **fields)` | Adds catalog entries for the session |
| `sync(sources=None, keys=None, full=False)` | Runs a sync and returns the report |
| `table(dataset, as_of=None, **filters)` | Sub-project 3 |
| `documents(source, **filters)` | Sub-project 4 |

- A key containing a colon is a key; otherwise it is an alias.
- An unknown key or alias raises an error naming it. It does not return an empty table.
- `series` omits periods whose current value is NaN. `revisions` shows them.
- Projections are excluded unless `projections=True`, which adds a `projection` column.

## CLI

Three commands in a click group of their own, exposed as the console script `data-pipeline` and
as `python -m data_pipeline.store`. They are not added to the existing `data_pipeline.equity`
command group, because that would make the equity engine import the store and break dependency
rule 4. Sub-project 0 merges the two command sets under one script.

```
data-pipeline sync --root D:/datos --catalog catalog.yaml [--source fred] [--full] [--env-file PATH]
data-pipeline status --root D:/datos [--catalog catalog.yaml]
data-pipeline show fred:UNRATE --root D:/datos [--as-of 2026-06-15]
```

`--root` is required unless `DATA_PIPELINE_ROOT` is set. There is no hidden default folder.
Without `--catalog`, `status` reports only what the index knows and cannot report `missing`.

## Testing

Same conventions as the rest of the repository: pytest, `tests/unit/` mirroring the package,
`monkeypatch` (ruff bans `unittest.mock`), no network in the suite. The new subpackage is
checked with mypy in strict mode.

| Technique | Purpose |
|---|---|
| `httpx.MockTransport` | Sources and the client answer from recorded responses |
| Injected clock, sleep and today | Retries, pacing and freshness run without waiting |
| Recorded real responses | Fixtures come from the real API |
| Fixture key scan | Fails if a recorded response contains something shaped like a key |
| Property tests (hypothesis) | The store invariants below |
| `live` marker, off by default | One minimal real call per source, run by hand to detect API changes |

Store invariants:

1. Syncing the same data twice adds zero rows.
2. A normal read always returns the last value received.
3. An as-of read never returns something known after that date.
4. No operation deletes or changes a row already written.

Per unit:

- `http`: retries, pacing, secrets scrubbed
- `storage`: new, same, changed, NaN against number, tolerance, atomic write
- `sync`: the `since` computation, `full`, each case of the failure policy, stop on quota and
  resume, the persisted daily budget, the lock
- `api`: alias, projections, unknown key, as-of
- `cli`: exit codes, ASCII-only output
- `fred`: parsing, the `.` missing value, unknown id, bad key, unsupported frequency

## Acceptance criteria for sub-project 1

1. A full load of the 33 FRED series in Investment_Process's catalog gives values identical to
   its store on the same day.
2. An immediate second sync adds zero rows.
3. A simulated revision leaves two rows, and an as-of read returns the older one.
4. An interrupted run leaves the store readable and the next run completes it.
5. A missing key gives a clear message and exit code 1, with no raw traceback.
6. The existing equity tests stay green; mypy strict and ruff are clean on the new subpackage.
7. `pip install` of the distribution makes `import data_pipeline` work, whether or not the
   rename has happened.

## Out of scope

- Analytics: resampling, filling, percentage changes, z-scores.
- A scheduler, notifications, automatic retry the next day. The operating system schedules the
  command and reads its exit code.
- Reading the content of filings.
- IFRS filers in XBRL (us-gaap only, as `xbrl_fetch.py` today).
- JSON output for agents and an MCP server; considered at the public release.
- Changes to the config panel; they arrive with the bridge.
- ALFRED vintages.

## Porting map

| Origin (`Investment_Process/.../publicos/`) | New (`data_pipeline/store/`) | Change |
|---|---|---|
| `http.py` | `http.py` | English names; pacing read from the source |
| `llaves.py` | `keys.py` | Same variable names, more of them |
| `modelo.py` | `model.py` | Adds `published_at`, seasonal adjustment, `attrs`, `Kind`, `QUOTA_EXHAUSTED` |
| `periodos.py` | `periods.py` | Unchanged in behaviour |
| `almacen.py` | `storage.py` | Upsert becomes append-only |
| `actualizar.py` | `sync.py` | Adds batches, `full`, the daily budget, the lock, the report |
| `lector.py` | `api.py` | Adds as-of, `frame`, `revisions` |
| `catalogo.py` | `catalog.py` | Simpler entries; templates wait for sub-project 2 |
| `fuentes/base.py`, `fuentes/fred.py` | `sources/base.py`, `sources/fred.py` | `fetch` yields batches; `since` per request |
| `cli.py` | `cli.py` | click commands, exit codes |
| `descubrir.py`, `reporte.py` | not ported | The first sync is the verification |
| `comercio.py`, `fuentes/comtrade.py`, `fuentes/sdmx.py`, `fuentes/worldbank.py`, `fuentes/banxico.py`, `fuentes/inegi.py`, `paises.py` | later sub-projects | |

## Open items for later specs

- Sub-project 0: the old prefix of the equity credentials, the extension namespace
  (`data_pipeline_extensions`), the build backend, the first version number, and the
  entry scripts of existing workspaces that import `data_pipeline.equity`.
- Sub-project 5: what happens to DBnomics and its 1,262 catalog columns; and the date
  convention, since the store stamps a period at its last day while the existing `e_*` layer
  stamps it at its first.
- Before the public release: confirm that the code ported from Investment_Process and
  research_analyst may be published under this library's licence, and review each source's
  terms for redistribution.
