# Design — store series sources, group A: BLS, Banxico, INEGI, DBnomics and the macro catalog

Date: 2026-10-02
Status: Approved
Builds on: `docs/superpowers/specs/2026-10-01-finport-datapipeline-design.md` (the store's
architecture, source contract, sync engine and catalog). Nothing in that spec changes except
where this one says so.

## Goal

Add four sources to `data_pipeline.store` and ship the user's curated macro catalog with the
library, so that one `sync` fills a local store with US labour data from BLS, Mexican data from
Banxico and INEGI, and the rest of the world through DBnomics.

## Why these four, and why now

Sub-project 2 of the roadmap listed six sources. They are of two kinds:

| Kind | Sources | What it needs |
|---|---|---|
| One id is one series | BLS, Banxico, INEGI, DBnomics | Fits the engine as built |
| One id is many series, one per country | SDMX (BIS, OECD, ECB, IMF, Eurostat), World Bank | A change to the core: keys per country, alias templates |

This spec covers the first kind. The second kind is group B and gets its own spec. Group B is an
improvement rather than a gap: DBnomics already reaches the World Bank, Eurostat, the IMF, the
OECD and the BIS, and the macro catalog uses it for 1,262 of its 1,294 series.

## Locked decisions

| Decision | Choice |
|---|---|
| Split | Group A (this spec) first, group B after |
| BLS series ids | Native ids, unchanged; `Unemployment_Analysis` slices them by position |
| BLS annual averages | Not requested. They would share a date with December |
| Frequency | The source's own report wins; otherwise the catalog's `frequency`; otherwise the series fails with a clear reason |
| Macro catalog | Converted to YAML and shipped inside the package; aliases are today's `e_*` column names |
| BLS footnotes (preliminary, revised) | Not stored in this group |

## Changes to the core

Three small ones. Everything else in the core stays as it is.

### 1. `Client.post`

BLS only accepts POST with a JSON body. The client gains:

```python
def post(self, source: str, url: str, *, json: Mapping[str, Any],
         headers: Mapping[str, str] | None = None,
         per_minute: int = DEFAULT_PER_MINUTE) -> httpx.Response
```

Same pacing, retries, call counting and scrubbing as `get`. `get` and `post` share one private
method so the retry loop exists once.

### 2. Catalog fields `frequency` and `name`, and the single-series spelling

`CatalogEntry` gains `frequency: Frequency | None` and `name: str | None`.

- `frequency` is one of `D`, `W`, `M`, `Q`, `A`. It is used only when the source does not report
  one. Any other value is a catalog error naming the entry.
- `name` replaces the source's name in the index. It exists because the macro catalog carries
  names curated by hand.

An entry can be written in either of two ways. The existing one, for several ids that share
their fields:

```yaml
- source: bls
  ids: [LNS14000000, CES0000000001]
  alias:
    LNS14000000: usa.empleo.desempleo
```

And a new one, for one series with its own fields:

```yaml
- source: dbnomics
  id: IMF/WEO:2025-04/ARG.GGXCNL_NGDP.pcent_gdp
  alias: e_ar_budget_balance_gdp
  name: Argentina general govt budget balance (% GDP)
  frequency: A
  attrs: {region: AR, commercial_ok: restricted}
```

Rules: an entry has `ids` or `id`, never both. With `id`, `alias` is a text. With `ids`, `alias`
stays a mapping and `name` is not allowed (one name cannot fit several series). `frequency` is
allowed in both and applies to every id of the entry.

A series id may contain colons (86 ids in the macro catalog do). A key is still
`<source>:<native id>`: the source is everything before the first colon.

### 3. Bundled catalogs

`Store(root, catalog="macro")` and `--catalog macro` load a catalog shipped in the package at
`data_pipeline/store/catalogs/macro.yaml`. Resolution: if the text names an existing file, that
file is used; otherwise, if a bundled catalog has that name, the bundled one is used; otherwise
the usual "does not exist" error, which now also lists the bundled names.

## The sources

Each is one file under `store/sources/`, one registry line and one citation title. All four are
`Kind.SERIES`.

### BLS

- `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` with `seriesid`, `startyear`,
  `endyear`, `registrationkey` and `catalog: true`. Key in `BLS_API_KEY`; without it every
  series fails with `KEY_ERROR` and nothing is requested.
- Limits of the API: 50 series and 20 years per request, 500 requests per day.
  `requests_per_minute = 50`, `daily_budget = 450` (a margin below the limit, because the budget
  check runs between batches).
- Requests are grouped by their start year, then cut into groups of at most 50. One group is one
  `FetchBatch`.
- Start year of a group: the earliest `since` among its requests. For requests without `since`
  (first load, or `full`): the entry's `start` if it has one; otherwise the source walks back
  from the current year in 20-year windows and stops at the first window in which no series of
  the group returns an observation.
- Name comes from the catalog block's `series_title`; seasonal adjustment from its `seasonality`
  (`SA` or `NSA`). If the block is absent the name is the id.
- Frequency comes from the period codes: `M01` to `M12` monthly, `Q01` to `Q04` quarterly, `A01`
  annual. `M13` and `Q05` (annual averages) are ignored if they appear. A series whose
  observations are only semiannual (`S01`, `S02`) fails with `SOURCE_ERROR`, "unsupported
  frequency".
- A series that returns no observation in any window fails with `NOT_FOUND`, carrying the API's
  message for that id when there is one.
- Status `REQUEST_NOT_PROCESSED`: when the message mentions the daily threshold the source raises
  `QuotaExhaustedError`; when it mentions the registration key it raises `KeyRejectedError`;
  otherwise every series of the group fails with `SOURCE_ERROR` and the message.
- Units are left empty: the data endpoint does not report them.

### Banxico SIE

- `GET .../SieAPIRest/service/v1/series/{id}/datos`, or `.../datos/{since}/{today}` when `since`
  is set. Token in the `Bmx-Token` header (`BANXICO_TOKEN`). The title comes with the data.
- Frequency: the catalog's `frequency` if declared, and then that is the only call. Otherwise a
  first call, `GET .../series/{id}`, asks for the metadata: its `periodicidad` (`Diaria`,
  `Semanal`, `Mensual`, `Trimestral`, `Anual`) gives the frequency and its `unidad` the unit. Any
  other periodicity fails with `SOURCE_ERROR`, "unsupported frequency".
- A declared frequency is checked against the dates of the data: Banxico dates a month, a
  quarter or a year by its first day, so a series declared `M`, `Q` or `A` with a date that does
  not start such a period, or declared `D` or `W` with every date on the first of a month, fails
  with `SOURCE_ERROR` naming the date. Without the check a daily series declared monthly kept
  one value a month, the last.
- Dates are `dd/mm/yyyy`; `N/E` is missing; thousands separators are stripped. All three are
  already handled by `read_period` and `number`.
- HTTP 404 is `NOT_FOUND`. A non-200 answer that mentions the token is `KeyRejectedError`.
- `requests_per_minute = 60`, no daily budget.

The shape of the metadata answer is taken from the API's documentation, not from a recorded
response. The live test of this source is what confirms it; until that test has run, the
declared `frequency` path is the verified one.

### INEGI

- `GET https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/{id}/es/{area}/false/{bank}/2.0/{token}?type=json`.
  Token in the URL path (`INEGI_TOKEN`), so the client's scrubbing matters here.
- Source-specific catalog fields: `bank` (default `BIE-BISE`; `BISE` for BISE ids) and `area`
  (default `00`, national).
- Frequency: the series' `FREQ` code (`8` monthly, `4` quarterly, `3` annual); otherwise the
  catalog's `frequency`; otherwise `SOURCE_ERROR`.
- INEGI returns every period, so `since` is applied after download.
- HTTP error with `ErrorCode:100` is `NOT_FOUND`. HTTP 401 or 403, or an error mentioning the
  token, is `KeyRejectedError`.
- The name is the catalog's `name` when given, otherwise the id: the API does not return a
  title. Units are `INEGI unit <code>`.
- `requests_per_minute = 60`, no daily budget.

### DBnomics

- `GET https://api.db.nomics.world/v22/series?series_ids={id}&observations=1`. No key.
- One series per request. Batching several ids per call is possible and is left for later; at
  60 requests per minute the full macro catalog takes about 21 minutes the first time.
- Observations arrive as two parallel arrays, `period` and `value`. `NA`, empty and non-finite
  values are missing.
- Frequency: the document's `@frequency` (`annual`, `quarterly`, `monthly`, `weekly`, `daily`);
  otherwise the catalog's `frequency`; otherwise `SOURCE_ERROR`.
- Name: the document's `series_name`. Units: empty.
- The endpoint has no date filter, so `since` is applied after download.
- An answer with no documents is `NOT_FOUND`.
- `requests_per_minute = 60`, no daily budget.

## The macro catalog

`scripts/convert_macro_catalog.py` reads the equity engine's
`equity/config_handlers/macro_catalog.json` (1,294 rows) and writes
`store/catalogs/macro.yaml`. The YAML is generated and committed; the script is rerun when the
JSON changes. A test fails if the two are out of step.

Per row:

| JSON | YAML |
|---|---|
| `provider` (`dbnomics`, `fred`, `banxico_sie`, `inegi`) | `source` (`banxico_sie` becomes `banxico`) |
| `series_id` | `id` |
| `column` (`e_us_cpi`) | `alias` |
| `name` | `name` |
| `frequency` (`monthly`, ...) | `frequency` (`M`, ...) |
| `region`, `commercial_ok` | `attrs` |

The two INEGI rows are BISE ids and get `bank: BISE`.

With it, `store.series("e_us_cpi")` works, and so does `store.info("e_us_cpi").label`.

The script lives outside both engines, so the rule that the store and the equity engine do not
import each other still holds.

## Error handling

Nothing new: the failure policy of the base spec applies. BLS is the first real user of the two
quota defences (its own persisted budget, and the server's signal) that the core already tests
with a fake source.

A series whose answer holds one period twice (twice with one publication day, for FRED's
vintages) fails with `SOURCE_ERROR`, "period ... comes more than once": its data is more
frequent than the frequency it is read with, or the answer repeats a row, and the store would
keep only one of the values. Every series source checks it (the SDMX sources already did).

## Testing

Same conventions as the existing store tests.

- One test file per source, answered by `httpx.MockTransport` from fixtures.
- Fixtures: Banxico data, INEGI and DBnomics answers are copied from the recorded responses in
  `Investment_Process/tests/macro/publicos/fixtures/` and from the equity engine's own tests.
  BLS answers and the Banxico metadata answer are written to the documented shape and confirmed
  by the live tests.
- `Client.post`: retries, pacing, counting and scrubbing, mirroring the `get` tests.
- Catalog: the single-series spelling, `frequency`, `name`, and every new error.
- BLS specifics: grouping in fifties, 20-year windows, the walk back to find the start, annual
  averages ignored, the three `REQUEST_NOT_PROCESSED` cases.
- Bundled catalog: `catalog="macro"` loads 1,294 entries with unique keys and unique aliases,
  every source registered, and every entry accepted by its source's `validate`.
- One live test per source, off by default, skipped when its key is absent.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing 1,103 tests stay green.
2. The four live tests pass with the user's keys.
3. BLS: a full load of the series in `Unemployment_Analysis`'s registry is compared with its
   cache (`data/cache/observations.parquet`, 1,107 series). Every difference is listed by
   series and period. Differences on periods revised by BLS after that cache was last written
   are expected; any difference on a period older than 24 months before that date is
   investigated before the group is closed.
4. Banxico and INEGI: a load of the series in Investment_Process's catalog is compared with its
   store, with the same reading of differences as was done for FRED.
5. DBnomics: a full sync of the bundled macro catalog completes; the report lists how many
   series were stored and which ids failed, for the user to curate.
6. A second sync of everything, run right after, adds zero observations.

Criteria 2 to 5 call real services with the user's keys and write outside the repository. They
are run only after the user says so.

## Out of scope

- SDMX and World Bank as direct sources, country expansion and alias templates: group B.
- BLS footnotes and the preliminary flag.
- Batching several ids per DBnomics call.
- Removing the equity engine's own FRED, Banxico, INEGI and DBnomics adapters: that is the
  bridge sub-project.
- Migrating `Unemployment_Analysis` or `Investment_Process` to read from the store.
