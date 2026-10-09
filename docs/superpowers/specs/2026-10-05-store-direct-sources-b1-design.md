# Design — store direct sources, group B1: World Bank and SDMX, and the macro catalog moved to them

Date: 2026-10-05
Status: Approved
Builds on: `2026-10-01-finport-datapipeline-design.md` (architecture) and
`2026-10-02-store-series-sources-a-design.md` (catalog fields, bundled catalog).

## Goal

Read the World Bank, the BIS, the ECB, Eurostat, the OECD and the IMF directly instead of
through DBnomics, and point the bundled macro catalog at those direct sources wherever the
series id translates mechanically.

## Why

A full sync of the macro catalog on 2026-10-05 showed that 1,066 of the 1,262 series it reads
through DBnomics are stale. DBnomics mirrors lag their origin, in some cases by years:

| Origin, read through DBnomics | Series | Stale | Typical age of the last observation |
|---|---|---|---|
| World Bank | 304 | 304 | about 1,000 days |
| OECD | 147 | 147 | about 1,000 days |
| BIS | 211 | 211 | about 490 days |
| IMF | 503 | 325 | about 490 days |
| Eurostat | 96 | 78 | about 310 days |

Three direct calls on the same day confirmed the premise for two of them and qualified it for
the third:

| Series | Last observation through DBnomics | Last observation read directly |
|---|---|---|
| World Bank, GDP of Chile | 2023 | 2025 |
| BIS, credit to the private sector, Argentina | 2024-Q4 | 2026-Q1 |
| Eurostat, HICP index, euro area | 2025-12 | 2025-12 |

The Eurostat case is a dataset its publisher stopped updating. A direct source does not fix
that; a new series id does. That kind of repair is group B2.

## Scope

| Family in the macro catalog | Series | In this group |
|---|---|---|
| World Bank WDI | 304 | Yes |
| BIS (`WS_EER`, `WS_TC`, `WS_SPP`, `WS_CBPOL`) | 211 | Yes |
| Eurostat | 96 | Yes |
| IMF WEO | 86 | Yes: the id maps to the current WEO, no longer pinned to the April 2025 edition |
| OECD `DSD_STES@DF_CLI`, `DSD_STES@DF_BTS` | 64 | Yes, if the live check confirms the dataflow version |
| ECB | 1 | Yes |
| IMF `IFS`, `DOT` | 417 | No. The IMF retired those datasets; new ids must be found. Group B2 |
| OECD `MEI` | 83 | No. Retired dataset. Group B2 |

762 series move; 500 stay on DBnomics until B2.

## Locked decisions

| Decision | Choice |
|---|---|
| Model | One id is one series, as today. No change to the sync engine |
| Efficiency | Sources group calls internally, the way BLS does |
| Aliases | The `e_*` aliases do not change; code that reads by alias is unaffected |
| Safety net | A series whose direct id fails the live check stays on DBnomics, listed in an exclusions file |
| New dependencies | None |

An earlier note said this group would need a change to the core, with one id expanding into
one series per country. It does not: the macro catalog already carries one fully specified
entry per country.

## The sources

### World Bank

- Id: `<indicator>/<economy>`, for example `NY.GDP.MKTP.CD/CHL`. The economy is the World
  Bank's own three-letter code, which for countries is ISO 3166 alpha-3.
- `GET https://api.worldbank.org/v2/country/{economies}/indicator/{indicator}?format=json&per_page=20000`,
  with the economies of every request for that indicator joined by `;`. One indicator is one
  batch. Pages are followed until the last.
- When `since` is set for every request of the indicator, `date={earliest since year}:{this year}`
  is sent; otherwise the full history is asked.
- Rows carry `countryiso3code`; each requested economy takes its own rows. A requested economy
  that returns no row fails with `NOT_FOUND`.
- An answer whose first element carries `message` means the indicator or one of the economies
  is invalid. The source then asks for each economy on its own, so one bad economy fails alone.
- Frequency is read from the `date` text: `2025` annual, `2025Q1` quarterly, `2025M01` monthly.
- Name: the indicator's name followed by the economy's name. Units: the row's `unit`.
- No key. `requests_per_minute = 60`, no daily budget.

### SDMX: one class, five providers

`bis`, `ecb`, `eurostat`, `oecd` and `imf` are five registered sources built from one class and
a table of provider settings (URL pattern, query parameters, headers).

| Source | URL pattern | Asks for CSV with |
|---|---|---|
| `bis` | `https://stats.bis.org/api/v1/data/{flow}/{key}/all` | `format=csv&detail=dataonly` |
| `ecb` | `https://data-api.ecb.europa.eu/service/data/{flow}/{key}` | `format=csvdata&detail=dataonly` |
| `eurostat` | `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{flow}/{key}` | `format=SDMX-CSV` |
| `oecd` | `https://sdmx.oecd.org/public/rest/data/{flow}/{key}` | `format=csvfile` |
| `imf` | `https://api.imf.org/external/sdmx/2.1/data/{flow}/{key}` | header `Accept: application/vnd.sdmx.data+csv;version=1.0.0` |

- Id: `<flow>/<key>`, split at the first `/`. Examples: `WS_TC/Q.AR.P.A.M.770.A`,
  `prc_hicp_midx/M.I15.CP00.EA`, `IMF.RES,WEO/ARG.GGXCNL_NGDP.A`,
  `OECD.SDD.STES,DSD_STES@DF_CLI,4.1/AUT.M.BCICP.IX._Z.AA.IX._Z.H`.
- One series per call. When `since` is set, `startPeriod={since year}` is sent; a year is valid
  for every frequency.
- Every provider answers with the columns `TIME_PERIOD` and `OBS_VALUE`. Rows without a period
  are skipped (the IMF answers an unknown key with one empty row).
- The key must select exactly one series. If a period appears twice, the series fails with
  `SOURCE_ERROR`: "the key returns several series: fix every dimension".
- Frequency is read from the period text: `2025` annual, `2026-Q1` quarterly, `2026-05` or
  `2026-M05` monthly, `2026-09-23` daily. Weekly periods (`2026-W05`) fail with "unsupported
  frequency".
- IMF WEO: rows whose year is later than the column `LATEST_ACTUAL_ANNUAL_DATA` are projections.
- Name: the column `SERIES_NAME` when present, otherwise the id. Units: the first of
  `UNIT_MEASURE`, `UNIT`, `unit` that is present.
- `UNIT_MULT`, when the answer carries it, is recorded in the index as `attrs.unit_mult` (its
  distinct values joined by commas), next to the catalog's attrs, which win on the same name.
  Values are stored as published and never rescaled: a series in millions and one in units of
  the same indicator show the difference there instead of changing stored data.
- HTTP 400 or 404, or an answer without observations, is `NOT_FOUND`. Any other status is
  `SOURCE_ERROR`.
- No key. Requests per minute: BIS 30, ECB 30, Eurostat 30, OECD 20, IMF 20. These are assumed,
  not taken from the providers' documentation. No daily budget.

`periods.py` gains `infer_frequency(text)`, used by both new sources.

## The macro catalog

`scripts/convert_macro_catalog.py` gains a translation step for DBnomics ids:

| DBnomics id | Becomes |
|---|---|
| `WB/WDI/A-<indicator>-<economy>` | `worldbank` `<indicator>/<economy>` |
| `BIS/<flow>/<key>` | `bis` `<flow>/<key>` |
| `Eurostat/<dataset>/<key>` | `eurostat` `<dataset>/<key>` |
| `ECB/<flow>/<key>` | `ecb` `<flow>/<key>` |
| `IMF/WEO:<edition>/<economy>.<indicator>.<unit>` | `imf` `IMF.RES,WEO/<economy>.<indicator>.A` |
| `OECD/DSD_STES@DF_CLI/<key>` | `oecd` `OECD.SDD.STES,DSD_STES@DF_CLI,4.1/<key>` |
| `OECD/DSD_STES@DF_BTS/<key>` | `oecd` `OECD.SDD.STES,DSD_STES@DF_BTS,<version>/<key>` |
| anything else | unchanged, still `dbnomics` |

The dataflow version for `DF_BTS` is found with a live call while the plan is written and then
fixed in the script.

`scripts/macro_direct_exclusions.txt` lists DBnomics ids that keep their DBnomics entry even
though a translation exists. It starts empty. The acceptance run fills it: every translated
series that fails directly is added, the catalog is regenerated, and the failure is reported.

Aliases, names, frequencies and attributes are carried over unchanged. Only `source` and `id`
change, so a store that already holds the DBnomics series keeps them under their old keys and
starts the direct series fresh. Reading by alias returns the direct series once it is synced.

## Error handling

The failure policy of the base spec applies unchanged.

## Testing

- `infer_frequency`: every spelling above, and text it cannot read.
- World Bank: grouping by indicator, the economies joined in one call, pagination, `since`,
  an economy without rows, the fallback to one call per economy on a `message` answer.
- SDMX: each provider's URL and parameters, `startPeriod`, the empty IMF row, a key that returns
  several series, the WEO projection flag, the unit columns, HTTP errors.
- Fixtures are the recorded answers in `Investment_Process/tests/macro/publicos/fixtures/`
  (`sdmx_bis.csv`, `sdmx_ecb.csv`, `sdmx_imf_weo.csv`, `sdmx_imf_vacio.csv`,
  `worldbank_pib.json`, `worldbank_error.json`).
- Catalog conversion: each translation rule, the exclusions file, and the existing test that the
  bundled YAML is in step with its source.
- One live test per source (six), off by default. None needs a key.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing 1,175 tests stay green.
2. The six live tests pass.
3. A sync of the macro catalog restricted to the six direct sources completes. Every series that
   fails directly is moved to the exclusions file and reported.
4. The number of stale series in the macro catalog is reported before and after, by origin.
5. For a sample of twenty series, the direct values agree with the DBnomics values on the
   periods both hold, or the difference is explained (a revision, or a different WEO edition).

Criteria 2 to 5 call public services without keys and write to a temporary folder.

## Out of scope

- New ids for IMF `IFS` and `DOT`, OECD `MEI`, and Eurostat datasets retired at origin: group B2.
- Batching several series per SDMX call.
- Country normalization across sources (ISO2 to ISO3): no series key needs it.
- Removing the DBnomics source: 500 series still use it.
