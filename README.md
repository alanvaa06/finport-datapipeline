# finport-datapipeline

[![PyPI](https://img.shields.io/pypi/v/finport-datapipeline)](https://pypi.org/project/finport-datapipeline/)
[![Python](https://img.shields.io/pypi/pyversions/finport-datapipeline)](https://pypi.org/project/finport-datapipeline/)
[![CI](https://github.com/alanvaa06/finport-datapipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alanvaa06/finport-datapipeline/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://github.com/alanvaa06/finport-datapipeline/blob/main/LICENSE)
[![pandas 2](https://img.shields.io/badge/pandas-2.x-150458)](https://pandas.pydata.org)
[![Typed](https://img.shields.io/badge/typing-typed-informational)](https://peps.python.org/pep-0561/)

**A local store of public economic and company data that never forgets.** It downloads a
series' history once, then only what changed. A revision adds a row instead of overwriting one,
so you can read any series **as it was known on a past date**: what a backtest needs to avoid
look-ahead bias.

| | |
|---|---|
| **License** | MIT |
| **Python** | 3.12, 3.13, 3.14 |
| **Dependencies** | pandas 2 (2.2.3 or later; pandas 3 is not supported yet), pyarrow, DuckDB 2, httpx, click, PyYAML, python-dotenv |
| **Status** | Alpha (0.1): the API may still change |
| **Docs** | [Guide](https://github.com/alanvaa06/finport-datapipeline/blob/main/docs/guide.md) · [Changelog](https://github.com/alanvaa06/finport-datapipeline/blob/main/CHANGELOG.md) · [Issues](https://github.com/alanvaa06/finport-datapipeline/issues) |

```text
 FRED  BLS  Banxico  INEGI  World Bank  BIS  ECB  Eurostat  OECD  IMF  DBnomics  Comtrade  SEC  Sharadar
                                          |
                           data-pipeline sync   (only what changed)
                                          v
                  a folder you choose: Parquet and JSON, every revision kept
                                          |
           series   frame   table   revisions   as_of=...   sql      (no key, no network)
```

## What's inside

| Data | What you get |
|---|---|
| **Macro** | A bundled catalog of 1,293 curated series across 44 economies and 61 indicators, each with a stable `e_*` name (`e_us_cpi`, `e_mx_target_rate`) |
| **Publishers** | FRED, BLS, Banxico SIE, INEGI, the World Bank, the BIS, the ECB, Eurostat, the OECD and the IMF, read directly; DBnomics for 96 series whose publisher retired them |
| **Tables** | UN Comtrade goods trade, and the XBRL facts companies report to the SEC, each version dated with its filing day |
| **Equities** | Sharadar prices, corporate actions, fundamentals, ticker records and daily metrics of US stocks and ETFs, with your own subscription |
| **Documents** | SEC filings (10-K, 10-Q, 8-K with exhibits, 20-F, 40-F), stored as the files the SEC publishes |

## Quick start

```bash
pip install finport-datapipeline
data-pipeline setup                                # asks for each key, checks it, saves it in .env
data-pipeline sync --root D:/data --catalog macro  # the first run downloads; later runs, what changed
data-pipeline show e_us_cpi --root D:/data
```

```python
import data_pipeline as dp

store = dp.Store("D:/data")
store.series("fred:UNRATE")                      # date, period, value
store.series("fred:UNRATE", as_of="2026-06-15")  # as it was known that day
store.frame(["fred:UNRATE", "fred:DGS10"])       # one column per series
store.table("sec_xbrl", "AAPL", concept="Assets", as_of="2024-03-01")  # as filed by that day
store.sql("SELECT * FROM fred WHERE key = 'fred:UNRATE'")              # DuckDB over the store

dp.fetch("fred", ["UNRATE", "DGS10"])            # no store: a DataFrame, right away
```

The latest commit on `main` installs with
`pip install git+https://github.com/alanvaa06/finport-datapipeline.git`.

## Command line

| Command | What it does |
|---|---|
| `setup` | Asks for each key, checks it with one request to its source, and saves it in the project's `.env` (added to `.gitignore` in a git repository) |
| `keys` | Shows which keys are set and where each comes from, never the values |
| `catalog` | Finds series by alias, name or key; needs no store, no key and no network |
| `sync` | Downloads what changed since the last run, for a whole catalog or `--source` / `--key` |
| `status` | Gives each series a state: `ok`, `stale`, `missing` or `failed` |
| `show` | Prints a series, optionally `--as-of` a date |
| `fetch` | Downloads without a store, to the screen or a Parquet, CSV or DuckDB file |
| `sql` | Runs one `SELECT` over the store through DuckDB |

Every option, exit code and catalog format is in the
[guide](https://github.com/alanvaa06/finport-datapipeline/blob/main/docs/guide.md#command-line).

## Keys

| Variable | Source | Needed for |
|---|---|---|
| `FRED_API_KEY` | FRED | United States macro |
| `BLS_API_KEY` | BLS | United States labour and prices |
| `BANXICO_TOKEN` | Banxico SIE | Mexico rates and FX |
| `INEGI_TOKEN` | INEGI | Mexico CPI and jobs |
| `COMTRADE_API_KEY` | UN Comtrade | Goods trade |
| `SEC_EDGAR_UA` | SEC EDGAR | Filings and XBRL facts; not a key but a contact: `Your Name you@domain.com` |
| `SHARADAR_API_KEY` | Sharadar | US stocks and ETFs; a paid subscription |

The World Bank, the BIS, the ECB, Eurostat, the OECD, the IMF and DBnomics need no key. Where
the `.env` lives and how `setup` guards it:
[guide](https://github.com/alanvaa06/finport-datapipeline/blob/main/docs/guide.md#keys).

## Reading as of a past date

`as_of` gives each period the last value known by the end of that day. How far back that
reaches depends on whether the source says when it published:

| Source | Point in time from |
|---|---|
| FRED series kept in ALFRED (`UNRATE`, `GDP`, `CPIAUCSL`, ...) | ALFRED's first vintage, years before your first sync |
| SEC XBRL facts and filings, Sharadar's as-reported fundamentals | The day of each filing |
| Sources that give no publication day | Your first sync: keep syncing, each run records what changed |

A value's `date` is the end of its period, not the day it came out: September's CPI is dated
2026-09-30 and published in mid-October. For a backtest, read with `as_of` set to each day you
simulate. The full rules are in the
[guide](https://github.com/alanvaa06/finport-datapipeline/blob/main/docs/guide.md#reading-as-of-a-past-date).

**Known limitation:** values are returned as stored, not rescaled. 38 macro series carry
`attrs.units_differ` (millions of USD next to USD, for example): check it before you combine
series. [#1](https://github.com/alanvaa06/finport-datapipeline/issues/1) tracks a scale per
series.

## Data terms

This library ships code, not data: each user brings their own keys, and the data goes to their
own disk. Each source has its own terms of use, and every catalog entry has a `commercial_ok`
flag:

- `yes`: Eurostat, the World Bank.
- `restricted`: the IMF, the BIS, the OECD, and DBnomics, which mirrors IMF and OECD data under
  their terms.
- `no`: FRED, Sharadar.
- `unverified`: Banxico, INEGI and the ECB. Their terms have not been reviewed for this flag;
  read them before a commercial use.

FRED limits redistribution of large datasets and the use of its data to train machine-learning
models. Sharadar's [Personal Use License](https://sharadar.com/terms) is for one person's own use
and forbids sharing its data, so this repository holds none: its test fixtures are invented. A
commercial use needs Sharadar's commercial license, sold through Nasdaq Data Link, whose API
this library does not read.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check src tests scripts
uv run mypy
```

CI runs these on every pull request, on Linux and Windows with Python 3.12, 3.13 and 3.14. Tests
that call the real APIs are marked `live` and run only when asked: `pytest -m live tests/live`.

## Authors

Alan Vazquez ([@alanvaa06](https://github.com/alanvaa06)) and Eduardo Ramos, CFA
([@EERamos](https://github.com/EERamos)).

## License

MIT, see [LICENSE](https://github.com/alanvaa06/finport-datapipeline/blob/main/LICENSE).
