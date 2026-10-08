# finport-datapipeline

A local store of public economic and company data that never forgets. It downloads a series'
full history once, then only what changed. A revision never overwrites a value: it adds a new
row, so you can read a series **as it was known on a past date**. That is what a backtest needs
to avoid look-ahead bias. How far back that works depends on the source: see
[Reading as of a past date](#reading-as-of-a-past-date).

- **Macro:** a bundled catalog of **1,293 curated series across 44 economies**. It covers rates,
  money and credit, prices, activity, sentiment, labour and the external sector. Every series
  has a stable `e_*` name, like `e_us_cpi` or `e_mx_target_rate`.
- **Sources, read directly from each publisher:** FRED, BLS, Banxico SIE, INEGI, the World Bank,
  the BIS, the ECB, Eurostat, the OECD and the IMF. DBnomics covers what has no direct id yet.
- **Tables:** goods trade from UN Comtrade, and the XBRL facts that companies report to the SEC.
  Each version of a fact is dated with the day it was filed.
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

Each source's key lives in one `.env` file at the root of your project; it is found from any
subfolder. A key passed in code wins over the environment, which wins over the file.

```bash
data-pipeline setup
```

`setup` asks for each key, hidden as you type it, and checks it with one request to its source
before saving. To see which keys are set and where each one comes from, run `data-pipeline keys`.
It never shows the values.

| Variable | Source | Needed for |
|---|---|---|
| `FRED_API_KEY` | FRED | United States macro |
| `BLS_API_KEY` | BLS | United States labour and prices |
| `BANXICO_TOKEN` | Banxico SIE | Mexico rates and FX |
| `INEGI_TOKEN` | INEGI | Mexico CPI and jobs |
| `COMTRADE_API_KEY` | UN Comtrade | Goods trade |
| `SEC_EDGAR_UA` | SEC EDGAR | Filings and XBRL facts; not a key, a contact: `Your Name you@domain.com` |

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

`--catalog` takes the name of a bundled catalog (`macro`) or the path to your own YAML file:

```yaml
- source: fred
  ids: [UNRATE, DGS10]
  alias:
    UNRATE: us.unemployment
```

`sync` exits with 0 when everything is up to date, 1 when there were failures, 2 for a
configuration error and 3 when a quota stopped it (run it again tomorrow). If a source is down,
the store keeps what it already had. A source that breaks (an unexpected answer, a damaged file)
fails its own series and the run goes on with the next source.

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
store.documents("sec_filings", "AAPL", form="10-K")  # the files on disk, with their path
```

### Reading as of a past date

`as_of` returns, for each period, the last value known by the end of that day. A value is known
from the day its source published it, when the source says so; otherwise from the day the store
fetched it.

- **FRED series kept in ALFRED** (most macro series, such as `UNRATE`, `GDP`, `CPIAUCSL`,
  `DGS10`): every vintage is stored with the day FRED published it, so `as_of` works back to
  ALFRED's first vintage, years before your first sync. `store.revisions(...)` lists each one.
  FRED gives the day, not the hour, so a vintage counts as known from the end of that day (UTC):
  `as_of="2026-10-02"` sees what came out that day, `as_of="2026-10-02T12:00Z"` does not.
- **SEC XBRL facts:** each version is dated with the day the filing was received, and also
  counts as known from the end of that day.
- **Every other source,** and FRED series that ALFRED does not keep (such as `SP500`): the
  source does not say when a value was published, so the store dates it by its own fetch. On
  those, `as_of` sees nothing before your first sync, and point-in-time history starts that day.
  Keep syncing regularly: each run records what changed.

To download from Python, give the store a catalog, or add entries for the session:

```python
store = dp.Store("D:/data", "macro")
store.add("fred", ["UNRATE"])
report = store.sync()
```

## Data terms

This library ships code, not data. Each user brings their own keys, and the data goes to that
user's own disk. Each source has its own terms of use. Every catalog entry has a `commercial_ok`
flag:

- `yes`: Eurostat, the World Bank.
- `restricted`: the IMF, the BIS, the OECD.
- `no`: FRED.

FRED limits redistribution of large datasets and the use of its data to train machine-learning
models. Check those terms before you build a commercial product on it.

## Development

```bash
uv venv
uv pip install -e . hypothesis mypy pandas-stubs pytest ruff types-PyYAML
pytest
```

Tests that call the real APIs are marked `live`, and only run when you ask for them:
`pytest -m live tests/live`.

## License

MIT, see [LICENSE](LICENSE).
