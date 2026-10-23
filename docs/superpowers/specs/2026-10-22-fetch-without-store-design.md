# Design — Fetching without a store

Date: 2026-10-22
Status: Approved
Builds on: the `Source` protocol (`src/data_pipeline/store/sources/base.py`), `keys_check.py`
(which already drives a source without a store), and `2026-10-06-credentials-design.md`.

## Goal

A public, documented way to download data from any registered source without writing to the
store, returning something an AI agent reads easily:

```python
import data_pipeline as dp

df = dp.fetch("sharadar_prices", ["AAPL", "MSFT"])      # a pandas DataFrame, nothing stored
dp.fetch("fred", ["UNRATE"], to="unrate.parquet")       # or also straight to a file
```

and its command-line counterpart, for an agent that only runs commands:

```
data-pipeline fetch sharadar_prices AAPL MSFT --out prices.parquet
```

`fetch` knows nothing of any particular source: a source added later (such as the Sharadar
fundamentals being designed in parallel) works with it as soon as it is in the registry.

## Locked decisions

| Decision | Choice |
|---|---|
| Shape of series | Long: one row per id and period, columns `id, date, period, value` |
| Shape of tables | `id`, then the columns `Store.table` returns: key columns, `date`, value columns, attribute columns |
| Versioned tables (`sec_xbrl`) | The latest version of each key, as `Store.table` reads it |
| Documents (`sec_filings`) | Out of scope: refused before any request |
| Failures | Strict: any failed id raises `FetchError`, which carries what did arrive |
| File output | One file for all ids, `.parquet` or `.csv` by extension, replaced atomically |
| A failed fetch with `to=` | Writes what arrived, then raises |
| How it is built | Drives the source directly, as `keys_check.py` does; no store, no temporary store |
| Console without `--out` | A summary and the first 10 rows, and a hint to use `--out` |

## Python interface

`data_pipeline.fetch`, exported from the package with `FetchError`:

```python
def fetch(
    source: str,
    ids: str | Sequence[str],
    *,
    to: str | os.PathLike[str] | None = None,
    start: datetime.date | str | None = None,
    frequency: str | None = None,
    credentials: Mapping[str, str] | None = None,
    env_file: str | os.PathLike[str] | None = None,
    transport: httpx.BaseTransport | None = None,
    sleep: Callable[[float], None] = time.sleep,
    **params: object,
) -> pd.DataFrame
```

- `ids` is one id or a list of them. Repeated ids are asked for once; the order given is kept.
- `start`, `frequency` and `params` are the catalog's fields, with the catalog's meaning. They go
  through `catalog.build_entries(..., where="fetch()")`, the function `Store.add` uses, and then
  through the source's own `validate`. `start` may be a date or text such as `"2020-01-01"`;
  text is read with `datetime.date.fromisoformat` first, since `build_entries` takes a date only,
  and text it cannot read is a `CatalogError`.
- `credentials` and `env_file` mean what they mean for `Store`: keys given in code come first,
  then the process environment, then the nearest `.env` (or the file given).
- `transport` lets tests serve the answers and `sleep` replaces the wait between requests, as
  `Store(transport=..., sleep=...)` do.
- The frame is returned also when `to=` is given.

### What the frame holds

Series sources (FRED, BLS, Banxico, INEGI, World Bank, SDMX providers, DBnomics):

| Column | Meaning |
|---|---|
| `id` | The id as asked for (`UNRATE`), not the store key |
| `date` | The last day of the period |
| `period` | The canonical label: `2026-09-22`, `2026-08`, `2026Q2`, `2026` |
| `value` | A number |

As `Store.series` reads by default: periods without a value and projections are left out. Rows
are sorted by `id` in the order asked for, then by `date`.

Table sources (Sharadar prices and actions, Comtrade, SEC XBRL): `id`, then the columns
`Store.table` returns for that source, in the same order and with the same types (`id` repeats
`ticker` in the Sharadar tables; in `sec_xbrl` it is the only column that tells companies
apart). Each id's rows are those of the latest version of each key: built with
`storage.table_frame` and `storage.latest(..., by_publication=versioned)`, the functions the
store itself writes and reads with, so the two cannot drift apart. Ids follow in the order asked
for.

When nothing arrived, the frame is empty: with the four columns for a series source, with only
`id` for a table source (no row means no known schema).

### What is not offered

- Versions and `as_of`: only the store keeps them.
- Every call downloads again and spends the source's quota again. For data read repeatedly,
  the store (`sync`) is the tool.
- No daily budget: without a store there is no record of the calls made today, so `fetch` stops
  only where the source itself says the quota is spent (HTTP 429, an exhausted quota).
- No incremental download: no `since`, no `held`. Each id comes with its whole history, or from
  `start`.
- Documents.

## Command line

```
data-pipeline fetch SOURCE ID... [--out FILE] [--start YYYY-MM-DD] [--frequency M]
                                 [--param NAME=VALUE]... [--env-file PATH]
```

- `--param` is repeatable. The value is read as YAML, as the catalog is, so
  `--param annual_from=2020` gives a number and `--param partners=[USA,CAN]` a list.
- Without `--out`: one summary line (`fred: 2 ids, 914 rows`), the first 10 rows (`SHOWN_ROWS`
  of the store's `show`), and, when rows were left out,
  `[i]   the first 10 of 914 rows; use --out FILE for all of them`. With `--out`: the summary
  line and `[ok]  written to <path>`.
- `--param` cannot name one of `fetch`'s own arguments: `start`, `frequency`, `to` and
  `env_file` have options of their own; `credentials`, `transport` and `sleep` are refused.
- Failures: one line per failed id (`[x]   fred:XYZ  not_found: ...`).
- Exit codes, those of `sync`: 0 every id arrived, 1 some id failed, 2 a configuration error
  (unknown source, invalid field, documents source, unsupported extension, missing folder, a
  `--param` it cannot read), 3 the quota ran out and nothing else failed.
- Everything printed is ASCII. Data that is not ASCII reaches the file untouched; only what is
  printed is replaced.

The command lives in `src/data_pipeline/fetch_cli.py` and joins the `CommandCollection` of
`src/data_pipeline/cli.py`, as `keys_cli` does.

## How it works

The module is `src/data_pipeline/fetching.py`. It is not called `fetch.py`: importing a
submodule `data_pipeline.fetch` would set the package's attribute `fetch` to the module,
replacing the function.

1. **Check before any request.** The destination: a known extension and an existing folder.
   The entries: `build_entries`. The credentials: `credentials.resolve(...)`. The client:
   `store.http.Client` with the resolved secrets. The source: `sources.create(...)`, which
   refuses an unknown name and lists the known ones. A source of kind document is refused. Each
   entry goes through `source.validate`.
2. **Ask.** `source.fetch([Request(entry) for entry in entries])`: no `since`, no `held`, not
   `full`.
3. **Collect**, by the rules of `sync`:
   - a series: the first one received for a key counts; a failure for a key already received
     is ignored;
   - a table: every `TableData` of a key is kept (a key may come in several batches);
   - a failure: the first one for a key counts, its reason passed through `client.scrub`;
   - `QuotaExhaustedError`: the download stops; every id not yet received fails with
     `quota_exhausted`;
   - any other exception from the source: the download stops; every id not yet received fails
     with `source_error`, its reason naming the exception's type and message (scrubbed);
   - an id the source never returned fails with `not_found`.
   An id may both bring rows and fail (a table source that failed part of an id): its rows are
   in the frame and its failure is reported.
4. **Shape** the frame as described above.
5. **Write**, when `to=` is given: the frame to bytes (Parquet with `index=False`; CSV with
   `index=False`, UTF-8), then `_files.write_atomic`, the atomic replacement the store and `.env`
   already use. Also when there were failures.
6. **Raise** `FetchError` when any id failed.

### Destinations

`to=` takes a path only. Inside, a registry maps a suffix to a writer
(`.parquet`, `.csv`). The SQL destination planned next is another entry of that registry (say
`.duckdb`, its table named after the source): `fetch(..., to="data.duckdb")` and
`--out data.duckdb` then work with no change to either signature. No public `Destination`
protocol is offered now.

### Errors

```python
class FetchError(StoreError):
    data: pd.DataFrame                # what arrived, shaped as a successful fetch would be
    failures: tuple[Failure, ...]     # one per failed id: entry, outcome, reason
    quota_exhausted: bool
```

Its message starts with `sharadar_prices: 1 of 3 ids failed; the 5032 rows that arrived are in
.data` (or `nothing arrived`), followed by one line per failure:
`sharadar_prices:XYZ  not_found: ...`.

| Case | Raised | Before any request |
|---|---|---|
| Unknown source | `CatalogError`, listing the known sources | yes |
| Field the source does not accept | `CatalogError` from `validate` | yes |
| Documents source | `CatalogError`: fetch returns frames; use `Store.sync` and `Store.documents` | yes |
| Unsupported extension, missing folder | `CatalogError`, naming `.parquet` and `.csv` | yes |
| Missing key | `FetchError`, `key_error`, with `credentials.missing_message` | yes (the source asks nothing) |
| Rejected key, unknown id, network, odd answer | `FetchError` with the source's outcome | no |
| Quota | `FetchError`, `quota_exhausted=True` | no |

Catching the source's unexpected exceptions keeps what was already downloaded, as `sync` does;
the exception's type in the reason keeps a bug visible.

## Testing

Tests come first. Data is invented; no answer of any vendor is recorded.

- `tests/unit/fetching_test.py`, against fake sources: `FakeSource` of
  `tests/unit/store/helpers.py` (and a fake table source beside it) put in `sources.REGISTRY`
  with `monkeypatch`.
  - Series: the long shape, order, no missing values, no projections, repeated ids, one id as
    text.
  - Tables: `id` first, the columns of `Store.table`, a key that comes in several batches.
  - `FetchError`: partial `.data`, `.failures`, `.quota_exhausted`, ids left after a quota, an
    exception from the source, an id never returned, a reason scrubbed of the key.
  - Refused before any request, with a transport that fails the test when called: a documents
    source, an unsupported extension, a missing folder, a field the source does not accept, an
    unknown source.
  - `to=`: Parquet and CSV read back equal to the frame; an existing file replaced; a failed
    fetch writes what arrived and then raises.
- Parity with the store: the same invented answers, served by `httpx.MockTransport`, go through
  `Store.sync` then `Store.table`, and through `fetch`; the frames are equal but for `id`. For
  `sharadar_prices` and for `sec_xbrl` (the versioned one). One series source (`fred`) goes the
  whole way the same way.
- `tests/unit/fetch_cli_test.py`, with `CliRunner`: the preview and its hint, the summary with
  `--out`, `--param` read as YAML (a number and a list), exit codes 0, 1, 2 and 3, output in
  ASCII only.
- No new live test.

## Documentation

- README: a section "Fetching without a store" between "Python" and "The macro catalog": one
  Python example, one command-line example, the columns per kind of source, `FetchError`, and
  what is not offered.
- The docstring of `fetch` and the help of the command say the same, so `help(dp.fetch)` and
  `data-pipeline fetch --help` are enough for an agent.

## Out of scope

- The SQL destination (DuckDB or SQLite file): the next piece. Here only the suffix registry it
  will join.
- Documents.
- A lenient mode (`strict=False`), an `end=`, every version of a versioned table. Each can be
  added later without changing what is described here.
