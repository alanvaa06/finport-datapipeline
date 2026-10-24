# Design — SQL: `fetch` into DuckDB, and SQL over the store

Date: 2026-10-23
Status: Approved; brought up to date with what was built
Builds on: `2026-10-22-fetch-without-store-design.md` (its registry of destinations by suffix),
the store's reading rules (`Store.table`, `Store.series`, `storage.latest`, `storage.as_of`) and
the Sharadar specs (the data terms apply to anything this writes).

## Goal

Two ways to use SQL with the library, both on DuckDB:

1. `fetch` writes into a DuckDB file, one table per source, building a local database ticker by
   ticker.
2. The store answers SQL: `store.sql("SELECT ...")` and `data-pipeline sql "SELECT ..."` read the
   store's Parquet files in place, each source as its latest rows, as of a day when asked, or with
   every stored version.

## Locked decisions

| Decision | Choice |
|---|---|
| Engine | DuckDB, a regular dependency of the package (`duckdb>=2.0,<3`, the version tested) |
| `fetch` into SQL | `to="name.duckdb"` / `--out name.duckdb`, one more entry of the registry of destinations; no signature changes |
| Table of a `fetch` | Named after the source; the columns of the frame `fetch` returns |
| A second `fetch` into the same file | The rows of the ids fetched replace that source's rows of those ids, all of them (a later `start` leaves only the later days); every other row stays |
| SQL over the store | `Store.sql(query, *, as_of=None)` returns a DataFrame; `data-pipeline sql` with `--root`, `--as-of`, `--out` |
| What a query sees | Per source with data: `<source>` (the latest row of each key, or the one in force at `as_of`) and `<source>_versions` (every stored row); `store_index` (the index). A table source's views start with `id`, the id of its catalog entry |
| Writing through `sql` | Never: one `SELECT` (or `WITH ... SELECT`) per call, anything else refused before it runs; the statement runs as a subquery and the connection's settings are locked |
| `sql --out x.duckdb` | The table `result`, replaced whole; the file's other tables stay |
| A `.duckdb` file of views for other tools | Out of scope |

## `fetch` into a DuckDB file

- `fetching.py`'s registry gains `.duckdb`. Its writer opens (or creates) the file with
  `duckdb.connect(path)` and, in one transaction:
  1. creates the table `<source>` from the frame when it does not exist (a text column whose first
     values are empty is created as text);
  2. adds to the table every column of the frame it lacks (a source that gained a column);
  3. deletes the table's rows whose `id` is among the ids of this fetch;
  4. inserts the frame's rows by column name; columns of the table the frame lacks stay empty.
  If any step fails, the transaction rolls back and the file is as it was; a rollback that fails
  too never hides the first error.
- The frame's columns are the ones `fetch` already returns: series `id, date, period, value`;
  tables `id`, then the key columns, `date`, values and attributes. Types follow the frame (text,
  floating point, dates and timestamps with their zone).
- The rules of `fetch` hold: when some ids fail, what arrived is written and `FetchError` is raised;
  when nothing arrived, the file is not touched; a file that cannot be written (another program
  holds DuckDB's lock on it) is a `FetchError` that carries the rows.
- The table name is the source's name, which is already a safe identifier (letters, digits and
  `_`); it is quoted anyway.

## SQL over the store

### Interface

```python
store.sql("SELECT ticker, period, close FROM sharadar_prices WHERE period = '2026-10-22'")
store.sql("SELECT * FROM sharadar_fundamentals WHERE dimension = 'ARQ'", as_of="2024-03-01")
store.sql("SELECT * FROM sharadar_prices_versions WHERE ticker = 'AAPL'")
```

```
data-pipeline sql --root D:/data "SELECT ..." [--as-of 2024-03-01] [--out result.parquet]
```

- `as_of` takes what `Store.table(..., as_of=)` takes, with the same meaning.
- `--out` takes the destinations of `fetch` (`.parquet`, `.csv`, `.duckdb`); without it, the
  console shows a summary and the first rows, as `fetch` does. Everything printed is ASCII.
- Exit codes: 0 done, 1 `--out` could not be written, 2 a query refused or failed, a bad option,
  a `--root` that does not exist or an `--out` inside the store (nothing written).

### What a query sees

A fresh in-memory DuckDB connection per call. Before the query runs, the library defines, for
every source with stored data:

- `<source>_versions`: every stored row, read from the source's files
  (`series/<source>.parquet`, or `tables/<source>/*.parquet` with `union_by_name`, so a file
  written before a column existed reads it as missing), with `fetched_at` and `published_at`.
- `<source>`: the rows `Store.series` or `Store.table` would return for that source: the latest
  version of each key, or the one in force at `as_of`, by exactly the store's rules (versioned
  tables by publication, others by fetch; a moment as `storage.to_moment` reads it). Columns as
  `Store.table` returns them; for series, the columns `Store.series` returns plus the series key.
- `store_index`: the index, as `Store.index()` returns it.

A source without files has no names. A name that does not exist fails the query with DuckDB's
message and the list of the names defined. Both views of a source read its files through one
hidden view; a source whose files DuckDB cannot read fails the call with `StoreError` naming it.
A column of the schema that no file has reads as empty. The library's working columns start
with `__`, so a source column never clashes with them.

How the latest row is chosen in SQL (window functions over the key columns of the source's
`schema.json`, ordered as `storage.latest` and `storage.as_of` order) is checked against the
store's own functions by tests, row for row; where SQL cannot match them exactly, the view is
built from the store's functions instead.

### What `sql` refuses

The query is parsed with DuckDB's own parser before it runs. Anything but a single statement of
type `SELECT` (which includes `WITH ... SELECT`) is refused: `CREATE`, `INSERT`, `COPY ... TO`,
`ATTACH`, `INSTALL`, `SET`, several statements. DuckDB 2.0 accepts a `COPY`, `INSERT`, `UPDATE`
or `DELETE` inside a `WITH` and calls the whole statement a `SELECT`; so the statement runs as
`SELECT * FROM (<statement>)`, where DuckDB refuses a `WITH` that writes. A call to `query()` or
`json_execute_serialized_sql()` is refused before it runs, found among the statement's tokens:
both run SQL from text at the top level, where a `WITH` that writes is accepted, and `query()`
goes too because its text can spell the other's name at run time. The connection turns
off loading extensions on its own, sets its time zone, a temporary folder of its own and no
reading of Python variables, then locks its settings, so `enable_logging` and the like cannot
write files. So `sql` does not write a file or change the store. It is not a sandbox:
A `SELECT` can still read other local files (`read_csv('...')`): it runs on the user's own
machine, with the user's rights; the README says so.

## Error handling

| Case | Outcome |
|---|---|
| A query that is not a single `SELECT` | `StoreError` "only one SELECT is accepted", before it runs; exit 2 |
| A call to `query()` or `json_execute_serialized_sql()` | `StoreError`, before it runs; exit 2 |
| A query DuckDB refuses (syntax, unknown name, type) | `StoreError` with DuckDB's message and the names defined; exit 2 |
| A source whose files DuckDB cannot read | `StoreError` naming the source, for any query; exit 2 |
| `sql --root` a folder that does not exist, or `--out` inside the store | Refused, nothing written; exit 2 |
| `sql --out` a file that cannot be written | The message; exit 1 |
| An empty store | Only `store_index` is defined |
| `fetch` into a `.duckdb` another program holds | `FetchError` with the rows; the file is unchanged |

## Limits

- Each query reads the files when it runs, with no index, and defines the views of every source
  whatever it reads: two sources of 900 files each took about 1 s a query. Fine for the store of
  2026-10-23 (234 MB, about 1,760 files); a store ten times larger is not measured.
- DuckDB lets one program write a `.duckdb` file at a time.
- The refusals were checked against DuckDB 2.0's functions; a later 2.x could add a function that
  runs SQL from text, which the check would not know.
- The statement runs as a subquery, so output columns of the same name are numbered (`a`, `a_1`).
- A database built by `fetch` keeps no versions: only the store answers `as_of`.

## Testing

Temporary stores and invented data only (the Sharadar data terms apply).

- SQL against `Store.table` and `Store.series`, row for row, with and without `as_of`: a series
  source with revisions, a table not versioned, a versioned table (as `sec_xbrl` and the Sharadar
  fundamentals), a table whose files differ in columns.
- `<source>_versions` holds every stored row; `store_index` matches `Store.index()`.
- Refusals: `CREATE`, `INSERT`, `COPY ... TO`, `ATTACH`, two statements, a `WITH` that writes,
  `enable_logging`, `query()` and `json_execute_serialized_sql()` in every spelling; a refused
  query writes nothing.
- `fetch` into `.duckdb`: a first fetch creates the table; a second with other ids keeps the first
  ones; a fetch of an id already there replaces its rows; a new column; a fetch that fails for some
  ids writes what arrived; a fetch that brings nothing leaves the file alone; a locked file.
- CLI: `sql` with and without `--out`, `--as-of`, a refused query, exit codes; `fetch --out
  x.duckdb`.

## Documentation

README: `--out x.duckdb` in the `fetch` section; a section "SQL" with `store.sql`, the names a
query sees, `as_of`, what is refused and the note on reading local files. CHANGELOG.

## Out of scope

- A `.duckdb` file of views for tools outside Python (DBeaver, Excel, Power BI).
- SQLite, Postgres or any server.
- Writing to the store through SQL.
